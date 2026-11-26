import type { SqlClient, SqlError } from 'effect/sql';

import { Effect, Schema } from 'effect';

import type {
  ClaimRequest,
  OwnedRun,
  RecentRunsQuery,
  RunClaim,
  RunResult,
} from './ledger-types.ts';
import type { RetryPolicy } from './policy.ts';
import type { BoundSettleHook } from './settle-hook.ts';

import { failedNextAttemptAt } from './ledger-retry.ts';
import { refusePastDeadlineSql } from './ledger-sql-deadline.ts';
import { JobRunRecord, JobRunStatus } from './ledger-types.ts';
import { decideClaim } from './policy.ts';
import { runSettleHook } from './settle-hook.ts';

/**
 * What the SQL ledger needs besides the client: the qualified table, the retry rules and the settle
 * hook (already bound to its services).
 */
export type SqlLedgerContext = {
  /**
   * Client without name transforms, sharing the caller's transactions.
   */
  readonly sql: SqlClient.SqlClient;

  /**
   * Qualified run ledger table, such as `public.job_runs`.
   */
  readonly runsTable: string;

  /**
   * Retry rules for `next_attempt_at`.
   */
  readonly retry: RetryPolicy;

  /**
   * Settle hook with its services provided, or `undefined`.
   */
  readonly onSettle: BoundSettleHook | undefined;
};

/**
 * Row schema for `INSERT ... ON CONFLICT DO NOTHING RETURNING attempt`.
 */
const ClaimAttemptRow = Schema.Struct({
  /**
   * Assigned 1-based attempt number.
   */
  attempt: Schema.Int,

  /**
   * Failures the key already had.
   */
  failures: Schema.Int,
});

/**
 * Locked existing ledger row inspected inside `claimRunSql` when the key already exists.
 */
const ExistingRunRow = Schema.Struct({
  /**
   * Current status on the existing row.
   */
  status: JobRunStatus,

  /**
   * Workflow `execution_id` recorded on the existing row, or `null`.
   */
  executionId: Schema.NullOr(Schema.String),

  /**
   * Current 1-based attempt counter.
   */
  attempt: Schema.Int,

  /**
   * `true` when `lease_expires_at < NOW()` on the database clock.
   */
  leaseExpired: Schema.Boolean,

  /**
   * Scheduled retry timestamp, or `null`.
   */
  nextAttemptAt: Schema.NullOr(Schema.Date),

  /**
   * Current database transaction timestamp `NOW()`.
   */
  now: Schema.Date,
});

/**
 * Row schema returned by the first phase of a failed settle update.
 */
const FailedSettleRow = Schema.Struct({
  /**
   * Post-increment non-interrupted failure count.
   */
  failures: Schema.Int,

  /**
   * Database transaction `NOW()` timestamp used to compute `nextAttemptAt`.
   */
  now: Schema.Date,
});

/**
 * Serializes an optional JSON-safe payload into JSON text for a `::jsonb` cast. Returns `null` for
 * `undefined` and for values `JSON.stringify` cannot represent (functions, symbols).
 */
export const serializeJsonb = (value: unknown): string | null => {
  if (value === undefined) {
    return null;
  }

  const encoded: unknown = JSON.stringify(value);
  return typeof encoded === 'string' ? encoded : null;
};

/**
 * Locks the existing `(request.job, request.runKey)` row `FOR UPDATE` inside the caller's
 * transaction and reads it with lease expiry and `NOW()` evaluated on the database clock. Returns
 * `undefined` when the row vanished between the conflicting insert and the lock.
 */
const lockExistingRunSql = (
  { sql, runsTable }: SqlLedgerContext,
  request: ClaimRequest,
): Effect.Effect<typeof ExistingRunRow.Type | undefined, SqlError.SqlError> =>
  sql`
    SELECT
      status,
      execution_id AS "executionId",
      attempt,
      (lease_expires_at < NOW()) AS "leaseExpired",
      next_attempt_at AS "nextAttemptAt",
      NOW() AS now
    FROM ${sql(runsTable)}
    WHERE job = ${request.job}
      AND run_key = ${request.runKey}
    FOR UPDATE
  `.pipe(
    Effect.flatMap((rows) =>
      Schema.decodeUnknownEffect(Schema.Array(ExistingRunRow))(rows).pipe(Effect.orDie),
    ),
    Effect.map((rows) => rows[0]),
  );

/**
 * Atomically claims or resumes `(request.job, request.runKey)` or classifies why the row could not
 * be claimed. Resuming a `running` row with the same `executionId` rotates `claim_token`, extends
 * the lease, increments `attempt`, and returns `resumed: true`. Finding a `completed` row with the
 * same `executionId` returns `AlreadySettled`. `newToken` mints the fence UUID written to
 * `claim_token`. A row that fails to decode is a bug in the SQL or the row schemas, so it dies
 * (rolling the transaction back) instead of failing with `SchemaError`.
 */
export const claimRunSql = (
  context: SqlLedgerContext,
  request: ClaimRequest,
  leaseMs: number,
  newToken: Effect.Effect<string>,
): Effect.Effect<RunClaim, SqlError.SqlError> => {
  const { sql, runsTable } = context;
  if (request.pastDeadline === true) {
    return refusePastDeadlineSql(context, request);
  }

  return sql.withTransaction(
    Effect.gen(function* () {
      const token = yield* newToken;
      const executionId = request.executionId ?? null;
      const paramsJson = serializeJsonb(request.params) ?? '{}';
      const inserted = yield* sql`
        INSERT INTO ${sql(runsTable)} (
          job,
          run_key,
          execution_id,
          status,
          claim_token,
          lease_expires_at,
          trigger,
          params
        )
        VALUES (
          ${request.job},
          ${request.runKey},
          ${executionId},
          'running',
          ${token}::uuid,
          NOW() + (${leaseMs}::double precision * INTERVAL '1 millisecond'),
          ${request.trigger},
          ${paramsJson}::jsonb
        )
        ON CONFLICT (job, run_key) DO NOTHING
        RETURNING attempt, failures
      `;
      const decodedInserted = yield* Schema.decodeUnknownEffect(Schema.Array(ClaimAttemptRow))(
        inserted,
      ).pipe(Effect.orDie);
      const created = decodedInserted[0];
      if (created !== undefined) {
        return {
          _tag: 'Acquired',
          token,
          attempt: created.attempt,
          resumed: false,
          failures: created.failures,
        };
      }

      const existing = yield* lockExistingRunSql(context, request);
      if (existing === undefined) {
        return { _tag: 'Busy' };
      }

      const decision = decideClaim(
        { ...existing, nowMs: existing.now.getTime() },
        { trigger: request.trigger, force: request.force, executionId },
      );
      if (decision._tag === 'Refuse') {
        return decision.claim;
      }

      const resumingSameExecution = decision.resumed;
      const updated = yield* sql`
        UPDATE ${sql(runsTable)}
        SET
          status = 'running',
          execution_id = ${executionId},
          claim_token = ${token}::uuid,
          lease_expires_at = NOW() + (${leaseMs}::double precision * INTERVAL '1 millisecond'),
          attempt = attempt + 1,
          next_attempt_at = NULL,
          trigger = ${request.trigger},
          params = ${paramsJson}::jsonb,
          stats = NULL,
          error_message = NULL,
          started_at = CASE WHEN ${resumingSameExecution} THEN started_at ELSE NOW() END,
          finished_at = NULL
        WHERE job = ${request.job}
          AND run_key = ${request.runKey}
        RETURNING attempt, failures
      `;
      const decodedUpdated = yield* Schema.decodeUnknownEffect(Schema.Array(ClaimAttemptRow))(
        updated,
      ).pipe(Effect.orDie);
      const claimed = decodedUpdated[0];
      if (claimed === undefined) {
        return { _tag: 'Busy' };
      }

      return {
        _tag: 'Acquired',
        token,
        attempt: claimed.attempt,
        resumed: resumingSameExecution,
        failures: claimed.failures,
      };
    }),
  );
};

/**
 * Extends the lease on an active run when its claim token still matches.
 */
export const heartbeatRunSql = (
  { sql, runsTable }: SqlLedgerContext,
  run: OwnedRun,
  leaseMs: number,
): Effect.Effect<boolean, SqlError.SqlError> =>
  sql`
    UPDATE ${sql(runsTable)}
    SET lease_expires_at = NOW() + (${leaseMs}::double precision * INTERVAL '1 millisecond')
    WHERE job = ${run.job}
      AND run_key = ${run.runKey}
      AND status = 'running'
      AND claim_token = ${run.token}::uuid
    RETURNING 1
  `.pipe(Effect.map((rows) => rows.length > 0));

/**
 * Token-fenced completion update and the settle hook in one transaction.
 */
const settleCompletedSql = (
  { sql, runsTable, onSettle }: SqlLedgerContext,
  run: OwnedRun,
  result: Extract<RunResult, { readonly _tag: 'Completed' }>,
): Effect.Effect<boolean, SqlError.SqlError> =>
  sql.withTransaction(
    Effect.gen(function* () {
      const statsJson = serializeJsonb(result.stats);
      const updated = yield* sql`
        UPDATE ${sql(runsTable)}
        SET
          status = 'completed',
          stats = ${statsJson}::jsonb,
          error_message = NULL,
          next_attempt_at = NULL,
          finished_at = NOW()
        WHERE job = ${run.job}
          AND run_key = ${run.runKey}
          AND status = 'running'
          AND claim_token = ${run.token}::uuid
        RETURNING 1
      `;
      if (updated.length === 0) {
        return false;
      }

      yield* runSettleHook(sql, onSettle, {
        job: run.job,
        runKey: run.runKey,
        attempt: run.attempt,
        trigger: result.trigger,
        annotations: result.annotations,
        outcome: { _tag: 'Completed', stats: result.stats },
      });
      return true;
    }),
  );

/**
 * Token-fenced failure update, `next_attempt_at` computation and the settle hook in one
 * transaction.
 */
const settleFailedSql = (
  { sql, runsTable, retry, onSettle }: SqlLedgerContext,
  run: OwnedRun,
  result: Extract<RunResult, { readonly _tag: 'Failed' }>,
): Effect.Effect<boolean, SqlError.SqlError> =>
  sql.withTransaction(
    Effect.gen(function* () {
      const interrupted = result.reason === 'interrupted';
      const failureIncrement = interrupted ? 0 : 1;
      const updated = yield* sql`
        UPDATE ${sql(runsTable)}
        SET
          status = 'failed',
          error_message = ${result.errorMessage},
          failures = failures + ${failureIncrement},
          finished_at = NOW()
        WHERE job = ${run.job}
          AND run_key = ${run.runKey}
          AND status = 'running'
          AND claim_token = ${run.token}::uuid
        RETURNING failures, NOW() AS now
      `;
      if (updated.length === 0) {
        return false;
      }

      const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(FailedSettleRow))(
        updated,
      ).pipe(Effect.orDie);
      const row = decoded[0];
      if (row === undefined) {
        return false;
      }

      // An exhausted key stays exhausted; otherwise a workflow's override decides.
      const retryAt = failedNextAttemptAt(retry, result, {
        runKey: run.runKey,
        failures: row.failures,
        now: row.now,
      });
      yield* sql`
        UPDATE ${sql(runsTable)}
        SET next_attempt_at = ${retryAt}::timestamptz
        WHERE job = ${run.job}
          AND run_key = ${run.runKey}
          AND claim_token = ${run.token}::uuid
      `;
      yield* runSettleHook(sql, onSettle, {
        job: run.job,
        runKey: run.runKey,
        attempt: run.attempt,
        trigger: result.trigger,
        annotations: result.annotations,
        outcome: {
          _tag: 'Failed',
          errorMessage: result.errorMessage,
          reason: result.reason,
          failures: row.failures,
          nextAttemptAt: retryAt,
        },
      });
      return true;
    }),
  );

/**
 * Dispatches `JobRuns.settle` based on the terminal `RunResult` variant. A settle that matched no
 * running row still returns `true` when `run`'s own claim already settled it the same way: a repeat
 * of a committed settle (retried after its connection dropped, say), not applied twice.
 */
export const settleRunSql = (
  context: SqlLedgerContext,
  run: OwnedRun,
  result: RunResult,
): Effect.Effect<boolean, SqlError.SqlError> => {
  const { sql, runsTable } = context;
  const status = result._tag === 'Completed' ? 'completed' : 'failed';
  const settle =
    result._tag === 'Completed'
      ? settleCompletedSql(context, run, result)
      : settleFailedSql(context, run, result);
  const settledByClaim = sql`
    SELECT 1 FROM ${sql(runsTable)}
    WHERE job = ${run.job} AND run_key = ${run.runKey}
      AND status = ${status} AND claim_token = ${run.token}::uuid
  `.pipe(Effect.map((rows) => rows.length > 0));
  return Effect.flatMap(settle, (settled) => (settled ? Effect.succeed(true) : settledByClaim));
};

/**
 * Checks whether `(job, runKey)` currently has `status = 'completed'`.
 */
export const isRunCompletedSql = (
  { sql, runsTable }: SqlLedgerContext,
  job: string,
  runKey: string,
): Effect.Effect<boolean, SqlError.SqlError> =>
  sql`
    SELECT 1
    FROM ${sql(runsTable)}
    WHERE job = ${job}
      AND run_key = ${runKey}
      AND status = 'completed'
  `.pipe(Effect.map((rows) => rows.length > 0));

/**
 * Queries recent run ledger rows ordered newest first, optionally filtered to one job. A row that
 * fails to decode as `JobRunRecord` dies: it means the table and the schema disagree, a bug rather
 * than a failure the caller can handle.
 */
export const queryRecentRunsSql = (
  { sql, runsTable }: SqlLedgerContext,
  query: RecentRunsQuery,
): Effect.Effect<ReadonlyArray<JobRunRecord>, SqlError.SqlError> => {
  const limit = Math.max(0, Math.trunc(query.limit));
  if (limit === 0) {
    return Effect.succeed([]);
  }

  const jobFilter = query.job === undefined ? sql`` : sql`WHERE job = ${query.job}`;
  return Effect.gen(function* () {
    const rows = yield* sql`
      SELECT
        job,
        run_key AS "runKey",
        execution_id AS "executionId",
        status,
        attempt,
        failures,
        next_attempt_at AS "nextAttemptAt",
        trigger,
        params,
        stats,
        error_message AS "errorMessage",
        started_at AS "startedAt",
        finished_at AS "finishedAt"
      FROM ${sql(runsTable)}
      ${jobFilter}
      ORDER BY started_at DESC, run_key DESC
      LIMIT ${limit}
    `;
    return yield* Schema.decodeUnknownEffect(Schema.Array(JobRunRecord))(rows).pipe(Effect.orDie);
  });
};
