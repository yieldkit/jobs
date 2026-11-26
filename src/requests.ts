import type { SqlClient, SqlError } from 'effect/sql';

import { Context, Duration, Effect, Schema } from 'effect';

import { serializeJsonb } from './ledger-sql.ts';
import { JobRunRecord } from './ledger-types.ts';

/**
 * Strict canonical UUID pattern checked before casting a caller-supplied `requestId` to PostgreSQL
 * `uuid`, so malformed IDs return `undefined` instead of raising a SQL syntax error.
 */
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Schema of a request row. Effect Cluster owns queueing, delivery and crash recovery; the request
 * table records which workflow execution an API request started and how it ended.
 */
export const JobRequestRecord = Schema.Struct({
  /**
   * Unique UUID assigned to this API request.
   */
  id: Schema.String,

  /**
   * Registered job name targeted by this request.
   */
  job: Schema.String,

  /**
   * JSON-encoded parameter overrides validated by the API.
   */
  params: Schema.Unknown,

  /**
   * Whether the workflow should pass `force: true` when claiming `(job, runKey)`.
   */
  force: Schema.Boolean,

  /**
   * Effect Workflow `executionId` linked to this request, or `null` when none was recorded.
   */
  executionId: Schema.NullOr(Schema.String),

  /**
   * Timestamp when the API recorded the request row.
   */
  requestedAt: Schema.Date,

  /**
   * Timestamp when the workflow started processing the request, or `null` while pending.
   */
  pickedAt: Schema.NullOr(Schema.Date),

  /**
   * Timestamp when the workflow recorded the terminal outcome, or `null` until finished.
   */
  finishedAt: Schema.NullOr(Schema.Date),

  /**
   * Ledger run key executed for this request, or `null` until it started.
   */
  runKey: Schema.NullOr(Schema.String),

  /**
   * Serialized terminal `JobOutcome` (or failure summary) written on finish, or `null`.
   */
  outcome: Schema.Unknown,
});

/**
 * Decoded request row.
 */
export type JobRequestRecord = typeof JobRequestRecord.Type;

/**
 * Input passed to `JobRequests.create`.
 */
export type CreateJobRequest = {
  /**
   * Pre-generated UUID for the request row (also used as the workflow's `attemptNonce`).
   */
  readonly id: string;

  /**
   * Registered job name.
   */
  readonly job: string;

  /**
   * JSON-safe encoded param overrides persisted in the request row.
   */
  readonly params: unknown;

  /**
   * Whether the worker should re-run an already completed key.
   */
  readonly force: boolean;

  /**
   * Deterministic Effect Workflow `executionId` dispatched for this request.
   */
  readonly executionId: string;
};

/**
 * Input passed to `JobRequests.upsertStarted` from the workflow's first activity.
 */
export type UpsertStartedJobRequest = {
  /**
   * Request UUID.
   */
  readonly id: string;

  /**
   * Registered job name.
   */
  readonly job: string;

  /**
   * Encoded parameter overrides (used only if the API crashed before inserting the row).
   */
  readonly params: unknown;

  /**
   * Force flag (used only if the API crashed before inserting the row).
   */
  readonly force: boolean;

  /**
   * Effect Workflow `executionId`.
   */
  readonly executionId: string;

  /**
   * Ledger run key claimed or targeted by the workflow.
   */
  readonly runKey: string | null;
};

/**
 * Input passed to `JobRequests.markFinished`.
 */
export type FinishJobRequest = {
  /**
   * Request UUID.
   */
  readonly id: string;

  /**
   * Ledger run key executed by the workflow, or `null` if stopped before claiming.
   */
  readonly runKey: string | null;

  /**
   * JSON-safe terminal outcome or error summary stored in the request row.
   */
  readonly outcome: unknown;
};

/**
 * Input passed to `JobRequests.markDispatchFailed` when workflow enqueue fails after inserting the
 * metadata row.
 */
export type DispatchFailedJobRequest = {
  /**
   * Request UUID.
   */
  readonly id: string;

  /**
   * Registered job name.
   */
  readonly job: string;

  /**
   * Redacted dispatch failure message.
   */
  readonly errorMessage: string;
};

/**
 * Combined status returned by `JobRequests.getStatus`: the request row paired with its matching run
 * ledger row when there is one.
 */
export type JobRequestStatus = {
  /**
   * Persisted request row.
   */
  readonly request: JobRequestRecord;

  /**
   * Ledger row of `(request.job, request.runKey)` while it belongs to the request's workflow
   * execution, or `null` while pending, absent, or held by another execution (a run that took the
   * key from it, or one that ran before it).
   */
  readonly run: JobRunRecord | null;
};

/**
 * Returns `true` when `value` matches the canonical 36-character UUID syntax accepted by
 * PostgreSQL's `uuid` type.
 */
export const isUuidString = (value: string): boolean => UUID_PATTERN.test(value);

/**
 * The client and qualified tables the SQL request store uses.
 */
export type SqlRequestsContext = {
  /**
   * Client without name transforms, sharing the caller's transactions.
   */
  readonly sql: SqlClient.SqlClient;

  /**
   * Qualified request table, such as `public.job_requests`.
   */
  readonly requestsTable: string;

  /**
   * Qualified run ledger table, joined by `getStatus`.
   */
  readonly runsTable: string;
};

/**
 * Inserts a pending request row linked to `input.executionId`.
 */
const createRequestSql = Effect.fnUntraced(function* (
  { sql, requestsTable }: SqlRequestsContext,
  input: CreateJobRequest,
): Effect.fn.Return<JobRequestRecord, SqlError.SqlError | Schema.SchemaError> {
  const paramsJson = serializeJsonb(input.params) ?? '{}';
  const rows = yield* sql`
    INSERT INTO ${sql(requestsTable)} AS existing (id, job, params, force, execution_id)
    VALUES (${input.id}::uuid, ${input.job}, ${paramsJson}::jsonb, ${input.force}, ${input.executionId})
    ON CONFLICT (id) DO UPDATE SET
      execution_id = COALESCE(existing.execution_id, EXCLUDED.execution_id)
    RETURNING
      id::text AS id,
      job,
      params,
      force,
      execution_id AS "executionId",
      requested_at AS "requestedAt",
      picked_at AS "pickedAt",
      finished_at AS "finishedAt",
      run_key AS "runKey",
      outcome
  `;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(JobRequestRecord))(rows);
  const row = decoded[0];
  if (row === undefined) {
    return yield* Effect.die(new Error('The request insert returned no row.'));
  }

  return row;
});

/**
 * Idempotently upserts the request row when the durable workflow begins executing, so it exists
 * with `picked_at` and `run_key` even if the dispatching process crashed right after dispatch. A
 * row that finished without ever being picked up holds a provisional `markDispatchFailed` outcome
 * (the dispatch reached Cluster after all), so pickup clears it.
 */
const upsertStartedSql = (
  { sql, requestsTable }: SqlRequestsContext,
  input: UpsertStartedJobRequest,
): Effect.Effect<void, SqlError.SqlError> => {
  const paramsJson = serializeJsonb(input.params) ?? '{}';
  return sql`
    INSERT INTO ${sql(requestsTable)} AS existing (id, job, params, force, execution_id, picked_at, run_key)
    VALUES (
      ${input.id}::uuid,
      ${input.job},
      ${paramsJson}::jsonb,
      ${input.force},
      ${input.executionId},
      NOW(),
      ${input.runKey}
    )
    ON CONFLICT (id) DO UPDATE SET
      execution_id = COALESCE(existing.execution_id, EXCLUDED.execution_id),
      picked_at = COALESCE(existing.picked_at, NOW()),
      run_key = COALESCE(EXCLUDED.run_key, existing.run_key),
      finished_at = CASE WHEN existing.picked_at IS NULL THEN NULL ELSE existing.finished_at END,
      outcome = CASE WHEN existing.picked_at IS NULL THEN NULL ELSE existing.outcome END
  `.pipe(Effect.asVoid);
};

/**
 * Records the terminal `run_key`, `outcome` and `finished_at` of a request that has not finished,
 * or whose only outcome is a provisional `markDispatchFailed` one (it was never picked up).
 */
const markFinishedSql = (
  { sql, requestsTable }: SqlRequestsContext,
  input: FinishJobRequest,
): Effect.Effect<boolean, SqlError.SqlError> => {
  const outcomeJson = serializeJsonb(input.outcome);
  return sql`
    UPDATE ${sql(requestsTable)}
    SET
      run_key = COALESCE(${input.runKey}, run_key),
      outcome = ${outcomeJson}::jsonb,
      picked_at = COALESCE(picked_at, NOW()),
      finished_at = NOW()
    WHERE id = ${input.id}::uuid
      AND (finished_at IS NULL OR picked_at IS NULL)
    RETURNING 1
  `.pipe(Effect.map((rows) => rows.length > 0));
};

/**
 * Marks a never-picked request row as failed when workflow dispatch throws. Leaves rows already
 * picked up by a worker untouched so a post-commit network error never erases an active run.
 */
const markDispatchFailedSql = (
  { sql, requestsTable }: SqlRequestsContext,
  input: DispatchFailedJobRequest,
): Effect.Effect<void, SqlError.SqlError> => {
  const outcomeJson = serializeJsonb({
    job: input.job,
    trigger: 'api',
    status: 'failed',
    errorTag: 'WorkflowDispatchError',
    errorMessage: input.errorMessage,
  });
  return sql`
    UPDATE ${sql(requestsTable)}
    SET
      finished_at = NOW(),
      outcome = ${outcomeJson}::jsonb
    WHERE id = ${input.id}::uuid
      AND picked_at IS NULL
      AND finished_at IS NULL
  `.pipe(Effect.asVoid);
};

/**
 * Fetches `(job, requestId)` together with its ledger row when `run_key` is set and the row belongs
 * to the request's execution. Returns `undefined` for missing rows and malformed UUIDs.
 */
const getRequestStatusSql = Effect.fnUntraced(function* (
  { sql, requestsTable, runsTable }: SqlRequestsContext,
  job: string,
  requestId: string,
): Effect.fn.Return<JobRequestStatus | undefined, SqlError.SqlError | Schema.SchemaError> {
  if (!isUuidString(requestId)) {
    return undefined;
  }

  const requestRows = yield* sql`
    SELECT
      id::text AS id,
      job,
      params,
      force,
      execution_id AS "executionId",
      requested_at AS "requestedAt",
      picked_at AS "pickedAt",
      finished_at AS "finishedAt",
      run_key AS "runKey",
      outcome
    FROM ${sql(requestsTable)}
    WHERE id = ${requestId}::uuid
      AND job = ${job}
  `;
  const decodedRequests = yield* Schema.decodeUnknownEffect(Schema.Array(JobRequestRecord))(
    requestRows,
  );
  const request = decodedRequests[0];
  if (request === undefined) {
    return undefined;
  }

  if (request.runKey === null) {
    return { request, run: null };
  }

  const runRows = yield* sql`
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
    WHERE job = ${request.job}
      AND run_key = ${request.runKey}
      AND execution_id = ${request.executionId}
  `;
  const decodedRuns = yield* Schema.decodeUnknownEffect(Schema.Array(JobRunRecord))(runRows);
  return {
    request,
    run: decodedRuns[0] ?? null,
  };
});

/**
 * Deletes up to `limit` request rows that finished more than `olderThan` ago. Unfinished rows are
 * kept. Returns the deleted row count.
 */
const pruneRequestsSql = (
  { sql, requestsTable }: SqlRequestsContext,
  olderThan: Duration.Input,
  limit: number,
): Effect.Effect<number, SqlError.SqlError> => {
  const cutoffMs = Math.max(0, Duration.toMillis(Duration.fromInputUnsafe(olderThan)));
  const batch = Math.max(1, Math.trunc(limit));
  return sql`
    DELETE FROM ${sql(requestsTable)}
    WHERE id IN (
      SELECT id
      FROM ${sql(requestsTable)}
      WHERE finished_at IS NOT NULL
        AND finished_at < NOW() - (${cutoffMs}::double precision * INTERVAL '1 millisecond')
      LIMIT ${batch}
    )
    RETURNING 1
  `.pipe(Effect.map((rows) => rows.length));
};

/**
 * Request store: links each API request to its Effect Workflow `executionId` and ledger row.
 * Queueing, delivery, retries and crash recovery are Effect Cluster's job.
 */
export class JobRequests extends Context.Service<
  JobRequests,
  {
    /**
     * Persists a pending request row linked to `executionId`.
     */
    readonly create: (
      input: CreateJobRequest,
    ) => Effect.Effect<JobRequestRecord, SqlError.SqlError | Schema.SchemaError>;

    /**
     * Idempotently marks the request as picked up and linked to `runKey`, creating the row if the
     * dispatching process crashed before persisting it.
     */
    readonly upsertStarted: (
      input: UpsertStartedJobRequest,
    ) => Effect.Effect<void, SqlError.SqlError>;

    /**
     * Records the terminal `runKey`, `outcome` and `finishedAt` of a request. Returns `false` when
     * it had already finished.
     */
    readonly markFinished: (input: FinishJobRequest) => Effect.Effect<boolean, SqlError.SqlError>;

    /**
     * Marks a never-picked request row as failed after dispatching its workflow failed.
     */
    readonly markDispatchFailed: (
      input: DispatchFailedJobRequest,
    ) => Effect.Effect<void, SqlError.SqlError>;

    /**
     * Looks up a request by `(job, requestId)` together with its ledger row, when that row belongs
     * to the request's execution. Returns `undefined` when it is missing or `requestId` is not a
     * valid UUID.
     */
    readonly getStatus: (
      job: string,
      requestId: string,
    ) => Effect.Effect<JobRequestStatus | undefined, SqlError.SqlError | Schema.SchemaError>;

    /**
     * Deletes up to `limit` rows that finished more than `olderThan` ago and returns how many it
     * deleted. Unfinished rows are kept.
     */
    readonly prune: (
      olderThan: Duration.Input,
      limit: number,
    ) => Effect.Effect<number, SqlError.SqlError>;
  }
>()('@yieldkit/jobs/JobRequests') {}

/**
 * Builds the SQL `JobRequests` service over `context`.
 */
export const makeSqlJobRequests = (context: SqlRequestsContext): JobRequests['Service'] =>
  JobRequests.of({
    create: (input) => createRequestSql(context, input),
    upsertStarted: (input) => upsertStartedSql(context, input),
    markFinished: (input) => markFinishedSql(context, input),
    markDispatchFailed: (input) => markDispatchFailedSql(context, input),
    getStatus: (job, requestId) => getRequestStatusSql(context, job, requestId),
    prune: (olderThan, limit) => pruneRequestsSql(context, olderThan, limit),
  });
