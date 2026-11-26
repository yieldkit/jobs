import type { SqlError } from 'effect/sql';

import { Context, Duration, Effect, Schema } from 'effect';

import type { SqlLedgerContext } from './ledger-sql.ts';
import type {
  ClaimRequest,
  JobRunRecord,
  OwnedRun,
  RecentRunsQuery,
  RunClaim,
  RunResult,
} from './ledger-types.ts';
import type { JobsPolicy } from './policy.ts';

import {
  claimRunSql,
  heartbeatRunSql,
  isRunCompletedSql,
  queryRecentRunsSql,
  settleRunSql,
} from './ledger-sql.ts';

/**
 * Run ledger: `(job, run_key)` leases, fenced settles with the `onSettle` hook, history queries and
 * pruning. Built by `JobStorage.layerStorage` on PostgreSQL or by `makeJobRunsMemory` in tests.
 */
export class JobRuns extends Context.Service<
  JobRuns,
  {
    /**
     * Claims `(request.job, request.runKey)` or returns why an existing row blocked it. A ledger
     * row that fails to decode is a defect, not a typed failure.
     */
    readonly claim: (request: ClaimRequest) => Effect.Effect<RunClaim, SqlError.SqlError>;

    /**
     * Extends the lease. Returns `false` when another attempt owns the key now.
     */
    readonly heartbeat: (run: OwnedRun) => Effect.Effect<boolean, SqlError.SqlError>;

    /**
     * Fenced settle plus the `onSettle` hook in one transaction. Returns `false`, without calling
     * the hook, when the claim was lost. Repeating a settle that already committed under the same
     * token returns `true` again without applying it or calling the hook twice.
     */
    readonly settle: (
      run: OwnedRun,
      result: RunResult,
    ) => Effect.Effect<boolean, SqlError.SqlError>;

    /**
     * Returns `true` when `(job, runKey)` is stored with `status = 'completed'`.
     */
    readonly isCompleted: (
      job: string,
      runKey: string,
    ) => Effect.Effect<boolean, SqlError.SqlError>;

    /**
     * Returns recent ledger rows ordered newest first. A row that fails to decode as `JobRunRecord`
     * is a defect, not a typed failure.
     */
    readonly recent: (
      query: RecentRunsQuery,
    ) => Effect.Effect<ReadonlyArray<JobRunRecord>, SqlError.SqlError>;

    /**
     * Deletes rows of `jobs` that started more than `olderThan` ago, whatever their status, in
     * batches of `policy.maintenance.batchSize`. Returns the deleted row count.
     */
    readonly prune: (
      jobs: ReadonlyArray<string>,
      olderThan: Duration.Input,
    ) => Effect.Effect<number, SqlError.SqlError>;

    /**
     * Lease written on claim and heartbeat (`policy.lease.duration`).
     */
    readonly leaseDuration: Duration.Duration;

    /**
     * The resolved policy this ledger was built with. Workflows, the scheduler and `runJob` read
     * their retry, lease and maintenance settings from here.
     */
    readonly policy: JobsPolicy;
  }
>()('@yieldkit/jobs/JobRuns') {}

/**
 * Row returned by one batch of `pruneRunsSql`.
 */
const PrunedBatchRow = Schema.Struct({
  /**
   * Rows the batch deleted, counted by the database.
   */
  deleted: Schema.Int,
});

/**
 * Deletes rows belonging to `jobs` whose `started_at` is older than `olderThan`, whatever their
 * status, `batchSize` rows per statement until a batch deletes fewer, so no single statement holds
 * locks on or returns every old row. Returns the total count.
 */
const pruneRunsSql = (
  { sql, runsTable }: SqlLedgerContext,
  jobs: ReadonlyArray<string>,
  olderThan: Duration.Input,
  batchSize: number,
): Effect.Effect<number, SqlError.SqlError> =>
  Effect.gen(function* () {
    const cutoffMs = Math.max(0, Duration.toMillis(Duration.fromInputUnsafe(olderThan)));
    const batch = Math.max(1, Math.trunc(batchSize));
    const jobNames = [...jobs];
    const eligible = sql`job = ANY(${jobNames}::text[])
      AND started_at < NOW() - (${cutoffMs}::double precision * INTERVAL '1 millisecond')`;
    let total = 0;
    while (jobNames.length > 0) {
      const rows = yield* sql`
        WITH pruned AS (
          DELETE FROM ${sql(runsTable)}
          WHERE ${eligible}
            AND (job, run_key) IN (
              SELECT job, run_key FROM ${sql(runsTable)}
              WHERE ${eligible}
              LIMIT ${batch}
            )
          RETURNING 1
        )
        SELECT count(*)::int AS deleted FROM pruned
      `;
      const [row] = yield* Schema.decodeUnknownEffect(Schema.Array(PrunedBatchRow))(rows).pipe(
        Effect.orDie,
      );
      const deleted = row?.deleted ?? 0;
      total += deleted;
      if (deleted < batch) {
        break;
      }
    }

    return total;
  });

/**
 * Builds the SQL `JobRuns` service over `context`, minting claim tokens with `newToken`.
 */
export const makeSqlJobRuns = (
  context: SqlLedgerContext,
  policy: JobsPolicy,
  newToken: Effect.Effect<string>,
): JobRuns['Service'] => {
  const leaseMs = Math.max(1, Duration.toMillis(policy.lease.duration));
  return JobRuns.of({
    claim: (request) => claimRunSql(context, request, leaseMs, newToken),
    heartbeat: (run) => heartbeatRunSql(context, run, leaseMs),
    settle: (run, result) => settleRunSql(context, run, result),
    isCompleted: (job, runKey) => isRunCompletedSql(context, job, runKey),
    recent: (query) => queryRecentRunsSql(context, query),
    prune: (jobs, olderThan) =>
      pruneRunsSql(context, jobs, olderThan, policy.maintenance.batchSize),
    leaseDuration: policy.lease.duration,
    policy,
  });
};
