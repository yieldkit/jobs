import type { Context, Effect } from 'effect';

import { Schema } from 'effect';

import type { DailySchedule } from './calendar.ts';
import type { JobCadence } from './job.ts';

import { JobTrigger } from './job.ts';

/**
 * Schema of the run ledger's persisted `status` values.
 */
export const JobRunStatus = Schema.Literals(['running', 'completed', 'failed']);

/**
 * Persisted status of a run ledger row.
 */
export type JobRunStatus = typeof JobRunStatus.Type;

/**
 * Result of attempting to acquire or resume a `(job, runKey)` row in the ledger.
 */
export type RunClaim =
  | {
      /**
       * Ownership acquired (or resumed for the same `executionId`) with a fresh fence `token` and
       * 1-based `attempt`.
       */
      readonly _tag: 'Acquired';

      /**
       * Fence UUID required by `heartbeat` and `settle`. Rotated on every claim/resume so a zombie
       * process sharing `executionId` cannot overwrite a newer owner. A claim reply the workflow
       * replays from its journal keeps its token: the workflow only renews that token's lease.
       */
      readonly token: string;

      /**
       * 1-based attempt number for this `(job, runKey)`.
       */
      readonly attempt: number;

      /**
       * `true` when this call resumed an unfinished `running` row carrying the same `executionId`
       * after a worker crash or shard move.
       */
      readonly resumed: boolean;

      /**
       * Failures the key already had (the ledger's `failures`), so a workflow can back off from the
       * key's count. Missing from replies journaled by earlier releases.
       */
      readonly failures?: number | undefined;
    }
  | {
      /**
       * Another worker or process currently holds a live lease on `(job, runKey)`.
       */
      readonly _tag: 'Busy';
    }
  | {
      /**
       * The key is already `completed` and `force` was `false`.
       */
      readonly _tag: 'Completed';
    }
  | {
      /**
       * The key was already settled as `completed` by this exact `executionId` (idempotent replay).
       */
      readonly _tag: 'AlreadySettled';
    }
  | {
      /**
       * The key is `failed` and scheduled retry backoff has not elapsed yet.
       */
      readonly _tag: 'RetryLater';

      /**
       * Earliest timestamp at which a scheduled run may reclaim this key.
       */
      readonly at: Date;
    }
  | {
      /**
       * The key is `failed` with `next_attempt_at IS NULL` and needs a `manual` or `api` run to
       * retry it.
       */
      readonly _tag: 'Exhausted';
    };

/**
 * Input passed to `JobRuns.claim`.
 */
export type ClaimRequest = {
  /**
   * Job name (the ledger's `job` column).
   */
  readonly job: string;

  /**
   * Deterministic cadence key (the ledger's `run_key` column).
   */
  readonly runKey: string;

  /**
   * How the run was started.
   */
  readonly trigger: JobTrigger;

  /**
   * When `true`, allows reclaiming a `completed` row owned by a different execution.
   */
  readonly force: boolean;

  /**
   * JSON-safe encoded params persisted in the ledger's `params` column.
   */
  readonly params: unknown;

  /**
   * Optional Effect Workflow `executionId` associated with this claim. When supplied, resuming an
   * active `running` row with the same `executionId` rotates `claim_token` and extends the lease
   * without failing as `Busy`, and a `completed` row with the same `executionId` returns
   * `AlreadySettled`.
   */
  readonly executionId?: string | null | undefined;

  /**
   * `true` when the run must not start, because it is a scheduled run of a `Daily` key past its
   * `until`. The ledger then takes nothing and creates no row: it marks a `failed` row exhausted
   * (`next_attempt_at = NULL`) and returns `Exhausted`, or `Completed` or `AlreadySettled` for a
   * completed row.
   */
  readonly pastDeadline?: boolean | undefined;
};

/**
 * Fenced ownership handle for an active run attempt.
 */
export type OwnedRun = {
  /**
   * Claimed job name.
   */
  readonly job: string;

  /**
   * Claimed ledger run key.
   */
  readonly runKey: string;

  /**
   * Fence UUID generated on claim.
   */
  readonly token: string;

  /**
   * 1-based attempt number assigned when claimed.
   */
  readonly attempt: number;
};

/**
 * Why a run settled as failed: the job failed or died (`error`), it was interrupted or lost its
 * claim (`interrupted`, which does not count as a failure), or one workflow execution crashed and
 * resumed more often than `policy.retry.maxCrashAttempts` (`crash-cap`).
 */
export type JobFailureReason = 'error' | 'interrupted' | 'crash-cap';

/**
 * Terminal or per-attempt outcome written by `JobRuns.settle`.
 */
export type RunResult =
  | {
      /**
       * Run succeeded and produced JSON-safe `stats`.
       */
      readonly _tag: 'Completed';

      /**
       * Encoded completion stats stored in the ledger's `stats` column.
       */
      readonly stats: unknown;

      /**
       * Trigger of the run, passed on to the `onSettle` hook.
       */
      readonly trigger: JobTrigger;

      /**
       * The job definition's annotations, passed on to the `onSettle` hook.
       */
      readonly annotations: Context.Context<never>;
    }
  | {
      /**
       * Run failed, died, was interrupted, or lost its lease.
       */
      readonly _tag: 'Failed';

      /**
       * Redacted failure summary stored in the ledger's `error_message` column.
       */
      readonly errorMessage: string;

      /**
       * Why the run failed. `interrupted` does not increment `failures`.
       */
      readonly reason: JobFailureReason;

      /**
       * Cadence tag used by `nextAttemptAt` to compute backoff or exhaustion.
       */
      readonly cadence: JobCadence['_tag'];

      /**
       * Failures after which `nextAttemptAt` exhausts the key, in place of the policy's budget for
       * the cadence. `Infinity` for a `Daily` job with a deadline.
       */
      readonly maxFailures?: number | undefined;

      /**
       * Deadline of a `Daily` job with `at`, `window` or `until`: a failure settled after `until`
       * on the run key's local date leaves the key exhausted, whatever the trigger.
       */
      readonly deadline?: DailySchedule | undefined;

      /**
       * Trigger of the run, passed on to the `onSettle` hook.
       */
      readonly trigger: JobTrigger;

      /**
       * The job definition's annotations, passed on to the `onSettle` hook.
       */
      readonly annotations: Context.Context<never>;

      /**
       * Explicit `next_attempt_at`, for example when a workflow schedules a durable retry or forces
       * exhaustion. Omit to compute it with `nextAttemptAt`. Ignored when `nextAttemptAt` says the
       * key is exhausted or `deadline` has passed, which always stores `null`.
       */
      readonly nextAttemptAtOverride?: Date | null | undefined;
    };

/**
 * Facts about one run that just settled, handed to the storage layer's `onSettle` hook.
 */
export type JobSettled = {
  /**
   * Job name.
   */
  readonly job: string;

  /**
   * Ledger run key.
   */
  readonly runKey: string;

  /**
   * 1-based attempt number of the settled run.
   */
  readonly attempt: number;

  /**
   * How the run was started.
   */
  readonly trigger: JobTrigger;

  /**
   * The job definition's `annotations`; read them with `Context.getOption`.
   */
  readonly annotations: Context.Context<never>;

  /**
   * What the run produced.
   */
  readonly outcome:
    | {
        /**
         * The run completed.
         */
        readonly _tag: 'Completed';

        /**
         * Encoded completion stats, as stored in the ledger.
         */
        readonly stats: unknown;
      }
    | {
        /**
         * The run failed.
         */
        readonly _tag: 'Failed';

        /**
         * Redacted failure summary, as stored in the ledger.
         */
        readonly errorMessage: string;

        /**
         * Why the run failed.
         */
        readonly reason: JobFailureReason;

        /**
         * Failures of this run key after the settle; interruptions do not count.
         */
        readonly failures: number;

        /**
         * When a scheduled run may retry the key, or `null` when it is exhausted.
         */
        readonly nextAttemptAt: Date | null;
      };
};

/**
 * Hook called once per successful fenced settle, inside the settle transaction. Its writes through
 * the same `SqlClient` commit or roll back with the settle; a failure is rolled back on its own and
 * logged, and the settle still commits.
 */
export type JobSettleHook<R = never> = (event: JobSettled) => Effect.Effect<void, unknown, R>;

/**
 * Schema of a run ledger row.
 */
export const JobRunRecord = Schema.Struct({
  /**
   * Job identifier.
   */
  job: Schema.String,

  /**
   * Cadence ledger key (`YYYY-MM-DD`, `YYYY-MM-DDTHH:MMZ`, or `'once'`).
   */
  runKey: Schema.String,

  /**
   * Effect Workflow `executionId` linked to the latest claim, or `null` for runs outside a
   * workflow.
   */
  executionId: Schema.NullOr(Schema.String),

  /**
   * Current ledger status (`'running'`, `'completed'`, or `'failed'`).
   */
  status: JobRunStatus,

  /**
   * 1-based attempt number for this `(job, runKey)`.
   */
  attempt: Schema.Int,

  /**
   * Count of non-interrupted failures accumulated on this key.
   */
  failures: Schema.Int,

  /**
   * Earliest scheduled retry timestamp, or `null` when completed, running, or exhausted.
   */
  nextAttemptAt: Schema.NullOr(Schema.Date),

  /**
   * Trigger of the most recent claim attempt.
   */
  trigger: JobTrigger,

  /**
   * Encoded JSON parameter payload from the most recent claim.
   */
  params: Schema.Unknown,

  /**
   * Encoded JSON stats payload from the last completion, or `null`.
   */
  stats: Schema.Unknown,

  /**
   * Last recorded failure message, or `null`.
   */
  errorMessage: Schema.NullOr(Schema.String),

  /**
   * When the execution holding the key first claimed it. A claim by the same workflow execution
   * resuming the key keeps this value; a claim by any other run sets it to the claim time.
   */
  startedAt: Schema.Date,

  /**
   * Completion or failure timestamp of the latest attempt, or `null` while running.
   */
  finishedAt: Schema.NullOr(Schema.Date),
});

/**
 * Decoded run ledger row.
 */
export type JobRunRecord = typeof JobRunRecord.Type;

/**
 * Query filter for `JobRuns.recent`.
 */
export type RecentRunsQuery = {
  /**
   * Optional job name filter; omit to query across all jobs.
   */
  readonly job?: string | undefined;

  /**
   * Maximum number of rows returned, ordered newest first.
   */
  readonly limit: number;
};
