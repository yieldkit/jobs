import { Duration, Effect } from 'effect';

import type { JobCadence } from './job.ts';
import type { ClaimRequest, JobRunStatus, RunClaim } from './ledger-types.ts';

import { ClusterConfigError } from './errors.ts';

/**
 * Lease and heartbeat timing of a claimed run.
 */
export type LeasePolicy = {
  /**
   * Lease written on claim and extended by every heartbeat. A run whose lease expires may be taken
   * over by another worker. Default 2 minutes.
   */
  readonly duration: Duration.Duration;

  /**
   * Heartbeats per lease: the heartbeat interval is `duration / heartbeatsPerLease`. Default 4
   * (every 30 seconds). Must be an integer of at least 2, so a renewal has time to complete before
   * the lease it extends runs out; a smaller value fails policy resolution with
   * `ClusterConfigError`.
   */
  readonly heartbeatsPerLease: number;
};

/**
 * Retry and backoff rules for failed and blocked runs.
 */
export type RetryPolicy = {
  /**
   * Failures (interruptions excluded) of one `Daily` run key after which scheduled runs stop
   * retrying it (`Exhausted`) until a `manual` or `api` run takes it. Default 6.
   */
  readonly dailyMaxFailures: number;

  /**
   * Backoff after the first failure of an automatic `Once` job, doubled per further failure.
   * Default 1 hour.
   */
  readonly onceBase: Duration.Duration;

  /**
   * Longest backoff between `Once` attempts. Default 24 hours.
   */
  readonly onceCap: Duration.Duration;

  /**
   * Backoff after the first failure of an `OnDemand` run, doubled per further failure. Default 1
   * minute.
   */
  readonly onDemandBase: Duration.Duration;

  /**
   * Longest backoff between `OnDemand` attempts. Default 1 hour.
   */
  readonly onDemandCap: Duration.Duration;

  /**
   * Failures of one `OnDemand` run key after which its dispatched workflow gives up (`Exhausted`).
   * Default 5.
   */
  readonly onDemandMaxFailures: number;

  /**
   * Durable wait before a scheduled `Daily`, automatic `Once` or dispatched `OnDemand` workflow
   * tries again after finding the key `Busy`. Default 1 minute.
   */
  readonly busyDelay: Duration.Duration;

  /**
   * `Busy` waits a scheduled `Daily` or dispatched `OnDemand` workflow makes before it settles as
   * skipped. Automatic `Once` workflows wait without limit. Default 15.
   */
  readonly busyMaxRetries: number;

  /**
   * Times one workflow execution may claim its own `running` row again after a crash before it
   * settles as failed with `MaxAttemptsExceeded`. A run whose claim reply is already stored resumes
   * without a limit. Default 3.
   */
  readonly maxCrashAttempts: number;
};

/**
 * How long the maintenance loop keeps finished data.
 */
export type RetentionPolicy = {
  /**
   * Ledger rows of `Every` and `Cron` jobs. `Daily` and `Once` rows are kept. Default 14 days.
   */
  readonly everyRuns: Duration.Duration;

  /**
   * Processed Cluster messages of `Every` and `Cron` workflows, their crons and durable clocks.
   * Default 14 days.
   */
  readonly everyCluster: Duration.Duration;

  /**
   * Processed Cluster messages of `Daily` workflows and their crons. `Once` workflows are never
   * pruned, so a completed `Once` job stays deduplicated. Default 30 days.
   */
  readonly dailyCluster: Duration.Duration;

  /**
   * Ledger rows and processed Cluster messages of `OnDemand` jobs. A run key deduplicates for this
   * long after its run started. Default 30 days.
   */
  readonly onDemand: Duration.Duration;

  /**
   * Finished request rows (`JobRequests`). Unfinished rows are kept. Default 30 days.
   */
  readonly requests: Duration.Duration;
};

/**
 * When the maintenance loop runs and how much it deletes per statement.
 */
export type MaintenancePolicy = {
  /**
   * Time between maintenance sweeps; the first one runs when the maintenance singleton starts.
   * Default 1 day.
   */
  readonly interval: Duration.Duration;

  /**
   * Rows deleted per statement when pruning ledger rows, Cluster messages and request rows. Default 1000.
   */
  readonly batchSize: number;
};

/**
 * Every tunable timing and limit of the run ledger, the workflows and the maintenance loop. Given
 * once to the storage layer and read by everything else from `JobRuns.policy`, so the ledger's
 * retry arithmetic and the workflows' durable sleeps always agree.
 */
export type JobsPolicy = {
  /**
   * Lease and heartbeat timing.
   */
  readonly lease: LeasePolicy;

  /**
   * Retry and backoff rules.
   */
  readonly retry: RetryPolicy;

  /**
   * Data retention of the maintenance loop.
   */
  readonly retention: RetentionPolicy;

  /**
   * Schedule and batch size of the maintenance loop.
   */
  readonly maintenance: MaintenancePolicy;
};

/**
 * Overrides of one policy group: durations accept any `Duration.Input`.
 */
type GroupOverrides<Group> = {
  readonly [Key in keyof Group]?: Group[Key] extends Duration.Duration
    ? Duration.Input | undefined
    : Group[Key] | undefined;
};

/**
 * Partial policy merged over `defaultJobsPolicy` group by group: an omitted group or field keeps
 * its default.
 */
export type JobsPolicyOverrides = {
  readonly [Group in keyof JobsPolicy]?: GroupOverrides<JobsPolicy[Group]> | undefined;
};

/**
 * The defaults: a 2-minute lease with 4 heartbeats, 6 `Daily` failures, `Once` backoff from 1 to 24
 * hours, `OnDemand` backoff from 1 minute to 1 hour over 5 failures, 1-minute `Busy` waits (15 for
 * `Daily` and `OnDemand`), 3 crash resumes, 14/14/30/30/30 days of retention and a daily
 * maintenance sweep deleting 1000 rows per statement.
 */
export const defaultJobsPolicy: JobsPolicy = {
  /**
   * 2-minute lease renewed four times per lease.
   */
  lease: { duration: Duration.minutes(2), heartbeatsPerLease: 4 },

  /**
   * Retry limits and backoff.
   */
  retry: {
    dailyMaxFailures: 6,
    onceBase: Duration.hours(1),
    onceCap: Duration.hours(24),
    onDemandBase: Duration.minutes(1),
    onDemandCap: Duration.hours(1),
    onDemandMaxFailures: 5,
    busyDelay: Duration.minutes(1),
    busyMaxRetries: 15,
    maxCrashAttempts: 3,
  },

  /**
   * Retention windows of the maintenance loop.
   */
  retention: {
    everyRuns: Duration.days(14),
    everyCluster: Duration.days(14),
    dailyCluster: Duration.days(30),
    onDemand: Duration.days(30),
    requests: Duration.days(30),
  },

  /**
   * Daily sweep, 1000 rows per delete.
   */
  maintenance: { interval: Duration.days(1), batchSize: 1000 },
};

/**
 * Default `checkEvery` of a `Daily` cadence.
 */
export const DEFAULT_DAILY_CHECK_EVERY: Duration.Input = '1 hour';

/**
 * Default `catchUpWithin` of a `Cron` cadence.
 */
export const DEFAULT_CRON_CATCH_UP: Duration.Input = '1 hour';

/**
 * Shards per shard group of `makeTestClusterLayer`. Your own Cluster sets its own `shardsPerGroup`.
 */
export const DEFAULT_SHARDS_PER_GROUP = 16;

/**
 * Longest duration any policy field accepts: ten years, so every retry instant, lease and cutoff
 * computed from one stays a valid date.
 */
const MAX_POLICY_DURATION = Duration.days(3650);

/**
 * Fewest heartbeats per lease: with one, the renew loop sleeps the whole lease and has no time left
 * to renew it before it runs out.
 */
const MIN_HEARTBEATS_PER_LEASE = 2;

/**
 * Resolves a duration override, failing with `ClusterConfigError` unless it is above zero and at
 * most ten years.
 */
const positiveDuration = (
  name: string,
  input: Duration.Input | undefined,
  fallback: Duration.Duration,
): Effect.Effect<Duration.Duration, ClusterConfigError> => {
  if (input === undefined) {
    return Effect.succeed(fallback);
  }

  const decoded = Duration.fromInput(input);
  if (
    decoded._tag === 'None' ||
    !Duration.isFinite(decoded.value) ||
    Duration.toMillis(decoded.value) <= 0 ||
    Duration.isGreaterThan(decoded.value, MAX_POLICY_DURATION)
  ) {
    return Effect.fail(
      new ClusterConfigError({
        message: `policy.${name} must be a duration above zero and at most 3650 days.`,
      }),
    );
  }

  return Effect.succeed(decoded.value);
};

/**
 * Resolves a count override, failing with `ClusterConfigError` unless it is an integer of at least
 * `minimum` (1 when omitted).
 */
const positiveInteger = (
  name: string,
  input: number | undefined,
  fallback: number,
  minimum = 1,
): Effect.Effect<number, ClusterConfigError> => {
  if (input === undefined) {
    return Effect.succeed(fallback);
  }

  if (!Number.isSafeInteger(input) || input < minimum) {
    return Effect.fail(
      new ClusterConfigError({
        message: `policy.${name} must be an integer of at least ${minimum}.`,
      }),
    );
  }

  return Effect.succeed(input);
};

/**
 * Resolves the `lease` group over its defaults.
 */
const resolveLease = Effect.fnUntraced(function* (
  overrides: GroupOverrides<LeasePolicy> | undefined,
): Effect.fn.Return<LeasePolicy, ClusterConfigError> {
  const defaults = defaultJobsPolicy.lease;
  return {
    duration: yield* positiveDuration('lease.duration', overrides?.duration, defaults.duration),
    heartbeatsPerLease: yield* positiveInteger(
      'lease.heartbeatsPerLease',
      overrides?.heartbeatsPerLease,
      defaults.heartbeatsPerLease,
      MIN_HEARTBEATS_PER_LEASE,
    ),
  };
});

/**
 * Resolves the `retry` group over its defaults.
 */
const resolveRetry = Effect.fnUntraced(function* (
  overrides: GroupOverrides<RetryPolicy> | undefined,
): Effect.fn.Return<RetryPolicy, ClusterConfigError> {
  const defaults = defaultJobsPolicy.retry;
  return {
    dailyMaxFailures: yield* positiveInteger(
      'retry.dailyMaxFailures',
      overrides?.dailyMaxFailures,
      defaults.dailyMaxFailures,
    ),
    onceBase: yield* positiveDuration('retry.onceBase', overrides?.onceBase, defaults.onceBase),
    onceCap: yield* positiveDuration('retry.onceCap', overrides?.onceCap, defaults.onceCap),
    onDemandBase: yield* positiveDuration(
      'retry.onDemandBase',
      overrides?.onDemandBase,
      defaults.onDemandBase,
    ),
    onDemandCap: yield* positiveDuration(
      'retry.onDemandCap',
      overrides?.onDemandCap,
      defaults.onDemandCap,
    ),
    onDemandMaxFailures: yield* positiveInteger(
      'retry.onDemandMaxFailures',
      overrides?.onDemandMaxFailures,
      defaults.onDemandMaxFailures,
    ),
    busyDelay: yield* positiveDuration('retry.busyDelay', overrides?.busyDelay, defaults.busyDelay),
    busyMaxRetries: yield* positiveInteger(
      'retry.busyMaxRetries',
      overrides?.busyMaxRetries,
      defaults.busyMaxRetries,
    ),
    maxCrashAttempts: yield* positiveInteger(
      'retry.maxCrashAttempts',
      overrides?.maxCrashAttempts,
      defaults.maxCrashAttempts,
    ),
  };
});

/**
 * Resolves the `retention` group over its defaults.
 */
const resolveRetention = Effect.fnUntraced(function* (
  overrides: GroupOverrides<RetentionPolicy> | undefined,
): Effect.fn.Return<RetentionPolicy, ClusterConfigError> {
  const defaults = defaultJobsPolicy.retention;
  return {
    everyRuns: yield* positiveDuration(
      'retention.everyRuns',
      overrides?.everyRuns,
      defaults.everyRuns,
    ),
    everyCluster: yield* positiveDuration(
      'retention.everyCluster',
      overrides?.everyCluster,
      defaults.everyCluster,
    ),
    dailyCluster: yield* positiveDuration(
      'retention.dailyCluster',
      overrides?.dailyCluster,
      defaults.dailyCluster,
    ),
    onDemand: yield* positiveDuration('retention.onDemand', overrides?.onDemand, defaults.onDemand),
    requests: yield* positiveDuration('retention.requests', overrides?.requests, defaults.requests),
  };
});

/**
 * Resolves the `maintenance` group over its defaults.
 */
const resolveMaintenance = Effect.fnUntraced(function* (
  overrides: GroupOverrides<MaintenancePolicy> | undefined,
): Effect.fn.Return<MaintenancePolicy, ClusterConfigError> {
  const defaults = defaultJobsPolicy.maintenance;
  return {
    interval: yield* positiveDuration(
      'maintenance.interval',
      overrides?.interval,
      defaults.interval,
    ),
    batchSize: yield* positiveInteger(
      'maintenance.batchSize',
      overrides?.batchSize,
      defaults.batchSize,
    ),
  };
});

/**
 * Merges `overrides` over `defaultJobsPolicy` group by group and validates every value: durations
 * must be above zero and at most ten years (3650 days), counts integers of at least 1. Fails with
 * `ClusterConfigError` naming the first invalid field.
 */
export const resolveJobsPolicy = Effect.fnUntraced(function* (
  overrides: JobsPolicyOverrides | undefined,
): Effect.fn.Return<JobsPolicy, ClusterConfigError> {
  return {
    lease: yield* resolveLease(overrides?.lease),
    retry: yield* resolveRetry(overrides?.retry),
    retention: yield* resolveRetention(overrides?.retention),
    maintenance: yield* resolveMaintenance(overrides?.maintenance),
  };
});

/**
 * Heartbeat interval for `lease`: `duration / heartbeatsPerLease`, at least 1 millisecond. 30
 * seconds with the defaults.
 */
export const heartbeatIntervalFor = (lease: LeasePolicy): Duration.Duration => {
  const leaseMs = Math.max(1, Duration.toMillis(lease.duration));
  return Duration.millis(Math.max(1, Math.floor(leaseMs / lease.heartbeatsPerLease)));
};

/**
 * Backoff of an automatic `Once` job after its `failures`-th failure (1-based): `min(onceBase *
 * 2^(failures - 1), onceCap)`.
 */
export const onceRetryDelay = (retry: RetryPolicy, failures: number): Duration.Duration => {
  const exponent = Math.max(0, failures - 1);
  const delayMs = Math.min(
    Duration.toMillis(retry.onceBase) * 2 ** exponent,
    Duration.toMillis(retry.onceCap),
  );
  return Duration.millis(delayMs);
};

/**
 * Backoff of an `OnDemand` run after its `failures`-th failure (1-based): `min(onDemandBase *
 * 2^(failures - 1), onDemandCap)`.
 */
export const onDemandRetryDelay = (retry: RetryPolicy, failures: number): Duration.Duration => {
  const exponent = Math.max(0, failures - 1);
  const delayMs = Math.min(
    Duration.toMillis(retry.onDemandBase) * 2 ** exponent,
    Duration.toMillis(retry.onDemandCap),
  );
  return Duration.millis(delayMs);
};

/**
 * Facts `nextAttemptAt` decides from.
 */
export type NextAttemptInput = {
  /**
   * Cadence of the job.
   */
  readonly cadence: JobCadence['_tag'];

  /**
   * Failures of the run key after the settle (incremented for real failures, unchanged for
   * interruptions).
   */
  readonly failures: number;

  /**
   * Whether the run was interrupted or lost its claim.
   */
  readonly interrupted: boolean;

  /**
   * Settle time.
   */
  readonly now: Date;

  /**
   * Failures after which the key is exhausted, in place of the policy's budget for the cadence.
   * `Infinity` for a `Daily` job with a deadline, whose workflow stops retrying at `until`.
   */
  readonly maxFailures?: number | undefined;
};

/**
 * Computes the ledger's `next_attempt_at` after a failed run settles:
 *
 * - `Daily` and `OnDemand`: `null` (exhausted) once `failures` reaches the budget
 *   (`retry.dailyMaxFailures`, `retry.onDemandMaxFailures` or `maxFailures`), also after an
 *   interruption, so an interrupted run that took an exhausted key leaves it exhausted
 * - Other interrupted (or lost-claim) runs: `now`, without backoff and without using up the budget
 * - `Every` and `Cron`: `now` (the next tick uses a new run key anyway)
 * - `Once`: `now + onceRetryDelay(retry, failures)` (never exhausts)
 * - `OnDemand`: `now + onDemandRetryDelay(retry, failures)` while `failures <
 *   retry.onDemandMaxFailures`, otherwise `null`
 */
export const nextAttemptAt = (
  retry: RetryPolicy,
  { cadence, failures, interrupted, now, maxFailures }: NextAttemptInput,
): Date | null => {
  if (cadence === 'OnDemand' && failures >= (maxFailures ?? retry.onDemandMaxFailures)) {
    return null;
  }

  if (interrupted && cadence !== 'Daily') {
    return new Date(now.getTime());
  }

  switch (cadence) {
    case 'Daily':
      return failures < (maxFailures ?? retry.dailyMaxFailures) ? new Date(now.getTime()) : null;
    case 'Every':
    case 'Cron':
      return new Date(now.getTime());
    case 'OnDemand':
      return new Date(now.getTime() + Duration.toMillis(onDemandRetryDelay(retry, failures)));

    case 'Once': {
      const delayMs = Duration.toMillis(onceRetryDelay(retry, failures));
      return new Date(now.getTime() + delayMs);
    }

    default: {
      const unexpected: never = cadence;
      return unexpected;
    }
  }
};

/**
 * Snapshot of an existing `job_runs` row as seen by `JobRuns.claim`, with lease expiry and the
 * current time already evaluated against the ledger's own clock (database `NOW()` for SQL, Effect
 * `Clock` for memory).
 */
export type ExistingRunSnapshot = {
  /**
   * Current status of the row.
   */
  readonly status: JobRunStatus;

  /**
   * Workflow `executionId` recorded on the row, or `null`.
   */
  readonly executionId: string | null;

  /**
   * Scheduled retry instant for a `failed` row, or `null` when exhausted or not failed.
   */
  readonly nextAttemptAt: Date | null;

  /**
   * Whether the row's lease ended before `nowMs`.
   */
  readonly leaseExpired: boolean;

  /**
   * Current time in epoch milliseconds on the ledger's clock.
   */
  readonly nowMs: number;
};

/**
 * Outcome of `decideClaim`: either take the row over (`resumed` when the same execution continues
 * its own `running` row) or refuse with the `RunClaim` the caller returns as-is.
 */
export type ClaimDecision =
  | {
      /**
       * The caller may rotate the claim token and take the row.
       */
      readonly _tag: 'Take';

      /**
       * `true` when the same `executionId` resumes its own unfinished `running` row.
       */
      readonly resumed: boolean;
    }
  | {
      /**
       * The row must not be taken.
       */
      readonly _tag: 'Refuse';

      /**
       * Non-acquired claim result explaining the refusal.
       */
      readonly claim: Exclude<RunClaim, { readonly _tag: 'Acquired' }>;
    };

/**
 * Classifies why `existing` cannot be claimed, from its status alone.
 */
const classifyRefusal = (
  existing: ExistingRunSnapshot,
): Exclude<RunClaim, { readonly _tag: 'Acquired' }> => {
  switch (existing.status) {
    case 'completed':
      return { _tag: 'Completed' };
    case 'running':
      return { _tag: 'Busy' };
    case 'failed':
      return existing.nextAttemptAt === null
        ? { _tag: 'Exhausted' }
        : { _tag: 'RetryLater', at: existing.nextAttemptAt };
    default: {
      const unexpected: never = existing.status;
      return unexpected;
    }
  }
};

/**
 * Shared claim rule for every `JobRuns` implementation. A `completed` row settled by the same
 * execution is `AlreadySettled`. Otherwise the row is taken when the same execution resumes its own
 * `running` row, when a `failed` row is retried manually or its backoff elapsed, when `force`
 * re-runs a `completed` row, or when a `running` row's lease expired; every other case is refused
 * with the reason from the row's status.
 */
export const decideClaim = (
  existing: ExistingRunSnapshot,
  request: Pick<ClaimRequest, 'trigger' | 'force'> & { readonly executionId: string | null },
): ClaimDecision => {
  const sameExecution =
    request.executionId !== null && existing.executionId === request.executionId;
  if (existing.status === 'completed' && sameExecution) {
    return { _tag: 'Refuse', claim: { _tag: 'AlreadySettled' } };
  }

  const resumed = existing.status === 'running' && sameExecution;
  const canTakeFailed =
    existing.status === 'failed' &&
    (request.trigger !== 'schedule' ||
      (existing.nextAttemptAt !== null && existing.nextAttemptAt.getTime() <= existing.nowMs));
  const canTakeCompleted = request.force && existing.status === 'completed';
  const canTakeExpired = existing.status === 'running' && existing.leaseExpired;
  if (resumed || canTakeFailed || canTakeCompleted || canTakeExpired) {
    return { _tag: 'Take', resumed };
  }

  return { _tag: 'Refuse', claim: classifyRefusal(existing) };
};
