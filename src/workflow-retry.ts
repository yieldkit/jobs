import type { WorkflowEngine } from 'effect/workflow';

import { Clock, Duration, Effect, Result, Schema } from 'effect';
import { Activity, DurableClock } from 'effect/workflow';

import type { AnyJobDefinition, JobIdentity } from './job.ts';
import type { JobRuns } from './ledger.ts';
import type { RetryPolicy } from './policy.ts';
import type { JobWorkflowPayload } from './workflow-schema.ts';

import { dailyScheduleFor, failureBudgetFor, isWithinDailyDeadline } from './calendar.ts';
import { DEFAULT_DAILY_CHECK_EVERY, onDemandRetryDelay, onceRetryDelay } from './policy.ts';

/**
 * Computes the durable sleep duration before the next scheduled retry round, or `undefined` when
 * the failure is terminal. `failureCount` counts failed rounds of this execution, lost leases
 * included.
 */
export const scheduledRetrySleepFor = (
  retry: RetryPolicy,
  failure: {
    readonly cadence: AnyJobDefinition['cadence'];
    readonly trigger: JobWorkflowPayload['trigger'];
    readonly failureCount: number;
  },
): Duration.Duration | undefined => {
  const { cadence, trigger, failureCount } = failure;
  if (trigger !== 'schedule') {
    return undefined;
  }

  switch (cadence._tag) {
    case 'Daily':
      return failureCount < (failureBudgetFor(cadence) ?? retry.dailyMaxFailures)
        ? Duration.fromInputUnsafe(cadence.checkEvery ?? DEFAULT_DAILY_CHECK_EVERY)
        : undefined;
    case 'Once':
      return cadence.start === 'automatic' ? onceRetryDelay(retry, failureCount) : undefined;
    case 'OnDemand':
      return failureCount < retry.onDemandMaxFailures
        ? onDemandRetryDelay(retry, failureCount)
        : undefined;
    case 'Every':
    case 'Cron':
      return undefined;
    default: {
      const unexpected: never = cadence;
      return unexpected;
    }
  }
};

/**
 * Failure count of the round that just failed, which sizes its retry: one more than the workflow's
 * own count, or for `OnDemand` the key's count in the ledger when that is higher (failures of
 * `manual` runs included), so the workflow sleeps exactly until the `next_attempt_at` the ledger
 * stores. `failureCount` is the workflow's count before this round, `priorFailures` the key's count
 * at claim time (missing from older journals).
 */
export const failureCountAfter = (
  round: {
    readonly definition: JobIdentity;
    readonly failureCount: number;
    readonly priorFailures: number | undefined;
  },
  interrupted: boolean,
): number => {
  const local = round.failureCount + 1;
  if (round.definition.cadence._tag !== 'OnDemand' || round.priorFailures === undefined) {
    return local;
  }

  return Math.max(local, round.priorFailures + (interrupted ? 0 : 1));
};

/**
 * Whether a retry of `runKey` starting at `startMs` is still allowed: always, except for a `Daily`
 * job with a deadline, whose retries must start on the run's local date at or before `until`.
 */
export const isRetryBeforeDeadline = (
  definition: JobIdentity,
  runKey: string,
  startMs: number,
): boolean => {
  const { cadence } = definition;
  if (cadence._tag !== 'Daily') {
    return true;
  }

  const schedule = dailyScheduleFor(definition.name, cadence);
  return Result.isFailure(schedule) || isWithinDailyDeadline(schedule.success, runKey, startMs);
};

/**
 * Whether `definition` has a retry deadline (a `Daily` cadence with `at`, `window` or `until`).
 */
const hasRetryDeadline = (definition: JobIdentity): boolean =>
  failureBudgetFor(definition.cadence) !== undefined;

/**
 * For a round that ended without a settle of its own (its claim was lost, or its completion could
 * not be written): when a scheduled retry applies, counts the round as failed, sleeps
 * `retry-backoff-N` (at once when the journal holds that sleep) and returns `true`, so the caller
 * goes on to the next round. Returns `false` when no retry applies.
 */
export const sleepForScheduledRetry = Effect.fnUntraced(function* (args: {
  readonly definition: JobIdentity;
  readonly runs: JobRuns['Service'];
  readonly payload: JobWorkflowPayload;
  readonly round: number;
  readonly counters: { failureCount: number };
  readonly priorFailures: number | undefined;
}): Effect.fn.Return<
  boolean,
  never,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance
> {
  // Counted like a failed settle, so a replay that finds its settled round over sleeps the same.
  const failureCount = failureCountAfter(
    { ...args, failureCount: args.counters.failureCount },
    false,
  );
  const retrySleep = scheduledRetrySleepFor(args.runs.policy.retry, {
    cadence: args.definition.cadence,
    trigger: args.payload.trigger,
    failureCount,
  });
  if (retrySleep === undefined) {
    return false;
  }

  if (hasRetryDeadline(args.definition)) {
    const allowed = yield* Activity.make({
      name: `retry-deadline-${String(args.round)}`,
      success: Schema.Boolean,
      execute: Clock.currentTimeMillis.pipe(
        Effect.map((nowMs) =>
          isRetryBeforeDeadline(
            args.definition,
            args.payload.runKey,
            nowMs + Duration.toMillis(retrySleep),
          ),
        ),
      ),
    });
    if (!allowed) {
      return false;
    }
  }

  args.counters.failureCount = failureCount;
  yield* DurableClock.sleep({ name: `retry-backoff-${String(args.round)}`, duration: retrySleep });
  return true;
});
