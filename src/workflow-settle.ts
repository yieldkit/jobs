import type { Cause } from 'effect';
import type { WorkflowEngine } from 'effect/workflow';

import { describeError, logWarning } from '@yieldkit/logger';
import { DateTime, Duration, Effect } from 'effect';
import { Activity } from 'effect/workflow';

import type { JobIdentity } from './job.ts';
import type { OwnedRun } from './ledger-types.ts';
import type { JobRuns } from './ledger.ts';
import type { JobWorkflowPayload, WorkflowJobOutcome } from './workflow-schema.ts';

import { RUN_CLAIM_LOST_MESSAGE, RUN_CLAIM_LOST_TAG } from './errors.ts';
import { recordJobRunMetrics } from './metrics.ts';
import {
  analyzeFailureExit,
  annotationsOf,
  elapsedMillisSince,
  retryTransientSql,
  settleOrLogLost,
} from './run-support.ts';
import { finishRequestIfPresent, sleepUntilInstant } from './workflow-activities.ts';
import {
  failureCountAfter,
  isRetryBeforeDeadline,
  scheduledRetrySleepFor,
} from './workflow-retry.ts';
import {
  MAX_CRASH_ATTEMPTS_TAG,
  SettleActivityResult,
  buildFailedOutcome,
  maxCrashAttemptsMessage,
} from './workflow-schema.ts';

/**
 * Checks a claim reply replayed from the journal. A token that still holds the row renews its lease
 * and returns `false`, so the round goes on. A token that lost the row, or a ledger that cannot be
 * reached after the short retry (logged as `job.heartbeat.failed`), returns `true`: the round is
 * over (it settled before the workflow suspended, or another run took the key) and its body must
 * not run again.
 */
export const isReplayedRoundOver = (
  runs: JobRuns['Service'],
  owned: OwnedRun,
): Effect.Effect<boolean> =>
  runs.heartbeat(owned).pipe(
    retryTransientSql,
    Effect.catch((error) =>
      logWarning('job.heartbeat.failed', {
        job: owned.job,
        runKey: owned.runKey,
        attempt: owned.attempt,
        errorMessage: describeError(error),
      }).pipe(Effect.as(false)),
    ),
    Effect.map((stillOwned) => !stillOwned),
  );

/**
 * Builds the `RunClaimLost` failed outcome when a settle call fails its fencing check.
 */
const buildClaimLostOutcome = (
  job: string,
  payload: JobWorkflowPayload,
  elapsedMs: number,
  attempt: number,
): WorkflowJobOutcome =>
  buildFailedOutcome({
    job,
    runKey: payload.runKey,
    trigger: payload.trigger,
    elapsedMs,
    attempt,
    errorMessage: RUN_CLAIM_LOST_MESSAGE,
    isDefect: false,
    interrupted: true,
    errorTag: RUN_CLAIM_LOST_TAG,
    retryAfterSeconds: undefined,
  });

/**
 * Records metrics and the request's terminal status when workflow param decoding fails.
 */
export const settleInvalidParams = (
  job: string,
  payload: JobWorkflowPayload,
  errorTag: string,
  errorMessage: string,
): Effect.Effect<
  WorkflowJobOutcome,
  never,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance
> => {
  const outcome = buildFailedOutcome({
    job,
    runKey: payload.runKey,
    trigger: payload.trigger,
    elapsedMs: 0,
    attempt: 0,
    errorMessage,
    isDefect: false,
    interrupted: false,
    errorTag,
    retryAfterSeconds: undefined,
  });
  return Activity.make({
    name: 'settle-invalid-params',
    execute: finishRequestIfPresent(payload.requestId, payload.runKey, outcome).pipe(
      Effect.andThen(
        recordJobRunMetrics({
          job,
          status: 'failed',
          trigger: payload.trigger,
          elapsedMs: 0,
          leaseLost: false,
        }),
      ),
    ),
  }).pipe(Effect.as(outcome));
};

/**
 * Settles a crash-looping workflow once its same-`executionId` resume count exceeds
 * `policy.retry.maxCrashAttempts`.
 */
export const settleCrashCap = (params: {
  readonly runs: JobRuns['Service'];
  readonly definition: JobIdentity;
  readonly payload: JobWorkflowPayload;
  readonly owned: OwnedRun;
  readonly round: number;
}): Effect.Effect<
  WorkflowJobOutcome,
  never,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance
> => {
  const crashMessage = maxCrashAttemptsMessage(params.runs.policy.retry.maxCrashAttempts);
  const crashOutcome = buildFailedOutcome({
    job: params.definition.name,
    runKey: params.payload.runKey,
    trigger: params.payload.trigger,
    elapsedMs: 0,
    attempt: params.owned.attempt,
    errorMessage: crashMessage,
    isDefect: true,
    interrupted: false,
    errorTag: MAX_CRASH_ATTEMPTS_TAG,
    retryAfterSeconds: undefined,
  });
  return Activity.make({
    name: `settle-crash-cap-${String(params.round)}`,
    execute: Effect.gen(function* () {
      yield* settleOrLogLost(params.runs, params.owned, {
        _tag: 'Failed',
        errorMessage: crashMessage,
        reason: 'crash-cap',
        cadence: params.definition.cadence._tag,
        trigger: params.payload.trigger,
        annotations: annotationsOf(params.definition),
        nextAttemptAtOverride: null,
      });
      yield* recordJobRunMetrics({
        job: params.definition.name,
        status: 'failed',
        trigger: params.payload.trigger,
        elapsedMs: 0,
        leaseLost: false,
      });
      yield* finishRequestIfPresent(params.payload.requestId, params.payload.runKey, crashOutcome);
    }),
  }).pipe(Effect.as(crashOutcome));
};

/**
 * Runs the `settle-completed-N` durable activity after a job body finishes successfully. A settle
 * that still fails after the short retry counts as not settled (`RunClaimLost`); `settled` tells
 * the caller whether the completion was written.
 */
export const settleCompletedRound = Effect.fnUntraced(function* (params: {
  readonly runs: JobRuns['Service'];
  readonly definition: JobIdentity;
  readonly payload: JobWorkflowPayload;
  readonly owned: OwnedRun;
  readonly startedMs: number;
  readonly encodedStats: unknown;
  readonly round: number;
}): Effect.fn.Return<
  { readonly settled: boolean; readonly outcome: WorkflowJobOutcome },
  never,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance
> {
  /**
   * Converts a `settle` result into the matching completed or claim-lost `WorkflowJobOutcome`.
   */
  const outcomeFor = (settled: boolean, elapsedMs: number): WorkflowJobOutcome =>
    settled
      ? {
          job: params.definition.name,
          runKey: params.payload.runKey,
          trigger: params.payload.trigger,
          elapsedMs,
          status: 'completed',
          attempt: params.owned.attempt,
          dryRun: false,
          stats: params.encodedStats,
        }
      : buildClaimLostOutcome(
          params.definition.name,
          params.payload,
          elapsedMs,
          params.owned.attempt,
        );

  const settledInfo = yield* Activity.make({
    name: `settle-completed-${String(params.round)}`,
    success: SettleActivityResult,
    execute: Effect.gen(function* () {
      const settled = yield* settleOrLogLost(params.runs, params.owned, {
        _tag: 'Completed',
        stats: params.encodedStats,
        trigger: params.payload.trigger,
        annotations: annotationsOf(params.definition),
      });
      const elapsedMs = yield* elapsedMillisSince(params.startedMs);
      const outcome = outcomeFor(settled, elapsedMs);
      yield* recordJobRunMetrics({
        job: params.definition.name,
        status: outcome.status,
        trigger: params.payload.trigger,
        elapsedMs,
        leaseLost: !settled,
      });
      yield* finishRequestIfPresent(params.payload.requestId, params.payload.runKey, outcome);
      return { settled, elapsedMs };
    }),
  });
  return {
    settled: settledInfo.settled,
    outcome: outcomeFor(settledInfo.settled, settledInfo.elapsedMs),
  };
});

/**
 * `next_attempt_at` a failed round writes: the wake-up of its durable retry; `null` when a
 * scheduled `Daily` or `OnDemand` workflow gives the key up, so the row reads exhausted too;
 * otherwise `undefined`, leaving the ledger's own retry rules. The ledger keeps `null` whenever its
 * rules say exhausted.
 */
const nextAttemptOverrideFor = (
  retrySleep: Duration.Duration | undefined,
  now: DateTime.Utc,
  params: { readonly payload: JobWorkflowPayload; readonly definition: JobIdentity },
): Date | null | undefined => {
  if (retrySleep !== undefined) {
    return DateTime.toDateUtc(DateTime.addDuration(now, retrySleep));
  }

  const givesUp =
    params.payload.trigger === 'schedule' &&
    (params.definition.cadence._tag === 'Daily' || params.definition.cadence._tag === 'OnDemand');
  return givesUp ? null : undefined;
};

/**
 * Runs the `settle-failed-N` durable activity after a failed round and returns either `undefined`
 * (after durably sleeping for the next retry) or the terminal `failed` outcome.
 */
export const settleFailedRound = Effect.fnUntraced(function* (params: {
  readonly runs: JobRuns['Service'];
  readonly definition: JobIdentity;
  readonly payload: JobWorkflowPayload;
  readonly owned: OwnedRun;
  readonly startedMs: number;
  readonly cause: Cause.Cause<unknown>;
  readonly round: number;
  readonly failureCount: number;
  readonly priorFailures: number | undefined;
}): Effect.fn.Return<
  { readonly outcome: WorkflowJobOutcome | undefined; readonly failureCount: number },
  never,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance
> {
  const failure = analyzeFailureExit(params.definition, params.payload.trigger, params.cause);
  const nextFailureCount = failureCountAfter(params, failure.interrupted);
  const retrySleep = scheduledRetrySleepFor(params.runs.policy.retry, {
    cadence: params.definition.cadence,
    trigger: params.payload.trigger,
    failureCount: nextFailureCount,
  });

  /**
   * Converts a `settle` result into the matching failed or claim-lost `WorkflowJobOutcome`.
   */
  const outcomeFor = (settled: boolean, elapsedMs: number): WorkflowJobOutcome =>
    settled
      ? buildFailedOutcome({
          job: params.definition.name,
          runKey: params.payload.runKey,
          trigger: params.payload.trigger,
          elapsedMs,
          attempt: params.owned.attempt,
          errorMessage: failure.errorMessage,
          isDefect: failure.isDefect,
          interrupted: failure.interrupted,
          errorTag: failure.errorTag,
          retryAfterSeconds: failure.retryAfterSeconds,
        })
      : buildClaimLostOutcome(
          params.definition.name,
          params.payload,
          elapsedMs,
          params.owned.attempt,
        );

  const settledInfo = yield* Activity.make({
    name: `settle-failed-${String(params.round)}`,
    success: SettleActivityResult,
    execute: Effect.gen(function* () {
      const now = yield* DateTime.now;
      const nowMs = DateTime.toEpochMillis(now);
      const elapsedMs = Math.max(0, nowMs - params.startedMs);
      const retries =
        retrySleep !== undefined &&
        isRetryBeforeDeadline(
          params.definition,
          params.payload.runKey,
          nowMs + Duration.toMillis(retrySleep),
        );
      const result = {
        ...failure.runResult,
        nextAttemptAtOverride: nextAttemptOverrideFor(
          retries ? retrySleep : undefined,
          now,
          params,
        ),
      };
      const settled = yield* settleOrLogLost(params.runs, params.owned, result);
      yield* recordJobRunMetrics({
        job: params.definition.name,
        status: 'failed',
        trigger: params.payload.trigger,
        elapsedMs,
        leaseLost: !settled || failure.isRunClaimLost,
      });
      if (!retries) {
        yield* finishRequestIfPresent(
          params.payload.requestId,
          params.payload.runKey,
          outcomeFor(settled, elapsedMs),
        );
      }

      return { settled, elapsedMs, retries };
    }),
  });
  // Replies stored by earlier releases lack `retries`; they had no deadline to check.
  if (retrySleep !== undefined && (settledInfo.retries ?? true)) {
    // Wake at the `next_attempt_at` the settle wrote, however long the settle reply took.
    yield* sleepUntilInstant(
      `retry-backoff-${String(params.round)}`,
      params.startedMs + settledInfo.elapsedMs + Duration.toMillis(retrySleep),
    );
    return { outcome: undefined, failureCount: nextFailureCount };
  }

  return {
    outcome: outcomeFor(settledInfo.settled, settledInfo.elapsedMs),
    failureCount: nextFailureCount,
  };
});
