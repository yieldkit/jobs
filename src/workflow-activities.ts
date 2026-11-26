import type { WorkflowEngine } from 'effect/workflow';

import { describeError, logInfo, logWarning } from '@yieldkit/logger';
import { Cause, Clock, DateTime, Duration, Effect, Exit, Option, Schema } from 'effect';
import { Activity, DurableClock, Workflow } from 'effect/workflow';

import type { AnyJobDefinition, JobIdentity } from './job.ts';
import type { OwnedRun, RunClaim } from './ledger-types.ts';
import type { JobRuns } from './ledger.ts';
import type { RetryPolicy } from './policy.ts';
import type {
  ClaimActivityResult,
  JobWorkflowPayload,
  WorkflowJobOutcome,
} from './workflow-schema.ts';

import { recordJobRunMetrics } from './metrics.ts';
import { omitUndefined } from './omit-undefined.ts';
import { JobRequests } from './requests.ts';
import {
  annotationsOf,
  retryTransientSql,
  settleBudgetFor,
  settleOrLogLost,
} from './run-support.ts';
import { isRetryBeforeDeadline } from './workflow-retry.ts';
import {
  ClaimActivityResult as ClaimActivityResultSchema,
  INTERRUPTED_WORKFLOW_MESSAGE,
  buildFailedOutcome,
  claimToSkipReason,
  encodeRunClaim,
} from './workflow-schema.ts';

/**
 * Shortest durable sleep before reclaiming a `RetryLater` key, so a retry instant that is already
 * due (or skewed into the past) still yields one second instead of spinning.
 */
const MIN_RETRY_LATER_WAIT_MS = 1000;

/**
 * Mutable ownership state shared between the workflow loop and its explicit-interrupt finalizer.
 */
export type WorkflowExecutionState = {
  /**
   * Currently leased run, or `undefined` when idle or settled.
   */
  activeOwned: OwnedRun | undefined;

  /**
   * `true` once the workflow has durably settled its terminal outcome.
   */
  terminalSettled: boolean;
};

/**
 * Returns `true` when scheduled runs of `cadence` wait durably across `Busy`/`RetryLater` claims:
 * `Daily`, automatic `Once` and dispatched `OnDemand` runs.
 */
const isScheduledDurableCadence = (
  cadence: AnyJobDefinition['cadence'],
  trigger: JobWorkflowPayload['trigger'],
): boolean =>
  trigger === 'schedule' &&
  (cadence._tag === 'Daily' ||
    cadence._tag === 'OnDemand' ||
    (cadence._tag === 'Once' && cadence.start === 'automatic'));

/**
 * Records the request's terminal outcome with `JobRequests.markFinished` when `requestId` is set.
 * Skipped when no `JobRequests` is provided.
 */
export const finishRequestIfPresent = Effect.fnUntraced(function* (
  requestId: string | null,
  runKey: string | null,
  outcome: WorkflowJobOutcome,
): Effect.fn.Return<void> {
  if (requestId === null) {
    return;
  }

  const maybeRequests = yield* Effect.serviceOption(JobRequests);
  if (Option.isNone(maybeRequests)) {
    return;
  }

  yield* maybeRequests.value.markFinished({ id: requestId, runKey, outcome }).pipe(
    retryTransientSql,
    Effect.catch((error) =>
      logWarning('worker.request.finish_failed', {
        requestId,
        runKey,
        errorMessage: describeError(error),
      }),
    ),
  );
});

/**
 * Runs the `claim-round-N` durable activity: marks the request as started and claims `(job,
 * runKey)` in the ledger with our `executionId`. A scheduled run of a `Daily` job past its `until`
 * claims with `pastDeadline`, so it never takes the key and a failed row reads exhausted; each
 * claim attempt reads the clock again to decide it, and the claim reply that results is journaled.
 * A claim that still fails after the short retry is logged as `job.claim.failed` and reported as
 * `RetryLater` in `busyDelay`, never stored as a defect. `replayed` is `true` when the claim reply
 * came from the workflow's journal instead of a claim made now.
 */
export const runClaimRoundActivity = (params: {
  readonly runs: JobRuns['Service'];
  readonly definition: JobIdentity;
  readonly payload: JobWorkflowPayload;
  readonly executionId: string;
  readonly force: boolean;
  readonly encodedParams: unknown;
  readonly round: number;
}): Effect.Effect<
  { readonly claim: ClaimActivityResult; readonly replayed: boolean },
  never,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance
> => {
  let executed = false;
  return Activity.make({
    name: `claim-round-${String(params.round)}`,
    success: ClaimActivityResultSchema,
    execute: Effect.gen(function* () {
      executed = true;
      const startedMs = yield* Clock.currentTimeMillis;
      const job = params.definition.name;
      if (params.payload.requestId !== null) {
        const maybeRequests = yield* Effect.serviceOption(JobRequests);
        if (Option.isSome(maybeRequests)) {
          yield* maybeRequests.value
            .upsertStarted({
              id: params.payload.requestId,
              job,
              params: params.payload.params,
              force: params.payload.force,
              executionId: params.executionId,
              runKey: params.payload.runKey,
            })
            .pipe(
              retryTransientSql,
              Effect.catch((error) =>
                logWarning('worker.request.start_failed', {
                  requestId: params.payload.requestId,
                  runKey: params.payload.runKey,
                  errorMessage: describeError(error),
                }),
              ),
            );
        }
      }

      let claimCalls = 0;
      const claimRun = Effect.gen(function* () {
        claimCalls += 1;
        // Decided per attempt, so a claim that the retry carries past `until` does not take the key.
        const claimMs = yield* Clock.currentTimeMillis;
        const pastDeadline =
          params.payload.trigger === 'schedule' &&
          !isRetryBeforeDeadline(params.definition, params.payload.runKey, claimMs);
        return yield* params.runs.claim({
          job,
          runKey: params.payload.runKey,
          trigger: params.payload.trigger,
          force: params.force,
          params: params.encodedParams,
          executionId: params.executionId,
          ...omitUndefined({ pastDeadline: pastDeadline ? true : undefined }),
        });
      });
      const claim = yield* claimRun.pipe(
        retryTransientSql,
        Effect.catch((error) =>
          logWarning('job.claim.failed', {
            job,
            runKey: params.payload.runKey,
            errorMessage: describeError(error),
          }).pipe(
            Effect.andThen(Clock.currentTimeMillis),
            Effect.map((failedMs): RunClaim => ({
              _tag: 'RetryLater',
              at: new Date(failedMs + Duration.toMillis(params.runs.policy.retry.busyDelay)),
            })),
          ),
        ),
      );
      return encodeRunClaim(claim, startedMs, claimCalls - 1);
    }),
  }).pipe(Effect.map((claim) => ({ claim, replayed: !executed })));
};

/**
 * Handles a non-acquired claim: either sleeps durably via `DurableClock.sleep` and signals the
 * caller to retry (`undefined`), or settles the workflow as `skipped`. The `job.skipped` line
 * carries the job's `logFields`, which never override its own fields.
 */
export const handleNonAcquiredClaim = Effect.fnUntraced(function* (params: {
  readonly job: string;
  readonly cadence: AnyJobDefinition['cadence'];
  readonly logFields: AnyJobDefinition['logFields'];
  readonly payload: JobWorkflowPayload;
  readonly claimResult: Exclude<ClaimActivityResult, { readonly _tag: 'Acquired' }>;
  readonly round: number;
  readonly busyRetries: number;
  readonly retry: RetryPolicy;
}): Effect.fn.Return<
  { readonly outcome: WorkflowJobOutcome | undefined; readonly busyRetries: number },
  never,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance
> {
  const durableSchedule = isScheduledDurableCadence(params.cadence, params.payload.trigger);
  if (durableSchedule && params.claimResult._tag === 'Busy') {
    const nextBusyRetries = params.busyRetries + 1;
    const canWait =
      params.cadence._tag === 'Once' || nextBusyRetries <= params.retry.busyMaxRetries;
    if (canWait) {
      yield* DurableClock.sleep({
        name: `busy-backoff-${String(params.round)}`,
        duration: params.retry.busyDelay,
      });
      return { outcome: undefined, busyRetries: nextBusyRetries };
    }
  }

  if (durableSchedule && params.claimResult._tag === 'RetryLater') {
    const nowMs = yield* Activity.make({
      name: `retry-later-clock-${String(params.round)}`,
      success: Schema.Finite,
      execute: Clock.currentTimeMillis,
    });
    const waitMs = Math.max(
      MIN_RETRY_LATER_WAIT_MS,
      DateTime.toEpochMillis(DateTime.makeUnsafe(params.claimResult.at)) - nowMs,
    );
    yield* DurableClock.sleep({
      name: `retry-later-wait-${String(params.round)}`,
      duration: Duration.millis(waitMs),
    });
    return { outcome: undefined, busyRetries: params.busyRetries };
  }

  const reason = claimToSkipReason(params.claimResult);
  const skippedOutcome: WorkflowJobOutcome = {
    job: params.job,
    runKey: params.payload.runKey,
    trigger: params.payload.trigger,
    elapsedMs: params.claimResult.elapsedMs,
    status: 'skipped',
    reason,
  };
  yield* Activity.make({
    name: `settle-skipped-${String(params.round)}`,
    execute: Effect.gen(function* () {
      yield* logInfo('job.skipped', {
        ...params.logFields,
        job: params.job,
        runKey: params.payload.runKey,
        trigger: params.payload.trigger,
        status: 'skipped',
        skipReason: reason._tag,
        retryAt: reason._tag === 'RetryLater' ? reason.at : undefined,
      });
      yield* recordJobRunMetrics({
        job: params.job,
        status: 'skipped',
        trigger: params.payload.trigger,
        elapsedMs: params.claimResult.elapsedMs,
        leaseLost: false,
      });
      yield* finishRequestIfPresent(
        params.payload.requestId,
        params.payload.runKey,
        skippedOutcome,
      );
    }),
  });
  return { outcome: skippedOutcome, busyRetries: params.busyRetries };
});

/**
 * Registers the explicit-interrupt workflow finalizer. Ignores terminal completions and superseded
 * `Exit.void` exits so abandoned runs never change the ledger.
 */
export const registerInterruptFinalizer = (params: {
  readonly runs: JobRuns['Service'];
  readonly definition: JobIdentity;
  readonly payload: JobWorkflowPayload;
  readonly state: WorkflowExecutionState;
}): Effect.Effect<void, never, WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance> =>
  Workflow.addFinalizer((exit) =>
    Effect.gen(function* () {
      if (
        params.state.terminalSettled ||
        Exit.isSuccess(exit) ||
        !Cause.hasInterruptsOnly(exit.cause)
      ) {
        return;
      }

      const owned = params.state.activeOwned;
      if (owned !== undefined) {
        yield* settleOrLogLost(params.runs, owned, {
          _tag: 'Failed',
          errorMessage: INTERRUPTED_WORKFLOW_MESSAGE,
          reason: 'interrupted',
          cadence: params.definition.cadence._tag,
          ...settleBudgetFor(params.definition.cadence),
          trigger: params.payload.trigger,
          annotations: annotationsOf(params.definition),
        });
      }

      const interruptedOutcome = buildFailedOutcome({
        job: params.definition.name,
        runKey: params.payload.runKey,
        trigger: params.payload.trigger,
        elapsedMs: 0,
        attempt: owned?.attempt ?? 0,
        errorMessage: INTERRUPTED_WORKFLOW_MESSAGE,
        isDefect: false,
        interrupted: true,
        errorTag: 'Interrupted',
        retryAfterSeconds: undefined,
      });
      yield* finishRequestIfPresent(
        params.payload.requestId,
        params.payload.runKey,
        interruptedOutcome,
      );
    }),
  );

/**
 * Sleeps durably on the clock `name` until the epoch millisecond `atMs`, at once when it has
 * passed. The time left is read from the clock, not journaled, so a replay waits only for what is
 * left (the engine keeps the first wake-up of a clock it already scheduled) and skips the wait once
 * `atMs` is past; either way the workflow goes on to the same next step.
 */
export const sleepUntilInstant = Effect.fnUntraced(function* (
  name: string,
  atMs: number,
): Effect.fn.Return<void, never, WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance> {
  const waitMs = atMs - (yield* Clock.currentTimeMillis);
  if (waitMs > 0) {
    yield* DurableClock.sleep({ name, duration: Duration.millis(waitMs) });
  }
});

/**
 * Waits durably until `runAt` (ISO-8601) before the first claim of a delayed `dispatchJob` run, on
 * the clock `run-at-<epoch ms of runAt>`. The name changes with every dispatch, so the processed
 * clock of an earlier run of the same key, which Cluster may keep longer than the workflow, cannot
 * swallow the wake-up. A crash during the wait resumes it for the time left only.
 */
export const waitUntilRunAt = Effect.fnUntraced(function* (
  runAt: string,
): Effect.fn.Return<void, never, WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance> {
  const runAtMs = Date.parse(runAt);
  if (!Number.isFinite(runAtMs)) {
    return yield* Effect.die(new Error(`runAt "${runAt}" is not an ISO-8601 instant.`));
  }

  return yield* sleepUntilInstant(`run-at-${String(runAtMs)}`, runAtMs);
});
