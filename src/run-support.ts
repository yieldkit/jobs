import type { LogFields } from '@yieldkit/logger';
import type { SqlError } from 'effect/sql';

import { describeError, extractCauseSummary, logWarning, redactLogString } from '@yieldkit/logger';
import { Cause, Clock, Context, Duration, Effect, Result, Schedule, Schema } from 'effect';

import type { AnyJobDefinition, JobCadence, JobContext, JobDefinition, JobTrigger } from './job.ts';
import type { OwnedRun, RunResult } from './ledger-types.ts';
import type { JobRuns } from './ledger.ts';
import type { LeasePolicy } from './policy.ts';

import { findJobStageError, findRetryAfterSeconds } from './activity.ts';
import { dailyScheduleFor, failureBudgetFor } from './calendar.ts';
import { RUN_CLAIM_LOST_MESSAGE, RUN_CLAIM_LOST_TAG, RunClaimLost } from './errors.ts';
import { heartbeatIntervalFor } from './policy.ts';

/**
 * Parsed failure details together with the ledger `RunResult` passed to `JobRuns.settle`.
 */
export type AnalyzedFailure = {
  /**
   * `true` when the failure cause was `RunClaimLost`.
   */
  readonly isRunClaimLost: boolean;

  /**
   * `true` when the job died with an unhandled defect.
   */
  readonly isDefect: boolean;

  /**
   * `true` when the run was interrupted or lost its lease claim.
   */
  readonly interrupted: boolean;

  /**
   * Redacted failure summary.
   */
  readonly errorMessage: string;

  /**
   * `_tag` of the typed error, when present.
   */
  readonly errorTag: string | undefined;

  /**
   * Finite `retryAfterSeconds` field of the typed error, or `undefined`.
   */
  readonly retryAfterSeconds: number | undefined;

  /**
   * Ledger settle payload, always the `Failed` variant.
   */
  readonly runResult: Extract<RunResult, { readonly _tag: 'Failed' }>;
};

/**
 * Stats a finished job body returned, decoded for the caller and encoded for storage.
 */
export type EncodedRunOutput<Stats> = {
  /**
   * Decoded stats returned to the caller on the outcome.
   */
  readonly stats: Stats;

  /**
   * JSON-safe encoded stats stored in the ledger and the completion log line.
   */
  readonly encoded: unknown;
};

/**
 * Resolves the effective `force` flag: implied `true` only for `Every` and `Cron` jobs with
 * non-`schedule` triggers unless explicitly passed by the caller.
 */
export const resolveForce = (
  cadence: JobCadence,
  trigger: JobTrigger,
  explicit: boolean | undefined,
): boolean => {
  if (explicit !== undefined) {
    return explicit;
  }

  return (cadence._tag === 'Every' || cadence._tag === 'Cron') && trigger !== 'schedule';
};

/**
 * Milliseconds elapsed on the Effect `Clock` since `startedMs`, never negative.
 */
export const elapsedMillisSince = (startedMs: number): Effect.Effect<number> =>
  Clock.currentTimeMillis.pipe(Effect.map((nowMs) => Math.max(0, nowMs - startedMs)));

/**
 * Runs the job body for `ctx` and encodes its stats through `definition.stats`. Callers wrap the
 * result in their logging scope, so an encoding failure settles as a `failed` run like any other.
 */
export const executeAndEncodeStats = <Name extends string, P, S, E, R, PE>(
  definition: JobDefinition<Name, P, S, E, R, PE>,
  ctx: JobContext<P>,
): Effect.Effect<EncodedRunOutput<S>, E | Schema.SchemaError, R> =>
  Effect.suspend(() => definition.run(ctx)).pipe(
    Effect.flatMap((stats) =>
      Schema.encodeUnknownEffect(definition.stats)(stats).pipe(
        Effect.map((encoded) => ({ stats, encoded })),
      ),
    ),
  );

/**
 * Stats keys the `job.completed` summary leaves out, because the line already carries them as the
 * run's own status, duration and identity.
 */
const RESERVED_SUMMARY_KEYS: ReadonlySet<string> = new Set([
  'status',
  'durationMs',
  'job',
  'jobRunId',
  'runKey',
  'attempt',
  'trigger',
]);

/**
 * Converts encoded completion stats into flat log fields for `job.completed`. An object's keys
 * become fields, except `status`, `durationMs`, `job`, `jobRunId`, `runKey`, `attempt` and
 * `trigger`, which are dropped so they cannot overwrite the run's own fields. Any other non-object
 * value is logged as `stats`, and `undefined` adds nothing.
 */
export const summarizeEncodedStats = (encoded: unknown): LogFields => {
  if (typeof encoded === 'object' && encoded !== null && !Array.isArray(encoded)) {
    const entries: Array<[string, unknown]> = [];
    for (const [key, value] of Object.entries(encoded)) {
      if (!RESERVED_SUMMARY_KEYS.has(key)) {
        entries.push([key, value]);
      }
    }

    return Object.fromEntries(entries);
  }

  return encoded === undefined ? {} : { stats: encoded };
};

/**
 * Options for `raceWithLeaseRenewal`.
 */
export type LeaseRenewalOptions<L> = {
  /**
   * Extends the lease. `true` means it is still held, `false` that it was lost.
   */
  readonly renew: Effect.Effect<boolean, SqlError.SqlError>;

  /**
   * Lease written by `renew`. Renewals run every `heartbeatIntervalFor(lease)`.
   */
  readonly lease: LeasePolicy;

  /**
   * Builds the error the race fails with once the lease is lost.
   */
  readonly lost: () => L;

  /**
   * Log event written for each renewal that fails with a `SqlError` or times out.
   */
  readonly failureEvent: string;

  /**
   * Fields added to every failure log line.
   */
  readonly fields: LogFields;
};

/**
 * Races `work` against a renewal loop that calls `options.renew` every
 * `heartbeatIntervalFor(lease)`. Fails with `options.lost()`, interrupting `work`, as soon as a
 * renewal returns `false`, or once renewals have failed for a full lease. A renewal fails with a
 * `SqlError` or by taking longer than one interval. Each failed renewal is logged as
 * `options.failureEvent`.
 */
export const raceWithLeaseRenewal = <A, E, R, L>(
  work: Effect.Effect<A, E, R>,
  options: LeaseRenewalOptions<L>,
): Effect.Effect<A, E | L, R> => {
  const leaseMs = Math.max(1, Duration.toMillis(options.lease.duration));
  const interval = heartbeatIntervalFor(options.lease);

  const renewLoop: Effect.Effect<never, L> = Effect.gen(function* () {
    let lastSuccessMs = yield* Clock.currentTimeMillis;

    // Each round yields whether the lease is still ours; `false` ends the race with `lost()`.
    return yield* Effect.forever(
      Effect.gen(function* () {
        // Neither the wait nor the renewal may outlast the lease granted by the last success.
        const untilDeadline = Effect.map(Clock.currentTimeMillis, (nowMs) =>
          Duration.min(interval, Duration.millis(Math.max(0, lastSuccessMs + leaseMs - nowMs))),
        );
        yield* Effect.sleep(yield* untilDeadline);
        // The renewed lease starts when the request is sent, not when its reply arrives, so a late
        // acknowledgement cannot stretch the budget past what the database granted.
        const requestMs = yield* Clock.currentTimeMillis;
        const result = yield* options.renew.pipe(
          Effect.timeout(yield* untilDeadline),
          Effect.result,
        );
        const nowMs = yield* Clock.currentTimeMillis;
        if (result._tag === 'Success') {
          if (result.success) {
            lastSuccessMs = requestMs;
          }

          return result.success;
        }

        const elapsedWithoutSuccessMs = nowMs - lastSuccessMs;
        yield* logWarning(options.failureEvent, {
          ...options.fields,
          errorMessage: describeError(result.failure),
          elapsedSinceSuccessMs: elapsedWithoutSuccessMs,
        });
        return elapsedWithoutSuccessMs < leaseMs;
      }).pipe(
        Effect.filterOrFail(
          (stillOwned) => stillOwned,
          () => options.lost(),
        ),
      ),
    );
  });
  return Effect.raceFirst(work, renewLoop);
};

/**
 * Heartbeat loop racing against the job body. Extends the lease every `heartbeatIntervalFor` of the
 * ledger's lease policy and tolerates transient `SqlError`s and renewals slower than one interval
 * for up to a full lease before failing with `RunClaimLost`. Fails with `RunClaimLost` at once if
 * `heartbeat` returns `false`.
 */
export const runWithHeartbeat = <A, E, R>(
  runs: JobRuns['Service'],
  owned: OwnedRun,
  work: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | RunClaimLost, R> =>
  raceWithLeaseRenewal(work, {
    renew: runs.heartbeat(owned),
    lease: runs.policy.lease,
    lost: () => new RunClaimLost({ job: owned.job, runKey: owned.runKey }),
    failureEvent: 'job.heartbeat.failed',
    fields: { job: owned.job, runKey: owned.runKey, attempt: owned.attempt },
  });

/**
 * Annotations of `definition`, or an empty context.
 */
export const annotationsOf = (
  definition: Pick<AnyJobDefinition, 'annotations'>,
): Context.Context<never> => definition.annotations ?? Context.empty();

/**
 * Replaces NUL characters, which PostgreSQL `text` columns reject, with U+FFFD.
 */
const withoutNul = (text: string): string =>
  text.includes('\u0000') ? text.replaceAll('\u0000', '\uFFFD') : text;

/**
 * The budget a failed settle of `cadence` passes to the ledger: `maxFailures` from
 * `failureBudgetFor`, and for a `Daily` cadence with a deadline its schedule, so a failure after
 * `until` leaves the key exhausted whatever the trigger.
 */
export const settleBudgetFor = (
  cadence: JobCadence,
): Pick<Extract<RunResult, { readonly _tag: 'Failed' }>, 'maxFailures' | 'deadline'> => {
  const maxFailures = failureBudgetFor(cadence);
  if (cadence._tag !== 'Daily' || maxFailures === undefined) {
    return { maxFailures };
  }

  const schedule = dailyScheduleFor('', cadence);
  return Result.isSuccess(schedule) ? { maxFailures, deadline: schedule.success } : { maxFailures };
};

/**
 * Analyzes a failed `Exit` cause into failure metadata and the matching ledger `RunResult`. The
 * error message is redacted and has NUL characters replaced, so the ledger can always store it.
 */
export const analyzeFailureExit = (
  definition: Pick<AnyJobDefinition, 'cadence' | 'annotations'>,
  trigger: JobTrigger,
  cause: Cause.Cause<unknown>,
): AnalyzedFailure => {
  const stageError = findJobStageError(cause);
  if (stageError !== undefined) {
    const isRunClaimLost = stageError.errorTag === RUN_CLAIM_LOST_TAG;
    const interrupted = isRunClaimLost;
    const errorMessage = withoutNul(redactLogString(stageError.message));
    return {
      isRunClaimLost,
      isDefect: stageError.isDefect,
      interrupted,
      errorMessage,
      errorTag: stageError.errorTag ?? undefined,
      retryAfterSeconds: stageError.retryAfterSeconds ?? undefined,
      runResult: {
        _tag: 'Failed',
        errorMessage,
        reason: interrupted ? 'interrupted' : 'error',
        cadence: definition.cadence._tag,
        ...settleBudgetFor(definition.cadence),
        trigger,
        annotations: annotationsOf(definition),
      },
    };
  }

  const summary = extractCauseSummary(cause);
  const isRunClaimLost = summary?.tag === RUN_CLAIM_LOST_TAG;
  const interrupted = Cause.hasInterruptsOnly(cause) || isRunClaimLost;
  const isDefect = summary?.kind === 'die';
  const errorMessage = isRunClaimLost
    ? RUN_CLAIM_LOST_MESSAGE
    : withoutNul(redactLogString(summary?.message ?? Cause.pretty(cause)));
  return {
    isRunClaimLost,
    isDefect,
    interrupted,
    errorMessage,
    errorTag: summary?.tag,
    retryAfterSeconds: findRetryAfterSeconds(cause),
    runResult: {
      _tag: 'Failed',
      errorMessage,
      reason: interrupted ? 'interrupted' : 'error',
      cadence: definition.cadence._tag,
      ...settleBudgetFor(definition.cadence),
      trigger,
      annotations: annotationsOf(definition),
    },
  };
};

/**
 * Retries `effect` five times with backoff (6.2 seconds of waiting), whatever the `SqlError`.
 * Enough for a reset connection; a longer outage is left to the caller's durable path. Each try's
 * own duration comes on top: a try that waits for a pool connection adds the acquire or connect
 * timeout each time (about 36 s in all with 5-second timeouts), and a hung server is bounded only
 * by the pool's statement timeout. That is also how long it can hold up a worker's shutdown.
 */
export const retryTransientSql = <A, R>(
  effect: Effect.Effect<A, SqlError.SqlError, R>,
): Effect.Effect<A, SqlError.SqlError, R> =>
  Effect.retry(effect, { schedule: Schedule.exponential('200 millis', 2), times: 5 });

/**
 * Settles `owned` with `result`, retrying transient SQL errors, and returns whether the row was
 * written. A settle that is rejected (another run holds the key) or still fails after the retry
 * returns `false` and logs `job.settle.lost`, with the error's message when there was one.
 */
export const settleOrLogLost = (
  runs: JobRuns['Service'],
  owned: OwnedRun,
  result: RunResult,
): Effect.Effect<boolean> => {
  /**
   * Logs the lost settle, with the error's message when the ledger could not be reached.
   */
  const logLost = (error: SqlError.SqlError | undefined) =>
    logWarning('job.settle.lost', {
      job: owned.job,
      runKey: owned.runKey,
      attempt: owned.attempt,
      errorMessage: error === undefined ? undefined : describeError(error),
    }).pipe(Effect.as(false));
  return runs.settle(owned, result).pipe(
    retryTransientSql,
    Effect.matchEffect({
      onFailure: logLost,
      onSuccess: (settled) => (settled ? Effect.succeed(true) : logLost(undefined)),
    }),
  );
};
