import { Duration, Effect, Result } from 'effect';

import type { ClusterConfigError } from './errors.ts';
import type { AnyJobDefinition, JobCadence } from './job.ts';

import { latestCronTick, parseCronCadence, runKeyProblem } from './calendar.ts';
import { validateJobCadence } from './cron.ts';
import { InvalidRunKey } from './errors.ts';
import { localTimeOf } from './time-zone.ts';

/**
 * Constant `run_key` used by all `Once` jobs.
 */
export const ONCE_RUN_KEY = 'once';

/**
 * Zero-pads a non-negative integer to at least `width` digits.
 */
const pad = (value: number, width: number): string => String(value).padStart(width, '0');

/**
 * Formats a UTC millisecond timestamp as `YYYY-MM-DD`.
 */
const formatUtcDay = (ms: number): string => {
  const date = new Date(ms);
  const year = pad(date.getUTCFullYear(), 4);
  const month = pad(date.getUTCMonth() + 1, 2);
  const day = pad(date.getUTCDate(), 2);
  return `${year}-${month}-${day}`;
};

/**
 * Formats a UTC millisecond timestamp as `YYYY-MM-DDTHH:MMZ` for `Every` bucket keys.
 */
const formatUtcBucket = (ms: number): string => {
  const date = new Date(ms);
  const day = formatUtcDay(ms);
  const hours = pad(date.getUTCHours(), 2);
  const minutes = pad(date.getUTCMinutes(), 2);
  return `${day}T${hours}:${minutes}Z`;
};

/**
 * A cadence whose run keys come from the clock: every cadence except `OnDemand`, whose keys the
 * caller chooses.
 */
export type ClockCadence = Exclude<JobCadence, { readonly _tag: 'OnDemand' }>;

/**
 * Computes the deterministic ledger key of the run due at `nowMs` (epoch milliseconds):
 *
 * - `Daily`: the local date `YYYY-MM-DD` in the cadence's time zone (UTC by default)
 * - `Every`: bucket start `floor(nowMs / intervalMs) * intervalMs` as `YYYY-MM-DDTHH:MMZ`
 * - `Cron`: the latest tick at or before `nowMs`, in UTC, as `YYYY-MM-DDTHH:MMZ`
 * - `Once`: `'once'`
 *
 * Throws for a `Daily` time zone or `Cron` expression that `makeSchedulerLayer` would reject.
 */
export const runKeyFor = (cadence: ClockCadence, nowMs: number): string => {
  switch (cadence._tag) {
    case 'Daily':
      return localTimeOf(cadence.timeZone ?? 'UTC', nowMs).date;
    case 'Every': {
      const intervalMs = Math.max(1, Duration.toMillis(Duration.fromInputUnsafe(cadence.interval)));
      const bucketStartMs = Math.floor(nowMs / intervalMs) * intervalMs;
      return formatUtcBucket(bucketStartMs);
    }

    case 'Cron': {
      const cron = Result.getOrThrowWith(
        parseCronCadence(cadence),
        (message) => new Error(message),
      );
      return formatUtcBucket(latestCronTick(cron, nowMs));
    }

    case 'Once':
      return ONCE_RUN_KEY;
    default: {
      const unexpected: never = cadence;
      return unexpected;
    }
  }
};

/**
 * Resolves the run key of a run of `job` starting at `nowMs`: the caller's `runKey` for an
 * `OnDemand` job, which needs one, else `runKeyFor` of the cadence, which takes none. Fails with
 * `InvalidRunKey` otherwise or when the caller's key is malformed, and with `ClusterConfigError`
 * when the cadence is invalid (an unknown time zone, an expression that never matches, an interval
 * the scheduler would reject).
 */
export const resolveRunKey = (
  job: Pick<AnyJobDefinition, 'name' | 'cadence'>,
  nowMs: number,
  runKey: string | undefined,
): Effect.Effect<string, InvalidRunKey | ClusterConfigError> => {
  const { cadence } = job;
  if (cadence._tag !== 'OnDemand') {
    return runKey === undefined
      ? validateJobCadence(job).pipe(Effect.map(() => runKeyFor(cadence, nowMs)))
      : Effect.fail(
          new InvalidRunKey({
            job: job.name,
            message: `Job "${job.name}" has a ${cadence._tag} cadence: its run keys come from the clock. Only OnDemand jobs take a run key.`,
          }),
        );
  }

  return resolveCallerRunKey(job.name, runKey);
};

/**
 * Checks the caller-chosen `runKey` of a run of the `OnDemand` job `job`. Fails with
 * `InvalidRunKey` when it is missing or malformed.
 */
export const resolveCallerRunKey = (
  job: string,
  runKey: string | undefined,
): Effect.Effect<string, InvalidRunKey> => {
  if (runKey === undefined) {
    return Effect.fail(
      new InvalidRunKey({ job, message: `OnDemand job "${job}" needs a run key.` }),
    );
  }

  const problem = runKeyProblem(runKey);
  return problem === undefined
    ? Effect.succeed(runKey)
    : Effect.fail(new InvalidRunKey({ job, message: problem }));
};
