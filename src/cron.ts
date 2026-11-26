import { Cron, Duration, Effect, Option, Result } from 'effect';

import type { AnyJobDefinition, JobCadence } from './job.ts';

import { parseCronCadence, validateCadence } from './calendar.ts';
import { ClusterConfigError } from './errors.ts';
import { DEFAULT_DAILY_CHECK_EVERY } from './policy.ts';

/**
 * Number of milliseconds in one minute.
 */
const MILLIS_PER_MINUTE = 60_000;

/**
 * Number of minutes in one hour.
 */
const MINUTES_PER_HOUR = 60;

/**
 * Number of hours in one day.
 */
const HOURS_PER_DAY = 24;

/**
 * Cron for a fixed interval: every N minutes when N divides an hour, every H hours when H divides a
 * day, and midnight UTC for a whole day. Fails with `ClusterConfigError` naming `label` for any
 * other interval, or for input that is not a duration.
 */
const cronForInterval = (
  label: string,
  interval: Duration.Input,
): Effect.Effect<Cron.Cron, ClusterConfigError> => {
  const decoded = Duration.fromInput(interval);
  if (Option.isNone(decoded)) {
    return Effect.fail(new ClusterConfigError({ message: `${label} must be a duration.` }));
  }

  const intervalMs = Duration.toMillis(decoded.value);
  if (intervalMs < MILLIS_PER_MINUTE || intervalMs % MILLIS_PER_MINUTE !== 0) {
    return Effect.fail(
      new ClusterConfigError({
        message: `${label} must be a whole number of minutes (got ${String(intervalMs)}ms).`,
      }),
    );
  }

  const minutes = intervalMs / MILLIS_PER_MINUTE;
  if (minutes < MINUTES_PER_HOUR && MINUTES_PER_HOUR % minutes === 0) {
    const expr = minutes === 1 ? '* * * * *' : `*/${String(minutes)} * * * *`;
    return Effect.succeed(Cron.parseUnsafe(expr, 'UTC'));
  }

  if (minutes === MINUTES_PER_HOUR * HOURS_PER_DAY) {
    return Effect.succeed(Cron.parseUnsafe('0 0 * * *', 'UTC'));
  }

  if (minutes % MINUTES_PER_HOUR === 0) {
    const hours = minutes / MINUTES_PER_HOUR;
    if (hours >= 1 && hours < HOURS_PER_DAY && HOURS_PER_DAY % hours === 0) {
      const expr = hours === 1 ? '0 * * * *' : `0 */${String(hours)} * * *`;
      return Effect.succeed(Cron.parseUnsafe(expr, 'UTC'));
    }
  }

  return Effect.fail(
    new ClusterConfigError({
      message: `${label} of ${String(minutes)} minutes is not supported; it must evenly divide 60 minutes or 24 hours.`,
    }),
  );
};

/**
 * Converts a recurring `JobCadence` (`Every`, `Daily` or `Cron`) into the `Cron.Cron` the scheduler
 * runs:
 *
 * - `Daily`: every `checkEvery` in UTC (default 1 hour, `0 * * * *`), with the local date as the
 *   workflow idempotency key, so a missed check catches up on the next one while completed or
 *   running days deduplicate. The scheduler skips checks before the day's `at` or window time and
 *   after `until`.
 * - `Every`: every `interval` in UTC, on minute (`*\/N * * * *`) or hour (`0 *\/H * * *`) boundaries,
 *   or at midnight UTC (`0 0 * * *`) for one day.
 * - `Cron`: the expression in its time zone.
 *
 * Fails with `ClusterConfigError` when the interval is not a duration or does not evenly divide an
 * hour or a day, or the expression or time zone is invalid.
 */
export const cronForCadence = (
  cadence: Extract<JobCadence, { readonly _tag: 'Daily' | 'Every' | 'Cron' }>,
): Effect.Effect<Cron.Cron, ClusterConfigError> => {
  switch (cadence._tag) {
    case 'Daily':
      return cronForInterval('Daily checkEvery', cadence.checkEvery ?? DEFAULT_DAILY_CHECK_EVERY);
    case 'Every':
      return cronForInterval('Every interval', cadence.interval);
    case 'Cron':
      return Result.match(parseCronCadence(cadence), {
        onFailure: (message) => Effect.fail(new ClusterConfigError({ message })),
        onSuccess: Effect.succeed,
      });
    default: {
      const unexpected: never = cadence;
      return unexpected;
    }
  }
};

/**
 * Validates the cadence of `job` as the scheduler would, whether or not this worker runs it:
 * `validateCadence`, and for `Every`, `Daily` and `Cron` the cron the scheduler derives. `runJob`
 * and `dispatchApiJobWorkflow` run it too, so a cadence the scheduler rejects never gets a run key.
 * Fails with `ClusterConfigError`.
 */
export const validateJobCadence = Effect.fnUntraced(function* (
  job: Pick<AnyJobDefinition, 'name' | 'cadence'>,
): Effect.fn.Return<void, ClusterConfigError> {
  const { cadence } = job;
  yield* validateCadence(job.name, cadence);
  if (cadence._tag === 'Every' || cadence._tag === 'Daily' || cadence._tag === 'Cron') {
    yield* cronForCadence(cadence);
  }
});
