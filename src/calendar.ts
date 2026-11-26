import { Cron, Duration, Effect, Option, Result } from 'effect';

import type { DailyCadence, DailyWindow, JobCadence } from './job.ts';

import { ClusterConfigError } from './errors.ts';
import { DEFAULT_CRON_CATCH_UP, DEFAULT_DAILY_CHECK_EVERY } from './policy.ts';
import { isValidTimeZone, latestInstantOf, localTimeOf } from './time-zone.ts';

/**
 * Number of minutes in one day; also the `until` value `24:00`.
 */
const MINUTES_PER_DAY = 1440;

/**
 * Number of milliseconds in one minute.
 */
const MILLIS_PER_MINUTE = 60_000;

/**
 * Parses `HH:MM` into minutes since midnight, `24:00` only when `allowEndOfDay`. Returns
 * `undefined` for anything else.
 */
const parseTimeOfDay = (value: string, allowEndOfDay: boolean): number | undefined => {
  const match = /^(\d{2}):(\d{2})$/.exec(value);
  if (match === null) {
    return undefined;
  }

  const minutes = Number(match[1]) * 60 + Number(match[2]);
  if (Number(match[2]) >= 60) {
    return undefined;
  }

  return minutes < MINUTES_PER_DAY || (allowEndOfDay && minutes === MINUTES_PER_DAY)
    ? minutes
    : undefined;
};

/**
 * 32-bit FNV-1a hash of `value`, used to pick a window slot that every worker agrees on.
 */
const fnv1a = (value: string): number => {
  let hash = 0x81_1c_9d_c5;
  for (let index = 0; index < value.length; index += 1) {
    hash = Math.imul(hash ^ value.charCodeAt(index), 0x01_00_01_93) >>> 0;
  }

  return hash;
};

/**
 * A validated `Daily` cadence: when each day's run is due and the latest time it may start.
 */
export type DailySchedule = {
  /**
   * IANA time zone of the run key and the times below.
   */
  readonly timeZone: string;

  /**
   * Scheduler check interval in minutes; also the retry wait.
   */
  readonly checkEveryMinutes: number;

  /**
   * Local minute of the day the run is due on `date`: `at`, a slot of the window, or `0`.
   */
  readonly dueMinuteOn: (date: string) => number;

  /**
   * Latest local minute a run may start, or `undefined` for no limit.
   */
  readonly untilMinute: number | undefined;

  /**
   * Local minute the day's first start must come strictly before, or `undefined` when only
   * `untilMinute` bounds it: `window.to`, unless an explicit `until` is later.
   */
  readonly firstStartBeforeMinute: number | undefined;
};

/**
 * The schedule of a `Daily` cadence with `at`: due at `at`, until `until` or `24:00`.
 */
const atSchedule = (
  base: Pick<DailySchedule, 'timeZone' | 'checkEveryMinutes'>,
  at: string,
  untilMinute: number | undefined,
): Result.Result<DailySchedule, string> => {
  const atMinute = parseTimeOfDay(at, false);
  if (atMinute === undefined) {
    return Result.fail(`Daily at "${at}" must be HH:MM between 00:00 and 23:59.`);
  }

  const deadline = untilMinute ?? MINUTES_PER_DAY;
  if (deadline - atMinute < base.checkEveryMinutes) {
    return Result.fail(
      `Daily until must be at least checkEvery (${String(base.checkEveryMinutes)} minutes) after at.`,
    );
  }

  return Result.succeed({
    ...base,
    dueMinuteOn: () => atMinute,
    untilMinute: deadline,
    firstStartBeforeMinute: undefined,
  });
};

/**
 * The schedule of a `Daily` cadence with `window`: due at a slot of `checkEvery` steps from
 * `window.from`, picked by hashing the job name and date, until `until` or `window.to`.
 */
const windowSchedule = (
  base: Pick<DailySchedule, 'timeZone' | 'checkEveryMinutes'>,
  job: string,
  window: DailyWindow,
  untilMinute: number | undefined,
): Result.Result<DailySchedule, string> => {
  const from = parseTimeOfDay(window.from, false);
  const to = parseTimeOfDay(window.to, true);
  if (from === undefined || to === undefined) {
    return Result.fail('Daily window from and to must be HH:MM.');
  }

  const step = base.checkEveryMinutes;
  const slots = Math.floor((to - from) / step);
  if (slots < 1) {
    return Result.fail(`Daily window must be at least checkEvery (${String(step)} minutes) long.`);
  }

  const deadline = untilMinute ?? to;
  if (deadline < to) {
    return Result.fail('Daily until must not be before window.to.');
  }

  return Result.succeed({
    ...base,
    dueMinuteOn: (date) => from + (fnv1a(`${job}:${date}`) % slots) * step,
    untilMinute: deadline,
    firstStartBeforeMinute: deadline > to ? undefined : to,
  });
};

/**
 * Validates a `Daily` cadence of job `job` and resolves its schedule, or returns why it is invalid.
 */
export const dailyScheduleFor = (
  job: string,
  cadence: DailyCadence,
): Result.Result<DailySchedule, string> => {
  const timeZone = cadence.timeZone ?? 'UTC';
  if (!isValidTimeZone(timeZone)) {
    return Result.fail(`Daily timeZone "${timeZone}" is not a known IANA time zone.`);
  }

  const checkEvery = Duration.fromInput(cadence.checkEvery ?? DEFAULT_DAILY_CHECK_EVERY);
  if (Option.isNone(checkEvery)) {
    return Result.fail('Daily checkEvery must be a duration.');
  }

  const checkEveryMs = Duration.toMillis(checkEvery.value);
  const base = {
    timeZone,
    checkEveryMinutes: Math.max(1, Math.floor(checkEveryMs / MILLIS_PER_MINUTE)),
  };
  const untilMinute = cadence.until === undefined ? undefined : parseTimeOfDay(cadence.until, true);
  if (cadence.until !== undefined && untilMinute === undefined) {
    return Result.fail(`Daily until "${cadence.until}" must be HH:MM between 00:00 and 24:00.`);
  }

  if (cadence.at !== undefined && cadence.window !== undefined) {
    return Result.fail('Daily takes at or window, not both.');
  }

  if (cadence.at !== undefined) {
    return atSchedule(base, cadence.at, untilMinute);
  }

  if (cadence.window !== undefined) {
    return windowSchedule(base, job, cadence.window, untilMinute);
  }

  if (untilMinute !== undefined && untilMinute < base.checkEveryMinutes) {
    return Result.fail(
      `Daily until must be at least checkEvery (${String(base.checkEveryMinutes)} minutes) after 00:00.`,
    );
  }

  return Result.succeed({
    ...base,
    dueMinuteOn: () => 0,
    untilMinute,
    firstStartBeforeMinute: undefined,
  });
};

/**
 * `dailyScheduleFor` for a cadence the scheduler already validated; an invalid one is a defect.
 */
export const dailyScheduleOrThrow = (job: string, cadence: DailyCadence): DailySchedule => {
  const resolved = dailyScheduleFor(job, cadence);
  if (Result.isFailure(resolved)) {
    throw new Error(`Job "${job}": ${resolved.failure}`);
  }

  return resolved.success;
};

/**
 * The `maxFailures` a failed settle of `cadence` passes to the ledger: `Infinity` for a `Daily`
 * cadence with a deadline, whose workflow stops at `until` instead of after
 * `retry.dailyMaxFailures` failures; `undefined` (the policy's budget) otherwise.
 */
export const failureBudgetFor = (cadence: JobCadence): number | undefined => {
  if (cadence._tag !== 'Daily') {
    return undefined;
  }

  const resolved = dailyScheduleFor('', cadence);
  return Result.isSuccess(resolved) && resolved.success.untilMinute !== undefined
    ? Number.POSITIVE_INFINITY
    : undefined;
};

/**
 * Whether a run of the day `runKey` may still start at `ms`: always without `until`, else when the
 * minute of `ms` is at or before the minute `until` falls on that local date (`24:00` counts as
 * 23:59). Instants are compared, so a deadline in the hour skipped by a daylight saving change
 * lands that long after the change, and one in a repeated hour at its second occurrence.
 */
export const isWithinDailyDeadline = (
  schedule: DailySchedule,
  runKey: string,
  ms: number,
): boolean => {
  if (schedule.untilMinute === undefined) {
    return true;
  }

  const deadlineMs = latestInstantOf(
    schedule.timeZone,
    runKey,
    Math.min(schedule.untilMinute, MINUTES_PER_DAY - 1),
  );
  return Math.floor(ms / MILLIS_PER_MINUTE) * MILLIS_PER_MINUTE <= deadlineMs;
};

/**
 * Whether the scheduler should start the day's run on a check scheduled at `scheduledMs` and
 * handled at `nowMs`: the check's local time has reached the day's due time, `nowMs` is not past
 * `until` and, for a window, `nowMs` is before `window.to` unless an explicit `until` is later or
 * the spring change skips the day's slot and `window.to` with it.
 */
export const isDailyRunDue = (
  schedule: DailySchedule,
  scheduledMs: number,
  nowMs: number,
): boolean => {
  const { date, minute } = localTimeOf(schedule.timeZone, scheduledMs);
  const dueMinute = schedule.dueMinuteOn(date);
  if (minute < dueMinute) {
    return false;
  }

  const startBefore = schedule.firstStartBeforeMinute;
  if (startBefore !== undefined) {
    const endMs = latestInstantOf(schedule.timeZone, date, startBefore);
    // A window the spring change skips entirely starts at the first check after it, as `at` does.
    const skipped = latestInstantOf(schedule.timeZone, date, dueMinute) >= endMs;
    if (!skipped && nowMs >= endMs) {
      return false;
    }
  }

  return isWithinDailyDeadline(schedule, date, nowMs);
};

/**
 * Parses the expression of a `Cron` cadence in its time zone, or returns why it is invalid. Only
 * five fields are accepted, since run keys have minute precision. A local time repeated by a
 * daylight saving change runs once, at its first occurrence.
 */
export const parseCronCadence = (
  cadence: Extract<JobCadence, { readonly _tag: 'Cron' }>,
): Result.Result<Cron.Cron, string> => {
  const fields = cadence.expression.trim().split(/\s+/).length;
  if (fields !== 5) {
    return Result.fail(
      `Cron expression "${cadence.expression}" must have five fields (minute hour day month weekday).`,
    );
  }

  return Cron.parse(cadence.expression, cadence.timeZone ?? 'UTC', {
    onDstOverlap: 'earlier',
  }).pipe(
    Result.mapError(
      (error) => `Cron expression "${cadence.expression}" is invalid: ${error.message}`,
    ),
    Result.flatMap((cron) =>
      hasOccurrences(cron)
        ? Result.succeed(cron)
        : Result.fail(`Cron expression "${cadence.expression}" never matches a date.`),
    ),
  );
};

/**
 * Reference instant for `hasOccurrences`; five-field expressions repeat every few years at most.
 */
const OCCURRENCE_REFERENCE = new Date(Date.UTC(2000, 0, 1));

/**
 * Whether `cron` has a tick before and after a reference date. An expression such as `0 0 31 2 *`
 * parses but never matches, and `Cron.next` and `Cron.prev` throw for it.
 */
const hasOccurrences = (cron: Cron.Cron): boolean => {
  try {
    Cron.next(cron, OCCURRENCE_REFERENCE);
    Cron.prev(cron, OCCURRENCE_REFERENCE);
    return true;
  } catch {
    return false;
  }
};

/**
 * Checks that the `catchUpWithin` of a `Cron` cadence is a finite duration above zero.
 */
const cronCatchUpProblem = (
  cadence: Extract<JobCadence, { readonly _tag: 'Cron' }>,
): Result.Result<void, string> => {
  const catchUp = Duration.fromInput(cronCatchUpFor(cadence));
  return Option.isSome(catchUp) &&
    Duration.isFinite(catchUp.value) &&
    Duration.toMillis(catchUp.value) > 0
    ? Result.void
    : Result.fail('Cron catchUpWithin must be a finite duration above zero.');
};

/**
 * Latest tick of `cron` at or before `nowMs`, in epoch milliseconds.
 */
export const latestCronTick = (cron: Cron.Cron, nowMs: number): number =>
  Cron.prev(cron, new Date(nowMs + 1)).getTime();

/**
 * How late a missed tick of a `Cron` cadence may still run.
 */
export const cronCatchUpFor = (
  cadence: Extract<JobCadence, { readonly _tag: 'Cron' }>,
): Duration.Input => cadence.catchUpWithin ?? DEFAULT_CRON_CATCH_UP;

/**
 * Validates the parts of `cadence` the scheduler and `runKeyFor` read at run time: the time zone,
 * times and window of a `Daily` cadence, and the expression and `catchUpWithin` of a `Cron`
 * cadence. Fails with `ClusterConfigError` naming `job`.
 */
export const validateCadence = (
  job: string,
  cadence: JobCadence,
): Effect.Effect<void, ClusterConfigError> => {
  let checked: Result.Result<unknown, string> = Result.succeed(undefined);
  if (cadence._tag === 'Daily') {
    checked = dailyScheduleFor(job, cadence);
  } else if (cadence._tag === 'Cron') {
    checked = parseCronCadence(cadence).pipe(Result.flatMap(() => cronCatchUpProblem(cadence)));
  }

  return Result.isSuccess(checked)
    ? Effect.void
    : Effect.fail(new ClusterConfigError({ message: `Job "${job}": ${checked.failure}` }));
};

/**
 * Longest run key an `OnDemand` caller may choose.
 */
export const MAX_RUN_KEY_LENGTH = 200;

/**
 * Returns why `runKey` is not a valid caller-chosen run key, or `undefined` when it is: 1 to
 * `MAX_RUN_KEY_LENGTH` printable ASCII characters, no spaces.
 */
export const runKeyProblem = (runKey: unknown): string | undefined => {
  if (typeof runKey !== 'string') {
    return 'Run key must be a string.';
  }

  if (runKey.length === 0 || runKey.length > MAX_RUN_KEY_LENGTH) {
    return `Run key must be 1 to ${String(MAX_RUN_KEY_LENGTH)} characters long.`;
  }

  return /^[!-~]+$/.test(runKey)
    ? undefined
    : 'Run key may only hold printable ASCII characters without spaces.';
};
