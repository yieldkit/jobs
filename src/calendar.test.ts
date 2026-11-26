import type { Duration } from 'effect';

import { Effect, Result } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import type { DailyCadence, DailyWindow, JobCadence } from './job.ts';

import {
  dailyScheduleFor,
  failureBudgetFor,
  isDailyRunDue,
  isWithinDailyDeadline,
  parseCronCadence,
  runKeyProblem,
  validateCadence,
} from './calendar.ts';
import { defaultJobsPolicy, nextAttemptAt, resolveJobsPolicy } from './policy.ts';
import { resolveRunKey, runKeyFor } from './run-key.ts';
import { localTimeOf } from './time-zone.ts';

const at = (iso: string): number => Date.parse(iso);

/**
 * The resolved schedule of `cadence` for job `job`, failing the test when it is invalid.
 */
const scheduleOf = (cadence: DailyCadence, job = 'test-job') =>
  Result.getOrThrowWith(dailyScheduleFor(job, cadence), (message) => new Error(message));

/**
 * Why `cadence` is invalid, or `undefined` when it is valid.
 */
const problemOf = (cadence: DailyCadence): string | undefined =>
  Result.match(dailyScheduleFor('test-job', cadence), {
    onFailure: (message) => message,
    onSuccess: () => undefined,
  });

describe('local time', () => {
  it('reads the local date and minute in a time zone, across daylight saving time', () => {
    expect(localTimeOf('UTC', at('2026-11-26T09:05:00Z'))).toEqual({
      date: '2026-11-26',
      minute: 9 * 60 + 5,
    });
    expect(localTimeOf('Europe/Brussels', at('2026-11-26T23:30:00Z'))).toEqual({
      date: '2026-11-27',
      minute: 30,
    });
    expect(localTimeOf('Europe/Brussels', at('2026-07-01T07:00:00Z')).minute).toBe(9 * 60);
    expect(localTimeOf('America/New_York', at('2026-11-26T03:00:00Z')).date).toBe('2026-11-25');
  });
});

describe('Daily schedules', () => {
  it('rejects invalid time zones, times, windows and deadlines', () => {
    expect(problemOf({ _tag: 'Daily', timeZone: 'Mars/Olympus' })).toContain('timeZone');
    expect(problemOf({ _tag: 'Daily', at: '9:00' })).toContain('at "9:00"');
    expect(problemOf({ _tag: 'Daily', at: '24:00' })).toContain('at "24:00"');
    expect(problemOf({ _tag: 'Daily', at: '09:60' })).toContain('at "09:60"');
    expect(problemOf({ _tag: 'Daily', until: '25:00' })).toContain('until');
    expect(
      problemOf({ _tag: 'Daily', at: '09:00', window: { from: '08:00', to: '14:00' } }),
    ).toContain('not both');
    expect(problemOf({ _tag: 'Daily', at: '09:00', until: '09:30' })).toContain('checkEvery');
    expect(
      problemOf({ _tag: 'Daily', checkEvery: '15 minutes', at: '09:00', until: '09:15' }),
    ).toBeUndefined();
    expect(problemOf({ _tag: 'Daily', window: { from: '08:00', to: '08:30' } })).toContain(
      'window',
    );
    expect(problemOf({ _tag: 'Daily', window: { from: '14:00', to: '08:00' } })).toContain(
      'window',
    );
    expect(
      problemOf({ _tag: 'Daily', window: { from: '08:00', to: '14:00' }, until: '12:00' }),
    ).toContain('window.to');
    expect(problemOf({ _tag: 'Daily' })).toBeUndefined();
  });

  it('defaults until to 24:00 with at, to window.to with a window, and to none otherwise', () => {
    expect(scheduleOf({ _tag: 'Daily', at: '09:00' }).untilMinute).toBe(24 * 60);
    expect(
      scheduleOf({ _tag: 'Daily', checkEvery: '5 minutes', window: { from: '08:00', to: '14:00' } })
        .untilMinute,
    ).toBe(14 * 60);
    expect(scheduleOf({ _tag: 'Daily' }).untilMinute).toBeUndefined();
    expect(scheduleOf({ _tag: 'Daily', until: '06:00' }).untilMinute).toBe(6 * 60);
  });

  it('starts at the first check at or after at, and not after until', () => {
    const schedule = scheduleOf({
      _tag: 'Daily',
      checkEvery: '15 minutes',
      at: '09:00',
      until: '12:00',
    });
    expect(isDailyRunDue(schedule, at('2026-11-26T08:45:00Z'), at('2026-11-26T08:45:00Z'))).toBe(
      false,
    );
    expect(isDailyRunDue(schedule, at('2026-11-26T09:00:00Z'), at('2026-11-26T09:00:00Z'))).toBe(
      true,
    );
    expect(isDailyRunDue(schedule, at('2026-11-26T11:45:00Z'), at('2026-11-26T12:00:59Z'))).toBe(
      true,
    );
    // A check scheduled in time but handled after until, and a check after until.
    expect(isDailyRunDue(schedule, at('2026-11-26T11:45:00Z'), at('2026-11-26T12:30:00Z'))).toBe(
      false,
    );
    expect(isDailyRunDue(schedule, at('2026-11-26T12:15:00Z'), at('2026-11-26T12:15:00Z'))).toBe(
      false,
    );
  });

  it('reads at and until in the time zone, and runs a time skipped by DST at the next check', () => {
    const brussels = scheduleOf({
      _tag: 'Daily',
      checkEvery: '30 minutes',
      at: '02:30',
      timeZone: 'Europe/Brussels',
    });
    // 2026-03-29: clocks go from 02:00 to 03:00 in Brussels, so 02:30 never happens.
    expect(isDailyRunDue(brussels, at('2026-03-29T00:30:00Z'), at('2026-03-29T00:30:00Z'))).toBe(
      false,
    );
    expect(isDailyRunDue(brussels, at('2026-03-29T01:00:00Z'), at('2026-03-29T01:00:00Z'))).toBe(
      true,
    );
    const summer = scheduleOf({ _tag: 'Daily', at: '09:00', timeZone: 'Europe/Brussels' });
    expect(isDailyRunDue(summer, at('2026-07-01T06:00:00Z'), at('2026-07-01T06:00:00Z'))).toBe(
      false,
    );
    expect(isDailyRunDue(summer, at('2026-07-01T07:00:00Z'), at('2026-07-01T07:00:00Z'))).toBe(
      true,
    );
  });

  it('picks a window slot per day that every worker agrees on and that leaves a check before to', () => {
    const cadence: DailyCadence = {
      _tag: 'Daily',
      checkEvery: '5 minutes',
      window: { from: '08:00', to: '14:00' },
    };
    const schedule = scheduleOf(cadence);
    const dues = Array.from({ length: 60 }, (_, day) => {
      const date = new Date(Date.UTC(2026, 10, 1 + day)).toISOString().slice(0, 10);
      return schedule.dueMinuteOn(date);
    });
    for (const due of dues) {
      expect(due).toBeGreaterThanOrEqual(8 * 60);
      expect(due).toBeLessThanOrEqual(14 * 60 - 5);
      expect((due - 8 * 60) % 5).toBe(0);
    }

    expect(new Set(dues).size).toBeGreaterThan(30);
    expect(scheduleOf(cadence).dueMinuteOn('2026-11-26')).toBe(dues[25]);
    expect(scheduleOf(cadence, 'another-job').dueMinuteOn('2026-11-26')).not.toBe(dues[25]);
  });

  it('runs an at and until span skipped by the spring change at the first check after it', () => {
    const schedule = scheduleOf({
      _tag: 'Daily',
      at: '02:00',
      until: '02:30',
      checkEvery: '30 minutes',
      timeZone: 'America/New_York',
    });
    // 2026-03-08: New York clocks go from 02:00 to 03:00 (07:00Z), so 02:00 to 02:30 never happens.
    const checks = Array.from(
      { length: 16 },
      (_, index) => at('2026-03-08T05:00:00Z') + index * 30 * 60_000,
    );
    const due = checks.filter((ms) => isDailyRunDue(schedule, ms, ms));
    expect(due.map((ms) => new Date(ms).toISOString())).toEqual([
      '2026-03-08T07:00:00.000Z',
      '2026-03-08T07:30:00.000Z',
    ]);
    // The deadline moves by the length of the gap: 02:30 reads as 03:30 local (07:30Z).
    expect(isWithinDailyDeadline(schedule, '2026-03-08', at('2026-03-08T07:30:59Z'))).toBe(true);
    expect(isWithinDailyDeadline(schedule, '2026-03-08', at('2026-03-08T07:31:00Z'))).toBe(false);
  });

  it('keeps retrying through the repeated hour of the autumn change', () => {
    const schedule = scheduleOf({
      _tag: 'Daily',
      at: '00:30',
      until: '01:30',
      checkEvery: '30 minutes',
      timeZone: 'America/New_York',
    });
    // 2026-11-01: 01:30 New York happens at 05:30Z (EDT) and again at 06:30Z (EST).
    expect(isWithinDailyDeadline(schedule, '2026-11-01', at('2026-11-01T06:30:00Z'))).toBe(true);
    expect(isWithinDailyDeadline(schedule, '2026-11-01', at('2026-11-01T06:31:00Z'))).toBe(false);
  });

  it('starts a window run strictly before window.to unless until is later', () => {
    const window: DailyWindow = { from: '08:00', to: '09:00' };
    const plain = scheduleOf({ _tag: 'Daily', checkEvery: '1 hour', window });
    expect(isDailyRunDue(plain, at('2026-11-26T08:00:00Z'), at('2026-11-26T08:59:59Z'))).toBe(true);
    expect(isDailyRunDue(plain, at('2026-11-26T08:00:00Z'), at('2026-11-26T09:00:00Z'))).toBe(
      false,
    );
    expect(isDailyRunDue(plain, at('2026-11-26T09:00:00Z'), at('2026-11-26T09:00:00Z'))).toBe(
      false,
    );
    // Retries still run up to until, which defaults to window.to.
    expect(isWithinDailyDeadline(plain, '2026-11-26', at('2026-11-26T09:00:30Z'))).toBe(true);
    const later = scheduleOf({ _tag: 'Daily', checkEvery: '1 hour', window, until: '11:00' });
    expect(isDailyRunDue(later, at('2026-11-26T10:00:00Z'), at('2026-11-26T10:00:00Z'))).toBe(true);
    const brussels = scheduleOf({
      _tag: 'Daily',
      checkEvery: '1 hour',
      window,
      timeZone: 'Europe/Brussels',
    });
    expect(isDailyRunDue(brussels, at('2026-11-26T07:00:00Z'), at('2026-11-26T07:30:00Z'))).toBe(
      true,
    );
    expect(isDailyRunDue(brussels, at('2026-11-26T07:00:00Z'), at('2026-11-26T08:00:00Z'))).toBe(
      false,
    ); // 2026-03-29: 02:00 to 03:00 never happens in Brussels; the run starts at 03:00 local (01:00Z).
    const gap = scheduleOf({
      _tag: 'Daily',
      checkEvery: '15 minutes',
      window: { from: '02:00', to: '03:00' },
      timeZone: 'Europe/Brussels',
    });
    expect(isDailyRunDue(gap, at('2026-03-29T01:00:00Z'), at('2026-03-29T01:00:00Z'))).toBe(true);
    expect(isDailyRunDue(gap, at('2026-03-30T01:00:00Z'), at('2026-03-30T01:00:00Z'))).toBe(false);
  });

  it('allows retries up to until on the run date only', () => {
    const schedule = scheduleOf({ _tag: 'Daily', at: '09:00', until: '12:00' });
    expect(isWithinDailyDeadline(schedule, '2026-11-26', at('2026-11-26T12:00:30Z'))).toBe(true);
    expect(isWithinDailyDeadline(schedule, '2026-11-26', at('2026-11-26T12:01:00Z'))).toBe(false);
    expect(isWithinDailyDeadline(schedule, '2026-11-26', at('2026-11-27T08:00:00Z'))).toBe(false);
    expect(
      isWithinDailyDeadline(scheduleOf({ _tag: 'Daily' }), '2026-11-26', at('2026-11-28T00:00Z')),
    ).toBe(true);
    const endOfDay = scheduleOf({ _tag: 'Daily', at: '09:00' });
    expect(isWithinDailyDeadline(endOfDay, '2026-11-26', at('2026-11-26T23:59:59Z'))).toBe(true);
    expect(isWithinDailyDeadline(endOfDay, '2026-11-26', at('2026-11-27T00:00:00Z'))).toBe(false);
    expect(isWithinDailyDeadline(endOfDay, 'not-a-date', at('2026-11-26T09:00:00Z'))).toBe(false);
  });

  it('reads until in the time zone', () => {
    const brussels = scheduleOf({
      _tag: 'Daily',
      at: '09:00',
      until: '12:00',
      timeZone: 'Europe/Brussels',
    });
    // 12:00 in Brussels is 11:00Z in November.
    expect(isWithinDailyDeadline(brussels, '2026-11-26', at('2026-11-26T11:00:30Z'))).toBe(true);
    expect(isWithinDailyDeadline(brussels, '2026-11-26', at('2026-11-26T11:30:00Z'))).toBe(false);
  });

  it('lifts the failure budget only for a Daily cadence with a deadline', () => {
    expect(failureBudgetFor({ _tag: 'Daily', at: '09:00' })).toBe(Number.POSITIVE_INFINITY);
    expect(failureBudgetFor({ _tag: 'Daily', until: '12:00' })).toBe(Number.POSITIVE_INFINITY);
    expect(failureBudgetFor({ _tag: 'Daily' })).toBeUndefined();
    expect(failureBudgetFor({ _tag: 'Daily', timeZone: 'Europe/Brussels' })).toBeUndefined();
    expect(failureBudgetFor({ _tag: 'OnDemand' })).toBeUndefined();
  });
});

/**
 * Whether `parseCronCadence` rejects `cadence`.
 */
const problem = (cadence: Extract<JobCadence, { readonly _tag: 'Cron' }>) =>
  Result.isFailure(parseCronCadence(cadence));

/**
 * The run key `resolveRunKey` gives a job on `cadence` with `runKey`, or its error's `_tag`.
 */
const resolve = (cadence: JobCadence, runKey: string | undefined) =>
  Effect.runPromise(
    resolveRunKey({ name: 'job', cadence }, at('2026-11-26T09:00:00Z'), runKey).pipe(
      Effect.match({ onFailure: (error) => error._tag, onSuccess: (key) => key }),
    ),
  );

/**
 * What `validateCadence` says about a daily 09:00 `Cron` cadence with `catchUpWithin`.
 */
const catchUpError = (catchUpWithin: Duration.Input) =>
  Effect.runPromise(
    validateCadence('nightly', { _tag: 'Cron', expression: '0 9 * * *', catchUpWithin }).pipe(
      Effect.match({
        onFailure: (error) => `${error._tag}: ${error.message}`,
        onSuccess: () => 'valid',
      }),
    ),
  );

/**
 * A type guard that lies: it accepts any defined value as a string, as an untyped caller would.
 */
const claimsString = (value: unknown): value is string => value !== undefined;

/**
 * `value` typed as a string without checking it, as JavaScript or JSON input can arrive.
 */
const untypedString = (value: unknown): string => (claimsString(value) ? value : '');

describe('Cron cadences', () => {
  it('rejects seconds fields, bad expressions and bad time zones', () => {
    expect(problem({ _tag: 'Cron', expression: '0 0 9 * * *' })).toBe(true);
    expect(problem({ _tag: 'Cron', expression: '0 25 * * *' })).toBe(true);
    expect(problem({ _tag: 'Cron', expression: '0 9 * * *', timeZone: 'Nowhere/Land' })).toBe(true);
    expect(
      problem({ _tag: 'Cron', expression: ' 0 9 * * 1-5 ', timeZone: 'Europe/Brussels' }),
    ).toBe(false);
  });

  it('rejects an expression that never matches a date', () => {
    expect(problem({ _tag: 'Cron', expression: '0 0 31 2 *' })).toBe(true);
    expect(problem({ _tag: 'Cron', expression: '0 0 30 2 *' })).toBe(true);
    expect(problem({ _tag: 'Cron', expression: '0 0 29 2 *' })).toBe(false);
    expect(problem({ _tag: 'Cron', expression: '0 0 31 * *' })).toBe(false);
  });

  it('rejects a catchUpWithin that is not a finite duration above zero', async () => {
    const invalid: ReadonlyArray<Duration.Input> = [
      -60_000,
      0,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      '-5 minutes',
    ];
    const errors = await Promise.all(invalid.map(catchUpError));
    for (const error of errors) {
      expect(error).toContain('ClusterConfigError: Job "nightly"');
    }

    // @ts-expect-error -- an untyped caller passes a string that is not a duration
    expect(await catchUpError('banana')).toContain('catchUpWithin');
    expect(await catchUpError('10 minutes')).toBe('valid');
    expect(await catchUpError(1)).toBe('valid');
  });

  it('keys a run by its tick in UTC, the same at the tick and until the next one', () => {
    const weekdays: JobCadence = {
      _tag: 'Cron',
      expression: '0 9 * * 1-5',
      timeZone: 'Europe/Brussels',
    };
    expect(runKeyFor(weekdays, at('2026-11-26T08:00:00Z'))).toBe('2026-11-26T08:00Z');
    expect(runKeyFor(weekdays, at('2026-11-26T07:59:59Z'))).toBe('2026-11-25T08:00Z');
    expect(runKeyFor(weekdays, at('2026-11-28T12:00:00Z'))).toBe('2026-11-27T08:00Z');
    expect(runKeyFor(weekdays, at('2026-07-01T07:00:00Z'))).toBe('2026-07-01T07:00Z');
  });

  it('runs a local time repeated by DST once, at its first occurrence', () => {
    const nightly: JobCadence = {
      _tag: 'Cron',
      expression: '30 2 * * *',
      timeZone: 'Europe/Brussels',
    };
    // 2026-10-25: 02:30 Brussels happens at 00:30Z and again at 01:30Z.
    expect(runKeyFor(nightly, at('2026-10-25T00:30:00Z'))).toBe('2026-10-25T00:30Z');
    expect(runKeyFor(nightly, at('2026-10-25T01:45:00Z'))).toBe('2026-10-25T00:30Z');
    // 2026-03-29: 02:30 Brussels is skipped; the tick moves to 03:30 local (01:30Z).
    expect(runKeyFor(nightly, at('2026-03-29T01:30:00Z'))).toBe('2026-03-29T01:30Z');
    expect(runKeyFor(nightly, at('2026-03-29T01:29:00Z'))).toBe('2026-03-28T01:30Z');
  });
});

describe('run keys', () => {
  it('keys a Daily run by its local date', () => {
    const brussels: JobCadence = { _tag: 'Daily', timeZone: 'Europe/Brussels' };
    expect(runKeyFor(brussels, at('2026-11-26T23:30:00Z'))).toBe('2026-11-27');
    expect(runKeyFor({ _tag: 'Daily' }, at('2026-11-26T23:30:00Z'))).toBe('2026-11-26');
  });

  it('accepts 1 to 200 printable ASCII characters as a caller key', () => {
    expect(runKeyProblem('welcome-email:user-42')).toBeUndefined();
    expect(runKeyProblem('a'.repeat(200))).toBeUndefined();
    expect(runKeyProblem('')).toBeDefined();
    expect(runKeyProblem('a'.repeat(201))).toBeDefined();
    expect(runKeyProblem('has space')).toBeDefined();
    expect(runKeyProblem('naïve')).toBeDefined();
    expect(runKeyProblem(42)).toBeDefined();
    expect(runKeyProblem(null)).toBeDefined();
  });

  it('takes a caller key for OnDemand jobs only', async () => {
    expect(await resolve({ _tag: 'OnDemand' }, 'k-1')).toBe('k-1');
    expect(await resolve({ _tag: 'OnDemand' }, undefined)).toBe('InvalidRunKey');
    expect(await resolve({ _tag: 'OnDemand' }, 'bad key')).toBe('InvalidRunKey');
    expect(await resolve({ _tag: 'Daily' }, 'k-1')).toBe('InvalidRunKey');
    expect(await resolve({ _tag: 'Daily' }, undefined)).toBe('2026-11-26');
    expect(await resolve({ _tag: 'OnDemand' }, untypedString(42))).toBe('InvalidRunKey');
  });

  it('fails with ClusterConfigError for a clock cadence that cannot give a key', async () => {
    expect(await resolve({ _tag: 'Daily', timeZone: 'Moon/Base' }, undefined)).toBe(
      'ClusterConfigError',
    );
    expect(await resolve({ _tag: 'Cron', expression: '0 0 31 2 *' }, undefined)).toBe(
      'ClusterConfigError',
    );
  });

  it('reports an invalid cadence as ClusterConfigError naming the job', async () => {
    const error = await Effect.runPromise(
      validateCadence('nightly', { _tag: 'Cron', expression: 'not a cron' }).pipe(Effect.flip),
    );
    expect(error._tag).toBe('ClusterConfigError');
    expect(error.message).toContain('Job "nightly"');
    await Effect.runPromise(validateCadence('ok', { _tag: 'Every', interval: '5 minutes' }));
  });
});

describe('OnDemand and deadline retry policy', () => {
  const now = new Date('2026-11-26T09:00:00Z');
  const { retry } = defaultJobsPolicy;

  it('backs OnDemand off from 1 minute, doubling, and exhausts after 5 failures', () => {
    const next = (failures: number) =>
      nextAttemptAt(retry, { cadence: 'OnDemand', failures, interrupted: false, now });
    expect(next(1)?.toISOString()).toBe('2026-11-26T09:01:00.000Z');
    expect(next(2)?.toISOString()).toBe('2026-11-26T09:02:00.000Z');
    expect(next(4)?.toISOString()).toBe('2026-11-26T09:08:00.000Z');
    expect(next(5)).toBeNull();
    expect(
      nextAttemptAt(retry, { cadence: 'OnDemand', failures: 2, interrupted: true, now }),
    ).toEqual(now);
    expect(
      nextAttemptAt(retry, { cadence: 'OnDemand', failures: 9, interrupted: true, now }),
    ).toBeNull();
  });

  it('keeps a Daily key with a deadline open past dailyMaxFailures', () => {
    const failures = retry.dailyMaxFailures + 3;
    expect(
      nextAttemptAt(retry, { cadence: 'Daily', failures, interrupted: false, now }),
    ).toBeNull();
    expect(
      nextAttemptAt(retry, {
        cadence: 'Daily',
        failures,
        interrupted: false,
        now,
        maxFailures: Number.POSITIVE_INFINITY,
      }),
    ).toEqual(now);
    expect(nextAttemptAt(retry, { cadence: 'Cron', failures, interrupted: false, now })).toEqual(
      now,
    );
  });

  it('validates the OnDemand policy fields', async () => {
    const error = await Effect.runPromise(
      resolveJobsPolicy({ retry: { onDemandMaxFailures: 0 } }).pipe(Effect.flip),
    );
    expect(error.message).toContain('retry.onDemandMaxFailures');
    const policy = await Effect.runPromise(
      resolveJobsPolicy({
        retry: { onDemandBase: '30 seconds' },
        retention: { onDemand: '7 days' },
      }),
    );
    expect(policy.retry.onDemandCap).toEqual(defaultJobsPolicy.retry.onDemandCap);
  });
});
