import { Duration } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import type { JobCadence } from './job.ts';

import { ONCE_RUN_KEY, runKeyFor } from './run-key.ts';

describe('run-key', () => {
  it('defines ONCE_RUN_KEY as once', () => {
    expect(ONCE_RUN_KEY).toBe('once');
  });

  it('formats Daily keys as YYYY-MM-DD in UTC across day, month, and year boundaries', () => {
    const daily: JobCadence = { _tag: 'Daily', checkEvery: '1 hour' };
    expect(runKeyFor(daily, Date.parse('2026-01-05T12:00:00.000Z'))).toBe('2026-01-05');
    expect(runKeyFor(daily, Date.parse('2026-11-24T00:00:00.000Z'))).toBe('2026-11-24');
    expect(runKeyFor(daily, Date.parse('2026-11-24T23:59:59.999Z'))).toBe('2026-11-24');
    expect(runKeyFor(daily, Date.parse('2026-11-25T00:00:00.000Z'))).toBe('2026-11-25');
    expect(runKeyFor(daily, Date.parse('2026-12-31T23:59:59.999Z'))).toBe('2026-12-31');
    expect(runKeyFor(daily, Date.parse('2027-01-01T00:00:00.000Z'))).toBe('2027-01-01');
  });

  it('floors Every keys with a 5-minute interval to UTC bucket starts', () => {
    const every5m: JobCadence = { _tag: 'Every', interval: '5 minutes' };
    const every5mDuration: JobCadence = { _tag: 'Every', interval: Duration.minutes(5) };

    expect(runKeyFor(every5m, Date.parse('2026-11-24T10:05:00.000Z'))).toBe('2026-11-24T10:05Z');
    expect(runKeyFor(every5m, Date.parse('2026-11-24T10:07:30.500Z'))).toBe('2026-11-24T10:05Z');
    expect(runKeyFor(every5m, Date.parse('2026-11-24T10:09:59.999Z'))).toBe('2026-11-24T10:05Z');
    expect(runKeyFor(every5m, Date.parse('2026-11-24T10:10:00.000Z'))).toBe('2026-11-24T10:10Z');
    expect(runKeyFor(every5mDuration, Date.parse('2026-11-24T00:04:59.999Z'))).toBe(
      '2026-11-24T00:00Z',
    );
  });

  it('floors Every keys with a 30-minute interval across bucket and day boundaries', () => {
    const every30m: JobCadence = { _tag: 'Every', interval: '30 minutes' };
    expect(runKeyFor(every30m, Date.parse('2026-11-24T10:00:00.000Z'))).toBe('2026-11-24T10:00Z');
    expect(runKeyFor(every30m, Date.parse('2026-11-24T10:14:59.999Z'))).toBe('2026-11-24T10:00Z');
    expect(runKeyFor(every30m, Date.parse('2026-11-24T10:29:59.999Z'))).toBe('2026-11-24T10:00Z');
    expect(runKeyFor(every30m, Date.parse('2026-11-24T10:30:00.000Z'))).toBe('2026-11-24T10:30Z');
    expect(runKeyFor(every30m, Date.parse('2026-11-24T23:59:59.999Z'))).toBe('2026-11-24T23:30Z');
    expect(runKeyFor(every30m, Date.parse('2026-11-25T00:00:00.000Z'))).toBe('2026-11-25T00:00Z');
  });

  it('returns ONCE_RUN_KEY for automatic and manual Once cadences', () => {
    const automatic: JobCadence = { _tag: 'Once', start: 'automatic' };
    const manual: JobCadence = { _tag: 'Once', start: 'manual' };
    const nowMs = Date.parse('2026-11-24T10:05:00.000Z');
    expect(runKeyFor(automatic, nowMs)).toBe(ONCE_RUN_KEY);
    expect(runKeyFor(manual, nowMs)).toBe(ONCE_RUN_KEY);
  });
});
