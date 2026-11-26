import { Duration, Effect } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import type { JobCadence } from './job.ts';
import type { JobsPolicyOverrides } from './policy.ts';

import {
  defaultJobsPolicy,
  heartbeatIntervalFor,
  nextAttemptAt,
  onDemandRetryDelay,
  onceRetryDelay,
  resolveJobsPolicy,
} from './policy.ts';

const HOUR_MS = Duration.toMillis(Duration.hours(1));
const { retry } = defaultJobsPolicy;

describe('policy', () => {
  it('defaults to a 2-minute lease with 30-second heartbeats and the documented limits', () => {
    expect(Duration.toMillis(defaultJobsPolicy.lease.duration)).toBe(
      Duration.toMillis(Duration.minutes(2)),
    );
    expect(Duration.toMillis(heartbeatIntervalFor(defaultJobsPolicy.lease))).toBe(
      Duration.toMillis(Duration.seconds(30)),
    );
    expect(retry.dailyMaxFailures).toBe(6);
    expect(Duration.toMillis(retry.onceBase)).toBe(HOUR_MS);
    expect(Duration.toMillis(retry.onceCap)).toBe(24 * HOUR_MS);
    expect(Duration.toMillis(retry.busyDelay)).toBe(Duration.toMillis(Duration.minutes(1)));
    expect(retry.busyMaxRetries).toBe(15);
    expect(retry.maxCrashAttempts).toBe(3);
    expect(Duration.toMillis(defaultJobsPolicy.retention.everyRuns)).toBe(14 * 24 * HOUR_MS);
    expect(Duration.toMillis(defaultJobsPolicy.retention.everyCluster)).toBe(14 * 24 * HOUR_MS);
    expect(Duration.toMillis(defaultJobsPolicy.retention.dailyCluster)).toBe(30 * 24 * HOUR_MS);
    expect(Duration.toMillis(defaultJobsPolicy.retention.requests)).toBe(30 * 24 * HOUR_MS);
    expect(Duration.toMillis(defaultJobsPolicy.maintenance.interval)).toBe(24 * HOUR_MS);
    expect(defaultJobsPolicy.maintenance.batchSize).toBe(1000);
  });

  it('merges overrides group by group and keeps every other default', async () => {
    const policy = await Effect.runPromise(
      resolveJobsPolicy({
        lease: { duration: '20 seconds' },
        retry: { maxCrashAttempts: 5 },
        retention: { requests: '7 days' },
      }),
    );
    expect(Duration.toMillis(policy.lease.duration)).toBe(20_000);
    expect(policy.lease.heartbeatsPerLease).toBe(4);
    expect(Duration.toMillis(heartbeatIntervalFor(policy.lease))).toBe(5000);
    expect(policy.retry).toEqual({ ...retry, maxCrashAttempts: 5 });
    expect(Duration.toMillis(policy.retention.requests)).toBe(7 * 24 * HOUR_MS);
    expect(policy.retention.everyRuns).toEqual(defaultJobsPolicy.retention.everyRuns);
    expect(policy.maintenance).toEqual(defaultJobsPolicy.maintenance);
  });

  it('rejects non-positive durations and counts that are not positive integers', async () => {
    const failures = await Effect.runPromise(
      Effect.forEach(
        [
          { lease: { duration: 0 } },
          { retry: { onceCap: Duration.infinity } },
          { retry: { dailyMaxFailures: 0 } },
          { maintenance: { batchSize: 2.5 } },
          { lease: { heartbeatsPerLease: -1 } },
        ],
        (overrides) => Effect.flip(resolveJobsPolicy(overrides)),
      ),
    );
    expect(failures.map((error) => error._tag)).toEqual(Array(5).fill('ClusterConfigError'));
    expect(failures.map((error) => error.message.split(' ')[0])).toEqual([
      'policy.lease.duration',
      'policy.retry.onceCap',
      'policy.retry.dailyMaxFailures',
      'policy.maintenance.batchSize',
      'policy.lease.heartbeatsPerLease',
    ]);
  });

  it('returns now immediately for interrupted runs, except on an exhausted Daily key', () => {
    const now = new Date('2026-11-24T12:00:00.000Z');
    const cadences: ReadonlyArray<JobCadence['_tag']> = ['Daily', 'Every', 'Once'];
    for (const cadence of cadences) {
      for (const failures of [0, 1, 5]) {
        const result = nextAttemptAt(retry, { cadence, failures, interrupted: true, now });
        expect(result).toEqual(now);
        expect(result).not.toBe(now);
      }
    }

    for (const failures of [retry.dailyMaxFailures, 10]) {
      expect(
        nextAttemptAt(retry, { cadence: 'Daily', failures, interrupted: true, now }),
      ).toBeNull();
      expect(nextAttemptAt(retry, { cadence: 'Once', failures, interrupted: true, now })).toEqual(
        now,
      );
    }
  });

  it('retries Daily failures immediately below dailyMaxFailures and exhausts at the limit', () => {
    const now = new Date('2026-11-24T12:00:00.000Z');
    for (const failures of [1, 2, 3, 4, 5]) {
      expect(nextAttemptAt(retry, { cadence: 'Daily', failures, interrupted: false, now })).toEqual(
        now,
      );
    }

    for (const failures of [retry.dailyMaxFailures, 7, 10]) {
      expect(
        nextAttemptAt(retry, { cadence: 'Daily', failures, interrupted: false, now }),
      ).toBeNull();
    }

    const stricter = { ...retry, dailyMaxFailures: 2 };
    expect(
      nextAttemptAt(stricter, { cadence: 'Daily', failures: 2, interrupted: false, now }),
    ).toBeNull();
  });

  it('returns now immediately for non-interrupted Every failures', () => {
    const now = new Date('2026-11-24T12:00:00.000Z');
    for (const failures of [1, 2, 6, 20]) {
      expect(nextAttemptAt(retry, { cadence: 'Every', failures, interrupted: false, now })).toEqual(
        now,
      );
    }
  });

  it('backs off Once failures exponentially from onceBase and caps at onceCap', () => {
    const now = new Date('2026-11-24T12:00:00.000Z');
    const cases: ReadonlyArray<readonly [failures: number, expectedHours: number]> = [
      [1, 1],
      [2, 2],
      [3, 4],
      [4, 8],
      [5, 16],
      [6, 24],
      [10, 24],
    ];
    for (const [failures, expectedHours] of cases) {
      const result = nextAttemptAt(retry, { cadence: 'Once', failures, interrupted: false, now });
      expect(result).toEqual(new Date(now.getTime() + expectedHours * HOUR_MS));
    }

    const custom = { ...retry, onceBase: Duration.minutes(10), onceCap: Duration.minutes(30) };
    expect(Duration.toMillis(onceRetryDelay(custom, 1))).toBe(600_000);
    expect(Duration.toMillis(onceRetryDelay(custom, 3))).toBe(1_800_000);
  });

  it('applies the OnDemand overrides and caps its backoff at onDemandCap', async () => {
    const policy = await Effect.runPromise(
      resolveJobsPolicy({
        retry: { onDemandBase: '30 seconds', onDemandCap: '5 minutes', onDemandMaxFailures: 10 },
        retention: { onDemand: '2 days' },
      }),
    );
    expect(Duration.toMillis(policy.retry.onDemandBase)).toBe(30_000);
    expect(Duration.toMillis(policy.retry.onDemandCap)).toBe(300_000);
    expect(Duration.toMillis(policy.retention.onDemand)).toBe(48 * HOUR_MS);
    const delays = [1, 2, 3, 4, 5, 9].map((failures) =>
      Duration.toMillis(onDemandRetryDelay(policy.retry, failures)),
    );
    expect(delays).toEqual([30_000, 60_000, 120_000, 240_000, 300_000, 300_000]);
    const now = new Date('2026-11-24T12:00:00.000Z');
    expect(
      nextAttemptAt(policy.retry, { cadence: 'OnDemand', failures: 9, interrupted: false, now }),
    ).toEqual(new Date(now.getTime() + 300_000));
    expect(
      nextAttemptAt(policy.retry, { cadence: 'OnDemand', failures: 10, interrupted: false, now }),
    ).toBeNull();
  });

  it('keeps an exhausted OnDemand key exhausted after an interruption', () => {
    const now = new Date('2026-11-24T12:00:00.000Z');
    const at = (failures: number) =>
      nextAttemptAt(retry, { cadence: 'OnDemand', failures, interrupted: true, now });
    expect(at(retry.onDemandMaxFailures - 1)).toEqual(now);
    expect(at(retry.onDemandMaxFailures)).toBeNull();
    expect(at(9)).toBeNull();
  });

  it('rejects one heartbeat per lease and accepts two', async () => {
    const error = await Effect.runPromise(
      Effect.flip(resolveJobsPolicy({ lease: { heartbeatsPerLease: 1 } })),
    );
    expect(error._tag).toBe('ClusterConfigError');
    expect(error.message).toBe('policy.lease.heartbeatsPerLease must be an integer of at least 2.');
    const policy = await Effect.runPromise(resolveJobsPolicy({ lease: { heartbeatsPerLease: 2 } }));
    expect(policy.lease.heartbeatsPerLease).toBe(2);
  });

  it('rejects durations too long to add to a date, and accepts ten years', async () => {
    const tooLong: ReadonlyArray<JobsPolicyOverrides> = [
      { retry: { onDemandBase: 1e16, onDemandCap: 1e16 } },
      { retry: { onceCap: Duration.days(3651) } },
      { retention: { requests: '20000 weeks' } },
    ];
    const failures = await Effect.runPromise(
      Effect.forEach(tooLong, (overrides) => Effect.flip(resolveJobsPolicy(overrides))),
    );
    expect(failures.map((error) => error._tag)).toEqual(Array(3).fill('ClusterConfigError'));
    expect(failures.map((error) => error.message.split(' ')[0])).toEqual([
      'policy.retry.onDemandBase',
      'policy.retry.onceCap',
      'policy.retention.requests',
    ]);
    const policy = await Effect.runPromise(
      resolveJobsPolicy({
        retry: { onDemandBase: Duration.days(3650), onDemandCap: Duration.days(3650) },
      }),
    );
    const at = nextAttemptAt(policy.retry, {
      cadence: 'OnDemand',
      failures: 4,
      interrupted: false,
      now: new Date('2026-11-24T12:00:00.000Z'),
    });
    expect(at?.toISOString()).toBe('2036-11-21T12:00:00.000Z');
  });
});
