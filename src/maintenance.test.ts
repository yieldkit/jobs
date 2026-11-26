import { Duration, Effect, Layer, Schema } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { JobCadence } from './job.ts';
import type { JobsPolicyOverrides } from './policy.ts';

import { makeTestClusterLayer } from './cluster.ts';
import { defineJob } from './job.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';
import { clusterPruneTargetsFor } from './maintenance.ts';
import { defaultJobsPolicy, resolveJobsPolicy } from './policy.ts';
import { makeJobRequestsMemory } from './requests-memory.ts';
import { makeSchedulerLayer } from './scheduler.ts';
import { testClockLayer } from './test-clock.fixture.ts';

/**
 * A job on `cadence` that succeeds at once.
 */
const makeJob = (name: string, cadence: JobCadence) =>
  defineJob({
    name,
    description: name,
    cadence,
    params: { schema: Schema.Struct({}), defaults: {} },
    stats: Schema.Struct({}),
    run: () => Effect.succeed({}),
  });

/**
 * A policy whose retention periods all differ, so a swapped one shows.
 */
const DISTINCT_RETENTION: JobsPolicyOverrides = {
  retention: {
    everyRuns: '10 days',
    everyCluster: '2 days',
    dailyCluster: '3 days',
    onDemand: '1 hour',
  },
};

const JOBS = [
  makeJob('mailer', { _tag: 'OnDemand' }),
  makeJob('nightly', { _tag: 'Cron', expression: '0 3 * * *' }),
  makeJob('sync', { _tag: 'Every', interval: '5 minutes' }),
  makeJob('digest', { _tag: 'Daily' }),
  makeJob('import', { _tag: 'Once', start: 'automatic' }),
];

describe('maintenance', () => {
  it('prunes each cadence’s Cluster messages after its own retention', async () => {
    const policy = await Effect.runPromise(resolveJobsPolicy(DISTINCT_RETENTION));
    const targets = clusterPruneTargetsFor(JOBS, policy).map(
      (target) => `${target.entityType} ${Duration.format(target.retention)}`,
    );
    expect(targets.toSorted()).toEqual(
      [
        'ClusterCron/digest 3d',
        'ClusterCron/nightly 2d',
        'ClusterCron/sync 2d',
        'Workflow/-/DurableClock 1h',
        'Workflow/digest 3d',
        'Workflow/mailer 1h',
        'Workflow/nightly 2d',
        'Workflow/sync 2d',
      ].toSorted(),
    );
  });

  it('prunes durable clocks after retention.everyCluster when OnDemand workflows outlast it', async () => {
    const clock = clusterPruneTargetsFor(JOBS, defaultJobsPolicy).find(
      (target) => target.entityType === 'Workflow/-/DurableClock',
    );
    expect(clock === undefined ? undefined : Duration.format(clock.retention)).toBe('14d');
  });

  it('deletes OnDemand ledger rows older than retention.onDemand', async () => {
    const rows = await Effect.gen(function* () {
      const runs = yield* makeJobRunsMemory({
        policy: { ...DISTINCT_RETENTION, maintenance: { interval: '1 day' } },
      });
      const requests = yield* makeJobRequestsMemory();
      const ledger = yield* JobRuns.pipe(Effect.provide(runs.layer));
      /**
       * Claims `runKey` of `job` now, leaving a `running` row.
       */
      const claim = (job: string, runKey: string) =>
        ledger.claim({ job, runKey, trigger: 'schedule', force: false, params: {} });
      yield* claim('mailer', 'old');
      yield* claim('sync', '1970-01-01T00:00Z');
      yield* TestClock.adjust('2 hours');
      yield* claim('mailer', 'young');
      const jobs = JOBS.slice(0, 3);
      yield* Layer.build(
        makeSchedulerLayer(jobs, { only: ['mailer'] }).pipe(
          Layer.provide([
            makeTestClusterLayer(jobs, ['mailer']),
            runs.layer,
            requests.layer,
            SecureRandom.layerWebCrypto,
          ]),
        ),
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust('1 second');
      return runs
        .rows()
        .map((row) => `${row.job}/${row.runKey}`)
        .toSorted();
    }).pipe(Effect.scoped, Effect.provide(testClockLayer), Effect.runPromise);
    expect(rows).toEqual(['mailer/young', 'sync/1970-01-01T00:00Z']);
  });
});
