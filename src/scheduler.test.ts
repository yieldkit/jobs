import type { Duration } from 'effect';

import { Cron, Effect, Layer, Option, Schema } from 'effect';
import { ShardingConfig } from 'effect/cluster';
import { SecureRandom } from 'effect/crypto';
import { TestClock } from 'effect/testing';
import { WorkflowEngine } from 'effect/workflow';
import { describe, expect, it } from 'vite-plus/test';

import type { JobCadence } from './job.ts';
import type { OperatorJobParams } from './workflow.ts';

import {
  DEFAULT_SHARD_GROUP,
  MAINTENANCE_SHARD_GROUP,
  assignedShardGroupsFor,
  availableShardGroupsFor,
  checkShardingConfig,
  makeTestClusterLayer,
} from './cluster.ts';
import { cronForCadence } from './cron.ts';
import { defineJob } from './job.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';
import { makeJobRequestsMemory } from './requests-memory.ts';
import { makeSchedulerLayer, selectWorkerJobs } from './scheduler.ts';
import { testClockLayer } from './test-clock.fixture.ts';

const EmptyParams = { schema: Schema.Struct({}), defaults: {} };
const EmptyStats = Schema.Struct({});

const makeJob = (
  name: string,
  cadence:
    | { readonly _tag: 'Every'; readonly interval: '5 minutes' | '15 minutes' | '30 minutes' }
    | { readonly _tag: 'Daily'; readonly checkEvery: '1 hour' }
    | { readonly _tag: 'Once'; readonly start: 'automatic' | 'manual' },
) =>
  defineJob({
    name,
    description: name,
    cadence,
    params: EmptyParams,
    stats: EmptyStats,
    run: () => Effect.succeed({}),
  });

/**
 * Builds `makeSchedulerLayer(jobs, { only })` on the in-memory Cluster, with `config` in place of
 * the test layer's `ShardingConfig`, and returns its failure, or `undefined` when it builds.
 */
const schedulerFailure = (
  jobs: ReadonlyArray<ReturnType<typeof makeJob>>,
  config: Partial<ShardingConfig.ShardingConfig['Service']>,
  only?: ReadonlyArray<string>,
) =>
  Effect.gen(function* () {
    const runs = yield* makeJobRunsMemory();
    const requests = yield* makeJobRequestsMemory();
    return yield* Layer.build(
      makeSchedulerLayer(jobs, { only }).pipe(
        Layer.provide(ShardingConfig.layer(config)),
        Layer.provide([
          makeTestClusterLayer(jobs, only),
          runs.layer,
          requests.layer,
          SecureRandom.layerWebCrypto,
        ]),
      ),
    ).pipe(
      Effect.as(undefined),
      Effect.catch((error) => Effect.succeed(error)),
    );
  }).pipe(Effect.scoped, Effect.runPromise);

/**
 * The `checkShardingConfig` failure message for `Every` jobs named `names`, or `undefined` when it
 * passes.
 */
const shardingProblemWith = (names: ReadonlyArray<string>) => {
  const jobs = names.map((name) => makeJob(name, { _tag: 'Every', interval: '5 minutes' }));
  return Effect.runPromise(
    checkShardingConfig(jobs).pipe(
      Effect.match({ onFailure: (error) => error.message, onSuccess: () => undefined }),
      Effect.provide(ShardingConfig.layer({ availableShardGroups: availableShardGroupsFor(jobs) })),
    ),
  );
};

/**
 * A job on `cadence` whose `limit` param has `defaults` (none when omitted).
 */
const makeLimitJob = (
  name: string,
  cadence: JobCadence,
  defaults: { readonly limit?: number } = { limit: 1 },
) =>
  defineJob({
    name,
    description: name,
    cadence,
    params: { schema: Schema.Struct({ limit: Schema.Int }), defaults },
    stats: EmptyStats,
    run: () => Effect.succeed({}),
  });

/**
 * Builds `makeSchedulerLayer(jobs, { params })` on the in-memory Cluster and returns its failure,
 * or `undefined` when it builds.
 */
const startupFailureWith = (
  jobs: ReadonlyArray<ReturnType<typeof makeLimitJob>>,
  params: OperatorJobParams,
) =>
  Effect.gen(function* () {
    const runs = yield* makeJobRunsMemory();
    const requests = yield* makeJobRequestsMemory();
    return yield* Layer.build(
      makeSchedulerLayer(jobs, { params }).pipe(
        Layer.provide([
          makeTestClusterLayer(jobs),
          runs.layer,
          requests.layer,
          SecureRandom.layerWebCrypto,
        ]),
      ),
    ).pipe(
      Effect.as(undefined),
      Effect.catch((error) => Effect.succeed(error)),
    );
  }).pipe(Effect.scoped, Effect.runPromise);

const SAMPLE_JOBS = [
  makeJob('sync-orders', { _tag: 'Every', interval: '5 minutes' }),
  makeJob('sync-prices', { _tag: 'Every', interval: '15 minutes' }),
  makeJob('report-digest', { _tag: 'Every', interval: '30 minutes' }),
  makeJob('daily-report', { _tag: 'Daily', checkEvery: '1 hour' }),
  makeJob('initial-import', { _tag: 'Once', start: 'automatic' }),
  makeJob('manual-backfill', { _tag: 'Once', start: 'manual' }),
] as const;

/**
 * Starts a worker for `jobs` (only `only`, when given) on a memory ledger holding one `running` row
 * of the `Every` job `sync-orders` that is two hours old, with `retention.everyRuns` of one hour.
 * Advances `TestClock` by `after` and returns the jobs that still have ledger rows.
 */
const ledgerRowsAfterSweep = (options: {
  readonly only?: ReadonlyArray<string>;
  readonly interval: Duration.Input;
  readonly after: Duration.Input;
}) => {
  const jobs = [
    makeJob('daily-report', { _tag: 'Daily', checkEvery: '1 hour' }),
    makeJob('sync-orders', { _tag: 'Every', interval: '5 minutes' }),
  ];
  return Effect.gen(function* () {
    const runs = yield* makeJobRunsMemory({
      policy: { retention: { everyRuns: '1 hour' }, maintenance: { interval: options.interval } },
    });
    const requests = yield* makeJobRequestsMemory();
    yield* JobRuns.use((ledger) =>
      ledger.claim({
        job: 'sync-orders',
        runKey: '1970-01-01T00:00Z',
        trigger: 'schedule',
        force: false,
        params: {},
      }),
    ).pipe(Effect.provide(runs.layer));
    yield* TestClock.adjust('2 hours');
    const cluster = makeTestClusterLayer(jobs, options.only);
    yield* Layer.build(
      makeSchedulerLayer(jobs, { only: options.only }).pipe(
        Layer.provide([cluster, runs.layer, requests.layer, SecureRandom.layerWebCrypto]),
      ),
    );
    yield* Effect.yieldNow;
    yield* TestClock.adjust(options.after);
    return runs.rows().map((row) => `${row.job}/${row.runKey}`);
  }).pipe(Effect.scoped, Effect.provide(testClockLayer), Effect.runPromise);
};

describe('Effect Cluster scheduler and shard groups', () => {
  it('computes explicit UTC Cron expressions independent of host timezone', async () => {
    const prevTz = process.env.TZ;
    process.env.TZ = 'Asia/Tokyo';
    try {
      const every5 = await Effect.runPromise(
        cronForCadence({ _tag: 'Every', interval: '5 minutes' }),
      );
      const every15 = await Effect.runPromise(
        cronForCadence({ _tag: 'Every', interval: '15 minutes' }),
      );
      const every30 = await Effect.runPromise(
        cronForCadence({ _tag: 'Every', interval: '30 minutes' }),
      );
      const daily = await Effect.runPromise(
        cronForCadence({ _tag: 'Daily', checkEvery: '1 hour' }),
      );

      expect(Cron.format(every5)).toBe('0-55/5 * * * *');
      expect(Cron.format(every15)).toBe('0-45/15 * * * *');
      expect(Cron.format(every30)).toBe('0,30 * * * *');
      // Daily runs hourly UTC so a missed 00:00 tick catches up while deduping on YYYY-MM-DD.
      expect(Cron.format(daily)).toBe('0 * * * *');
      const dailyDefault = await Effect.runPromise(cronForCadence({ _tag: 'Daily' }));
      expect(Cron.format(dailyDefault)).toBe('0 * * * *');
      const dailyEvery15 = await Effect.runPromise(
        cronForCadence({ _tag: 'Daily', checkEvery: '15 minutes' }),
      );
      expect(Cron.format(dailyEvery15)).toBe('0-45/15 * * * *');
      expect(Cron.next(every5, '2026-11-24T23:55:00.000Z').toISOString()).toBe(
        '2026-11-25T00:00:00.000Z',
      );
      expect(Cron.next(every15, '2026-11-24T23:45:00.000Z').toISOString()).toBe(
        '2026-11-25T00:00:00.000Z',
      );
      expect(Cron.next(every30, '2026-11-24T23:30:00.000Z').toISOString()).toBe(
        '2026-11-25T00:00:00.000Z',
      );

      // Verify UTC matching even under TZ=Asia/Tokyo (+09:00).
      expect(Option.isSome(daily.tz)).toBe(true);
      expect(Cron.next(daily, '2026-11-24T23:30:00.000Z').toISOString()).toBe(
        '2026-11-25T00:00:00.000Z',
      );
    } finally {
      if (prevTz === undefined) {
        delete process.env.TZ;
      } else {
        process.env.TZ = prevTz;
      }
    }
  });

  it('rejects unsupported Every intervals and Daily checkEvery values with ClusterConfigError', async () => {
    const bad = await Effect.runPromise(
      cronForCadence({ _tag: 'Every', interval: '7 minutes' }).pipe(Effect.result),
    );
    expect(bad._tag).toBe('Failure');
    expect(bad).toMatchObject({ failure: { _tag: 'ClusterConfigError' } });

    const badDaily = await Effect.runPromise(
      Effect.flip(cronForCadence({ _tag: 'Daily', checkEvery: '90 minutes' })),
    );
    expect(badDaily._tag).toBe('ClusterConfigError');
    expect(badDaily.message).toContain('Daily checkEvery');
  });

  it('excludes manual Once jobs from shard groups and worker selection', async () => {
    const available = availableShardGroupsFor(SAMPLE_JOBS);
    expect(available).toEqual([
      DEFAULT_SHARD_GROUP,
      MAINTENANCE_SHARD_GROUP,
      'daily-report',
      'initial-import',
      'report-digest',
      'sync-orders',
      'sync-prices',
    ]);
    expect(available).not.toContain('manual-backfill');

    const subset = assignedShardGroupsFor(SAMPLE_JOBS, ['sync-orders', 'manual-backfill']);
    expect(subset).toEqual([DEFAULT_SHARD_GROUP, MAINTENANCE_SHARD_GROUP, 'sync-orders']);

    const selected = await Effect.runPromise(selectWorkerJobs(SAMPLE_JOBS, undefined));
    expect(selected.enabledJobs.map((j) => j.name)).not.toContain('manual-backfill');

    const unknown = await Effect.runPromise(
      selectWorkerJobs(SAMPLE_JOBS, ['sync-orders', 'ghost']).pipe(Effect.result),
    );
    expect(unknown._tag).toBe('Failure');
    expect(unknown).toMatchObject({ failure: { _tag: 'UnknownJobError', names: ['ghost'] } });
  });

  it('starts on a ShardingConfig that adds the jobs groups to your own', async () => {
    const jobs = [makeJob('sync-orders', { _tag: 'Every', interval: '5 minutes' })];
    const failure = await schedulerFailure(jobs, {
      availableShardGroups: ['default', 'my-entities', ...availableShardGroupsFor(jobs)],
      assignedShardGroups: ['default', 'my-entities', ...assignedShardGroupsFor(jobs, undefined)],
      shardsPerGroup: 300,
    });
    expect(failure).toBeUndefined();
  });

  it('fails at start-up when the ShardingConfig lacks or misassigns a jobs shard group', async () => {
    const jobs = [
      makeJob('sync-orders', { _tag: 'Every', interval: '5 minutes' }),
      makeJob('sync-prices', { _tag: 'Every', interval: '15 minutes' }),
    ];
    const available = [...availableShardGroupsFor(jobs)];

    // Effect's defaults: only `default`, so the jobs' messages would be stored and never run.
    const defaults = await schedulerFailure(jobs, {});
    expect(defaults).toMatchObject({ _tag: 'ClusterConfigError' });
    expect(String(defaults?.message)).toContain('availableShardGroups lacks yieldkit-jobs');

    // A worker that runs sync-orders must claim its group and the maintenance group.
    const unclaimed = await schedulerFailure(jobs, {
      availableShardGroups: available,
      assignedShardGroups: ['default'],
    });
    expect(String(unclaimed?.message)).toContain(
      'assignedShardGroups lacks yieldkit-jobs, sync-orders, sync-prices',
    );

    // Claiming a job's group without registering the job leaves its workflows without a handler.
    const unhandled = await schedulerFailure(
      jobs,
      { availableShardGroups: available, assignedShardGroups: available },
      ['sync-orders'],
    );
    expect(String(unhandled?.message)).toContain('claims sync-prices');
  });

  it('prunes finished requests older than retention.requests on each maintenance sweep', async () => {
    const manualOnly = [makeJob('manual-backfill', { _tag: 'Once', start: 'manual' })];
    const rows = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runs = yield* makeJobRunsMemory({
            policy: {
              retention: { requests: '3 minutes' },
              maintenance: { interval: '1 minute', batchSize: 1 },
            },
          });
          const requests = yield* makeJobRequestsMemory();
          const ids = [
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000002',
          ];
          for (const id of ids) {
            yield* requests.service.create({
              id,
              job: 'manual-backfill',
              params: {},
              force: false,
              executionId: id,
            });
          }

          yield* requests.service.create({
            id: '00000000-0000-4000-8000-000000000003',
            job: 'manual-backfill',
            params: {},
            force: false,
            executionId: 'pending',
          });
          for (const id of ids) {
            yield* requests.service.markFinished({ id, runKey: 'once', outcome: {} });
          }

          const cluster = makeTestClusterLayer(manualOnly);
          yield* Layer.build(
            makeSchedulerLayer(manualOnly).pipe(
              Layer.provide([cluster, runs.layer, requests.layer, SecureRandom.layerWebCrypto]),
            ),
          );
          const afterOneDay = yield* Effect.gen(function* () {
            yield* TestClock.adjust('1 minute');
            return requests.rows().length;
          });
          for (let minute = 0; minute < 3; minute += 1) {
            yield* Effect.yieldNow;
            yield* TestClock.adjust('1 minute');
          }

          return { afterOneDay, remaining: requests.rows().map((row) => row.executionId) };
        }),
      ).pipe(Effect.provide(testClockLayer)),
    );
    expect(rows.afterOneDay).toBe(3);
    expect(rows.remaining).toEqual(['pending']);
  });

  it('prunes ledger rows of Every jobs that run on other workers', async () => {
    const rows = await ledgerRowsAfterSweep({
      only: ['daily-report'],
      interval: '1 minute',
      after: '1 minute',
    });
    expect(rows).not.toContain('sync-orders/1970-01-01T00:00Z');
  });

  it('runs the first maintenance sweep at start-up', async () => {
    const rows = await ledgerRowsAfterSweep({ interval: '1 day', after: '1 second' });
    expect(rows).not.toContain('sync-orders/1970-01-01T00:00Z');
  });

  it('retries the start-up dispatch of an automatic Once job until it reaches Cluster', async () => {
    const jobs = [makeJob('initial-import', { _tag: 'Once', start: 'automatic' })];
    let executes = 0;
    const rows = await Effect.gen(function* () {
      const runs = yield* makeJobRunsMemory();
      const requests = yield* makeJobRequestsMemory();
      const cluster = makeTestClusterLayer(jobs);
      const flakyEngine = Layer.effect(
        WorkflowEngine.WorkflowEngine,
        Effect.map(WorkflowEngine.WorkflowEngine, (engine) =>
          WorkflowEngine.WorkflowEngine.of({
            ...engine,
            execute: (workflow, options) => {
              executes += 1;
              return executes === 1
                ? Effect.die(new Error('cluster unreachable'))
                : engine.execute(workflow, options);
            },
          }),
        ),
      ).pipe(Layer.provideMerge(cluster));
      yield* Layer.build(
        makeSchedulerLayer(jobs).pipe(
          Layer.provide([flakyEngine, runs.layer, requests.layer, SecureRandom.layerWebCrypto]),
        ),
      );
      // Several minutes, so a singleton that starts late under load still reaches its retry.
      for (let second = 0; second < 300; second += 1) {
        yield* Effect.yieldNow;
        yield* TestClock.adjust('1 second');
      }

      return runs.rows();
    }).pipe(Effect.scoped, Effect.provide(testClockLayer), Effect.runPromise);
    expect(executes).toBe(2);
    expect(rows).toMatchObject([{ job: 'initial-import', runKey: 'once', status: 'completed' }]);
  });

  it('accepts a one-day interval as a daily midnight cron', async () => {
    const every = await Effect.runPromise(cronForCadence({ _tag: 'Every', interval: '1 day' }));
    const daily = await Effect.runPromise(
      cronForCadence({ _tag: 'Daily', checkEvery: '24 hours' }),
    );
    expect(Cron.format(every)).toBe('0 0 * * *');
    expect(Cron.format(daily)).toBe('0 0 * * *');
  });

  it('rejects job names that do not fit shard ids or take the maintenance group', async () => {
    const check = (
      jobs: ReadonlyArray<ReturnType<typeof makeJob>>,
      config: Partial<ShardingConfig.ShardingConfig['Service']>,
    ) =>
      Effect.runPromise(
        checkShardingConfig(jobs).pipe(
          Effect.flip,
          Effect.provide(
            ShardingConfig.layer({
              availableShardGroups: availableShardGroupsFor(jobs),
              ...config,
            }),
          ),
        ),
      );
    const fortySeven = makeJob('x'.repeat(47), { _tag: 'Every', interval: '5 minutes' });
    // `<name>:<n>` has to fit VARCHAR(50): 47 + 1 + 2 digits fits, 3 digits does not.
    const tooLong = await check([fortySeven], { shardsPerGroup: 300 });
    expect(tooLong.message).toContain('Job names longer than 46 characters');
    const fits = await Effect.runPromise(
      checkShardingConfig([fortySeven]).pipe(
        Effect.provide(
          ShardingConfig.layer({
            availableShardGroups: availableShardGroupsFor([fortySeven]),
            shardsPerGroup: 16,
          }),
        ),
      ),
    );
    expect(fits).toBeUndefined();

    const reserved = await check(
      [makeJob('yieldkit-jobs', { _tag: 'Every', interval: '5 minutes' })],
      {},
    );
    expect(reserved.message).toContain('reserved');
  });

  it('rejects job names that Effect Cluster cannot store or that take one of its groups', async () => {
    const invalid = ['daily"report', "owner's-report", '\u{1F600}'.repeat(24), '-/DurableClock'];
    const problems = await Promise.all(invalid.map((name) => shardingProblemWith([name])));
    for (const [index, name] of invalid.entries()) {
      expect(problems[index]).toContain(name);
    }

    expect(await shardingProblemWith([DEFAULT_SHARD_GROUP])).toBe(
      'Job name "default" is reserved for the default shard group.',
    );
    expect(await shardingProblemWith(['daily-report_v2.1'])).toBeUndefined();
  });

  it('rejects two jobs with the same name', async () => {
    const jobs = [
      makeJob('review', { _tag: 'Every', interval: '5 minutes' }),
      makeJob('review', { _tag: 'Once', start: 'manual' }),
      makeJob('sync-orders', { _tag: 'Every', interval: '5 minutes' }),
    ];
    const failure = await Effect.runPromise(
      checkShardingConfig(jobs).pipe(
        Effect.flip,
        Effect.provide(
          ShardingConfig.layer({ availableShardGroups: availableShardGroupsFor(jobs) }),
        ),
      ),
    );
    expect(failure).toMatchObject({ _tag: 'ClusterConfigError' });
    expect(failure.message).toContain('review');
    expect(failure.message).not.toContain('sync-orders');
  });

  it('fails at start-up when the operator params of a job it starts itself do not decode', async () => {
    const every = await startupFailureWith(
      [makeLimitJob('sync-orders', { _tag: 'Every', interval: '5 minutes' })],
      { 'sync-orders': { limit: 'many' } },
    );
    const once = await startupFailureWith(
      [makeLimitJob('initial-import', { _tag: 'Once', start: 'automatic' })],
      { 'initial-import': { limit: 'many' } },
    );
    expect(every).toMatchObject({ _tag: 'ClusterConfigError' });
    expect(every?.message).toContain('Job "sync-orders"');
    expect(once).toMatchObject({ _tag: 'ClusterConfigError' });
    expect(once?.message).toContain('Job "initial-import"');
  });

  it('starts with an OnDemand job whose required params come with each dispatch', async () => {
    const failure = await startupFailureWith(
      [makeLimitJob('send-report', { _tag: 'OnDemand' }, {})],
      {},
    );
    expect(failure).toBeUndefined();
  });
});
