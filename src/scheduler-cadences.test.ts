import type { Duration } from 'effect';

import { Clock, DateTime, Effect, Layer, Predicate, Schema } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { TestClock } from 'effect/testing';
import { WorkflowEngine } from 'effect/workflow';
import { describe, expect, it } from 'vite-plus/test';

import type { InvalidDispatch, InvalidJobParams, InvalidRunKey } from './errors.ts';
import type { DailyWindow, JobCadence } from './job.ts';
import type { JobsPolicyOverrides } from './policy.ts';
import type { DispatchJobOptions } from './workflow.ts';

import { makeTestClusterLayer } from './cluster.ts';
import { defineJob } from './job.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { makeJobRequestsMemory } from './requests-memory.ts';
import { makeSchedulerLayer } from './scheduler.ts';
import { testClockLayer } from './test-clock.fixture.ts';
import { dispatchJob } from './workflow.ts';

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

type TestJob = ReturnType<typeof makeJob>;

/**
 * Starts a worker for `jobs` at `startIso` under `TestClock`, then for each checkpoint advances the
 * clock by `step` until the checkpoint's time and records the ledger rows as `job/runKey/status`.
 * `beforeCheckpoints` runs once the worker is up.
 */
const rowsOverTime = (options: {
  readonly jobs: ReadonlyArray<TestJob>;
  readonly startIso: string;
  readonly step: Duration.Input;
  readonly checkpoints: ReadonlyArray<string>;
  readonly policy?: JobsPolicyOverrides;
  readonly beforeCheckpoints?: Effect.Effect<
    unknown,
    InvalidRunKey | InvalidJobParams | InvalidDispatch,
    WorkflowEngine.WorkflowEngine
  >;
}) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(options.startIso));
    const runs = yield* makeJobRunsMemory({ policy: options.policy });
    const requests = yield* makeJobRequestsMemory();
    const cluster = makeTestClusterLayer(options.jobs);
    const context = yield* Layer.build(
      Layer.merge(
        cluster,
        makeSchedulerLayer(options.jobs).pipe(
          Layer.provide([cluster, runs.layer, requests.layer, SecureRandom.layerWebCrypto]),
        ),
      ),
    );
    yield* Effect.yieldNow;
    if (options.beforeCheckpoints !== undefined) {
      yield* Effect.provide(options.beforeCheckpoints, context);
    }

    const seen: Record<string, ReadonlyArray<string>> = {};
    for (const checkpoint of options.checkpoints) {
      const target = Date.parse(checkpoint);
      while ((yield* Clock.currentTimeMillis) < target) {
        yield* TestClock.adjust(options.step);
      }

      seen[checkpoint.slice(11, 16)] = runs
        .rows()
        .map((row) => `${row.job}/${row.runKey}/${row.status}`)
        .toSorted();
    }

    return seen;
  }).pipe(Effect.scoped, Effect.provide(testClockLayer), Effect.runPromise);

/**
 * The error `makeSchedulerLayer` fails with for one job on `cadence`.
 */
const failureOf = (cadence: JobCadence) => {
  const jobs = [makeJob('broken', cadence)];
  return Effect.gen(function* () {
    const runs = yield* makeJobRunsMemory();
    const requests = yield* makeJobRequestsMemory();
    return yield* Layer.build(
      makeSchedulerLayer(jobs).pipe(
        Layer.provide([
          makeTestClusterLayer(jobs),
          runs.layer,
          requests.layer,
          SecureRandom.layerWebCrypto,
        ]),
      ),
    ).pipe(Effect.flip);
  }).pipe(Effect.scoped, Effect.runPromise);
};

describe('scheduler cadences', () => {
  it('starts a Daily at run at the first check at or after the local time', async () => {
    const seen = await rowsOverTime({
      jobs: [
        makeJob('morning-digest', {
          _tag: 'Daily',
          checkEvery: '15 minutes',
          at: '09:00',
          timeZone: 'Europe/Brussels',
        }),
      ],
      startIso: '2026-11-26T07:00:00Z',
      step: '5 minutes',
      checkpoints: ['2026-11-26T07:55:00Z', '2026-11-26T08:05:00Z', '2026-11-26T10:00:00Z'],
    });
    expect(seen).toEqual({
      '07:55': [],
      '08:05': ['morning-digest/2026-11-26/completed'],
      '10:00': ['morning-digest/2026-11-26/completed'],
    });
  });

  it('does not start a Daily run after until, at start-up or on a later check', async () => {
    const seen = await rowsOverTime({
      jobs: [
        makeJob('late-digest', { _tag: 'Daily', at: '09:00', until: '12:00' }),
        makeJob('plain-daily', { _tag: 'Daily' }),
      ],
      startIso: '2026-11-26T12:30:00Z',
      step: '15 minutes',
      checkpoints: ['2026-11-26T14:00:00Z'],
    });
    expect(seen['14:00']).toEqual(['plain-daily/2026-11-26/completed']);
  });

  it('runs a Cron job at its ticks only, keyed by the tick', async () => {
    const seen = await rowsOverTime({
      jobs: [makeJob('weekday-report', { _tag: 'Cron', expression: '0 9 * * 1-5' })],
      startIso: '2026-11-26T08:50:00Z',
      step: '5 minutes',
      checkpoints: ['2026-11-26T08:55:00Z', '2026-11-26T09:05:00Z'],
    });
    expect(seen).toEqual({
      '08:55': [],
      '09:05': ['weekday-report/2026-11-26T09:00Z/completed'],
    });
  });

  it('registers OnDemand jobs without scheduling them', async () => {
    const mailer = makeJob('mailer', { _tag: 'OnDemand' });
    const seen = await rowsOverTime({
      jobs: [mailer],
      startIso: '2026-11-26T09:00:00Z',
      step: '1 minute',
      checkpoints: ['2026-11-26T09:10:00Z'],
      beforeCheckpoints: dispatchJob(mailer, { runKey: 'welcome:42' }),
    });
    expect(seen['09:10']).toEqual(['mailer/welcome:42/completed']);
  });

  it('runs a delayed OnDemand dispatch that outlives the durable clock retention', async () => {
    const mailer = makeJob('slow-mailer', { _tag: 'OnDemand' });
    const seen = await rowsOverTime({
      jobs: [mailer],
      startIso: '2026-11-26T09:00:00Z',
      step: '10 minutes',
      checkpoints: ['2026-11-26T12:50:00Z', '2026-11-26T13:10:00Z'],
      policy: {
        // retention.onDemand must outlast the delay: the sweep deletes a waiting workflow's messages.
        retention: { everyCluster: '1 hour', dailyCluster: '1 hour' },
        maintenance: { interval: '30 minutes' },
      },
      beforeCheckpoints: dispatchJob(mailer, { runKey: 'later:1', delay: '4 hours' }),
    });
    expect(seen).toEqual({
      '12:50': [],
      '13:10': ['slow-mailer/later:1/completed'],
    });
  });

  it('starts a window run inside the window only', async () => {
    const window: DailyWindow = { from: '08:00', to: '09:00' };
    const late = await rowsOverTime({
      jobs: [makeJob('window-late', { _tag: 'Daily', checkEvery: '1 hour', window })],
      startIso: '2026-11-26T09:00:00Z',
      step: '1 minute',
      checkpoints: ['2026-11-26T11:00:00Z'],
    });
    const inside = await rowsOverTime({
      jobs: [makeJob('window-inside', { _tag: 'Daily', checkEvery: '1 hour', window })],
      startIso: '2026-11-26T08:30:00Z',
      step: '1 minute',
      checkpoints: ['2026-11-26T08:31:00Z'],
    });
    expect(late['11:00']).toEqual([]);
    expect(inside['08:31']).toEqual(['window-inside/2026-11-26/completed']);
  });

  it('waits for runAt when a delay is given as well', async () => {
    const mailer = makeJob('run-at-mailer', { _tag: 'OnDemand' });
    const seen = await rowsOverTime({
      jobs: [mailer],
      startIso: '2026-11-26T09:00:00Z',
      step: '1 minute',
      checkpoints: ['2026-11-26T09:10:00Z', '2026-11-26T10:01:00Z'],
      beforeCheckpoints: dispatchJob(mailer, {
        runKey: 'both:1',
        runAt: DateTime.makeUnsafe('2026-11-26T10:00:00Z'),
        delay: '1 minute',
      }),
    });
    expect(seen).toEqual({
      '09:10': [],
      '10:01': ['run-at-mailer/both:1/completed'],
    });
  });

  it('rejects a delay or runAt that is not a valid instant before dispatching', async () => {
    const mailer = makeJob('strict-mailer', { _tag: 'OnDemand' });
    const invalid: ReadonlyArray<DispatchJobOptions> = [
      { runKey: 'k', delay: Number.POSITIVE_INFINITY },
      { runKey: 'k', delay: Number.NaN },
      { runKey: 'k', delay: -60_000 },
      { runKey: 'k', delay: '-5 minutes' },
      { runKey: 'k', delay: 1e300 },
      // @ts-expect-error -- an untyped caller passes a string that is not a duration
      { runKey: 'k', delay: 'banana' },
      // @ts-expect-error -- an untyped caller passes a string for runAt
      { runKey: 'k', runAt: 'tomorrow' },
    ];
    const results = await Effect.runPromise(
      Effect.forEach(invalid, (options) =>
        dispatchJob(mailer, options).pipe(
          Effect.match({ onFailure: (error) => error._tag, onSuccess: () => 'dispatched' }),
        ),
      ).pipe(Effect.provide(makeTestClusterLayer([mailer]))),
    );
    expect(results).toEqual(invalid.map(() => 'InvalidDispatch'));
  });

  it('checks the cadence of every job, including those only leaves out', async () => {
    const fine = makeJob('fine', { _tag: 'Every', interval: '5 minutes' });
    /**
     * The error a worker running only `fine` fails with when `excluded` is deployed beside it.
     */
    const failureWith = (excluded: TestJob) =>
      Effect.gen(function* () {
        const runs = yield* makeJobRunsMemory();
        const requests = yield* makeJobRequestsMemory();
        const deployed = [excluded, fine];
        return yield* Layer.build(
          makeSchedulerLayer(deployed, { only: ['fine'] }).pipe(
            Layer.provide([
              makeTestClusterLayer(deployed, ['fine']),
              runs.layer,
              requests.layer,
              SecureRandom.layerWebCrypto,
            ]),
          ),
        ).pipe(Effect.flip);
      }).pipe(Effect.scoped, Effect.runPromise);
    const badZone = await failureWith(
      makeJob('moon-daily', { _tag: 'Daily', timeZone: 'Moon/Base' }),
    );
    const badInterval = await failureWith(
      makeJob('odd-every', { _tag: 'Every', interval: '7 minutes' }),
    );
    expect(badZone._tag).toBe('ClusterConfigError');
    expect(badZone.message).toContain('Moon/Base');
    expect(badInterval._tag).toBe('ClusterConfigError');
  });

  it('fails to start on an invalid cadence', async () => {
    const badZone = await failureOf({ _tag: 'Daily', at: '09:00', timeZone: 'Moon/Base' });
    const badCron = await failureOf({ _tag: 'Cron', expression: '61 * * * *' });
    expect(badZone._tag).toBe('ClusterConfigError');
    expect(badZone.message).toContain('Moon/Base');
    expect(badCron._tag).toBe('ClusterConfigError');
  });

  it('retries a failed dispatch of an Every tick under the same run key', async () => {
    const jobs = [makeJob('every-minute', { _tag: 'Every', interval: '1 minute' })];
    const failingKey = '2026-11-26T09:01Z';
    let failed = false;
    const keys = await Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse('2026-11-26T09:00:00Z'));
      const runs = yield* makeJobRunsMemory();
      const requests = yield* makeJobRequestsMemory();
      const cluster = makeTestClusterLayer(jobs);
      const flakyEngine = Layer.effect(
        WorkflowEngine.WorkflowEngine,
        Effect.map(WorkflowEngine.WorkflowEngine, (engine) =>
          WorkflowEngine.WorkflowEngine.of({
            ...engine,
            execute: (workflow, executeOptions) => {
              const { payload } = executeOptions;
              if (
                !failed &&
                Predicate.hasProperty(payload, 'runKey') &&
                payload.runKey === failingKey
              ) {
                failed = true;
                return Effect.die(new Error('workflow engine unavailable'));
              }

              return engine.execute(workflow, executeOptions);
            },
          }),
        ),
      ).pipe(Layer.provideMerge(cluster));
      yield* Layer.build(
        makeSchedulerLayer(jobs).pipe(
          Layer.provide([flakyEngine, runs.layer, requests.layer, SecureRandom.layerWebCrypto]),
        ),
      );
      while ((yield* Clock.currentTimeMillis) < Date.parse('2026-11-26T09:02:30Z')) {
        yield* Effect.yieldNow;
        yield* TestClock.adjust('1 second');
      }

      return runs.rows().map((row) => `${row.runKey}/${row.status}`);
    }).pipe(Effect.scoped, Effect.provide(testClockLayer), Effect.runPromise);
    expect(failed).toBe(true);
    expect(keys.toSorted()).toEqual([
      '2026-11-26T09:00Z/completed',
      '2026-11-26T09:01Z/completed',
      '2026-11-26T09:02Z/completed',
    ]);
  });
});
