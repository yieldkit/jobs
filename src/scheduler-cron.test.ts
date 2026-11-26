import type { Context, Duration } from 'effect';

import { Clock, Effect, Exit, Layer, Schema, Scope } from 'effect';
import {
  ClusterWorkflowEngine,
  MessageStorage,
  RunnerHealth,
  Runners,
  RunnerStorage,
  Sharding,
  ShardingConfig,
} from 'effect/cluster';
import { SecureRandom } from 'effect/crypto';
import { TestClock } from 'effect/testing';
import { WorkflowEngine } from 'effect/workflow';
import { describe, expect, it } from 'vite-plus/test';

import type { JobCadence } from './job.ts';

import {
  assignedShardGroupsFor,
  availableShardGroupsFor,
  makeTestClusterLayer,
} from './cluster.ts';
import { cronForCadence } from './cron.ts';
import { defineJob } from './job.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { makeJobRequestsMemory } from './requests-memory.ts';
import { makeSchedulerLayer } from './scheduler.ts';

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
 * Advances `TestClock` by `step` until it reads `iso`, yielding between steps.
 */
const advanceTo = (iso: string, step: Duration.Input) =>
  Effect.gen(function* () {
    while ((yield* Clock.currentTimeMillis) < Date.parse(iso)) {
      yield* Effect.yieldNow;
      yield* TestClock.adjust(step);
    }
  });

/**
 * Starts a worker at `startIso` for one `Cron` job on `cadence` whose workflow dispatches fail
 * while `failWhile(nowMs)` holds, advances to `untilIso` and returns its ledger rows as
 * `runKey/status`.
 */
const cronRunsWithFlakyDispatch = (options: {
  readonly cadence: Extract<JobCadence, { readonly _tag: 'Cron' }>;
  readonly startIso: string;
  readonly untilIso: string;
  readonly failWhile: (nowMs: number) => boolean;
}) => {
  const jobs = [makeJob('flaky-cron', options.cadence)];
  return Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(options.startIso));
    const runs = yield* makeJobRunsMemory();
    const requests = yield* makeJobRequestsMemory();
    const cluster = makeTestClusterLayer(jobs);
    const flakyEngine = Layer.effect(
      WorkflowEngine.WorkflowEngine,
      Effect.map(WorkflowEngine.WorkflowEngine, (engine) =>
        WorkflowEngine.WorkflowEngine.of({
          ...engine,
          execute: (workflow, executeOptions) =>
            Effect.flatMap(Clock.currentTimeMillis, (nowMs) =>
              options.failWhile(nowMs)
                ? Effect.die(new Error('storage blip'))
                : engine.execute(workflow, executeOptions),
            ),
        }),
      ),
    ).pipe(Layer.provideMerge(cluster));
    yield* Layer.build(
      makeSchedulerLayer(jobs).pipe(
        Layer.provide([flakyEngine, runs.layer, requests.layer, SecureRandom.layerWebCrypto]),
      ),
    );
    yield* advanceTo(options.untilIso, '30 seconds');
    return runs.rows().map((row) => `${row.runKey}/${row.status}`);
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise);
};

/**
 * In-memory Cluster like `makeTestClusterLayer`, on an already built memory `MessageStorage`, so a
 * worker built after another one was torn down sees the messages it left behind.
 */
const clusterOnStorage = (
  jobs: ReadonlyArray<ReturnType<typeof makeJob>>,
  storage: Context.Context<MessageStorage.MessageStorage>,
) => {
  const shardingConfig = ShardingConfig.layer({
    availableShardGroups: [...availableShardGroupsFor(jobs)],
    assignedShardGroups: [...assignedShardGroupsFor(jobs, undefined)],
  });
  const runner = Sharding.layer.pipe(
    Layer.provideMerge(Runners.layerNoop),
    Layer.provideMerge(Layer.succeedContext(storage)),
    Layer.provide([RunnerStorage.layerMemory, RunnerHealth.layerNoop]),
    Layer.provideMerge(shardingConfig),
  );
  return ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(runner));
};

/**
 * Starts a worker at 08:50Z for a 09:00 `Cron` job with `catchUpWithin`, stops it at 08:55Z, starts
 * another on the same message storage at 09:30Z, so the pending 09:00 tick is delivered 30 minutes
 * late, and returns the ledger rows at 09:35Z.
 */
const rowsAfterLateTick = (catchUpWithin: Duration.Input) => {
  const jobs = [makeJob('late-tick', { _tag: 'Cron', expression: '0 9 * * *', catchUpWithin })];
  return Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse('2026-11-26T08:50:00Z'));
    const runs = yield* makeJobRunsMemory();
    const requests = yield* makeJobRequestsMemory();
    const storage = yield* Layer.build(
      MessageStorage.layerMemory.pipe(Layer.provide(ShardingConfig.layer({}))),
    );
    /**
     * Builds a worker for `jobs` on `storage` in `scope`.
     */
    const startWorker = (scope: Scope.Scope) => {
      const cluster = clusterOnStorage(jobs, storage);
      return Layer.buildWithScope(
        makeSchedulerLayer(jobs).pipe(
          Layer.provide([cluster, runs.layer, requests.layer, SecureRandom.layerWebCrypto]),
        ),
        scope,
      );
    };

    const first = yield* Scope.make();
    yield* startWorker(first);
    yield* advanceTo('2026-11-26T08:55:00Z', '1 minute');
    yield* Scope.close(first, Exit.void);
    yield* TestClock.setTime(Date.parse('2026-11-26T09:30:00Z'));
    yield* startWorker(yield* Effect.scope);
    yield* advanceTo('2026-11-26T09:35:00Z', '1 minute');
    return runs.rows().map((row) => `${row.runKey}/${row.status}`);
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise);
};

describe('Cron scheduling', () => {
  it('retries a failed dispatch of a tick under the same run key', async () => {
    const rows = await cronRunsWithFlakyDispatch({
      cadence: { _tag: 'Cron', expression: '0 9 * * *', catchUpWithin: '1 hour' },
      startIso: '2026-11-26T08:55:00Z',
      untilIso: '2026-11-26T10:00:00Z',
      failWhile: (nowMs) => nowMs < Date.parse('2026-11-26T09:02:00Z'),
    });
    expect(rows).toEqual(['2026-11-26T09:00Z/completed']);
  });

  it('stops retrying a tick at catchUpWithin', async () => {
    const rows = await cronRunsWithFlakyDispatch({
      cadence: { _tag: 'Cron', expression: '0 9 * * *', catchUpWithin: '10 minutes' },
      startIso: '2026-11-26T08:55:00Z',
      untilIso: '2026-11-26T10:00:00Z',
      failWhile: (nowMs) => nowMs < Date.parse('2026-11-26T09:15:00Z'),
    });
    expect(rows).toEqual([]);
  });

  it('stops retrying a tick before the next one, so later ticks still run', async () => {
    const rows = await cronRunsWithFlakyDispatch({
      cadence: { _tag: 'Cron', expression: '*/5 * * * *', catchUpWithin: '1 hour' },
      startIso: '2026-11-26T08:59:00Z',
      untilIso: '2026-11-26T09:16:00Z',
      failWhile: (nowMs) => nowMs < Date.parse('2026-11-26T09:12:00Z'),
    });
    expect(rows.toSorted()).toEqual(['2026-11-26T09:10Z/completed', '2026-11-26T09:15Z/completed']);
  });

  it('runs a tick delivered late only within catchUpWithin', async () => {
    const [withinHour, withinTenMinutes] = await Promise.all([
      rowsAfterLateTick('1 hour'),
      rowsAfterLateTick('10 minutes'),
    ]);
    expect(withinHour).toEqual(['2026-11-26T09:00Z/completed']);
    expect(withinTenMinutes).toEqual([]);
  });

  it('fails with ClusterConfigError for an interval that is not a duration', async () => {
    const cadence: Extract<JobCadence, { readonly _tag: 'Every' }> = {
      _tag: 'Every',
      interval: '1e1 minutes',
    };
    const jobs = [makeJob('unparseable', cadence)];
    const direct = await Effect.runPromise(Effect.flip(cronForCadence(cadence)));
    const startup = await Effect.gen(function* () {
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
    expect(direct).toMatchObject({
      _tag: 'ClusterConfigError',
      message: 'Every interval must be a duration.',
    });
    expect(startup).toMatchObject({ _tag: 'ClusterConfigError' });
  });
});
