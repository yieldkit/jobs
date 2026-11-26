import type { Duration } from 'effect';

import { Clock, Context, DateTime, Effect, Fiber, Layer, Option, Schema } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { TestClock } from 'effect/testing';
import { DurableClock, WorkflowEngine } from 'effect/workflow';
import { describe, expect, it } from 'vite-plus/test';

import type { AnyJobDefinition, JobCadence } from './job.ts';
import type { JobRunsMemoryHarness } from './ledger-memory.ts';
import type { JobsPolicyOverrides } from './policy.ts';
import type { JobWorkflowPayload } from './workflow.ts';

import { durableStage } from './activity.ts';
import { makeTestClusterLayer } from './cluster.ts';
import { defineJob } from './job.ts';
import { UpstreamError } from './jobs.fixture.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';
import { makeJobRequestsMemory } from './requests-memory.ts';
import { runJob } from './run.ts';
import { makeSchedulerLayer } from './scheduler.ts';
import { testRuntimeLayer } from './test-runtime.fixture.ts';
import {
  dispatchApiJobWorkflow,
  dispatchJob,
  makeAnyJobWorkflowLayer,
  makeJobWorkflow,
} from './workflow.ts';

const Stats = Schema.Struct({ call: Schema.Int });

/**
 * Suspends the workflow running the job body on a long durable clock, so an explicit interrupt can
 * reach it; outside a workflow it never returns.
 */
const suspendDurably = Effect.gen(function* () {
  const engine = yield* Effect.serviceOption(WorkflowEngine.WorkflowEngine);
  const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance);
  if (Option.isNone(engine) || Option.isNone(instance)) {
    return yield* Effect.never;
  }

  yield* DurableClock.sleep({ name: 'hold', duration: '1 day' }).pipe(
    Effect.provideService(WorkflowEngine.WorkflowEngine, engine.value),
    Effect.provideService(WorkflowEngine.WorkflowInstance, instance.value),
  );
  return yield* Effect.never;
});

/**
 * What call `call` of a recording job's body does: complete, fail, interrupt itself, or suspend its
 * workflow until interrupted.
 */
type Behavior = (call: number) => 'ok' | 'fail' | 'block' | 'interrupt';

/**
 * A job on `cadence` that records the clock time of every body run; the first `failures` runs fail
 * (all of them when `failures` is `'always'`), or `failures` decides each call.
 */
const makeRecordingJob = (
  name: string,
  cadence: JobCadence,
  failures: number | 'always' | Behavior,
) => {
  const behave: Behavior =
    typeof failures === 'function'
      ? failures
      : (call) => (failures === 'always' || call <= failures ? 'fail' : 'ok');
  const startedAt: Array<string> = [];
  const job = defineJob({
    name,
    description: 'Records its runs',
    cadence,
    params: { schema: Schema.Struct({ to: Schema.String }), defaults: { to: 'nobody' } },
    stats: Stats,
    run: () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        startedAt.push(DateTime.formatIso(now).slice(11, 16));
        const call = startedAt.length;
        const what = behave(call);
        if (what === 'fail') {
          return yield* new UpstreamError({ status: 503, message: `call ${String(call)}` });
        }

        if (what === 'block') {
          return yield* suspendDurably;
        }

        if (what === 'interrupt') {
          return yield* Effect.interrupt;
        }

        return { call };
      }),
  });
  return { job, startedAt };
};

/**
 * Runs `scenario` from `startIso` under `TestClock`, on a memory ledger and the test Cluster with
 * `job`'s workflow registered.
 */
const runScenario = <A, E>(
  job: AnyJobDefinition<never>,
  startIso: string,
  scenario: (
    memory: JobRunsMemoryHarness,
  ) => Effect.Effect<A, E, JobRuns | WorkflowEngine.WorkflowEngine>,
  policy?: JobsPolicyOverrides,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(startIso));
        const memory = yield* makeJobRunsMemory({ policy });
        const cluster = makeTestClusterLayer([job]);
        const workflow = makeAnyJobWorkflowLayer(job).pipe(
          Layer.provide(memory.layer),
          Layer.provide(cluster),
        );
        const env = Layer.mergeAll(cluster, memory.layer, workflow);
        return yield* Effect.provide(scenario(memory), env);
      }),
    ).pipe(Effect.provide(testRuntimeLayer)),
  );

/**
 * Advances `TestClock` by `step`, `times` times, letting the workflow run after each step.
 */
const advance = (step: Duration.Input, times: number) =>
  Effect.gen(function* () {
    for (let index = 0; index < times; index += 1) {
      yield* TestClock.adjust(step);
    }
  });

/**
 * Starts a worker running `jobs` at `startIso`, then moves the clock by `step` to each checkpoint
 * in turn and runs its action there.
 */
const runWorker = (options: {
  readonly jobs: ReadonlyArray<AnyJobDefinition<never>>;
  readonly startIso: string;
  readonly step: Duration.Input;
  readonly checkpoints: ReadonlyArray<
    readonly [string, Effect.Effect<unknown, never, WorkflowEngine.WorkflowEngine>]
  >;
  readonly policy?: JobsPolicyOverrides;
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
    for (const [iso, action] of options.checkpoints) {
      while ((yield* Clock.currentTimeMillis) < Date.parse(iso)) {
        yield* TestClock.adjust(options.step);
      }

      yield* Effect.provide(action, context);
      yield* Effect.yieldNow;
    }

    return runs.rows();
  }).pipe(Effect.scoped, Effect.provide(testRuntimeLayer), Effect.runPromise);

/**
 * A scheduled payload for `runKey`.
 */
const scheduled = (runKey: string): JobWorkflowPayload => ({
  runKey,
  trigger: 'schedule',
  params: {},
  force: false,
  requestId: null,
  attemptNonce: null,
});

/**
 * The `_tag` of `effect`'s failure, or `'ok'` when it succeeds.
 */
const tagOf = <A, E extends { readonly _tag: string }, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.match({ onFailure: (error) => error._tag, onSuccess: () => 'ok' }));

describe('Daily deadline', () => {
  it('retries every checkEvery until until, past dailyMaxFailures, then exhausts the key', async () => {
    const { job, startedAt } = makeRecordingJob(
      'deadline-daily',
      { _tag: 'Daily', checkEvery: '15 minutes', at: '09:00', until: '10:00' },
      'always',
    );
    const result = await runScenario(
      job,
      '2026-11-26T09:00:00Z',
      (memory) =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute(scheduled('2026-11-26'))
            .pipe(Effect.forkChild);
          yield* advance('15 minutes', 6);
          const outcome = yield* Fiber.join(fiber);
          return { outcome, row: memory.rows()[0] };
        }),
      { retry: { dailyMaxFailures: 2 } },
    );
    expect(startedAt).toEqual(['09:00', '09:15', '09:30', '09:45', '10:00']);
    expect(result.outcome.status).toBe('failed');
    expect(result.row?.failures).toBe(5);
    expect(result.row?.nextAttemptAt).toBeNull();
  });

  it('completes on a retry before until', async () => {
    const { job, startedAt } = makeRecordingJob(
      'deadline-daily-recovers',
      { _tag: 'Daily', checkEvery: '20 minutes', at: '09:00', until: '12:00' },
      2,
    );
    const outcome = await runScenario(job, '2026-11-26T09:00:00Z', () =>
      Effect.gen(function* () {
        const fiber = yield* makeJobWorkflow(job)
          .execute(scheduled('2026-11-26'))
          .pipe(Effect.forkChild);
        yield* advance('20 minutes', 3);
        return yield* Fiber.join(fiber);
      }),
    );
    expect(startedAt).toEqual(['09:00', '09:20', '09:40']);
    expect(outcome.status).toBe('completed');
  });
});

/**
 * Claims the day's key with a manual run and, unless `hold`, settles it as failed.
 */
const manualClaim = (job: AnyJobDefinition<never>, hold: boolean) =>
  Effect.gen(function* () {
    const runs = yield* JobRuns;
    const runKey = '2026-11-26';
    const owned = yield* runs.claim({
      job: job.name,
      runKey,
      trigger: 'manual',
      force: false,
      params: {},
    });
    if (!hold && owned._tag === 'Acquired') {
      yield* runs.settle(
        { job: job.name, runKey, token: owned.token, attempt: owned.attempt },
        {
          _tag: 'Failed',
          errorMessage: 'down',
          reason: 'error',
          cadence: 'Daily',
          maxFailures: Number.POSITIVE_INFINITY,
          trigger: 'manual',
          annotations: Context.empty(),
        },
      );
    }
  }).pipe(Effect.orDie);

describe('Daily deadline regressions', () => {
  const deadline: JobCadence = {
    _tag: 'Daily',
    at: '09:00',
    until: '12:00',
    checkEvery: '15 minutes',
  };

  it('does not start a scheduled run after until, even after waiting on a busy key', async () => {
    const { job, startedAt } = makeRecordingJob('deadline-busy', deadline, 0);
    const outcome = await runScenario(job, '2026-11-26T11:59:00Z', () =>
      Effect.gen(function* () {
        // The manual run holds the key for its 2-minute lease and never settles.
        yield* manualClaim(job, true);
        const fiber = yield* makeJobWorkflow(job)
          .execute(scheduled('2026-11-26'))
          .pipe(Effect.forkChild);
        yield* advance('1 minute', 6);
        return yield* Fiber.join(fiber);
      }),
    );
    expect(startedAt).toEqual([]);
    expect(outcome).toMatchObject({ status: 'skipped', reason: { _tag: 'Exhausted' } });
  });

  it('marks a failed key exhausted when its scheduled run would start after until', async () => {
    const { job, startedAt } = makeRecordingJob('deadline-late', deadline, 0);
    const result = await runScenario(job, '2026-11-26T11:00:00Z', (memory) =>
      Effect.gen(function* () {
        yield* manualClaim(job, false);
        const retryable = memory.rows()[0]?.nextAttemptAt;
        yield* TestClock.adjust('65 minutes');
        const outcome = yield* makeJobWorkflow(job).execute(scheduled('2026-11-26'));
        return { retryable, outcome, row: memory.rows()[0] };
      }),
    );
    expect(result.retryable).toBeInstanceOf(Date);
    expect(startedAt).toEqual([]);
    expect(result.outcome).toMatchObject({ status: 'skipped', reason: { _tag: 'Exhausted' } });
    expect(result.row).toMatchObject({ status: 'failed', failures: 1, nextAttemptAt: null });
  });

  it('keeps a key with time left retryable when its workflow is interrupted', async () => {
    const { job, startedAt } = makeRecordingJob('deadline-interrupted', deadline, (call) =>
      call === 1 ? 'fail' : 'block',
    );
    const row = await runScenario(
      job,
      '2026-11-26T09:00:00Z',
      (memory) =>
        Effect.gen(function* () {
          const workflow = makeJobWorkflow(job);
          const payload = scheduled('2026-11-26');
          yield* workflow.execute(payload, { discard: true });
          yield* advance('1 minute', 16);
          const executionId = yield* workflow.executionId(payload);
          yield* workflow.interrupt(executionId);
          yield* advance('1 minute', 2);
          return memory.rows()[0];
        }),
      { retry: { dailyMaxFailures: 1 } },
    );
    // The interrupt replays the run up to the body's durable clock, so the body starts again.
    expect(startedAt).toEqual(['09:00', '09:15', '09:16']);
    expect(row).toMatchObject({
      status: 'failed',
      failures: 1,
      errorMessage: expect.stringMatching(/interrupted/),
    });
    expect(row?.nextAttemptAt).toBeInstanceOf(Date);
  });

  it('keeps a key exhausted at until when a manual run fails after it', async () => {
    const { job } = makeRecordingJob(
      'deadline-manual',
      { _tag: 'Daily', at: '09:00', until: '12:00', checkEvery: '1 minute' },
      'always',
    );
    const result = await runScenario(job, '2026-11-26T12:00:00Z', (memory) =>
      Effect.gen(function* () {
        const fiber = yield* makeJobWorkflow(job)
          .execute(scheduled('2026-11-26'))
          .pipe(Effect.forkChild);
        yield* TestClock.adjust('2 minutes');
        yield* Fiber.join(fiber);
        const afterScheduled = memory.rows()[0]?.nextAttemptAt;
        yield* runJob(job, { trigger: 'manual' }).pipe(Effect.provide(SecureRandom.layerWebCrypto));
        return { afterScheduled, afterManual: memory.rows()[0] };
      }),
    );
    expect(result.afterScheduled).toBeNull();
    expect(result.afterManual).toMatchObject({ failures: 2, nextAttemptAt: null });
  });

  it('leaves a scheduled retry pending when a manual run fails before until', async () => {
    const { job, startedAt } = makeRecordingJob('deadline-manual-early', deadline, (call) =>
      call <= 2 ? 'fail' : 'ok',
    );
    const outcome = await runScenario(job, '2026-11-26T11:40:00Z', () =>
      Effect.gen(function* () {
        const fiber = yield* makeJobWorkflow(job)
          .execute(scheduled('2026-11-26'))
          .pipe(Effect.forkChild);
        yield* advance('1 minute', 10);
        yield* runJob(job, { trigger: 'manual' }).pipe(Effect.provide(SecureRandom.layerWebCrypto));
        yield* advance('1 minute', 10);
        return yield* Fiber.join(fiber);
      }),
    );
    expect(startedAt).toEqual(['11:40', '11:50', '11:55']);
    expect(outcome).toMatchObject({ status: 'completed', attempt: 3 });
  });

  it('reads until in the cadence time zone', async () => {
    const { job, startedAt } = makeRecordingJob(
      'deadline-brussels',
      {
        _tag: 'Daily',
        at: '09:00',
        until: '12:00',
        timeZone: 'Europe/Brussels',
        checkEvery: '1 hour',
      },
      'always',
    );
    const result = await runScenario(job, '2026-11-26T08:00:00Z', (memory) =>
      Effect.gen(function* () {
        const fiber = yield* makeJobWorkflow(job)
          .execute(scheduled('2026-11-26'))
          .pipe(Effect.forkChild);
        yield* advance('1 hour', 6);
        const outcome = yield* Fiber.join(fiber);
        return { outcome, row: memory.rows()[0] };
      }),
    );
    // 09:00 to 12:00 in Brussels (UTC+1); a 13:00 local retry is past until.
    expect(startedAt).toEqual(['08:00', '09:00', '10:00', '11:00']);
    expect(result.outcome.status).toBe('failed');
    expect(result.row).toMatchObject({ failures: 4, nextAttemptAt: null });
  });

  it('retries a stage failure until until, past dailyMaxFailures', async () => {
    let calls = 0;
    const job = defineJob({
      name: 'deadline-stage',
      description: 'Its only stage always fails',
      cadence: { _tag: 'Daily', at: '09:00', until: '10:00', checkEvery: '15 minutes' },
      params: { schema: Schema.Struct({}), defaults: {} },
      stats: Stats,
      run: () =>
        durableStage({
          name: 'send',
          schema: Stats,
          execute: Effect.suspend(() => {
            calls += 1;
            return Effect.fail(new UpstreamError({ status: 503, message: 'down' }));
          }),
        }),
    });
    const row = await runScenario(
      job,
      '2026-11-26T09:00:00Z',
      (memory) =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute(scheduled('2026-11-26'))
            .pipe(Effect.forkChild);
          yield* advance('15 minutes', 6);
          yield* Fiber.join(fiber);
          return memory.rows()[0];
        }),
      { retry: { dailyMaxFailures: 2 } },
    );
    expect(calls).toBe(5);
    expect(row).toMatchObject({ failures: 5, nextAttemptAt: null });
  });
});

describe('OnDemand jobs', () => {
  const onDemand: JobCadence = { _tag: 'OnDemand' };

  it('runs a dispatched key once, retrying with backoff, however often it is dispatched', async () => {
    const { job, startedAt } = makeRecordingJob('send-email', onDemand, 2);
    const result = await runScenario(job, '2026-11-26T09:00:00Z', (memory) =>
      Effect.gen(function* () {
        const first = yield* dispatchJob(job, {
          runKey: 'welcome:user-42',
          params: { to: 'a@example.com' },
        });
        const again = yield* dispatchJob(job, {
          runKey: 'welcome:user-42',
          params: { to: 'b@example.com' },
        });
        yield* advance('1 minute', 4);
        const afterDone = yield* dispatchJob(job, { runKey: 'welcome:user-42' });
        yield* advance('1 minute', 2);
        return { first, again, afterDone, rows: memory.rows() };
      }),
    );
    expect(startedAt).toEqual(['09:00', '09:01', '09:03']);
    expect(result.again.executionId).toBe(result.first.executionId);
    expect(result.afterDone.executionId).toBe(result.first.executionId);
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      runKey: 'welcome:user-42',
      status: 'completed',
      attempt: 3,
      trigger: 'schedule',
      params: { to: 'a@example.com' },
    });
  });

  it('gives up after onDemandMaxFailures, and a manual runJob with the key runs it again', async () => {
    const { job, startedAt } = makeRecordingJob('flaky-webhook', onDemand, 3);
    const result = await runScenario(
      job,
      '2026-11-26T09:00:00Z',
      (memory) =>
        Effect.gen(function* () {
          yield* dispatchJob(job, { runKey: 'hook-1' });
          yield* advance('1 minute', 5);
          const exhausted = memory.rows()[0];
          const manual = yield* runJob(job, { trigger: 'manual', runKey: 'hook-1' }).pipe(
            Effect.provide([memory.layer, SecureRandom.layerWebCrypto]),
          );
          return { exhausted, manual };
        }),
      { retry: { onDemandMaxFailures: 3 } },
    );
    expect(startedAt).toEqual(['09:00', '09:01', '09:03', '09:05']);
    expect(result.exhausted?.failures).toBe(3);
    expect(result.exhausted?.nextAttemptAt).toBeNull();
    expect(result.manual).toMatchObject({ status: 'completed', attempt: 4, runKey: 'hook-1' });
  });

  it('waits until runAt before claiming the key', async () => {
    const { job, startedAt } = makeRecordingJob('reminder', onDemand, 0);
    const result = await runScenario(job, '2026-11-26T09:00:00Z', (memory) =>
      Effect.gen(function* () {
        yield* dispatchJob(job, { runKey: 'reminder:42', delay: '1 hour' });
        const runAt = DateTime.makeUnsafe(Date.parse('2026-11-26T09:30:00Z'));
        yield* dispatchJob(job, { runKey: 'reminder:43', runAt });
        yield* advance('10 minutes', 5);
        const before = memory.rows().map((row) => row.runKey);
        yield* advance('10 minutes', 2);
        return { before, after: memory.rows().map((row) => `${row.runKey}/${row.status}`) };
      }),
    );
    expect(result.before).toEqual(['reminder:43']);
    expect(startedAt).toEqual(['09:30', '10:00']);
    expect(result.after.toSorted()).toEqual(['reminder:42/completed', 'reminder:43/completed']);
  });

  it('runs a key reused after retention again, after its delay', async () => {
    const { job, startedAt } = makeRecordingJob('reused-key', onDemand, 0);
    const dispatch = dispatchJob(job, { runKey: 'k', delay: '10 minutes' }).pipe(Effect.orDie);
    await runWorker({
      jobs: [job],
      startIso: '2026-11-26T09:00:00Z',
      step: '5 minutes',
      // OnDemand workflows are pruned long before retention.everyCluster.
      policy: {
        retention: { onDemand: '30 minutes', everyCluster: '1 day' },
        maintenance: { interval: '5 minutes' },
      },
      checkpoints: [
        ['2026-11-26T09:01:00Z', dispatch],
        ['2026-11-26T10:00:00Z', dispatch],
        ['2026-11-26T10:30:00Z', Effect.void],
      ],
    });
    expect(startedAt).toEqual(['09:15', '10:10']);
  });

  it('retries a key reused after retention on time, after a failure', async () => {
    const { job, startedAt } = makeRecordingJob('reused-retry-key', onDemand, (call) =>
      call % 2 === 1 ? 'fail' : 'ok',
    );
    const dispatch = dispatchJob(job, { runKey: 'k' }).pipe(Effect.orDie);
    const rows = await runWorker({
      jobs: [job],
      startIso: '2026-11-26T09:00:00Z',
      step: '1 minute',
      // OnDemand workflows are pruned long before retention.everyCluster, and both runs of the key
      // sleep on a retry clock of the same name.
      policy: {
        retry: { onDemandBase: '2 minutes' },
        retention: { onDemand: '30 minutes', everyCluster: '1 day' },
        maintenance: { interval: '5 minutes' },
      },
      checkpoints: [
        ['2026-11-26T09:00:00Z', dispatch],
        ['2026-11-26T10:00:00Z', dispatch],
        ['2026-11-26T10:12:00Z', Effect.void],
      ],
    });
    expect(startedAt).toEqual(['09:00', '09:02', '10:00', '10:02']);
    expect(rows.map((row) => `${row.runKey}/${row.status}`)).toEqual(['k/completed']);
  });

  it('backs off from the key failure count, manual failures included', async () => {
    const { job, startedAt } = makeRecordingJob('backoff-key', onDemand, 'always');
    const thirdRetryAt = await runScenario(
      job,
      '2026-11-26T09:00:00Z',
      (memory) =>
        Effect.gen(function* () {
          const manual = runJob(job, { trigger: 'manual', runKey: 'k' }).pipe(
            Effect.provide(SecureRandom.layerWebCrypto),
          );
          yield* manual;
          yield* manual;
          yield* dispatchJob(job, { runKey: 'k' });
          yield* advance('1 minute', 20);
          const third = memory.settles()[2]?.event.outcome;
          return third?._tag === 'Failed' ? third.nextAttemptAt?.toISOString() : undefined;
        }),
      { retry: { onDemandMaxFailures: 4 } },
    );
    expect(startedAt).toEqual(['09:00', '09:00', '09:02', '09:06']);
    expect(thirdRetryAt).toBe('2026-11-26T09:06:00.000Z');
  });

  it('keeps an exhausted key exhausted when a manual run is interrupted', async () => {
    const { job } = makeRecordingJob('interrupted-key', onDemand, (call) =>
      call === 1 ? 'fail' : 'interrupt',
    );
    const result = await runScenario(
      job,
      '2026-11-26T09:00:00Z',
      (memory) =>
        Effect.gen(function* () {
          const manual = runJob(job, { trigger: 'manual', runKey: 'k' }).pipe(
            Effect.provide(SecureRandom.layerWebCrypto),
            Effect.exit,
          );
          yield* manual;
          const first = memory.rows()[0]?.nextAttemptAt;
          yield* manual;
          return { first, second: memory.rows()[0] };
        }),
      { retry: { onDemandMaxFailures: 1 } },
    );
    expect(result.first).toBeNull();
    expect(result.second).toMatchObject({ attempt: 2, failures: 1, nextAttemptAt: null });
  });

  it('does not run a payload whose runAt is not an instant', async () => {
    const { job, startedAt } = makeRecordingJob('bad-run-at', onDemand, 0);
    const exit = await runScenario(job, '2026-11-26T09:00:00Z', () =>
      makeJobWorkflow(job)
        .execute({ ...scheduled('k'), runAt: 'tomorrow' })
        .pipe(Effect.exit),
    );
    expect(startedAt).toEqual([]);
    expect(exit._tag).toBe('Failure');
  });

  it('rejects other cadences, bad keys and bad params before dispatching', async () => {
    const { job } = makeRecordingJob('mailer', onDemand, 0);
    const { job: daily } = makeRecordingJob('daily-mailer', { _tag: 'Daily' }, 0);
    const errors = await runScenario(job, '2026-11-26T09:00:00Z', (memory) =>
      Effect.gen(function* () {
        const requests = yield* makeJobRequestsMemory();
        return {
          daily: yield* tagOf(dispatchJob(daily, { runKey: 'k' })),
          space: yield* tagOf(dispatchJob(job, { runKey: 'has space' })),
          params: yield* tagOf(dispatchJob(job, { runKey: 'k', params: { to: 42 } })),
          unknown: yield* tagOf(dispatchJob(job, { runKey: 'k', params: { cc: 'x' } })),
          api: yield* tagOf(
            dispatchApiJobWorkflow(job, { params: {}, force: false }).pipe(
              Effect.provide([requests.layer, SecureRandom.layerWebCrypto]),
            ),
          ),
          runJobWithoutKey: yield* tagOf(
            runJob(job, { trigger: 'manual' }).pipe(
              Effect.provide([memory.layer, SecureRandom.layerWebCrypto]),
            ),
          ),
          rows: memory.rows().length,
        };
      }),
    );
    expect(errors).toEqual({
      daily: 'InvalidRunKey',
      space: 'InvalidRunKey',
      params: 'InvalidJobParams',
      unknown: 'InvalidJobParams',
      api: 'InvalidRunKey',
      runJobWithoutKey: 'InvalidRunKey',
      rows: 0,
    });
  });
});
