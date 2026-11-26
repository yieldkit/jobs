import type { Duration } from 'effect';

import { makeTestLogger } from '@yieldkit/logger/testing';
import { Context, Deferred, Effect, Fiber, Layer, Option, Schema } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { SqlError } from 'effect/sql';
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
import { JobLabel, UpstreamError } from './jobs.fixture.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';
import { defaultJobsPolicy } from './policy.ts';
import { makeJobRequestsMemory } from './requests-memory.ts';
import { JobRequests } from './requests.ts';
import { runJob } from './run.ts';
import { testRuntimeLayer } from './test-runtime.fixture.ts';
import {
  WorkerJobParams,
  dispatchApiJobWorkflow,
  makeAnyJobWorkflowLayer,
  makeJobWorkflow,
  makeJobWorkflowLayer,
} from './workflow.ts';

const MAX_SCHEDULED_DAILY_FAILURES = defaultJobsPolicy.retry.dailyMaxFailures;
const MAX_CRASH_ATTEMPTS = defaultJobsPolicy.retry.maxCrashAttempts;
const TEST_ANNOTATIONS = Context.make(JobLabel, 'workflow-test');

const StageCountSchema = Schema.Struct({ count: Schema.Int });

const MultiStepStats = Schema.Struct({
  orders: Schema.Int,
  refunds: Schema.Int,
  invoices: Schema.Int,
});

const EmptyParams = Schema.Struct({});

/**
 * What `@effect/sql-pg` builds when a demoted primary refuses a write (SQLSTATE 25006).
 */
const readOnlyTransaction = (): SqlError.SqlError =>
  new SqlError.SqlError({
    reason: new SqlError.UnknownError({
      cause: Object.assign(new Error('cannot execute UPDATE in a read-only transaction'), {
        code: '25006',
      }),
      message: 'PgConnection: Query failed',
    }),
  });

const connectionReset = (): SqlError.SqlError =>
  new SqlError.SqlError({
    reason: new SqlError.ConnectionError({
      cause: new Error('reset'),
      message: 'connection reset',
    }),
  });

/**
 * Opens `dropped`, then fails like a query on a reset connection. A test awaits `dropped` before it
 * advances `TestClock`: the workflow reaches the call through WebCrypto digests (Effect hashes
 * execution ids and activity keys), real promises `TestClock.adjust` does not wait for, and a retry
 * backoff that starts after the adjustment is never woken.
 */
const dropConnection = (dropped: Deferred.Deferred<void>) =>
  Deferred.succeed(dropped, undefined).pipe(Effect.andThen(Effect.fail(connectionReset())));

/**
 * A job without stages (hourly `Daily` unless `cadence` says otherwise) that counts its runs. The
 * first `failures` runs fail after `takes`.
 */
const makeCountingJob = (
  name: string,
  options: {
    readonly failures?: number;
    readonly takes?: Duration.Input;
    readonly cadence?: JobCadence;
  } = {},
) => {
  const calls = { body: 0 };
  const job = defineJob({
    name,
    description: 'Counts its runs',
    cadence: options.cadence ?? { _tag: 'Daily', checkEvery: '1 hour' },
    params: { schema: EmptyParams, defaults: {} },
    stats: StageCountSchema,
    run: () =>
      Effect.suspend(() => {
        calls.body += 1;
        const call = calls.body;
        return Effect.sleep(options.takes ?? '0 millis').pipe(
          Effect.andThen(
            call <= (options.failures ?? 0)
              ? Effect.fail(new UpstreamError({ status: 503, message: `call ${String(call)}` }))
              : Effect.succeed({ count: call }),
          ),
        );
      }),
  });
  return { job, calls };
};

/**
 * Fails with `UpstreamError` on a stage's first call and succeeds with `value` after.
 */
const failFirstCall = (call: number, value: number) =>
  call === 1
    ? Effect.fail(new UpstreamError({ status: 503, message: `call ${String(call)} failed` }))
    : Effect.succeed({ count: value });

/**
 * A durable stage returning a count.
 */
const countStage = (name: string, execute: Effect.Effect<{ count: number }, UpstreamError>) =>
  durableStage({ name, schema: StageCountSchema, execute });

/**
 * Starts `job`'s scheduled workflow for `runKey`, advances `TestClock` by `step` `steps` times and
 * returns the outcome.
 */
const executeScheduled = (
  job: AnyJobDefinition<never>,
  runKey: string,
  drive: { readonly steps: number; readonly step: Duration.Input },
) =>
  Effect.gen(function* () {
    const fiber = yield* makeJobWorkflow(job)
      .execute(scheduledPayload(runKey))
      .pipe(Effect.forkChild);
    for (let step = 0; step < drive.steps; step += 1) {
      yield* TestClock.adjust(drive.step);
    }

    return yield* Fiber.join(fiber);
  });

const scheduledPayload = (runKey: string): JobWorkflowPayload => ({
  runKey,
  trigger: 'schedule',
  params: {},
  force: false,
  requestId: null,
  attemptNonce: null,
});

/**
 * Runs `scenario` against a memory ledger and the in-memory test Cluster with `job`'s workflow
 * registered, under `TestClock`. `wrapRuns` replaces ledger methods the workflow sees; `requests`
 * gives the workflow a request store; `logger` captures the log lines of the run.
 */
const runWorkflowScenario = <A>(
  job: AnyJobDefinition<never>,
  scenario: (
    memory: JobRunsMemoryHarness,
  ) => Effect.Effect<A, never, JobRuns | WorkflowEngine.WorkflowEngine | TestClock.TestClock>,
  options: {
    readonly wrapRuns?: (real: JobRuns['Service']) => JobRuns['Service'];
    readonly requests?: Layer.Layer<JobRequests>;
    readonly policy?: JobsPolicyOverrides;
    readonly logger?: Layer.Layer<never>;
    readonly testClock?: Layer.Layer<TestClock.TestClock>;
  } = {},
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory({ policy: options.policy });
        const wrapRuns = options.wrapRuns ?? ((real) => real);
        const wrapped = Layer.effect(JobRuns, Effect.map(JobRuns, wrapRuns)).pipe(
          Layer.provideMerge(memory.layer),
        );
        const clusterLayer = makeTestClusterLayer([job]);
        const workflowLayer = makeAnyJobWorkflowLayer(job).pipe(
          Layer.provide(options.requests ?? Layer.empty),
          Layer.provide(wrapped),
          Layer.provide(clusterLayer),
        );
        const env = Layer.mergeAll(clusterLayer, memory.layer, workflowLayer);
        return yield* scenario(memory).pipe(Effect.provide(env));
      }),
    ).pipe(
      Effect.provide(
        Layer.merge(options.testClock ?? testRuntimeLayer, options.logger ?? Layer.empty),
      ),
    ),
  );

describe('durable job workflows', () => {
  it('re-executes only the failed stage on durable retry while keeping completed stages memoized', async () => {
    let feedCalls = 0;
    let refundsCalls = 0;
    let invoiceCalls = 0;
    const invoiceFailed = await Effect.runPromise(Deferred.make<void>());

    const multiStepJob = defineJob({
      name: 'test-multi-step',
      description: 'Three-stage durable workflow test',
      annotations: TEST_ANNOTATIONS,
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: Schema.Struct({}), defaults: {} },
      stats: MultiStepStats,
      run: () =>
        Effect.gen(function* () {
          const orders = yield* durableStage({
            name: 'sync-orders',
            schema: StageCountSchema,
            execute: Effect.sync(() => {
              feedCalls += 1;
              return { count: 10 };
            }),
          });
          const refunds = yield* durableStage({
            name: 'sync-refunds',
            schema: StageCountSchema,
            execute: Effect.sync(() => {
              refundsCalls += 1;
              return { count: 4 };
            }),
          });
          const invoices = yield* durableStage({
            name: 'send-invoices',
            schema: StageCountSchema,
            execute: Effect.suspend(() => {
              invoiceCalls += 1;
              if (invoiceCalls === 1) {
                return Deferred.succeed(invoiceFailed, undefined).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new UpstreamError({
                        status: 503,
                        message: 'transient 503 on invoice download',
                      }),
                    ),
                  ),
                );
              }

              return Effect.succeed({ count: 7 });
            }),
          });
          return {
            orders: orders.count,
            refunds: refunds.count,
            invoices: invoices.count,
          };
        }),
    });

    const outcome = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runsMemory = yield* makeJobRunsMemory();
          const clusterLayer = makeTestClusterLayer([multiStepJob]);
          const workflowLayer = makeJobWorkflowLayer(multiStepJob).pipe(
            Layer.provide(runsMemory.layer),
            Layer.provide(clusterLayer),
          );
          const env = Layer.mergeAll(clusterLayer, runsMemory.layer, workflowLayer);
          const workflow = makeJobWorkflow(multiStepJob);

          return yield* Effect.gen(function* () {
            const fiber = yield* workflow
              .execute({
                runKey: '2026-11-25',
                trigger: 'schedule',
                params: {},
                force: false,
                requestId: null,
                attemptNonce: null,
              })
              .pipe(Effect.forkChild);

            yield* Deferred.await(invoiceFailed);
            yield* TestClock.adjust('10 seconds');
            yield* TestClock.adjust('1 hour');
            yield* TestClock.adjust('15 seconds');
            const result = yield* Fiber.join(fiber);
            return { result, rows: runsMemory.rows(), settles: runsMemory.settles() };
          }).pipe(Effect.provide(env));
        }),
      ).pipe(Effect.provide(testRuntimeLayer)),
    );
    expect(outcome.result).toMatchObject({
      status: 'completed',
      stats: { orders: 10, refunds: 4, invoices: 7 },
    });

    // Completed stages ran strictly once; failed stage genuinely ran twice.
    expect(feedCalls).toBe(1);
    expect(refundsCalls).toBe(1);
    expect(invoiceCalls).toBe(2);

    // The failed first round and the completed second round each settled once, with annotations.
    expect(outcome.settles).toHaveLength(2);
    expect(outcome.settles[0]?.event).toMatchObject({
      trigger: 'schedule',
      annotations: TEST_ANNOTATIONS,
      outcome: { _tag: 'Failed', reason: 'error', failures: 1 },
    });
    expect(outcome.settles[1]?.result._tag).toBe('Completed');
    expect(outcome.rows[0]?.failures).toBe(1);
    expect(outcome.rows[0]?.status).toBe('completed');
  });

  it('retries scheduled Daily failures up to MAX_SCHEDULED_DAILY_FAILURES and settles Exhausted', async () => {
    let stageAttempts = 0;

    const alwaysFailingDaily = defineJob({
      name: 'test-daily-exhaust',
      description: 'Always fails to verify daily cap',
      annotations: TEST_ANNOTATIONS,
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: Schema.Struct({}), defaults: {} },
      stats: StageCountSchema,
      run: () =>
        durableStage({
          name: 'only-stage',
          schema: StageCountSchema,
          execute: Effect.suspend(() => {
            stageAttempts += 1;
            return Effect.fail(
              new UpstreamError({
                status: 502,
                message: `boom attempt ${String(stageAttempts)}`,
              }),
            );
          }),
        }),
    });

    const outcome = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runsMemory = yield* makeJobRunsMemory();
          const clusterLayer = makeTestClusterLayer([alwaysFailingDaily]);
          const workflowLayer = makeJobWorkflowLayer(alwaysFailingDaily).pipe(
            Layer.provide(runsMemory.layer),
            Layer.provide(clusterLayer),
          );
          const env = Layer.mergeAll(clusterLayer, runsMemory.layer, workflowLayer);
          const workflow = makeJobWorkflow(alwaysFailingDaily);

          return yield* Effect.gen(function* () {
            const fiber = yield* workflow
              .execute({
                runKey: '2026-11-25',
                trigger: 'schedule',
                params: {},
                force: false,
                requestId: null,
                attemptNonce: null,
              })
              .pipe(Effect.forkChild);

            for (let i = 0; i < MAX_SCHEDULED_DAILY_FAILURES + 1; i += 1) {
              yield* TestClock.adjust('10 seconds');
              yield* TestClock.adjust('1 hour');
            }

            const result = yield* Fiber.join(fiber);
            return { result, rows: runsMemory.rows(), settles: runsMemory.settles() };
          }).pipe(Effect.provide(env));
        }),
      ).pipe(Effect.provide(testRuntimeLayer)),
    );
    expect(outcome.result.status).toBe('failed');
    expect(stageAttempts).toBe(MAX_SCHEDULED_DAILY_FAILURES);
    expect(outcome.settles).toHaveLength(MAX_SCHEDULED_DAILY_FAILURES);
    expect(outcome.rows[0]?.failures).toBe(MAX_SCHEDULED_DAILY_FAILURES);
    expect(outcome.rows[0]?.nextAttemptAt).toBeNull();
  });

  it('waits durably on Busy during scheduled Daily runs instead of permanently completing skipped', async () => {
    let bodyCalls = 0;
    const busyJob = defineJob({
      name: 'test-daily-busy',
      description: 'Waits for a concurrent manual run to finish',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: Schema.Struct({}), defaults: {} },
      stats: StageCountSchema,
      run: () =>
        durableStage({
          name: 'work',
          schema: StageCountSchema,
          execute: Effect.sync(() => {
            bodyCalls += 1;
            return { count: 42 };
          }),
        }),
    });

    const outcome = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runsMemory = yield* makeJobRunsMemory();
          const clusterLayer = makeTestClusterLayer([busyJob]);
          const workflowLayer = makeJobWorkflowLayer(busyJob).pipe(
            Layer.provide(runsMemory.layer),
            Layer.provide(clusterLayer),
          );
          const env = Layer.mergeAll(clusterLayer, runsMemory.layer, workflowLayer);
          const workflow = makeJobWorkflow(busyJob);

          return yield* Effect.gen(function* () {
            // Simulate a concurrent manual run holding a live lease on today's key.
            const runs = yield* JobRuns;
            const manualClaim = yield* runs.claim({
              job: busyJob.name,
              runKey: '2026-11-25',
              trigger: 'manual',
              force: false,
              params: {},
            });
            if (manualClaim._tag !== 'Acquired') {
              return yield* Effect.die(new Error('expected the manual claim'));
            }

            const fiber = yield* workflow
              .execute({
                runKey: '2026-11-25',
                trigger: 'schedule',
                params: {},
                force: false,
                requestId: null,
                attemptNonce: null,
              })
              .pipe(Effect.forkChild);

            yield* TestClock.adjust('10 seconds');
            // The manual run fails so the scheduled workflow can reclaim after its durable Busy wait.
            yield* runs.settle(
              {
                job: busyJob.name,
                runKey: '2026-11-25',
                token: manualClaim.token,
                attempt: manualClaim.attempt,
              },
              {
                _tag: 'Failed',
                errorMessage: 'manual run interrupted',
                reason: 'interrupted',
                cadence: 'Daily',
                trigger: 'manual',
                annotations: Context.empty(),
              },
            );

            yield* TestClock.adjust('2 minutes');
            yield* TestClock.adjust('15 seconds');
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(env));
        }),
      ).pipe(Effect.provide(testRuntimeLayer)),
    );
    expect(outcome.status).toBe('completed');
    expect(bodyCalls).toBe(1);
  });

  it('re-filters forged API params inside the workflow and updates job_requests atomically', async () => {
    let observedParams: { readonly limit: number; readonly secretFlag: boolean } | undefined;
    const paramJob = defineJob({
      name: 'test-api-params',
      description: 'Validates apiParams enforcement in workflow handler',
      cadence: { _tag: 'Every', interval: '5 minutes' },
      params: {
        schema: Schema.Struct({
          limit: Schema.Int,
          secretFlag: Schema.Boolean,
        }),
        defaults: { limit: 10, secretFlag: false },
        apiParams: ['limit'],
      },
      stats: StageCountSchema,
      run: ({ params }) =>
        durableStage({
          name: 'work',
          schema: StageCountSchema,
          execute: Effect.sync(() => {
            observedParams = params;
            return { count: params.limit };
          }),
        }),
    });

    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runsMemory = yield* makeJobRunsMemory();
          const requestsMemory = yield* makeJobRequestsMemory({
            findRun: (job, runKey) =>
              runsMemory.rows().find((row) => row.job === job && row.runKey === runKey),
          });
          const clusterLayer = makeTestClusterLayer([paramJob]);
          const workflowLayer = makeJobWorkflowLayer(paramJob).pipe(
            Layer.provide(WorkerJobParams.layer({ 'test-api-params': { secretFlag: false } })),
            Layer.provide(runsMemory.layer),
            Layer.provide(requestsMemory.layer),
            Layer.provide(clusterLayer),
          );
          const env = Layer.mergeAll(
            clusterLayer,
            runsMemory.layer,
            requestsMemory.layer,
            workflowLayer,
          );
          const workflow = makeJobWorkflow(paramJob);

          return yield* Effect.gen(function* () {
            const dispatched = yield* dispatchApiJobWorkflow(paramJob, {
              params: { limit: 25, secretFlag: true },
              force: true,
            });
            // Also test forged payload directly smuggling secretFlag: true.
            const forgedOutcome = yield* workflow.execute({
              runKey: '2026-11-25T00:00Z',
              trigger: 'api',
              params: { limit: 15, secretFlag: true },
              force: true,
              requestId: dispatched.requestId,
              attemptNonce: 'forged-nonce',
            });
            const status = yield* requestsMemory.service.getStatus(
              paramJob.name,
              dispatched.requestId,
            );
            return { forgedOutcome, status };
          }).pipe(Effect.provide(env));
        }),
      ),
    );
    expect(result.forgedOutcome.status).toBe('completed');
    expect(observedParams).toEqual({ limit: 15, secretFlag: false });
    expect(result.status?.request.finishedAt).toBeInstanceOf(Date);
    // `run` is only ever the request's own execution's row, never the forged execution's.
    expect([null, result.status?.request.executionId]).toContain(
      result.status?.run?.executionId ?? null,
    );
  });

  it('caps repeated crash-resumes of the same executionId at MAX_CRASH_ATTEMPTS and treats completed same executionId as AlreadySettled', async () => {
    const simpleJob = defineJob({
      name: 'test-crash-cap',
      description: 'Crash cap test',
      cadence: { _tag: 'Every', interval: '5 minutes' },
      params: { schema: Schema.Struct({}), defaults: {} },
      stats: StageCountSchema,
      run: () => Effect.succeed({ count: 1 }),
    });

    const checks = await Effect.runPromise(
      Effect.gen(function* () {
        const runsMemory = yield* makeJobRunsMemory();
        return yield* Effect.gen(function* () {
          const runs = yield* JobRuns;
          const executionId = 'exec-same-id';
          const c1 = yield* runs.claim({
            job: simpleJob.name,
            runKey: '2026-11-25T00:00Z',
            trigger: 'schedule',
            force: false,
            params: {},
            executionId,
          });
          const c2 = yield* runs.claim({
            job: simpleJob.name,
            runKey: '2026-11-25T00:00Z',
            trigger: 'schedule',
            force: false,
            params: {},
            executionId,
          });
          const c3 = yield* runs.claim({
            job: simpleJob.name,
            runKey: '2026-11-25T00:00Z',
            trigger: 'schedule',
            force: false,
            params: {},
            executionId,
          });
          const c4 = yield* runs.claim({
            job: simpleJob.name,
            runKey: '2026-11-25T00:00Z',
            trigger: 'schedule',
            force: false,
            params: {},
            executionId,
          });
          if (c4._tag !== 'Acquired') {
            return yield* Effect.die(new Error('expected Acquired'));
          }

          yield* runs.settle(
            {
              job: simpleJob.name,
              runKey: '2026-11-25T00:00Z',
              token: c4.token,
              attempt: c4.attempt,
            },
            {
              _tag: 'Completed',
              stats: { count: 1 },
              trigger: 'schedule',
              annotations: Context.empty(),
            },
          );
          const afterForceReplay = yield* runs.claim({
            job: simpleJob.name,
            runKey: '2026-11-25T00:00Z',
            trigger: 'api',
            force: true,
            params: {},
            executionId,
          });
          return { c1, c2, c3, c4, afterForceReplay };
        }).pipe(Effect.provide(runsMemory.layer));
      }),
    );
    expect(checks.c1._tag === 'Acquired' && checks.c1.resumed).toBe(false);
    expect(checks.c2._tag === 'Acquired' && checks.c2.resumed).toBe(true);
    expect(checks.c4.attempt > MAX_CRASH_ATTEMPTS).toBe(true);
    expect(checks.afterForceReplay._tag).toBe('AlreadySettled');
  });

  it('keeps the scheduled retry budget after a failed API run of a Daily key', async () => {
    let bodyCalls = 0;
    const job = defineJob({
      name: 'test-api-then-schedule',
      description: 'Fails its first run',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: StageCountSchema,
      run: () =>
        Effect.suspend(() => {
          bodyCalls += 1;
          return bodyCalls === 1
            ? Effect.fail(new UpstreamError({ status: 503, message: 'upstream 503' }))
            : Effect.succeed({ count: bodyCalls });
        }),
    });
    const workflow = makeJobWorkflow(job);

    const outcome = await runWorkflowScenario(job, (memory) =>
      Effect.gen(function* () {
        const apiFiber = yield* workflow
          .execute({ ...scheduledPayload('2026-11-25'), trigger: 'api', attemptNonce: 'api-1' })
          .pipe(Effect.forkChild);
        yield* TestClock.adjust('10 seconds');
        const api = yield* Fiber.join(apiFiber);
        const rowAfterApi = memory.rows()[0];
        const fiber = yield* workflow
          .execute(scheduledPayload('2026-11-25'))
          .pipe(Effect.forkChild);
        yield* TestClock.adjust('10 seconds');
        const scheduled = yield* Fiber.join(fiber);
        return { api, rowAfterApi, scheduled };
      }),
    );
    expect(outcome.api.status).toBe('failed');
    expect(outcome.rowAfterApi).toMatchObject({ status: 'failed', failures: 1 });
    expect(outcome.rowAfterApi?.nextAttemptAt).toBeInstanceOf(Date);
    expect(outcome.scheduled).toMatchObject({ status: 'completed', attempt: 2 });
    expect(bodyCalls).toBe(2);
  });

  it('retries a scheduled Daily round that lost its lease instead of ending the key', async () => {
    let heartbeats = 0;
    // The heartbeat and the body's sleep start after the claim, so the clock moves only once the
    // body runs.
    const started = await Effect.runPromise(Deferred.make<void>());
    const job = defineJob({
      name: 'test-daily-lease-lost',
      description: 'Loses its lease on the first heartbeat',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: StageCountSchema,
      run: () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.sleep('10 minutes')),
          Effect.as({ count: 1 }),
        ),
    });
    const workflow = makeJobWorkflow(job);

    const outcome = await runWorkflowScenario(
      job,
      (memory) =>
        Effect.gen(function* () {
          const fiber = yield* workflow
            .execute(scheduledPayload('2026-11-25'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(started);
          for (let minute = 0; minute < 90; minute += 1) {
            yield* TestClock.adjust('1 minute');
          }

          const result = yield* Fiber.join(fiber);
          return { result, rows: memory.rows() };
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            heartbeat: (run) => {
              heartbeats += 1;
              return heartbeats === 1 ? Effect.succeed(false) : real.heartbeat(run);
            },
          }),
      },
    );
    expect(outcome.result).toMatchObject({ status: 'completed', attempt: 2 });
    expect(outcome.rows[0]).toMatchObject({ status: 'completed', failures: 0 });
  });

  it('retries a claim that hit a dropped connection instead of storing a defect', async () => {
    const { job, calls } = makeCountingJob('test-transient-claim');
    const workflow = makeJobWorkflow(job);
    const dropped = await Effect.runPromise(Deferred.make<void>());
    let claims = 0;

    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* workflow
            .execute(scheduledPayload('2026-11-25'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(dropped);
          yield* TestClock.adjust('10 seconds');
          const first = yield* Fiber.join(fiber);
          const again = yield* workflow
            .execute(scheduledPayload('2026-11-25'))
            .pipe(Effect.forkChild);
          yield* TestClock.adjust('10 seconds');
          const later = yield* Fiber.join(again);
          return { first, later };
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            claim: (request) =>
              Effect.suspend(() => {
                claims += 1;
                return claims === 1 ? dropConnection(dropped) : real.claim(request);
              }),
          }),
      },
    );
    expect(outcome.first).toMatchObject({ status: 'completed', attempt: 1 });
    expect(outcome.later).toMatchObject({ status: 'completed', attempt: 1 });
    expect(claims).toBe(2);
    expect(calls.body).toBe(1);
  });

  it('retries a completed settle that hit a dropped connection', async () => {
    const { job, calls } = makeCountingJob('test-transient-settle');
    const workflow = makeJobWorkflow(job);
    const dropped = await Effect.runPromise(Deferred.make<void>());
    let settles = 0;

    const outcome = await runWorkflowScenario(
      job,
      (memory) =>
        Effect.gen(function* () {
          const fiber = yield* workflow
            .execute(scheduledPayload('2026-11-25'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(dropped);
          yield* TestClock.adjust('10 seconds');
          const result = yield* Fiber.join(fiber);
          return { result, rows: memory.rows() };
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            settle: (run, result) =>
              Effect.suspend(() => {
                settles += 1;
                return settles === 1 ? dropConnection(dropped) : real.settle(run, result);
              }),
          }),
      },
    );
    expect(outcome.result).toMatchObject({ status: 'completed', attempt: 1 });
    expect(outcome.rows[0]?.status).toBe('completed');
    expect(calls.body).toBe(1);
  });

  it('retries recording a request outcome that hit a dropped connection', async () => {
    const { job } = makeCountingJob('test-transient-finish');
    const workflow = makeJobWorkflow(job);
    const requestsMemory = await Effect.runPromise(makeJobRequestsMemory());
    const dropped = await Effect.runPromise(Deferred.make<void>());
    let finishes = 0;
    const requests = Layer.succeed(
      JobRequests,
      JobRequests.of({
        ...requestsMemory.service,
        markFinished: (input) =>
          Effect.suspend(() => {
            finishes += 1;
            return finishes === 1
              ? dropConnection(dropped)
              : requestsMemory.service.markFinished(input);
          }),
      }),
    );
    const requestId = '3d7e9b10-5a42-4f6c-8e21-7c9d0a1b2e43';

    const result = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* workflow
            .execute({
              ...scheduledPayload('2026-11-25'),
              trigger: 'api',
              requestId,
              attemptNonce: requestId,
            })
            .pipe(Effect.forkChild);
          yield* Deferred.await(dropped);
          yield* TestClock.adjust('10 seconds');
          return yield* Fiber.join(fiber);
        }),
      { requests },
    );
    expect(result.status).toBe('completed');
    expect(requestsMemory.rows()[0]?.finishedAt).toBeInstanceOf(Date);
  });

  it('logs worker.request.start_failed and still runs when the request row cannot be marked started', async () => {
    const { job } = makeCountingJob('test-request-start-failed');
    const logger = makeTestLogger({ minLevel: 'Info' });
    const requestsMemory = await Effect.runPromise(makeJobRequestsMemory());
    const dropped = await Effect.runPromise(Deferred.make<void>());
    const requests = Layer.succeed(
      JobRequests,
      JobRequests.of({
        ...requestsMemory.service,
        upsertStarted: () => dropConnection(dropped),
      }),
    );
    const requestId = '9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d';

    const result = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute({
              ...scheduledPayload('2026-11-25'),
              trigger: 'api',
              requestId,
              attemptNonce: requestId,
            })
            .pipe(Effect.forkChild);
          yield* Deferred.await(dropped);
          for (let second = 0; second < 30; second += 1) {
            yield* TestClock.adjust('1 second');
          }

          return yield* Fiber.join(fiber);
        }),
      { requests, logger: logger.layer },
    );
    expect(result.status).toBe('completed');
    const startLog = logger.records.find(
      (record) => record.message === 'worker.request.start_failed',
    );
    expect(startLog?.level).toBe('WARN');
    expect(startLog?.annotations).toMatchObject({
      requestId,
      runKey: '2026-11-25',
      errorMessage: expect.stringContaining('connection reset'),
    });
  });

  it('retries the interrupt settle that hit a dropped connection', async () => {
    const paused = await Effect.runPromise(Deferred.make<void>());
    const job = defineJob({
      name: 'test-interrupt-settle',
      description: 'Suspends durably',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: StageCountSchema,
      // Sleeps durably inside the body, so the workflow suspends and reads the interrupt.
      run: () =>
        Effect.gen(function* () {
          const engine = yield* Effect.serviceOption(WorkflowEngine.WorkflowEngine);
          const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance);
          if (Option.isNone(engine) || Option.isNone(instance)) {
            return yield* Effect.die(new Error('expected to run inside a workflow'));
          }

          yield* Deferred.succeed(paused, undefined);
          yield* DurableClock.sleep({ name: 'pause', duration: '5 minutes' }).pipe(
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine.value),
            Effect.provideService(WorkflowEngine.WorkflowInstance, instance.value),
          );
          return { count: 1 };
        }),
    });
    const workflow = makeJobWorkflow(job);
    const requestsMemory = await Effect.runPromise(makeJobRequestsMemory());
    const finished = await Effect.runPromise(Deferred.make<void>());
    const requests = Layer.succeed(
      JobRequests,
      JobRequests.of({
        ...requestsMemory.service,
        markFinished: (input) =>
          requestsMemory.service
            .markFinished(input)
            .pipe(Effect.tap(() => Deferred.succeed(finished, undefined))),
      }),
    );
    const dropped = await Effect.runPromise(Deferred.make<void>());
    let settles = 0;
    const requestId = '6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b';
    const payload: JobWorkflowPayload = {
      ...scheduledPayload('2026-11-25'),
      trigger: 'api',
      requestId,
      attemptNonce: requestId,
    };
    const row = await runWorkflowScenario(
      job,
      (memory) =>
        Effect.gen(function* () {
          yield* workflow.execute(payload, { discard: true });
          yield* Deferred.await(paused);
          yield* workflow.interrupt(yield* workflow.executionId(payload));
          yield* Deferred.await(dropped);
          yield* TestClock.adjust('1 second');
          yield* Deferred.await(finished);
          return memory.rows()[0];
        }),
      {
        requests,
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            settle: (run, result) =>
              Effect.suspend(() => {
                settles += 1;
                return settles === 1 ? dropConnection(dropped) : real.settle(run, result);
              }),
          }),
      },
    );
    expect(settles).toBe(2);
    expect(row?.status).toBe('failed');
    expect(requestsMemory.rows()[0]?.outcome).toMatchObject({ errorTag: 'Interrupted' });
  });

  it('runs an unstaged body once per round across durable retry sleeps', async () => {
    const { job, calls } = makeCountingJob('test-replay-once', { failures: 1 });
    const outcome = await runWorkflowScenario(job, () =>
      executeScheduled(job, '2026-11-25', { steps: 10, step: '30 minutes' }),
    );
    expect(outcome).toMatchObject({ status: 'completed', attempt: 2 });
    expect(calls.body).toBe(2);
  });

  it('reaches the second round with an unstaged body longer than a heartbeat', async () => {
    const { job, calls } = makeCountingJob('test-replay-long', { failures: 1, takes: '1 minute' });
    const outcome = await runWorkflowScenario(job, () =>
      executeScheduled(job, '2026-11-25', { steps: 400, step: '30 seconds' }),
    );
    expect(outcome).toMatchObject({ status: 'completed', attempt: 2 });
    expect(calls.body).toBe(2);
  });

  it('keeps stage results across three rounds while running the body once per round', async () => {
    const calls = { a: 0, b: 0, c: 0, d: 0, body: 0 };
    const job = defineJob({
      name: 'test-replay-stages',
      description: 'b fails in round 1, d in round 2',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: Schema.Struct({ a: Schema.Int, b: Schema.Int, c: Schema.Int, d: Schema.Int }),
      run: () =>
        Effect.gen(function* () {
          calls.body += 1;
          const a = yield* countStage(
            'a',
            Effect.sync(() => ({ count: (calls.a += 1) * 10 })),
          );
          const b = yield* countStage(
            'b',
            Effect.suspend(() => failFirstCall((calls.b += 1), 4)),
          );
          const c = yield* countStage(
            'c',
            Effect.sync(() => ({ count: (calls.c += 1) * 7 })),
          );
          const d = yield* countStage(
            'd',
            Effect.suspend(() => failFirstCall((calls.d += 1), 1)),
          );
          return { a: a.count, b: b.count, c: c.count, d: d.count };
        }),
    });
    const outcome = await runWorkflowScenario(job, () =>
      executeScheduled(job, '2026-11-25', { steps: 10, step: '30 minutes' }),
    );
    expect(outcome).toMatchObject({ status: 'completed', stats: { a: 10, b: 4, c: 7, d: 1 } });
    expect(calls).toEqual({ a: 1, b: 2, c: 1, d: 2, body: 3 });
  });

  it('does not run a settled round again after another run took the key during the retry sleep', async () => {
    const { job, calls } = makeCountingJob('test-replay-takeover', { failures: 1 });
    const failedRound = await Effect.runPromise(Deferred.make<void>());
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const runs = yield* JobRuns;
          const fiber = yield* makeJobWorkflow(job)
            .execute(scheduledPayload('2026-11-25'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(failedRound);
          yield* TestClock.adjust('10 minutes');
          const takeover = yield* runs
            .claim({
              job: job.name,
              runKey: '2026-11-25',
              trigger: 'api',
              force: true,
              params: {},
              executionId: 'another-execution',
            })
            .pipe(Effect.orDie);
          if (takeover._tag !== 'Acquired') {
            return yield* Effect.die(new Error(`expected Acquired, got ${takeover._tag}`));
          }

          yield* runs
            .settle(
              {
                job: job.name,
                runKey: '2026-11-25',
                token: takeover.token,
                attempt: takeover.attempt,
              },
              {
                _tag: 'Completed',
                stats: { count: 0 },
                trigger: 'api',
                annotations: Context.empty(),
              },
            )
            .pipe(Effect.orDie);
          for (let step = 0; step < 6; step += 1) {
            yield* TestClock.adjust('30 minutes');
          }

          return yield* Fiber.join(fiber);
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            settle: (run, result) =>
              real
                .settle(run, result)
                .pipe(
                  Effect.tap(() =>
                    result._tag === 'Failed'
                      ? Deferred.succeed(failedRound, undefined)
                      : Effect.void,
                  ),
                ),
          }),
      },
    );
    expect(outcome).toMatchObject({ status: 'skipped', reason: { _tag: 'AlreadyCompleted' } });
    expect(calls.body).toBe(1);
  });

  it('logs job.heartbeat.failed when a replayed claim cannot reach the ledger', async () => {
    const { job, calls } = makeCountingJob('test-replay-heartbeat-down', { failures: 1 });
    const logger = makeTestLogger({ minLevel: 'Info' });
    const failedRound = await Effect.runPromise(Deferred.make<void>());
    let roundFailed = false;
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute(scheduledPayload('2026-11-25'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(failedRound);
          for (let step = 0; step < 300; step += 1) {
            yield* TestClock.adjust('1 minute');
          }

          return yield* Fiber.join(fiber);
        }),
      {
        logger: logger.layer,
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            // Only the replay checks the round-1 claim after its failed settle.
            heartbeat: (run) =>
              roundFailed && run.attempt === 1
                ? Effect.fail(connectionReset())
                : real.heartbeat(run),
            settle: (run, result) =>
              real.settle(run, result).pipe(
                Effect.tap(() => {
                  roundFailed ||= result._tag === 'Failed';
                  return result._tag === 'Failed'
                    ? Deferred.succeed(failedRound, undefined)
                    : Effect.void;
                }),
              ),
          }),
      },
    );
    expect(outcome).toMatchObject({ status: 'completed' });
    expect(calls.body).toBe(2);
    const heartbeatLog = logger.records.find((record) => record.message === 'job.heartbeat.failed');
    expect(heartbeatLog?.annotations).toMatchObject({
      job: 'test-replay-heartbeat-down',
      runKey: '2026-11-25',
      attempt: 1,
      errorMessage: expect.stringContaining('connection reset'),
    });
  });

  it('leaves an API request pending when its dispatch is interrupted', async () => {
    const { job } = makeCountingJob('test-dispatch-interrupted');
    const requestsMemory = await Effect.runPromise(makeJobRequestsMemory());
    const dispatching = await Effect.runPromise(Deferred.make<void>());
    const stalledEngine = Layer.effect(
      WorkflowEngine.WorkflowEngine,
      Effect.map(WorkflowEngine.WorkflowEngine, (engine) => ({
        ...engine,
        execute: () => Deferred.succeed(dispatching, undefined).pipe(Effect.andThen(Effect.never)),
      })),
    ).pipe(Layer.provide(makeTestClusterLayer([job])));
    const rows = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* dispatchApiJobWorkflow(job, { params: {}, force: false }).pipe(
          Effect.forkChild,
        );
        yield* Deferred.await(dispatching);
        yield* Fiber.interrupt(fiber);
        return requestsMemory.rows();
      }).pipe(
        Effect.provide(
          Layer.mergeAll(requestsMemory.layer, SecureRandom.layerWebCrypto, stalledEngine),
        ),
      ),
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]?.finishedAt).toBeNull();
  });

  it('lets the workflow finish a provisional dispatch failure it never picked up', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const requests = yield* makeJobRequestsMemory();
        const id = '3d7e9b10-5a42-4f6c-8e21-7c9d0a1b2e43';
        const job = 'dispatch-failed-unpicked';
        yield* requests.service.create({ id, job, params: {}, force: false, executionId: 'e1' });
        yield* requests.service.markDispatchFailed({ id, job, errorMessage: 'ack lost' });
        const finished = yield* requests.service.markFinished({
          id,
          runKey: null,
          outcome: { status: 'failed', errorTag: 'InvalidJobParams' },
        });
        const again = yield* requests.service.markFinished({
          id,
          runKey: null,
          outcome: { status: 'overwritten' },
        });
        return { finished, again, row: requests.rows()[0] };
      }),
    );
    expect(result.finished).toBe(true);
    expect(result.again).toBe(false);
    expect(result.row?.outcome).toEqual({ status: 'failed', errorTag: 'InvalidJobParams' });
  });

  it('clears a provisional dispatch failure when the workflow picks the request up', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const requests = yield* makeJobRequestsMemory();
        const id = '3d7e9b10-5a42-4f6c-8e21-7c9d0a1b2e43';
        const job = 'dispatch-failed';
        const request = { id, job, params: {}, force: false, executionId: 'e1' };
        yield* requests.service.create(request);
        yield* requests.service.markDispatchFailed({ id, job, errorMessage: 'ack lost' });
        yield* requests.service.upsertStarted({ ...request, runKey: 'once' });
        const afterPickup = requests.rows()[0];
        const finished = yield* requests.service.markFinished({
          id,
          runKey: 'once',
          outcome: { status: 'failed', errorTag: 'WorkflowDispatchError' },
        });
        const again = yield* requests.service.markFinished({
          id,
          runKey: 'once',
          outcome: { status: 'overwritten' },
        });
        return { afterPickup, finished, again, row: requests.rows()[0] };
      }),
    );
    expect(result.afterPickup).toMatchObject({ finishedAt: null, outcome: null });
    expect(result.finished).toBe(true);
    // A job error that happens to carry the dispatch tag is still final.
    expect(result.again).toBe(false);
    expect(result.row?.outcome).toEqual({ status: 'failed', errorTag: 'WorkflowDispatchError' });
  });

  it('waits out a ledger that keeps refusing claims and then completes the key', async () => {
    const { job, calls } = makeCountingJob('test-claim-outage');
    // The first refused claim starts the backoff, so the clock moves only once it is registered.
    const claiming = await Effect.runPromise(Deferred.make<void>());
    let claims = 0;
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute(scheduledPayload('2026-11-25'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(claiming);
          for (let step = 0; step < 240; step += 1) {
            yield* TestClock.adjust('10 seconds');
          }

          const first = yield* Fiber.join(fiber);
          const later = yield* executeScheduled(job, '2026-11-25', {
            steps: 6,
            step: '10 seconds',
          });
          return { first, later };
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            claim: (request) =>
              Effect.suspend(() => {
                claims += 1;
                if (claims > 100) {
                  return real.claim(request);
                }

                return Deferred.succeed(claiming, undefined).pipe(
                  Effect.andThen(Effect.fail(readOnlyTransaction())),
                );
              }),
          }),
      },
    );
    expect(outcome.first).toMatchObject({ status: 'completed', attempt: 1 });
    expect(outcome.later).toMatchObject({ status: 'completed', attempt: 1 });
    expect(calls.body).toBe(1);
  });

  it('stops retrying a settle that keeps failing, so the worker can shut down', async () => {
    const { job } = makeCountingJob('test-settle-outage');
    const dropped = await Effect.runPromise(Deferred.make<void>());
    let settles = 0;
    await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          yield* makeJobWorkflow(job)
            .execute(scheduledPayload('2026-11-25'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(dropped);
          for (let second = 0; second < 30; second += 1) {
            yield* TestClock.adjust('1 second');
          }
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            settle: () =>
              Effect.suspend(() => {
                settles += 1;
                return dropConnection(dropped);
              }),
          }),
      },
    );
    // One try and five retries; closing the scenario's scope above returned.
    expect(settles).toBe(6);
  });

  it('logs job.settle.lost with the error when a settle keeps failing', async () => {
    const { job } = makeCountingJob('test-settle-lost-log');
    const logger = makeTestLogger({ minLevel: 'Info' });
    const dropped = await Effect.runPromise(Deferred.make<void>());
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute({ ...scheduledPayload('2026-11-25'), trigger: 'manual', attemptNonce: 'm-1' })
            .pipe(Effect.forkChild);
          yield* Deferred.await(dropped);
          for (let second = 0; second < 30; second += 1) {
            yield* TestClock.adjust('1 second');
          }

          return yield* Fiber.join(fiber);
        }),
      {
        logger: logger.layer,
        wrapRuns: (real) => JobRuns.of({ ...real, settle: () => dropConnection(dropped) }),
      },
    );
    expect(outcome).toMatchObject({ status: 'failed', errorTag: 'RunClaimLost' });
    const lostLog = logger.records.find((record) => record.message === 'job.settle.lost');
    expect(lostLog?.level).toBe('WARN');
    expect(lostLog?.annotations).toMatchObject({
      job: 'test-settle-lost-log',
      runKey: '2026-11-25',
      attempt: 1,
      errorMessage: expect.stringContaining('connection reset'),
    });
  });

  it('does not count earlier claiming rounds or a claim retried after its commit as crashes', async () => {
    const { job, calls } = makeCountingJob('test-claim-lost-ack', { failures: 2 });
    let claims = 0;
    const outcome = await runWorkflowScenario(
      job,
      () => executeScheduled(job, '2026-11-25', { steps: 400, step: '1 minute' }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            claim: (request) =>
              Effect.suspend(() => {
                claims += 1;
                // The third claim commits and then loses its reply.
                return claims === 3
                  ? real.claim(request).pipe(Effect.andThen(Effect.fail(connectionReset())))
                  : real.claim(request);
              }),
          }),
      },
    );
    expect(outcome).toMatchObject({ status: 'completed', attempt: 4 });
    expect(calls.body).toBe(3);
  });

  it.each([
    { apiFailures: 0, lostReplies: MAX_CRASH_ATTEMPTS },
    { apiFailures: MAX_CRASH_ATTEMPTS - 1, lostReplies: 1 },
  ])(
    'runs the scheduled body with API failures ($apiFailures) and lost claim replies ($lostReplies)',
    async ({ apiFailures, lostReplies }) => {
      const { job, calls } = makeCountingJob('test-scheduled-claim-retries', {
        failures: apiFailures,
      });
      const runKey = '2026-11-25';
      const dropped = await Effect.runPromise(Deferred.make<void>());
      let repliesToLose = lostReplies;
      const outcome = await runWorkflowScenario(
        job,
        () =>
          Effect.gen(function* () {
            const workflow = makeJobWorkflow(job);
            for (let attempt = 0; attempt < apiFailures; attempt += 1) {
              const api = yield* workflow
                .execute({
                  ...scheduledPayload(runKey),
                  trigger: 'api',
                  attemptNonce: String(attempt),
                })
                .pipe(Effect.forkChild);
              yield* TestClock.adjust('10 seconds');
              yield* Fiber.join(api);
            }

            const fiber = yield* workflow.execute(scheduledPayload(runKey)).pipe(Effect.forkChild);
            yield* Deferred.await(dropped);
            yield* TestClock.adjust('10 seconds');
            return yield* Fiber.join(fiber);
          }),
        {
          wrapRuns: (real) =>
            JobRuns.of({
              ...real,
              claim: (request) =>
                real.claim(request).pipe(
                  Effect.tap(() => {
                    if (request.trigger === 'schedule' && repliesToLose > 0) {
                      repliesToLose -= 1;
                      return dropConnection(dropped);
                    }

                    return Effect.void;
                  }),
                ),
            }),
        },
      );
      expect(outcome).toMatchObject({
        status: 'completed',
        attempt: apiFailures + lostReplies + 1,
      });
      expect(calls.body).toBe(apiFailures + 1);
    },
  );

  it('does not run an API body again after a manual run took the key while it was suspended', async () => {
    const calls = { starts: 0, writes: 0 };
    const started = await Effect.runPromise(Deferred.make<void>());
    // Sleeps durably inside the body, the way `durableStage` reaches the workflow's services.
    const suspendingBody = Effect.gen(function* () {
      const engine = yield* Effect.serviceOption(WorkflowEngine.WorkflowEngine);
      const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance);
      if (Option.isNone(engine) || Option.isNone(instance)) {
        return yield* Effect.die(new Error('expected to run inside a workflow'));
      }

      calls.starts += 1;
      yield* Deferred.succeed(started, undefined);
      yield* DurableClock.sleep({ name: 'user-wait', duration: '5 minutes' }).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine.value),
        Effect.provideService(WorkflowEngine.WorkflowInstance, instance.value),
      );
      calls.writes += 1;
      return { count: calls.writes };
    });
    const job = defineJob({
      name: 'test-api-takeover',
      description: 'Sleeps durably inside its body',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: StageCountSchema,
      run: () => suspendingBody,
    });
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute({ ...scheduledPayload('2026-11-25'), trigger: 'api', attemptNonce: 'api-1' })
            .pipe(Effect.forkChild);
          yield* Deferred.await(started);
          yield* TestClock.adjust('1 second');
          yield* TestClock.adjust('3 minutes');
          yield* JobRuns.use((runs) =>
            runs.claim({
              job: job.name,
              runKey: '2026-11-25',
              trigger: 'manual',
              force: false,
              params: {},
              executionId: 'manual-execution',
            }),
          ).pipe(Effect.orDie);
          for (let minute = 0; minute < 20; minute += 1) {
            yield* TestClock.adjust('1 minute');
          }

          return yield* Fiber.join(fiber);
        }),
      // The interrupted body's 5-minute durable sleep still fires after the run lost its key, and
      // the in-memory cluster keeps that wake-up running without suspending. The default 1-second
      // TestClock wait lets the clock move past it, as the test intends.
      { testClock: TestClock.layer() },
    );
    expect(outcome).toMatchObject({ status: 'failed', errorTag: 'RunClaimLost' });
    expect(calls).toEqual({ starts: 1, writes: 0 });
  });

  it('claims again after a completion it could not write instead of ending the key', async () => {
    const { job, calls } = makeCountingJob('test-unwritten-completion');
    let settles = 0;
    const outcome = await runWorkflowScenario(
      job,
      () => executeScheduled(job, '2026-11-25', { steps: 10, step: '30 minutes' }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            settle: (run, result) =>
              Effect.suspend(() => {
                settles += 1;
                return settles === 1 ? Effect.succeed(false) : real.settle(run, result);
              }),
          }),
      },
    );
    expect(outcome).toMatchObject({ status: 'completed', attempt: 2 });
    // The row stayed this run's own, so the replayed first round cannot tell it already ran and
    // runs its body once more before the stored "not settled" reply sends it on to round 2.
    expect(calls.body).toBe(3);
  });

  it('lets the ledger end a Daily key whose budget earlier API failures used up', async () => {
    const { job, calls } = makeCountingJob('test-api-budget', { failures: 3 });
    const outcome = await runWorkflowScenario(
      job,
      (memory) =>
        Effect.gen(function* () {
          const apiFiber = yield* makeJobWorkflow(job)
            .execute({ ...scheduledPayload('2026-11-25'), trigger: 'api', attemptNonce: 'api-1' })
            .pipe(Effect.forkChild);
          yield* TestClock.adjust('10 seconds');
          yield* Fiber.join(apiFiber);
          const scheduled = yield* executeScheduled(job, '2026-11-25', {
            steps: 300,
            step: '1 minute',
          });
          return { scheduled, row: memory.rows()[0] };
        }),
      { policy: { retry: { dailyMaxFailures: 2 } } },
    );
    expect(outcome.scheduled).toMatchObject({ status: 'skipped', reason: { _tag: 'Exhausted' } });
    expect(outcome.row).toMatchObject({ failures: 2, nextAttemptAt: null });
    expect(calls.body).toBe(2);
  });

  it('marks a Daily key exhausted when the workflow gives up after a lost-lease round', async () => {
    let heartbeats = 0;
    let body = 0;
    const job = defineJob({
      name: 'test-budget-divergence',
      description: 'Loses its lease, then fails',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: StageCountSchema,
      run: () =>
        Effect.suspend(() => {
          body += 1;
          return body === 1
            ? Effect.sleep('10 minutes').pipe(Effect.as({ count: 1 }))
            : Effect.fail(new UpstreamError({ status: 503, message: 'upstream 503' }));
        }),
    });
    const outcome = await runWorkflowScenario(
      job,
      (memory) =>
        Effect.gen(function* () {
          const result = yield* executeScheduled(job, '2026-11-25', {
            steps: 200,
            step: '1 minute',
          });
          return { result, row: memory.rows()[0] };
        }),
      {
        policy: { retry: { dailyMaxFailures: 2 } },
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            heartbeat: (run) => {
              heartbeats += 1;
              return heartbeats === 1 ? Effect.succeed(false) : real.heartbeat(run);
            },
          }),
      },
    );
    expect(outcome.result).toMatchObject({ status: 'failed', errorTag: 'UpstreamError' });
    expect(outcome.row).toMatchObject({ status: 'failed', failures: 1, nextAttemptAt: null });
  });

  it('retries the settle of a crash-capped run that hit a dropped connection', async () => {
    const { job, calls } = makeCountingJob('test-crash-cap-settle');
    const workflow = makeJobWorkflow(job);
    const dropped = await Effect.runPromise(Deferred.make<void>());
    let settles = 0;
    const outcome = await runWorkflowScenario(
      job,
      (memory) =>
        Effect.gen(function* () {
          const payload = scheduledPayload('2026-11-25');
          const executionId = yield* workflow.executionId(payload);
          const runs = yield* JobRuns;
          for (let crash = 0; crash < 3; crash += 1) {
            yield* runs
              .claim({
                job: job.name,
                runKey: payload.runKey,
                trigger: 'schedule',
                force: false,
                params: {},
                executionId,
              })
              .pipe(Effect.orDie);
          }

          const fiber = yield* workflow.execute(payload).pipe(Effect.forkChild);
          yield* Deferred.await(dropped);
          yield* TestClock.adjust('10 seconds');
          const result = yield* Fiber.join(fiber);
          return { result, row: memory.rows()[0] };
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            settle: (run, result) =>
              Effect.suspend(() => {
                settles += 1;
                return settles === 1 ? dropConnection(dropped) : real.settle(run, result);
              }),
          }),
      },
    );
    expect(outcome.result).toMatchObject({ status: 'failed', errorTag: 'MaxAttemptsExceeded' });
    expect(outcome.row?.status).toBe('failed');
    expect(calls.body).toBe(0);
  });

  it('does not run a stage again when its stored success no longer decodes', async () => {
    const calls = { a: 0, body: 0 };
    const Renamed = Schema.Struct({ total: Schema.Int });
    const job = defineJob({
      name: 'test-stage-codec-change',
      description: 'Stage a changes its schema between rounds',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: StageCountSchema,
      run: () =>
        Effect.gen(function* () {
          calls.body += 1;
          const execute = Effect.sync(() => {
            calls.a += 1;
            return { count: 1, total: 1 };
          });
          // Round 1 stores `{ count }`; later rounds read it back with a schema it does not fit.
          yield* calls.body === 1
            ? durableStage({ name: 'a', schema: StageCountSchema, execute })
            : durableStage({ name: 'a', schema: Renamed, execute });
          return yield* countStage(
            'b',
            Effect.suspend(() => failFirstCall(calls.body, 2)),
          );
        }),
    });
    const outcome = await runWorkflowScenario(
      job,
      () => executeScheduled(job, '2026-11-25', { steps: 10, step: '30 minutes' }),
      { policy: { retry: { dailyMaxFailures: 2 } } },
    );
    expect(outcome.status).toBe('failed');
    expect(calls.a).toBe(1);
  });

  it('adds the job logFields to job.skipped', async () => {
    const logger = makeTestLogger({ minLevel: 'Info' });
    const job = defineJob({
      name: 'test-skipped-fields',
      description: 'Skips a completed key',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: StageCountSchema,
      logFields: { tenant: 'acme' },
      run: () => Effect.succeed({ count: 1 }),
    });
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          yield* executeScheduled(job, '2026-11-25', { steps: 1, step: '1 second' });
          // A manual run of the same key is a new execution, which finds the key completed.
          return yield* makeJobWorkflow(job).execute({
            ...scheduledPayload('2026-11-25'),
            trigger: 'manual',
            attemptNonce: 'manual-1',
          });
        }),
      { logger: logger.layer },
    );
    expect(outcome).toMatchObject({ status: 'skipped', reason: { _tag: 'AlreadyCompleted' } });
    const skippedLog = logger.records.find((record) => record.message === 'job.skipped');
    expect(skippedLog?.annotations).toMatchObject({
      tenant: 'acme',
      job: 'test-skipped-fields',
      skipReason: 'AlreadyCompleted',
    });
  });

  it('reports a RetryLater time counted from the last failed claim, not from its first', async () => {
    const { job } = makeCountingJob('test-retry-later-at');
    // The first claim starts the five-second wait, so the clock moves only once it is registered.
    const claiming = await Effect.runPromise(Deferred.make<void>());
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute({ ...scheduledPayload('2026-11-25'), trigger: 'api', attemptNonce: 'api-1' })
            .pipe(Effect.forkChild);
          yield* Deferred.await(claiming);
          for (let second = 0; second < 60; second += 1) {
            yield* TestClock.adjust('1 second');
          }

          return yield* Fiber.join(fiber);
        }),
      {
        policy: { retry: { busyDelay: '10 seconds' } },
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            claim: () =>
              Deferred.succeed(claiming, undefined).pipe(
                Effect.andThen(Effect.sleep('5 seconds')),
                Effect.andThen(Effect.fail(connectionReset())),
              ),
          }),
      },
    );
    // Six five-second claims and 6.2 seconds of backoff end after 36.2 s; the retry is ten seconds
    // after that (counting from the activity's start would give 10 s, already past).
    expect(outcome).toMatchObject({ status: 'skipped', reason: { _tag: 'RetryLater' } });
    const at =
      outcome.status === 'skipped' && outcome.reason._tag === 'RetryLater' ? outcome.reason.at : '';
    expect(Date.parse(at)).toBeGreaterThanOrEqual(46_200);
  });
});

/**
 * A scheduled Every job whose only durable stage is called `name`; `ran` counts its executions.
 */
const stageJob = (name: string) => {
  const ran = { count: 0 };
  const job = defineJob({
    name: 'test-reserved-stage',
    description: 'Runs one named stage',
    cadence: { _tag: 'Every', interval: '5 minutes' },
    params: { schema: EmptyParams, defaults: {} },
    stats: StageCountSchema,
    run: () =>
      countStage(
        name,
        Effect.sync(() => {
          ran.count += 1;
          return { count: 42 };
        }),
      ),
  });
  return { job, ran };
};

describe('reserved stage names', () => {
  it.each([
    'claim-round-1',
    'claim-deadline-2',
    'settle-completed-1',
    'settle-failed-3',
    'settle-skipped-1',
    'settle-crash-cap-1',
    'settle-invalid-params',
    'retry-later-clock-1',
    'retry-deadline-1',
    'run-at-clock',
    'DurableClock/run-at-1764147600000',
  ])('refuses a stage named %s in a workflow and in runJob', async (name) => {
    const { job, ran } = stageJob(name);
    const result = await runWorkflowScenario(job, (memory) =>
      Effect.gen(function* () {
        const workflow = yield* executeScheduled(job, '1970-01-01T00:00Z', {
          steps: 1,
          step: '1 second',
        });
        const direct = yield* runJob(job, { trigger: 'manual' }).pipe(
          Effect.provide([memory.layer, SecureRandom.layerWebCrypto]),
          Effect.orDie,
        );
        return { workflow, direct };
      }),
    );
    expect(ran.count).toBe(0);
    for (const outcome of [result.workflow, result.direct]) {
      expect(outcome).toMatchObject({
        status: 'failed',
        isDefect: true,
        errorMessage: expect.stringContaining('reserved'),
      });
    }
  });

  it.each(['claim-round', 'claim-round-x', 'my-run-at-clock', 'settle-failed-1b'])(
    'accepts the look-alike stage name %s',
    async (name) => {
      const { job, ran } = stageJob(name);
      const outcome = await runWorkflowScenario(job, () =>
        executeScheduled(job, '1970-01-01T00:00Z', { steps: 1, step: '1 second' }),
      );
      expect(outcome).toMatchObject({ status: 'completed', stats: { count: 42 } });
      expect(ran.count).toBe(1);
    },
  );
});

describe('Daily deadline on a lost claim', () => {
  it('does not retry past until after a round whose completion could not be written', async () => {
    const { job, calls } = makeCountingJob('test-deadline-lost-claim', {
      cadence: { _tag: 'Daily', at: '00:00', until: '01:00', checkEvery: '15 minutes' },
    });
    const unwritten = await Effect.runPromise(Deferred.make<void>());
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          const fiber = yield* makeJobWorkflow(job)
            .execute(scheduledPayload('1970-01-01'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(unwritten);
          for (let step = 0; step < 12; step += 1) {
            yield* TestClock.adjust('15 minutes');
          }

          return yield* Fiber.join(fiber);
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            // The claim is lost: heartbeats and the completion both find another owner.
            heartbeat: () => Effect.succeed(false),
            settle: (run, result) =>
              result._tag === 'Completed'
                ? Deferred.succeed(unwritten, undefined).pipe(Effect.as(false))
                : real.settle(run, result),
          }),
      },
    );
    // Rounds at 00:00, 00:15, 00:30, 00:45 and 01:00; a 01:15 retry is past until.
    expect(calls.body).toBe(5);
    expect(outcome).toMatchObject({ status: 'failed', errorTag: 'RunClaimLost' });
  });

  it('does not run the body when a claim retried past until succeeds', async () => {
    const { job, calls } = makeCountingJob('test-deadline-claim-retry', {
      cadence: { _tag: 'Daily', checkEvery: '1 minute', until: '00:05' },
    });
    const dropped = await Effect.runPromise(Deferred.make<void>());
    let claims = 0;
    const outcome = await runWorkflowScenario(
      job,
      () =>
        Effect.gen(function* () {
          // 00:05:59, the last minute a run may start in.
          yield* TestClock.setTime(5 * 60_000 + 59_000);
          const fiber = yield* makeJobWorkflow(job)
            .execute(scheduledPayload('1970-01-01'))
            .pipe(Effect.forkChild);
          yield* Deferred.await(dropped);
          for (let step = 0; step < 120; step += 1) {
            yield* TestClock.adjust('100 millis');
          }

          return yield* Fiber.join(fiber);
        }),
      {
        wrapRuns: (real) =>
          JobRuns.of({
            ...real,
            // The first three claims fail, so the backoffs carry the fourth past 00:06:00.
            claim: (request) =>
              Effect.suspend(() => {
                claims += 1;
                return claims <= 3 ? dropConnection(dropped) : real.claim(request);
              }),
          }),
      },
    );
    expect(outcome).toMatchObject({ status: 'skipped', reason: { _tag: 'Exhausted' } });
    expect(calls.body).toBe(0);
  });
});
