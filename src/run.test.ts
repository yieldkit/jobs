import { makeTestLogger } from '@yieldkit/logger/testing';
import { Context, Deferred, Duration, Effect, Fiber, Layer, Option, Schema } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { SqlError } from 'effect/sql';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { JobCadence, JobTrigger } from './job.ts';
import type { JobSettled } from './ledger-types.ts';

import { defineJob } from './job.ts';
import { JobLabel, RateLimitedError, UpstreamError } from './jobs.fixture.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';
import { defaultJobsPolicy } from './policy.ts';
import { raceWithLeaseRenewal } from './run-support.ts';
import { runJob } from './run.ts';
import { testRuntimeLayer } from './test-runtime.fixture.ts';

const MAX_SCHEDULED_DAILY_FAILURES = defaultJobsPolicy.retry.dailyMaxFailures;
const TEST_ANNOTATIONS = Context.make(JobLabel, 'test-label');
const BASE_TIME_MS = Date.UTC(2026, 10, 24, 10, 7, 0);
const SHORT_LEASE = Duration.seconds(20);
const SHORT_HEARTBEAT_INTERVAL = Duration.seconds(5);
const ONCE_BACKOFF_MS = Duration.toMillis(Duration.hours(1));

const EmptyParams = Schema.Struct({});
const CountStats = Schema.Struct({
  count: Schema.Int,
});

type CountStats = typeof CountStats.Type;

class TestStore extends Context.Service<
  TestStore,
  {
    readonly readCount: Effect.Effect<number>;
    readonly writeCount: (count: number) => Effect.Effect<void>;
  }
>()('@yieldkit/jobs/test/TestStore') {}

const makeSqlBlipError = (message: string): SqlError.SqlError =>
  new SqlError.SqlError({
    reason: new SqlError.ConnectionError({
      cause: new Error(message),
      message,
    }),
  });

/**
 * An `onClaimed` listener that throws, as a caller's broken callback would.
 */
const throwingListener = (): void => {
  throw new Error('listener failed');
};

const makeSimpleJob = <E = never>(options: {
  readonly name: string;
  readonly cadence?: JobCadence;
  readonly annotations?: Context.Context<never> | undefined;
  readonly run: (attempt: number) => Effect.Effect<CountStats, E>;
}) =>
  defineJob({
    name: options.name,
    description: `Test job ${options.name}`,
    annotations: options.annotations,
    cadence: options.cadence ?? { _tag: 'Daily', checkEvery: '1 hour' },
    params: { schema: EmptyParams, defaults: {} },
    stats: CountStats,
    run: (ctx) => options.run(ctx.attempt),
  });

const runWithTestClock = <A, E>(
  effect: Effect.Effect<A, E, TestClock.TestClock | SecureRandom.SecureRandom>,
): Promise<A> => {
  const quietLogger = makeTestLogger({ minLevel: 'Fatal' });
  return Effect.runPromise(
    Effect.gen(function* () {
      yield* TestClock.setTime(BASE_TIME_MS);
      return yield* effect;
    }).pipe(
      Effect.provide(
        Layer.mergeAll(quietLogger.layer, testRuntimeLayer, SecureRandom.layerWebCrypto),
      ),
    ),
  );
};

describe('runJob', () => {
  it('completes a run with stats, dryRun false, ledger row, and job log annotations', async () => {
    const logger = makeTestLogger({ minLevel: 'Debug' });
    const job = makeSimpleJob({
      name: 'daily-ok',
      annotations: TEST_ANNOTATIONS,
      run: () => Effect.succeed({ count: 7 }),
    });

    const { outcome, rows } = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        const runOutcome = yield* runJob(job, { trigger: 'schedule' }).pipe(
          Effect.provide(Layer.mergeAll(memory.layer, logger.layer)),
        );
        return { outcome: runOutcome, rows: memory.rows() };
      }),
    );
    expect(outcome).toMatchObject({
      job: 'daily-ok',
      runKey: '2026-11-24',
      trigger: 'schedule',
      status: 'completed',
      attempt: 1,
      dryRun: false,
      stats: { count: 7 },
    });
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      job: 'daily-ok',
      runKey: '2026-11-24',
      status: 'completed',
      attempt: 1,
      failures: 0,
      stats: { count: 7 },
    });
    expect(rows[0]?.finishedAt).toBeInstanceOf(Date);

    const completedLog = logger.records.find((record) => record.message === 'job.completed');
    expect(completedLog?.annotations).toMatchObject({
      job: 'daily-ok',
      runKey: '2026-11-24',
      jobRunId: 'daily-ok:2026-11-24:1',
      trigger: 'schedule',
      attempt: 1,
      count: 7,
    });
  });

  it('skips with AlreadyCompleted, Busy, RetryLater, and Exhausted', async () => {
    const results = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();

        const completedJob = makeSimpleJob({
          name: 'skip-completed',
          run: () => Effect.succeed({ count: 1 }),
        });
        yield* runJob(completedJob, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer));
        const alreadyCompleted = yield* runJob(completedJob, { trigger: 'schedule' }).pipe(
          Effect.provide(memory.layer),
        );

        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const busyJob = makeSimpleJob({
          name: 'skip-busy',
          run: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
              return { count: 1 };
            }),
        });
        const activeFiber = yield* Effect.forkChild(
          runJob(busyJob, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer)),
        );
        yield* Deferred.await(started);
        const busy = yield* runJob(busyJob, { trigger: 'schedule' }).pipe(
          Effect.provide(memory.layer),
        );
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(activeFiber);

        const onceFailingJob = makeSimpleJob({
          name: 'skip-retry-later',
          cadence: { _tag: 'Once', start: 'automatic' },
          run: () =>
            Effect.fail(
              new UpstreamError({
                status: 503,
                message: 'temporarily down',
              }),
            ),
        });
        yield* runJob(onceFailingJob, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer));
        const retryLater = yield* runJob(onceFailingJob, { trigger: 'schedule' }).pipe(
          Effect.provide(memory.layer),
        );

        const dailyExhaustJob = makeSimpleJob({
          name: 'skip-exhausted',
          run: () =>
            Effect.fail(
              new UpstreamError({
                status: 502,
                message: 'bad gateway',
              }),
            ),
        });
        for (let attempt = 0; attempt < MAX_SCHEDULED_DAILY_FAILURES; attempt += 1) {
          yield* runJob(dailyExhaustJob, { trigger: 'schedule' }).pipe(
            Effect.provide(memory.layer),
          );
        }

        const exhausted = yield* runJob(dailyExhaustJob, { trigger: 'schedule' }).pipe(
          Effect.provide(memory.layer),
        );
        return { alreadyCompleted, busy, retryLater, exhausted };
      }),
    );
    expect(results.alreadyCompleted).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'AlreadyCompleted' },
    });
    expect(results.busy).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'Busy' },
    });
    expect(results.retryLater).toMatchObject({
      status: 'skipped',
      reason: {
        _tag: 'RetryLater',
        at: new Date(BASE_TIME_MS + ONCE_BACKOFF_MS).toISOString(),
      },
    });
    expect(results.exhausted).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'Exhausted' },
    });
  });

  it('maps typed errors, any retryAfterSeconds field, and defects to failed outcomes', async () => {
    const outcomes = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();

        const upstreamJob = makeSimpleJob({
          name: 'err-upstream',
          run: () =>
            Effect.fail(
              new UpstreamError({
                status: 502,
                message: 'upstream failed',
              }),
            ),
        });
        const rateLimitedWithRetryJob = makeSimpleJob({
          name: 'err-rate-limit-seconds',
          run: () =>
            Effect.fail(
              new RateLimitedError({
                retryAfterSeconds: 45,
                message: 'slow down',
              }),
            ),
        });
        const rateLimitedNullJob = makeSimpleJob({
          name: 'err-rate-limit-null',
          run: () =>
            Effect.fail(
              new RateLimitedError({
                retryAfterSeconds: null,
                message: 'slow down without header',
              }),
            ),
        });
        const defectJob = makeSimpleJob({
          name: 'err-defect',
          run: () => Effect.die(new Error('unexpected invariant boom')),
        });

        const upstream = yield* runJob(upstreamJob, { trigger: 'api' }).pipe(
          Effect.provide(memory.layer),
        );
        const rateLimitedWithRetry = yield* runJob(rateLimitedWithRetryJob, {
          trigger: 'api',
        }).pipe(Effect.provide(memory.layer));
        const rateLimitedNull = yield* runJob(rateLimitedNullJob, { trigger: 'api' }).pipe(
          Effect.provide(memory.layer),
        );
        const defect = yield* runJob(defectJob, { trigger: 'api' }).pipe(
          Effect.provide(memory.layer),
        );
        return { upstream, rateLimitedWithRetry, rateLimitedNull, defect };
      }),
    );
    expect(outcomes.upstream).toMatchObject({
      status: 'failed',
      attempt: 1,
      errorMessage: 'upstream failed',
      errorTag: 'UpstreamError',
      isDefect: false,
      interrupted: false,
      retryAfterSeconds: undefined,
    });
    expect(outcomes.rateLimitedWithRetry).toMatchObject({
      status: 'failed',
      attempt: 1,
      errorMessage: 'slow down',
      errorTag: 'RateLimitedError',
      isDefect: false,
      interrupted: false,
      retryAfterSeconds: 45,
    });
    expect(outcomes.rateLimitedNull).toMatchObject({
      status: 'failed',
      attempt: 1,
      errorTag: 'RateLimitedError',
      isDefect: false,
      interrupted: false,
      retryAfterSeconds: undefined,
    });
    expect(outcomes.defect).toMatchObject({
      status: 'failed',
      attempt: 1,
      errorMessage: 'unexpected invariant boom',
      errorTag: undefined,
      isDefect: true,
      interrupted: false,
      retryAfterSeconds: undefined,
    });
  });

  it('settles interrupted runs as interrupted, with nextAttemptAt now and unchanged failures', async () => {
    const result = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        const started = yield* Deferred.make<void>();
        const job = makeSimpleJob({
          name: 'interrupt-job',
          annotations: TEST_ANNOTATIONS,
          run: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(started, undefined);
              return yield* Effect.never;
            }),
        });
        const fiber = yield* Effect.forkChild(
          runJob(job, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer)),
        );
        yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);
        return { rows: memory.rows(), settles: memory.settles() };
      }),
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      job: 'interrupt-job',
      status: 'failed',
      attempt: 1,
      failures: 0,
      nextAttemptAt: new Date(BASE_TIME_MS),
    });
    expect(result.settles).toHaveLength(1);
    expect(result.settles[0]?.result).toMatchObject({
      _tag: 'Failed',
      reason: 'interrupted',
      annotations: TEST_ANNOTATIONS,
    });
    expect(result.settles[0]?.event).toMatchObject({
      job: 'interrupt-job',
      trigger: 'schedule',
      outcome: { _tag: 'Failed', reason: 'interrupted', failures: 0 },
    });
  });

  it('never exhausts a Daily key after six interruptions while six real failures exhaust it', async () => {
    const result = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        let interruptRun = 0;

        const interruptibleJob = makeSimpleJob({
          name: 'daily-interrupted',
          run: () => {
            interruptRun += 1;
            if (interruptRun <= MAX_SCHEDULED_DAILY_FAILURES) {
              return Effect.never;
            }

            return Effect.succeed({ count: 99 });
          },
        });

        for (let count = 0; count < MAX_SCHEDULED_DAILY_FAILURES; count += 1) {
          const started = yield* Deferred.make<void>();
          const wrapped = makeSimpleJob({
            name: 'daily-interrupted',
            run: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(started, undefined);
                return yield* interruptibleJob.run({
                  params: {},
                  runKey: '2026-11-24',
                  trigger: 'schedule',
                  attempt: count + 1,
                });
              }),
          });
          const fiber = yield* Effect.forkChild(
            runJob(wrapped, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer)),
          );
          yield* Deferred.await(started);
          yield* Fiber.interrupt(fiber);
        }

        const seventhAfterInterrupts = yield* runJob(interruptibleJob, {
          trigger: 'schedule',
        }).pipe(Effect.provide(memory.layer));

        const realFailureJob = makeSimpleJob({
          name: 'daily-real-failures',
          run: () =>
            Effect.fail(
              new UpstreamError({
                status: 500,
                message: 'server error',
              }),
            ),
        });
        for (let count = 0; count < MAX_SCHEDULED_DAILY_FAILURES; count += 1) {
          yield* runJob(realFailureJob, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer));
        }

        const seventhAfterFailures = yield* runJob(realFailureJob, { trigger: 'schedule' }).pipe(
          Effect.provide(memory.layer),
        );
        return {
          seventhAfterInterrupts,
          seventhAfterFailures,
          rows: memory.rows(),
        };
      }),
    );
    expect(result.seventhAfterInterrupts).toMatchObject({
      status: 'completed',
      attempt: MAX_SCHEDULED_DAILY_FAILURES + 1,
      stats: { count: 99 },
    });
    expect(result.seventhAfterFailures).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'Exhausted' },
    });

    const failedRow = result.rows.find((row) => row.job === 'daily-real-failures');
    expect(failedRow).toMatchObject({
      failures: MAX_SCHEDULED_DAILY_FAILURES,
      nextAttemptAt: null,
    });
  });

  it('fails with claim lost when heartbeat returns false or settle returns false', async () => {
    const result = await runWithTestClock(
      Effect.gen(function* () {
        const memoryA = yield* makeJobRunsMemory({ policy: { lease: { duration: SHORT_LEASE } } });
        const losingHeartbeatLayer = Layer.effect(
          JobRuns,
          Effect.gen(function* () {
            const inner = yield* JobRuns;
            return JobRuns.of({
              ...inner,
              heartbeat: () => Effect.succeed(false),
            });
          }),
        ).pipe(Layer.provide(memoryA.layer));

        const workStarted = yield* Deferred.make<void>();
        const longJob = makeSimpleJob({
          name: 'lost-heartbeat',
          run: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(workStarted, undefined);
              return yield* Effect.never;
            }),
        });
        const fiber = yield* Effect.forkChild(
          runJob(longJob, { trigger: 'schedule' }).pipe(Effect.provide(losingHeartbeatLayer)),
        );
        yield* Deferred.await(workStarted);
        yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        const heartbeatLostOutcome = yield* Fiber.join(fiber);

        const memoryB = yield* makeJobRunsMemory({ policy: { lease: { duration: SHORT_LEASE } } });
        const losingSettleLayer = Layer.effect(
          JobRuns,
          Effect.gen(function* () {
            const inner = yield* JobRuns;
            return JobRuns.of({
              ...inner,
              settle: () => Effect.succeed(false),
            });
          }),
        ).pipe(Layer.provide(memoryB.layer));

        const fastJob = makeSimpleJob({
          name: 'lost-settle',
          run: () => Effect.succeed({ count: 3 }),
        });
        const settleLostOutcome = yield* runJob(fastJob, { trigger: 'schedule' }).pipe(
          Effect.provide(losingSettleLayer),
        );
        return { heartbeatLostOutcome, settleLostOutcome };
      }),
    );
    expect(result.heartbeatLostOutcome).toMatchObject({
      status: 'failed',
      attempt: 1,
      errorMessage: 'lost its run claim',
      errorTag: 'RunClaimLost',
      interrupted: true,
      isDefect: false,
    });
    expect(result.settleLostOutcome).toMatchObject({
      status: 'failed',
      attempt: 1,
      errorMessage: 'lost its run claim',
      errorTag: 'RunClaimLost',
      interrupted: true,
      isDefect: false,
    });
  });

  it('logs job.settle.lost after job.completed when the settle is rejected', async () => {
    const logger = makeTestLogger({ minLevel: 'Debug' });
    const outcome = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        const rejectingLayer = Layer.effect(
          JobRuns,
          Effect.map(JobRuns, (inner) =>
            JobRuns.of({ ...inner, settle: () => Effect.succeed(false) }),
          ),
        ).pipe(Layer.provide(memory.layer));
        const job = makeSimpleJob({
          name: 'rejected-settle',
          run: () => Effect.succeed({ count: 1 }),
        });
        return yield* runJob(job, { trigger: 'manual' }).pipe(
          Effect.provide(Layer.mergeAll(rejectingLayer, logger.layer)),
        );
      }),
    );
    expect(outcome).toMatchObject({ status: 'failed', errorTag: 'RunClaimLost' });
    const events = logger.records.map((record) => record.message);
    expect(events.indexOf('job.settle.lost')).toBeGreaterThan(events.indexOf('job.completed'));
    expect(events.indexOf('job.completed')).toBeGreaterThanOrEqual(0);
    const lostLog = logger.records.find((record) => record.message === 'job.settle.lost');
    expect(lostLog?.level).toBe('WARN');
    expect(lostLog?.annotations).toMatchObject({ job: 'rejected-settle', attempt: 1 });
  });

  it('retries transient heartbeat SQL errors and ends the run if failures persist for a full lease', async () => {
    const logger = makeTestLogger({ minLevel: 'Warn' });
    const result = await runWithTestClock(
      Effect.gen(function* () {
        const memoryTransient = yield* makeJobRunsMemory({
          policy: { lease: { duration: SHORT_LEASE } },
        });
        let transientCalls = 0;
        const transientLayer = Layer.effect(
          JobRuns,
          Effect.gen(function* () {
            const inner = yield* JobRuns;
            return JobRuns.of({
              ...inner,
              heartbeat: (run) =>
                Effect.gen(function* () {
                  transientCalls += 1;
                  if (transientCalls <= 2) {
                    return yield* makeSqlBlipError('transient connection blip');
                  }

                  return yield* inner.heartbeat(run);
                }),
            });
          }),
        ).pipe(Layer.provide(memoryTransient.layer));

        const healthyJob = makeSimpleJob({
          name: 'transient-heartbeat',
          run: () =>
            Effect.gen(function* () {
              yield* Effect.sleep(Duration.seconds(16));
              return { count: 42 };
            }),
        });
        const transientFiber = yield* Effect.forkChild(
          runJob(healthyJob, { trigger: 'schedule' }).pipe(
            Effect.provide(Layer.mergeAll(transientLayer, logger.layer)),
          ),
        );
        yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        yield* TestClock.adjust(Duration.seconds(1));
        const transientOutcome = yield* Fiber.join(transientFiber);

        const memoryPersistent = yield* makeJobRunsMemory({
          policy: { lease: { duration: SHORT_LEASE } },
        });
        const persistentLayer = Layer.effect(
          JobRuns,
          Effect.gen(function* () {
            const inner = yield* JobRuns;
            return JobRuns.of({
              ...inner,
              heartbeat: () => Effect.fail(makeSqlBlipError('database unreachable')),
            });
          }),
        ).pipe(Layer.provide(memoryPersistent.layer));

        const persistentJob = makeSimpleJob({
          name: 'persistent-heartbeat',
          run: () => Effect.never,
        });
        const persistentFiber = yield* Effect.forkChild(
          runJob(persistentJob, { trigger: 'schedule' }).pipe(
            Effect.provide(Layer.mergeAll(persistentLayer, logger.layer)),
          ),
        );
        yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        const stillRunningBeforeLease = persistentFiber.pollUnsafe() === undefined;
        yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        const persistentOutcome = yield* Fiber.join(persistentFiber);
        return {
          transientCalls,
          transientOutcome,
          stillRunningBeforeLease,
          persistentOutcome,
        };
      }),
    );
    expect(result.transientCalls).toBe(3);
    expect(result.transientOutcome).toMatchObject({
      status: 'completed',
      dryRun: false,
      stats: { count: 42 },
    });
    expect(result.stillRunningBeforeLease).toBe(true);
    expect(result.persistentOutcome).toMatchObject({
      status: 'failed',
      errorMessage: 'lost its run claim',
      errorTag: 'RunClaimLost',
      interrupted: true,
    });
    expect(
      logger.records.filter((record) => record.message === 'job.heartbeat.failed').length,
    ).toBeGreaterThanOrEqual(6);
  });

  it('counts a renewal that hangs past its interval as failed and ends the run after a lease', async () => {
    const outcome = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory({ policy: { lease: { duration: SHORT_LEASE } } });
        const hangingLayer = Layer.effect(
          JobRuns,
          Effect.map(JobRuns, (inner) => JobRuns.of({ ...inner, heartbeat: () => Effect.never })),
        ).pipe(Layer.provide(memory.layer));
        const job = makeSimpleJob({ name: 'hanging-heartbeat', run: () => Effect.never });
        const fiber = yield* Effect.forkChild(
          runJob(job, { trigger: 'schedule' }).pipe(
            Effect.provide(
              Layer.mergeAll(hangingLayer, makeTestLogger({ minLevel: 'Fatal' }).layer),
            ),
          ),
        );
        for (let beat = 0; beat < 9; beat += 1) {
          yield* TestClock.adjust(SHORT_HEARTBEAT_INTERVAL);
        }

        return fiber.pollUnsafe();
      }),
    );
    expect(outcome).toMatchObject({
      _tag: 'Success',
      value: { status: 'failed', errorTag: 'RunClaimLost' },
    });
  });

  it('ends the race at the lease deadline when renewals hang and the interval does not divide it', async () => {
    const exit = await Effect.gen(function* () {
      const fiber = yield* raceWithLeaseRenewal(
        Effect.sleep('150 seconds').pipe(Effect.as('done')),
        {
          renew: Effect.never,
          lease: { duration: Duration.seconds(120), heartbeatsPerLease: 3 },
          lost: () => 'lost',
          failureEvent: 'test.renewal.failed',
          fields: {},
        },
      ).pipe(Effect.exit, Effect.forkChild);
      for (let second = 0; second < 130; second += 1) {
        yield* TestClock.adjust('1 second');
      }

      return fiber.pollUnsafe();
    }).pipe(
      Effect.provide(Layer.merge(testRuntimeLayer, makeTestLogger({ minLevel: 'Fatal' }).layer)),
      Effect.runPromise,
    );
    expect(exit).toMatchObject({ _tag: 'Success', value: { _tag: 'Failure' } });
  });

  it('ends the race at the lease deadline when each renewal fails ten seconds late', async () => {
    const exit = await Effect.gen(function* () {
      const lateFailure = Effect.sleep('10 seconds').pipe(
        Effect.andThen(Effect.fail(makeSqlBlipError('connection reset'))),
      );
      const fiber = yield* raceWithLeaseRenewal(
        Effect.sleep('130 seconds').pipe(Effect.as('done')),
        {
          renew: lateFailure,
          lease: { duration: Duration.seconds(120), heartbeatsPerLease: 3 },
          lost: () => 'lost',
          failureEvent: 'test.renewal.failed',
          fields: {},
        },
      ).pipe(Effect.exit, Effect.forkChild);
      for (let second = 0; second < 121; second += 1) {
        yield* TestClock.adjust('1 second');
      }

      return fiber.pollUnsafe();
    }).pipe(
      Effect.provide(Layer.merge(testRuntimeLayer, makeTestLogger({ minLevel: 'Fatal' }).layer)),
      Effect.runPromise,
    );
    expect(exit).toMatchObject({ _tag: 'Success', value: { _tag: 'Failure' } });
  });

  it('hands the trigger, annotations and failure reason of every settle to the onSettle hook', async () => {
    const triggers: ReadonlyArray<JobTrigger> = ['schedule', 'manual', 'api'];
    const { events, settles } = await runWithTestClock(
      Effect.gen(function* () {
        const hooked: Array<JobSettled> = [];
        const memory = yield* makeJobRunsMemory({
          onSettle: (event) =>
            Effect.sync(() => {
              hooked.push(event);
            }),
        });

        for (const trigger of triggers) {
          const okJob = makeSimpleJob({
            name: `settle-ok-${trigger}`,
            annotations: TEST_ANNOTATIONS,
            run: () => Effect.succeed({ count: 1 }),
          });
          yield* runJob(okJob, { trigger }).pipe(Effect.provide(memory.layer));

          const failJob = makeSimpleJob({
            name: `settle-fail-${trigger}`,
            annotations: TEST_ANNOTATIONS,
            run: () => Effect.fail(new UpstreamError({ status: 500, message: 'boom' })),
          });
          yield* runJob(failJob, { trigger }).pipe(Effect.provide(memory.layer));

          const started = yield* Deferred.make<void>();
          const interruptJob = makeSimpleJob({
            name: `settle-interrupt-${trigger}`,
            run: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(started, undefined);
                return yield* Effect.never;
              }),
          });
          const fiber = yield* Effect.forkChild(
            runJob(interruptJob, { trigger }).pipe(Effect.provide(memory.layer)),
          );
          yield* Deferred.await(started);
          yield* Fiber.interrupt(fiber);
        }

        return { events: hooked, settles: memory.settles() };
      }),
    );
    expect(events).toEqual(settles.map((entry) => entry.event));
    const eventFor = (jobName: string): JobSettled | undefined =>
      events.find((event) => event.job === jobName);
    for (const trigger of triggers) {
      const ok = eventFor(`settle-ok-${trigger}`);
      expect(ok).toMatchObject({ trigger, attempt: 1, outcome: { _tag: 'Completed' } });
      expect(Option.getOrUndefined(Context.getOption(ok!.annotations, JobLabel))).toBe(
        'test-label',
      );
      expect(eventFor(`settle-fail-${trigger}`)).toMatchObject({
        trigger,
        outcome: { _tag: 'Failed', reason: 'error', failures: 1, errorMessage: 'boom' },
      });
      const interrupted = eventFor(`settle-interrupt-${trigger}`);
      expect(interrupted).toMatchObject({
        trigger,
        outcome: { _tag: 'Failed', reason: 'interrupted', failures: 0 },
      });
      expect(Option.isNone(Context.getOption(interrupted!.annotations, JobLabel))).toBe(true);
    }
  });

  it('keeps settling when the onSettle hook fails or dies', async () => {
    const result = await runWithTestClock(
      Effect.gen(function* () {
        let calls = 0;
        const memory = yield* makeJobRunsMemory({
          onSettle: () =>
            Effect.suspend(() => {
              calls += 1;
              return calls === 1 ? Effect.fail('hook failed') : Effect.die(new Error('hook died'));
            }),
        });
        const first = makeSimpleJob({
          name: 'hook-fails',
          run: () => Effect.succeed({ count: 1 }),
        });
        const second = makeSimpleJob({
          name: 'hook-dies',
          run: () => Effect.succeed({ count: 2 }),
        });
        const outcomes = [
          yield* runJob(first, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer)),
          yield* runJob(second, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer)),
        ];
        return { outcomes, rows: memory.rows(), calls };
      }),
    );
    expect(result.calls).toBe(2);
    expect(result.outcomes.map((outcome) => outcome.status)).toEqual(['completed', 'completed']);
    expect(result.rows.map((row) => row.status)).toEqual(['completed', 'completed']);
  });

  it('implies force only for Every jobs with non-schedule triggers unless explicitly false', async () => {
    const outcomes = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        const everyJob = makeSimpleJob({
          name: 'force-every',
          cadence: { _tag: 'Every', interval: '5 minutes' },
          run: (attempt) => Effect.succeed({ count: attempt }),
        });
        const dailyJob = makeSimpleJob({
          name: 'force-daily',
          cadence: { _tag: 'Daily', checkEvery: '1 hour' },
          run: (attempt) => Effect.succeed({ count: attempt }),
        });
        const onceJob = makeSimpleJob({
          name: 'force-once',
          cadence: { _tag: 'Once', start: 'manual' },
          run: (attempt) => Effect.succeed({ count: attempt }),
        });

        yield* runJob(everyJob, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer));
        const everyScheduleRepeat = yield* runJob(everyJob, { trigger: 'schedule' }).pipe(
          Effect.provide(memory.layer),
        );
        const everyCliImplied = yield* runJob(everyJob, { trigger: 'manual' }).pipe(
          Effect.provide(memory.layer),
        );
        const everyApiImplied = yield* runJob(everyJob, { trigger: 'api' }).pipe(
          Effect.provide(memory.layer),
        );
        const everyCliExplicitFalse = yield* runJob(everyJob, {
          trigger: 'manual',
          force: false,
        }).pipe(Effect.provide(memory.layer));

        yield* runJob(dailyJob, { trigger: 'schedule' }).pipe(Effect.provide(memory.layer));
        const dailyCliNoForce = yield* runJob(dailyJob, { trigger: 'manual' }).pipe(
          Effect.provide(memory.layer),
        );
        const dailyApiNoForce = yield* runJob(dailyJob, { trigger: 'api' }).pipe(
          Effect.provide(memory.layer),
        );
        const dailyCliExplicitTrue = yield* runJob(dailyJob, {
          trigger: 'manual',
          force: true,
        }).pipe(Effect.provide(memory.layer));

        yield* runJob(onceJob, { trigger: 'manual' }).pipe(Effect.provide(memory.layer));
        const onceCliNoForce = yield* runJob(onceJob, { trigger: 'manual' }).pipe(
          Effect.provide(memory.layer),
        );
        const onceApiNoForce = yield* runJob(onceJob, { trigger: 'api' }).pipe(
          Effect.provide(memory.layer),
        );
        const onceCliExplicitTrue = yield* runJob(onceJob, {
          trigger: 'manual',
          force: true,
        }).pipe(Effect.provide(memory.layer));
        return {
          everyScheduleRepeat,
          everyCliImplied,
          everyApiImplied,
          everyCliExplicitFalse,
          dailyCliNoForce,
          dailyApiNoForce,
          dailyCliExplicitTrue,
          onceCliNoForce,
          onceApiNoForce,
          onceCliExplicitTrue,
        };
      }),
    );
    expect(outcomes.everyScheduleRepeat).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'AlreadyCompleted' },
    });
    expect(outcomes.everyCliImplied).toMatchObject({
      status: 'completed',
      attempt: 2,
      stats: { count: 2 },
    });
    expect(outcomes.everyApiImplied).toMatchObject({
      status: 'completed',
      attempt: 3,
      stats: { count: 3 },
    });
    expect(outcomes.everyCliExplicitFalse).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'AlreadyCompleted' },
    });

    expect(outcomes.dailyCliNoForce).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'AlreadyCompleted' },
    });
    expect(outcomes.dailyApiNoForce).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'AlreadyCompleted' },
    });
    expect(outcomes.dailyCliExplicitTrue).toMatchObject({
      status: 'completed',
      attempt: 2,
      stats: { count: 2 },
    });

    expect(outcomes.onceCliNoForce).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'AlreadyCompleted' },
    });
    expect(outcomes.onceApiNoForce).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'AlreadyCompleted' },
    });
    expect(outcomes.onceCliExplicitTrue).toMatchObject({
      status: 'completed',
      attempt: 2,
      stats: { count: 2 },
    });
  });

  it('fails with InvalidJobParams before claiming anything', async () => {
    let executed = false;
    const job = defineJob({
      name: 'needs-valid-params',
      description: 'Requires positive limit and file path',
      cadence: { _tag: 'Once', start: 'manual' },
      params: {
        schema: Schema.Struct({
          file: Schema.NonEmptyString,
          limit: Schema.Int.check(Schema.isGreaterThan(0)),
        }),
        defaults: { limit: 10 },
      },
      stats: CountStats,
      run: () =>
        Effect.sync(() => {
          executed = true;
          return { count: 1 };
        }),
    });

    const result = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        const missingRequired = yield* runJob(job, { trigger: 'manual' }).pipe(
          Effect.provide(memory.layer),
          Effect.result,
        );
        const invalidBound = yield* runJob(job, {
          trigger: 'manual',
          params: { file: '/tmp/test.db', limit: 0 },
        }).pipe(Effect.provide(memory.layer), Effect.result);
        return { missingRequired, invalidBound, rows: memory.rows() };
      }),
    );

    expect(executed).toBe(false);
    expect(result.rows).toHaveLength(0);
    expect(result.missingRequired._tag).toBe('Failure');
    expect(result.missingRequired).toMatchObject({
      failure: { _tag: 'InvalidJobParams', job: 'needs-valid-params' },
    });
    expect(result.invalidBound._tag).toBe('Failure');
    expect(result.invalidBound).toMatchObject({ failure: { _tag: 'InvalidJobParams' } });
  });

  it('rejects dryRun without a dryRun layer and runs with the dryRun layer without claiming', async () => {
    const liveWrites: Array<number> = [];
    const dryWrites: Array<number> = [];
    const liveStoreLayer = Layer.succeed(
      TestStore,
      TestStore.of({
        readCount: Effect.succeed(11),
        writeCount: (count) =>
          Effect.sync(() => {
            liveWrites.push(count);
          }),
      }),
    );
    const dryStoreLayer = Layer.effect(
      TestStore,
      Effect.gen(function* () {
        const live = yield* TestStore;
        return TestStore.of({
          readCount: live.readCount,
          writeCount: (count) =>
            Effect.sync(() => {
              dryWrites.push(count);
            }),
        });
      }),
    );

    const unsupportedJob = makeSimpleJob({
      name: 'no-dry-run',
      run: () => Effect.succeed({ count: 1 }),
    });
    const supportedJob = defineJob({
      name: 'supports-dry-run',
      description: 'Supports dry-run store substitution',
      annotations: TEST_ANNOTATIONS,
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: EmptyParams, defaults: {} },
      stats: CountStats,
      dryRun: dryStoreLayer,
      run: () =>
        Effect.gen(function* () {
          const store = yield* TestStore;
          const count = yield* store.readCount;
          yield* store.writeCount(count);
          return { count };
        }),
    });

    const result = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        const unsupported = yield* runJob(unsupportedJob, {
          trigger: 'manual',
          dryRun: true,
        }).pipe(Effect.provide(memory.layer), Effect.result);
        const supported = yield* runJob(supportedJob, {
          trigger: 'manual',
          dryRun: true,
        }).pipe(Effect.provide(Layer.merge(memory.layer, liveStoreLayer)));
        return {
          unsupported,
          supported,
          rows: memory.rows(),
          settles: memory.settles(),
        };
      }),
    );
    expect(result.unsupported._tag).toBe('Failure');
    expect(result.unsupported).toMatchObject({
      failure: { _tag: 'DryRunUnsupported', job: 'no-dry-run' },
    });

    expect(result.supported).toMatchObject({
      job: 'supports-dry-run',
      runKey: '2026-11-24',
      trigger: 'manual',
      status: 'completed',
      attempt: 0,
      dryRun: true,
      stats: { count: 11 },
    });
    expect(liveWrites).toEqual([]);
    expect(dryWrites).toEqual([11]);
    expect(result.rows).toHaveLength(0);
    expect(result.settles).toHaveLength(0);
  });

  it('settles a run as a failed defect when onClaimed throws, leaving the key free', async () => {
    const result = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        const job = makeSimpleJob({
          name: 'throwing-on-claimed',
          run: () => Effect.succeed({ count: 1 }),
        });
        const first = yield* runJob(job, {
          trigger: 'manual',
          onClaimed: throwingListener,
        }).pipe(Effect.provide(memory.layer));
        const rowAfterFirst = memory.rows()[0];
        const second = yield* runJob(job, { trigger: 'manual', force: true }).pipe(
          Effect.provide(memory.layer),
        );
        return { first, rowAfterFirst, second };
      }),
    );
    expect(result.first).toMatchObject({ status: 'failed', isDefect: true });
    expect(result.rowAfterFirst).toMatchObject({ status: 'failed' });
    expect(result.second).toMatchObject({ status: 'completed', attempt: 2 });
  });

  it('retries a settle that fails with a transient SqlError', async () => {
    const result = await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        let settleFailures = 0;
        const flakySettleLayer = Layer.effect(
          JobRuns,
          Effect.gen(function* () {
            const inner = yield* JobRuns;
            return JobRuns.of({
              ...inner,
              settle: (run, runResult) =>
                Effect.suspend(() => {
                  settleFailures += 1;
                  return settleFailures === 1
                    ? Effect.fail(makeSqlBlipError('connection reset'))
                    : inner.settle(run, runResult);
                }),
            });
          }),
        ).pipe(Layer.provide(memory.layer));
        const job = makeSimpleJob({
          name: 'flaky-settle',
          run: () => Effect.succeed({ count: 4 }),
        });
        const fiber = yield* runJob(job, { trigger: 'schedule' }).pipe(
          Effect.provide(flakySettleLayer),
          Effect.forkChild,
        );
        yield* TestClock.adjust('1 second');
        const outcome = yield* Fiber.join(fiber);
        return { outcome, rows: memory.rows() };
      }),
    );
    expect(result.outcome).toMatchObject({ status: 'completed', stats: { count: 4 } });
    expect(result.rows).toMatchObject([{ status: 'completed', attempt: 1 }]);
  });

  it('keeps the run identity on job.completed when stats use the same keys', async () => {
    const logger = makeTestLogger({ minLevel: 'Info' });
    const IdentityStats = Schema.Struct({
      job: Schema.String,
      jobRunId: Schema.String,
      runKey: Schema.String,
      attempt: Schema.Int,
      trigger: Schema.String,
      count: Schema.Int,
    });
    const job = defineJob({
      name: 'stats-identity',
      description: 'Test job stats-identity',
      cadence: { _tag: 'Once', start: 'manual' },
      params: { schema: EmptyParams, defaults: {} },
      stats: IdentityStats,
      run: () =>
        Effect.succeed({
          job: 'previous-import',
          jobRunId: 'previous-import:yesterday:9',
          runKey: 'yesterday',
          attempt: 9,
          trigger: 'api',
          count: 2,
        }),
    });
    await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        return yield* runJob(job, { trigger: 'manual' }).pipe(
          Effect.provide(Layer.mergeAll(memory.layer, logger.layer)),
        );
      }),
    );
    const completedLog = logger.records.find((record) => record.message === 'job.completed');
    expect(completedLog?.annotations).toMatchObject({
      job: 'stats-identity',
      jobRunId: 'stats-identity:once:1',
      runKey: 'once',
      attempt: 1,
      trigger: 'manual',
      count: 2,
    });
  });

  it('adds the job logFields to job.skipped', async () => {
    const logger = makeTestLogger({ minLevel: 'Info' });
    const job = defineJob({
      name: 'skipped-fields',
      description: 'Test job skipped-fields',
      cadence: { _tag: 'Once', start: 'manual' },
      params: { schema: EmptyParams, defaults: {} },
      stats: CountStats,
      logFields: { tenant: 'acme' },
      run: () => Effect.succeed({ count: 1 }),
    });
    await runWithTestClock(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        const layer = Layer.mergeAll(memory.layer, logger.layer);
        yield* runJob(job, { trigger: 'manual' }).pipe(Effect.provide(layer));
        return yield* runJob(job, { trigger: 'manual' }).pipe(Effect.provide(layer));
      }),
    );
    const skippedLog = logger.records.find((record) => record.message === 'job.skipped');
    expect(skippedLog?.annotations).toMatchObject({
      tenant: 'acme',
      job: 'skipped-fields',
      skipReason: 'AlreadyCompleted',
    });
  });
});
