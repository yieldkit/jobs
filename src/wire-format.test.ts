import type { Duration } from 'effect';
import type { SqlError } from 'effect/sql';

import { Context, Effect, Fiber, Layer, Schema } from 'effect';
import { MessageStorage } from 'effect/cluster';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { JobCadence, JobDefinition } from './job.ts';
import type { JobWorkflowPayload } from './workflow-schema.ts';

import { durableStage } from './activity.ts';
import { makeTestClusterLayer } from './cluster.ts';
import { defineJob } from './job.ts';
import { UpstreamError } from './jobs.fixture.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';
import { defaultJobsPolicy } from './policy.ts';
import { clusterPruneTargetsFor } from './scheduler.ts';
import { testRuntimeLayer } from './test-runtime.fixture.ts';
import {
  JobOutcomeSchema,
  JobWorkflowPayload as JobWorkflowPayloadSchema,
  makeJobWorkflow,
  makeJobWorkflowLayer,
} from './workflow.ts';

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
 * One persisted Cluster message, reduced to the parts that must never change.
 */
type JournalEntry = {
  readonly entityType: string;
  readonly shardGroup: string;
  readonly tag: string;
  readonly name: string | undefined;
};

/**
 * The persisted envelope fields this test pins.
 */
const EnvelopeShape = Schema.Struct({
  address: Schema.Struct({
    entityType: Schema.String,
    shardId: Schema.Struct({ group: Schema.String }),
  }),
  tag: Schema.String,
  payload: Schema.optional(Schema.Unknown),
});

/**
 * Payload of an activity message: its name.
 */
const isNamedPayload = Schema.is(Schema.Struct({ name: Schema.String }));

/**
 * Reads the entity type, shard group, tag and activity name of an encoded envelope.
 */
const toJournalEntry = (envelope: unknown): JournalEntry => {
  const { address, tag, payload } = Schema.decodeUnknownSync(EnvelopeShape)(envelope);
  return {
    entityType: address.entityType,
    shardGroup: address.shardId.group,
    tag,
    name: isNamedPayload(payload) ? payload.name : undefined,
  };
};

/**
 * Runs `job`'s workflow with `payload` on the in-memory cluster, advancing the test clock in `step`
 * increments until it finishes, and returns the outcome, the executionId and the persisted
 * journal.
 */
const runWorkflow = <Name extends string, P, S, E, PE>(
  job: JobDefinition<Name, P, S, E, never, PE>,
  payload: JobWorkflowPayload,
  seed: (
    runs: JobRuns['Service'],
    executionId: string,
  ) => Effect.Effect<void, SqlError.SqlError> = () => Effect.void,
  step: Duration.Input = '1 minute',
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runs = yield* makeJobRunsMemory();
        const cluster = makeTestClusterLayer([job]);
        const env = Layer.mergeAll(
          cluster,
          runs.layer,
          makeJobWorkflowLayer(job).pipe(Layer.provide(runs.layer), Layer.provide(cluster)),
        );
        return yield* Effect.gen(function* () {
          const workflow = makeJobWorkflow(job);
          yield* seed(yield* JobRuns, yield* workflow.executionId(payload));
          const fiber = yield* workflow.execute(payload).pipe(Effect.forkChild);
          // Durable sleeps start whenever the workflow reaches them, so keep moving the clock.
          for (let round = 0; round < 500 && fiber.pollUnsafe() === undefined; round += 1) {
            yield* Effect.yieldNow;
            yield* TestClock.adjust(step);
          }

          const outcome = yield* Fiber.join(fiber);
          const executionId = yield* workflow.executionId(payload);
          const driver = yield* MessageStorage.MemoryDriver;
          return { outcome, executionId, journal: driver.journal.map(toJournalEntry) };
        }).pipe(Effect.provide(env));
      }),
    ).pipe(Effect.provide(testRuntimeLayer)),
  );

const StageCount = Schema.Struct({ count: Schema.Int });

/**
 * A one-stage job with `cadence` that always completes.
 */
const makeJob = (name: string, cadence: JobCadence) =>
  defineJob({
    name,
    description: 'Pins persisted names',
    cadence,
    params: { schema: Schema.Struct({}), defaults: {} },
    stats: StageCount,
    run: () => Effect.succeed({ count: 1 }),
  });
/**
 * Activity and clock names of `journal`, in order.
 */
const names = (journal: ReadonlyArray<JournalEntry>) =>
  journal.flatMap((entry) => (entry.name === undefined ? [] : [entry.name]));
/**
 * A manual claim of `(job, runKey)`, optionally as workflow `executionId`.
 */
const claim = (runs: JobRuns['Service'], job: string, runKey: string, executionId?: string) =>
  runs.claim({ job, runKey, trigger: 'manual', force: false, params: {}, executionId });
/**
 * Settle fields shared by the seeded runs.
 */
const settleBase = { trigger: 'manual', annotations: Context.empty() } as const;

describe('persisted names and shapes', () => {
  it('keeps the workflow entity, shard group, executionId and activity names of a completed run', async () => {
    const job = defineJob({
      name: 'wire-job',
      description: 'Pins persisted names',
      cadence: { _tag: 'Daily' },
      params: { schema: Schema.Struct({}), defaults: {} },
      stats: StageCount,
      run: () =>
        durableStage({
          name: 'stage-a',
          schema: StageCount,
          execute: Effect.succeed({ count: 1 }),
        }),
    });
    const result = await runWorkflow(job, scheduled('2026-11-25'));
    expect(result.outcome.status).toBe('completed');
    // SHA-256 of the idempotency key `2026-11-25` under the workflow name: stored message ids.
    expect(result.executionId).toBe('4f8f408e4912b18306b9c351f254209b');
    expect(result.journal).toEqual([
      { entityType: 'Workflow/wire-job', shardGroup: 'wire-job', tag: 'run', name: undefined },
      {
        entityType: 'Workflow/wire-job',
        shardGroup: 'wire-job',
        tag: 'activity',
        name: 'claim-round-1',
      },
      { entityType: 'Workflow/wire-job', shardGroup: 'wire-job', tag: 'activity', name: 'stage-a' },
      {
        entityType: 'Workflow/wire-job',
        shardGroup: 'wire-job',
        tag: 'activity',
        name: 'settle-completed-1',
      },
    ]);
  });

  it('keeps the activity and clock names of a failed scheduled round and its retry', async () => {
    let calls = 0;
    const job = defineJob({
      name: 'wire-retry',
      description: 'Fails once, then completes',
      cadence: { _tag: 'Daily', checkEvery: '1 hour' },
      params: { schema: Schema.Struct({}), defaults: {} },
      stats: StageCount,
      run: () =>
        durableStage({
          name: 'stage-a',
          schema: StageCount,
          execute: Effect.suspend(() => {
            calls += 1;
            return calls === 1
              ? Effect.fail(new UpstreamError({ status: 503, message: 'down' }))
              : Effect.succeed({ count: 2 });
          }),
        }),
    });
    const result = await runWorkflow(job, scheduled('2026-11-25'));
    expect(result.outcome.status).toBe('completed');
    expect(
      result.journal.flatMap((entry) => (entry.name === undefined ? [] : [entry.name])),
    ).toEqual([
      'claim-round-1',
      'stage-a',
      'settle-failed-1',
      'retry-backoff-1',
      'DurableClock/retry-backoff-1',
      'claim-round-2',
      'stage-a',
      'settle-completed-2',
    ]);
  });

  it('keeps the activity and clock names of skipped, busy, backed-off and crash-capped runs', async () => {
    // A completed key: the scheduled run is skipped.
    const done = makeJob('wire-done', { _tag: 'Every', interval: '5 minutes' });
    const skipped = await runWorkflow(done, scheduled('2026-11-25T00:00Z'), (runs) =>
      Effect.gen(function* () {
        const owned = yield* claim(runs, 'wire-done', '2026-11-25T00:00Z');
        if (owned._tag === 'Acquired') {
          yield* runs.settle(
            { job: 'wire-done', runKey: '2026-11-25T00:00Z', token: owned.token, attempt: 1 },
            { _tag: 'Completed', stats: {}, ...settleBase },
          );
        }
      }),
    );
    expect(skipped.outcome).toMatchObject({
      status: 'skipped',
      reason: { _tag: 'AlreadyCompleted' },
    });
    expect(names(skipped.journal)).toEqual(['claim-round-1', 'settle-skipped-1']);

    // A key another run holds: the scheduled Daily run waits durably until that lease expires.
    const busy = makeJob('wire-busy', { _tag: 'Daily' });
    const waited = await runWorkflow(busy, scheduled('2026-11-25'), (runs) =>
      claim(runs, 'wire-busy', '2026-11-25').pipe(Effect.asVoid),
    );
    expect(waited.outcome.status).toBe('completed');
    expect(names(waited.journal).slice(0, 4)).toEqual([
      'claim-round-1',
      'DurableClock/busy-backoff-1',
      'claim-round-2',
      'DurableClock/busy-backoff-2',
    ]);
    expect(names(waited.journal).at(-1)).toMatch(/^settle-completed-\d+$/);

    // A failed Once key in backoff: the scheduled run reads the clock, sleeps, then runs.
    const once = makeJob('wire-once', { _tag: 'Once', start: 'automatic' });
    const backedOff = await runWorkflow(once, scheduled('once'), (runs) =>
      Effect.gen(function* () {
        const owned = yield* claim(runs, 'wire-once', 'once');
        if (owned._tag === 'Acquired') {
          yield* runs.settle(
            { job: 'wire-once', runKey: 'once', token: owned.token, attempt: 1 },
            { _tag: 'Failed', errorMessage: 'x', reason: 'error', cadence: 'Once', ...settleBase },
          );
        }
      }),
    );
    expect(backedOff.outcome.status).toBe('completed');
    expect(names(backedOff.journal)).toEqual([
      'claim-round-1',
      'retry-later-clock-1',
      'retry-later-wait-1',
      'DurableClock/retry-later-wait-1',
      'claim-round-2',
      'settle-completed-2',
    ]);

    // The same execution resumed its own running row too often: it settles at the crash cap.
    const crashy = makeJob('wire-crash', { _tag: 'Every', interval: '5 minutes' });
    const capped = await runWorkflow(crashy, scheduled('2026-11-25T00:00Z'), (runs, executionId) =>
      Effect.forEach([1, 2, 3], () => claim(runs, 'wire-crash', '2026-11-25T00:00Z', executionId), {
        discard: true,
      }),
    );
    expect(capped.outcome).toMatchObject({ status: 'failed', errorTag: 'MaxAttemptsExceeded' });
    expect(names(capped.journal)).toEqual(['claim-round-1', 'settle-crash-cap-1']);
  });

  it('keeps the activity name of a run with invalid params', async () => {
    const job = defineJob({
      name: 'wire-params',
      description: 'Needs a param',
      cadence: { _tag: 'Every', interval: '5 minutes' },
      params: { schema: Schema.Struct({ limit: Schema.Int }), defaults: {} },
      stats: StageCount,
      run: ({ params }) => Effect.succeed({ count: params.limit }),
    });
    const invalid = await runWorkflow(job, scheduled('2026-11-25T00:00Z'));
    expect(invalid.outcome).toMatchObject({ status: 'failed', errorTag: 'InvalidJobParams' });
    expect(invalid.journal.map((entry) => entry.name)).toEqual([
      undefined,
      'settle-invalid-params',
    ]);
  });

  it('keeps the field names of payloads and outcomes', () => {
    expect(Object.keys(scheduled('once'))).toEqual([
      'runKey',
      'trigger',
      'params',
      'force',
      'requestId',
      'attemptNonce',
    ]);
    const encode = Schema.encodeUnknownSync(JobOutcomeSchema);
    expect(
      Object.keys(
        encode({
          job: 'j',
          runKey: 'once',
          trigger: 'api',
          elapsedMs: 1,
          status: 'failed',
          attempt: 1,
          errorMessage: 'x',
          isDefect: false,
          interrupted: false,
          errorTag: 'T',
          retryAfterSeconds: 3,
        }),
      ),
    ).toEqual([
      'job',
      'runKey',
      'trigger',
      'elapsedMs',
      'status',
      'attempt',
      'errorMessage',
      'isDefect',
      'interrupted',
      'errorTag',
      'retryAfterSeconds',
    ]);
  });

  it('keeps the runAt field of a delayed payload and the clock it waits on', async () => {
    const runAt = '1970-01-01T00:10:00.000Z';
    const delayed: JobWorkflowPayload = { ...scheduled('k'), runAt };
    expect(Object.keys(Schema.encodeUnknownSync(JobWorkflowPayloadSchema)(delayed))).toEqual([
      'runKey',
      'trigger',
      'params',
      'force',
      'requestId',
      'attemptNonce',
      'runAt',
    ]);
    const stored = {
      runKey: 'k',
      trigger: 'schedule',
      params: {},
      force: false,
      requestId: null,
      attemptNonce: null,
      runAt,
    };
    expect(Schema.decodeUnknownSync(JobWorkflowPayloadSchema)(stored)).toEqual(delayed);

    const job = makeJob('wire-delayed', { _tag: 'OnDemand' });
    const result = await runWorkflow(job, delayed);
    expect(result.outcome.status).toBe('completed');
    expect(names(result.journal)).toEqual([
      'run-at-600000',
      'DurableClock/run-at-600000',
      'claim-round-1',
      'settle-completed-1',
    ]);
  });

  it('keeps the idempotency key of nonce runs and the Cluster entity types it prunes', async () => {
    const workflow = makeJobWorkflow({ name: 'wire-job' });
    const withNonce = await Effect.runPromise(
      workflow.executionId({ ...scheduled('2026-11-25'), attemptNonce: 'n-1' }),
    );
    const sameKey = await Effect.runPromise(
      makeJobWorkflow({ name: 'wire-job' }).executionId({
        ...scheduled('2026-11-25:n-1'),
        trigger: 'api',
      }),
    );
    expect(withNonce).toBe(sameKey);

    const targets = clusterPruneTargetsFor(
      [
        { name: 'every-job', cadence: { _tag: 'Every', interval: '5 minutes' } },
        { name: 'daily-job', cadence: { _tag: 'Daily' } },
        { name: 'once-job', cadence: { _tag: 'Once', start: 'automatic' } },
      ],
      defaultJobsPolicy,
    );
    expect(targets.map((target) => target.entityType)).toEqual([
      'Workflow/every-job',
      'ClusterCron/every-job',
      'Workflow/daily-job',
      'ClusterCron/daily-job',
      'Workflow/-/DurableClock',
    ]);
  });
});
