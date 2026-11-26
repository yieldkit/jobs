import type { PostgresContainer } from '@yieldkit/pg/testing';
import type { Redacted } from 'effect';

import { Context, Effect, Layer, Schema } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { SqlClient } from 'effect/sql';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import {
  appPool,
  migrateJobs,
  quietLogger,
  startIntegrationPostgres,
} from './integration.fixture.ts';
import { JobRuns } from './ledger.ts';
import { JobRequests } from './requests.ts';
import { layerStorage } from './storage.ts';

const UNKNOWN_UUID = '00000000-0000-0000-0000-000000000000';

/**
 * Ledger and request store on an application pool, with the pool itself and `SecureRandom`.
 */
const buildTestLayer = (url: Redacted.Redacted) =>
  Layer.mergeAll(
    appPool(url),
    layerStorage().pipe(Layer.provide(appPool(url))),
    SecureRandom.layerWebCrypto,
    quietLogger,
  );

let postgres: PostgresContainer;
let testLayer: ReturnType<typeof buildTestLayer>;

const runDb = <A, E>(
  effect: Effect.Effect<A, E, Layer.Success<ReturnType<typeof buildTestLayer>>>,
): Promise<A> => Effect.runPromise(effect.pipe(Effect.provide(testLayer), Effect.scoped));

beforeAll(async () => {
  postgres = await startIntegrationPostgres();
  await migrateJobs(postgres.connectionUri);
  testLayer = buildTestLayer(postgres.connectionUri);
});

afterAll(async () => {
  await postgres.stop();
});

describe('JobRequests on PostgreSQL', () => {
  it('creates a request linked to executionId, upserts started state idempotently, and joins job_runs in getStatus', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const requests = yield* JobRequests;
        const runs = yield* JobRuns;
        const random = yield* SecureRandom.SecureRandom;
        const job = 'status-join-job';
        const runKey = '2026-11-24';
        const requestId = yield* random.uuidV4();
        const executionId = `exec-${requestId}`;
        const created = yield* requests.create({
          id: requestId,
          job,
          params: { maxItems: 1 },
          force: true,
          executionId,
        });
        const pendingStatus = yield* requests.getStatus(job, requestId);

        yield* requests.upsertStarted({
          id: requestId,
          job,
          params: { maxItems: 1 },
          force: true,
          executionId,
          runKey,
        });

        const claim = yield* runs.claim({
          job,
          runKey,
          trigger: 'api',
          force: true,
          params: { maxItems: 1 },
          executionId,
        });
        if (claim._tag !== 'Acquired') {
          return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
        }

        yield* runs.settle(
          { job, runKey, token: claim.token, attempt: claim.attempt },
          {
            _tag: 'Completed',
            stats: { processed: 7 },
            trigger: 'api',
            annotations: Context.empty(),
          },
        );
        const finished = yield* requests.markFinished({
          id: requestId,
          runKey,
          outcome: {
            job,
            runKey,
            trigger: 'api',
            status: 'completed',
            stats: { processed: 7 },
          },
        });
        const secondFinish = yield* requests.markFinished({
          id: requestId,
          runKey,
          outcome: { status: 'overwritten' },
        });

        const finishedStatus = yield* requests.getStatus(job, requestId);
        const wrongJobStatus = yield* requests.getStatus('different-job', requestId);
        const missingStatus = yield* requests.getStatus(job, UNKNOWN_UUID);
        const malformedStatus = yield* requests.getStatus(job, 'not-a-valid-uuid');
        return {
          created,
          executionId,
          pendingStatus,
          finished,
          secondFinish,
          finishedStatus,
          wrongJobStatus,
          missingStatus,
          malformedStatus,
        };
      }),
    );
    expect(result.created.executionId).toBe(result.executionId);
    expect(result.pendingStatus?.request.id).toBe(result.created.id);
    expect(result.pendingStatus?.request.pickedAt).toBeNull();
    expect(result.pendingStatus?.run).toBeNull();
    expect(result.finished).toBe(true);
    expect(result.secondFinish).toBe(false);
    expect(result.finishedStatus?.request.pickedAt).toBeInstanceOf(Date);
    expect(result.finishedStatus?.request.finishedAt).toBeInstanceOf(Date);
    expect(result.finishedStatus?.request.runKey).toBe('2026-11-24');
    expect(result.finishedStatus?.request.executionId).toBe(result.executionId);
    expect(result.finishedStatus?.run).toMatchObject({
      job: 'status-join-job',
      runKey: '2026-11-24',
      executionId: result.executionId,
      status: 'completed',
      trigger: 'api',
      stats: { processed: 7 },
    });
    expect(result.wrongJobStatus).toBeUndefined();
    expect(result.missingStatus).toBeUndefined();
    expect(result.malformedStatus).toBeUndefined();
  });

  it('self-heals via upsertStarted when the dispatcher crashed before creating the row and guards markDispatchFailed', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const requests = yield* JobRequests;
        const random = yield* SecureRandom.SecureRandom;
        const selfHealedId = yield* random.uuidV4();
        yield* requests.upsertStarted({
          id: selfHealedId,
          job: 'self-heal-job',
          params: { limit: 5 },
          force: false,
          executionId: 'exec-self-heal',
          runKey: '2026-11-25',
        });
        // A late dispatch-error handler must not overwrite a row already picked by a worker.
        yield* requests.markDispatchFailed({
          id: selfHealedId,
          job: 'self-heal-job',
          errorMessage: 'transient network timeout',
        });
        const selfHealedStatus = yield* requests.getStatus('self-heal-job', selfHealedId);

        const failedDispatchId = yield* random.uuidV4();
        yield* requests.create({
          id: failedDispatchId,
          job: 'failed-dispatch-job',
          params: {},
          force: false,
          executionId: 'exec-failed-dispatch',
        });
        yield* requests.markDispatchFailed({
          id: failedDispatchId,
          job: 'failed-dispatch-job',
          errorMessage: 'cluster unreachable',
        });
        const failedStatus = yield* requests.getStatus('failed-dispatch-job', failedDispatchId);
        return { selfHealedStatus, failedStatus };
      }),
    );
    expect(result.selfHealedStatus?.request.pickedAt).toBeInstanceOf(Date);
    expect(result.selfHealedStatus?.request.finishedAt).toBeNull();
    expect(result.failedStatus?.request.finishedAt).toBeInstanceOf(Date);
    expect(result.failedStatus?.request.outcome).toMatchObject({
      status: 'failed',
      errorTag: 'WorkflowDispatchError',
      errorMessage: 'cluster unreachable',
    });
  });

  it('reports no run for a request whose execution never held the key', async () => {
    const status = await runDb(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        const requests = yield* JobRequests;
        const random = yield* SecureRandom.SecureRandom;
        const job = 'foreign-run-job';
        const runKey = '2026-11-26';
        yield* runs.claim({
          job,
          runKey,
          trigger: 'schedule',
          force: false,
          params: { bucket: 'operator-only' },
          executionId: 'exec-schedule',
        });
        const id = yield* random.uuidV4();
        const request = { id, job, params: {}, force: false, executionId: 'exec-api' };
        yield* requests.create(request);
        yield* requests.upsertStarted({ ...request, runKey });
        yield* requests.markFinished({ id, runKey, outcome: { status: 'skipped' } });
        return yield* requests.getStatus(job, id);
      }),
    );
    expect(status?.request.runKey).toBe('2026-11-26');
    expect(status?.run).toBeNull();
  });

  it('lets the workflow finish a provisional dispatch failure it never picked up', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const requests = yield* JobRequests;
        const random = yield* SecureRandom.SecureRandom;
        const id = yield* random.uuidV4();
        const job = 'dispatch-failed-unpicked';
        yield* requests.create({ id, job, params: {}, force: false, executionId: 'exec-unpicked' });
        yield* requests.markDispatchFailed({ id, job, errorMessage: 'ack lost' });
        const finished = yield* requests.markFinished({
          id,
          runKey: null,
          outcome: { status: 'failed', errorTag: 'InvalidJobParams' },
        });
        const again = yield* requests.markFinished({
          id,
          runKey: null,
          outcome: { status: 'overwritten' },
        });
        const status = yield* requests.getStatus(job, id);
        return { finished, again, outcome: status?.request.outcome };
      }),
    );
    expect(result.finished).toBe(true);
    expect(result.again).toBe(false);
    expect(result.outcome).toEqual({ status: 'failed', errorTag: 'InvalidJobParams' });
  });

  it('clears a provisional dispatch failure when the workflow picks the request up', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const requests = yield* JobRequests;
        const random = yield* SecureRandom.SecureRandom;
        const id = yield* random.uuidV4();
        const job = 'dispatch-failed-job';
        const request = { id, job, params: {}, force: false, executionId: 'exec-lost-ack' };
        yield* requests.create(request);
        yield* requests.markDispatchFailed({ id, job, errorMessage: 'ack lost' });
        yield* requests.upsertStarted({ ...request, runKey: 'once' });
        const afterPickup = yield* requests.getStatus(job, id);
        const finished = yield* requests.markFinished({
          id,
          runKey: 'once',
          outcome: { status: 'failed', errorTag: 'WorkflowDispatchError' },
        });
        const again = yield* requests.markFinished({
          id,
          runKey: 'once',
          outcome: { status: 'overwritten' },
        });
        const status = yield* requests.getStatus(job, id);
        return {
          afterPickup: afterPickup?.request,
          finished,
          again,
          outcome: status?.request.outcome,
        };
      }),
    );
    expect(result.afterPickup).toMatchObject({ finishedAt: null, outcome: null });
    expect(result.afterPickup?.pickedAt).toBeInstanceOf(Date);
    expect(result.finished).toBe(true);
    expect(result.again).toBe(false);
    expect(result.outcome).toEqual({ status: 'failed', errorTag: 'WorkflowDispatchError' });
  });

  it('prunes finished requests older than the retention in batches and keeps unfinished ones', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const requests = yield* JobRequests;
        yield* sql`
          INSERT INTO public.job_requests (id, job, finished_at)
          VALUES
            ('11111111-1111-4111-8111-111111111111', 'prune-requests', NOW() - INTERVAL '40 days'),
            ('22222222-2222-4222-8222-222222222222', 'prune-requests', NOW() - INTERVAL '35 days'),
            ('33333333-3333-4333-8333-333333333333', 'prune-requests', NOW() - INTERVAL '1 day'),
            ('44444444-4444-4444-8444-444444444444', 'prune-requests', NULL)
        `;
        const first = yield* requests.prune('30 days', 1);
        const second = yield* requests.prune('30 days', 1);
        const third = yield* requests.prune('30 days', 1);
        const rows = yield* sql`
          SELECT id::text AS id FROM public.job_requests WHERE job = 'prune-requests' ORDER BY id
        `;
        const remaining = yield* Schema.decodeUnknownEffect(
          Schema.Array(Schema.Struct({ id: Schema.String })),
        )(rows);
        return { deleted: [first, second, third], remaining: remaining.map((row) => row.id) };
      }),
    );
    expect(result.deleted).toEqual([1, 1, 0]);
    expect(result.remaining).toEqual([
      '33333333-3333-4333-8333-333333333333',
      '44444444-4444-4444-8444-444444444444',
    ]);
  });
});
