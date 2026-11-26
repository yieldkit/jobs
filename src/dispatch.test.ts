import type { Duration } from 'effect';
import type { WorkflowEngine } from 'effect/workflow';

import { Effect, Layer, Schema } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { AnyJobDefinition } from './job.ts';
import type { JobRequests } from './requests.ts';

import { makeTestClusterLayer } from './cluster.ts';
import { dispatchApiJobWorkflow, dispatchJob } from './dispatch.ts';
import { defineJob } from './job.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { makeJobRequestsMemory } from './requests-memory.ts';
import { makeAnyJobWorkflowLayer } from './workflow.ts';

/**
 * An `OnDemand` job that succeeds at once.
 */
const mailer = defineJob({
  name: 'mailer',
  description: 'mailer',
  cadence: { _tag: 'OnDemand' },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Schema.Struct({}),
  run: () => Effect.succeed({}),
});

/**
 * An `Every` job whose `limit` an API caller may set.
 */
const syncOrders = defineJob({
  name: 'sync-orders',
  description: 'sync-orders',
  cadence: { _tag: 'Every', interval: '5 minutes' },
  params: {
    schema: Schema.Struct({ limit: Schema.Int }),
    defaults: { limit: 10 },
    apiParams: ['limit'],
  },
  stats: Schema.Struct({}),
  run: () => Effect.succeed({}),
});

/**
 * Runs `dispatch` on an in-memory Cluster that has a handler for `job`, under `TestClock`, and
 * returns its error `_tag` or `dispatched`, plus the params of the request rows it left.
 */
const outcomeOf = <E extends { readonly _tag: string }>(
  job: AnyJobDefinition<never>,
  dispatch: Effect.Effect<
    unknown,
    E,
    WorkflowEngine.WorkflowEngine | JobRequests | SecureRandom.SecureRandom
  >,
) =>
  Effect.gen(function* () {
    const runs = yield* makeJobRunsMemory();
    const requests = yield* makeJobRequestsMemory();
    const cluster = makeTestClusterLayer([job]);
    const handler = makeAnyJobWorkflowLayer(job).pipe(
      Layer.provide([cluster, runs.layer, SecureRandom.layerWebCrypto]),
    );
    const result = yield* dispatch.pipe(
      Effect.match({ onFailure: (error) => error._tag, onSuccess: () => 'dispatched' }),
      Effect.provide(Layer.mergeAll(cluster, handler, requests.layer, SecureRandom.layerWebCrypto)),
    );
    return { result, requests: requests.rows().map((row) => row.params) };
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise);

describe('dispatchJob', () => {
  it('rejects a delay holding a number that is not finite', async () => {
    const invalid: ReadonlyArray<Duration.Input> = [
      { minutes: Number.NaN },
      { seconds: Number.POSITIVE_INFINITY },
      [Number.NaN, 0],
    ];
    const outcomes = await Promise.all(
      [...invalid, { minutes: 1 }].map((delay) =>
        outcomeOf(mailer, dispatchJob(mailer, { runKey: 'k', delay })),
      ),
    );
    expect(outcomes.map((outcome) => outcome.result)).toEqual([
      ...invalid.map(() => 'InvalidDispatch'),
      'dispatched',
    ]);
  });
});

describe('dispatchApiJobWorkflow', () => {
  it('fails with InvalidJobParams before recording a request when the params do not decode', async () => {
    const outcome = await outcomeOf(
      syncOrders,
      dispatchApiJobWorkflow(syncOrders, { params: { limit: 'abc' }, force: false }),
    );
    expect(outcome).toEqual({ result: 'InvalidJobParams', requests: [] });
  });

  it('records and dispatches a request whose params decode', async () => {
    const outcome = await outcomeOf(
      syncOrders,
      dispatchApiJobWorkflow(syncOrders, { params: { limit: 7 }, force: false }),
    );
    expect(outcome).toEqual({ result: 'dispatched', requests: [{ limit: 7 }] });
  });
});
