# Testing

`@yieldkit/jobs/testing` replaces PostgreSQL and Cluster with in-memory versions. Time comes from the Effect `Clock`, so `TestClock` drives leases, backoff and durable sleeps.

## The ledger and request store

```ts
import { defineJob, runJob } from '@yieldkit/jobs';
import { makeJobRunsMemory } from '@yieldkit/jobs/testing';
import { Effect, Schema } from 'effect';

const job = defineJob({
  name: 'count',
  description: 'Counts to three',
  cadence: { _tag: 'Daily' },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Schema.Struct({ count: Schema.Int }),
  run: () => Effect.succeed({ count: 3 }),
});

const test = Effect.gen(function* () {
  const ledger = yield* makeJobRunsMemory({ policy: { lease: { duration: '20 seconds' } } });
  const outcome = yield* runJob(job, { trigger: 'schedule' }).pipe(Effect.provide(ledger.layer));
  return { outcome, rows: ledger.rows(), settles: ledger.settles() };
});
```

| Factory                                     | Returns                                  |
| ------------------------------------------- | ---------------------------------------- |
| `makeJobRunsMemory({ onSettle?, policy? })` | `layer`, `rows()` and `settles()`        |
| `makeJobRequestsMemory({ findRun? })`       | `layer`, a `service` handle and `rows()` |

The memory ledger has the SQL ledger's claim, lease, fence, backoff, query and prune rules. Its `layer` also provides the `SecureRandom` that `runJob` needs. `settles()` lists each successful settle with its `RunResult` and the `JobSettled` event the hook received.

## Workflows on an in-memory cluster

`makeTestClusterLayer(allJobs, only?)` runs workflows, crons and singletons without sockets, and provides a `ShardingConfig` with the jobs' groups. Run the scheduler on it to test cadences:

```ts
import { defineJob, makeSchedulerLayer } from '@yieldkit/jobs';
import {
  makeJobRequestsMemory,
  makeJobRunsMemory,
  makeTestClusterLayer,
} from '@yieldkit/jobs/testing';
import { Effect, Layer, Schema } from 'effect';
import { TestClock } from 'effect/testing';

const report = defineJob({
  name: 'report',
  description: 'Builds a report',
  cadence: { _tag: 'Every', interval: '5 minutes' },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Schema.Struct({}),
  run: () => Effect.succeed({}),
});

const test = Effect.gen(function* () {
  const ledger = yield* makeJobRunsMemory();
  const requests = yield* makeJobRequestsMemory();
  const cluster = makeTestClusterLayer([report]);
  yield* Layer.build(
    makeSchedulerLayer([report]).pipe(Layer.provide([cluster, ledger.layer, requests.layer])),
  );
  // Runs finish after real async work the test clock does not wait for, and a tick handled after
  // the clock passed the next one skips it, so move one tick at a time and wait for its row.
  const rowsOnceThere = (count: number) =>
    Effect.yieldNow.pipe(
      Effect.map(() => ledger.rows()),
      Effect.repeat({ until: (rows) => rows.length >= count }),
    );
  yield* rowsOnceThere(1);
  yield* TestClock.adjust('5 minutes');
  yield* rowsOnceThere(2);
  yield* TestClock.adjust('5 minutes');
  const rows = yield* rowsOnceThere(3);
  return rows.map((row) => row.runKey); // ['1970-01-01T00:00Z', '...T00:05Z', '...T00:10Z']
}).pipe(Effect.scoped, Effect.provide(TestClock.layer()));
```

To drive one workflow directly:

<!-- prettier-ignore -->
```ts
import { defineJob, makeJobWorkflow, makeJobWorkflowLayer } from '@yieldkit/jobs';
import { makeJobRunsMemory, makeTestClusterLayer } from '@yieldkit/jobs/testing';
import { Effect, Fiber, Layer, Schema } from 'effect';
import { TestClock } from 'effect/testing';

const job = defineJob({
  name: 'report',
  description: 'Builds a report',
  cadence: { _tag: 'Daily' },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Schema.Struct({}),
  run: () => Effect.succeed({}),
});

const test = Effect.gen(function* () {
  const ledger = yield* makeJobRunsMemory();
  const cluster = makeTestClusterLayer([job]);
  const env = Layer.mergeAll(
    cluster,
    ledger.layer,
    makeJobWorkflowLayer(job).pipe(Layer.provide([ledger.layer, cluster])),
  );
  return yield* Effect.gen(function* () {
    const fiber = yield* makeJobWorkflow(job)
      .execute({ runKey: '2026-11-25', trigger: 'schedule', params: {}, force: false, requestId: null, attemptNonce: null })
      .pipe(Effect.forkChild);
    // The workflow reaches each durable sleep through real async work, so keep advancing.
    while (fiber.pollUnsafe() === undefined) {
      yield* TestClock.adjust('1 minute');
    }
    return yield* Fiber.join(fiber);
  }).pipe(Effect.provide(env));
}).pipe(Effect.scoped, Effect.provide(TestClock.layer()));
```

A workflow that retries needs nothing more: the loop steps through each backoff sleep as the workflow reaches it.

## Against PostgreSQL

`startPostgresContainer` from `@yieldkit/pg/testing` starts a database (it needs `@testcontainers/postgresql`). `JobStorage.layerWith()` migrates the library's tables on build, then provides the storage:

```ts
import { JobStorage } from '@yieldkit/jobs';
import { makePgLayer } from '@yieldkit/pg';
import { startPostgresContainer } from '@yieldkit/pg/testing';
import { Layer } from 'effect';

const container = await startPostgresContainer();

export const StorageLive = JobStorage.layerWith().pipe(
  Layer.provide(makePgLayer({ url: container.connectionUri, applicationName: 'test' })),
);
```

For an end-to-end test on a real Cluster, migrate your Cluster's storage the way your deploy step does, for example with Effect's `SqlMessageStorage.layerWith` in the test.

## Limits

- The in-memory cluster runs internal timers, so advance the clock in minutes or hours rather than days.
- It gets slower as its journal grows, because Effect's memory driver scans it. Use it for behaviour tests, not soak tests.
