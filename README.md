# @yieldkit/jobs

[![npm](https://img.shields.io/npm/v/@yieldkit/jobs)](https://www.npmjs.com/package/@yieldkit/jobs)
[![CI](https://github.com/yieldkit/jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/yieldkit/jobs/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/@yieldkit/jobs)](LICENSE)

Scheduled and on-demand jobs for Effect on PostgreSQL, on the Effect Cluster you already run. Each job runs as a durable Cluster workflow, and a run ledger table fences its runs: one ledger row and one scheduled workflow execution per job and run key.

Define a job once with `defineJob`, then run it on a schedule (`Every`, `Daily` at a time or in a window, `Cron` or `Once`), once per item under your own key (`OnDemand` with `dispatchJob`, optionally delayed), from an API (`dispatchApiJobWorkflow`) or from a script (`runJob`). Failed stages retry durably without repeating the ones that succeeded. Job code runs [at least once](docs/jobs.md#limits), not exactly once.

You own the Cluster: its tables, schema, prefix, transport, serialization and migrations. The library owns only its two tables, and its storage API has the same shape as Cluster's `SqlMessageStorage`: `JobStorage.migrations`, `layerMigrations`, `layerStorage`, `layerWith` and `layer`. Nothing creates tables at start-up.

## Install

```bash
pnpm add @yieldkit/jobs @yieldkit/pg @effect/sql-pg effect
```

`effect` and `@effect/sql-pg` 4.1.6 or a later 4.1 patch are peer dependencies; a new Effect minor needs a new release, since the package builds on Effect's unstable Cluster, Workflow and SQL modules. Your runner also needs a platform package such as `@effect/platform-node`. Runs on Node.js 22.18+, 24.11+ and 26+, and on Bun, against PostgreSQL 15 to 19.

| Import                   | Contents                                                                                   |
| ------------------------ | ------------------------------------------------------------------------------------------ |
| `@yieldkit/jobs`         | Job definitions, `runJob`, workflows, the scheduler, `JobStorage`, the shard-group helpers |
| `@yieldkit/jobs/testing` | In-memory ledger, request store and Cluster for unit tests                                 |

## Quick start

The four steps: set up your Cluster, run its migrations, run the library's migrations, then start the jobs engine on your Cluster.

<!-- prettier-ignore -->
```ts
import * as NodeClusterHttp from '@effect/platform-node/NodeClusterHttp';
import {
  assignedShardGroupsFor,
  availableShardGroupsFor,
  defineJob,
  JobStorage,
  makeSchedulerLayer,
} from '@yieldkit/jobs';
import { makePgLive } from '@yieldkit/pg';
import { Effect, Layer, Option, Schema } from 'effect';
import {
  ClusterWorkflowEngine,
  RunnerAddress,
  ShardingConfig,
  SqlMessageStorage,
  SqlRunnerStorage,
} from 'effect/cluster';
import { Digest, SecureRandom } from 'effect/crypto';

const syncOrders = defineJob({
  name: 'sync-orders',
  description: 'Copies new orders from the shop',
  cadence: { _tag: 'Every', interval: '5 minutes' },
  params: { schema: Schema.Struct({ limit: Schema.Int }), defaults: { limit: 100 }, apiParams: ['limit'] },
  stats: Schema.Struct({ copied: Schema.Int }),
  run: ({ params }) => Effect.succeed({ copied: params.limit }),
});

const jobs = [syncOrders] as const;

// One pool whose search_path holds your Cluster's tables and the library's.
const AppPg = makePgLive({ applicationName: 'worker', searchPath: 'app' });

// 1. Your Cluster: your table prefix, your shard groups plus the ones the jobs need.
const CLUSTER_PREFIX = 'app_cluster';
const shardingConfig = {
  // Where other nodes reach this worker. Set it explicitly: Effect would otherwise read the generic
  // HOST and PORT. One address per worker; see docs/deployment.md for several nodes.
  runnerAddress: Option.some(RunnerAddress.make('127.0.0.1', 34431)),
  availableShardGroups: [...availableShardGroupsFor(jobs)],
  assignedShardGroups: [...assignedShardGroupsFor(jobs, undefined)],
};
const ShardingConfigLive = ShardingConfig.layerFromEnv(shardingConfig);
const ClusterStorageLive = Layer.mergeAll(
  SqlMessageStorage.layerStorage({ prefix: CLUSTER_PREFIX }).pipe(
    Layer.provide(Digest.layerWebCrypto),
  ),
  SqlRunnerStorage.layerStorage({ prefix: CLUSTER_PREFIX }),
).pipe(Layer.provide([AppPg, ShardingConfigLive]));
const ClusterLive = ClusterWorkflowEngine.layer.pipe(
  Layer.provideMerge(NodeClusterHttp.layer({ transport: 'http', storage: 'byo', shardingConfig })),
  Layer.provideMerge(ClusterStorageLive),
  Layer.provideMerge(ShardingConfigLive),
);

// 2 and 4. Deploy step: your Cluster's migrations and the library's, each into its own history.
// Nothing else creates tables.
export const DeployLive = Layer.mergeAll(
  SqlMessageStorage.layerMigrations({ prefix: CLUSTER_PREFIX }),
  SqlRunnerStorage.layerMigrations({ prefix: CLUSTER_PREFIX }),
  JobStorage.layerMigrations(),
).pipe(Layer.provide(AppPg));

// 3. Worker process: the jobs engine on your Cluster.
export const WorkerLive = makeSchedulerLayer(jobs).pipe(
  Layer.provide([
    ClusterLive,
    JobStorage.layerStorage().pipe(Layer.provide(AppPg)),
    SecureRandom.layerWebCrypto,
  ]),
);
```

Run `DeployLive` once per release, then `WorkerLive` in as many processes as you like, each with its own reachable `runnerAddress`. Each 5-minute bucket runs once across all of them.

```ts
import * as NodeRuntime from '@effect/platform-node/NodeRuntime';

// deploy.ts: builds the layer, then exits
NodeRuntime.runMain(Layer.build(DeployLive).pipe(Effect.scoped));
// worker.ts: runs until SIGTERM
NodeRuntime.runMain(Layer.launch(WorkerLive));
```

[Deployment](docs/deployment.md) covers several nodes, addresses and failover.

`makeSchedulerLayer` fails at start-up with `ClusterConfigError` when your `ShardingConfig` lacks or misassigns a jobs group, and with `JobsNotMigratedError` before `JobStorage.layerMigrations` has run. See [Workers and clients](docs/workers-and-clients.md).

## Common tasks

The samples import `syncOrders` and `allJobs`, the list of every job, from your own `./jobs.ts`.

### Split work into durable stages

`durableStage` checkpoints a stage's result. When a later stage fails, the retry reads finished stages from storage and runs only the failed one. Code outside a stage runs again on each retry.

```ts
import { defineJob, durableStage } from '@yieldkit/jobs';
import { Effect, Schema } from 'effect';

const Count = Schema.Struct({ count: Schema.Int });

export const nightlyReport = defineJob({
  name: 'nightly-report',
  description: 'Builds and mails the daily report',
  cadence: { _tag: 'Daily' }, // checked hourly; run key YYYY-MM-DD (UTC)
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Count,
  run: () =>
    Effect.gen(function* () {
      const rows = yield* durableStage({
        name: 'collect',
        schema: Count,
        execute: Effect.succeed({ count: 42 }),
      });
      return yield* durableStage({ name: 'mail', schema: Count, execute: Effect.succeed(rows) });
    }),
});
```

### Start a job from an API

`dispatchApiJobWorkflow` records a request row and hands the workflow to your Cluster, which runs it on a worker even if the API process exits. The API runs a client-only Cluster: no runner address, so it claims no shards.

<!-- prettier-ignore -->
```ts
import * as NodeClusterHttp from '@effect/platform-node/NodeClusterHttp';
import { availableShardGroupsFor, checkShardingConfig, dispatchApiJobWorkflow, JobStorage } from '@yieldkit/jobs';
import { makePgLive } from '@yieldkit/pg';
import { Effect, Layer, Option } from 'effect';
import { ClusterWorkflowEngine, ShardingConfig, SqlMessageStorage, SqlRunnerStorage } from 'effect/cluster';
import { Digest, SecureRandom } from 'effect/crypto';

import { allJobs, syncOrders } from './jobs.ts';

const AppPg = makePgLive({ applicationName: 'api', searchPath: 'app' });
const shardingConfig = {
  runnerAddress: Option.none(),                           // not a runner: claims no shards
  availableShardGroups: [...availableShardGroupsFor(allJobs)],
  assignedShardGroups: [],
  shardLockDisableAdvisory: true,
};
const ShardingConfigLive = ShardingConfig.layerFromEnv(shardingConfig);

const ApiLive = Layer.mergeAll(
  ClusterWorkflowEngine.layer.pipe(
    Layer.provideMerge(
      NodeClusterHttp.layer({ transport: 'http', clientOnly: true, storage: 'byo', shardingConfig }),
    ),
    Layer.provideMerge(
      Layer.mergeAll(
        SqlMessageStorage.layerStorage({ prefix: 'app_cluster' }).pipe(Layer.provide(Digest.layerWebCrypto)),
        SqlRunnerStorage.layerStorage({ prefix: 'app_cluster' }),
      ).pipe(Layer.provide([AppPg, ShardingConfigLive])),
    ),
  ),
  Layer.effectDiscard(checkShardingConfig(allJobs)).pipe(Layer.provide(ShardingConfigLive)),
  JobStorage.layerStorage().pipe(Layer.provide(AppPg)),
  SecureRandom.layerWebCrypto,
);

export const startSync = dispatchApiJobWorkflow(syncOrders, {
  params: { limit: 10 },
  force: false,
}).pipe(
  Effect.provide(ApiLive), // { requestId, executionId }
);
```

Only params listed in the job's `apiParams` are kept. The rest are decoded before anything is recorded; a value that does not decode fails with `InvalidJobParams`. Your API still has to [authenticate callers and decide `force`](docs/workers-and-clients.md#limits).

### Run a job once per item

An `OnDemand` job runs once per key your code chooses, such as one email per user. Dispatching a key again joins the run already started under it, and a failed run retries with backoff.

<!-- prettier-ignore -->
```ts
import { defineJob, dispatchJob } from '@yieldkit/jobs';
import { Effect, Schema } from 'effect';

export const sendEmail = defineJob({
  name: 'send-email',
  description: 'Sends one email',
  cadence: { _tag: 'OnDemand' },
  params: { schema: Schema.Struct({ to: Schema.String }), defaults: {} },
  stats: Schema.Struct({ sent: Schema.Boolean }),
  run: () => Effect.succeed({ sent: true }),
});

// From your API, on the same client-only Cluster as above.
export const sendWelcome = (userId: string, to: string) =>
  dispatchJob(sendEmail, { runKey: `welcome:${userId}`, params: { to }, delay: '10 minutes' });
```

[Scheduling recipes](docs/scheduling.md) covers `OnDemand`, `Daily` with `at`, `window` and `until`, and `Cron`.

### Run a job from a script

`runJob` claims the run key, runs the job and settles the ledger without Cluster. A failing job yields a `failed` outcome, not an error.

```ts
import { JobStorage, runJob } from '@yieldkit/jobs';
import { makePgLive } from '@yieldkit/pg';
import { Effect, Layer } from 'effect';
import { SecureRandom } from 'effect/crypto';

import { syncOrders } from './jobs.ts';

export const backfill = runJob(syncOrders, { trigger: 'manual', params: { limit: 5000 } }).pipe(
  Effect.provide([
    JobStorage.layerStorage().pipe(Layer.provide(makePgLive())),
    SecureRandom.layerWebCrypto,
  ]),
);
```

### Record something in the same transaction as the settle

<!-- prettier-ignore -->
```ts
import { JobStorage } from '@yieldkit/jobs';
import { SqlClient } from 'effect/sql';

const StorageLive = JobStorage.layerStorage({
  onSettle: (event) =>
    SqlClient.SqlClient.use(
      (sql) => sql`INSERT INTO job_audit (job, outcome) VALUES (${event.job}, ${event.outcome._tag})`,
    ),
});
```

The [hook](docs/storage.md#the-settle-hook) runs once per successful settle, in a savepoint of the settle transaction. If it fails, its writes roll back, `job.settle_hook_failed` is logged, and the settle still commits.

### Test a job without a database

```ts
import { defineJob, runJob } from '@yieldkit/jobs';
import { makeJobRunsMemory } from '@yieldkit/jobs/testing';
import { Effect, Schema } from 'effect';

const greet = defineJob({
  name: 'greet',
  description: 'Says hello once',
  cadence: { _tag: 'Once', start: 'manual' },
  params: { schema: Schema.Struct({ name: Schema.String }), defaults: { name: 'world' } },
  stats: Schema.Struct({ greeted: Schema.String }),
  run: ({ params }) => Effect.succeed({ greeted: params.name }),
});

const outcome = Effect.gen(function* () {
  const ledger = yield* makeJobRunsMemory();
  return yield* runJob(greet, { trigger: 'manual' }).pipe(Effect.provide(ledger.layer));
});
// { job: 'greet', runKey: 'once', trigger: 'manual', status: 'completed', attempt: 1, ... }
```

## Documentation

| Page                                               | Covers                                                                       |
| -------------------------------------------------- | ---------------------------------------------------------------------------- |
| [Jobs](docs/jobs.md)                               | `defineJob`, cadences and run keys, params, stages, `runJob`, outcomes       |
| [Scheduling recipes](docs/scheduling.md)           | Every N minutes, daily at a time or in a window, cron, one-off jobs per item |
| [Workers and clients](docs/workers-and-clients.md) | Your Cluster, shard groups, the scheduler, API dispatch                      |
| [Storage and the settle hook](docs/storage.md)     | `JobStorage` layers, table options, `onSettle`, the ledger                   |
| [Migrations](docs/migrations.md)                   | `JobStorage.layerMigrations` and `migrations`, grants, adoption              |
| [Policy](docs/policy.md)                           | Leases, retries, retention and maintenance, with defaults                    |
| [Logs and metrics](docs/logs-and-metrics.md)       | Log events, metric names, error types                                        |
| [Deployment](docs/deployment.md)                   | Several workers, addresses and ports, failover, Kubernetes                   |
| [Testing](docs/testing.md)                         | The in-memory ledger, request store and Cluster                              |

## Development

See [CONTRIBUTING.md](https://github.com/yieldkit/jobs/blob/main/CONTRIBUTING.md). `pnpm test` runs the unit tests. `pnpm test:integration` starts PostgreSQL with Testcontainers, so it needs Docker.

## License

[MIT](LICENSE)
