# Deployment: several workers and high availability

Run as many workers as you like against one PostgreSQL database. They form one Effect Cluster: PostgreSQL holds the membership, shard locks and messages, and workers call each other over HTTP only to deliver work sooner. Most of this page is Effect Cluster 4.1.6 behaviour; the library adds the run ledger.

## The moving parts

| Process | Cluster role                       | Address                                       | Needs to reach                 |
| ------- | ---------------------------------- | --------------------------------------------- | ------------------------------ |
| Worker  | Runner: owns shards, runs jobs     | One advertised `host:port`, unique per worker | PostgreSQL, every other worker |
| API     | Client only: stores and dispatches | None (`runnerAddress: Option.none()`)         | PostgreSQL, every worker       |
| Deploy  | Not part of the Cluster            | None                                          | PostgreSQL                     |

- **Membership:** a worker registers its advertised address in `<prefix>_runners` and counts as a member while its heartbeat is newer than `shardLockExpiration` (35 s).
- **Ownership:** each group's `shardsPerGroup` shards are spread over the workers that claim it, each held by a PostgreSQL advisory lock. Two Clusters in one database need different prefixes.
- **Delivery:** a dispatched workflow is written to PostgreSQL, then the owner is woken over HTTP; if that fails, it finds the message on its next poll (10 s).
- **Fencing:** each run also claims its ledger row with a lease, so only one owner can settle it during a handover.

## Addresses and ports

Every worker has two addresses:

| Setting               | Meaning                                                          | Typical value                                   |
| --------------------- | ---------------------------------------------------------------- | ----------------------------------------------- |
| `runnerAddress`       | What other workers and API processes dial; the worker's identity | The node's private IP or DNS name, and the port |
| `runnerListenAddress` | What this worker binds; defaults to `runnerAddress`              | `0.0.0.0` and the same port                     |

The rules:

- **Advertise an address everyone can reach**: the private IP or a per-instance DNS name, never `0.0.0.0` or `localhost` unless every process shares one host. Listen on `0.0.0.0`.
- **One port per worker per host.** Containers with their own IP can share a port.
- **No load balancer** in front of the runner port: messages go to the owner of a specific shard.
- **Private network only**: see [Security](workers-and-clients.md#security).
- **The address is the worker's identity.** A restart at a new IP registers a new runner; the old entry goes at once after a clean stop, or after 35 s after a crash.

### Do not let `HOST` and `PORT` choose the address

`ShardingConfig.layerFromEnv` reads Effect `Config` keys from the environment, upper-cased with underscores: `HOST`, `PORT`, `LISTEN_HOST`, `LISTEN_PORT`, `SHARD_GROUPS`, `AVAILABLE_SHARD_GROUPS`, `SHARDS_PER_GROUP` and so on. Explicit options win over them. `HOST` and `PORT` are generic names that platforms set for your web server, so a worker that does not set `runnerAddress` itself can end up advertising `0.0.0.0:8080`. Always pass `runnerAddress` (and `runnerListenAddress`) explicitly, read from names of your own, as below.

## A worker per node

Every node runs the same code, configured by its environment:

<!-- prettier-ignore -->
```ts
import * as NodeClusterHttp from '@effect/platform-node/NodeClusterHttp';
import { assignedShardGroupsFor, availableShardGroupsFor, JobStorage, makeSchedulerLayer } from '@yieldkit/jobs';
import { makePgLive } from '@yieldkit/pg';
import { Config, Effect, Layer, Option } from 'effect';
import { ClusterWorkflowEngine, RunnerAddress, ShardingConfig, SqlMessageStorage, SqlRunnerStorage } from 'effect/cluster';
import { Digest, SecureRandom } from 'effect/crypto';

import { allJobs } from './jobs.ts';

// Per node. Your own names: Effect's `layerFromEnv` would read the generic HOST and PORT.
const NodeConfig = Config.all({
  host: Config.String('CLUSTER_HOST'),                       // this node's IP or DNS name, as other nodes reach it
  port: Config.Port('CLUSTER_PORT').pipe(Config.withDefault(34431)),
  listenHost: Config.String('CLUSTER_LISTEN_HOST').pipe(Config.withDefault('0.0.0.0')),
  only: Config.option(Config.String('JOBS_ONLY')),           // e.g. "nightly-report,sync-orders"
});

const AppPg = makePgLive({ applicationName: 'worker', searchPath: 'app' });

export const WorkerLive = Layer.unwrap(
  Effect.gen(function* () {
    const node = yield* NodeConfig;
    const only = Option.getOrUndefined(node.only)?.split(',');
    const shardingConfig = {
      runnerAddress: Option.some(RunnerAddress.make(node.host, node.port)),
      runnerListenAddress: Option.some(RunnerAddress.make(node.listenHost, node.port)),
      // Identical on every node:
      availableShardGroups: [...availableShardGroupsFor(allJobs)],
      shardsPerGroup: 300,
      // Per node:
      assignedShardGroups: [...assignedShardGroupsFor(allJobs, only)],
    };
    const ShardingConfigLive = ShardingConfig.layerFromEnv(shardingConfig);
    const ClusterLive = ClusterWorkflowEngine.layer.pipe(
      Layer.provideMerge(NodeClusterHttp.layer({ transport: 'http', storage: 'byo', shardingConfig })),
      Layer.provideMerge(
        Layer.mergeAll(
          SqlMessageStorage.layerStorage({ prefix: 'app_cluster' }).pipe(Layer.provide(Digest.layerWebCrypto)),
          SqlRunnerStorage.layerStorage({ prefix: 'app_cluster' }),
        ).pipe(Layer.provide([AppPg, ShardingConfigLive])),
      ),
      Layer.provideMerge(ShardingConfigLive),
    );
    return makeSchedulerLayer(allJobs, { only }).pipe(
      Layer.provide([ClusterLive, JobStorage.layerStorage().pipe(Layer.provide(AppPg)), SecureRandom.layerWebCrypto]),
    );
  }),
);
```

Launch it with `NodeRuntime.runMain(Layer.launch(WorkerLive))`, so `SIGTERM` shuts it down cleanly.

### What must match across nodes

| Setting                                               | Same everywhere?                | Why                                                                             |
| ----------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------- |
| `allJobs` passed to the helpers and the scheduler     | Yes                             | Shard groups and maintenance come from it                                       |
| `availableShardGroups`, `shardsPerGroup`              | Yes                             | Every process must map an entity to the same shard; Cluster does not check this |
| Cluster table prefix and schema, `JobStorage` options | Yes                             | One set of tables per Cluster                                                   |
| Serialization (`binary` or `ndjson`) and transport    | Yes                             | Workers decode each other's RPCs                                                |
| `effect` and `@yieldkit/jobs` versions                | Yes, apart from rolling deploys | Persisted message and payload formats                                           |
| `runnerAddress`, `runnerListenAddress`                | No: unique per worker           |                                                                                 |
| `only` and `assignedShardGroups`                      | No                              | Lets worker pools specialise; see below                                         |
| `runnerShardWeight`                                   | No                              | A worker with weight 2 gets about twice the shards of one with weight 1         |

## Many identical workers

With no `only`, every worker claims every group and the shards spread evenly. Each run key still runs once: a cron or singleton runs only on its shard's owner, a second dispatch of a key joins the first execution, and the ledger claim fences the rest.

## Worker pools with `only`

To keep heavy jobs off your general workers, or run a job on dedicated machines, give each pool its own `only`. Then derive its `assignedShardGroups` with `assignedShardGroupsFor(allJobs, only)`, as `JOBS_ONLY` does above.

- Every worker still claims Cluster's `default` group and `yieldkit-jobs` (maintenance), so maintenance can run on any pool. A job's durable clocks stay in the job's group.
- A job's group is claimed only by the pools that run it. Its workflows never land on a worker that has no handler for them, and `makeSchedulerLayer` refuses a config that would allow that.
- Give each job at least two workers for redundancy. While a job's whole pool is down, its dispatched workflows wait in PostgreSQL and its crons do not fire. When a worker of the pool starts, its start-up singleton dispatches the current run key; earlier buckets that were missed are not run.

## API processes

API servers run the [README's client](../README.md#start-a-job-from-an-api) with the workers' `availableShardGroups`, `shardsPerGroup`, prefix and serialization. They should reach every worker's address; if not, dispatches arrive by polling, only slower. They hold no shards, so scaling them never rebalances.

## Scaling, restarts and failures

| Event                                | What happens                                                                                                                                                                                             | How fast (defaults)                                               |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| A worker joins                       | It registers. On their next assignment refresh, all workers recompute the hash ring and hand over the shards that move.                                                                                  | `refreshAssignmentsInterval` (3 s), plus `runnerJoinDebounce` (0) |
| A worker stops cleanly (`SIGTERM`)   | It stops its entities (waiting up to `entityTerminationTimeout`), releases its shard locks and unregisters. The others take its shards on their next refresh.                                            | Seconds                                                           |
| A worker crashes or its machine dies | Its PostgreSQL session ends, so its advisory locks go at once. It leaves membership once its heartbeat is older than `shardLockExpiration`, or sooner if a health check fails.                           | Up to `shardLockExpiration` (35 s)                                |
| A worker freezes or is partitioned   | PostgreSQL ends its idle session after `shardLockExpiration` (PostgreSQL 14+), releasing its locks. If it wakes and can't refresh its locks for that long, it unregisters itself and stops its entities. | `shardLockExpiration` (35 s)                                      |
| A worker restarts on a new IP        | A new runner registers; the old entry is handled as a stop or a crash above.                                                                                                                             | As above                                                          |
| PostgreSQL is unavailable            | Nothing can be stored, locked or claimed. Workers retry, and a worker that can't refresh its locks for `shardLockExpiration` gives up its shards until the database is back.                             | Until it recovers                                                 |

When shards move, in-flight workflows resume on the new owner from PostgreSQL; finished durable stages are not repeated, and the interrupted body runs again ([at least once](jobs.md#limits)).

Health checks: `NodeClusterHttp.layer` pings each runner (`runnerHealth: 'ping'`, every `runnerHealthCheckInterval`, 1 minute). On Kubernetes, `runnerHealth: 'k8s'` uses pod readiness instead, matching the pod by its IP, so advertise the pod IP.

## Rolling deploys

1. Run the deploy step first: your Cluster's migrations, then `JobStorage.layerMigrations`. Workers of the old version accept a newer library history, so they keep running.
2. Replace workers one or a few at a time. Each one stopping hands its shards to the others. Set a termination grace period longer than `entityTerminationTimeout` (15 s), or Kubernetes kills workers mid-handover.
3. Don't rename jobs, stages or other [persisted names](workers-and-clients.md#persisted-names) in the same deploy: in-flight workflows would be orphaned.

## Kubernetes

- A `Deployment` (or several, one per `only` pool) of workers, each pod with `CLUSTER_HOST` from the pod IP (`valueFrom.fieldRef.fieldPath: status.podIP`), `CLUSTER_PORT` fixed, and `CLUSTER_LISTEN_HOST=0.0.0.0`.
- A `containerPort` for the runner port. You need no `Service` for it, because nobody load-balances runner traffic; a `NetworkPolicy` should limit it to workers and API pods.
- `terminationGracePeriodSeconds` above 15, and `runnerHealth: 'k8s'` if you prefer readiness over pings. Its pod list needs RBAC to list pods.
- The deploy step as a `Job` or a pre-deploy hook, before the rollout.

## Tuning

These are `ShardingConfig` options from Effect, with Effect's defaults. Lowering them speeds up failover at the cost of more database traffic.

| Option                       | Default | Controls                                                                  |
| ---------------------------- | ------- | ------------------------------------------------------------------------- |
| `shardLockExpiration`        | 35 s    | How long a crashed or frozen worker can keep its shards and stay a member |
| `shardLockRefreshInterval`   | 10 s    | How often a worker renews its locks and heartbeat                         |
| `refreshAssignmentsInterval` | 3 s     | How often workers re-read membership and rebalance                        |
| `entityMessagePollInterval`  | 10 s    | The longest a stored message waits when its HTTP wake-up did not arrive   |
| `runnerHealthCheckInterval`  | 1 min   | How often runners are pinged                                              |
| `entityTerminationTimeout`   | 15 s    | How long a stopping worker waits for its entities                         |
| `runnerJoinDebounce`         | 0       | How long joins are held back to rebalance once                            |
| `runnerShardWeight`          | 1       | This worker's share of shards relative to others                          |
