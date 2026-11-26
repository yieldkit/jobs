# Workers and clients

Jobs run on your Effect Cluster. A **worker** is a runner that claims shards and runs `makeSchedulerLayer`: the jobs' workflows, crons and singletons. A **client**, usually an API server, only stores and dispatches workflows. The library builds neither: you compose the Cluster (storage, transport, serialization, health checks) and hand it in.

## What the jobs engine needs from your Cluster

- **Services.** `makeSchedulerLayer` needs `Sharding`, `MessageStorage`, `WorkflowEngine` (from `ClusterWorkflowEngine.layer`) and your `ShardingConfig`. `dispatchApiJobWorkflow`, `dispatchJob`, `makeJobWorkflow(...).poll` and `runJob` need only `WorkflowEngine` or nothing Cluster-related.
- **Shard groups.** Each job that is not a manual `Once` job, `OnDemand` jobs included, runs in a group named after it. Maintenance runs in `yieldkit-jobs`. A job's durable clocks run in the job's own group.
- **Migrated tables.** Your Cluster's storage is yours to migrate, for example with `SqlMessageStorage.layerMigrations` and `SqlRunnerStorage.layerMigrations`. The library's tables come from [`JobStorage.layerMigrations`](migrations.md).

Add the jobs' groups to your own with two helpers:

| Helper                                  | Use in                                                                                              |
| --------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `availableShardGroupsFor(allJobs)`      | `availableShardGroups` of every runner and client: `default`, `yieldkit-jobs` and every job's group |
| `assignedShardGroupsFor(allJobs, only)` | `assignedShardGroups` of a worker: `default`, `yieldkit-jobs` and the groups of the jobs it runs    |

Every process of one Cluster must agree on `availableShardGroups` and `shardsPerGroup`; Cluster does not check that across processes.

## Worker

The [README quick start](../README.md#quick-start) builds a full worker. `makeSchedulerLayer` takes two options:

```ts
import { makeSchedulerLayer } from '@yieldkit/jobs';

import { allJobs } from './jobs.ts';

export const WorkerLive = makeSchedulerLayer(allJobs, {
  only: ['sync-orders'], // jobs this worker runs; default all but manual Once jobs
  params: { 'sync-orders': { limit: 500 } }, // operator params per job, under API params
});
```

Operator params that do not decode for a scheduled job fail the layer with `ClusterConfigError`.

Derive the worker's groups from the helpers, next to your own: `assignedShardGroups: ['billing', ...assignedShardGroupsFor(allJobs, only)]`. Pass the same options to `NodeClusterHttp.layer` and `ShardingConfig.layerFromEnv`, and always set `runnerAddress` ([why](deployment.md#do-not-let-host-and-port-choose-the-address)).

`makeSchedulerLayer` registers:

- one workflow per job;
- a `ClusterCron` per `Every` and `Daily` job (in UTC) and per `Cron` job (in its `timeZone`); `OnDemand` jobs get none;
- a start-up singleton per `Every`, `Daily` and automatic `Once` job that dispatches the current run key, again every minute until Cluster accepts it (a `Daily` job only once its due time has passed and before `until`);
- one [maintenance singleton](policy.md#maintenance), in the `yieldkit-jobs` group.

It also needs the services the jobs' `run` effects use. At start-up it fails with:

- `UnknownJobError`: `only` names an unknown job;
- `ClusterConfigError`: `availableShardGroups` lacks a group of `availableShardGroupsFor(allJobs)`; `assignedShardGroups` lacks `yieldkit-jobs` or the group of a job this worker runs, or claims the group of a job it does not run (its workflows would land on this runner with no handler); a job name is too long for Cluster's shard ids at your `shardsPerGroup`; or a job name breaks the [naming rules](jobs.md#limits).

## Client

A client runs Cluster with `clientOnly: true`, `runnerAddress: Option.none()`, no assigned groups and `shardLockDisableAdvisory: true`, and the workers' `availableShardGroups` and `shardsPerGroup`. Without `Option.none()`, Effect's default address registers the client as a runner that claims `default` shards it cannot serve. The [README](../README.md#start-a-job-from-an-api) shows one. `checkShardingConfig(allJobs)` checks its groups.

`dispatchApiJobWorkflow` drops keys outside `apiParams` and fails with `InvalidJobParams` when the rest do not decode; to reject disallowed keys instead of dropping them, check `findDisallowedApiParams` first.

To follow a request, read its row and ask Cluster for the workflow's reply:

<!-- prettier-ignore -->
```ts
import { JobRequests, makeJobWorkflow } from '@yieldkit/jobs';
import { Effect } from 'effect';

import { syncOrders } from './jobs.ts';

export const requestStatus = (requestId: string, executionId: string) =>
  Effect.gen(function* () {
    const requests = yield* JobRequests;
    const status = yield* requests.getStatus(syncOrders.name, requestId); // request row and its ledger row
    const reply = yield* makeJobWorkflow(syncOrders).poll(executionId);   // Option.none() while there is no reply
    return { pickedAt: status?.request.pickedAt, run: status?.run, reply };
  });
```

[JobRequests](storage.md#jobrequests) describes what `getStatus` returns.

## Security

Cluster's runner RPCs are unauthenticated. Keep the runner port on a private network, and the Cluster's database private: anyone who can write its messages can start a `manual` run with any params.

## Persisted names

Changing these orphans in-flight executions, since Cluster's tables store them:

- the job name: workflow `Workflow/<name>`, cron `ClusterCron/<name>` and shard group;
- stage names;
- the workflow's own activity and durable clock names (`claim-round-N`, `settle-completed-N`, `settle-failed-N`, `retry-backoff-N`, `run-at-<epoch ms>`, ...);
- the idempotency key: `runKey`, or `runKey:attemptNonce`;
- the payload field names.

Singleton names (`@yieldkit/jobs/startup/<job>`, `@yieldkit/jobs/maintenance`) are only held in memory.

## Limits

- The `apiParams` filter covers `dispatchApiJobWorkflow` and payloads with `trigger: 'api'`, nothing else. Your API must authenticate and rate-limit callers and decide `force` itself.
- A request whose `pickedAt` is still `null` has no pickup recorded.
- An empty `workflow.poll` cannot tell a never-sent execution from a queued one or one running without a reply. Check `workflow.poll(executionId)` before dispatching again: a new dispatch is a new execution, and with `force` it runs too.
- `workflow.interrupt` takes effect at the next activity or sleep, so a body that was just claimed can still start.
- Pass every job of the deployment to each worker's `makeSchedulerLayer`, even with `only`: the maintenance singleton runs on one worker and prunes only for the jobs it was given.
- Operator `params` are read each time a workflow starts or resumes, so change them between runs, not during one.
