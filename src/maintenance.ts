import type { Layer } from 'effect';
import type { Sharding } from 'effect/cluster';

import { describeError, logInfo, logWarning } from '@yieldkit/logger';
import { Clock, Duration, Effect } from 'effect';
import { MessageStorage, Singleton } from 'effect/cluster';

import type { AnyJobDefinition } from './job.ts';
import type { JobsPolicy } from './policy.ts';

import { MAINTENANCE_SHARD_GROUP } from './cluster.ts';
import { JobRuns } from './ledger.ts';
import { JobRequests } from './requests.ts';

/**
 * Entity type used by `ClusterWorkflowEngine` for durable clock wakeups.
 */
const DURABLE_CLOCK_ENTITY_TYPE = 'Workflow/-/DurableClock';

/**
 * Name of the cluster-wide maintenance Singleton.
 */
const MAINTENANCE_SINGLETON_NAME = '@yieldkit/jobs/maintenance';

/**
 * Deletes processed cluster messages for `entityType` older than `retention` in bounded batches.
 */
const pruneEntityMessages = Effect.fnUntraced(function* (
  storage: MessageStorage.MessageStorage['Service'],
  entityType: string,
  retention: Duration.Duration,
  batchSize: number,
): Effect.fn.Return<number> {
  const nowMs = yield* Clock.currentTimeMillis;
  const completedBefore = nowMs - Duration.toMillis(retention);
  let totalDeleted = 0;

  for (;;) {
    const deleted = yield* storage
      .deleteProcessed({
        entityType,
        completedBefore,
        limit: batchSize,
      })
      .pipe(
        Effect.catch((error) =>
          logWarning('worker.maintenance.cluster_prune_failed', {
            entityType,
            errorMessage: describeError(error),
          }).pipe(Effect.as(0)),
        ),
      );
    totalDeleted += deleted;
    if (deleted < batchSize) {
      return totalDeleted;
    }
  }
});

/**
 * Deletes finished request rows older than `retention` in batches of `batchSize`. A failed batch is
 * logged and ends the sweep.
 */
const pruneRequests = Effect.fnUntraced(function* (
  requests: JobRequests['Service'],
  retention: Duration.Duration,
  batchSize: number,
): Effect.fn.Return<number> {
  let totalDeleted = 0;
  for (;;) {
    const deleted = yield* requests.prune(retention, batchSize).pipe(
      Effect.catch((error) =>
        logWarning('worker.maintenance.requests_prune_failed', {
          errorMessage: describeError(error),
        }).pipe(Effect.as(0)),
      ),
    );
    totalDeleted += deleted;
    if (deleted < batchSize) {
      return totalDeleted;
    }
  }
});

/**
 * Processed-message retention for one Effect Cluster entity type.
 */
export type EntityPruneTarget = {
  /**
   * Entity type whose processed messages are deleted, such as `Workflow/sync-orders`.
   */
  readonly entityType: string;

  /**
   * How long processed messages are kept before they are deleted.
   */
  readonly retention: Duration.Duration;
};

/**
 * Lists the workflow and cron entity types of `jobNames`, in job order, all kept for `retention`.
 */
const entityPruneTargets = (
  jobNames: ReadonlyArray<string>,
  retention: Duration.Duration,
): ReadonlyArray<EntityPruneTarget> =>
  jobNames.flatMap((name) => [
    { entityType: `Workflow/${name}`, retention },
    { entityType: `ClusterCron/${name}`, retention },
  ]);

/**
 * Names of the jobs in `jobs` whose cadence is one of `tags`.
 */
const jobNamesWith = (
  jobs: ReadonlyArray<Pick<AnyJobDefinition, 'name' | 'cadence'>>,
  tags: ReadonlyArray<AnyJobDefinition['cadence']['_tag']>,
): ReadonlyArray<string> =>
  jobs.flatMap((job) => (tags.includes(job.cadence._tag) ? [job.name] : []));

/**
 * Lists every processed-message prune target of `jobs` under `policy`: `Every` and `Cron` workflows
 * and crons after `retention.everyCluster`, `Daily` ones after `retention.dailyCluster`, `OnDemand`
 * workflows after `retention.onDemand`, and durable clocks after the shorter of
 * `retention.everyCluster` and `retention.onDemand`, because a processed clock that outlives its
 * pruned `OnDemand` workflow would swallow the same wake-up of a later run of that key. `Once`
 * workflows are never pruned, so a completed `Once` job stays deduplicated.
 */
export const clusterPruneTargetsFor = (
  jobs: ReadonlyArray<Pick<AnyJobDefinition, 'name' | 'cadence'>>,
  policy: JobsPolicy,
): ReadonlyArray<EntityPruneTarget> => [
  ...entityPruneTargets(jobNamesWith(jobs, ['Every', 'Cron']), policy.retention.everyCluster),
  ...entityPruneTargets(jobNamesWith(jobs, ['Daily']), policy.retention.dailyCluster),
  ...jobNamesWith(jobs, ['OnDemand']).map((name) => ({
    entityType: `Workflow/${name}`,
    retention: policy.retention.onDemand,
  })),
  {
    entityType: DURABLE_CLOCK_ENTITY_TYPE,
    retention: Duration.min(policy.retention.everyCluster, policy.retention.onDemand),
  },
];

/**
 * Cluster-wide maintenance `Singleton`. When it starts and then every `policy.maintenance.interval`
 * it prunes `Every` and `Cron` ledger rows older than `retention.everyRuns`, `OnDemand` rows older
 * than `retention.onDemand`, finished request rows older than `retention.requests`, and processed
 * Cluster messages (see `clusterPruneTargetsFor`). `jobs` is every job of the deployment, so the
 * one worker holding the singleton prunes for all of them.
 */
export const makeMaintenanceLayer = (
  jobs: ReadonlyArray<AnyJobDefinition>,
): Layer.Layer<
  never,
  never,
  Sharding.Sharding | JobRuns | JobRequests | MessageStorage.MessageStorage
> => {
  const everyJobNames = jobNamesWith(jobs, ['Every', 'Cron']);
  const onDemandJobNames = jobNamesWith(jobs, ['OnDemand']);

  return Singleton.make(
    MAINTENANCE_SINGLETON_NAME,
    Effect.gen(function* () {
      const runs = yield* JobRuns;
      const requests = yield* JobRequests;
      const storage = yield* MessageStorage.MessageStorage;
      const { policy } = runs;
      const clusterPruneTargets = clusterPruneTargetsFor(jobs, policy);
      return yield* Effect.forever(
        Effect.gen(function* () {
          if (everyJobNames.length > 0) {
            yield* runs.prune(everyJobNames, policy.retention.everyRuns).pipe(
              Effect.tap((deleted) =>
                deleted > 0
                  ? logInfo('worker.supervisor.pruned', { deleted, jobs: everyJobNames })
                  : Effect.void,
              ),
              Effect.catch((error) =>
                logWarning('worker.supervisor.prune_failed', {
                  errorMessage: describeError(error),
                }),
              ),
            );
          }

          if (onDemandJobNames.length > 0) {
            yield* runs.prune(onDemandJobNames, policy.retention.onDemand).pipe(
              Effect.tap((deleted) =>
                deleted > 0
                  ? logInfo('worker.supervisor.pruned', { deleted, jobs: onDemandJobNames })
                  : Effect.void,
              ),
              Effect.catch((error) =>
                logWarning('worker.supervisor.prune_failed', {
                  errorMessage: describeError(error),
                }),
              ),
            );
          }

          const requestsDeleted = yield* pruneRequests(
            requests,
            policy.retention.requests,
            policy.maintenance.batchSize,
          );
          if (requestsDeleted > 0) {
            yield* logInfo('worker.maintenance.requests_pruned', { deleted: requestsDeleted });
          }

          let clusterDeleted = 0;
          for (const target of clusterPruneTargets) {
            clusterDeleted += yield* pruneEntityMessages(
              storage,
              target.entityType,
              target.retention,
              policy.maintenance.batchSize,
            );
          }

          if (clusterDeleted > 0) {
            yield* logInfo('worker.maintenance.cluster_pruned', { deleted: clusterDeleted });
          }

          yield* Effect.sleep(policy.maintenance.interval);
        }),
      );
    }),
    { shardGroup: MAINTENANCE_SHARD_GROUP },
  );
};
