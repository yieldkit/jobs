import type { WorkflowEngine } from 'effect/workflow';

import { Effect, Layer } from 'effect';
import {
  ClusterWorkflowEngine,
  MessageStorage,
  RunnerHealth,
  Runners,
  RunnerStorage,
  Sharding,
  ShardingConfig,
} from 'effect/cluster';

import type { AnyJobDefinition } from './job.ts';

import { ClusterConfigError } from './errors.ts';
import { isManualOnceJob } from './job.ts';
import { DEFAULT_SHARDS_PER_GROUP } from './policy.ts';

/**
 * Effect Cluster's default shard group, which entities without a group of their own use. A job's
 * durable clocks run in the job's own group, not here.
 */
export const DEFAULT_SHARD_GROUP = 'default';

/**
 * Shard group of the library's maintenance singleton. It is the library's own, so runners of your
 * application that do not run `makeSchedulerLayer` never claim it and keep it from running.
 */
export const MAINTENANCE_SHARD_GROUP = 'yieldkit-jobs';

/**
 * Cluster services the jobs engine uses: a worker needs all of them, a client only
 * `WorkflowEngine`.
 */
export type ClusterServices =
  | Sharding.Sharding
  | Runners.Runners
  | MessageStorage.MessageStorage
  | WorkflowEngine.WorkflowEngine;

/**
 * Width of Effect Cluster's `shard_id` columns (`VARCHAR(50)`), which store `<shard group>:<n>`.
 */
const MAX_SHARD_ID_LENGTH = 50;

/**
 * Characters a job name may hold. Effect Cluster's SQL runner storage writes shard ids, which hold
 * the name, into its queries without escaping them, PostgreSQL counts the column aliases built from
 * them in bytes, and its own entities use names with `/`, so names keep to this safe set.
 */
const JOB_NAME_PATTERN = /^[A-Za-z0-9._-]+$/;

/**
 * The sorted shard groups of every job that is not a manual `Once` job: each such job's workflow,
 * cron and start-up singleton live in a group named after it.
 */
const jobShardGroupsFor = (
  allJobs: ReadonlyArray<Pick<AnyJobDefinition, 'name' | 'cadence'>>,
): ReadonlyArray<string> => [
  ...new Set(allJobs.flatMap((job) => (isManualOnceJob(job) ? [] : [job.name])).toSorted()),
];

/**
 * Shard groups the jobs engine needs in `ShardingConfig.availableShardGroups` of every runner and
 * client: `default`, the maintenance group and one group per job that is not a manual `Once` job.
 * Add them to your own groups.
 */
export const availableShardGroupsFor = (
  allJobs: ReadonlyArray<Pick<AnyJobDefinition, 'name' | 'cadence'>>,
): ReadonlyArray<string> => [
  DEFAULT_SHARD_GROUP,
  MAINTENANCE_SHARD_GROUP,
  ...jobShardGroupsFor(allJobs),
];

/**
 * Shard groups a worker running `makeSchedulerLayer(allJobs, { only })` must claim in
 * `ShardingConfig.assignedShardGroups`: `default`, the maintenance group, and the groups of the
 * jobs `only` selects (every job that is not a manual `Once` job without it). Add them to your
 * own.
 */
export const assignedShardGroupsFor = (
  allJobs: ReadonlyArray<Pick<AnyJobDefinition, 'name' | 'cadence'>>,
  only: ReadonlyArray<string> | undefined,
): ReadonlyArray<string> => {
  const jobGroups = jobShardGroupsFor(allJobs);
  const selected = only === undefined ? jobGroups : jobGroups.filter((g) => only.includes(g));
  return [DEFAULT_SHARD_GROUP, MAINTENANCE_SHARD_GROUP, ...selected];
};

/**
 * Checks the `ShardingConfig` of a client or worker against `allJobs`: every job name is unique,
 * holds only `A-Z`, `a-z`, `0-9`, `.`, `_` and `-`, and is not the default or maintenance group,
 * `availableShardGroups` holds every group of `availableShardGroupsFor`, and each job's shard ids
 * fit Effect Cluster's `shard_id` column with the configured `shardsPerGroup`. A missing group
 * would not fail anywhere else: its messages would be stored and never run. Fails with
 * `ClusterConfigError` naming the offending names or groups. `makeSchedulerLayer` runs it; clients
 * can run it at start-up.
 */
export const checkShardingConfig = Effect.fnUntraced(function* (
  allJobs: ReadonlyArray<Pick<AnyJobDefinition, 'name' | 'cadence'>>,
): Effect.fn.Return<void, ClusterConfigError, ShardingConfig.ShardingConfig> {
  const config = yield* ShardingConfig.ShardingConfig;
  const nameProblem = jobNamesProblem(allJobs.map((job) => job.name));
  if (nameProblem !== undefined) {
    return yield* new ClusterConfigError({ message: nameProblem });
  }

  const available = new Set(config.availableShardGroups);
  const missing = availableShardGroupsFor(allJobs).filter((group) => !available.has(group));
  if (missing.length > 0) {
    return yield* new ClusterConfigError({
      message:
        `ShardingConfig.availableShardGroups lacks ${missing.join(', ')}. ` +
        'Add availableShardGroupsFor(allJobs) to it on every runner and client.',
    });
  }

  const maxNameLength = MAX_SHARD_ID_LENGTH - 1 - String(config.shardsPerGroup).length;
  // `VARCHAR(50)` counts code points, not UTF-16 units.
  const tooLong = jobShardGroupsFor(allJobs).filter(
    (group) => Array.from(group).length > maxNameLength,
  );
  if (tooLong.length > 0) {
    return yield* new ClusterConfigError({
      message: `Job names longer than ${String(maxNameLength)} characters do not fit Effect Cluster's shard ids with shardsPerGroup ${String(config.shardsPerGroup)}: ${tooLong.join(', ')}.`,
    });
  }

  return yield* Effect.void;
});

/**
 * Returns why the job names `names` cannot run on Effect Cluster, or `undefined` when they can: one
 * takes the default or maintenance group, holds a character outside `JOB_NAME_PATTERN`, or is used
 * twice.
 */
const jobNamesProblem = (names: ReadonlyArray<string>): string | undefined => {
  if (names.includes(MAINTENANCE_SHARD_GROUP)) {
    return `Job name "${MAINTENANCE_SHARD_GROUP}" is reserved for the maintenance shard group.`;
  }

  if (names.includes(DEFAULT_SHARD_GROUP)) {
    return `Job name "${DEFAULT_SHARD_GROUP}" is reserved for the default shard group.`;
  }

  const malformed = names.filter((name) => !JOB_NAME_PATTERN.test(name));
  if (malformed.length > 0) {
    return `Job names may only hold A-Z, a-z, 0-9, ".", "_" and "-": ${malformed.join(', ')}.`;
  }

  const duplicates = new Set(names.filter((name, index) => names.indexOf(name) !== index));
  return duplicates.size > 0
    ? `Job names must be unique; more than one job is named ${[...duplicates].join(', ')}.`
    : undefined;
};

/**
 * Checks a worker's `ShardingConfig.assignedShardGroups` against the jobs it registers: it claims
 * the maintenance group and the group of every enabled job, and no group of a job it does not
 * register (it would own those workflows' shards without a handler, and they would never run).
 */
export const checkWorkerAssignment = Effect.fnUntraced(function* (
  allJobs: ReadonlyArray<Pick<AnyJobDefinition, 'name' | 'cadence'>>,
  enabledJobNames: ReadonlyArray<string>,
): Effect.fn.Return<void, ClusterConfigError, ShardingConfig.ShardingConfig> {
  const config = yield* ShardingConfig.ShardingConfig;
  const assigned = new Set(config.assignedShardGroups);
  const enabled = new Set(enabledJobNames);
  const unclaimed = [MAINTENANCE_SHARD_GROUP, ...enabledJobNames].filter((g) => !assigned.has(g));
  if (unclaimed.length > 0) {
    return yield* new ClusterConfigError({
      message:
        `ShardingConfig.assignedShardGroups lacks ${unclaimed.join(', ')}, which this worker ` +
        'runs. Add assignedShardGroupsFor(allJobs, only) to it.',
    });
  }

  const unhandled = jobShardGroupsFor(allJobs).filter((g) => assigned.has(g) && !enabled.has(g));
  if (unhandled.length > 0) {
    return yield* new ClusterConfigError({
      message:
        `ShardingConfig.assignedShardGroups claims ${unhandled.join(', ')}, but this worker does ` +
        'not run those jobs. Remove them, or include them in `only`.',
    });
  }

  return yield* Effect.void;
});

/**
 * In-memory Cluster, `ClusterWorkflowEngine` and its `ShardingConfig` for unit tests of workflows,
 * durable sleeps, crons and singletons, without PostgreSQL or sockets.
 */
export const makeTestClusterLayer = (
  allJobs: ReadonlyArray<Pick<AnyJobDefinition, 'name' | 'cadence'>>,
  only?: ReadonlyArray<string>,
): Layer.Layer<ClusterServices | ShardingConfig.ShardingConfig | MessageStorage.MemoryDriver> => {
  const shardingConfigLayer = ShardingConfig.layer({
    availableShardGroups: [...availableShardGroupsFor(allJobs)],
    assignedShardGroups: [...assignedShardGroupsFor(allJobs, only)],
    shardsPerGroup: DEFAULT_SHARDS_PER_GROUP,
  });
  const runnerLayer = Sharding.layer.pipe(
    Layer.provideMerge(Runners.layerNoop),
    Layer.provideMerge(MessageStorage.layerMemory),
    Layer.provide([RunnerStorage.layerMemory, RunnerHealth.layerNoop]),
    Layer.provideMerge(shardingConfigLayer),
  );
  return ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(runnerLayer));
};
