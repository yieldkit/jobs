import type { MessageStorage, Sharding, ShardingConfig } from 'effect/cluster';
import type { SecureRandom } from 'effect/crypto';
import type { WorkflowEngine } from 'effect/workflow';

import { describeError, logInfo, logWarning } from '@yieldkit/logger';
import { Clock, Cron, DateTime, Duration, Effect, Layer } from 'effect';
import { ClusterCron, Singleton } from 'effect/cluster';

import type { AnyJobDefinition, JobServices } from './job.ts';
import type { JobRuns } from './ledger.ts';
import type { JobRequests } from './requests.ts';
import type { ClockCadence } from './run-key.ts';
import type { OperatorJobParams } from './workflow.ts';

import {
  cronCatchUpFor,
  dailyScheduleOrThrow,
  isDailyRunDue,
  validateCadence,
} from './calendar.ts';
import { checkShardingConfig, checkWorkerAssignment } from './cluster.ts';
import { cronForCadence, validateJobCadence } from './cron.ts';
import { ClusterConfigError, UnknownJobError } from './errors.ts';
import { isManualOnceJob } from './job.ts';
import { makeMaintenanceLayer } from './maintenance.ts';
import { decodeJobParams } from './params.ts';
import { DEFAULT_DAILY_CHECK_EVERY } from './policy.ts';
import { runKeyFor } from './run-key.ts';
import { WorkerJobParams, makeAnyJobWorkflowLayer, makeJobWorkflow } from './workflow.ts';

export { clusterPruneTargetsFor, type EntityPruneTarget } from './maintenance.ts';

/**
 * Name of the Singleton that dispatches a job's run at start-up.
 */
const startupSingletonName = (job: string): string => `@yieldkit/jobs/startup/${job}`;

/**
 * Options for `makeSchedulerLayer`.
 */
export type SchedulerOptions = {
  /**
   * Job names to run on this worker; defaults to every job that is not a manual `Once` job. Unknown
   * names fail at start-up with `UnknownJobError`.
   */
  readonly only?: ReadonlyArray<string> | undefined;

  /**
   * Operator param overrides per job name, merged under API params and over each job's defaults.
   */
  readonly params?: OperatorJobParams | undefined;
};

/**
 * Resolved non-manual jobs selected for registration on this worker.
 */
type SelectedWorkerJobs<R> = {
  /**
   * Non-manual jobs enabled on this worker (manual `Once` jobs are never registered on workers).
   */
  readonly enabledJobs: ReadonlyArray<AnyJobDefinition<R>>;

  /**
   * Names of jobs excluded by `options.only`.
   */
  readonly filteredOutJobs: ReadonlyArray<string>;
};

/**
 * Validates `only` against `jobs` and returns the non-manual jobs enabled for this worker.
 */
export const selectWorkerJobs = <R>(
  jobs: ReadonlyArray<AnyJobDefinition<R>>,
  only: ReadonlyArray<string> | undefined,
): Effect.Effect<SelectedWorkerJobs<R>, UnknownJobError> => {
  if (only === undefined) {
    return Effect.succeed({
      enabledJobs: jobs.filter((job) => !isManualOnceJob(job)),
      filteredOutJobs: [],
    });
  }

  const knownNames = new Set(jobs.map((job) => job.name));
  const unknown = only.filter((name) => !knownNames.has(name));
  if (unknown.length > 0) {
    return Effect.fail(new UnknownJobError({ names: unknown }));
  }

  const selected = new Set(only);
  const enabledJobs = jobs.filter((job) => selected.has(job.name) && !isManualOnceJob(job));
  const filteredOutJobs = jobs.flatMap((job) => (selected.has(job.name) ? [] : [job.name]));
  return Effect.succeed({ enabledJobs, filteredOutJobs });
};

/**
 * Formats a job's cadence into a short human-readable string for startup logs.
 */
const formatCadence = (job: AnyJobDefinition): string => {
  switch (job.cadence._tag) {
    case 'Daily': {
      const { at, window, until, timeZone } = job.cadence;
      const checkEvery = job.cadence.checkEvery ?? DEFAULT_DAILY_CHECK_EVERY;
      const details = [
        `every ${Duration.format(Duration.fromInputUnsafe(checkEvery))}`,
        at === undefined ? undefined : `at ${at}`,
        window === undefined ? undefined : `window ${window.from}-${window.to}`,
        until === undefined ? undefined : `until ${until}`,
        timeZone === undefined ? undefined : timeZone,
      ].filter((detail) => detail !== undefined);
      return `daily(${details.join(', ')})`;
    }

    case 'Every':
      return `every(${Duration.format(Duration.fromInputUnsafe(job.cadence.interval))})`;
    case 'Cron':
      return `cron(${job.cadence.expression}${job.cadence.timeZone === undefined ? '' : `, ${job.cadence.timeZone}`})`;
    case 'Once':
      return `once(${job.cadence.start})`;
    case 'OnDemand':
      return 'on-demand';
    default: {
      const unexpected: never = job.cadence;
      return unexpected;
    }
  }
};

/**
 * Wait between start-up dispatch attempts of a job whose last attempt failed.
 */
const STARTUP_DISPATCH_RETRY = Duration.minutes(1);

/**
 * Dispatches one idempotent scheduled workflow run of `job` under `runKey`. A failure is logged as
 * `worker.scheduler.dispatch_failed`; the result says whether the run reached Cluster.
 */
const dispatchScheduledRun = (
  job: AnyJobDefinition,
  runKey: string,
): Effect.Effect<boolean, never, WorkflowEngine.WorkflowEngine> => {
  const workflow = makeJobWorkflow(job);
  return workflow
    .execute(
      {
        runKey,
        trigger: 'schedule',
        params: {},
        force: false,
        requestId: null,
        attemptNonce: null,
      },
      { discard: true },
    )
    .pipe(
      Effect.as(true),
      Effect.catchCause((cause) =>
        logWarning('worker.scheduler.dispatch_failed', {
          job: job.name,
          runKey,
          errorMessage: describeError(cause),
        }).pipe(Effect.as(false)),
      ),
    );
};

/**
 * Wait between dispatch attempts of a `Cron` or `Every` tick whose last attempt failed.
 */
const CRON_DISPATCH_RETRY = Duration.seconds(30);

/**
 * Dispatches the run `runKey` of the `Cron` or `Every` tick at `scheduledMs`, from a `ClusterCron`
 * handler. A failed dispatch is tried again under the same run key every 30 seconds while the retry
 * starts within `skipIfOlderThan` (`catchUpWithin` for `Cron`, one interval for `Every`) of the
 * tick and before the next tick, since `ClusterCron` only schedules the next tick, from the time
 * this handler returns, and would otherwise drop the run.
 */
const dispatchCronTick = Effect.fnUntraced(function* (
  job: AnyJobDefinition,
  runKey: string,
  cron: Cron.Cron,
  scheduledMs: number,
): Effect.fn.Return<void, never, WorkflowEngine.WorkflowEngine | ClusterCron.CronSchedule> {
  const { skipIfOlderThan } = yield* ClusterCron.CronSchedule;
  const deadlineMs = Math.min(
    scheduledMs + Duration.toMillis(skipIfOlderThan),
    Cron.next(cron, scheduledMs).getTime(),
  );
  while (!(yield* dispatchScheduledRun(job, runKey))) {
    const nowMs = yield* Clock.currentTimeMillis;
    if (nowMs + Duration.toMillis(CRON_DISPATCH_RETRY) >= deadlineMs) {
      yield* logWarning('worker.scheduler.dispatch_abandoned', { job: job.name, runKey });
      return;
    }

    yield* Effect.sleep(CRON_DISPATCH_RETRY);
  }
});

/**
 * Builds the start-up `Singleton` of a scheduled job: it dispatches the run due now, when `isDue`
 * says one is, and tries again every minute until the dispatch reaches Cluster, since an automatic
 * `Once` job has no cron to make up for it.
 */
const makeStartupTrigger = (
  job: AnyJobDefinition,
  cadence: ClockCadence,
  isDue: (scheduledMs: number, nowMs: number) => boolean,
): Layer.Layer<never, never, Sharding.Sharding | WorkflowEngine.WorkflowEngine> =>
  Singleton.make(
    startupSingletonName(job.name),
    Effect.gen(function* () {
      for (;;) {
        const nowMs = yield* Clock.currentTimeMillis;
        if (!isDue(nowMs, nowMs)) {
          return;
        }

        if (yield* dispatchScheduledRun(job, runKeyFor(cadence, nowMs))) {
          return;
        }

        yield* Effect.sleep(STARTUP_DISPATCH_RETRY);
      }
    }),
    { shardGroup: job.name },
  );

/**
 * Builds the schedule of one job, scoped to its dedicated shard group (`job.name`):
 *
 * - `Every` and `Daily`: a `ClusterCron` and a start-up `Singleton` that dispatches the current run.
 *   A failed dispatch of an `Every` tick is retried (see `dispatchCronTick`). A `Daily` check
 *   dispatches only once the day's `at` or window time is reached and while `until` has not passed;
 *   a check whose dispatch fails is made up for by the next one, under the same run key.
 * - `Cron`: a `ClusterCron` on the expression; a tick missed by up to `catchUpWithin` still runs, and
 *   a failed dispatch of a tick is retried (see `dispatchCronTick`).
 * - Automatic `Once`: the start-up `Singleton` only.
 * - `OnDemand`: nothing; `dispatchJob` starts its runs.
 *
 * Fails with `ClusterConfigError` for an invalid interval, time, time zone or expression.
 */
const makeJobScheduleLayer = Effect.fnUntraced(function* (
  job: AnyJobDefinition,
): Effect.fn.Return<
  Layer.Layer<never, never, Sharding.Sharding | WorkflowEngine.WorkflowEngine>,
  ClusterConfigError
> {
  const { cadence } = job;
  yield* validateCadence(job.name, cadence);
  if (cadence._tag === 'OnDemand') {
    return Layer.empty;
  }

  if (cadence._tag === 'Once') {
    return makeStartupTrigger(job, cadence, () => true);
  }

  const daily = cadence._tag === 'Daily' ? dailyScheduleOrThrow(job.name, cadence) : undefined;
  /**
   * Whether a check scheduled at `scheduledMs`, handled at `nowMs`, starts the run: always, except
   * before a `Daily` job's due time or after its `until`.
   */
  const isDue = (scheduledMs: number, nowMs: number): boolean =>
    daily === undefined || isDailyRunDue(daily, scheduledMs, nowMs);
  const cron = yield* cronForCadence(cadence);
  const cronLayer = ClusterCron.make({
    name: job.name,
    cron,
    shardGroup: job.name,
    calculateNextRunFromPrevious: false,
    skipIfOlderThan: skipIfOlderThanFor(cadence),
    execute: Effect.gen(function* () {
      const scheduledMs = DateTime.toEpochMillis(yield* ClusterCron.CurrentScheduledTime);
      const nowMs = yield* Clock.currentTimeMillis;
      const runKey = runKeyFor(cadence, scheduledMs);
      if (cadence._tag !== 'Daily') {
        yield* dispatchCronTick(job, runKey, cron, scheduledMs);
      } else if (isDue(scheduledMs, nowMs)) {
        yield* dispatchScheduledRun(job, runKey);
      }
    }),
  });
  if (cadence._tag === 'Cron') {
    return cronLayer;
  }

  return Layer.mergeAll(makeStartupTrigger(job, cadence, isDue), cronLayer);
});

/**
 * How old a `ClusterCron` tick of `cadence` may be and still run: one interval for `Every`, a day
 * for `Daily` (its own checks bound it further) and `catchUpWithin` for `Cron`.
 */
const skipIfOlderThanFor = (
  cadence: Extract<ClockCadence, { readonly _tag: 'Every' | 'Daily' | 'Cron' }>,
): Duration.Input => {
  switch (cadence._tag) {
    case 'Every':
      return cadence.interval;
    case 'Daily':
      return Duration.days(1);
    case 'Cron':
      return cronCatchUpFor(cadence);
    default: {
      const unexpected: never = cadence;
      return unexpected;
    }
  }
};

/**
 * Decodes the operator params of `job` over its defaults when the scheduler starts its runs itself,
 * with no caller params, so a bad value fails at start-up instead of failing every scheduled run
 * (and for an automatic `Once` job, storing a failure its later dispatches join). `OnDemand` jobs
 * are skipped, since their required params come with each dispatch.
 */
const checkOperatorParams = (
  job: AnyJobDefinition,
  params: OperatorJobParams | undefined,
): Effect.Effect<void, ClusterConfigError> => {
  if (job.cadence._tag === 'OnDemand') {
    return Effect.void;
  }

  return decodeJobParams(job, params?.[job.name]).pipe(
    Effect.asVoid,
    Effect.mapError(
      (error) => new ClusterConfigError({ message: `Job "${job.name}": ${error.message}` }),
    ),
  );
};

/**
 * Everything a worker runs for `jobs`: a durable `Workflow` handler per job, `ClusterCron`
 * schedules, a start-up `Singleton` per `Every`, `Daily` and automatic `Once` job that dispatches
 * the current run, and the maintenance `Singleton`. `OnDemand` jobs get a handler but no schedule.
 * Manual `Once` jobs are never registered. Provide it with your own Cluster (built with
 * `ClusterWorkflowEngine.layer`) and its `ShardingConfig`, `JobStorage.layerStorage`, the services
 * the jobs need and `SecureRandom` for job logging. At start-up it checks the `ShardingConfig` with
 * `checkShardingConfig` and `checkWorkerAssignment`, and fails with `ClusterConfigError` when a
 * shard group the jobs need is missing or misassigned, when the cadence of any job in `jobs`,
 * including one `only` leaves out, is invalid, or when `params` for a job this worker schedules
 * itself (any but `OnDemand`) do not decode over its defaults.
 */
export const makeSchedulerLayer = <Jobs extends ReadonlyArray<AnyJobDefinition<JobServices<Jobs>>>>(
  jobs: Jobs,
  options: SchedulerOptions = {},
): Layer.Layer<
  never,
  UnknownJobError | ClusterConfigError,
  | JobServices<Jobs>
  | JobRuns
  | JobRequests
  | Sharding.Sharding
  | ShardingConfig.ShardingConfig
  | MessageStorage.MessageStorage
  | WorkflowEngine.WorkflowEngine
  | SecureRandom.SecureRandom
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      yield* Effect.forEach(jobs, validateJobCadence, { discard: true });
      const { enabledJobs, filteredOutJobs } = yield* selectWorkerJobs<JobServices<Jobs>>(
        jobs,
        options.only,
      );
      yield* Effect.forEach(enabledJobs, (job) => checkOperatorParams(job, options.params), {
        discard: true,
      });
      yield* checkShardingConfig(jobs);
      yield* checkWorkerAssignment(
        jobs,
        enabledJobs.map((job) => job.name),
      );
      yield* logInfo('worker.supervisor.starting', {
        jobCount: enabledJobs.length,
        jobs: enabledJobs.map((job) => ({
          ...job.logFields,
          name: job.name,
          cadence: formatCadence(job),
        })),
        filteredOutJobs: filteredOutJobs.length > 0 ? filteredOutJobs : undefined,
      });
      for (const jobName of filteredOutJobs) {
        yield* logInfo('worker.supervisor.job_filtered', { job: jobName });
      }

      const workflowLayers = enabledJobs.map((job) => makeAnyJobWorkflowLayer(job));
      const scheduleLayers = yield* Effect.forEach(enabledJobs, makeJobScheduleLayer);
      const maintenanceLayer = makeMaintenanceLayer(jobs);
      const lifecycleLayer = Layer.effectDiscard(
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            logInfo('worker.supervisor.stopped', { jobCount: enabledJobs.length }),
          );
          yield* logInfo('worker.supervisor.running', {
            jobCount: enabledJobs.length,
            jobNames: enabledJobs.map((job) => job.name),
          });
        }),
      );
      return Layer.mergeAll(
        maintenanceLayer,
        lifecycleLayer,
        ...workflowLayers,
        ...scheduleLayers,
      ).pipe(Layer.provide(WorkerJobParams.layer(options.params ?? {})));
    }),
  );
