import type { LogFields } from '@yieldkit/logger';
import type { Context, Duration, Effect, Layer } from 'effect';

import { Schema } from 'effect';

/**
 * Schema of how a job run was started. `schedule` runs follow the retry rules for failed keys,
 * `api` runs only accept the job's `apiParams`, and `manual` runs are started by an operator with
 * trusted params. The `apiParams` filter applies to `dispatchApiJobWorkflow` and to `api` workflow
 * payloads only: `runJob` trusts its caller, and anyone who can write Cluster's messages can start
 * a `manual` run. Persisted in the run ledger and in workflow payloads.
 */
export const JobTrigger = Schema.Literals(['schedule', 'manual', 'api']);

/**
 * How a job run was started: `'schedule'`, `'manual'` or `'api'`.
 */
export type JobTrigger = typeof JobTrigger.Type;

/**
 * Local wall-clock time `HH:MM` (24-hour, `00:00` to `23:59`; `until` also takes `24:00`, the end
 * of the day).
 */
export type TimeOfDay = `${number}:${number}`;

/**
 * Local time window of a `Daily` cadence: each day's run starts at a time drawn from `[from, to)`.
 */
export type DailyWindow = {
  /**
   * Earliest start, inclusive.
   */
  readonly from: TimeOfDay;

  /**
   * Latest start, exclusive.
   */
  readonly to: TimeOfDay;
};

/**
 * Once a day, by default just after midnight UTC.
 */
export type DailyCadence = {
  /**
   * Once per day. The scheduler checks every `checkEvery`; the run key is the local date
   * `YYYY-MM-DD` in `timeZone`.
   */
  readonly _tag: 'Daily';

  /**
   * How often the scheduler checks whether today's run is due, and how long a failed scheduled run
   * waits before its next attempt. Must evenly divide an hour or a day. Defaults to 1 hour.
   */
  readonly checkEvery?: Duration.Input | undefined;

  /**
   * IANA time zone of the run key, `at`, `window` and `until`, such as `Europe/Brussels`. Defaults
   * to `UTC`.
   */
  readonly timeZone?: string | undefined;

  /**
   * Local time the run is due. The first check at or after it starts the run. Not with `window`.
   */
  readonly at?: TimeOfDay | undefined;

  /**
   * Local window the run starts in, at a time that differs per day but is the same on every worker.
   * Not with `at`.
   */
  readonly window?: DailyWindow | undefined;

  /**
   * Latest local time a run may start, first attempt or retry. Defaults to `window.to` with a
   * window, `24:00` with `at`, and no limit otherwise. With a limit, retries go on until it instead
   * of stopping after `retry.dailyMaxFailures` failures.
   */
  readonly until?: TimeOfDay | undefined;
};

/**
 * When a job is due and what one idempotent unit of work (`runKey`) represents.
 */
export type JobCadence =
  | DailyCadence
  | {
      /**
       * Every tick of a cron expression. The run key is the tick in UTC (`YYYY-MM-DDTHH:MMZ`).
       */
      readonly _tag: 'Cron';

      /**
       * Five-field cron expression (`minute hour day month weekday`), such as `0 9 * * 1-5`.
       */
      readonly expression: string;

      /**
       * IANA time zone the expression is read in. Defaults to `UTC`.
       */
      readonly timeZone?: string | undefined;

      /**
       * How late a missed tick may still run, for example after every worker was down. Defaults to
       * 1 hour.
       */
      readonly catchUpWithin?: Duration.Input | undefined;
    }
  | {
      /**
       * Never scheduled. Each run is started with `dispatchJob` (or `runJob`) under a run key the
       * caller chooses, such as `welcome-email:user-42`.
       */
      readonly _tag: 'OnDemand';
    }
  | {
      /**
       * Fixed interval bucket. The run key is the UTC bucket start (`YYYY-MM-DDTHH:MMZ`), so
       * replicas dedupe within each bucket.
       */
      readonly _tag: 'Every';

      /**
       * Duration of each execution bucket.
       */
      readonly interval: Duration.Input;
    }
  | {
      /**
       * Once ever (for example a full historical import). The run key is `'once'`.
       */
      readonly _tag: 'Once';

      /**
       * `automatic` jobs are run by the scheduler until they complete. `manual` jobs run only when
       * an operator starts them with `runJob`; the scheduler never registers them and
       * `isApiRequestable` rejects them.
       */
      readonly start: 'automatic' | 'manual';
    };

/**
 * Runtime context passed into `JobDefinition.run` after parameter validation and run claiming.
 */
export type JobContext<Params> = {
  /**
   * Decoded and validated job parameters merged from `defaults` and caller overrides.
   */
  readonly params: Params;

  /**
   * Ledger key of this run: `YYYY-MM-DD`, `YYYY-MM-DDTHH:MMZ`, `'once'`, or the caller's key for an
   * `OnDemand` job.
   */
  readonly runKey: string;

  /**
   * What triggered this run.
   */
  readonly trigger: JobTrigger;

  /**
   * 1-based attempt number at this `(job, runKey)`. `0` during a dry run.
   */
  readonly attempt: number;
};

/**
 * Non-enumerable symbol attaching an existential rank-2 continuation to jobs created with
 * `defineJob`, allowing the scheduler to invoke `runJob` with correlated `Params` without casts.
 */
export const JOB_CONTINUATION: unique symbol = Symbol.for('@yieldkit/jobs/JobContinuation');

/**
 * Existential continuation preserving the invariant relationship between `params.schema`,
 * `params.defaults`, `run`, `stats`, and `dryRun` across heterogeneous job lists.
 */
export type JobContinuation<R> = <Result>(
  invoke: <Name extends string, P, S, E, PE>(
    definition: JobDefinition<Name, P, S, E, R, PE>,
  ) => Result,
) => Result;

/**
 * Typed specification of one job: identity, cadence, validated params, persisted completion stats,
 * the effect that does the work, and an optional dry-run layer.
 */
export type JobDefinition<Name extends string, Params, Stats, E, R, ParamsEncoded = Params> = {
  /**
   * Unique job name. It is the ledger's `job` column, the workflow name and the shard group, so
   * renaming a job orphans its history and in-flight executions.
   */
  readonly name: Name;

  /**
   * Short human-readable description for listings and logs.
   */
  readonly description: string;

  /**
   * Schedule and idempotent ledger key cadence.
   */
  readonly cadence: JobCadence;

  /**
   * Validated runtime parameters. `{ ...defaults, ...overrides }` is decoded before claiming.
   */
  readonly params: {
    /**
     * Codec validating caller overrides + defaults and encoding validated params for `job_runs`.
     */
    readonly schema: Schema.Codec<Params, ParamsEncoded>;

    /**
     * Encoded default parameter values merged beneath caller overrides before decoding.
     */
    readonly defaults: Partial<ParamsEncoded>;

    /**
     * Param keys an API caller may set. Every other key stays with the operator (`runJob` params
     * and the scheduler's `params`). Defaults to none, so an API request can only start the job
     * with the worker's params. `findDisallowedApiParams` lists the keys a request must be rejected
     * for, and the workflow drops every other key again before running. Opt in only params that
     * cannot widen what the job reads, deletes or how long it runs.
     */
    readonly apiParams?: ReadonlyArray<Extract<keyof ParamsEncoded, string>> | undefined;
  };

  /**
   * Codec for the finished run's stats, encoded into the ledger's `stats` column and the completion
   * log line.
   */
  readonly stats: Schema.Codec<Stats, unknown>;

  /**
   * Performs one unit of work for `ctx`.
   */
  readonly run: (ctx: JobContext<Params>) => Effect.Effect<Stats, E, R>;

  /**
   * Optional layer that replaces the job's writes (for example with log-only stores) for a dry run.
   */
  readonly dryRun?: Layer.Layer<never, never, R> | undefined;

  /**
   * Typed metadata handed to the storage layer's `onSettle` hook as `JobSettled.annotations`, built
   * with `Context.make`. Defaults to an empty context.
   */
  readonly annotations?: Context.Context<never> | undefined;

  /**
   * Static fields added to every log line of this job's runs and to the scheduler's start-up
   * listing.
   */
  readonly logFields?: LogFields | undefined;

  /**
   * Internal non-enumerable rank-2 continuation attached by `defineJob`.
   */
  readonly [JOB_CONTINUATION]?: JobContinuation<R> | undefined;
};

/**
 * Widest `JobDefinition` shape a heterogeneous job registry or scheduler list can hold without
 * using `any`. Parameterized by `R` (default `unknown`) so callers can preserve the union of
 * required services.
 */
export type AnyJobDefinition<R = unknown> = {
  /**
   * Registered job name.
   */
  readonly name: string;

  /**
   * Human-readable job summary.
   */
  readonly description: string;

  /**
   * Job scheduling cadence.
   */
  readonly cadence: JobCadence;

  /**
   * Type-erased parameter schema and defaults.
   */
  readonly params: {
    /**
     * Parameter codec narrowed covariantly to `unknown`.
     */
    readonly schema: Schema.Codec<unknown, unknown>;

    /**
     * Encoded parameter defaults as a JSON object, merged beneath JSON overrides before decoding.
     */
    readonly defaults: Schema.JsonObject;

    /**
     * Param keys an API caller may set; none when missing.
     */
    readonly apiParams?: ReadonlyArray<string> | undefined;
  };

  /**
   * Stats codec narrowed covariantly to `unknown`.
   */
  readonly stats: Schema.Codec<unknown, unknown>;

  /**
   * Type-erased job runner accepting `JobContext<never>` in contravariant position.
   */
  readonly run: (ctx: JobContext<never>) => Effect.Effect<unknown, unknown, R>;

  /**
   * Optional dry-run layer narrowed to `Layer<never, never, R>`.
   */
  readonly dryRun?: Layer.Layer<never, never, R> | undefined;

  /**
   * Typed metadata handed to the `onSettle` hook.
   */
  readonly annotations?: Context.Context<never> | undefined;

  /**
   * Static fields added to the job's log lines.
   */
  readonly logFields?: LogFields | undefined;

  /**
   * Non-enumerable rank-2 continuation attached by `defineJob` for type-safe scheduler invocation.
   */
  readonly [JOB_CONTINUATION]?: JobContinuation<R> | undefined;
};

/**
 * Identity fields of a job (name, cadence, annotations, log fields) that settling and logging read.
 * Any `JobDefinition` satisfies it, whatever its parameter types.
 */
export type JobIdentity = Pick<AnyJobDefinition, 'name' | 'cadence' | 'annotations' | 'logFields'>;

/**
 * The part of a job definition that decides which params an API caller may set. Any `JobDefinition`
 * satisfies it, whatever its parameter types.
 */
export type JobApiParamsPolicy = {
  /**
   * Parameter spec holding the `apiParams` allowlist.
   */
  readonly params: Pick<AnyJobDefinition['params'], 'apiParams'>;
};

/**
 * Returns `true` when `job` is a manual one-time job that should not start a scheduled loop or be
 * registered on cluster workers.
 */
export const isManualOnceJob = (job: Pick<AnyJobDefinition, 'cadence'>): boolean =>
  job.cadence._tag === 'Once' && job.cadence.start === 'manual';

/**
 * Returns `true` when `job` may be requested through the API (`dispatchApiJobWorkflow`). Manual
 * `Once` jobs are excluded: they are one-off operator tasks, so only an operator starts them.
 * `OnDemand` jobs are excluded too: they need a run key, so they start with `dispatchJob`.
 */
export const isApiRequestable = (job: Pick<AnyJobDefinition, 'cadence'>): boolean =>
  !isManualOnceJob(job) && job.cadence._tag !== 'OnDemand';

/**
 * Union of Effect service requirements across all jobs in a tuple/array of `JobDefinition`s.
 */
export type JobServices<Jobs extends ReadonlyArray<AnyJobDefinition>> = Effect.Services<
  ReturnType<Jobs[number]['run']>
>;

/**
 * Defines a typed job, preserving the literal name and inferring `Params`, `ParamsEncoded`,
 * `Stats`, `E` and `R`. Attaches a non-enumerable continuation so heterogeneous job lists can still
 * be run and registered without casts; jobs not built with `defineJob` cannot be scheduled.
 */
export const defineJob = <const Name extends string, Params, Stats, E, R, ParamsEncoded = Params>(
  definition: JobDefinition<Name, Params, Stats, E, R, ParamsEncoded>,
): JobDefinition<Name, Params, Stats, E, R, ParamsEncoded> => {
  const defined = { ...definition };
  Object.defineProperty(defined, JOB_CONTINUATION, {
    value: ((invoke) => invoke(definition)) satisfies JobContinuation<R>,
    enumerable: false,
    configurable: true,
  });
  return defined;
};
