import { Schema } from 'effect';

/**
 * Raised before a run is claimed when param overrides (from `runJob`, the scheduler's `params` or
 * an API request) fail validation against the job's `params.schema`, or when `parseSetFlags` gets a
 * malformed `key=value` flag.
 */
export class InvalidJobParams extends Schema.TaggedError<
  InvalidJobParams,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('InvalidJobParams', {
  /**
   * Name of the job whose parameters failed validation.
   */
  job: Schema.String,

  /**
   * Human-readable summary listing the schema or flag validation issues.
   */
  message: Schema.String,
}) {}

/**
 * Raised before anything is dispatched or claimed when a caller-chosen run key is missing,
 * malformed, or given for a job whose run keys come from its cadence. Only `OnDemand` jobs take a
 * run key, and they always need one.
 */
export class InvalidRunKey extends Schema.TaggedError<
  InvalidRunKey,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('InvalidRunKey', {
  /**
   * Name of the job.
   */
  job: Schema.String,

  /**
   * What is wrong with the run key.
   */
  message: Schema.String,
}) {}

/**
 * Raised by `dispatchJob` before anything is dispatched when `delay` is not a finite duration of
 * zero or more, or `runAt` (or the instant `delay` after now) is not a valid date.
 */
export class InvalidDispatch extends Schema.TaggedError<
  InvalidDispatch,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('InvalidDispatch', {
  /**
   * Name of the job.
   */
  job: Schema.String,

  /**
   * What is wrong with the dispatch options.
   */
  message: Schema.String,
}) {}

/**
 * Raised when `dryRun: true` is requested for a job that does not declare a `dryRun` layer.
 */
export class DryRunUnsupported extends Schema.TaggedError<
  DryRunUnsupported,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('DryRunUnsupported', {
  /**
   * Name of the job that lacks a `dryRun` layer.
   */
  job: Schema.String,
}) {}

/**
 * `_tag` of `RunClaimLost`, also written as the `errorTag` of outcomes for runs that lost their
 * claim (including a fenced settle that found another owner, where no `RunClaimLost` is raised).
 * Compare against this rather than a string literal.
 */
export const RUN_CLAIM_LOST_TAG = 'RunClaimLost';

/**
 * Raised when an active run loses ownership of its `(job, run_key)` lease—either because a
 * heartbeat returned `false` or failed for a full lease duration.
 */
export class RunClaimLost extends Schema.TaggedError<
  RunClaimLost,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()(RUN_CLAIM_LOST_TAG, {
  /**
   * Job name whose claim was lost.
   */
  job: Schema.String,

  /**
   * Ledger run key whose claim was lost.
   */
  runKey: Schema.String,
}) {}

/**
 * Redacted failure summary recorded on the ledger, the outcome and the request when a run loses its
 * `(job, run_key)` claim, whether a heartbeat lost it or a fenced settle found another owner.
 */
export const RUN_CLAIM_LOST_MESSAGE = 'lost its run claim';

/**
 * Raised when the scheduler's `only` option names one or more jobs that are not in its job list.
 */
export class UnknownJobError extends Schema.TaggedError<
  UnknownJobError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('UnknownJobError', {
  /**
   * Unknown job names passed in `only`.
   */
  names: Schema.Array(Schema.String),
}) {}

/**
 * Raised when cluster, schedule or policy configuration is invalid: a `ShardingConfig` that lacks
 * or misassigns a shard group the jobs need, a job name too long for Cluster's shard ids, a cadence
 * interval that does not divide an hour or a day, a policy duration that is not positive or a count
 * that is not a positive integer.
 */
export class ClusterConfigError extends Schema.TaggedError<
  ClusterConfigError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('ClusterConfigError', {
  /**
   * Human-readable explanation of the configuration error.
   */
  message: Schema.String,
}) {}

/**
 * Which migration history `JobsNotMigratedError` found behind. The library only checks its own;
 * your Cluster's storage is yours to migrate and check.
 */
export const JobsStorageComponent = Schema.Literals(['jobs']);

/**
 * A migration history checked by the runtime layers.
 */
export type JobsStorageComponent = typeof JobsStorageComponent.Type;

/**
 * Raised when a runtime layer starts against a database whose tables are missing or older than the
 * installed code expects. Run `JobStorage.layerMigrations` (or `Migrator` with
 * `JobStorage.migrations`) before starting workers and clients. A newer history than the code knows
 * is accepted.
 */
export class JobsNotMigratedError extends Schema.TaggedError<
  JobsNotMigratedError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('JobsNotMigratedError', {
  /**
   * Which migration history is behind.
   */
  component: JobsStorageComponent,

  /**
   * History table that was checked, as written in SQL (schema-qualified for the library's own).
   */
  table: Schema.String,

  /**
   * Highest migration id recorded in the history, `0` when the table does not exist.
   */
  found: Schema.Int,

  /**
   * Highest migration id the installed code ships.
   */
  expected: Schema.Int,

  /**
   * Operator message naming the table and pointing at `migrate()`.
   */
  message: Schema.String,
}) {}

/**
 * Raised by `JobRuns.page` when the continuation cursor is malformed.
 */
export class InvalidRunCursor extends Schema.TaggedError<
  InvalidRunCursor,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('InvalidRunCursor', {
  /**
   * Description of the invalid cursor.
   */
  message: Schema.String,
}) {}
