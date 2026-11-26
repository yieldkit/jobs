import { redactLogString } from '@yieldkit/logger';
import { Context, Layer, Schema } from 'effect';
import { ClusterSchema } from 'effect/cluster';
import { Workflow } from 'effect/workflow';

import type { AnyJobDefinition } from './job.ts';
import type { RunClaim } from './ledger-types.ts';
import type { SkipReason } from './run.ts';

import { JobTrigger } from './job.ts';
import { omitUndefined } from './omit-undefined.ts';

/**
 * Parameter overrides passed to a single job workflow. Values are JSON because they travel inside
 * the persisted workflow payload; each job's `params.schema` validates them before claiming.
 */
export type WorkflowParamOverrides = { readonly [key: string]: Schema.Json };

/**
 * Operator param overrides configured on the worker (the scheduler's `params`), keyed by job name.
 */
export type OperatorJobParams = Readonly<Record<string, WorkflowParamOverrides>>;

/**
 * Context service carrying the worker's operator params into durable workflow handlers. Defaults to
 * an empty map when not provided.
 */
export class WorkerJobParams extends Context.Service<WorkerJobParams, OperatorJobParams>()(
  '@yieldkit/jobs/WorkerJobParams',
) {
  /**
   * Layer supplying `params` to workflow handlers.
   */
  static readonly layer = (params: OperatorJobParams = {}): Layer.Layer<WorkerJobParams> =>
    Layer.succeed(this, params);
}

/**
 * ISO-8601 UTC instant such as `2026-11-26T09:30:00.000Z`, the `runAt` format `dispatchJob` writes.
 */
const IsoInstant = Schema.String.check(
  Schema.makeFilter(
    (value: string) =>
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?Z$/u.test(value) &&
      Number.isFinite(Date.parse(value)),
    { expected: 'an ISO-8601 UTC instant' },
  ),
);

/**
 * Serializable payload of every job `Workflow`. Its field names are persisted in Cluster messages.
 */
export const JobWorkflowPayload = Schema.Struct({
  /**
   * Ledger run key (`YYYY-MM-DD`, `YYYY-MM-DDTHH:MMZ`, `'once'`, or an `OnDemand` caller's key).
   */
  runKey: Schema.String,

  /**
   * How this execution was started.
   */
  trigger: JobTrigger,

  /**
   * Caller parameter overrides (re-filtered through `apiParams` when `trigger === 'api'`).
   */
  params: Schema.JsonObject,

  /**
   * Whether a completed `(job, runKey)` row may be re-run.
   */
  force: Schema.Boolean,

  /**
   * Linked request id when `trigger === 'api'`, else `null`.
   */
  requestId: Schema.NullOr(Schema.String),

  /**
   * Optional uniqueness nonce (`requestId` for API runs, a fresh UUID for other forced runs, `null`
   * for idempotent scheduled runs).
   */
  attemptNonce: Schema.NullOr(Schema.String),

  /**
   * ISO-8601 UTC instant the run must not start before; the workflow waits durably until then. Set
   * by `dispatchJob` for a delayed run, missing otherwise. Any other string fails decoding, so the
   * job never runs without its wait.
   */
  runAt: Schema.optional(IsoInstant),
});

/**
 * Decoded `JobWorkflowPayload` type.
 */
export type JobWorkflowPayload = typeof JobWorkflowPayload.Type;

/**
 * Serializable `SkipReason` schema persisted in Cluster replies.
 */
const SkipReasonSchema = Schema.Union([
  /**
   * Variant indicating the cadence key is already completed.
   */
  Schema.TaggedStruct('AlreadyCompleted', {}),
  /**
   * Variant indicating another worker holds a live lease on the key.
   */
  Schema.TaggedStruct('Busy', {}),
  /**
   * Variant indicating scheduled retry backoff has not expired yet.
   */
  Schema.TaggedStruct('RetryLater', {
    /**
     * ISO-8601 UTC timestamp when the key becomes eligible for retry.
     */
    at: Schema.String,
  }),
  /**
   * Variant indicating the key exhausted its scheduled retry budget.
   */
  Schema.TaggedStruct('Exhausted', {}),
]);

/**
 * Serializable `JobOutcome` schema returned by every job `Workflow`.
 */
export const JobOutcomeSchema = Schema.Union([
  Schema.Struct({
    /**
     * Registered job name.
     */
    job: Schema.String,

    /**
     * Deterministic cadence key executed by this workflow.
     */
    runKey: Schema.String,

    /**
     * How the run was started.
     */
    trigger: JobTrigger,

    /**
     * Elapsed duration in milliseconds measured via the Effect clock.
     */
    elapsedMs: Schema.Finite,

    /**
     * Terminal status discriminant for completed runs.
     */
    status: Schema.Literal('completed'),

    /**
     * 1-based attempt number recorded in the ledger.
     */
    attempt: Schema.Int,

    /**
     * Whether the run executed in dry-run mode (always `false` in cluster workflows).
     */
    dryRun: Schema.Boolean,

    /**
     * Schema-encoded completion stats returned by the job.
     */
    stats: Schema.Unknown,
  }),
  Schema.Struct({
    /**
     * Registered job name.
     */
    job: Schema.String,

    /**
     * Deterministic cadence key skipped by this workflow.
     */
    runKey: Schema.String,

    /**
     * How the run was started.
     */
    trigger: JobTrigger,

    /**
     * Elapsed duration in milliseconds measured via the Effect clock.
     */
    elapsedMs: Schema.Finite,

    /**
     * Terminal status discriminant for skipped runs.
     */
    status: Schema.Literal('skipped'),

    /**
     * Structured reason explaining why the run was skipped.
     */
    reason: SkipReasonSchema,
  }),
  Schema.Struct({
    /**
     * Registered job name.
     */
    job: Schema.String,

    /**
     * Deterministic cadence key that failed.
     */
    runKey: Schema.String,

    /**
     * How the run was started.
     */
    trigger: JobTrigger,

    /**
     * Elapsed duration in milliseconds measured via the Effect clock.
     */
    elapsedMs: Schema.Finite,

    /**
     * Terminal status discriminant for failed runs.
     */
    status: Schema.Literal('failed'),

    /**
     * 1-based attempt number recorded in the ledger (`0` for pre-claim failures).
     */
    attempt: Schema.Int,

    /**
     * Redacted failure summary safe for logs, API responses and Cluster replies.
     */
    errorMessage: Schema.String,

    /**
     * Whether the failure originated as an unhandled defect.
     */
    isDefect: Schema.Boolean,

    /**
     * Whether the failure resulted from an interruption or lost lease claim.
     */
    interrupted: Schema.Boolean,

    /**
     * Optional original `_tag` of the typed domain error.
     */
    errorTag: Schema.optional(Schema.String),

    /**
     * Optional `retryAfterSeconds` hint copied from the typed error.
     */
    retryAfterSeconds: Schema.optional(Schema.Finite),
  }),
]);

/**
 * Decoded workflow outcome matching `JobOutcomeSchema`.
 */
export type WorkflowJobOutcome = typeof JobOutcomeSchema.Type;

/**
 * Serializable result of the `claim-round` activity.
 */
export const ClaimActivityResult = Schema.Union([
  /**
   * Variant indicating the workflow holds the run lease.
   */
  Schema.TaggedStruct('Acquired', {
    /**
     * Fencing claim token persisted in the ledger.
     */
    token: Schema.String,

    /**
     * 1-based attempt number recorded in the ledger.
     */
    attempt: Schema.Int,

    /**
     * `true` when resuming a prior crashed execution with the same `executionId`.
     */
    resumed: Schema.Boolean,

    /**
     * Epoch millisecond timestamp captured when the claim activity started.
     */
    startedMs: Schema.Finite,

    /**
     * Retries within this activity whose earlier claims may have committed.
     */
    retries: Schema.optional(Schema.Int),

    /**
     * Failures the key already had when claimed; missing from replies of earlier releases.
     */
    failures: Schema.optional(Schema.Int),
  }),
  /**
   * Variant indicating another replica holds a live lease.
   */
  Schema.TaggedStruct('Busy', {
    /**
     * Elapsed milliseconds for the skipped claim check.
     */
    elapsedMs: Schema.Finite,
  }),
  /**
   * Variant indicating the cadence key already completed.
   */
  Schema.TaggedStruct('Completed', {
    /**
     * Elapsed milliseconds for the skipped claim check.
     */
    elapsedMs: Schema.Finite,
  }),
  /**
   * Variant indicating this exact `executionId` already settled in the ledger.
   */
  Schema.TaggedStruct('AlreadySettled', {
    /**
     * Elapsed milliseconds for the skipped claim check.
     */
    elapsedMs: Schema.Finite,
  }),
  /**
   * Variant indicating the key is still within its retry backoff window.
   */
  Schema.TaggedStruct('RetryLater', {
    /**
     * ISO-8601 UTC timestamp when the backoff window expires.
     */
    at: Schema.String,

    /**
     * Elapsed milliseconds for the skipped claim check.
     */
    elapsedMs: Schema.Finite,
  }),
  /**
   * Variant indicating the key exhausted its scheduled retry budget.
   */
  Schema.TaggedStruct('Exhausted', {
    /**
     * Elapsed milliseconds for the skipped claim check.
     */
    elapsedMs: Schema.Finite,
  }),
]);

/**
 * Decoded `ClaimActivityResult` type.
 */
export type ClaimActivityResult = typeof ClaimActivityResult.Type;

/**
 * Serializable result of a `settle` activity.
 */
export const SettleActivityResult = Schema.Struct({
  /**
   * `true` when `JobRuns.settle` matched our fencing claim token.
   */
  settled: Schema.Boolean,

  /**
   * Elapsed run duration in milliseconds.
   */
  elapsedMs: Schema.Finite,

  /**
   * Whether a failed round goes on to a durable retry, decided at settle time so a `Daily` deadline
   * is checked once. Missing in replies stored by earlier releases.
   */
  retries: Schema.optional(Schema.Boolean),
});

/**
 * Error tag recorded when the same workflow execution crashes and resumes more than
 * `policy.retry.maxCrashAttempts` times.
 */
export const MAX_CRASH_ATTEMPTS_TAG = 'MaxAttemptsExceeded';

/**
 * Error message recorded when the same workflow execution crashed and resumed more than
 * `maxCrashAttempts` times.
 */
export const maxCrashAttemptsMessage = (maxCrashAttempts: number): string =>
  `The run crashed and resumed more than ${String(maxCrashAttempts)} times without finishing.`;

/**
 * Error message recorded when a workflow is explicitly interrupted via `Workflow.interrupt`.
 */
export const INTERRUPTED_WORKFLOW_MESSAGE = 'The workflow was interrupted before the run finished.';

/**
 * Type of the durable Effect `Workflow` constructed for one job.
 */
export type JobWorkflow<Name extends string = string> = Workflow.Workflow<
  Name,
  typeof JobWorkflowPayload,
  typeof JobOutcomeSchema,
  typeof Schema.Never
>;

/**
 * Builds the Effect `Workflow` declaration for `definition`, annotated with its own
 * `ClusterSchema.ShardGroup` (`definition.name`) so workers assigned a subset of the jobs never
 * claim messages of the others. The workflow name and the idempotency key (`runKey`, or
 * `runKey:attemptNonce`) are persisted; changing them orphans in-flight executions.
 */
export const makeJobWorkflow = <Name extends string>(
  definition: Pick<AnyJobDefinition, 'name'> & { readonly name: Name },
): JobWorkflow<Name> =>
  Workflow.make(definition.name, {
    payload: JobWorkflowPayload,
    success: JobOutcomeSchema,
    error: Schema.Never,
    idempotencyKey: (payload) =>
      payload.attemptNonce === null ? payload.runKey : `${payload.runKey}:${payload.attemptNonce}`,
  }).annotate(ClusterSchema.ShardGroup, () => definition.name);

/**
 * Converts a non-acquired `ClaimActivityResult` into its public `SkipReason`.
 */
export const claimToSkipReason = (
  claim: Extract<
    ClaimActivityResult,
    { readonly _tag: 'Completed' | 'Busy' | 'RetryLater' | 'Exhausted' | 'AlreadySettled' }
  >,
): SkipReason => {
  switch (claim._tag) {
    case 'Completed':
    case 'AlreadySettled':
      return { _tag: 'AlreadyCompleted' };
    case 'Busy':
      return { _tag: 'Busy' };
    case 'RetryLater':
      return { _tag: 'RetryLater', at: claim.at };
    case 'Exhausted':
      return { _tag: 'Exhausted' };
    default: {
      const unexpected: never = claim;
      return unexpected;
    }
  }
};

/**
 * Encodes a ledger claim for the activity journal. An acquired claim records how many times the
 * activity retried it, defaulting to zero.
 */
export const encodeRunClaim = (
  claim: RunClaim,
  startedMs: number,
  retries = 0,
): ClaimActivityResult => {
  switch (claim._tag) {
    case 'Acquired':
      return {
        _tag: 'Acquired',
        token: claim.token,
        attempt: claim.attempt,
        resumed: claim.resumed,
        startedMs,
        retries,
        ...omitUndefined({ failures: claim.failures }),
      };
    case 'Busy':
      return { _tag: 'Busy', elapsedMs: 0 };
    case 'Completed':
      return { _tag: 'Completed', elapsedMs: 0 };
    case 'AlreadySettled':
      return { _tag: 'AlreadySettled', elapsedMs: 0 };
    case 'RetryLater':
      return { _tag: 'RetryLater', at: claim.at.toISOString(), elapsedMs: 0 };
    case 'Exhausted':
      return { _tag: 'Exhausted', elapsedMs: 0 };
    default: {
      const unexpected: never = claim;
      return unexpected;
    }
  }
};

/**
 * Arguments for `buildFailedOutcome`.
 */
export type BuildFailedOutcomeArgs = {
  /**
   * Job name.
   */
  readonly job: string;

  /**
   * Cadence ledger key.
   */
  readonly runKey: string;

  /**
   * Trigger origin.
   */
  readonly trigger: JobWorkflowPayload['trigger'];

  /**
   * Elapsed duration in milliseconds.
   */
  readonly elapsedMs: number;

  /**
   * 1-based attempt number (`0` for pre-claim failures).
   */
  readonly attempt: number;

  /**
   * Raw failure message (redacted before returning).
   */
  readonly errorMessage: string;

  /**
   * Whether the failure originated as a defect.
   */
  readonly isDefect: boolean;

  /**
   * Whether the failure was an interruption or lost lease claim.
   */
  readonly interrupted: boolean;

  /**
   * Optional typed error tag.
   */
  readonly errorTag: string | undefined;

  /**
   * Optional `Retry-After` delay in seconds.
   */
  readonly retryAfterSeconds: number | undefined;
};

/**
 * Builds a `failed` `WorkflowJobOutcome` matching `JobOutcomeSchema`.
 */
export const buildFailedOutcome = (args: BuildFailedOutcomeArgs): WorkflowJobOutcome => ({
  job: args.job,
  runKey: args.runKey,
  trigger: args.trigger,
  elapsedMs: args.elapsedMs,
  status: 'failed',
  attempt: args.attempt,
  errorMessage: redactLogString(args.errorMessage),
  isDefect: args.isDefect,
  interrupted: args.interrupted,
  ...omitUndefined({ errorTag: args.errorTag, retryAfterSeconds: args.retryAfterSeconds }),
});
