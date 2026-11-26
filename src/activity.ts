import type { LogFields } from '@yieldkit/logger';

import { describeError, extractCauseSummary, redactLogString } from '@yieldkit/logger';
import { Cause, Context, Effect, Exit, Option, Schema } from 'effect';
import { Activity, WorkflowEngine } from 'effect/workflow';

import { withJobStage } from './logging.ts';

/**
 * Serializable, redacted error persisted in Effect Cluster's replies when a durable job stage fails
 * or dies. Keeps the error tag and a `retryAfterSeconds` hint across replays without letting raw
 * secrets into storage.
 */
export class JobStageError extends Schema.TaggedError<
  JobStageError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('JobStageError', {
  /**
   * Name of the durable stage that failed.
   */
  stage: Schema.String,

  /**
   * Redacted human-readable failure message.
   */
  message: Schema.String,

  /**
   * Original `_tag` of the typed error (for example `RateLimitedError`), or `null` when untagged.
   */
  errorTag: Schema.NullOr(Schema.String),

  /**
   * `true` when the failure originated from an unhandled defect (`Cause.Die`).
   */
  isDefect: Schema.Boolean,

  /**
   * Finite `retryAfterSeconds` field of the original error, whatever its tag, or `null`.
   */
  retryAfterSeconds: Schema.NullOr(Schema.Finite),
}) {}

/**
 * Structural shape of any error carrying a finite `retryAfterSeconds` field, such as a rate-limit
 * error from an HTTP client.
 */
const RetryAfterShape = Schema.Struct({
  /**
   * Finite retry delay in seconds.
   */
  retryAfterSeconds: Schema.Finite,
});

/**
 * Per-workflow execution state: the active durable retry round (`1`-based). Provided by
 * `workflow.ts` so `durableStage` runs a stage under `Activity.CurrentAttempt = round` and reads
 * its results from earlier rounds.
 */
export type WorkflowRoundState = {
  /**
   * 1-based durable retry round passed as `Activity.CurrentAttempt` to the stage's activity.
   */
  readonly round: number;
};

/**
 * Context service carrying `WorkflowRoundState` into durable job stages. Optional so jobs can also
 * execute directly inside unit tests outside a workflow engine.
 */
export class WorkflowExecutionRound extends Context.Service<
  WorkflowExecutionRound,
  WorkflowRoundState
>()('@yieldkit/jobs/WorkflowExecutionRound') {}

/**
 * Returns the first typed failure in `cause` when it is a `JobStageError`, or `undefined` when the
 * cause holds no typed failure or its first one is another error.
 */
export const findJobStageError = (cause: Cause.Cause<unknown>): JobStageError | undefined =>
  Cause.findErrorOption(cause).pipe(Option.filter(Schema.is(JobStageError)), Option.getOrUndefined);

/**
 * Returns the finite `retryAfterSeconds` field of the first typed failure in `cause`, whatever its
 * tag, or `undefined` when it has none.
 */
export const findRetryAfterSeconds = (cause: Cause.Cause<unknown>): number | undefined =>
  Cause.findErrorOption(cause).pipe(
    Option.filter(Schema.is(RetryAfterShape)),
    Option.map((error) => error.retryAfterSeconds),
    Option.getOrUndefined,
  );

/**
 * Converts a non-interrupt failure or defect cause into a redacted `JobStageError` safe to persist
 * in Effect Cluster's replies.
 */
export const toJobStageError = (stage: string, cause: Cause.Cause<unknown>): JobStageError => {
  const stageError = findJobStageError(cause);
  if (stageError !== undefined) {
    return stageError;
  }

  const summary = extractCauseSummary(cause);
  const rawMessage = summary?.message ?? describeError(Cause.squash(cause));
  return new JobStageError({
    stage,
    message: redactLogString(rawMessage),
    errorTag: summary?.tag ?? null,
    isDefect: summary?.kind === 'die',
    retryAfterSeconds: findRetryAfterSeconds(cause) ?? null,
  });
};

/**
 * Maps typed errors and unhandled defects on `effect` into `JobStageError` while letting
 * interruptions propagate untouched.
 */
const mapStageFailures = <A, E, R>(
  stage: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, JobStageError, R> =>
  effect.pipe(
    Effect.mapError((error) => toJobStageError(stage, Cause.fail(error))),
    Effect.catchDefect((defect) => Effect.fail(toJobStageError(stage, Cause.die(defect)))),
  );

/**
 * Options for `durableStage`.
 */
export type DurableStageOptions<A, Encoded, E, R> = {
  /**
   * Stable stage name within the job (used as both the `Activity` name and `withJobStage` label).
   */
  readonly name: string;

  /**
   * Schema encoding and decoding the stage's return value for durable checkpointing in Effect
   * Cluster's replies.
   */
  readonly schema: Schema.Codec<A, Encoded>;

  /**
   * Stage work effect.
   */
  readonly execute: Effect.Effect<A, E, R>;

  /**
   * Optional structured log fields attached to `withJobStage`.
   */
  readonly fields?: LogFields | undefined;
};

/**
 * The stored reply of a stage attempt that never ran because its round failed before reaching it.
 */
const notRunInRound = (stage: string): JobStageError =>
  new JobStageError({
    stage,
    message: 'The stage did not run in this round.',
    errorTag: null,
    isDefect: false,
    retryAfterSeconds: null,
  });

/**
 * Activity names the job workflow uses itself (`N` is a round number), and old journals may still
 * hold. A stage under one of these names would read the workflow's journaled replies.
 */
const RESERVED_STAGE_NAME =
  /^(?:(?:claim-round|claim-deadline|settle-completed|settle-failed|settle-skipped|settle-crash-cap|retry-later-clock|retry-deadline)-\d+|settle-invalid-params|run-at-clock|DurableClock\/.*)$/u;

/**
 * Whether `name` is reserved for the job workflow's own activities and clocks: `claim-round-N`,
 * `claim-deadline-N`, `settle-completed-N`, `settle-failed-N`, `settle-skipped-N`,
 * `settle-crash-cap-N`, `retry-later-clock-N`, `retry-deadline-N`, `settle-invalid-params`,
 * `run-at-clock`, and anything starting with `DurableClock/`.
 */
export const isReservedStageName = (name: string): boolean => RESERVED_STAGE_NAME.test(name);

/**
 * The defect a stage under a reserved name fails with, before anything runs.
 */
const reservedStageName = (stage: string): JobStageError =>
  new JobStageError({
    stage,
    message: `Stage name "${stage}" is reserved for the job workflow; rename the stage.`,
    errorTag: null,
    isDefect: true,
    retryAfterSeconds: null,
  });

/**
 * Runs a named job stage as a durable Effect `Activity` when executing inside a `Workflow`, or as a
 * plain logged stage (`withJobStage`) when the job runs outside a workflow (`runJob`, unit tests).
 *
 * Inside a workflow, in durable retry round `round`:
 *
 * - The stage first reads its replies for attempts `1` to `round - 1`. A stored success is returned
 *   without running anything. An attempt the stage never reached stores a placeholder failure,
 *   once, so later rounds skip it the same way.
 * - Otherwise the stage runs under `Activity.CurrentAttempt = round`, so a durable retry genuinely
 *   re-runs a failed stage instead of replaying its stored failure.
 * - Typed errors and defects (excluding interrupts) are redacted and serialized as `JobStageError`.
 *
 * A stage named like one of the workflow's own activities (see `isReservedStageName`) fails at
 * once, inside or outside a workflow, with a `JobStageError` marked as a defect.
 */
export const durableStage = Effect.fnUntraced(function* <A, Encoded, E, R>(
  options: DurableStageOptions<A, Encoded, E, R>,
): Effect.fn.Return<A, E | JobStageError, R> {
  if (isReservedStageName(options.name)) {
    return yield* reservedStageName(options.name);
  }

  const engine = yield* Effect.serviceOption(WorkflowEngine.WorkflowEngine);
  const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance);
  const roundState = yield* Effect.serviceOption(WorkflowExecutionRound);
  const logged = options.execute.pipe(withJobStage(options.name, options.fields));
  if (Option.isNone(engine) || Option.isNone(instance)) {
    return yield* logged;
  }

  const round = Option.isSome(roundState) ? roundState.value.round : 1;
  /**
   * The stage's activity for `attempt`, running `execute` unless the journal holds its reply.
   */
  const runAttempt = <E2>(execute: Effect.Effect<A, E2, R>, attempt: number) =>
    Activity.make({
      name: options.name,
      success: options.schema,
      error: JobStageError,
      execute: mapStageFailures(options.name, execute),
    }).pipe(
      Effect.provideService(Activity.CurrentAttempt, attempt),
      Effect.provideService(WorkflowEngine.WorkflowEngine, engine.value),
      Effect.provideService(WorkflowEngine.WorkflowInstance, instance.value),
    );

  for (let attempt = 1; attempt < round; attempt += 1) {
    const earlier = yield* Effect.exit(
      runAttempt(Effect.fail(notRunInRound(options.name)), attempt),
    );
    if (Exit.isSuccess(earlier)) {
      return earlier.value;
    }

    if (Cause.hasInterrupts(earlier.cause) || Cause.hasDies(earlier.cause)) {
      return yield* Effect.failCause(earlier.cause);
    }
  }

  return yield* runAttempt(logged, round);
});
