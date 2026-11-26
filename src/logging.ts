import type { LogFields } from '@yieldkit/logger';
import type { LogLevel } from 'effect';

import {
  compactLogFields,
  extractErrorFields,
  logErrorCause,
  logWithFields,
  withLogContext,
} from '@yieldkit/logger';
import { Cause, Clock, Effect, Exit, Schema } from 'effect';
import { SecureRandom } from 'effect/crypto';

import type { JobTrigger } from './job.ts';

export type { JobTrigger } from './job.ts';

/**
 * Fields attached to every log line inside one job run.
 */
export type JobLogContext = {
  /**
   * Stable job name, such as `sync-orders`.
   */
  readonly job: string;

  /**
   * Correlation id for this run. Generated when omitted.
   */
  readonly jobRunId?: string | undefined;

  /**
   * What started the run.
   */
  readonly trigger?: JobTrigger | undefined;

  /**
   * 1-based attempt counter for a scheduled loop.
   */
  readonly attempt?: number | undefined;

  /**
   * Extra annotations copied onto every line in the run.
   */
  readonly fields?: LogFields | undefined;
};

/**
 * Options for wrapping one job execution with start, completion, and failure logs.
 */
export type JobLoggingOptions<A = unknown> = JobLogContext & {
  /**
   * Severity of `job.started`. Defaults to `Debug`, so the default minimum hides it.
   */
  readonly startLevel?: LogLevel.Severity | undefined;

  /**
   * Severity of `job.completed`. Defaults to `Info`.
   */
  readonly completeLevel?: LogLevel.Severity | undefined;

  /**
   * Severity of `job.skipped`. Defaults to `Info`.
   */
  readonly skipLevel?: LogLevel.Severity | undefined;

  /**
   * Trace span name. Defaults to `job.${job}`.
   */
  readonly spanName?: string | undefined;

  /**
   * Log span name. Defaults to `job.run`.
   */
  readonly logSpanName?: string | undefined;

  /**
   * When this returns true, a successful result is logged as `job.skipped` instead of completed.
   */
  readonly isSkipped?: ((result: A) => boolean) | undefined;

  /**
   * Extra fields copied onto the completion or skip line.
   */
  readonly summarizeResult?: ((result: A) => LogFields) | undefined;
};

/**
 * Allocates a new random UUIDv4 run id from `SecureRandom`. Callers that already have one should
 * pass it instead.
 */
export const generateJobRunId: Effect.Effect<string, never, SecureRandom.SecureRandom> =
  SecureRandom.SecureRandom.use((random) => random.uuidV4());

/**
 * Drops omitted context fields so they are not written as `undefined`.
 */
const buildJobAnnotations = (context: JobLogContext, jobRunId: string): LogFields =>
  compactLogFields({
    ...compactLogFields(context.fields ?? {}),
    job: context.job,
    jobRunId,
    trigger: context.trigger,
    attempt: context.attempt,
  });

/**
 * Annotates logs inside `effect` with the job name, run id, trigger, attempt and `fields`. Does not
 * emit start or completion events. Requires `SecureRandom` to generate a run id when `jobRunId` is
 * omitted.
 */
export const withJobContext =
  (
    context: JobLogContext,
  ): (<A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R | SecureRandom.SecureRandom>) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | SecureRandom.SecureRandom> =>
    Effect.gen(function* () {
      const jobRunId = context.jobRunId ?? (yield* generateJobRunId);
      return yield* effect.pipe(Effect.annotateLogs(buildJobAnnotations(context, jobRunId)));
    });

/**
 * Names one step inside a job. Adds a `stage` annotation, a log span, and a trace span.
 */
export const withJobStage =
  (
    stage: string,
    fields?: LogFields,
  ): (<A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    effect.pipe(
      withLogContext({ stage }),
      withLogContext(fields ?? {}),
      Effect.withLogSpan(`stage.${stage}`),
      Effect.withSpan(`job.stage.${stage}`, { attributes: { stage } }),
    );

/**
 * Structural shape matching `JobStageError` for typed log extraction without a circular import from
 * `activity.ts`.
 */
const JobStageErrorShape = Schema.TaggedStruct('JobStageError', {
  /**
   * Name of the durable stage that failed.
   */
  stage: Schema.String,

  /**
   * Redacted human-readable failure message.
   */
  message: Schema.String,

  /**
   * Original `_tag` of the underlying error, or `null`.
   */
  errorTag: Schema.NullOr(Schema.String),

  /**
   * Whether the failure originated as a defect.
   */
  isDefect: Schema.Boolean,
});

/**
 * Failure annotations shared by a typed error and a defect. Omitted summary fields are dropped.
 */
const buildFailureFields = (cause: Cause.Cause<unknown>, durationMs: number): LogFields => {
  const failReason = cause.reasons.find(Cause.isFailReason);
  if (failReason !== undefined && Schema.is(JobStageErrorShape)(failReason.error)) {
    return compactLogFields({
      status: 'failed',
      durationMs,
      isDefect: failReason.error.isDefect,
      errorTag: failReason.error.errorTag ?? 'JobStageError',
      errorMessage: failReason.error.message,
      stage: failReason.error.stage,
    });
  }

  const reason = failReason ?? cause.reasons.find(Cause.isDieReason);
  const isDefect = reason !== undefined && Cause.isDieReason(reason);
  const error = isDefect ? reason.defect : failReason?.error;
  return compactLogFields({
    status: 'failed',
    durationMs,
    isDefect,
    ...(reason === undefined ? {} : extractErrorFields(error)),
  });
};

/**
 * Logs `job.started`, then `job.completed`, `job.skipped`, `job.interrupted`, or `job.failed`.
 * Duration comes from the Effect clock. A missing `jobRunId` is generated from `SecureRandom`,
 * which the returned effect therefore requires.
 */
export const withJobLogging =
  <A>(
    options: JobLoggingOptions<A>,
  ): (<E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R | SecureRandom.SecureRandom>) =>
  <E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | SecureRandom.SecureRandom> =>
    Effect.gen(function* () {
      const jobRunId = options.jobRunId ?? (yield* generateJobRunId);
      const annotations = buildJobAnnotations(options, jobRunId);
      const startedMs = yield* Clock.currentTimeMillis;
      const startLevel = options.startLevel ?? 'Debug';
      const completeLevel = options.completeLevel ?? 'Info';
      const skipLevel = options.skipLevel ?? 'Info';
      const spanName = options.spanName ?? `job.${options.job}`;
      const logSpanName = options.logSpanName ?? 'job.run';

      const tracked = Effect.gen(function* () {
        yield* logWithFields(startLevel, 'job.started', { status: 'started' });

        return yield* effect.pipe(
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              const durationMs = Math.max(0, (yield* Clock.currentTimeMillis) - startedMs);

              if (Exit.isSuccess(exit)) {
                const summary = options.summarizeResult?.(exit.value);
                const skipped = options.isSkipped?.(exit.value) ?? false;
                yield* logWithFields(
                  skipped ? skipLevel : completeLevel,
                  skipped ? 'job.skipped' : 'job.completed',
                  {
                    ...summary,
                    status: skipped ? 'skipped' : 'completed',
                    durationMs,
                  },
                );
                return;
              }

              if (Cause.hasInterruptsOnly(exit.cause)) {
                yield* logWithFields('Warn', 'job.interrupted', {
                  status: 'interrupted',
                  durationMs,
                });
                return;
              }

              yield* logErrorCause(
                'job.failed',
                exit.cause,
                buildFailureFields(exit.cause, durationMs),
              );
            }),
          ),
        );
      });
      return yield* tracked.pipe(
        Effect.annotateLogs(annotations),
        Effect.withLogSpan(logSpanName),
        Effect.withSpan(spanName, { attributes: annotations }),
      );
    });
