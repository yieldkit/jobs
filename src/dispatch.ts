import type { Schema } from 'effect';
import type { SqlError } from 'effect/sql';
import type { WorkflowEngine } from 'effect/workflow';

import { describeError, logWarning, redactLogString } from '@yieldkit/logger';
import { Cause, Clock, DateTime, Duration, Effect, Option, Predicate } from 'effect';
import { SecureRandom } from 'effect/crypto';

import type { ClusterConfigError, InvalidJobParams } from './errors.ts';
import type { AnyJobDefinition } from './job.ts';
import type { JobWorkflowPayload, WorkflowParamOverrides } from './workflow-schema.ts';

import { InvalidDispatch, InvalidRunKey } from './errors.ts';
import { omitUndefined } from './omit-undefined.ts';
import { decodeJobParams, pickApiParams } from './params.ts';
import { JobRequests } from './requests.ts';
import { resolveCallerRunKey, resolveRunKey } from './run-key.ts';
import { makeJobWorkflow } from './workflow-schema.ts';

/**
 * Options for `dispatchApiJobWorkflow`.
 */
export type DispatchApiJobWorkflowOptions = {
  /**
   * API-validated parameter overrides (already checked against `apiParams`).
   */
  readonly params: WorkflowParamOverrides;

  /**
   * Whether the workflow should force re-running an already completed cadence key.
   */
  readonly force: boolean;
};

/**
 * Result returned by `dispatchApiJobWorkflow`.
 */
export type DispatchedApiJobWorkflow = {
  /**
   * Random UUIDv4 from `SecureRandom`, the request row's id.
   */
  readonly requestId: string;

  /**
   * Deterministic Effect Workflow `executionId`, persisted in Cluster's messages.
   */
  readonly executionId: string;
};

/**
 * Starts a job for an external caller: keeps only the params `apiParams` allows, records a request
 * row linked to the workflow's deterministic `executionId`, and dispatches the durable workflow
 * with `trigger: 'api'` without waiting for it. If dispatch fails, the row is marked failed unless
 * a worker already picked it up; the worker can still finish it if the workflow did start. An
 * interrupted dispatch leaves the row pending. The request id is a UUIDv4 drawn from
 * `SecureRandom`. Check `isApiRequestable` and `findDisallowedApiParams` before calling it. An
 * `OnDemand` job fails with `InvalidRunKey`: start it with `dispatchJob`. An invalid cadence fails
 * with `ClusterConfigError` before anything is recorded. Params that do not decode over the job's
 * defaults fail with `InvalidJobParams` before anything is recorded.
 */
export const dispatchApiJobWorkflow = Effect.fn('dispatchApiJobWorkflow')(function* (
  definition: AnyJobDefinition,
  options: DispatchApiJobWorkflowOptions,
): Effect.fn.Return<
  DispatchedApiJobWorkflow,
  InvalidRunKey | InvalidJobParams | ClusterConfigError | SqlError.SqlError | Schema.SchemaError,
  JobRequests | WorkflowEngine.WorkflowEngine | SecureRandom.SecureRandom
> {
  const requests = yield* JobRequests;
  const random = yield* SecureRandom.SecureRandom;
  const workflow = makeJobWorkflow(definition);
  const nowMs = yield* Clock.currentTimeMillis;
  const runKey = yield* resolveRunKey(definition, nowMs, undefined);
  const allowedParams = pickApiParams(definition, options.params);
  yield* decodeJobParams(definition, allowedParams);
  const requestId = yield* random.uuidV4();
  const payload: JobWorkflowPayload = {
    runKey,
    trigger: 'api',
    params: allowedParams,
    force: options.force,
    requestId,
    attemptNonce: requestId,
  };
  const executionId = yield* workflow.executionId(payload);
  yield* requests.create({
    id: requestId,
    job: definition.name,
    params: allowedParams,
    force: options.force,
    executionId,
  });
  yield* workflow.execute(payload, { discard: true }).pipe(
    Effect.onError((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.void
        : requests
            .markDispatchFailed({
              id: requestId,
              job: definition.name,
              errorMessage: redactLogString(describeError(Cause.squash(cause))),
            })
            .pipe(
              Effect.catch((error) =>
                logWarning('job.dispatch.record_failed', {
                  job: definition.name,
                  requestId,
                  errorMessage: describeError(error),
                }),
              ),
            ),
    ),
  );
  return { requestId, executionId };
});

/**
 * Options for `dispatchJob`.
 */
export type DispatchJobOptions = {
  /**
   * The run's key, chosen by the caller, such as `welcome-email:user-42`: 1 to 200 printable ASCII
   * characters. Dispatching a key again joins the run already started under it.
   */
  readonly runKey: string;

  /**
   * Param overrides merged over the job's defaults. Validated before dispatch; every key is
   * allowed, since the caller is trusted application code.
   */
  readonly params?: Schema.JsonObject | undefined;

  /**
   * Instant the run must not start before. Takes precedence over `delay`.
   */
  readonly runAt?: DateTime.DateTime | undefined;

  /**
   * How long after dispatch the run may start.
   */
  readonly delay?: Duration.Input | undefined;
};

/**
 * Result of `dispatchJob`.
 */
export type DispatchedJob = {
  /**
   * The run key, as given.
   */
  readonly runKey: string;

  /**
   * Deterministic Effect Workflow `executionId`, derived from the job name and run key.
   */
  readonly executionId: string;
};

/**
 * Milliseconds of a `delay`, or `undefined` unless it is a finite duration of zero or more. A
 * number, and each number among the own values of an object or tuple, is checked before decoding,
 * since `Duration.fromInput` reads `NaN` as zero.
 */
const delayMillis = (delay: Duration.Input): number | undefined => {
  const parts = Predicate.isObjectOrArray(delay) ? Object.values(delay) : [delay];
  if (parts.some((part) => typeof part === 'number' && !Number.isFinite(part))) {
    return undefined;
  }

  const decoded = Duration.fromInput(delay);
  if (Option.isNone(decoded) || !Duration.isFinite(decoded.value)) {
    return undefined;
  }

  const millis = Duration.toMillis(decoded.value);
  return millis >= 0 ? millis : undefined;
};

/**
 * Epoch milliseconds of `options.runAt`, else of `delay` after `nowMs`, else `undefined`. Fails
 * with `InvalidDispatch` for a `delay` that is not a finite duration of zero or more, or an instant
 * that is not a valid date.
 */
const runAtMillis = (
  job: string,
  options: DispatchJobOptions,
  nowMs: number,
): Effect.Effect<number | undefined, InvalidDispatch> => {
  if (options.runAt === undefined && options.delay === undefined) {
    return Effect.undefined;
  }

  const delay = options.delay === undefined ? undefined : delayMillis(options.delay);
  if (options.runAt === undefined && delay === undefined) {
    return Effect.fail(
      new InvalidDispatch({
        job,
        message: 'delay must be a finite duration of zero or more.',
      }),
    );
  }

  const runAtMs = options.runAt === undefined ? nowMs + (delay ?? 0) : epochMillisOf(options.runAt);
  return runAtMs !== undefined && Option.isSome(DateTime.make(runAtMs))
    ? Effect.succeed(runAtMs)
    : Effect.fail(new InvalidDispatch({ job, message: 'runAt must be a valid date.' }));
};

/**
 * Epoch milliseconds of `runAt`, or `undefined` when it is not a `DateTime` holding a valid date.
 */
const epochMillisOf = (runAt: DateTime.DateTime): number | undefined => {
  if (!DateTime.isDateTime(runAt)) {
    return undefined;
  }

  const ms = DateTime.toEpochMillis(runAt);
  return Number.isFinite(ms) ? ms : undefined;
};

/**
 * Starts a run of an `OnDemand` job under a caller-chosen run key, optionally delayed, and returns
 * without waiting for it. The workflow runs on a worker even if the caller exits. Each key is one
 * execution: dispatching it again, with any params, joins that execution instead of starting
 * another, so a retried request does not run the job twice. A failed run retries with backoff
 * (`retry.onDemandBase` to `retry.onDemandCap`) up to `retry.onDemandMaxFailures` failures; after
 * that, `runJob` with `trigger: 'manual'` and the same key runs it again. Runs carry `trigger:
 * 'schedule'`.
 *
 * Fails with `InvalidRunKey` for a job that is not `OnDemand` or a malformed key, with
 * `InvalidJobParams` when the params do not decode, and with `InvalidDispatch` for a `delay` that
 * is not a finite duration of zero or more or a `runAt` that is not a valid date, all before
 * anything is dispatched.
 */
export const dispatchJob = Effect.fn('dispatchJob')(function* (
  definition: AnyJobDefinition,
  options: DispatchJobOptions,
): Effect.fn.Return<
  DispatchedJob,
  InvalidRunKey | InvalidJobParams | InvalidDispatch,
  WorkflowEngine.WorkflowEngine
> {
  if (definition.cadence._tag !== 'OnDemand') {
    return yield* new InvalidRunKey({
      job: definition.name,
      message: `dispatchJob starts OnDemand jobs only; "${definition.name}" has a ${definition.cadence._tag} cadence.`,
    });
  }

  const nowMs = yield* Clock.currentTimeMillis;
  const runKey = yield* resolveCallerRunKey(definition.name, options.runKey);
  const params = options.params ?? {};
  yield* decodeJobParams(definition, params);
  const runAtMs = yield* runAtMillis(definition.name, options, nowMs);
  const payload: JobWorkflowPayload = omitUndefined({
    runKey,
    trigger: 'schedule' as const,
    params,
    force: false,
    requestId: null,
    attemptNonce: null,
    runAt:
      runAtMs !== undefined && runAtMs > nowMs
        ? DateTime.formatIso(DateTime.makeUnsafe(runAtMs))
        : undefined,
  });
  const workflow = makeJobWorkflow(definition);
  const executionId = yield* workflow.executionId(payload);
  yield* workflow.execute(payload, { discard: true });
  return { runKey, executionId };
});
