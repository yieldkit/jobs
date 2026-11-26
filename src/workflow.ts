import type { SecureRandom } from 'effect/crypto';
import type { WorkflowEngine } from 'effect/workflow';

import { Cause, Effect, Exit, Layer, Option } from 'effect';

import type { AnyJobDefinition, JobContext, JobDefinition, JobIdentity } from './job.ts';
import type { OwnedRun } from './ledger-types.ts';
import type { EncodedRunOutput } from './run-support.ts';
import type { WorkflowExecutionState } from './workflow-activities.ts';
import type {
  JobWorkflowPayload,
  OperatorJobParams,
  WorkflowJobOutcome,
  WorkflowParamOverrides,
} from './workflow-schema.ts';

import { WorkflowExecutionRound } from './activity.ts';
import { RunClaimLost } from './errors.ts';
import { JOB_CONTINUATION } from './job.ts';
import { JobRuns } from './ledger.ts';
import { withJobLogging } from './logging.ts';
import { decodeParams, pickApiParams } from './params.ts';
import {
  executeAndEncodeStats,
  resolveForce,
  runWithHeartbeat,
  summarizeEncodedStats,
} from './run-support.ts';
import {
  handleNonAcquiredClaim,
  registerInterruptFinalizer,
  runClaimRoundActivity,
  waitUntilRunAt,
} from './workflow-activities.ts';
import { sleepForScheduledRetry } from './workflow-retry.ts';
import {
  JobOutcomeSchema,
  JobWorkflowPayload as JobWorkflowPayloadSchema,
  MAX_CRASH_ATTEMPTS_TAG,
  WorkerJobParams,
  makeJobWorkflow,
  maxCrashAttemptsMessage,
} from './workflow-schema.ts';
import {
  settleCompletedRound,
  settleCrashCap,
  settleFailedRound,
  settleInvalidParams,
  isReplayedRoundOver,
} from './workflow-settle.ts';

export {
  JobOutcomeSchema,
  JobWorkflowPayloadSchema as JobWorkflowPayload,
  MAX_CRASH_ATTEMPTS_TAG,
  WorkerJobParams,
  makeJobWorkflow,
  maxCrashAttemptsMessage,
};
export type {
  JobWorkflow,
  OperatorJobParams,
  WorkflowJobOutcome,
  WorkflowParamOverrides,
} from './workflow-schema.ts';

/**
 * Resolves effective parameter overrides for a workflow execution, re-enforcing `apiParams` when
 * `trigger === 'api'` so a forged `api` payload cannot set operator-only parameters. A `manual`
 * payload is trusted as it is.
 */
const resolveWorkflowParamOverrides = <Name extends string, P, S, E, R, PE>(
  definition: JobDefinition<Name, P, S, E, R, PE>,
  payload: JobWorkflowPayload,
  operatorParams: OperatorJobParams,
): WorkflowParamOverrides => {
  const base = operatorParams[definition.name];
  const caller: WorkflowParamOverrides =
    payload.trigger === 'api' ? pickApiParams(definition, payload.params) : payload.params;
  return { ...base, ...caller };
};

/**
 * Runs a claimed job body with heartbeat fencing and structured job logging.
 */
const runTrackedJobBody = <Name extends string, P, S, E, R, PE>(args: {
  readonly definition: JobDefinition<Name, P, S, E, R, PE>;
  readonly runs: JobRuns['Service'];
  readonly owned: OwnedRun;
  readonly payload: JobWorkflowPayload;
  readonly executionId: string;
  readonly params: P;
  readonly round: number;
}) => {
  const ctx: JobContext<P> = {
    params: args.params,
    runKey: args.payload.runKey,
    trigger: args.payload.trigger,
    attempt: args.owned.attempt,
  };
  return runWithHeartbeat(
    args.runs,
    args.owned,
    executeAndEncodeStats(args.definition, ctx).pipe(
      Effect.provideService(WorkflowExecutionRound, { round: args.round }),
    ),
  ).pipe(
    withJobLogging({
      job: args.definition.name,
      jobRunId: `${args.definition.name}:${args.payload.runKey}:${String(args.owned.attempt)}`,
      trigger: args.payload.trigger,
      attempt: args.owned.attempt,
      fields: {
        ...args.definition.logFields,
        runKey: args.payload.runKey,
        executionId: args.executionId,
      },
      summarizeResult: (output) => summarizeEncodedStats(output.encoded),
    }),
  );
};

/**
 * Settles a round whose body ran (or whose claim was found lost) and returns its terminal outcome,
 * or `undefined` after a durable sleep when the workflow should run the next round. A completion
 * that could not be written counts as a failed round for a scheduled retry.
 */
const settleRound = Effect.fnUntraced(function* (args: {
  readonly definition: JobIdentity;
  readonly runs: JobRuns['Service'];
  readonly payload: JobWorkflowPayload;
  readonly owned: OwnedRun;
  readonly startedMs: number;
  readonly exit: Exit.Exit<EncodedRunOutput<unknown>, unknown>;
  readonly round: number;
  readonly state: WorkflowExecutionState;
  readonly counters: { failureCount: number };
  readonly priorFailures: number | undefined;
}): Effect.fn.Return<
  WorkflowJobOutcome | undefined,
  never,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance
> {
  if (Exit.isSuccess(args.exit)) {
    const completed = yield* settleCompletedRound({
      ...args,
      encodedStats: args.exit.value.encoded,
    });
    args.state.activeOwned = undefined;
    if (!completed.settled && (yield* sleepForScheduledRetry(args))) {
      return undefined;
    }

    args.state.terminalSettled = true;
    return completed.outcome;
  }

  const failed = yield* settleFailedRound({
    ...args,
    cause: args.exit.cause,
    failureCount: args.counters.failureCount,
  });
  args.state.activeOwned = undefined;
  args.counters.failureCount = failed.failureCount;
  if (failed.outcome !== undefined) {
    args.state.terminalSettled = true;
  }

  return failed.outcome;
});

/**
 * Executes one durable claim-run-settle round of a job workflow, returning either a terminal
 * `WorkflowJobOutcome` or `undefined` when the workflow slept durably and should run the next
 * round.
 */
const executeWorkflowRound = Effect.fnUntraced(function* <
  Name extends string,
  P,
  S,
  E,
  R,
  PE,
>(args: {
  readonly definition: JobDefinition<Name, P, S, E, R, PE>;
  readonly runs: JobRuns['Service'];
  readonly payload: JobWorkflowPayload;
  readonly executionId: string;
  readonly force: boolean;
  readonly params: P;
  readonly encodedParams: unknown;
  readonly round: number;
  readonly state: WorkflowExecutionState;
  readonly counters: { busyRetries: number; failureCount: number; claims: number };
}): Effect.fn.Return<
  WorkflowJobOutcome | undefined,
  never,
  R | WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance | SecureRandom.SecureRandom
> {
  const { claim: claimResult, replayed } = yield* runClaimRoundActivity({
    runs: args.runs,
    definition: args.definition,
    payload: args.payload,
    executionId: args.executionId,
    force: args.force,
    encodedParams: args.encodedParams,
    round: args.round,
  });
  if (claimResult._tag !== 'Acquired') {
    const skip = yield* handleNonAcquiredClaim({
      job: args.definition.name,
      cadence: args.definition.cadence,
      logFields: args.definition.logFields,
      payload: args.payload,
      claimResult,
      round: args.round,
      busyRetries: args.counters.busyRetries,
      retry: args.runs.policy.retry,
    });
    args.counters.busyRetries = skip.busyRetries;
    if (skip.outcome !== undefined) {
      args.state.terminalSettled = true;
    }

    return skip.outcome;
  }

  const owned: OwnedRun = {
    job: args.definition.name,
    runKey: args.payload.runKey,
    token: claimResult.token,
    attempt: claimResult.attempt,
  };
  const retries = claimResult.resumed ? (claimResult.retries ?? 0) : 0;
  // A lost claim reply can advance the ledger attempt several times within one round.
  const resumes = claimResult.attempt - args.counters.claims - retries;
  args.counters.claims += 1 + retries;
  // A replayed claim that lost the row belongs to a round that is over: its body never runs again.
  const roundOver = replayed && (yield* isReplayedRoundOver(args.runs, owned));
  if (
    roundOver &&
    (yield* sleepForScheduledRetry({ ...args, priorFailures: claimResult.failures }))
  ) {
    return undefined;
  }

  args.state.activeOwned = owned;

  if (!roundOver && claimResult.resumed && resumes > args.runs.policy.retry.maxCrashAttempts) {
    const crashOutcome = yield* settleCrashCap({ ...args, owned });
    args.state.activeOwned = undefined;
    args.state.terminalSettled = true;
    return crashOutcome;
  }

  const exit = roundOver
    ? Exit.fail(new RunClaimLost({ job: owned.job, runKey: owned.runKey }))
    : yield* Effect.exit(runTrackedJobBody({ ...args, owned }));
  if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
    return yield* Effect.interrupt;
  }

  return yield* settleRound({
    ...args,
    owned,
    startedMs: claimResult.startedMs,
    exit,
    priorFailures: claimResult.failures,
  });
});

/**
 * Builds the Effect `Layer` registering the durable workflow handler for one `JobDefinition`.
 */
export const makeJobWorkflowLayer = <Name extends string, P, S, E, R, PE>(
  definition: JobDefinition<Name, P, S, E, R, PE>,
): Layer.Layer<
  never,
  never,
  R | JobRuns | WorkflowEngine.WorkflowEngine | SecureRandom.SecureRandom
> => {
  const workflow = makeJobWorkflow(definition);
  return workflow.toLayer((payload, executionId) =>
    Effect.gen(function* () {
      const runs = yield* JobRuns;
      const maybeOperatorParams = yield* Effect.serviceOption(WorkerJobParams);
      const operatorParams = Option.getOrElse(maybeOperatorParams, (): OperatorJobParams => ({}));
      const mergedOverrides = resolveWorkflowParamOverrides(definition, payload, operatorParams);
      const decodedParamsResult = yield* decodeParams(
        definition.name,
        definition.params,
        mergedOverrides,
      ).pipe(Effect.result);
      if (decodedParamsResult._tag === 'Failure') {
        return yield* settleInvalidParams(
          definition.name,
          payload,
          decodedParamsResult.failure._tag,
          decodedParamsResult.failure.message,
        );
      }

      const { params, encoded: encodedParams } = decodedParamsResult.success;
      const force = resolveForce(definition.cadence, payload.trigger, payload.force);
      const state: WorkflowExecutionState = { activeOwned: undefined, terminalSettled: false };
      const counters = { busyRetries: 0, failureCount: 0, claims: 0 };
      yield* registerInterruptFinalizer({ runs, definition, payload, state });
      if (payload.runAt !== undefined) {
        yield* waitUntilRunAt(payload.runAt);
      }

      for (let round = 1; ; round += 1) {
        const terminal = yield* executeWorkflowRound({
          definition,
          runs,
          payload,
          executionId,
          force,
          params,
          encodedParams,
          round,
          state,
          counters,
        });
        if (terminal !== undefined) {
          return terminal;
        }
      }
    }),
  );
};

/**
 * Registers the durable workflow handler layer for a type-erased `AnyJobDefinition<R>` through its
 * rank-2 `JOB_CONTINUATION`, avoiding casts.
 */
export const makeAnyJobWorkflowLayer = <R>(
  definition: AnyJobDefinition<R>,
): Layer.Layer<
  never,
  never,
  R | JobRuns | WorkflowEngine.WorkflowEngine | SecureRandom.SecureRandom
> => {
  const continuation = definition[JOB_CONTINUATION];
  if (continuation === undefined) {
    return Layer.effectDiscard(
      Effect.die(
        new Error(
          `Job "${definition.name}" must be constructed with defineJob before registering.`,
        ),
      ),
    );
  }

  return continuation((concrete) => makeJobWorkflowLayer(concrete));
};

export {
  dispatchApiJobWorkflow,
  dispatchJob,
  type DispatchApiJobWorkflowOptions,
  type DispatchedApiJobWorkflow,
  type DispatchedJob,
  type DispatchJobOptions,
} from './dispatch.ts';
