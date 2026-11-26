import type { Schema } from 'effect';
import type { SecureRandom } from 'effect/crypto';
import type { SqlError } from 'effect/sql';

import { logInfo, logWarning } from '@yieldkit/logger';
import { Clock, Effect, Exit, Layer } from 'effect';

import type {
  ClusterConfigError,
  InvalidJobParams,
  InvalidRunKey,
  RunClaimLost,
} from './errors.ts';
import type { AnyJobDefinition, JobContext, JobDefinition, JobTrigger } from './job.ts';
import type { OwnedRun, RunClaim, RunResult } from './ledger-types.ts';
import type { AnalyzedFailure, EncodedRunOutput } from './run-support.ts';

import { DryRunUnsupported, RUN_CLAIM_LOST_MESSAGE, RUN_CLAIM_LOST_TAG } from './errors.ts';
import { JOB_CONTINUATION } from './job.ts';
import { JobRuns } from './ledger.ts';
import { withJobLogging } from './logging.ts';
import { recordJobRunMetrics } from './metrics.ts';
import { decodeParams } from './params.ts';
import { resolveRunKey } from './run-key.ts';
import {
  analyzeFailureExit,
  annotationsOf,
  elapsedMillisSince,
  executeAndEncodeStats,
  resolveForce,
  retryTransientSql,
  runWithHeartbeat,
  summarizeEncodedStats,
} from './run-support.ts';

/**
 * Why a claimed run did not execute.
 */
export type SkipReason =
  | {
      /**
       * The `(job, runKey)` row is already `completed` and `force` was `false`.
       */
      readonly _tag: 'AlreadyCompleted';
    }
  | {
      /**
       * Another replica currently holds a live lease on `(job, runKey)`.
       */
      readonly _tag: 'Busy';
    }
  | {
      /**
       * A scheduled run encountered a `failed` key whose retry backoff expires at ISO timestamp
       * `at`.
       */
      readonly _tag: 'RetryLater';

      /**
       * ISO-8601 UTC timestamp when the key becomes eligible for scheduled retry.
       */
      readonly at: string;
    }
  | {
      /**
       * A scheduled run encountered a `failed` key whose `next_attempt_at` is `NULL`.
       */
      readonly _tag: 'Exhausted';
    };

/**
 * Non-failing outcome returned by `runJob`. Typed errors, defects, interruptions, and lost claims
 * inside the job body all settle into the `failed` variant.
 */
export type JobOutcome<Name extends string = string, Stats = unknown> = {
  /**
   * Job name.
   */
  readonly job: Name;

  /**
   * Cadence ledger key for this execution.
   */
  readonly runKey: string;

  /**
   * Trigger that started the run.
   */
  readonly trigger: JobTrigger;

  /**
   * Wall/clock elapsed duration in milliseconds measured via Effect `Clock`.
   */
  readonly elapsedMs: number;
} & (
  | {
      /**
       * Run finished successfully.
       */
      readonly status: 'completed';

      /**
       * 1-based attempt number (`0` for a dry run).
       */
      readonly attempt: number;

      /**
       * `true` when executed with `dryRun: true` (no claim and no ledger write).
       */
      readonly dryRun: boolean;

      /**
       * Decoded completion stats returned by the job.
       */
      readonly stats: Stats;
    }
  | {
      /**
       * Run skipped because `JobRuns.claim` did not acquire the key.
       */
      readonly status: 'skipped';

      /**
       * Structured reason explaining why the claim was skipped.
       */
      readonly reason: SkipReason;
    }
  | {
      /**
       * Run failed, died, was interrupted, or lost its lease claim.
       */
      readonly status: 'failed';

      /**
       * 1-based attempt number (`0` for a dry run).
       */
      readonly attempt: number;

      /**
       * Redacted failure summary (or `'lost its run claim'` when fence check fails).
       */
      readonly errorMessage: string;

      /**
       * `true` when the failure was an unmasked defect.
       */
      readonly isDefect: boolean;

      /**
       * `true` when interrupted or when the run lost its claim.
       */
      readonly interrupted: boolean;

      /**
       * `_tag` of the typed error, if any.
       */
      readonly errorTag: string | undefined;

      /**
       * Finite `retryAfterSeconds` field of the typed error, or `undefined`.
       */
      readonly retryAfterSeconds: number | undefined;
    }
);

/**
 * Options controlling one `runJob` execution.
 */
export type RunJobOptions = {
  /**
   * How this run was started.
   */
  readonly trigger: JobTrigger;

  /**
   * Re-runs a `completed` key. Defaults to `true` only for `Every` and `Cron` jobs with
   * non-`schedule` triggers unless explicitly passed.
   */
  readonly force?: boolean | undefined;

  /**
   * Run key of an `OnDemand` job, which needs one. Other jobs take their key from the clock and
   * fail with `InvalidRunKey` when given one.
   */
  readonly runKey?: string | undefined;

  /**
   * Partial param overrides merged over `definition.params.defaults` and validated before claim.
   */
  readonly params?: Schema.JsonObject | undefined;

  /**
   * Executes `definition.run` with `definition.dryRun` provided, without claiming or writing the
   * ledger. Fails with `DryRunUnsupported` if the job defines no `dryRun` layer.
   */
  readonly dryRun?: boolean | undefined;

  /**
   * Called with the run key once the `(job, runKey)` claim is acquired, before the job body runs.
   * Lets a caller link the ledger row even when the run is interrupted before `runJob` returns. Not
   * called for skipped claims or dry runs. It runs as part of the job body: if it throws, the run
   * settles `failed` as a defect and `runJob` returns that outcome.
   */
  readonly onClaimed?: ((runKey: string) => void) | undefined;
};

/**
 * Converts a non-`Acquired` `RunClaim` into its public `SkipReason`.
 */
const toSkipReason = (claim: Exclude<RunClaim, { readonly _tag: 'Acquired' }>): SkipReason => {
  switch (claim._tag) {
    case 'Completed':
    case 'AlreadySettled':
      return { _tag: 'AlreadyCompleted' };
    case 'Busy':
      return { _tag: 'Busy' };
    case 'RetryLater':
      return { _tag: 'RetryLater', at: claim.at.toISOString() };
    case 'Exhausted':
      return { _tag: 'Exhausted' };
    default: {
      const unexpected: never = claim;
      return unexpected;
    }
  }
};

/**
 * The `failed`-variant fields of a `JobOutcome` for an analyzed failure at `attempt`, without the
 * run identity and timing that every variant shares.
 */
const failedOutcomeFields = (
  failure: AnalyzedFailure,
  attempt: number,
): Omit<
  Extract<JobOutcome, { readonly status: 'failed' }>,
  'job' | 'runKey' | 'trigger' | 'elapsedMs'
> => ({
  status: 'failed',
  attempt,
  errorMessage: failure.errorMessage,
  isDefect: failure.isDefect,
  interrupted: failure.interrupted,
  errorTag: failure.errorTag,
  retryAfterSeconds: failure.retryAfterSeconds,
});

/**
 * Runs a dry run with `definition.dryRun` provided, without claiming or writing the ledger.
 */
const executeDryRun = Effect.fnUntraced(function* <Name extends string, P, S, E, R, PE>(
  definition: JobDefinition<Name, P, S, E, R, PE>,
  ctx: JobContext<P>,
  startedMs: number,
): Effect.fn.Return<JobOutcome<Name, S>, DryRunUnsupported, R | SecureRandom.SecureRandom> {
  const dryRunLayer = definition.dryRun;
  if (dryRunLayer === undefined) {
    return yield* new DryRunUnsupported({ job: definition.name });
  }

  const work = executeAndEncodeStats(definition, ctx).pipe(
    (effect) =>
      Effect.scoped(
        Layer.build(dryRunLayer).pipe(
          Effect.flatMap((context) => effect.pipe(Effect.provideContext(context))),
        ),
      ),
    withJobLogging({
      job: definition.name,
      jobRunId: `${definition.name}:${ctx.runKey}:0`,
      trigger: ctx.trigger,
      attempt: 0,
      fields: { ...definition.logFields, runKey: ctx.runKey, dryRun: true },
      summarizeResult: (output) => summarizeEncodedStats(output.encoded),
    }),
  );
  const exit = yield* Effect.exit(work);
  const base = {
    job: definition.name,
    runKey: ctx.runKey,
    trigger: ctx.trigger,
    elapsedMs: yield* elapsedMillisSince(startedMs),
  };
  const outcome: JobOutcome<Name, S> = Exit.isSuccess(exit)
    ? { ...base, status: 'completed', attempt: 0, dryRun: true, stats: exit.value.stats }
    : {
        ...base,
        ...failedOutcomeFields(analyzeFailureExit(definition, ctx.trigger, exit.cause), 0),
      };
  yield* recordJobRunMetrics({ ...outcome, leaseLost: false });
  return outcome;
});

/**
 * `JobOutcome` of a claimed run together with whether the run lost its lease, which metrics record
 * separately from the outcome status.
 */
type ClaimedOutcome<Name extends string, S> = {
  /**
   * Public outcome returned to the caller.
   */
  readonly outcome: JobOutcome<Name, S>;

  /**
   * `true` when the run lost its lease claim, either while running or because `settle` did not
   * match its fence token.
   */
  readonly leaseLost: boolean;
};

/**
 * Builds the `JobOutcome` for an `Exit` after settling (or failing to settle) in the ledger.
 */
const buildClaimedOutcome = <Name extends string, P, S, E, R, PE>(args: {
  readonly definition: JobDefinition<Name, P, S, E, R, PE>;
  readonly ctx: JobContext<P>;
  readonly exit: Exit.Exit<EncodedRunOutput<S>, E | Schema.SchemaError | RunClaimLost>;
  readonly settled: boolean;
  readonly elapsedMs: number;
}): ClaimedOutcome<Name, S> => {
  const { definition, ctx, exit, settled, elapsedMs } = args;
  const base = {
    job: definition.name,
    runKey: ctx.runKey,
    trigger: ctx.trigger,
    elapsedMs,
    attempt: ctx.attempt,
  };
  if (!settled) {
    return {
      leaseLost: true,
      outcome: {
        ...base,
        status: 'failed',
        errorMessage: RUN_CLAIM_LOST_MESSAGE,
        isDefect: false,
        interrupted: true,
        errorTag: RUN_CLAIM_LOST_TAG,
        retryAfterSeconds: undefined,
      },
    };
  }

  if (Exit.isSuccess(exit)) {
    return {
      leaseLost: false,
      outcome: { ...base, status: 'completed', dryRun: false, stats: exit.value.stats },
    };
  }

  const failure = analyzeFailureExit(definition, ctx.trigger, exit.cause);
  return {
    leaseLost: failure.isRunClaimLost,
    outcome: { ...base, ...failedOutcomeFields(failure, ctx.attempt) },
  };
};

/**
 * Claims `(definition.name, runKey)`, runs `definition.run` raced with heartbeats inside
 * `uninterruptibleMask`, settles the ledger atomically, and records metrics before leaving the
 * uninterruptible region.
 */
const executeClaimed = <Name extends string, P, S, E, R, PE>(args: {
  readonly definition: JobDefinition<Name, P, S, E, R, PE>;
  readonly ctxBase: Omit<JobContext<P>, 'attempt'>;
  readonly encodedParams: PE;
  readonly force: boolean;
  readonly startedMs: number;
  readonly onClaimed: ((runKey: string) => void) | undefined;
}): Effect.Effect<
  JobOutcome<Name, S>,
  SqlError.SqlError,
  R | JobRuns | SecureRandom.SecureRandom
> =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const { definition, ctxBase, encodedParams, force, startedMs, onClaimed } = args;
      const runs = yield* JobRuns;
      const claim = yield* runs.claim({
        job: definition.name,
        runKey: ctxBase.runKey,
        trigger: ctxBase.trigger,
        force,
        params: encodedParams,
      });
      if (claim._tag !== 'Acquired') {
        const reason = toSkipReason(claim);
        yield* logInfo('job.skipped', {
          ...definition.logFields,
          job: definition.name,
          runKey: ctxBase.runKey,
          trigger: ctxBase.trigger,
          status: 'skipped',
          skipReason: reason._tag,
          retryAt: reason._tag === 'RetryLater' ? reason.at : undefined,
        });
        const elapsedMs = yield* elapsedMillisSince(startedMs);
        const outcome: JobOutcome<Name, S> = {
          job: definition.name,
          runKey: ctxBase.runKey,
          trigger: ctxBase.trigger,
          elapsedMs,
          status: 'skipped',
          reason,
        };
        yield* recordJobRunMetrics({ ...outcome, leaseLost: false });
        return outcome;
      }

      const owned: OwnedRun = {
        job: definition.name,
        runKey: ctxBase.runKey,
        token: claim.token,
        attempt: claim.attempt,
      };
      const ctx: JobContext<P> = {
        params: ctxBase.params,
        runKey: ctxBase.runKey,
        trigger: ctxBase.trigger,
        attempt: claim.attempt,
      };
      // A throwing `onClaimed` is a defect of the run, so it settles like one instead of leaving
      // the claimed row `running` until its lease expires.
      const work = Effect.sync(() => onClaimed?.(ctxBase.runKey)).pipe(
        Effect.andThen(executeAndEncodeStats(definition, ctx)),
      );
      const tracked = runWithHeartbeat(runs, owned, work).pipe(
        withJobLogging({
          job: definition.name,
          jobRunId: `${definition.name}:${ctx.runKey}:${String(claim.attempt)}`,
          trigger: ctx.trigger,
          attempt: claim.attempt,
          fields: { ...definition.logFields, runKey: ctx.runKey },
          summarizeResult: (output) => summarizeEncodedStats(output.encoded),
        }),
      );
      const exit = yield* Effect.exit(restore(tracked));
      const runResult: RunResult = Exit.isSuccess(exit)
        ? {
            _tag: 'Completed',
            stats: exit.value.encoded,
            trigger: ctx.trigger,
            annotations: annotationsOf(definition),
          }
        : analyzeFailureExit(definition, ctx.trigger, exit.cause).runResult;
      const settled = yield* runs.settle(owned, runResult).pipe(retryTransientSql);
      if (!settled) {
        yield* logWarning('job.settle.lost', {
          job: owned.job,
          runKey: owned.runKey,
          attempt: owned.attempt,
        });
      }

      const elapsedMs = yield* elapsedMillisSince(startedMs);
      const { outcome, leaseLost } = buildClaimedOutcome({
        definition,
        ctx,
        exit,
        settled,
        elapsedMs,
      });
      yield* recordJobRunMetrics({ ...outcome, leaseLost });
      return outcome;
    }),
  );

/**
 * Runs a `JobDefinition` once, outside a workflow: validates params, derives the run key, handles
 * `dryRun`, claims in `JobRuns`, races the job against heartbeats under `withJobLogging`, settles
 * (calling the storage layer's `onSettle` hook) and records metrics. Errors and defects inside
 * `definition.run` become a `failed` `JobOutcome`. Requires `SecureRandom` for job logging: provide
 * `SecureRandom.layerWebCrypto` with `JobStorage.layerStorage`; `makeJobRunsMemory`'s layer already
 * includes it. Fails with `ClusterConfigError` when the job's cadence is invalid, before anything
 * is claimed.
 */
export const runJob = Effect.fnUntraced(function* <Name extends string, P, S, E, R, PE = P>(
  definition: JobDefinition<Name, P, S, E, R, PE>,
  options: RunJobOptions,
): Effect.fn.Return<
  JobOutcome<Name, S>,
  InvalidJobParams | InvalidRunKey | ClusterConfigError | DryRunUnsupported | SqlError.SqlError,
  R | JobRuns | SecureRandom.SecureRandom
> {
  const startedMs = yield* Clock.currentTimeMillis;
  const runKey = yield* resolveRunKey(definition, startedMs, options.runKey);
  const { params, encoded } = yield* decodeParams(
    definition.name,
    definition.params,
    options.params,
  );
  const ctxBase = {
    params,
    runKey,
    trigger: options.trigger,
  };

  if (options.dryRun === true) {
    return yield* executeDryRun(definition, { ...ctxBase, attempt: 0 }, startedMs);
  }

  const force = resolveForce(definition.cadence, options.trigger, options.force);
  return yield* executeClaimed({
    definition,
    ctxBase,
    encodedParams: encoded,
    force,
    startedMs,
    onClaimed: options.onClaimed,
  });
});

/**
 * Runs a job drawn from a heterogeneous `ReadonlyArray<AnyJobDefinition<R>>` through its rank-2
 * `JOB_CONTINUATION` attached by `defineJob`, avoiding any `any` or unsafe type assertions.
 * Requires `SecureRandom` like `runJob`.
 */
export const runAnyJob = <R>(
  definition: AnyJobDefinition<R>,
  options: RunJobOptions,
): Effect.Effect<
  JobOutcome,
  InvalidJobParams | InvalidRunKey | ClusterConfigError | DryRunUnsupported | SqlError.SqlError,
  R | JobRuns | SecureRandom.SecureRandom
> => {
  const continuation = definition[JOB_CONTINUATION];
  if (continuation === undefined) {
    return Effect.die(
      new Error(`Job "${definition.name}" must be constructed with defineJob before supervising.`),
    );
  }

  return continuation((concrete) => runJob(concrete, options));
};
