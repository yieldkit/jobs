import { Clock, DateTime, Duration, Effect, Layer, Option } from 'effect';
import { SecureRandom } from 'effect/crypto';

import type { ClusterConfigError } from './errors.ts';
import type {
  ClaimRequest,
  JobRunRecord,
  JobSettled,
  JobSettleHook,
  OwnedRun,
  RunClaim,
  RunResult,
} from './ledger-types.ts';
import type { JobsPolicy, JobsPolicyOverrides, RetryPolicy } from './policy.ts';
import type { BoundSettleHook } from './settle-hook.ts';

import { cloneRunRecord, pageMemory, recentMemory, rowKey } from './ledger-memory-history.ts';
import { failedNextAttemptAt } from './ledger-retry.ts';
import { JobRuns } from './ledger.ts';
import { decideClaim, resolveJobsPolicy } from './policy.ts';
import { bindSettleHook } from './settle-hook.ts';

/**
 * Internal mutable state for one `(job, runKey)` row in the in-memory ledger.
 */
type MemoryRow = {
  /**
   * Public `JobRunRecord` snapshot.
   */
  record: JobRunRecord;

  /**
   * Fence UUID matching the latest `claim`.
   */
  claimToken: string;

  /**
   * Lease expiration instant in epoch milliseconds on the Effect `Clock`.
   */
  leaseExpiresAtMs: number;
};

/**
 * Recorded settle invocation captured by `makeJobRunsMemory` for assertions.
 */
export type MemorySettleEntry = {
  /**
   * Ownership handle that settled the run.
   */
  readonly run: OwnedRun;

  /**
   * Terminal `RunResult` passed to `settle`.
   */
  readonly result: RunResult;

  /**
   * The event handed to the `onSettle` hook for this settle.
   */
  readonly event: JobSettled;
};

/**
 * Options of `makeJobRunsMemory`.
 */
export type JobRunsMemoryOptions<R = never> = {
  /**
   * Hook called after every successful settle, like the SQL ledger's. A failure is swallowed (and
   * the settle still counts), an interruption is not.
   */
  readonly onSettle?: JobSettleHook<R> | undefined;

  /**
   * Policy overrides merged over `defaultJobsPolicy`.
   */
  readonly policy?: JobsPolicyOverrides | undefined;
};

/**
 * In-memory `JobRuns` test harness exposing its Effect `layer` alongside recorded `settles` and
 * current `rows`.
 */
export type JobRunsMemoryHarness<R = never> = {
  /**
   * Standalone layer requiring no database (only the hook's services). Also provides the WebCrypto
   * `SecureRandom` it mints claim tokens from, so `runJob` and job logging run on this layer
   * alone.
   */
  readonly layer: Layer.Layer<JobRuns | SecureRandom.SecureRandom, never, R>;

  /**
   * Every successful settle in order, with its `RunResult` and the hook event.
   */
  readonly settles: () => ReadonlyArray<MemorySettleEntry>;

  /**
   * Current ledger rows, for assertions on status, attempt, failures, and `nextAttemptAt`.
   */
  readonly rows: () => ReadonlyArray<JobRunRecord>;
};

/**
 * Returns the row of `run` while it is still `running` under `run.token`, or `undefined` once it
 * settled, vanished, or another claim rotated the token.
 */
const findOwnedRow = (store: Map<string, MemoryRow>, run: OwnedRun): MemoryRow | undefined => {
  const existing = store.get(rowKey(run.job, run.runKey));
  const owned =
    existing !== undefined &&
    existing.record.status === 'running' &&
    existing.claimToken === run.token;
  return owned ? existing : undefined;
};

/**
 * Answers a claim of a run past its deadline without taking the row: a `completed` row is
 * `Completed` (`AlreadySettled` for its own execution); any other key is `Exhausted`, and a
 * `failed` row is marked exhausted.
 */
const refusePastDeadline = (
  existing: MemoryRow | undefined,
  executionId: string | null,
): Exclude<RunClaim, { readonly _tag: 'Acquired' }> => {
  if (existing?.record.status === 'completed') {
    return executionId !== null && existing.record.executionId === executionId
      ? { _tag: 'AlreadySettled' }
      : { _tag: 'Completed' };
  }

  if (existing?.record.status === 'failed') {
    existing.record = { ...existing.record, nextAttemptAt: null };
  }

  return { _tag: 'Exhausted' };
};

/**
 * Claims or resumes `(request.job, request.runKey)` using Effect `Clock` time so `TestClock` drives
 * lease expiration and retry timestamps deterministically. `newToken` mints the fence UUID.
 */
const claimMemory = Effect.fnUntraced(function* (
  store: Map<string, MemoryRow>,
  request: ClaimRequest,
  leaseMs: number,
  newToken: Effect.Effect<string>,
): Effect.fn.Return<RunClaim> {
  const now = yield* DateTime.nowAsDate;
  const nowMs = now.getTime();
  const key = rowKey(request.job, request.runKey);
  const executionId = request.executionId ?? null;
  const existing = store.get(key);
  if (request.pastDeadline === true) {
    return refusePastDeadline(existing, executionId);
  }

  let resumed = false;
  if (existing !== undefined) {
    const decision = decideClaim(
      {
        status: existing.record.status,
        executionId: existing.record.executionId,
        nextAttemptAt: existing.record.nextAttemptAt,
        leaseExpired: existing.leaseExpiresAtMs < nowMs,
        nowMs,
      },
      { trigger: request.trigger, force: request.force, executionId },
    );
    if (decision._tag === 'Refuse') {
      return decision.claim;
    }

    resumed = decision.resumed;
  }

  const token = yield* newToken;
  const attempt = existing === undefined ? 1 : existing.record.attempt + 1;
  const failures = existing === undefined ? 0 : existing.record.failures;
  const startedAt = resumed && existing !== undefined ? existing.record.startedAt : now;
  store.set(key, {
    record: {
      job: request.job,
      runKey: request.runKey,
      executionId,
      status: 'running',
      attempt,
      failures,
      nextAttemptAt: null,
      trigger: request.trigger,
      params: request.params ?? {},
      stats: null,
      errorMessage: null,
      startedAt,
      finishedAt: null,
    },
    claimToken: token,
    leaseExpiresAtMs: nowMs + leaseMs,
  });
  return { _tag: 'Acquired', token, attempt, resumed, failures };
});

/**
 * Extends the in-memory lease when `run.token` still owns a running row.
 */
const heartbeatMemory = Effect.fnUntraced(function* (
  store: Map<string, MemoryRow>,
  run: OwnedRun,
  leaseMs: number,
): Effect.fn.Return<boolean> {
  const existing = findOwnedRow(store, run);
  if (existing === undefined) {
    return false;
  }

  const nowMs = yield* Clock.currentTimeMillis;
  existing.leaseExpiresAtMs = nowMs + leaseMs;
  return true;
});

/**
 * Whether `run`'s own claim already settled its row the way `result` would, so a repeated settle
 * counts as settled without being applied again, like the SQL ledger.
 */
const isSettledByClaim = (
  store: Map<string, MemoryRow>,
  run: OwnedRun,
  result: RunResult,
): boolean => {
  const existing = store.get(rowKey(run.job, run.runKey));
  const status = result._tag === 'Completed' ? 'completed' : 'failed';
  return existing?.claimToken === run.token && existing.record.status === status;
};

/**
 * Mutable state shared by every service built from one memory harness.
 */
type MemoryLedgerState = {
  /**
   * Rows by `(job, runKey)`.
   */
  readonly store: Map<string, MemoryRow>;

  /**
   * Every successful settle, in order.
   */
  readonly settleLog: Array<MemorySettleEntry>;

  /**
   * Retry rules for `next_attempt_at`.
   */
  readonly retry: RetryPolicy;
};

/**
 * Settles an owned in-memory row and records the settle entry when `run.token` still owns it.
 */
const settleMemory = Effect.fnUntraced(function* (
  state: MemoryLedgerState,
  run: OwnedRun,
  result: RunResult,
): Effect.fn.Return<JobSettled | undefined> {
  const { store, settleLog, retry } = state;
  const existing = findOwnedRow(store, run);
  if (existing === undefined) {
    return undefined;
  }

  const now = yield* DateTime.nowAsDate;
  const identity = {
    job: run.job,
    runKey: run.runKey,
    attempt: run.attempt,
    trigger: result.trigger,
    annotations: result.annotations,
  };

  if (result._tag === 'Completed') {
    existing.record = {
      ...existing.record,
      status: 'completed',
      stats: result.stats ?? null,
      errorMessage: null,
      nextAttemptAt: null,
      finishedAt: now,
    };
    const event: JobSettled = { ...identity, outcome: { _tag: 'Completed', stats: result.stats } };
    settleLog.push({ run, result, event });
    return event;
  }

  const interrupted = result.reason === 'interrupted';
  const failures = existing.record.failures + (interrupted ? 0 : 1);
  const retryAt = failedNextAttemptAt(retry, result, { runKey: run.runKey, failures, now });
  existing.record = {
    ...existing.record,
    status: 'failed',
    errorMessage: result.errorMessage,
    failures,
    nextAttemptAt: retryAt,
    finishedAt: now,
  };
  const event: JobSettled = {
    ...identity,
    outcome: {
      _tag: 'Failed',
      errorMessage: result.errorMessage,
      reason: result.reason,
      failures,
      nextAttemptAt: retryAt,
    },
  };
  settleLog.push({ run, result, event });
  return event;
});

/**
 * Runs `hook` for `event` like the SQL ledger: a failure or defect is dropped, an interruption is
 * not. Resolves to whether the settle matched its claim.
 */
const runMemoryHook = (
  hook: BoundSettleHook | undefined,
  event: JobSettled | undefined,
): Effect.Effect<boolean> => {
  if (event === undefined) {
    return Effect.succeed(false);
  }

  if (hook === undefined) {
    return Effect.succeed(true);
  }

  return hook(event).pipe(
    Effect.catchTag('SettleHookFailed', () => Effect.void),
    Effect.as(true),
  );
};

/**
 * Builds an in-memory `JobRuns` with the claim, lease, fence, backoff, query and prune semantics of
 * the SQL ledger, reading time from the Effect `Clock` so `TestClock` controls every timestamp. It
 * calls `onSettle` after each successful settle and records the events. Fails with
 * `ClusterConfigError` for invalid policy overrides.
 */
export const makeJobRunsMemory = <R = never>(
  options?: JobRunsMemoryOptions<R>,
): Effect.Effect<JobRunsMemoryHarness<R>, ClusterConfigError> =>
  Effect.gen(function* () {
    const policy: JobsPolicy = yield* resolveJobsPolicy(options?.policy);
    const leaseDuration = policy.lease.duration;
    const leaseMs = Math.max(1, Duration.toMillis(leaseDuration));
    const store = new Map<string, MemoryRow>();
    const settleLog: Array<MemorySettleEntry> = [];

    /**
     * Builds the service over the shared `store` and `settleLog`, minting claim fence tokens from
     * `random` so every layer built from this harness sees the same rows.
     */
    const makeService = (
      random: SecureRandom.SecureRandom['Service'],
      hook: BoundSettleHook | undefined,
    ): JobRuns['Service'] =>
      JobRuns.of({
        claim: (request) => claimMemory(store, request, leaseMs, random.uuidV4()),
        heartbeat: (run) => heartbeatMemory(store, run, leaseMs),
        settle: (run, result) =>
          settleMemory({ store, settleLog, retry: policy.retry }, run, result).pipe(
            Effect.flatMap((event) =>
              event === undefined && isSettledByClaim(store, run, result)
                ? Effect.succeed(true)
                : runMemoryHook(hook, event),
            ),
          ),
        isCompleted: (job, runKey) =>
          Effect.sync(() => store.get(rowKey(job, runKey))?.record.status === 'completed'),
        get: ({ job, runKey }) =>
          Effect.sync(() => {
            const existing = store.get(rowKey(job, runKey));
            return existing === undefined
              ? Option.none()
              : Option.some(cloneRunRecord(existing.record));
          }),
        recent: (query) => recentMemory(store, query),
        page: (query) => pageMemory(store, query),
        prune: (jobs, olderThan) =>
          Effect.gen(function* () {
            if (jobs.length === 0) {
              return 0;
            }

            const nowMs = yield* Clock.currentTimeMillis;
            const cutoffMs =
              nowMs - Math.max(0, Duration.toMillis(Duration.fromInputUnsafe(olderThan)));
            const jobSet = new Set(jobs);
            let deleted = 0;

            for (const [key, entry] of store.entries()) {
              if (jobSet.has(entry.record.job) && entry.record.startedAt.getTime() < cutoffMs) {
                store.delete(key);
                deleted += 1;
              }
            }

            return deleted;
          }),
        leaseDuration,
        policy,
      });
    return {
      layer: Layer.effect(
        JobRuns,
        Effect.gen(function* () {
          const random = yield* SecureRandom.SecureRandom;
          const context = yield* Effect.context<R>();
          return makeService(random, bindSettleHook(options?.onSettle, context));
        }),
      ).pipe(Layer.provideMerge(SecureRandom.layerWebCrypto)),
      settles: () => [...settleLog],
      rows: () => [...store.values()].map((entry) => cloneRunRecord(entry.record)),
    };
  });
