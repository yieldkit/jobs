import { Clock, DateTime, Duration, Effect, Layer } from 'effect';

import type { JobRunRecord } from './ledger-types.ts';
import type {
  CreateJobRequest,
  DispatchFailedJobRequest,
  FinishJobRequest,
  JobRequestRecord,
  JobRequestStatus,
  UpsertStartedJobRequest,
} from './requests.ts';

import { isUuidString, JobRequests } from './requests.ts';

/**
 * Options for `makeJobRequestsMemory`.
 */
export type JobRequestsMemoryOptions = {
  /**
   * Optional lookup resolving `(job, runKey)` to a `JobRunRecord` (for example from
   * `makeJobRunsMemory().rows()`) so `getStatus` joins the ledger like the SQL store.
   */
  readonly findRun?: ((job: string, runKey: string) => JobRunRecord | undefined) | undefined;
};

/**
 * In-memory `JobRequests` test harness exposing its Effect `layer`, direct `service` handle, and
 * current `rows` snapshot.
 */
export type JobRequestsMemoryHarness = {
  /**
   * Standalone `Layer<JobRequests>` requiring no database.
   */
  readonly layer: Layer.Layer<JobRequests>;

  /**
   * Direct `JobRequests` service handle for test assertions without `Effect.provide`.
   */
  readonly service: JobRequests['Service'];

  /**
   * Current request rows in insertion order.
   */
  readonly rows: () => ReadonlyArray<JobRequestRecord>;
};

/**
 * Inserts or updates a pending request row in the in-memory store.
 */
const createInMemory = Effect.fnUntraced(function* (
  store: Array<JobRequestRecord>,
  input: CreateJobRequest,
): Effect.fn.Return<JobRequestRecord> {
  const existingIndex = store.findIndex((row) => row.id === input.id);
  const existing = existingIndex >= 0 ? store[existingIndex] : undefined;
  if (existing !== undefined) {
    const updated: JobRequestRecord = {
      ...existing,
      executionId: existing.executionId ?? input.executionId,
    };
    store[existingIndex] = updated;
    return updated;
  }

  const row: JobRequestRecord = {
    id: input.id,
    job: input.job,
    params: input.params ?? {},
    force: input.force,
    executionId: input.executionId,
    requestedAt: yield* DateTime.nowAsDate,
    pickedAt: null,
    finishedAt: null,
    runKey: null,
    outcome: null,
  };
  store.push(row);
  return row;
});

/**
 * Marks a request row started in the in-memory store, inserting one if absent.
 */
const upsertStartedInMemory = Effect.fnUntraced(function* (
  store: Array<JobRequestRecord>,
  input: UpsertStartedJobRequest,
): Effect.fn.Return<void> {
  const now = yield* DateTime.nowAsDate;
  const existingIndex = store.findIndex((row) => row.id === input.id);
  const existing = existingIndex >= 0 ? store[existingIndex] : undefined;
  if (existing !== undefined) {
    store[existingIndex] = {
      ...existing,
      executionId: existing.executionId ?? input.executionId,
      finishedAt: existing.pickedAt === null ? null : existing.finishedAt,
      outcome: existing.pickedAt === null ? null : existing.outcome,
      pickedAt: existing.pickedAt ?? now,
      runKey: input.runKey ?? existing.runKey,
    };
    return;
  }

  store.push({
    id: input.id,
    job: input.job,
    params: input.params ?? {},
    force: input.force,
    executionId: input.executionId,
    requestedAt: now,
    pickedAt: now,
    finishedAt: null,
    runKey: input.runKey,
    outcome: null,
  });
});

/**
 * Marks a request row finished in the in-memory store unless it already finished after a pickup.
 */
const markFinishedInMemory = Effect.fnUntraced(function* (
  store: Array<JobRequestRecord>,
  input: FinishJobRequest,
): Effect.fn.Return<boolean> {
  const existingIndex = store.findIndex((row) => row.id === input.id);
  const existing = existingIndex >= 0 ? store[existingIndex] : undefined;
  if (existing === undefined || (existing.finishedAt !== null && existing.pickedAt !== null)) {
    return false;
  }

  const now = yield* DateTime.nowAsDate;
  store[existingIndex] = {
    ...existing,
    pickedAt: existing.pickedAt ?? now,
    runKey: input.runKey ?? existing.runKey,
    outcome: input.outcome ?? null,
    finishedAt: now,
  };
  return true;
});

/**
 * Marks an unpicked request row failed on dispatch error in the in-memory store.
 */
const markDispatchFailedInMemory = Effect.fnUntraced(function* (
  store: Array<JobRequestRecord>,
  input: DispatchFailedJobRequest,
): Effect.fn.Return<void> {
  const existingIndex = store.findIndex((row) => row.id === input.id);
  const existing = existingIndex >= 0 ? store[existingIndex] : undefined;
  if (existing === undefined || existing.pickedAt !== null || existing.finishedAt !== null) {
    return;
  }

  store[existingIndex] = {
    ...existing,
    finishedAt: yield* DateTime.nowAsDate,
    outcome: {
      job: input.job,
      trigger: 'api',
      status: 'failed',
      errorTag: 'WorkflowDispatchError',
      errorMessage: input.errorMessage,
    },
  };
});

/**
 * Builds an in-memory `JobRequests` that reads time through the Effect `Clock`, so `TestClock`
 * controls every timestamp.
 */
export const makeJobRequestsMemory = (
  options?: JobRequestsMemoryOptions,
): Effect.Effect<JobRequestsMemoryHarness> =>
  Effect.sync(() => {
    const store: Array<JobRequestRecord> = [];
    const service = JobRequests.of({
      create: (input) => createInMemory(store, input),
      upsertStarted: (input) => upsertStartedInMemory(store, input),
      markFinished: (input) => markFinishedInMemory(store, input),
      markDispatchFailed: (input) => markDispatchFailedInMemory(store, input),
      getStatus: (job: string, requestId: string): Effect.Effect<JobRequestStatus | undefined> =>
        Effect.sync(() => {
          if (!isUuidString(requestId)) {
            return undefined;
          }

          const request = store.find((row) => row.id === requestId && row.job === job);
          if (request === undefined) {
            return undefined;
          }

          const found =
            request.runKey === null ? undefined : options?.findRun?.(request.job, request.runKey);
          const ownRun =
            found !== undefined &&
            request.executionId !== null &&
            found.executionId === request.executionId;
          return { request, run: ownRun ? found : null };
        }),
      prune: (olderThan, limit) =>
        Effect.gen(function* () {
          const nowMs = yield* Clock.currentTimeMillis;
          const cutoffMs =
            nowMs - Math.max(0, Duration.toMillis(Duration.fromInputUnsafe(olderThan)));
          let remaining = Math.max(1, Math.trunc(limit));
          let deleted = 0;
          for (let index = store.length - 1; index >= 0 && remaining > 0; index -= 1) {
            const finishedAt = store[index]?.finishedAt;
            if (
              finishedAt !== undefined &&
              finishedAt !== null &&
              finishedAt.getTime() < cutoffMs
            ) {
              store.splice(index, 1);
              deleted += 1;
              remaining -= 1;
            }
          }

          return deleted;
        }),
    });
    return {
      layer: Layer.succeed(JobRequests, service),
      service,
      rows: () => [...store],
    };
  });
