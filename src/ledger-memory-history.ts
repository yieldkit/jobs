import { Effect } from 'effect';

import type { InvalidRunCursor } from './errors.ts';
import type { RunPagePosition } from './ledger-page.ts';
import type {
  JobRunRecord,
  JobRunsPage,
  JobRunsPageQuery,
  RecentRunsQuery,
} from './ledger-types.ts';

import { decodeRunPageCursor, makeRunsPage, pageLimit, runTimestampMicros } from './ledger-page.ts';

/**
 * Stored memory ledger entry viewed by read queries.
 */
type ReadableMemoryRow = {
  /**
   * Public `JobRunRecord` snapshot.
   */
  readonly record: JobRunRecord;
};

/**
 * Formats the collision-free composite `Map` key for `(job, runKey)`.
 */
export const rowKey = (job: string, runKey: string): string => JSON.stringify([job, runKey]);

/**
 * Copies a nullable `Date` so callers cannot mutate ledger timestamps.
 */
const cloneDate = (value: Date | null): Date | null =>
  value === null ? null : new Date(value.getTime());

/**
 * Returns a detached copy of a stored `JobRunRecord`.
 */
export const cloneRunRecord = (record: JobRunRecord): JobRunRecord => ({
  ...record,
  nextAttemptAt: cloneDate(record.nextAttemptAt),
  params: structuredClone(record.params),
  stats: structuredClone(record.stats),
  startedAt: new Date(record.startedAt.getTime()),
  finishedAt: cloneDate(record.finishedAt),
});

/**
 * Encodes text as UTF-8 for the bytewise comparison used by the SQL ledger.
 */
const utf8 = new TextEncoder();

/**
 * Compares text in descending UTF-8 byte order, matching PostgreSQL's C collation.
 */
const compareTextDescending = (left: string, right: string): number => {
  if (left === right) {
    return 0;
  }

  const leftBytes = utf8.encode(left);
  const rightBytes = utf8.encode(right);
  for (const [index, leftByte] of leftBytes.entries()) {
    const rightByte = rightBytes[index];
    if (rightByte === undefined) {
      return -1;
    }

    if (leftByte !== rightByte) {
      return rightByte - leftByte;
    }
  }

  return rightBytes.length - leftBytes.length;
};

/**
 * Compares cursor positions in history order, newest first.
 */
const compareRunPositions = (left: RunPagePosition, right: RunPagePosition): number => {
  if (left.startedAtMicros !== right.startedAtMicros) {
    return left.startedAtMicros > right.startedAtMicros ? -1 : 1;
  }

  return (
    compareTextDescending(left.runKey, right.runKey) || compareTextDescending(left.job, right.job)
  );
};

/**
 * Projects matching rows into their history sort position.
 */
const matchingRuns = (store: ReadonlyMap<string, ReadableMemoryRow>, job: string | undefined) =>
  store
    .values()
    .filter(({ record }) => job === undefined || record.job === job)
    .map(({ record }) => ({
      run: record,
      startedAtMicros: runTimestampMicros(record.startedAt),
      job: record.job,
      runKey: record.runKey,
    }));

/**
 * Returns the newest rows in the SQL ledger's keyset order.
 */
export const recentMemory = (
  store: ReadonlyMap<string, ReadableMemoryRow>,
  query: RecentRunsQuery,
): Effect.Effect<ReadonlyArray<JobRunRecord>> =>
  Effect.sync(() => {
    const limit = pageLimit(query.limit);
    if (limit === 0) {
      return [];
    }

    return matchingRuns(store, query.job)
      .toArray()
      .toSorted(compareRunPositions)
      .slice(0, limit)
      .map(({ run }) => cloneRunRecord(run));
  });

/**
 * Returns rows strictly after the optional cursor using the SQL ledger's keyset order.
 */
export const pageMemory = Effect.fnUntraced(function* (
  store: ReadonlyMap<string, ReadableMemoryRow>,
  query: JobRunsPageQuery,
): Effect.fn.Return<JobRunsPage, InvalidRunCursor> {
  const after = yield* decodeRunPageCursor(query.cursor);
  const limit = pageLimit(query.limit);
  if (limit === 0) {
    return { runs: [], nextCursor: null };
  }

  const eligible = matchingRuns(store, query.job)
    .filter((row) => after === undefined || compareRunPositions(row, after) > 0)
    .toArray();
  const page = makeRunsPage(eligible.toSorted(compareRunPositions), limit);
  return { ...page, runs: page.runs.map(cloneRunRecord) };
});
