import type { SqlClient, SqlError } from 'effect/sql';

import { Effect, Option, Schema } from 'effect';

import type { InvalidRunCursor } from './errors.ts';
import type { RunPagePosition } from './ledger-page.ts';
import type { SqlLedgerContext } from './ledger-sql.ts';
import type { JobRunKey, JobRunsPage, JobRunsPageQuery, RecentRunsQuery } from './ledger-types.ts';

import {
  decodeRunPageCursor,
  isPostgresText,
  makeRunsPage,
  MICROS_PER_DAY,
  pageLimit,
  RunTimestampMicros,
} from './ledger-page.ts';
import { JobRunRecord } from './ledger-types.ts';

/**
 * Public record projection shared by the ledger's history queries.
 */
const runRecordColumns = (sql: SqlClient.SqlClient) => sql`
  job,
  run_key AS "runKey",
  execution_id AS "executionId",
  status,
  attempt,
  failures,
  next_attempt_at AS "nextAttemptAt",
  trigger,
  params,
  stats,
  error_message AS "errorMessage",
  started_at AS "startedAt",
  finished_at AS "finishedAt"
`;

/**
 * Decodes public run rows. A disagreement between a stored row and the schema is a defect.
 */
const decodeRunRecords = (rows: unknown): Effect.Effect<ReadonlyArray<JobRunRecord>> =>
  Schema.decodeUnknownEffect(Schema.Array(JobRunRecord))(rows).pipe(Effect.orDie);

/**
 * Finds the run under the composite primary key, or returns `None` if it is absent.
 */
export const getRunSql = (
  { sql, runsTable }: SqlLedgerContext,
  key: JobRunKey,
): Effect.Effect<Option.Option<JobRunRecord>, SqlError.SqlError> => {
  if (!isPostgresText(key.job) || !isPostgresText(key.runKey)) {
    return Effect.succeedNone;
  }

  return sql`
    SELECT ${runRecordColumns(sql)}
    FROM ${sql(runsTable)}
    WHERE job = ${key.job} AND run_key = ${key.runKey}
  `.pipe(
    Effect.flatMap(decodeRunRecords),
    Effect.map((rows) => Option.fromUndefinedOr(rows[0])),
  );
};

/**
 * Queries recent run ledger rows ordered newest first, optionally filtered to one job. A row that
 * fails to decode as `JobRunRecord` dies: it means the table and the schema disagree, a bug rather
 * than a failure the caller can handle.
 */
export const queryRecentRunsSql = (
  { sql, runsTable }: SqlLedgerContext,
  query: RecentRunsQuery,
): Effect.Effect<ReadonlyArray<JobRunRecord>, SqlError.SqlError> => {
  const limit = pageLimit(query.limit);
  if (limit === 0 || (query.job !== undefined && !isPostgresText(query.job))) {
    return Effect.succeed([]);
  }

  const jobFilter = query.job === undefined ? sql`` : sql`WHERE job = ${query.job}`;
  return sql`
    SELECT ${runRecordColumns(sql)}
    FROM ${sql(runsTable)}
    ${jobFilter}
    ORDER BY started_at DESC, run_key COLLATE "C" DESC, job COLLATE "C" DESC
    LIMIT ${limit}
  `.pipe(Effect.flatMap(decodeRunRecords));
};

/**
 * History row with the precise start time that the JavaScript Date column cannot represent.
 */
const PagedRunRow = Schema.Struct({
  ...JobRunRecord.fields,
  /**
   * Exact start time in microseconds since the PostgreSQL epoch.
   */
  startedAtMicros: RunTimestampMicros,
});

/**
 * Restricts the query to rows after the cursor. Splitting into days and sub-day microseconds keeps
 * interval arithmetic exact and leaves the indexed `started_at` column bare.
 */
const pageBoundarySql = (sql: SqlClient.SqlClient, after: RunPagePosition | undefined) => {
  if (after === undefined) {
    return sql`TRUE`;
  }

  const days = Number(after.startedAtMicros / MICROS_PER_DAY);
  const micros = Number(after.startedAtMicros % MICROS_PER_DAY);
  return sql`
    (started_at, run_key COLLATE "C", job COLLATE "C") <
    ((DATE '2000-01-01' + ${days}::int + ${micros}::double precision * INTERVAL '1 microsecond')
       AT TIME ZONE 'UTC', ${after.runKey}::text, ${after.job}::text)
  `;
};

/**
 * Reads one keyset page and projects a lossless boundary timestamp for its continuation.
 */
export const pageRunsSql = Effect.fnUntraced(function* (
  { sql, runsTable }: SqlLedgerContext,
  query: JobRunsPageQuery,
): Effect.fn.Return<JobRunsPage, SqlError.SqlError | InvalidRunCursor> {
  const after = yield* decodeRunPageCursor(query.cursor);
  const limit = pageLimit(query.limit);
  if (limit === 0 || (query.job !== undefined && !isPostgresText(query.job))) {
    return { runs: [], nextCursor: null };
  }

  const jobFilter = query.job === undefined ? sql`TRUE` : sql`job = ${query.job}`;
  const rows = yield* sql`
    SELECT ${runRecordColumns(sql)},
      trunc((EXTRACT(EPOCH FROM started_at) -
             EXTRACT(EPOCH FROM TIMESTAMPTZ '2000-01-01 00:00:00+00')) * 1000000)::text
        AS "startedAtMicros"
    FROM ${sql(runsTable)}
    WHERE ${jobFilter} AND ${pageBoundarySql(sql, after)}
    ORDER BY started_at DESC, run_key COLLATE "C" DESC, job COLLATE "C" DESC
    LIMIT ${limit + 1}
  `;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(PagedRunRow))(rows).pipe(
    Effect.orDie,
  );
  return makeRunsPage(
    decoded.map(({ startedAtMicros, ...run }) => ({ run, startedAtMicros })),
    limit,
  );
});
