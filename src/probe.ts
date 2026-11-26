import type { SqlError } from 'effect/sql';
import type { Migrator } from 'effect/sql';

import { Effect, Schema } from 'effect';
import { SqlClient } from 'effect/sql';

import type { JobsStorageComponent } from './errors.ts';

import { JobsNotMigratedError } from './errors.ts';

/**
 * One migration history a runtime layer checks before it starts.
 */
export type HistoryProbe = {
  /**
   * Which part of the storage the history belongs to.
   */
  readonly component: JobsStorageComponent;

  /**
   * History table as written in SQL: `schema.table`, or a bare name resolved through the session's
   * `search_path`.
   */
  readonly history: string;

  /**
   * Tables the history's steps create, written like `history`. A history that is current while one
   * of them is missing belongs to someone else, such as Cluster storage sharing the prefix.
   */
  readonly tables: ReadonlyArray<string>;

  /**
   * Loader of the installed code; its highest id is the version the runtime needs.
   */
  readonly loader: Migrator.Loader;
};

/**
 * Highest migration id recorded in a history, `0` when it has no rows.
 */
const MaxIdRow = Schema.Struct({
  /**
   * `COALESCE(MAX(migration_id), 0)`.
   */
  found: Schema.Int,
});

/**
 * Row of the existence check: whether `to_regclass` found the table.
 */
const ExistsRow = Schema.Struct({
  /**
   * `to_regclass(...) IS NOT NULL`.
   */
  present: Schema.Boolean,
});

/**
 * Whether `table` (`schema.table` or a bare name, both plain lowercase identifiers) exists, as the
 * session's `search_path` resolves it.
 */
export const tableExists = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  table: string,
): Effect.fn.Return<boolean, SqlError.SqlError> {
  const rows = yield* sql`SELECT to_regclass(${table}) IS NOT NULL AS present`;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(ExistsRow))(rows).pipe(
    Effect.orDie,
  );
  return decoded[0]?.present === true;
});

/**
 * Highest migration id recorded in `history`, `0` when the table is missing or empty.
 */
export const recordedMigrationId = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  history: string,
): Effect.fn.Return<number, SqlError.SqlError> {
  if (!(yield* tableExists(sql, history))) {
    return 0;
  }

  const rows = yield* sql`SELECT COALESCE(MAX(migration_id), 0)::int AS found FROM ${sql(history)}`;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(MaxIdRow))(rows).pipe(
    Effect.orDie,
  );
  return decoded[0]?.found ?? 0;
});

/**
 * Highest id in the installed loader. A loader that cannot be read is a bug in this package.
 */
const expectedMigrationId = (loader: Migrator.Loader): Effect.Effect<number> =>
  loader.pipe(
    Effect.orDie,
    Effect.map((steps) => steps.reduce((highest, [id]) => Math.max(highest, id), 0)),
  );

/**
 * Compares the history in `probe` with its loader. Fails with `JobsNotMigratedError` when the table
 * is missing, its highest id is lower than the installed code ships, or it is current while one of
 * `probe.tables` is missing; a higher id (newer code ran the migration) is accepted. Connection and
 * permission failures stay `SqlError`.
 */
export const probeHistory = Effect.fnUntraced(function* (
  probe: HistoryProbe,
): Effect.fn.Return<void, JobsNotMigratedError | SqlError.SqlError, SqlClient.SqlClient> {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  const found = yield* recordedMigrationId(sql, probe.history);
  const expected = yield* expectedMigrationId(probe.loader);
  if (found < expected) {
    return yield* new JobsNotMigratedError({
      component: probe.component,
      table: probe.history,
      found,
      expected,
      message:
        `${probe.history} is not migrated (found ${String(found)}, needs ${String(expected)}). ` +
        'Run JobStorage.layerMigrations first.',
    });
  }

  for (const table of probe.tables) {
    if (!(yield* tableExists(sql, table))) {
      return yield* new JobsNotMigratedError({
        component: probe.component,
        table: probe.history,
        found,
        expected,
        message:
          `${probe.history} is current but ${table} does not exist. Another component, such as ` +
          'Cluster storage with the same prefix, may share that history; give the library its own prefix.',
      });
    }
  }

  return yield* Effect.void;
});
