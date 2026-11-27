import type { PgConfigError } from '@yieldkit/pg';
import type { SqlError } from 'effect/sql';

import { describeError, logInfo } from '@yieldkit/logger';
import { MigrationError } from '@yieldkit/pg';
import { Data, Effect, Layer, Predicate, Schema } from 'effect';
import { Migrator, SqlClient } from 'effect/sql';

import type { JobsStorageOptions, ResolvedStorage } from './storage-options.ts';

import { recordedMigrationId, tableExists } from './probe.ts';
import { resolveStorage } from './storage-options.ts';

/**
 * A migration precondition does not hold: a missing schema, a missing grant, or an adopted table
 * the library cannot use as it is.
 */
export class MigrationStepError extends Data.TaggedError('MigrationStepError')<{
  /**
   * What is wrong and how to fix it.
   */
  readonly message: string;
}> {}

/**
 * Succeeds when `holds`, otherwise fails with `MigrationStepError` carrying `message`.
 */
export const requireThat = (
  holds: boolean,
  message: string,
): Effect.Effect<void, MigrationStepError> =>
  holds ? Effect.void : Effect.fail(new MigrationStepError({ message }));

/**
 * Failures a migration step or `Migrator` run can end with.
 */
export type MigratorFailure =
  | Migrator.MigrationError
  | MigrationStepError
  | SqlError.SqlError
  | Schema.SchemaError;

/**
 * Row read by the trigger CHECK guard at the end of step 0001.
 */
const ConstraintRow = Schema.Struct({
  /**
   * `pg_get_constraintdef` of the constraint.
   */
  definition: Schema.String,
});

/**
 * Trigger values the run ledger stores, each of which an adopted trigger CHECK must mention.
 */
const REQUIRED_TRIGGER_VALUES = ["'schedule'", "'manual'", "'api'"];

/**
 * Fails step 0001 when the run ledger already has a trigger CHECK whose definition does not mention
 * all of `'schedule'`, `'manual'` and `'api'`, which a table created by an earlier,
 * application-owned migration may have. Without this guard the readiness check would pass and the
 * first run with a missing trigger would hit a constraint violation. It reads the definition's text
 * only, so a CHECK that names all three values but still rejects one is not caught.
 */
const assertTriggerCheckAllowsManual = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  storage: ResolvedStorage,
): Effect.fn.Return<void, MigratorFailure> {
  const rows = yield* sql`
    SELECT pg_get_constraintdef(c.oid) AS definition
    FROM pg_constraint c
    WHERE c.conrelid = to_regclass(${storage.runsTable})
      AND c.conname = ${storage.runsTriggerCheck}
  `;
  const constraints = yield* Schema.decodeUnknownEffect(Schema.Array(ConstraintRow))(rows);
  const existing = constraints[0];
  yield* requireThat(
    existing === undefined ||
      REQUIRED_TRIGGER_VALUES.every((value) => existing.definition.includes(value)),
    `${storage.runsTable} has a trigger CHECK (${storage.runsTriggerCheck}) that does not allow ` +
      `'schedule', 'manual' and 'api'. ` +
      `Replace it with CHECK (trigger IN ('schedule', 'manual', 'api')) before ` +
      'migrating; see "Adopting existing tables" in docs/migrations.md.',
  );
});

/**
 * Step 0001: the run ledger, its index and the request table. Every statement is idempotent, so it
 * also adopts tables that exist without a history row; `CREATE TABLE IF NOT EXISTS` leaves an
 * existing table's columns and constraints as they are.
 */
const createTables = (
  storage: ResolvedStorage,
): Effect.Effect<void, MigratorFailure, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();
    yield* sql`
      CREATE TABLE IF NOT EXISTS ${sql(storage.runsTable)} (
        job              TEXT        NOT NULL,
        run_key          TEXT        NOT NULL,
        status           TEXT        NOT NULL
                         CONSTRAINT ${sql(storage.runsStatusCheck)}
                         CHECK (status IN ('running', 'completed', 'failed')),
        claim_token      UUID        NOT NULL,
        lease_expires_at TIMESTAMPTZ NOT NULL,
        attempt          INTEGER     NOT NULL DEFAULT 1,
        failures         INTEGER     NOT NULL DEFAULT 0,
        next_attempt_at  TIMESTAMPTZ,
        trigger          TEXT        NOT NULL
                         CONSTRAINT ${sql(storage.runsTriggerCheck)}
                         CHECK (trigger IN ('schedule', 'manual', 'api')),
        params           JSONB       NOT NULL DEFAULT '{}'::jsonb,
        stats            JSONB,
        error_message    TEXT,
        started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at      TIMESTAMPTZ,
        execution_id     TEXT,
        PRIMARY KEY (job, run_key)
      )
    `;
    yield* sql`ALTER TABLE ${sql(storage.runsTable)} ADD COLUMN IF NOT EXISTS execution_id TEXT`;
    yield* sql`
      CREATE INDEX IF NOT EXISTS ${sql(storage.runsRecentIndex)}
      ON ${sql(storage.runsTable)} (job, started_at DESC)
    `;
    yield* sql`
      CREATE TABLE IF NOT EXISTS ${sql(storage.requestsTable)} (
        id           UUID        PRIMARY KEY,
        job          TEXT        NOT NULL,
        params       JSONB       NOT NULL DEFAULT '{}'::jsonb,
        force        BOOLEAN     NOT NULL DEFAULT FALSE,
        requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        picked_at    TIMESTAMPTZ,
        finished_at  TIMESTAMPTZ,
        run_key      TEXT,
        outcome      JSONB,
        execution_id TEXT
      )
    `;
    yield* sql`ALTER TABLE ${sql(storage.requestsTable)} ADD COLUMN IF NOT EXISTS execution_id TEXT`;
    yield* assertTriggerCheckAllowsManual(sql, storage);
  });

/**
 * Step 0002: index for keyset-paginated run history across all jobs.
 */
const addRunsHistoryIndex = (
  storage: ResolvedStorage,
): Effect.Effect<void, MigratorFailure, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();
    yield* sql`
      CREATE INDEX IF NOT EXISTS ${sql(storage.runsHistoryIndex)}
      ON ${sql(storage.runsTable)} (started_at DESC, run_key COLLATE "C" DESC, job COLLATE "C" DESC)
    `;
  });

/**
 * The library's migration loader for resolved names. New steps are appended with higher ids and
 * must stay idempotent. `Migrator` runs every pending step in one transaction, so a step that
 * rewrites a large table belongs in its own deploy step instead.
 */
export const jobsMigrationLoader = (storage: ResolvedStorage): Migrator.Loader =>
  Migrator.fromRecord({
    '0001_create_tables': createTables(storage),
    '0002_add_runs_history_index': addRunsHistoryIndex(storage),
  });

/**
 * Migration loader for the library's tables, like Effect Cluster's `SqlMessageStorage.migrations`,
 * for callers who run `Migrator` themselves. Record its history in `<prefix>_migrations` (in
 * `schema` when one is set), the table `layerStorage` checks. Invalid storage options fail the
 * loader with Effect's `Migrator.MigrationError`.
 */
export const migrations = (options?: JobsStorageOptions): Migrator.Loader =>
  resolveStorage(options).pipe(
    Effect.mapError(
      (error) => new Migrator.MigrationError({ kind: 'BadState', message: error.message }),
    ),
    Effect.flatMap(jobsMigrationLoader),
  );

/**
 * Text of a failure from Effect's `Migrator`, with the failing step's cause appended.
 */
export const describeMigratorFailure = (error: unknown): string => {
  if (
    Predicate.isTagged(error, 'MigrationError') &&
    Predicate.hasProperty(error, 'cause') &&
    error.cause !== undefined
  ) {
    return `${describeError(error)}: ${describeError(error.cause)}`;
  }

  return describeError(error);
};

/**
 * Row of the target schema check.
 */
const SchemaRow = Schema.Struct({
  /**
   * The configured schema, or the first one of `search_path` that exists.
   */
  schema: Schema.NullOr(Schema.String),

  /**
   * Whether that schema exists.
   */
  present: Schema.Boolean,

  /**
   * Whether the current role may create tables in it.
   */
  canCreate: Schema.Boolean,
});

/**
 * Checks, before any DDL, that the target schema exists and the role may create tables in it, and
 * that the history is not shared with another component: a history with rows while the run ledger
 * is missing means Cluster storage, or something else, records its steps there too, and the
 * library's steps would be skipped as already applied.
 */
const preflight = Effect.fnUntraced(function* (
  storage: ResolvedStorage,
): Effect.fn.Return<void, MigratorFailure, SqlClient.SqlClient> {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  const target =
    storage.schema === undefined ? sql`current_schema()` : sql`${storage.schema}::text`;
  const rows = yield* sql`
    SELECT s.name AS schema,
           (s.name IS NOT NULL AND EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = s.name)) AS present,
           (s.name IS NOT NULL
             AND EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = s.name)
             AND has_schema_privilege(current_user, s.name, 'CREATE')) AS "canCreate"
    FROM (SELECT ${target} AS name) s
  `;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(SchemaRow))(rows);
  const row = decoded[0];
  yield* requireThat(
    row?.present === true,
    storage.schema === undefined
      ? "No schema of the connection's search_path exists. Create one, or set `schema`."
      : `Schema "${storage.schema}" does not exist. Create it before running the migrations.`,
  );
  yield* requireThat(
    row?.canCreate === true,
    `The migration role cannot create tables in schema "${row?.schema ?? ''}". ` +
      `GRANT CREATE ON SCHEMA ${row?.schema ?? ''} to it, or migrate as the schema owner.`,
  );

  const recorded = yield* recordedMigrationId(sql, storage.migrationsTable);
  yield* requireThat(
    recorded === 0 || (yield* tableExists(sql, storage.runsTable)),
    `${storage.migrationsTable} has steps recorded but ${storage.runsTable} does not exist. ` +
      'Another component, such as Cluster storage with the same prefix, uses that history; ' +
      'give the library its own prefix.',
  );
});

/**
 * Runs the library's loader into `<prefix>_migrations` and returns the applied steps as
 * `<history>:<id>_<name>`. `Migrator` locks the history table, so concurrent runs apply each step
 * once.
 */
export const runJobsMigrations = (
  storage: ResolvedStorage,
): Effect.Effect<ReadonlyArray<string>, MigratorFailure, SqlClient.SqlClient> =>
  preflight(storage).pipe(
    Effect.andThen(
      Migrator.make({})({
        loader: jobsMigrationLoader(storage),
        table: storage.migrationsTable,
      }),
    ),
    Effect.map((applied) =>
      applied.map(
        ([id, name]) => `${storage.migrationsTable}:${String(id).padStart(4, '0')}_${name}`,
      ),
    ),
  );

/**
 * Runs the library's migrations on the provided `SqlClient`, like Effect Cluster's
 * `SqlMessageStorage.layerMigrations`: use an owner connection in a deploy step, then
 * `layerStorage` with a DML-only runtime connection. Creates the library's tables only, never
 * Cluster's. Without `schema`, the tables go to the first existing schema of the connection's
 * `search_path`. The schema must exist. Unlike Effect's, failures are typed: `MigrationError`, or
 * `PgConfigError` for invalid options. Logs `jobs.migrate.completed` with the number of applied
 * steps.
 */
export const layerMigrations = (
  options?: JobsStorageOptions,
): Layer.Layer<never, MigrationError | PgConfigError, SqlClient.SqlClient> =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const storage = yield* resolveStorage(options);
      const applied = yield* runJobsMigrations(storage).pipe(
        Effect.mapError(
          (error) =>
            new MigrationError({ message: describeMigratorFailure(error), direction: 'up' }),
        ),
      );
      yield* logInfo('jobs.migrate.completed', {
        history: storage.migrationsTable,
        applied: applied.length,
      });
    }),
  );
