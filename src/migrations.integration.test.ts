import type { PgConfigError } from '@yieldkit/pg';
import type { PostgresContainer } from '@yieldkit/pg/testing';
import type { Redacted } from 'effect';
import type { SqlError } from 'effect/sql';

import { makePgLayer } from '@yieldkit/pg';
import { Effect, Layer, Schema } from 'effect';
import { SqlMessageStorage, SqlRunnerStorage } from 'effect/cluster';
import { SqlClient } from 'effect/sql';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { JobsStorageOptions } from './storage-options.ts';

import {
  appPool,
  createDatabase,
  quietLogger,
  runSql,
  startIntegrationPostgres,
  withCredentials,
} from './integration.fixture.ts';
import { JobRuns } from './ledger.ts';
import { runJobsMigrations } from './migrations.ts';
import { resolveStorage } from './storage-options.ts';
import { layerMigrations, layerStorage } from './storage.ts';

/**
 * A row of the table listing.
 */
const TableRow = Schema.Struct({ schema: Schema.String, name: Schema.String });

/**
 * Every ordinary table outside the system schemas, as `schema.table`, sorted.
 */
const listTables = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`
    SELECT table_schema AS schema, table_name AS name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY 1, 2
  `;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(TableRow))(rows);
  return decoded.map((row) => `${row.schema}.${row.name}`);
});

/**
 * A pool on `url` whose `search_path` is `schema`, like an application's own.
 */
const searchPathPool = (url: Redacted.Redacted, schema: string) =>
  makePgLayer({ url, applicationName: 'jobs-search-path', maxConnections: 2, searchPath: schema });

/**
 * Runs the library's migrations with `options` on `pool` (a fresh application pool on `url` by
 * default) and returns the applied steps.
 */
const applyJobs = (
  url: Redacted.Redacted,
  options?: JobsStorageOptions,
  pool: Layer.Layer<SqlClient.SqlClient, SqlError.SqlError | PgConfigError> = appPool(url),
) =>
  Effect.runPromise(
    resolveStorage(options).pipe(
      Effect.flatMap(runJobsMigrations),
      Effect.provide(pool),
      Effect.scoped,
      Effect.provide(quietLogger),
    ),
  );

/**
 * Builds `layer` and returns its failure, or `undefined` when it builds.
 */
const buildError = <A, E>(layer: Layer.Layer<A, E>): Promise<E | undefined> =>
  Effect.runPromise(
    Layer.build(layer).pipe(
      Effect.scoped,
      Effect.as(undefined),
      Effect.catch((error) => Effect.succeed(error)),
      Effect.provide(quietLogger),
    ),
  );

/**
 * Builds `JobStorage.layerMigrations(options)` on `url` and returns its failure.
 */
const migrationError = (url: Redacted.Redacted, options?: JobsStorageOptions) =>
  buildError(layerMigrations(options).pipe(Layer.provide(appPool(url))));

let postgres: PostgresContainer;

beforeAll(async () => {
  postgres = await startIntegrationPostgres();
});

afterAll(async () => {
  await postgres.stop();
});

describe('JobStorage.layerMigrations', () => {
  it("creates only the library's tables on the search_path, then finds nothing to do", async () => {
    const url = await createDatabase(postgres, 'defaults');
    expect(await applyJobs(url)).toEqual([
      'job_migrations:0001_create_tables',
      'job_migrations:0002_add_runs_history_index',
    ]);
    // No Cluster table anywhere: the application's Cluster is the application's to migrate.
    expect(await runSql(url, listTables)).toEqual([
      'public.job_migrations',
      'public.job_requests',
      'public.job_runs',
    ]);
    expect(await applyJobs(url)).toEqual([]);
  });

  it('shares a schema and search_path with Cluster tables the application migrated itself', async () => {
    const url = await createDatabase(postgres, 'shared_schema');
    await runSql(
      url,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE SCHEMA app`;
      }),
    );
    const pool = searchPathPool(url, 'app');
    await Effect.runPromise(
      Layer.build(
        Layer.mergeAll(
          SqlMessageStorage.layerMigrations({ prefix: 'platform' }),
          SqlRunnerStorage.layerMigrations({ prefix: 'platform' }),
        ).pipe(Layer.provide(pool)),
      ).pipe(Effect.scoped, Effect.provide(quietLogger)),
    );
    expect(await applyJobs(url, undefined, pool)).toEqual([
      'job_migrations:0001_create_tables',
      'job_migrations:0002_add_runs_history_index',
    ]);
    expect(await runSql(url, listTables)).toEqual([
      'app.job_migrations',
      'app.job_requests',
      'app.job_runs',
      'app.platform_locks',
      'app.platform_messages',
      'app.platform_migrations',
      'app.platform_replies',
      'app.platform_runner_migrations',
      'app.platform_runners',
    ]);
    expect(await buildError(layerStorage().pipe(Layer.provide(pool)))).toBe(undefined);
  });

  it('puts the tables in an explicit schema with a custom prefix, whatever the search_path', async () => {
    const url = await createDatabase(postgres, 'custom');
    await runSql(
      url,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE SCHEMA billing`;
      }),
    );
    const options = { schema: 'billing', prefix: 'task' };
    expect(await applyJobs(url, options)).toEqual([
      'billing.task_migrations:0001_create_tables',
      'billing.task_migrations:0002_add_runs_history_index',
    ]);
    expect(await runSql(url, listTables)).toEqual([
      'billing.task_migrations',
      'billing.task_requests',
      'billing.task_runs',
    ]);
    expect(await buildError(layerStorage(options).pipe(Layer.provide(appPool(url))))).toBe(
      undefined,
    );
  });

  it('applies the steps exactly once under concurrent deploys', async () => {
    const url = await createDatabase(postgres, 'concurrent');
    const results = await Promise.all(Array.from({ length: 8 }, () => applyJobs(url)));
    expect(results.filter((applied) => applied.length > 0)).toEqual([
      ['job_migrations:0001_create_tables', 'job_migrations:0002_add_runs_history_index'],
    ]);
    expect(results.filter((applied) => applied.length === 0)).toHaveLength(7);
  });

  it('fails layerStorage with JobsNotMigratedError before migrating and when behind', async () => {
    const url = await createDatabase(postgres, 'probe');
    const storage = layerStorage().pipe(Layer.provide(appPool(url)));
    expect(await buildError(storage)).toMatchObject({
      _tag: 'JobsNotMigratedError',
      component: 'jobs',
      table: 'job_migrations',
      found: 0,
      expected: 2,
    });

    await applyJobs(url);
    expect(await buildError(storage)).toBe(undefined);

    // A database at step 1 reads as behind until step 2 runs.
    await runSql(
      url,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM public.job_migrations WHERE migration_id = 2`;
      }),
    );
    expect(await buildError(storage)).toMatchObject({
      _tag: 'JobsNotMigratedError',
      found: 1,
      expected: 2,
    });
    expect(await applyJobs(url)).toEqual(['job_migrations:0002_add_runs_history_index']);
    expect(await buildError(storage)).toBe(undefined);

    // A history that lost its rows reads as behind.
    await runSql(
      url,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM public.job_migrations`;
      }),
    );
    const behind = await buildError(storage);
    expect(behind).toMatchObject({ _tag: 'JobsNotMigratedError', found: 0, expected: 2 });
    expect(behind?.message).toBe(
      'job_migrations is not migrated (found 0, needs 2). Run JobStorage.layerMigrations first.',
    );
  });

  it('refuses a history that Cluster storage with the same prefix already uses', async () => {
    const url = await createDatabase(postgres, 'shared_history');
    await Effect.runPromise(
      Layer.build(
        SqlMessageStorage.layerMigrations({ prefix: 'shared' }).pipe(Layer.provide(appPool(url))),
      ).pipe(Effect.scoped, Effect.provide(quietLogger)),
    );

    // At runtime: the history is current, but the ledger it should describe does not exist.
    const runtime = await buildError(
      layerStorage({ prefix: 'shared' }).pipe(Layer.provide(appPool(url))),
    );
    expect(runtime).toMatchObject({ _tag: 'JobsNotMigratedError', table: 'shared_migrations' });
    expect(runtime?.message).toContain('shared_runs does not exist');

    // At deploy time: nothing is created, and the shared history is left as it was.
    const deploy = await migrationError(url, { prefix: 'shared' });
    expect(deploy).toMatchObject({ _tag: 'MigrationError' });
    expect(deploy?.message).toContain('shared_migrations has steps recorded but shared_runs');
    expect(await runSql(url, listTables)).not.toContain('public.shared_runs');
  });

  it('lets Cluster storage fail loudly when the library took the shared history first', async () => {
    const url = await createDatabase(postgres, 'reverse_history');
    await applyJobs(url, { prefix: 'shared' });
    // Effect skips its own step 1 as applied, and its step 2 then fails on the missing table.
    const exit = await Effect.runPromiseExit(
      Layer.build(
        SqlMessageStorage.layerMigrations({ prefix: 'shared' }).pipe(Layer.provide(appPool(url))),
      ).pipe(Effect.scoped, Effect.provide(quietLogger)),
    );
    expect(exit._tag).toBe('Failure');
    expect(String(exit._tag === 'Failure' ? exit.cause : '')).toContain(
      'relation "shared_messages" does not exist',
    );
  });

  it('refuses a missing schema and a role that cannot create tables', async () => {
    const url = await createDatabase(postgres, 'privileges');
    const missing = await migrationError(url, { schema: 'absent' });
    expect(missing).toMatchObject({ _tag: 'MigrationError' });
    expect(missing?.message).toContain('Schema "absent" does not exist');

    const password = 'limited-s3cret';
    await runSql(
      url,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql.unsafe(`CREATE ROLE limited LOGIN PASSWORD '${password}'`);
        yield* sql`CREATE SCHEMA locked`;
        yield* sql`GRANT USAGE ON SCHEMA locked TO limited`;
      }),
    );
    const denied = await migrationError(withCredentials(url, 'limited', password), {
      schema: 'locked',
    });
    expect(denied).toMatchObject({ _tag: 'MigrationError' });
    expect(denied?.message).toContain(
      'The migration role cannot create tables in schema "locked". GRANT CREATE ON SCHEMA locked',
    );
    expect(denied?.message).not.toContain(password);
  });

  it('adopts tables an application created before, after it rewrites their trigger CHECK', async () => {
    const url = await createDatabase(postgres, 'adoption');
    await runSql(url, createLegacyTables);

    // The old CHECK does not allow 'manual': the guard fails the migration and records nothing.
    const refused = await migrationError(url);
    expect(refused).toMatchObject({ _tag: 'MigrationError' });
    expect(refused?.message).toContain("does not allow 'schedule', 'manual' and 'api'");
    expect(refused?.message).toContain('Adopting existing tables');
    // Effect's `Migrator` creates the history before its transaction: it stays, with no rows.
    expect(await runSql(url, historyRows)).toBe(0);

    await runSql(url, rewriteLegacyTriggerCheck);
    // Step 0001 is a no-op on the adopted tables; step 0002 adds the history index.
    expect(await applyJobs(url)).toEqual([
      'job_migrations:0001_create_tables',
      'job_migrations:0002_add_runs_history_index',
    ]);
    expect(await runSql(url, legacySnapshot)).toEqual({
      runs: [
        { job: 'legacy-daily', runKey: '2026-11-20', trigger: 'schedule', status: 'completed' },
        { job: 'legacy-import', runKey: 'once', trigger: 'manual', status: 'completed' },
      ],
      requests: 1,
      leftoverColumns: ['attempts', 'lease_expires_at', 'lease_token'],
    });

    const recent = await Effect.runPromise(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        const claim = yield* runs.claim({
          job: 'legacy-import',
          runKey: 'once',
          trigger: 'manual',
          force: true,
          params: {},
        });
        expect(claim._tag).toBe('Acquired');
        return yield* runs.recent({ job: 'legacy-daily', limit: 5 });
      }).pipe(
        Effect.provide(
          Layer.mergeAll(layerStorage().pipe(Layer.provide(appPool(url))), quietLogger),
        ),
        Effect.scoped,
      ),
    );
    expect(recent.map((row) => row.runKey)).toEqual(['2026-11-20']);
  });

  it.each([
    ['rejects manual', "trigger <> 'manual'"],
    ['lacks api', "trigger IN ('schedule', 'manual')"],
  ])('refuses to adopt a trigger CHECK that %s', async (label, check) => {
    const url = await createDatabase(postgres, `adoption_${label.replace(' ', '_')}`);
    await runSql(url, createLegacyTables);
    await runSql(
      url,
      runScript(`
        ALTER TABLE job_runs DROP CONSTRAINT job_runs_trigger_check;
        UPDATE job_runs SET trigger = 'schedule' WHERE trigger = 'cli';
        ALTER TABLE job_runs ADD CONSTRAINT job_runs_trigger_check CHECK (${check});
      `),
    );
    const refused = await migrationError(url);
    expect(refused).toMatchObject({ _tag: 'MigrationError' });
    expect(refused?.message).toContain("does not allow 'schedule', 'manual' and 'api'");
  });
});

/**
 * Runs each `;`-terminated statement of `script` in order.
 */
const runScript = (script: string) =>
  SqlClient.SqlClient.use((sql) =>
    Effect.forEach(
      script.split(/;\s*\n/).filter((statement) => statement.trim().length > 0),
      (statement) => sql.unsafe(statement),
      { discard: true },
    ),
  );

/**
 * The tables an application owned before adopting the library, created with its own DDL: the
 * library's columns plus a `('schedule', 'cli', 'api')` trigger CHECK, leftover queue columns and
 * indexes, and a few rows.
 */
const createLegacyTables = runScript(`
    CREATE TABLE job_runs (
      job              TEXT        NOT NULL,
      run_key          TEXT        NOT NULL,
      status           TEXT        NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
      claim_token      UUID        NOT NULL,
      lease_expires_at TIMESTAMPTZ NOT NULL,
      attempt          INTEGER     NOT NULL DEFAULT 1,
      failures         INTEGER     NOT NULL DEFAULT 0,
      next_attempt_at  TIMESTAMPTZ,
      trigger          TEXT        NOT NULL CHECK (trigger IN ('schedule', 'cli', 'api')),
      params           JSONB       NOT NULL DEFAULT '{}'::jsonb,
      stats            JSONB,
      error_message    TEXT,
      started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      finished_at      TIMESTAMPTZ,
      PRIMARY KEY (job, run_key)
    );
    CREATE INDEX idx_job_runs_recent ON job_runs (job, started_at DESC);
    CREATE TABLE job_requests (
      id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
      job          TEXT        NOT NULL,
      params       JSONB       NOT NULL DEFAULT '{}'::jsonb,
      force        BOOLEAN     NOT NULL DEFAULT FALSE,
      requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      picked_at    TIMESTAMPTZ,
      finished_at  TIMESTAMPTZ,
      run_key      TEXT,
      outcome      JSONB
    );
    ALTER TABLE job_requests
      ADD COLUMN lease_token UUID,
      ADD COLUMN lease_expires_at TIMESTAMPTZ,
      ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
    CREATE INDEX idx_job_requests_unfinished ON job_requests (requested_at, id)
      WHERE finished_at IS NULL;
    ALTER TABLE job_runs ADD COLUMN execution_id TEXT;
    CREATE INDEX idx_job_runs_execution_id ON job_runs (execution_id)
      WHERE execution_id IS NOT NULL;
    ALTER TABLE job_requests ADD COLUMN execution_id TEXT;
    INSERT INTO job_runs (job, run_key, status, claim_token, lease_expires_at, trigger, stats)
    VALUES
      ('legacy-daily', '2026-11-20', 'completed', gen_random_uuid(), NOW(), 'schedule', '{"n":1}'),
      ('legacy-import', 'once', 'completed', gen_random_uuid(), NOW(), 'cli', '{"n":2}');
    INSERT INTO job_requests (job, params, force) VALUES ('legacy-daily', '{}', false);
`);

/**
 * The application's one-off migration before adopting the library: rewrite `cli` rows to `manual`
 * and replace the trigger CHECK.
 */
const rewriteLegacyTriggerCheck = runScript(`
    ALTER TABLE job_runs DROP CONSTRAINT job_runs_trigger_check;
    UPDATE job_runs SET trigger = 'manual' WHERE trigger = 'cli';
    ALTER TABLE job_runs ADD CONSTRAINT job_runs_trigger_check
      CHECK (trigger IN ('schedule', 'manual', 'api'));
`);

/**
 * Rows in `public.job_migrations`.
 */
const historyRows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`SELECT count(*)::int AS count FROM public.job_migrations`;
  const [row] = yield* Schema.decodeUnknownEffect(
    Schema.Array(Schema.Struct({ count: Schema.Int })),
  )(rows);
  return row?.count;
});

/**
 * The adopted rows and leftover columns, to check that adoption lost nothing.
 */
const legacySnapshot = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const runs = yield* sql`
    SELECT job, run_key AS "runKey", trigger, status FROM public.job_runs ORDER BY job
  `;
  const requests = yield* sql`SELECT count(*)::int AS count FROM public.job_requests`;
  const columns = yield* sql`
    SELECT column_name AS name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'job_requests'
      AND column_name IN ('lease_token', 'lease_expires_at', 'attempts')
    ORDER BY 1
  `;
  const decodedRuns = yield* Schema.decodeUnknownEffect(
    Schema.Array(
      Schema.Struct({
        job: Schema.String,
        runKey: Schema.String,
        trigger: Schema.String,
        status: Schema.String,
      }),
    ),
  )(runs);
  const count = yield* Schema.decodeUnknownEffect(Schema.Struct({ count: Schema.Int }))(
    requests[0],
  );
  const names = yield* Schema.decodeUnknownEffect(
    Schema.Array(Schema.Struct({ name: Schema.String })),
  )(columns);
  return {
    runs: decodedRuns,
    requests: count.count,
    leftoverColumns: names.map((column) => column.name),
  };
});
