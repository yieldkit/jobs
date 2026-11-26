import type { PgConfigError } from '@yieldkit/pg';
import type { PostgresContainer } from '@yieldkit/pg/testing';
import type { SqlError } from 'effect/sql';

import { makeTestLogger } from '@yieldkit/logger/testing';
import { makePgLayer, POSTGRES_TEST_IMAGE } from '@yieldkit/pg';
import { startPostgresContainer } from '@yieldkit/pg/testing';
import { Effect, Layer, Redacted } from 'effect';
import { SqlClient } from 'effect/sql';

import type { JobsStorageOptions } from './storage-options.ts';

import { layerMigrations } from './migrations.ts';

/**
 * Image the integration tests run: `PG_TEST_IMAGE` when set, so CI can run the suite against
 * several PostgreSQL versions, and `@yieldkit/pg`'s default otherwise.
 */
export const INTEGRATION_IMAGE = process.env['PG_TEST_IMAGE'] ?? POSTGRES_TEST_IMAGE;

/**
 * Starts the PostgreSQL container a test file uses.
 */
export const startIntegrationPostgres = (): Promise<PostgresContainer> =>
  startPostgresContainer({ image: INTEGRATION_IMAGE });

/**
 * Logger that drops everything below `Fatal`, so expected failures do not flood the test output.
 */
export const quietLogger: Layer.Layer<never> = makeTestLogger({ minLevel: 'Fatal' }).layer;

/**
 * An application pool on `url`, the kind `JobStorage.layerStorage` runs on.
 */
export const appPool = (
  url: Redacted.Redacted,
  applicationName = 'jobs-integration',
): Layer.Layer<SqlClient.SqlClient, SqlError.SqlError | PgConfigError> =>
  makePgLayer({ url, applicationName, maxConnections: 4 });

/**
 * `url` with `user` and `password` in place of its credentials.
 */
export const withCredentials = (
  url: Redacted.Redacted,
  user: string,
  password: string,
): Redacted.Redacted => {
  const parsed = new URL(Redacted.value(url));
  parsed.username = user;
  parsed.password = password;
  return Redacted.make(parsed.toString());
};

/**
 * Creates database `name` in `postgres` and returns its connection URI.
 */
export const createDatabase = async (
  postgres: PostgresContainer,
  name: string,
): Promise<Redacted.Redacted> => {
  await runSql(
    postgres.connectionUri,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE DATABASE ${sql(name)}`;
    }),
  );
  const url = new URL(Redacted.value(postgres.connectionUri));
  url.pathname = `/${name}`;
  return Redacted.make(url.toString());
};

/**
 * Runs `effect` against a fresh application pool on `url`, with logs silenced.
 */
export const runSql = <A, E>(
  url: Redacted.Redacted,
  effect: Effect.Effect<A, E, SqlClient.SqlClient>,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(Effect.provide(Layer.mergeAll(appPool(url), quietLogger)), Effect.scoped),
  );

/**
 * Runs `JobStorage.layerMigrations(options)` on a fresh application pool on `url`, with logs
 * silenced: the deploy step for the library's own tables.
 */
export const migrateJobs = (url: Redacted.Redacted, options?: JobsStorageOptions): Promise<void> =>
  Effect.runPromise(
    Layer.build(layerMigrations(options).pipe(Layer.provide(appPool(url)))).pipe(
      Effect.scoped,
      Effect.asVoid,
      Effect.provide(quietLogger),
    ),
  );
