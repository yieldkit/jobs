import type { MigrationError } from '@yieldkit/pg';
import type { SqlError } from 'effect/sql';

import { PgConfigError } from '@yieldkit/pg';
import { Context, Effect, Layer } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { SqlClient } from 'effect/sql';

import type { ClusterConfigError, JobsNotMigratedError } from './errors.ts';
import type { JobSettleHook } from './ledger-types.ts';
import type { JobsPolicyOverrides } from './policy.ts';
import type { JobsStorageOptions } from './storage-options.ts';

import { makeSqlJobRuns, JobRuns } from './ledger.ts';
import { jobsMigrationLoader, layerMigrations } from './migrations.ts';
import { resolveJobsPolicy } from './policy.ts';
import { probeHistory } from './probe.ts';
import { JobRequests, makeSqlJobRequests } from './requests.ts';
import { bindSettleHook } from './settle-hook.ts';
import { resolveStorage } from './storage-options.ts';

export { layerMigrations, migrations } from './migrations.ts';

/**
 * Options of `layerStorage` and `layerWith`: where the tables are, the settle hook and policy
 * overrides.
 */
export type JobStorageLayerOptions<R = never> = JobsStorageOptions & {
  /**
   * Called once per successful fenced settle, inside the settle transaction and in a savepoint of
   * it. Writes through the same `SqlClient` commit with the settle; a failure or defect rolls back
   * only the hook's writes, is logged as `job.settle_hook_failed`, and the settle still commits. A
   * settle that lost its claim never calls it.
   */
  readonly onSettle?: JobSettleHook<R> | undefined;

  /**
   * Policy overrides merged over `defaultJobsPolicy`. Invalid values fail the layer with
   * `ClusterConfigError`.
   */
  readonly policy?: JobsPolicyOverrides | undefined;
};

/**
 * Fails with `PgConfigError` unless `sql` talks to PostgreSQL: the ledger relies on `FOR UPDATE`,
 * `ON CONFLICT`, `jsonb` and advisory locks.
 */
const assertPostgres = (sql: SqlClient.SqlClient): Effect.Effect<void, PgConfigError> =>
  sql.onDialectOrElse({
    pg: () => Effect.void,
    orElse: () =>
      Effect.fail(
        new PgConfigError({ message: '@yieldkit/jobs requires a PostgreSQL SqlClient.' }),
      ),
  });

/**
 * Run ledger and request store on the provided `SqlClient`, without DDL, like Effect Cluster's
 * `SqlMessageStorage.layerStorage`. Run `layerMigrations` with the same options first. On build it
 * checks that `<prefix>_migrations` is at least at the installed code's version and that the ledger
 * and request tables exist, and fails with `JobsNotMigratedError` otherwise; the role needs
 * `SELECT` on the history plus DML on the ledger and request tables. Without `schema`, tables
 * resolve through the client's `search_path`.
 */
export const layerStorage = <R = never>(
  options?: JobStorageLayerOptions<R>,
): Layer.Layer<
  JobRuns | JobRequests,
  JobsNotMigratedError | PgConfigError | ClusterConfigError | SqlError.SqlError,
  SqlClient.SqlClient | R
> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const storage = yield* resolveStorage(options);
      const policy = yield* resolveJobsPolicy(options?.policy);
      const sql = (yield* SqlClient.SqlClient).withoutTransforms();
      yield* assertPostgres(sql);
      yield* probeHistory({
        component: 'jobs',
        history: storage.migrationsTable,
        tables: [storage.runsTable, storage.requestsTable],
        loader: jobsMigrationLoader(storage),
      });
      const random = yield* SecureRandom.SecureRandom;
      const hookContext = yield* Effect.context<R>();
      const runs = makeSqlJobRuns(
        {
          sql,
          runsTable: storage.runsTable,
          retry: policy.retry,
          onSettle: bindSettleHook(options?.onSettle, hookContext),
        },
        policy,
        random.uuidV4(),
      );
      const requests = makeSqlJobRequests({
        sql,
        requestsTable: storage.requestsTable,
        runsTable: storage.runsTable,
      });
      return Context.make(JobRuns, runs).pipe(Context.add(JobRequests, requests));
    }),
  ).pipe(Layer.provide(SecureRandom.layerWebCrypto));

/**
 * `layerStorage` that first runs `layerMigrations` on the same client, like Effect Cluster's
 * `SqlMessageStorage.layerWith`, for tests and single-process tools. The role needs DDL rights;
 * production deployments should run `layerMigrations` as a separate deploy step instead.
 */
export const layerWith = <R = never>(
  options?: JobStorageLayerOptions<R>,
): Layer.Layer<
  JobRuns | JobRequests,
  MigrationError | JobsNotMigratedError | PgConfigError | ClusterConfigError | SqlError.SqlError,
  SqlClient.SqlClient | R
> => layerStorage(options).pipe(Layer.provide(layerMigrations(options)));

/**
 * `layerWith()` with default options, like Effect Cluster's `SqlMessageStorage.layer`: tables
 * `job_runs` and `job_requests` on the client's `search_path`, the default policy and no settle
 * hook.
 */
export const layer: Layer.Layer<
  JobRuns | JobRequests,
  MigrationError | JobsNotMigratedError | PgConfigError | ClusterConfigError | SqlError.SqlError,
  SqlClient.SqlClient
> = layerWith();
