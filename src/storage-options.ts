import { PgConfigError } from '@yieldkit/pg';
import { Effect } from 'effect';

/**
 * Where the library's tables live, in the shape of Effect Cluster's `SqlMessageStorage` options.
 * Shared by `JobStorage.migrations`, `layerMigrations`, `layerStorage`, `layerWith` and `layer`;
 * pass the same value to all of them.
 */
export type JobsStorageOptions = {
  /**
   * Prefix of the library's tables: `<prefix>_runs`, `<prefix>_requests` and the history
   * `<prefix>_migrations`. Default `job`. It must differ from your Cluster storage's prefix when
   * both share a schema, since both keep their history in `<prefix>_migrations`.
   */
  readonly prefix?: string | undefined;

  /**
   * Schema of the library's tables. When omitted, table names are unqualified and resolve through
   * the connection's `search_path`, as Effect Cluster's tables do. When set, every statement names
   * it explicitly.
   */
  readonly schema?: string | undefined;
};

/**
 * Validated table and schema names derived from `JobsStorageOptions`. Table names are written
 * `schema.table` when a schema is set and bare otherwise, and passed to `sql(...)`, which quotes
 * each part.
 */
export type ResolvedStorage = {
  /**
   * Schema of the library's tables, or `undefined` to resolve them through `search_path`.
   */
  readonly schema: string | undefined;

  /**
   * Prefix of the library's tables.
   */
  readonly prefix: string;

  /**
   * Run ledger table, schema-qualified when a schema is set.
   */
  readonly runsTable: string;

  /**
   * Request table, schema-qualified when a schema is set.
   */
  readonly requestsTable: string;

  /**
   * Migration history of the library's tables, schema-qualified when a schema is set.
   */
  readonly migrationsTable: string;

  /**
   * Unqualified name of the migration history.
   */
  readonly migrationsTableName: string;

  /**
   * Index on `(job, started_at DESC)` of the run ledger.
   */
  readonly runsRecentIndex: string;

  /**
   * CHECK constraint on the run ledger's `status`.
   */
  readonly runsStatusCheck: string;

  /**
   * CHECK constraint on the run ledger's `trigger`.
   */
  readonly runsTriggerCheck: string;
};

/**
 * Default prefix of the library's tables.
 */
export const DEFAULT_PREFIX = 'job';

/**
 * Effect Cluster's default table prefix. The library refuses it: its `<prefix>_migrations` would
 * share a history with Cluster's message storage, and each would skip the other's steps.
 */
const CLUSTER_DEFAULT_PREFIX = 'cluster';

/**
 * Accepted identifiers: lowercase letters, digits and underscores, not starting with a digit. No
 * quoting rules or case folding apply to them, so quoting is exact and name transforms are no-ops.
 */
const IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]*$/;

/**
 * Longest identifier PostgreSQL keeps without truncating it.
 */
const MAX_IDENTIFIER_LENGTH = 63;

/**
 * Longest `prefix`: leaves room under 63 bytes for `<prefix>_runs_trigger_check` and names of
 * future steps.
 */
const MAX_PREFIX_LENGTH = 37;

/**
 * Checks one identifier against `IDENTIFIER_PATTERN` and `maxLength`, failing with `PgConfigError`
 * that names the option.
 */
const validateIdentifier = (
  option: string,
  value: string,
  maxLength: number,
): Effect.Effect<string, PgConfigError> => {
  if (!IDENTIFIER_PATTERN.test(value) || value.length > maxLength) {
    return Effect.fail(
      new PgConfigError({
        message:
          `Invalid ${option} "${value}": expected lowercase letters, digits and underscores, ` +
          `starting with a letter or underscore, at most ${String(maxLength)} characters.`,
      }),
    );
  }

  return Effect.succeed(value);
};

/**
 * `name` qualified with `schema` when one is set.
 */
const qualify = (schema: string | undefined, name: string): string =>
  schema === undefined ? name : `${schema}.${name}`;

/**
 * Validates `options` and derives every table, index and constraint name. Fails with
 * `PgConfigError` when a name is not a plain lowercase identifier, the prefix is too long, or the
 * prefix is Effect Cluster's default `cluster`.
 */
export const resolveStorage = Effect.fnUntraced(function* (
  options: JobsStorageOptions | undefined,
): Effect.fn.Return<ResolvedStorage, PgConfigError> {
  const schema =
    options?.schema === undefined
      ? undefined
      : yield* validateIdentifier('schema', options.schema, MAX_IDENTIFIER_LENGTH);
  const prefix = yield* validateIdentifier(
    'prefix',
    options?.prefix ?? DEFAULT_PREFIX,
    MAX_PREFIX_LENGTH,
  );
  if (prefix === CLUSTER_DEFAULT_PREFIX) {
    return yield* new PgConfigError({
      message:
        `prefix "${CLUSTER_DEFAULT_PREFIX}" is Effect Cluster's default: both would keep their ` +
        `history in ${CLUSTER_DEFAULT_PREFIX}_migrations. Choose a different prefix.`,
    });
  }

  return {
    schema,
    prefix,
    runsTable: qualify(schema, `${prefix}_runs`),
    requestsTable: qualify(schema, `${prefix}_requests`),
    migrationsTable: qualify(schema, `${prefix}_migrations`),
    migrationsTableName: `${prefix}_migrations`,
    runsRecentIndex: `idx_${prefix}_runs_recent`,
    runsStatusCheck: `${prefix}_runs_status_check`,
    runsTriggerCheck: `${prefix}_runs_trigger_check`,
  };
});
