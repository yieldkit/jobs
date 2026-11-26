import type { SqlConnection } from 'effect/sql';

import { PgClient } from '@effect/sql-pg';
import { Effect, Layer, Stream } from 'effect';
import { Reactivity } from 'effect/reactivity';
import { SqlClient } from 'effect/sql';

/**
 * One statement the recording client was asked to run.
 */
export type RecordedStatement = {
  /**
   * SQL text as compiled for PostgreSQL, with `$n` placeholders.
   */
  readonly sql: string;

  /**
   * Bound parameter values.
   */
  readonly params: ReadonlyArray<unknown>;
};

/**
 * A `SqlClient` on PostgreSQL's compiler that records every statement instead of sending it, and
 * the statements it recorded.
 */
export type RecordingSql = {
  /**
   * Layer providing the recording client.
   */
  readonly layer: Layer.Layer<SqlClient.SqlClient>;

  /**
   * Every statement run so far, transaction control included.
   */
  readonly statements: Array<RecordedStatement>;
};

/**
 * Builds a recording `SqlClient`. `respond` returns the rows of each statement from its SQL and
 * bound parameters; by default every statement returns no rows.
 */
export const makeRecordingSql = (
  respond: (sql: string, params: ReadonlyArray<unknown>) => ReadonlyArray<object> = () => [],
): RecordingSql => {
  const statements: Array<RecordedStatement> = [];
  const run = (sql: string, params: ReadonlyArray<unknown>) =>
    Effect.sync(() => {
      statements.push({ sql, params });
      return respond(sql, params);
    });
  const connection: SqlConnection.Connection = {
    execute: (sql, params) => run(sql, params),
    executeRaw: (sql, params) => run(sql, params),
    executeStream: () => Stream.empty,
    executeValues: () => Effect.succeed([]),
    executeValuesUnprepared: () => Effect.succeed([]),
    executeUnprepared: (sql, params) => run(sql, params),
  };
  const layer = Layer.effect(
    SqlClient.SqlClient,
    SqlClient.make({
      acquirer: Effect.succeed(connection),
      compiler: PgClient.makeCompiler(),
      spanAttributes: [],
      driver: 'pg',
    }),
  ).pipe(Layer.provide(Reactivity.layer));
  return { layer, statements };
};

/**
 * Statements that read or write a table, without transaction control and savepoints.
 */
export const dataStatements = (statements: ReadonlyArray<RecordedStatement>): Array<string> =>
  statements.flatMap(({ sql }) =>
    /^(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)/i.test(sql.trim()) ? [] : [sql],
  );
