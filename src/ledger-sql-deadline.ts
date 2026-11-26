import type { SqlError } from 'effect/sql';

import { Effect, Schema } from 'effect';

import type { SqlLedgerContext } from './ledger-sql.ts';
import type { ClaimRequest, RunClaim } from './ledger-types.ts';

import { JobRunStatus } from './ledger-types.ts';

/**
 * Row schema of the key read by a claim past its deadline.
 */
const PastDeadlineRow = Schema.Struct({
  /**
   * Current status of the row.
   */
  status: JobRunStatus,

  /**
   * Workflow `execution_id` recorded on the row, or `null`.
   */
  executionId: Schema.NullOr(Schema.String),
});

/**
 * Answers a claim of a run past its deadline without taking the row or creating one: marks a
 * `failed` row exhausted, then returns `Completed` (`AlreadySettled` for its own execution) for a
 * `completed` row and `Exhausted` otherwise.
 */
export const refusePastDeadlineSql = (
  { sql, runsTable }: SqlLedgerContext,
  request: ClaimRequest,
): Effect.Effect<Exclude<RunClaim, { readonly _tag: 'Acquired' }>, SqlError.SqlError> =>
  sql`
    WITH exhausted AS (
      UPDATE ${sql(runsTable)}
      SET next_attempt_at = NULL
      WHERE job = ${request.job}
        AND run_key = ${request.runKey}
        AND status = 'failed'
      RETURNING 1
    )
    SELECT status, execution_id AS "executionId"
    FROM ${sql(runsTable)}
    WHERE job = ${request.job}
      AND run_key = ${request.runKey}
  `.pipe(
    Effect.flatMap((rows) =>
      Schema.decodeUnknownEffect(Schema.Array(PastDeadlineRow))(rows).pipe(Effect.orDie),
    ),
    Effect.map(([row]) => {
      if (row?.status !== 'completed') {
        return { _tag: 'Exhausted' };
      }

      const sameExecution =
        request.executionId !== undefined &&
        request.executionId !== null &&
        row.executionId === request.executionId;
      return sameExecution ? { _tag: 'AlreadySettled' } : { _tag: 'Completed' };
    }),
  );
