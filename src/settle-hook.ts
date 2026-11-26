import type { Context } from 'effect';
import type { SqlClient, SqlError } from 'effect/sql';

import { describeError, logWarning } from '@yieldkit/logger';
import { Cause, Data, Effect } from 'effect';

import type { JobSettled } from './ledger-types.ts';

/**
 * A settle hook failed or died. Carries the hook's cause for the warning log line.
 */
export class SettleHookFailed extends Data.TaggedError('SettleHookFailed')<{
  /**
   * What the hook failed with.
   */
  readonly cause: Cause.Cause<unknown>;
}> {}

/**
 * A settle hook with its services provided and its failures turned into `SettleHookFailed`.
 */
export type BoundSettleHook = (event: JobSettled) => Effect.Effect<void, SettleHookFailed>;

/**
 * Binds `hook` to the services captured in `context` and maps any failure or defect to
 * `SettleHookFailed`. Interruptions are kept as interruptions. Returns `undefined` without a hook.
 */
export const bindSettleHook = <R, E>(
  hook: ((event: JobSettled) => Effect.Effect<void, E, R>) | undefined,
  context: Context.Context<R>,
): BoundSettleHook | undefined => {
  if (hook === undefined) {
    return undefined;
  }

  return (event) =>
    Effect.suspend(() => hook(event)).pipe(
      Effect.provideContext(context),
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.fail(new SettleHookFailed({ cause })),
      ),
    );
};

/**
 * Runs the settle hook for `event` in a nested transaction level (a savepoint) of the settle
 * transaction. A failure or defect rolls back only the hook's writes and drops its `afterCommit`
 * callbacks, is logged as `job.settle_hook_failed`, and the settle still commits. A `SELECT 1` ends
 * the level, so a hook that succeeded after catching one of its own SQL errors (which leaves the
 * level aborted) is rolled back the same way. An interruption is not swallowed.
 */
export const runSettleHook = (
  sql: SqlClient.SqlClient,
  hook: BoundSettleHook | undefined,
  event: JobSettled,
): Effect.Effect<void, SqlError.SqlError> => {
  if (hook === undefined) {
    return Effect.void;
  }

  return sql.withTransaction(hook(event).pipe(Effect.andThen(sql`SELECT 1`))).pipe(
    Effect.asVoid,
    Effect.catch((error) =>
      logWarning('job.settle_hook_failed', {
        job: event.job,
        runKey: event.runKey,
        attempt: event.attempt,
        outcome: event.outcome._tag,
        errorMessage: describeError(
          error._tag === 'SettleHookFailed' ? Cause.squash(error.cause) : error,
        ),
      }),
    ),
  );
};
