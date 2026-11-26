import type { RunResult } from './ledger-types.ts';
import type { RetryPolicy } from './policy.ts';

import { isWithinDailyDeadline } from './calendar.ts';
import { nextAttemptAt } from './policy.ts';

/**
 * `next_attempt_at` a failed settle of `runKey` stores, shared by every `JobRuns` implementation:
 * `null` when `nextAttemptAt` exhausts the key or its deadline has passed, else the result's
 * override, else `nextAttemptAt`'s instant. `failures` is the key's count after the settle.
 */
export const failedNextAttemptAt = (
  retry: RetryPolicy,
  result: Extract<RunResult, { readonly _tag: 'Failed' }>,
  settled: { readonly runKey: string; readonly failures: number; readonly now: Date },
): Date | null => {
  const ruled = nextAttemptAt(retry, {
    cadence: result.cadence,
    failures: settled.failures,
    interrupted: result.reason === 'interrupted',
    now: settled.now,
    maxFailures: result.maxFailures,
  });
  const pastDeadline =
    result.deadline !== undefined &&
    !isWithinDailyDeadline(result.deadline, settled.runKey, settled.now.getTime());
  if (ruled === null || pastDeadline) {
    return null;
  }

  return result.nextAttemptAtOverride === undefined ? ruled : result.nextAttemptAtOverride;
};
