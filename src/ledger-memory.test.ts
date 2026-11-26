import { Context, Effect } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import type { RunResult } from './ledger-types.ts';

import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';

const FAILED: RunResult = {
  _tag: 'Failed',
  errorMessage: 'upstream 503',
  reason: 'error',
  cadence: 'Daily',
  trigger: 'schedule',
  annotations: Context.empty(),
};

describe('makeJobRunsMemory', () => {
  it('reports a repeated settle with the same token as settled without applying it twice', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        return yield* Effect.gen(function* () {
          const runs = yield* JobRuns;
          const claim = yield* runs.claim({
            job: 'settle-twice',
            runKey: '2026-11-25',
            trigger: 'schedule',
            force: false,
            params: {},
          });
          if (claim._tag !== 'Acquired') {
            return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
          }

          const owned = { job: 'settle-twice', runKey: '2026-11-25', ...claim };
          const first = yield* runs.settle(owned, FAILED);
          const repeated = yield* runs.settle(owned, FAILED);
          const asCompleted = yield* runs.settle(owned, {
            _tag: 'Completed',
            stats: {},
            trigger: 'schedule',
            annotations: Context.empty(),
          });
          return { first, repeated, asCompleted, rows: memory.rows(), settles: memory.settles() };
        }).pipe(Effect.provide(memory.layer));
      }),
    );
    expect(result.first).toBe(true);
    expect(result.repeated).toBe(true);
    expect(result.asCompleted).toBe(false);
    expect(result.rows[0]).toMatchObject({ status: 'failed', failures: 1 });
    expect(result.settles).toHaveLength(1);
  });

  it('keeps a Daily key exhausted after an interrupted run that took it manually', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory({ policy: { retry: { dailyMaxFailures: 2 } } });
        return yield* Effect.gen(function* () {
          const runs = yield* JobRuns;
          const key = { job: 'exhausted-interrupt', runKey: '2026-11-25' };
          const reasons = ['error', 'error', 'interrupted'] as const;
          for (const reason of reasons) {
            const claim = yield* runs.claim({ ...key, trigger: 'api', force: false, params: {} });
            if (claim._tag !== 'Acquired') {
              return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
            }

            yield* runs.settle({ ...key, ...claim }, { ...FAILED, trigger: 'api', reason });
          }

          const scheduled = yield* runs.claim({
            ...key,
            trigger: 'schedule',
            force: false,
            params: {},
          });
          return { scheduled, row: memory.rows()[0] };
        }).pipe(Effect.provide(memory.layer));
      }),
    );
    expect(result.row).toMatchObject({ failures: 2, nextAttemptAt: null });
    expect(result.scheduled).toEqual({ _tag: 'Exhausted' });
  });
});
