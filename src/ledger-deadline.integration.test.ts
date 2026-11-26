import type { PostgresContainer } from '@yieldkit/pg/testing';

import { Context, Effect, Layer, Result } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import { dailyScheduleFor } from './calendar.ts';
import {
  appPool,
  migrateJobs,
  quietLogger,
  startIntegrationPostgres,
} from './integration.fixture.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';
import { omitUndefined } from './omit-undefined.ts';
import { layerStorage } from './storage.ts';

let postgres: PostgresContainer;

beforeAll(async () => {
  postgres = await startIntegrationPostgres();
  await migrateJobs(postgres.connectionUri);
});

afterAll(async () => {
  await postgres.stop();
});

const deadline = Result.getOrThrow(
  dailyScheduleFor('j', { _tag: 'Daily', at: '09:00', until: '12:00' }),
);

/**
 * A manual claim request of `(job, runKey)`.
 */
const manualClaim = (job: string, runKey: string, pastDeadline?: boolean) => ({
  job,
  runKey,
  trigger: 'manual' as const,
  force: false,
  params: {},
  ...omitUndefined({ pastDeadline }),
});

/**
 * Runs the same script against one ledger: failed settles of a past and a future day, a claim of
 * each that is past its deadline, of a missing key and of a completed key. Returns what it saw.
 */
const script = (job: string) =>
  Effect.gen(function* () {
    const runs = yield* JobRuns;
    const fail = (runKey: string) =>
      Effect.gen(function* () {
        const claim = yield* runs.claim(manualClaim(job, runKey));
        if (claim._tag !== 'Acquired') {
          return claim._tag;
        }

        yield* runs.settle(
          { job, runKey, token: claim.token, attempt: claim.attempt },
          {
            _tag: 'Failed',
            errorMessage: 'down',
            reason: 'error',
            cadence: 'Daily',
            maxFailures: Number.POSITIVE_INFINITY,
            deadline,
            trigger: 'manual',
            annotations: Context.empty(),
          },
        );
        return claim.failures;
      });
    const rows = () =>
      runs
        .recent({ job, limit: 10 })
        .pipe(
          Effect.map((all) =>
            Object.fromEntries(
              all.map((row) => [row.runKey, `${row.status}/${String(row.nextAttemptAt !== null)}`]),
            ),
          ),
        );

    const pastFailures = yield* fail('2000-01-01');
    const futureFailures = [yield* fail('2999-01-01'), yield* fail('2999-01-01')];
    const afterSettles = yield* rows();
    const completed = yield* runs.claim(manualClaim(job, '2999-01-02'));
    if (completed._tag === 'Acquired') {
      yield* runs.settle(
        { job, runKey: '2999-01-02', token: completed.token, attempt: completed.attempt },
        { _tag: 'Completed', stats: {}, trigger: 'manual', annotations: Context.empty() },
      );
    }

    const pastDeadline = {
      failed: (yield* runs.claim(manualClaim(job, '2999-01-01', true)))._tag,
      missing: (yield* runs.claim(manualClaim(job, '2999-01-03', true)))._tag,
      completed: (yield* runs.claim(manualClaim(job, '2999-01-02', true)))._tag,
    };
    return { pastFailures, futureFailures, afterSettles, pastDeadline, end: yield* rows() };
  });

const expected = {
  pastFailures: 0,
  futureFailures: [0, 1],
  afterSettles: { '2000-01-01': 'failed/false', '2999-01-01': 'failed/true' },
  pastDeadline: { failed: 'Exhausted', missing: 'Exhausted', completed: 'Completed' },
  end: {
    '2000-01-01': 'failed/false',
    '2999-01-01': 'failed/false',
    '2999-01-02': 'completed/false',
  },
};

describe('deadline and claim failures in both ledgers', () => {
  it('match in the SQL ledger', async () => {
    const layer = Layer.mergeAll(
      layerStorage().pipe(Layer.provide(appPool(postgres.connectionUri))),
      quietLogger,
    );
    const seen = await Effect.runPromise(
      script('sql-deadline').pipe(Effect.provide(layer), Effect.scoped),
    );
    expect(seen).toEqual(expected);
  });

  it('match in the memory ledger', async () => {
    const seen = await Effect.runPromise(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        return yield* script('memory-deadline').pipe(Effect.provide(memory.layer));
      }),
    );
    expect(seen).toEqual(expected);
  });
});
