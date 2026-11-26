import type { PostgresContainer } from '@yieldkit/pg/testing';
import type { Redacted } from 'effect';

import { makeTestLogger } from '@yieldkit/logger/testing';
import {
  Cause,
  Context,
  Data,
  DateTime,
  Duration,
  Effect,
  Exit,
  Layer,
  Result,
  Schedule,
  Schema,
} from 'effect';
import { SecureRandom } from 'effect/crypto';
import { SqlClient } from 'effect/sql';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { JobSettled, OwnedRun, RunClaim } from './ledger-types.ts';

import {
  appPool,
  migrateJobs,
  quietLogger,
  runSql,
  startIntegrationPostgres,
} from './integration.fixture.ts';
import { defineJob } from './job.ts';
import { UpstreamError } from './jobs.fixture.ts';
import { JobRuns } from './ledger.ts';
import { defaultJobsPolicy } from './policy.ts';
import { runJob } from './run.ts';
import { layerStorage, layerWith } from './storage.ts';

const TEST_LEASE_DURATION = '1 second';
const LEASE_POLL_INTERVAL = Duration.millis(50);
const LEASE_POLL_ATTEMPTS = 120;
const MAX_SCHEDULED_DAILY_FAILURES = defaultJobsPolicy.retry.dailyMaxFailures;

const JobKeyRow = Schema.Struct({
  job: Schema.String,
  runKey: Schema.String,
});

/**
 * The ledger with a one-second lease on an application pool, logs silenced.
 */
const buildTestLayer = (url: Redacted.Redacted) =>
  Layer.mergeAll(
    appPool(url),
    layerStorage({ policy: { lease: { duration: TEST_LEASE_DURATION } } }).pipe(
      Layer.provide(appPool(url)),
    ),
    quietLogger,
  );

let postgres: PostgresContainer;
let testLayer: ReturnType<typeof buildTestLayer>;

const runDb = <A, E>(
  effect: Effect.Effect<A, E, Layer.Success<ReturnType<typeof buildTestLayer>>>,
): Promise<A> => Effect.runPromise(effect.pipe(Effect.provide(testLayer), Effect.scoped));

/**
 * Retried by `pollUntilAcquired` while another claim's lease on the key has not yet expired.
 */
class LeaseStillActive extends Data.TaggedError('LeaseStillActive')<{
  /**
   * `_tag` of the non-acquired `RunClaim` returned by the last attempt.
   */
  readonly claim: string;
}> {}

const pollUntilAcquired = (
  runs: JobRuns['Service'],
  job: string,
  runKey: string,
): Effect.Effect<Extract<RunClaim, { readonly _tag: 'Acquired' }>, LeaseStillActive> =>
  Effect.gen(function* () {
    const claim = yield* runs
      .claim({
        job,
        runKey,
        trigger: 'schedule',
        force: false,
        params: {},
      })
      .pipe(Effect.orDie);
    if (claim._tag !== 'Acquired') {
      return yield* new LeaseStillActive({ claim: claim._tag });
    }

    return claim;
  }).pipe(
    Effect.retry({
      times: LEASE_POLL_ATTEMPTS,
      schedule: Schedule.spaced(LEASE_POLL_INTERVAL),
    }),
  );

/**
 * Settle payload fields every test result shares.
 */
const SETTLE_BASE = { trigger: 'schedule', annotations: Context.empty() } as const;

/**
 * A claim token no claim minted, standing in for a stale owner.
 */
const STALE_TOKEN = '0b5f1d6e-8c2a-4e7b-9f31-2d4c6a8e0b13';

beforeAll(async () => {
  postgres = await startIntegrationPostgres();
  await migrateJobs(postgres.connectionUri);
  testLayer = buildTestLayer(postgres.connectionUri);
});

afterAll(async () => {
  await postgres.stop();
});

describe('JobRuns SQL ledger', () => {
  it('grants one Acquired and one Busy for two concurrent claims on the same key', async () => {
    const claims = await runDb(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        const request = {
          job: 'concurrent-job',
          runKey: '2026-11-24T10:00Z',
          trigger: 'schedule' as const,
          force: false,
          params: { limit: 10 },
        };
        return yield* Effect.all([runs.claim(request), runs.claim(request)], { concurrency: 2 });
      }),
    );
    const tags = claims.map((claim) => claim._tag).toSorted();
    expect(tags).toEqual(['Acquired', 'Busy']);

    const acquired = claims.find((claim) => claim._tag === 'Acquired');
    expect(acquired?.attempt).toBe(1);
  });

  it('takes over an expired lease with incremented attempt and fences out the old token', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        const job = 'lease-takeover-job';
        const runKey = '2026-11-24';
        const first = yield* runs.claim({
          job,
          runKey,
          trigger: 'schedule',
          force: false,
          params: {},
        });
        if (first._tag !== 'Acquired') {
          return yield* Effect.die(new Error(`expected Acquired, got ${first._tag}`));
        }

        const immediateBusy = yield* runs.claim({
          job,
          runKey,
          trigger: 'schedule',
          force: false,
          params: {},
        });
        const second = yield* pollUntilAcquired(runs, job, runKey);

        const oldOwned: OwnedRun = {
          job,
          runKey,
          token: first.token,
          attempt: first.attempt,
        };
        const newOwned: OwnedRun = {
          job,
          runKey,
          token: second.token,
          attempt: second.attempt,
        };

        const oldHeartbeat = yield* runs.heartbeat(oldOwned);
        const oldComplete = yield* runs.settle(oldOwned, {
          _tag: 'Completed',
          stats: { stale: true },
          ...SETTLE_BASE,
        });
        const oldFail = yield* runs.settle(oldOwned, {
          _tag: 'Failed',
          errorMessage: 'stale worker failure',
          reason: 'error',
          cadence: 'Daily',
          ...SETTLE_BASE,
        });
        const newSettle = yield* runs.settle(newOwned, {
          _tag: 'Completed',
          stats: { winner: 2 },
          ...SETTLE_BASE,
        });
        const completed = yield* runs.isCompleted(job, runKey);
        const recent = yield* runs.recent({ job, limit: 1 });
        return {
          firstToken: first.token,
          immediateBusy,
          secondAttempt: second.attempt,
          secondToken: second.token,
          oldHeartbeat,
          oldComplete,
          oldFail,
          newSettle,
          completed,
          latest: recent[0],
        };
      }),
    );
    expect(result.immediateBusy).toEqual({ _tag: 'Busy' });
    expect(result.secondAttempt).toBe(2);
    expect(result.secondToken).not.toBe(result.firstToken);
    expect(result.oldHeartbeat).toBe(false);
    expect(result.oldComplete).toBe(false);
    expect(result.oldFail).toBe(false);
    expect(result.newSettle).toBe(true);
    expect(result.completed).toBe(true);
    expect(result.latest).toMatchObject({
      job: 'lease-takeover-job',
      runKey: '2026-11-24',
      status: 'completed',
      attempt: 2,
      stats: { winner: 2 },
    });
  });

  it('reports a repeated settle with the same token as settled without applying it twice', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        const job = 'settle-twice-job';
        const runKey = '2026-11-24';
        const claim = yield* runs.claim({
          job,
          runKey,
          trigger: 'schedule',
          force: false,
          params: {},
        });
        if (claim._tag !== 'Acquired') {
          return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
        }

        const owned: OwnedRun = { job, runKey, token: claim.token, attempt: claim.attempt };
        const failed = {
          _tag: 'Failed',
          errorMessage: 'upstream 503',
          reason: 'error',
          cadence: 'Daily',
          ...SETTLE_BASE,
        } as const;
        const first = yield* runs.settle(owned, failed);
        const repeated = yield* runs.settle(owned, failed);
        const asCompleted = yield* runs.settle(owned, {
          _tag: 'Completed',
          stats: {},
          ...SETTLE_BASE,
        });
        const recent = yield* runs.recent({ job, limit: 1 });
        return { first, repeated, asCompleted, latest: recent[0] };
      }),
    );
    expect(result.first).toBe(true);
    expect(result.repeated).toBe(true);
    expect(result.asCompleted).toBe(false);
    expect(result.latest).toMatchObject({ status: 'failed', failures: 1 });
  });

  it('settles a run whose error message contains a NUL character', async () => {
    const job = defineJob({
      name: 'nul-error-job',
      description: 'Fails with a NUL in its message',
      cadence: { _tag: 'Daily' },
      params: { schema: Schema.Struct({}), defaults: {} },
      stats: Schema.Struct({}),
      run: () =>
        Effect.fail(new UpstreamError({ status: 502, message: 'upstream said: bad\u0000body' })),
    });
    const result = await runDb(
      Effect.gen(function* () {
        const outcome = yield* runJob(job, { trigger: 'manual' });
        const recent = yield* (yield* JobRuns).recent({ job: job.name, limit: 1 });
        return { outcome, latest: recent[0] };
      }).pipe(Effect.provide(SecureRandom.layerWebCrypto)),
    );
    expect(result.outcome).toMatchObject({
      status: 'failed',
      errorMessage: 'upstream said: bad\uFFFDbody',
    });
    expect(result.latest).toMatchObject({
      status: 'failed',
      errorMessage: 'upstream said: bad\uFFFDbody',
    });
  });

  it('keeps an exhausted Daily key exhausted whatever next_attempt_at a settle asks for', async () => {
    const latest = await Effect.runPromise(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        const job = 'exhausted-override-job';
        const runKey = '2026-11-24';
        const inAnHour = DateTime.toDateUtc(DateTime.addDuration(yield* DateTime.now, '1 hour'));
        for (let failure = 0; failure < 2; failure += 1) {
          const claim = yield* runs.claim({
            job,
            runKey,
            trigger: 'manual',
            force: false,
            params: {},
          });
          if (claim._tag !== 'Acquired') {
            return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
          }

          yield* runs.settle(
            { job, runKey, token: claim.token, attempt: claim.attempt },
            {
              _tag: 'Failed',
              errorMessage: 'upstream 503',
              reason: 'error',
              cadence: 'Daily',
              trigger: 'manual',
              annotations: Context.empty(),
              nextAttemptAtOverride: inAnHour,
            },
          );
        }

        const recent = yield* runs.recent({ job, limit: 1 });
        return recent[0];
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            layerStorage({ policy: { retry: { dailyMaxFailures: 2 } } }).pipe(
              Layer.provide(appPool(postgres.connectionUri)),
            ),
            quietLogger,
          ),
        ),
        Effect.scoped,
      ),
    );
    expect(latest).toMatchObject({ status: 'failed', failures: 2, nextAttemptAt: null });
  });

  it('enforces next_attempt_at rules for schedule vs manual across RetryLater and Exhausted', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const runs = yield* JobRuns;

        const onceJob = 'once-retry-rules';
        const onceFirst = yield* runs.claim({
          job: onceJob,
          runKey: 'once',
          trigger: 'schedule',
          force: false,
          params: {},
        });
        if (onceFirst._tag !== 'Acquired') {
          return yield* Effect.die(new Error(`expected Acquired, got ${onceFirst._tag}`));
        }

        yield* runs.settle(
          { job: onceJob, runKey: 'once', token: onceFirst.token, attempt: onceFirst.attempt },
          {
            _tag: 'Failed',
            errorMessage: 'transient import error',
            reason: 'error',
            cadence: 'Once',
            ...SETTLE_BASE,
          },
        );
        const scheduleRetryLater = yield* runs.claim({
          job: onceJob,
          runKey: 'once',
          trigger: 'schedule',
          force: false,
          params: {},
        });
        const manualTakeoverRetryLater = yield* runs.claim({
          job: onceJob,
          runKey: 'once',
          trigger: 'manual',
          force: false,
          params: {},
        });

        const dailyJob = 'daily-exhaust-rules';
        const dailyKey = '2026-11-24';
        for (let count = 0; count < MAX_SCHEDULED_DAILY_FAILURES; count += 1) {
          const claim = yield* runs.claim({
            job: dailyJob,
            runKey: dailyKey,
            trigger: 'schedule',
            force: false,
            params: {},
          });
          if (claim._tag !== 'Acquired') {
            return yield* Effect.die(new Error(`expected Acquired at ${String(count)}`));
          }

          yield* runs.settle(
            { job: dailyJob, runKey: dailyKey, token: claim.token, attempt: claim.attempt },
            {
              _tag: 'Failed',
              errorMessage: `failure ${String(count + 1)}`,
              reason: 'error',
              cadence: 'Daily',
              ...SETTLE_BASE,
            },
          );
        }

        const scheduleExhausted = yield* runs.claim({
          job: dailyJob,
          runKey: dailyKey,
          trigger: 'schedule',
          force: false,
          params: {},
        });
        const manualTakeoverExhausted = yield* runs.claim({
          job: dailyJob,
          runKey: dailyKey,
          trigger: 'manual',
          force: false,
          params: {},
        });
        return {
          scheduleRetryLater,
          manualTakeoverRetryLater,
          scheduleExhausted,
          manualTakeoverExhausted,
        };
      }),
    );
    expect(result.scheduleRetryLater._tag).toBe('RetryLater');
    const retryAt =
      result.scheduleRetryLater._tag === 'RetryLater' ? result.scheduleRetryLater.at : null;
    expect(retryAt?.getTime()).toBeGreaterThan(Date.now());
    expect(result.manualTakeoverRetryLater).toMatchObject({ _tag: 'Acquired', attempt: 2 });
    expect(result.scheduleExhausted).toEqual({ _tag: 'Exhausted' });
    expect(result.manualTakeoverExhausted).toMatchObject({
      _tag: 'Acquired',
      attempt: MAX_SCHEDULED_DAILY_FAILURES + 1,
    });
  });

  it('dies with the SchemaError when a ledger row does not decode as JobRunRecord', async () => {
    const url = postgres.connectionUri;
    // A ledger under its own prefix, with the trigger CHECK dropped so it can hold a bad row.
    await runSql(
      url,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* Layer.build(layerWith({ prefix: 'broken' })).pipe(Effect.scoped);
        yield* sql`ALTER TABLE public.broken_runs DROP CONSTRAINT broken_runs_trigger_check`;
        yield* sql`
          INSERT INTO public.broken_runs (job, run_key, status, claim_token, lease_expires_at, trigger)
          VALUES ('decode-defect', 'bad-trigger', 'completed', gen_random_uuid(), NOW(), 'bogus')
        `;
      }),
    );
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        return yield* Effect.exit(runs.recent({ job: 'decode-defect', limit: 1 }));
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            layerStorage({ prefix: 'broken' }).pipe(Layer.provide(appPool(url))),
            quietLogger,
          ),
        ),
        Effect.scoped,
      ),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    const cause = Exit.isFailure(exit) ? exit.cause : Cause.empty;
    expect(Cause.hasFails(cause)).toBe(false);
    const defect = Cause.findDefect(cause);
    expect(Result.isSuccess(defect) && Schema.isSchemaError(defect.success)).toBe(true);
  });

  it('prunes more rows than one batch in several batches and counts them all', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const runs = yield* JobRuns;
        yield* sql`
          INSERT INTO public.job_runs (job, run_key, status, claim_token, lease_expires_at, trigger, started_at, finished_at)
          SELECT 'prune-batched', 'old-' || n, 'completed', gen_random_uuid(), NOW(), 'schedule',
                 NOW() - INTERVAL '20 days', NOW() - INTERVAL '20 days'
          FROM generate_series(1, 7) AS n
          UNION ALL
          SELECT 'prune-batched', 'recent', 'completed', gen_random_uuid(), NOW(), 'schedule', NOW(), NOW()
        `;
        const deleted = yield* runs.prune(['prune-batched'], '14 days');
        const remaining = yield* runs.recent({ job: 'prune-batched', limit: 10 });
        return { deleted, remaining: remaining.map((row) => row.runKey) };
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            appPool(postgres.connectionUri),
            layerStorage({ policy: { maintenance: { batchSize: 3 } } }).pipe(
              Layer.provide(appPool(postgres.connectionUri)),
            ),
            quietLogger,
          ),
        ),
        Effect.scoped,
      ),
    );
    expect(result.deleted).toBe(7);
    expect(result.remaining).toEqual(['recent']);
  });

  it('preserves a running row in another partition when pruning a job', async () => {
    const result = await runSql(
      postgres.connectionUri,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE TABLE public.parts_runs (LIKE public.job_runs INCLUDING ALL) PARTITION BY LIST (job)`;
        yield* sql`CREATE TABLE public.parts_runs_a PARTITION OF public.parts_runs FOR VALUES IN ('a')`;
        yield* sql`CREATE TABLE public.parts_runs_b PARTITION OF public.parts_runs FOR VALUES IN ('b')`;

        yield* sql`
          INSERT INTO public.parts_runs (job, run_key, status, claim_token, lease_expires_at, trigger, started_at)
          VALUES
            ('a', 'shared', 'completed', gen_random_uuid(), NOW(), 'schedule', NOW() - INTERVAL '3 days'),
            ('b', 'shared', 'running', gen_random_uuid(), NOW() + INTERVAL '1 hour', 'schedule', NOW())
        `;

        const storage = layerWith({
          prefix: 'parts',
          policy: { maintenance: { batchSize: 1 } },
        });
        const deleted = yield* JobRuns.use((runs) => runs.prune(['a'], '1 day')).pipe(
          Effect.provide(storage),
        );
        const remaining = yield* sql`SELECT job, status FROM public.parts_runs`;
        return { deleted, remaining };
      }),
    );
    expect(result).toEqual({ deleted: 1, remaining: [{ job: 'b', status: 'running' }] });
  });

  it('prunes only old rows of the named jobs while keeping recent rows and unnamed jobs', async () => {
    const result = await runDb(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const runs = yield* JobRuns;

        yield* sql`
          INSERT INTO public.job_runs (job, run_key, status, claim_token, lease_expires_at, trigger, started_at, finished_at)
          VALUES
            ('prune-a', 'old-a', 'completed', gen_random_uuid(), NOW(), 'schedule', NOW() - INTERVAL '20 days', NOW() - INTERVAL '20 days'),
            ('prune-a', 'recent-a', 'completed', gen_random_uuid(), NOW(), 'schedule', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
            ('prune-b', 'old-b', 'completed', gen_random_uuid(), NOW(), 'schedule', NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days'),
            ('prune-other', 'old-other', 'completed', gen_random_uuid(), NOW(), 'schedule', NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days')
        `;

        const deleted = yield* runs.prune(['prune-a', 'prune-b'], '14 days');
        const remainingRows = yield* sql`
          SELECT job, run_key AS "runKey"
          FROM public.job_runs
          WHERE job IN ('prune-a', 'prune-b', 'prune-other')
          ORDER BY job ASC, run_key ASC
        `;
        const remaining = yield* Schema.decodeUnknownEffect(Schema.Array(JobKeyRow))(remainingRows);
        return { deleted, remaining };
      }),
    );
    expect(result.deleted).toBe(2);
    expect(result.remaining).toEqual([
      { job: 'prune-a', runKey: 'recent-a' },
      { job: 'prune-other', runKey: 'old-other' },
    ]);
  });
});

/**
 * A row of the application table the settle hooks write to.
 */
const AuditRow = Schema.Struct({ job: Schema.String, outcome: Schema.String });

/**
 * Status of the ledger row the hook saw from inside the settle transaction.
 */
const StatusRow = Schema.Struct({ status: Schema.String });

/**
 * Rows of `audit` for `job`.
 */
const auditRows = (job: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql`SELECT job, outcome FROM public.audit WHERE job = ${job}`;
    return yield* Schema.decodeUnknownEffect(Schema.Array(AuditRow))(rows);
  });

/**
 * Status of `(job, runKey)` in the ledger.
 */
const ledgerStatus = (job: string, runKey: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql`
      SELECT status FROM public.job_runs WHERE job = ${job} AND run_key = ${runKey}
    `;
    const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(StatusRow))(rows);
    return decoded[0]?.status;
  });

/**
 * Claims `(job, runKey)` and settles it as completed through a ledger whose `onSettle` is `hook`,
 * with a logger capturing warnings. Returns the settle result and the captured log messages.
 */
const settleWithHook = (
  job: string,
  hook: (event: JobSettled) => Effect.Effect<void, unknown, SqlClient.SqlClient>,
) => {
  const logger = makeTestLogger({ minLevel: 'Warn' });
  const url = postgres.connectionUri;
  return Effect.runPromise(
    Effect.gen(function* () {
      const runs = yield* JobRuns;
      const claim = yield* runs.claim({
        job,
        runKey: '2026-11-24',
        trigger: 'schedule',
        force: false,
        params: {},
      });
      if (claim._tag !== 'Acquired') {
        return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
      }

      const settled = yield* runs.settle(
        { job, runKey: '2026-11-24', token: claim.token, attempt: claim.attempt },
        { _tag: 'Completed', stats: { n: 1 }, ...SETTLE_BASE },
      );
      return { settled, logs: logger.records.map((record) => record.message) };
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          layerStorage({ onSettle: hook }).pipe(Layer.provide(appPool(url))),
          logger.layer,
        ),
      ),
      Effect.scoped,
    ),
  );
};

/**
 * Hook that records the outcome in `audit` through the caller's `SqlClient`.
 */
const writeAudit = (event: JobSettled) =>
  SqlClient.SqlClient.use(
    (sql) =>
      sql`INSERT INTO public.audit (job, outcome) VALUES (${event.job}, ${event.outcome._tag})`,
  );

describe('the onSettle hook', () => {
  beforeAll(async () => {
    await runSql(
      postgres.connectionUri,
      SqlClient.SqlClient.use((sql) => sql`CREATE TABLE public.audit (job TEXT, outcome TEXT)`),
    );
  });

  it('runs inside the settle transaction and commits with it', async () => {
    const seen: Array<string | undefined> = [];
    const result = await settleWithHook('hook-commits', (event) =>
      Effect.gen(function* () {
        // Only the settle's own transaction can see the row as completed before COMMIT.
        seen.push(yield* ledgerStatus(event.job, event.runKey));
        yield* writeAudit(event);
      }),
    );
    expect(result.settled).toBe(true);
    expect(seen).toEqual(['completed']);
    expect(await runSql(postgres.connectionUri, auditRows('hook-commits'))).toEqual([
      { job: 'hook-commits', outcome: 'Completed' },
    ]);
  });

  it('rolls back a failing hook, logs it and still commits the settle', async () => {
    const result = await settleWithHook('hook-fails', (event) =>
      writeAudit(event).pipe(Effect.andThen(Effect.fail('the hook gave up'))),
    );
    expect(result.settled).toBe(true);
    expect(result.logs).toContain('job.settle_hook_failed');
    expect(await runSql(postgres.connectionUri, auditRows('hook-fails'))).toEqual([]);
    expect(await runSql(postgres.connectionUri, ledgerStatus('hook-fails', '2026-11-24'))).toBe(
      'completed',
    );
  });

  it('keeps the settle when the hook hits a SQL error that aborts its statement', async () => {
    // Without a savepoint the failed statement would abort the transaction and COMMIT would
    // silently roll the settle back while settle returned true.
    const result = await settleWithHook('hook-sql-error', (event) =>
      writeAudit(event).pipe(
        Effect.andThen(
          SqlClient.SqlClient.use((sql) => sql`INSERT INTO public.missing_table VALUES (1)`),
        ),
      ),
    );
    expect(result.settled).toBe(true);
    expect(result.logs).toContain('job.settle_hook_failed');
    expect(await runSql(postgres.connectionUri, auditRows('hook-sql-error'))).toEqual([]);
    expect(await runSql(postgres.connectionUri, ledgerStatus('hook-sql-error', '2026-11-24'))).toBe(
      'completed',
    );
  });

  it('keeps the settle when the hook catches its own SQL error', async () => {
    // The caught error leaves the savepoint aborted, so it cannot be released and is rolled back.
    const result = await settleWithHook('hook-swallows', (event) =>
      writeAudit(event).pipe(
        Effect.andThen(
          SqlClient.SqlClient.use((sql) => sql`INSERT INTO public.missing_table VALUES (1)`),
        ),
        Effect.ignore,
      ),
    );
    expect(result.settled).toBe(true);
    expect(result.logs).toContain('job.settle_hook_failed');
    expect(await runSql(postgres.connectionUri, auditRows('hook-swallows'))).toEqual([]);
    expect(await runSql(postgres.connectionUri, ledgerStatus('hook-swallows', '2026-11-24'))).toBe(
      'completed',
    );
  });

  it('drops the afterCommit callbacks of a failed hook', async () => {
    const committed: Array<string> = [];
    const result = await settleWithHook('hook-after-commit', (event) =>
      SqlClient.SqlClient.use((sql) =>
        sql.afterCommit(Effect.sync(() => committed.push(event.job))),
      ).pipe(Effect.andThen(Effect.fail('the hook gave up'))),
    );
    expect(result.settled).toBe(true);
    expect(result.logs).toContain('job.settle_hook_failed');
    expect(committed).toEqual([]);
  });

  it('keeps the settle when the hook dies', async () => {
    const result = await settleWithHook('hook-dies', () => Effect.die(new Error('hook defect')));
    expect(result.settled).toBe(true);
    expect(result.logs).toContain('job.settle_hook_failed');
    expect(await runSql(postgres.connectionUri, ledgerStatus('hook-dies', '2026-11-24'))).toBe(
      'completed',
    );
  });

  it('is not called for a settle that lost its claim', async () => {
    const calls: Array<string> = [];
    const url = postgres.connectionUri;
    const settled = await Effect.runPromise(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        const claim = yield* runs.claim({
          job: 'hook-lost',
          runKey: 'once',
          trigger: 'schedule',
          force: false,
          params: {},
        });
        if (claim._tag !== 'Acquired') {
          return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
        }

        return yield* runs.settle(
          { job: 'hook-lost', runKey: 'once', token: STALE_TOKEN, attempt: claim.attempt },
          { _tag: 'Completed', stats: {}, ...SETTLE_BASE },
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            layerStorage({
              onSettle: (event) =>
                Effect.sync(() => {
                  calls.push(event.job);
                }),
            }).pipe(Layer.provide(appPool(url))),
            quietLogger,
          ),
        ),
        Effect.scoped,
      ),
    );
    expect(settled).toBe(false);
    expect(calls).toEqual([]);
  });
});
