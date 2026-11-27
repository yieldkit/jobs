import { Context, Effect, Exit, Layer } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import { JobsNotMigratedError } from './errors.ts';
import { JobRuns } from './ledger.ts';
import { JobRequests } from './requests.ts';
import { dataStatements, makeRecordingSql } from './sql.fixture.ts';
import { layerMigrations, layerStorage } from './storage.ts';

/**
 * Storage options used throughout: a custom schema and prefix, so every statement must name them.
 */
const CUSTOM = { schema: 'app_jobs', prefix: 'task' } as const;

/**
 * Fixed UUIDs: claim tokens and request ids only need to be well-formed here.
 */
const TOKEN = '6f1c2a54-2d3b-4c1e-9a7f-0b8e5d4c3a21';
const REQUEST_ID = '3d7e9b10-5a42-4f6c-8e21-7c9d0a1b2e43';

/**
 * Answers the readiness check as migrated (history present at id 2) and every other statement with
 * `rows`.
 */
const migratedResponder =
  (rows: ReadonlyArray<object> = []) =>
  (sql: string): ReadonlyArray<object> => {
    if (sql.includes('to_regclass')) {
      return [{ present: true }];
    }

    if (sql.includes('MAX(migration_id)')) {
      return [{ found: 2 }];
    }

    return rows;
  };

/**
 * Answers the migration preflight as a schema that exists and accepts tables, and every other
 * statement with no rows.
 */
const freshDatabase = (sql: string): ReadonlyArray<object> =>
  sql.includes('has_schema_privilege')
    ? [{ schema: 'app_jobs', present: true, canCreate: true }]
    : [];

/**
 * Answers as a database where every claim finds a running row whose lease expired, so it takes the
 * row over, a failed settle matches its row, and the request exists with a run key, so `getStatus`
 * reads the ledger too. Everything else is answered as by `migratedResponder`.
 */
const takeoverResponder = (sql: string): ReadonlyArray<object> => {
  const now = new Date();
  if (sql.includes('FOR UPDATE')) {
    return [
      {
        status: 'running',
        executionId: null,
        attempt: 1,
        leaseExpired: true,
        nextAttemptAt: null,
        now,
      },
    ];
  }

  if (sql.trim().startsWith('UPDATE') && sql.includes('RETURNING attempt, failures')) {
    return [{ attempt: 2, failures: 0 }];
  }

  if (sql.includes('RETURNING failures, NOW() AS now')) {
    return [{ failures: 1, now }];
  }

  if (sql.includes('requested_at AS "requestedAt"')) {
    return [
      {
        id: REQUEST_ID,
        job: 'sync',
        params: {},
        force: false,
        executionId: 'e',
        requestedAt: now,
        pickedAt: now,
        finishedAt: null,
        runKey: '2026-11-24',
        outcome: null,
      },
    ];
  }

  return migratedResponder()(sql);
};

describe('JobStorage SQL', () => {
  it('creates every table, index and constraint under the configured schema and prefix', async () => {
    const recording = makeRecordingSql(freshDatabase);
    await Effect.runPromise(
      Layer.build(layerMigrations(CUSTOM).pipe(Layer.provide(recording.layer))).pipe(Effect.scoped),
    );
    const sql = dataStatements(recording.statements).join('\n');
    expect(sql).toContain('CREATE TABLE "app_jobs"."task_migrations"');
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS "app_jobs"."task_runs"');
    expect(sql).toContain('CONSTRAINT "task_runs_status_check"');
    expect(sql).toContain('CONSTRAINT "task_runs_trigger_check"');
    expect(sql).toContain("CHECK (trigger IN ('schedule', 'manual', 'api'))");
    expect(sql).toContain('CREATE INDEX IF NOT EXISTS "idx_task_runs_recent"');
    expect(sql).toContain('ON "app_jobs"."task_runs" (job, started_at DESC)');
    expect(sql).toContain('CREATE INDEX IF NOT EXISTS "idx_task_runs_history"');
    expect(sql).toContain(
      'ON "app_jobs"."task_runs" (started_at DESC, run_key COLLATE "C" DESC, job COLLATE "C" DESC)',
    );
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS "app_jobs"."task_requests"');
    expect(sql).toContain('ALTER TABLE "app_jobs"."task_requests" ADD COLUMN IF NOT EXISTS');
    expect(sql).not.toMatch(/job_runs|job_requests|public/);
  });

  it('leaves table names unqualified without a schema, as Cluster storage does', async () => {
    const recording = makeRecordingSql(freshDatabase);
    await Effect.runPromise(
      Layer.build(layerMigrations().pipe(Layer.provide(recording.layer))).pipe(Effect.scoped),
    );
    const sql = dataStatements(recording.statements).join('\n');
    expect(sql).toContain('CREATE TABLE "job_migrations"');
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS "job_runs"');
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS "job_requests"');
    expect(sql).toContain('current_schema()');
    expect(sql).not.toMatch(/"[a-z_]+"\."job_/);
  });

  it('refuses to migrate when the history has steps but the ledger is missing', async () => {
    const recording = makeRecordingSql((sql, params) => {
      if (sql.includes('has_schema_privilege')) {
        return [{ schema: 'public', present: true, canCreate: true }];
      }

      if (sql.includes('to_regclass')) {
        // The history exists (Cluster's, same prefix); the ledger does not.
        return [{ present: params[0] === 'shared_migrations' }];
      }

      return sql.includes('MAX(migration_id)') ? [{ found: 3 }] : [];
    });
    const error = await Effect.runPromise(
      Effect.flip(
        Layer.build(
          layerMigrations({ prefix: 'shared' }).pipe(Layer.provide(recording.layer)),
        ).pipe(Effect.scoped),
      ),
    );
    expect(error.message).toContain('shared_migrations has steps recorded but shared_runs');
    expect(dataStatements(recording.statements).join('\n')).not.toContain('CREATE TABLE');
  });

  it('names the configured tables in every runtime statement', async () => {
    const recording = makeRecordingSql(takeoverResponder);
    const owned = { job: 'sync', runKey: '2026-11-24', token: TOKEN, attempt: 1 };
    const claim = {
      job: 'sync',
      runKey: '2026-11-24',
      trigger: 'manual',
      force: false,
      params: {},
    } as const;
    const program = Effect.gen(function* () {
      const runs = yield* JobRuns;
      const requests = yield* JobRequests;
      const takenOver = yield* runs.claim(claim);
      const pastDeadline = yield* runs.claim({ ...claim, pastDeadline: true });
      yield* runs.heartbeat(owned);
      yield* runs.settle(owned, {
        _tag: 'Completed',
        stats: {},
        trigger: 'manual',
        annotations: Context.empty(),
      });
      yield* runs.settle(owned, {
        _tag: 'Failed',
        errorMessage: 'boom',
        reason: 'error',
        cadence: 'Daily',
        trigger: 'manual',
        annotations: Context.empty(),
      });
      yield* runs.isCompleted('sync', '2026-11-24');
      yield* runs.get({ job: 'sync', runKey: '2026-11-24' });
      yield* runs.recent({ job: 'sync', limit: 5 });
      yield* runs.page({ job: 'sync', limit: 5 });
      yield* runs.prune(['sync'], '1 day');
      const id = REQUEST_ID;
      yield* requests.create({ id, job: 'sync', params: {}, force: false, executionId: 'e' });
      yield* requests.upsertStarted({
        id,
        job: 'sync',
        params: {},
        force: false,
        executionId: 'e',
        runKey: '2026-11-24',
      });
      yield* requests.markFinished({ id, runKey: '2026-11-24', outcome: {} });
      yield* requests.markDispatchFailed({ id, job: 'sync', errorMessage: 'down' });
      const status = yield* requests.getStatus('sync', id);
      yield* requests.prune('30 days', 100);
      return { takenOver, pastDeadline, status };
    });
    const outcome = await Effect.runPromise(
      program.pipe(Effect.provide(layerStorage(CUSTOM).pipe(Layer.provide(recording.layer)))),
    );
    expect(outcome.takenOver).toMatchObject({ _tag: 'Acquired', attempt: 2 });
    expect(outcome.pastDeadline).toEqual({ _tag: 'Exhausted' });
    expect(outcome.status?.request.runKey).toBe('2026-11-24');

    const statements = dataStatements(recording.statements).filter(
      (sql) => !sql.includes('to_regclass') && !sql.includes('migration_id'),
    );
    expect(statements.length).toBeGreaterThanOrEqual(21);
    for (const sql of statements) {
      expect(sql).toMatch(/"app_jobs"\."task_(runs|requests)"/);
      expect(sql).not.toMatch(/\bjob_runs\b|\bjob_requests\b/);
    }
  });

  it('prunes ledger rows in batches of maintenance.batchSize until a batch comes up short', async () => {
    const batches = [{ deleted: 2 }, { deleted: 2 }, { deleted: 1 }];
    const recording = makeRecordingSql((sql) =>
      sql.trim().startsWith('WITH')
        ? [batches.shift() ?? { deleted: 0 }]
        : migratedResponder()(sql),
    );
    const deleted = await Effect.runPromise(
      JobRuns.use((runs) => runs.prune(['sync'], '1 day')).pipe(
        Effect.provide(
          layerStorage({ ...CUSTOM, policy: { maintenance: { batchSize: 2 } } }).pipe(
            Layer.provide(recording.layer),
          ),
        ),
      ),
    );
    const deletes = dataStatements(recording.statements).filter((sql) => sql.includes('DELETE'));
    expect(deleted).toBe(5);
    expect(deletes).toHaveLength(3);
    expect(deletes[0]).toContain('LIMIT');
    expect(deletes[0]).not.toMatch(/RETURNING 1\s*$/);
  });

  it('fails with JobsNotMigratedError when the history table is missing', async () => {
    const recording = makeRecordingSql();
    const error = await Effect.runPromise(
      Effect.flip(
        Layer.build(layerStorage(CUSTOM).pipe(Layer.provide(recording.layer))).pipe(Effect.scoped),
      ),
    );
    expect(error).toBeInstanceOf(JobsNotMigratedError);
    expect(error).toMatchObject({
      component: 'jobs',
      table: 'app_jobs.task_migrations',
      found: 0,
      expected: 2,
      message:
        'app_jobs.task_migrations is not migrated (found 0, needs 2). ' +
        'Run JobStorage.layerMigrations first.',
    });
  });

  it('accepts a history that is newer than the installed code', async () => {
    const recording = makeRecordingSql((sql) => {
      if (sql.includes('to_regclass')) {
        return [{ present: true }];
      }

      return sql.includes('MAX(migration_id)') ? [{ found: 7 }] : [];
    });
    const exit = await Effect.runPromiseExit(
      Layer.build(layerStorage(CUSTOM).pipe(Layer.provide(recording.layer))).pipe(Effect.scoped),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
  });

  it('runs the settle hook in a savepoint and keeps the settle when the hook fails', async () => {
    const recording = makeRecordingSql(migratedResponder([{ failures: 1, now: new Date() }]));
    const owned = { job: 'sync', runKey: '2026-11-24', token: TOKEN, attempt: 1 };
    const settled = await Effect.runPromise(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        return yield* runs.settle(owned, {
          _tag: 'Failed',
          errorMessage: 'boom',
          reason: 'error',
          cadence: 'Daily',
          trigger: 'schedule',
          annotations: Context.empty(),
        });
      }).pipe(
        Effect.provide(
          layerStorage({ ...CUSTOM, onSettle: () => Effect.fail('hook failed') }).pipe(
            Layer.provide(recording.layer),
          ),
        ),
      ),
    );
    expect(settled).toBe(true);
    const control = recording.statements
      .map((statement) => statement.sql.trim().split(/\s+/).slice(0, 3).join(' '))
      .filter((sql) => /^(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)/i.test(sql));
    expect(control[0]).toBe('BEGIN');
    expect(control[1]).toMatch(/^SAVEPOINT/);
    expect(control[2]).toMatch(/^ROLLBACK TO SAVEPOINT/);
    expect(control.at(-1)).toBe('COMMIT');
  });

  it('fails the layer with ClusterConfigError for an invalid policy', async () => {
    const recording = makeRecordingSql(migratedResponder());
    const exit = await Effect.runPromiseExit(
      Layer.build(
        layerStorage({ policy: { retry: { maxCrashAttempts: 0 } } }).pipe(
          Layer.provide(recording.layer),
        ),
      ).pipe(Effect.scoped),
    );
    expect(String(exit)).toContain('ClusterConfigError');
  });
});
