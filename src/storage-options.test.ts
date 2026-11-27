import { Effect } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import { resolveStorage } from './storage-options.ts';

describe('resolveStorage', () => {
  it('defaults to unqualified job_* tables that resolve through search_path', async () => {
    const storage = await Effect.runPromise(resolveStorage(undefined));
    expect(storage).toEqual({
      schema: undefined,
      prefix: 'job',
      runsTable: 'job_runs',
      requestsTable: 'job_requests',
      migrationsTable: 'job_migrations',
      migrationsTableName: 'job_migrations',
      runsRecentIndex: 'idx_job_runs_recent',
      runsHistoryIndex: 'idx_job_runs_history',
      runsStatusCheck: 'job_runs_status_check',
      runsTriggerCheck: 'job_runs_trigger_check',
    });
  });

  it('qualifies every table with an explicit schema', async () => {
    const storage = await Effect.runPromise(resolveStorage({ schema: 'billing', prefix: 'tasks' }));
    expect(storage.runsTable).toBe('billing.tasks_runs');
    expect(storage.requestsTable).toBe('billing.tasks_requests');
    expect(storage.migrationsTable).toBe('billing.tasks_migrations');
    expect(storage.migrationsTableName).toBe('tasks_migrations');
  });

  it('rejects names that need quoting, contain dots or are too long', async () => {
    const invalid = [
      { schema: 'Public' },
      { schema: 'app.jobs' },
      { prefix: 'job"; DROP TABLE x; --' },
      { prefix: '1job' },
      { prefix: 'a'.repeat(38) },
    ];
    const errors = await Effect.runPromise(
      Effect.forEach(invalid, (options) => Effect.flip(resolveStorage(options))),
    );
    expect(errors.map((error) => error._tag)).toEqual(Array(invalid.length).fill('PgConfigError'));
    expect(errors[2]?.message).toContain('prefix');
  });

  it('accepts the longest allowed prefix', async () => {
    const storage = await Effect.runPromise(resolveStorage({ prefix: 'a'.repeat(37) }));
    expect(storage.runsTriggerCheck.length).toBeLessThanOrEqual(63);
    expect(storage.runsHistoryIndex.length).toBeLessThanOrEqual(63);
  });

  it("refuses Effect Cluster's default prefix, whose history Cluster storage also uses", async () => {
    const error = await Effect.runPromise(Effect.flip(resolveStorage({ prefix: 'cluster' })));
    expect(error._tag).toBe('PgConfigError');
    expect(error.message).toContain('cluster_migrations');
  });
});
