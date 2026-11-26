# Migrations

The library migrates its own tables only: the run ledger `<prefix>_runs`, the request table `<prefix>_requests` and their history `<prefix>_migrations`. Your Cluster's tables are yours to migrate, with Effect's own loaders. The runtime layers create nothing.

`JobStorage.migrations` and `JobStorage.layerMigrations` mirror `SqlMessageStorage.migrations` and `.layerMigrations`; the runtime layers are in [Storage](storage.md#layers). Options are `{ prefix?, schema? }` ([table options](storage.md#table-options)).

## The deploy step

Run your Cluster's migrations and the library's on an owner connection, once per release, before workers and clients start:

<!-- prettier-ignore -->
```ts
import { JobStorage } from '@yieldkit/jobs';
import { makePgLive } from '@yieldkit/pg';
import { Layer } from 'effect';
import { SqlMessageStorage, SqlRunnerStorage } from 'effect/cluster';

const OwnerPg = makePgLive({ applicationName: 'deploy', searchPath: 'app' });

export const DeployLive = Layer.mergeAll(
  SqlMessageStorage.layerMigrations({ prefix: 'app_cluster' }),   // your Cluster, your prefix
  SqlRunnerStorage.layerMigrations({ prefix: 'app_cluster' }),
  JobStorage.layerMigrations(),                                   // job_runs, job_requests, job_migrations
).pipe(Layer.provide(OwnerPg));
```

`JobStorage.layerMigrations` fails with `MigrationError` (from `@yieldkit/pg`), or `PgConfigError` for invalid options, rather than dying as Effect's does. It logs `jobs.migrate.completed` with the number of applied steps.

## How a run works

1. Check that the target schema exists (`schema`, or the first existing schema of `search_path`) and that the role may create tables in it, so a missing schema or grant reads as such. Schemas are never created.
2. Refuse to share a history: if `<prefix>_migrations` has rows but `<prefix>_runs` does not exist, fail without creating anything. Effect's message storage uses the same history table and step names, so a prefix shared with your Cluster would skip steps. The prefix `cluster` is refused outright.
3. Run Effect's `Migrator` with the library's loader. It creates the history table when missing, locks it (`ACCESS EXCLUSIVE`), and applies the pending steps in one transaction, so concurrent deploys apply each step once. A failed step records nothing, though the empty history table stays.

`JobStorage.layerStorage` checks at start-up that the history is current and that `<prefix>_runs` and `<prefix>_requests` exist, and fails with `JobsNotMigratedError` otherwise.

## Grants

| Role                | Needs                                                                                                                       |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Migration           | `USAGE` and `CREATE` on the schema; ownership of existing tables                                                            |
| Workers and clients | `USAGE` on the schema; `SELECT` on `<prefix>_migrations`; `SELECT, INSERT, UPDATE, DELETE` on the ledger and request tables |

Your Cluster's grants are yours; Effect's storage needs DML on its tables. The package does not create roles or grant anything.

## Adopting existing tables

`layerMigrations` adopts tables your own migrations created. Step 0001 uses `CREATE TABLE IF NOT EXISTS` and `ADD COLUMN IF NOT EXISTS`, so existing tables, rows, extra columns and indexes stay, and the step is recorded.

The ledger may have a `<prefix>_runs_trigger_check` constraint, PostgreSQL's name for an inline `CHECK` on `trigger`. If it does not allow `'schedule'`, `'manual'` and `'api'`, the migration fails with a message pointing here and records nothing. Otherwise the first manual run would hit a constraint violation. Rewrite the constraint in your own migration first, for example for a table that used `'cli'`:

```sql
ALTER TABLE job_runs DROP CONSTRAINT IF EXISTS job_runs_trigger_check;
UPDATE job_runs SET trigger = 'manual' WHERE trigger = 'cli';
ALTER TABLE job_runs ADD CONSTRAINT job_runs_trigger_check
  CHECK (trigger IN ('schedule', 'manual', 'api'));
```

Code that still writes the old value fails afterwards, so ship this migration with the code that adopts the library.

## Limits

- Lock and statement timeouts are your pool's: configure `lockTimeout` and `statementTimeout` on the deploy pool if you want the DDL to give up rather than wait.
- To adopt existing tables, the migration role must own them or be a member of the owning role, since step 0001 alters them.
- From PostgreSQL 15, only the database owner may create tables in `public` by default. Use a schema the migration role owns, or grant it `CREATE`.
- A future step that rewrites a large table should be its own deploy step.
