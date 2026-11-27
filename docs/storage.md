# Storage and the settle hook

The run ledger (`JobRuns`) and the request store (`JobRequests`) run on your application's `SqlClient`, so a settle hook can write your rows in the settle transaction. It can be the same pool your Cluster's storage uses.

```ts
import { JobStorage } from '@yieldkit/jobs';
import { makePgLive } from '@yieldkit/pg';
import { Layer } from 'effect';

export const StorageLive = JobStorage.layerStorage().pipe(
  Layer.provide(makePgLive({ applicationName: 'worker' })),
);
```

## Layers

| Constructor                                                                  | Does                                                                                         |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `JobStorage.layerStorage(options?)`                                          | Provides `JobRuns` and `JobRequests` without DDL; checks the migration history at start-up   |
| `JobStorage.layerWith(options?)`                                             | `layerMigrations` then `layerStorage` on the same client, for tests and single-process tools |
| `JobStorage.layer`                                                           | `layerWith()` with default options                                                           |
| `JobStorage.layerMigrations(options?)` and `JobStorage.migrations(options?)` | See [Migrations](migrations.md)                                                              |

`layerStorage` fails with:

- `JobsNotMigratedError`: `<prefix>_migrations` is missing or behind the code, or the ledger or request table is missing;
- `ClusterConfigError`: an invalid `policy`;
- `PgConfigError`: invalid table options or a non-PostgreSQL client.

A newer history is accepted, so older workers keep running during a rolling deploy.

## Options

<!-- prettier-ignore -->
```ts
import { JobStorage } from '@yieldkit/jobs';
import { Effect } from 'effect';

const StorageLive = JobStorage.layerStorage({
  prefix: 'job',                          // job_runs, job_requests, job_migrations
  schema: undefined,                      // default: the client's search_path, like Cluster's tables
  onSettle: () => Effect.void,            // see below
  policy: { lease: { duration: '5 minutes' } }, // see Policy
});
```

### Table options

`prefix` and `schema` mirror the options of Effect Cluster's SQL storage. Pass the same values to `layerMigrations` and `layerStorage`.

- Without `schema`, table names are unqualified and resolve through the client's `search_path`, exactly like Cluster's tables, so one pool with one `search_path` serves both.
- With `schema`, every statement names it, whatever the `search_path`.
- If your Cluster's tables share the schema, give them a different prefix: both keep their history in `<prefix>_migrations`. The library refuses `cluster`, Effect's default, and [checks for a shared history](migrations.md#how-a-run-works).

Statements ignore the client's name transforms.

## The settle hook

`onSettle` runs once for every settle that still held its claim, inside the settle transaction:

<!-- prettier-ignore -->
```ts
import { JobStorage, type JobSettled } from '@yieldkit/jobs';
import { Context, Effect, Option } from 'effect';
import { SqlClient } from 'effect/sql';

class Tenant extends Context.Service<Tenant, string>()('app/Tenant') {}

const recordHealth = (event: JobSettled) =>
  Effect.gen(function* () {
    const tenant = Context.getOption(event.annotations, Tenant);
    if (Option.isNone(tenant) || event.trigger === 'manual') {
      return;
    }

    const sql = yield* SqlClient.SqlClient;
    const healthy = event.outcome._tag === 'Completed';
    yield* sql`UPDATE tenant SET healthy = ${healthy} WHERE id = ${tenant.value}`;
  });

export const StorageLive = JobStorage.layerStorage({ onSettle: recordHealth });
```

`JobSettled` carries `job`, `runKey`, `attempt`, `trigger`, the job's `annotations` and an `outcome`: `{ _tag: 'Completed', stats }` or `{ _tag: 'Failed', errorMessage, reason, failures, nextAttemptAt }`, where `reason` is `error`, `interrupted` (also a lost lease) or `crash-cap`.

- Writes through the layer's own `SqlClient` commit or roll back with the settle.
- The hook runs in a savepoint: if it fails, its writes roll back, `job.settle_hook_failed` is logged and the settle still commits.
- A settle that lost its claim skips the hook; a repeated settle does not call it twice.
- The hook's services become requirements of the layer.

## JobRuns

```ts
import { JobRuns } from '@yieldkit/jobs';
import { Effect } from 'effect';

export const runHistory = (cursor?: string) =>
  Effect.gen(function* () {
    const runs = yield* JobRuns;
    const job = 'sync-orders';
    const run = yield* runs.get({ job, runKey: '2026-11-25T10:00Z' });
    const page = yield* runs.page({ job, limit: 20, cursor });
    return { run, page };
  });
```

| Member                           | Does                                                                                                                                          |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `claim(request)`                 | Takes `(job, runKey)` and returns `Acquired` with a fence token, or why not: `Busy`, `Completed`, `AlreadySettled`, `RetryLater`, `Exhausted` |
| `heartbeat(run)`                 | Extends the lease; `false` when another attempt owns the key                                                                                  |
| `settle(run, result)`            | Fenced update plus the hook; `false` when the claim was lost                                                                                  |
| `isCompleted(job, runKey)`       | Whether the key is completed                                                                                                                  |
| `get({ job, runKey })`           | `Option<JobRunRecord>` for the key                                                                                                            |
| `recent({ job?, limit })`        | Newest rows first, as `JobRunRecord`                                                                                                          |
| `page({ job?, limit, cursor? })` | Newest rows first with an opaque continuation cursor                                                                                          |
| `prune(jobs, olderThan)`         | Deletes rows of `jobs` that started before the cutoff, `maintenance.batchSize` rows per statement                                             |
| `leaseDuration`, `policy`        | The lease and the resolved [policy](policy.md)                                                                                                |

`page` and `recent` order rows by start time, then run key, then job, all descending, comparing text bytewise (`COLLATE "C"` on a UTF-8 database) for ties. Pass the returned `nextCursor` with the same job filter to continue. An empty page or the last page returns `nextCursor: null`. A malformed cursor fails with `InvalidRunCursor`. `page` reads live ledger state, so a run reclaimed while walking can move ahead of an existing cursor; run the walk inside a single `REPEATABLE READ` transaction when you need a point-in-time snapshot. An undecodable ledger row is a defect in all three read methods.

A claim with the same workflow `executionId` resumes its own `running` row after a crash, with a fresh token. Heartbeats and settles match the token, so a stale process never overwrites a newer owner.

## JobRequests

Records the requests `dispatchApiJobWorkflow` starts: `create`, `upsertStarted`, `markFinished`, `markDispatchFailed`, `getStatus` and `prune(olderThan, limit)`, which deletes finished rows only.

`getStatus(job, requestId)` returns the request row with its ledger row, or `undefined` for an unknown or malformed id. Its `run` is the ledger row of the request's own execution, or `null`:

- while the request is pending;
- when another execution holds the key: the request was skipped as `Busy` or `AlreadyCompleted`, or a later run took the key over.

## Tables

| Table                        | Columns                                                                                                                                                                                                                                                                          |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `<schema>.<prefix>_runs`     | Primary key `(job, run_key)`; `status` (`running`, `completed`, `failed`), `claim_token`, `lease_expires_at`, `attempt`, `failures`, `next_attempt_at`, `trigger` (`schedule`, `manual`, `api`), `params`, `stats`, `error_message`, `started_at`, `finished_at`, `execution_id` |
| `<schema>.<prefix>_requests` | `id` (UUID), `job`, `params`, `force`, `requested_at`, `picked_at`, `finished_at`, `run_key`, `outcome`, `execution_id`                                                                                                                                                          |

Query them directly if you like; write only through the services.

## Limits

- A workflow tries each ledger and request call six times (6.2 s of backoff), then a claim reports `RetryLater` and a settle `RunClaimLost`. With a database down, one call can hold a worker's shutdown for about 36 s; give the jobs pool a `statementTimeout` so a hung server is bounded too.
- The ledger row, not the request row, is the source of truth when the two disagree.
- Names must be lowercase identifiers (`^[a-z_][a-z0-9_]*$`). `prefix` is at most 37 characters, so derived index names fit PostgreSQL's 63-byte limit.
- Without `schema`, the tables follow the `search_path`: point the deploy pool and the runtime pools at the same one.
