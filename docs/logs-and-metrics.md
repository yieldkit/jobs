# Logs and metrics

Logs go through `@yieldkit/logger` and share its formatting and redaction. With its `LoggerLive` (add `@yieldkit/logger` to your own dependencies to import it), every run logs a line when it ends:

```ts
import { defineJob, runJob } from '@yieldkit/jobs';
import { makeJobRunsMemory } from '@yieldkit/jobs/testing';
import { LoggerLive } from '@yieldkit/logger';
import { Effect, Schema } from 'effect';

const greet = defineJob({
  name: 'greet',
  description: 'Says hello once',
  cadence: { _tag: 'Once', start: 'manual' },
  params: { schema: Schema.Struct({ name: Schema.String }), defaults: { name: 'world' } },
  stats: Schema.Struct({ greeted: Schema.String }),
  logFields: { tenant: 'acme' },
  run: ({ params }) => Effect.succeed({ greeted: params.name }),
});

const logged = Effect.gen(function* () {
  const ledger = yield* makeJobRunsMemory();
  return yield* runJob(greet, { trigger: 'manual' }).pipe(Effect.provide(ledger.layer));
}).pipe(Effect.provide(LoggerLive({ service: 'worker', format: 'json' })));
```

The `job.completed` line, without `timestamp`, `traceId` and the other standard fields:

```json
{
  "level": "INFO",
  "message": "job.completed",
  "annotations": {
    "tenant": "acme",
    "runKey": "once",
    "job": "greet",
    "jobRunId": "greet:once:1",
    "trigger": "manual",
    "attempt": 1,
    "greeted": "world",
    "status": "completed",
    "durationMs": 0
  },
  "spans": { "job.run": 0 }
}
```

Every line of a run carries `job`, `jobRunId` (`<job>:<runKey>:<attempt>`), `runKey`, `trigger`, `attempt`, the job's `logFields` and, in a workflow, `executionId`. Runs are traced as `job.<name>`, stages as `job.stage.<stage>`.

## Events

| Event                                                                                                                   | Level | When                                                                                                                                                                                                                                           |
| ----------------------------------------------------------------------------------------------------------------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `job.started`                                                                                                           | Debug | A claimed run starts                                                                                                                                                                                                                           |
| `job.completed`                                                                                                         | Info  | It completed; the encoded stats are added as fields, with `durationMs`                                                                                                                                                                         |
| `job.skipped`                                                                                                           | Info  | The claim was refused; `skipReason`, `retryAt`                                                                                                                                                                                                 |
| `job.failed`                                                                                                            | Error | It failed or died; `errorTag`, `errorMessage`, `stage`, `isDefect`                                                                                                                                                                             |
| `job.interrupted`                                                                                                       | Warn  | It was interrupted                                                                                                                                                                                                                             |
| `job.claim.failed`                                                                                                      | Warn  | A claim failed after retries; the round reports `RetryLater` in `busyDelay`                                                                                                                                                                    |
| `job.heartbeat.failed`                                                                                                  | Warn  | A SQL heartbeat failed or timed out; the run ends after a full lease of them. Also logged, with `errorMessage`, when a replayed claim cannot reach the ledger; that round ends without running the body                                        |
| `job.settle.lost`                                                                                                       | Warn  | A settle did not write the row: another run holds the key, or the write still failed after retries (then with `errorMessage`). The outcome is `RunClaimLost`. Also logged when the settle of an interrupted or crash-capped workflow run fails |
| `job.settle_hook_failed`                                                                                                | Warn  | The `onSettle` hook failed; the settle still committed                                                                                                                                                                                         |
| `worker.supervisor.starting`, `.running`, `.stopped`, `.job_filtered`                                                   | Info  | Scheduler lifecycle; `starting` lists each job's cadence and `logFields`                                                                                                                                                                       |
| `worker.scheduler.dispatch_failed`                                                                                      | Warn  | A cron or start-up dispatch failed; start-up dispatches retry each minute, `Every` and `Cron` ticks every 30 s                                                                                                                                 |
| `worker.scheduler.dispatch_abandoned`                                                                                   | Warn  | An `Every` or `Cron` tick whose dispatch kept failing until its catch-up window or next tick                                                                                                                                                   |
| `worker.supervisor.pruned`, `worker.maintenance.cluster_pruned`, `worker.maintenance.requests_pruned`                   | Info  | Rows deleted by a maintenance sweep                                                                                                                                                                                                            |
| `worker.supervisor.prune_failed`, `worker.maintenance.cluster_prune_failed`, `worker.maintenance.requests_prune_failed` | Warn  | A sweep's delete failed; the next sweep tries again                                                                                                                                                                                            |
| `worker.request.finish_failed`                                                                                          | Warn  | A request row could not be marked finished                                                                                                                                                                                                     |
| `worker.request.start_failed`                                                                                           | Warn  | A request row could not be marked started; the run still goes ahead                                                                                                                                                                            |
| `job.dispatch.record_failed`                                                                                            | Warn  | `dispatchApiJobWorkflow` could not mark its request row as failed after the dispatch failed; the dispatch error still reaches the caller                                                                                                       |
| `jobs.migrate.completed`                                                                                                | Info  | `JobStorage.layerMigrations` finished; `applied` is the step count                                                                                                                                                                             |

`withJobLogging`, `withJobContext` and `withJobStage` give your own code the same lines and spans:

<!-- prettier-ignore -->
```ts
import { withJobLogging, withJobStage } from '@yieldkit/jobs';
import { Effect } from 'effect';

export const backfill = Effect.succeed({ rows: 120 }).pipe(
  withJobStage('copy'),                                   // stage annotation, log and trace span
  withJobLogging({ job: 'backfill', trigger: 'manual' }), // job.started, then job.completed or job.failed
);
```

## Metrics

| Metric                 | Type      | Attributes                                                    |
| ---------------------- | --------- | ------------------------------------------------------------- |
| `job_runs_total`       | Counter   | `job`, `status` (`completed`, `skipped`, `failed`), `trigger` |
| `job_duration_ms`      | Histogram | `job`                                                         |
| `job_lease_lost_total` | Counter   | `job`                                                         |

Scope them per service with your exporter's resource attributes.

## Errors

| Error                             | Raised by                                                                                                                                        |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `InvalidJobParams`                | Params that fail the job's schema, or a malformed `parseSetFlags` flag                                                                           |
| `InvalidRunKey`                   | A missing or malformed `OnDemand` run key, or a run key given for a job of another cadence                                                       |
| `InvalidDispatch`                 | `dispatchJob` with a negative, non-finite or unparseable `delay`, or an invalid `runAt`                                                          |
| `DryRunUnsupported`               | `runJob` with `dryRun: true` for a job without a `dryRun` layer                                                                                  |
| `UnknownJobError`                 | The scheduler's `only` naming a job that is not in its list                                                                                      |
| `ClusterConfigError`              | A bad cadence (interval, time, time zone, window or cron expression) or policy value, or a `ShardingConfig` missing or misassigning a jobs group |
| `JobsNotMigratedError`            | Runtime layers started against missing or older tables; `component`, `table`, `found`, `expected`                                                |
| `JobStageError`                   | A failed durable stage, stored in Cluster's replies with a redacted message                                                                      |
| `RunClaimLost`                    | A run whose heartbeat found another owner; it ends as a `failed` outcome                                                                         |
| `InvalidRunCursor`                | `JobRuns.page` received a malformed continuation cursor                                                                                          |
| `MigrationError`, `PgConfigError` | `JobStorage.layerMigrations` and invalid storage options (from `@yieldkit/pg`)                                                                   |

They are tagged errors, so `Effect.catchTag` picks one out:

```ts
import { JobRuns, JobStorage } from '@yieldkit/jobs';
import { makePgLive } from '@yieldkit/pg';
import { Effect, Layer } from 'effect';

export const latest = Effect.gen(function* () {
  const runs = yield* JobRuns;
  return yield* runs.recent({ limit: 10 });
}).pipe(
  Effect.provide(JobStorage.layerStorage().pipe(Layer.provide(makePgLive()))),
  Effect.catchTag('JobsNotMigratedError', (error) =>
    Effect.die(
      `run JobStorage.layerMigrations first: ${error.table} is at ${String(error.found)}, needs ${String(error.expected)}`,
    ),
  ),
);
```
