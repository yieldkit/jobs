# Policy

Every timing and limit lives in one `JobsPolicy`, given to the storage layer. Workflows, the scheduler and `runJob` read it from `JobRuns.policy`, so the ledger's retry arithmetic and the durable sleeps agree.

```ts
import { JobStorage } from '@yieldkit/jobs';

const StorageLive = JobStorage.layerStorage({
  policy: { retry: { maxCrashAttempts: 5 }, retention: { requests: '7 days' } },
});
```

Overrides merge group by group over `defaultJobsPolicy`. Durations take any `Duration.Input` above zero and at most 3650 days; counts are integers of at least 1. Anything else fails the layer with `ClusterConfigError` naming the field. `makeJobRunsMemory({ policy })` takes the same overrides.

| Field                       | Default   | Meaning                                                                                                                            |
| --------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `lease.duration`            | 2 minutes | Lease written on claim and heartbeat; an expired lease may be taken over                                                           |
| `lease.heartbeatsPerLease`  | 4         | Heartbeat every `duration / heartbeatsPerLease` (30 seconds); at least 2                                                           |
| `retry.dailyMaxFailures`    | 6         | Failures of one `Daily` key after which scheduled runs stop (`Exhausted`); not used when the cadence has `at`, `window` or `until` |
| `retry.onceBase`            | 1 hour    | First backoff of an automatic `Once` job, doubled per failure                                                                      |
| `retry.onceCap`             | 24 hours  | Longest `Once` backoff                                                                                                             |
| `retry.onDemandBase`        | 1 minute  | First backoff of a dispatched `OnDemand` run, doubled per failure                                                                  |
| `retry.onDemandCap`         | 1 hour    | Longest `OnDemand` backoff                                                                                                         |
| `retry.onDemandMaxFailures` | 5         | Failures of one `OnDemand` key after which its workflow gives up (`Exhausted`)                                                     |
| `retry.busyDelay`           | 1 minute  | Durable wait of a scheduled `Daily`, `Once` or `OnDemand` workflow that found its key `Busy`                                       |
| `retry.busyMaxRetries`      | 15        | `Busy` waits of a `Daily` or `OnDemand` workflow before it settles as skipped (`Once` waits forever)                               |
| `retry.maxCrashAttempts`    | 3         | Claims of the key by one execution, the first included, before it fails with `MaxAttemptsExceeded`                                 |
| `retention.everyRuns`       | 14 days   | Ledger rows of `Every` and `Cron` jobs; `Daily` and `Once` rows are kept                                                           |
| `retention.everyCluster`    | 14 days   | Processed Cluster messages of `Every` and `Cron` workflows, their crons and durable clocks (`retention.onDemand` if shorter)       |
| `retention.dailyCluster`    | 30 days   | Processed Cluster messages of `Daily` workflows and crons; `Once` workflows are kept                                               |
| `retention.onDemand`        | 30 days   | Ledger rows and processed Cluster messages of `OnDemand` jobs; a key deduplicates this long                                        |
| `retention.requests`        | 30 days   | Finished request rows; unfinished rows are kept                                                                                    |
| `maintenance.interval`      | 1 day     | Time between maintenance sweeps; the first runs when the maintenance singleton starts                                              |
| `maintenance.batchSize`     | 1000      | Rows per delete statement when pruning ledger rows, Cluster messages and requests                                                  |

## Retry rules

A failed run sets the ledger's `failures` (interruptions and lost leases don't count) and `next_attempt_at`:

- Interrupted or lost lease: now, without using up the budget. An exhausted `Daily` or `OnDemand` key stays exhausted.
- `Daily`: now while `failures < dailyMaxFailures`, then never (`Exhausted`) until a `manual` or `api` run takes the key. A scheduled workflow sleeps `checkEvery` between attempts.
- `Daily` with `at`, `window` or `until`: retried every `checkEvery` while the retry would start on the run's date at or before `until`, then exhausted. `dailyMaxFailures` does not apply.
- `Every` and `Cron`: now; the next bucket or tick has a new key anyway. Scheduled runs are not retried.
- `Once`: `min(onceBase * 2^(failures - 1), onceCap)` from now. It never exhausts.
- `OnDemand`: `min(onDemandBase * 2^(failures - 1), onDemandCap)` from now while `failures < onDemandMaxFailures`, then never (`Exhausted`). `failures` is the key's count, `manual` failures included. The dispatched workflow sleeps until that instant between attempts.

`manual` and `api` runs ignore `next_attempt_at`. `nextAttemptAt`, `onceRetryDelay` and `heartbeatIntervalFor` expose the arithmetic:

<!-- prettier-ignore -->
```ts
import { defaultJobsPolicy, heartbeatIntervalFor, nextAttemptAt, onceRetryDelay } from '@yieldkit/jobs';

const now = new Date('2026-11-25T00:00:00Z');
const { retry, lease } = defaultJobsPolicy;

nextAttemptAt(retry, { cadence: 'Once', failures: 3, interrupted: false, now });  // 2026-11-25T04:00:00.000Z
nextAttemptAt(retry, { cadence: 'Daily', failures: 6, interrupted: false, now }); // null: exhausted
nextAttemptAt(retry, { cadence: 'OnDemand', failures: 2, interrupted: false, now }); // 2026-11-25T00:02:00.000Z
onceRetryDelay(retry, 10);                                                        // 1 day (onceCap)
heartbeatIntervalFor(lease);                                                      // 30 seconds
```

## Maintenance

One maintenance singleton applies the `retention.*` and `maintenance.*` rows above, and [logs](logs-and-metrics.md) what each sweep deleted. It runs in the library's own shard group, `yieldkit-jobs`, so only workers that run `makeSchedulerLayer` claim it.

Durable clocks are not per job: `ClusterWorkflowEngine` keeps every workflow's clock wake-ups under one entity type, `Workflow/-/DurableClock`. On a Cluster that also runs your own workflows, the sweep deletes their processed clock messages after the shorter of `retention.everyCluster` and `retention.onDemand` too. Keep that retention longer than any workflow of yours that sleeps.

## Limits

- The retry rules apply whatever the trigger: a failed `manual` or `api` run of a `Daily` key uses one failure of the budget and leaves the key to its scheduled workflow.
- The ledger decides exhaustion. Once `manual` or `api` failures use up a key's budget, its scheduled workflow ends `skipped` (`Exhausted`) at its next claim instead of running the body.
- A settle never moves an exhausted key's `next_attempt_at` off `null`.
- Pruning deletes by age, whatever the status. Keep `retention.everyRuns` longer than your longest `Every` run.
- Keep `retention.everyCluster` and `retention.dailyCluster` longer than your longest workflow, retries included, or it loses its stored stage results and runs those stages again.
