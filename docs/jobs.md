# Jobs

A job is a value built with `defineJob`: a name, a cadence, a params schema, a stats schema and the effect that does the work.

<!-- prettier-ignore -->
```ts
import { defineJob } from '@yieldkit/jobs';
import { Effect, Schema } from 'effect';

export const syncOrders = defineJob({
  name: 'sync-orders',                                // ledger key, workflow name and shard group
  description: 'Copies new orders from the shop',
  cadence: { _tag: 'Every', interval: '5 minutes' },
  params: {
    schema: Schema.Struct({ limit: Schema.Int, dryRunEmail: Schema.Boolean }),
    defaults: { limit: 100, dryRunEmail: false },     // encoded values, merged under overrides
    apiParams: ['limit'],                             // keys an API caller may set; default none
  },
  stats: Schema.Struct({ copied: Schema.Int }),       // stored in the ledger, logged on completion
  run: ({ params, runKey, trigger, attempt }) => Effect.succeed({ copied: params.limit }),
});
```

Use `defineJob`, not a plain object. Its hidden continuation lets the scheduler, `runAnyJob` and `makeAnyJobWorkflowLayer` run mixed job lists without casts.

## Cadences and run keys

The run key makes a run idempotent: one ledger row and one scheduled workflow execution per `(job, runKey)`. `runKeyFor(cadence, nowMs)` computes it for every cadence except `OnDemand`, whose caller chooses it.

| Cadence                                        | Run key                                 | Scheduled                                                                                            |
| ---------------------------------------------- | --------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `{ _tag: 'Every', interval }`                  | Bucket start, `YYYY-MM-DDTHH:mmZ` (UTC) | A UTC cron every `interval`; it must evenly divide an hour or a day                                  |
| `{ _tag: 'Daily', checkEvery? }`               | `YYYY-MM-DD` (UTC)                      | A UTC cron every `checkEvery` (default 1 hour), so a missed midnight catches up                      |
| `{ _tag: 'Daily', at, until?, timeZone? }`     | `YYYY-MM-DD` in `timeZone`              | The first check at or after `at`; retried every `checkEvery` until `until` (default `24:00`)         |
| `{ _tag: 'Daily', window, until?, timeZone? }` | `YYYY-MM-DD` in `timeZone`              | A time in `[window.from, window.to)` that changes daily; retried until `until` (default `window.to`) |
| `{ _tag: 'Cron', expression, timeZone? }`      | The tick, `YYYY-MM-DDTHH:mmZ` (UTC)     | Each tick of a five-field cron expression; a missed tick runs up to `catchUpWithin` (1 hour) late    |
| `{ _tag: 'Once', start: 'automatic' }`         | `once`                                  | At worker start-up until it completes; retried with backoff                                          |
| `{ _tag: 'Once', start: 'manual' }`            | `once`                                  | Never; only `runJob` starts it, and `isApiRequestable` returns `false`                               |
| `{ _tag: 'OnDemand' }`                         | The caller's key, such as `welcome:42`  | Never; `dispatchJob` starts each run, optionally delayed, and retries it with backoff                |

For `Daily`, `at`, `window` and `until` are local `HH:MM` times in `timeZone` (IANA, default `UTC`), and `checkEvery` is also the retry wait. [Scheduling recipes](scheduling.md) covers retries, catch-up and daylight saving time for each cadence.

`makeSchedulerLayer`, `runJob` and `dispatchApiJobWorkflow` validate cadences and fail with `ClusterConfigError` for a bad interval, time, time zone, window or cron expression, or `at` together with `window`.

## Triggers

Every run has a trigger, stored in the ledger and the workflow payload:

- `schedule`: the scheduler, or `dispatchJob` for an `OnDemand` job. It retries a failed key once `next_attempt_at` has passed and leaves an exhausted `Daily` or `OnDemand` key alone.
- `manual`: an operator, with trusted params. It may take a failed or exhausted key at any time.
- `api`: `dispatchApiJobWorkflow`. Params outside `apiParams` are dropped, and dropped again inside the workflow, so a forged payload cannot set them ([what the filter does not cover](workers-and-clients.md#limits)).

`force` re-runs a completed key. It defaults to `true` for `manual` and `api` runs of `Every` and `Cron` jobs, else `false`.

## Params

Overrides are merged over `params.defaults`, decoded with `params.schema` and stored encoded as `jsonb`. Unknown keys are rejected. Every failure is `InvalidJobParams`, raised before anything is claimed.

```ts
import { decodeJobParams, findDisallowedApiParams, parseSetFlags } from '@yieldkit/jobs';
import { Effect } from 'effect';

import { syncOrders } from './jobs.ts';

const fromFlags = Effect.gen(function* () {
  const overrides = yield* parseSetFlags(['limit=10']); // { limit: 10 }; JSON, else a string
  return yield* decodeJobParams(syncOrders, overrides); // { params, encoded }
});

const rejected = findDisallowedApiParams(syncOrders, { dryRunEmail: true }); // ['dryRunEmail']
```

## Durable stages

`durableStage({ name, schema, execute })` runs `execute` as an Effect `Activity` and stores its result, encoded with `schema`.

<!-- prettier-ignore -->
```ts
import { defineJob, durableStage } from '@yieldkit/jobs';
import { Effect, Schema } from 'effect';

const Rows = Schema.Struct({ count: Schema.Int });

export const nightlyReport = defineJob({
  name: 'nightly-report',
  description: 'Builds and mails the daily report',
  cadence: { _tag: 'Daily' },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Rows,
  run: () =>
    Effect.gen(function* () {
      const rows = yield* durableStage({ name: 'collect', schema: Rows, execute: Effect.succeed({ count: 42 }) });
      yield* Effect.log('runs again in every retry round');    // outside a stage
      return yield* durableStage({ name: 'mail', schema: Rows, execute: Effect.succeed(rows) });
    }),
});
```

Code outside a stage runs again in each retry round; a stage that succeeded returns its stored result. A failed stage is stored as a `JobStageError` with a redacted message, the error's `_tag` and any `retryAfterSeconds`. Outside a workflow (`runJob`, tests) a stage just runs `execute`.

## Running a job directly

`runJob(job, { trigger, params?, force?, runKey?, dryRun?, onClaimed? })` claims the key, runs the job with heartbeats and settles the ledger, without Cluster ([README example](../README.md#run-a-job-from-a-script)). `dryRun: true` provides the job's `dryRun` layer instead of your services and claims and writes nothing:

```ts
import { defineJob, runJob } from '@yieldkit/jobs';
import { Context, Effect, Layer, Schema } from 'effect';

class Mailer extends Context.Service<Mailer, { send: (to: string) => Effect.Effect<void> }>()(
  'app/Mailer',
) {}

export const digest = defineJob({
  name: 'digest',
  description: 'Mails the digest',
  cadence: { _tag: 'Daily' },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Schema.Struct({ sent: Schema.Int }),
  run: () => Mailer.use((mailer) => mailer.send('ops@example.com')).pipe(Effect.as({ sent: 1 })),
  dryRun: Layer.succeed(Mailer, { send: (to) => Effect.log(`would mail ${to}`) }),
});

export const preview = runJob(digest, { trigger: 'manual', dryRun: true });
```

`runJob` keeps the job's services in its type with `dryRun: true`, so the caller still provides them; the run uses the `dryRun` layer's.

Job errors, defects and lost leases become a `failed` outcome. Interrupting the `runJob` fiber itself settles the row as failed, and the fiber exits interrupted instead of returning an outcome. `runJob` itself fails only with `InvalidJobParams`, `InvalidRunKey`, `ClusterConfigError` (an invalid cadence), `DryRunUnsupported` or a ledger `SqlError`.

`runKey` is for `OnDemand` jobs, which need one; any other job fails with `InvalidRunKey` when given one. A `manual` run with the key of an `OnDemand` run that gave up runs it again.

Without a `dryRun` layer, `dryRun: true` fails with `DryRunUnsupported`. `runAnyJob` does the same for an element of a mixed job list.

## Outcomes

`runJob` returns a `JobOutcome`. Workflows return the same shape (`JobOutcomeSchema`), stored in Cluster's replies.

| `status`    | Fields                                                                                           |
| ----------- | ------------------------------------------------------------------------------------------------ |
| `completed` | `attempt`, `dryRun`, `stats`                                                                     |
| `skipped`   | `reason`: `AlreadyCompleted`, `Busy`, `RetryLater` (with `at`) or `Exhausted`                    |
| `failed`    | `attempt`, `errorMessage` (redacted), `isDefect`, `interrupted`, `errorTag`, `retryAfterSeconds` |

All three also carry `job`, `runKey`, `trigger` and `elapsedMs`.

```ts
import type { JobOutcome } from '@yieldkit/jobs';

const describe = (outcome: JobOutcome): string => {
  switch (outcome.status) {
    case 'completed':
      return `attempt ${String(outcome.attempt)} completed`;
    case 'skipped':
      return outcome.reason._tag === 'RetryLater'
        ? `retry at ${outcome.reason.at}`
        : outcome.reason._tag;
    case 'failed':
      return `${outcome.errorTag ?? 'untagged'}: ${outcome.errorMessage}`;
  }
};
```

A run that lost its lease is `failed` with `errorTag: 'RunClaimLost'` and `interrupted: true`.

## Annotations and log fields

Two optional fields carry your own metadata:

<!-- prettier-ignore -->
```ts
import { defineJob } from '@yieldkit/jobs';
import { Context, Effect, Schema } from 'effect';

class Tenant extends Context.Service<Tenant, string>()('app/Tenant') {}

export const tenantSync = defineJob({
  name: 'tenant-sync',
  description: 'Syncs one tenant',
  cadence: { _tag: 'Daily' },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Schema.Struct({}),
  annotations: Context.make(Tenant, 'acme'),   // handed to the onSettle hook
  logFields: { tenant: 'acme' },               // added to every log line of its runs
  run: () => Effect.succeed({}),
});
```

The [settle hook](storage.md#the-settle-hook) reads them back with `Context.getOption(event.annotations, Tenant)`.

## Limits

- Job code runs at least once, not exactly once. A crash repeats the attempt; a stage repeats if its worker dies before its result is stored.
- Each API dispatch is its own workflow execution, so two forced dispatches of one key can both run.
- Do not call `runJob` from a job body that runs in a workflow: its stages would share the parent's journal. Dispatch the other job instead.
- The `name` is persisted as the ledger's `job` column, the workflow name (`Workflow/<name>`) and the shard group. Renaming a job orphans its history and in-flight workflows. Renaming a stage makes in-flight workflows run it again. [Persisted names](workers-and-clients.md#persisted-names) has the full list.
- Cluster stores shard ids as `<name>:<n>` in 50 characters, so a name is at most `50 - 1 - digits of shardsPerGroup` characters: 47 with 16 to 99 shards, 46 with Effect's default 300. Longer names fail `makeSchedulerLayer` and `checkShardingConfig` with `ClusterConfigError`. A name holds only letters, digits, `.`, `_` and `-`, is unique in the job list, and is not `default` or `yieldkit-jobs`; `makeSchedulerLayer` and `checkShardingConfig` fail with `ClusterConfigError` otherwise.
- `Every.interval` and `Daily.checkEvery` must be whole minutes that divide an hour, or whole hours that divide a day (one day included). Anything else, an unparseable duration included, fails `makeSchedulerLayer`, `runJob` and `dispatchApiJobWorkflow` with `ClusterConfigError`.
- Stage names the workflow uses itself (`claim-round-N`, `claim-deadline-N`, `settle-completed-N`, `settle-failed-N`, `settle-skipped-N`, `settle-crash-cap-N`, `settle-invalid-params`, `retry-later-clock-N`, `retry-deadline-N`, `run-at-clock`, anything under `DurableClock/`) are refused with a `JobStageError` before anything runs.
- Stats must encode to JSON (no `bigint`), and neither stats nor params may contain `\u0000`.
- `failed` with `RunClaimLost` also covers a settle that could not be written, and a worker restart right at the settle: the ledger row says what happened, whatever the workflow result or request row reads.
