# Scheduling recipes

| Schedule                                        | Cadence                                                     | Section                                               |
| ----------------------------------------------- | ----------------------------------------------------------- | ----------------------------------------------------- |
| Every 5 minutes                                 | `{ _tag: 'Every', interval: '5 minutes' }`                  | [Every N minutes](#every-n-minutes)                   |
| Once a day, any time                            | `{ _tag: 'Daily' }`                                         | [Once a day](#once-a-day)                             |
| Daily at 9 am, retried until noon               | `{ _tag: 'Daily', at: '09:00', until: '12:00' }`            | [Daily at a fixed time](#daily-at-a-fixed-time)       |
| Daily at a random time between 8 am and 2 pm    | `{ _tag: 'Daily', window: { from: '08:00', to: '14:00' } }` | [Daily in a random window](#daily-in-a-random-window) |
| Cron expression (`0 9 * * 1-5`)                 | `{ _tag: 'Cron', expression: '0 9 * * 1-5' }`               | [Cron](#cron)                                         |
| Once ever (a backfill, an import)               | `{ _tag: 'Once', start: 'automatic' }`                      | [Once ever](#once-ever)                               |
| One-off per item (send this email, maybe later) | `{ _tag: 'OnDemand' }` and `dispatchJob`                    | [One-off work per item](#one-off-work-per-item)       |

`Daily` and `Cron` take a `timeZone` (an IANA name such as `Europe/Brussels`, default `UTC`).

## Every N minutes

```ts
import { defineJob } from '@yieldkit/jobs';
import { Effect, Schema } from 'effect';

export const syncOrders = defineJob({
  name: 'sync-orders',
  description: 'Copies new orders from the shop',
  cadence: { _tag: 'Every', interval: '5 minutes' },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Schema.Struct({ copied: Schema.Int }),
  run: () => Effect.succeed({ copied: 0 }),
});
```

The scheduler runs a UTC cron (`*/5 * * * *`) and the run key is the bucket start, such as `2026-11-26T09:05Z`. The interval must be a whole number of minutes that evenly divides an hour (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30 minutes) or a day (1, 2, 3, 4, 6, 8, 12, 24 hours). A failed run is not retried, and a bucket missed while every worker was down is skipped: the next bucket is a new key.

## Once a day

<!-- prettier-ignore -->
```ts
cadence: { _tag: 'Daily' } // or { _tag: 'Daily', checkEvery: '15 minutes' }
```

The run key is the UTC date (`2026-11-26`). The scheduler checks every `checkEvery` (default 1 hour), and the first check of the day starts the run, so it runs just after midnight UTC. A failed run is retried at each check, up to `retry.dailyMaxFailures` (6) failures. A day missed while the workers were down is caught up by the first check after they start.

## Daily at a fixed time

```ts
import { defineJob } from '@yieldkit/jobs';
import { Effect, Schema } from 'effect';

export const morningDigest = defineJob({
  name: 'morning-digest',
  description: 'Mails the morning digest',
  cadence: {
    _tag: 'Daily',
    at: '09:00',
    until: '12:00',
    timeZone: 'Europe/Brussels',
    checkEvery: '15 minutes',
  },
  params: { schema: Schema.Struct({}), defaults: {} },
  stats: Schema.Struct({ sent: Schema.Int }),
  run: () => Effect.succeed({ sent: 1 }),
});
```

- **When it starts.** The run key is the local date in `timeZone`. The scheduler still checks every `checkEvery`, but a check before 09:00 local time does nothing. The first check at or after 09:00 starts the day's run, and later checks deduplicate on the date.
- **Retries.** A failed run is retried every `checkEvery` (09:15, 09:30, ...) as long as the retry would start at or before `until`. `retry.dailyMaxFailures` does not apply. A failure at 11:50 is the last attempt here, since the next one would start at 12:05; the key is then exhausted, and only a `manual` or `api` run takes it that day.
- **Catch-up.** If no worker is up at 09:00, the first check after one starts runs the job, as long as that is before `until`. A worker that starts at 12:30 does not run it that day.
- **`until`.** It defaults to `24:00` (end of the local day). It must leave at least one `checkEvery` after `at`, or no check could start the run.
- **Daylight saving time.** A time skipped in spring runs at the first check after the gap; a repeated autumn time runs once. An `until` inside the gap moves forward by its length.
- **Precision.** The run starts at the first check at or after `at`: with the default hourly `checkEvery`, `at: '09:20'` runs at 10:00.

## Daily in a random window

```ts
cadence: {
  _tag: 'Daily',
  window: { from: '08:00', to: '14:00' },
  timeZone: 'Europe/Brussels',
  checkEvery: '5 minutes',
}
```

Each day's run is due at one slot of the window, `from` plus a multiple of `checkEvery`, picked from a hash of the job name and the date, so every worker agrees without sharing state. The first attempt starts before `to`. Retries, catch-up and daylight saving time work as for `at`; `until` defaults to `window.to`. The window must be at least one `checkEvery` long, and renaming the job changes its slots.

## Cron

```ts
cadence: { _tag: 'Cron', expression: '0 9 * * 1-5', timeZone: 'Europe/Brussels' }
```

- **Expression.** Five fields: minute, hour, day of month, month, day of week. A seconds field is rejected, since run keys have minute precision, and so is an expression that never matches a date (`0 0 31 2 *`).
- **Run key.** The tick in UTC, `YYYY-MM-DDTHH:mmZ`, the same format as `Every`. A `manual` or `api` run uses the latest tick at or before now.
- **Failures.** Like `Every`: a failed run is not retried, and the next tick is a new key. A failed dispatch is retried every 30 seconds within `catchUpWithin`.
- **Catch-up.** Cluster keeps one pending message, for the next tick. If the workers were down at that tick, it still runs when one starts, as long as it is at most `catchUpWithin` old (default 1 hour; it must be a finite duration above zero). Later ticks missed during the same outage are skipped, and there is no start-up run.
- **Daylight saving time.** Ticks follow local time in `timeZone`. A local time repeated by the autumn change runs once, at its first occurrence. A local time skipped by the spring change runs at the same offset after the change (02:30 runs at 03:30). When an expression has several ticks inside the skipped hour (`0,30 2 * * *`), only the first runs that day.

Use `Cron` for what `Every` and `Daily` can't express: weekdays only, several fixed times a day, the first of the month.

## Once ever

```ts
cadence: { _tag: 'Once', start: 'automatic' } // scheduler runs it at worker start-up until it completes
cadence: { _tag: 'Once', start: 'manual' }    // only runJob starts it
```

The run key is the constant `'once'`, so the job completes once per job name, for the life of the database. Use it for backfills and imports. It is not a way to send one email: a `Once` job named `send-email` sends one email, ever. For that, use `OnDemand`.

## One-off work per item

An `OnDemand` job is never scheduled. Your code starts each run with `dispatchJob` under a run key it chooses, one per email, webhook or export:

<!-- prettier-ignore -->
```ts
import { defineJob, dispatchJob } from '@yieldkit/jobs';
import { Effect, Schema } from 'effect';

export const sendEmail = defineJob({
  name: 'send-email',
  description: 'Sends one email',
  cadence: { _tag: 'OnDemand' },
  params: {
    schema: Schema.Struct({ to: Schema.String, template: Schema.String }),
    defaults: {},
  },
  stats: Schema.Struct({ providerId: Schema.String }),
  run: ({ params, runKey }) =>
    Effect.succeed({ providerId: `${runKey}:${params.template}` }), // call your provider here
});

// In your API, after you authenticated the caller. Needs WorkflowEngine (a client-only Cluster).
export const sendWelcome = (userId: string, to: string) =>
  dispatchJob(sendEmail, {
    runKey: `welcome:${userId}`,       // 1 to 200 printable ASCII characters, no spaces
    params: { to, template: 'welcome' },
  });

export const sendReminder = (userId: string, to: string) =>
  dispatchJob(sendEmail, {
    runKey: `reminder:${userId}`,
    params: { to, template: 'reminder' },
    delay: '24 hours',                  // or runAt: a DateTime
  });
```

- **One run per key.** The workflow's execution id is derived from the job name and the run key. Dispatching a key again, with any params, joins the run already started under it, so a retried API request sends one email. `dispatchJob` returns `{ runKey, executionId }` without waiting.
- **Validation.** Params are decoded before dispatch (`InvalidJobParams`); a bad key or a non-`OnDemand` job fails with `InvalidRunKey`, a bad `delay` or `runAt` with `InvalidDispatch`. `apiParams` does not apply.
- **Retries.** A failed run is retried after 1, 2, 4 and 8 minutes, then the key is exhausted ([policy](policy.md#retry-rules)). `runJob(sendEmail, { trigger: 'manual', runKey })` runs it again.
- **Delays.** The workflow waits durably before it claims the key. Keep every delay shorter than `retention.onDemand` (30 days), or the sweep deletes the waiting workflow.
- **Ledger.** One row per key, `trigger: 'schedule'`, pruned after `retention.onDemand`; after that the key runs again.
- `dispatchApiJobWorkflow` rejects `OnDemand` jobs; call `dispatchJob` from your API. Pass the run key to your provider as its idempotency key, since a crash can repeat the run.
