import { Effect, Schema } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import type { JobCadence } from './job.ts';

import { defineJob } from './job.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { runJob } from './run.ts';

/**
 * Runs a job on `cadence` once with `runJob` and `runKey` on a memory ledger, and returns the error
 * `_tag` (a defect rethrows), or `completed`, plus whether the body ran and how many ledger rows it
 * wrote.
 */
const runOnce = (cadence: JobCadence, runKey?: string) => {
  const calls: Array<string> = [];
  const job = defineJob({
    name: 'validated',
    description: 'validated',
    cadence,
    params: { schema: Schema.Struct({}), defaults: {} },
    stats: Schema.Struct({}),
    run: ({ runKey: key }) => Effect.sync(() => calls.push(key)).pipe(Effect.as({})),
  });
  return Effect.gen(function* () {
    const memory = yield* makeJobRunsMemory();
    const result = yield* runJob(job, { trigger: 'manual', runKey }).pipe(
      Effect.match({ onFailure: (error) => error._tag, onSuccess: (outcome) => outcome.status }),
      Effect.provide(memory.layer),
    );
    return { result, calls, rows: memory.rows().length };
  }).pipe(Effect.runPromise);
};

/**
 * A type guard that lies: it accepts any defined value as a string, as an untyped caller would.
 */
const claimsString = (value: unknown): value is string => value !== undefined;

/**
 * `value` typed as a string without checking it, as JavaScript or JSON input can arrive.
 */
const untypedString = (value: unknown): string => (claimsString(value) ? value : '');

describe('runJob input validation', () => {
  it('fails with ClusterConfigError, not a defect, for a cadence that cannot give a key', async () => {
    expect(await runOnce({ _tag: 'Daily', timeZone: 'Moon/Base' })).toEqual({
      result: 'ClusterConfigError',
      calls: [],
      rows: 0,
    });
    expect(await runOnce({ _tag: 'Cron', expression: '0 0 31 2 *' })).toEqual({
      result: 'ClusterConfigError',
      calls: [],
      rows: 0,
    });
    expect(await runOnce({ _tag: 'Cron', expression: '0 0 29 2 *' })).toEqual({
      result: 'completed',
      calls: [expect.stringMatching(/-02-29T00:00Z$/u)],
      rows: 1,
    });
  });

  it('rejects an interval or checkEvery the scheduler would reject, before claiming', async () => {
    const invalid: ReadonlyArray<JobCadence> = [
      { _tag: 'Every', interval: Number.POSITIVE_INFINITY },
      { _tag: 'Every', interval: 'Infinity' },
      { _tag: 'Every', interval: '30 seconds' },
      { _tag: 'Every', interval: '7 minutes' },
      { _tag: 'Every', interval: '90 minutes' },
      // @ts-expect-error -- an untyped caller passes a string that is not a duration
      { _tag: 'Every', interval: 'bogus' },
      { _tag: 'Daily', checkEvery: '7 minutes' },
      // @ts-expect-error -- an untyped caller passes a string that is not a duration
      { _tag: 'Daily', checkEvery: 'bogus' },
    ];
    const results = await Promise.all(invalid.map((cadence) => runOnce(cadence)));
    expect(results).toEqual(
      invalid.map(() => ({ result: 'ClusterConfigError', calls: [], rows: 0 })),
    );
  });

  it('rejects a run key that is not a string', async () => {
    expect(await runOnce({ _tag: 'OnDemand' }, untypedString(42))).toEqual({
      result: 'InvalidRunKey',
      calls: [],
      rows: 0,
    });
  });
});
