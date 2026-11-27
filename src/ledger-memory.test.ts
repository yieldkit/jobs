import { Context, Effect, Option, Result, Schema } from 'effect';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { RunResult } from './ledger-types.ts';

import { InvalidRunCursor } from './errors.ts';
import { makeJobRunsMemory } from './ledger-memory.ts';
import { JobRuns } from './ledger.ts';

const FAILED: RunResult = {
  _tag: 'Failed',
  errorMessage: 'upstream 503',
  reason: 'error',
  cadence: 'Daily',
  trigger: 'schedule',
  annotations: Context.empty(),
};

/**
 * Runs a history query against a fresh ledger with a controllable clock.
 */
const runHistory = <A, E>(effect: Effect.Effect<A, E, JobRuns>): Promise<A> =>
  Effect.gen(function* () {
    const memory = yield* makeJobRunsMemory();
    return yield* effect.pipe(Effect.provide(memory.layer));
  }).pipe(Effect.provide(TestClock.layer()), Effect.runPromise);

/**
 * Builds malformed tokens that pass the base64url layer of cursor validation.
 */
const encodeBase64Url = Schema.encodeSync(Schema.StringFromBase64Url);

/**
 * Encodes raw bytes as unpadded base64url to test strict UTF-8 cursor validation.
 */
const encodeBytesBase64Url = Schema.encodeSync(Schema.Uint8ArrayFromBase64Url);

/**
 * Valid JSON cursor structure containing a raw invalid UTF-8 byte (`0xff`) inside `runKey`.
 */
const INVALID_UTF8_CURSOR = encodeBytesBase64Url(
  new Uint8Array([
    ...new TextEncoder().encode('{"job":"alpha","runKey":"'),
    0xff,
    ...new TextEncoder().encode('","startedAtMicros":"0"}'),
  ]),
);

describe('makeJobRunsMemory', () => {
  it('looks up a run by both job and run key', async () => {
    const result = await runHistory(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        const key = { job: 'alpha', runKey: 'same' };
        yield* runs.claim({ ...key, trigger: 'manual', force: false, params: { limit: 2 } });
        const hit = yield* runs.get(key);
        const wrongJob = yield* runs.get({ ...key, job: 'beta' });
        const wrongKey = yield* runs.get({ ...key, runKey: 'other' });
        return { hit, wrongJob, wrongKey };
      }),
    );
    expect(Option.getOrUndefined(result.hit)).toMatchObject({
      job: 'alpha',
      runKey: 'same',
      status: 'running',
      params: { limit: 2 },
    });
    expect(result.wrongJob).toEqual(Option.none());
    expect(result.wrongKey).toEqual(Option.none());
  });

  it('pages through tied start times and filters history by job', async () => {
    const result = await runHistory(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        /**
         * Leaves a run started at the current test-clock time.
         */
        const claim = (job: string, runKey: string) =>
          runs.claim({ job, runKey, trigger: 'manual', force: false, params: {} });
        yield* claim('beta', 'zzz-old');
        yield* TestClock.adjust(1);
        yield* claim('alpha', 'same');
        yield* claim('alpha', 'aa');
        yield* claim('beta', 'same');
        yield* claim('alpha', 'zz');

        const first = yield* runs.page({ limit: 2 });
        // An insert ahead of the boundary must not shift the next page.
        yield* TestClock.adjust(1);
        yield* claim('beta', 'new');
        const second = yield* runs.page({ limit: 2, cursor: first.nextCursor ?? undefined });
        const third = yield* runs.page({ limit: 2, cursor: second.nextCursor ?? undefined });
        const filtered = yield* runs.page({ job: 'alpha', limit: 2 });
        const filteredTail = yield* runs.page({
          job: 'alpha',
          limit: 2,
          cursor: filtered.nextCursor ?? undefined,
        });
        const empty = yield* runs.page({ job: 'missing', limit: 2 });
        return { first, second, third, filtered, filteredTail, empty };
      }),
    );
    expect(result.first.runs).toMatchObject([
      { job: 'alpha', runKey: 'zz' },
      { job: 'beta', runKey: 'same' },
    ]);
    expect(result.second.runs).toMatchObject([
      { job: 'alpha', runKey: 'same' },
      { job: 'alpha', runKey: 'aa' },
    ]);
    expect(result.third.runs).toMatchObject([{ job: 'beta', runKey: 'zzz-old' }]);
    expect(result.third.nextCursor).toBeNull();
    expect(result.filtered.runs.map(({ runKey }) => runKey)).toEqual(['zz', 'same']);
    expect(result.filteredTail.runs.map(({ runKey }) => runKey)).toEqual(['aa']);
    expect(result.filteredTail.nextCursor).toBeNull();
    expect(result.empty).toEqual({ runs: [], nextCursor: null });
  });

  it.each([
    ['base64url', '???'],
    ['UTF-8 bytes', INVALID_UTF8_CURSOR],
    ['JSON', encodeBase64Url('{')],
    ['cursor shape', encodeBase64Url('{"job":"alpha"}')],
    [
      'NUL byte',
      encodeBase64Url(String.raw`{"job":"alpha","runKey":"a\u0000b","startedAtMicros":"0"}`),
    ],
    [
      'lone surrogate',
      encodeBase64Url(String.raw`{"job":"alpha","runKey":"\ud800","startedAtMicros":"0"}`),
    ],
    [
      'timestamp above max',
      encodeBase64Url('{"job":"alpha","runKey":"same","startedAtMicros":"99999999999999999999"}'),
    ],
    [
      'timestamp below min',
      encodeBase64Url('{"job":"alpha","runKey":"same","startedAtMicros":"-211813488000000001"}'),
    ],
  ])('fails with typed InvalidRunCursor for invalid %s', async (_, cursor) => {
    const outcome = await runHistory(
      JobRuns.use((runs) => Effect.result(runs.page({ limit: 2, cursor }))),
    );
    const failure = Result.isFailure(outcome) ? outcome.failure : undefined;
    expect(failure?._tag).toBe('InvalidRunCursor');
    expect(Schema.is(InvalidRunCursor)(failure)).toBe(true);
  });

  it('matches bytewise tie order across page boundaries, exact final pages, and recent', async () => {
    const result = await runHistory(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        /**
         * Claims one tied run at the current test-clock timestamp.
         */
        const claim = (job: string, runKey: string) =>
          runs.claim({ job, runKey, trigger: 'manual', force: false, params: {} });
        for (const runKey of ['Z', 'a', '\uE000', '\u{1F600}']) {
          yield* claim('utf8', runKey);
        }

        const first = yield* runs.page({ job: 'utf8', limit: 2 });
        const second = yield* runs.page({
          job: 'utf8',
          limit: 2,
          cursor: first.nextCursor ?? undefined,
        });
        const recentUtf8 = yield* runs.recent({ job: 'utf8', limit: 4 });

        yield* TestClock.adjust(1);
        yield* claim('alpha', 'tie');
        yield* claim('beta', 'tie');
        const recentAll = yield* runs.recent({ limit: 2 });
        const pageAll = yield* runs.page({ limit: 2 });
        return { first, second, recentUtf8, recentAll, pageAll };
      }),
    );
    expect(result.first.runs.map(({ runKey }) => runKey)).toEqual(['\u{1F600}', '\uE000']);
    expect(result.first.nextCursor).not.toBeNull();
    expect(result.second.runs.map(({ runKey }) => runKey)).toEqual(['a', 'Z']);
    expect(result.second.nextCursor).toBeNull();
    expect(result.recentUtf8.map(({ runKey }) => runKey)).toEqual([
      '\u{1F600}',
      '\uE000',
      'a',
      'Z',
    ]);
    expect(result.recentAll.map(({ job }) => job)).toEqual(['beta', 'alpha']);
    expect(result.pageAll.runs.map(({ job }) => job)).toEqual(['beta', 'alpha']);
  });

  it('normalizes edge-case limits consistently for recent and page', async () => {
    const result = await runHistory(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        yield* runs.claim({
          job: 'lim',
          runKey: 'one',
          trigger: 'manual',
          force: false,
          params: {},
        });
        yield* runs.claim({
          job: 'lim',
          runKey: 'two',
          trigger: 'manual',
          force: false,
          params: {},
        });
        const limits = [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 1.9, 1e20] as const;
        const recent = yield* Effect.forEach(limits, (limit) => runs.recent({ job: 'lim', limit }));
        const paged = yield* Effect.forEach(limits, (limit) => runs.page({ job: 'lim', limit }));
        return { recent, paged };
      }),
    );
    expect(result.recent.map((rows) => rows.length)).toEqual([0, 0, 0, 0, 1, 2]);
    expect(result.paged.map((page) => page.runs.length)).toEqual([0, 0, 0, 0, 1, 2]);
    expect(result.paged[4]?.nextCursor).not.toBeNull();
    expect(result.paged[5]?.nextCursor).toBeNull();
  });

  it('isolates returned records from mutation and separates NUL-containing keys', async () => {
    const result = await runHistory(
      Effect.gen(function* () {
        const runs = yield* JobRuns;
        yield* runs.claim({
          job: 'a',
          runKey: 'b\0c',
          trigger: 'manual',
          force: false,
          params: { nested: 1 },
        });
        const collision = yield* runs.get({ job: 'a\0b', runKey: 'c' });

        yield* runs.claim({
          job: 'iso',
          runKey: 'r1',
          trigger: 'manual',
          force: false,
          params: { count: 1 },
        });
        const firstRead = yield* runs.get({ job: 'iso', runKey: 'r1' });
        if (Option.isSome(firstRead)) {
          firstRead.value.startedAt.setTime(Number.NaN);
        }

        const secondRead = yield* runs.get({ job: 'iso', runKey: 'r1' });
        const page = yield* runs.page({ job: 'iso', limit: 1 });
        return { collision, secondRead, page };
      }),
    );
    expect(result.collision).toEqual(Option.none());
    const secondRead = Option.getOrUndefined(result.secondRead);
    expect(secondRead).toBeDefined();
    expect(Number.isFinite(secondRead?.startedAt.getTime())).toBe(true);

    expect(result.page.runs).toHaveLength(1);
    expect(Schema.isSchema(InvalidRunCursor)).toBe(true);
  });

  it('reports a repeated settle with the same token as settled without applying it twice', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory();
        return yield* Effect.gen(function* () {
          const runs = yield* JobRuns;
          const claim = yield* runs.claim({
            job: 'settle-twice',
            runKey: '2026-11-25',
            trigger: 'schedule',
            force: false,
            params: {},
          });
          if (claim._tag !== 'Acquired') {
            return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
          }

          const owned = { job: 'settle-twice', runKey: '2026-11-25', ...claim };
          const first = yield* runs.settle(owned, FAILED);
          const repeated = yield* runs.settle(owned, FAILED);
          const asCompleted = yield* runs.settle(owned, {
            _tag: 'Completed',
            stats: {},
            trigger: 'schedule',
            annotations: Context.empty(),
          });
          return { first, repeated, asCompleted, rows: memory.rows(), settles: memory.settles() };
        }).pipe(Effect.provide(memory.layer));
      }),
    );
    expect(result.first).toBe(true);
    expect(result.repeated).toBe(true);
    expect(result.asCompleted).toBe(false);
    expect(result.rows[0]).toMatchObject({ status: 'failed', failures: 1 });
    expect(result.settles).toHaveLength(1);
  });

  it('keeps a Daily key exhausted after an interrupted run that took it manually', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const memory = yield* makeJobRunsMemory({ policy: { retry: { dailyMaxFailures: 2 } } });
        return yield* Effect.gen(function* () {
          const runs = yield* JobRuns;
          const key = { job: 'exhausted-interrupt', runKey: '2026-11-25' };
          const reasons = ['error', 'error', 'interrupted'] as const;
          for (const reason of reasons) {
            const claim = yield* runs.claim({ ...key, trigger: 'api', force: false, params: {} });
            if (claim._tag !== 'Acquired') {
              return yield* Effect.die(new Error(`expected Acquired, got ${claim._tag}`));
            }

            yield* runs.settle({ ...key, ...claim }, { ...FAILED, trigger: 'api', reason });
          }

          const scheduled = yield* runs.claim({
            ...key,
            trigger: 'schedule',
            force: false,
            params: {},
          });
          return { scheduled, row: memory.rows()[0] };
        }).pipe(Effect.provide(memory.layer));
      }),
    );
    expect(result.row).toMatchObject({ failures: 2, nextAttemptAt: null });
    expect(result.scheduled).toEqual({ _tag: 'Exhausted' });
  });
});
