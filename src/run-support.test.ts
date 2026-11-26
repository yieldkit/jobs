import { makeTestLogger } from '@yieldkit/logger/testing';
import { Duration, Effect, Exit, Fiber, Layer } from 'effect';
import { SqlError } from 'effect/sql';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import { raceWithLeaseRenewal, retryTransientSql } from './run-support.ts';
import { testRuntimeLayer } from './test-runtime.fixture.ts';

/**
 * What `@effect/sql-pg` builds when a demoted primary refuses a write (SQLSTATE 25006).
 */
const readOnlyTransaction = (): SqlError.SqlError =>
  new SqlError.SqlError({
    reason: new SqlError.UnknownError({
      cause: Object.assign(new Error('read-only transaction'), { code: '25006' }),
      message: 'PgConnection: Query failed',
    }),
  });

/**
 * Runs a query that fails `failures` times and then succeeds, through `retryTransientSql`.
 */
const runFailing = (failures: number) =>
  Effect.gen(function* () {
    let calls = 0;
    const query = Effect.suspend(() => {
      calls += 1;
      return calls <= failures ? Effect.fail(readOnlyTransaction()) : Effect.succeed('ok');
    });
    const fiber = yield* retryTransientSql(query).pipe(Effect.exit, Effect.forkChild);
    yield* TestClock.adjust('1 minute');
    const exit = yield* Fiber.join(fiber);
    return { exit, calls };
  }).pipe(Effect.provide(testRuntimeLayer), Effect.runPromise);

describe('retryTransientSql', () => {
  it('retries any SqlError five times, then gives up', async () => {
    const recovered = await runFailing(5);
    const exhausted = await runFailing(6);
    expect(Exit.isSuccess(recovered.exit)).toBe(true);
    expect(recovered.calls).toBe(6);
    expect(Exit.isFailure(exhausted.exit)).toBe(true);
    expect(exhausted.calls).toBe(6);
  });
});

describe('raceWithLeaseRenewal', () => {
  it('counts a renewed lease from the request, so a late reply does not extend it', async () => {
    const exit = await Effect.gen(function* () {
      let renewals = 0;
      // The first renewal commits at once but is acknowledged four seconds later; every later one
      // fails, so the lease granted at five seconds runs out at twenty-five.
      const renew = Effect.suspend(() => {
        renewals += 1;
        return renewals === 1
          ? Effect.sleep('4 seconds').pipe(Effect.as(true))
          : Effect.fail(readOnlyTransaction());
      });
      const fiber = yield* raceWithLeaseRenewal(
        Effect.sleep('60 seconds').pipe(Effect.as('done')),
        {
          renew,
          lease: { duration: Duration.seconds(20), heartbeatsPerLease: 4 },
          lost: () => 'lost',
          failureEvent: 'test.renewal.failed',
          fields: {},
        },
      ).pipe(Effect.exit, Effect.forkChild);
      for (let second = 0; second < 25; second += 1) {
        yield* TestClock.adjust('1 second');
      }

      return fiber.pollUnsafe();
    }).pipe(
      Effect.provide(Layer.merge(testRuntimeLayer, makeTestLogger({ minLevel: 'Fatal' }).layer)),
      Effect.runPromise,
    );
    expect(exit).toMatchObject({ _tag: 'Success', value: { _tag: 'Failure' } });
  });
});
