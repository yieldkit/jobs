import { TestClock } from 'effect/testing';

/**
 * `TestClock` for the unit tests. After an adjustment wakes a fiber, the clock waits for it to
 * suspend again, but only for `warningDelay` of real time (1 second by default); past that it logs
 * a warning and moves on while the fiber is still running. With many test files running at once on
 * a small CI runner, a woken workflow can take longer than a second, so this allows a minute. See
 * `src/digest.setup.ts` for the other source of real time in these tests.
 */
export const testRuntimeLayer = TestClock.layer({ warningDelay: '1 minute' });
