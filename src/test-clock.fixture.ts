import { TestClock } from 'effect/testing';

/**
 * `TestClock` for the unit tests. After an adjustment wakes a fiber, the clock waits for it to
 * suspend again, but only for `warningDelay` of real time (1 second by default); past that it logs
 * a warning and moves on while the fiber is still running. When many test files run in parallel on
 * a small CI runner, a woken workflow can take longer than a second of real time, so the next
 * adjustment lands before it schedules its retry and the retry drifts a step late. A minute of real
 * time leaves room for a starved worker.
 */
export const testClockLayer = TestClock.layer({ warningDelay: '1 minute' });
