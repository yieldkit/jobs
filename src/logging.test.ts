import { logDebug } from '@yieldkit/logger';
import { captureLogs, makeTestLogger } from '@yieldkit/logger/testing';
import { Data, Duration, Effect, Exit, Fiber, Layer } from 'effect';
import { SecureRandom } from 'effect/crypto';
import { Headers } from 'effect/http';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import { withJobContext, withJobLogging, withJobStage } from './logging.ts';
import { testClockLayer } from './test-clock.fixture.ts';

class UpstreamTestError extends Data.TaggedError('UpstreamTestError')<{
  readonly message: string;
  readonly status: number;
}> {}

describe('job logging', () => {
  it('propagates job context, measures deterministic duration, and logs stages and summary', async () => {
    const harness = makeTestLogger({ service: 'worker', minLevel: 'Debug' });
    const jobProgram = Effect.gen(function* () {
      yield* Effect.gen(function* () {
        yield* Effect.sleep(Duration.millis(250));
        yield* logDebug('stage.progress', { fetched: 30 });
      }).pipe(withJobStage('fetch'));

      return { itemCount: 30, newItemCount: 5 };
    }).pipe(
      withJobLogging({
        job: 'sync-orders',
        jobRunId: 'run-001',
        fields: { region: 'eu' },
        trigger: 'schedule',
        attempt: 1,
        summarizeResult: (result) => ({
          itemCount: result.itemCount,
          newItemCount: result.newItemCount,
        }),
      }),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const running = yield* Effect.forkChild(jobProgram);
        yield* TestClock.adjust(Duration.millis(250));
        return yield* Fiber.join(running);
      }).pipe(
        Effect.provide(Layer.mergeAll(harness.layer, testClockLayer, SecureRandom.layerWebCrypto)),
      ),
    );
    expect(result).toEqual({ itemCount: 30, newItemCount: 5 });
    expect(harness.records.map((entry) => entry.message)).toEqual([
      'job.started',
      'stage.progress',
      'job.completed',
    ]);

    const [started, progress, completed] = harness.records;
    expect(started?.level).toBe('DEBUG');
    expect(started?.annotations).toMatchObject({
      job: 'sync-orders',
      jobRunId: 'run-001',
      region: 'eu',
      trigger: 'schedule',
      attempt: 1,
      status: 'started',
    });
    expect(progress?.annotations).toMatchObject({
      job: 'sync-orders',
      jobRunId: 'run-001',
      stage: 'fetch',
      fetched: 30,
    });
    expect(progress?.spans['stage.fetch']).toBe(250);
    expect(completed?.level).toBe('INFO');
    expect(completed?.annotations).toMatchObject({
      job: 'sync-orders',
      jobRunId: 'run-001',
      status: 'completed',
      durationMs: 250,
      itemCount: 30,
      newItemCount: 5,
    });
    expect(completed?.spans['job.run']).toBe(250);
    expect(completed?.traceId).toBeDefined();
    expect(completed?.spanId).toBeDefined();
  });

  it('logs skipped, failed, defect, and interrupted job exits', async () => {
    const skippedRun = await Effect.runPromise(
      captureLogs(
        Effect.succeed({ skipped: true }).pipe(
          withJobLogging({
            job: 'skills-daily',
            jobRunId: 'run-skip',
            isSkipped: (result) => result.skipped,
          }),
        ),
      ).pipe(Effect.provide(SecureRandom.layerWebCrypto)),
    );
    expect(skippedRun.logs).toHaveLength(1);
    expect(skippedRun.logs[0]?.message).toBe('job.skipped');
    expect(skippedRun.logs[0]?.annotations).toMatchObject({
      job: 'skills-daily',
      jobRunId: 'run-skip',
      status: 'skipped',
    });

    const failHarness = makeTestLogger({ minLevel: 'Info' });
    const failExit = await Effect.runPromiseExit(
      Effect.fail(new UpstreamTestError({ message: 'Bad Gateway', status: 502 })).pipe(
        withJobLogging({ job: 'github-rising', jobRunId: 'run-fail' }),
        Effect.provide(Layer.mergeAll(failHarness.layer, SecureRandom.layerWebCrypto)),
      ),
    );
    expect(Exit.isFailure(failExit)).toBe(true);
    expect(failHarness.records).toHaveLength(1);
    expect(failHarness.records[0]?.message).toBe('job.failed');
    expect(failHarness.records[0]?.annotations).toMatchObject({
      job: 'github-rising',
      jobRunId: 'run-fail',
      status: 'failed',
      isDefect: false,
      errorTag: 'UpstreamTestError',
      errorStatus: 502,
    });
    expect(failHarness.records[0]?.error?.kind).toBe('fail');

    const dieHarness = makeTestLogger({ minLevel: 'Info' });
    const dieExit = await Effect.runPromiseExit(
      Effect.die(new Error('unexpected invariant violation')).pipe(
        withJobLogging({ job: 'daily-report', jobRunId: 'run-die' }),
        Effect.provide(Layer.mergeAll(dieHarness.layer, SecureRandom.layerWebCrypto)),
      ),
    );
    expect(Exit.isFailure(dieExit)).toBe(true);
    expect(dieHarness.records).toHaveLength(1);
    expect(dieHarness.records[0]?.annotations).toMatchObject({
      job: 'daily-report',
      status: 'failed',
      isDefect: true,
      errorMessage: 'unexpected invariant violation',
    });
    expect(dieHarness.records[0]?.error?.kind).toBe('die');

    const interruptHarness = makeTestLogger({ minLevel: 'Info' });
    await Effect.runPromise(
      Effect.gen(function* () {
        const running = yield* Effect.forkChild(
          Effect.never.pipe(withJobLogging({ job: 'sync-orders', jobRunId: 'run-int' })),
        );
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(running);
      }).pipe(Effect.provide(Layer.mergeAll(interruptHarness.layer, SecureRandom.layerWebCrypto))),
    );
    expect(interruptHarness.records).toHaveLength(1);
    expect(interruptHarness.records[0]?.level).toBe('WARN');
    expect(interruptHarness.records[0]?.message).toBe('job.interrupted');
    expect(interruptHarness.records[0]?.annotations).toMatchObject({
      job: 'sync-orders',
      jobRunId: 'run-int',
      status: 'interrupted',
    });
  });

  it('omits the error message when a failed object has no message of its own', async () => {
    const harness = makeTestLogger({ redaction: { sensitiveKeys: ['customerRef'] } });
    await Effect.runPromiseExit(
      Effect.fail({ customerRef: 'fixture-42' }).pipe(
        withJobLogging({ job: 'sync-orders', jobRunId: 'run-fail' }),
        Effect.provide(Layer.mergeAll(harness.layer, SecureRandom.layerWebCrypto)),
      ),
    );
    const [record] = harness.records;
    expect(record?.message).toBe('job.failed');
    expect(record?.annotations).not.toHaveProperty('errorMessage');
    expect(JSON.stringify(record)).not.toContain('fixture-42');
  });

  it('masks custom redacted headers in job context fields', async () => {
    const fields = Headers.fromInput({ 'x-example': 'fixture-42', job: 'header' });
    const program = Effect.logInfo('progress').pipe(
      withJobContext({ job: 'sync-orders', jobRunId: 'run-001', fields }),
      Effect.provideService(Headers.CurrentRedactedNames, ['x-example']),
      Effect.provide(SecureRandom.layerWebCrypto),
    );
    const { logs } = await Effect.runPromise(captureLogs(program));
    expect(logs[0]?.annotations).toMatchObject({ job: 'sync-orders', 'x-example': '<redacted>' });
  });

  it('masks custom redacted headers in stage fields', async () => {
    const fields = Headers.fromInput({ 'x-example': 'fixture-42', stage: 'header' });
    const program = Effect.logInfo('progress').pipe(
      withJobStage('fetch', fields),
      Effect.provideService(Headers.CurrentRedactedNames, ['x-example']),
    );
    const { logs } = await Effect.runPromise(captureLogs(program));
    expect(logs[0]?.annotations).toMatchObject({ stage: 'fetch', 'x-example': '<redacted>' });
  });

  it('generates a UUIDv4 run id from SecureRandom when jobRunId is omitted', async () => {
    const harness = makeTestLogger({ minLevel: 'Info' });
    const zeroBytes = SecureRandom.make((size) => new Uint8Array(size));
    await Effect.runPromise(
      Effect.succeed({ done: true }).pipe(
        withJobLogging({ job: 'sync-orders' }),
        Effect.provide(
          Layer.mergeAll(harness.layer, Layer.succeed(SecureRandom.SecureRandom, zeroBytes)),
        ),
      ),
    );
    expect(harness.records).toHaveLength(1);
    expect(harness.records[0]?.annotations).toMatchObject({
      job: 'sync-orders',
      jobRunId: '00000000-0000-4000-8000-000000000000',
      status: 'completed',
    });
  });
});
