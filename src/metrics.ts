import { durationHistogram } from '@yieldkit/logger';
import { Effect, Metric } from 'effect';

import type { JobTrigger } from './job.ts';

/**
 * Counter of finished job runs tagged by `job`, `status`, and `trigger`.
 */
export const jobRunsTotal: Metric.Counter<number> = Metric.counter('job_runs_total', {
  description: 'Finished job runs by job, status and trigger',
  incremental: true,
});

/**
 * Histogram of job run durations in milliseconds tagged by `job`.
 */
export const jobDurationMs: Metric.Histogram<number> = durationHistogram(
  'job_duration_ms',
  'Job run duration in milliseconds by job',
);

/**
 * Counter of runs that lost their `(job, run_key)` lease claim before settling.
 */
export const jobLeaseLostTotal: Metric.Counter<number> = Metric.counter('job_lease_lost_total', {
  description: 'Job runs that lost their run claim before settling',
  incremental: true,
});

/**
 * Records `job_runs_total`, `job_duration_ms` and, when `leaseLost` is `true`,
 * `job_lease_lost_total`.
 */
export const recordJobRunMetrics = (params: {
  readonly job: string;
  readonly status: 'completed' | 'skipped' | 'failed';
  readonly trigger: JobTrigger;
  readonly elapsedMs: number;
  readonly leaseLost: boolean;
}): Effect.Effect<void> => {
  const updates: Array<Effect.Effect<void>> = [
    Metric.update(
      Metric.withAttributes(jobRunsTotal, {
        job: params.job,
        status: params.status,
        trigger: params.trigger,
      }),
      1,
    ),
    Metric.update(
      Metric.withAttributes(jobDurationMs, { job: params.job }),
      Math.max(0, params.elapsedMs),
    ),
  ];

  if (params.leaseLost) {
    updates.push(Metric.update(Metric.withAttributes(jobLeaseLostTotal, { job: params.job }), 1));
  }

  return Effect.all(updates, { discard: true });
};
