import { Context, Data } from 'effect';

/**
 * A failed call to some upstream service, as a job would report it.
 */
export class UpstreamError extends Data.TaggedError('UpstreamError')<{
  /**
   * HTTP status the upstream answered with.
   */
  readonly status: number;

  /**
   * Human-readable failure summary.
   */
  readonly message: string;
}> {}

/**
 * A rate-limited upstream call carrying the server's retry hint, which the package copies into
 * `retryAfterSeconds` whatever the error's tag.
 */
export class RateLimitedError extends Data.TaggedError('RateLimitedError')<{
  /**
   * Seconds until the upstream accepts calls again, or `null` without a hint.
   */
  readonly retryAfterSeconds: number | null;

  /**
   * Human-readable failure summary.
   */
  readonly message: string;
}> {}

/**
 * Annotation key a test job uses to tag itself for the settle hook.
 */
export class JobLabel extends Context.Service<JobLabel, string>()('@yieldkit/jobs/test/JobLabel') {}
