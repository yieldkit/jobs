import { Effect, Schema, SchemaIssue, SchemaTransformation } from 'effect';

import type { JobRunRecord, JobRunsPage } from './ledger-types.ts';

import { InvalidRunCursor } from './errors.ts';

/**
 * Start of PostgreSQL's timestamp epoch, in Unix milliseconds for JavaScript dates.
 */
const POSTGRES_EPOCH_MS = Date.UTC(2000, 0, 1);

/**
 * PostgreSQL stores timestamps in microseconds; JavaScript clocks have millisecond precision.
 */
const MICROS_PER_MILLISECOND = 1000n;

/**
 * Microseconds per 24-hour UTC day, used to reconstruct a PostgreSQL timestamp exactly.
 */
export const MICROS_PER_DAY = 86_400_000_000n;

/**
 * PostgreSQL's first finite timestamp, in microseconds since its epoch.
 */
const MIN_TIMESTAMP_MICROS = -211_813_488_000_000_000n;

/**
 * PostgreSQL's last finite timestamp, in microseconds since its epoch.
 *
 * @see https://doxygen.postgresql.org/datatype_2timestamp_8h_source.html
 */
const MAX_TIMESTAMP_MICROS = 9_223_371_331_199_999_999n;

/**
 * Unpadded URL-safe base64 alphabet used by cursor tokens.
 */
const BASE64URL_PATTERN = /^[A-Za-z0-9_-]+$/u;

/**
 * Strict UTF-8 decoder rejecting invalid byte sequences instead of replacing them with U+FFFD.
 */
const utf8Decoder = new TextDecoder('utf-8', { fatal: true });

/**
 * UTF-8 encoder paired with `utf8Decoder` for cursor round-trips.
 */
const utf8Encoder = new TextEncoder();

/**
 * Whether `value` is valid Unicode text that PostgreSQL stores without zero-byte errors or
 * replacement characters.
 */
export const isPostgresText = (value: string): boolean =>
  value.isWellFormed() && !value.includes('\0');

/**
 * Unicode text that can be sent losslessly as a PostgreSQL key.
 */
const CursorKey = Schema.String.check(
  Schema.makeFilter(isPostgresText, {
    description: 'a well-formed key without zero bytes',
  }),
);

/**
 * Base64url payload decoded strictly as UTF-8 text.
 */
const StrictUtf8FromBase64Url = Schema.Uint8ArrayFromBase64Url.pipe(
  Schema.decodeTo(
    Schema.String,
    SchemaTransformation.transformEffect({
      decode: (bytes, options) =>
        Effect.try({
          try: () => utf8Decoder.decode(bytes),
          catch: () => new SchemaIssue.InvalidValue({ message: 'Invalid UTF-8' }, bytes, options),
        }),
      encode: (text) => Effect.succeed(utf8Encoder.encode(text)),
    }),
  ),
);

/**
 * Finite PostgreSQL timestamp in microseconds since 2000-01-01 UTC, encoded as decimal text.
 */
export const RunTimestampMicros = Schema.BigIntFromString.check(
  Schema.isGreaterThanOrEqualToBigInt(MIN_TIMESTAMP_MICROS),
  Schema.isLessThanOrEqualToBigInt(MAX_TIMESTAMP_MICROS),
);

/**
 * Base64url JSON carrying all columns in the history sort order.
 */
const RunPageCursor = Schema.String.check(Schema.isPattern(BASE64URL_PATTERN)).pipe(
  Schema.decodeTo(StrictUtf8FromBase64Url),
  Schema.decodeTo(
    Schema.fromJsonString(
      Schema.Struct({
        /**
         * Full precision of the boundary row's start time.
         */
        startedAtMicros: RunTimestampMicros,

        /**
         * Run key of the boundary row.
         */
        runKey: CursorKey,

        /**
         * Job name of the boundary row.
         */
        job: CursorKey,
      }),
    ),
  ),
);

/**
 * Decoded position of one row in the history sort order.
 */
export type RunPagePosition = typeof RunPageCursor.Type;

/**
 * A run with the exact timestamp used to order it, which may be more precise than `run.startedAt`.
 */
type PositionedRun = {
  /**
   * The public record to include in the page.
   */
  readonly run: JobRunRecord;

  /**
   * Full precision of the start time, in microseconds since the PostgreSQL epoch.
   */
  readonly startedAtMicros: bigint;
};

/**
 * Decodes a continuation token, or starts at the newest run when the token is absent.
 */
export const decodeRunPageCursor = (
  cursor: string | undefined,
): Effect.Effect<RunPagePosition | undefined, InvalidRunCursor> => {
  if (cursor === undefined) {
    return Effect.undefined;
  }

  return Schema.decodeEffect(RunPageCursor)(cursor).pipe(
    Effect.mapError(() => new InvalidRunCursor({ message: 'Invalid run history cursor' })),
  );
};

/**
 * Truncates finite limits to whole rows and reserves room for one lookahead row. Negative and
 * non-finite limits return zero.
 */
export const pageLimit = (limit: number): number =>
  Number.isFinite(limit)
    ? Math.min(Number.MAX_SAFE_INTEGER - 1, Math.max(0, Math.trunc(limit)))
    : 0;

/**
 * Returns at most `limit` records, encoding the last returned row when a lookahead row exists.
 */
export const makeRunsPage = (rows: ReadonlyArray<PositionedRun>, limit: number): JobRunsPage => {
  const runs = rows.slice(0, limit).map(({ run }) => run);
  const last = rows[limit - 1];
  if (rows.length <= limit || last === undefined) {
    return { runs, nextCursor: null };
  }

  const { job, runKey } = last.run;
  const nextCursor = Schema.encodeSync(RunPageCursor)({
    job,
    runKey,
    startedAtMicros: last.startedAtMicros,
  });
  return { runs, nextCursor };
};

/**
 * Converts the memory clock's millisecond timestamp into the shared paging representation.
 */
export const runTimestampMicros = (startedAt: Date): bigint =>
  (BigInt(startedAt.getTime()) - BigInt(POSTGRES_EPOCH_MS)) * MICROS_PER_MILLISECOND;
