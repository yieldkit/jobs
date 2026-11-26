import { Effect, Option, Schema } from 'effect';

import type { AnyJobDefinition, JobApiParamsPolicy } from './job.ts';
import type { WorkflowParamOverrides } from './workflow-schema.ts';

import { InvalidJobParams } from './errors.ts';
import { JOB_CONTINUATION } from './job.ts';

/**
 * Decoded runtime job parameters paired with their JSON-safe encoded representation for storage in
 * `job_runs.params`.
 */
export type DecodedJobParams<Params, ParamsEncoded> = {
  /**
   * Validated runtime parameters passed to `JobDefinition.run`.
   */
  readonly params: Params;

  /**
   * Encoded JSON-safe representation stored on the `job_runs` row.
   */
  readonly encoded: ParamsEncoded;
};

/**
 * JSON-value decoder used for CLI `--set key=value` flag parsing.
 */
const decodeJsonValue = Schema.decodeOption(Schema.fromJsonString(Schema.Json));

/**
 * Strict decode options reporting every schema issue and rejecting unknown override keys.
 */
const PARAMS_PARSE_OPTIONS = {
  /**
   * Collect all schema validation errors instead of stopping at the first issue.
   */
  errors: 'all',

  /**
   * Reject unrecognized override keys so flag typos fail loudly before claiming.
   */
  onExcessProperty: 'error',
} as const;

/**
 * Merges `spec.defaults` with `overrides` (overrides win per key), validates through `spec.schema`,
 * and encodes the validated parameters back into `ParamsEncoded` for ledger storage. Maps schema
 * issues into `InvalidJobParams`.
 */
export const decodeParams = <Params, ParamsEncoded>(
  job: string,
  spec: {
    /**
     * Parameter codec used for both decoding input overrides and encoding for persistence.
     */
    readonly schema: Schema.Codec<Params, ParamsEncoded>;

    /**
     * Encoded default parameter values applied underneath `overrides`.
     */
    readonly defaults: Partial<ParamsEncoded>;
  },
  overrides?: WorkflowParamOverrides,
): Effect.Effect<DecodedJobParams<Params, ParamsEncoded>, InvalidJobParams> =>
  Effect.gen(function* () {
    const mergedInput = { ...spec.defaults, ...overrides };
    const params = yield* Schema.decodeUnknownEffect(
      spec.schema,
      PARAMS_PARSE_OPTIONS,
    )(mergedInput);
    const encoded = yield* Schema.encodeUnknownEffect(spec.schema)(params);
    return { params, encoded };
  }).pipe(Effect.mapError((error) => new InvalidJobParams({ job, message: error.message })));

/**
 * Validates and encodes parameter overrides for any `JobDefinition` (including unions drawn from
 * `ALL_JOBS`) without executing the job, using its rank-2 `JOB_CONTINUATION` when present.
 */
export const decodeJobParams = (
  definition: AnyJobDefinition,
  overrides?: WorkflowParamOverrides,
): Effect.Effect<DecodedJobParams<unknown, unknown>, InvalidJobParams> => {
  const continuation = definition[JOB_CONTINUATION];
  if (continuation !== undefined) {
    return continuation((concrete) => decodeParams(concrete.name, concrete.params, overrides));
  }

  return decodeParams(definition.name, definition.params, overrides);
};

/**
 * Lists the keys of `overrides` that `definition.params.apiParams` does not allow an API caller to
 * set, in their original order. Every key counts, whatever its value. Returns an empty array when
 * `overrides` is missing or sets only allowed keys.
 */
export const findDisallowedApiParams = (
  definition: JobApiParamsPolicy,
  overrides: WorkflowParamOverrides | undefined,
): ReadonlyArray<string> => {
  const allowed = new Set(definition.params.apiParams ?? []);
  return Object.keys(overrides ?? {}).filter((key) => !allowed.has(key));
};

/**
 * Narrows an override value to a JSON value; `undefined`, functions, and other non-JSON values
 * fail.
 */
const isJsonValue = Schema.is(Schema.Json);

/**
 * Returns a copy of `params` holding only the keys `definition.params.apiParams` allows an API
 * caller to set, so every other param comes from the worker. Entries whose value is not JSON are
 * dropped too, since overrides travel through the JSON workflow payload. Returns an empty record
 * when `params` is not an object.
 */
export const pickApiParams = (
  definition: JobApiParamsPolicy,
  params: unknown,
): Schema.JsonObject => {
  if (typeof params !== 'object' || params === null || Array.isArray(params)) {
    return {};
  }

  const allowed = new Set(definition.params.apiParams ?? []);
  return Object.fromEntries(
    Object.entries(params).flatMap(([key, value]): Array<[string, Schema.Json]> =>
      allowed.has(key) && isJsonValue(value) ? [[key, value]] : [],
    ),
  );
};

/**
 * Parses one `--set key=value` flag into a key and value. The value is decoded via
 * `Schema.fromJsonString(Schema.Json)`, falling back to the raw string when JSON decoding fails.
 * Fails with `InvalidJobParams` when the flag lacks `=` or has an empty key.
 */
const parseSetFlag = (
  flag: string,
): Effect.Effect<readonly [string, Schema.Json], InvalidJobParams> => {
  const equalsIndex = flag.indexOf('=');
  if (equalsIndex <= 0) {
    return Effect.fail(
      new InvalidJobParams({
        job: '--set',
        message: `Invalid --set value "${flag}". Expected key=value with a non-empty key.`,
      }),
    );
  }

  const rawValue = flag.slice(equalsIndex + 1);
  const parsed = decodeJsonValue(rawValue);
  return Effect.succeed([flag.slice(0, equalsIndex), Option.getOrElse(parsed, () => rawValue)]);
};

/**
 * Parses repeated `--set key=value` CLI flags into an override record, later flags overriding
 * earlier ones for the same key. Each value is decoded via `Schema.fromJsonString(Schema.Json)`,
 * falling back to the raw string when JSON decoding fails. Fails with `InvalidJobParams` (job
 * `--set`) on the first entry lacking `=` or using an empty key.
 */
export const parseSetFlags = (
  flags: ReadonlyArray<string>,
): Effect.Effect<Schema.JsonObject, InvalidJobParams> =>
  Effect.forEach(flags, parseSetFlag).pipe(Effect.map((entries) => Object.fromEntries(entries)));
