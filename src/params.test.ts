import { Effect, Schema } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import { InvalidJobParams } from './errors.ts';
import { decodeParams, parseSetFlags } from './params.ts';

const SampleParams = Schema.Struct({
  concurrency: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 8 })),
  maxItems: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 5000 })),
  note: Schema.NonEmptyString,
});

const TransformedParams = Schema.Struct({
  limit: Schema.FiniteFromString,
  label: Schema.String,
});

describe('decodeParams', () => {
  it('merges overrides over defaults, keeps defaults for absent keys, and returns decoded and encoded values', async () => {
    const result = await Effect.runPromise(
      decodeParams(
        'sync-daily',
        {
          schema: SampleParams,
          defaults: {
            concurrency: 4,
            maxItems: 60,
            note: 'default-note',
          },
        },
        {
          maxItems: 5,
        },
      ),
    );
    expect(result).toEqual({
      params: {
        concurrency: 4,
        maxItems: 5,
        note: 'default-note',
      },
      encoded: {
        concurrency: 4,
        maxItems: 5,
        note: 'default-note',
      },
    });
  });

  it('encodes transformed params back to their JSON-safe representation', async () => {
    const result = await Effect.runPromise(
      decodeParams(
        'transformed-job',
        {
          schema: TransformedParams,
          defaults: { limit: '10', label: 'initial' },
        },
        { label: 'updated' },
      ),
    );
    expect(result).toEqual({
      params: { limit: 10, label: 'updated' },
      encoded: { limit: '10', label: 'updated' },
    });
  });

  it('fails with InvalidJobParams when a required param is missing', async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        decodeParams('manual-import', {
          schema: Schema.Struct({ file: Schema.NonEmptyString }),
          defaults: {},
        }),
      ),
    );
    expect(error).toBeInstanceOf(InvalidJobParams);
    expect(error._tag).toBe('InvalidJobParams');
    expect(error.job).toBe('manual-import');
    expect(error.message.length).toBeGreaterThan(0);
  });

  it('fails with InvalidJobParams when a param is out of bounds or unknown', async () => {
    const outOfBounds = await Effect.runPromise(
      Effect.flip(
        decodeParams(
          'sync-daily',
          {
            schema: SampleParams,
            defaults: {
              concurrency: 4,
              maxItems: 60,
              note: 'default-note',
            },
          },
          { concurrency: 99 },
        ),
      ),
    );
    expect(outOfBounds).toBeInstanceOf(InvalidJobParams);
    expect(outOfBounds.job).toBe('sync-daily');

    const unknownKey = await Effect.runPromise(
      Effect.flip(
        decodeParams(
          'sync-daily',
          {
            schema: SampleParams,
            defaults: {
              concurrency: 4,
              maxItems: 60,
              note: 'default-note',
            },
          },
          { unknownKnob: 1 },
        ),
      ),
    );
    expect(unknownKey).toBeInstanceOf(InvalidJobParams);
    expect(unknownKey.job).toBe('sync-daily');
  });
});

describe('parseSetFlags', () => {
  it('parses JSON literals and falls back to raw strings', () => {
    const parsed = Effect.runSync(
      parseSetFlags([
        'maxItems=5',
        'keepCache=true',
        'tags=["a"]',
        'empty=null',
        'opts={"nested":1}',
        'file=/data/import.db',
      ]),
    );
    expect(parsed).toEqual({
      maxItems: 5,
      keepCache: true,
      tags: ['a'],
      empty: null,
      opts: { nested: 1 },
      file: '/data/import.db',
    });
  });

  it('preserves equals signs inside values and applies later flags over earlier ones', () => {
    const parsed = Effect.runSync(
      parseSetFlags([
        'url=https://example.com?a=1&b=2',
        'token=abc=def==',
        'empty=',
        'maxItems=10',
        'maxItems=20',
      ]),
    );
    expect(parsed).toEqual({
      url: 'https://example.com?a=1&b=2',
      token: 'abc=def==',
      empty: '',
      maxItems: 20,
    });
  });

  it('rejects flags missing an equals sign or having an empty key with InvalidJobParams', () => {
    for (const invalid of ['maxItems', '', '=value', '=']) {
      const error = Effect.runSync(Effect.flip(parseSetFlags(['ok=1', invalid])));
      expect(error).toBeInstanceOf(InvalidJobParams);
      expect(error.job).toBe('--set');
      expect(error.message).toBe(
        `Invalid --set value "${invalid}". Expected key=value with a non-empty key.`,
      );
    }
  });
});
