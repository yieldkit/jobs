import { fmtConfig } from '@yieldkit/oxlint-config';
import oxlintConfig from '@yieldkit/oxlint-config/effect';
import { defineConfig } from 'vite-plus';

/**
 * Paths neither the formatter nor the linter touches: build output, packed tarballs and coverage.
 */
const ignoredPatterns = ['coverage/**', '*.tgz', 'pnpm-lock.yaml'];

/**
 * Tests that start PostgreSQL in Docker through Testcontainers.
 */
const integrationTests = 'src/**/*.integration.test.ts';

/**
 * Per-test and per-hook limit for the integration suite: a cold run pulls the image first.
 */
const INTEGRATION_TIMEOUT_MS = 300_000;

/**
 * Per-test limit for the unit suite. The tests drive the in-memory Cluster on `TestClock`, which
 * waits in real time for each woken workflow to suspend (see `src/test-runtime.fixture.ts` and
 * `src/digest.setup.ts`); with every test file running in parallel on a small CI runner, a test can
 * take well over the default 5 seconds without anything being wrong.
 */
const UNIT_TIMEOUT_MS = 60_000;

export default defineConfig({
  pack: {
    entry: ['src/index.ts', 'src/testing/index.ts'],
    platform: 'neutral',
    format: ['esm'],
    dts: true,
    unbundle: true,
    fixedExtension: false,
  },
  staged: {
    '*': 'effect-tsgo vp check --fix',
  },
  fmt: {
    ...fmtConfig,
    ignorePatterns: [...fmtConfig.ignorePatterns, ...ignoredPatterns],
  },
  lint: {
    ...oxlintConfig,
    ignorePatterns: [...oxlintConfig.ignorePatterns, ...ignoredPatterns],
    rules: {
      ...oxlintConfig.rules,
      // Cluster, Workflow, `SqlClient` and `Migrator` live in Effect's unstable modules; this
      // package builds on them by design, so its `effect` and `@effect/sql-pg` peer ranges
      // (`~4.1.7`) only admit patch releases of the minor version it is tested against.
      'effecttsgo/unstable-api-usage': 'off',
    },
    overrides: [
      ...oxlintConfig.overrides,
      {
        // Tests and fixtures run effects directly.
        files: ['src/**/*.test.ts', 'src/**/*.fixture.ts'],
        rules: {
          'slop/no-effect-run-outside-entry': 'off',
        },
      },
      {
        // Fixtures build the layers a test provides, like the tests themselves.
        files: ['src/**/*.fixture.ts'],
        rules: {
          'effecttsgo/strict-effect-provide': 'off',
        },
      },
      {
        files: ['src/index.ts', 'src/testing/index.ts'],
        rules: {
          'oxc/no-barrel-file': 'off',
        },
      },
      {
        files: ['scripts/**/*.mjs'],
        rules: {
          'no-console': 'off',
          // Plain Node scripts without Effect: JSON comes from files this repository controls.
          'slop/no-raw-json-parse': 'off',
        },
      },
    ],
  },
  test: {
    projects: [
      {
        extends: true,
        test: {
          name: 'unit',
          include: ['src/**/*.test.ts'],
          exclude: [integrationTests],
          setupFiles: ['src/digest.setup.ts'],
          testTimeout: UNIT_TIMEOUT_MS,
        },
      },
      {
        extends: true,
        test: {
          name: 'integration',
          include: [integrationTests],
          testTimeout: INTEGRATION_TIMEOUT_MS,
          hookTimeout: INTEGRATION_TIMEOUT_MS,
          // Each file starts its own container; one at a time keeps lease and lock timings
          // stable on small CI runners.
          fileParallelism: false,
        },
      },
    ],
  },
  run: {
    cache: { scripts: false, tasks: true },
  },
});
