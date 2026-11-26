import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

/**
 * How long `npm install` of the tarball may take: it downloads every dependency on a cold cache.
 */
const INSTALL_TIMEOUT_MS = 300_000;

/**
 * How long one type-check or script run in the consumer may take.
 */
const COMMAND_TIMEOUT_MS = 120_000;

/**
 * TypeScript sources other than declarations, which must not ship: consumers load the built modules
 * from `node_modules`, where Node does not strip types.
 */
const TYPESCRIPT_SOURCE_PATTERN = /(?<!\.d)\.(ts|tsx)$/;

/**
 * Platform packages the library never loads: the application's own Cluster brings its HTTP server.
 */
const PLATFORM_PACKAGES = ['@effect/platform-node', '@effect/platform-bun'];

/**
 * Version specifiers pnpm must have resolved before packing.
 */
const UNRESOLVED_VERSION_PATTERN = /^(catalog|workspace|link|file):/;

/**
 * The repository the tarball was packed from.
 */
const root = resolve(import.meta.dirname, '..');

/**
 * The tarball supplied by CI or a local `pnpm pack` command.
 */
const tarball = process.argv[2];
assert.ok(tarball, 'Usage: pnpm test:package /absolute/path/to/jobs.tgz');

/**
 * The manifest of the checkout, which the packed tarball must match.
 */
const sourceManifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));

/**
 * Reads the version of a dependency installed in this checkout, so the consumer runs the same
 * versions CI tested against.
 */
const installedVersion = (name) =>
  JSON.parse(readFileSync(join(root, 'node_modules', name, 'package.json'), 'utf8')).version;

/**
 * An isolated consumer project outside the checkout and its development dependencies, removed when
 * the script exits.
 */
const consumer = mkdtempSync(join(tmpdir(), 'jobs-package-'));
process.on('exit', () => rmSync(consumer, { recursive: true, force: true }));

writeFileSync(
  join(consumer, 'package.json'),
  JSON.stringify({ name: 'consumer', private: true, type: 'module' }, null, 2),
);

/**
 * Installs `specs` into the consumer. Dependencies resolve outside the lockfile here, so install
 * scripts stay off.
 */
const npmInstall = (specs) =>
  execFileSync('npm', ['install', '--no-audit', '--no-fund', '--ignore-scripts', ...specs], {
    cwd: consumer,
    stdio: 'inherit',
    timeout: INSTALL_TIMEOUT_MS,
  });

// Without any platform package: the library must never need one.
npmInstall([
  resolve(tarball),
  `effect@${installedVersion('effect')}`,
  `@effect/sql-pg@${installedVersion('@effect/sql-pg')}`,
  `typescript@${installedVersion('typescript')}`,
  `@types/node@${installedVersion('@types/node')}`,
]);

/**
 * Where npm installed the package under test inside the consumer.
 */
const installed = join(consumer, 'node_modules', sourceManifest.name);

/**
 * The manifest npm installed from the tarball.
 */
const manifest = JSON.parse(readFileSync(join(installed, 'package.json'), 'utf8'));
assert.equal(manifest.version, sourceManifest.version);

for (const section of ['dependencies', 'peerDependencies', 'optionalDependencies']) {
  for (const version of Object.values(manifest[section] ?? {})) {
    assert.ok(
      !UNRESOLVED_VERSION_PATTERN.test(version),
      `pnpm must resolve catalog, workspace, link and file versions before publishing (${version})`,
    );
  }
}

for (const name of PLATFORM_PACKAGES) {
  assert.equal(manifest.peerDependencies?.[name], undefined, `${name} must not be a peer`);
}

/**
 * Every path inside the installed package.
 */
const shipped = readdirSync(installed, { recursive: true }).map(String);

/**
 * Shipped TypeScript sources, which must be empty.
 */
const sources = shipped.filter((name) => TYPESCRIPT_SOURCE_PATTERN.test(name));
assert.deepEqual(sources, [], 'The package must not ship TypeScript sources');
assert.ok(!shipped.some((name) => name.includes('.test.')), 'The package must not ship tests');
assert.ok(
  !shipped.some((name) => name.includes('.fixture.')),
  'The package must not ship fixtures',
);

/**
 * Static and dynamic module specifiers in a built file: `from "x"`, `import "x"` and `import("x")`.
 */
const IMPORT_PATTERN = /(?:\bfrom|\bimport)\s*\(?\s*"([^"]+)"/g;

/**
 * Returns every built module `entry` loads, itself included, by following relative imports, with
 * the bare package specifiers each one imports.
 */
const moduleGraph = (entry) => {
  const modules = new Map();
  const pending = [entry];
  while (pending.length > 0) {
    const file = pending.pop();
    if (!modules.has(file)) {
      const specifiers = [...readFileSync(file, 'utf8').matchAll(IMPORT_PATTERN)].map(
        ([, specifier]) => specifier,
      );
      modules.set(file, specifiers);
      for (const specifier of specifiers) {
        if (specifier.startsWith('.')) {
          pending.push(resolve(dirname(file), specifier));
        }
      }
    }
  }

  return modules;
};

/**
 * Built files reachable from `entry` that import `packageName` or one of its subpaths.
 */
const filesImporting = (entry, packageName) =>
  [...moduleGraph(join(installed, entry))].flatMap(([file, specifiers]) =>
    specifiers.some(
      (specifier) => specifier === packageName || specifier.startsWith(`${packageName}/`),
    )
      ? [file]
      : [],
  );

for (const entry of ['dist/index.js', 'dist/testing/index.js']) {
  for (const name of [...PLATFORM_PACKAGES, '@testcontainers/postgresql', '@yieldkit/pg/testing']) {
    assert.deepEqual(filesImporting(entry, name), [], `${entry} must not load ${name}`);
  }
}

// The package's main promise without a database: a durable two-stage job on the in-memory cluster
// re-runs only its failed stage, `runJob` settles through the memory ledger and calls the settle
// hook, the scheduler refuses a `ShardingConfig` without the jobs' shard groups, and
// `JobStorage.layerMigrations` against a refused port fails without leaking the password. It runs
// before any platform package is installed, so loading one would fail.
writeFileSync(
  join(consumer, 'base.ts'),
  `import { defineJob, durableStage, JobStorage, makeJobWorkflow, makeJobWorkflowLayer, makeSchedulerLayer, runJob, type JobSettled } from '${sourceManifest.name}';
import { makeJobRequestsMemory, makeJobRunsMemory, makeTestClusterLayer } from '${sourceManifest.name}/testing';
import { makePgLayer } from '@yieldkit/pg';
import { Data, Effect, Fiber, Layer, Logger, Redacted, Schema } from 'effect';
import { ShardingConfig } from 'effect/cluster';
import { SecureRandom } from 'effect/crypto';
import { TestClock } from 'effect/testing';
import { createServer } from 'node:net';

class Upstream extends Data.TaggedError('Upstream')<{ readonly message: string }> {}

const Count = Schema.Struct({ count: Schema.Int });
const calls = { fetch: 0, store: 0 };
const job = defineJob({
  name: 'smoke-daily',
  description: 'Two durable stages; the second fails once',
  cadence: { _tag: 'Daily' },
  params: { schema: Schema.Struct({ limit: Schema.Int }), defaults: { limit: 3 } },
  stats: Count,
  run: ({ params }) =>
    Effect.gen(function* () {
      const fetched = yield* durableStage({
        name: 'fetch',
        schema: Count,
        execute: Effect.sync(() => {
          calls.fetch += 1;
          return { count: params.limit };
        }),
      });
      return yield* durableStage({
        name: 'store',
        schema: Count,
        execute: Effect.suspend(() => {
          calls.store += 1;
          return calls.store === 1
            ? Effect.fail(new Upstream({ message: 'store unavailable' }))
            : Effect.succeed(fetched);
        }),
      });
    }),
});

const silent = Logger.layer([]);

const workflowOutcome = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const runs = yield* makeJobRunsMemory();
      const cluster = makeTestClusterLayer([job]);
      const env = Layer.mergeAll(
        cluster,
        runs.layer,
        makeJobWorkflowLayer(job).pipe(Layer.provide(runs.layer), Layer.provide(cluster)),
      );
      return yield* Effect.gen(function* () {
        const fiber = yield* makeJobWorkflow(job)
          .execute({ runKey: '2026-11-25', trigger: 'schedule', params: {}, force: false, requestId: null, attemptNonce: null })
          .pipe(Effect.forkChild);
        for (let round = 0; round < 500 && fiber.pollUnsafe() === undefined; round += 1) {
          yield* Effect.yieldNow;
          yield* TestClock.adjust('1 minute');
        }
        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(env));
    }),
  ).pipe(Effect.provide(Layer.mergeAll(TestClock.layer(), silent))),
);
if (workflowOutcome.status !== 'completed' || calls.fetch !== 1 || calls.store !== 2) {
  throw new Error(\`Unexpected workflow run: \${JSON.stringify({ workflowOutcome, calls })}\`);
}

const events: Array<JobSettled> = [];
const outcome = await Effect.runPromise(
  Effect.gen(function* () {
    const runs = yield* makeJobRunsMemory({
      onSettle: (event) => Effect.sync(() => { events.push(event); }),
    });
    return yield* runJob(job, { trigger: 'manual', params: { limit: 5 } }).pipe(Effect.provide(runs.layer));
  }).pipe(Effect.provide(silent)),
);
if (outcome.status !== 'completed' || events[0]?.trigger !== 'manual') {
  throw new Error(\`Unexpected runJob outcome: \${JSON.stringify({ outcome, events })}\`);
}

const PASSWORD = 'smoke-s3cret';
const port = await new Promise<number>((done) => {
  const server = createServer();
  server.listen(0, '127.0.0.1', () => {
    const address = server.address();
    server.close(() => done(typeof address === 'object' && address !== null ? address.port : 0));
  });
});
const failure = await Effect.runPromise(
  Effect.flip(
    Layer.build(
      JobStorage.layerMigrations().pipe(
        Layer.provide(
          makePgLayer({
            url: Redacted.make(\`postgres://app:\${PASSWORD}@127.0.0.1:\${String(port)}/app\`),
            applicationName: 'smoke',
          }),
        ),
      ),
    ).pipe(Effect.scoped),
  ).pipe(Effect.provide(silent)),
);
if (JSON.stringify(failure).includes(PASSWORD) || String(failure).includes(PASSWORD)) {
  throw new Error(\`JobStorage.layerMigrations must not leak the password: \${String(failure)}\`);
}

// Effect's default ShardingConfig has only the \`default\` group: the jobs would never run on it.
const misconfigured = await Effect.runPromise(
  Effect.gen(function* () {
    const runs = yield* makeJobRunsMemory();
    const requests = yield* makeJobRequestsMemory();
    return yield* Effect.flip(
      Layer.build(
        makeSchedulerLayer([job]).pipe(
          Layer.provide(ShardingConfig.layer({})),
          Layer.provide([makeTestClusterLayer([job]), runs.layer, requests.layer, SecureRandom.layerWebCrypto]),
        ),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(silent)),
);
if (misconfigured._tag !== 'ClusterConfigError') {
  throw new Error(\`makeSchedulerLayer must refuse a ShardingConfig without the jobs' groups: \${String(misconfigured)}\`);
}
`,
);

/**
 * The first `ts` code block after the README's "Quick start" heading.
 */
const QUICK_START_PATTERN = /## Quick start[\s\S]*?```ts\n([\s\S]*?)```/;

/**
 * The README quick start, which must type-check against the built declarations. It needs
 * PostgreSQL, so it is not run.
 */
const quickStart = QUICK_START_PATTERN.exec(readFileSync(join(root, 'README.md'), 'utf8'));
assert.ok(quickStart, 'README.md must have a Quick start with a ts code block');
writeFileSync(join(consumer, 'quick-start.ts'), `${quickStart[1]}\nexport {};\n`);

/**
 * Process options shared by the consumer's type-check and runs.
 */
const options = {
  /**
   * Run inside the consumer project, so the installed tarball resolves.
   */
  cwd: consumer,

  /**
   * Show compiler and runtime output in the CI log.
   */
  stdio: 'inherit',

  /**
   * Fail instead of hanging on a stuck process.
   */
  timeout: COMMAND_TIMEOUT_MS,
};

/**
 * Whether Bun is on the `PATH`. CI installs it; a local run without it skips the Bun runs.
 */
const hasBun = spawnSync('bun', ['--version'], { stdio: 'ignore' }).status === 0;

// Node strips the types of a file outside `node_modules`, so the fixtures run as written.
execFileSync(process.execPath, ['base.ts'], options);
if (hasBun) {
  execFileSync('bun', ['base.ts'], options);
}

// The README quick start builds the application's own Cluster with Node's HTTP server.
npmInstall([`@effect/platform-node@${installedVersion('@effect/platform-node')}`]);

execFileSync(
  join(consumer, 'node_modules/.bin/tsc'),
  [
    '--ignoreConfig',
    '--noEmit',
    '--strict',
    '--exactOptionalPropertyTypes',
    '--skipLibCheck',
    '--target',
    'ES2025',
    '--module',
    'nodenext',
    '--moduleResolution',
    'nodenext',
    '--types',
    'node',
    'base.ts',
    'quick-start.ts',
  ],
  options,
);

if (!hasBun) {
  console.log('Bun is not on the PATH; skipped the Bun run.');
}

console.log(`Package smoke test passed for ${manifest.name}@${manifest.version}.`);
