# Contributing

Thanks for helping improve `@yieldkit/jobs`.

## Setup

Use Node.js 22.18+, 24.11+ or 26+ and the pnpm version declared in `package.json` (`devEngines`).

```bash
pnpm install
pnpm check
pnpm test
```

`pnpm check` formats, lints and type-checks through `effect-tsgo`, which adds Effect's type-aware rules to `vp check`. The lint preset is [`@yieldkit/oxlint-config`](https://github.com/yieldkit/oxlint-config).

## Tests

- `pnpm test` runs the unit tests (`src/**/*.test.ts`). They need no database and no Docker: workflows run on the in-memory cluster and SQL is checked against a recording client.
- `pnpm test:integration` runs `src/**/*.integration.test.ts` against PostgreSQL started with Testcontainers, one file at a time. It needs a running Docker daemon. It uses `@yieldkit/pg`'s `POSTGRES_TEST_IMAGE` (PostgreSQL 19); set `PG_TEST_IMAGE` (for example `postgres:15-trixie`) to run it against another release, as CI does for PostgreSQL 15.

CI runs the unit tests on every Node version and operating system in the matrix, the integration tests on Ubuntu only, since macOS runners have no Docker, and the installed-package test once more on Bun.

## Layout

- `src/index.ts`: the public API. Anything not exported here or from `src/testing/index.ts` is internal. No module may import a platform package: the application's Cluster brings its own.
- `src/cluster.ts`: the shard-group contract with the application's Cluster and its start-up checks.
- `src/policy.ts`: every default timing and limit; `src/storage-options.ts`: table and schema names.
- `src/migrations.ts`: the library's migration steps. New steps get the next id and must be idempotent; a step that rewrites a large table belongs in its own deploy step, since `Migrator` runs all pending steps in one transaction.
- `src/wire-format.test.ts`: pins every name and shape persisted in Cluster's tables. If it fails, you are about to orphan in-flight workflows.
- `docs/`: one page per topic, linked from `README.md`.

## Before opening a pull request

```bash
pnpm check
pnpm test
pnpm test:integration
pnpm pack --out /tmp/jobs.tgz
pnpm test:package /tmp/jobs.tgz
```

`test:package` installs the packed tarball into a temporary project, checks that the root and `testing` entry points load no platform package, runs a durable workflow, `runJob`, a scheduler on a `ShardingConfig` without the jobs' groups and a failing `JobStorage.layerMigrations` without a database (also on Bun when it is on `PATH`), and type-checks the README quick start.

## Commits and releases

Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`).

To release, bump `version` in `package.json` on `main`, wait for CI to pass, then publish a GitHub release tagged `v<version>`. The publish workflow checks the tag matches `package.json` and CI passed for that commit, runs the unit and integration tests, then publishes to npm with trusted publishing and provenance.
