import type { PostgresContainer } from '@yieldkit/pg/testing';
import type { Config, Redacted } from 'effect';

import * as BunClusterHttp from '@effect/platform-bun/BunClusterHttp';
import * as NodeClusterHttp from '@effect/platform-node/NodeClusterHttp';
import { makePgLayer } from '@yieldkit/pg';
import { Context, Effect, Exit, Layer, Option, Schedule, Schema, Scope } from 'effect';
import {
  ClusterWorkflowEngine,
  RunnerAddress,
  ShardingConfig,
  SqlMessageStorage,
  SqlRunnerStorage,
} from 'effect/cluster';
import { Digest, SecureRandom } from 'effect/crypto';
import { SqlClient } from 'effect/sql';
import { createServer } from 'node:net';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { AnyJobDefinition, JobTrigger } from './job.ts';

import { durableStage } from './activity.ts';
import { assignedShardGroupsFor, availableShardGroupsFor, checkShardingConfig } from './cluster.ts';
import {
  createDatabase,
  quietLogger,
  runSql,
  startIntegrationPostgres,
} from './integration.fixture.ts';
import { defineJob } from './job.ts';
import { JobRequests } from './requests.ts';
import { makeSchedulerLayer } from './scheduler.ts';
import { layerMigrations, layerStorage } from './storage.ts';
import { dispatchApiJobWorkflow, makeJobWorkflow } from './workflow.ts';

/**
 * The application's schema: it holds the application's Cluster tables and the library's, and is the
 * `search_path` of every pool below.
 */
const APP_SCHEMA = 'app';

/**
 * The application's Cluster table prefix, unlike Effect's and the library's defaults.
 */
const CLUSTER_PREFIX = 'platform';

/**
 * A shard group of the application's own entities, next to the jobs' groups.
 */
const APP_GROUP = 'billing-entities';

/**
 * Shards per group of the application's Cluster, unlike the library's test default.
 */
const SHARDS_PER_GROUP = 64;

/**
 * Effect's Cluster HTTP layer of the runtime the suite runs on, as an application picks its
 * platform package: Node's HTTP client (undici) does not run on Bun.
 */
const clusterHttpLayer: typeof NodeClusterHttp.layer =
  process.versions.bun === undefined ? NodeClusterHttp.layer : BunClusterHttp.layer;

/**
 * The application's pool: `search_path` is its schema, so Cluster and `JobStorage` both resolve
 * unqualified tables there. `makePgLive`'s default size, as in the README.
 */
const applicationPool = (url: Redacted.Redacted, applicationName: string) =>
  makePgLayer({ url, applicationName, maxConnections: 10, searchPath: APP_SCHEMA });

/**
 * The type of `applicationPool`'s layer.
 */
type ApplicationPool = ReturnType<typeof applicationPool>;

/**
 * The application's own deploy step for its Cluster: Effect's migrations with its prefix.
 */
const migrateApplicationCluster = (url: Redacted.Redacted): Promise<void> =>
  Effect.runPromise(
    Layer.build(
      Layer.mergeAll(
        SqlMessageStorage.layerMigrations({ prefix: CLUSTER_PREFIX }),
        SqlRunnerStorage.layerMigrations({ prefix: CLUSTER_PREFIX }),
      ).pipe(Layer.provide(applicationPool(url, 'app-deploy'))),
    ).pipe(Effect.scoped, Effect.asVoid, Effect.provide(quietLogger)),
  );

/**
 * The application's Cluster storage: DML only, on its pool.
 */
const applicationClusterStorage = (
  pool: ApplicationPool,
  shardingConfig: Layer.Layer<ShardingConfig.ShardingConfig, Config.ConfigError>,
) =>
  Layer.mergeAll(
    SqlMessageStorage.layerStorage({ prefix: CLUSTER_PREFIX }).pipe(
      Layer.provide(Digest.layerWebCrypto),
    ),
    SqlRunnerStorage.layerStorage({ prefix: CLUSTER_PREFIX }),
  ).pipe(Layer.provide([pool, shardingConfig]));

/**
 * The application's worker Cluster, composed as the README's quick start does: Effect's Cluster
 * HTTP layer with its own storage, its own groups plus the jobs', its own shard count, and the same
 * `ShardingConfig` options exposed for `makeSchedulerLayer`.
 */
const applicationWorkerCluster = (
  pool: ApplicationPool,
  port: number,
  jobs: ReadonlyArray<AnyJobDefinition>,
  only?: ReadonlyArray<string>,
) => {
  const shardingConfig = {
    runnerAddress: Option.some(RunnerAddress.make('127.0.0.1', port)),
    availableShardGroups: [APP_GROUP, ...availableShardGroupsFor(jobs)],
    assignedShardGroups: [APP_GROUP, ...assignedShardGroupsFor(jobs, only)],
    shardsPerGroup: SHARDS_PER_GROUP,
  };
  const shardingConfigLive = ShardingConfig.layerFromEnv(shardingConfig);
  return ClusterWorkflowEngine.layer.pipe(
    Layer.provideMerge(clusterHttpLayer({ transport: 'http', storage: 'byo', shardingConfig })),
    Layer.provideMerge(applicationClusterStorage(pool, shardingConfigLive)),
    Layer.provideMerge(shardingConfigLive),
  );
};

/**
 * The application's API Cluster, composed as the README's API example does: client only, no runner
 * address, no shards, same groups and shard count.
 */
const applicationClientCluster = (pool: ApplicationPool, jobs: ReadonlyArray<AnyJobDefinition>) => {
  const shardingConfig = {
    runnerAddress: Option.none(),
    availableShardGroups: [APP_GROUP, ...availableShardGroupsFor(jobs)],
    assignedShardGroups: [],
    shardsPerGroup: SHARDS_PER_GROUP,
    shardLockDisableAdvisory: true,
  };
  const shardingConfigLive = ShardingConfig.layerFromEnv(shardingConfig);
  return Layer.mergeAll(
    ClusterWorkflowEngine.layer.pipe(
      Layer.provideMerge(
        clusterHttpLayer({
          transport: 'http',
          clientOnly: true,
          storage: 'byo',
          shardingConfig,
        }),
      ),
      Layer.provideMerge(applicationClusterStorage(pool, shardingConfigLive)),
    ),
    Layer.effectDiscard(checkShardingConfig(jobs)).pipe(Layer.provide(shardingConfigLive)),
  );
};

/**
 * Picks a free loopback port for the worker's HTTP runner.
 */
const allocateLoopbackPort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address !== null ? address.port : 0;
      server.close(() => resolve(port));
    });
  });

/**
 * Rows counted by the polling queries below.
 */
const CountRow = Schema.Struct({ count: Schema.Int });

let postgres: PostgresContainer;

/**
 * Steps 1, 2 and 4 on database `url`: the application's schema and Cluster migrations, then the
 * library's tables on the same `search_path`.
 */
const prepareApplicationDatabase = async (url: Redacted.Redacted): Promise<void> => {
  await runSql(
    url,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE SCHEMA ${sql(APP_SCHEMA)}`;
    }),
  );
  await migrateApplicationCluster(url);
  await Effect.runPromise(
    Layer.build(layerMigrations().pipe(Layer.provide(applicationPool(url, 'app-deploy')))).pipe(
      Effect.scoped,
      Effect.provide(quietLogger),
    ),
  );
};

beforeAll(async () => {
  postgres = await startIntegrationPostgres();
  await prepareApplicationDatabase(postgres.connectionUri);
});

afterAll(async () => {
  await postgres.stop();
});

describe("Jobs on the application's own Cluster", () => {
  it('runs a multi-stage workflow that a client dispatched on a separate worker', async () => {
    const stepLog: Array<string> = [];
    const e2eJob = defineJob({
      name: 'e2e-durable-job',
      description: 'End-to-end durable workflow',
      cadence: { _tag: 'Daily' },
      params: {
        schema: Schema.Struct({ factor: Schema.Int }),
        defaults: { factor: 2 },
        apiParams: ['factor'],
      },
      stats: Schema.Struct({ stageOne: Schema.Int, stageTwo: Schema.Int }),
      run: ({ params }) =>
        Effect.gen(function* () {
          const first = yield* durableStage({
            name: 'stage-one',
            schema: Schema.Struct({ value: Schema.Int }),
            execute: Effect.sync(() => {
              stepLog.push('stage-one');
              return { value: 5 * params.factor };
            }),
          });
          const second = yield* durableStage({
            name: 'stage-two',
            schema: Schema.Struct({ value: Schema.Int }),
            execute: Effect.sync(() => {
              stepLog.push('stage-two');
              return { value: first.value + 3 };
            }),
          });
          return { stageOne: first.value, stageTwo: second.value };
        }),
    });

    const url = postgres.connectionUri;
    const port = await allocateLoopbackPort();
    // 3. The jobs engine on the application's Cluster.
    // One pool per process, shared by the Cluster's storage and the library's.
    const workerPool = applicationPool(url, 'e2e-worker');
    const apiPool = applicationPool(url, 'e2e-api');
    const workerRuntimeLayer = makeSchedulerLayer([e2eJob]).pipe(
      Layer.provide([
        applicationWorkerCluster(workerPool, port, [e2eJob]),
        layerStorage().pipe(Layer.provide(workerPool)),
        SecureRandom.layerWebCrypto,
      ]),
    );
    const clientEnv = Layer.mergeAll(
      applicationClientCluster(apiPool, [e2eJob]),
      layerStorage().pipe(Layer.provide(apiPool)),
      apiPool,
      SecureRandom.layerWebCrypto,
    );

    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* Layer.build(workerRuntimeLayer);
          return yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            // Let the start-up Singleton finish today's scheduled run before the forced API run, so
            // the two runs are ordered.
            for (let i = 0; i < 100; i += 1) {
              const rows = yield* sql`
                SELECT count(*)::int AS count FROM job_runs
                WHERE job = ${e2eJob.name} AND status = 'completed'
              `;
              const [completed] = yield* Schema.decodeUnknownEffect(Schema.Array(CountRow))(rows);
              if ((completed?.count ?? 0) > 0) {
                break;
              }

              yield* Effect.sleep('100 millis');
            }

            const dispatched = yield* dispatchApiJobWorkflow(e2eJob, {
              params: { factor: 3 },
              force: true,
            });
            const workflow = makeJobWorkflow(e2eJob);
            const requests = yield* JobRequests;
            let completed = false;
            for (let i = 0; i < 100 && !completed; i += 1) {
              const polled = yield* workflow.poll(dispatched.executionId);
              completed = Option.isSome(polled) && polled.value._tag === 'Complete';
              if (!completed) {
                yield* Effect.sleep('200 millis');
              }
            }

            const status = yield* requests.getStatus(e2eJob.name, dispatched.requestId);
            const messages = yield* sql`
              SELECT count(*)::int AS count FROM platform_messages
              WHERE entity_type = ${`Workflow/${e2eJob.name}`} AND entity_id = ${dispatched.executionId}
            `;
            const [messageCount] = yield* Schema.decodeUnknownEffect(Schema.Array(CountRow))(
              messages,
            );
            return { dispatched, completed, status, messageCount: messageCount?.count ?? 0 };
          }).pipe(Effect.provide(clientEnv));
        }),
      ).pipe(Effect.provide(quietLogger)),
    );
    expect(result.completed).toBe(true);
    expect(stepLog).toContain('stage-one');
    expect(stepLog).toContain('stage-two');
    expect(result.messageCount).toBeGreaterThanOrEqual(1);
    expect(result.status?.request.finishedAt).toBeInstanceOf(Date);
    expect(result.status?.request.executionId).toBe(result.dispatched.executionId);
    expect(result.status?.run).toMatchObject({
      job: e2eJob.name,
      status: 'completed',
      trigger: 'api',
      stats: { stageOne: 15, stageTwo: 18 },
    });
  }, 120_000);
});

/**
 * Which worker a job body ran on: each worker's scheduler is provided its own name.
 */
class WorkerName extends Context.Service<WorkerName, string>()('test/WorkerName') {}

/**
 * Advisory locks held per pool, by `application_name`: Cluster's shard locks on PostgreSQL.
 */
const LockRow = Schema.Struct({ pool: Schema.String, locks: Schema.Int });

/**
 * Shard locks held by each worker pool.
 */
const shardLocksByPool = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`
    SELECT a.application_name AS pool, count(*)::int AS locks
    FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype = 'advisory' AND l.granted AND a.application_name LIKE 'ha-%'
    GROUP BY 1 ORDER BY 1
  `;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(LockRow))(rows);
  return Object.fromEntries(decoded.map((row) => [row.pool, row.locks]));
});

/**
 * Runs `effect` on `url` every 250 ms until `done` holds or 240 tries pass, and returns the last
 * value.
 */
const pollUntil = <A, E>(
  url: Redacted.Redacted,
  effect: Effect.Effect<A, E, SqlClient.SqlClient>,
  done: (value: A) => boolean,
): Promise<A> =>
  runSql(
    url,
    effect.pipe(
      Effect.repeat({ schedule: Schedule.spaced('250 millis'), until: done, times: 240 }),
    ),
  );

/**
 * Starts a worker named `name` in its own scope, as its own process would: its own pool (named
 * `ha-<name>`), port and Cluster, running `only` of `jobs`. Returns the scope to stop it with.
 */
const startWorker = async (
  url: Redacted.Redacted,
  name: string,
  jobs: ReadonlyArray<AnyJobDefinition<WorkerName>>,
  only?: ReadonlyArray<string>,
): Promise<Scope.Closeable> => {
  const port = await allocateLoopbackPort();
  const pool = applicationPool(url, `ha-${name}`);
  const worker = makeSchedulerLayer(jobs, { only }).pipe(
    Layer.provide([
      applicationWorkerCluster(pool, port, jobs, only),
      layerStorage().pipe(Layer.provide(pool)),
      SecureRandom.layerWebCrypto,
      Layer.succeed(WorkerName, name),
    ]),
  );
  const scope = await Effect.runPromise(Scope.make());
  await Effect.runPromise(
    Layer.buildWithScope(worker, scope).pipe(Effect.asVoid, Effect.provide(quietLogger)),
  );
  return scope;
};

/**
 * Stops a worker started by `startWorker`, as a graceful shutdown of its process would.
 */
const stopWorker = (scope: Scope.Closeable): Promise<void> =>
  Effect.runPromise(Scope.close(scope, Exit.void).pipe(Effect.provide(quietLogger)));

/**
 * One body call of a recording job: the worker it ran on, its run key and what triggered it.
 */
type RecordedRun = {
  readonly worker: string;
  readonly runKey: string;
  readonly trigger: JobTrigger;
};

/**
 * A job that records which worker ran it, for which run key.
 */
const recordingJob = (name: string, ranOn: Array<RecordedRun>) =>
  defineJob({
    name,
    description: 'Records the worker it ran on',
    cadence: { _tag: 'Every', interval: '5 minutes' },
    params: { schema: Schema.Struct({}), defaults: {} },
    stats: Schema.Struct({}),
    run: ({ runKey, trigger }) =>
      Effect.gen(function* () {
        ranOn.push({ worker: yield* WorkerName, runKey, trigger });
        return {};
      }),
  });

/**
 * Completed ledger rows of `job`, or only those of `runKey` when given.
 */
const completedRuns = (job: string, runKey?: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const ofKey = runKey === undefined ? sql`` : sql`AND run_key = ${runKey}`;
    const rows = yield* sql`
      SELECT count(*)::int AS count FROM app.job_runs
      WHERE job = ${job} AND status = 'completed' ${ofKey}
    `;
    const [row] = yield* Schema.decodeUnknownEffect(Schema.Array(CountRow))(rows);
    return row?.count ?? 0;
  });

describe('Several workers on one Cluster', () => {
  it('splits the shards, runs each scheduled run once, and fails over when a worker stops', async () => {
    const url = await createDatabase(postgres, 'ha_failover');
    await prepareApplicationDatabase(url);
    const ranOn: Array<RecordedRun> = [];
    const job = recordingJob('ha-every', ranOn);
    const jobs = [job];
    const [a, b] = await Promise.all([startWorker(url, 'a', jobs), startWorker(url, 'b', jobs)]);
    try {
      // Four groups (the application's, default, yieldkit-jobs, ha-every) of 64 shards each.
      const total = 4 * SHARDS_PER_GROUP;
      // The first worker to register can take every shard and hand some over once it sees the
      // other, so wait for the rebalanced split rather than the first full count.
      const split = await pollUntil(
        url,
        shardLocksByPool,
        (locks) =>
          (locks['ha-a'] ?? 0) > 0 &&
          (locks['ha-b'] ?? 0) > 0 &&
          (locks['ha-a'] ?? 0) + (locks['ha-b'] ?? 0) === total,
      );
      expect((split['ha-a'] ?? 0) + (split['ha-b'] ?? 0)).toBe(total);
      expect(split['ha-a']).toBeGreaterThan(0);
      expect(split['ha-b']).toBeGreaterThan(0);

      // Both start-up singletons and crons race for the current 5-minute run key: one run. The
      // next key can start meanwhile when a 5-minute boundary passes, so count only the first key.
      await pollUntil(url, completedRuns(job.name), (count) => count > 0);
      const first = ranOn[0];
      const ofFirstKey = () => ranOn.filter((run) => run.runKey === first?.runKey);
      // Give a duplicate run, if there were one, time to land.
      expect(
        await runSql(
          url,
          Effect.sleep('2 seconds').pipe(Effect.andThen(completedRuns(job.name, first?.runKey))),
        ),
      ).toBe(1);
      expect(ofFirstKey()).toHaveLength(1);

      // A stops gracefully: it releases its shards and B takes all of them.
      await stopWorker(a);
      const after = await pollUntil(url, shardLocksByPool, (locks) => locks['ha-b'] === total);
      expect(after).toEqual({ 'ha-b': total });

      // Work dispatched now runs on B, whichever worker owned its shard before.
      const apiPool = applicationPool(url, 'ha-api');
      const completed = await Effect.runPromise(
        Effect.gen(function* () {
          const dispatched = yield* dispatchApiJobWorkflow(job, { params: {}, force: true });
          for (let i = 0; i < 150; i += 1) {
            const polled = yield* makeJobWorkflow(job).poll(dispatched.executionId);
            if (Option.isSome(polled) && polled.value._tag === 'Complete') {
              return true;
            }

            yield* Effect.sleep('200 millis');
          }

          return false;
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              applicationClientCluster(apiPool, jobs),
              layerStorage().pipe(Layer.provide(apiPool)),
              SecureRandom.layerWebCrypto,
            ),
          ),
          Effect.scoped,
          Effect.provide(quietLogger),
        ),
      );
      expect(completed).toBe(true);
      expect(ofFirstKey().filter((run) => run.trigger !== 'api')).toEqual([first]);
      expect(ranOn.filter((run) => run.trigger === 'api')).toMatchObject([{ worker: 'b' }]);
    } finally {
      await stopWorker(b);
    }
  }, 180_000);

  it('runs each job only on the workers whose `only` and shard groups include it', async () => {
    const url = await createDatabase(postgres, 'ha_only');
    await prepareApplicationDatabase(url);
    const ranOn: Array<RecordedRun> = [];
    const reports = recordingJob('ha-reports', ranOn);
    const imports = recordingJob('ha-imports', ranOn);
    const jobs = [reports, imports];
    const workers = await Promise.all([
      startWorker(url, 'reports', jobs, ['ha-reports']),
      startWorker(url, 'imports', jobs, ['ha-imports']),
    ]);
    try {
      await pollUntil(
        url,
        Effect.all([completedRuns('ha-reports'), completedRuns('ha-imports')]),
        ([r, i]) => r > 0 && i > 0,
      );
      expect(ranOn.map((run) => run.worker).toSorted()).toEqual(['imports', 'reports']);
      const byPool = await runSql(url, shardLocksByPool);
      // Each claims default, yieldkit-jobs, the application's group and its own job's group.
      expect((byPool['ha-reports'] ?? 0) + (byPool['ha-imports'] ?? 0)).toBe(5 * SHARDS_PER_GROUP);
    } finally {
      await Promise.all(workers.map(stopWorker));
    }
  }, 180_000);
});
