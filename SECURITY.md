# Security policy

## Supported versions

Security fixes go into the latest published version of `@yieldkit/jobs`. Upgrade to the newest release before reporting an issue that may already be fixed.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use [GitHub private vulnerability reporting](https://github.com/yieldkit/jobs/security/advisories/new) to share the details with the maintainer.

Include the affected version, the `effect`, `@effect/sql-pg` and `@yieldkit/pg` versions, the PostgreSQL version, reproduction steps and the expected impact. Do not include real connection strings, passwords or data; use made-up values.

## Scope

Most useful are reports where:

- an API caller can set a param outside the job's `apiParams`, through `dispatchApiJobWorkflow` or a forged workflow payload with `trigger: 'api'`;
- a storage option (`schema` or `prefix`) that passed validation ends up interpreted as SQL, or a library statement reads or writes a table other than the configured one;
- a stale run can heartbeat or settle a run key after another attempt took it over, or two runs of one `(job, runKey)` execute at the same time while both hold valid leases;
- the connection URI or password reaches one of this package's log lines or errors. The package never handles a URI itself; it runs on the `SqlClient` you provide.

Out of scope: secrets your jobs write into their own stats, params, error messages or log fields; your Effect Cluster's configuration, including its unauthenticated runner RPCs (keep the runner port on a private network); and the database role privileges you grant.
