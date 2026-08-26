# Performance measurement

The repository includes a bounded memory-bench binary for the performance
decision in NTL-722. It runs one fixed workload:

1. open a persistent SQLite connection and complete one-time migrations;
2. insert unique facts into one workspace;
3. search the same workspace repeatedly.

Run the SQLite fallback baseline with:

    MEMORY_MCP_BENCH_ITERATIONS=128 cargo run --release --bin memory-bench

Set MEMORY_MCP_REDIS_URL (or REDIS_URL) to run the same fact workload against
the optional namespaced Redis adapter. Password-only URLs such as
`redis://:password@host:6379/0` and URLs with an explicit username are both
supported; credentials are read from the environment and are not written to
the report. The binary reports both backend timings only when Redis passes its
connection and PING probe. If the URL is missing or unavailable, it reports
selected_backend=sqlite and an explicit fallback reason.

The JSON output separates migration/setup, writes, searches, and total time.
It is an observation for one environment, not a general speedup claim. A
paired decision requires the same release build, host, iteration count,
workspace/data shape, warm-up policy, Redis persistence settings, and repeated
samples with p50/p95 results. Until those controls and an agreed threshold are
recorded, performance_efficacy remains not_claimed.

The current Redis benchmark remains a core-fact measurement adapter; it is not
evidence that the MCP server has switched backends. The Redis-first contract
requires all advertised tools to use Redis when its probe succeeds and the
complete SQLite implementation only when Redis is unavailable. The server must
not enable a partial mode that sends facts to Redis while contexts, lifecycle
records, graph data, provenance, telemetry, or backups continue on SQLite.
Full-backend performance claims therefore wait for the parity, replication,
and failover gates described in `current-contract.md`. Standby lag,
reconciliation duration, and recovery success are separate reliability
measurements; they must not be folded into a Redis speedup percentage.

The current coordinator implementation is a correctness-first Redis-primary
snapshot path. A stateful operation serializes the complete bounded SQLite
image into the namespaced Redis state key, so its network and Redis write cost
must be measured separately from the existing core-fact benchmark. It also
queues one small idempotency-marker `SET ... EX` per stateful operation in the
same transaction; the seven-day TTL bounds marker retention but does not remove
the snapshot cost. The local SQLite store is the materialized full-route engine
and standby; this design does not provide evidence of native Redis per-entity
performance.

The replication budget is part of the acceptance contract. The watcher uses a
small revision/health read, fetches a state batch only after a revision change,
and applies bounded batches with exponential backoff on errors. Resource
measurements must report watcher CPU time, Redis commands/bytes, SQLite write
bytes, standby lag, and reconciliation duration under idle, steady-write, and
recovery workloads. A full-dataset scan on every health tick fails this gate.
