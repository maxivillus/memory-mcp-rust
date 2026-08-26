# Performance measurement

The repository includes a bounded memory-bench binary for the performance
decision in NTL-722. It runs one fixed workload:

1. open a persistent SQLite connection and complete one-time migrations;
2. insert unique facts into one workspace;
3. search the same workspace repeatedly.

Run the SQLite fallback baseline with:

    MEMORY_MCP_BENCH_ITERATIONS=128 cargo run --release --bin memory-bench

Set MEMORY_MCP_REDIS_URL (or REDIS_URL) to run the same fact workload against
the optional namespaced Redis adapter. Docker-style split settings are also
accepted when a URL is absent: MEMORY_MCP_REDIS_HOST or REDIS_HOST,
MEMORY_MCP_REDIS_PORT or REDIS_PORT, optional *_DATABASE or *_DB, optional
*_USERNAME or *_USER, and *_PASSWORD or *_PASS. Explicit URL settings take
precedence. Password-only URLs such as
`redis://:password@host:6379/0` and URLs with an explicit username are both
supported; credentials are read from the environment and are not written to
the report. The binary reports both backend timings only when Redis passes its
connection and PING probe. If the configured endpoint is unavailable, it reports
selected_backend=sqlite and an explicit fallback reason.

The JSON output separates migration/setup, writes, searches, and total time.
When the Redis adapter is selected, it also reports payload-free
`redis_commands`, `redis_request_bytes`, and `redis_response_bytes` counters;
the SQLite row leaves those fields null. These counters make command and wire
cost visible without logging memory contents.
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
migration path. A stateful operation still serializes the complete bounded
SQLite image for standby/restart recovery, but it now also replaces a bounded
workspace-scoped native entity projection in the same Redis transaction. The
projection is capped at 4096 entities and 8 MiB per publish; each record is
individually addressable through a hashed key and a workspace index. The same
transaction writes a durable operation ledger record with no TTL plus the
seven-day compatibility marker. These bounds make command and wire cost
observable, but the local SQLite store remains the full-route materialized
engine and standby; no native Redis performance win is claimed.

The native projection is deliberately an incremental migration boundary. It
covers the exported workspace entities (facts, contexts, events, handoffs,
graph, decisions, evidence, categories, runs, measurements, feedback, and
registered workspaces), while fact history, context lineage, selected database
metadata, and native Redis reads remain follow-up work. Measurements must
separate projection cost from snapshot backup cost and report rejection/fallback
behavior when either bound is reached.

The replication budget is part of the acceptance contract. The watcher uses a
small revision/health read, fetches a state batch only after a revision change,
and applies bounded batches with exponential backoff on errors. Resource
measurements must report watcher CPU time, Redis commands/bytes, SQLite write
bytes, standby lag, and reconciliation duration under idle, steady-write, and
recovery workloads. A full-dataset scan on every health tick fails this gate.
The coordinator status counters cover Redis commands/bytes and watcher
ticks/errors/last duration; CPU time and SQLite write-byte accounting still
require the real-service measurement harness.
