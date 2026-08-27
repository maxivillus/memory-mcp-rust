# Performance measurement

The repository includes a bounded memory-bench binary for the performance
decision record. It runs one fixed workload:

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
precedence. The bundled plaintext adapter accepts only localhost or an IP
loopback address; use a local TLS proxy/sidecar when Redis is remote.
Credentials are read from the environment and are not written to the report.
The binary reports both backend timings only when Redis passes its connection
and PING probe. If the configured endpoint is unavailable, it reports
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

The current coordinator implements the selected Redis-primary model. A
stateful operation runs against the private in-memory compatibility engine,
then atomically publishes only changed native records and removed keys together
with the next Redis revision, manifests, durable operation ledger, and
idempotency marker. The SQLite standby is updated by a background, pointwise
outbox replay after the Redis commit; it is not written before Redis accepts the
operation and remains the fallback image. A complete bounded snapshot is
reserved for attach, schema rebuild, recovery, and an amortized checkpoint
every 256 committed revisions. The projection is capped at 4096 entities and
8 MiB per delta batch; each record is individually addressable through a hashed
key and a workspace index. A version-2
schema marker triggers a bounded full rebuild for legacy snapshot-only
namespaces. No native Redis performance win is claimed.

The native projection covers the exported workspace entities (facts, contexts,
events, fact history, context lineage, handoffs, graph, decisions, evidence,
categories, runs, measurements, feedback, and workspaces) plus database
metadata in the reserved system scope. The complete database catalog, including
named in-memory database snapshots, is carried in the Redis-owned state image.
Measurements must separate projection cost from snapshot backup cost and report
rejection/fallback behavior when either bound is reached.

The replication budget is part of the acceptance contract. The watcher uses a
small revision/health read, mirrors only durable outbox operations in bounded
batches, fetches a full state image only after a revision change or recovery,
and makes a compatibility checkpoint only at the 256-revision boundary. It
applies exponential backoff on errors. The RESP client batches the Redis
transaction commands into one network write/flush. Resource measurements must
report watcher CPU time, Redis commands/bytes, SQLite write bytes, standby lag,
and reconciliation duration under idle, steady-write, and recovery workloads.
A full-dataset scan on every health tick fails this gate.
The coordinator status counters cover Redis commands/bytes and watcher
ticks/errors/last duration; CPU time and SQLite write-byte accounting still
require the real-service measurement harness.
