## Status

Accepted by PM after the product-owner selection of Option A and the QA,
AppSec, artifact, and release readbacks recorded for the performance decision
record. The decision
accepts Redis as the canonical primary state with a Redis-restored in-memory
compatibility engine and SQLite as the standby/fallback image; it does not
claim a performance improvement. This file is an immutable, decision-time
record: the Context and implementation checkpoint sections below are historical
snapshots, so phrases such as “currently”, “not yet”, and “Proposed” describe
those checkpoints rather than the current repository. The current behavior is
summarized in [`docs/current-contract.md`](../current-contract.md), and the
pointwise refinement is recorded in
[`ADR-0002`](ADR-0002-pointwise-redis-replication.md).

## Context

The Rust server currently has one SQLite `Store` implementing the complete
compatibility surface and a small RESP2 `RedisAdapter` used by the benchmark.
The adapter currently implements only fact remember/list/search/reset operations;
the stdio dispatcher still calls SQLite for the full tool inventory. The product
requirement is explicit: use Redis for every operation when Redis is available,
and use SQLite when Redis is unavailable.

Wiring the existing adapter into only the fact tools would create a split-brain
mode and would violate that requirement. The backend boundary must cover facts,
contexts, events, handoffs, runs, measurements, feedback, categories,
graph/decisions/evidence, retrieval helpers, exports, database lifecycle, and
workspace lifecycle.

## Decision proposal

Introduce one backend coordinator consumed by the protocol dispatcher. At
process startup, it probes the configured Redis endpoint (including auth and
database selection). A successful probe selects the Redis-primary state;
missing or unreachable Redis selects the complete SQLite state. Selection is
reported only as a backend name and a safe reason, never as a URL or credential.

While Redis is primary, a bounded background replicator advances a cursor over
confirmed Redis revisions and applies them transactionally to a SQLite hot
standby. The standby stores the last confirmed Redis revision, so failover can
serve a known point rather than an arbitrary local cache. Every stateful MCP
operation, including database/workspace lifecycle operations, belongs to the
same revision and idempotency protocol.

On a Redis connection loss, the coordinator atomically enters `sqlite_failover`
and serves the standby. Degraded-mode writes are committed to SQLite and a
durable outbox before acknowledgement. Each outbox item carries an operation
idempotency key and the Redis revision on which it was based; credentials and
free-form secret-bearing payloads are excluded from diagnostics.

When Redis returns, the coordinator enters `reconciling`: it first applies all
Redis revisions after the standby cursor, then replays only non-conflicting
outbox items using their idempotency keys. If the same logical record changed
in both stores, Redis wins as requested; the rejected local item remains in the
reconciliation audit until it is explicitly handled. The coordinator refreshes
SQLite from the resulting Redis state and switches normal traffic back to Redis
only after the Redis revision and standby cursor are durable.

The synchronizer is deliberately resource-bounded. It uses one coordinator
health/revision watcher with a configurable interval and bounded timeout, reads
only a small revision/health value while the revision is unchanged, fetches
state deltas in bounded batches, and applies exponential backoff after an
error. It does not perform a full Redis/SQLite scan on every tick. The watcher
has a clean stop path and exposes only safe counters/lag state. The 80-tool
coverage check is a hard gate: all advertised tools and the `add_fact` alias
must use the same coordinator path.

## Implementation checkpoint

The first implementation slice wires `BackendCoordinator` into the shipped
stdio server and routes the complete 80-tool inventory plus `add_fact` through
it. Redis currently holds a bounded namespaced SQLite snapshot and revision;
the local `Store` is the materialized execution engine and SQLite hot standby.
Stateful calls append to a durable JSONL outbox before execution, publish a
revision-checked snapshot with `WATCH`/`MULTI`/`EXEC` while Redis is primary,
and replay bounded offline writes with Redis priority after recovery. The
publish transaction also writes a SHA-256 operation marker with a seven-day
TTL. Recovery checks that marker before replay, which closes the response-loss
window without retaining operation payloads. The watcher reads the revision key
only while state is unchanged and uses bounded backoff.

This checkpoint is intentionally correctness-first. It proves the route,
snapshot durability, fallback, recovery, bounded watcher behavior, and a
bounded Redis marker for response-loss recovery, but it does not claim a native
per-entity Redis schema, a complete operation ledger/conflict history, or
production performance. Those remain acceptance work; the ADR status stays
Proposed until PM/TechLead review.

## Incremental native projection checkpoint

The next migration slice adds a bounded workspace-scoped native projection
without changing the public Store contract. After each successful state change,
the coordinator derives the selected workspace export into individually
addressable Redis JSON records, replaces that workspace's index, and writes a
schema manifest. Snapshot, projection keys, monotonic revision, durable ledger,
and compatibility marker are committed in one revision-checked transaction.
The ledger stores only operation hash/name, workspace hash, status, revision,
entity count, and an optional bounded conflict reason; it has no TTL, so replay
handling remains available after the seven-day marker expires.

This is an incremental projection and standby migration boundary, not yet a
native Redis execution engine. The SQLite Store remains the complete
materialized engine and restart backup; selected-database metadata, native
Redis reads for all tools, migration from snapshot-only namespaces, and
production measurements remain gated follow-up work. Fact history and context
lineage are now included in the workspace projection, but their native records
are not yet authoritative read paths.

The implementation is staged behind a coverage gate:

1. define and test one backend interface and coordinator state machine;
2. define Redis revisions, key/schema model, atomic idempotency primitives,
   replication cursor, and SQLite outbox;
3. port each Store operation group and add Redis integration tests;
4. add startup fallback, standby lag, forced connection-loss, offline-write,
   replay, conflict, and safe switch-back tests;
5. run the complete formatter, test, lint, AppSec, artifact, QA, and PM gates.

## Option A implementation checkpoint

The product decision for the next stage is Option A: Redis is the canonical
primary for the complete 80-tool surface and SQLite is fallback/standby. The
coordinator now enforces that boundary at the state layer:

- a Redis-primary process restores a private in-memory compatibility engine
  from the Redis-owned snapshot before serving calls;
- state-changing calls publish the next snapshot, native workspace entities,
  database metadata, schema marker, revision, and operation ledger in one
  revision-checked transaction;
- the file-backed SQLite image is refreshed only after that Redis transaction
  commits, so it is not a competing durable write path;
- named databases in the active in-memory engine use snapshot-backed catalog
  records, preserving create/list/select/archive/reset/delete/backup semantics
  across Redis restart and migration;
- attaching to a namespace without the current native schema marker performs a
  bounded full projection rebuild before normal traffic resumes.

The native JSON records are independently addressable for inspection and
workspace/database indexing. The in-memory compatibility engine remains the
deterministic implementation of the public semantics, but it is rebuilt from
Redis and never persists independently while Redis is healthy. This preserves
the full route without introducing a partial fact-only Redis mode. Performance
efficacy remains unclaimed until the real-service measurements and release
gates are complete.

## Consequences

This avoids a misleading partial Redis mode and keeps the current SQLite parity
behavior intact while the coordinator is built. It requires more work than
adding an environment variable: every protocol operation and every stateful
database/workspace transition needs a Redis revision, standby application,
outbox/replay behavior, and an isolation/idempotency test. The benchmark
adapter cannot be used as acceptance evidence for the full backend.

## Acceptance evidence

- one dispatcher backend contract covers all 80 advertised tools and the
  `add_fact` compatibility alias;
- reachable Redis integration exercises every operation group and verifies
  workspace/database isolation and replay behavior;
- unavailable Redis selects SQLite and preserves the existing SQLite tests;
- connection-loss behavior serves the last confirmed standby revision, records
  degraded writes durably, and does not silently discard outbox entries;
- recovery applies Redis-priority reconciliation and switches back only after
  both Redis and the SQLite standby are durable;
- idle and steady-write watcher measurements stay within the agreed CPU,
  command, byte, and lag budgets, with no unbounded polling or full scan;
- the machine-readable route matrix covers all 80 advertised tools and the
  `add_fact` alias;
- no credentials appear in logs, reports, fixtures, or protocol responses;
- QA and AppSec are green before PM acceptance.
