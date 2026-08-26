## Status

Proposed. PM/TechLead acceptance is required before treating this architecture
as an accepted project decision.

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

The implementation is staged behind a coverage gate:

1. define and test one backend interface and coordinator state machine;
2. define Redis revisions, key/schema model, atomic idempotency primitives,
   replication cursor, and SQLite outbox;
3. port each Store operation group and add Redis integration tests;
4. add startup fallback, standby lag, forced connection-loss, offline-write,
   replay, conflict, and safe switch-back tests;
5. run the complete formatter, test, lint, AppSec, artifact, QA, and PM gates.

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
