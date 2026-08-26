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

Introduce one backend contract consumed by the protocol dispatcher. At process
startup, the runtime probes the configured Redis endpoint (including auth and
database selection). A successful probe selects the complete Redis backend;
missing or unreachable Redis selects the complete SQLite backend. Selection is
reported only as a backend name and a safe reason, never as a URL or credential.

The Redis implementation must provide workspace/database isolation, stable ids,
atomic idempotency, lifecycle transitions, bounded payloads, deterministic
queries, and export/backup behavior equivalent to the SQLite contract. The
existing SQLite store remains the fallback implementation and is not used for a
request while Redis is selected.

The implementation is staged behind a coverage gate:

1. define and test the backend interface and dispatcher routing;
2. define the Redis key/schema model and atomic write/idempotency primitives;
3. port each Store operation group and add Redis integration tests;
4. add startup fallback and controlled connection-loss tests, including the
   no-divergent-acknowledged-write invariant;
5. run the complete formatter, test, lint, AppSec, artifact, QA, and PM gates.

## Runtime-loss policy to resolve

The startup choice is deterministic. A connection loss after a Redis-backed
write has been acknowledged must not silently switch to SQLite, because the two
stores may diverge. The implementation must either reconnect and complete the
operation against Redis or return a safe backend-unavailable error until a
controlled resynchronization is possible. PM/TechLead should confirm whether
the product additionally requires automatic live failover after such a loss; if
yes, that path needs an explicit resynchronization and acknowledgement protocol.

## Consequences

This avoids a misleading partial Redis mode and keeps the current SQLite parity
behavior intact while the Redis backend is built. It requires more work than
adding an environment variable: every protocol operation and every stateful
database/workspace transition needs a Redis implementation and an isolation/
idempotency test. The benchmark adapter cannot be used as acceptance evidence
for the full backend.

## Acceptance evidence

- one dispatcher backend contract covers all 80 advertised tools and the
  `add_fact` compatibility alias;
- reachable Redis integration exercises every operation group and verifies
  workspace/database isolation and replay behavior;
- unavailable Redis selects SQLite and preserves the existing SQLite tests;
- connection-loss behavior has no silently acknowledged divergent writes;
- no credentials appear in logs, reports, fixtures, or protocol responses;
- QA and AppSec are green before PM acceptance.
