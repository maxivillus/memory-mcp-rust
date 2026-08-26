# Tech radar

## Pattern and dependency inventory

| Pattern or dependency | Purpose | Where used |
| --- | --- | --- |
| Rust stdio JSON-RPC dispatcher | Keep the MCP process as one self-contained executable with bounded line-oriented I/O. | `src/main.rs`, `src/protocol.rs` |
| `rusqlite` with bundled SQLite/FTS5 | Provide the complete local materialized store and avoid a host SQLite ABI dependency. | `src/store.rs`, `Cargo.toml` |
| SQLite backup API snapshots | Copy a consistent bounded database image for standby refresh and recovery. | `src/store.rs` |
| Redis RESP2 adapter | Provide the reachable Redis transport, URL or Docker-style environment configuration, authentication, revision key, and bounded snapshot state. | `src/redis.rs` |
| Redis-primary coordinator | Select Redis when reachable and route the full advertised tool set through one backend boundary. | `src/backend.rs`, `src/protocol.rs` |
| Revision-checked `WATCH`/`MULTI`/`EXEC` publish | Prevent stale snapshot writes from silently overwriting a newer Redis state. | `src/redis.rs` |
| Durable SQLite outbox | Preserve acknowledged fallback writes until Redis recovery reconciliation completes. | `src/backend.rs` |
| Bounded Redis operation markers | Detect a committed operation after a lost response without retaining operation payloads. | `src/redis.rs`, `src/backend.rs` |
| Workspace-scoped native Redis entity projection | Store individually addressable bounded JSON records and a workspace index for the exported memory entities during the incremental Redis migration. | `src/redis.rs`, `src/backend.rs` |
| Durable Redis operation ledger | Preserve committed/conflict operation metadata beyond the compatibility marker TTL without retaining request payloads. | `src/redis.rs`, `src/backend.rs` |
| Revision watcher with backoff | Avoid full scans while idle, retry failures without a busy loop, and stop with the coordinator. | `src/backend.rs` |
| Payload-free resource counters | Measure Redis commands/bytes and synchronization ticks/errors/duration without exposing memory contents. | `src/redis.rs`, `src/backend.rs`, `src/bin/memory-bench.rs` |

No new third-party dependency was introduced by the coordinator or its resource
counters; the implementation uses existing dependencies and the Rust standard
library.

## Project map

- `src/main.rs`: owns the stdio process and creates one `BackendCoordinator`.
- `src/protocol.rs`: validates JSON-RPC and maps all 80 advertised tools plus
  the `add_fact` alias to the coordinator route.
- `src/backend.rs`: owns backend selection, SQLite standby, durable outbox,
  recovery reconciliation, watcher lifecycle, and safe status counters.
- `src/store.rs`: owns the full SQLite schema, migrations, FTS5 behavior,
  snapshot/restore, and tool operation semantics.
- `src/redis.rs`: owns the bounded RESP2 connection, Redis state snapshot,
  revision publish, native entity projection/index/manifest, operation marker
  and durable ledger primitives, and connection resource counters.
- `src/tools.rs`: owns the advertised tool inventory and explicit mutation
  classification.
- `src/bin/memory-bench.rs`: runs the bounded SQLite/core-fact Redis timing
  workload and reports Redis command/byte counters when Redis is reachable.
- `docs/`: current contract, ADR, performance caveats, and this architecture
  inventory.
