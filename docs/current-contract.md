# Current memory-mcp contract

This document describes the current Rust server contract. The Rust tool
descriptors and protocol tests are the source of truth for the advertised tool
surface.

## Runtime and wire protocol

- The server is a single stdio process. Input and output are newline-delimited
  JSON-RPC 2.0 messages; each response is flushed immediately.
- `initialize` echoes the requested `protocolVersion` (default
  `2024-11-05`) and returns `capabilities.tools.listChanged=false` plus
  `serverInfo.name=memory-mcp` and `serverInfo.version=0.23.0`.
- `tools/list` returns the 80 advertised tools below, each with its exact
  `description` and JSON `inputSchema`.
- `tools/call` requires a string `params.name` and an object
  `params.arguments`. The handler result is serialized as one MCP text content
  item. `isError` is true when the result has a top-level `error` key.
- Parse errors use JSON-RPC code `-32700`; invalid requests use `-32600`;
  invalid params use `-32602`; unknown methods with an id use `-32601`.
  Notifications produce no response. A tool exception is logged to stderr and
  returns the generic client payload `{"error":"tool execution failed"}`.
- The `add_fact` alias is handled by the server but is not advertised by
  `tools/list`.

## Public tool inventory

The names and schemas are grouped here for review. The Rust tool descriptor set
is authoritative for every parameter, default, enum, and bound.

### Facts, retrieval, lifecycle, and review

`remember_fact`, `absorb`, `chunk_fact`, `search_facts`, `search_semantic`,
`embed_backfill`, `list_facts`, `summarize_index`, `forget_fact`, `stats`, `export`,
`sweep_freshness`, `verify_facts`, `consolidate`, `fact_history`,
`review_pending`, `confirm_fact`, `facts_for_session`, `list_sessions`,
`fact_references`, `list_forgotten`, `restore_fact`, `ingest_turn`,
`compose_recall`, `auto_orient`, `search_guard`, `decay_sweep`.

### Immutable contexts and local documents

`put_context`, `ingest_document`, `list_context`, `resolve_context`,
`read_context`, `search_context`, `chunk_context`, `reduce_context`.

### Lifecycle events and handoffs

`capture_event`, `list_events`, `read_event`, `handoff_begin`, `list_handoffs`,
`handoff_accept`, `handoff_cancel`.

### Runs, measurements, anchors, and summaries

`run_begin`, `run_end`, `link_run`, `query_run`, `record_measurement`,
`query_measurement`, `prepare_summary`, `query_anchored`, `context_map`.

### Graph, decisions, provenance, and categories

`remember_entity`, `remember_relation`, `search_graph`, `record_decision`,
`query_decisions`, `find_precedents`, `get_causal_chain`, `get_provenance`,
`attach_evidence`, `detect_conflicts`, `export_rdf`, `list_categories`,
`search_index`, `categorize_pending`.

### Feedback, databases, and workspaces

`record_feedback`, `query_feedback`, `create_database`, `list_databases`,
`archive_database`, `backup_database`, `delete_database`, `select_database`,
`current_database`, `reset_database`, `create_workspace`, `list_workspaces`,
`reset_workspace`, `archive_workspace`, `backup_workspace`.

The descriptor set contains 80 advertised tool names. `add_fact` remains a
handler alias and is not advertised.

## Resource and security bounds

- Fact text is limited to 16,000 Unicode scalar values before persistence.
- Context content is limited to `MEMORY_MCP_CONTEXT_MAX_BYTES`, defaulting to
  4 MiB and capped at 16 MiB.
- Lifecycle payloads and metadata are sanitized for sensitive-key values,
  credential-shaped strings, URLs, and filesystem paths, honor `exclude_paths`,
  and are limited to 16 KiB after sanitization. Event identifiers containing
  restricted data are rejected.
- Document ingestion requires a caller-supplied root plus a relative path and
  reads at most 16 MiB through a bounded stream; the root is never persisted.
- Explicit backup arguments are file names resolved below a mode-0700 private
  `backups/` directory, and backup files use mode 0600.
- The bundled plaintext Redis and HTTP provider adapters accept only loopback
  endpoints. HTTP provider credentials are rejected unconditionally; remote
  services require a local TLS proxy or sidecar.
- Stderr diagnostics use stable generic messages and never include backend,
  provider, database-operation, command-line, or caller-controlled error
  details.
- Database and backup protocol responses expose names, sizes, and bounded
  generated file names only; absolute filesystem paths remain internal.

## Persistence contract

- The active SQLite path is `MEMORY_MCP_DB`; without it the default is
  `data/facts.db` relative to the process working directory.
- Every connection enables WAL mode, `busy_timeout=5000`, and foreign keys.
  The connection timeout is 10 seconds. The store is designed for multiple
  local writers sharing one explicitly configured file.
- `select_database` changes the database for the current process. Named
  databases live under the sibling `databases/` directory. The active store
  cannot be archived or deleted, and a selected database cannot be archived or
  deleted until it is deselected.
- The empty workspace id (`workspace_id=''`) is the shared pool for fact, graph,
  decision, and evidence data. Workspace-aware reads and writes preserve the
  workspace scope rules; context operations require an explicit workspace and
  do not fall back to the shared pool.
- Database initialization is idempotent. It creates the schema, applies
  additive upgrades or atomic rebuilds, and repairs FTS indexes before serving
  calls.

## SQLite schema baseline

Fresh stores contain the following persistent tables and virtual tables. Column
names and constraints are listed to make the Rust schema reviewable without
duplicating the SQL implementation.

| Table | Contract and key constraints |
| --- | --- |
| `categories` | `id`, `name`, `workspace_id`, timestamps; unique `(name, workspace_id)`. |
| `facts` | Fact text, SHA-256, source/project/domain, trust, strong/importance, validity and lifecycle fields, workspace, access counters, and nullable `category_id`; trust is `high\|medium\|low`, lifecycle is `active\|degraded\|forgotten`; unique `(sha256, workspace_id)`. |
| `facts_fts` | FTS5 external-content index over `facts.text`, maintained by insert/update/delete triggers. |
| `entities` | Display name plus normalized `canonical_name`, type, aliases, workspace, timestamps; unique `(name, workspace_id)`, indexed by `(canonical_name, workspace_id)`. |
| `relations` | Subject/object entity FKs, predicate, optional source fact, workspace, timestamp; unique `(subject_id, predicate, object_id)`. Entity deletion cascades; source-fact deletion sets the source to null. |
| `decisions` | Category, subject, scenario, reasoning, outcome, confidence, decision maker, issue ref, code `path`/`symbol` anchors, optional parent, workspace, timestamps. |
| `decisions_fts` | FTS5 external-content index over decision scenario, reasoning, and category, maintained by triggers. |
| `evidence` | Fact FK, source/checksum/fetched metadata, repository ref/path/symbol and line/column range, selected-text hash, resolution status, timestamp; unique `(fact_id, source_ref)`. |
| `contexts` | Immutable `ref`, name, content, schema metadata, source, SHA-256, required workspace, timestamps, expiry, and byte size; `ref` is unique. |
| `context_lineage` | Parent/child context refs, relation, workspace, timestamp; unique `(parent_ref, child_ref, relation)`. |
| `lifecycle_events` | Idempotency key, event metadata, unique immutable `context_ref`, workspace, payload hash/size/truncation, timestamp; unique `(workspace_id, idempotency_key)`. |
| `handoffs` | Immutable context ref, owner/session/source, workspace, sharing flag, TTL, idempotency key, acceptance/cancellation audit fields; state is `open\|accepted\|cancelled\|expired`. |
| `workspaces` | Named scope id, status `active\|archived\|reset`, timestamps. |
| `activity_days` | One row per UTC day with at least one `tools/call`; decay uses this activity signal. |
| `runs` | Client-supplied run/issue/PR/session/Git facts, bounded files/diff, state `open\|closed`, workspace, timestamps; unique `(workspace_id, run_id)`. The server never shells out to Git. |
| `memory_access_events` | Bounded pull/push telemetry: workspace, site, query hash, result count, latency, timestamp; no payloads. |
| `memory_feedback` | Opaque feedback id, site, item type/ref, signal, query hash, workspace, timestamp; signals are `helpful\|not_helpful\|stale\|irrelevant\|unsafe`, unique `(workspace_id, feedback_id)`. |
| `measurement_observations` | Aggregate baseline/memory metrics keyed by workspace, measurement, sample, and variant; no prompt or free-text payload; numeric range checks and unique pair key. |
| `fact_embeddings` | Optional fact vector BLOB, model, timestamp; FK to `facts`. Created even when embeddings are disabled. |
| `decision_embeddings` | Optional decision vector BLOB, model, timestamp; FK to `decisions`. |

Foreign keys are enabled on every connection. The FTS tables are external
content indexes and are repaired during database initialization when needed.

## Current coordinator implementation

The stdio server uses `BackendCoordinator` to select and synchronize its
backends:

- Redis stores a bounded, namespaced SQLite state snapshot and a monotonic
  revision; `WATCH`/`MULTI`/`EXEC` protects the revision-checked publish.
- While Redis is healthy, the coordinator restores a private in-memory
  materialization from the Redis snapshot and executes all 80 advertised tools
  plus the `add_fact` alias against that Redis-owned state. The file-backed
  SQLite store is updated by bounded background replay and is used as
  standby/fallback.
- The coordinator selects Redis when its probe succeeds. Otherwise it serves
  the complete SQLite implementation, records degraded writes in a durable,
  idempotent JSONL outbox, and reconciles back with Redis priority.
- Each stateful Redis publish can atomically record SHA-256 idempotency markers;
  recovery checks those markers before replay and keeps them for a bounded
  seven-day duplicate-replay detection window.
- The watcher reads only the small revision key while the state is unchanged,
  fetches a snapshot after a revision change, uses bounded backoff, and stops
  with the coordinator lifecycle.
- The watcher makes a complete snapshot checkpoint only after each bounded
  256-revision interval, so restart recovery remains bounded without copying the
  database on every write.
- Each state-changing operation also receives a system projection for database
  metadata, and each touched workspace receives bounded native entity
  projections for facts, contexts, events, fact history, context lineage,
  handoffs, graph, decisions, evidence, categories, runs, measurements,
  feedback, and registered workspaces.
- Normal native writes apply only changed entities and removed keys. The
  pointwise projection, monotonic revision, schema marker, manifests, and
  durable operation ledger are committed in one `WATCH`/`MULTI`/`EXEC`
  transaction. A per-workspace manifest records the projection schema version,
  revision, and bounded entity count. A version-2 schema marker rebuilds older
  snapshot-only namespaces on attach.
- The operation ledger is keyed by the SHA-256 operation idempotency key and
  has no TTL. It stores only operation name, workspace hash, status, revision,
  entity count, and a bounded conflict reason. The seven-day marker remains a
  fast path, while recovery consults the durable ledger first.
- `BackendCoordinator::status()` exposes only backend, connection, revision,
  lag, outbox, Redis command/byte, and synchronization tick/error/duration
  counters; it never returns payloads or credentials.

This remains a correctness-first implementation, not a performance claim. Redis
is the sole durable primary when configured: its native entity/database
projections and revision are the source of the active write path; the complete
snapshot remains the attach/rebuild/recovery transport. The in-memory `Store`
is retained as a deterministic compatibility/query engine, not as a second
durable primary; the file-backed SQLite image is mirrored pointwise in the
background after a Redis commit and is the fallback/standby image. The direct
`handle_line` API remains a SQLite fixture path, while the shipped stdio binary
uses `handle_line_with_coordinator`.

## Redis-first backend contract

- When the configured Redis endpoint passes the reachability/authentication
  probe, Redis is the primary backend and every advertised MCP operation,
  including database and workspace lifecycle operations, is routed through that
  backend.
- When Redis is not configured or cannot be reached, the complete operation
  surface starts on the existing SQLite backend as the fallback.
- While Redis is primary, a background coordinator keeps a SQLite hot standby
  current from confirmed Redis revisions.
- If the active Redis connection is lost, the coordinator serves the last
  confirmed SQLite revision and records every degraded-mode write in a durable,
  idempotent outbox.
- After Redis recovers, the coordinator uses the local SQLite fallback image to
  publish queued point changes, gives the Redis revision priority for conflicts,
  mirrors committed operations back into SQLite, and switches normal traffic to
  Redis.
- The pointwise state publish and its operation markers are one Redis
  transaction. A complete snapshot is reserved for attach, schema rebuild,
  recovery, and the amortized 256-revision restart checkpoint; it is not part
  of each write.
- Recovery treats an existing marker as already committed and removes the
  corresponding outbox item without applying it a second time. Markers expire
  after seven days, so duplicate-replay protection is intentionally bounded.
- A partial Redis route is not an acceptable mode: an operation must not
  silently use SQLite while Redis is the selected backend.
- An acknowledged Redis write must be present in the Redis revision stream
  before the response is returned. An acknowledged degraded-mode write must be
  present in the SQLite database and outbox before the response is returned.
- The coordinator exposes standby lag and reconciliation state without exposing
  payloads or credentials.
- The background synchronizer is event/revision driven and bounded. It polls a
  small health/revision key at a configurable interval, fetches state only when
  the revision changes, applies bounded batches, checkpoints the snapshot only
  at the 256-revision boundary, and backs off on errors. It must not rescan or
  rewrite the complete dataset on every tick.
- The Redis watcher shares the coordinator's connection and lifecycle, avoids a
  busy loop, uses bounded timeouts, and stops cleanly with the process.
- The full 80-tool route is an acceptance gate: every advertised tool and the
  `add_fact` alias must cross the same coordinator, with no direct dispatcher
  path that silently bypasses Redis, the standby, or the outbox.
- Redis may be configured with an explicit `MEMORY_MCP_REDIS_URL`/`REDIS_URL`
  or with host, port, database, user, and password variables. Explicit URLs
  take precedence, loopback-only endpoint validation is applied before connect,
  and credentials remain environment-only and are never emitted in logs,
  reports, test fixtures, or protocol responses.

The architecture and recovery protocol are recorded in
`docs/decisions/ADR-0001-redis-primary-with-sqlite-fallback.md`; the pointwise
write refinement is recorded in
`docs/decisions/ADR-0002-pointwise-redis-replication.md`. A reachable Redis
endpoint selects the coordinator's Redis-primary pointwise mode, and an
unavailable endpoint falls back to SQLite without enabling a partial fact-only
route. The recorded benchmark makes no speedup claim.
