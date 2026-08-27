# Current memory-mcp contract

This document is the compatibility baseline for the Rust rewrite. It records
the reviewed upstream contract snapshot kept in `docs/upstream-tools.json`.
The pinned contract snapshot and the Rust parity tests remain the source of
truth for the tool surface.

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
- The compatibility alias `add_fact` is handled by the server but is not
  advertised by `tools/list`.

## Public tool inventory

The names and schemas are grouped here for review. The upstream `TOOLS` map is
the authoritative schema for every parameter, default, enum, and bound.

## Resource and security bounds

- fact text is limited to 16,000 Unicode scalar values before persistence;
- context content is limited to `MEMORY_MCP_CONTEXT_MAX_BYTES`, defaulting to
  4 MiB and capped at 16 MiB;
- lifecycle payloads and metadata are sanitized for sensitive-key values,
  credential-shaped strings, URLs, and filesystem paths, honor `exclude_paths`,
  and are limited to 16 KiB after sanitization; event identifiers containing
  restricted data are rejected;
- document ingestion requires a caller-supplied root plus a relative path and
  reads at most 16 MiB through a bounded stream; the root is never persisted;
- explicit backup arguments are file names resolved below a mode-0700 private
  `backups/` directory, and backup files use mode 0600;
- the bundled plaintext Redis and HTTP provider adapters accept only loopback
  endpoints. HTTP provider credentials are rejected unconditionally; remote
  services require a local TLS proxy or sidecar.
- stderr diagnostics use stable generic messages and never include backend,
  provider, migration, command-line, or caller-controlled error details.
- database and backup protocol responses expose names, sizes, and bounded
  generated file names only; absolute filesystem paths remain internal.

### Facts, retrieval, lifecycle, and review

`remember_fact`, `absorb`, `chunk_fact`, `search_facts`, `search_semantic`,
`embed_backfill`, `list_facts`, `summarize_index`, `forget_fact`, `stats`, `export`,
`sweep_freshness`, `verify_facts`, `consolidate`, `fact_history`,
`review_pending`, `confirm_fact`, `facts_for_session`, `list_sessions`,
`fact_references`, `list_forgotten`, `restore_fact`,
`ingest_turn`, `compose_recall`, `auto_orient`, `search_guard`.

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

The pinned upstream `TOOLS` map also advertises `decay_sweep`; it was omitted
from the first contract-capture list. `add_fact` remains a handler alias and is
not advertised. The corrected inventory therefore contains 80 advertised tool
names.

The inventory above intentionally describes groups, not a new API. Rust must
preserve the exact upstream names and schemas, including aliases and bounded
optional fields.

## Persistence contract

- The active SQLite path is `MEMORY_MCP_DB`; without it the default is the
  script-relative `<repo>/data/facts.db`.
- Every connection enables WAL mode, `busy_timeout=5000`, and foreign keys.
  The connection timeout is 10 seconds. The store is designed for multiple
  local writers sharing one explicitly configured file.
- `select_database` changes the database for the current process. Named
  databases live under the sibling `databases/` directory. The active store
  cannot be archived or deleted, and a selected database cannot be archived or
  deleted until it is deselected.
- The empty workspace id (`workspace_id=''`) is the shared pool for legacy fact,
  graph, decision, and evidence data. Workspace-aware reads and writes must
  preserve the upstream scope rules; context operations require an explicit
  workspace and do not fall back to the shared pool.
- Database initialization is idempotent. It creates the schema, runs additive
  migrations or atomic rebuilds, and repairs FTS indexes before serving calls.

## SQLite schema baseline

Fresh stores contain the following persistent tables and virtual tables. Column
names and constraints are listed to make the Rust schema reviewable without
copying the whole upstream SQL string.

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
| `runs` | Client-supplied run/issue/PR/session/git facts, bounded files/diff, state `open\|closed`, workspace, timestamps; unique `(workspace_id, run_id)`. The server never shells out to git. |
| `memory_access_events` | Bounded pull/push telemetry: workspace, site, query hash, result count, latency, timestamp; no payloads. |
| `memory_feedback` | Opaque feedback id, site, item type/ref, signal, query hash, workspace, timestamp; signals are `helpful\|not_helpful\|stale\|irrelevant\|unsafe`, unique `(workspace_id, feedback_id)`. |
| `measurement_observations` | Aggregate baseline/memory metrics keyed by workspace, measurement, sample, and variant; no prompt or free-text payload; numeric range checks and unique pair key. |
| `fact_embeddings` | Optional fact vector BLOB, model, timestamp; FK to `facts`. Created even when embeddings are disabled. |
| `decision_embeddings` | Optional decision vector BLOB, model, timestamp; FK to `decisions`. |

Foreign keys are enabled on every connection. The FTS tables are external
content indexes and must be rebuilt when a legacy store first receives them.

## Migration compatibility

The upstream open path applies these compatibility steps in order:

1. v0.3 graph, decisions, and provenance tables.
2. v0.4/v0.5 fact columns, workspace columns, and the rebuild from global
   `UNIQUE(sha256)` to workspace-scoped deduplication.
3. v0.6 workspace registry and v0.7 activity/decay columns.
4. v0.8 foreign-key rebuilds for `evidence` and `relations`.
5. v0.10 categories and `facts.category_id`.
6. v0.11 immutable contexts and lineage; v0.13 lifecycle events and typed
   handoffs.
7. v0.14 workspace-scoped entity uniqueness and canonical-name normalization.
8. v0.16 structured evidence anchors.
9. v0.18 run records, access telemetry, and decision anchors.
10. v0.20 aggregate paired measurements.
11. v0.22 bounded feedback and retention indexes.
12. v0.23 strict evidence admission and typed retrieval-abstention behavior.

Rust must open a legacy SQLite store without data loss, preserve ids and
relations during rebuilds, keep workspace-scoped deduplication, and rebuild
FTS5 indexes when required. Migration tests must cover both a fresh store and
the legacy fixtures used by the upstream suite.

## Copy-first database migration

The shipped `memory-mcp-rust` binary also provides a migration subcommand:

```text
memory-mcp-rust migrate --source LEGACY.db --target RUST.db
```

The source is opened read-only inside one consistent SQLite snapshot. The
target must not exist. Rust schema migrations run on a private temporary copy,
which is published without overwrite only after durable source-table row counts,
per-row typed fingerprints, and target integrity checks pass. The command
prints counts and fingerprints only; it does not print database paths or
memory payloads. `scripts/memory-mcp-migrate.sh` is the environment-variable
wrapper for the same operation.

## Compatibility test baseline

The upstream CI runs Python 3.11 and both suites:

```text
python -m unittest discover -s tests -v
python -m unittest -q test_memory_mcp
```

At the pinned revision, `tests/test_memory_mcp.py` contains 157 test methods
and the legacy `test_memory_mcp.py` contains 78. The Rust port must turn these
behavioral areas into parity tests rather than treating a successful process
start as compatibility:

- fact deduplication, filters, bounded chunks, lifecycle, review, retention,
  conflict detection, export, and workspace isolation;
- strict/advisory admission, typed empty-result abstention, feedback
  idempotency, provenance, anchors, and graph/decision traversal;
- immutable context catalog/read/chunk/reduction, lineage, expiry, local
  document path safety, event sanitization, event retention, and one-shot
  handoffs;
- run/issue/PR links, aggregate telemetry and paired measurements;
- database/workspace management, backups, resets, archives, migrations, FTS
  rebuilds, optional embeddings, and disabled-provider fallbacks;
- stdio JSON-RPC errors for malformed input, non-object requests, bad params,
  unknown methods, and notification handling.

## Rust acceptance boundary for this stage

This document completes the contract-capture stage only. The next implementation
stage can be accepted when the Rust project has:

1. a single executable entry point with the wire behavior above and the
   copy-first migration subcommand;
2. a schema/migration test fixture covering a fresh store and legacy stores;
3. a machine-readable tool-schema parity check against the 80-name inventory;
4. deterministic tests for the core fact/context round trips before optional
   Redis or embedding adapters are introduced.

No performance claim is made by this baseline. Benchmarking starts after the
SQLite parity slice is working, and Redis remains an optional measured adapter
with deterministic SQLite fallback.

## Rust parity implementation status

The current implementation provides one stdio executable, bundled SQLite/FTS5
initialization, additive legacy-store upgrades, the pinned 80-tool descriptors,
and concrete compatibility handlers for the full advertised contract. The
provider/pipeline layer preserves the Python deployment's feature switches for
extraction, embeddings, recall, verification, categorization, and LLM-backed
operations; deterministic test providers are available for isolated checks.

The deployment migration slice adds a copy-first SQLite migration command and
two bounded launch checks. `scripts/memory-mcp-preflight.py` compares both
servers' initialize identity, all 80 tool descriptors, launcher environment
names, SQLite row counts, and disposable empty-argument routes. It treats
missing required arguments as valid route evidence, but blocks unknown or
unimplemented handlers. The actual legacy-Python-versus-Rust preflight remains
a release gate: it must be run with the deployed Python command, its migrated
database copy, and the complete set of environment names that affect provider
and pipeline behavior. The Rust entry point accepts those names and routes the
corresponding operations through the compatibility layer; removing them to make
the comparison green would invalidate the parity check.

The slice descriptions below record historical implementation milestones. The
Redis-first coordinator and the completed provider/pipeline compatibility
surface described later are the current behavior and supersede earlier
"staged" or "out of scope" notes.

The second implementation slice adds SQLite-backed fact lifecycle operations
(`forget_fact`, `restore_fact`, and `list_forgotten`) plus workspace lifecycle
operations (`create_workspace`, `list_workspaces`, `archive_workspace`, and
`reset_workspace`). Forgotten facts are excluded from normal list/search results,
and reset deletes only the selected workspace's facts and contexts. Those
operations were initially implemented on SQLite; the current coordinator routes
the complete surface through Redis when it is reachable and retains the SQLite
implementation as the standby/fallback path.

The third implementation slice adds the first context retrieval boundary:

- context writes retain schema, source, expires_at, and UTF-8 byte size;
  references are immutable and context reads require an explicit workspace;
- resolve_context resolves an exact reference before an exact name, while
  search_context performs deterministic workspace-scoped matching over the
  reference, name, and content and omits expired contexts;
- chunk_context returns ordered, UTF-8-safe byte-bounded chunks without
  splitting a code point;
- reduce_context creates an immutable derived context and records each input
  reference in context_lineage; context_map reads that lineage with optional
  reference, parent, and child filters;
- additive migration upgrades legacy contexts tables with the new metadata
  columns and creates the lineage indexes without replacing existing rows.

This slice deliberately keeps local document ingestion, lifecycle events,
typed handoffs, graph/decision tables, and database management for later
parity slices. Redis and embedding adapters remain out of scope until the
SQLite behavior has a broader deterministic test baseline.

The fourth implementation slice adds SQLite-backed lifecycle events and
one-shot handoffs:

- capture_event sanitizes secrets, removes caller-selected payload paths,
  bounds payload and metadata bytes, then stores the sanitized envelope behind
  an immutable workspace-scoped context reference; the idempotency key is
  replay-safe and conflicting replays are rejected;
- list_events and read_event provide deterministic workspace-scoped reads;
- handoff_begin stores owner/session/source, sharing, expiry, and state, with
  idempotency and one handoff per context in a workspace;
- list_handoffs materializes expired open handoffs as expired, while
  handoff_accept and handoff_cancel enforce one-way state transitions and keep
  actor/timestamp audit fields;
- fresh and additive legacy-table migration paths are covered by tests, and
  all handlers are reachable through the single stdio JSON-RPC dispatcher.

Database management, local document ingestion, graph/decision/provenance
tables, aggregate measurements, feedback, and the remaining fact retrieval
tools are still separate parity slices. No Redis or embedding implementation
is claimed.

The fifth implementation slice exposes the existing fact metadata columns:

- remember_fact retains source, project, domain, trust, strong, and bounded
  importance metadata while preserving workspace-scoped content deduplication;
- search_facts and list_facts accept deterministic equality filters for those
  metadata fields, and the same metadata is returned by list_forgotten;
- verify_facts recomputes SHA-256 over every visible workspace fact and reports
  the checked count plus invalid ids without mutating data;
- the protocol advertises schemas for the implemented metadata/filter tools,
  and tests cover metadata round trips, deduplication, filter isolation, and
  deliberate corruption detection.

The sixth implementation slice adds workspace-scoped graph and decision
storage:

- remember_entity canonicalizes names and preserves aliases;
- remember_relation resolves entity references, deduplicates edges, and can
  retain a source fact id;
- search_graph returns matching entities and their matching relations;
- record_decision retains category, reasoning, confidence, issue/code anchors,
  and an optional parent decision;
- query_decisions and find_precedents use the SQLite FTS5 index with a
  deterministic fallback, get_causal_chain walks parentage, and
  detect_conflicts groups distinct outcomes for the same subject/scenario.

The remaining provenance/evidence attachment, export, database management,
measurement, feedback, and advanced retrieval tools remain staged. No Redis
or embedding implementation is claimed.

The seventh implementation slice adds evidence and deterministic export:

- attach_evidence stores a workspace-scoped fact anchor with source/checksum,
  repository path and symbol, line/column ranges, selected-text SHA-256, and
  resolution status; duplicate anchors replay safely and conflicting anchors
  are rejected;
- get_provenance reads the fact's evidence anchors without exposing stored
  selected text;
- export returns a JSON-serializable workspace snapshot across facts,
  contexts, events, handoffs, graph, decisions, and evidence;
- export_rdf emits stable relation triples ordered by relation id.

Database selection/backup, aggregate measurement, feedback, and advanced
retrieval remain staged. No Redis or embedding implementation is claimed.

The eighth implementation slice adds bounded lexical ingestion and retrieval:

- absorb deduplicates and stores a batch of facts, while ingest_turn provides
  the single-turn form;
- chunk_fact reuses the UTF-8-safe byte-bound contract already used by
  chunk_context and preserves ordered chunk metadata;
- search_semantic is explicitly a deterministic SQLite lexical fallback; it
  does not claim an embedding provider or semantic similarity;
- compose_recall and search_index combine workspace-scoped fact and context
  matches into one recall payload;
- store and stdio protocol tests cover deduplication, UTF-8 boundaries,
  workspace isolation, aliases, and the recall payload.

Database selection/backup, aggregate measurement, feedback, and the remaining
advanced retrieval and document-ingestion tools remain staged. No Redis or
embedding implementation is claimed.

The ninth implementation slice adds bounded observability and classification:

- run_begin, run_end, link_run, and query_run persist idempotent workspace-scoped
  run/issue/PR/session/Git references with bounded files, diff, and summary
  fields; the server never shells out to Git;
- record_measurement and query_measurement persist aggregate numeric
  observations keyed by workspace, measurement, sample, and variant, with
  deterministic conflict detection for duplicate keys;
- record_feedback and query_feedback persist only bounded item metadata and
  one of the contract signals (`helpful`, `not_helpful`, `stale`, `irrelevant`,
  or `unsafe`), with idempotent feedback ids;
- list_categories and categorize_pending create workspace categories and assign
  them to matching unclassified active facts without crossing workspace
  boundaries;
- fresh-store migrations, JSON-RPC handlers, schemas, export fields, and tests
  cover the new tables and replay/isolation behavior.

Database selection/backup, local document path ingestion, retention/review
flows, and the remaining advanced retrieval tools remain staged. No Redis or
embedding implementation is claimed.

The tenth implementation slice adds review and abstention-facing helpers:

- facts carry validity, session, category, and access-count metadata; review
  transitions are recorded in an immutable fact_history table;
- review_pending and confirm_fact expose deterministic validity review, while
  fact_history and fact_references provide audit/provenance reads;
- facts_for_session and list_sessions provide workspace-scoped session views;
- search_guard returns a typed `ok` match or `abstain`/`no_match` result, and
  auto_orient/prepare_summary compose the existing lexical recall safely;
- summarize_index reports deterministic counts across the implemented SQLite
  surfaces, and the migration remains additive for legacy facts tables;
- tests cover the `add_fact` compatibility alias, review history, sessions,
  typed abstention, summaries, and JSON-RPC reachability.

Local document path ingestion, retention/decay policy, database selection and
backups, and optional embedding or Redis adapters remain staged. No semantic
provider is claimed by the SQLite fallback.

The eleventh implementation slice adds local-document and freshness boundaries:

- ingest_document requires an explicit local root and repository-relative path,
  admits only a regular, valid-UTF-8 file within the configured byte bound,
  rejects traversal and symlink escapes, and stores bounded chunks as immutable
  workspace contexts without persisting the root path;
- sweep_freshness and decay_sweep mark old valid active facts as `degraded` and
  record each transition in fact_history; the age threshold is explicit and
  negative thresholds are rejected;
- embed_backfill has an explicit disabled-provider result (`updated: 0`) so
  the SQLite lexical fallback is observable without claiming embeddings;
- tests cover bounded document reads, path safety, freshness transitions,
  history, and stdio reachability.

Database selection and backups, richer retention policy, and optional embedding
or Redis adapters remain staged. No provider or performance claim is made.

The twelfth implementation slice adds deterministic anchor and backup helpers:

- query_anchored searches decision issue/path/symbol anchors and evidence
  source/path/symbol anchors within one workspace;
- consolidate reports the exact-duplicate invariant enforced by
  `(sha256, workspace_id)` without silently merging semantically different
  facts;
- backup_workspace writes an atomic JSON workspace snapshot under the private
  `backups/` directory with mode 0600; explicit output arguments are restricted
  to one file name and cannot escape that directory;
- store and stdio tests cover anchor matching, consolidation reporting, and
  backup readback.

At the twelfth implementation slice, named database
selection/archive/delete and physical database backups remained staged because
the connection was intentionally single-store; the implemented backup was an
explicit workspace JSON snapshot. Optional embedding and Redis adapters
were still treated as optional and unclaimed at that historical slice.

The thirteenth implementation slice completes the SQLite database lifecycle:

- create_database initializes a safe named database under the sibling
  databases/ directory, and list_databases reports active, named, and
  archived files;
- select_database swaps the single-process connection to an existing named
  store, while archive_database and delete_database reject the active store;
- reset_database clears the selected store or an inactive named store without
  changing its schema;
- backup_database uses SQLite VACUUM INTO a private temporary file and then
  atomically publishes a mode-0600 file under `backups/`, so active WAL state
  is included without accepting arbitrary output paths;
- store and stdio tests cover isolation across selected databases, archive and
  deletion safeguards, reset behavior, path validation, and backup readback.

The SQLite implementation now covers the file-backed database lifecycle in
the pinned tool inventory. Database files are opened as regular mode-0600
files and symbolic-link database paths are rejected.

The fourteenth implementation slice adds a bounded performance harness and an
optional Redis core-fact adapter:

- memory-bench measures the same persistent-connection workload against
  SQLite, and can run the corresponding namespaced fact workload against a
  configured Redis endpoint;
- the Redis adapter uses a persistent RESP2 connection, supports password-only
  and username/password AUTH plus optional database selection, hashed
  workspace/fact keys, SET-NX deduplication, and deterministic list/search/reset
  operations;
- Redis configuration is optional: no URL, an invalid endpoint, or an
  unavailable service selects the SQLite fallback without exposing connection
  credentials. The bundled plaintext RESP adapter accepts only loopback
  endpoints; remote deployments must provide a local TLS proxy or sidecar;
- the benchmark emits the selected backend and fallback reason, but keeps
  performance efficacy explicitly not claimed until a paired workload and
  environment review is available.

This was the initial experimental core-fact comparison surface. The subsequent
coordinator implementation below supersedes it: Redis now owns the canonical
state for the full MCP contract when reachable, while SQLite remains the
standby/fallback image.

## Current coordinator implementation

The first Redis-primary implementation slice is now wired into the stdio
server through `BackendCoordinator`:

- Redis stores a bounded, namespaced SQLite state snapshot and a monotonic
  revision; `WATCH`/`MULTI`/`EXEC` protects the revision-checked publish;
- while Redis is healthy, the coordinator restores a private in-memory
  materialization from the Redis snapshot and executes all 80 advertised tools
  plus the `add_fact` compatibility alias against that Redis-owned state; the
  file-backed SQLite store is updated by a background pointwise replay after
  the Redis revision commits and is used as standby/fallback;
- the coordinator selects Redis when its probe succeeds, otherwise serves the
  complete SQLite implementation, records degraded writes in a durable,
  idempotent JSONL outbox, and reconciles back with Redis priority;
- each stateful Redis publish can atomically record the SHA-256 idempotency
  marker(s) that produced the revision; recovery checks those markers before
  replay and keeps them for a bounded seven-day duplicate-replay detection
  window;
- the watcher reads only the small revision key while the state is unchanged,
  fetches a snapshot only after a revision change, uses bounded backoff, and
  stops with the coordinator lifecycle;
- the watcher makes a complete compatibility snapshot checkpoint only after
  each bounded 256-revision interval, so restart recovery remains bounded
  without copying the database on every write;
- each state-changing operation also receives a system projection for database
  metadata, and each touched workspace receives a
  bounded native entity projection: hashed workspace/index keys point to
  individually addressable JSON records for facts, contexts, events,
  fact history, context lineage, handoffs, graph, decisions, evidence,
  categories, runs, measurements, feedback, and registered workspaces;
- normal native writes apply only changed entities and removed keys; the
  pointwise projection, monotonic revision, schema marker, manifests, and
  durable operation ledger are committed in one `WATCH`/`MULTI`/`EXEC`
  transaction. A per-workspace manifest records the projection schema
  version, revision, and bounded entity count. A version-2 native schema
  marker makes older snapshot-only namespaces rebuild on attach;
- the operation ledger is keyed by the SHA-256 operation idempotency key and
  has no TTL. It stores only operation name, workspace hash, status, revision,
  entity count, and a bounded conflict reason. The seven-day marker remains a
  compatibility fast path, while recovery consults the durable ledger first;
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

The original backend requirement is restored and supersedes the optional-adapter
policy above for the next implementation stages:

- when the configured Redis endpoint passes the reachability/authentication
  probe, Redis is the primary backend and every advertised MCP operation,
  including database and workspace lifecycle operations, is routed through
  that backend;
- when Redis is not configured or cannot be reached, the complete operation
  surface starts on the existing SQLite backend as the fallback;
- while Redis is primary, a background coordinator keeps a SQLite hot standby
  current from confirmed Redis revisions;
- if the active Redis connection is lost, the coordinator serves the last
  confirmed SQLite revision and records every degraded-mode write in a durable,
  idempotent outbox;
- after Redis recovers, the coordinator uses the local SQLite fallback image to
  publish queued point changes, gives the Redis revision priority for
  conflicts, mirrors committed operations back into SQLite, and switches
  normal traffic to Redis;
- the pointwise state publish and its operation marker(s) are one Redis
  transaction; a complete snapshot is reserved for attach, migration, recovery,
  and the amortized 256-revision restart checkpoint; it is not part of each
  write;
  recovery treats an existing marker as already committed and removes the
  corresponding outbox item without applying it a second time; markers expire
  after seven days, so duplicate-replay protection is intentionally bounded;
- a partial Redis route is not an acceptable intermediate claim: an operation
  must not silently use SQLite while Redis is the selected backend;
- an acknowledged Redis write must be present in the Redis revision stream
  before the response is returned; an acknowledged degraded-mode write must be
  present in the SQLite database and outbox before the response is returned;
- the coordinator must expose standby lag and reconciliation state without
  exposing payloads or credentials;
- the background synchronizer must be event/revision driven and bounded: it
  polls a small health/revision key at a configurable interval, fetches the
  state only when the revision changes, applies bounded batches, checkpoints
  the compatibility snapshot only at the 256-revision boundary, and backs off
  on errors; it must not rescan or rewrite the complete dataset on every tick;
- the Redis watcher must share the coordinator's connection/lifecycle, avoid a
  busy loop, use bounded timeouts, and stop cleanly with the process;
- the full 80-tool route is an acceptance gate: every advertised tool and the
  `add_fact` alias must cross the same coordinator, with no direct dispatcher
  path that silently bypasses Redis, the standby, or the outbox;
- Redis may be configured with an explicit `MEMORY_MCP_REDIS_URL`/`REDIS_URL`
  or with host, port, database, user, and password variables; explicit URLs
  take precedence, loopback-only endpoint validation is applied before connect,
  and credentials remain environment-only and are never emitted in logs,
  reports, test fixtures, or protocol responses.

Option A is now the selected implementation model: Redis owns the canonical
revision and the complete operation state, while the compatibility engine is
rebuilt from that state for each active process. Normal writes use atomic
pointwise projection deltas; the version-2 schema marker and atomic full
projection publish migrate legacy snapshot-only namespaces;
the virtual database catalog is included in the Redis snapshot and database
metadata is independently indexed under the reserved system scope. Remaining
release acceptance is limited to real-service migration/isolation/conflict/
lag/backoff measurements, the formatter/test/lint/AppSec/artifact/QA/PM gates,
and a separate performance decision. No speedup claim is made here.

The architecture and recovery protocol are recorded in
`docs/decisions/ADR-0001-redis-primary-with-sqlite-fallback.md`; the pointwise
write refinement is recorded in
`docs/decisions/ADR-0002-pointwise-redis-replication.md`. ADR-0001 remains
accepted for this implementation slice after the PM, QA, AppSec, artifact, and
release readbacks. A reachable Redis endpoint now selects the coordinator's
Redis-primary pointwise mode, and an unavailable endpoint falls back to SQLite
without enabling a partial fact-only route; the recorded benchmark makes no
speedup claim.
