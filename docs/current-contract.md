# Current memory-mcp contract

This document is the compatibility baseline for the Rust rewrite. It records
the behavior of the upstream Python server at commit
[`13d30a0f840e49b71a983609fd4a180e31ff219c`](https://github.com/maxivillus/memory-mcp/tree/13d30a0f840e49b71a983609fd4a180e31ff219c).
The upstream `memory_mcp.py`, its `TOOLS` map, and the test suites remain the
source of truth until a Rust parity test replaces each item below.

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

1. a single executable entry point with the wire behavior above;
2. a schema/migration test fixture covering a fresh store and legacy stores;
3. a machine-readable tool-schema parity check against the 80-name inventory;
4. deterministic tests for the core fact/context round trips before optional
   Redis or embedding adapters are introduced.

No performance claim is made by this baseline. Benchmarking starts after the
SQLite parity slice is working, and Redis remains an optional measured adapter
with deterministic SQLite fallback.

## Rust parity slice status

The first implementation slice now provides one stdio executable, bundled
SQLite/FTS5 initialization, a fresh-store round trip, a legacy `facts`-table
upgrade fixture, and an 80-name inventory gate. The generic entries returned by
`tools/list` intentionally mark per-tool schema/handler parity as incremental;
the pinned upstream descriptions and input schemas remain the acceptance source
for the subsequent port stages.

The second implementation slice adds SQLite-backed fact lifecycle operations
(`forget_fact`, `restore_fact`, and `list_forgotten`) plus workspace lifecycle
operations (`create_workspace`, `list_workspaces`, `archive_workspace`, and
`reset_workspace`). Forgotten facts are excluded from normal list/search results,
and reset deletes only the selected workspace's facts and contexts. Redis remains
out of scope until the SQLite behavior is measured and the parity surface is
complete enough for a meaningful adapter comparison.

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

- capture_event stores event type, metadata, payload hash/size/truncation, and
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
