# memory-mcp-rust

`memory-mcp-rust` is a Rust MCP server: one executable, newline-delimited
JSON-RPC 2.0, and 80 advertised tools for durable, searchable memory.

The default backend is bundled SQLite with FTS5. When a supported loopback
Redis endpoint is configured and reachable, Redis becomes the primary backend
for the complete tool surface; SQLite remains the hot standby and fallback.
The server is an advisory memory store, not an authorization or safety
decision source.

## What it provides

- MCP over stdin/stdout: one JSON-RPC request per line and one newline-delimited
  response per request with an id. Notifications produce no response;
  diagnostics go to stderr.
- The server identity `memory-mcp` version `0.23.0` during initialization.
- Exactly 80 tools in `tools/list`; the compatibility alias `add_fact` is
  callable but intentionally not advertised.
- Durable facts, immutable context artifacts, lifecycle events, typed
  handoffs, run records, measurements, provenance, graphs, decisions,
  databases, and workspaces.
- Optional local/provider-backed embeddings, extraction, recall, verification,
  and categorization, each behind an explicit environment flag.

## Technologies and design ideas

| Layer | Choice | Why it matters |
| --- | --- | --- |
| Runtime | Rust 2021 | A single process with explicit ownership and predictable resource bounds. |
| Wire protocol | MCP over JSON-RPC 2.0, newline-delimited stdio | Works with MCP clients that launch a command and connect stdin/stdout directly. |
| Durable store | SQLite via `rusqlite` with the `bundled` feature, plus FTS5 | The default install has no database service dependency and still provides full-text search. |
| Serialization and integrity | `serde`, `serde_json`, SHA-256 | Stable JSON contracts, checksums, deduplication, and idempotency markers. |
| Redis integration | Bounded in-tree RESP2 adapter | Redis can own the complete state without adding a client-library runtime dependency. |
| Optional intelligence | Loopback HTTP adapters and deterministic `test` providers | Extraction, embeddings, recall, verification, and categorization stay opt-in and advisory. |

The main design boundaries are stable protocol contracts, bounded payloads,
immutable context references, explicit workspace scope, idempotent state
changes, and safe recovery. Model output can suggest a fact or category, but only a human
review path can make it trusted; retrieved memory never authorizes a
safety-critical action.

## Quick start

### Build

Install a current Rust toolchain, then run:

```sh
cargo build --release
```

The binary is written to `target/release/memory-mcp-rust`.

### Run the stdio server

The server creates `data/facts.db` relative to its working directory unless
`MEMORY_MCP_DB` is set. Set the path explicitly for a client or container:

```sh
MEMORY_MCP_DB="$PWD/data/facts.db" \
  ./target/release/memory-mcp-rust
```

The process is an MCP transport, not an interactive shell. A small protocol
smoke test looks like this:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-test","version":"1"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"remember_fact","arguments":{"text":"Rust memory server is running","workspace":"demo"}}}' \
  '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"search_facts","arguments":{"query":"Rust","workspace":"demo"}}}' \
  | MEMORY_MCP_DB="$PWD/data/facts.db" ./target/release/memory-mcp-rust
```

For an MCP client that accepts a command and environment map, the equivalent
configuration is:

```json
{
  "mcpServers": {
    "memory-mcp": {
      "command": "/absolute/path/to/target/release/memory-mcp-rust",
      "env": {
        "MEMORY_MCP_DB": "/absolute/path/to/data/facts.db"
      }
    }
  }
}
```

The exact configuration key is client-specific; keep the server attached to
stdin/stdout and give its database directory write access.

## How the backend works

```text
MCP client
    │ newline-delimited JSON-RPC over stdio
    ▼
BackendCoordinator ── Redis reachable ──► Redis primary
       │                                  │
       │                                  └─ pointwise mirror → SQLite standby
       │
       └─ Redis absent/unreachable ──────► SQLite fallback + durable outbox
                                              │
                                              └─ recovery reconciliation → Redis
```

The coordinator keeps the public compatibility engine behind one backend
boundary. In Redis-primary mode, successful state changes are published as
bounded pointwise deltas with a revision and idempotency marker; SQLite is
updated after the Redis commit. During an outage, writes are committed to
SQLite and the durable outbox before acknowledgement. Recovery reconciles the
outbox with Redis, gives Redis priority on conflicts, refreshes SQLite, and
only then switches normal traffic back.

A partial Redis mode is not supported: all advertised tools and the `add_fact`
alias cross the same coordinator. The implementation is correctness-first;
the repository makes no general Redis speed-up claim. See
[`docs/performance.md`](docs/performance.md) for the bounded benchmark and
measurement rules.

### Backend configuration

| Variable | Meaning |
| --- | --- |
| `MEMORY_MCP_DB` | SQLite path. Defaults to `data/facts.db` relative to the process working directory. |
| `MEMORY_MCP_REDIS_URL` / `REDIS_URL` | Redis URL. An explicit URL takes precedence over host fields. |
| `MEMORY_MCP_REDIS_HOST` / `REDIS_HOST` | Redis host when no URL is set. Only loopback hosts are accepted. |
| `MEMORY_MCP_REDIS_PORT` / `REDIS_PORT` | Redis port used with host configuration. |
| `MEMORY_MCP_REDIS_DATABASE` / `MEMORY_MCP_REDIS_DB` and `REDIS_DATABASE` / `REDIS_DB` | Optional Redis database number. |
| `MEMORY_MCP_REDIS_USERNAME` / `MEMORY_MCP_REDIS_USER` and `REDIS_USERNAME` / `REDIS_USER` | Optional Redis username. |
| `MEMORY_MCP_REDIS_PASSWORD` / `MEMORY_MCP_REDIS_PASS` and `REDIS_PASSWORD` / `REDIS_PASS` | Optional Redis password; keep it in the runtime secret store. |
| `MEMORY_MCP_REDIS_NAMESPACE` | Redis key namespace. Defaults to `memory-mcp`. |
| `MEMORY_MCP_REDIS_WATCH_INTERVAL_MS` | Bounded standby/recovery watcher interval. Default is 5 seconds. |
| `MEMORY_MCP_REDIS_MAX_BACKOFF_MS` | Maximum watcher backoff after an error. Default is 60 seconds. |

Redis endpoint validation is deliberately loopback-only. A separate Docker
service hostname or remote Redis endpoint is rejected by the binary; use a
local TLS proxy or sidecar if a remote service is required. Redis credentials
must never appear in logs, reports, fixtures, or MCP responses.

## Optional provider features

The SQLite/FTS5 core works without any model or network service. Optional
features are disabled unless their flag is exactly `1`:

| Flag | Enables | Provider settings |
| --- | --- | --- |
| `MEMORY_MCP_EMBEDDINGS=1` | Embedding-backed `search_semantic`, hybrid `search_facts`, and `embed_backfill`. | `MEMORY_MCP_EMBED_PROVIDER` (`ollama` by default, `openai`, `fastembed`, or deterministic `test`), plus `_URL` and `_MODEL`. `fastembed` currently reports unavailable in the single binary. |
| `MEMORY_MCP_EXTRACT=1` | LLM-backed `ingest_turn` conversation extraction. | `MEMORY_MCP_LLM_PROVIDER`, `MEMORY_MCP_LLM_URL`, `MEMORY_MCP_LLM_MODEL`; minimum transcript length uses `MEMORY_MCP_EXTRACT_MIN_CHARS` and defaults to 800. |
| `MEMORY_MCP_RECALL=1` | `compose_recall` and `sweep_freshness`. | Uses the LLM/embedding settings when the selected operation needs them. |
| `MEMORY_MCP_VERIFY=1` | `verify_facts`, `consolidate`, and verification during ingestion. | `MEMORY_MCP_VERIFY_MIN_CONFIDENCE` defaults to `0.8`. |
| `MEMORY_MCP_CATEGORIZE=1` | LLM batch categorization through `categorize_pending`. | Uses `MEMORY_MCP_LLM_*` settings. |
| `MEMORY_MCP_CONTEXT_MAP=1` | Opt-in `context_map` repository-context manifest. | No model is required. The result remains bounded and advisory. |

The LLM provider defaults to `ollama` with model `qwen2.5:14b`; the embedding
provider defaults to `ollama` with model `nomic-embed-text`. Local Ollama is the
simplest private setup. Provider HTTP endpoints must be loopback-only, and the
current adapter rejects credential-bearing plaintext HTTP; put remote services
behind a local encrypted gateway/sidecar. Model output is treated as an
unconfirmed candidate and never grants authority.

## Tool catalog

The table below covers every advertised tool. Exact parameter names, required
fields, defaults, enums, and bounds are defined by the Rust tool descriptors and
verified by protocol tests. Most workspace-aware tools accept either `workspace`
or `workspace_id`; context operations require an explicit non-empty workspace.

### Facts, retrieval, lifecycle, and review

| Tool | Purpose |
| --- | --- |
| `remember_fact` | Upsert a durable fact, deduplicated by SHA-256 of its text; fact text is capped at 16,000 characters and strict admission stores only evidence hashes/metadata. |
| `absorb` | Preview or explicitly commit a bounded batch of candidate facts; exact duplicates are no-ops and related/update/contradiction candidates remain review-only. |
| `chunk_fact` | Read one active fact as bounded, offset-addressable chunks instead of returning its full text in one payload. |
| `search_facts` | Advisory FTS5 search over facts with bounded results; optional `semantic=true` merges lexical and embedding rankings when embeddings are enabled. |
| `search_semantic` | Advisory embedding search over stored facts; requires `MEMORY_MCP_EMBEDDINGS=1` and cannot authorize safety-critical work. |
| `embed_backfill` | Compute missing fact embeddings after embeddings have been enabled. |
| `ingest_turn` | Extract candidate facts from a conversation transcript through the configured LLM provider; model authority stays unconfirmed until review. |
| `compose_recall` | Build an advisory `<memory-recall>` block focused on the latest user intent; rejects `purpose="safety_critical"` and requires `MEMORY_MCP_RECALL=1`. |
| `auto_orient` | Build one bounded first-input recall block for a runtime session, capped at six hits and 2.5 seconds, with silent degradation on failure. |
| `search_guard` | Return a non-blocking hint after repeated external searches without a memory lookup; use `action="memory"` after consulting memory. |
| `sweep_freshness` | Archive facts older than their type-specific retention window while keeping strong facts; requires `MEMORY_MCP_RECALL=1`. |
| `decay_sweep` | Recompute active-day decay for facts; degraded and forgotten facts follow the lifecycle thresholds, while strong/confirmed facts do not decay. |
| `verify_facts` | Ask the configured LLM to cross-check a fact against stored facts and report conflicts or supersessions; requires `MEMORY_MCP_VERIFY=1`. |
| `consolidate` | LLM-merge paraphrased facts into one fact and invalidate the inputs bi-temporally; strong/confirmed facts are protected and verification must be enabled. |
| `fact_history` | Walk one fact's bi-temporal `superseded_by` chain from oldest to newest. |
| `review_pending` | List active, unconfirmed facts in importance order for human review. |
| `confirm_fact` | Mark one fact as human-confirmed with high trust. |
| `facts_for_session` | List active facts recorded from one session, ordered by importance. |
| `list_sessions` | List session sources with active-fact counts, freshest first. |
| `fact_references` | Show one fact's supersession chain, consolidation links, and evidence impact. |
| `list_facts` | List recent non-archived facts with optional project, domain, and category filters. |
| `summarize_index` | Return a compact, capped one-line-per-fact index for prompt budgets, including category tags. |
| `list_categories` | Return the workspace card catalog with active/total fact counts and optional name filtering. |
| `search_index` | Find short, category-grouped fact snippets; use `get_provenance` for a full fact and evidence. |
| `categorize_pending` | Assign categories to uncategorized facts in an LLM batch; requires `MEMORY_MCP_CATEGORIZE=1`. |
| `forget_fact` | Soft-delete a fact by id or SHA-256 without immediately destroying its stored history. |
| `list_forgotten` | List forgotten facts in an explicit workspace for direct review. |
| `restore_fact` | Return a forgotten or degraded fact to the active lifecycle and reset its revival counter. |
| `export` | Export all facts, including archived rows, as JSON for migration or backup. |

### Immutable contexts and local documents

| Tool | Purpose |
| --- | --- |
| `put_context` | Store an immutable named context artifact and return its reference, checksum, metadata, and optional lineage. |
| `ingest_document` | Preview or commit one UTF-8 document below an explicit local root as bounded, immutable workspace-scoped chunks; the root path is never stored or returned. |
| `list_context` | List context metadata only; payloads are never returned by the catalog, and expired/out-of-scope refs stay hidden. |
| `resolve_context` | Resolve one context by exact reference or name and return bounded metadata/lineage without its payload. |
| `read_context` | Read a bounded character slice from one non-expired context reference. |
| `search_context` | Search context names, metadata, and payloads inside one workspace while returning metadata only. |
| `chunk_context` | Read an ordered sequence of bounded, UTF-8-safe context chunks with server-enforced response and workspace limits. |
| `reduce_context` | Create a new immutable context by deterministically joining ordered references; this is concatenation, not semantic summarization. |
| `context_map` | Return an opt-in bounded repository-context manifest over existing anchors and run history; it stores no source code and remains advisory. |

### Events and handoffs

| Tool | Purpose |
| --- | --- |
| `capture_event` | Capture one sanitized, byte-bounded lifecycle envelope behind an immutable context ref with idempotent retries. |
| `list_events` | List lifecycle-event metadata in one exact workspace. |
| `read_event` | Read one bounded sanitized lifecycle envelope by event reference or idempotency key. |
| `handoff_begin` | Create an expiring typed handoff over one immutable context with owner, workspace, checksum, and optional idempotency. |
| `list_handoffs` | List typed handoff metadata and materialize expired open rows as expired before readback. |
| `handoff_accept` | Atomically accept one open handoff once and return one bounded payload slice after owner/shared, workspace, cwd, and expiry checks. |
| `handoff_cancel` | Cancel one open handoff exactly once; only the owner may cancel it and terminal rows remain auditable. |

### Runs, measurements, summaries, and feedback

| Tool | Purpose |
| --- | --- |
| `run_begin` | Open an idempotent run record for one execution window such as an issue or task turn. |
| `run_end` | Close a run with bounded client-supplied Git facts, changed files, and diff; the server never shells out to Git. |
| `link_run` | Bind a run to issue or pull-request references; at least one reference is required. |
| `query_run` | Read one run or a bounded filtered list by state or issue reference. |
| `record_measurement` | Record one aggregate-only baseline or memory observation for a paired sample; prompts and payloads are rejected. |
| `query_measurement` | Summarize complete baseline/memory pairs with bounded median and p95 metrics; remains `not_claimed` until the required pairs exist. |
| `prepare_summary` | Assemble a ready-to-post Markdown summary from the run's own records; it never posts the result. |
| `query_anchored` | Advisory lookup of facts and decisions attached to a repository path or symbol; it cannot authorize safety-critical work. |
| `record_feedback` | Record one retry-safe aggregate usage signal without free-text notes or raw payloads. |
| `query_feedback` | Return bounded aggregate feedback counts and metadata for one exact workspace. |
| `stats` | Return bounded store statistics, provenance/run/measurement counts, access counts, and pull hit-rate telemetry. |

### Graph, decisions, provenance, and export

| Tool | Purpose |
| --- | --- |
| `remember_entity` | Upsert an entity node with a workspace-local name, type, and optional aliases. |
| `remember_relation` | Record and deduplicate a subject-predicate-object edge; referenced entities are created automatically. |
| `search_graph` | Run a bounded breadth-first search over relations in both directions. |
| `record_decision` | Persist a decision with category, scenario, reasoning, outcome, confidence, maker, issue/code anchors, and optional parent. |
| `query_decisions` | List decisions with filters for category, subject, outcome, maker, issue, path, or symbol. |
| `find_precedents` | Advisory BM25 lookup of similar decision scenarios; it cannot authorize safety-critical work. |
| `get_causal_chain` | Walk `parent_decision_id` links from a decision to its oldest root. |
| `get_provenance` | Return one fact together with its evidence rows and optional repository/path/symbol/line-range anchors. |
| `attach_evidence` | Link a fact to a source and optional code-local anchor, deduplicated by fact and source reference. |
| `detect_conflicts` | Find near-duplicate facts and decisions with the same subject but distinct outcomes. |
| `export_rdf` | Export bounded W3C PROV-flavoured Turtle records for facts, entities, relations, decisions, evidence, and supersession edges. |

### Databases and workspaces

| Tool | Purpose |
| --- | --- |
| `create_database` | Create a new named SQLite database under `databases/`; the active store cannot be recreated. |
| `list_databases` | List the active and named databases, including archived entries. |
| `archive_database` | Soft-archive a named database by renaming it to `<name>.db.archived`; hard deletion requires `hard=true` and `confirm=true`. |
| `backup_database` | Back up the selected, active, or named archived database through SQLite's online backup API. |
| `delete_database` | Permanently delete a named database only with `confirm=true`; the active and selected databases are protected. |
| `select_database` | Point subsequent tools in the current session at a named database; create it first with `create_database`. |
| `current_database` | Return the name of the database selected for the current session. |
| `reset_database` | Return the current session to the active `MEMORY_MCP_DB` store. |
| `create_workspace` | Register or reactivate a named workspace access scope in the active database. |
| `list_workspaces` | List workspace status and full data counts for facts, entities, relations, decisions, evidence, and related records. |
| `reset_workspace` | Soft-reset a workspace by hiding its data; hard purge requires `hard=true` and `confirm=true`. |
| `archive_workspace` | Soft-archive a workspace by hiding its data; hard purge requires `hard=true` and `confirm=true`. |
| `backup_workspace` | Export versioned, schema-complete workspace data as private JSON with per-table counts. |

### Compatibility alias

`add_fact` is handled by the server as a compatibility alias for fact creation,
but it is intentionally absent from `tools/list`. Clients should prefer
`remember_fact` when they can choose the name.

## Docker

The repository does not publish an image or ship a Dockerfile. The following
is a minimal example for building a local image around the release binary; pin
base-image tags in a production build:

Save this snippet as `Dockerfile` when using the example:

```dockerfile
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/target/release/memory-mcp-rust /usr/local/bin/memory-mcp-rust
ENTRYPOINT ["/usr/local/bin/memory-mcp-rust"]
```

Build and attach the container as an interactive stdio process with a named
volume for the SQLite directory:

```sh
docker build -t memory-mcp-rust:local .
docker volume create memory-mcp-data
docker run --rm -i \
  --mount type=volume,src=memory-mcp-data,dst=/var/lib/memory-mcp \
  -e MEMORY_MCP_DB=/var/lib/memory-mcp/facts.db \
  memory-mcp-rust:local
```

Do not run the server detached when an MCP client owns its stdin/stdout. A
separate `redis` Docker service is not a supported Redis endpoint by default:
the current safety boundary accepts loopback Redis only. Leave Redis variables
unset for the SQLite fallback, or place a supported local proxy/sidecar in
front of remote Redis and point the binary at its loopback listener.

## Development

Useful local checks:

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

The repository layout follows the runtime boundary:

| Path | Responsibility |
| --- | --- |
| `src/main.rs` | Stdio entry point and command dispatch. |
| `src/protocol.rs` | JSON-RPC/MCP request handling and tool dispatch. |
| `src/tools.rs` | Advertised tool names and descriptor schemas. |
| `src/store.rs` | SQLite schema, FTS5 indexes, upgrades, and operation semantics. |
| `src/backend.rs` | Redis/SQLite selection, standby, outbox, reconciliation, and watcher. |
| `src/redis.rs` | Bounded RESP2 connection and Redis state/projection operations. |
| `src/pipeline.rs` and `src/providers.rs` | Optional extraction, recall, verification, categorization, and embedding adapters. |
| `docs/current-contract.md` | Current protocol and safety contract. |
| `docs/decisions/` | Architecture decisions for Redis-first storage and pointwise replication. |
| `docs/documentation-roadmap.md` | Scope and verification record for the documentation set. |

## Further reading

- [`docs/current-contract.md`](docs/current-contract.md) — protocol, bounds,
  persistence, parity, and current backend contract.
- [`docs/performance.md`](docs/performance.md) — benchmark procedure and why
  performance efficacy remains `not_claimed`.
- [`docs/decisions/ADR-0001-redis-primary-with-sqlite-fallback.md`](docs/decisions/ADR-0001-redis-primary-with-sqlite-fallback.md)
  and [`docs/decisions/ADR-0002-pointwise-redis-replication.md`](docs/decisions/ADR-0002-pointwise-redis-replication.md)
  — the selected backend design.
