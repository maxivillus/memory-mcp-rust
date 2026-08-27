use serde_json::{json, Value};

/// The advertised names from the pinned upstream `TOOLS` map.
///
/// `add_fact` is intentionally absent: upstream keeps it as a compatibility
/// handler alias but does not advertise it to clients.
pub const TOOL_NAMES: [&str; 80] = [
    "remember_fact",
    "absorb",
    "chunk_fact",
    "put_context",
    "ingest_document",
    "list_context",
    "resolve_context",
    "read_context",
    "search_context",
    "chunk_context",
    "reduce_context",
    "capture_event",
    "list_events",
    "read_event",
    "handoff_begin",
    "list_handoffs",
    "handoff_accept",
    "handoff_cancel",
    "run_begin",
    "run_end",
    "link_run",
    "query_run",
    "record_measurement",
    "query_measurement",
    "prepare_summary",
    "query_anchored",
    "context_map",
    "search_facts",
    "search_semantic",
    "embed_backfill",
    "ingest_turn",
    "compose_recall",
    "auto_orient",
    "search_guard",
    "sweep_freshness",
    "verify_facts",
    "consolidate",
    "fact_history",
    "review_pending",
    "confirm_fact",
    "facts_for_session",
    "list_sessions",
    "fact_references",
    "export_rdf",
    "list_facts",
    "summarize_index",
    "list_categories",
    "search_index",
    "categorize_pending",
    "remember_entity",
    "remember_relation",
    "search_graph",
    "record_decision",
    "query_decisions",
    "find_precedents",
    "get_causal_chain",
    "get_provenance",
    "attach_evidence",
    "detect_conflicts",
    "forget_fact",
    "stats",
    "record_feedback",
    "query_feedback",
    "export",
    "create_database",
    "list_databases",
    "archive_database",
    "backup_database",
    "delete_database",
    "select_database",
    "current_database",
    "reset_database",
    "create_workspace",
    "list_workspaces",
    "reset_workspace",
    "archive_workspace",
    "backup_workspace",
    "decay_sweep",
    "list_forgotten",
    "restore_fact",
];

pub fn advertised_tools() -> Vec<Value> {
    serde_json::from_str(include_str!("../docs/upstream-tools.json"))
        .expect("embedded upstream tool contract is valid JSON")
}

#[allow(dead_code)]
fn tool_contract(name: &str) -> (&'static str, Value) {
    match name {
        "remember_fact" => (
            "Store a deduplicated fact with optional provenance metadata; text is limited to 16000 characters.",
            json!({
                "type": "object",
                "properties": {
                    "text": {"type": "string", "maxLength": 16000},
                    "source": {"type": "string"},
                    "project": {"type": "string"},
                    "domain": {"type": "string"},
                    "trust": {"type": "string", "enum": ["high", "medium", "low"]},
                    "strong": {"type": "boolean"},
                    "importance": {"type": "number", "minimum": 0, "maximum": 1},
                    "validity": {"type": "string", "enum": ["valid", "pending", "invalid"]},
                    "session_id": {"type": "string"},
                    "session": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["text"]
            }),
        ),
        "absorb" => (
            "Deduplicate and store one or more facts.",
            json!({
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "texts": {"type": "array", "items": {"type": "string"}},
                    "facts": {"type": "array", "items": {"type": "string"}},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                }
            }),
        ),
        "ingest_turn" => (
            "Ingest one turn as a deduplicated fact.",
            json!({
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "turn": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["text"]
            }),
        ),
        "review_pending" => (
            "List active facts that require validity review.",
            workspace_schema(),
        ),
        "confirm_fact" => (
            "Confirm a fact and record the review transition.",
            json!({
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "fact_id": {"type": "integer"},
                    "note": {"type": "string"},
                    "reason": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["id", "workspace"]
            }),
        ),
        "fact_history" => (
            "Read immutable fact lifecycle history.",
            json!({
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "fact_id": {"type": "integer"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["id", "workspace"]
            }),
        ),
        "facts_for_session" => (
            "List active facts associated with a session.",
            json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string"},
                    "session": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["session_id", "workspace"]
            }),
        ),
        "list_sessions" => (
            "List session identifiers associated with active facts.",
            workspace_schema(),
        ),
        "fact_references" => (
            "List evidence references attached to a fact.",
            json!({
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "fact_id": {"type": "integer"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["id", "workspace"]
            }),
        ),
        "search_guard" => (
            "Return a typed lexical recall result or abstain on no match.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query", "workspace"]
            }),
        ),
        "auto_orient" => (
            "Compose workspace-scoped recall for orientation.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query", "workspace"]
            }),
        ),
        "summarize_index" => (
            "Return deterministic workspace index counts.",
            workspace_schema(),
        ),
        "prepare_summary" => (
            "Prepare index counts and lexical recall for a workspace.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        "sweep_freshness" | "decay_sweep" => (
            "Mark valid active facts older than a bounded age as degraded.",
            json!({
                "type": "object",
                "properties": {
                    "max_age_seconds": {"type": "integer", "minimum": 0},
                    "max_age": {"type": "integer", "minimum": 0},
                    "ttl": {"type": "integer", "minimum": 0},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        "embed_backfill" => (
            "Report the explicit disabled-provider embedding fallback.",
            workspace_schema(),
        ),
        "consolidate" => (
            "Report deterministic exact-duplicate consolidation for active facts.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        "search_facts" => (
            "Search active facts with optional provenance filters.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "source": {"type": "string"},
                    "project": {"type": "string"},
                    "domain": {"type": "string"},
                    "trust": {"type": "string", "enum": ["high", "medium", "low"]},
                    "strong": {"type": "boolean"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query"]
            }),
        ),
        "list_facts" => (
            "List active facts with optional provenance filters.",
            json!({
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "project": {"type": "string"},
                    "domain": {"type": "string"},
                    "trust": {"type": "string", "enum": ["high", "medium", "low"]},
                    "strong": {"type": "boolean"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                }
            }),
        ),
        "verify_facts" => (
            "Verify stored fact SHA-256 hashes in a workspace.",
            workspace_schema(),
        ),
        "chunk_fact" => (
            "Return ordered UTF-8-safe byte-bounded fact chunks.",
            json!({
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "fact_id": {"type": "integer"},
                    "max_bytes": {"type": "integer", "minimum": 1},
                    "chunk_size": {"type": "integer", "minimum": 1},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["id"]
            }),
        ),
        "search_semantic" => (
            "Search facts using the deterministic SQLite lexical fallback.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query"]
            }),
        ),
        "compose_recall" | "search_index" => (
            "Compose a workspace-scoped lexical recall from facts and contexts.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query", "workspace"]
            }),
        ),
        "run_begin" => (
            "Begin an idempotent bounded run record.",
            json!({
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "id": {"type": "string"},
                    "issue_ref": {"type": "string"},
                    "issue": {"type": "string"},
                    "pr_ref": {"type": "string"},
                    "pr": {"type": "string"},
                    "session": {"type": "string"},
                    "git_ref": {"type": "string"},
                    "ref": {"type": "string"},
                    "commit": {"type": "string"},
                    "files": {},
                    "changed_files": {},
                    "diff": {},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["run_id", "workspace"]
            }),
        ),
        "run_end" => (
            "Close a run idempotently with an optional bounded summary.",
            json!({
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "id": {"type": "string"},
                    "summary": {"type": "string"},
                    "result": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["run_id", "workspace"]
            }),
        ),
        "link_run" => (
            "Attach issue, pull request, session, or Git references to a run.",
            json!({
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "id": {"type": "string"},
                    "issue_ref": {"type": "string"},
                    "issue": {"type": "string"},
                    "pr_ref": {"type": "string"},
                    "pr": {"type": "string"},
                    "session": {"type": "string"},
                    "git_ref": {"type": "string"},
                    "ref": {"type": "string"},
                    "commit": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["run_id", "workspace"]
            }),
        ),
        "query_run" => (
            "Query bounded run records in a workspace.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        "record_measurement" => (
            "Record one idempotent aggregate measurement observation.",
            json!({
                "type": "object",
                "properties": {
                    "measurement": {"type": "string"},
                    "name": {"type": "string"},
                    "sample": {"type": "string"},
                    "sample_id": {"type": "string"},
                    "variant": {"type": "string"},
                    "value": {"type": "number"},
                    "baseline": {"type": "boolean"},
                    "is_baseline": {"type": "boolean"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["measurement", "sample", "value", "workspace"]
            }),
        ),
        "query_measurement" => (
            "Query aggregate measurement observations in a workspace.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        "record_feedback" => (
            "Record bounded idempotent feedback for a memory item.",
            json!({
                "type": "object",
                "properties": {
                    "feedback_id": {"type": "string"},
                    "id": {"type": "string"},
                    "site": {"type": "string"},
                    "item_type": {"type": "string"},
                    "type": {"type": "string"},
                    "item_ref": {"type": "string"},
                    "ref": {"type": "string"},
                    "signal": {"type": "string", "enum": ["helpful", "not_helpful", "stale", "irrelevant", "unsafe"]},
                    "query_hash": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["feedback_id", "item_type", "item_ref", "signal", "workspace"]
            }),
        ),
        "query_feedback" => (
            "Query bounded feedback records in a workspace.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        "list_categories" => ("List workspace categories.", workspace_schema()),
        "categorize_pending" => (
            "Assign a category to unclassified active facts.",
            json!({
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "category_name": {"type": "string"},
                    "query": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1},
                    "max_results": {"type": "integer", "minimum": 1},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["category", "workspace"]
            }),
        ),
        "list_forgotten" => (
            "List forgotten facts with provenance metadata.",
            workspace_schema(),
        ),
        "remember_entity" => (
            "Store a workspace-scoped graph entity.",
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "type": {"type": "string"},
                    "entity_type": {"type": "string"},
                    "aliases": {"type": "array", "items": {"type": "string"}},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["name"]
            }),
        ),
        "remember_relation" => (
            "Store a deduplicated graph relation between two entities.",
            json!({
                "type": "object",
                "properties": {
                    "subject": {"type": "string"},
                    "predicate": {"type": "string"},
                    "object": {"type": "string"},
                    "source_fact_id": {"type": "integer"},
                    "fact_id": {"type": "integer"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["subject", "predicate", "object"]
            }),
        ),
        "search_graph" => (
            "Search graph entities and relations.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query"]
            }),
        ),
        "record_decision" => (
            "Record a decision with optional parent and code anchors.",
            json!({
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "subject": {"type": "string"},
                    "scenario": {"type": "string"},
                    "reasoning": {"type": "string"},
                    "outcome": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "decision_maker": {"type": "string"},
                    "maker": {"type": "string"},
                    "issue_ref": {"type": "string"},
                    "path": {"type": "string"},
                    "symbol": {"type": "string"},
                    "parent_id": {"type": "integer"},
                    "parent": {"type": "integer"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["subject", "scenario", "outcome"]
            }),
        ),
        "query_decisions" | "find_precedents" => (
            "Search recorded decisions.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query"]
            }),
        ),
        "get_causal_chain" => (
            "Read a decision parent chain from root to decision.",
            json!({
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "decision_id": {"type": "integer"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["id"]
            }),
        ),
        "detect_conflicts" => (
            "Find distinct outcomes for the same decision subject and scenario.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query"]
            }),
        ),
        "query_anchored" => (
            "Query decision and evidence records by code or issue anchors.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "path": {"type": "string"},
                    "symbol": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        "attach_evidence" => (
            "Attach a bounded source anchor and selected-text hash to a fact.",
            json!({
                "type": "object",
                "properties": {
                    "fact_id": {"type": "integer"},
                    "id": {"type": "integer"},
                    "source_ref": {"type": "string"},
                    "source": {"type": "string"},
                    "checksum": {"type": "string"},
                    "fetched_at": {"type": "string"},
                    "repository_ref": {"type": "string"},
                    "repo_ref": {"type": "string"},
                    "path": {"type": "string"},
                    "symbol": {"type": "string"},
                    "line_start": {"type": "integer", "minimum": 0},
                    "line_end": {"type": "integer", "minimum": 0},
                    "column_start": {"type": "integer", "minimum": 0},
                    "column_end": {"type": "integer", "minimum": 0},
                    "selected_text": {"type": "string"},
                    "resolution_status": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["fact_id", "source_ref", "workspace"]
            }),
        ),
        "get_provenance" => (
            "List evidence anchors attached to a fact.",
            json!({
                "type": "object",
                "properties": {
                    "fact_id": {"type": "integer"},
                    "id": {"type": "integer"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["fact_id", "workspace"]
            }),
        ),
        "export" => (
            "Export a deterministic workspace-scoped memory snapshot.",
            workspace_schema(),
        ),
        "export_rdf" => (
            "Export workspace graph relations as deterministic RDF-like triples.",
            json!({
                "type": "object",
                "properties": {
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                }
            }),
        ),
        "put_context" => (
            "Store an immutable workspace-scoped context.",
            json!({
                "type": "object",
                "properties": {
                    "ref": {"type": "string"},
                    "name": {"type": "string"},
                    "content": {"type": "string", "description": "UTF-8 context payload bounded by MEMORY_MCP_CONTEXT_MAX_BYTES (default 4194304 bytes)."},
                    "schema": {"type": "string"},
                    "source": {"type": "string"},
                    "expires_at": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"},
                    "parent_ref": {"type": "string"},
                    "relation": {"type": "string"}
                },
                "required": ["ref", "name", "content", "workspace"]
            }),
        ),
        "ingest_document" => (
            "Preview or commit one UTF-8 document from an explicit local root as bounded immutable workspace-scoped context chunks; the root path is never stored or returned.",
            json!({
                "type": "object",
                "properties": {
                    "root": {"type": "string", "description": "Explicit local directory root used only for this read."},
                    "path": {"type": "string"},
                    "name": {"type": "string"},
                    "chunk_chars": {"type": "integer", "minimum": 256, "maximum": 16000, "default": 4000},
                    "max_bytes": {"type": "integer", "minimum": 1, "maximum": 16777216, "default": 4194304},
                    "ttl_seconds": {"type": "integer", "minimum": 0, "maximum": 604800},
                    "commit": {"type": "boolean", "default": false},
                    "workspace": {"type": "string"}
                },
                "required": ["root", "path", "workspace"]
            }),
        ),
        "list_context" => (
            "List non-expired contexts in a workspace.",
            workspace_schema(),
        ),
        "read_context" => (
            "Read one non-expired context by reference.",
            json!({
                "type": "object",
                "properties": {
                    "ref": {"type": "string"},
                    "reference": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["ref", "workspace"]
            }),
        ),
        "resolve_context" => (
            "Resolve a context by exact reference or name.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "ref": {"type": "string"},
                    "reference": {"type": "string"},
                    "name": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query", "workspace"]
            }),
        ),
        "search_context" => (
            "Search non-expired context references, names, and content.",
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["query", "workspace"]
            }),
        ),
        "chunk_context" => (
            "Return ordered UTF-8-safe byte-bounded context chunks.",
            json!({
                "type": "object",
                "properties": {
                    "ref": {"type": "string"},
                    "reference": {"type": "string"},
                    "max_bytes": {"type": "integer", "minimum": 1},
                    "chunk_size": {"type": "integer", "minimum": 1},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["ref", "workspace"]
            }),
        ),
        "reduce_context" => (
            "Create an immutable context from ordered context references.",
            json!({
                "type": "object",
                "properties": {
                    "references": {"type": "array", "items": {"type": "string"}},
                    "refs": {"type": "array", "items": {"type": "string"}},
                    "ref": {"type": "string"},
                    "reference": {"type": "string"},
                    "name": {"type": "string"},
                    "schema": {"type": "string"},
                    "source": {"type": "string"},
                    "expires_at": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["references", "workspace"]
            }),
        ),
        "context_map" => (
            "Read context lineage links in a workspace.",
            json!({
                "type": "object",
                "properties": {
                    "ref": {"type": "string"},
                    "reference": {"type": "string"},
                    "parent_ref": {"type": "string"},
                    "child_ref": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        "capture_event" => (
            "Capture one sanitized, byte-bounded lifecycle envelope in the exact workspace; secrets and excluded paths are removed before persistence.",
            json!({
                "type": "object",
                "properties": {
                    "idempotency_key": {"type": "string", "maxLength": 256},
                    "event_id": {"type": "string", "maxLength": 256},
                    "event_kind": {"type": "string", "maxLength": 64},
                    "session_id": {"type": "string", "maxLength": 256},
                    "source": {"type": "string", "maxLength": 256},
                    "cwd": {"type": "string", "maxLength": 1024},
                    "path": {"type": "string", "maxLength": 1024},
                    "tool_name": {"type": "string", "maxLength": 256},
                    "payload": {"description": "JSON value or text; secrets are redacted and the payload is byte-bounded."},
                    "content": {"type": "string", "description": "Alias for a text payload."},
                    "exclude_paths": {"type": "array", "items": {"type": "string"}, "maxItems": 32},
                    "capture": {"type": "boolean", "default": true},
                    "workspace": {"type": "string"}
                },
                "required": ["idempotency_key", "event_kind", "workspace"],
                "anyOf": [{"required": ["payload"]}, {"required": ["content"]}]
            }),
        ),
        "list_events" => ("List lifecycle events in a workspace.", workspace_schema()),
        "read_event" => (
            "Read one lifecycle event by idempotency key.",
            json!({
                "type": "object",
                "properties": {
                    "idempotency_key": {"type": "string"},
                    "event_id": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["idempotency_key", "workspace"]
            }),
        ),
        "handoff_begin" => (
            "Open one idempotent one-shot handoff for a context.",
            json!({
                "type": "object",
                "properties": {
                    "idempotency_key": {"type": "string"},
                    "handoff_id": {"type": "string"},
                    "context_ref": {"type": "string"},
                    "context": {"type": "string"},
                    "owner": {"type": "string"},
                    "session": {"type": "string"},
                    "source": {"type": "string"},
                    "shared": {"type": "boolean"},
                    "ttl_seconds": {"type": "integer", "minimum": 0},
                    "expires_at": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["idempotency_key", "context_ref", "owner", "workspace"]
            }),
        ),
        "list_handoffs" => (
            "List handoffs and refresh expired open states.",
            workspace_schema(),
        ),
        "handoff_accept" => (
            "Accept an open handoff exactly once.",
            json!({
                "type": "object",
                "properties": {
                    "idempotency_key": {"type": "string"},
                    "handoff_id": {"type": "string"},
                    "actor": {"type": "string"},
                    "accepted_by": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["idempotency_key", "actor", "workspace"]
            }),
        ),
        "handoff_cancel" => (
            "Cancel an open handoff exactly once.",
            json!({
                "type": "object",
                "properties": {
                    "idempotency_key": {"type": "string"},
                    "handoff_id": {"type": "string"},
                    "actor": {"type": "string"},
                    "cancelled_by": {"type": "string"},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["idempotency_key", "actor", "workspace"]
            }),
        ),
        "create_database" => (
            "Create and initialize a named SQLite database.",
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "database": {"type": "string"}
                },
                "required": ["name"]
            }),
        ),
        "list_databases" => (
            "List the active, named, and archived SQLite databases.",
            json!({
                "type": "object",
                "properties": {}
            }),
        ),
        "archive_database" => (
            "Archive an inactive named SQLite database.",
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "database": {"type": "string"}
                },
                "required": ["name"]
            }),
        ),
        "backup_database" => (
            "Create a private physical SQLite backup in the store's backups directory.",
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "database": {"type": "string"}
                }
            }),
        ),
        "delete_database" => (
            "Delete an inactive named or archived SQLite database.",
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "database": {"type": "string"}
                },
                "required": ["name"]
            }),
        ),
        "select_database" => (
            "Select a named SQLite database for subsequent calls in this process.",
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "database": {"type": "string"}
                },
                "required": ["name"]
            }),
        ),
        "current_database" => (
            "Return the database selected for the current process.",
            json!({
                "type": "object",
                "properties": {}
            }),
        ),
        "reset_database" => (
            "Clear all data from a named or current SQLite database.",
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "database": {"type": "string"}
                },
                "required": ["name"]
            }),
        ),
        "backup_workspace" => (
            "Write a deterministic JSON snapshot to the private backups directory.",
            json!({
                "type": "object",
                "properties": {
                    "workspace": {"type": "string"}
                },
                "required": ["workspace"]
            }),
        ),
        _ => (
            "Compatibility inventory entry; handler and schema parity is ported incrementally.",
            json!({
                "type": "object",
                "properties": {}
            }),
        ),
    }
}

fn workspace_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "workspace": {"type": "string"},
            "workspace_id": {"type": "string"}
        },
        "required": ["workspace"]
    })
}

pub fn is_advertised(name: &str) -> bool {
    TOOL_NAMES.contains(&name)
}

/// Return whether a tool changes durable memory state.
///
/// The coordinator uses this small explicit table to avoid exporting a full
/// SQLite image for read-only calls. Tools that only write an external backup
/// file are intentionally excluded: the database state itself is unchanged.
pub fn is_state_mutating(name: &str) -> bool {
    matches!(
        name,
        "remember_fact"
            | "add_fact"
            | "absorb"
            | "ingest_turn"
            | "confirm_fact"
            | "sweep_freshness"
            | "decay_sweep"
            | "embed_backfill"
            | "run_begin"
            | "run_end"
            | "link_run"
            | "record_measurement"
            | "record_feedback"
            | "categorize_pending"
            | "forget_fact"
            | "restore_fact"
            | "put_context"
            | "ingest_document"
            | "reduce_context"
            | "capture_event"
            | "handoff_begin"
            | "handoff_accept"
            | "handoff_cancel"
            | "remember_entity"
            | "remember_relation"
            | "record_decision"
            | "attach_evidence"
            | "create_database"
            | "archive_database"
            | "delete_database"
            | "select_database"
            | "reset_database"
            | "create_workspace"
            | "archive_workspace"
            | "reset_workspace"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_has_upstream_count_and_excludes_alias() {
        assert_eq!(TOOL_NAMES.len(), 80);
        assert!(!is_advertised("add_fact"));
        assert!(is_advertised("decay_sweep"));
        assert_eq!(advertised_tools().len(), 80);
        let put_context = advertised_tools()
            .into_iter()
            .find(|tool| tool["name"] == "put_context")
            .expect("put_context schema");
        assert_eq!(
            put_context["inputSchema"]["required"],
            json!(["name", "content", "workspace"])
        );
        let absorb = advertised_tools()
            .into_iter()
            .find(|tool| tool["name"] == "absorb")
            .expect("absorb schema");
        assert_eq!(
            absorb["inputSchema"]["properties"]["facts"]["type"],
            "array"
        );
        let recall = advertised_tools()
            .into_iter()
            .find(|tool| tool["name"] == "compose_recall")
            .expect("compose_recall schema");
        assert_eq!(recall["inputSchema"]["required"], json!(["turn_text"]));
    }

    #[test]
    fn state_mutation_table_has_explicit_read_only_boundary() {
        assert!(is_state_mutating("remember_fact"));
        assert!(is_state_mutating("add_fact"));
        assert!(is_state_mutating("reset_workspace"));
        assert!(!is_state_mutating("search_facts"));
        assert!(!is_state_mutating("backup_workspace"));
    }
}
