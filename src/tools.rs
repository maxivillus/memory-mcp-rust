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
    TOOL_NAMES
        .iter()
        .map(|name| {
            let (description, input_schema) = tool_contract(name);
            json!({
                "name": name,
                "description": description,
                "inputSchema": input_schema
            })
        })
        .collect()
}

fn tool_contract(name: &str) -> (&'static str, Value) {
    match name {
        "put_context" => (
            "Store an immutable workspace-scoped context.",
            json!({
                "type": "object",
                "properties": {
                    "ref": {"type": "string"},
                    "name": {"type": "string"},
                    "content": {"type": "string"},
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
            "Capture one idempotent lifecycle event for a context.",
            json!({
                "type": "object",
                "properties": {
                    "idempotency_key": {"type": "string"},
                    "event_id": {"type": "string"},
                    "event_type": {"type": "string"},
                    "type": {"type": "string"},
                    "context_ref": {"type": "string"},
                    "context": {"type": "string"},
                    "metadata": {"type": "object"},
                    "payload": {},
                    "workspace": {"type": "string"},
                    "workspace_id": {"type": "string"}
                },
                "required": ["idempotency_key", "event_type", "context_ref", "workspace"]
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
            json!(["ref", "name", "content", "workspace"])
        );
    }
}
