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
            json!({
                "name": name,
                "description": "Compatibility inventory entry; handler and schema parity is ported incrementally.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            })
        })
        .collect()
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
    }
}
