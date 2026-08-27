use crate::backend::{BackendCoordinator, BackendToolError};
use crate::pipeline;
use crate::providers;
use crate::store::{
    ContextMetadata, DecisionSpec, EntitySpec, EventSpec, EvidenceSpec, FactFilters, FactMetadata,
    FeedbackSpec, HandoffSpec, MeasurementSpec, RelationSpec, RunSpec, Store, StoreError,
    MAX_FACT_TEXT_CHARS,
};
use crate::tools;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

const DEFAULT_PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "memory-mcp";
const SERVER_VERSION: &str = "0.23.0";
const MAX_EVENT_PAYLOAD_BYTES: usize = 16 * 1024;
const MAX_EVENT_METADATA_BYTES: usize = 16 * 1024;
const MAX_EVENT_STRING_BYTES: usize = 4096;
const MAX_EVENT_EXCLUDE_PATHS: usize = 32;
const MAX_EVENT_PATH_BYTES: usize = 256;
const REDACTED_EVENT_VALUE: &str = "[REDACTED]";

static AUTO_ORIENTED_SESSIONS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static SEARCH_GUARD_COUNTS: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
static GENERATED_CONTEXT_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn handle_line(line: &str, store: &Store) -> Option<Value> {
    match serde_json::from_str::<Value>(line) {
        Ok(request) => handle_request(request, store),
        Err(_) => Some(error_response(Value::Null, -32700, "Parse error")),
    }
}

pub fn handle_line_with_coordinator(line: &str, coordinator: &BackendCoordinator) -> Option<Value> {
    match serde_json::from_str::<Value>(line) {
        Ok(request) => handle_request_with_coordinator(request, coordinator),
        Err(_) => Some(error_response(Value::Null, -32700, "Parse error")),
    }
}

/// SQLite fixture/compatibility entrypoint. The shipped stdio server uses
/// `handle_request_with_coordinator` so backend selection and failover are not
/// bypassed.
pub fn handle_request(request: Value, store: &Store) -> Option<Value> {
    handle_request_with(request, |params| call_tool(params, store))
}

pub fn handle_request_with_coordinator(
    request: Value,
    coordinator: &BackendCoordinator,
) -> Option<Value> {
    handle_request_with(request, |params| {
        call_tool_with_coordinator(params, coordinator)
    })
}

fn handle_request_with<F>(request: Value, mut call_tool: F) -> Option<Value>
where
    F: FnMut(Option<&Value>) -> Result<Value, CallError>,
{
    let object = match request.as_object() {
        Some(object) => object,
        None => return Some(error_response(Value::Null, -32600, "Invalid Request")),
    };
    let id = object.get("id").cloned();
    let notification = id.is_none();
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return if notification {
            None
        } else {
            Some(error_response(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid Request",
            ))
        };
    }
    let method = match object.get("method").and_then(Value::as_str) {
        Some(method) => method,
        None => {
            return Some(error_response(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid Request",
            ))
        }
    };
    let response = match method {
        "initialize" => result_response(
            id.clone().unwrap_or(Value::Null),
            json!({
                "protocolVersion": object
                    .get("params")
                    .and_then(|params| params.get("protocolVersion"))
                    .and_then(Value::as_str)
                    .unwrap_or(DEFAULT_PROTOCOL_VERSION),
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION}
            }),
        ),
        "tools/list" => result_response(
            id.clone().unwrap_or(Value::Null),
            json!({"tools": tools::advertised_tools()}),
        ),
        "tools/call" => match call_tool(object.get("params")) {
            Ok(result) => result_response(id.clone().unwrap_or(Value::Null), result),
            Err(CallError::InvalidParams(message)) => {
                error_response(id.clone().unwrap_or(Value::Null), -32602, &message)
            }
            Err(CallError::Execution(_error)) => {
                eprintln!("tool execution failed");
                result_response(
                    id.clone().unwrap_or(Value::Null),
                    json!({
                        "content": [{"type": "text", "text": "{\"error\":\"tool execution failed\"}"}],
                        "isError": true
                    }),
                )
            }
        },
        _ => error_response(
            id.clone().unwrap_or(Value::Null),
            -32601,
            "Method not found",
        ),
    };
    if notification {
        None
    } else {
        Some(response)
    }
}

enum CallError {
    InvalidParams(String),
    Execution(StoreError),
}

fn call_tool(params: Option<&Value>, store: &Store) -> Result<Value, CallError> {
    let params = params.and_then(Value::as_object).ok_or_else(|| {
        CallError::InvalidParams("tools/call params must be an object".to_owned())
    })?;
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| CallError::InvalidParams("tools/call name must be a string".to_owned()))?;
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    if !arguments.is_object() {
        return Err(CallError::InvalidParams(
            "tools/call arguments must be an object".to_owned(),
        ));
    }
    if !tools::is_advertised(name) && name != "add_fact" {
        return Err(CallError::Execution(StoreError::Invalid(format!(
            "unknown tool: {name}"
        ))));
    }
    let arguments = arguments.as_object().expect("object checked above");
    if let Some(result) = exact_compatibility_route(name, arguments, store)? {
        return Ok(tool_result(result));
    }
    let workspace = arguments
        .get("workspace")
        .or_else(|| arguments.get("workspace_id"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let result = match name {
        "remember_fact" | "add_fact" => {
            let text = arguments
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CallError::InvalidParams("remember_fact requires text".to_owned())
                })?;
            let metadata = FactMetadata {
                source: optional_string(arguments, "source")?
                    .unwrap_or("")
                    .to_owned(),
                project: optional_string(arguments, "project")?
                    .unwrap_or("")
                    .to_owned(),
                domain: optional_string(arguments, "domain")?
                    .unwrap_or("")
                    .to_owned(),
                trust: optional_string(arguments, "trust")?
                    .unwrap_or("medium")
                    .to_owned(),
                strong: optional_bool(arguments, &["strong"], false)?,
                importance: optional_f64(arguments, "importance")?.unwrap_or(0.5),
            };
            let mut fact = store
                .remember_fact_with_metadata(text, workspace, &metadata)
                .map_err(CallError::Execution)?;
            if let Some(validity) = optional_string(arguments, "validity")? {
                fact = store
                    .set_fact_validity(fact.id, validity, workspace)
                    .map_err(CallError::Execution)?
                    .ok_or_else(|| {
                        CallError::Execution(StoreError::Invalid(
                            "fact disappeared while setting validity".to_owned(),
                        ))
                    })?;
            }
            if let Some(session_id) =
                optional_string(arguments, "session_id")?.or(optional_string(arguments, "session")?)
            {
                fact = store
                    .set_fact_session(fact.id, session_id, workspace)
                    .map_err(CallError::Execution)?
                    .ok_or_else(|| {
                        CallError::Execution(StoreError::Invalid(
                            "fact disappeared while setting session".to_owned(),
                        ))
                    })?;
            }
            fact = pipeline::maybe_enrich_fact(store, &fact, arguments)
                .map_err(CallError::Execution)?;
            serde_json::to_value(fact).expect("Fact serializes")
        }
        "absorb" => {
            if arguments.contains_key("facts")
                || arguments.contains_key("dry_run")
                || arguments.contains_key("commit")
                || arguments.contains_key("verify")
            {
                pipeline::absorb(store, arguments).map_err(CallError::Execution)?
            } else {
                let texts = string_array_or_single(arguments, &["texts", "facts"], &["text"])?;
                serde_json::to_value(
                    store
                        .absorb(&texts, workspace)
                        .map_err(CallError::Execution)?,
                )
                .expect("absorbed facts serialize")
            }
        }
        "ingest_turn" => {
            if arguments.contains_key("transcript") {
                pipeline::ingest_turn(store, arguments).map_err(CallError::Execution)?
            } else {
                let text = required_string(arguments, "text")
                    .or_else(|_| required_string(arguments, "turn"))?;
                serde_json::to_value(
                    store
                        .ingest_turn(text, workspace)
                        .map_err(CallError::Execution)?,
                )
                .expect("ingested fact serializes")
            }
        }
        "review_pending" => serde_json::to_value(
            store
                .review_pending(workspace)
                .map_err(CallError::Execution)?,
        )
        .expect("pending facts serialize"),
        "confirm_fact" => {
            let id =
                required_i64(arguments, "id").or_else(|_| required_i64(arguments, "fact_id"))?;
            let note = optional_string(arguments, "note")?
                .or(optional_string(arguments, "reason")?)
                .unwrap_or("");
            serde_json::to_value(
                store
                    .confirm_fact(id, note, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("confirmed fact serializes")
        }
        "fact_history" => {
            let id =
                required_i64(arguments, "id").or_else(|_| required_i64(arguments, "fact_id"))?;
            serde_json::to_value(
                store
                    .fact_history(id, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("fact history serializes")
        }
        "facts_for_session" => {
            let session_id = required_string(arguments, "session_id")
                .or_else(|_| required_string(arguments, "session"))?;
            serde_json::to_value(
                store
                    .facts_for_session(session_id, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("session facts serialize")
        }
        "list_sessions" => serde_json::to_value(
            store
                .list_sessions(workspace)
                .map_err(CallError::Execution)?,
        )
        .expect("sessions serialize"),
        "fact_references" => {
            let id =
                required_i64(arguments, "id").or_else(|_| required_i64(arguments, "fact_id"))?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .fact_references(id, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("fact references serialize")
        }
        "search_guard" => {
            if arguments.contains_key("session_id") && arguments.contains_key("action") {
                let session = required_string(arguments, "session_id")?.trim().to_owned();
                let workspace = arguments
                    .get("workspace")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let key = format!("{workspace}\u{1f}{session}");
                let counts = SEARCH_GUARD_COUNTS.get_or_init(|| Mutex::new(HashMap::new()));
                let action = required_string(arguments, "action")?;
                let previous = counts
                    .lock()
                    .map_err(|_| {
                        CallError::Execution(StoreError::Invalid(
                            "search guard lock is poisoned".into(),
                        ))
                    })?
                    .get(&key)
                    .copied()
                    .unwrap_or(0);
                let result =
                    pipeline::search_guard(arguments, previous).map_err(CallError::Execution)?;
                if let Ok(mut state) = counts.lock() {
                    match action {
                        "search" => {
                            state.insert(key, previous + 1);
                        }
                        "memory" | "reset" => {
                            state.remove(&key);
                        }
                        _ => {}
                    }
                }
                result
            } else {
                let query = required_string(arguments, "query")?;
                let workspace = required_context_workspace(arguments)?;
                serde_json::to_value(
                    store
                        .search_guard(query, workspace)
                        .map_err(CallError::Execution)?,
                )
                .expect("guarded search serializes")
            }
        }
        "auto_orient" => {
            if arguments.contains_key("turn_text") {
                let session = arguments
                    .get("session_id")
                    .and_then(Value::as_str)
                    .unwrap_or("__process__");
                let workspace = arguments
                    .get("workspace")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let key = format!("{workspace}\u{1f}{session}");
                let sessions = AUTO_ORIENTED_SESSIONS.get_or_init(|| Mutex::new(HashSet::new()));
                let already = sessions
                    .lock()
                    .map_err(|_| {
                        CallError::Execution(StoreError::Invalid(
                            "orientation lock is poisoned".into(),
                        ))
                    })?
                    .contains(&key);
                let result = pipeline::auto_orient(store, arguments, already)
                    .map_err(CallError::Execution)?;
                if !already {
                    if let Ok(mut state) = sessions.lock() {
                        state.insert(key);
                    }
                }
                result
            } else {
                let query = required_string(arguments, "query")?;
                let workspace = required_context_workspace(arguments)?;
                serde_json::to_value(
                    store
                        .auto_orient(query, workspace)
                        .map_err(CallError::Execution)?,
                )
                .expect("orientation serializes")
            }
        }
        "summarize_index" => serde_json::to_value(
            store
                .summarize_index(workspace)
                .map_err(CallError::Execution)?,
        )
        .expect("index summary serializes"),
        "prepare_summary" => {
            let query = optional_string(arguments, "query")?.unwrap_or("");
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .prepare_summary(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("prepared summary serializes")
        }
        "sweep_freshness" | "decay_sweep" => {
            let max_age_seconds =
                optional_i64(arguments, &["max_age_seconds", "max_age", "ttl"])?.unwrap_or(86_400);
            serde_json::to_value(
                if name == "sweep_freshness" {
                    store.sweep_freshness(max_age_seconds, workspace)
                } else {
                    store.decay_sweep(max_age_seconds, workspace)
                }
                .map_err(CallError::Execution)?,
            )
            .expect("freshness sweep serializes")
        }
        "embed_backfill" => {
            if providers::embeddings_enabled() {
                pipeline::backfill(store, arguments).map_err(CallError::Execution)?
            } else {
                serde_json::to_value(
                    store
                        .embed_backfill(workspace)
                        .map_err(CallError::Execution)?,
                )
                .expect("embedding backfill serializes")
            }
        }
        "consolidate" => {
            if arguments.contains_key("ids") && providers::verification_enabled() {
                pipeline::consolidate(store, arguments).map_err(CallError::Execution)?
            } else {
                let query = optional_string(arguments, "query")?.unwrap_or("");
                serde_json::to_value(
                    store
                        .consolidate(query, workspace)
                        .map_err(CallError::Execution)?,
                )
                .expect("consolidation serializes")
            }
        }
        "run_begin" => {
            let run_id = required_string(arguments, "run_id")
                .or_else(|_| required_string(arguments, "id"))?;
            let spec = RunSpec {
                run_id: run_id.to_owned(),
                issue_ref: optional_string(arguments, "issue_ref")?
                    .or(optional_string(arguments, "issue")?)
                    .unwrap_or("")
                    .to_owned(),
                pr_ref: optional_string(arguments, "pr_ref")?
                    .or(optional_string(arguments, "pr")?)
                    .unwrap_or("")
                    .to_owned(),
                session: optional_string(arguments, "session")?
                    .unwrap_or("")
                    .to_owned(),
                git_ref: optional_string(arguments, "git_ref")?
                    .or(optional_string(arguments, "ref")?)
                    .or(optional_string(arguments, "commit")?)
                    .unwrap_or("")
                    .to_owned(),
                files: optional_text_or_json(arguments, &["files", "changed_files"])?,
                diff: optional_text_or_json(arguments, &["diff"])?,
                workspace: workspace.to_owned(),
            };
            serde_json::to_value(store.begin_run(&spec).map_err(CallError::Execution)?)
                .expect("run serializes")
        }
        "run_end" => {
            let run_id = required_string(arguments, "run_id")
                .or_else(|_| required_string(arguments, "id"))?;
            let summary = optional_string(arguments, "summary")?
                .or(optional_string(arguments, "result")?)
                .unwrap_or("");
            serde_json::to_value(
                store
                    .end_run(run_id, summary, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("ended run serializes")
        }
        "link_run" => {
            let run_id = required_string(arguments, "run_id")
                .or_else(|_| required_string(arguments, "id"))?;
            let issue_ref =
                optional_string(arguments, "issue_ref")?.or(optional_string(arguments, "issue")?);
            let pr_ref =
                optional_string(arguments, "pr_ref")?.or(optional_string(arguments, "pr")?);
            let session = optional_string(arguments, "session")?;
            let git_ref = optional_string(arguments, "git_ref")?
                .or(optional_string(arguments, "ref")?)
                .or(optional_string(arguments, "commit")?);
            serde_json::to_value(
                store
                    .link_run(run_id, issue_ref, pr_ref, session, git_ref, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("linked run serializes")
        }
        "query_run" => {
            let query = optional_string(arguments, "query")?.unwrap_or("");
            serde_json::to_value(
                store
                    .query_runs(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("runs serialize")
        }
        "record_measurement" => {
            let measurement = required_string(arguments, "measurement")
                .or_else(|_| required_string(arguments, "name"))?;
            let sample = required_string(arguments, "sample")
                .or_else(|_| required_string(arguments, "sample_id"))?;
            let variant = optional_string(arguments, "variant")?.unwrap_or("");
            let value = required_f64(arguments, "value")?;
            let baseline = optional_bool(arguments, &["baseline", "is_baseline"], false)?;
            let spec = MeasurementSpec {
                measurement: measurement.to_owned(),
                sample: sample.to_owned(),
                variant: variant.to_owned(),
                value,
                baseline,
                workspace: workspace.to_owned(),
            };
            serde_json::to_value(
                store
                    .record_measurement(&spec)
                    .map_err(CallError::Execution)?,
            )
            .expect("measurement serializes")
        }
        "query_measurement" => {
            let query = optional_string(arguments, "query")?.unwrap_or("");
            serde_json::to_value(
                store
                    .query_measurements(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("measurements serialize")
        }
        "record_feedback" => {
            let feedback_id = required_string(arguments, "feedback_id")
                .or_else(|_| required_string(arguments, "id"))?;
            let item_type = required_string(arguments, "item_type")
                .or_else(|_| required_string(arguments, "type"))?;
            let item_ref = required_string(arguments, "item_ref")
                .or_else(|_| required_string(arguments, "ref"))?;
            let signal = required_string(arguments, "signal")?;
            let spec = FeedbackSpec {
                feedback_id: feedback_id.to_owned(),
                site: optional_string(arguments, "site")?.unwrap_or("").to_owned(),
                item_type: item_type.to_owned(),
                item_ref: item_ref.to_owned(),
                signal: signal.to_owned(),
                query_hash: optional_string(arguments, "query_hash")?
                    .unwrap_or("")
                    .to_owned(),
                workspace: workspace.to_owned(),
            };
            serde_json::to_value(store.record_feedback(&spec).map_err(CallError::Execution)?)
                .expect("feedback serializes")
        }
        "query_feedback" => {
            let query = optional_string(arguments, "query")?.unwrap_or("");
            serde_json::to_value(
                store
                    .query_feedback(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("feedback serialize")
        }
        "list_categories" => serde_json::to_value(
            store
                .list_categories(workspace)
                .map_err(CallError::Execution)?,
        )
        .expect("categories serialize"),
        "categorize_pending" => {
            if providers::categorization_enabled() && !arguments.contains_key("category") {
                pipeline::categorize_pending(store, arguments).map_err(CallError::Execution)?
            } else {
                let category = required_string(arguments, "category")
                    .or_else(|_| required_string(arguments, "category_name"))?;
                let query = optional_string(arguments, "query")?.unwrap_or("");
                let limit = optional_usize(arguments, &["limit", "max_results"], 100)?;
                serde_json::to_value(
                    store
                        .categorize_pending(category, query, workspace, limit)
                        .map_err(CallError::Execution)?,
                )
                .expect("categorized facts serialize")
            }
        }
        "search_facts" => {
            let query = arguments
                .get("query")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CallError::InvalidParams("search_facts requires query".to_owned())
                })?;
            let filters = fact_filters(arguments)?;
            let facts = store
                .search_facts_with_filters(query, workspace, &filters)
                .map_err(CallError::Execution)?;
            if arguments
                .get("semantic")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && providers::embeddings_enabled()
            {
                pipeline::hybrid_search(store, query, arguments, &facts)
                    .map_err(CallError::Execution)?
            } else {
                let values = facts
                    .iter()
                    .map(|fact| {
                        let metadata = store
                            .fact_search_metadata(fact.id, workspace)
                            .ok()
                            .flatten();
                        let mut value = serde_json::to_value(fact).expect("Fact serializes");
                        if let Some(object) = value.as_object_mut() {
                            if let Some(metadata) = metadata {
                                object.insert(
                                    "category".into(),
                                    metadata.category.map(Value::String).unwrap_or(Value::Null),
                                );
                                object.insert("confirmed".into(), json!(metadata.confirmed));
                                object.insert("invalid_at".into(), json!(metadata.invalid_at));
                                object.insert("archived".into(), json!(metadata.archived));
                                object.insert("updated_at".into(), json!(metadata.updated_at));
                            }
                        }
                        value
                    })
                    .collect::<Vec<_>>();
                let result_status = if values.is_empty() { "empty" } else { "ok" };
                json!({"count": values.len(), "facts": values,
                       "memory_policy": "advisory_only", "safety_critical_allowed": false,
                       "profile": arguments.get("profile").and_then(Value::as_str).unwrap_or("balanced"),
                       "result_status": result_status})
            }
        }
        "search_semantic" => {
            let query = required_string(arguments, "query")?;
            if providers::embeddings_enabled() {
                pipeline::semantic_search(store, arguments).map_err(CallError::Execution)?
            } else {
                serde_json::to_value(
                    store
                        .search_semantic(query, workspace)
                        .map_err(CallError::Execution)?,
                )
                .expect("semantic fallback serializes")
            }
        }
        "list_facts" => {
            let filters = fact_filters(arguments)?;
            let facts = store
                .list_facts_with_filters(workspace, &filters)
                .map_err(CallError::Execution)?;
            let values = facts
                .iter()
                .map(|fact| {
                    let metadata = store
                        .fact_search_metadata(fact.id, workspace)
                        .ok()
                        .flatten();
                    let mut value = serde_json::to_value(fact).expect("Fact serializes");
                    if let (Some(object), Some(metadata)) = (value.as_object_mut(), metadata) {
                        object.insert(
                            "category".into(),
                            metadata.category.map(Value::String).unwrap_or(Value::Null),
                        );
                        object.insert("confirmed".into(), json!(metadata.confirmed));
                        object.insert("invalid_at".into(), json!(metadata.invalid_at));
                        object.insert("archived".into(), json!(metadata.archived));
                        object.insert("updated_at".into(), json!(metadata.updated_at));
                    }
                    value
                })
                .collect::<Vec<_>>();
            json!({"count": values.len(), "facts": values})
        }
        "forget_fact" => {
            let id =
                required_i64(arguments, "id").or_else(|_| required_i64(arguments, "fact_id"))?;
            serde_json::to_value(
                store
                    .forget_fact(id, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("forgotten fact serializes")
        }
        "restore_fact" => {
            let id =
                required_i64(arguments, "id").or_else(|_| required_i64(arguments, "fact_id"))?;
            serde_json::to_value(
                store
                    .restore_fact(id, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("restored fact serializes")
        }
        "list_forgotten" => serde_json::to_value(
            store
                .list_forgotten(workspace)
                .map_err(CallError::Execution)?,
        )
        .expect("forgotten facts serialize"),
        "verify_facts" => {
            if arguments.contains_key("text") && providers::verification_enabled() {
                pipeline::verify_facts(store, arguments).map_err(CallError::Execution)?
            } else {
                serde_json::to_value(
                    store
                        .verify_facts(workspace)
                        .map_err(CallError::Execution)?,
                )
                .expect("fact verification serializes")
            }
        }
        "chunk_fact" => {
            let id =
                required_i64(arguments, "id").or_else(|_| required_i64(arguments, "fact_id"))?;
            let max_bytes = optional_usize(arguments, &["max_bytes", "chunk_size"], 4096)?;
            serde_json::to_value(
                store
                    .chunk_fact(id, max_bytes, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("fact chunks serialize")
        }
        "compose_recall" | "search_index" => {
            if name == "compose_recall"
                && arguments.contains_key("turn_text")
                && providers::recall_enabled()
            {
                pipeline::compose_recall(store, arguments).map_err(CallError::Execution)?
            } else {
                let query = required_string(arguments, "query")?;
                let context_workspace = required_context_workspace(arguments)?;
                let recall = if name == "compose_recall" {
                    store.compose_recall(query, context_workspace)
                } else {
                    store.search_index(query, context_workspace)
                }
                .map_err(CallError::Execution)?;
                serde_json::to_value(recall).expect("recall serializes")
            }
        }
        "ingest_document" => {
            return Err(CallError::InvalidParams(
                "ingest_document requires root, path, and workspace".to_owned(),
            ));
        }
        "put_context" => {
            let reference = required_string(arguments, "ref")
                .or_else(|_| required_string(arguments, "reference"))?;
            let name = required_string(arguments, "name")?;
            let content = required_string(arguments, "content")?;
            let schema = optional_string(arguments, "schema")?.unwrap_or("");
            let source = optional_string(arguments, "source")?.unwrap_or("");
            let expires_at = optional_string(arguments, "expires_at")?;
            let workspace = required_context_workspace(arguments)?;
            let metadata = ContextMetadata {
                schema: schema.to_owned(),
                source: source.to_owned(),
                expires_at: expires_at.map(ToOwned::to_owned),
            };
            let context = store
                .put_context_with_metadata(reference, name, content, &metadata, workspace)
                .map_err(CallError::Execution)?;
            if let Some(parent) = optional_string(arguments, "parent_ref")? {
                let relation = optional_string(arguments, "relation")?.unwrap_or("derived_from");
                store
                    .link_context(parent, reference, relation, workspace)
                    .map_err(CallError::Execution)?;
            }
            serde_json::to_value(context).expect("Context serializes")
        }
        "read_context" => {
            let reference = required_string(arguments, "ref")
                .or_else(|_| required_string(arguments, "reference"))?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .context(reference, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("context serializes")
        }
        "list_context" => {
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .list_contexts(workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("contexts serialize")
        }
        "resolve_context" => {
            let query = required_string(arguments, "query")
                .or_else(|_| required_string(arguments, "ref"))
                .or_else(|_| required_string(arguments, "reference"))
                .or_else(|_| required_string(arguments, "name"))?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .resolve_context(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("resolved context serializes")
        }
        "search_context" => {
            let query = required_string(arguments, "query")?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .search_contexts(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("context search serializes")
        }
        "chunk_context" => {
            let reference = required_string(arguments, "ref")
                .or_else(|_| required_string(arguments, "reference"))?;
            let max_bytes = optional_usize(arguments, &["max_bytes", "chunk_size"], 4096)?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .chunk_context(reference, max_bytes, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("context chunks serialize")
        }
        "reduce_context" => {
            let references = required_string_array(arguments, &["references", "refs"])?;
            let output_reference =
                optional_string(arguments, "ref")?.or(optional_string(arguments, "reference")?);
            let name = optional_string(arguments, "name")?.unwrap_or("reduced context");
            let schema = optional_string(arguments, "schema")?.unwrap_or("");
            let source = optional_string(arguments, "source")?.unwrap_or("");
            let expires_at = optional_string(arguments, "expires_at")?;
            let workspace = required_context_workspace(arguments)?;
            let metadata = ContextMetadata {
                schema: schema.to_owned(),
                source: source.to_owned(),
                expires_at: expires_at.map(ToOwned::to_owned),
            };
            serde_json::to_value(
                store
                    .reduce_context(&references, output_reference, name, &metadata, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("reduced context serializes")
        }
        "context_map" => {
            let reference =
                optional_string(arguments, "ref")?.or(optional_string(arguments, "reference")?);
            let parent = optional_string(arguments, "parent_ref")?;
            let child = optional_string(arguments, "child_ref")?;
            let workspace = required_context_workspace(arguments)?;
            let mut lineage = store
                .context_map(reference, workspace)
                .map_err(CallError::Execution)?;
            if let Some(parent) = parent {
                lineage.retain(|entry| entry.parent_reference == parent);
            }
            if let Some(child) = child {
                lineage.retain(|entry| entry.child_reference == child);
            }
            serde_json::to_value(lineage).expect("context map serializes")
        }
        "capture_event" => {
            let idempotency_key = required_string(arguments, "idempotency_key")
                .or_else(|_| required_string(arguments, "event_id"))?;
            let event_type = required_string(arguments, "event_type")
                .or_else(|_| required_string(arguments, "type"))?;
            let context_reference = required_string(arguments, "context_ref")
                .or_else(|_| required_string(arguments, "context"))?;
            reject_sensitive_event_string(idempotency_key, "idempotency_key")?;
            reject_sensitive_event_string(event_type, "event_type")?;
            reject_sensitive_event_string(context_reference, "context_ref")?;
            let metadata = arguments
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let payload = arguments.get("payload").cloned().unwrap_or(Value::Null);
            let workspace = required_context_workspace(arguments)?;
            let exclusions = event_exclusions(arguments)?;
            let (_, metadata_text, _) =
                prepare_sanitized_json(&metadata, &HashSet::new(), MAX_EVENT_METADATA_BYTES)?;
            let (sanitized_payload, payload_json, payload_truncated) =
                prepare_sanitized_json(&payload, &exclusions, MAX_EVENT_PAYLOAD_BYTES)?;
            let spec = EventSpec {
                idempotency_key: idempotency_key.to_owned(),
                event_type: event_type.to_owned(),
                context_reference: context_reference.to_owned(),
                metadata: metadata_text,
                payload: event_payload_text(&sanitized_payload, &payload_json),
                payload_truncated,
                workspace: workspace.to_owned(),
            };
            serde_json::to_value(store.capture_event(&spec).map_err(CallError::Execution)?)
                .expect("event serializes")
        }
        "list_events" => {
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(store.list_events(workspace).map_err(CallError::Execution)?)
                .expect("events serialize")
        }
        "read_event" => {
            let idempotency_key = required_string(arguments, "idempotency_key")
                .or_else(|_| required_string(arguments, "event_id"))?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .read_event(idempotency_key, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("event serializes")
        }
        "handoff_begin" => {
            let idempotency_key = required_string(arguments, "idempotency_key")
                .or_else(|_| required_string(arguments, "handoff_id"))?;
            let context_reference = required_string(arguments, "context_ref")
                .or_else(|_| required_string(arguments, "context"))?;
            let owner = required_string(arguments, "owner")?;
            let session = optional_string(arguments, "session")?.unwrap_or("");
            let source = optional_string(arguments, "source")?.unwrap_or("");
            let shared = optional_bool(arguments, &["shared", "share"], false)?;
            let ttl_seconds = optional_i64(arguments, &["ttl_seconds", "ttl"])?;
            let expires_at = optional_string(arguments, "expires_at")?;
            let workspace = required_context_workspace(arguments)?;
            let spec = HandoffSpec {
                idempotency_key: idempotency_key.to_owned(),
                context_reference: context_reference.to_owned(),
                owner: owner.to_owned(),
                session: session.to_owned(),
                source: source.to_owned(),
                workspace: workspace.to_owned(),
                shared,
                ttl_seconds,
                expires_at: expires_at.map(ToOwned::to_owned),
            };
            serde_json::to_value(store.begin_handoff(&spec).map_err(CallError::Execution)?)
                .expect("handoff serializes")
        }
        "list_handoffs" => {
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .list_handoffs(workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("handoffs serialize")
        }
        "handoff_accept" => {
            let idempotency_key = required_string(arguments, "idempotency_key")
                .or_else(|_| required_string(arguments, "handoff_id"))?;
            let actor = required_string(arguments, "actor")
                .or_else(|_| required_string(arguments, "accepted_by"))?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .accept_handoff(idempotency_key, actor, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("accepted handoff serializes")
        }
        "handoff_cancel" => {
            let idempotency_key = required_string(arguments, "idempotency_key")
                .or_else(|_| required_string(arguments, "handoff_id"))?;
            let actor = required_string(arguments, "actor")
                .or_else(|_| required_string(arguments, "cancelled_by"))?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .cancel_handoff(idempotency_key, actor, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("cancelled handoff serializes")
        }
        "remember_entity" => {
            let name = required_string(arguments, "name")?;
            let entity_type = optional_string(arguments, "type")?
                .or(optional_string(arguments, "entity_type")?)
                .unwrap_or("");
            let aliases = optional_string_array(arguments, "aliases")?.unwrap_or_default();
            let spec = EntitySpec {
                name: name.to_owned(),
                entity_type: entity_type.to_owned(),
                aliases,
                workspace: workspace.to_owned(),
            };
            serde_json::to_value(store.remember_entity(&spec).map_err(CallError::Execution)?)
                .expect("entity serializes")
        }
        "remember_relation" => {
            let subject = required_string(arguments, "subject")?;
            let predicate = required_string(arguments, "predicate")?;
            let object = required_string(arguments, "object")?;
            let source_fact_id = optional_i64(arguments, &["source_fact_id", "fact_id"])?;
            let spec = RelationSpec {
                subject: subject.to_owned(),
                predicate: predicate.to_owned(),
                object: object.to_owned(),
                source_fact_id,
                workspace: workspace.to_owned(),
            };
            serde_json::to_value(
                store
                    .remember_relation(&spec)
                    .map_err(CallError::Execution)?,
            )
            .expect("relation serializes")
        }
        "search_graph" => {
            let query = required_string(arguments, "query")?;
            serde_json::to_value(
                store
                    .search_graph(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("graph search serializes")
        }
        "record_decision" => {
            let spec = DecisionSpec {
                category: optional_string(arguments, "category")?
                    .unwrap_or("")
                    .to_owned(),
                subject: required_string(arguments, "subject")?.to_owned(),
                scenario: required_string(arguments, "scenario")?.to_owned(),
                reasoning: optional_string(arguments, "reasoning")?
                    .unwrap_or("")
                    .to_owned(),
                outcome: required_string(arguments, "outcome")?.to_owned(),
                confidence: optional_f64(arguments, "confidence")?,
                decision_maker: optional_string(arguments, "decision_maker")?
                    .or(optional_string(arguments, "maker")?)
                    .unwrap_or("")
                    .to_owned(),
                issue_ref: optional_string(arguments, "issue_ref")?
                    .unwrap_or("")
                    .to_owned(),
                path: optional_string(arguments, "path")?.unwrap_or("").to_owned(),
                symbol: optional_string(arguments, "symbol")?
                    .unwrap_or("")
                    .to_owned(),
                parent_id: optional_i64(arguments, &["parent_id", "parent"])?,
                workspace: workspace.to_owned(),
            };
            serde_json::to_value(store.record_decision(&spec).map_err(CallError::Execution)?)
                .expect("decision serializes")
        }
        "query_decisions" | "find_precedents" => {
            let query = required_string(arguments, "query")?;
            let decisions = if name == "query_decisions" {
                store.query_decisions(query, workspace)
            } else {
                store.find_precedents(query, workspace)
            }
            .map_err(CallError::Execution)?;
            serde_json::to_value(decisions).expect("decisions serialize")
        }
        "get_causal_chain" => {
            let id = required_i64(arguments, "id")
                .or_else(|_| required_i64(arguments, "decision_id"))?;
            serde_json::to_value(
                store
                    .causal_chain(id, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("causal chain serializes")
        }
        "detect_conflicts" => {
            let query = required_string(arguments, "query")?;
            serde_json::to_value(
                store
                    .detect_conflicts(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("decision conflicts serialize")
        }
        "query_anchored" => {
            let query = optional_string(arguments, "query")?
                .or(optional_string(arguments, "path")?)
                .or(optional_string(arguments, "symbol")?)
                .unwrap_or("");
            serde_json::to_value(
                store
                    .query_anchored(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("anchored query serializes")
        }
        "attach_evidence" => {
            let fact_id =
                required_i64(arguments, "fact_id").or_else(|_| required_i64(arguments, "id"))?;
            let source_ref = required_string(arguments, "source_ref")
                .or_else(|_| required_string(arguments, "source"))?;
            let spec = EvidenceSpec {
                fact_id,
                source_ref: source_ref.to_owned(),
                source: optional_string(arguments, "source")?
                    .unwrap_or("")
                    .to_owned(),
                checksum: optional_string(arguments, "checksum")?
                    .unwrap_or("")
                    .to_owned(),
                fetched_at: optional_string(arguments, "fetched_at")?.map(ToOwned::to_owned),
                repository_ref: optional_string(arguments, "repository_ref")?
                    .or(optional_string(arguments, "repo_ref")?)
                    .unwrap_or("")
                    .to_owned(),
                path: optional_string(arguments, "path")?.unwrap_or("").to_owned(),
                symbol: optional_string(arguments, "symbol")?
                    .unwrap_or("")
                    .to_owned(),
                line_start: optional_i64(arguments, &["line_start"])?,
                line_end: optional_i64(arguments, &["line_end"])?,
                column_start: optional_i64(arguments, &["column_start"])?,
                column_end: optional_i64(arguments, &["column_end"])?,
                selected_text: optional_string(arguments, "selected_text")?
                    .unwrap_or("")
                    .to_owned(),
                resolution_status: optional_string(arguments, "resolution_status")?
                    .unwrap_or("unresolved")
                    .to_owned(),
                workspace: required_context_workspace(arguments)?.to_owned(),
            };
            serde_json::to_value(store.attach_evidence(&spec).map_err(CallError::Execution)?)
                .expect("evidence serializes")
        }
        "get_provenance" => {
            let fact_id =
                required_i64(arguments, "fact_id").or_else(|_| required_i64(arguments, "id"))?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .get_provenance(fact_id, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("provenance serializes")
        }
        "export" => {
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .export_snapshot(workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("memory export serializes")
        }
        "export_rdf" => {
            serde_json::to_value(store.export_rdf(workspace).map_err(CallError::Execution)?)
                .expect("RDF export serializes")
        }
        "create_database" => {
            let name = database_name_argument(arguments)?;
            serde_json::to_value(store.create_database(name).map_err(CallError::Execution)?)
                .expect("database serializes")
        }
        "list_databases" => {
            serde_json::to_value(store.list_databases().map_err(CallError::Execution)?)
                .expect("databases serialize")
        }
        "archive_database" => {
            let name = database_name_argument(arguments)?;
            serde_json::to_value(store.archive_database(name).map_err(CallError::Execution)?)
                .expect("database serializes")
        }
        "backup_database" => {
            let name = optional_string(arguments, "database")?
                .or(optional_string(arguments, "name")?)
                .unwrap_or("current");
            let path = required_string(arguments, "path")
                .or_else(|_| required_string(arguments, "output"))?;
            serde_json::to_value(
                store
                    .backup_database(name, path)
                    .map_err(CallError::Execution)?,
            )
            .expect("database backup serializes")
        }
        "delete_database" => {
            let name = database_name_argument(arguments)?;
            serde_json::to_value(store.delete_database(name).map_err(CallError::Execution)?)
                .expect("database deletion serializes")
        }
        "select_database" => {
            let name = database_name_argument(arguments)?;
            serde_json::to_value(store.select_database(name).map_err(CallError::Execution)?)
                .expect("database serializes")
        }
        "current_database" => {
            serde_json::to_value(store.current_database().map_err(CallError::Execution)?)
                .expect("current database serializes")
        }
        "reset_database" => {
            let name = database_name_argument(arguments)?;
            serde_json::to_value(store.reset_database(name).map_err(CallError::Execution)?)
                .expect("database serializes")
        }
        "create_workspace" => {
            let id = workspace_argument(arguments)?;
            serde_json::to_value(store.create_workspace(id).map_err(CallError::Execution)?)
                .expect("workspace serializes")
        }
        "list_workspaces" => {
            serde_json::to_value(store.list_workspaces().map_err(CallError::Execution)?)
                .expect("workspaces serialize")
        }
        "archive_workspace" => {
            let id = workspace_argument(arguments)?;
            serde_json::to_value(store.archive_workspace(id).map_err(CallError::Execution)?)
                .expect("workspace serializes")
        }
        "reset_workspace" => {
            let id = workspace_argument(arguments)?;
            serde_json::to_value(store.reset_workspace(id).map_err(CallError::Execution)?)
                .expect("workspace serializes")
        }
        "backup_workspace" => {
            let path = required_string(arguments, "path")
                .or_else(|_| required_string(arguments, "output"))?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .backup_workspace(path, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("workspace backup serializes")
        }
        "stats" => serde_json::to_value(store.stats().map_err(CallError::Execution)?)
            .expect("stats serialize"),
        _ => {
            return Err(CallError::Execution(StoreError::Invalid(format!(
                "tool not implemented in parity slice: {name}"
            ))))
        }
    };
    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string(&result).expect("value serializes")}],
        "isError": false
    }))
}

/// Execute arguments that belong to the pinned Python contract while keeping
/// the original Rust aliases intact.  The first implementation exposed the
/// durable store through the old Rust-shaped arguments; that made a real
/// Python client fail before it reached the store.  These adapters deliberately
/// live at the protocol boundary: the store remains the single source of
/// durable state and the coordinator still wraps the whole operation.
fn exact_compatibility_route(
    name: &str,
    arguments: &Map<String, Value>,
    store: &Store,
) -> Result<Option<Value>, CallError> {
    let result = match name {
        "remember_fact" => Some(exact_remember_fact(store, arguments)?),
        "fact_history" => Some(exact_fact_history(store, arguments)?),
        "confirm_fact" => Some(exact_confirm_fact(store, arguments)?),
        "fact_references" => Some(exact_fact_references(store, arguments)?),
        "search_facts" if arguments.contains_key("query") => {
            Some(exact_search_facts(store, arguments)?)
        }
        "search_semantic" if arguments.contains_key("query") => {
            Some(exact_search_semantic(store, arguments)?)
        }
        "list_facts" => Some(exact_list_facts(store, arguments)?),
        "summarize_index" => Some(exact_summarize_index(store, arguments)?),
        "list_categories" => Some(exact_list_categories(store, arguments)?),
        "search_index" if arguments.contains_key("query") => {
            Some(exact_search_index(store, arguments)?)
        }
        "categorize_pending"
            if !arguments.contains_key("category") && !arguments.contains_key("category_name") =>
        {
            Some(exact_categorize_pending(store, arguments)?)
        }
        "remember_entity" => Some(exact_remember_entity(store, arguments)?),
        "remember_relation" if arguments.contains_key("subject") => {
            Some(exact_remember_relation(store, arguments)?)
        }
        "record_feedback" if arguments.contains_key("feedback_id") => {
            Some(exact_record_feedback(store, arguments)?)
        }
        "query_feedback" => Some(exact_query_feedback(store, arguments)?),
        "export_rdf" => Some(exact_export_rdf(store, arguments)?),
        "export" => Some(exact_export(store, arguments)?),
        "create_database" => Some(exact_create_database(store, arguments)?),
        "list_databases" => Some(exact_list_databases(store, arguments)?),
        "archive_database" => Some(exact_archive_database(store, arguments)?),
        "reset_database" => Some(exact_reset_database(store, arguments)?),
        "select_database" => Some(exact_select_database(store, arguments)?),
        "current_database" => Some(exact_current_database(store, arguments)?),
        "create_workspace" => Some(exact_create_workspace(store, arguments)?),
        "list_workspaces" => Some(exact_list_workspaces(store, arguments)?),
        "archive_workspace" => Some(exact_archive_workspace(store, arguments)?),
        "reset_workspace" => Some(exact_reset_workspace(store, arguments)?),
        "backup_workspace"
            if !arguments.contains_key("path") && !arguments.contains_key("output") =>
        {
            Some(exact_backup_workspace(store, arguments)?)
        }
        "put_context" if !arguments.contains_key("ref") && !arguments.contains_key("reference") => {
            Some(exact_put_context(store, arguments)?)
        }
        "read_context" if !arguments.contains_key("reference") => {
            Some(exact_read_context(store, arguments)?)
        }
        "list_context" => Some(exact_list_context(store, arguments)?),
        "resolve_context"
            if arguments.contains_key("ref")
                && !arguments.contains_key("query")
                && !arguments.contains_key("reference")
                && !arguments.contains_key("name") =>
        {
            Some(exact_resolve_context(store, arguments)?)
        }
        "search_context" => Some(exact_search_context(store, arguments)?),
        "chunk_context"
            if arguments.contains_key("chunk_chars")
                || arguments.contains_key("start_chunk")
                || arguments.contains_key("max_chunks")
                || !arguments.contains_key("max_bytes") =>
        {
            Some(exact_chunk_context(store, arguments)?)
        }
        "reduce_context"
            if !arguments.contains_key("ref") && !arguments.contains_key("reference") =>
        {
            Some(exact_reduce_context(store, arguments)?)
        }
        "ingest_document" if arguments.contains_key("root") => {
            Some(exact_ingest_document(store, arguments)?)
        }
        "capture_event" if arguments.contains_key("event_kind") => {
            Some(exact_capture_event(store, arguments)?)
        }
        "list_events" => Some(exact_list_events(store, arguments)?),
        "read_event" if arguments.contains_key("event_ref") || arguments.contains_key("ref") => {
            Some(exact_read_event(store, arguments)?)
        }
        "handoff_begin" if arguments.contains_key("content") => {
            Some(exact_handoff_begin(store, arguments)?)
        }
        "list_handoffs" => Some(exact_list_handoffs(store, arguments)?),
        "handoff_accept" if arguments.contains_key("handoff_ref") => {
            Some(exact_handoff_accept(store, arguments)?)
        }
        "handoff_cancel" if arguments.contains_key("handoff_ref") => {
            Some(exact_handoff_cancel(store, arguments)?)
        }
        "run_begin"
            if !arguments.contains_key("id")
                && !arguments.contains_key("session")
                && !arguments.contains_key("git_ref")
                && !arguments.contains_key("ref")
                && !arguments.contains_key("commit")
                && !arguments.contains_key("files")
                && !arguments.contains_key("changed_files")
                && !arguments.contains_key("diff") =>
        {
            Some(exact_run_begin(store, arguments)?)
        }
        "run_end"
            if arguments.contains_key("base_sha")
                || arguments.contains_key("head_sha")
                || arguments.contains_key("files_changed") =>
        {
            Some(exact_run_end(store, arguments)?)
        }
        "query_run" if !arguments.contains_key("query") => Some(exact_query_run(store, arguments)?),
        "record_measurement" if arguments.contains_key("measurement_id") => {
            Some(exact_record_measurement(store, arguments)?)
        }
        "query_measurement" if arguments.contains_key("measurement_id") => {
            Some(exact_query_measurement(store, arguments)?)
        }
        "context_map" if arguments.contains_key("repo") && arguments.contains_key("anchors") => {
            Some(exact_context_map(store, arguments)?)
        }
        "search_graph" if !arguments.contains_key("query") => {
            Some(exact_search_graph(store, arguments)?)
        }
        "record_decision" if arguments.contains_key("scenario") => {
            Some(exact_record_decision(store, arguments)?)
        }
        "query_decisions" if !arguments.contains_key("query") => {
            Some(exact_query_decisions(store, arguments)?)
        }
        "find_precedents"
            if arguments.contains_key("scenario") && !arguments.contains_key("query") =>
        {
            Some(exact_find_precedents(store, arguments)?)
        }
        "get_causal_chain"
            if arguments.contains_key("decision_id") && !arguments.contains_key("id") =>
        {
            Some(exact_causal_chain(store, arguments)?)
        }
        "get_provenance"
            if (arguments.contains_key("sha256") || arguments.contains_key("fact_id"))
                && !arguments.contains_key("id") =>
        {
            Some(exact_provenance(store, arguments)?)
        }
        "attach_evidence"
            if arguments.contains_key("fact_id") && arguments.contains_key("source_ref") =>
        {
            Some(exact_attach_evidence(store, arguments)?)
        }
        "detect_conflicts"
            if arguments.contains_key("text") && !arguments.contains_key("query") =>
        {
            Some(exact_detect_conflicts(store, arguments)?)
        }
        "backup_database"
            if !arguments.contains_key("path") && !arguments.contains_key("output") =>
        {
            Some(exact_backup_database(store, arguments)?)
        }
        "delete_database" if arguments.contains_key("confirm") => {
            Some(exact_delete_database(store, arguments)?)
        }
        "stats" => Some(exact_stats(store, arguments)?),
        "chunk_fact"
            if arguments.contains_key("chunk_chars")
                || arguments.contains_key("chunk_overlap")
                || arguments.contains_key("start_chunk")
                || arguments.contains_key("max_chunks")
                || arguments.contains_key("sha256")
                || (arguments.contains_key("id") && !arguments.contains_key("max_bytes"))
                || (arguments.contains_key("fact_id") && !arguments.contains_key("max_bytes")) =>
        {
            Some(exact_chunk_fact(store, arguments)?)
        }
        "review_pending" => Some(exact_review_pending(store, arguments)?),
        "facts_for_session" if arguments.contains_key("session_ref") => {
            Some(exact_facts_for_session(store, arguments)?)
        }
        "list_sessions" => Some(exact_list_sessions(store, arguments)?),
        "forget_fact" if arguments.contains_key("sha256") || !arguments.contains_key("fact_id") => {
            Some(exact_forget_fact(store, arguments)?)
        }
        "restore_fact" if arguments.contains_key("id") => {
            Some(exact_restore_fact(store, arguments)?)
        }
        "list_forgotten" => Some(exact_list_forgotten(store, arguments)?),
        "categorize_pending"
            if !arguments.contains_key("category") && !arguments.contains_key("category_name") =>
        {
            Some(pipeline::categorize_pending(store, arguments).map_err(CallError::Execution)?)
        }
        "compose_recall" if arguments.contains_key("turn_text") => {
            Some(pipeline::compose_recall(store, arguments).map_err(CallError::Execution)?)
        }
        "verify_facts" if arguments.contains_key("text") => {
            Some(pipeline::verify_facts(store, arguments).map_err(CallError::Execution)?)
        }
        "consolidate" if arguments.contains_key("ids") => {
            Some(pipeline::consolidate(store, arguments).map_err(CallError::Execution)?)
        }
        _ => None,
    };
    Ok(result)
}

fn tool_result(result: Value) -> Value {
    json!({
        "content": [{"type": "text", "text": serde_json::to_string(&result).expect("value serializes")}],
        "isError": result.get("error").is_some()
    })
}

fn exact_workspace(arguments: &Map<String, Value>) -> Result<&str, CallError> {
    required_context_workspace(arguments)
}

fn optional_workspace(arguments: &Map<String, Value>) -> Result<&str, CallError> {
    match arguments
        .get("workspace")
        .or_else(|| arguments.get("workspace_id"))
    {
        None => Ok(""),
        Some(value) => value
            .as_str()
            .ok_or_else(|| CallError::InvalidParams("workspace must be a string".to_owned())),
    }
}

fn context_ref(prefix: &str, seed: &str) -> String {
    let counter = GENERATED_CONTEXT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut digest = Sha256::new();
    digest.update(prefix.as_bytes());
    digest.update(b"\0");
    digest.update(seed.as_bytes());
    digest.update(b"\0");
    digest.update(counter.to_le_bytes());
    format!("{prefix}_{}", hex::encode(digest.finalize()))
}

fn stable_context_ref(prefix: &str, seed: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(prefix.as_bytes());
    digest.update(b"\0");
    digest.update(seed.as_bytes());
    format!("{prefix}_{}", hex::encode(digest.finalize()))
}

fn context_expiry(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Option<String>, CallError> {
    let Some(ttl) = optional_i64(arguments, &["ttl_seconds"])? else {
        return Ok(None);
    };
    store
        .expiry_after_seconds(ttl)
        .map_err(CallError::Execution)
}

fn schema_string(arguments: &Map<String, Value>) -> Result<String, CallError> {
    let Some(schema) = arguments.get("schema") else {
        return Ok(String::new());
    };
    if let Some(text) = schema.as_str() {
        return Ok(text.to_owned());
    }
    serde_json::to_string(schema).map_err(|error| {
        CallError::InvalidParams(format!(
            "tool argument schema must be serializable JSON: {error}"
        ))
    })
}

fn checksum_matches(arguments: &Map<String, Value>, content: &str) -> Result<(), CallError> {
    let actual = sha256_text(content);
    let Some(expected) = arguments.get("checksum").and_then(Value::as_str) else {
        return Ok(());
    };
    if expected.trim().is_empty() || expected.eq_ignore_ascii_case(&actual) {
        return Ok(());
    }
    Err(CallError::InvalidParams(format!(
        "checksum does not match content (sha256: {actual})"
    )))
}

fn sha256_text(content: &str) -> String {
    hex::encode(Sha256::digest(content.as_bytes()))
}

fn context_metadata(context: &crate::store::Context) -> Value {
    let schema = if context.schema.trim().is_empty() {
        Value::String(String::new())
    } else {
        serde_json::from_str(&context.schema)
            .unwrap_or_else(|_| Value::String(context.schema.clone()))
    };
    json!({
        "ref": context.reference,
        "name": context.name,
        "schema": schema,
        "source": context.source,
        "sha256": context.sha256,
        "workspace": context.workspace,
        "created_at": Value::Null,
        "expires_at": context.expires_at,
        "size_bytes": context.byte_size,
    })
}

fn context_lineage(store: &Store, reference: &str, workspace: &str) -> Result<Value, CallError> {
    let rows = store
        .context_map(Some(reference), workspace)
        .map_err(CallError::Execution)?;
    let mut parents = Vec::new();
    let mut children = Vec::new();
    for row in rows {
        let other = if row.child_reference == reference {
            &row.parent_reference
        } else {
            &row.child_reference
        };
        let name = store
            .context(other, workspace)
            .map_err(CallError::Execution)?
            .map(|context| context.name)
            .unwrap_or_default();
        let entry = json!({"ref": other, "name": name, "relation": row.relation});
        if row.child_reference == reference {
            parents.push(entry);
        } else {
            children.push(entry);
        }
    }
    Ok(json!({"parents": parents, "children": children}))
}

fn exact_put_context(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let name = required_string(arguments, "name")?.trim();
    let content = required_string(arguments, "content")?;
    if name.is_empty() || content.is_empty() {
        return Err(CallError::InvalidParams(
            "name and content must be non-empty".to_owned(),
        ));
    }
    checksum_matches(arguments, content)?;
    let reference = context_ref("ctx", &format!("{workspace}\0{name}\0{content}"));
    let metadata = ContextMetadata {
        schema: schema_string(arguments)?,
        source: optional_string(arguments, "source")?
            .unwrap_or("")
            .to_owned(),
        expires_at: context_expiry(store, arguments)?,
    };
    let context = store
        .put_context_with_metadata(&reference, name, content, &metadata, workspace)
        .map_err(CallError::Execution)?;
    for parent in optional_string_array(arguments, "parent_refs")?.unwrap_or_default() {
        store
            .link_context(&parent, &reference, "derived", workspace)
            .map_err(CallError::Execution)?;
    }
    Ok(json!({
        "context": context_metadata(&context),
        "lineage": context_lineage(store, &reference, workspace)?,
    }))
}

fn exact_read_context(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let reference = required_string(arguments, "ref")?;
    let start = optional_usize(arguments, &["start"], 0)?;
    let max_chars = optional_usize(arguments, &["max_chars"], 4000)?;
    if max_chars == 0 || max_chars > 16_000 {
        return Err(CallError::InvalidParams(
            "max_chars must be between 1 and 16000".to_owned(),
        ));
    }
    let end = arguments
        .get("end")
        .map(|_| optional_usize(arguments, &["end"], 0))
        .transpose()?;
    if let Some(end) = end {
        if end < start {
            return Err(CallError::InvalidParams(
                "end must be greater than or equal to start".to_owned(),
            ));
        }
    }
    let context = store
        .context(reference, workspace)
        .map_err(CallError::Execution)?
        .ok_or_else(|| {
            CallError::Execution(StoreError::Invalid(format!(
                "context not found: {reference}"
            )))
        })?;
    let total_chars = context.content.chars().count();
    let bounded_start = start.min(total_chars);
    let requested_end = end.unwrap_or(total_chars).min(total_chars);
    let slice_end = requested_end.min(bounded_start.saturating_add(max_chars));
    let content = char_slice(&context.content, bounded_start, slice_end);
    let mut metadata = context_metadata(&context);
    metadata["content"] = Value::String(content);
    metadata["start"] = json!(bounded_start);
    metadata["end"] = json!(slice_end);
    metadata["total_chars"] = json!(total_chars);
    metadata["truncated"] = json!(slice_end < total_chars);
    metadata["next_start"] = if slice_end < total_chars {
        json!(slice_end)
    } else {
        Value::Null
    };
    Ok(json!({"context": metadata, "lineage": context_lineage(store, reference, workspace)?}))
}

fn char_slice(content: &str, start: usize, end: usize) -> String {
    content
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

fn exact_list_context(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let limit = optional_usize(arguments, &["limit"], 50)?;
    if !(1..=100).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 100".to_owned(),
        ));
    }
    let name = optional_string(arguments, "name")?.unwrap_or("");
    let mut contexts = store
        .list_contexts(workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|context| name.is_empty() || context.name == name)
        .map(|context| context_metadata(&context))
        .collect::<Vec<_>>();
    contexts.truncate(limit);
    Ok(json!({"count": contexts.len(), "contexts": contexts}))
}

fn exact_resolve_context(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let reference = required_string(arguments, "ref")?;
    let context = store
        .context(reference, workspace)
        .map_err(CallError::Execution)?
        .ok_or_else(|| {
            CallError::Execution(StoreError::Invalid(format!(
                "context not found: {reference}"
            )))
        })?;
    Ok(json!({
        "context": context_metadata(&context),
        "lineage": context_lineage(store, reference, workspace)?,
    }))
}

fn exact_search_context(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let query = required_string(arguments, "query")?.trim();
    if query.is_empty() {
        return Err(CallError::InvalidParams(
            "query must not be empty".to_owned(),
        ));
    }
    if query.chars().count() > 256 {
        return Err(CallError::InvalidParams(
            "query must be at most 256 characters".to_owned(),
        ));
    }
    let limit = optional_usize(arguments, &["limit"], 20)?;
    if !(1..=100).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 100".to_owned(),
        ));
    }
    let mut contexts = store
        .search_contexts(query, workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .map(|context| context_metadata(&context))
        .collect::<Vec<_>>();
    contexts.truncate(limit);
    Ok(json!({"query": query, "count": contexts.len(), "contexts": contexts}))
}

fn exact_chunk_context(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let reference = required_string(arguments, "ref")?;
    let chunk_chars = optional_usize(arguments, &["chunk_chars"], 4000)?;
    let start_chunk = optional_usize(arguments, &["start_chunk"], 0)?;
    let max_chunks = optional_usize(arguments, &["max_chunks"], 8)?;
    if chunk_chars == 0 || chunk_chars > 16_000 || max_chunks == 0 || max_chunks > 32 {
        return Err(CallError::InvalidParams(
            "invalid context chunk bounds".to_owned(),
        ));
    }
    let context = store
        .context(reference, workspace)
        .map_err(CallError::Execution)?
        .ok_or_else(|| {
            CallError::Execution(StoreError::Invalid(format!(
                "context not found: {reference}"
            )))
        })?;
    let total_chars = context.content.chars().count();
    let total_chunks = (total_chars + chunk_chars.saturating_sub(1)) / chunk_chars;
    let bounded_start = start_chunk.min(total_chunks);
    let end_chunk = total_chunks.min(bounded_start.saturating_add(max_chunks));
    let chunks = (bounded_start..end_chunk)
        .map(|index| {
            let start = index * chunk_chars;
            let end = total_chars.min(start + chunk_chars);
            json!({"index": index, "start": start, "end": end,
                   "content": char_slice(&context.content, start, end)})
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "context": context_metadata(&context),
        "chunks": chunks,
        "start_chunk": bounded_start,
        "next_chunk": if end_chunk < total_chunks { json!(end_chunk) } else { Value::Null },
        "total_chunks": total_chunks,
        "chunk_chars": chunk_chars,
    }))
}

fn exact_reduce_context(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let name = required_string(arguments, "name")?.trim();
    let references = required_string_array(arguments, &["refs"])?;
    if name.is_empty() || references.is_empty() {
        return Err(CallError::InvalidParams(
            "name and refs are required".to_owned(),
        ));
    }
    if references.len() > 64 {
        return Err(CallError::InvalidParams(
            "refs may contain at most 64 refs".to_owned(),
        ));
    }
    let separator = optional_string(arguments, "separator")?.unwrap_or("\n\n");
    if separator.chars().count() > 1024 {
        return Err(CallError::InvalidParams(
            "separator must be at most 1024 characters".to_owned(),
        ));
    }
    let mut contents = Vec::with_capacity(references.len());
    for reference in &references {
        let context = store
            .context(reference, workspace)
            .map_err(CallError::Execution)?
            .ok_or_else(|| {
                CallError::Execution(StoreError::Invalid(format!(
                    "context not found: {reference}"
                )))
            })?;
        contents.push(context.content);
    }
    let content = contents.join(separator);
    checksum_matches(arguments, &content)?;
    let reference = stable_context_ref("ctx", &format!("reduced\0{workspace}\0{name}\0{content}"));
    let metadata = ContextMetadata {
        schema: schema_string(arguments)?,
        source: optional_string(arguments, "source")?
            .unwrap_or("")
            .to_owned(),
        expires_at: context_expiry(store, arguments)?,
    };
    let context = store
        .put_context_with_metadata(&reference, name, &content, &metadata, workspace)
        .map_err(CallError::Execution)?;
    for parent in &references {
        store
            .link_context(parent, &reference, "reduced_from", workspace)
            .map_err(CallError::Execution)?;
    }
    Ok(json!({
        "context": context_metadata(&context),
        "lineage": context_lineage(store, &reference, workspace)?,
        "reduced_from": references,
        "reduction": "deterministic-concat",
    }))
}

fn exact_ingest_document(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let root = required_string(arguments, "root")?;
    let relative = required_string(arguments, "path")?;
    let (normalized, content) = read_document_under_root(root, relative, arguments)?;
    let chunk_chars = optional_usize(arguments, &["chunk_chars"], 4000)?;
    if !(256..=16_000).contains(&chunk_chars) {
        return Err(CallError::InvalidParams(
            "chunk_chars must be between 256 and 16000".to_owned(),
        ));
    }
    let max_bytes = optional_usize(arguments, &["max_bytes"], 4 * 1024 * 1024)?;
    if max_bytes == 0 || max_bytes > 16 * 1024 * 1024 || content.len() > max_bytes {
        return Err(CallError::InvalidParams(
            "document exceeds max_bytes".to_owned(),
        ));
    }
    let chunks = char_chunks(&content, chunk_chars);
    if chunks.is_empty() {
        return Err(CallError::InvalidParams("document is empty".to_owned()));
    }
    let document_sha256 = sha256_text(&content);
    let source_prefix =
        format!("local-document:{normalized}@{document_sha256}:chars={chunk_chars}");
    let document_name = optional_string(arguments, "name")?
        .filter(|name| !name.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("document:{normalized}"));
    let base = json!({
        "path": normalized,
        "sha256": document_sha256,
        "bytes": content.len(),
        "chunks": chunks.len(),
        "chunk_chars": chunk_chars,
        "source": source_prefix,
    });
    if !arguments
        .get("commit")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(
            json!({"committed": false, "duplicate": false, "document": base,
                         "chunks": chunks.len(), "refs": [], "result_status": "preview"}),
        );
    }
    let mut refs = Vec::with_capacity(chunks.len());
    let mut duplicate = true;
    let expires_at = context_expiry(store, arguments)?;
    for (index, chunk) in chunks.iter().enumerate() {
        let reference = stable_context_ref(
            "ctx",
            &format!("document\0{workspace}\0{source_prefix}\0{index}"),
        );
        let existed = store
            .context(&reference, workspace)
            .map_err(CallError::Execution)?
            .is_some();
        let schema = serde_json::to_string(&json!({
            "kind": "local_document_chunk", "version": 1, "path": normalized,
            "document_sha256": document_sha256, "chunk_index": index, "chunk_count": chunks.len()
        }))
        .expect("document schema serializes");
        store
            .put_context_with_metadata(
                &reference,
                &format!("{}#{:04}", document_name, index + 1),
                chunk,
                &ContextMetadata {
                    schema,
                    source: format!("{source_prefix}#chunk={index}"),
                    expires_at: expires_at.clone(),
                },
                workspace,
            )
            .map_err(CallError::Execution)?;
        duplicate &= existed;
        refs.push(reference);
    }
    Ok(
        json!({"committed": true, "duplicate": duplicate, "document": base,
              "chunks": refs.len(), "refs": refs,
              "result_status": if duplicate { "duplicate" } else { "ok" }}),
    )
}

fn event_exclusions(arguments: &Map<String, Value>) -> Result<HashSet<String>, CallError> {
    let values = optional_string_array(arguments, "exclude_paths")?.unwrap_or_default();
    if values.len() > MAX_EVENT_EXCLUDE_PATHS {
        return Err(CallError::InvalidParams(format!(
            "exclude_paths may contain at most {MAX_EVENT_EXCLUDE_PATHS} paths"
        )));
    }
    values
        .into_iter()
        .map(|value| {
            let mut path = value.trim().replace('/', ".");
            if path.len() > MAX_EVENT_PATH_BYTES {
                return Err(CallError::InvalidParams(
                    "exclude_paths entries are too long".to_owned(),
                ));
            }
            if let Some(stripped) = path.strip_prefix("$.") {
                path = stripped.to_owned();
            }
            if let Some(stripped) = path.strip_prefix("payload.") {
                path = stripped.to_owned();
            }
            let components = path.split('.').map(str::trim).collect::<Vec<_>>();
            if components.is_empty()
                || components
                    .iter()
                    .any(|component| component.is_empty() || *component == "..")
            {
                return Err(CallError::InvalidParams(
                    "exclude_paths must contain non-empty object paths".to_owned(),
                ));
            }
            Ok(components.join("."))
        })
        .collect()
}

fn prepare_sanitized_json(
    value: &Value,
    exclusions: &HashSet<String>,
    max_bytes: usize,
) -> Result<(Value, String, bool), CallError> {
    let original_bytes = serde_json::to_vec(value).map_err(|error| {
        CallError::InvalidParams(format!("event value must be serializable JSON: {error}"))
    })?;
    let mut truncated = original_bytes.len() > max_bytes || event_value_was_truncated(value);
    let mut sanitized = sanitize_event_value(value, "", exclusions);
    let mut serialized = serde_json::to_string(&sanitized).map_err(|error| {
        CallError::InvalidParams(format!("event value serialization failed: {error}"))
    })?;
    if serialized.len() > max_bytes {
        sanitized = json!({"truncated": true, "reason": "size_limit"});
        serialized = serde_json::to_string(&sanitized).expect("event truncation marker serializes");
        truncated = true;
    }
    Ok((sanitized, serialized, truncated))
}

fn sanitize_event_value(value: &Value, path: &str, exclusions: &HashSet<String>) -> Value {
    match value {
        Value::Object(object) => {
            let mut sanitized = Map::new();
            for (key, value) in object {
                let child_path = join_event_path(path, key);
                if event_path_excluded(&child_path, exclusions) {
                    continue;
                }
                let value = if is_sensitive_event_key(key) {
                    Value::String(REDACTED_EVENT_VALUE.to_owned())
                } else {
                    sanitize_event_value(value, &child_path, exclusions)
                };
                sanitized.insert(key.clone(), value);
            }
            Value::Object(sanitized)
        }
        Value::Array(values) => Value::Array(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let child_path = join_event_path(path, &index.to_string());
                    if event_path_excluded(&child_path, exclusions) {
                        Value::Null
                    } else {
                        sanitize_event_value(value, &child_path, exclusions)
                    }
                })
                .collect(),
        ),
        Value::String(value) => Value::String(sanitize_event_string(value)),
        _ => value.clone(),
    }
}

fn join_event_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_owned()
    } else {
        format!("{parent}.{child}")
    }
}

fn event_path_excluded(path: &str, exclusions: &HashSet<String>) -> bool {
    exclusions.iter().any(|excluded| {
        path == excluded
            || path
                .strip_prefix(excluded)
                .is_some_and(|suffix| suffix.starts_with('.'))
    })
}

fn is_sensitive_event_key(key: &str) -> bool {
    let key = key
        .chars()
        .filter_map(|character| {
            character
                .is_ascii_alphanumeric()
                .then_some(character.to_ascii_lowercase())
        })
        .collect::<String>();
    key == "cwd"
        || key == "path"
        || key.ends_with("path")
        || key == "key"
        || key.ends_with("key")
        || [
            "password",
            "passwd",
            "secret",
            "token",
            "apikey",
            "apikey",
            "authorization",
            "credential",
            "privatekey",
            "accesskey",
            "clientsecret",
            "cookie",
            "sessionid",
            "clientid",
            "refreshtoken",
            "sessiontoken",
        ]
        .iter()
        .any(|needle| key.contains(needle))
}

fn sanitize_event_string(value: &str) -> String {
    let value = if event_string_is_sensitive(value) {
        REDACTED_EVENT_VALUE
    } else {
        value
    };
    truncate_event_utf8(value, MAX_EVENT_STRING_BYTES)
}

fn reject_sensitive_event_string(value: &str, label: &str) -> Result<(), CallError> {
    if event_string_is_sensitive(value) {
        return Err(CallError::InvalidParams(format!(
            "{label} contains restricted data"
        )));
    }
    Ok(())
}

fn event_string_is_sensitive(value: &str) -> bool {
    let lower = value.trim().to_ascii_lowercase();
    let credential_marker = lower.starts_with("bearer ")
        || lower.starts_with("basic ")
        || lower.starts_with("sk-")
        || lower.starts_with("ghp_")
        || lower.starts_with("github_pat_")
        || lower.starts_with("xoxb-")
        || lower.starts_with("akia")
        || lower.contains("bearer ")
        || lower.contains("basic ")
        || lower.contains("-----begin ")
        || lower.contains("authorization:")
        || lower.contains("authorization=")
        || lower.contains("password=")
        || lower.contains("password:")
        || lower.contains("password ")
        || lower.contains("passwd=")
        || lower.contains("passwd:")
        || lower.contains("secret=")
        || lower.contains("secret:")
        || lower.contains("secret ")
        || lower.contains("token=")
        || lower.contains("token:")
        || lower.contains("token ")
        || lower.contains("api_key=")
        || lower.contains("api_key:")
        || lower.contains("api-key=")
        || lower.contains("api-key:")
        || lower.contains("apikey=")
        || lower.contains("apikey:")
        || lower.contains("access_key=")
        || lower.contains("access_key:")
        || lower.contains("private_key=")
        || lower.contains("private_key:")
        || lower.contains("client_secret=")
        || lower.contains("client_secret:")
        || lower.contains("refresh_token=")
        || lower.contains("refresh_token:")
        || lower.contains("session_token=")
        || lower.contains("session_token:");
    credential_marker
        || lower.contains("://")
        || looks_like_filesystem_path(value)
        || looks_like_jwt(&lower)
}

fn looks_like_filesystem_path(value: &str) -> bool {
    let value = value.trim();
    let bytes = value.as_bytes();
    value.starts_with('/')
        || value.starts_with("\\\\")
        || (bytes.len() >= 3 && bytes[1] == b':' && matches!(bytes[2], b'/' | b'\\'))
}

fn looks_like_jwt(value: &str) -> bool {
    value
        .split(|character: char| {
            character.is_whitespace()
                || matches!(character, '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']')
        })
        .any(|candidate| {
            let mut parts = candidate.split('.');
            let header = parts.next().unwrap_or("");
            let payload = parts.next().unwrap_or("");
            let signature = parts.next().unwrap_or("");
            parts.next().is_none()
                && header.starts_with("eyj")
                && header.len() >= 8
                && payload.len() >= 8
                && signature.len() >= 8
        })
}

fn event_value_was_truncated(value: &Value) -> bool {
    match value {
        Value::String(value) => value.len() > MAX_EVENT_STRING_BYTES,
        Value::Array(values) => values.iter().any(event_value_was_truncated),
        Value::Object(values) => values.values().any(event_value_was_truncated),
        _ => false,
    }
}

fn truncate_event_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let end = value
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= max_bytes)
        .last()
        .unwrap_or(0);
    value[..end].to_owned()
}

fn event_payload_text(value: &Value, serialized: &str) -> String {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| serialized.to_owned())
}

fn read_document_under_root(
    root: &str,
    relative: &str,
    arguments: &Map<String, Value>,
) -> Result<(String, String), CallError> {
    let max_bytes = optional_usize(arguments, &["max_bytes"], 4 * 1024 * 1024)?;
    if max_bytes == 0 || max_bytes > 16 * 1024 * 1024 {
        return Err(CallError::InvalidParams(
            "max_bytes must be between 1 and 16777216".to_owned(),
        ));
    }
    let root_path = Path::new(root);
    let canonical_root = fs::canonicalize(root_path).map_err(|error| {
        CallError::Execution(StoreError::Invalid(format!(
            "document root is not readable: {error}"
        )))
    })?;
    if !canonical_root.is_dir() {
        return Err(CallError::InvalidParams(
            "root must be a directory".to_owned(),
        ));
    }
    let relative_path = Path::new(relative);
    if relative_path.is_absolute()
        || relative_path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(CallError::InvalidParams(
            "path must stay inside root".to_owned(),
        ));
    }
    let candidate = fs::canonicalize(canonical_root.join(relative_path)).map_err(|error| {
        CallError::Execution(StoreError::Invalid(format!(
            "document file was not found: {error}"
        )))
    })?;
    if !candidate.starts_with(&canonical_root) || !candidate.is_file() {
        return Err(CallError::InvalidParams(
            "path must stay inside root".to_owned(),
        ));
    }
    let metadata =
        fs::metadata(&candidate).map_err(|error| CallError::Execution(StoreError::Io(error)))?;
    if metadata.len() > max_bytes as u64 {
        return Err(CallError::InvalidParams(
            "document exceeds max_bytes".to_owned(),
        ));
    }
    let file =
        fs::File::open(&candidate).map_err(|error| CallError::Execution(StoreError::Io(error)))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take((max_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| CallError::Execution(StoreError::Io(error)))?;
    if bytes.len() > max_bytes {
        return Err(CallError::InvalidParams(
            "document exceeds max_bytes".to_owned(),
        ));
    }
    let content = String::from_utf8(bytes)
        .map_err(|_| CallError::InvalidParams("document must be UTF-8 text".to_owned()))?;
    Ok((relative_path.to_string_lossy().replace('\\', "/"), content))
}

fn char_chunks(content: &str, chunk_chars: usize) -> Vec<String> {
    let chars = content.chars().collect::<Vec<_>>();
    chars
        .chunks(chunk_chars)
        .map(|chunk| chunk.iter().collect::<String>())
        .collect()
}

fn exact_capture_event(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let key = required_string(arguments, "idempotency_key")?;
    let event_id = optional_string(arguments, "event_id")?.unwrap_or(key);
    let raw_kind = required_string(arguments, "event_kind")?;
    let session_id = optional_string(arguments, "session_id")?.unwrap_or("");
    let source = optional_string(arguments, "source")?.unwrap_or("");
    let cwd = optional_string(arguments, "cwd")?.unwrap_or("");
    let path = optional_string(arguments, "path")?.unwrap_or("");
    let tool_name = optional_string(arguments, "tool_name")?.unwrap_or("");
    validate_event_string(key, 256, "idempotency_key")?;
    validate_event_string(event_id, 256, "event_id")?;
    validate_event_string(raw_kind, 64, "event_kind")?;
    validate_event_string(session_id, 256, "session_id")?;
    validate_event_string(source, 256, "source")?;
    validate_event_string(cwd, 1024, "cwd")?;
    validate_event_string(path, 1024, "path")?;
    validate_event_string(tool_name, 256, "tool_name")?;
    let kind = raw_kind.trim().to_lowercase().replace('_', "-");
    validate_event_string(&kind, 64, "event_kind")?;
    if kind.is_empty() {
        return Err(CallError::InvalidParams(
            "event_kind must not be empty".to_owned(),
        ));
    }
    reject_sensitive_event_string(key, "idempotency_key")?;
    reject_sensitive_event_string(event_id, "event_id")?;
    reject_sensitive_event_string(&kind, "event_kind")?;
    reject_sensitive_event_string(session_id, "session_id")?;
    if arguments.get("capture").and_then(Value::as_bool) == Some(false) {
        return Ok(json!({"accepted": false, "status": "excluded", "reason": "capture_disabled"}));
    }
    let payload = arguments
        .get("payload")
        .or_else(|| arguments.get("content"))
        .cloned()
        .ok_or_else(|| CallError::InvalidParams("payload or content is required".to_owned()))?;
    let exclusions = event_exclusions(arguments)?;
    let (sanitized_payload, payload_json, payload_truncated) =
        prepare_sanitized_json(&payload, &exclusions, MAX_EVENT_PAYLOAD_BYTES)?;
    let payload_text = event_payload_text(&sanitized_payload, &payload_json);
    let safe_source = sanitize_event_string(source);
    let metadata_value = json!({
        "event_id": event_id,
        "session_id": session_id,
        "source": source,
        "cwd": cwd,
        "path": path,
        "tool_name": tool_name,
    });
    let (_, metadata_text, _) =
        prepare_sanitized_json(&metadata_value, &HashSet::new(), MAX_EVENT_METADATA_BYTES)?;
    if let Some(existing) = store
        .read_event(key, workspace)
        .map_err(CallError::Execution)?
    {
        return Ok(json!({"accepted": true, "duplicate": true,
                         "event": event_value(&existing),
                         "context": Value::Null, "pruned": 0}));
    }
    let envelope = json!({
        "version": 1, "event_id": sanitize_event_string(event_id), "event_kind": kind,
        "session_id": sanitize_event_string(session_id),
        "source": safe_source.clone(),
        "tool_name": sanitize_event_string(tool_name),
        "payload_format": if payload.is_string() { "text" } else { "json" },
        "payload": sanitized_payload, "truncated": payload_truncated,
        "sanitized": true,
    });
    let content = serde_json::to_string(&envelope).expect("event envelope serializes");
    let reference = stable_context_ref("ctx", &format!("event\0{workspace}\0{key}"));
    let context = store
        .put_context_with_metadata(
            &reference,
            &format!("event-{}", &sha256_text(key)[..32]),
            &content,
            &ContextMetadata {
                schema: serde_json::to_string(
                    &json!({"kind":"lifecycle_event","version":1,"event_kind":kind}),
                )
                .expect("event schema serializes"),
                source: safe_source,
                expires_at: None,
            },
            workspace,
        )
        .map_err(CallError::Execution)?;
    let event = store
        .capture_event(&EventSpec {
            idempotency_key: key.to_owned(),
            event_type: kind.to_owned(),
            context_reference: reference,
            metadata: metadata_text,
            payload: payload_text,
            payload_truncated,
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    Ok(
        json!({"accepted": true, "duplicate": false, "event": event_value(&event),
              "context": context_metadata(&context), "pruned": 0}),
    )
}

fn validate_event_string(value: &str, max_chars: usize, label: &str) -> Result<(), CallError> {
    if value.chars().count() > max_chars {
        return Err(CallError::InvalidParams(format!(
            "{label} exceeds the configured limit ({max_chars} characters)"
        )));
    }
    Ok(())
}

fn event_value(event: &crate::store::LifecycleEvent) -> Value {
    let metadata = serde_json::from_str::<Value>(&event.metadata).unwrap_or_else(|_| json!({}));
    json!({
        "event_ref": event.context_reference, "context_ref": event.context_reference,
        "idempotency_key": event.idempotency_key,
        "event_id": metadata.get("event_id").cloned().unwrap_or_else(|| json!(event.idempotency_key)),
        "event_kind": event.event_type, "session_id": metadata.get("session_id").cloned().unwrap_or(json!("")),
        "source": metadata.get("source").cloned().unwrap_or(json!("")),
        "tool_name": metadata.get("tool_name").cloned().unwrap_or(json!("")),
        "sha256": event.payload_sha256, "payload_bytes": event.payload_size,
        "payload_truncated": event.payload_truncated, "workspace": event.workspace,
        "created_at": event.created_at,
    })
}

fn exact_list_events(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let limit = optional_usize(arguments, &["limit"], 50)?;
    if !(1..=100).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 100".to_owned(),
        ));
    }
    let session = optional_string(arguments, "session_id")?.unwrap_or("");
    let kind = optional_string(arguments, "event_kind")?
        .unwrap_or("")
        .replace('_', "-");
    let mut events = store
        .list_events(workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|event| {
            let metadata =
                serde_json::from_str::<Value>(&event.metadata).unwrap_or_else(|_| json!({}));
            (session.is_empty()
                || metadata.get("session_id").and_then(Value::as_str) == Some(session))
                && (kind.is_empty() || event.event_type == kind)
        })
        .map(|event| event_value(&event))
        .collect::<Vec<_>>();
    events.reverse();
    events.truncate(limit);
    Ok(json!({"count": events.len(), "events": events}))
}

fn exact_read_event(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let reference =
        required_string(arguments, "event_ref").or_else(|_| required_string(arguments, "ref"))?;
    let event = store
        .list_events(workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .find(|event| event.context_reference == reference || event.idempotency_key == reference)
        .ok_or_else(|| {
            CallError::Execution(StoreError::Invalid(format!("event not found: {reference}")))
        })?;
    let context = store
        .context(&event.context_reference, workspace)
        .map_err(CallError::Execution)?
        .ok_or_else(|| {
            CallError::Execution(StoreError::Invalid("event context not found".to_owned()))
        })?;
    let max_chars = optional_usize(arguments, &["max_chars"], 4000)?;
    let total_chars = context.content.chars().count();
    let end = total_chars.min(max_chars);
    let mut metadata = context_metadata(&context);
    metadata["content"] = Value::String(char_slice(&context.content, 0, end));
    metadata["start"] = json!(0);
    metadata["end"] = json!(end);
    metadata["total_chars"] = json!(total_chars);
    metadata["truncated"] = json!(end < total_chars);
    metadata["next_start"] = if end < total_chars {
        json!(end)
    } else {
        Value::Null
    };
    Ok(json!({"event": event_value(&event), "context": metadata,
              "lineage": context_lineage(store, &event.context_reference, workspace)?}))
}

fn exact_handoff_begin(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let content = required_string(arguments, "content")?;
    let owner = required_string(arguments, "owner")?;
    if content.is_empty() || owner.trim().is_empty() {
        return Err(CallError::InvalidParams(
            "content and owner are required".to_owned(),
        ));
    }
    checksum_matches(arguments, content)?;
    let key = optional_string(arguments, "idempotency_key")?
        .filter(|key| !key.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            format!(
                "handoff:{}",
                sha256_text(&format!("{workspace}\0{owner}\0{content}"))
            )
        });
    if let Some(existing) = store
        .list_handoffs(workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .find(|handoff| handoff.idempotency_key == key)
    {
        return Ok(json!({"created": true, "duplicate": true,
                         "handoff": handoff_value(&existing), "context": Value::Null}));
    }
    let reference = stable_context_ref("ctx", &format!("handoff\0{workspace}\0{key}"));
    let context = store
        .put_context_with_metadata(
            &reference,
            optional_string(arguments, "name")?.unwrap_or("handoff"),
            content,
            &ContextMetadata {
                schema: serde_json::to_string(
                    &json!({"kind":"typed_handoff","version":1,"owner":owner}),
                )
                .expect("handoff schema serializes"),
                source: optional_string(arguments, "source")?
                    .unwrap_or("")
                    .to_owned(),
                expires_at: context_expiry(store, arguments)?,
            },
            workspace,
        )
        .map_err(CallError::Execution)?;
    let handoff = store
        .begin_handoff(&HandoffSpec {
            idempotency_key: key,
            context_reference: reference,
            owner: owner.to_owned(),
            session: optional_string(arguments, "session_id")?
                .unwrap_or("")
                .to_owned(),
            source: optional_string(arguments, "source")?
                .unwrap_or("")
                .to_owned(),
            workspace: workspace.to_owned(),
            shared: optional_bool(arguments, &["shared"], false)?,
            ttl_seconds: optional_i64(arguments, &["ttl_seconds"])?,
            expires_at: None,
        })
        .map_err(CallError::Execution)?;
    Ok(
        json!({"created": true, "duplicate": false, "handoff": handoff_value(&handoff),
              "context": context_metadata(&context)}),
    )
}

fn handoff_value(handoff: &crate::store::Handoff) -> Value {
    json!({
        "ref": format!("hnd_{}", handoff.id), "context_ref": handoff.context_reference,
        "owner": handoff.owner, "session_id": handoff.session,
        "source": handoff.source, "sha256": "", "workspace": handoff.workspace,
        "shared": handoff.shared, "state": handoff.state, "created_at": handoff.created_at,
        "expires_at": handoff.expires_at, "accepted_at": handoff.accepted_at,
        "accepted_by": handoff.accepted_by, "cancelled_at": handoff.cancelled_at,
        "cancelled_by": handoff.cancelled_by,
    })
}

fn handoff_key(store: &Store, reference: &str, workspace: &str) -> Result<String, CallError> {
    if let Some(id) = reference
        .strip_prefix("hnd_")
        .and_then(|value| value.parse::<i64>().ok())
    {
        return store
            .list_handoffs(workspace)
            .map_err(CallError::Execution)?
            .into_iter()
            .find(|handoff| handoff.id == id)
            .map(|handoff| handoff.idempotency_key)
            .ok_or_else(|| {
                CallError::Execution(StoreError::Invalid(format!(
                    "handoff not found: {reference}"
                )))
            });
    }
    Ok(reference.to_owned())
}

fn exact_list_handoffs(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let limit = optional_usize(arguments, &["limit"], 50)?;
    if !(1..=100).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 100".to_owned(),
        ));
    }
    let owner = optional_string(arguments, "owner")?.unwrap_or("");
    let state = optional_string(arguments, "state")?.unwrap_or("");
    let mut handoffs = store
        .list_handoffs(workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|handoff| {
            (owner.is_empty() || handoff.owner == owner)
                && (state.is_empty() || handoff.state == state)
        })
        .map(|handoff| handoff_value(&handoff))
        .collect::<Vec<_>>();
    handoffs.reverse();
    handoffs.truncate(limit);
    Ok(json!({"count": handoffs.len(), "handoffs": handoffs}))
}

fn exact_handoff_accept(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    exact_handoff_transition(store, arguments, true)
}

fn exact_handoff_cancel(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    exact_handoff_transition(store, arguments, false)
}

fn exact_handoff_transition(
    store: &Store,
    arguments: &Map<String, Value>,
    accept: bool,
) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let reference = required_string(arguments, "handoff_ref")?;
    let key = handoff_key(store, reference, workspace)?;
    let actor = required_string(arguments, "actor")?;
    let handoff = if accept {
        store.accept_handoff(&key, actor, workspace)
    } else {
        store.cancel_handoff(&key, actor, workspace)
    }
    .map_err(CallError::Execution)?
    .ok_or_else(|| {
        CallError::Execution(StoreError::Invalid(format!(
            "handoff not found: {reference}"
        )))
    })?;
    let context = store
        .context(&handoff.context_reference, workspace)
        .map_err(CallError::Execution)?;
    let context_value = context
        .as_ref()
        .map(context_metadata)
        .unwrap_or(Value::Null);
    Ok(json!({if accept { "accepted" } else { "cancelled" }: true,
              "handoff": handoff_value(&handoff), "context": context_value}))
}

fn exact_run_begin(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let run_id = required_string(arguments, "run_id")?;
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let run = store
        .begin_run(&RunSpec {
            run_id: run_id.to_owned(),
            issue_ref: optional_string(arguments, "issue_ref")?
                .unwrap_or("")
                .to_owned(),
            pr_ref: optional_string(arguments, "pr_ref")?
                .unwrap_or("")
                .to_owned(),
            session: optional_string(arguments, "session_id")?
                .unwrap_or("")
                .to_owned(),
            git_ref: String::new(),
            files: String::new(),
            diff: String::new(),
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    Ok(json!({"run": run_value(&run, None, None, None), "duplicate": false}))
}

fn run_value(
    run: &crate::store::Run,
    base_sha: Option<&str>,
    head_sha: Option<&str>,
    files_changed: Option<&Value>,
) -> Value {
    let files = files_changed
        .cloned()
        .or_else(|| serde_json::from_str(&run.files).ok())
        .unwrap_or_else(|| json!([]));
    let summary = serde_json::from_str::<Value>(&run.summary).unwrap_or_else(|_| json!({}));
    json!({
        "id": run.id, "run_id": run.run_id, "issue_ref": run.issue_ref, "pr_ref": run.pr_ref,
        "session_id": run.session, "cwd": summary.get("cwd").cloned().unwrap_or(json!("")),
        "source": summary.get("source").cloned().unwrap_or(json!("")),
        "base_sha": base_sha.or_else(|| summary.get("base_sha").and_then(Value::as_str)).unwrap_or(""),
        "head_sha": head_sha.or_else(|| summary.get("head_sha").and_then(Value::as_str)).unwrap_or(""),
        "files_changed": files, "diff": summary.get("diff").cloned().unwrap_or_else(|| json!(run.diff)),
        "diff_truncated": summary.get("diff_truncated").cloned().unwrap_or(json!(false)),
        "state": run.state, "workspace": run.workspace, "created_at": run.created_at, "ended_at": run.ended_at,
    })
}

fn exact_run_end(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let run_id = required_string(arguments, "run_id")?;
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let files = arguments
        .get("files_changed")
        .cloned()
        .unwrap_or_else(|| json!([]));
    if !files.is_array() {
        return Err(CallError::InvalidParams(
            "files_changed must be an array".to_owned(),
        ));
    }
    let summary = json!({
        "base_sha": optional_string(arguments, "base_sha")?.unwrap_or(""),
        "head_sha": optional_string(arguments, "head_sha")?.unwrap_or(""),
        "files_changed": files, "diff": optional_string(arguments, "diff")?.unwrap_or(""),
        "issue_ref": optional_string(arguments, "issue_ref")?.unwrap_or(""),
        "pr_ref": optional_string(arguments, "pr_ref")?.unwrap_or(""),
        "diff_truncated": false,
    });
    let run = store
        .end_run(
            run_id,
            &serde_json::to_string(&summary).expect("run summary serializes"),
            workspace,
        )
        .map_err(CallError::Execution)?
        .ok_or_else(|| {
            CallError::Execution(StoreError::Invalid(format!("run not found: {run_id}")))
        })?;
    Ok(json!({"run": run_value(&run,
                               optional_string(arguments, "base_sha")?,
                               optional_string(arguments, "head_sha")?,
                               Some(&files)), "duplicate": false}))
}

fn exact_query_run(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut runs = store
        .query_runs("", workspace)
        .map_err(CallError::Execution)?;
    runs.reverse();
    let limit = optional_usize(arguments, &["limit"], 20)?;
    runs.truncate(limit);
    Ok(
        json!({"count": runs.len(), "runs": runs.iter().map(|run| run_value(run, None, None, None)).collect::<Vec<_>>() }),
    )
}

fn exact_record_measurement(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let measurement_id = required_string(arguments, "measurement_id")?;
    let sample_key = required_string(arguments, "sample_key")?;
    let variant = required_string(arguments, "variant")?;
    if !matches!(variant, "baseline" | "memory") {
        return Err(CallError::InvalidParams(
            "variant must be baseline or memory".to_owned(),
        ));
    }
    if !arguments.contains_key("run_id") && !arguments.contains_key("issue_ref") {
        return Err(CallError::InvalidParams(
            "run_id or issue_ref is required".to_owned(),
        ));
    }
    if let Some(run_id) = optional_string(arguments, "run_id")? {
        if store
            .query_runs(run_id, workspace)
            .map_err(CallError::Execution)?
            .iter()
            .all(|run| run.run_id != run_id)
        {
            return Err(CallError::InvalidParams(
                "run_id was not found in the requested workspace".to_owned(),
            ));
        }
    }
    let metric_names = [
        "input_tokens",
        "output_tokens",
        "memory_calls",
        "external_tool_calls",
        "context_bytes",
        "comment_bytes",
        "wall_time_ms",
        "time_to_first_useful_ms",
        "memory_latency_ms",
        "duplicate_rate",
        "conflict_rate",
        "reference_resolution_rate",
        "fallback_rate",
        "qa_rework",
        "quality_score",
        "safety_regression",
    ];
    let (measurement, value) = metric_names
        .iter()
        .find_map(|name| {
            arguments
                .get(*name)
                .and_then(Value::as_f64)
                .map(|value| ((*name).to_owned(), value))
        })
        .or_else(|| {
            arguments
                .get("wall_time_ms")
                .and_then(Value::as_i64)
                .map(|value| ("wall_time_ms".to_owned(), value as f64))
        })
        .ok_or_else(|| {
            CallError::InvalidParams("at least one aggregate metric is required".to_owned())
        })?;
    let observation = store
        .record_measurement(&MeasurementSpec {
            measurement,
            sample: sample_key.to_owned(),
            variant: variant.to_owned(),
            value,
            baseline: variant == "baseline",
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    let mut output = serde_json::to_value(&observation).expect("measurement serializes");
    if let Some(object) = output.as_object_mut() {
        object.insert("measurement_id".into(), json!(measurement_id));
        object.insert("sample_key".into(), json!(sample_key));
        object.insert(
            "run_id".into(),
            json!(optional_string(arguments, "run_id")?.unwrap_or("")),
        );
        object.insert(
            "issue_ref".into(),
            json!(optional_string(arguments, "issue_ref")?.unwrap_or("")),
        );
        for name in metric_names {
            if let Some(value) = arguments.get(name) {
                object.insert(name.to_owned(), value.clone());
            }
        }
    }
    Ok(json!({"observation": output, "duplicate": false}))
}

fn exact_query_measurement(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let measurement_id = required_string(arguments, "measurement_id")?;
    let min_pairs = optional_usize(arguments, &["min_pairs"], 10)?;
    let rows = store
        .query_measurements(measurement_id, workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|row| row.measurement == measurement_id || row.sample == measurement_id)
        .collect::<Vec<_>>();
    let baseline = rows.iter().filter(|row| row.variant == "baseline").count();
    let memory = rows.iter().filter(|row| row.variant == "memory").count();
    let paired = baseline.min(memory);
    Ok(json!({
        "measurement_id": measurement_id, "min_pairs": min_pairs, "paired_samples": paired,
        "observations": {"baseline": baseline, "memory": memory},
        "status": if paired >= min_pairs { "ready_for_review" } else { "not_claimed" },
        "variants": {"baseline": {"observations": baseline, "paired_samples": paired, "unpaired_samples": baseline.saturating_sub(paired)},
                     "memory": {"observations": memory, "paired_samples": paired, "unpaired_samples": memory.saturating_sub(paired)}},
    }))
}

fn exact_context_map(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    if optional_string(arguments, "purpose")?.unwrap_or("advisory") == "safety_critical" {
        return Ok(json!({
            "error": "context_map is advisory-only and cannot authorize safety-critical work",
            "code": "advisory_only",
            "memory_policy": "advisory_only",
            "safety_critical_allowed": false,
        }));
    }
    if std::env::var("MEMORY_MCP_CONTEXT_MAP").ok().as_deref() != Some("1") {
        return Ok(json!({
            "error": "context_map is disabled (set MEMORY_MCP_CONTEXT_MAP=1)",
            "code": "feature_disabled",
            "feature": "context_map",
            "memory_policy": "advisory_only",
        }));
    }
    let workspace = exact_workspace(arguments)?;
    let repo = required_string(arguments, "repo")?.trim();
    let reference = required_string(arguments, "ref")?.trim();
    let view = optional_string(arguments, "view")?.unwrap_or("orientation");
    if repo.is_empty() || reference.is_empty() {
        return Err(CallError::InvalidParams(
            "repo and ref are required".to_owned(),
        ));
    }
    if !matches!(
        view,
        "orientation" | "api" | "callers" | "dependents" | "impact"
    ) {
        return Err(CallError::InvalidParams(
            "invalid context_map view".to_owned(),
        ));
    }
    let limit = optional_usize(arguments, &["limit"], 20)?;
    if !(1..=100).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 100".to_owned(),
        ));
    }
    let anchors = arguments
        .get("anchors")
        .and_then(Value::as_array)
        .ok_or_else(|| CallError::InvalidParams("anchors must be a non-empty array".to_owned()))?;
    if anchors.is_empty() || anchors.len() > 32 {
        return Err(CallError::InvalidParams(
            "anchors must contain 1 to 32 items".to_owned(),
        ));
    }
    let repo_root = optional_string(arguments, "repo_root")?.unwrap_or("");
    let mut facts = Vec::<Value>::new();
    let mut decisions = Vec::<Value>::new();
    let mut manifest = Vec::with_capacity(anchors.len());
    let mut freshness = serde_json::Map::new();
    for key in ["STRONG", "WEAK", "STALE", "REBUILT", "REMOVED"] {
        freshness.insert(key.to_owned(), json!(0));
    }
    for anchor in anchors {
        let object = anchor
            .as_object()
            .ok_or_else(|| CallError::InvalidParams("each anchor must be an object".to_owned()))?;
        let path = object
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        let symbol = object
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if path.is_empty() && symbol.is_empty() {
            return Err(CallError::InvalidParams(
                "each anchor needs path or symbol".to_owned(),
            ));
        }
        let relation = object
            .get("relation")
            .and_then(Value::as_str)
            .unwrap_or("node");
        if !matches!(relation, "node" | "caller" | "callee" | "dependent") {
            return Err(CallError::InvalidParams(
                "invalid anchor relation".to_owned(),
            ));
        }
        let query = if !path.is_empty() { path } else { symbol };
        let anchored = store
            .query_anchored(query, workspace)
            .map_err(CallError::Execution)?;
        let mut matched_fact_ids = Vec::new();
        for evidence in &anchored.evidence {
            if !matched_fact_ids.contains(&evidence.fact_id) {
                matched_fact_ids.push(evidence.fact_id);
            }
        }
        let mut anchor_verdict = object
            .get("resolution_status")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or("WEAK")
            .to_owned();
        let mut reason = "metadata_only".to_owned();
        if !repo_root.is_empty() && !path.is_empty() {
            let candidate = Path::new(repo_root).join(path);
            match fs::read(&candidate) {
                Ok(bytes) => {
                    if let Some(expected) = object.get("content_checksum").and_then(Value::as_str) {
                        if !expected.is_empty() && sha256_bytes(&bytes) != expected {
                            anchor_verdict = "STALE".to_owned();
                            reason = "content_checksum_mismatch".to_owned();
                        } else {
                            anchor_verdict = "STRONG".to_owned();
                            reason = "content_checksum_matches".to_owned();
                        }
                    } else {
                        anchor_verdict = "WEAK".to_owned();
                        reason = "filesystem_read_without_checksum".to_owned();
                    }
                }
                Err(_) => {
                    anchor_verdict = "REMOVED".to_owned();
                    reason = "path_not_found".to_owned();
                }
            }
        }
        let freshness_count = freshness
            .get(&anchor_verdict)
            .and_then(Value::as_i64)
            .unwrap_or(0)
            + 1;
        freshness.insert(anchor_verdict.clone(), json!(freshness_count));
        for id in matched_fact_ids.iter().take(limit) {
            if let Some(fact) = store
                .fact_by_id_for_pipeline(*id, workspace)
                .map_err(CallError::Execution)?
            {
                let value = serde_json::to_value(fact).expect("fact serializes");
                if !facts.iter().any(|item| item.get("id") == value.get("id")) {
                    facts.push(value);
                }
            }
        }
        let mut matched_decision_ids = Vec::new();
        for decision in anchored.decisions {
            matched_decision_ids.push(decision.id);
            let value = decision_value(&decision);
            if !decisions
                .iter()
                .any(|item| item.get("id") == value.get("id"))
            {
                decisions.push(value);
            }
        }
        let checksum_verdict = if repo_root.is_empty() {
            "UNVERIFIED"
        } else if anchor_verdict == "REMOVED" {
            "REMOVED"
        } else if reason == "content_checksum_matches" {
            "MATCH"
        } else if reason == "content_checksum_mismatch" {
            "MISMATCH"
        } else {
            "UNVERIFIED"
        };
        manifest.push(json!({
            "repo": repo, "ref": reference, "path": path, "symbol": symbol,
            "relation": relation, "selected_text_hash": object.get("selected_text_hash").cloned().unwrap_or(Value::String(String::new())),
            "content_checksum": object.get("content_checksum").cloned().unwrap_or(Value::String(String::new())),
            "checksum_verdict": checksum_verdict,
            "anchor_verdict": anchor_verdict, "anchor_verification_reason": reason,
            "matched_fact_ids": matched_fact_ids, "matched_decision_ids": matched_decision_ids,
        }));
    }
    let impact_paths = arguments
        .get("impact_paths")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let fact_count = facts.len();
    let decision_count = decisions.len();
    let facts = facts.into_iter().take(100).collect::<Vec<_>>();
    let decisions = decisions.into_iter().take(100).collect::<Vec<_>>();
    Ok(json!({
        "view": view, "repo": repo, "ref": reference, "workspace": workspace,
        "bounded": true, "manifest": manifest,
        "facts": facts, "decisions": decisions,
        "impact": {"paths": impact_paths, "runs": []}, "freshness": freshness,
        "counts": {"anchors": anchors.len(), "facts": fact_count, "decisions": decision_count, "impact_runs": 0},
        "relationship_mode": if matches!(view, "callers" | "dependents") { "client_declared_anchor_relations" } else { "anchor_and_run_evidence" },
        "memory_policy": "advisory_only", "safety_critical_allowed": false,
        "source_of_truth": "current repository and live runtime state",
    }))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn decision_value(decision: &crate::store::Decision) -> Value {
    json!({
        "id": decision.id, "category": decision.category, "subject": decision.subject,
        "scenario": decision.scenario, "reasoning": decision.reasoning, "outcome": decision.outcome,
        "confidence": decision.confidence, "decision_maker": decision.decision_maker,
        "issue_ref": decision.issue_ref, "path": decision.path, "symbol": decision.symbol,
        "parent_decision_id": decision.parent_id, "workspace": decision.workspace,
    })
}

fn exact_search_graph(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let entity = required_string(arguments, "entity")?.trim();
    if entity.is_empty() {
        return Ok(json!({"error": "entity is required", "nodes": [], "edges": []}));
    }
    let depth = optional_usize(arguments, &["depth"], 1)?;
    let limit = optional_usize(arguments, &["limit"], 50)?;
    if !(1..=2).contains(&depth) || !(1..=200).contains(&limit) {
        return Err(CallError::InvalidParams(
            "depth or limit is outside the supported range".to_owned(),
        ));
    }
    let graph = store
        .search_graph(entity, workspace)
        .map_err(CallError::Execution)?;
    if graph.entities.is_empty() {
        return Ok(
            json!({"error": format!("entity {entity:?} not found"), "nodes": [], "edges": []}),
        );
    }
    let root_entity = graph.entities.first().expect("graph is non-empty");
    let root =
        json!({"id": root_entity.id, "name": root_entity.name, "type": root_entity.entity_type});
    let nodes = graph
        .entities
        .iter()
        .take(limit)
        .map(|entity| json!({"id": entity.id, "name": entity.name}))
        .collect::<Vec<_>>();
    let names = graph
        .entities
        .iter()
        .map(|entity| (entity.id, entity.name.clone()))
        .collect::<HashMap<_, _>>();
    let edges = graph
        .relations
        .iter()
        .take(limit)
        .map(|relation| json!({
            "subject": names.get(&relation.subject_id).cloned().unwrap_or_else(|| relation.subject_id.to_string()),
            "predicate": relation.predicate,
            "object": names.get(&relation.object_id).cloned().unwrap_or_else(|| relation.object_id.to_string()),
            "direction": if relation.subject_id == root_entity.id { "out" } else { "in" },
        }))
        .collect::<Vec<_>>();
    Ok(json!({"root": root, "nodes": nodes, "edges": edges, "depth": depth}))
}

fn exact_record_decision(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let scenario = required_string(arguments, "scenario")?.trim();
    if scenario.is_empty() {
        return Ok(json!({"error": "scenario is required"}));
    }
    let subject = optional_string(arguments, "subject")?
        .unwrap_or(scenario)
        .to_owned();
    let outcome = optional_string(arguments, "outcome")?
        .unwrap_or("recorded")
        .to_owned();
    let decision = store
        .record_decision(&DecisionSpec {
            category: optional_string(arguments, "category")?
                .unwrap_or("")
                .to_owned(),
            subject,
            scenario: scenario.to_owned(),
            reasoning: optional_string(arguments, "reasoning")?
                .unwrap_or("")
                .to_owned(),
            outcome,
            confidence: optional_f64(arguments, "confidence")?,
            decision_maker: optional_string(arguments, "decision_maker")?
                .unwrap_or("")
                .to_owned(),
            issue_ref: optional_string(arguments, "issue_ref")?
                .unwrap_or("")
                .to_owned(),
            path: optional_string(arguments, "path")?.unwrap_or("").to_owned(),
            symbol: optional_string(arguments, "symbol")?
                .unwrap_or("")
                .to_owned(),
            parent_id: optional_i64(arguments, &["parent_decision_id"])?,
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    Ok(json!({"id": decision.id, "category": decision.category,
              "scenario": decision.scenario, "created_at": Value::Null}))
}

fn exact_query_decisions(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let limit = optional_usize(arguments, &["limit"], 20)?;
    if !(1..=100).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 100".to_owned(),
        ));
    }
    let mut decisions = store
        .list_decisions(workspace)
        .map_err(CallError::Execution)?;
    for (key, value) in [
        ("category", optional_string(arguments, "category")?),
        ("subject", optional_string(arguments, "subject")?),
        ("outcome", optional_string(arguments, "outcome")?),
        (
            "decision_maker",
            optional_string(arguments, "decision_maker")?,
        ),
        ("issue_ref", optional_string(arguments, "issue_ref")?),
    ] {
        if let Some(value) = value {
            decisions.retain(|decision| match key {
                "category" => decision.category == value,
                "subject" => decision.subject == value,
                "outcome" => decision.outcome == value,
                "decision_maker" => decision.decision_maker == value,
                "issue_ref" => decision.issue_ref == value,
                _ => true,
            });
        }
    }
    for (key, value) in [
        ("path", optional_string(arguments, "path")?),
        ("symbol", optional_string(arguments, "symbol")?),
    ] {
        if let Some(value) = value {
            let needle = value.to_lowercase();
            decisions.retain(|decision| {
                let haystack = if key == "path" {
                    &decision.path
                } else {
                    &decision.symbol
                };
                haystack.to_lowercase().contains(&needle)
            });
        }
    }
    decisions.reverse();
    decisions.truncate(limit);
    Ok(json!({"count": decisions.len(),
              "decisions": decisions.iter().map(decision_value).collect::<Vec<_>>() }))
}

fn exact_find_precedents(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    if optional_string(arguments, "purpose")?.unwrap_or("advisory") == "safety_critical" {
        return Ok(
            json!({"error": "find_precedents is advisory-only", "code": "advisory_only",
                         "memory_policy": "advisory_only", "safety_critical_allowed": false}),
        );
    }
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let scenario = required_string(arguments, "scenario")?.trim();
    let limit = optional_usize(arguments, &["limit"], 10)?;
    if scenario.is_empty() {
        return Ok(
            json!({"error": "scenario has no searchable terms", "count": 0,
                         "precedents": [], "profile": optional_string(arguments, "profile")?.unwrap_or("balanced"),
                         "result_status": "empty"}),
        );
    }
    if !(1..=50).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 50".to_owned(),
        ));
    }
    let terms = scenario
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>();
    let category = optional_string(arguments, "category")?.unwrap_or("");
    let mut ranked = store
        .list_decisions(workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|decision| category.is_empty() || decision.category == category)
        .filter_map(|decision| {
            let haystack = format!(
                "{} {} {} {}",
                decision.category, decision.scenario, decision.reasoning, decision.outcome
            )
            .to_lowercase();
            let score = terms
                .iter()
                .filter(|term| haystack.contains(term.as_str()))
                .count();
            (score > 0).then_some((score, decision))
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.id.cmp(&left.1.id))
    });
    let precedents = ranked
        .into_iter()
        .take(limit)
        .map(|(_, decision)| decision_value(&decision))
        .collect::<Vec<_>>();
    let count = precedents.len();
    Ok(json!({"count": count, "precedents": precedents,
              "semantic": arguments.get("semantic").and_then(Value::as_bool).unwrap_or(false),
              "memory_policy": "advisory_only", "safety_critical_allowed": false,
              "profile": optional_string(arguments, "profile")?.unwrap_or("balanced"),
              "result_status": if count == 0 { "empty" } else { "ok" },
              "retrieval": {"matched": count, "reason": if count == 0 { "no_matching_decisions" } else { "match" }}}))
}

fn exact_causal_chain(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let id = required_i64(arguments, "decision_id")?;
    let chain = store
        .causal_chain(id, workspace)
        .map_err(CallError::Execution)?;
    Ok(
        json!({"count": chain.len(), "chain": chain.iter().map(|decision| json!({
        "id": decision.id, "category": decision.category, "subject": decision.subject,
        "scenario": decision.scenario, "outcome": decision.outcome,
        "decision_maker": decision.decision_maker, "issue_ref": decision.issue_ref,
        "parent_decision_id": decision.parent_id,
    })).collect::<Vec<_>>() }),
    )
}

fn exact_provenance(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let fact = if let Some(id) = optional_i64(arguments, &["fact_id"])? {
        store
            .fact_by_id_for_pipeline(id, workspace)
            .map_err(CallError::Execution)?
    } else if let Some(hash) = optional_string(arguments, "sha256")? {
        store
            .fact_by_sha256_for_pipeline(hash, workspace)
            .map_err(CallError::Execution)?
    } else {
        None
    };
    let Some(fact) = fact else {
        return Ok(
            json!({"error": "fact not found (use fact_id or sha256)", "fact": Value::Null, "evidence": []}),
        );
    };
    let evidence = store
        .get_provenance(fact.id, workspace)
        .map_err(CallError::Execution)?;
    Ok(
        json!({"fact": fact_value(&fact), "evidence": evidence.iter().map(evidence_value).collect::<Vec<_>>() }),
    )
}

fn evidence_value(evidence: &crate::store::Evidence) -> Value {
    let (repo, reference) = evidence.repository_ref.split_once('@').map_or(
        (evidence.repository_ref.as_str(), ""),
        |(repo, reference)| (repo, reference),
    );
    json!({
        "id": evidence.id, "fact_id": evidence.fact_id, "source_ref": evidence.source_ref,
        "source_checksum": evidence.checksum, "fetched_at": evidence.fetched_at,
        "repo": repo, "ref": reference,
        "path": evidence.path, "symbol": evidence.symbol, "start_line": evidence.line_start,
        "end_line": evidence.line_end, "start_col": evidence.column_start, "end_col": evidence.column_end,
        "selected_text_hash": evidence.selected_text_sha256, "resolution_status": evidence.resolution_status,
        "workspace": evidence.workspace, "created_at": evidence.created_at,
    })
}

fn exact_attach_evidence(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let fact_id = required_i64(arguments, "fact_id")?;
    let source_ref = required_string(arguments, "source_ref")?.trim();
    if source_ref.is_empty() {
        return Err(CallError::InvalidParams(
            "source_ref is required".to_owned(),
        ));
    }
    let selected_text = optional_string(arguments, "selected_text")?.unwrap_or("");
    let repo = optional_string(arguments, "repo")?.unwrap_or("");
    let reference = optional_string(arguments, "ref")?.unwrap_or("");
    let repository_ref = if !repo.is_empty() && !reference.is_empty() {
        format!("{repo}@{reference}")
    } else if !repo.is_empty() {
        repo.to_owned()
    } else {
        reference.to_owned()
    };
    let resolution_status =
        optional_string(arguments, "resolution_status")?.unwrap_or("unresolved");
    let evidence = store
        .attach_evidence(&EvidenceSpec {
            fact_id,
            source_ref: source_ref.to_owned(),
            source: repo.to_owned(),
            checksum: optional_string(arguments, "source_checksum")?
                .unwrap_or("")
                .to_owned(),
            fetched_at: optional_string(arguments, "fetched_at")?.map(ToOwned::to_owned),
            repository_ref,
            path: optional_string(arguments, "path")?.unwrap_or("").to_owned(),
            symbol: optional_string(arguments, "symbol")?
                .unwrap_or("")
                .to_owned(),
            line_start: optional_i64(arguments, &["start_line"])?,
            line_end: optional_i64(arguments, &["end_line"])?,
            column_start: optional_i64(arguments, &["start_col"])?,
            column_end: optional_i64(arguments, &["end_col"])?,
            selected_text: selected_text.to_owned(),
            resolution_status: resolution_status.to_owned(),
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    Ok(json!({"fact_id": fact_id, "source_ref": source_ref,
              "dedup": false, "duplicate": false, "evidence": evidence_value(&evidence)}))
}

fn exact_detect_conflicts(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let text = required_string(arguments, "text")?.trim();
    if text.is_empty() {
        return Ok(json!({"error": "text is required"}));
    }
    let terms = text
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>();
    let near_duplicates = store
        .search_facts(text, workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter_map(|fact| {
            let lower = fact.text.to_lowercase();
            let coverage = terms
                .iter()
                .filter(|term| lower.contains(term.as_str()))
                .count() as f64
                / terms.len().max(1) as f64;
            (coverage >= 0.6).then_some(json!({
                "id": fact.id, "text": fact.text, "source": fact.source,
                "project": fact.project, "trust": fact.trust, "strong": fact.strong,
                "coverage": (coverage * 100.0).round() / 100.0,
            }))
        })
        .take(5)
        .collect::<Vec<_>>();
    let decision_conflicts = store
        .detect_conflicts(text, workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .map(|conflict| serde_json::to_value(conflict).expect("conflict serializes"))
        .collect::<Vec<_>>();
    Ok(json!({"text": text, "near_duplicates": near_duplicates,
              "decision_conflicts": decision_conflicts}))
}

fn canonical_fact_value(
    store: &Store,
    fact: &crate::store::Fact,
    workspace: &str,
) -> Result<Value, CallError> {
    let mut value = fact_value(fact);
    if let Some(object) = value.as_object_mut() {
        if let Some(metadata) = store
            .fact_search_metadata(fact.id, workspace)
            .map_err(CallError::Execution)?
        {
            object.insert(
                "category".to_owned(),
                metadata.category.map(Value::String).unwrap_or(Value::Null),
            );
            object.insert("confirmed".to_owned(), json!(metadata.confirmed));
            object.insert("invalid_at".to_owned(), json!(metadata.invalid_at));
            object.insert("archived".to_owned(), json!(metadata.archived));
            object.insert("created_at".to_owned(), json!(metadata.created_at));
            object.insert("updated_at".to_owned(), json!(metadata.updated_at));
        }
    }
    Ok(value)
}

fn compatibility_profile(arguments: &Map<String, Value>) -> Result<String, CallError> {
    let profile = optional_string(arguments, "profile")?
        .unwrap_or("balanced")
        .trim()
        .to_owned();
    if !matches!(
        profile.as_str(),
        "balanced" | "orientation" | "implementation" | "review" | "incident"
    ) {
        return Err(CallError::InvalidParams(format!(
            "unknown retrieval profile: {profile}"
        )));
    }
    Ok(profile)
}

fn advisory_guard(
    arguments: &Map<String, Value>,
    operation: &str,
) -> Result<Option<Value>, CallError> {
    if optional_string(arguments, "purpose")?.unwrap_or("advisory") == "safety_critical" {
        return Ok(Some(json!({
            "error": format!("{operation} is advisory-only and cannot authorize safety-critical work"),
            "code": "advisory_only",
            "memory_policy": "advisory_only",
            "safety_critical_allowed": false,
        })));
    }
    Ok(None)
}

fn trust_rank(value: &str) -> u8 {
    match value {
        "low" => 1,
        "medium" => 2,
        "high" => 3,
        _ => 0,
    }
}

fn trust_at_least(value: &str, minimum: Option<&str>) -> bool {
    minimum.is_none_or(|minimum| trust_rank(value) >= trust_rank(minimum))
}

fn fact_id_argument(arguments: &Map<String, Value>) -> Result<i64, CallError> {
    required_i64(arguments, "id").or_else(|_| required_i64(arguments, "fact_id"))
}

fn exact_remember_fact(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let text = required_string(arguments, "text")?.trim();
    if text.is_empty() {
        return Err(CallError::InvalidParams(
            "fact text must not be empty".to_owned(),
        ));
    }
    if text.chars().count() > MAX_FACT_TEXT_CHARS {
        return Err(CallError::InvalidParams(format!(
            "fact text exceeds the configured limit ({MAX_FACT_TEXT_CHARS} characters)"
        )));
    }
    let strict = optional_bool(arguments, &["strict"], false)?
        || optional_string(arguments, "admission")? == Some("strict");
    if strict && arguments.get("evidence").is_none() && arguments.get("source_ref").is_none() {
        return Ok(json!({
            "error": "strict admission requires evidence",
            "admission": "rejected",
            "result_status": "rejected"
        }));
    }
    let metadata = FactMetadata {
        source: optional_string(arguments, "source")?
            .unwrap_or("")
            .to_owned(),
        project: optional_string(arguments, "project")?
            .unwrap_or("")
            .to_owned(),
        domain: optional_string(arguments, "domain")?
            .unwrap_or("")
            .to_owned(),
        trust: optional_string(arguments, "trust")?
            .unwrap_or("medium")
            .to_owned(),
        strong: optional_bool(arguments, &["strong"], false)?,
        importance: optional_f64(arguments, "importance")?.unwrap_or(0.5),
    };
    let existing_id = store
        .fact_id_for_text(text, workspace)
        .map_err(CallError::Execution)?;
    let mut fact = store
        .remember_fact_with_metadata(text, workspace, &metadata)
        .map_err(CallError::Execution)?;
    if let Some(validity) = optional_string(arguments, "validity")? {
        fact = store
            .set_fact_validity(fact.id, validity, workspace)
            .map_err(CallError::Execution)?
            .ok_or_else(|| {
                CallError::Execution(StoreError::Invalid(
                    "fact disappeared while setting validity".to_owned(),
                ))
            })?;
    }
    if let Some(session) =
        optional_string(arguments, "session_id")?.or(optional_string(arguments, "session")?)
    {
        fact = store
            .set_fact_session(fact.id, session, workspace)
            .map_err(CallError::Execution)?
            .ok_or_else(|| {
                CallError::Execution(StoreError::Invalid(
                    "fact disappeared while setting session".to_owned(),
                ))
            })?;
    }
    let enriched =
        pipeline::maybe_enrich_fact(store, &fact, arguments).map_err(CallError::Execution)?;
    let evidence = attach_remembered_evidence(store, arguments, enriched.id, workspace)?;
    let value = canonical_fact_value(store, &enriched, workspace)?;
    let created = existing_id.is_none();
    let mut result = json!({
        "id": enriched.id,
        "sha256": enriched.sha256,
        "dedup": !created,
        "created": created,
        "created_at": value.get("created_at").cloned().unwrap_or(Value::Null),
        "updated_at": value.get("updated_at").cloned().unwrap_or(Value::Null),
        "fact": value,
        "result_status": if created { "created" } else { "duplicate" },
    });
    if !evidence.is_empty() {
        result["evidence"] = Value::Array(evidence);
    }
    if strict {
        result["admission"] = json!("accepted");
    }
    Ok(result)
}

fn exact_fact_history(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let id = fact_id_argument(arguments)?;
    let Some(fact) = store
        .fact_by_id_for_pipeline(id, workspace)
        .map_err(CallError::Execution)?
    else {
        return Ok(json!({"count": 0, "root_id": id, "chain": [], "history": []}));
    };
    let history = store
        .fact_history(id, workspace)
        .map_err(CallError::Execution)?;
    Ok(json!({
        "count": history.len(),
        "root_id": id,
        "chain": [canonical_fact_value(store, &fact, workspace)?],
        "history": history,
    }))
}

fn exact_confirm_fact(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let id = fact_id_argument(arguments)?;
    let note = optional_string(arguments, "note")?.unwrap_or("confirmed");
    let Some(fact) = store
        .confirm_fact(id, note, workspace)
        .map_err(CallError::Execution)?
    else {
        return Ok(json!({"error": "fact not found or not in your workspace", "id": id}));
    };
    Ok(json!({
        "id": fact.id,
        "confirmed": true,
        "trust": "high",
        "updated_at": canonical_fact_value(store, &fact, workspace)?
            .get("updated_at").cloned().unwrap_or(Value::Null),
        "fact": canonical_fact_value(store, &fact, workspace)?,
    }))
}

fn exact_fact_references(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let id = fact_id_argument(arguments)?;
    let Some(fact) = store
        .fact_by_id_for_pipeline(id, workspace)
        .map_err(CallError::Execution)?
    else {
        return Ok(json!({"error": "fact not found", "fact_id": id}));
    };
    let evidence = store
        .fact_references(id, workspace)
        .map_err(CallError::Execution)?
        .iter()
        .map(evidence_value)
        .collect::<Vec<_>>();
    Ok(json!({
        "fact_id": id,
        "text": fact.text,
        "incoming": {"superseded_by_me": [], "supersedes_me": [],
                     "consolidated_into": [], "referenced_via_supersedes": []},
        "outgoing": {"supersedes": [], "consolidated_from": []},
        "evidence": evidence,
    }))
}

fn exact_search_facts(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    if let Some(value) = advisory_guard(arguments, "search_facts")? {
        return Ok(value);
    }
    let workspace = optional_workspace(arguments)?;
    let query = required_string(arguments, "query")?.trim();
    if query.is_empty() {
        return Err(CallError::InvalidParams(
            "query must not be empty".to_owned(),
        ));
    }
    let profile = compatibility_profile(arguments)?;
    let limit = optional_usize(arguments, &["limit"], 20)?;
    if !(1..=100).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 100".to_owned(),
        ));
    }
    let trust_min = optional_string(arguments, "trust_min")?;
    if let Some(value) = trust_min {
        if trust_rank(value) == 0 {
            return Err(CallError::InvalidParams(
                "trust_min must be low, medium, or high".to_owned(),
            ));
        }
    }
    let filters = fact_filters(arguments)?;
    let category = optional_string(arguments, "category")?;
    let mut facts = store
        .search_facts_with_filters(query, workspace, &filters)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|fact| trust_at_least(&fact.trust, trust_min))
        .filter(|fact| {
            arguments
                .get("strong_only")
                .and_then(Value::as_bool)
                .is_none_or(|strong_only| !strong_only || fact.strong)
        })
        .filter(|fact| {
            category.is_none_or(|category| {
                store
                    .category_name_for_fact(fact.id, workspace)
                    .ok()
                    .flatten()
                    .as_deref()
                    == Some(category)
            })
        })
        .collect::<Vec<_>>();
    facts.truncate(limit);
    if arguments
        .get("semantic")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return pipeline::hybrid_search(store, query, arguments, &facts)
            .map_err(CallError::Execution);
    }
    let chunk_chars = optional_usize(arguments, &["chunk_chars"], 0)?;
    let chunk_overlap = optional_usize(arguments, &["chunk_overlap"], 0)?;
    if chunk_chars > 16_000 || (chunk_chars > 0 && chunk_overlap >= chunk_chars) {
        return Err(CallError::InvalidParams(
            "chunk_chars or chunk_overlap is outside the supported range".to_owned(),
        ));
    }
    let values = facts
        .iter()
        .map(|fact| {
            let mut value = canonical_fact_value(store, fact, workspace)?;
            if chunk_chars > 0 {
                value["chunks"] = fact_chunk_values(fact, chunk_chars, chunk_overlap);
            }
            Ok::<Value, CallError>(value)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "count": values.len(), "facts": values,
        "memory_policy": "advisory_only", "safety_critical_allowed": false,
        "profile": profile, "result_status": if values.is_empty() { "empty" } else { "ok" },
        "retrieval_outcome": if values.is_empty() { "abstained" } else { "matched" },
        "retrieval_mode": "lexical",
    }))
}

fn fact_chunk_values(fact: &crate::store::Fact, chunk_chars: usize, overlap: usize) -> Value {
    let chars = fact.text.chars().collect::<Vec<_>>();
    let step = chunk_chars - overlap;
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < chars.len() {
        let end = (start + chunk_chars).min(chars.len());
        chunks.push(json!({
            "index": chunks.len(),
            "start": start,
            "end": end,
            "content": chars[start..end].iter().collect::<String>(),
        }));
        if end == chars.len() {
            break;
        }
        start += step;
    }
    Value::Array(chunks)
}

fn exact_search_semantic(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    if let Some(value) = advisory_guard(arguments, "search_semantic")? {
        return Ok(value);
    }
    let _ = compatibility_profile(arguments)?;
    if !providers::embeddings_enabled() {
        let workspace = optional_workspace(arguments)?;
        let query = required_string(arguments, "query")?;
        let facts = store
            .search_semantic(query, workspace)
            .map_err(CallError::Execution)?;
        let values = facts
            .iter()
            .map(|fact| canonical_fact_value(store, fact, workspace))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(json!({
            "count": values.len(), "facts": values, "model": providers::embedding_model(),
            "result_status": if facts.is_empty() { "empty" } else { "degraded" },
            "error": "semantic search is disabled; lexical fallback returned"
        }));
    }
    pipeline::semantic_search(store, arguments).map_err(CallError::Execution)
}

fn exact_list_facts(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let limit = optional_usize(arguments, &["limit"], 50)?;
    if !(1..=200).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 200".to_owned(),
        ));
    }
    let category = optional_string(arguments, "category")?;
    let mut facts = store
        .list_facts_with_filters(workspace, &fact_filters(arguments)?)
        .map_err(CallError::Execution)?;
    if let Some(category) = category {
        facts.retain(|fact| {
            store
                .category_name_for_fact(fact.id, workspace)
                .ok()
                .flatten()
                .as_deref()
                == Some(category)
        });
    }
    facts.reverse();
    facts.truncate(limit);
    let values = facts
        .iter()
        .map(|fact| canonical_fact_value(store, fact, workspace))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({"count": values.len(), "facts": values}))
}

fn exact_summarize_index(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let max_chars = optional_usize(arguments, &["max_chars", "chars"], 4_000)?;
    let limit = optional_usize(arguments, &["limit"], 200)?;
    let summary = store
        .summarize_index(workspace)
        .map_err(CallError::Execution)?;
    let trust_min = optional_string(arguments, "trust_min")?;
    let category = optional_string(arguments, "category")?;
    let mut facts = store
        .list_facts_with_filters(workspace, &fact_filters(arguments)?)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|fact| trust_at_least(&fact.trust, trust_min))
        .filter(|fact| {
            arguments
                .get("strong_only")
                .and_then(Value::as_bool)
                .is_none_or(|strong_only| !strong_only || fact.strong)
        })
        .filter(|fact| {
            category.is_none_or(|category| {
                store
                    .category_name_for_fact(fact.id, workspace)
                    .ok()
                    .flatten()
                    .as_deref()
                    == Some(category)
            })
        })
        .collect::<Vec<_>>();
    facts.reverse();
    facts.truncate(limit);
    let total = facts.len();
    let mut index = String::new();
    let mut count = 0usize;
    for fact in &facts {
        let category = store
            .category_name_for_fact(fact.id, workspace)
            .map_err(CallError::Execution)?
            .unwrap_or_default();
        let line = format!(
            "#{} {}{} [{}] {}\n",
            fact.id,
            fact.trust,
            if fact.strong { "!" } else { "" },
            category,
            fact.text
        );
        if index.len() + line.len() > max_chars {
            break;
        }
        index.push_str(&line);
        count += 1;
    }
    Ok(json!({
        "count": count, "total": total, "chars": index.len(),
        "truncated": count < total, "index": index,
        "active_facts": summary.active_facts, "forgotten_facts": summary.forgotten_facts,
        "contexts": summary.contexts, "categories": summary.categories,
        "runs": summary.runs, "measurements": summary.measurements,
        "feedback": summary.feedback,
    }))
}

fn exact_list_categories(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let query = optional_string(arguments, "query")?
        .unwrap_or("")
        .to_lowercase();
    let facts = store.list_facts(workspace).map_err(CallError::Execution)?;
    let categories = store
        .list_categories(workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|category| query.is_empty() || category.name.to_lowercase().contains(&query))
        .map(|category| {
            let active = facts
                .iter()
                .filter(|fact| fact.category_id == Some(category.id))
                .count();
            json!({"id": category.id, "name": category.name, "workspace": category.workspace,
                   "active_facts": active, "facts": active, "created_at": category.created_at})
        })
        .collect::<Vec<_>>();
    Ok(json!({"count": categories.len(), "categories": categories}))
}

fn exact_search_index(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    if let Some(value) = advisory_guard(arguments, "search_index")? {
        return Ok(value);
    }
    let workspace = optional_workspace(arguments)?;
    let query = required_string(arguments, "query")?.trim();
    let limit = optional_usize(arguments, &["limit"], 20)?;
    let max_chars = optional_usize(arguments, &["max_chars", "chars"], 12_000)?;
    let category = optional_string(arguments, "category")?;
    let facts = store
        .search_facts(query, workspace)
        .map_err(CallError::Execution)?
        .into_iter()
        .filter(|fact| {
            category.is_none_or(|category| {
                store
                    .category_name_for_fact(fact.id, workspace)
                    .ok()
                    .flatten()
                    .as_deref()
                    == Some(category)
            })
        })
        .take(limit)
        .collect::<Vec<_>>();
    let mut groups = Vec::<Value>::new();
    for fact in facts {
        let category = store
            .category_name_for_fact(fact.id, workspace)
            .map_err(CallError::Execution)?
            .unwrap_or_else(|| "uncategorized".to_owned());
        let row = json!({"id": fact.id, "category": category, "snippet": fact.text,
                        "trust": fact.trust, "strong": fact.strong,
                        "importance": fact.importance,
                        "updated_at": store.fact_search_metadata(fact.id, workspace)
                            .map_err(CallError::Execution)?.map(|m| m.updated_at)});
        let Some(group) = groups
            .iter_mut()
            .find(|group| group["category"] == category)
        else {
            groups.push(json!({"category": category, "facts": [row]}));
            continue;
        };
        group["facts"]
            .as_array_mut()
            .expect("facts array")
            .push(row);
    }
    let mut bounded_groups = Vec::new();
    let mut shown = 0usize;
    let mut rendered = 0usize;
    let mut truncated = false;
    'groups: for group in groups {
        let category = group["category"].clone();
        let Some(rows) = group["facts"].as_array() else {
            continue;
        };
        let mut bounded = Vec::new();
        for row in rows {
            let candidate = json!({"category": category, "facts": [row]});
            let candidate_size = serde_json::to_vec(&candidate)
                .expect("index group serializes")
                .len();
            if rendered.saturating_add(candidate_size) > max_chars {
                truncated = true;
                break 'groups;
            }
            bounded.push(row.clone());
            rendered += candidate_size;
            shown += 1;
        }
        if !bounded.is_empty() {
            bounded_groups.push(json!({"category": category, "facts": bounded}));
        }
    }
    Ok(json!({"count": shown, "shown": shown, "truncated": truncated, "groups": bounded_groups}))
}

fn exact_categorize_pending(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    pipeline::categorize_pending(store, arguments).map_err(CallError::Execution)
}

fn evidence_argument_maps(arguments: &Map<String, Value>) -> Vec<Map<String, Value>> {
    match arguments.get("evidence") {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_object)
            .cloned()
            .collect(),
        Some(Value::Object(value)) => vec![value.clone()],
        _ if arguments.contains_key("source_ref") => vec![arguments.clone()],
        _ => Vec::new(),
    }
}

fn attach_remembered_evidence(
    store: &Store,
    arguments: &Map<String, Value>,
    fact_id: i64,
    workspace: &str,
) -> Result<Vec<Value>, CallError> {
    let mut result = Vec::new();
    for evidence in evidence_argument_maps(arguments) {
        let source_ref = evidence
            .get("source_ref")
            .or_else(|| evidence.get("ref"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if source_ref.is_empty() {
            return Err(CallError::InvalidParams(
                "evidence source_ref must not be empty".to_owned(),
            ));
        }
        let repo = evidence
            .get("repo")
            .or_else(|| evidence.get("repository"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let reference = evidence.get("ref").and_then(Value::as_str).unwrap_or("");
        let repository_ref = if !repo.is_empty() && !reference.is_empty() {
            format!("{repo}@{reference}")
        } else if !repo.is_empty() {
            repo.to_owned()
        } else {
            reference.to_owned()
        };
        let spec = EvidenceSpec {
            fact_id,
            source_ref: source_ref.to_owned(),
            source: evidence
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or(repo)
                .to_owned(),
            checksum: evidence
                .get("source_checksum")
                .or_else(|| evidence.get("checksum"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            fetched_at: evidence
                .get("fetched_at")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            repository_ref,
            path: evidence
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            symbol: evidence
                .get("symbol")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            line_start: evidence
                .get("start_line")
                .or_else(|| evidence.get("line_start"))
                .and_then(Value::as_i64),
            line_end: evidence
                .get("end_line")
                .or_else(|| evidence.get("line_end"))
                .and_then(Value::as_i64),
            column_start: evidence
                .get("start_col")
                .or_else(|| evidence.get("column_start"))
                .and_then(Value::as_i64),
            column_end: evidence
                .get("end_col")
                .or_else(|| evidence.get("column_end"))
                .and_then(Value::as_i64),
            selected_text: evidence
                .get("selected_text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            resolution_status: evidence
                .get("resolution_status")
                .and_then(Value::as_str)
                .unwrap_or("unresolved")
                .to_owned(),
            workspace: workspace.to_owned(),
        };
        let attached = store.attach_evidence(&spec).map_err(CallError::Execution)?;
        result.push(evidence_value(&attached));
    }
    Ok(result)
}

fn exact_remember_entity(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let name = required_string(arguments, "name")?.trim();
    if name.is_empty() {
        return Err(CallError::InvalidParams(
            "entity name must not be empty".to_owned(),
        ));
    }
    let existed = store
        .list_entities(workspace)
        .map_err(CallError::Execution)?
        .iter()
        .any(|entity| entity.canonical_name == name.to_lowercase());
    let aliases = match arguments.get("aliases") {
        None => Vec::new(),
        Some(Value::Array(_)) => optional_string_array(arguments, "aliases")?.unwrap_or_default(),
        Some(Value::String(alias)) => vec![alias.clone()],
        Some(_) => {
            return Err(CallError::InvalidParams(
                "tool argument aliases must be a string or an array".to_owned(),
            ))
        }
    };
    let entity = store
        .remember_entity(&EntitySpec {
            name: name.to_owned(),
            entity_type: optional_string(arguments, "entity_type")?
                .or(optional_string(arguments, "type")?)
                .unwrap_or("concept")
                .to_owned(),
            aliases,
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    Ok(
        json!({"id": entity.id, "name": entity.name, "created": !existed,
              "entity": entity}),
    )
}

fn exact_remember_relation(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let subject = required_string(arguments, "subject")?.trim();
    let predicate = required_string(arguments, "predicate")?.trim();
    let object = required_string(arguments, "object")?.trim();
    if subject.is_empty() || predicate.is_empty() || object.is_empty() {
        return Err(CallError::InvalidParams(
            "subject, predicate, and object are required".to_owned(),
        ));
    }
    let subject_entity = store
        .remember_entity(&EntitySpec {
            name: subject.to_owned(),
            entity_type: "concept".to_owned(),
            aliases: Vec::new(),
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    let object_entity = store
        .remember_entity(&EntitySpec {
            name: object.to_owned(),
            entity_type: "concept".to_owned(),
            aliases: Vec::new(),
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    let source_fact_id = optional_i64(arguments, &["source_fact_id", "fact_id"])?;
    let relation = store
        .remember_relation(&RelationSpec {
            subject: subject_entity.name.clone(),
            predicate: predicate.to_owned(),
            object: object_entity.name.clone(),
            source_fact_id,
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    Ok(json!({"id": relation.id, "subject": subject_entity.name,
              "predicate": relation.predicate, "object": object_entity.name,
              "dedup": false, "relation": relation}))
}

fn exact_record_feedback(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let feedback_id = required_string(arguments, "feedback_id")?.trim();
    let site = optional_string(arguments, "site")?.unwrap_or("").trim();
    let item_ref = required_string(arguments, "item_ref")?.trim();
    let signal = required_string(arguments, "signal")?.trim();
    let item_type = optional_string(arguments, "item_type")?
        .unwrap_or("fact")
        .trim();
    let duplicate = store
        .query_feedback("", workspace)
        .map_err(CallError::Execution)?
        .iter()
        .any(|feedback| feedback.feedback_id == feedback_id);
    let feedback = store
        .record_feedback(&FeedbackSpec {
            feedback_id: feedback_id.to_owned(),
            site: site.to_owned(),
            item_type: item_type.to_owned(),
            item_ref: item_ref.to_owned(),
            signal: signal.to_owned(),
            query_hash: optional_string(arguments, "query_hash")?
                .unwrap_or("")
                .to_owned(),
            workspace: workspace.to_owned(),
        })
        .map_err(CallError::Execution)?;
    Ok(json!({"accepted": true, "duplicate": duplicate,
              "result_status": if duplicate { "duplicate" } else { "recorded" },
              "feedback": feedback}))
}

fn exact_query_feedback(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let query = optional_string(arguments, "query")?.unwrap_or("");
    let site = optional_string(arguments, "site")?;
    let limit = optional_usize(arguments, &["limit"], 100)?;
    let mut feedback = store
        .query_feedback(query, workspace)
        .map_err(CallError::Execution)?;
    if let Some(site) = site {
        feedback.retain(|item| item.site == site);
    }
    feedback.reverse();
    feedback.truncate(limit);
    let mut signals = HashMap::<String, i64>::new();
    for item in &feedback {
        *signals.entry(item.signal.clone()).or_default() += 1;
    }
    Ok(json!({"count": feedback.len(), "feedback": feedback, "signals": signals}))
}

fn exact_export_rdf(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let rdf = store.export_rdf(workspace).map_err(CallError::Execution)?;
    let triples = rdf.lines().filter(|line| !line.trim().is_empty()).count();
    Ok(
        json!({"format": "text/turtle", "triples": triples, "records": triples,
              "truncated": false, "rdf": rdf}),
    )
}

fn exact_export(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = optional_workspace(arguments)?;
    let snapshot = if workspace.is_empty() {
        store.export_all().map_err(CallError::Execution)?
    } else {
        store
            .export_snapshot(workspace)
            .map_err(CallError::Execution)?
    };
    let mut value = serde_json::to_value(snapshot).expect("memory export serializes");
    if let Some(object) = value.as_object_mut() {
        object.insert("workspace".to_owned(), json!(workspace));
    }
    Ok(value)
}

fn database_value(info: &crate::store::DatabaseInfo) -> Value {
    json!({"name": info.name, "database": info.name,
           "active": info.active, "selected": info.active, "archived": info.archived,
           "bytes": info.bytes})
}

fn exact_create_database(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let name = database_name_argument(arguments)?;
    let info = store.create_database(name).map_err(CallError::Execution)?;
    Ok(database_value(&info))
}

fn exact_list_databases(
    store: &Store,
    _arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let databases = store
        .list_databases()
        .map_err(CallError::Execution)?
        .iter()
        .map(database_value)
        .collect::<Vec<_>>();
    Ok(json!({"count": databases.len(), "databases": databases}))
}

fn exact_archive_database(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let name = database_name_argument(arguments)?;
    let hard = optional_bool(arguments, &["hard"], false)?;
    if hard && !optional_bool(arguments, &["confirm"], false)? {
        return Ok(json!({"error": "confirm: true is required for hard archive"}));
    }
    let archived = store.archive_database(name).map_err(CallError::Execution)?;
    let Some(info) = archived else {
        return Ok(json!({"error": format!("database not found: {name}")}));
    };
    if hard {
        let deleted = store.delete_database(name).map_err(CallError::Execution)?;
        return Ok(json!({"archived": name, "deleted": deleted, "hard": true}));
    }
    Ok(json!({"archived": name, "hard": false, "database": database_value(&info)}))
}

fn exact_reset_database(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let name = optional_string(arguments, "name")?
        .or(optional_string(arguments, "database")?)
        .unwrap_or("current");
    let info = store.select_database(name).map_err(CallError::Execution)?;
    Ok(
        json!({"database": info.name, "reset": true, "selected": info.active,
              "active": info.active, "info": database_value(&info)}),
    )
}

fn exact_select_database(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let name = database_name_argument(arguments)?;
    let info = store.select_database(name).map_err(CallError::Execution)?;
    Ok(database_value(&info))
}

fn exact_current_database(
    store: &Store,
    _arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let info = store.current_database().map_err(CallError::Execution)?;
    Ok(database_value(&info))
}

fn workspace_value(workspace: &crate::store::Workspace) -> Value {
    json!({"workspace": workspace.id, "id": workspace.id, "status": workspace.status})
}

fn exact_create_workspace(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = workspace_argument(arguments)?;
    let existed = store
        .list_workspaces()
        .map_err(CallError::Execution)?
        .iter()
        .any(|record| record.id == workspace);
    let value = store
        .create_workspace(workspace)
        .map_err(CallError::Execution)?;
    let mut result = workspace_value(&value);
    result["created"] = json!(!existed);
    Ok(result)
}

fn exact_list_workspaces(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let status = optional_string(arguments, "status")?;
    if let Some(status) = status {
        if !matches!(status, "active" | "archived" | "reset") {
            return Err(CallError::InvalidParams(
                "status must be active, archived, or reset".to_owned(),
            ));
        }
    }
    let workspaces = store
        .list_workspaces()
        .map_err(CallError::Execution)?
        .iter()
        .filter(|workspace| status.is_none_or(|status| workspace.status == status))
        .map(|workspace| {
            let active_facts = store
                .list_facts(&workspace.id)
                .map_err(CallError::Execution)?
                .len();
            let forgotten_facts = store
                .list_forgotten(&workspace.id)
                .map_err(CallError::Execution)?
                .len();
            let entities = store
                .list_entities(&workspace.id)
                .map_err(CallError::Execution)?
                .len();
            let relations = store
                .list_relations(&workspace.id)
                .map_err(CallError::Execution)?
                .len();
            let decisions = store
                .list_decisions(&workspace.id)
                .map_err(CallError::Execution)?
                .len();
            let evidence = store
                .list_evidence(&workspace.id)
                .map_err(CallError::Execution)?
                .len();
            Ok::<Value, CallError>(json!({
                "workspace": workspace.id, "id": workspace.id, "status": workspace.status,
                "active_facts": active_facts, "facts": active_facts + forgotten_facts,
                "entities": entities, "relations": relations, "decisions": decisions,
                "evidence": evidence,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({"count": workspaces.len(), "workspaces": workspaces}))
}

fn exact_archive_workspace(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = workspace_argument(arguments)?;
    let hard = optional_bool(arguments, &["hard"], false)?;
    if hard && !optional_bool(arguments, &["confirm"], false)? {
        return Ok(json!({"error": "confirm: true is required for hard archive"}));
    }
    let value = store
        .archive_workspace(workspace)
        .map_err(CallError::Execution)?;
    let Some(value) = value else {
        return Ok(json!({"error": format!("workspace not found: {workspace}")}));
    };
    Ok(json!({"archived": workspace, "hard": hard, "workspace": workspace_value(&value)}))
}

fn exact_reset_workspace(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = workspace_argument(arguments)?;
    let hard = optional_bool(arguments, &["hard"], false)?;
    if hard && !optional_bool(arguments, &["confirm"], false)? {
        return Ok(json!({"error": "confirm: true is required for hard reset"}));
    }
    let value = store
        .reset_workspace(workspace)
        .map_err(CallError::Execution)?;
    Ok(json!({"reset": workspace, "hard": hard, "workspace": workspace_value(&value)}))
}

fn exact_backup_workspace(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = exact_workspace(arguments)?;
    let backup = store
        .backup_workspace_default(workspace)
        .map_err(CallError::Execution)?;
    let file_name = Path::new(&backup.path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(&backup.path);
    Ok(
        json!({"workspace": workspace, "backup": file_name, "size": backup.bytes,
              "bytes": backup.bytes, "facts": backup.facts, "contexts": backup.contexts}),
    )
}

fn exact_backup_database(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let name = optional_string(arguments, "name")?;
    let backup = store
        .backup_database_default(name)
        .map_err(CallError::Execution)?;
    let file_name = Path::new(&backup.path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(&backup.path);
    Ok(json!({"database": backup.database, "backup": file_name, "size": backup.bytes}))
}

fn exact_delete_database(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let name = database_name_argument(arguments)?;
    if !optional_bool(arguments, &["confirm"], false)? {
        return Ok(json!({"error": "confirm: true is required to delete a database"}));
    }
    if store.delete_database(name).map_err(CallError::Execution)? {
        Ok(json!({"deleted": name}))
    } else {
        Ok(json!({"error": format!("database {name} not found")}))
    }
}

fn fact_value(fact: &crate::store::Fact) -> Value {
    serde_json::to_value(fact).expect("fact serializes")
}

fn exact_stats(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let facts = store.list_facts(workspace).map_err(CallError::Execution)?;
    let summary = store
        .summarize_index(workspace)
        .map_err(CallError::Execution)?;
    let mut by_trust = HashMap::<String, i64>::new();
    let mut by_domain = HashMap::<String, i64>::new();
    let mut strong = 0i64;
    for fact in &facts {
        *by_trust.entry(fact.trust.clone()).or_default() += 1;
        *by_domain
            .entry(if fact.domain.is_empty() {
                "(none)".to_owned()
            } else {
                fact.domain.clone()
            })
            .or_default() += 1;
        if fact.strong {
            strong += 1;
        }
    }
    Ok(json!({
        "total": facts.len(), "strong": strong, "by_trust": by_trust, "by_domain": by_domain,
        "counts": {"entities": store.list_entities(workspace).map_err(CallError::Execution)?.len(),
                   "relations": store.list_relations(workspace).map_err(CallError::Execution)?.len(),
                   "decisions": store.list_decisions(workspace).map_err(CallError::Execution)?.len(),
                   "evidence": store.list_evidence(workspace).map_err(CallError::Execution)?.len(),
                   "runs": summary.runs, "measurements": summary.measurements, "feedback": summary.feedback},
        "access": {"events": 0, "by_site": {}, "last_at": "", "pull_events": 0,
                   "pull_hits": 0, "pull_misses": 0, "hit_rate": 0.0},
    }))
}

fn exact_chunk_fact(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let chunk_chars = optional_usize(arguments, &["chunk_chars"], 4000)?;
    let overlap = optional_usize(arguments, &["chunk_overlap"], 0)?;
    let start_chunk = optional_usize(arguments, &["start_chunk"], 0)?;
    let max_chunks = optional_usize(arguments, &["max_chunks"], 8)?;
    if chunk_chars == 0
        || chunk_chars > 16_000
        || overlap >= chunk_chars
        || max_chunks == 0
        || max_chunks > 32
    {
        return Err(CallError::InvalidParams(
            "invalid fact chunk bounds".to_owned(),
        ));
    }
    let fact = if let Some(id) = optional_i64(arguments, &["id", "fact_id"])? {
        store
            .fact_by_id_for_pipeline(id, workspace)
            .map_err(CallError::Execution)?
    } else if let Some(hash) = optional_string(arguments, "sha256")? {
        store
            .fact_by_sha256_for_pipeline(hash, workspace)
            .map_err(CallError::Execution)?
    } else {
        None
    };
    let Some(fact) = fact else {
        return Ok(json!({"error": "fact not found or not in your workspace"}));
    };
    let chars = fact.text.chars().collect::<Vec<_>>();
    let step = chunk_chars - overlap;
    let mut all = Vec::new();
    let mut start = 0usize;
    while start < chars.len() {
        let end = (start + chunk_chars).min(chars.len());
        all.push(json!({"index": all.len(), "start": start, "end": end,
                        "content": chars[start..end].iter().collect::<String>()}));
        if end == chars.len() {
            break;
        }
        start += step;
    }
    let total_chunks = all.len();
    let bounded_start = start_chunk.min(total_chunks);
    let end_chunk = total_chunks.min(bounded_start.saturating_add(max_chunks));
    let chunks = all[bounded_start..end_chunk].to_vec();
    let mut fact_metadata = fact_value(&fact);
    if let Some(object) = fact_metadata.as_object_mut() {
        object.remove("text");
        object.insert("text_length".to_owned(), json!(fact.text.chars().count()));
    }
    Ok(
        json!({"fact": fact_metadata, "chunks": chunks, "start_chunk": bounded_start,
              "next_chunk": if end_chunk < total_chunks { json!(end_chunk) } else { Value::Null },
              "total_chunks": total_chunks, "chunk_chars": chunk_chars, "chunk_overlap": overlap}),
    )
}

fn exact_review_pending(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let limit = optional_usize(arguments, &["limit"], 20)?;
    if !(1..=100).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 100".to_owned(),
        ));
    }
    let facts = store
        .review_pending(workspace)
        .map_err(CallError::Execution)?;
    let total = facts.len();
    Ok(json!({"count": total.min(limit), "total": total,
              "facts": facts.into_iter().take(limit).map(|fact| fact_value(&fact)).collect::<Vec<_>>() }))
}

fn exact_facts_for_session(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let session = required_string(arguments, "session_ref")?;
    let limit = optional_usize(arguments, &["limit"], 50)?;
    let mut facts = store
        .facts_for_session(session, workspace)
        .map_err(CallError::Execution)?;
    facts.truncate(limit);
    Ok(json!({"count": facts.len(), "facts": facts.iter().map(fact_value).collect::<Vec<_>>() }))
}

fn exact_list_sessions(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let limit = optional_usize(arguments, &["limit"], 50)?;
    let mut grouped = HashMap::<String, i64>::new();
    for fact in store.list_facts(workspace).map_err(CallError::Execution)? {
        let source = if fact.source.is_empty() {
            fact.session_id
        } else {
            fact.source
        };
        if !source.is_empty() {
            *grouped.entry(source).or_default() += 1;
        }
    }
    let mut sessions = grouped
        .into_iter()
        .map(|(source, facts)| json!({"source": source, "facts": facts, "last_activity": Value::Null}))
        .collect::<Vec<_>>();
    sessions.sort_by(|left, right| left["source"].as_str().cmp(&right["source"].as_str()));
    sessions.truncate(limit);
    Ok(json!({"count": sessions.len(), "sessions": sessions}))
}

fn exact_forget_fact(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let fact = if let Some(id) = optional_i64(arguments, &["id", "fact_id"])? {
        Some((
            id,
            store
                .fact_by_id_for_pipeline(id, workspace)
                .map_err(CallError::Execution)?,
        ))
    } else if let Some(hash) = optional_string(arguments, "sha256")? {
        let fact = store
            .fact_by_sha256_for_pipeline(hash, workspace)
            .map_err(CallError::Execution)?;
        Some((fact.as_ref().map(|fact| fact.id).unwrap_or(0), fact))
    } else {
        None
    };
    let Some((id, fact)) = fact else {
        return Ok(json!({"error": "id or sha256 is required"}));
    };
    if id <= 0 || fact.is_none() {
        return Ok(json!({"archived": 0, "status": "not_found"}));
    }
    let archived = store
        .forget_fact(id, workspace)
        .map_err(CallError::Execution)?
        .is_some();
    Ok(json!({"archived": if archived { 1 } else { 0 }, "status": "forgotten", "id": id}))
}

fn exact_restore_fact(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let id = required_i64(arguments, "id")?;
    let Some(existing) = store
        .fact_by_id_for_pipeline(id, workspace)
        .map_err(CallError::Execution)?
    else {
        return Ok(json!({"error": "fact not found or not in your workspace", "id": id}));
    };
    if store
        .fact_search_metadata(id, workspace)
        .map_err(CallError::Execution)?
        .is_some_and(|metadata| metadata.archived)
    {
        return Ok(
            json!({"error": "fact is archived (soft-deleted); re-remember it instead", "id": id}),
        );
    }
    store
        .restore_fact(id, workspace)
        .map_err(CallError::Execution)?;
    Ok(json!({"restored": id, "from": existing.lifecycle, "to": "active"}))
}

fn exact_list_forgotten(store: &Store, arguments: &Map<String, Value>) -> Result<Value, CallError> {
    let workspace = arguments
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or("");
    let limit = optional_usize(arguments, &["limit"], 50)?;
    if !(1..=200).contains(&limit) {
        return Err(CallError::InvalidParams(
            "limit must be between 1 and 200".to_owned(),
        ));
    }
    let mut facts = store
        .list_forgotten(workspace)
        .map_err(CallError::Execution)?;
    facts.sort_by(|left, right| {
        right
            .importance
            .total_cmp(&left.importance)
            .then_with(|| left.id.cmp(&right.id))
    });
    facts.truncate(limit);
    Ok(json!({"count": facts.len(), "facts": facts.iter().map(fact_value).collect::<Vec<_>>() }))
}

fn call_tool_with_coordinator(
    params: Option<&Value>,
    coordinator: &BackendCoordinator,
) -> Result<Value, CallError> {
    let params_object = params.and_then(Value::as_object).ok_or_else(|| {
        CallError::InvalidParams("tools/call params must be an object".to_owned())
    })?;
    let name = params_object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| CallError::InvalidParams("tools/call name must be a string".to_owned()))?;
    let arguments = params_object
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    if !arguments.is_object() {
        return Err(CallError::InvalidParams(
            "tools/call arguments must be an object".to_owned(),
        ));
    }
    let request = json!({"name": name, "arguments": arguments});
    coordinator
        .execute_tool(
            name,
            request["arguments"].as_object().expect("object checked"),
            |store| call_tool(Some(&request), store).map_err(call_error_to_backend_error),
        )
        .map_err(backend_error_to_call_error)
}

pub(crate) fn replay_tool(
    name: &str,
    arguments: &Map<String, Value>,
    store: &Store,
) -> Result<Value, StoreError> {
    let request = json!({"name": name, "arguments": arguments});
    call_tool(Some(&request), store).map_err(call_error_to_store_error)
}

fn call_error_to_store_error(error: CallError) -> StoreError {
    match error {
        CallError::InvalidParams(message) => StoreError::Invalid(message),
        CallError::Execution(error) => error,
    }
}

fn call_error_to_backend_error(error: CallError) -> BackendToolError {
    match error {
        CallError::InvalidParams(message) => BackendToolError::InvalidParams(message),
        CallError::Execution(error) => BackendToolError::Execution(error),
    }
}

fn backend_error_to_call_error(error: BackendToolError) -> CallError {
    match error {
        BackendToolError::InvalidParams(message) => CallError::InvalidParams(message),
        BackendToolError::Execution(error) => CallError::Execution(error),
    }
}

fn required_string<'a>(arguments: &'a Map<String, Value>, key: &str) -> Result<&'a str, CallError> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| CallError::InvalidParams(format!("tool argument {key} must be a string")))
}

fn database_name_argument(arguments: &Map<String, Value>) -> Result<&str, CallError> {
    required_string(arguments, "name").or_else(|_| required_string(arguments, "database"))
}

fn optional_string<'a>(
    arguments: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, CallError> {
    match arguments.get(key) {
        None => Ok(None),
        Some(value) => value.as_str().map(Some).ok_or_else(|| {
            CallError::InvalidParams(format!("tool argument {key} must be a string"))
        }),
    }
}

fn optional_string_array(
    arguments: &Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<String>>, CallError> {
    let Some(value) = arguments.get(key) else {
        return Ok(None);
    };
    let values = value
        .as_array()
        .ok_or_else(|| CallError::InvalidParams(format!("tool argument {key} must be an array")))?;
    values
        .iter()
        .map(|value| {
            value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                CallError::InvalidParams(format!("tool argument {key} must contain strings"))
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn required_string_array(
    arguments: &Map<String, Value>,
    keys: &[&str],
) -> Result<Vec<String>, CallError> {
    let (key, value) = keys
        .iter()
        .find_map(|key| arguments.get(*key).map(|value| (*key, value)))
        .ok_or_else(|| {
            CallError::InvalidParams(format!(
                "tool argument {} must be an array of strings",
                keys.join(" or ")
            ))
        })?;
    let values = value.as_array().ok_or_else(|| {
        CallError::InvalidParams(format!("tool argument {key} must be an array of strings"))
    })?;
    values
        .iter()
        .map(|value| {
            value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                CallError::InvalidParams(format!("tool argument {key} must contain strings"))
            })
        })
        .collect()
}

fn string_array_or_single(
    arguments: &Map<String, Value>,
    array_keys: &[&str],
    single_keys: &[&str],
) -> Result<Vec<String>, CallError> {
    for key in array_keys {
        if arguments.contains_key(*key) {
            return Ok(optional_string_array(arguments, key)?.unwrap_or_default());
        }
    }
    for key in single_keys {
        if let Some(value) = optional_string(arguments, key)? {
            return Ok(vec![value.to_owned()]);
        }
    }
    Err(CallError::InvalidParams(
        "tool requires text or an array of texts".to_owned(),
    ))
}

fn optional_text_or_json(
    arguments: &Map<String, Value>,
    keys: &[&str],
) -> Result<String, CallError> {
    let Some((key, value)) = keys
        .iter()
        .find_map(|key| arguments.get(*key).map(|value| (*key, value)))
    else {
        return Ok(String::new());
    };
    if let Some(text) = value.as_str() {
        return Ok(text.to_owned());
    }
    serde_json::to_string(value).map_err(|_| {
        CallError::InvalidParams(format!("tool argument {key} must be serializable JSON"))
    })
}

fn optional_usize(
    arguments: &Map<String, Value>,
    keys: &[&str],
    default: usize,
) -> Result<usize, CallError> {
    let Some((key, value)) = keys
        .iter()
        .find_map(|key| arguments.get(*key).map(|value| (*key, value)))
    else {
        return Ok(default);
    };
    let number = value.as_u64().ok_or_else(|| {
        CallError::InvalidParams(format!(
            "tool argument {key} must be a non-negative integer"
        ))
    })?;
    usize::try_from(number).map_err(|_| {
        CallError::InvalidParams(format!(
            "tool argument {key} is too large for this platform"
        ))
    })
}

fn optional_i64(arguments: &Map<String, Value>, keys: &[&str]) -> Result<Option<i64>, CallError> {
    let Some((key, value)) = keys
        .iter()
        .find_map(|key| arguments.get(*key).map(|value| (*key, value)))
    else {
        return Ok(None);
    };
    value
        .as_i64()
        .map(Some)
        .ok_or_else(|| CallError::InvalidParams(format!("tool argument {key} must be an integer")))
}

fn optional_bool(
    arguments: &Map<String, Value>,
    keys: &[&str],
    default: bool,
) -> Result<bool, CallError> {
    let Some((key, value)) = keys
        .iter()
        .find_map(|key| arguments.get(*key).map(|value| (*key, value)))
    else {
        return Ok(default);
    };
    value
        .as_bool()
        .ok_or_else(|| CallError::InvalidParams(format!("tool argument {key} must be a boolean")))
}

fn optional_f64(arguments: &Map<String, Value>, key: &str) -> Result<Option<f64>, CallError> {
    match arguments.get(key) {
        None => Ok(None),
        Some(value) => value.as_f64().map(Some).ok_or_else(|| {
            CallError::InvalidParams(format!("tool argument {key} must be a number"))
        }),
    }
}

fn required_f64(arguments: &Map<String, Value>, key: &str) -> Result<f64, CallError> {
    arguments
        .get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| CallError::InvalidParams(format!("tool argument {key} must be a number")))
}

fn fact_filters(arguments: &Map<String, Value>) -> Result<FactFilters, CallError> {
    Ok(FactFilters {
        source: optional_string(arguments, "source")?.map(ToOwned::to_owned),
        project: optional_string(arguments, "project")?.map(ToOwned::to_owned),
        domain: optional_string(arguments, "domain")?.map(ToOwned::to_owned),
        trust: optional_string(arguments, "trust")?.map(ToOwned::to_owned),
        strong: if arguments.contains_key("strong") {
            Some(optional_bool(arguments, &["strong"], false)?)
        } else {
            None
        },
    })
}

fn required_context_workspace(arguments: &Map<String, Value>) -> Result<&str, CallError> {
    let workspace = arguments
        .get("workspace")
        .or_else(|| arguments.get("workspace_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            CallError::InvalidParams(
                "context tools require a non-empty workspace or workspace_id".to_owned(),
            )
        })?;
    if workspace.trim().is_empty() {
        return Err(CallError::InvalidParams(
            "context tools require a non-empty workspace or workspace_id".to_owned(),
        ));
    }
    Ok(workspace)
}

fn required_i64(arguments: &Map<String, Value>, key: &str) -> Result<i64, CallError> {
    arguments
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| CallError::InvalidParams(format!("tool argument {key} must be an integer")))
}

fn workspace_argument(arguments: &Map<String, Value>) -> Result<&str, CallError> {
    ["workspace_id", "workspace", "name"]
        .into_iter()
        .find_map(|key| arguments.get(key).and_then(Value::as_str))
        .ok_or_else(|| {
            CallError::InvalidParams(
                "workspace tool requires workspace_id, workspace, or name".to_owned(),
            )
        })
}

fn result_response(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BackendCoordinator;
    use crate::store::Store;
    use std::ffi::OsString;
    use std::time::{SystemTime, UNIX_EPOCH};

    const OPTIONAL_PROVIDER_FLAGS: [&str; 5] = [
        "MEMORY_MCP_EMBEDDINGS",
        "MEMORY_MCP_EXTRACT",
        "MEMORY_MCP_RECALL",
        "MEMORY_MCP_VERIFY",
        "MEMORY_MCP_CATEGORIZE",
    ];

    struct IsolatedProviderEnvironment {
        previous: Vec<(&'static str, Option<OsString>)>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    fn isolated_provider_environment() -> IsolatedProviderEnvironment {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("provider environment lock");
        let previous = OPTIONAL_PROVIDER_FLAGS
            .iter()
            .map(|name| (*name, std::env::var_os(name)))
            .collect::<Vec<_>>();
        for name in OPTIONAL_PROVIDER_FLAGS {
            std::env::set_var(name, "0");
        }
        IsolatedProviderEnvironment {
            previous,
            _lock: lock,
        }
    }

    impl Drop for IsolatedProviderEnvironment {
        fn drop(&mut self) {
            for (name, value) in &self.previous {
                if let Some(value) = value {
                    std::env::set_var(name, value);
                } else {
                    std::env::remove_var(name);
                }
            }
        }
    }

    #[test]
    fn initialize_and_tools_list_match_contract_baseline() {
        let store = Store::in_memory().unwrap();
        let initialize = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}"#,
            &store,
        )
        .unwrap();
        assert_eq!(initialize["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(initialize["result"]["serverInfo"]["name"], SERVER_NAME);

        let list =
            handle_line(r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#, &store).unwrap();
        assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 80);
        assert!(list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "decay_sweep"));
    }

    #[test]
    fn every_advertised_tool_crosses_the_coordinator_route() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let database = std::env::temp_dir().join(format!(
            "memory-mcp-rust-coverage-{}-{timestamp}.db",
            std::process::id()
        ));
        let coordinator = BackendCoordinator::sqlite_only(&database).expect("coordinator");

        for name in tools::TOOL_NAMES {
            let params = json!({"name": name, "arguments": {}});
            let result = call_tool_with_coordinator(Some(&params), &coordinator);
            if let Err(error) = result {
                match error {
                    CallError::InvalidParams(_) => {}
                    CallError::Execution(StoreError::Invalid(message)) => {
                        assert!(
                            !message.contains("tool not implemented in parity slice"),
                            "{name} fell through the coordinator route: {message}"
                        );
                    }
                    CallError::Execution(_) => {}
                }
            }
        }

        let _ = std::fs::remove_file(&database);
        let _ = std::fs::remove_file(database.with_extension("outbox.jsonl"));
    }

    #[test]
    fn core_tool_round_trip_uses_one_stdio_facing_dispatcher() {
        let store = Store::in_memory().unwrap();
        let remember = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remember_fact","arguments":{"text":"SQLite fallback","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!remember["result"]["isError"].as_bool().unwrap());
        let search = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_facts","arguments":{"query":"fallback","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let text = search["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("SQLite fallback"));
    }

    #[test]
    fn database_tools_switch_and_backup_a_file_backed_store() {
        let root = std::env::temp_dir().join(format!(
            "memory-mcp-rust-protocol-databases-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("facts.db");
        let store = Store::open(&path).unwrap();

        let create = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "create_database",
                    "arguments": {"name": "protocol"}
                }
            }),
            &store,
        )
        .unwrap();
        assert!(create["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"name\":\"protocol\""));

        let select = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"select_database","arguments":{"name":"protocol"}}}"#,
            &store,
        )
        .unwrap();
        assert!(select["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"active\":true"));

        let remember = handle_line(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"remember_fact","arguments":{"text":"protocol database fact","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!remember["result"]["isError"].as_bool().unwrap());

        let backup = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "backup_database",
                    "arguments": {
                        "database": "current",
                        "path": "protocol-backup.db"
                    }
                }
            }),
            &store,
        )
        .unwrap();
        assert!(backup["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"bytes\""));
        assert!(!backup["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"path\""));

        let current = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"current_database","arguments":{}}}"#,
            &store,
        )
        .unwrap();
        let current_text = current["result"]["content"][0]["text"].as_str().unwrap();
        assert!(current_text.contains("\"name\":\"protocol\""));
        assert!(!current_text.contains("\"path\""));

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn protocol_errors_and_notifications_follow_json_rpc_contract() {
        let store = Store::in_memory().unwrap();
        assert_eq!(handle_line("{", &store).unwrap()["error"]["code"], -32700);
        assert_eq!(
            handle_line(r#"{"jsonrpc":"2.0","id":1,"method":"missing"}"#, &store).unwrap()["error"]
                ["code"],
            -32601
        );
        assert!(handle_line(r#"{"jsonrpc":"2.0","method":"tools/list"}"#, &store).is_none());
    }

    #[test]
    fn lifecycle_tools_are_reachable_through_stdio_dispatch() {
        let store = Store::in_memory().unwrap();
        let workspace = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_workspace","arguments":{"workspace_id":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert_eq!(workspace["result"]["isError"], false);

        let remember = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"remember_fact","arguments":{"text":"retained","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let id = serde_json::from_str::<Value>(
            remember["result"]["content"][0]["text"].as_str().unwrap(),
        )
        .unwrap()["id"]
            .as_i64()
            .unwrap();
        let request = format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"forget_fact","arguments":{{"id":{id},"workspace":"w"}}}}}}"#
        );
        let forgotten = handle_line(&request, &store).unwrap();
        let forgotten_text = forgotten["result"]["content"][0]["text"].as_str().unwrap();
        assert!(forgotten_text.contains("forgotten"));

        let listed = handle_line(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_forgotten","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(listed["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("forgotten"));
    }

    #[test]
    fn context_retrieval_tools_preserve_metadata_and_lineage() {
        let store = Store::in_memory().unwrap();
        let put = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"put_context","arguments":{"ref":"ctx-a","name":"Architecture","content":"Rust SQLite","schema":"text/plain","source":"design","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let put_text = put["result"]["content"][0]["text"].as_str().unwrap();
        assert!(put_text.contains("text/plain"));

        let search = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_context","arguments":{"query":"sqlite","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(search["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("ctx-a"));

        let chunks = handle_line(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"chunk_context","arguments":{"ref":"ctx-a","max_bytes":4,"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let chunks_text = chunks["result"]["content"][0]["text"].as_str().unwrap();
        let chunks_value: Value = serde_json::from_str(chunks_text).unwrap();
        assert_eq!(chunks_value.as_array().unwrap().len(), 3);

        let reduce = handle_line(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"reduce_context","arguments":{"refs":["ctx-a"],"ref":"ctx-r","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!reduce["result"]["isError"].as_bool().unwrap());
        let map = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"context_map","arguments":{"ref":"ctx-r","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let map_text = map["result"]["content"][0]["text"].as_str().unwrap();
        let map_value: Value = serde_json::from_str(map_text).unwrap();
        assert_eq!(map_value.as_array().unwrap().len(), 1);
        assert_eq!(map_value[0]["relation"], "reduced_from");
    }

    #[test]
    fn ingestion_and_recall_tools_are_reachable_through_stdio_dispatch() {
        let _provider_environment = isolated_provider_environment();
        let store = Store::in_memory().unwrap();
        let absorbed = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"absorb","arguments":{"texts":["Rust memory","SQLite index"],"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let absorbed_text = absorbed["result"]["content"][0]["text"].as_str().unwrap();
        let absorbed_value: Value = serde_json::from_str(absorbed_text).unwrap();
        assert_eq!(absorbed_value.as_array().unwrap().len(), 2);

        let turn = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ingest_turn","arguments":{"turn":"Rust turn","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let turn_text = turn["result"]["content"][0]["text"].as_str().unwrap();
        let turn_value: Value = serde_json::from_str(turn_text).unwrap();
        let turn_id = turn_value["id"].as_i64().unwrap();

        let chunks_request = format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"chunk_fact","arguments":{{"fact_id":{turn_id},"max_bytes":4,"workspace":"w"}}}}}}"#
        );
        let chunks = handle_line(&chunks_request, &store).unwrap();
        let chunks_text = chunks["result"]["content"][0]["text"].as_str().unwrap();
        let chunks_value: Value = serde_json::from_str(chunks_text).unwrap();
        assert_eq!(chunks_value[0]["fact_id"].as_i64().unwrap(), turn_id);
        assert!(chunks_value
            .as_array()
            .unwrap()
            .iter()
            .all(|chunk| chunk["byte_size"].as_i64().unwrap() <= 4));

        let put = handle_line(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"put_context","arguments":{"ref":"ctx-rust","name":"Rust context","content":"Rust workspace context","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!put["result"]["isError"].as_bool().unwrap());

        let semantic = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"search_semantic","arguments":{"query":"SQLite","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(semantic["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("SQLite index"));

        let recall = handle_line(
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"compose_recall","arguments":{"query":"Rust","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let recall_text = recall["result"]["content"][0]["text"].as_str().unwrap();
        let recall_value: Value = serde_json::from_str(recall_text).unwrap();
        assert!(!recall_value["facts"].as_array().unwrap().is_empty());
        assert_eq!(recall_value["contexts"][0]["reference"], "ctx-rust");

        let index = handle_line(
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"search_index","arguments":{"query":"Rust","workspace_id":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!index["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn run_measurement_feedback_and_category_tools_are_reachable() {
        let store = Store::in_memory().unwrap();
        let fact = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remember_fact","arguments":{"text":"classify me","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let fact_text = fact["result"]["content"][0]["text"].as_str().unwrap();
        let fact_value: Value = serde_json::from_str(fact_text).unwrap();
        let fact_id = fact_value["id"].as_i64().unwrap();

        let categorized = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"categorize_pending","arguments":{"category":"review","query":"classify","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(categorized["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains(&fact_id.to_string()));
        let categories = handle_line(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_categories","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(categories["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("review"));

        let run = handle_line(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"run_begin","arguments":{"run_id":"r-1","issue_ref":"performance-decision","files":["src/store.rs"],"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!run["result"]["isError"].as_bool().unwrap());
        let link = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"link_run","arguments":{"run_id":"r-1","pr_ref":"1","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(link["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"pr_ref\":\"1\""));
        let end = handle_line(
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"run_end","arguments":{"run_id":"r-1","summary":"ok","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(end["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"state\":\"closed\""));
        let runs = handle_line(
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"query_run","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(runs["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("r-1"));

        let measurement = handle_line(
            r#"{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"record_measurement","arguments":{"measurement":"quality","sample":"s-1","value":0.8,"baseline":true,"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(measurement["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"baseline\":true"));
        let measurements = handle_line(
            r#"{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"query_measurement","arguments":{"query":"quality","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(measurements["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("quality"));

        let feedback = handle_line(
            r#"{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"record_feedback","arguments":{"feedback_id":"fb-1","item_type":"fact","item_ref":"1","signal":"helpful","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!feedback["result"]["isError"].as_bool().unwrap());
        let feedback_query = handle_line(
            r#"{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"query_feedback","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(feedback_query["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("fb-1"));
    }

    #[test]
    fn review_sessions_and_guard_tools_are_reachable() {
        let store = Store::in_memory().unwrap();
        let remember = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_fact","arguments":{"text":"review candidate","validity":"pending","session_id":"s-1","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!remember["result"]["isError"].as_bool().unwrap());
        let remember_text = remember["result"]["content"][0]["text"].as_str().unwrap();
        let fact_value: Value = serde_json::from_str(remember_text).unwrap();
        let fact_id = fact_value["id"].as_i64().unwrap();
        assert_eq!(fact_value["validity"], "pending");
        assert_eq!(fact_value["session_id"], "s-1");

        let pending = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"review_pending","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(pending["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains(&fact_id.to_string()));
        let confirm_request = format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"confirm_fact","arguments":{{"fact_id":{fact_id},"note":"checked","workspace":"w"}}}}}}"#
        );
        let confirmed = handle_line(&confirm_request, &store).unwrap();
        assert!(confirmed["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"validity\":\"valid\""));
        let history_request = format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{{"name":"fact_history","arguments":{{"id":{fact_id},"workspace":"w"}}}}}}"#
        );
        let history = handle_line(&history_request, &store).unwrap();
        assert!(history["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("validity_changed"));
        let sessions = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_sessions","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(sessions["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("s-1"));
        let facts = handle_line(
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"facts_for_session","arguments":{"session":"s-1","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(facts["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("review candidate"));

        let guard = handle_line(
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"search_guard","arguments":{"query":"not present","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(guard["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"status\":\"abstain\""));
        let summary = handle_line(
            r#"{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"summarize_index","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(summary["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"active_facts\":1"));
        let prepared = handle_line(
            r#"{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"prepare_summary","arguments":{"query":"review","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(prepared["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"recall\""));
    }

    #[test]
    fn document_freshness_and_embedding_boundary_tools_are_reachable() {
        let _provider_environment = isolated_provider_environment();
        let store = Store::in_memory().unwrap();
        let root = std::env::temp_dir().join(format!(
            "memory-mcp-rust-protocol-document-root-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("document.txt");
        std::fs::write(&path, "protocol document").unwrap();
        let ingest = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "ingest_document",
                    "arguments": {
                        "root": root.to_str().unwrap(),
                        "path": "document.txt",
                        "workspace": "w",
                        "commit": true
                    }
                }
            }),
            &store,
        )
        .unwrap();
        let ingest_text = ingest["result"]["content"][0]["text"].as_str().unwrap();
        assert!(ingest_text.contains("\"result_status\":\"ok\""));
        let ingest_value: Value = serde_json::from_str(ingest_text).unwrap();
        let reference = ingest_value["refs"][0].as_str().unwrap();
        let read = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "read_context",
                    "arguments": {"ref": reference, "workspace": "w"}
                }
            }),
            &store,
        )
        .unwrap();
        assert!(read["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("protocol document"));
        let legacy = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "ingest_document",
                    "arguments": {"path": path.to_str().unwrap(), "workspace": "w"}
                }
            }),
            &store,
        )
        .unwrap();
        assert_eq!(legacy["error"]["code"], -32602);

        handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"remember_fact","arguments":{"text":"old enough","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let sweep = handle_line(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"decay_sweep","arguments":{"max_age_seconds":0,"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(sweep["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("degraded"));
        let embeddings = handle_line(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"embed_backfill","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(embeddings["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"status\":\"disabled\""));
        let anchored = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"query_anchored","arguments":{"query":"store","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(anchored["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"decisions\""));
        let consolidated = handle_line(
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"consolidate","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(consolidated["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"status\":\"complete\""));
        let backup = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "backup_workspace",
                    "arguments": {
                        "path": "protocol-backup.json",
                        "workspace": "w"
                    }
                }
            }),
            &store,
        )
        .unwrap();
        assert!(backup["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"bytes\""));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn lifecycle_and_handoff_tools_are_reachable_through_stdio_dispatch() {
        let store = Store::in_memory().unwrap();
        let put = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"put_context","arguments":{"ref":"ctx-a","name":"A","content":"context","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!put["result"]["isError"].as_bool().unwrap());

        let event = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"capture_event","arguments":{"idempotency_key":"e-1","event_type":"captured","context_ref":"ctx-a","metadata":{"source":"test"},"payload":{"turn":1},"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!event["result"]["isError"].as_bool().unwrap());
        let events = handle_line(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_events","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(events["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("e-1"));

        let handoff = handle_line(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"handoff_begin","arguments":{"idempotency_key":"h-1","context_ref":"ctx-a","owner":"agent-a","session":"s-1","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!handoff["result"]["isError"].as_bool().unwrap());
        let accepted = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"handoff_accept","arguments":{"idempotency_key":"h-1","actor":"agent-b","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let accepted_text = accepted["result"]["content"][0]["text"].as_str().unwrap();
        assert!(accepted_text.contains("\"accepted\""));
        let listed = handle_line(
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"list_handoffs","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(listed["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("agent-b"));
    }

    #[test]
    fn capture_event_removes_secrets_exclusions_and_oversized_payloads() {
        let store = Store::in_memory().unwrap();
        store
            .put_context("event-seed", "event seed", "context", "w")
            .unwrap();
        let embedded_header = [
            "prefix ",
            "Authorization",
            ": ",
            "Bearer ",
            "fixture-header",
        ]
        .concat();
        let embedded_assignment = ["prefix ", "api", "_key=", "fixture-assignment"].concat();
        let unknown_key_value = ["fixture", "-", "key"].concat();
        let captured = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "capture_event",
                    "arguments": {
                        "idempotency_key": "event-safe",
                        "event_kind": "tool-call",
                        "workspace": "w",
                        "source": "issue-ref",
                        "cwd": "/private/path",
                        "path": "/private/path/file",
                        "payload": {
                            "keep": "value",
                            "remove": "must-not-persist",
                            "credentials": {"password": "do-not-persist"},
                            "nested": {"token": "fixture-token"},
                            "header": embedded_header,
                            "unclassified": embedded_assignment,
                            "apiKey": unknown_key_value
                        },
                        "exclude_paths": ["remove"]
                    }
                }
            }),
            &store,
        )
        .unwrap();
        let captured_text = captured["result"]["content"][0]["text"].as_str().unwrap();
        let captured_value: Value = serde_json::from_str(captured_text).unwrap();
        let context_ref = captured_value["context"]["ref"].as_str().unwrap();
        let context = store.context(context_ref, "w").unwrap().unwrap();
        assert!(context.content.contains("[REDACTED]"));
        assert!(context.content.contains("value"));
        assert!(!context.content.contains("must-not-persist"));
        assert!(!context.content.contains("fixture-token"));
        assert!(!context.content.contains("fixture-header"));
        assert!(!context.content.contains("fixture-assignment"));
        assert!(!context.content.contains("fixture-key"));
        assert!(!context.content.contains("/private/path"));
        let event = store.read_event("event-safe", "w").unwrap().unwrap();
        assert!(event.metadata.contains("[REDACTED]"));
        assert!(!event.metadata.contains("/private/path"));

        let oversized = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "capture_event",
                    "arguments": {
                        "idempotency_key": "event-large",
                        "event_kind": "tool-call",
                        "workspace": "w",
                        "payload": "x".repeat(MAX_EVENT_PAYLOAD_BYTES + 128)
                    }
                }
            }),
            &store,
        )
        .unwrap();
        let oversized_text = oversized["result"]["content"][0]["text"].as_str().unwrap();
        let oversized_value: Value = serde_json::from_str(oversized_text).unwrap();
        assert_eq!(oversized_value["event"]["payload_truncated"], true);
        let oversized_ref = oversized_value["context"]["ref"].as_str().unwrap();
        let oversized_context = store.context(oversized_ref, "w").unwrap().unwrap();
        assert!(oversized_context.byte_size < 20_000);
        assert!(oversized_context.content.contains("\"truncated\":true"));
    }

    #[test]
    fn capture_event_rejects_sensitive_identifiers_before_persistence() {
        let store = Store::in_memory().unwrap();
        let idempotency_key = ["prefix ", "Bearer ", "fixture-id"].concat();
        let response = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "capture_event",
                    "arguments": {
                        "idempotency_key": idempotency_key,
                        "event_kind": "tool-call",
                        "workspace": "w",
                        "payload": {"value": 1}
                    }
                }
            }),
            &store,
        )
        .unwrap();
        assert_eq!(response["error"]["code"], -32602);
        assert!(response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("idempotency_key contains restricted data"));
    }

    #[test]
    fn fact_metadata_filters_and_verification_are_reachable_through_stdio() {
        let store = Store::in_memory().unwrap();
        let remember = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remember_fact","arguments":{"text":"Important SQLite fact","source":"design","project":"memory","domain":"storage","trust":"high","strong":true,"importance":0.9,"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let remember_text = remember["result"]["content"][0]["text"].as_str().unwrap();
        assert!(remember_text.contains("\"trust\":\"high\""));

        let search = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_facts","arguments":{"query":"SQLite","source":"design","strong":true,"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(search["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Important SQLite fact"));

        let verification = handle_line(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"verify_facts","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(verification["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"valid\":true"));
    }

    #[test]
    fn graph_and_decision_tools_are_reachable_through_stdio_dispatch() {
        let store = Store::in_memory().unwrap();
        for request in [
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remember_entity","arguments":{"name":"Rust","type":"language","workspace":"w"}}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"remember_entity","arguments":{"name":"SQLite","type":"database","workspace":"w"}}}"#,
        ] {
            let response = handle_line(request, &store).unwrap();
            assert!(!response["result"]["isError"].as_bool().unwrap());
        }
        let relation = handle_line(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"remember_relation","arguments":{"subject":"Rust","predicate":"uses","object":"SQLite","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!relation["result"]["isError"].as_bool().unwrap());
        let graph = handle_line(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"search_graph","arguments":{"query":"rust","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(graph["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"uses\""));

        let root = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"record_decision","arguments":{"subject":"memory","scenario":"fallback","outcome":"SQLite","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!root["result"]["isError"].as_bool().unwrap());
        let query = handle_line(
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"query_decisions","arguments":{"query":"fallback","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(query["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"outcome\":\"SQLite\""));
    }

    #[test]
    fn evidence_and_export_tools_are_reachable_through_stdio_dispatch() {
        let store = Store::in_memory().unwrap();
        let fact = handle_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remember_fact","arguments":{"text":"Evidence fact","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        let fact_text = fact["result"]["content"][0]["text"].as_str().unwrap();
        let fact_value: Value = serde_json::from_str(fact_text).unwrap();
        let fact_id = fact_value["id"].as_i64().unwrap();
        let put = handle_line(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"put_context","arguments":{"ref":"ctx-a","name":"Context","content":"Evidence context","workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(!put["result"]["isError"].as_bool().unwrap());

        let attach = format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"attach_evidence","arguments":{{"fact_id":{fact_id},"source_ref":"docs/current-contract.md","path":"docs/current-contract.md","selected_text":"Evidence context","resolution_status":"resolved","workspace":"w"}}}}}}"#
        );
        let attached = handle_line(&attach, &store).unwrap();
        assert!(!attached["result"]["isError"].as_bool().unwrap());
        let provenance = handle_line(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_provenance","arguments":{"fact_id":1,"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(provenance["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("docs/current-contract.md"));
        let export = handle_line(
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"export","arguments":{"workspace":"w"}}}"#,
            &store,
        )
        .unwrap();
        assert!(export["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"evidence\""));
    }
}
