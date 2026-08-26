use crate::store::{
    ContextMetadata, DecisionSpec, EntitySpec, EventSpec, EvidenceSpec, FactFilters, FactMetadata,
    FeedbackSpec, HandoffSpec, MeasurementSpec, RelationSpec, RunSpec, Store, StoreError,
};
use crate::tools;
use serde_json::{json, Map, Value};

const DEFAULT_PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "memory-mcp";
const SERVER_VERSION: &str = "0.23.0";

pub fn handle_line(line: &str, store: &Store) -> Option<Value> {
    match serde_json::from_str::<Value>(line) {
        Ok(request) => handle_request(request, store),
        Err(_) => Some(error_response(Value::Null, -32700, "Parse error")),
    }
}

pub fn handle_request(request: Value, store: &Store) -> Option<Value> {
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
        "tools/call" => match call_tool(object.get("params"), store) {
            Ok(result) => result_response(id.clone().unwrap_or(Value::Null), result),
            Err(CallError::InvalidParams(message)) => {
                error_response(id.clone().unwrap_or(Value::Null), -32602, &message)
            }
            Err(CallError::Execution(error)) => {
                eprintln!("tool execution failed: {error}");
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
            serde_json::to_value(fact).expect("Fact serializes")
        }
        "absorb" => {
            let texts = string_array_or_single(arguments, &["texts", "facts"], &["text"])?;
            serde_json::to_value(
                store
                    .absorb(&texts, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("absorbed facts serialize")
        }
        "ingest_turn" => {
            let text = required_string(arguments, "text")
                .or_else(|_| required_string(arguments, "turn"))?;
            serde_json::to_value(
                store
                    .ingest_turn(text, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("ingested fact serializes")
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
            let query = required_string(arguments, "query")?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .search_guard(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("guarded search serializes")
        }
        "auto_orient" => {
            let query = required_string(arguments, "query")?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .auto_orient(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("orientation serializes")
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
        "embed_backfill" => serde_json::to_value(
            store
                .embed_backfill(workspace)
                .map_err(CallError::Execution)?,
        )
        .expect("embedding backfill serializes"),
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
        "search_facts" => {
            let query = arguments
                .get("query")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CallError::InvalidParams("search_facts requires query".to_owned())
                })?;
            let filters = fact_filters(arguments)?;
            serde_json::to_value(
                store
                    .search_facts_with_filters(query, workspace, &filters)
                    .map_err(CallError::Execution)?,
            )
            .expect("facts serialize")
        }
        "search_semantic" => {
            let query = required_string(arguments, "query")?;
            serde_json::to_value(
                store
                    .search_semantic(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("semantic fallback serializes")
        }
        "list_facts" => {
            let filters = fact_filters(arguments)?;
            serde_json::to_value(
                store
                    .list_facts_with_filters(workspace, &filters)
                    .map_err(CallError::Execution)?,
            )
            .expect("facts serialize")
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
        "verify_facts" => serde_json::to_value(
            store
                .verify_facts(workspace)
                .map_err(CallError::Execution)?,
        )
        .expect("fact verification serializes"),
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
        "ingest_document" => {
            let path = required_string(arguments, "path")?;
            let reference =
                optional_string(arguments, "ref")?.or(optional_string(arguments, "reference")?);
            let name = optional_string(arguments, "name")?;
            let max_bytes = optional_usize(arguments, &["max_bytes", "limit"], 1_048_576)?;
            let workspace = required_context_workspace(arguments)?;
            serde_json::to_value(
                store
                    .ingest_document(path, reference, name, max_bytes, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("document context serializes")
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
            let metadata = arguments
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let payload = arguments.get("payload").cloned().unwrap_or(Value::Null);
            let workspace = required_context_workspace(arguments)?;
            let spec = EventSpec {
                idempotency_key: idempotency_key.to_owned(),
                event_type: event_type.to_owned(),
                context_reference: context_reference.to_owned(),
                metadata: serde_json::to_string(&metadata).expect("event metadata serializes"),
                payload: serde_json::to_string(&payload).expect("event payload serializes"),
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

fn required_string<'a>(arguments: &'a Map<String, Value>, key: &str) -> Result<&'a str, CallError> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| CallError::InvalidParams(format!("tool argument {key} must be a string")))
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
    use crate::store::Store;

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
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"run_begin","arguments":{"run_id":"r-1","issue_ref":"NTL-722","files":["src/store.rs"],"workspace":"w"}}}"#,
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
        let store = Store::in_memory().unwrap();
        let path = std::env::temp_dir().join(format!(
            "memory-mcp-rust-protocol-document-{}.txt",
            std::process::id()
        ));
        std::fs::write(&path, "protocol document").unwrap();
        let ingest = handle_request(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "ingest_document",
                    "arguments": {
                        "path": path.to_str().unwrap(),
                        "workspace": "w"
                    }
                }
            }),
            &store,
        )
        .unwrap();
        assert!(ingest["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("protocol document"));

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
        let _ = std::fs::remove_file(path);
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
