use crate::store::{ContextMetadata, Store, StoreError};
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
    if !tools::is_advertised(name) {
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
        "remember_fact" => {
            let text = arguments
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CallError::InvalidParams("remember_fact requires text".to_owned())
                })?;
            serde_json::to_value(
                store
                    .remember_fact(text, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("Fact serializes")
        }
        "search_facts" => {
            let query = arguments
                .get("query")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CallError::InvalidParams("search_facts requires query".to_owned())
                })?;
            serde_json::to_value(
                store
                    .search_facts(query, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("facts serialize")
        }
        "list_facts" => {
            serde_json::to_value(store.list_facts(workspace).map_err(CallError::Execution)?)
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
}
