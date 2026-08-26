use crate::store::{Store, StoreError};
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
        "put_context" => {
            let reference = required_string(arguments, "ref")
                .or_else(|_| required_string(arguments, "reference"))?;
            let name = required_string(arguments, "name")?;
            let content = required_string(arguments, "content")?;
            serde_json::to_value(
                store
                    .put_context(reference, name, content, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("Context serializes")
        }
        "read_context" => {
            let reference = required_string(arguments, "ref")
                .or_else(|_| required_string(arguments, "reference"))?;
            serde_json::to_value(
                store
                    .context(reference, workspace)
                    .map_err(CallError::Execution)?,
            )
            .expect("context serializes")
        }
        "list_context" => serde_json::to_value(
            store
                .list_contexts(workspace)
                .map_err(CallError::Execution)?,
        )
        .expect("contexts serialize"),
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
}
