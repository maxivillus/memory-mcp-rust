use crate::backend::BackendStatus;
use crate::providers;
use crate::store::{SchemaDiagnostics, Store};
use crate::tools;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::time::Instant;

pub(crate) const CAPABILITIES_DOCTOR: &str = "capabilities_doctor";
pub(crate) const SEARCH_DIAGNOSE: &str = "search_diagnose";
pub(crate) const AUDIT_COVERAGE: &str = "audit_coverage";
pub(crate) const MEASUREMENT_STATUS: &str = "measurement_status";

const SERVER_NAME: &str = "memory-mcp";
const SERVER_VERSION: &str = "0.23.0";
const DEFAULT_SEARCH_TIMEOUT_MS: u64 = 2_500;
const MAX_SEARCH_TIMEOUT_MS: u64 = 60_000;
const MAX_QUERY_CHARS: usize = 4_096;
const MAX_REFERENCE_CHARS: usize = 256;
const MAX_ACCESS_RESULT_COUNT: u64 = 1_000_000;
const MAX_ACCESS_LATENCY_MS: f64 = 86_400_000.0;

pub(crate) fn validate_reference(value: &str, label: &str) -> Result<(), String> {
    if value.chars().count() > MAX_REFERENCE_CHARS
        || value.chars().any(char::is_control)
        || contains_sensitive_marker(value)
    {
        return Err(format!("{label} must be a safe bounded reference"));
    }
    Ok(())
}

pub(crate) fn capabilities_doctor(
    store: &Store,
    workspace: &str,
    backend: Option<&BackendStatus>,
) -> Value {
    let schema = match store.diagnostic_schema() {
        Ok(schema) => schema_value(&schema),
        Err(_) => json!({
            "status": "unavailable",
            "version": 1,
            "migrations": {"status": "unavailable", "missing": []},
            "fts5": {"status": "unavailable"},
        }),
    };
    let schema_ready = schema["status"] == "ready";
    let telemetry = match store.query_access_telemetry(workspace, "", "", "", 1) {
        Ok(page) => json!({
            "status": "ready",
            "storage": "lifecycle_events",
            "mode": "aggregate_only",
            "coverage": if page.events.is_empty() { "empty" } else { "available" },
            "bounded": true,
            "telemetry_gap": page.events.is_empty(),
        }),
        Err(_) => json!({
            "status": "unavailable",
            "storage": "lifecycle_events",
            "mode": "aggregate_only",
            "coverage": "unavailable",
            "bounded": true,
            "telemetry_gap": true,
        }),
    };
    let telemetry_ready = telemetry["status"] == "ready";
    let overall_status = if schema_ready && telemetry_ready {
        "ok"
    } else {
        "degraded"
    };
    let next_action = if !schema_ready {
        "repair the database schema before relying on diagnostics"
    } else if !telemetry_ready {
        "inspect telemetry storage before relying on coverage"
    } else {
        "run search_diagnose for the requested workspace"
    };
    let backend = backend
        .map(|status| {
            let mut value = serde_json::to_value(status).unwrap_or_else(|_| json!({}));
            if let Some(object) = value.as_object_mut() {
                object.insert("status".to_owned(), json!("available"));
            }
            value
        })
        .unwrap_or_else(
            || json!({"status": "available", "backend": "sqlite", "source": "direct_store"}),
        );
    json!({
        "status": overall_status,
        "server": {"name": SERVER_NAME, "version": SERVER_VERSION},
        "tools": {
            "count": tools::TOOL_NAMES.len(),
            "names": tools::TOOL_NAMES.iter().map(|name| Value::String((*name).to_owned())).collect::<Vec<_>>(),
            "bounded": true,
        },
        "scope": {
            "workspace_hash": opaque_hash(workspace),
            "explicit": true,
            "shared_pool": false,
            "requires_explicit": true,
        },
        "providers": provider_states(),
        "schema": schema,
        "backend": backend,
        "telemetry": telemetry,
        "memory_policy": "advisory_only",
        "next_action": next_action,
    })
}

pub(crate) fn search_diagnose(
    store: &Store,
    query: &str,
    workspace: &str,
    arguments: &Map<String, Value>,
) -> Result<Value, String> {
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err("query exceeds the configured limit (4096 characters)".to_owned());
    }
    let query_hash = opaque_hash(query);
    let scope_hash = opaque_hash(workspace);
    let mode = optional_string(arguments, &["mode", "search_mode"])?.unwrap_or("lexical");
    let profile = optional_string(arguments, &["profile"])?.unwrap_or("balanced");
    let timeout_ms = optional_u64(arguments, "timeout_ms", DEFAULT_SEARCH_TIMEOUT_MS)?;
    if timeout_ms > MAX_SEARCH_TIMEOUT_MS {
        return Err("timeout_ms must be between 0 and 60000".to_owned());
    }
    let fallback = if matches!(mode, "semantic" | "hybrid") {
        json!({"attempted": true, "used": false, "selected": mode})
    } else {
        json!({"attempted": false, "used": false, "selected": "lexical"})
    };

    if let Some(expected_workspace) = expected_workspace(arguments)? {
        if expected_workspace != workspace {
            return Ok(search_status(
                "scope_mismatch",
                "scope_mismatch",
                &query_hash,
                &scope_hash,
                0,
                0,
                0.0,
                json!({"attempted": false, "used": false, "selected": "none"}),
                "retry with the requested workspace scope",
            ));
        }
    }
    if !matches!(mode, "lexical" | "semantic" | "hybrid") {
        return Ok(search_status(
            "unsupported",
            "unsupported_mode",
            &query_hash,
            &scope_hash,
            0,
            0,
            0.0,
            fallback,
            "use lexical, semantic, or hybrid search mode",
        ));
    }
    if !matches!(
        profile,
        "balanced" | "orientation" | "implementation" | "review" | "incident"
    ) {
        return Ok(search_status(
            "unsupported",
            "unsupported_profile",
            &query_hash,
            &scope_hash,
            0,
            0,
            0.0,
            fallback,
            "use one of the bounded retrieval profiles",
        ));
    }
    if matches!(mode, "semantic" | "hybrid") && !providers::embeddings_enabled() {
        return Ok(search_status(
            "unsupported",
            "provider_disabled",
            &query_hash,
            &scope_hash,
            0,
            0,
            0.0,
            json!({"attempted": true, "used": true, "selected": "lexical", "reason": "provider_disabled"}),
            "enable the embeddings provider before requesting semantic search",
        ));
    }
    if timeout_ms == 0 {
        return Ok(search_status(
            "timeout",
            "deadline_elapsed",
            &query_hash,
            &scope_hash,
            0,
            0,
            0.0,
            fallback,
            "retry with a positive bounded timeout",
        ));
    }
    if !has_searchable_terms(query) {
        return Ok(search_status(
            "abstained",
            "no_searchable_terms",
            &query_hash,
            &scope_hash,
            0,
            0,
            0.0,
            fallback,
            "provide a more specific searchable query",
        ));
    }

    let started = Instant::now();
    let facts = match store.search_facts_for_diagnostics(query, workspace) {
        Ok(facts) => facts,
        Err(_) => {
            return Ok(search_status(
                "unavailable",
                "sqlite_fts_read_failed",
                &query_hash,
                &scope_hash,
                0,
                0,
                elapsed_ms(started),
                fallback,
                "repair the SQLite/FTS5 read path before retrying",
            ));
        }
    };
    let elapsed = elapsed_ms(started);
    if elapsed > timeout_ms as f64 {
        return Ok(search_status(
            "timeout",
            "deadline_elapsed",
            &query_hash,
            &scope_hash,
            0,
            0,
            elapsed,
            fallback,
            "retry with a larger bounded timeout or a narrower query",
        ));
    }

    let conflicts = match store.detect_conflicts(query, workspace) {
        Ok(conflicts) => conflicts.len(),
        Err(_) => {
            return Ok(search_status(
                "unavailable",
                "conflict_read_failed",
                &query_hash,
                &scope_hash,
                0,
                0,
                elapsed,
                fallback,
                "repair the SQLite/FTS5 read path before retrying",
            ));
        }
    };
    let active = facts
        .iter()
        .filter(|fact| fact.lifecycle == "active" && fact.validity == "valid")
        .count();
    let stale = facts.len().saturating_sub(active);
    if conflicts > 0 {
        return Ok(search_status_with_conflicts(
            "conflicting",
            "conflicting_decisions",
            &query_hash,
            &scope_hash,
            facts.len(),
            active,
            stale,
            conflicts,
            elapsed,
            fallback,
            "review conflicting decisions before using the search result",
        ));
    }
    if active > 0 && matches!(profile, "review" | "incident") {
        let mut resolved = 0usize;
        for fact in facts
            .iter()
            .filter(|fact| fact.lifecycle == "active" && fact.validity == "valid")
        {
            match store.fact_evidence_summary(fact.id, workspace) {
                Ok(summary) if summary.resolved > 0 => resolved += 1,
                Ok(_) => {}
                Err(_) => {
                    return Ok(search_status(
                        "unavailable",
                        "evidence_read_failed",
                        &query_hash,
                        &scope_hash,
                        0,
                        0,
                        elapsed,
                        fallback,
                        "repair the evidence read path before retrying",
                    ));
                }
            }
        }
        if resolved == 0 {
            return Ok(search_status(
                "abstained",
                "resolved_evidence_required",
                &query_hash,
                &scope_hash,
                facts.len(),
                active,
                elapsed,
                fallback,
                "attach or review a resolved evidence anchor before using this profile",
            ));
        }
    }
    if active > 0 {
        return Ok(search_status(
            "matched",
            "match",
            &query_hash,
            &scope_hash,
            facts.len(),
            active,
            elapsed,
            fallback,
            "review the bounded advisory result in its current workspace",
        ));
    }
    if !facts.is_empty() {
        return Ok(search_status(
            "stale",
            "only_stale_matches",
            &query_hash,
            &scope_hash,
            facts.len(),
            0,
            elapsed,
            fallback,
            "review or refresh the stale memory entries before relying on them",
        ));
    }
    Ok(search_status(
        "no_match",
        "no_matching_facts",
        &query_hash,
        &scope_hash,
        0,
        0,
        elapsed,
        fallback,
        "broaden the query or consult reviewed evidence",
    ))
}

pub(crate) fn audit_coverage(
    store: &Store,
    workspace: &str,
    issue_ref: &str,
    run_id: &str,
    site: &str,
    limit: usize,
) -> Value {
    let page = match store.query_access_telemetry(workspace, issue_ref, run_id, site, limit) {
        Ok(page) => page,
        Err(_) => {
            return json!({
                "status": "unavailable",
                "reason": "telemetry_read_failed",
                "bounded": true,
                "telemetry_gap": true,
                "scope": coverage_scope(workspace, issue_ref, run_id, site),
                "next_action": "inspect telemetry storage before relying on coverage",
                "memory_policy": "advisory_only",
            });
        }
    };
    let attempted = page.events.len() as i64;
    let succeeded = page
        .events
        .iter()
        .filter(|event| event.outcome == "succeeded")
        .count() as i64;
    let fallback = page
        .events
        .iter()
        .filter(|event| event.fallback || event.outcome == "fallback")
        .count() as i64;
    let failed = page
        .events
        .iter()
        .filter(|event| matches!(event.outcome.as_str(), "failed" | "unavailable" | "timeout"))
        .count() as i64;
    let unmapped = page
        .events
        .iter()
        .filter(|event| event.issue_ref.is_empty() && event.run_id.is_empty())
        .count() as i64;
    let mapped_issue = page
        .events
        .iter()
        .filter(|event| !event.issue_ref.is_empty())
        .count() as i64;
    let mapped_run = page
        .events
        .iter()
        .filter(|event| !event.run_id.is_empty())
        .count() as i64;
    let latencies = page
        .events
        .iter()
        .map(|event| event.latency_ms)
        .collect::<Vec<_>>();
    let telemetry_gap = page.events.is_empty() || page.truncated || unmapped > 0;
    let next_action = if page.events.is_empty() {
        "capture sanitized memory-access telemetry for the requested scope"
    } else if page.truncated {
        "narrow the coverage filter or increase the bounded limit"
    } else if unmapped > 0 {
        "add issue_ref or run_id to each memory-access event"
    } else {
        "continue collecting independent coverage samples"
    };
    json!({
        "status": if telemetry_gap { "telemetry_gap" } else { "ok" },
        "reason": if telemetry_gap { "telemetry_gap" } else { "covered" },
        "bounded": true,
        "scope": coverage_scope(workspace, issue_ref, run_id, site),
        "coverage": {
            "attempted": attempted,
            "succeeded": succeeded,
            "fallback": fallback,
            "failed": failed,
            "latency_ms": latency_summary(&latencies),
        },
        "mapping": {"issue": mapped_issue, "run": mapped_run, "unmapped": unmapped},
        "returned": page.events.len(),
        "truncated": page.truncated,
        "telemetry_gap": telemetry_gap,
        "source": "lifecycle_events.metadata",
        "memory_policy": "advisory_only",
        "next_action": next_action,
    })
}

pub(crate) fn measurement_status(
    store: &Store,
    workspace: &str,
    measurement_id: &str,
    min_pairs: usize,
) -> Value {
    let rows = match store.query_measurements(measurement_id, workspace) {
        Ok(rows) => rows
            .into_iter()
            .filter(|row| row.measurement == measurement_id || row.sample == measurement_id)
            .collect::<Vec<_>>(),
        Err(_) => {
            return json!({
                "status": "unavailable",
                "reason": "measurement_read_failed",
                "bounded": true,
                "measurement_id": measurement_id,
                "scope": {"workspace_hash": opaque_hash(workspace)},
                "memory_policy": "advisory_only",
                "next_action": "repair the measurement read path before evaluating coverage",
            });
        }
    };
    let baseline = rows
        .iter()
        .filter(|row| row.variant == "baseline")
        .map(|row| row.sample.as_str())
        .collect::<BTreeSet<_>>();
    let memory = rows
        .iter()
        .filter(|row| row.variant == "memory")
        .map(|row| row.sample.as_str())
        .collect::<BTreeSet<_>>();
    let paired = baseline.intersection(&memory).count();
    let ready = paired >= min_pairs;
    json!({
        "status": "not_claimed",
        "reason": "not_claimed",
        "bounded": true,
        "measurement_id": measurement_id,
        "scope": {"workspace_hash": opaque_hash(workspace)},
        "min_pairs": min_pairs,
        "observations": {"baseline": baseline.len(), "memory": memory.len(), "paired_samples": paired},
        "missing_pairs": {
            "baseline": min_pairs.saturating_sub(baseline.len()),
            "memory": min_pairs.saturating_sub(memory.len()),
        },
        "readiness": if ready { "ready_for_review" } else { "insufficient_pairs" },
        "independent_check": "not_run",
        "efficacy": "not_claimed",
        "memory_policy": "advisory_only",
        "next_action": if ready {
            "run an independent QA check before evaluating efficacy"
        } else {
            "collect the missing baseline and memory pairs"
        },
    })
}

/// Build the only accepted payload for a memory-access lifecycle event.
/// Unknown keys are rejected so a telemetry row cannot become raw prompt or
/// comment storage.
pub(crate) fn access_telemetry(arguments: &Map<String, Value>) -> Result<Value, String> {
    const FIELDS: [&str; 8] = [
        "issue_ref",
        "run_id",
        "site",
        "outcome",
        "fallback",
        "result_count",
        "latency_ms",
        "query_hash",
    ];
    const COMMON_FIELDS: [&str; 11] = [
        "idempotency_key",
        "event_kind",
        "event_id",
        "session_id",
        "source",
        "cwd",
        "path",
        "tool_name",
        "workspace",
        "workspace_id",
        "capture",
    ];
    for key in arguments.keys() {
        if !COMMON_FIELDS.contains(&key.as_str())
            && !FIELDS.contains(&key.as_str())
            && !matches!(key.as_str(), "telemetry" | "payload" | "content")
        {
            return Err(format!(
                "memory-access telemetry field is not allowed: {key}"
            ));
        }
    }
    let mut fields = Map::new();
    if let Some(value) = arguments.get("telemetry") {
        merge_telemetry_object(&mut fields, value, "telemetry", &FIELDS)?;
    }
    for key in ["payload", "content"] {
        if let Some(value) = arguments.get(key) {
            let object = value.as_object().ok_or_else(|| {
                "memory-access telemetry payload must be an aggregate object".to_owned()
            })?;
            if let Some(telemetry) = object.get("telemetry") {
                if object.keys().any(|field| field != "telemetry") {
                    return Err(
                        "memory-access telemetry wrapper contains a non-aggregate field".to_owned(),
                    );
                }
                merge_telemetry_object(&mut fields, telemetry, key, &FIELDS)?;
            } else {
                merge_telemetry_object(&mut fields, value, key, &FIELDS)?;
            }
        }
    }
    for key in FIELDS {
        if let Some(value) = arguments.get(key) {
            merge_field(&mut fields, key, value)?;
        }
    }

    let issue_ref = bounded_opaque_string(&fields, "issue_ref")?;
    let run_id = bounded_opaque_string(&fields, "run_id")?;
    let site = bounded_opaque_string(&fields, "site")?;
    let outcome = fields
        .get("outcome")
        .map(string_value)
        .transpose()?
        .unwrap_or_else(|| "succeeded".to_owned());
    if !matches!(
        outcome.as_str(),
        "succeeded"
            | "fallback"
            | "failed"
            | "abstained"
            | "timeout"
            | "unsupported"
            | "unavailable"
    ) {
        return Err("memory-access telemetry outcome is invalid".to_owned());
    }
    let fallback = fields
        .get("fallback")
        .map(bool_value)
        .transpose()?
        .unwrap_or(false);
    let result_count = fields
        .get("result_count")
        .map(u64_value)
        .transpose()?
        .unwrap_or(0);
    if result_count > MAX_ACCESS_RESULT_COUNT {
        return Err("memory-access telemetry result_count is too large".to_owned());
    }
    let latency_ms = fields
        .get("latency_ms")
        .map(f64_value)
        .transpose()?
        .unwrap_or(0.0);
    if !latency_ms.is_finite() || !(0.0..=MAX_ACCESS_LATENCY_MS).contains(&latency_ms) {
        return Err("memory-access telemetry latency_ms is out of range".to_owned());
    }
    let query_hash = fields
        .get("query_hash")
        .map(string_value)
        .transpose()?
        .unwrap_or_default();
    if !query_hash.is_empty()
        && (query_hash.len() != 64 || !query_hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err("memory-access telemetry query_hash must be a SHA-256 hex string".to_owned());
    }
    Ok(json!({
        "version": 1,
        "issue_ref": issue_ref,
        "run_id": run_id,
        "site": site,
        "outcome": outcome,
        "fallback": fallback,
        "result_count": result_count,
        "latency_ms": round3(latency_ms),
        "query_hash": query_hash,
    }))
}

fn schema_value(schema: &SchemaDiagnostics) -> Value {
    let fts_ready = schema.facts_fts_present
        && schema.facts_fts_readable
        && schema.decisions_fts_present
        && schema.decisions_fts_readable;
    let ready = schema.missing_tables.is_empty() && fts_ready;
    json!({
        "status": if ready { "ready" } else { "degraded" },
        "version": schema.schema_version,
        "sqlite_user_version": schema.sqlite_user_version,
        "tables": {"required": schema.required_tables, "present": schema.present_tables, "missing": schema.missing_tables},
        "migrations": {"status": if ready { "ready" } else { "degraded" }, "missing": schema.missing_tables},
        "fts5": {
            "status": if fts_ready { "ready" } else { "degraded" },
            "facts": {"present": schema.facts_fts_present, "readable": schema.facts_fts_readable},
            "decisions": {"present": schema.decisions_fts_present, "readable": schema.decisions_fts_readable},
        },
    })
}

fn provider_states() -> Value {
    json!({
        "embeddings": provider_state(providers::embeddings_enabled(), Some(providers::embedding_provider())),
        "extraction": provider_state(providers::extraction_enabled(), Some(providers::llm_provider())),
        "recall": provider_state(providers::recall_enabled(), None),
        "verification": provider_state(providers::verification_enabled(), Some(providers::llm_provider())),
        "categorization": provider_state(providers::categorization_enabled(), Some(providers::llm_provider())),
        "probe": "not_run",
    })
}

fn provider_state(enabled: bool, provider: Option<String>) -> Value {
    let mut value = Map::new();
    value.insert("enabled".to_owned(), json!(enabled));
    value.insert(
        "status".to_owned(),
        json!(if enabled { "enabled" } else { "disabled" }),
    );
    if let Some(provider) = provider {
        value.insert("provider".to_owned(), Value::String(provider));
    }
    Value::Object(value)
}

#[allow(clippy::too_many_arguments)]
fn search_status(
    status: &str,
    reason: &str,
    query_hash: &str,
    scope_hash: &str,
    total: usize,
    active: usize,
    latency_ms: f64,
    fallback: Value,
    next_action: &str,
) -> Value {
    search_status_with_counts(
        status,
        reason,
        query_hash,
        scope_hash,
        total,
        active,
        total.saturating_sub(active),
        0,
        latency_ms,
        fallback,
        next_action,
    )
}

#[allow(clippy::too_many_arguments)]
fn search_status_with_conflicts(
    status: &str,
    reason: &str,
    query_hash: &str,
    scope_hash: &str,
    total: usize,
    active: usize,
    stale: usize,
    conflicts: usize,
    latency_ms: f64,
    fallback: Value,
    next_action: &str,
) -> Value {
    search_status_with_counts(
        status,
        reason,
        query_hash,
        scope_hash,
        total,
        active,
        stale,
        conflicts,
        latency_ms,
        fallback,
        next_action,
    )
}

#[allow(clippy::too_many_arguments)]
fn search_status_with_counts(
    status: &str,
    reason: &str,
    query_hash: &str,
    scope_hash: &str,
    total: usize,
    active: usize,
    stale: usize,
    conflicts: usize,
    latency_ms: f64,
    fallback: Value,
    next_action: &str,
) -> Value {
    json!({
        "status": status,
        "reason_code": reason,
        "bounded": true,
        "query_hash": query_hash,
        "scope": {"workspace_hash": scope_hash, "explicit": true},
        "matches": {"total": total, "active": active, "stale": stale, "conflicts": conflicts},
        "latency_ms": round3(latency_ms),
        "fallback": fallback,
        "memory_policy": "advisory_only",
        "next_action": next_action,
    })
}

fn coverage_scope(workspace: &str, issue_ref: &str, run_id: &str, site: &str) -> Value {
    json!({
        "workspace_hash": opaque_hash(workspace),
        "issue_ref": if issue_ref.is_empty() { Value::Null } else { json!(issue_ref) },
        "run_id": if run_id.is_empty() { Value::Null } else { json!(run_id) },
        "site": if site.is_empty() { Value::Null } else { json!(site) },
    })
}

fn latency_summary(values: &[f64]) -> Value {
    if values.is_empty() {
        return json!({"count": 0, "min": null, "median": null, "p95": null, "max": null});
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = if sorted.len().is_multiple_of(2) {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    };
    let p95_index = ((sorted.len() * 95).saturating_add(99) / 100)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    json!({
        "count": sorted.len(),
        "min": round3(sorted[0]),
        "median": round3(median),
        "p95": round3(sorted[p95_index]),
        "max": round3(sorted[sorted.len() - 1]),
    })
}

fn merge_telemetry_object(
    fields: &mut Map<String, Value>,
    value: &Value,
    source: &str,
    allowed: &[&str],
) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("memory-access {source} must be an aggregate object"))?;
    for (key, value) in object {
        if !allowed.contains(&key.as_str()) {
            return Err(format!(
                "memory-access telemetry field is not allowed: {key}"
            ));
        }
        merge_field(fields, key, value)?;
    }
    Ok(())
}

fn merge_field(fields: &mut Map<String, Value>, key: &str, value: &Value) -> Result<(), String> {
    if let Some(existing) = fields.get(key) {
        if existing != value {
            return Err(format!("memory-access telemetry field conflicts: {key}"));
        }
    } else {
        fields.insert(key.to_owned(), value.clone());
    }
    Ok(())
}

fn bounded_opaque_string(fields: &Map<String, Value>, key: &str) -> Result<String, String> {
    let value = fields
        .get(key)
        .map(string_value)
        .transpose()?
        .unwrap_or_default();
    if value.chars().count() > MAX_REFERENCE_CHARS
        || value.chars().any(char::is_control)
        || contains_sensitive_marker(&value)
    {
        return Err(format!(
            "memory-access telemetry {key} is not a safe opaque reference"
        ));
    }
    Ok(value)
}

fn string_value(value: &Value) -> Result<String, String> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| "memory-access telemetry field must be a string".to_owned())
}

fn bool_value(value: &Value) -> Result<bool, String> {
    value
        .as_bool()
        .ok_or_else(|| "memory-access telemetry fallback must be a boolean".to_owned())
}

fn u64_value(value: &Value) -> Result<u64, String> {
    value
        .as_u64()
        .ok_or_else(|| "memory-access telemetry result_count must be an integer".to_owned())
}

fn f64_value(value: &Value) -> Result<f64, String> {
    value
        .as_f64()
        .ok_or_else(|| "memory-access telemetry latency_ms must be a number".to_owned())
}

fn optional_string<'a>(
    arguments: &'a Map<String, Value>,
    keys: &[&str],
) -> Result<Option<&'a str>, String> {
    let Some((key, value)) = keys
        .iter()
        .find_map(|key| arguments.get(*key).map(|value| (*key, value)))
    else {
        return Ok(None);
    };
    value
        .as_str()
        .map(Some)
        .ok_or_else(|| format!("tool argument {key} must be a string"))
}

fn optional_u64(arguments: &Map<String, Value>, key: &str, default: u64) -> Result<u64, String> {
    match arguments.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| format!("tool argument {key} must be a non-negative integer")),
    }
}

fn expected_workspace(arguments: &Map<String, Value>) -> Result<Option<String>, String> {
    for key in ["expected_workspace", "requested_workspace"] {
        if let Some(value) = arguments.get(key) {
            return value
                .as_str()
                .map(|value| Some(value.to_owned()))
                .ok_or_else(|| format!("tool argument {key} must be a string"));
        }
    }
    if let Some(value) = arguments.get("scope") {
        if let Some(scope) = value.as_str() {
            return Ok(Some(scope.to_owned()));
        }
        if let Some(scope) = value.as_object() {
            return scope
                .get("workspace")
                .or_else(|| scope.get("workspace_id"))
                .map(string_value)
                .transpose();
        }
        return Err("tool argument scope must be a string or object".to_owned());
    }
    Ok(None)
}

fn has_searchable_terms(query: &str) -> bool {
    query
        .split_whitespace()
        .any(|term| term.chars().any(char::is_alphanumeric))
}

fn elapsed_ms(started: Instant) -> f64 {
    round3(started.elapsed().as_secs_f64() * 1_000.0)
}

fn round3(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

fn opaque_hash(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn contains_sensitive_marker(value: &str) -> bool {
    let lower = value.trim().to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let windows_path = bytes.len() >= 3 && bytes[1] == b':' && matches!(bytes[2], b'/' | b'\\');
    lower.contains("://")
        || lower.starts_with('/')
        || lower.starts_with("\\\\")
        || lower.starts_with("../")
        || lower.starts_with("..\\")
        || lower.starts_with("./")
        || lower.starts_with(".\\")
        || lower.starts_with("~/")
        || windows_path
        || lower.starts_with("bearer ")
        || lower.starts_with("basic ")
        || lower.contains("token=")
        || lower.contains("secret=")
        || lower.contains("password=")
        || lower.contains("api_key=")
        || lower.contains("private_key=")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn telemetry_input_rejects_raw_payload_fields() {
        let mut arguments = Map::new();
        arguments.insert(
            "payload".to_owned(),
            json!({"prompt": "do not persist this"}),
        );
        assert!(access_telemetry(&arguments).is_err());

        arguments.insert(
            "payload".to_owned(),
            json!({
                "telemetry": {"outcome": "succeeded"},
                "comment": "do not persist this"
            }),
        );
        assert!(access_telemetry(&arguments).is_err());

        arguments.remove("payload");
        arguments.insert("comments".to_owned(), json!("do not persist this"));
        assert!(access_telemetry(&arguments).is_err());
    }

    #[test]
    fn measurement_status_never_claims_efficacy() {
        let store = Store::in_memory().unwrap();
        let value = measurement_status(&store, "w", "missing", 10);
        assert_eq!(value["status"], "not_claimed");
        assert_eq!(value["efficacy"], "not_claimed");
        assert_eq!(value["independent_check"], "not_run");
    }
}
