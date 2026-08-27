//! Compatibility implementation of the optional Python provider/pipeline
//! modules.  The public MCP response shapes intentionally follow the pinned
//! Python implementation; the durable state remains owned by `Store`.

use crate::providers;
use crate::store::{EvidenceSpec, Fact, FactMetadata, Store, StoreError, MAX_FACT_TEXT_CHARS};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const DEFAULT_PROFILE: &str = "balanced";

pub fn maybe_enrich_fact(
    store: &Store,
    fact: &Fact,
    arguments: &Map<String, Value>,
) -> Result<Fact, StoreError> {
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let category = arguments
        .get("category")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            arguments
                .get("domain")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
        })
        .or_else(|| rule_category(&fact.text).map(str::to_owned));
    let fact = if let Some(category) = category {
        store
            .set_fact_category(fact.id, &category, &fact.workspace)
            .or_else(|_| store.set_fact_category(fact.id, &category, workspace))?
            .unwrap_or_else(|| fact.clone())
    } else {
        fact.clone()
    };
    if providers::embeddings_enabled() {
        if let Ok(vectors) = providers::embed(std::slice::from_ref(&fact.text)) {
            if let Some(vector) = vectors.first() {
                let _ = store.upsert_fact_embedding(
                    fact.id,
                    vector,
                    &providers::embedding_model(),
                    &fact.workspace,
                );
            }
        }
    }
    Ok(fact)
}

pub fn ingest_turn(store: &Store, arguments: &Map<String, Value>) -> Result<Value, StoreError> {
    if !providers::extraction_enabled() {
        return Ok(disabled("MEMORY_MCP_EXTRACT"));
    }
    let transcript = required_string(arguments, "transcript")?.trim().to_owned();
    if transcript.is_empty() {
        return Ok(json!({"error": "transcript is required"}));
    }
    let minimum = std::env::var("MEMORY_MCP_EXTRACT_MIN_CHARS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map(|value| value.max(100))
        .unwrap_or(800);
    if transcript.chars().count() < minimum {
        return Ok(json!({
            "error": format!("transcript too short ({} chars, min {minimum})", transcript.chars().count()),
            "ingested": 0,
            "stored": 0,
            "deduped": 0,
            "failed": 0,
        }));
    }
    let mut parsed = None;
    let prompt = "You extract durable facts from a conversation transcript. Return ONLY JSON matching {\"facts\":[{\"text\":\"...\",\"type\":\"user|feedback|project|reference\",\"trust\":\"high|medium|low\",\"strong\":false,\"scope\":\"project|global\",\"importance\":0.5}]}";
    for _ in 0..3 {
        match providers::chat_json(&[
            ("system", prompt),
            ("user", &format!("Transcript:\n{transcript}")),
        ]) {
            Ok(value) if value.get("facts").and_then(Value::as_array).is_some() => {
                parsed = Some(value);
                break;
            }
            Ok(_) | Err(_) => {}
        }
    }
    let Some(parsed) = parsed else {
        eprintln!("memory-mcp ingest_turn failed after bounded provider attempts");
        return Ok(json!({
            "error": "extraction failed after 3 attempts (provider error; see server stderr)",
            "ingested": 0,
            "stored": 0,
            "deduped": 0,
            "failed": 0,
        }));
    };
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let session = string_arg(arguments, "session_ref").unwrap_or("");
    let project = string_arg(arguments, "project").unwrap_or("");
    let domain = string_arg(arguments, "domain").unwrap_or("");
    let mut stored = 0_i64;
    let mut deduped = 0_i64;
    let mut failed = 0_i64;
    let mut new_facts = Vec::new();
    let facts = parsed
        .get("facts")
        .and_then(Value::as_array)
        .expect("shape checked above");
    for value in facts {
        let Some(object) = value.as_object() else {
            failed += 1;
            continue;
        };
        let text = object
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if text.is_empty() || text.chars().count() > MAX_FACT_TEXT_CHARS {
            failed += 1;
            continue;
        }
        let scope = object
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or("project");
        let fact_workspace = if scope == "global" { "" } else { workspace };
        let trust = match object.get("trust").and_then(Value::as_str) {
            Some("low") => "low",
            _ => "medium", // model confidence is advisory, never authority
        };
        let importance = object
            .get("importance")
            .and_then(Value::as_f64)
            .unwrap_or(0.7)
            .clamp(0.0, 1.0);
        let metadata = FactMetadata {
            source: session.to_owned(),
            project: project.to_owned(),
            domain: if domain.is_empty() {
                object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("project")
                    .to_owned()
            } else {
                domain.to_owned()
            },
            trust: trust.to_owned(),
            strong: false,
            importance,
        };
        let was_present = store.fact_exists(text, fact_workspace)?;
        match store.remember_fact_with_metadata(text, fact_workspace, &metadata) {
            Ok(mut fact) => {
                let args = pipeline_arguments(fact_workspace, &metadata, text);
                fact = maybe_enrich_fact(store, &fact, &args)?;
                if session.is_empty() {
                    // No evidence is attachable without a source reference.
                } else {
                    let evidence = EvidenceSpec {
                        fact_id: fact.id,
                        source_ref: session.to_owned(),
                        source: session.to_owned(),
                        checksum: String::new(),
                        fetched_at: None,
                        repository_ref: String::new(),
                        path: String::new(),
                        symbol: String::new(),
                        line_start: None,
                        line_end: None,
                        column_start: None,
                        column_end: None,
                        selected_text: String::new(),
                        resolution_status: "unresolved".to_owned(),
                        workspace: fact_workspace.to_owned(),
                    };
                    let _ = store.attach_evidence(&evidence);
                    let _ = store.set_fact_session(fact.id, session, fact_workspace);
                }
                if was_present {
                    deduped += 1;
                } else {
                    stored += 1;
                    new_facts.push((fact.id, text.to_owned(), fact_workspace.to_owned()));
                }
            }
            Err(_) => failed += 1,
        }
    }
    let mut result = json!({
        "ingested": facts.len(),
        "stored": stored,
        "deduped": deduped,
        "failed": failed,
    });
    if providers::verification_enabled() && !new_facts.is_empty() {
        result["verification"] = verify_new_facts(store, &new_facts)?;
    }
    Ok(result)
}

pub fn absorb(store: &Store, arguments: &Map<String, Value>) -> Result<Value, StoreError> {
    let Some(raw_facts) = arguments.get("facts").or_else(|| arguments.get("text")) else {
        return Ok(json!({"error": "facts must be a non-empty array"}));
    };
    let values = if let Some(values) = raw_facts.as_array() {
        values.clone()
    } else {
        vec![raw_facts.clone()]
    };
    if values.is_empty() {
        return Ok(json!({"error": "facts must be a non-empty array"}));
    }
    if values.len() > 100 {
        return Ok(json!({"error": "facts may contain at most 100 items"}));
    }
    let dry_run = arguments
        .get("dry_run")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let commit = arguments
        .get("commit")
        .and_then(Value::as_bool)
        .unwrap_or(!dry_run);
    if dry_run && commit {
        return Ok(json!({"error": "dry_run and commit cannot both be true"}));
    }
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let defaults = |key: &str| arguments.get(key).cloned();
    let mut items = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let (text, object) = if let Some(text) = value.as_str() {
            (text.to_owned(), Map::new())
        } else if let Some(object) = value.as_object() {
            (
                object
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                object.clone(),
            )
        } else {
            return Ok(json!({"error": format!("facts[{index}] must be a string or object")}));
        };
        let text = text.trim().to_owned();
        if text.is_empty() {
            return Ok(json!({"error": format!("facts[{index}].text is required")}));
        }
        if text.chars().count() > MAX_FACT_TEXT_CHARS {
            return Ok(
                json!({"error": format!("facts[{index}].text is too long (max {MAX_FACT_TEXT_CHARS} characters)")}),
            );
        }
        let mut merged = arguments.clone();
        for key in [
            "source",
            "project",
            "domain",
            "category",
            "trust",
            "strong",
            "importance",
        ] {
            if let Some(value) = defaults(key) {
                merged.insert(key.into(), value);
            }
            if let Some(value) = object.get(key) {
                merged.insert(key.into(), value.clone());
            }
        }
        merged.insert("text".into(), json!(text));
        items.push((text, merged));
    }
    let mut planned = Vec::new();
    for (index, (text, args)) in items.iter().enumerate() {
        let duplicate = store.fact_exists(text, workspace)?;
        let candidates = if duplicate {
            Vec::new()
        } else {
            candidates(store, text, workspace)?
        };
        let classification = if duplicate {
            "duplicate"
        } else if candidates.is_empty() {
            "new"
        } else {
            "related"
        };
        let action = if duplicate {
            "noop"
        } else if candidates.is_empty() {
            "create"
        } else {
            "review"
        };
        let mut item = json!({
            "index": index,
            "sha256": sha256(text),
            "text_preview": text.chars().take(300).collect::<String>(),
            "classification": classification,
            "action": action,
            "candidate_ids": candidates.iter().map(|fact| fact.id).collect::<Vec<_>>(),
            "evidence_count": 0,
        });
        if duplicate {
            if let Some(existing_id) = store.fact_id_for_text(text, workspace)? {
                item["existing_id"] = json!(existing_id);
            }
        }
        planned.push((item, args.clone(), classification.to_owned()));
    }
    let mut result = json!({
        "dry_run": !commit,
        "committed": false,
        "count": planned.len(),
        "created": 0,
        "deduped": planned.iter().filter(|(_, _, class)| class == "duplicate").count(),
        "pending_review": planned.iter().filter(|(_, _, class)| class == "related").count(),
        "rejected": 0,
        "admission": "advisory",
        "evidence_attached": 0,
        "items": planned.iter().map(|(item, _, _)| item.clone()).collect::<Vec<_>>(),
    });
    if !commit {
        result["result_status"] = json!("preview");
        return Ok(result);
    }
    for (index, (_, args, classification)) in planned.iter().enumerate() {
        if classification != "new" {
            continue;
        }
        let metadata = metadata_from_args(args);
        let fact = store.remember_fact_with_metadata(
            args.get("text").and_then(Value::as_str).unwrap_or(""),
            workspace,
            &metadata,
        )?;
        let fact = maybe_enrich_fact(store, &fact, args)?;
        if let Some(item) = result["items"]
            .as_array_mut()
            .and_then(|items| items.get_mut(index))
        {
            item["id"] = json!(fact.id);
        }
        result["created"] = json!(result["created"].as_i64().unwrap_or(0) + 1);
    }
    result["committed"] = json!(true);
    result["result_status"] = json!("committed");
    Ok(result)
}

pub fn verify_facts(store: &Store, arguments: &Map<String, Value>) -> Result<Value, StoreError> {
    if !providers::verification_enabled() {
        return Ok(disabled("MEMORY_MCP_VERIFY"));
    }
    let text = required_string(arguments, "text")?.trim().to_owned();
    if text.is_empty() {
        return Ok(json!({"error": "text is required"}));
    }
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let candidates = candidates(store, &text, workspace)?;
    let stored = candidate_prompt(&candidates);
    let user = format!("Stored facts:\n{stored}\n\nNew fact: {text}");
    match providers::chat_json(&[("system", "You verify a new memory fact against existing stored facts. Return ONLY JSON with action, target_id, reason, and confidence."), ("user", &user)]) {
        Ok(verdict) => Ok(json!({
            "text": text,
            "checked_against": candidates.len(),
            "verdict": verdict,
            "applied": false,
        })),
        Err(_) => Ok(json!({
            "error": "verification failed (provider error; see server stderr)",
            "checked_against": candidates.len(),
        })),
    }
}

pub fn consolidate(store: &Store, arguments: &Map<String, Value>) -> Result<Value, StoreError> {
    if !providers::verification_enabled() {
        return Ok(disabled("MEMORY_MCP_VERIFY"));
    }
    let Some(values) = arguments.get("ids").and_then(Value::as_array) else {
        return Ok(json!({"error": "ids: at least 2 distinct fact ids are required"}));
    };
    let mut ids = values.iter().filter_map(Value::as_i64).collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() < 2 {
        return Ok(json!({"error": "ids: at least 2 distinct fact ids are required"}));
    }
    if ids.len() > 20 {
        return Ok(json!({"error": "ids: at most 20 facts can be consolidated at once"}));
    }
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let mut facts = Vec::new();
    for id in &ids {
        let Some(fact) = store.fact_by_id_for_pipeline(*id, workspace)? else {
            return Ok(json!({"error": "some ids are not active facts", "found": facts.len()}));
        };
        if fact.lifecycle == "forgotten" || fact.validity == "invalid" {
            return Ok(json!({"error": "some ids are not active facts", "found": facts.len()}));
        }
        if fact.strong {
            return Ok(
                json!({"error": "strong/confirmed facts cannot be consolidated", "protected_ids": [fact.id]}),
            );
        }
        facts.push(fact);
    }
    let prompt = facts
        .iter()
        .map(|fact| {
            format!(
                "- id={}: {}",
                fact.id,
                fact.text.chars().take(500).collect::<String>()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let verdict = match providers::chat_json(&[("system", "You consolidate paraphrased memory facts into a single fact. Return ONLY JSON with merge, text, importance, and reason. You consolidate facts without inventing details."), ("user", &prompt)]) {
        Ok(value) => value,
        Err(_) => return Ok(json!({"error": "consolidation failed (provider error; see server stderr)"})),
    };
    if !verdict
        .get("merge")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(
            json!({"merged": false, "ids": ids, "reason": verdict.get("reason").and_then(Value::as_str).unwrap_or("")}),
        );
    }
    let text = verdict
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if text.is_empty() {
        return Ok(json!({"error": "consolidation returned an empty text"}));
    }
    let importance = verdict
        .get("importance")
        .and_then(Value::as_f64)
        .unwrap_or_else(|| facts.iter().map(|fact| fact.importance).fold(0.5, f64::max));
    let metadata = FactMetadata {
        source: "consolidate".into(),
        project: facts
            .first()
            .map(|fact| fact.project.clone())
            .unwrap_or_default(),
        domain: facts
            .first()
            .map(|fact| fact.domain.clone())
            .unwrap_or_default(),
        trust: "medium".into(),
        strong: false,
        importance: importance.clamp(0.0, 1.0),
    };
    let mut merged = store.remember_fact_with_metadata(text, workspace, &metadata)?;
    let args = pipeline_arguments(workspace, &metadata, text);
    merged = maybe_enrich_fact(store, &merged, &args)?;
    let mut invalidated = Vec::new();
    for fact in facts {
        if store.invalidate_fact(fact.id, merged.id, workspace, "consolidated")? {
            let evidence = EvidenceSpec {
                fact_id: merged.id,
                source_ref: format!("consolidated:{}", fact.id),
                source: "consolidate".into(),
                checksum: String::new(),
                fetched_at: None,
                repository_ref: String::new(),
                path: String::new(),
                symbol: String::new(),
                line_start: None,
                line_end: None,
                column_start: None,
                column_end: None,
                selected_text: String::new(),
                resolution_status: "unresolved".into(),
                workspace: workspace.into(),
            };
            let _ = store.attach_evidence(&evidence);
            invalidated.push(fact.id);
        }
    }
    Ok(json!({
        "merged": true,
        "new_id": merged.id,
        "source_ids": invalidated,
        "reason": verdict.get("reason").and_then(Value::as_str).unwrap_or(""),
        "text": text,
    }))
}

pub fn semantic_search(store: &Store, arguments: &Map<String, Value>) -> Result<Value, StoreError> {
    if !providers::embeddings_enabled() {
        return Ok(disabled("MEMORY_MCP_EMBEDDINGS"));
    }
    let query = required_string(arguments, "query")?.trim().to_owned();
    if query.is_empty() {
        return Ok(json!({"error": "query is required"}));
    }
    let limit = bounded_usize(arguments, "limit", 20, 1, 100)?;
    let threshold = arguments
        .get("threshold")
        .and_then(Value::as_f64)
        .unwrap_or(0.0) as f32;
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let query_vector = providers::embed(std::slice::from_ref(&query))
        .map_err(|error| StoreError::Invalid(error.to_string()))?
        .into_iter()
        .next()
        .ok_or_else(|| StoreError::Invalid("embedding provider returned no vector".into()))?;
    let trust_min = string_arg(arguments, "trust_min");
    let facts = store
        .fact_embeddings(workspace)?
        .into_iter()
        .filter_map(|row| {
            let metadata = store
                .fact_search_metadata(row.fact.id, workspace)
                .ok()
                .flatten()?;
            if metadata.archived
                || row.fact.lifecycle == "forgotten"
                || (row.fact.validity == "invalid"
                    && !valid_at_allows(&metadata.invalid_at, arguments.get("valid_at")))
            {
                return None;
            }
            if let Some(project) = string_arg(arguments, "project") {
                if row.fact.project != project {
                    return None;
                }
            }
            if let Some(domain) = string_arg(arguments, "domain") {
                if row.fact.domain != domain {
                    return None;
                }
            }
            if let Some(category) = string_arg(arguments, "category") {
                if metadata.category.as_deref() != Some(category) {
                    return None;
                }
            }
            if arguments
                .get("strong_only")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && !row.fact.strong
            {
                return None;
            }
            if !trust_matches(row.fact.trust.as_str(), trust_min) {
                return None;
            }
            let score = providers::cosine(&query_vector, &row.vector);
            (score >= threshold).then_some((score, row.fact, metadata))
        })
        .collect::<Vec<_>>();
    let mut facts = facts;
    facts.sort_by(|left, right| {
        right
            .0
            .total_cmp(&left.0)
            .then_with(|| left.1.id.cmp(&right.1.id))
    });
    let facts = facts
        .into_iter()
        .take(limit)
        .map(|(score, fact, metadata)| fact_value(&fact, Some(score as f64), Some(metadata)))
        .collect::<Vec<_>>();
    Ok(json!({
        "count": facts.len(),
        "model": providers::embedding_model(),
        "facts": facts,
        "memory_policy": "advisory_only",
        "safety_critical_allowed": false,
        "profile": profile(arguments)?,
        "result_status": if facts.is_empty() { "empty" } else { "ok" },
        "retrieval_outcome": if facts.is_empty() { "abstained" } else { "matched" },
    }))
}

pub fn backfill(store: &Store, arguments: &Map<String, Value>) -> Result<Value, StoreError> {
    if !providers::embeddings_enabled() {
        return Ok(disabled("MEMORY_MCP_EMBEDDINGS"));
    }
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let missing = store.missing_fact_texts(workspace, 500)?;
    let mut processed = 0_i64;
    let mut failed = 0_i64;
    for (id, text) in missing {
        match providers::embed(&[text]) {
            Ok(vectors) => {
                if let Some(vector) = vectors.first() {
                    store.upsert_fact_embedding(
                        id,
                        vector,
                        &providers::embedding_model(),
                        workspace,
                    )?;
                    processed += 1;
                } else {
                    failed += 1;
                }
            }
            Err(_) => failed += 1,
        }
    }
    let remaining = store.missing_fact_texts(workspace, 501)?.len();
    Ok(json!({"processed": processed, "failed": failed, "remaining": remaining}))
}

pub fn hybrid_search(
    store: &Store,
    query: &str,
    arguments: &Map<String, Value>,
    lexical: &[Fact],
) -> Result<Value, StoreError> {
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let limit = bounded_usize(arguments, "limit", 20, 1, 100)?;
    let semantic = if providers::embeddings_enabled() {
        semantic_search(store, &json_args(arguments, query, limit))?
    } else {
        Value::Null
    };
    let semantic_rows = semantic
        .get("facts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let semantic_index = semantic_rows
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            value
                .get("id")
                .and_then(Value::as_i64)
                .map(|id| (id, index))
        })
        .collect::<BTreeMap<_, _>>();
    let mut facts = Vec::new();
    for (index, fact) in lexical.iter().take(limit).enumerate() {
        let score = semantic_index
            .get(&fact.id)
            .map(|semantic_index| {
                1.0 / (61.0 + index as f64) + 1.0 / (61.0 + *semantic_index as f64)
            })
            .unwrap_or(1.0 / (61.0 + index as f64));
        let metadata = store.fact_search_metadata(fact.id, workspace)?;
        let mut value = fact_value(fact, None, metadata);
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "semantic_score".into(),
                json!((score * 10_000.0).round() / 10_000.0),
            );
        }
        facts.push(value);
    }
    if facts.is_empty() {
        facts = semantic_rows.into_iter().take(limit).collect();
    }
    Ok(
        json!({"count": facts.len(), "model": providers::embedding_model(), "facts": facts,
              "memory_policy": "advisory_only", "safety_critical_allowed": false,
              "profile": profile(arguments)?, "result_status": if facts.is_empty() { "empty" } else { "ok" }}),
    )
}

pub fn compose_recall(store: &Store, arguments: &Map<String, Value>) -> Result<Value, StoreError> {
    if !providers::recall_enabled() {
        return Ok(disabled("MEMORY_MCP_RECALL"));
    }
    let turn_text = required_string(arguments, "turn_text")?.trim().to_owned();
    if turn_text.is_empty() {
        return Ok(json!({"error": "turn_text is required"}));
    }
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let limit = bounded_usize(arguments, "limit", 8, 1, 20)?;
    // Store search intentionally treats a multi-term query as an AND query.
    // Recall's direct-turn contract is broader: each meaningful turn term is
    // an OR candidate, then duplicate facts are collapsed deterministically.
    let mut lexical = Vec::new();
    for term in lexical_terms(&turn_text) {
        for fact in store.search_facts(&term, workspace)? {
            if !lexical.iter().any(|existing: &Fact| existing.id == fact.id) {
                lexical.push(fact);
            }
        }
    }
    let mut hits = lexical.into_iter().take(limit).collect::<Vec<_>>();
    if arguments
        .get("semantic")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && providers::embeddings_enabled()
    {
        let semantic = semantic_search(store, &json_args(arguments, &turn_text, limit))?;
        hits = semantic
            .get("facts")
            .and_then(Value::as_array)
            .map(|rows| rows.iter().filter_map(value_to_fact).take(limit).collect())
            .unwrap_or(hits);
    }
    let chars = bounded_usize(arguments, "chars", 0, 0, 1_000_000)?;
    let mut block = String::from("<memory-recall>\n");
    for fact in &hits {
        let line = format!(
            "- id={} trust={} type={}\n  fact: {}\n",
            fact.id, fact.trust, fact.domain, fact.text
        );
        if chars > 0 && block.len() + line.len() + 14 > chars {
            break;
        }
        block.push_str(&line);
    }
    block.push_str("</memory-recall>");
    Ok(json!({
        "count": hits.len(),
        "authoritative": hits.iter().filter(|fact| fact.strong).count(),
        "background": hits.iter().filter(|fact| !fact.strong).count(),
        "graph": 0,
        "session_expanded": 0,
        "chars": block.len(),
        "block": block,
        "query_mode": "direct_turn",
        "memory_policy": "advisory_only",
        "safety_critical_allowed": false,
        "profile": profile(arguments)?,
        "retrieval_outcome": if hits.is_empty() { "abstained" } else { "matched" },
    }))
}

pub fn auto_orient(
    store: &Store,
    arguments: &Map<String, Value>,
    already_oriented: bool,
) -> Result<Value, StoreError> {
    if already_oriented {
        return Ok(
            json!({"oriented": false, "skipped": "already_oriented", "count": 0,
                         "block": "", "session_id": string_arg(arguments, "session_id").unwrap_or(""),
                         "memory_policy": "advisory_only", "safety_critical_allowed": false}),
        );
    }
    let turn_text = required_string(arguments, "turn_text")?;
    if !providers::recall_enabled() {
        return Ok(
            json!({"oriented": true, "degraded": true, "reason": "disabled",
                         "count": 0, "block": "", "session_id": string_arg(arguments, "session_id").unwrap_or(""),
                         "memory_policy": "advisory_only", "safety_critical_allowed": false}),
        );
    }
    let mut recall_args = arguments.clone();
    recall_args.insert("turn_text".into(), Value::String(turn_text.to_owned()));
    let result = compose_recall(store, &recall_args)?;
    if result.get("error").is_some() {
        return Ok(
            json!({"oriented": true, "degraded": true, "reason": "unavailable",
                         "count": 0, "block": "", "session_id": string_arg(arguments, "session_id").unwrap_or(""),
                         "memory_policy": "advisory_only", "safety_critical_allowed": false}),
        );
    }
    Ok(json!({"oriented": true, "degraded": false,
              "count": result.get("count").cloned().unwrap_or(json!(0)),
              "authoritative": result.get("authoritative").cloned().unwrap_or(json!(0)),
              "background": result.get("background").cloned().unwrap_or(json!(0)),
              "chars": result.get("chars").cloned().unwrap_or(json!(0)),
              "block": result.get("block").cloned().unwrap_or(json!("")),
              "query_mode": result.get("query_mode").cloned().unwrap_or(json!("")),
              "session_id": string_arg(arguments, "session_id").unwrap_or(""),
              "memory_policy": "advisory_only", "safety_critical_allowed": false}))
}

pub fn search_guard(
    arguments: &Map<String, Value>,
    consecutive_searches: usize,
) -> Result<Value, StoreError> {
    let session_id = required_string(arguments, "session_id")?.trim();
    let action = required_string(arguments, "action")?;
    if !matches!(action, "search" | "memory" | "reset") {
        return Ok(json!({"error": "action must be search, memory, or reset"}));
    }
    let threshold = bounded_usize(arguments, "threshold", 3, 1, 20)?;
    let count = if action == "search" {
        consecutive_searches + 1
    } else {
        0
    };
    let warn = action == "search" && count >= threshold;
    let mut value = json!({"session_id": session_id, "action": action,
                           "consecutive_searches": count, "threshold": threshold,
                           "warn": warn, "blocking": false, "memory_policy": "advisory_only"});
    if warn {
        value["message"] = json!(format!("Memory has not been consulted after {count} consecutive searches; consider a bounded memory lookup."));
    }
    Ok(value)
}

pub fn categorize_pending(
    store: &Store,
    arguments: &Map<String, Value>,
) -> Result<Value, StoreError> {
    if !providers::categorization_enabled() {
        return Ok(disabled("MEMORY_MCP_CATEGORIZE"));
    }
    let workspace = string_arg(arguments, "workspace").unwrap_or("");
    let limit = bounded_usize(arguments, "limit", 20, 1, 100)?;
    let existing = store
        .list_categories(workspace)?
        .into_iter()
        .map(|category| category.name)
        .collect::<Vec<_>>();
    let pending = store
        .list_facts(workspace)?
        .into_iter()
        .filter(|fact| fact.category_id.is_none())
        .take(limit)
        .collect::<Vec<_>>();
    let mut categorized = 0_i64;
    let mut errors = 0_i64;
    for fact in pending {
        match providers::category_for(&fact.text, &existing) {
            Ok(category)
                if store
                    .set_fact_category(fact.id, &category, workspace)?
                    .is_some() =>
            {
                categorized += 1;
            }
            _ => errors += 1,
        }
    }
    Ok(json!({"count": categorized + errors, "categorized": categorized, "errors": errors}))
}

fn verify_new_facts(
    store: &Store,
    new_facts: &[(i64, String, String)],
) -> Result<Value, StoreError> {
    let mut summary = json!({"checked": 0, "superseded": [], "conflicts": [], "applied": 0, "skipped_low_conf": 0});
    let threshold = std::env::var("MEMORY_MCP_VERIFY_MIN_CONFIDENCE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.8)
        .clamp(0.0, 1.0);
    for (new_id, text, workspace) in new_facts {
        let candidates = candidates(store, text, workspace)?
            .into_iter()
            .filter(|fact| fact.id != *new_id)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            continue;
        }
        let stored = candidate_prompt(&candidates);
        let prompt = format!("Stored facts:\n{stored}\n\nNew fact: {text}");
        let Ok(verdict) = providers::chat_json(&[("system", "You verify a new memory fact against existing stored facts. Return ONLY JSON with action, target_id, reason, and confidence."), ("user", &prompt)]) else {
            continue;
        };
        summary["checked"] = json!(summary["checked"].as_i64().unwrap_or(0) + 1);
        let action = verdict
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("add");
        if action == "noop" {
            if verdict.get("reason").and_then(Value::as_str) == Some("conflict") {
                if let Some(conflicts) = summary["conflicts"].as_array_mut() {
                    conflicts.push(json!({"new_id": new_id, "reason": "conflict"}));
                }
            }
            continue;
        }
        if action == "add" {
            continue;
        }
        let confidence = verdict
            .get("confidence")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        if confidence < threshold {
            summary["skipped_low_conf"] =
                json!(summary["skipped_low_conf"].as_i64().unwrap_or(0) + 1);
            continue;
        }
        let Some(target_id) = verdict.get("target_id").and_then(Value::as_i64) else {
            continue;
        };
        let Some(target) = candidates.iter().find(|fact| fact.id == target_id) else {
            continue;
        };
        if target.strong {
            if let Some(conflicts) = summary["conflicts"].as_array_mut() {
                conflicts.push(json!({"old_id": target_id, "new_id": new_id, "reason": "strong fact, not invalidated"}));
            }
            continue;
        }
        if store.invalidate_fact(
            target_id,
            *new_id,
            workspace,
            verdict
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("verified"),
        )? {
            summary["applied"] = json!(summary["applied"].as_i64().unwrap_or(0) + 1);
            let bucket = if action == "supersedes" || action == "update" {
                "superseded"
            } else {
                "deleted"
            };
            if let Some(rows) = summary[bucket].as_array_mut() {
                rows.push(json!({"old_id": target_id, "new_id": new_id, "reason": verdict.get("reason").cloned().unwrap_or(Value::Null)}));
            }
            let evidence = EvidenceSpec {
                fact_id: *new_id,
                source_ref: format!("{action}:{target_id}"),
                source: "verify".into(),
                checksum: String::new(),
                fetched_at: None,
                repository_ref: String::new(),
                path: String::new(),
                symbol: String::new(),
                line_start: None,
                line_end: None,
                column_start: None,
                column_end: None,
                selected_text: String::new(),
                resolution_status: "unresolved".into(),
                workspace: workspace.clone(),
            };
            let _ = store.attach_evidence(&evidence);
        }
    }
    Ok(summary)
}

fn candidates(store: &Store, text: &str, workspace: &str) -> Result<Vec<Fact>, StoreError> {
    let terms = lexical_terms(text);
    if terms.is_empty() {
        return store
            .list_facts(workspace)
            .map(|facts| facts.into_iter().take(8).collect());
    }
    let query = terms.join(" ");
    let mut facts = store.search_facts(&query, workspace)?;
    facts.truncate(8);
    Ok(facts)
}

fn candidate_prompt(facts: &[Fact]) -> String {
    facts
        .iter()
        .map(|fact| {
            format!(
                "- id={}: {}",
                fact.id,
                fact.text.chars().take(200).collect::<String>()
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn fact_value(
    fact: &Fact,
    score: Option<f64>,
    metadata: Option<crate::store::FactSearchMetadata>,
) -> Value {
    let mut value = serde_json::to_value(fact).expect("Fact serializes");
    if let Some(object) = value.as_object_mut() {
        if let Some(score) = score {
            object.insert("score".into(), json!((score * 10_000.0).round() / 10_000.0));
        }
        if let Some(metadata) = metadata {
            object.insert(
                "category".into(),
                metadata.category.map(Value::String).unwrap_or(Value::Null),
            );
            object.insert("confirmed".into(), json!(metadata.confirmed));
            object.insert("invalid_at".into(), json!(metadata.invalid_at));
            object.insert("archived".into(), json!(metadata.archived));
            object.insert("created_at".into(), json!(metadata.created_at));
            object.insert("updated_at".into(), json!(metadata.updated_at));
        }
    }
    value
}

fn value_to_fact(value: &Value) -> Option<Fact> {
    serde_json::from_value(value.clone()).ok()
}

fn pipeline_arguments(workspace: &str, metadata: &FactMetadata, text: &str) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("workspace".into(), json!(workspace));
    arguments.insert("text".into(), json!(text));
    arguments.insert("domain".into(), json!(metadata.domain));
    arguments
}

fn metadata_from_args(arguments: &Map<String, Value>) -> FactMetadata {
    FactMetadata {
        source: string_arg(arguments, "source").unwrap_or("").to_owned(),
        project: string_arg(arguments, "project").unwrap_or("").to_owned(),
        domain: string_arg(arguments, "domain").unwrap_or("").to_owned(),
        trust: string_arg(arguments, "trust")
            .unwrap_or("medium")
            .to_owned(),
        strong: arguments
            .get("strong")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        importance: arguments
            .get("importance")
            .and_then(Value::as_f64)
            .unwrap_or(0.5)
            .clamp(0.0, 1.0),
    }
}

fn sha256(text: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(text.as_bytes());
    hex::encode(digest.finalize())
}

fn json_args(arguments: &Map<String, Value>, query: &str, limit: usize) -> Map<String, Value> {
    let mut copy = arguments.clone();
    copy.insert("query".into(), json!(query));
    copy.insert("limit".into(), json!(limit));
    copy
}

fn disabled(flag: &str) -> Value {
    json!({"error": format!("disabled (set {flag}=1)")})
}

fn required_string<'a>(
    arguments: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, StoreError> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| StoreError::Invalid(format!("tool argument {key} must be a string")))
}

fn string_arg<'a>(arguments: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    arguments.get(key).and_then(Value::as_str)
}

fn bounded_usize(
    arguments: &Map<String, Value>,
    key: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, StoreError> {
    let value = arguments
        .get(key)
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| StoreError::Invalid(format!("{key} must be a non-negative integer")))
                .and_then(|value| {
                    usize::try_from(value)
                        .map_err(|_| StoreError::Invalid(format!("{key} is too large")))
                })
        })
        .transpose()?
        .unwrap_or(default);
    if value < minimum || value > maximum {
        return Err(StoreError::Invalid(format!(
            "{key} must be between {minimum} and {maximum}"
        )));
    }
    Ok(value)
}

fn profile(arguments: &Map<String, Value>) -> Result<String, StoreError> {
    let value = string_arg(arguments, "profile").unwrap_or(DEFAULT_PROFILE);
    if matches!(
        value,
        "balanced" | "orientation" | "implementation" | "review" | "incident"
    ) {
        Ok(value.to_owned())
    } else {
        Err(StoreError::Invalid(
            "profile must be one of balanced, orientation, implementation, review, incident".into(),
        ))
    }
}

fn lexical_terms(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|term| term.chars().count() > 1)
        .map(|term| term.to_lowercase())
        .collect()
}

fn rule_category(text: &str) -> Option<&'static str> {
    let text = text.to_lowercase();
    [
        ("memory-mcp", "memory-mcp"),
        ("memory_mcp", "memory-mcp"),
        ("sqlite", "database"),
        ("docker", "docker"),
        ("compose", "docker"),
        ("skill", "skills"),
        ("скил", "skills"),
        ("git", "git"),
        ("rust", "runtimes"),
        ("ollama", "llm"),
        ("embed", "llm"),
        ("test", "testing"),
    ]
    .into_iter()
    .find_map(|(needle, category)| text.contains(needle).then_some(category))
}

fn trust_matches(trust: &str, minimum: Option<&str>) -> bool {
    let rank = |value| match value {
        "high" => 0,
        "medium" => 1,
        _ => 2,
    };
    minimum
        .map(|value| rank(trust) <= rank(value))
        .unwrap_or(true)
}

fn valid_at_allows(invalid_at: &str, value: Option<&Value>) -> bool {
    let Some(value) = value.and_then(Value::as_str) else {
        return false;
    };
    invalid_at.is_empty() || invalid_at >= value
}
