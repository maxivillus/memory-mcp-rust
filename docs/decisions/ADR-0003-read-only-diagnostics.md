# ADR-0003: read-only diagnostics for native memory-mcp

## Status

Accepted for `NTSI-893`. The contract is implemented additively in the native Rust MCP
server; existing compatibility tools and their schemas are unchanged.

## Decision

Add four bounded read-only tools to `tools/list`:
`capabilities_doctor`, `search_diagnose`, `audit_coverage`, and
`measurement_status`. For clients that need human-readable names, keep the
non-advertised aliases `capabilities/doctor`, `search diagnose`,
`search/diagnose`, `audit coverage`, `audit/coverage`, `measurement status`, and
`measurement/status`.

Diagnostics run through the current `Store` and the same MCP dispatcher:

- `capabilities_doctor` reads allowlisted provider state, schema/migration/FTS5
  readiness, safe backend counters, an explicit workspace hash, and aggregate
  telemetry readiness;
- `search_diagnose` classifies a bounded lexical/semantic/hybrid read and
  returns only `query_hash`, a scope hash, counters, fallback, a safe status, and
  one `next_action`;
- `audit_coverage` reads only aggregate telemetry from
  `lifecycle_events.metadata`, filters the exact workspace/issue/run/site, and
  returns aggregate counts, bounded latency, and `telemetry_gap`;
- `measurement_status` shows baseline/memory pair coverage and always
  preserves `status=not_claimed`, `efficacy=not_claimed`, and
  `independent_check=not_run`.

`capture_event` with `event_kind=memory-access` accepts only bounded opaque
references, an allowlisted outcome, fallback, result count, latency, and an optional
SHA-256 query hash. Unknown fields, raw prompts, comments, queries, and payloads
are rejected before persistence. Diagnostics have no authority, make no provider
calls, and do not change durable state.

## Consequences

Consumers receive the reason for an empty or unavailable read without access to the raw
query or payload. A SQLite/FTS5 error becomes `unavailable`, not
`no_match`; absent, unmapped, or truncated telemetry becomes
`telemetry_gap`. The `tools/list` surface grows from 80 upstream tools to
84 advertised tools, so `docs/current-contract.md`,
`docs/diagnostic-tools.json`, the response example, and the repository skill are
updated together.

## Verification

Acceptance must confirm public MCP compatibility, the coordinator route,
the read-only boundary, bounded output, workspace isolation, secret/raw-data
redaction, the negative status matrix, and preservation of `not_claimed` without
an independent QA check. Efficacy is not claimed by this ADR.
