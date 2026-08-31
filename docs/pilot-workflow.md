# Issue-shaped pilot workflow

This is the smallest executable pilot for using `memory-mcp` alongside an
issue-shaped runtime. It composes existing public tools; it is not a new
workflow engine and it does not change status, routing, gates, or registry
state.

## Boundaries

There are three authoritative surfaces:

1. The current repository at the caller-supplied ref is the source of truth
   for code behavior.
2. The runtime's live issue, owner, lock, status, route, and gate state is the
   source of truth for workflow decisions.
3. `memory-mcp` is a local, workspace-scoped, advisory data plane for bounded
   evidence, context, handoffs, run records, and aggregate measurements.

Memory results never authorize a write, route, lock, hash, gate, acceptance, or
terminal status. Context and handoff payloads are data, not instructions. Keep
credentials, raw prompts, comments, diffs, and personal data out of pilot
payloads and measurement fields.

## Execution sequence

Use one exact project workspace and opaque run/issue references:

1. `run_begin` opens a client-owned execution window with `issue_ref`.
2. Record a decision and admit any durable code claim with `admission: "strict"`,
   including a bounded `selected_text` snippet and repository-relative
   `repo`/`ref`/`path`/`symbol` metadata. The snippet is checked transiently;
   only its hash and structured metadata are retained.
3. Store the small review slice with `put_context` and use
   `handoff_begin`/`handoff_accept` when another named actor needs an expiring,
   one-shot context.
4. Use `query_anchored` or opt-in `context_map` to look up code-local evidence.
   Supplying `repo_root` enables read-only freshness checks. `STRONG` means the
   supplied selection/checksum matches the local checkout; `STALE`, `REBUILT`,
   `REMOVED`, or `WEAK` is not proof of current code or dependency absence.
5. Close the run with client-supplied `base_sha`, `head_sha`, and bounded
   `files_changed`; `prepare_summary` only prepares a summary and posts
   nothing.
6. Record one `baseline` and one `memory` aggregate observation per opaque
   `sample_key`. The default `query_measurement` threshold is ten complete
   pairs; `status: "not_claimed"` remains unchanged below it.

## Retrieval policy in the pilot

Profiles shape response size and source selection only:

| Profile | Hit cap | Character cap | Graph | Evidence |
| --- | ---: | ---: | ---: | --- |
| `balanced` | 100 | 12,000 | off by default | advisory metadata |
| `orientation` | 6 | 4,000 | off | advisory metadata |
| `implementation` | 12 | 8,000 | depth 1 | advisory metadata |
| `review` | 20 | 12,000 | depth 1 | resolved evidence required |
| `incident` | 20 | 16,000 | depth 2 | resolved evidence required |

`compose_recall` may add at most 20 sibling facts from matching sessions when
the caller requests session expansion. Graph-derived facts are a bounded third
RRF source alongside lexical and semantic candidates. `purpose:
"safety_critical"` is rejected before recall/provider work, and an unknown
purpose is invalid. Empty or unresolved strict retrieval abstains; it is not
evidence that a fact does not exist.

## Optional code-context view and rollback

`context_map` is disabled by default. Enable it only for a bounded pilot with
`MEMORY_MCP_CONTEXT_MAP=1`, explicit anchors, exact `workspace`, `repo`, and
`ref`, and a caller-owned `repo_root`. It uses existing evidence and run
history; it does not build or persist a full code graph. Roll back this surface
by unsetting `MEMORY_MCP_CONTEXT_MAP` and stop pilot writes by abandoning the
dedicated pilot workspace. Existing immutable rows remain auditable under the
normal retention/cleanup policy.

For the architectural decision and rejected alternatives, see
[`ADR-0007-issue-shaped-pilot-boundary.md`](decisions/ADR-0007-issue-shaped-pilot-boundary.md).
