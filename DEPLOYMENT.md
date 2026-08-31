# Deploying the native memory-mcp server

`memory-mcp-rust` is a stdio MCP server. The client starts one native process
and exchanges newline-delimited JSON-RPC messages over stdin/stdout. Keep the
database directory writable by the process; the executable itself can be
mounted read-only.

## Native host rollout

Build the release binary from a verified feature branch:

```sh
git fetch --prune origin
git merge-base --is-ancestor origin/main HEAD
cargo build --release
```

Install the binary in the host's stable local bin directory. Keep the previous
launcher or binary in place until the smoke check passes so rollback remains a
configuration change:

```sh
install -m 0755 target/release/memory-mcp-rust \
  "${HOST_HOME:?HOST_HOME_required}/.local/bin/memory-mcp-rust"
```

For Codex, jcode, or another MCP-native client, change only the configured
server command and preserve the existing `MEMORY_MCP_*` environment entries:

```toml
[mcp_servers.memory-mcp]
command = "/path/to/host/.local/bin/memory-mcp-rust"
env = { MEMORY_MCP_DB = "/path/to/shared/facts.db", MEMORY_MCP_RECALL = "1" }
```

The equivalent Codex CLI operation is `codex mcp add memory-mcp --env
MEMORY_MCP_DB=/path/to/shared/facts.db --env MEMORY_MCP_RECALL=1 --
/path/to/host/.local/bin/memory-mcp-rust`. If a server with that name already
exists, remove and re-add it only after recording the current environment
settings. A new client session is needed before a host reloads its MCP tool
inventory.

Verify the actual configured command and the native contract:

```sh
codex mcp get memory-mcp
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  | MEMORY_MCP_DB=/path/to/smoke/facts.db /path/to/host/.local/bin/memory-mcp-rust
```

The smoke response must identify `memory-mcp`, expose 80 advertised tools, and
include the retrieval tools. Do not put real credentials or production data in
the smoke database. `purpose="safety_critical"` must remain fail-closed, and
retrieval results remain advisory rather than authorization.

## Data cutover and rollback

For a legacy SQLite database, use the Rust migration subcommand with a new
destination path, inspect the JSON integrity/data-match report, and only then
point the host at the new database:

```sh
memory-mcp-rust migrate --source /path/to/legacy.db --target /path/to/rust.db
```

The source is read-only and is not replaced in place. If the native smoke or
cutover checks fail, restore the prior MCP command (for example, the legacy
Python launcher) and keep the source database unchanged. Do not delete the old
launcher as part of a rollout.

## Retrieval rollout

Profiles are bounded response policies, not roles or permissions. `balanced`
is the compatibility default; `orientation`, `implementation`, `review`, and
`incident` progressively select smaller or graph-aware budgets. `review` and
`incident` require resolved evidence for candidate facts. Graph/session
expansion is bounded, and unknown purpose values or `safety_critical` requests
must not reach provider or recall work.

Feedback is aggregate and observational. Do not report efficacy until a paired
baseline/memory measurement slice reaches its configured threshold; below that
threshold the status remains `not_claimed`.

## Source of truth

The Rust descriptors and protocol tests define the live MCP contract. See
[`docs/current-contract.md`](docs/current-contract.md) for limits and safety,
[`docs/pilot-workflow.md`](docs/pilot-workflow.md) for a synthetic issue-shaped
pilot, and [`docs/performance.md`](docs/performance.md) for measurement rules.
