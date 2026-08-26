## Purpose

This runbook describes the controlled replacement of the Python memory-mcp
stdio server with `memory-mcp-rust`. The Python launcher remains the rollback
implementation until the complete gate is green.

The Rust server uses Redis as the preferred backend when a supported Redis
setting is configured and reachable. SQLite remains the standby and fallback
store. A Redis outage must therefore select SQLite, retain offline writes in
the durable outbox, and return to Redis only after recovery reconciliation has
completed.

## Preconditions

1. Build the Rust binary from a feature branch based on the current default
   branch. Do not build or publish from a stale branch.
2. Confirm that the old and new launchers use separate database paths for the
   migration rehearsal. Never test a migration against the live database
   without first creating a verified backup.
3. Keep Redis credentials in the runtime secret store. The migration and
   preflight commands below accept only database paths, command strings, and
   environment **names**; they do not print or persist environment values.
4. Do not restart an active agent runtime as part of this rehearsal. A real
   cutover requires the owner of the AI stack to schedule controlled recreation
   of every affected service.

## Copy-first migration

Use the Rust binary's migration subcommand or the wrapper script. The target
must be new; an existing target is rejected so the Python database remains a
rollback point.

```sh
MEMORY_MIGRATE_SOURCE=/path/to/verified-python-copy.db \
MEMORY_MIGRATE_TARGET=/path/to/new-rust.db \
MEMORY_MCP_RUST_BIN=/path/to/memory-mcp-rust \
  scripts/memory-mcp-migrate.sh
```

The command opens the source read-only, uses SQLite's online backup API, runs
Rust schema migrations only on a private temporary copy, and publishes it
atomically after these checks pass:

- source `quick_check` on every durable non-FTS table;
- target full `integrity_check`;
- every durable source table is present in the target with the same row count;
  derived FTS5 shadow tables may be rebuilt by Rust and are covered by the
  target integrity check instead;
- a per-row, type-aware SHA-256 fingerprint of the source tables matches the
  target projection;
- the destination was never overwritten.

The JSON result contains only checks, counts, and fingerprints. It does not
contain fact text, credentials, or database paths.

## Contract and launcher preflight

Run the preflight against the old Python command and the Rust command, pointing
each at its own migrated copy. Supply the environment variable names declared
by the two launchers, not their values:

```sh
python3 scripts/memory-mcp-preflight.py \
  --legacy-command 'python3 /path/to/memory_mcp.py' \
  --rust-command '/path/to/memory-mcp-rust' \
  --legacy-db /path/to/python-copy.db \
  --rust-db /path/to/rust-copy.db \
  --legacy-env MEMORY_MCP_DB \
  --legacy-env MEMORY_MCP_RECALL \
  --rust-env MEMORY_MCP_DB \
  --rust-env MEMORY_MCP_REDIS_URL
```

The gate is green only when all of the following are true:

- `initialize` has the same server identity and protocol behavior;
- `tools/list` has exactly 80 unique names, and every description and
  `inputSchema` matches;
- the copied SQLite data passes the read-only check and all source row counts
  are preserved;
- the empty-argument probe crosses all 80 advertised routes without an
  unknown-tool or not-implemented response;
- launcher environment names match, including the Redis configuration and
every legacy feature/limit variable that still affects behavior.

The Rust server also fails closed when one of the currently deployed legacy
provider/pipeline variables is present. This prevents an accidental switch
from silently disabling embeddings, extraction, recall, verification, or the
LLM-backed paths. Remove those variables only as part of a reviewed parity
change, not as a way to make the preflight appear green.

The route probe intentionally uses disposable databases. Errors caused only by
missing required arguments are valid route evidence; unknown or unimplemented
handlers are not.

## Cutover and rollback

Only after the preflight, Rust/Clippy/tests, AppSec review, QA acceptance, and
release gate are green may the AI-stack owner update the launchers and compose
entries. Update all configured host and container launchers consistently:

- point `MEMORY_MCP_CMD`/MCP configuration at the Rust binary;
- keep `MEMORY_MCP_DB` on the shared writable mount;
- pass Redis settings through the existing secret interpolation without
  copying or logging the password;
- retain the Python command and the verified source backup as rollback assets;
- recreate services in a scheduled maintenance window, then run the same
  smoke checks against the actual service boundaries.

If any check is red, do not switch the live launcher. Restore the Python
launcher only through the approved stack rollout procedure; do not delete the
verified database or discard the Rust target while investigating.

## Resource guardrails

The Redis watcher is bounded and sleeps between checks. Keep its interval and
backoff at the configured defaults unless measurements justify a change. The
reconciliation loop must process bounded batches, compact completed outbox
entries, and avoid exporting a full SQLite snapshot when only a revision check
is needed. Record resource measurements separately from the migration result.
