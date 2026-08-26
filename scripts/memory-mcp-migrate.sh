#!/bin/sh
set -eu

: "${MEMORY_MCP_RUST_BIN:=memory-mcp-rust}"
: "${MEMORY_MIGRATE_SOURCE:?set MEMORY_MIGRATE_SOURCE to the verified legacy SQLite copy}"
: "${MEMORY_MIGRATE_TARGET:?set MEMORY_MIGRATE_TARGET to a new Rust SQLite path}"

exec "$MEMORY_MCP_RUST_BIN" migrate \
  --source "$MEMORY_MIGRATE_SOURCE" \
  --target "$MEMORY_MIGRATE_TARGET"
