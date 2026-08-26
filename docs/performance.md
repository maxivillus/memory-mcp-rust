# Performance measurement

The repository includes a bounded memory-bench binary for the performance
decision in NTL-722. It runs one fixed workload:

1. open a persistent SQLite connection and complete one-time migrations;
2. insert unique facts into one workspace;
3. search the same workspace repeatedly.

Run the SQLite fallback baseline with:

    MEMORY_MCP_BENCH_ITERATIONS=128 cargo run --release --bin memory-bench

Set MEMORY_MCP_REDIS_URL (or REDIS_URL) to run the same fact workload against
the optional namespaced Redis adapter. The binary reports both backend timings
only when Redis passes its connection and PING probe. If the URL is missing or
unavailable, it reports selected_backend=sqlite and an explicit fallback
reason.

The JSON output separates migration/setup, writes, searches, and total time.
It is an observation for one environment, not a general speedup claim. A
paired decision requires the same release build, host, iteration count,
workspace/data shape, warm-up policy, Redis persistence settings, and repeated
samples with p50/p95 results. Until those controls and an agreed threshold are
recorded, performance_efficacy remains not_claimed.

The Redis slice intentionally covers only the core fact comparison surface.
The MCP server's complete parity path remains SQLite-backed; enabling Redis
does not silently change the source of truth for contexts, lifecycle records,
graph data, provenance, telemetry, or backups.
