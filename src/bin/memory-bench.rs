use memory_mcp_rust::redis::RedisAdapter;
use memory_mcp_rust::store::Store;
use serde::Serialize;
use std::fs;
use std::time::Instant;

const DEFAULT_ITERATIONS: usize = 128;
const MAX_ITERATIONS: usize = 10_000;
const BENCHMARK_WORKSPACE: &str = "memory-mcp-benchmark";

#[derive(Debug, Serialize)]
struct BackendMetrics {
    backend: &'static str,
    iterations: usize,
    inserted: usize,
    search_hits: usize,
    setup_micros: u128,
    write_micros: u128,
    search_micros: u128,
    total_micros: u128,
    redis_commands: Option<u64>,
    redis_request_bytes: Option<u64>,
    redis_response_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    workload: &'static str,
    iterations: usize,
    sqlite: BackendMetrics,
    redis: Option<BackendMetrics>,
    selected_backend: &'static str,
    fallback_reason: Option<&'static str>,
    performance_efficacy: &'static str,
}

fn main() {
    let iterations = iterations_from_env();
    let sqlite = measure_sqlite(iterations);
    let redis_attempt = Instant::now();
    let redis = match RedisAdapter::from_env_with_namespace_suffix(&format!(
        "benchmark-{}",
        std::process::id()
    )) {
        Ok(Some(adapter)) => match measure_redis(&adapter, iterations, redis_attempt) {
            Ok(metrics) => {
                let _ = adapter.reset_workspace(BENCHMARK_WORKSPACE);
                Some(metrics)
            }
            Err(_) => {
                let _ = adapter.reset_workspace(BENCHMARK_WORKSPACE);
                None
            }
        },
        Ok(None) | Err(_) => None,
    };
    let (selected_backend, fallback_reason) = if redis.is_some() {
        ("redis", None)
    } else if RedisAdapter::configured() {
        ("sqlite", Some("redis_unavailable"))
    } else {
        ("sqlite", Some("redis_not_configured"))
    };
    let report = BenchmarkReport {
        workload: "unique remember_fact followed by repeated search_facts in one workspace",
        iterations,
        sqlite,
        redis,
        selected_backend,
        fallback_reason,
        performance_efficacy: "not_claimed_without_a_paired_workload_and_environment_review",
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("benchmark report serializes")
    );
}

fn iterations_from_env() -> usize {
    std::env::var("MEMORY_MCP_BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ITERATIONS)
        .clamp(1, MAX_ITERATIONS)
}

fn measure_sqlite(iterations: usize) -> BackendMetrics {
    let path = std::env::temp_dir().join(format!(
        "memory-mcp-rust-benchmark-{}.db",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);
    let total_start = Instant::now();
    let setup_start = Instant::now();
    let store = Store::open(&path).expect("SQLite benchmark store opens");
    let setup_micros = setup_start.elapsed().as_micros();

    let write_start = Instant::now();
    for index in 0..iterations {
        store
            .remember_fact(&format!("benchmark fact {index}"), BENCHMARK_WORKSPACE)
            .expect("SQLite benchmark write");
    }
    let write_micros = write_start.elapsed().as_micros();

    let search_start = Instant::now();
    let mut search_hits = 0;
    for _ in 0..iterations {
        search_hits += store
            .search_facts("benchmark", BENCHMARK_WORKSPACE)
            .expect("SQLite benchmark search")
            .len();
    }
    let search_micros = search_start.elapsed().as_micros();
    drop(store);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(path.with_extension("db-wal"));
    let _ = fs::remove_file(path.with_extension("db-shm"));

    BackendMetrics {
        backend: "sqlite",
        iterations,
        inserted: iterations,
        search_hits,
        setup_micros,
        write_micros,
        search_micros,
        total_micros: total_start.elapsed().as_micros(),
        redis_commands: None,
        redis_request_bytes: None,
        redis_response_bytes: None,
    }
}

fn measure_redis(
    adapter: &RedisAdapter,
    iterations: usize,
    total_start: Instant,
) -> Result<BackendMetrics, memory_mcp_rust::redis::RedisError> {
    let setup_micros = total_start.elapsed().as_micros();
    let write_start = Instant::now();
    for index in 0..iterations {
        adapter.remember_fact(&format!("benchmark fact {index}"), BENCHMARK_WORKSPACE)?;
    }
    let write_micros = write_start.elapsed().as_micros();

    let search_start = Instant::now();
    let mut search_hits = 0;
    for _ in 0..iterations {
        search_hits += adapter
            .search_facts("benchmark", BENCHMARK_WORKSPACE)?
            .len();
    }
    let search_micros = search_start.elapsed().as_micros();
    let redis_metrics = adapter.metrics();
    Ok(BackendMetrics {
        backend: "redis",
        iterations,
        inserted: iterations,
        search_hits,
        setup_micros,
        write_micros,
        search_micros,
        total_micros: total_start.elapsed().as_micros(),
        redis_commands: Some(redis_metrics.commands),
        redis_request_bytes: Some(redis_metrics.request_bytes),
        redis_response_bytes: Some(redis_metrics.response_bytes),
    })
}
