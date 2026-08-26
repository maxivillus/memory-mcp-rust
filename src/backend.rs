use crate::protocol::replay_tool;
use crate::redis::{RedisAdapter, RedisError, RedisMetrics};
use crate::store::{Store, StoreError};
use crate::tools;
use hex::encode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_secs(5);
const MIN_WATCH_INTERVAL: Duration = Duration::from_millis(250);
const MAX_WATCH_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(60);
const MAX_OUTBOX_BYTES: u64 = 8 * 1024 * 1024;
const MAX_RECONCILIATION_AUDIT_BYTES: u64 = 1024 * 1024;
const MAX_REPLAY_BATCH: usize = 64;
static OUTBOX_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Redis,
    Sqlite,
}

/// Safe coordinator health information for diagnostics and acceptance tests.
/// It contains counters and revisions only; payloads and credentials never
/// cross this boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BackendStatus {
    pub backend: BackendKind,
    pub redis_configured: bool,
    pub redis_connected: bool,
    pub redis_revision: Option<u64>,
    pub standby_revision: u64,
    pub standby_lag: Option<u64>,
    pub outbox_depth: usize,
    pub redis_commands: u64,
    pub redis_request_bytes: u64,
    pub redis_response_bytes: u64,
    pub sync_ticks: u64,
    pub sync_errors: u64,
    pub last_sync_micros: u64,
}

#[derive(Debug)]
pub enum BackendToolError {
    InvalidParams(String),
    Execution(StoreError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OutboxEntry {
    idempotency_key: String,
    name: String,
    arguments: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum OutboxEvent {
    Pending(OutboxEntry),
    Completed { idempotency_key: String },
}

struct Outbox {
    path: PathBuf,
    audit_path: PathBuf,
}

impl Outbox {
    fn new(database_path: &Path) -> Self {
        Self {
            path: database_path.with_extension("outbox.jsonl"),
            audit_path: database_path.with_extension("reconciliation.jsonl"),
        }
    }

    fn pending(&self) -> Result<Vec<OutboxEntry>, StoreError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event = serde_json::from_str::<OutboxEvent>(&line).map_err(|error| {
                StoreError::Invalid(format!("outbox record is invalid: {error}"))
            })?;
            match event {
                OutboxEvent::Pending(entry) => {
                    if let Some(existing) =
                        entries.iter_mut().find(|existing: &&mut OutboxEntry| {
                            existing.idempotency_key == entry.idempotency_key
                        })
                    {
                        *existing = entry;
                    } else {
                        entries.push(entry);
                    }
                }
                OutboxEvent::Completed { idempotency_key } => {
                    entries.retain(|entry| entry.idempotency_key != idempotency_key);
                }
            }
        }
        Ok(entries)
    }

    fn append(&self, event: &OutboxEvent) -> Result<(), StoreError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let encoded = serde_json::to_vec(event).map_err(|error| {
            StoreError::Invalid(format!("outbox serialization failed: {error}"))
        })?;
        let current_size = fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let encoded_size = u64::try_from(encoded.len() + 1).unwrap_or(u64::MAX);
        if current_size.saturating_add(encoded_size) > MAX_OUTBOX_BYTES {
            return Err(StoreError::Invalid(
                "durable outbox exceeds the configured size limit".to_owned(),
            ));
        }
        let mut file = open_private_append(&self.path)?;
        file.write_all(&encoded)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    fn append_pending(&self, entry: OutboxEntry) -> Result<(), StoreError> {
        if self
            .pending()?
            .iter()
            .any(|pending| pending.idempotency_key == entry.idempotency_key)
        {
            return Ok(());
        }
        self.append(&OutboxEvent::Pending(entry))
    }

    fn complete(&self, idempotency_key: &str) -> Result<(), StoreError> {
        self.append(&OutboxEvent::Completed {
            idempotency_key: idempotency_key.to_owned(),
        })
    }

    fn reject(&self, idempotency_key: &str, reason: &str) -> Result<(), StoreError> {
        let event = json!({
            "idempotency_key": idempotency_key,
            "reason": reason,
        });
        append_json_line(
            &self.audit_path,
            &event,
            MAX_RECONCILIATION_AUDIT_BYTES,
            "reconciliation audit",
        )?;
        self.complete(idempotency_key)
    }

    fn compact(&self, entries: &[OutboxEntry]) -> Result<(), StoreError> {
        if entries.is_empty() {
            if self.path.exists() {
                let file = open_private_rewrite(&self.path)?;
                file.sync_all()?;
            }
            return Ok(());
        }
        let temporary = temporary_outbox_path(&self.path);
        let result = (|| {
            let mut file = open_private_new(&temporary)?;
            for entry in entries {
                let encoded =
                    serde_json::to_vec(&OutboxEvent::Pending(entry.clone())).map_err(|error| {
                        StoreError::Invalid(format!("outbox serialization failed: {error}"))
                    })?;
                file.write_all(&encoded)?;
                file.write_all(b"\n")?;
            }
            file.sync_all()?;
            fs::rename(&temporary, &self.path)?;
            Ok::<(), StoreError>(())
        })();
        let _ = fs::remove_file(&temporary);
        result
    }
}

struct CoordinatorInner {
    store: Store,
    redis: Option<RedisAdapter>,
    mode: BackendKind,
    revision: u64,
    last_remote_revision: Option<u64>,
    outbox: Outbox,
    redis_configured: bool,
    redis_totals: RedisMetrics,
    sync_ticks: u64,
    sync_errors: u64,
    last_sync_micros: u64,
}

impl CoordinatorInner {
    fn new(path: &Path, redis_configured: bool) -> Result<Self, StoreError> {
        Ok(Self {
            store: Store::open(path)?,
            redis: None,
            mode: BackendKind::Sqlite,
            revision: 0,
            last_remote_revision: None,
            outbox: Outbox::new(path),
            redis_configured,
            redis_totals: RedisMetrics::default(),
            sync_ticks: 0,
            sync_errors: 0,
            last_sync_micros: 0,
        })
    }

    fn attach_redis(&mut self, adapter: RedisAdapter) -> Result<(), StoreError> {
        let remote = read_consistent_state(&adapter)?;
        match remote {
            Some((revision, snapshot)) => {
                self.store.restore_snapshot_bytes(&snapshot)?;
                self.revision = revision;
                self.last_remote_revision = Some(revision);
            }
            None => {
                let snapshot = self.store.snapshot_bytes()?;
                self.revision = adapter
                    .publish_state(0, &snapshot)
                    .map_err(redis_store_error)?;
                self.last_remote_revision = Some(self.revision);
            }
        }
        self.redis = Some(adapter);
        self.mode = BackendKind::Redis;
        self.reconcile_outbox()
    }

    fn prepare_for_operation(&mut self) {
        if self.mode != BackendKind::Redis {
            return;
        }
        let result = (|| {
            let adapter = self.redis.as_ref().ok_or_else(|| {
                StoreError::Invalid("Redis connection is not available".to_owned())
            })?;
            let remote_revision = adapter.state_revision().map_err(redis_store_error)?;
            self.last_remote_revision = Some(remote_revision);
            if remote_revision != self.revision {
                self.sync_from_redis_current()?;
            }
            if !self.outbox.pending()?.is_empty() {
                self.reconcile_outbox()?;
            }
            Ok::<(), StoreError>(())
        })();
        if result.is_err() {
            self.enter_sqlite_fallback();
        }
    }

    fn sync_from_redis(&mut self, adapter: &RedisAdapter) -> Result<(), StoreError> {
        let Some((revision, snapshot)) = read_consistent_state(adapter)? else {
            return Err(StoreError::Invalid(
                "Redis state snapshot is missing".to_owned(),
            ));
        };
        self.store.restore_snapshot_bytes(&snapshot)?;
        self.revision = revision;
        self.last_remote_revision = Some(revision);
        Ok(())
    }

    fn sync_from_redis_current(&mut self) -> Result<(), StoreError> {
        let adapter = self
            .redis
            .take()
            .ok_or_else(|| StoreError::Invalid("Redis connection is not available".to_owned()))?;
        let result = self.sync_from_redis(&adapter);
        self.redis = Some(adapter);
        result
    }

    fn publish_local_state(&mut self, idempotency_keys: &[&str]) -> Result<(), RedisError> {
        let snapshot = self.store.snapshot_bytes().map_err(store_redis_error)?;
        let adapter = self
            .redis
            .as_ref()
            .ok_or_else(|| RedisError::Protocol("Redis connection is not available".to_owned()))?;
        self.revision =
            adapter.publish_state_with_operations(self.revision, &snapshot, idempotency_keys)?;
        self.last_remote_revision = Some(self.revision);
        Ok(())
    }

    fn reconcile_outbox(&mut self) -> Result<(), StoreError> {
        let pending = self.outbox.pending()?;
        if pending.is_empty() {
            return Ok(());
        }
        let mut applied = Vec::new();
        let mut rejected: Vec<(OutboxEntry, &'static str)> = Vec::new();
        for entry in pending.iter().take(MAX_REPLAY_BATCH) {
            let already_applied = self
                .redis
                .as_ref()
                .ok_or_else(|| StoreError::Invalid("Redis connection is not available".to_owned()))?
                .operation_applied(&entry.idempotency_key)
                .map_err(redis_store_error)?;
            if already_applied {
                rejected.push((entry.clone(), "already_committed_in_redis"));
                continue;
            }
            let Some(arguments) = entry.arguments.as_object() else {
                rejected.push((entry.clone(), "invalid_arguments"));
                continue;
            };
            match replay_tool(&entry.name, arguments, &self.store) {
                Ok(_) => applied.push(entry.clone()),
                // Redis is authoritative during recovery. A conflicting or
                // invalid replay is therefore recorded rather than allowed to
                // overwrite the recovered Redis state.
                Err(_) => rejected.push((entry.clone(), "redis_priority_replay_rejected")),
            }
        }

        if !applied.is_empty() {
            let operation_keys = applied
                .iter()
                .map(|entry| entry.idempotency_key.as_str())
                .collect::<Vec<_>>();
            match self.publish_local_state(&operation_keys) {
                Ok(()) => {}
                Err(RedisError::Conflict { .. }) => {
                    self.sync_from_redis_current()?;
                    rejected.extend(
                        applied
                            .drain(..)
                            .map(|entry| (entry, "redis_priority_conflict")),
                    );
                }
                Err(error) => return Err(redis_store_error(error)),
            }
        }

        for entry in &applied {
            self.outbox.complete(&entry.idempotency_key)?;
        }
        for (entry, reason) in rejected {
            self.outbox.reject(&entry.idempotency_key, reason)?;
        }
        let remaining = self.outbox.pending()?;
        self.outbox.compact(&remaining)?;
        Ok(())
    }

    fn enter_sqlite_fallback(&mut self) {
        if let Some(adapter) = self.redis.take() {
            self.record_redis_metrics(adapter.metrics());
        }
        self.mode = BackendKind::Sqlite;
    }

    fn record_redis_metrics(&mut self, metrics: RedisMetrics) {
        self.redis_totals.commands = self.redis_totals.commands.saturating_add(metrics.commands);
        self.redis_totals.request_bytes = self
            .redis_totals
            .request_bytes
            .saturating_add(metrics.request_bytes);
        self.redis_totals.response_bytes = self
            .redis_totals
            .response_bytes
            .saturating_add(metrics.response_bytes);
    }

    fn try_reconnect(&mut self) -> Result<(), StoreError> {
        if !self.redis_configured || self.redis.is_some() {
            return Ok(());
        }
        let Some(adapter) = RedisAdapter::from_env().map_err(redis_store_error)? else {
            return Ok(());
        };
        self.attach_redis(adapter)
    }

    fn tick(&mut self) -> Result<(), StoreError> {
        if self.redis.is_none() {
            return self.try_reconnect();
        }
        if self.mode == BackendKind::Redis {
            let adapter = self.redis.as_ref().ok_or_else(|| {
                StoreError::Invalid("Redis connection is not available".to_owned())
            })?;
            let remote_revision = adapter.state_revision().map_err(redis_store_error)?;
            self.last_remote_revision = Some(remote_revision);
            if remote_revision != self.revision {
                self.sync_from_redis_current()?;
            }
            self.reconcile_outbox()
        } else {
            self.try_reconnect()
        }
    }
}

pub struct BackendCoordinator {
    shared: Arc<Mutex<CoordinatorInner>>,
    stop: Option<Sender<()>>,
    watcher: Option<JoinHandle<()>>,
}

impl BackendCoordinator {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        let redis_configured = RedisAdapter::configured();
        Self::open_with_configuration(&path, redis_configured)
    }

    pub fn sqlite_only(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        Self::open_with_configuration(&path, false)
    }

    fn open_with_configuration(path: &Path, redis_configured: bool) -> Result<Self, StoreError> {
        let mut inner = CoordinatorInner::new(path, redis_configured)?;
        if redis_configured {
            if let Ok(Some(adapter)) = RedisAdapter::from_env() {
                if inner.attach_redis(adapter).is_err() {
                    inner.enter_sqlite_fallback();
                }
            }
        }
        let shared = Arc::new(Mutex::new(inner));
        let (stop, watcher) = if redis_configured {
            let (stop_tx, stop_rx) = mpsc::channel();
            let shared_for_thread = Arc::clone(&shared);
            let interval = configured_duration(
                "MEMORY_MCP_REDIS_WATCH_INTERVAL_MS",
                DEFAULT_WATCH_INTERVAL,
                MIN_WATCH_INTERVAL,
                MAX_WATCH_INTERVAL,
            );
            let max_backoff = configured_duration(
                "MEMORY_MCP_REDIS_MAX_BACKOFF_MS",
                DEFAULT_MAX_BACKOFF,
                MIN_WATCH_INTERVAL,
                MAX_WATCH_INTERVAL,
            );
            let handle = thread::Builder::new()
                .name("memory-mcp-redis-watcher".to_owned())
                .spawn(move || watcher_loop(shared_for_thread, stop_rx, interval, max_backoff))
                .map_err(StoreError::from)?;
            (Some(stop_tx), Some(handle))
        } else {
            (None, None)
        };
        Ok(Self {
            shared,
            stop,
            watcher,
        })
    }

    pub fn backend(&self) -> BackendKind {
        self.shared
            .lock()
            .map(|inner| inner.mode)
            .unwrap_or(BackendKind::Sqlite)
    }

    pub fn status(&self) -> Result<BackendStatus, StoreError> {
        let inner = self
            .shared
            .lock()
            .map_err(|_| StoreError::Invalid("backend coordinator lock is poisoned".to_owned()))?;
        let outbox_depth = inner.outbox.pending()?.len();
        let redis_connected = inner.redis.is_some();
        let redis_revision = redis_connected
            .then_some(inner.last_remote_revision)
            .flatten();
        let current_metrics = inner
            .redis
            .as_ref()
            .map(RedisAdapter::metrics)
            .unwrap_or_default();
        Ok(BackendStatus {
            backend: inner.mode,
            redis_configured: inner.redis_configured,
            redis_connected,
            redis_revision,
            standby_revision: inner.revision,
            standby_lag: redis_revision.map(|remote| remote.saturating_sub(inner.revision)),
            outbox_depth,
            redis_commands: inner
                .redis_totals
                .commands
                .saturating_add(current_metrics.commands),
            redis_request_bytes: inner
                .redis_totals
                .request_bytes
                .saturating_add(current_metrics.request_bytes),
            redis_response_bytes: inner
                .redis_totals
                .response_bytes
                .saturating_add(current_metrics.response_bytes),
            sync_ticks: inner.sync_ticks,
            sync_errors: inner.sync_errors,
            last_sync_micros: inner.last_sync_micros,
        })
    }

    pub fn execute_tool<F>(
        &self,
        name: &str,
        arguments: &Map<String, Value>,
        operation: F,
    ) -> Result<Value, BackendToolError>
    where
        F: FnOnce(&Store) -> Result<Value, BackendToolError>,
    {
        let mut inner = self.shared.lock().map_err(|_| {
            BackendToolError::Execution(StoreError::Invalid(
                "backend coordinator lock is poisoned".to_owned(),
            ))
        })?;
        inner.prepare_for_operation();
        let mut outbox_key = None;
        if tools::is_state_mutating(name) {
            let key = operation_key(name, arguments).map_err(BackendToolError::Execution)?;
            inner
                .outbox
                .append_pending(OutboxEntry {
                    idempotency_key: key.clone(),
                    name: name.to_owned(),
                    arguments: Value::Object(arguments.clone()),
                })
                .map_err(BackendToolError::Execution)?;
            outbox_key = Some(key);
        }

        let result = operation(&inner.store);
        match result {
            Ok(value) => {
                if tools::is_state_mutating(name) && inner.mode == BackendKind::Redis {
                    let key = outbox_key
                        .as_deref()
                        .expect("state mutation has an outbox key");
                    match inner.publish_local_state(&[key]) {
                        Ok(()) => {
                            inner
                                .outbox
                                .complete(key)
                                .map_err(BackendToolError::Execution)?;
                            let remaining = inner
                                .outbox
                                .pending()
                                .map_err(BackendToolError::Execution)?;
                            inner
                                .outbox
                                .compact(&remaining)
                                .map_err(BackendToolError::Execution)?;
                        }
                        Err(RedisError::Conflict { .. }) => {
                            inner.enter_sqlite_fallback();
                            return Err(BackendToolError::Execution(StoreError::Invalid(
                                "Redis state changed; operation was queued for reconciliation"
                                    .to_owned(),
                            )));
                        }
                        Err(_) => {
                            inner.enter_sqlite_fallback();
                        }
                    }
                }
                Ok(value)
            }
            Err(error @ BackendToolError::Execution(_))
            | Err(error @ BackendToolError::InvalidParams(_)) => {
                if let Some(key) = outbox_key {
                    inner
                        .outbox
                        .complete(&key)
                        .map_err(BackendToolError::Execution)?;
                    let remaining = inner
                        .outbox
                        .pending()
                        .map_err(BackendToolError::Execution)?;
                    inner
                        .outbox
                        .compact(&remaining)
                        .map_err(BackendToolError::Execution)?;
                }
                Err(error)
            }
        }
    }
}

impl Drop for BackendCoordinator {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(watcher) = self.watcher.take() {
            let _ = watcher.join();
        }
    }
}

fn watcher_loop(
    shared: Arc<Mutex<CoordinatorInner>>,
    stop: Receiver<()>,
    interval: Duration,
    max_backoff: Duration,
) {
    let mut delay = interval;
    loop {
        match stop.recv_timeout(delay) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
        let result = shared
            .lock()
            .map_err(|_| StoreError::Invalid("backend coordinator lock is poisoned".to_owned()))
            .and_then(|mut inner| {
                let started = Instant::now();
                let result = inner.tick();
                inner.sync_ticks = inner.sync_ticks.saturating_add(1);
                inner.last_sync_micros =
                    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                if result.is_err() {
                    inner.sync_errors = inner.sync_errors.saturating_add(1);
                }
                result
            });
        if result.is_ok() {
            delay = interval;
        } else {
            delay = std::cmp::min(delay.saturating_mul(2), max_backoff);
        }
    }
}

fn read_consistent_state(adapter: &RedisAdapter) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
    for _ in 0..2 {
        let before = adapter.state_revision().map_err(redis_store_error)?;
        let snapshot = adapter.state_snapshot().map_err(redis_store_error)?;
        let after = adapter.state_revision().map_err(redis_store_error)?;
        if before == after {
            return Ok(snapshot.map(|snapshot| (after, snapshot)));
        }
    }
    Err(StoreError::Invalid(
        "Redis state changed while reading a bounded snapshot".to_owned(),
    ))
}

fn operation_key(name: &str, arguments: &Map<String, Value>) -> Result<String, StoreError> {
    let request = json!({"name": name, "arguments": arguments});
    let encoded = serde_json::to_vec(&request)
        .map_err(|error| StoreError::Invalid(format!("operation key failed: {error}")))?;
    Ok(encode(Sha256::digest(encoded)))
}

fn redis_store_error(error: RedisError) -> StoreError {
    StoreError::Invalid(format!("Redis backend unavailable: {error}"))
}

fn store_redis_error(error: StoreError) -> RedisError {
    RedisError::Protocol(format!("SQLite snapshot failed: {error}"))
}

fn configured_duration(name: &str, default: Duration, min: Duration, max: Duration) -> Duration {
    let Some(milliseconds) = std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return default;
    };
    let value = Duration::from_millis(milliseconds);
    value.clamp(min, max)
}

fn append_json_line(
    path: &Path,
    value: &Value,
    max_bytes: u64,
    label: &str,
) -> Result<(), StoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let encoded = serde_json::to_vec(value)
        .map_err(|error| StoreError::Invalid(format!("{label} serialization failed: {error}")))?;
    let current_size = fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let encoded_size = u64::try_from(encoded.len() + 1).unwrap_or(u64::MAX);
    if current_size.saturating_add(encoded_size) > max_bytes {
        return Err(StoreError::Invalid(format!(
            "{label} exceeds the configured size limit"
        )));
    }
    let mut file = open_private_append(path)?;
    file.write_all(&encoded)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn temporary_outbox_path(path: &Path) -> PathBuf {
    let sequence = OUTBOX_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut temporary = path.as_os_str().to_os_string();
    temporary.push(format!(".{}.{}.tmp", std::process::id(), sequence));
    PathBuf::from(temporary)
}

fn open_private_new(path: &Path) -> Result<File, StoreError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    Ok(options.open(path)?)
}

fn open_private_append(path: &Path) -> Result<File, StoreError> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).append(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        let mut permissions = file.metadata()?.permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
        fs::set_permissions(path, permissions)?;
    }
    Ok(file)
}

fn open_private_rewrite(path: &Path) -> Result<File, StoreError> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        let mut permissions = file.metadata()?.permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
        fs::set_permissions(path, permissions)?;
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_database_path() -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "memory-mcp-rust-coordinator-{}-{timestamp}.db",
            std::process::id()
        ))
    }

    #[test]
    fn outbox_is_durable_and_compacts_completed_entries() {
        let database = test_database_path();
        let outbox = Outbox::new(&database);
        let entry = OutboxEntry {
            idempotency_key: "op-1".to_owned(),
            name: "remember_fact".to_owned(),
            arguments: json!({"text": "queued", "workspace": "w"}),
        };
        outbox.append_pending(entry.clone()).expect("append");
        assert_eq!(outbox.pending().unwrap(), vec![entry.clone()]);
        outbox.complete("op-1").expect("complete");
        outbox.compact(&outbox.pending().unwrap()).expect("compact");
        assert!(outbox.pending().unwrap().is_empty());
        let _ = fs::remove_file(outbox.path);
        let _ = fs::remove_file(outbox.audit_path);
        let _ = fs::remove_file(database);
    }

    #[test]
    fn rejected_replay_is_recorded_without_retaining_payloads() {
        let database = test_database_path();
        let outbox = Outbox::new(&database);
        let entry = OutboxEntry {
            idempotency_key: "op-rejected".to_owned(),
            name: "remember_fact".to_owned(),
            arguments: json!({"text": "sensitive memory", "workspace": "w"}),
        };
        outbox.append_pending(entry).expect("append");
        outbox
            .reject("op-rejected", "redis_priority_conflict")
            .expect("audit rejection");
        assert!(outbox.pending().unwrap().is_empty());
        let audit = fs::read_to_string(&outbox.audit_path).expect("audit file");
        assert!(audit.contains("op-rejected"));
        assert!(audit.contains("redis_priority_conflict"));
        assert!(!audit.contains("sensitive memory"));
        let _ = fs::remove_file(outbox.path);
        let _ = fs::remove_file(outbox.audit_path);
        let _ = fs::remove_file(database);
    }

    #[test]
    fn recovery_skips_outbox_entry_when_redis_marker_already_exists() {
        let database = test_database_path();
        let source = Store::open(&database).expect("source store");
        source
            .remember_fact("already committed", "w")
            .expect("source fact");
        let snapshot = source.snapshot_bytes().expect("source snapshot");
        drop(source);

        let arguments = json!({"text": "already committed", "workspace": "w"});
        let idempotency_key =
            operation_key("remember_fact", arguments.as_object().unwrap()).expect("operation key");
        let values = Arc::new(Mutex::new(HashMap::from([
            (
                b"coordinator-marker-test:state:revision".to_vec(),
                b"1".to_vec(),
            ),
            (b"coordinator-marker-test:state:snapshot".to_vec(), snapshot),
            (
                format!("coordinator-marker-test:operation:{idempotency_key}").into_bytes(),
                b"1".to_vec(),
            ),
        ])));
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        let server_values = Arc::clone(&values);
        let server = thread::spawn(move || run_snapshot_redis(listener, server_values, None));
        let adapter =
            RedisAdapter::connect(&format!("redis://{address}"), "coordinator-marker-test")
                .expect("Redis adapter");
        let mut inner = CoordinatorInner::new(&database, true).expect("coordinator state");
        inner
            .outbox
            .append_pending(OutboxEntry {
                idempotency_key,
                name: "remember_fact".to_owned(),
                arguments,
            })
            .expect("pending outbox entry");

        inner.attach_redis(adapter).expect("reconcile marked entry");
        assert!(inner.outbox.pending().expect("pending entries").is_empty());
        let audit = fs::read_to_string(&inner.outbox.audit_path).expect("reconciliation audit");
        assert!(audit.contains("already_committed_in_redis"));
        assert!(!audit.contains("already committed"));

        drop(inner);
        server.join().expect("Redis fixture");
        let _ = fs::remove_file(database.with_extension("outbox.jsonl"));
        let _ = fs::remove_file(database.with_extension("reconciliation.jsonl"));
        let _ = fs::remove_file(database.with_extension("db-wal"));
        let _ = fs::remove_file(database.with_extension("db-shm"));
        let _ = fs::remove_file(database);
    }

    #[test]
    fn sqlite_coordinator_routes_mutations_through_durable_store() {
        let database = test_database_path();
        let coordinator = BackendCoordinator {
            shared: Arc::new(Mutex::new(
                CoordinatorInner::new(&database, false).expect("coordinator"),
            )),
            stop: None,
            watcher: None,
        };
        let arguments = json!({"text": "fallback", "workspace": "w"});
        let result = coordinator
            .execute_tool("remember_fact", arguments.as_object().unwrap(), |store| {
                store
                    .remember_fact("fallback", "w")
                    .map(|fact| serde_json::to_value(fact).expect("fact"))
                    .map_err(BackendToolError::Execution)
            })
            .expect("operation");
        assert_eq!(result["id"], 1);
        assert_eq!(coordinator.backend(), BackendKind::Sqlite);
        assert_eq!(
            coordinator
                .shared
                .lock()
                .expect("coordinator lock")
                .outbox
                .pending()
                .unwrap()
                .len(),
            1
        );
        let _ = fs::remove_file(database.with_extension("outbox.jsonl"));
        let _ = fs::remove_file(database.with_extension("reconciliation.jsonl"));
        let _ = fs::remove_file(database);
    }

    #[test]
    fn coordinator_publishes_full_state_when_redis_is_reachable() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        let values = Arc::new(Mutex::new(HashMap::new()));
        let server_values = Arc::clone(&values);
        let server = thread::spawn(move || run_snapshot_redis(listener, server_values, None));
        let adapter = RedisAdapter::connect(&format!("redis://{address}"), "coordinator-test")
            .expect("Redis adapter");
        let database = test_database_path();
        let mut inner = CoordinatorInner::new(&database, true).expect("coordinator state");
        inner.attach_redis(adapter).expect("initial Redis state");
        let coordinator = BackendCoordinator {
            shared: Arc::new(Mutex::new(inner)),
            stop: None,
            watcher: None,
        };

        let arguments = json!({"text": "Redis primary fact", "workspace": "w"});
        coordinator
            .execute_tool("remember_fact", arguments.as_object().unwrap(), |store| {
                store
                    .remember_fact("Redis primary fact", "w")
                    .map(|fact| serde_json::to_value(fact).expect("fact"))
                    .map_err(BackendToolError::Execution)
            })
            .expect("Redis operation");

        let state = coordinator.shared.lock().expect("coordinator lock");
        assert_eq!(state.mode, BackendKind::Redis);
        assert_eq!(state.revision, 2);
        assert!(state.outbox.pending().unwrap().is_empty());
        let snapshot = state
            .redis
            .as_ref()
            .expect("Redis connection")
            .state_snapshot()
            .expect("state snapshot")
            .expect("published state");
        assert!(!snapshot.is_empty());
        drop(state);
        let status = coordinator.status().expect("status");
        assert_eq!(status.backend, BackendKind::Redis);
        assert_eq!(status.redis_revision, Some(2));
        assert_eq!(status.standby_lag, Some(0));
        assert_eq!(status.outbox_depth, 0);
        assert!(status.redis_commands > 0);
        assert!(status.redis_request_bytes > 0);
        assert!(status.redis_response_bytes > 0);
        drop(coordinator);
        server.join().expect("Redis fixture");
        let _ = fs::remove_file(database.with_extension("outbox.jsonl"));
        let _ = fs::remove_file(database.with_extension("reconciliation.jsonl"));
        let _ = fs::remove_file(database.with_extension("db-wal"));
        let _ = fs::remove_file(database.with_extension("db-shm"));
        let _ = fs::remove_file(database);
    }

    #[test]
    fn watcher_stops_without_waiting_for_the_next_tick() {
        let database = test_database_path();
        let inner = CoordinatorInner::new(&database, true).expect("coordinator state");
        let shared = Arc::new(Mutex::new(inner));
        let (stop_tx, stop_rx) = mpsc::channel();
        let watcher = thread::spawn(move || {
            watcher_loop(
                shared,
                stop_rx,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
        });
        stop_tx.send(()).expect("stop watcher");
        watcher.join().expect("watcher stopped");
        let _ = fs::remove_file(database.with_extension("outbox.jsonl"));
        let _ = fs::remove_file(database.with_extension("reconciliation.jsonl"));
        let _ = fs::remove_file(database.with_extension("db-wal"));
        let _ = fs::remove_file(database.with_extension("db-shm"));
        let _ = fs::remove_file(database);
    }

    #[test]
    fn watcher_uses_one_revision_command_when_state_is_unchanged() {
        let values = Arc::new(Mutex::new(HashMap::new()));
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        let server_values = Arc::clone(&values);
        let server = thread::spawn(move || run_snapshot_redis(listener, server_values, None));
        let adapter = RedisAdapter::connect(&format!("redis://{address}"), "watcher-metrics-test")
            .expect("Redis adapter");
        let database = test_database_path();
        let mut inner = CoordinatorInner::new(&database, true).expect("coordinator state");
        inner.attach_redis(adapter).expect("initial Redis state");
        let baseline_commands = inner
            .redis
            .as_ref()
            .expect("Redis connection")
            .metrics()
            .commands;
        let shared = Arc::new(Mutex::new(inner));
        let (stop_tx, stop_rx) = mpsc::channel();
        let watcher_shared = Arc::clone(&shared);
        let watcher = thread::spawn(move || {
            watcher_loop(
                watcher_shared,
                stop_rx,
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if shared.lock().expect("coordinator lock").sync_ticks > 0 {
                break;
            }
            assert!(Instant::now() < deadline, "watcher did not record a tick");
            thread::yield_now();
        }
        stop_tx.send(()).expect("stop watcher");
        watcher.join().expect("watcher stopped");

        let state = shared.lock().expect("coordinator lock");
        let ticks = state.sync_ticks;
        let commands = state
            .redis
            .as_ref()
            .expect("Redis connection")
            .metrics()
            .commands;
        assert_eq!(state.sync_errors, 0);
        assert_eq!(commands - baseline_commands, ticks);
        drop(state);
        drop(shared);
        server.join().expect("Redis fixture");
        let _ = fs::remove_file(database.with_extension("outbox.jsonl"));
        let _ = fs::remove_file(database.with_extension("reconciliation.jsonl"));
        let _ = fs::remove_file(database.with_extension("db-wal"));
        let _ = fs::remove_file(database.with_extension("db-shm"));
        let _ = fs::remove_file(database);
    }

    #[test]
    fn coordinator_fails_over_and_replays_outbox_after_redis_recovery() {
        let values = Arc::new(Mutex::new(HashMap::new()));
        let first_listener = TcpListener::bind("127.0.0.1:0").expect("first listener");
        let first_address = first_listener.local_addr().expect("first address");
        let first_values = Arc::clone(&values);
        let first_server = thread::spawn(move || {
            run_snapshot_redis(first_listener, first_values, Some(18));
        });
        let adapter = RedisAdapter::connect(
            &format!("redis://{first_address}"),
            "coordinator-recovery-test",
        )
        .expect("first Redis adapter");
        let database = test_database_path();
        let mut inner = CoordinatorInner::new(&database, true).expect("coordinator state");
        inner.attach_redis(adapter).expect("initial Redis state");
        let coordinator = BackendCoordinator {
            shared: Arc::new(Mutex::new(inner)),
            stop: None,
            watcher: None,
        };

        let first_arguments = json!({"text": "confirmed before loss", "workspace": "w"});
        coordinator
            .execute_tool(
                "remember_fact",
                first_arguments.as_object().unwrap(),
                |store| {
                    store
                        .remember_fact("confirmed before loss", "w")
                        .map(|fact| serde_json::to_value(fact).expect("fact"))
                        .map_err(BackendToolError::Execution)
                },
            )
            .expect("first Redis operation");
        first_server.join().expect("first Redis fixture");

        let fallback_arguments = json!({"text": "written during loss", "workspace": "w"});
        coordinator
            .execute_tool(
                "remember_fact",
                fallback_arguments.as_object().unwrap(),
                |store| {
                    store
                        .remember_fact("written during loss", "w")
                        .map(|fact| serde_json::to_value(fact).expect("fact"))
                        .map_err(BackendToolError::Execution)
                },
            )
            .expect("SQLite fallback operation");
        assert_eq!(coordinator.backend(), BackendKind::Sqlite);
        let fallback_status = coordinator.status().expect("fallback status");
        assert!(!fallback_status.redis_connected);
        assert!(fallback_status.redis_commands > 0);
        assert!(fallback_status.redis_request_bytes > 0);
        assert!(fallback_status.redis_response_bytes > 0);
        {
            let state = coordinator.shared.lock().expect("coordinator lock");
            assert_eq!(state.outbox.pending().unwrap().len(), 1);
        }

        let second_listener = TcpListener::bind("127.0.0.1:0").expect("second listener");
        let second_address = second_listener.local_addr().expect("second address");
        let second_values = Arc::clone(&values);
        let second_server =
            thread::spawn(move || run_snapshot_redis(second_listener, second_values, None));
        let second_adapter = RedisAdapter::connect(
            &format!("redis://{second_address}"),
            "coordinator-recovery-test",
        )
        .expect("recovered Redis adapter");
        coordinator
            .shared
            .lock()
            .expect("coordinator lock")
            .attach_redis(second_adapter)
            .expect("reconcile outbox");
        assert_eq!(coordinator.backend(), BackendKind::Redis);
        {
            let state = coordinator.shared.lock().expect("coordinator lock");
            assert!(state.outbox.pending().unwrap().is_empty());
        }
        let list_arguments = json!({"workspace": "w"});
        let facts = coordinator
            .execute_tool("list_facts", list_arguments.as_object().unwrap(), |store| {
                store
                    .list_facts("w")
                    .map(|facts| serde_json::to_value(facts).expect("facts"))
                    .map_err(BackendToolError::Execution)
            })
            .expect("recovered read");
        assert_eq!(facts.as_array().unwrap().len(), 2);

        drop(coordinator);
        second_server.join().expect("second Redis fixture");
        let _ = fs::remove_file(database.with_extension("outbox.jsonl"));
        let _ = fs::remove_file(database.with_extension("reconciliation.jsonl"));
        let _ = fs::remove_file(database.with_extension("db-wal"));
        let _ = fs::remove_file(database.with_extension("db-shm"));
        let _ = fs::remove_file(database);
    }

    fn run_snapshot_redis(
        listener: TcpListener,
        shared_values: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
        close_after: Option<usize>,
    ) {
        let (mut stream, _) = listener.accept().expect("Redis client");
        let mut transaction: Option<Vec<Vec<Vec<u8>>>> = None;
        let mut command_count = 0;
        while let Some(arguments) = read_request(&mut stream) {
            command_count += 1;
            let command = arguments[0].as_slice();
            if let Some(queue) = transaction.as_mut() {
                if command != b"EXEC" {
                    queue.push(arguments);
                    write_simple(&mut stream, b"QUEUED");
                    continue;
                }
            }
            match command {
                b"PING" => write_simple(&mut stream, b"PONG"),
                b"WATCH" | b"UNWATCH" => write_simple(&mut stream, b"OK"),
                b"MULTI" => {
                    transaction = Some(Vec::new());
                    write_simple(&mut stream, b"OK");
                }
                b"EXEC" => {
                    let queued = transaction.take().unwrap_or_default();
                    let result_count = queued.len();
                    let mut revision = 0;
                    for command in queued {
                        match command[0].as_slice() {
                            b"SET" => {
                                shared_values
                                    .lock()
                                    .expect("Redis values")
                                    .insert(command[1].clone(), command[2].clone());
                            }
                            b"INCR" => {
                                revision = shared_values
                                    .lock()
                                    .expect("Redis values")
                                    .get(&command[1])
                                    .map(|value| {
                                        String::from_utf8_lossy(value).parse::<i64>().unwrap()
                                    })
                                    .unwrap_or(0)
                                    + 1;
                                shared_values
                                    .lock()
                                    .expect("Redis values")
                                    .insert(command[1].clone(), revision.to_string().into_bytes());
                            }
                            _ => {}
                        }
                    }
                    stream
                        .write_all(format!("*{result_count}\r\n+OK\r\n:{revision}\r\n").as_bytes())
                        .expect("EXEC response");
                    for _ in 2..result_count {
                        write_simple(&mut stream, b"OK");
                    }
                }
                b"GET" => {
                    let values = shared_values.lock().expect("Redis values");
                    write_bulk(&mut stream, values.get(&arguments[1]));
                }
                b"SET" => {
                    shared_values
                        .lock()
                        .expect("Redis values")
                        .insert(arguments[1].clone(), arguments[2].clone());
                    write_simple(&mut stream, b"OK");
                }
                b"INCR" => {
                    let revision = shared_values
                        .lock()
                        .expect("Redis values")
                        .get(&arguments[1])
                        .map(|value| String::from_utf8_lossy(value).parse::<i64>().unwrap())
                        .unwrap_or(0)
                        + 1;
                    shared_values
                        .lock()
                        .expect("Redis values")
                        .insert(arguments[1].clone(), revision.to_string().into_bytes());
                    write_integer(&mut stream, revision);
                }
                _ => write_error(&mut stream, b"unsupported test command"),
            }
            if close_after == Some(command_count) {
                return;
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> Option<Vec<Vec<u8>>> {
        let mut prefix = [0; 1];
        stream.read_exact(&mut prefix).ok()?;
        if prefix[0] != b'*' {
            return None;
        }
        let count = read_line(stream)?.parse::<usize>().ok()?;
        (0..count)
            .map(|_| {
                let mut bulk = [0; 1];
                stream.read_exact(&mut bulk).ok()?;
                if bulk[0] != b'$' {
                    return None;
                }
                let length = read_line(stream)?.parse::<usize>().ok()?;
                let mut value = vec![0; length];
                stream.read_exact(&mut value).ok()?;
                let mut terminator = [0; 2];
                stream.read_exact(&mut terminator).ok()?;
                (terminator == *b"\r\n").then_some(value)
            })
            .collect()
    }

    fn read_line(stream: &mut TcpStream) -> Option<String> {
        let mut line = Vec::new();
        loop {
            let mut byte = [0; 1];
            stream.read_exact(&mut byte).ok()?;
            if byte[0] == b'\n' {
                if line.last() != Some(&b'\r') {
                    return None;
                }
                line.pop();
                return String::from_utf8(line).ok();
            }
            line.push(byte[0]);
        }
    }

    fn write_simple(stream: &mut TcpStream, value: &[u8]) {
        stream.write_all(b"+").expect("simple response");
        stream.write_all(value).expect("simple response");
        stream.write_all(b"\r\n").expect("simple response");
    }

    fn write_integer(stream: &mut TcpStream, value: i64) {
        stream
            .write_all(format!(":{value}\r\n").as_bytes())
            .expect("integer response");
    }

    fn write_bulk(stream: &mut TcpStream, value: Option<&Vec<u8>>) {
        let Some(value) = value else {
            stream.write_all(b"$-1\r\n").expect("bulk response");
            return;
        };
        stream
            .write_all(format!("${}\r\n", value.len()).as_bytes())
            .expect("bulk response");
        stream.write_all(value).expect("bulk response");
        stream.write_all(b"\r\n").expect("bulk response");
    }

    fn write_error(stream: &mut TcpStream, value: &[u8]) {
        stream.write_all(b"-").expect("error response");
        stream.write_all(value).expect("error response");
        stream.write_all(b"\r\n").expect("error response");
    }
}
