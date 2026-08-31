use hex::encode;
use rusqlite::{params, Connection, DatabaseName, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::{RefCell, UnsafeCell};
use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub enum StoreError {
    Io(std::io::Error),
    Sql(rusqlite::Error),
    Invalid(String),
}

impl Display for StoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Sql(error) => write!(f, "sqlite error: {error}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<std::io::Error> for StoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Fact {
    pub id: i64,
    pub text: String,
    pub sha256: String,
    pub workspace: String,
    pub lifecycle: String,
    pub source: String,
    pub project: String,
    pub domain: String,
    pub trust: String,
    pub strong: bool,
    pub importance: f64,
    pub category_id: Option<i64>,
    pub validity: String,
    pub session_id: String,
    pub access_count: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FactMetadata {
    pub source: String,
    pub project: String,
    pub domain: String,
    pub trust: String,
    pub strong: bool,
    pub importance: f64,
}

impl Default for FactMetadata {
    fn default() -> Self {
        Self {
            source: String::new(),
            project: String::new(),
            domain: String::new(),
            trust: "medium".to_owned(),
            strong: false,
            importance: 0.5,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FactFilters {
    pub source: Option<String>,
    pub project: Option<String>,
    pub domain: Option<String>,
    pub trust: Option<String>,
    pub strong: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FactVerification {
    pub checked: i64,
    pub valid: bool,
    pub invalid_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FactChunk {
    pub fact_id: i64,
    pub index: i64,
    pub total: i64,
    pub content: String,
    pub byte_size: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Recall {
    pub facts: Vec<Fact>,
    pub contexts: Vec<Context>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub canonical_name: String,
    pub entity_type: String,
    pub aliases: Vec<String>,
    pub workspace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntitySpec {
    pub name: String,
    pub entity_type: String,
    pub aliases: Vec<String>,
    pub workspace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationSpec {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub source_fact_id: Option<i64>,
    pub workspace: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Relation {
    pub id: i64,
    pub subject_id: i64,
    pub predicate: String,
    pub object_id: i64,
    pub source_fact_id: Option<i64>,
    pub workspace: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecisionSpec {
    pub category: String,
    pub subject: String,
    pub scenario: String,
    pub reasoning: String,
    pub outcome: String,
    pub confidence: Option<f64>,
    pub decision_maker: String,
    pub issue_ref: String,
    pub path: String,
    pub symbol: String,
    pub parent_id: Option<i64>,
    pub workspace: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Decision {
    pub id: i64,
    pub category: String,
    pub subject: String,
    pub scenario: String,
    pub reasoning: String,
    pub outcome: String,
    pub confidence: Option<f64>,
    pub decision_maker: String,
    pub issue_ref: String,
    pub path: String,
    pub symbol: String,
    pub parent_id: Option<i64>,
    pub workspace: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GraphSearch {
    pub entities: Vec<Entity>,
    pub relations: Vec<Relation>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DecisionConflict {
    pub subject: String,
    pub scenario: String,
    pub outcomes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceSpec {
    pub fact_id: i64,
    pub source_ref: String,
    pub source: String,
    pub checksum: String,
    pub fetched_at: Option<String>,
    pub repository_ref: String,
    pub path: String,
    pub symbol: String,
    pub line_start: Option<i64>,
    pub line_end: Option<i64>,
    pub column_start: Option<i64>,
    pub column_end: Option<i64>,
    pub selected_text: String,
    pub resolution_status: String,
    pub workspace: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Evidence {
    pub id: i64,
    pub fact_id: i64,
    pub source_ref: String,
    pub source: String,
    pub checksum: String,
    pub fetched_at: Option<String>,
    pub repository_ref: String,
    pub path: String,
    pub symbol: String,
    pub line_start: Option<i64>,
    pub line_end: Option<i64>,
    pub column_start: Option<i64>,
    pub column_end: Option<i64>,
    pub selected_text_sha256: String,
    pub resolution_status: String,
    pub workspace: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FactEvidenceSummary {
    pub total: usize,
    pub resolved: usize,
    pub stale: usize,
    pub unresolved: usize,
}

impl FactEvidenceSummary {
    pub fn status(self) -> &'static str {
        if self.resolved > 0 {
            "resolved"
        } else if self.stale > 0 {
            "stale"
        } else if self.unresolved > 0 {
            "unresolved"
        } else {
            "missing"
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MemoryExport {
    pub facts: Vec<Fact>,
    pub contexts: Vec<Context>,
    pub events: Vec<LifecycleEvent>,
    pub handoffs: Vec<Handoff>,
    pub entities: Vec<Entity>,
    pub relations: Vec<Relation>,
    pub decisions: Vec<Decision>,
    pub evidence: Vec<Evidence>,
    pub categories: Vec<Category>,
    pub runs: Vec<Run>,
    pub measurements: Vec<Measurement>,
    pub feedback: Vec<Feedback>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Context {
    pub reference: String,
    pub name: String,
    pub content: String,
    pub sha256: String,
    pub workspace: String,
    pub schema: String,
    pub source: String,
    pub expires_at: Option<String>,
    pub byte_size: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContextMetadata {
    pub schema: String,
    pub source: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextChunk {
    pub reference: String,
    pub index: i64,
    pub total: i64,
    pub content: String,
    pub byte_size: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextLineage {
    pub parent_reference: String,
    pub child_reference: String,
    pub relation: String,
    pub workspace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSpec {
    pub idempotency_key: String,
    pub event_type: String,
    pub context_reference: String,
    pub metadata: String,
    pub payload: String,
    pub payload_truncated: bool,
    pub workspace: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LifecycleEvent {
    pub id: i64,
    pub idempotency_key: String,
    pub event_type: String,
    pub context_reference: String,
    pub metadata: String,
    pub payload_sha256: String,
    pub payload_size: i64,
    pub payload_truncated: bool,
    pub workspace: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffSpec {
    pub idempotency_key: String,
    pub context_reference: String,
    pub owner: String,
    pub session: String,
    pub source: String,
    pub workspace: String,
    pub shared: bool,
    pub ttl_seconds: Option<i64>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Handoff {
    pub id: i64,
    pub idempotency_key: String,
    pub context_reference: String,
    pub owner: String,
    pub session: String,
    pub source: String,
    pub workspace: String,
    pub shared: bool,
    pub expires_at: Option<String>,
    pub state: String,
    pub accepted_at: Option<String>,
    pub accepted_by: Option<String>,
    pub cancelled_at: Option<String>,
    pub cancelled_by: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSpec {
    pub run_id: String,
    pub issue_ref: String,
    pub pr_ref: String,
    pub session: String,
    pub git_ref: String,
    pub files: String,
    pub diff: String,
    pub workspace: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Run {
    pub id: i64,
    pub run_id: String,
    pub issue_ref: String,
    pub pr_ref: String,
    pub session: String,
    pub git_ref: String,
    pub files: String,
    pub diff: String,
    pub summary: String,
    pub state: String,
    pub workspace: String,
    pub created_at: String,
    pub ended_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeasurementSpec {
    pub measurement: String,
    pub sample: String,
    pub variant: String,
    pub value: f64,
    pub baseline: bool,
    pub workspace: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Measurement {
    pub id: i64,
    pub measurement: String,
    pub sample: String,
    pub variant: String,
    pub value: f64,
    pub baseline: bool,
    pub workspace: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackSpec {
    pub feedback_id: String,
    pub site: String,
    pub item_type: String,
    pub item_ref: String,
    pub signal: String,
    pub query_hash: String,
    pub workspace: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Feedback {
    pub id: i64,
    pub feedback_id: String,
    pub site: String,
    pub item_type: String,
    pub item_ref: String,
    pub signal: String,
    pub query_hash: String,
    pub workspace: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Category {
    pub id: i64,
    pub name: String,
    pub workspace: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FactHistory {
    pub id: i64,
    pub fact_id: i64,
    pub event: String,
    pub from_lifecycle: String,
    pub to_lifecycle: String,
    pub note: String,
    pub workspace: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RetrievalGuard {
    pub status: String,
    pub reason: String,
    pub facts: Vec<Fact>,
    pub contexts: Vec<Context>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IndexSummary {
    pub facts: i64,
    pub active_facts: i64,
    pub forgotten_facts: i64,
    pub contexts: i64,
    pub categories: i64,
    pub runs: i64,
    pub measurements: i64,
    pub feedback: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PreparedSummary {
    pub summary: IndexSummary,
    pub recall: Recall,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EmbeddingBackfill {
    pub status: String,
    pub updated: i64,
    pub reason: String,
}

/// A stored vector together with the fact it belongs to.  The vector is kept
/// out of `Fact` so the normal Redis/native projection remains small and
/// backwards compatible with the pre-provider schema.
#[derive(Debug, Clone, PartialEq)]
pub struct FactEmbedding {
    pub fact: Fact,
    pub vector: Vec<f32>,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactSearchMetadata {
    pub category: Option<String>,
    pub confirmed: bool,
    pub invalid_at: String,
    pub archived: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AnchoredSearch {
    pub decisions: Vec<Decision>,
    pub evidence: Vec<Evidence>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConsolidationReport {
    pub status: String,
    pub scanned: i64,
    pub consolidated: i64,
    pub remaining: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkspaceBackup {
    #[serde(skip_serializing)]
    pub path: String,
    pub bytes: i64,
    pub facts: i64,
    pub contexts: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DatabaseInfo {
    pub name: String,
    #[serde(skip_serializing)]
    pub path: String,
    pub active: bool,
    pub archived: bool,
    pub bytes: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DatabaseBackup {
    pub database: String,
    #[serde(skip_serializing)]
    pub path: String,
    pub bytes: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Stats {
    pub facts: i64,
    pub contexts: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Workspace {
    pub id: String,
    pub status: String,
}

pub const MAX_FACT_TEXT_CHARS: usize = 16 * 1024;
pub const DEFAULT_CONTEXT_MAX_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_CONTEXT_MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_EVENT_PAYLOAD_BYTES: usize = 16 * 1024;
const MAX_EVENT_METADATA_BYTES: usize = 16 * 1024;
const MAX_RUN_FILES_BYTES: usize = 64 * 1024;
const MAX_RUN_DIFF_BYTES: usize = 128 * 1024;
const MAX_DATABASE_SNAPSHOT_BYTES: usize = 32 * 1024 * 1024;
static SNAPSHOT_COUNTER: AtomicU64 = AtomicU64::new(0);

struct ConnectionSlot {
    connection: UnsafeCell<Connection>,
}

impl ConnectionSlot {
    fn new(connection: Connection) -> Self {
        Self {
            connection: UnsafeCell::new(connection),
        }
    }

    fn replace(&self, connection: Connection) -> Connection {
        // SAFETY: Store is deliberately !Sync because this slot is only used by
        // the single-threaded stdio dispatcher. No public operation can hold a
        // reference into the slot across a database selection.
        unsafe { std::mem::replace(&mut *self.connection.get(), connection) }
    }

    fn backup<P: AsRef<Path>>(&self, destination: P) -> rusqlite::Result<()> {
        self.deref().backup(DatabaseName::Main, destination, None)
    }

    fn restore<P: AsRef<Path>>(&self, source: P) -> rusqlite::Result<()> {
        // SAFETY: Store serializes every operation through its dispatcher (and
        // the coordinator's mutex when the background watcher is enabled), so
        // no statement can be using the connection while it is restored.
        unsafe {
            (&mut *self.connection.get()).restore(
                DatabaseName::Main,
                source,
                None::<fn(rusqlite::backup::Progress)>,
            )
        }
    }

    fn into_inner(self) -> Connection {
        self.connection.into_inner()
    }
}

impl Deref for ConnectionSlot {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        // SAFETY: The slot is accessed serially by the single-threaded Store
        // dispatcher; see replace for the ownership invariant.
        unsafe { &*self.connection.get() }
    }
}

pub struct Store {
    connection: ConnectionSlot,
    database_path: RefCell<Option<PathBuf>>,
    default_database_path: RefCell<Option<PathBuf>>,
    database_root: Option<PathBuf>,
    memory_database_name: RefCell<Option<String>>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StoreError::Invalid(
                    "database path must not be a symbolic link".to_owned(),
                ));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(StoreError::Invalid(
                    "database path must reference a regular file".to_owned(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut options = OpenOptions::new();
                options.read(true).write(true).create_new(true);
                #[cfg(unix)]
                std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
                drop(options.open(path)?);
            }
            Err(error) => return Err(StoreError::Io(error)),
        }
        let connection = Connection::open(path)?;
        set_private_file_mode(path)?;
        Self::from_connection(connection, Some(path.to_path_buf()))
    }

    pub fn in_memory() -> Result<Self, StoreError> {
        Self::from_connection(Connection::open_in_memory()?, None)
    }

    /// Export the complete SQLite database as a bounded binary snapshot.
    ///
    /// The snapshot includes the schema, indexes, FTS tables, and all
    /// workspaces, unlike the user-facing workspace JSON export. It is used by
    /// the Redis coordinator as the standby replication payload.
    pub fn snapshot_bytes(&self) -> Result<Vec<u8>, StoreError> {
        let path = temporary_snapshot_path("export");
        create_private_file(&path, &[])?;
        let result = self
            .connection
            .backup(&path)
            .map_err(StoreError::from)
            .and_then(|_| {
                let bytes = fs::read(&path)?;
                if bytes.len() > MAX_DATABASE_SNAPSHOT_BYTES {
                    return Err(StoreError::Invalid(
                        "database snapshot exceeds the configured size limit".to_owned(),
                    ));
                }
                Ok(bytes)
            });
        let _ = fs::remove_file(&path);
        result
    }

    /// Restore a complete SQLite database snapshot into this store.
    pub fn restore_snapshot_bytes(&self, bytes: &[u8]) -> Result<(), StoreError> {
        if bytes.is_empty() {
            return Err(StoreError::Invalid(
                "database snapshot must not be empty".to_owned(),
            ));
        }
        if bytes.len() > MAX_DATABASE_SNAPSHOT_BYTES {
            return Err(StoreError::Invalid(
                "database snapshot exceeds the configured size limit".to_owned(),
            ));
        }
        let path = temporary_snapshot_path("restore");
        let result = create_private_file(&path, bytes)
            .and_then(|_| self.connection.restore(&path).map_err(StoreError::from));
        let _ = fs::remove_file(&path);
        result.and_then(|_| {
            self.adopt_memory_catalog_if_present()?;
            if self.memory_database_name.borrow().is_some() {
                self.initialize_memory_catalog()?;
                self.refresh_memory_database_name()?;
            }
            Ok(())
        })
    }

    fn from_connection(
        connection: Connection,
        database_path: Option<PathBuf>,
    ) -> Result<Self, StoreError> {
        connection.execute_batch(
            "PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;",
        )?;
        let database_root = database_path.as_deref().map(database_root_for_path);
        let in_memory = database_path.is_none();
        let store = Self {
            connection: ConnectionSlot::new(connection),
            default_database_path: RefCell::new(database_path.clone()),
            database_path: RefCell::new(database_path),
            database_root,
            memory_database_name: RefCell::new(in_memory.then(|| "memory".to_owned())),
        };
        store.migrate()?;
        if store.memory_database_name.borrow().is_some() {
            store.initialize_memory_catalog()?;
        } else {
            store.adopt_memory_catalog_if_present()?;
        }
        Ok(store)
    }

    fn migrate(&self) -> Result<(), StoreError> {
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS facts (
                id INTEGER PRIMARY KEY,
                text TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                project TEXT NOT NULL DEFAULT '',
                domain TEXT NOT NULL DEFAULT '',
                trust TEXT NOT NULL DEFAULT 'medium',
                strong INTEGER NOT NULL DEFAULT 0,
                importance REAL NOT NULL DEFAULT 0.5,
                lifecycle TEXT NOT NULL DEFAULT 'active',
                invalid_at TEXT NOT NULL DEFAULT '',
                superseded_by INTEGER,
                confirmed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                archived INTEGER NOT NULL DEFAULT 0,
                revival_count INTEGER NOT NULL DEFAULT 0,
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sha256, workspace_id)
            );
            CREATE TABLE IF NOT EXISTS contexts (
                ref TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                content TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                \"schema\" TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at TEXT,
                byte_size INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS workspaces (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'archived', 'reset')),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS memory_database_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                name TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS memory_database_catalog (
                name TEXT PRIMARY KEY,
                archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
                snapshot BLOB NOT NULL
            );",
        )?;

        self.ensure_fact_columns()?;
        self.ensure_context_columns()?;
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS context_lineage (
                parent_ref TEXT NOT NULL,
                child_ref TEXT NOT NULL,
                relation TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (parent_ref, child_ref, relation),
                FOREIGN KEY (parent_ref) REFERENCES contexts(ref) ON DELETE CASCADE,
                FOREIGN KEY (child_ref) REFERENCES contexts(ref) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS context_lineage_parent_idx
                ON context_lineage (workspace_id, parent_ref);
            CREATE INDEX IF NOT EXISTS context_lineage_child_idx
                ON context_lineage (workspace_id, child_ref);",
        )?;
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS lifecycle_events (
                id INTEGER PRIMARY KEY,
                idempotency_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                context_ref TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}',
                payload_sha256 TEXT NOT NULL,
                payload_size INTEGER NOT NULL DEFAULT 0,
                payload_truncated INTEGER NOT NULL DEFAULT 0,
                workspace_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (workspace_id, idempotency_key),
                UNIQUE (workspace_id, context_ref),
                FOREIGN KEY (context_ref) REFERENCES contexts(ref) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS handoffs (
                id INTEGER PRIMARY KEY,
                idempotency_key TEXT NOT NULL,
                context_ref TEXT NOT NULL,
                owner TEXT NOT NULL,
                session TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                workspace_id TEXT NOT NULL,
                shared INTEGER NOT NULL DEFAULT 0 CHECK (shared IN (0, 1)),
                expires_at TEXT,
                state TEXT NOT NULL DEFAULT 'open'
                    CHECK (state IN ('open', 'accepted', 'cancelled', 'expired')),
                accepted_at TEXT,
                accepted_by TEXT,
                cancelled_at TEXT,
                cancelled_by TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (workspace_id, idempotency_key),
                UNIQUE (workspace_id, context_ref),
                FOREIGN KEY (context_ref) REFERENCES contexts(ref) ON DELETE CASCADE
            );",
        )?;
        self.ensure_event_columns()?;
        self.ensure_handoff_columns()?;
        self.connection.execute_batch(
            "CREATE UNIQUE INDEX IF NOT EXISTS lifecycle_events_workspace_key_idx
                ON lifecycle_events (workspace_id, idempotency_key);
             CREATE UNIQUE INDEX IF NOT EXISTS lifecycle_events_workspace_context_idx
                ON lifecycle_events (workspace_id, context_ref);
             CREATE UNIQUE INDEX IF NOT EXISTS handoffs_workspace_key_idx
                ON handoffs (workspace_id, idempotency_key);
             CREATE UNIQUE INDEX IF NOT EXISTS handoffs_workspace_context_idx
                ON handoffs (workspace_id, context_ref);
             CREATE INDEX IF NOT EXISTS handoffs_state_idx
                ON handoffs (workspace_id, state);",
        )?;
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                canonical_name TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT '',
                aliases TEXT NOT NULL DEFAULT '[]',
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (name, workspace_id)
            );
            CREATE TABLE IF NOT EXISTS relations (
                id INTEGER PRIMARY KEY,
                subject_id INTEGER NOT NULL,
                predicate TEXT NOT NULL,
                object_id INTEGER NOT NULL,
                source_fact_id INTEGER,
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (workspace_id, subject_id, predicate, object_id),
                FOREIGN KEY (subject_id) REFERENCES entities(id) ON DELETE CASCADE,
                FOREIGN KEY (object_id) REFERENCES entities(id) ON DELETE CASCADE,
                FOREIGN KEY (source_fact_id) REFERENCES facts(id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL DEFAULT '',
                subject TEXT NOT NULL,
                scenario TEXT NOT NULL,
                reasoning TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL,
                confidence REAL,
                decision_maker TEXT NOT NULL DEFAULT '',
                issue_ref TEXT NOT NULL DEFAULT '',
                path TEXT NOT NULL DEFAULT '',
                symbol TEXT NOT NULL DEFAULT '',
                parent_id INTEGER,
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_id) REFERENCES decisions(id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS evidence (
                id INTEGER PRIMARY KEY,
                fact_id INTEGER NOT NULL,
                source_ref TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                checksum TEXT NOT NULL DEFAULT '',
                fetched_at TEXT,
                repository_ref TEXT NOT NULL DEFAULT '',
                path TEXT NOT NULL DEFAULT '',
                symbol TEXT NOT NULL DEFAULT '',
                line_start INTEGER,
                line_end INTEGER,
                column_start INTEGER,
                column_end INTEGER,
                selected_text_sha256 TEXT NOT NULL DEFAULT '',
                resolution_status TEXT NOT NULL DEFAULT 'unresolved',
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (workspace_id, fact_id, source_ref),
                FOREIGN KEY (fact_id) REFERENCES facts(id) ON DELETE CASCADE
            );
            ",
        )?;
        self.ensure_entity_columns()?;
        self.ensure_relation_columns()?;
        self.ensure_decision_columns()?;
        self.ensure_evidence_columns()?;
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (name, workspace_id)
            );
            CREATE TABLE IF NOT EXISTS fact_history (
                id INTEGER PRIMARY KEY,
                fact_id INTEGER NOT NULL,
                event TEXT NOT NULL,
                from_lifecycle TEXT NOT NULL DEFAULT '',
                to_lifecycle TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fact_id) REFERENCES facts(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS fact_embeddings (
                fact_id INTEGER PRIMARY KEY,
                vector BLOB,
                model TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fact_id) REFERENCES facts(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS decision_embeddings (
                decision_id INTEGER PRIMARY KEY,
                vector BLOB,
                model TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (decision_id) REFERENCES decisions(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                issue_ref TEXT NOT NULL DEFAULT '',
                pr_ref TEXT NOT NULL DEFAULT '',
                session TEXT NOT NULL DEFAULT '',
                git_ref TEXT NOT NULL DEFAULT '',
                files TEXT NOT NULL DEFAULT '',
                diff TEXT NOT NULL DEFAULT '',
                summary TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'open'
                    CHECK (state IN ('open', 'closed')),
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ended_at TEXT,
                UNIQUE (workspace_id, run_id)
            );
            CREATE TABLE IF NOT EXISTS measurement_observations (
                id INTEGER PRIMARY KEY,
                measurement TEXT NOT NULL,
                sample TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT '',
                value REAL NOT NULL,
                baseline INTEGER NOT NULL DEFAULT 0 CHECK (baseline IN (0, 1)),
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (workspace_id, measurement, sample, variant)
            );
            CREATE TABLE IF NOT EXISTS memory_feedback (
                id INTEGER PRIMARY KEY,
                feedback_id TEXT NOT NULL,
                site TEXT NOT NULL DEFAULT '',
                item_type TEXT NOT NULL DEFAULT '',
                item_ref TEXT NOT NULL DEFAULT '',
                signal TEXT NOT NULL,
                query_hash TEXT NOT NULL DEFAULT '',
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (workspace_id, feedback_id)
            );",
        )?;
        self.ensure_category_columns()?;
        self.ensure_fact_history_columns()?;
        self.ensure_run_columns()?;
        self.ensure_measurement_columns()?;
        self.ensure_feedback_columns()?;
        self.connection.execute_batch(
            "CREATE INDEX IF NOT EXISTS entities_canonical_idx
                ON entities (workspace_id, canonical_name);
             CREATE INDEX IF NOT EXISTS relations_subject_idx
                ON relations (workspace_id, subject_id);
             CREATE INDEX IF NOT EXISTS relations_object_idx
                ON relations (workspace_id, object_id);
             CREATE INDEX IF NOT EXISTS decisions_subject_idx
                ON decisions (workspace_id, subject);
             CREATE INDEX IF NOT EXISTS evidence_fact_idx
                ON evidence (workspace_id, fact_id);
             CREATE INDEX IF NOT EXISTS categories_workspace_idx
                ON categories (workspace_id, name);
             CREATE INDEX IF NOT EXISTS fact_history_fact_idx
                ON fact_history (workspace_id, fact_id, id);
             CREATE INDEX IF NOT EXISTS runs_workspace_state_idx
                ON runs (workspace_id, state, id);
             CREATE INDEX IF NOT EXISTS measurements_workspace_idx
                ON measurement_observations (workspace_id, measurement, sample);
             CREATE INDEX IF NOT EXISTS feedback_workspace_item_idx
                ON memory_feedback (workspace_id, item_type, item_ref);",
        )?;
        self.connection.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts
                USING fts5(category, scenario, reasoning,
                           content='decisions', content_rowid='id');
            CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
                INSERT INTO decisions_fts(rowid, category, scenario, reasoning)
                VALUES (new.id, new.category, new.scenario, new.reasoning);
            END;
            CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
                INSERT INTO decisions_fts(decisions_fts, rowid, category, scenario, reasoning)
                VALUES ('delete', old.id, old.category, old.scenario, old.reasoning);
            END;
            CREATE TRIGGER IF NOT EXISTS decisions_au
                AFTER UPDATE OF category, scenario, reasoning ON decisions BEGIN
                INSERT INTO decisions_fts(decisions_fts, rowid, category, scenario, reasoning)
                VALUES ('delete', old.id, old.category, old.scenario, old.reasoning);
                INSERT INTO decisions_fts(rowid, category, scenario, reasoning)
                VALUES (new.id, new.category, new.scenario, new.reasoning);
            END;
            INSERT INTO decisions_fts(decisions_fts) VALUES ('rebuild');",
        )?;
        self.connection.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts
                USING fts5(text, content='facts', content_rowid='id');
             CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN
                INSERT INTO facts_fts(rowid, text) VALUES (new.id, new.text);
             END;
             CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN
                INSERT INTO facts_fts(facts_fts, rowid, text) VALUES ('delete', old.id, old.text);
             END;
             CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE OF text ON facts BEGIN
                INSERT INTO facts_fts(facts_fts, rowid, text) VALUES ('delete', old.id, old.text);
                INSERT INTO facts_fts(rowid, text) VALUES (new.id, new.text);
             END;
             INSERT INTO facts_fts(facts_fts) VALUES ('rebuild');",
        )?;
        Ok(())
    }

    fn initialize_memory_catalog(&self) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT OR IGNORE INTO memory_database_state (id, name)
             VALUES (1, 'memory')",
            [],
        )?;
        Ok(())
    }

    fn refresh_memory_database_name(&self) -> Result<(), StoreError> {
        let name = self
            .connection
            .query_row(
                "SELECT name FROM memory_database_state WHERE id = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .unwrap_or_else(|| "memory".to_owned());
        validate_database_name(&name)?;
        *self.memory_database_name.borrow_mut() = Some(name);
        Ok(())
    }

    fn adopt_memory_catalog_if_present(&self) -> Result<(), StoreError> {
        if self.memory_database_name.borrow().is_some() {
            return Ok(());
        }
        let Some(name) = self
            .connection
            .query_row(
                "SELECT name FROM memory_database_state WHERE id = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        else {
            return Ok(());
        };
        validate_database_name(&name)?;
        *self.memory_database_name.borrow_mut() = Some(name);
        Ok(())
    }

    fn ensure_fact_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(facts)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("source", "TEXT NOT NULL DEFAULT ''"),
            ("project", "TEXT NOT NULL DEFAULT ''"),
            ("domain", "TEXT NOT NULL DEFAULT ''"),
            ("trust", "TEXT NOT NULL DEFAULT 'medium'"),
            ("strong", "INTEGER NOT NULL DEFAULT 0"),
            ("importance", "REAL NOT NULL DEFAULT 0.5"),
            ("category_id", "INTEGER"),
            ("validity", "TEXT NOT NULL DEFAULT 'valid'"),
            ("session_id", "TEXT NOT NULL DEFAULT ''"),
            ("access_count", "INTEGER NOT NULL DEFAULT 0"),
            ("last_accessed_at", "TEXT"),
            ("invalid_at", "TEXT NOT NULL DEFAULT ''"),
            ("superseded_by", "INTEGER"),
            ("confirmed", "INTEGER NOT NULL DEFAULT 0"),
            ("updated_at", "TEXT NOT NULL DEFAULT ''"),
            ("archived", "INTEGER NOT NULL DEFAULT 0"),
            ("revival_count", "INTEGER NOT NULL DEFAULT 0"),
            ("lifecycle", "TEXT NOT NULL DEFAULT 'active'"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            // SQLite requires a constant default for ALTER TABLE ADD COLUMN.
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE facts ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_context_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(contexts)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("schema", "TEXT NOT NULL DEFAULT ''"),
            ("source", "TEXT NOT NULL DEFAULT ''"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            // SQLite requires a constant default for ALTER TABLE ADD COLUMN.
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("expires_at", "TEXT"),
            ("byte_size", "INTEGER NOT NULL DEFAULT 0"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                let column_name = if name == "schema" { "\"schema\"" } else { name };
                self.connection.execute(
                    &format!("ALTER TABLE contexts ADD COLUMN {column_name} {definition}"),
                    [],
                )?;
            }
        }
        self.connection.execute(
            "UPDATE contexts
             SET byte_size = length(CAST(content AS BLOB))
             WHERE byte_size = 0 AND content <> ''",
            [],
        )?;
        Ok(())
    }

    fn ensure_event_columns(&self) -> Result<(), StoreError> {
        let mut statement = self
            .connection
            .prepare("PRAGMA table_info(lifecycle_events)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("idempotency_key", "TEXT NOT NULL DEFAULT ''"),
            ("event_type", "TEXT NOT NULL DEFAULT ''"),
            ("context_ref", "TEXT NOT NULL DEFAULT ''"),
            ("metadata", "TEXT NOT NULL DEFAULT '{}'"),
            ("payload_sha256", "TEXT NOT NULL DEFAULT ''"),
            ("payload_size", "INTEGER NOT NULL DEFAULT 0"),
            ("payload_truncated", "INTEGER NOT NULL DEFAULT 0"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE lifecycle_events ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_handoff_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(handoffs)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("idempotency_key", "TEXT NOT NULL DEFAULT ''"),
            ("context_ref", "TEXT NOT NULL DEFAULT ''"),
            ("owner", "TEXT NOT NULL DEFAULT ''"),
            ("session", "TEXT NOT NULL DEFAULT ''"),
            ("source", "TEXT NOT NULL DEFAULT ''"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("shared", "INTEGER NOT NULL DEFAULT 0"),
            ("expires_at", "TEXT"),
            ("state", "TEXT NOT NULL DEFAULT 'open'"),
            ("accepted_at", "TEXT"),
            ("accepted_by", "TEXT"),
            ("cancelled_at", "TEXT"),
            ("cancelled_by", "TEXT"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE handoffs ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_entity_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(entities)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("name", "TEXT NOT NULL DEFAULT ''"),
            ("canonical_name", "TEXT NOT NULL DEFAULT ''"),
            ("entity_type", "TEXT NOT NULL DEFAULT ''"),
            ("aliases", "TEXT NOT NULL DEFAULT '[]'"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE entities ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        self.connection.execute(
            "UPDATE entities
             SET canonical_name = lower(trim(name))
             WHERE canonical_name = '' AND name <> ''",
            [],
        )?;
        Ok(())
    }

    fn ensure_relation_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(relations)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("subject_id", "INTEGER NOT NULL DEFAULT 0"),
            ("predicate", "TEXT NOT NULL DEFAULT ''"),
            ("object_id", "INTEGER NOT NULL DEFAULT 0"),
            ("source_fact_id", "INTEGER"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE relations ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_decision_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(decisions)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("category", "TEXT NOT NULL DEFAULT ''"),
            ("subject", "TEXT NOT NULL DEFAULT ''"),
            ("scenario", "TEXT NOT NULL DEFAULT ''"),
            ("reasoning", "TEXT NOT NULL DEFAULT ''"),
            ("outcome", "TEXT NOT NULL DEFAULT ''"),
            ("confidence", "REAL"),
            ("decision_maker", "TEXT NOT NULL DEFAULT ''"),
            ("issue_ref", "TEXT NOT NULL DEFAULT ''"),
            ("path", "TEXT NOT NULL DEFAULT ''"),
            ("symbol", "TEXT NOT NULL DEFAULT ''"),
            ("parent_id", "INTEGER"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE decisions ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_evidence_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(evidence)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("fact_id", "INTEGER NOT NULL DEFAULT 0"),
            ("source_ref", "TEXT NOT NULL DEFAULT ''"),
            ("source", "TEXT NOT NULL DEFAULT ''"),
            ("checksum", "TEXT NOT NULL DEFAULT ''"),
            ("fetched_at", "TEXT"),
            ("repository_ref", "TEXT NOT NULL DEFAULT ''"),
            ("path", "TEXT NOT NULL DEFAULT ''"),
            ("symbol", "TEXT NOT NULL DEFAULT ''"),
            ("line_start", "INTEGER"),
            ("line_end", "INTEGER"),
            ("column_start", "INTEGER"),
            ("column_end", "INTEGER"),
            ("selected_text_sha256", "TEXT NOT NULL DEFAULT ''"),
            ("resolution_status", "TEXT NOT NULL DEFAULT 'unresolved'"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE evidence ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_category_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(categories)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("name", "TEXT NOT NULL DEFAULT ''"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE categories ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_fact_history_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(fact_history)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("fact_id", "INTEGER NOT NULL DEFAULT 0"),
            ("event", "TEXT NOT NULL DEFAULT ''"),
            ("from_lifecycle", "TEXT NOT NULL DEFAULT ''"),
            ("to_lifecycle", "TEXT NOT NULL DEFAULT ''"),
            ("note", "TEXT NOT NULL DEFAULT ''"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE fact_history ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_run_columns(&self) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(runs)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("run_id", "TEXT NOT NULL DEFAULT ''"),
            ("issue_ref", "TEXT NOT NULL DEFAULT ''"),
            ("pr_ref", "TEXT NOT NULL DEFAULT ''"),
            ("session", "TEXT NOT NULL DEFAULT ''"),
            ("git_ref", "TEXT NOT NULL DEFAULT ''"),
            ("files", "TEXT NOT NULL DEFAULT ''"),
            ("diff", "TEXT NOT NULL DEFAULT ''"),
            ("summary", "TEXT NOT NULL DEFAULT ''"),
            ("state", "TEXT NOT NULL DEFAULT 'open'"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("ended_at", "TEXT"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE runs ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_measurement_columns(&self) -> Result<(), StoreError> {
        let mut statement = self
            .connection
            .prepare("PRAGMA table_info(measurement_observations)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("measurement", "TEXT NOT NULL DEFAULT ''"),
            ("sample", "TEXT NOT NULL DEFAULT ''"),
            ("variant", "TEXT NOT NULL DEFAULT ''"),
            ("value", "REAL NOT NULL DEFAULT 0"),
            ("baseline", "INTEGER NOT NULL DEFAULT 0"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE measurement_observations ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_feedback_columns(&self) -> Result<(), StoreError> {
        let mut statement = self
            .connection
            .prepare("PRAGMA table_info(memory_feedback)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        let additions = [
            ("feedback_id", "TEXT NOT NULL DEFAULT ''"),
            ("site", "TEXT NOT NULL DEFAULT ''"),
            ("item_type", "TEXT NOT NULL DEFAULT ''"),
            ("item_ref", "TEXT NOT NULL DEFAULT ''"),
            ("signal", "TEXT NOT NULL DEFAULT ''"),
            ("query_hash", "TEXT NOT NULL DEFAULT ''"),
            ("workspace_id", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ];
        for (name, definition) in additions {
            if !columns.iter().any(|column| column == name) {
                self.connection.execute(
                    &format!("ALTER TABLE memory_feedback ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    pub fn remember_fact(&self, text: &str, workspace: &str) -> Result<Fact, StoreError> {
        self.remember_fact_with_metadata(text, workspace, &FactMetadata::default())
    }

    pub fn remember_fact_with_metadata(
        &self,
        text: &str,
        workspace: &str,
        metadata: &FactMetadata,
    ) -> Result<Fact, StoreError> {
        validate_fact_text(text)?;
        validate_fact_metadata(metadata)?;
        let sha256 = sha256(text);
        let was_present = self.fact_by_hash(&sha256, workspace)?.is_some();
        self.connection.execute(
            "INSERT OR IGNORE INTO facts
                (text, sha256, source, project, domain, trust, strong, importance, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                text,
                sha256,
                metadata.source.as_str(),
                metadata.project.as_str(),
                metadata.domain.as_str(),
                metadata.trust.as_str(),
                metadata.strong as i64,
                metadata.importance,
                workspace
            ],
        )?;
        let fact = self.fact_by_hash(&sha256, workspace)?.ok_or_else(|| {
            StoreError::Invalid("fact insert did not produce a readable row".to_owned())
        })?;
        if !was_present {
            self.record_fact_history(
                fact.id,
                "created",
                "",
                &fact.lifecycle,
                "fact inserted",
                workspace,
            )?;
        }
        Ok(fact)
    }

    pub fn fact_exists(&self, text: &str, workspace: &str) -> Result<bool, StoreError> {
        let hash = sha256(text);
        self.fact_by_hash(&hash, workspace)
            .map(|fact| fact.is_some())
    }

    /// Resolve the durable id of an existing fact without exposing the
    /// private lookup implementation to the pipeline/protocol adapters.
    /// Hash lookup is the same identity rule used by `remember_fact`, so
    /// absorb previews and commits cannot report a made-up duplicate id.
    pub fn fact_id_for_text(&self, text: &str, workspace: &str) -> Result<Option<i64>, StoreError> {
        let hash = sha256(text);
        Ok(self.fact_by_hash(&hash, workspace)?.map(|fact| fact.id))
    }

    pub fn absorb(&self, texts: &[String], workspace: &str) -> Result<Vec<Fact>, StoreError> {
        texts
            .iter()
            .map(|text| self.remember_fact(text, workspace))
            .collect()
    }

    pub fn ingest_turn(&self, text: &str, workspace: &str) -> Result<Fact, StoreError> {
        self.remember_fact(text, workspace)
    }

    pub fn create_category(&self, name: &str, workspace: &str) -> Result<Category, StoreError> {
        validate_graph_workspace(workspace)?;
        if name.trim().is_empty() {
            return Err(StoreError::Invalid(
                "category name must not be empty".to_owned(),
            ));
        }
        self.connection.execute(
            "INSERT OR IGNORE INTO categories (name, workspace_id) VALUES (?1, ?2)",
            params![name, workspace],
        )?;
        self.category_by_name(name, workspace)?.ok_or_else(|| {
            StoreError::Invalid("category insert did not produce a readable row".to_owned())
        })
    }

    pub fn list_categories(&self, workspace: &str) -> Result<Vec<Category>, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, name, workspace_id, created_at
             FROM categories
             WHERE workspace_id = ?1
             ORDER BY name, id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_category)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn categorize_pending(
        &self,
        category: &str,
        query: &str,
        workspace: &str,
        limit: usize,
    ) -> Result<Vec<Fact>, StoreError> {
        validate_graph_workspace(workspace)?;
        if category.trim().is_empty() {
            return Err(StoreError::Invalid(
                "categorize_pending requires a category".to_owned(),
            ));
        }
        if limit == 0 {
            return Err(StoreError::Invalid(
                "categorize_pending limit must be positive".to_owned(),
            ));
        }
        let category = self.create_category(category, workspace)?;
        let ids = {
            let mut statement = self.connection.prepare(
                "SELECT id
                 FROM facts
                 WHERE category_id IS NULL
                   AND (workspace_id = '' OR workspace_id = ?1)
                   AND lifecycle != 'forgotten'
                   AND (?2 = '' OR instr(lower(text), lower(?2)) > 0)
                 ORDER BY id
                 LIMIT ?3",
            )?;
            let rows = statement
                .query_map(params![workspace, query, limit as i64], |row| {
                    row.get::<_, i64>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let mut facts = Vec::with_capacity(ids.len());
        for id in ids {
            self.connection.execute(
                "UPDATE facts SET category_id = ?1 WHERE id = ?2 AND category_id IS NULL",
                params![category.id, id],
            )?;
            if let Some(fact) = self.fact_by_id(id, workspace)? {
                facts.push(fact);
            }
        }
        Ok(facts)
    }

    pub fn begin_run(&self, spec: &RunSpec) -> Result<Run, StoreError> {
        validate_run_spec(spec)?;
        let files = truncate_utf8(&spec.files, MAX_RUN_FILES_BYTES);
        let diff = truncate_utf8(&spec.diff, MAX_RUN_DIFF_BYTES);
        if let Some(existing) = self.run_by_key(&spec.run_id, &spec.workspace)? {
            if existing.issue_ref != spec.issue_ref
                || existing.pr_ref != spec.pr_ref
                || existing.session != spec.session
                || existing.git_ref != spec.git_ref
                || existing.files != files
                || existing.diff != diff
            {
                return Err(StoreError::Invalid(
                    "run id conflicts with an existing record".to_owned(),
                ));
            }
            return Ok(existing);
        }
        self.connection.execute(
            "INSERT INTO runs
                (run_id, issue_ref, pr_ref, session, git_ref, files, diff, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                spec.run_id,
                spec.issue_ref,
                spec.pr_ref,
                spec.session,
                spec.git_ref,
                files,
                diff,
                spec.workspace
            ],
        )?;
        self.run_by_key(&spec.run_id, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid("run insert did not produce a readable row".to_owned())
            })
    }

    pub fn end_run(
        &self,
        run_id: &str,
        summary: &str,
        workspace: &str,
    ) -> Result<Option<Run>, StoreError> {
        validate_run_key(run_id, workspace)?;
        let Some(existing) = self.run_by_key(run_id, workspace)? else {
            return Ok(None);
        };
        if existing.state == "closed" {
            if !summary.is_empty() && existing.summary != summary {
                return Err(StoreError::Invalid(
                    "closed run summary conflicts with an existing record".to_owned(),
                ));
            }
            return Ok(Some(existing));
        }
        let summary = truncate_utf8(summary, MAX_RUN_DIFF_BYTES);
        self.connection.execute(
            "UPDATE runs
             SET state = 'closed', summary = ?1, ended_at = CURRENT_TIMESTAMP
             WHERE run_id = ?2 AND workspace_id = ?3",
            params![summary, run_id, workspace],
        )?;
        self.run_by_key(run_id, workspace)
    }

    pub fn link_run(
        &self,
        run_id: &str,
        issue_ref: Option<&str>,
        pr_ref: Option<&str>,
        session: Option<&str>,
        git_ref: Option<&str>,
        workspace: &str,
    ) -> Result<Option<Run>, StoreError> {
        validate_run_key(run_id, workspace)?;
        if issue_ref.is_none() && pr_ref.is_none() && session.is_none() && git_ref.is_none() {
            return Err(StoreError::Invalid(
                "link_run requires at least one link field".to_owned(),
            ));
        }
        let Some(_) = self.run_by_key(run_id, workspace)? else {
            return Ok(None);
        };
        self.connection.execute(
            "UPDATE runs
             SET issue_ref = COALESCE(?1, issue_ref),
                 pr_ref = COALESCE(?2, pr_ref),
                 session = COALESCE(?3, session),
                 git_ref = COALESCE(?4, git_ref)
             WHERE run_id = ?5 AND workspace_id = ?6",
            params![issue_ref, pr_ref, session, git_ref, run_id, workspace],
        )?;
        self.run_by_key(run_id, workspace)
    }

    pub fn query_runs(&self, query: &str, workspace: &str) -> Result<Vec<Run>, StoreError> {
        validate_graph_workspace(workspace)?;
        let pattern = format!("%{}%", query.trim());
        let mut statement = self.connection.prepare(
            "SELECT id, run_id, issue_ref, pr_ref, session, git_ref, files, diff,
                    summary, state, workspace_id, created_at, ended_at
             FROM runs
             WHERE workspace_id = ?1
               AND (?2 = '%%'
                    OR run_id LIKE ?2 OR issue_ref LIKE ?2 OR pr_ref LIKE ?2
                    OR session LIKE ?2 OR git_ref LIKE ?2 OR summary LIKE ?2)
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace, pattern], map_run)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn record_measurement(&self, spec: &MeasurementSpec) -> Result<Measurement, StoreError> {
        validate_measurement_spec(spec)?;
        if let Some(existing) = self.measurement_by_key(
            &spec.measurement,
            &spec.sample,
            &spec.variant,
            &spec.workspace,
        )? {
            if existing.value != spec.value || existing.baseline != spec.baseline {
                return Err(StoreError::Invalid(
                    "measurement key conflicts with an existing observation".to_owned(),
                ));
            }
            return Ok(existing);
        }
        self.connection.execute(
            "INSERT INTO measurement_observations
                (measurement, sample, variant, value, baseline, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                spec.measurement,
                spec.sample,
                spec.variant,
                spec.value,
                spec.baseline as i64,
                spec.workspace
            ],
        )?;
        self.measurement_by_key(
            &spec.measurement,
            &spec.sample,
            &spec.variant,
            &spec.workspace,
        )?
        .ok_or_else(|| {
            StoreError::Invalid("measurement insert did not produce a readable row".to_owned())
        })
    }

    pub fn query_measurements(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<Vec<Measurement>, StoreError> {
        validate_graph_workspace(workspace)?;
        let pattern = format!("%{}%", query.trim());
        let mut statement = self.connection.prepare(
            "SELECT id, measurement, sample, variant, value, baseline,
                    workspace_id, created_at
             FROM measurement_observations
             WHERE workspace_id = ?1
               AND (?2 = '%%'
                    OR measurement LIKE ?2 OR sample LIKE ?2 OR variant LIKE ?2)
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace, pattern], map_measurement)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn record_feedback(&self, spec: &FeedbackSpec) -> Result<Feedback, StoreError> {
        validate_feedback_spec(spec)?;
        if let Some(existing) = self.feedback_by_key(&spec.feedback_id, &spec.workspace)? {
            if existing.site != spec.site
                || existing.item_type != spec.item_type
                || existing.item_ref != spec.item_ref
                || existing.signal != spec.signal
                || existing.query_hash != spec.query_hash
            {
                return Err(StoreError::Invalid(
                    "feedback id conflicts with an existing record".to_owned(),
                ));
            }
            return Ok(existing);
        }
        self.connection.execute(
            "INSERT INTO memory_feedback
                (feedback_id, site, item_type, item_ref, signal, query_hash, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                spec.feedback_id,
                spec.site,
                spec.item_type,
                spec.item_ref,
                spec.signal,
                spec.query_hash,
                spec.workspace
            ],
        )?;
        self.feedback_by_key(&spec.feedback_id, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid("feedback insert did not produce a readable row".to_owned())
            })
    }

    pub fn query_feedback(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<Vec<Feedback>, StoreError> {
        validate_graph_workspace(workspace)?;
        let pattern = format!("%{}%", query.trim());
        let mut statement = self.connection.prepare(
            "SELECT id, feedback_id, site, item_type, item_ref, signal,
                    query_hash, workspace_id, created_at
             FROM memory_feedback
             WHERE workspace_id = ?1
               AND (?2 = '%%'
                    OR feedback_id LIKE ?2 OR site LIKE ?2 OR item_type LIKE ?2
                    OR item_ref LIKE ?2 OR signal LIKE ?2 OR query_hash LIKE ?2)
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace, pattern], map_feedback)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn review_pending(&self, workspace: &str) -> Result<Vec<Fact>, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, text, sha256, workspace_id, lifecycle,
                    source, project, domain, trust, strong, importance, category_id,
                    validity, session_id, access_count
             FROM facts
             WHERE (workspace_id = '' OR workspace_id = ?1)
               AND lifecycle != 'forgotten'
               AND (validity != 'valid' OR lifecycle = 'degraded')
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_fact)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn set_fact_validity(
        &self,
        id: i64,
        validity: &str,
        workspace: &str,
    ) -> Result<Option<Fact>, StoreError> {
        validate_graph_workspace(workspace)?;
        if id <= 0 {
            return Err(StoreError::Invalid("fact id must be positive".to_owned()));
        }
        if !matches!(validity, "valid" | "pending" | "invalid") {
            return Err(StoreError::Invalid(
                "fact validity must be valid, pending, or invalid".to_owned(),
            ));
        }
        let Some(existing) = self.fact_by_id(id, workspace)? else {
            return Ok(None);
        };
        if existing.validity != validity {
            self.connection.execute(
                "UPDATE facts SET validity = ?1 WHERE id = ?2",
                params![validity, id],
            )?;
            self.record_fact_history(
                id,
                "validity_changed",
                &existing.lifecycle,
                &existing.lifecycle,
                validity,
                &existing.workspace,
            )?;
        }
        self.fact_by_id(id, workspace)
    }

    pub fn confirm_fact(
        &self,
        id: i64,
        note: &str,
        workspace: &str,
    ) -> Result<Option<Fact>, StoreError> {
        validate_graph_workspace(workspace)?;
        if id <= 0 {
            return Err(StoreError::Invalid("fact id must be positive".to_owned()));
        }
        let Some(existing) = self.fact_by_id(id, workspace)? else {
            return Ok(None);
        };
        self.connection.execute(
            "UPDATE facts
             SET validity = 'valid', lifecycle = 'active', confirmed = 1,
                 trust = 'high', updated_at = CURRENT_TIMESTAMP
             WHERE id = ?1",
            params![id],
        )?;
        if existing.validity != "valid"
            || existing.lifecycle != "active"
            || existing.trust != "high"
        {
            self.record_fact_history(
                id,
                "confirmed",
                &existing.lifecycle,
                "active",
                note,
                &existing.workspace,
            )?;
        }
        self.fact_by_id(id, workspace)
    }

    /// Return the SQLite timestamp used by context and handoff TTLs.  Keeping
    /// the calculation in SQLite avoids a second clock format implementation
    /// in the protocol layer and matches the store's validation rules.
    pub fn expiry_after_seconds(&self, seconds: i64) -> Result<Option<String>, StoreError> {
        if seconds < 0 {
            return Err(StoreError::Invalid(
                "ttl_seconds must not be negative".to_owned(),
            ));
        }
        self.connection
            .query_row(
                "SELECT datetime('now', ?1)",
                params![format!("+{seconds} seconds")],
                |row| row.get(0),
            )
            .map(Some)
            .map_err(StoreError::from)
    }

    pub fn sweep_freshness(
        &self,
        max_age_seconds: i64,
        workspace: &str,
    ) -> Result<Vec<Fact>, StoreError> {
        validate_graph_workspace(workspace)?;
        if max_age_seconds < 0 {
            return Err(StoreError::Invalid(
                "freshness max_age_seconds must not be negative".to_owned(),
            ));
        }
        let ids = {
            let mut statement = self.connection.prepare(
                "SELECT id
                 FROM facts
                 WHERE (workspace_id = '' OR workspace_id = ?1)
                   AND lifecycle = 'active'
                   AND validity = 'valid'
                   AND created_at <> ''
                   AND julianday(created_at) < julianday('now') - (?2 / 86400.0)
                 ORDER BY id",
            )?;
            let rows = statement
                .query_map(params![workspace, max_age_seconds], |row| {
                    row.get::<_, i64>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let mut degraded = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(fact) = self.update_fact_lifecycle(id, workspace, "degraded")? {
                degraded.push(fact);
            }
        }
        Ok(degraded)
    }

    pub fn decay_sweep(
        &self,
        max_age_seconds: i64,
        workspace: &str,
    ) -> Result<Vec<Fact>, StoreError> {
        self.sweep_freshness(max_age_seconds, workspace)
    }

    pub fn embed_backfill(&self, workspace: &str) -> Result<EmbeddingBackfill, StoreError> {
        validate_graph_workspace(workspace)?;
        Ok(EmbeddingBackfill {
            status: "disabled".to_owned(),
            updated: 0,
            reason: "no embedding provider is configured; SQLite lexical retrieval remains active"
                .to_owned(),
        })
    }

    /// Store one normalized provider vector after the fact transaction has
    /// committed.  Embedding failures therefore never roll back or partially
    /// commit the fact itself.
    pub fn upsert_fact_embedding(
        &self,
        fact_id: i64,
        vector: &[f32],
        model: &str,
        workspace: &str,
    ) -> Result<(), StoreError> {
        validate_graph_workspace(workspace)?;
        if vector.is_empty() {
            return Err(StoreError::Invalid(
                "fact embedding vector must not be empty".to_owned(),
            ));
        }
        if model.trim().is_empty() {
            return Err(StoreError::Invalid(
                "fact embedding model must not be empty".to_owned(),
            ));
        }
        if self.fact_by_id(fact_id, workspace)?.is_none() {
            return Err(StoreError::Invalid(format!("fact not found: {fact_id}")));
        }
        let bytes = vector
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        self.connection.execute(
            "INSERT INTO fact_embeddings (fact_id, vector, model)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(fact_id) DO UPDATE SET vector=excluded.vector, model=excluded.model,
                 created_at=CURRENT_TIMESTAMP",
            params![fact_id, bytes, model],
        )?;
        Ok(())
    }

    /// Return facts without a vector, bounded to the provider batch size.
    pub fn missing_fact_texts(
        &self,
        workspace: &str,
        limit: usize,
    ) -> Result<Vec<(i64, String)>, StoreError> {
        validate_graph_workspace(workspace)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            "SELECT f.id, f.text
             FROM facts f
             WHERE f.lifecycle != 'forgotten'
               AND f.validity != 'invalid'
               AND (f.workspace_id = '' OR f.workspace_id = ?1)
               AND NOT EXISTS (SELECT 1 FROM fact_embeddings e WHERE e.fact_id = f.id)
             ORDER BY f.id
             LIMIT ?2",
        )?;
        let result = statement
            .query_map(params![workspace, limit as i64], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from);
        result
    }

    /// Read the current vector set without exposing SQLite internals to the
    /// provider or protocol modules.
    pub fn fact_embeddings(&self, workspace: &str) -> Result<Vec<FactEmbedding>, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT e.fact_id, e.vector, e.model
             FROM fact_embeddings e
             JOIN facts f ON f.id = e.fact_id
             WHERE (f.workspace_id = '' OR f.workspace_id = ?1)
             ORDER BY e.fact_id",
        )?;
        let rows = statement
            .query_map(params![workspace], |row| {
                let fact_id = row.get::<_, i64>(0)?;
                let bytes = row.get::<_, Vec<u8>>(1)?;
                let model = row.get::<_, String>(2)?;
                Ok((fact_id, bytes, model))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut embeddings = Vec::with_capacity(rows.len());
        for (fact_id, bytes, model) in rows {
            let Some(fact) = self.fact_by_id(fact_id, workspace)? else {
                continue;
            };
            if bytes.len() % std::mem::size_of::<f32>() != 0 {
                return Err(StoreError::Invalid(format!(
                    "fact embedding {fact_id} has an invalid byte length"
                )));
            }
            let vector = bytes
                .chunks_exact(std::mem::size_of::<f32>())
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect::<Vec<_>>();
            embeddings.push(FactEmbedding {
                fact,
                vector,
                model,
            });
        }
        Ok(embeddings)
    }

    /// Assign an idempotent category to a fact.  Rules and the LLM pipeline
    /// both use this seam, keeping category writes in the active backend.
    pub fn set_fact_category(
        &self,
        fact_id: i64,
        category: &str,
        workspace: &str,
    ) -> Result<Option<Fact>, StoreError> {
        validate_graph_workspace(workspace)?;
        let category = category.trim();
        if category.is_empty() {
            return self.fact_by_id(fact_id, workspace);
        }
        let category_row = self.create_category(category, workspace)?;
        if self.fact_by_id(fact_id, workspace)?.is_none() {
            return Ok(None);
        }
        self.connection.execute(
            "UPDATE facts SET category_id = ?1, updated_at = CURRENT_TIMESTAMP WHERE id = ?2",
            params![category_row.id, fact_id],
        )?;
        self.fact_by_id(fact_id, workspace)
    }

    /// Mark an older fact invalid while retaining its row and history.  This
    /// is the Rust equivalent of Python's bi-temporal supersession hook.
    pub fn invalidate_fact(
        &self,
        old_id: i64,
        new_id: i64,
        workspace: &str,
        reason: &str,
    ) -> Result<bool, StoreError> {
        validate_graph_workspace(workspace)?;
        if old_id <= 0 || new_id <= 0 || old_id == new_id {
            return Ok(false);
        }
        let Some(existing) = self.fact_by_id(old_id, workspace)? else {
            return Ok(false);
        };
        if existing.strong {
            return Ok(false);
        }
        self.connection.execute(
            "UPDATE facts
             SET validity = 'invalid', invalid_at = CURRENT_TIMESTAMP,
                 superseded_by = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ? AND (workspace_id = '' OR workspace_id = ?)",
            params![new_id, old_id, workspace],
        )?;
        self.record_fact_history(
            old_id,
            "invalidated",
            &existing.lifecycle,
            &existing.lifecycle,
            reason,
            &existing.workspace,
        )?;
        Ok(true)
    }

    pub fn fact_by_id_for_pipeline(
        &self,
        id: i64,
        workspace: &str,
    ) -> Result<Option<Fact>, StoreError> {
        self.fact_by_id(id, workspace)
    }

    /// Read a fact by its content hash for protocol/provider adapters without
    /// exposing the store's private query helpers.
    pub fn fact_by_sha256_for_pipeline(
        &self,
        hash: &str,
        workspace: &str,
    ) -> Result<Option<Fact>, StoreError> {
        self.fact_by_hash(hash, workspace)
    }

    pub fn fact_search_metadata(
        &self,
        fact_id: i64,
        workspace: &str,
    ) -> Result<Option<FactSearchMetadata>, StoreError> {
        validate_graph_workspace(workspace)?;
        self.connection
            .query_row(
                "SELECT c.name, f.confirmed, f.invalid_at, f.archived,
                        f.created_at, f.updated_at
                 FROM facts f
                 LEFT JOIN categories c ON c.id = f.category_id
                 WHERE f.id = ?1 AND (f.workspace_id = '' OR f.workspace_id = ?2)",
                params![fact_id, workspace],
                |row| {
                    Ok(FactSearchMetadata {
                        category: row.get(0)?,
                        confirmed: row.get::<_, i64>(1)? != 0,
                        invalid_at: row.get(2)?,
                        archived: row.get::<_, i64>(3)? != 0,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn category_name_for_fact(
        &self,
        fact_id: i64,
        workspace: &str,
    ) -> Result<Option<String>, StoreError> {
        Ok(self
            .fact_search_metadata(fact_id, workspace)?
            .and_then(|metadata| metadata.category))
    }

    pub fn fact_history(&self, id: i64, workspace: &str) -> Result<Vec<FactHistory>, StoreError> {
        validate_graph_workspace(workspace)?;
        if id <= 0 {
            return Err(StoreError::Invalid("fact id must be positive".to_owned()));
        }
        let mut statement = self.connection.prepare(
            "SELECT id, fact_id, event, from_lifecycle, to_lifecycle,
                    note, workspace_id, created_at
             FROM fact_history
             WHERE fact_id = ?1 AND (workspace_id = '' OR workspace_id = ?2)
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![id, workspace], map_fact_history)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn set_fact_session(
        &self,
        id: i64,
        session_id: &str,
        workspace: &str,
    ) -> Result<Option<Fact>, StoreError> {
        validate_graph_workspace(workspace)?;
        if id <= 0 {
            return Err(StoreError::Invalid("fact id must be positive".to_owned()));
        }
        if self.fact_by_id(id, workspace)?.is_none() {
            return Ok(None);
        }
        self.connection.execute(
            "UPDATE facts SET session_id = ?1 WHERE id = ?2",
            params![session_id, id],
        )?;
        self.fact_by_id(id, workspace)
    }

    pub fn facts_for_session(
        &self,
        session_id: &str,
        workspace: &str,
    ) -> Result<Vec<Fact>, StoreError> {
        validate_graph_workspace(workspace)?;
        if session_id.trim().is_empty() {
            return Err(StoreError::Invalid(
                "session_id must not be empty".to_owned(),
            ));
        }
        let mut statement = self.connection.prepare(
            "SELECT id, text, sha256, workspace_id, lifecycle,
                    source, project, domain, trust, strong, importance, category_id,
                    validity, session_id, access_count
             FROM facts
             WHERE session_id = ?1
               AND (workspace_id = '' OR workspace_id = ?2)
               AND lifecycle != 'forgotten'
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![session_id, workspace], map_fact)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn list_sessions(&self, workspace: &str) -> Result<Vec<String>, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT session_id
             FROM facts
             WHERE session_id <> ''
               AND (workspace_id = '' OR workspace_id = ?1)
             ORDER BY session_id",
        )?;
        let rows = statement
            .query_map(params![workspace], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn fact_references(&self, id: i64, workspace: &str) -> Result<Vec<Evidence>, StoreError> {
        self.get_provenance(id, workspace)
    }

    pub fn search_guard(&self, query: &str, workspace: &str) -> Result<RetrievalGuard, StoreError> {
        let recall = self.compose_recall(query, workspace)?;
        let matched = !recall.facts.is_empty() || !recall.contexts.is_empty();
        Ok(RetrievalGuard {
            status: if matched { "ok" } else { "abstain" }.to_owned(),
            reason: if matched { "match" } else { "no_match" }.to_owned(),
            facts: recall.facts,
            contexts: recall.contexts,
        })
    }

    pub fn auto_orient(&self, query: &str, workspace: &str) -> Result<Recall, StoreError> {
        self.compose_recall(query, workspace)
    }

    pub fn summarize_index(&self, workspace: &str) -> Result<IndexSummary, StoreError> {
        validate_graph_workspace(workspace)?;
        let facts = self.connection.query_row(
            "SELECT COUNT(*) FROM facts WHERE workspace_id = '' OR workspace_id = ?1",
            params![workspace],
            |row| row.get(0),
        )?;
        let active_facts = self.connection.query_row(
            "SELECT COUNT(*) FROM facts
             WHERE (workspace_id = '' OR workspace_id = ?1) AND lifecycle = 'active'",
            params![workspace],
            |row| row.get(0),
        )?;
        let forgotten_facts = self.connection.query_row(
            "SELECT COUNT(*) FROM facts
             WHERE (workspace_id = '' OR workspace_id = ?1) AND lifecycle = 'forgotten'",
            params![workspace],
            |row| row.get(0),
        )?;
        let contexts = self.connection.query_row(
            "SELECT COUNT(*) FROM contexts WHERE workspace_id = ?1",
            params![workspace],
            |row| row.get(0),
        )?;
        let categories = self.connection.query_row(
            "SELECT COUNT(*) FROM categories WHERE workspace_id = ?1",
            params![workspace],
            |row| row.get(0),
        )?;
        let runs = self.connection.query_row(
            "SELECT COUNT(*) FROM runs WHERE workspace_id = ?1",
            params![workspace],
            |row| row.get(0),
        )?;
        let measurements = self.connection.query_row(
            "SELECT COUNT(*) FROM measurement_observations WHERE workspace_id = ?1",
            params![workspace],
            |row| row.get(0),
        )?;
        let feedback = self.connection.query_row(
            "SELECT COUNT(*) FROM memory_feedback WHERE workspace_id = ?1",
            params![workspace],
            |row| row.get(0),
        )?;
        Ok(IndexSummary {
            facts,
            active_facts,
            forgotten_facts,
            contexts,
            categories,
            runs,
            measurements,
            feedback,
        })
    }

    pub fn prepare_summary(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<PreparedSummary, StoreError> {
        Ok(PreparedSummary {
            summary: self.summarize_index(workspace)?,
            recall: self.compose_recall(query, workspace)?,
        })
    }

    pub fn query_anchored(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<AnchoredSearch, StoreError> {
        validate_graph_workspace(workspace)?;
        let pattern = format!("%{}%", query.trim());
        let decisions = {
            let mut statement = self.connection.prepare(
                "SELECT id, category, subject, scenario, reasoning,
                        outcome, confidence, decision_maker, issue_ref,
                        path, symbol, parent_id, workspace_id
                 FROM decisions
                 WHERE workspace_id = ?1
                   AND (?2 = '%%' OR path LIKE ?2 OR symbol LIKE ?2 OR issue_ref LIKE ?2)
                 ORDER BY id",
            )?;
            let rows = statement
                .query_map(params![workspace, pattern], map_decision)?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let evidence = {
            let mut statement = self.connection.prepare(
                "SELECT id, fact_id, source_ref, source, checksum, fetched_at,
                        repository_ref, path, symbol, line_start, line_end,
                        column_start, column_end, selected_text_sha256,
                        resolution_status, workspace_id, created_at
                 FROM evidence
                 WHERE workspace_id = ?1
                   AND (?2 = '%%' OR source_ref LIKE ?2 OR path LIKE ?2 OR symbol LIKE ?2)
                 ORDER BY id",
            )?;
            let rows = statement
                .query_map(params![workspace, pattern], map_evidence)?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        Ok(AnchoredSearch {
            decisions,
            evidence,
        })
    }

    pub fn consolidate(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<ConsolidationReport, StoreError> {
        let facts = if query.trim().is_empty() {
            self.list_facts(workspace)?
        } else {
            self.search_facts(query, workspace)?
        };
        let scanned = facts.len() as i64;
        Ok(ConsolidationReport {
            status: "complete".to_owned(),
            scanned,
            // The schema enforces SHA-256/workspace uniqueness, so exact
            // duplicate consolidation is already completed at ingestion.
            consolidated: 0,
            remaining: scanned,
        })
    }

    pub fn backup_workspace(
        &self,
        path: &str,
        workspace: &str,
    ) -> Result<WorkspaceBackup, StoreError> {
        validate_context_workspace(workspace)?;
        let backup_path = self.resolve_private_backup_path(path)?;
        let snapshot = self.export_snapshot(workspace)?;
        let encoded = serde_json::to_vec_pretty(&snapshot).map_err(|error| {
            StoreError::Invalid(format!("backup serialization failed: {error}"))
        })?;
        atomic_private_file(&backup_path, &encoded)?;
        Ok(WorkspaceBackup {
            path: backup_path.to_string_lossy().into_owned(),
            bytes: encoded.len() as i64,
            facts: snapshot.facts.len() as i64,
            contexts: snapshot.contexts.len() as i64,
        })
    }

    /// Create the private, generated workspace backup used by the Python
    /// compatibility contract when no explicit output path is supplied.
    pub fn backup_workspace_default(&self, workspace: &str) -> Result<WorkspaceBackup, StoreError> {
        validate_context_workspace(workspace)?;
        let sequence = SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed);
        self.backup_workspace(
            &format!("workspace-{}-{sequence}.json", std::process::id()),
            workspace,
        )
    }

    pub fn current_database(&self) -> Result<DatabaseInfo, StoreError> {
        if self.memory_database_name.borrow().is_some() {
            return self.memory_current_database();
        }
        let path = self.database_path.borrow().clone();
        match path {
            Some(path) => self.database_info(&path, true, false),
            None => Err(StoreError::Invalid(
                "database state is missing for the in-memory store".to_owned(),
            )),
        }
    }

    pub fn list_databases(&self) -> Result<Vec<DatabaseInfo>, StoreError> {
        if self.memory_database_name.borrow().is_some() {
            return self.memory_list_databases();
        }
        let root = self.database_root()?;
        fs::create_dir_all(&root)?;
        let active_path = self.database_path.borrow().clone();
        let mut databases = Vec::new();

        if let Some(path) = active_path.as_deref() {
            databases.push(self.database_info(path, true, false)?);
        }

        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            let path = entry.path();
            let Some(archived) = database_path_kind(&path) else {
                continue;
            };
            if active_path
                .as_deref()
                .is_some_and(|active| same_database_path(active, &path))
            {
                continue;
            }
            databases.push(self.database_info(&path, false, archived)?);
        }

        databases.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.path.cmp(&right.path))
        });
        Ok(databases)
    }

    pub fn create_database(&self, name: &str) -> Result<DatabaseInfo, StoreError> {
        validate_database_name(name)?;
        if self.memory_database_name.borrow().is_some() {
            return self.memory_create_database(name);
        }
        let root = self.database_root()?;
        fs::create_dir_all(&root)?;
        let path = root.join(format!("{name}.db"));
        let archived_path = archived_database_path(&path);
        if path.exists() || archived_path.exists() {
            return Err(StoreError::Invalid(format!(
                "database already exists: {name}"
            )));
        }
        drop(Self::open(&path)?);
        self.database_info(&path, false, false)
    }

    pub fn archive_database(&self, name: &str) -> Result<Option<DatabaseInfo>, StoreError> {
        validate_database_name(name)?;
        if self.memory_database_name.borrow().is_some() {
            return self.memory_archive_database(name);
        }
        let path = self.named_database_path(name)?;
        let archived_path = archived_database_path(&path);
        if !path.exists() {
            return if archived_path.exists() {
                Ok(Some(self.database_info(&archived_path, false, true)?))
            } else {
                Ok(None)
            };
        }
        if self.is_active_path(&path) {
            return Err(StoreError::Invalid(
                "active database cannot be archived; select another database first".to_owned(),
            ));
        }
        fs::rename(&path, &archived_path)?;
        Ok(Some(self.database_info(&archived_path, false, true)?))
    }

    pub fn backup_database(
        &self,
        name: &str,
        output_path: &str,
    ) -> Result<DatabaseBackup, StoreError> {
        if self.memory_database_name.borrow().is_some() {
            return self.memory_backup_database(name, output_path);
        }
        let source = self.database_source_path(name)?;
        let output = self.resolve_private_backup_path(output_path)?;
        if same_database_path(&source, &output) {
            return Err(StoreError::Invalid(
                "database backup output must differ from the source database".to_owned(),
            ));
        }
        let backup_dir = output
            .parent()
            .ok_or_else(|| StoreError::Invalid("backup directory is missing".to_owned()))?;
        let sequence = SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temporary = backup_dir.join(format!(".database-{}-{sequence}.tmp", std::process::id()));
        let temporary_name = temporary.to_string_lossy().into_owned();
        let active_path = self.database_path.borrow().clone();
        let vacuum = if active_path
            .as_deref()
            .is_some_and(|active| same_database_path(active, &source))
        {
            self.connection
                .execute("VACUUM INTO ?1", params![temporary_name])
                .map(|_| ())
                .map_err(StoreError::from)
        } else {
            let source_store = Self::open(&source)?;
            source_store
                .connection
                .execute("VACUUM INTO ?1", params![temporary_name])
                .map(|_| ())
                .map_err(StoreError::from)
        };
        if let Err(error) = vacuum {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        let result = set_private_file_mode(&temporary)
            .and_then(|_| fs::rename(&temporary, &output).map_err(StoreError::from));
        if let Err(error) = result {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        let bytes = fs::metadata(&output)?.len() as i64;
        Ok(DatabaseBackup {
            database: name.to_owned(),
            path: output.to_string_lossy().into_owned(),
            bytes,
        })
    }

    /// Create the default timestamped backup used by the Python contract.
    /// The caller supplies only an optional database name; the destination is
    /// kept beside the active database in a private backups directory.
    pub fn backup_database_default(
        &self,
        name: Option<&str>,
    ) -> Result<DatabaseBackup, StoreError> {
        let database = name.unwrap_or("current");
        if database != "current" {
            validate_database_name(database)?;
        }
        let sequence = SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed);
        self.backup_database(
            database,
            &format!("{database}-{}-{sequence}.db", std::process::id()),
        )
    }

    pub fn delete_database(&self, name: &str) -> Result<bool, StoreError> {
        validate_database_name(name)?;
        if self.memory_database_name.borrow().is_some() {
            return self.memory_delete_database(name);
        }
        let path = self.named_database_path(name)?;
        if self.is_active_path(&path) {
            return Err(StoreError::Invalid(
                "active database cannot be deleted; select another database first".to_owned(),
            ));
        }
        if path.exists() {
            fs::remove_file(path)?;
            return Ok(true);
        }
        let archived_path = archived_database_path(&path);
        if archived_path.exists() {
            fs::remove_file(archived_path)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn select_database(&self, name: &str) -> Result<DatabaseInfo, StoreError> {
        if self.memory_database_name.borrow().is_some() {
            return self.memory_select_database(name);
        }
        if name == "current" {
            let Some(default_path) = self.default_database_path.borrow().clone() else {
                return self.current_database();
            };
            if !self.is_active_path(&default_path) {
                let candidate = Self::open(&default_path)?;
                let replacement = candidate.into_connection();
                let previous = self.connection.replace(replacement);
                drop(previous);
                *self.database_path.borrow_mut() = Some(default_path);
            }
            return self.current_database();
        }
        validate_database_name(name)?;
        let path = self.named_database_path(name)?;
        if !path.exists() {
            if archived_database_path(&path).exists() {
                return Err(StoreError::Invalid(format!("database is archived: {name}")));
            }
            return Err(StoreError::Invalid(format!("database not found: {name}")));
        }

        let candidate = Self::open(&path)?;
        let replacement = candidate.into_connection();
        let previous = self.connection.replace(replacement);
        drop(previous);
        *self.database_path.borrow_mut() = Some(path.clone());
        self.current_database()
    }

    pub fn reset_database(&self, name: &str) -> Result<DatabaseInfo, StoreError> {
        if self.memory_database_name.borrow().is_some() {
            return self.memory_reset_database(name);
        }
        if name == "current" {
            self.clear_database()?;
            return self.current_database();
        }
        validate_database_name(name)?;
        let path = self.named_database_path(name)?;
        if self.is_active_path(&path) {
            self.clear_database()?;
            return self.current_database();
        }
        if !path.exists() {
            if archived_database_path(&path).exists() {
                return Err(StoreError::Invalid(format!("database is archived: {name}")));
            }
            return Err(StoreError::Invalid(format!("database not found: {name}")));
        }
        let candidate = Self::open(&path)?;
        candidate.clear_database()?;
        self.database_info(&path, false, false)
    }

    fn memory_current_database(&self) -> Result<DatabaseInfo, StoreError> {
        let name = self
            .memory_database_name
            .borrow()
            .clone()
            .ok_or_else(|| StoreError::Invalid("memory database name is missing".to_owned()))?;
        let bytes = self.snapshot_bytes()?.len() as i64;
        Ok(DatabaseInfo {
            path: format!(":memory:{name}"),
            name,
            active: true,
            archived: false,
            bytes,
        })
    }

    fn memory_database_record(&self, name: &str) -> Result<Option<(bool, Vec<u8>)>, StoreError> {
        self.connection
            .query_row(
                "SELECT archived, snapshot
                 FROM memory_database_catalog
                 WHERE name = ?1",
                params![name],
                |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn memory_list_databases(&self) -> Result<Vec<DatabaseInfo>, StoreError> {
        let current = self.memory_current_database()?;
        let current_name = current.name.clone();
        let mut databases = vec![current];
        let mut statement = self.connection.prepare(
            "SELECT name, archived, length(snapshot)
             FROM memory_database_catalog
             WHERE name <> ?1
             ORDER BY name",
        )?;
        let rows = statement.query_map(params![current_name], |row| {
            let name = row.get::<_, String>(0)?;
            let archived = row.get::<_, i64>(1)? != 0;
            let bytes = row.get::<_, i64>(2)?;
            Ok(DatabaseInfo {
                path: format!(":memory:{name}"),
                name,
                active: false,
                archived,
                bytes,
            })
        })?;
        for row in rows {
            databases.push(row?);
        }
        Ok(databases)
    }

    fn memory_create_database(&self, name: &str) -> Result<DatabaseInfo, StoreError> {
        let current_name = self
            .memory_database_name
            .borrow()
            .clone()
            .expect("memory database mode has a current name");
        if name == current_name || self.memory_database_record(name)?.is_some() {
            return Err(StoreError::Invalid(format!(
                "database already exists: {name}"
            )));
        }
        let candidate = Self::in_memory()?;
        candidate.set_memory_database_name(name)?;
        let snapshot = candidate.snapshot_bytes()?;
        self.connection.execute(
            "INSERT INTO memory_database_catalog (name, archived, snapshot)
             VALUES (?1, 0, ?2)",
            params![name, snapshot],
        )?;
        Ok(DatabaseInfo {
            name: name.to_owned(),
            path: format!(":memory:{name}"),
            active: false,
            archived: false,
            bytes: i64::try_from(snapshot.len()).unwrap_or(i64::MAX),
        })
    }

    fn memory_archive_database(&self, name: &str) -> Result<Option<DatabaseInfo>, StoreError> {
        let current_name = self
            .memory_database_name
            .borrow()
            .clone()
            .expect("memory database mode has a current name");
        if name == current_name {
            return Err(StoreError::Invalid(
                "active database cannot be archived; select another database first".to_owned(),
            ));
        }
        let Some((_, snapshot)) = self.memory_database_record(name)? else {
            return Ok(None);
        };
        self.connection.execute(
            "UPDATE memory_database_catalog SET archived = 1 WHERE name = ?1",
            params![name],
        )?;
        Ok(Some(DatabaseInfo {
            name: name.to_owned(),
            path: format!(":memory:{name}"),
            active: false,
            archived: true,
            bytes: i64::try_from(snapshot.len()).unwrap_or(i64::MAX),
        }))
    }

    fn memory_snapshot_for_database(&self, name: &str) -> Result<Vec<u8>, StoreError> {
        let current_name = self
            .memory_database_name
            .borrow()
            .clone()
            .expect("memory database mode has a current name");
        if name == "current" || name == current_name {
            return self.snapshot_bytes();
        }
        let Some((archived, snapshot)) = self.memory_database_record(name)? else {
            return Err(StoreError::Invalid(format!("database not found: {name}")));
        };
        if archived {
            return Err(StoreError::Invalid(format!("database is archived: {name}")));
        }
        Ok(snapshot)
    }

    fn memory_backup_database(
        &self,
        name: &str,
        output_path: &str,
    ) -> Result<DatabaseBackup, StoreError> {
        let output = self.resolve_private_backup_path(output_path)?;
        let snapshot = self.memory_snapshot_for_database(name)?;
        atomic_private_file(&output, &snapshot)?;
        Ok(DatabaseBackup {
            database: name.to_owned(),
            path: output.to_string_lossy().into_owned(),
            bytes: i64::try_from(snapshot.len()).unwrap_or(i64::MAX),
        })
    }

    fn memory_delete_database(&self, name: &str) -> Result<bool, StoreError> {
        let current_name = self
            .memory_database_name
            .borrow()
            .clone()
            .expect("memory database mode has a current name");
        if name == current_name {
            return Err(StoreError::Invalid(
                "active database cannot be deleted; select another database first".to_owned(),
            ));
        }
        Ok(self.connection.execute(
            "DELETE FROM memory_database_catalog WHERE name = ?1",
            params![name],
        )? > 0)
    }

    fn memory_catalog(&self) -> Result<Vec<(String, bool, Vec<u8>)>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT name, archived, snapshot
             FROM memory_database_catalog
             ORDER BY name",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? != 0,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn memory_restore_catalog(
        &self,
        catalog: &[(String, bool, Vec<u8>)],
    ) -> Result<(), StoreError> {
        self.connection
            .execute("DELETE FROM memory_database_catalog", [])?;
        for (name, archived, snapshot) in catalog {
            self.connection.execute(
                "INSERT INTO memory_database_catalog (name, archived, snapshot)
                 VALUES (?1, ?2, ?3)",
                params![name, i64::from(*archived), snapshot],
            )?;
        }
        Ok(())
    }

    fn memory_save_current(&self) -> Result<(), StoreError> {
        let name = self
            .memory_database_name
            .borrow()
            .clone()
            .expect("memory database mode has a current name");
        let snapshot = self.snapshot_bytes()?;
        self.connection.execute(
            "INSERT INTO memory_database_catalog (name, archived, snapshot)
             VALUES (?1, 0, ?2)
             ON CONFLICT(name) DO UPDATE SET archived = 0, snapshot = excluded.snapshot",
            params![name, snapshot],
        )?;
        Ok(())
    }

    fn set_memory_database_name(&self, name: &str) -> Result<(), StoreError> {
        validate_database_name(name)?;
        self.connection.execute(
            "INSERT INTO memory_database_state (id, name) VALUES (1, ?1)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name",
            params![name],
        )?;
        *self.memory_database_name.borrow_mut() = Some(name.to_owned());
        Ok(())
    }

    fn memory_select_database(&self, name: &str) -> Result<DatabaseInfo, StoreError> {
        if name == "current" {
            return self.current_database();
        }
        validate_database_name(name)?;
        let current_name = self
            .memory_database_name
            .borrow()
            .clone()
            .expect("memory database mode has a current name");
        if name == current_name {
            return self.current_database();
        }
        let Some((archived, target_snapshot)) = self.memory_database_record(name)? else {
            return Err(StoreError::Invalid(format!("database not found: {name}")));
        };
        if archived {
            return Err(StoreError::Invalid(format!("database is archived: {name}")));
        }
        self.memory_save_current()?;
        let catalog = self.memory_catalog()?;
        self.restore_snapshot_bytes(&target_snapshot)?;
        self.memory_restore_catalog(
            &catalog
                .into_iter()
                .filter(|(catalog_name, _, _)| catalog_name != name)
                .collect::<Vec<_>>(),
        )?;
        self.set_memory_database_name(name)?;
        self.current_database()
    }

    fn memory_reset_database(&self, name: &str) -> Result<DatabaseInfo, StoreError> {
        if name == "current" {
            self.clear_database()?;
            return self.current_database();
        }
        validate_database_name(name)?;
        let Some((archived, snapshot)) = self.memory_database_record(name)? else {
            return Err(StoreError::Invalid(format!("database not found: {name}")));
        };
        if archived {
            return Err(StoreError::Invalid(format!("database is archived: {name}")));
        }
        let candidate = Self::in_memory()?;
        candidate.restore_snapshot_bytes(&snapshot)?;
        candidate.clear_database()?;
        candidate.set_memory_database_name(name)?;
        let empty_snapshot = candidate.snapshot_bytes()?;
        self.connection.execute(
            "UPDATE memory_database_catalog SET snapshot = ?2 WHERE name = ?1",
            params![name, empty_snapshot],
        )?;
        Ok(DatabaseInfo {
            name: name.to_owned(),
            path: format!(":memory:{name}"),
            active: false,
            archived: false,
            bytes: i64::try_from(empty_snapshot.len()).unwrap_or(i64::MAX),
        })
    }

    fn into_connection(self) -> Connection {
        self.connection.into_inner()
    }

    fn private_backup_dir(&self) -> Result<PathBuf, StoreError> {
        let directory = self
            .database_path
            .borrow()
            .as_deref()
            .and_then(Path::parent)
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(|parent| parent.join("backups"))
            .unwrap_or_else(|| {
                std::env::temp_dir().join(format!("memory-mcp-rust-backups-{}", std::process::id()))
            });
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StoreError::Invalid(
                    "backup directory must not be a symbolic link".to_owned(),
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(StoreError::Invalid(
                    "backup directory must reference a directory".to_owned(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&directory)?;
            }
            Err(error) => return Err(StoreError::Io(error)),
        }
        set_private_directory_mode(&directory)?;
        Ok(directory)
    }

    fn resolve_private_backup_path(&self, requested: &str) -> Result<PathBuf, StoreError> {
        if requested.trim().is_empty() {
            return Err(StoreError::Invalid(
                "backup path must not be empty".to_owned(),
            ));
        }
        let requested = Path::new(requested);
        let mut components = requested.components();
        let Some(Component::Normal(name)) = components.next() else {
            return Err(StoreError::Invalid(
                "backup path must be one file name inside the private backup directory".to_owned(),
            ));
        };
        if components.next().is_some() {
            return Err(StoreError::Invalid(
                "backup path must be one file name inside the private backup directory".to_owned(),
            ));
        }
        let name = name.to_str().ok_or_else(|| {
            StoreError::Invalid("backup file name must be valid UTF-8".to_owned())
        })?;
        if name.is_empty() || name == "." || name == ".." {
            return Err(StoreError::Invalid(
                "backup file name must not be empty or a traversal component".to_owned(),
            ));
        }
        let output = self.private_backup_dir()?.join(name);
        match fs::symlink_metadata(&output) {
            Ok(_) => Err(StoreError::Invalid(
                "backup output already exists".to_owned(),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(output),
            Err(error) => Err(StoreError::Io(error)),
        }
    }

    fn database_root(&self) -> Result<PathBuf, StoreError> {
        self.database_root.clone().ok_or_else(|| {
            StoreError::Invalid("named database operations require a file-backed store".to_owned())
        })
    }

    fn named_database_path(&self, name: &str) -> Result<PathBuf, StoreError> {
        let root = self.database_root()?;
        Ok(root.join(format!("{name}.db")))
    }

    fn database_source_path(&self, name: &str) -> Result<PathBuf, StoreError> {
        if name == "current" {
            return self.database_path.borrow().clone().ok_or_else(|| {
                StoreError::Invalid(
                    "the in-memory database cannot be backed up as a file".to_owned(),
                )
            });
        }
        validate_database_name(name)?;
        let path = self.named_database_path(name)?;
        if path.exists() {
            return Ok(path);
        }
        let archived_path = archived_database_path(&path);
        if archived_path.exists() {
            return Ok(archived_path);
        }
        Err(StoreError::Invalid(format!("database not found: {name}")))
    }

    fn database_info(
        &self,
        path: &Path,
        active: bool,
        archived: bool,
    ) -> Result<DatabaseInfo, StoreError> {
        let bytes = fs::metadata(path)?.len() as i64;
        let name = database_name_from_path(path, archived).ok_or_else(|| {
            StoreError::Invalid(format!(
                "database path has no supported name: {}",
                path.display()
            ))
        })?;
        Ok(DatabaseInfo {
            name,
            path: path.to_string_lossy().into_owned(),
            active,
            archived,
            bytes,
        })
    }

    fn is_active_path(&self, path: &Path) -> bool {
        self.database_path
            .borrow()
            .as_deref()
            .is_some_and(|active| same_database_path(active, path))
    }

    fn clear_database(&self) -> Result<(), StoreError> {
        self.connection.execute_batch(
            "DELETE FROM handoffs;
             DELETE FROM lifecycle_events;
             DELETE FROM context_lineage;
             DELETE FROM evidence;
             DELETE FROM fact_history;
             DELETE FROM fact_embeddings;
             DELETE FROM relations;
             DELETE FROM facts;
             DELETE FROM decision_embeddings;
             DELETE FROM decisions;
             DELETE FROM contexts;
             DELETE FROM entities;
             DELETE FROM categories;
             DELETE FROM runs;
             DELETE FROM measurement_observations;
             DELETE FROM memory_feedback;
             DELETE FROM workspaces;",
        )?;
        Ok(())
    }

    fn category_by_name(
        &self,
        name: &str,
        workspace: &str,
    ) -> Result<Option<Category>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, name, workspace_id, created_at
                 FROM categories
                 WHERE name = ?1 AND workspace_id = ?2",
                params![name, workspace],
                map_category,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn run_by_key(&self, run_id: &str, workspace: &str) -> Result<Option<Run>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, run_id, issue_ref, pr_ref, session, git_ref,
                        files, diff, summary, state, workspace_id, created_at, ended_at
                 FROM runs
                 WHERE run_id = ?1 AND workspace_id = ?2",
                params![run_id, workspace],
                map_run,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn measurement_by_key(
        &self,
        measurement: &str,
        sample: &str,
        variant: &str,
        workspace: &str,
    ) -> Result<Option<Measurement>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, measurement, sample, variant, value, baseline,
                        workspace_id, created_at
                 FROM measurement_observations
                 WHERE measurement = ?1 AND sample = ?2 AND variant = ?3
                   AND workspace_id = ?4",
                params![measurement, sample, variant, workspace],
                map_measurement,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn feedback_by_key(
        &self,
        feedback_id: &str,
        workspace: &str,
    ) -> Result<Option<Feedback>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, feedback_id, site, item_type, item_ref, signal,
                        query_hash, workspace_id, created_at
                 FROM memory_feedback
                 WHERE feedback_id = ?1 AND workspace_id = ?2",
                params![feedback_id, workspace],
                map_feedback,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn record_fact_history(
        &self,
        fact_id: i64,
        event: &str,
        from_lifecycle: &str,
        to_lifecycle: &str,
        note: &str,
        workspace: &str,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO fact_history
                (fact_id, event, from_lifecycle, to_lifecycle, note, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                fact_id,
                event,
                from_lifecycle,
                to_lifecycle,
                note,
                workspace
            ],
        )?;
        Ok(())
    }

    pub fn search_facts(&self, query: &str, workspace: &str) -> Result<Vec<Fact>, StoreError> {
        self.search_facts_with_filters(query, workspace, &FactFilters::default())
    }

    pub fn search_facts_with_filters(
        &self,
        query: &str,
        workspace: &str,
        filters: &FactFilters,
    ) -> Result<Vec<Fact>, StoreError> {
        validate_fact_filters(filters)?;
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let fts_query = query
            .split_whitespace()
            .map(|term| format!("\"{}\"", term.replace('"', "")))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut statement = self.connection.prepare(
            "SELECT f.id, f.text, f.sha256, f.workspace_id, f.lifecycle,
                    f.source, f.project, f.domain, f.trust, f.strong, f.importance,
                    f.category_id, f.validity, f.session_id, f.access_count
             FROM facts_fts
             JOIN facts f ON f.id = facts_fts.rowid
             WHERE facts_fts MATCH ?1
               AND (f.workspace_id = '' OR f.workspace_id = ?2)
               AND f.lifecycle != 'forgotten'
               AND f.validity != 'invalid'
               AND (?3 IS NULL OR f.source = ?3)
               AND (?4 IS NULL OR f.project = ?4)
               AND (?5 IS NULL OR f.domain = ?5)
               AND (?6 IS NULL OR f.trust = ?6)
               AND (?7 IS NULL OR f.strong = ?7)
             ORDER BY f.id",
        )?;
        let strong = filters.strong.map(|value| value as i64);
        let rows = statement
            .query_map(
                params![
                    fts_query,
                    workspace,
                    filters.source.as_deref(),
                    filters.project.as_deref(),
                    filters.domain.as_deref(),
                    filters.trust.as_deref(),
                    strong
                ],
                map_fact,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        let like = format!("%{}%", query);
        let mut fallback = self.connection.prepare(
            "SELECT id, text, sha256, workspace_id, lifecycle,
                    source, project, domain, trust, strong, importance, category_id,
                    validity, session_id, access_count
             FROM facts
             WHERE text LIKE ?1
               AND (workspace_id = '' OR workspace_id = ?2)
               AND lifecycle != 'forgotten'
               AND validity != 'invalid'
               AND (?3 IS NULL OR source = ?3)
               AND (?4 IS NULL OR project = ?4)
               AND (?5 IS NULL OR domain = ?5)
               AND (?6 IS NULL OR trust = ?6)
               AND (?7 IS NULL OR strong = ?7)
             ORDER BY id",
        )?;
        let rows = fallback
            .query_map(
                params![
                    like,
                    workspace,
                    filters.source.as_deref(),
                    filters.project.as_deref(),
                    filters.domain.as_deref(),
                    filters.trust.as_deref(),
                    strong
                ],
                map_fact,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn list_facts(&self, workspace: &str) -> Result<Vec<Fact>, StoreError> {
        self.list_facts_with_filters(workspace, &FactFilters::default())
    }

    pub fn list_facts_with_filters(
        &self,
        workspace: &str,
        filters: &FactFilters,
    ) -> Result<Vec<Fact>, StoreError> {
        validate_fact_filters(filters)?;
        let mut statement = self.connection.prepare(
            "SELECT id, text, sha256, workspace_id, lifecycle,
                    source, project, domain, trust, strong, importance, category_id,
                    validity, session_id, access_count
             FROM facts
             WHERE (workspace_id = '' OR workspace_id = ?1)
               AND lifecycle != 'forgotten'
               AND validity != 'invalid'
               AND (?2 IS NULL OR source = ?2)
               AND (?3 IS NULL OR project = ?3)
               AND (?4 IS NULL OR domain = ?4)
               AND (?5 IS NULL OR trust = ?5)
               AND (?6 IS NULL OR strong = ?6)
             ORDER BY id",
        )?;
        let strong = filters.strong.map(|value| value as i64);
        let rows = statement
            .query_map(
                params![
                    workspace,
                    filters.source.as_deref(),
                    filters.project.as_deref(),
                    filters.domain.as_deref(),
                    filters.trust.as_deref(),
                    strong
                ],
                map_fact,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn put_context(
        &self,
        reference: &str,
        name: &str,
        content: &str,
        workspace: &str,
    ) -> Result<Context, StoreError> {
        self.put_context_with_metadata(
            reference,
            name,
            content,
            &ContextMetadata::default(),
            workspace,
        )
    }

    pub fn put_context_with_metadata(
        &self,
        reference: &str,
        name: &str,
        content: &str,
        metadata: &ContextMetadata,
        workspace: &str,
    ) -> Result<Context, StoreError> {
        validate_context(reference, name, workspace, metadata.expires_at.as_deref())?;
        let max_bytes = configured_context_max_bytes()?;
        if content.len() > max_bytes {
            return Err(StoreError::Invalid(format!(
                "context content exceeds the configured size limit ({max_bytes} bytes)"
            )));
        }
        if let Some(expires_at) = metadata.expires_at.as_deref() {
            self.validate_timestamp(expires_at, "context expiry")?;
        }
        let sha256 = sha256(content);
        let byte_size = content.len() as i64;
        if let Some(existing) = self.context_raw(reference, workspace)? {
            if existing.sha256 != sha256
                || existing.name != name
                || existing.schema != metadata.schema
                || existing.source != metadata.source
                || existing.expires_at != metadata.expires_at
            {
                return Err(StoreError::Invalid(format!(
                    "context ref is immutable: {reference}"
                )));
            }
            return Ok(existing);
        }
        self.connection.execute(
            "INSERT INTO contexts
                (ref, name, content, sha256, \"schema\", source, workspace_id, expires_at, byte_size)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                reference,
                name,
                content,
                sha256,
                metadata.schema,
                metadata.source,
                workspace,
                metadata.expires_at,
                byte_size
            ],
        )?;
        self.context_raw(reference, workspace)?.ok_or_else(|| {
            StoreError::Invalid("context insert did not produce a readable row".to_owned())
        })
    }

    pub fn context(&self, reference: &str, workspace: &str) -> Result<Option<Context>, StoreError> {
        validate_context_workspace(workspace)?;
        self.connection
            .query_row(
                "SELECT ref, name, content, sha256, workspace_id, \"schema\", source,
                        expires_at, byte_size
                 FROM contexts
                 WHERE ref = ?1 AND workspace_id = ?2
                   AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))",
                params![reference, workspace],
                map_context,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn context_raw(&self, reference: &str, workspace: &str) -> Result<Option<Context>, StoreError> {
        self.connection
            .query_row(
                "SELECT ref, name, content, sha256, workspace_id, \"schema\", source,
                        expires_at, byte_size
                 FROM contexts
                 WHERE ref = ?1 AND workspace_id = ?2",
                params![reference, workspace],
                map_context,
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn list_contexts(&self, workspace: &str) -> Result<Vec<Context>, StoreError> {
        validate_context_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT ref, name, content, sha256, workspace_id, \"schema\", source,
                    expires_at, byte_size
             FROM contexts
             WHERE workspace_id = ?1
               AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
             ORDER BY ref",
        )?;
        let rows = statement
            .query_map(params![workspace], map_context)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn resolve_context(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<Option<Context>, StoreError> {
        validate_context_workspace(workspace)?;
        if query.trim().is_empty() {
            return Ok(None);
        }
        if let Some(context) = self.context(query, workspace)? {
            return Ok(Some(context));
        }
        self.connection
            .query_row(
                "SELECT ref, name, content, sha256, workspace_id, \"schema\", source,
                        expires_at, byte_size
                 FROM contexts
                 WHERE name = ?1 AND workspace_id = ?2
                   AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
                 ORDER BY ref LIMIT 1",
                params![query, workspace],
                map_context,
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn search_contexts(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<Vec<Context>, StoreError> {
        validate_context_workspace(workspace)?;
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            "SELECT ref, name, content, sha256, workspace_id, \"schema\", source,
                    expires_at, byte_size
             FROM contexts
             WHERE workspace_id = ?1
               AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
               AND (instr(lower(ref), lower(?2)) > 0
                    OR instr(lower(name), lower(?2)) > 0
                    OR instr(lower(content), lower(?2)) > 0)
             ORDER BY ref",
        )?;
        let rows = statement
            .query_map(params![workspace, query], map_context)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn chunk_context(
        &self,
        reference: &str,
        max_bytes: usize,
        workspace: &str,
    ) -> Result<Vec<ContextChunk>, StoreError> {
        if max_bytes == 0 {
            return Err(StoreError::Invalid(
                "context chunk size must be positive".to_owned(),
            ));
        }
        let context = self
            .context(reference, workspace)?
            .ok_or_else(|| StoreError::Invalid(format!("context not found: {reference}")))?;
        let chunks = split_utf8_chunks(&context.content, max_bytes)?;
        let total = chunks.len() as i64;
        Ok(chunks
            .into_iter()
            .enumerate()
            .map(|(index, content)| ContextChunk {
                reference: reference.to_owned(),
                index: index as i64,
                total,
                byte_size: content.len() as i64,
                content,
            })
            .collect())
    }

    pub fn link_context(
        &self,
        parent_reference: &str,
        child_reference: &str,
        relation: &str,
        workspace: &str,
    ) -> Result<ContextLineage, StoreError> {
        validate_context_workspace(workspace)?;
        if parent_reference.trim().is_empty()
            || child_reference.trim().is_empty()
            || relation.trim().is_empty()
        {
            return Err(StoreError::Invalid(
                "context lineage references and relation must not be empty".to_owned(),
            ));
        }
        if self.context_raw(parent_reference, workspace)?.is_none()
            || self.context_raw(child_reference, workspace)?.is_none()
        {
            return Err(StoreError::Invalid(
                "context lineage references must belong to the workspace".to_owned(),
            ));
        }
        self.connection.execute(
            "INSERT OR IGNORE INTO context_lineage
                (parent_ref, child_ref, relation, workspace_id)
             VALUES (?1, ?2, ?3, ?4)",
            params![parent_reference, child_reference, relation, workspace],
        )?;
        self.connection
            .query_row(
                "SELECT parent_ref, child_ref, relation, workspace_id
                 FROM context_lineage
                 WHERE parent_ref = ?1 AND child_ref = ?2 AND relation = ?3
                   AND workspace_id = ?4",
                params![parent_reference, child_reference, relation, workspace],
                map_context_lineage,
            )
            .map_err(StoreError::from)
    }

    pub fn context_map(
        &self,
        reference: Option<&str>,
        workspace: &str,
    ) -> Result<Vec<ContextLineage>, StoreError> {
        validate_context_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT parent_ref, child_ref, relation, workspace_id
             FROM context_lineage
             WHERE workspace_id = ?1
               AND (?2 IS NULL OR parent_ref = ?2 OR child_ref = ?2)
             ORDER BY parent_ref, child_ref, relation",
        )?;
        let rows = statement
            .query_map(params![workspace, reference], map_context_lineage)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn reduce_context(
        &self,
        references: &[String],
        output_reference: Option<&str>,
        name: &str,
        metadata: &ContextMetadata,
        workspace: &str,
    ) -> Result<Context, StoreError> {
        validate_context_workspace(workspace)?;
        if references.is_empty() {
            return Err(StoreError::Invalid(
                "reduce_context requires at least one reference".to_owned(),
            ));
        }
        let mut contents = Vec::with_capacity(references.len());
        for reference in references {
            let context = self
                .context(reference, workspace)?
                .ok_or_else(|| StoreError::Invalid(format!("context not found: {reference}")))?;
            contents.push(context.content);
        }
        let content = contents.join("\n\n");
        let derived_reference = format!("reduced:{}", sha256(&content));
        let reference = output_reference.unwrap_or(&derived_reference);
        let reduced =
            self.put_context_with_metadata(reference, name, &content, metadata, workspace)?;
        for parent in references {
            self.link_context(parent, reference, "reduced_from", workspace)?;
        }
        Ok(reduced)
    }

    pub fn capture_event(&self, spec: &EventSpec) -> Result<LifecycleEvent, StoreError> {
        validate_event_spec(spec)?;
        if self
            .context(&spec.context_reference, &spec.workspace)?
            .is_none()
        {
            return Err(StoreError::Invalid(format!(
                "context not found: {}",
                spec.context_reference
            )));
        }
        let payload_size = i64::try_from(spec.payload.len())
            .map_err(|_| StoreError::Invalid("event payload is too large".to_owned()))?;
        let payload_sha256 = sha256(&spec.payload);
        let payload_truncated = spec.payload_truncated;
        if let Some(existing) = self.event_by_key(&spec.idempotency_key, &spec.workspace)? {
            if existing.event_type != spec.event_type
                || existing.context_reference != spec.context_reference
                || existing.metadata != spec.metadata
                || existing.payload_sha256 != payload_sha256
                || existing.payload_size != payload_size
                || existing.payload_truncated != payload_truncated
            {
                return Err(StoreError::Invalid(
                    "event idempotency key conflicts with an existing event".to_owned(),
                ));
            }
            return Ok(existing);
        }
        if self
            .event_by_context(&spec.context_reference, &spec.workspace)?
            .is_some()
        {
            return Err(StoreError::Invalid(
                "context already has a lifecycle event".to_owned(),
            ));
        }
        self.connection.execute(
            "INSERT INTO lifecycle_events
                (idempotency_key, event_type, context_ref, metadata,
                 payload_sha256, payload_size, payload_truncated, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                spec.idempotency_key,
                spec.event_type,
                spec.context_reference,
                spec.metadata,
                payload_sha256,
                payload_size,
                payload_truncated as i64,
                spec.workspace
            ],
        )?;
        self.event_by_key(&spec.idempotency_key, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid("event insert did not produce a readable row".to_owned())
            })
    }

    pub fn read_event(
        &self,
        idempotency_key: &str,
        workspace: &str,
    ) -> Result<Option<LifecycleEvent>, StoreError> {
        validate_context_workspace(workspace)?;
        self.event_by_key(idempotency_key, workspace)
    }

    pub fn list_events(&self, workspace: &str) -> Result<Vec<LifecycleEvent>, StoreError> {
        validate_context_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, idempotency_key, event_type, context_ref, metadata,
                    payload_sha256, payload_size, payload_truncated, workspace_id, created_at
             FROM lifecycle_events
             WHERE workspace_id = ?1
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_lifecycle_event)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn begin_handoff(&self, spec: &HandoffSpec) -> Result<Handoff, StoreError> {
        validate_handoff_spec(spec)?;
        if self
            .context(&spec.context_reference, &spec.workspace)?
            .is_none()
        {
            return Err(StoreError::Invalid(format!(
                "context not found: {}",
                spec.context_reference
            )));
        }
        let expires_at = self.handoff_expiry(spec)?;
        if let Some(existing) = self.handoff_by_key(&spec.idempotency_key, &spec.workspace)? {
            if existing.context_reference != spec.context_reference
                || existing.owner != spec.owner
                || existing.session != spec.session
                || existing.source != spec.source
                || existing.shared != spec.shared
            {
                return Err(StoreError::Invalid(
                    "handoff idempotency key conflicts with an existing handoff".to_owned(),
                ));
            }
            return Ok(existing);
        }
        if self
            .handoff_by_context(&spec.context_reference, &spec.workspace)?
            .is_some()
        {
            return Err(StoreError::Invalid(
                "context already has a handoff".to_owned(),
            ));
        }
        self.connection.execute(
            "INSERT INTO handoffs
                (idempotency_key, context_ref, owner, session, source,
                 workspace_id, shared, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                spec.idempotency_key,
                spec.context_reference,
                spec.owner,
                spec.session,
                spec.source,
                spec.workspace,
                spec.shared as i64,
                expires_at
            ],
        )?;
        self.handoff_by_key(&spec.idempotency_key, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid("handoff insert did not produce a readable row".to_owned())
            })
    }

    pub fn list_handoffs(&self, workspace: &str) -> Result<Vec<Handoff>, StoreError> {
        validate_context_workspace(workspace)?;
        self.refresh_expired_handoffs(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, idempotency_key, context_ref, owner, session, source,
                    workspace_id, shared, expires_at, state, accepted_at,
                    accepted_by, cancelled_at, cancelled_by, created_at
             FROM handoffs
             WHERE workspace_id = ?1
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_handoff)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn accept_handoff(
        &self,
        idempotency_key: &str,
        actor: &str,
        workspace: &str,
    ) -> Result<Option<Handoff>, StoreError> {
        validate_context_workspace(workspace)?;
        if actor.trim().is_empty() {
            return Err(StoreError::Invalid(
                "handoff acceptor must not be empty".to_owned(),
            ));
        }
        self.refresh_expired_handoffs(workspace)?;
        let Some(existing) = self.handoff_by_key(idempotency_key, workspace)? else {
            return Ok(None);
        };
        match existing.state.as_str() {
            "open" => {
                self.connection.execute(
                    "UPDATE handoffs
                     SET state = 'accepted', accepted_at = CURRENT_TIMESTAMP, accepted_by = ?1
                     WHERE id = ?2",
                    params![actor, existing.id],
                )?;
            }
            "accepted" => return Ok(Some(existing)),
            state => {
                return Err(StoreError::Invalid(format!(
                    "cannot accept handoff in state {state}"
                )))
            }
        }
        self.handoff_by_key(idempotency_key, workspace)
    }

    pub fn cancel_handoff(
        &self,
        idempotency_key: &str,
        actor: &str,
        workspace: &str,
    ) -> Result<Option<Handoff>, StoreError> {
        validate_context_workspace(workspace)?;
        if actor.trim().is_empty() {
            return Err(StoreError::Invalid(
                "handoff canceller must not be empty".to_owned(),
            ));
        }
        self.refresh_expired_handoffs(workspace)?;
        let Some(existing) = self.handoff_by_key(idempotency_key, workspace)? else {
            return Ok(None);
        };
        match existing.state.as_str() {
            "open" => {
                self.connection.execute(
                    "UPDATE handoffs
                     SET state = 'cancelled', cancelled_at = CURRENT_TIMESTAMP, cancelled_by = ?1
                     WHERE id = ?2",
                    params![actor, existing.id],
                )?;
            }
            "cancelled" => return Ok(Some(existing)),
            state => {
                return Err(StoreError::Invalid(format!(
                    "cannot cancel handoff in state {state}"
                )))
            }
        }
        self.handoff_by_key(idempotency_key, workspace)
    }

    pub fn remember_entity(&self, spec: &EntitySpec) -> Result<Entity, StoreError> {
        validate_entity_spec(spec)?;
        let canonical_name = canonical_name(&spec.name);
        let aliases = serde_json::to_string(&spec.aliases)
            .map_err(|error| StoreError::Invalid(error.to_string()))?;
        self.connection.execute(
            "INSERT OR IGNORE INTO entities
                (name, canonical_name, entity_type, aliases, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                spec.name,
                canonical_name,
                spec.entity_type,
                aliases,
                spec.workspace
            ],
        )?;
        self.entity_by_name(&spec.name, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid("entity insert did not produce a readable row".to_owned())
            })
    }

    pub fn remember_relation(&self, spec: &RelationSpec) -> Result<Relation, StoreError> {
        validate_relation_spec(spec)?;
        let subject_id = self
            .entity_id_for_reference(&spec.subject, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid(format!("subject entity not found: {}", spec.subject))
            })?;
        let object_id = self
            .entity_id_for_reference(&spec.object, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid(format!("object entity not found: {}", spec.object))
            })?;
        if let Some(fact_id) = spec.source_fact_id {
            if self.fact_by_id(fact_id, &spec.workspace)?.is_none() {
                return Err(StoreError::Invalid(format!(
                    "source fact not found: {fact_id}"
                )));
            }
        }
        self.connection.execute(
            "INSERT OR IGNORE INTO relations
                (subject_id, predicate, object_id, source_fact_id, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                subject_id,
                spec.predicate,
                object_id,
                spec.source_fact_id,
                spec.workspace
            ],
        )?;
        self.relation_by_key(subject_id, &spec.predicate, object_id, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid("relation insert did not produce a readable row".to_owned())
            })
    }

    pub fn search_graph(&self, query: &str, workspace: &str) -> Result<GraphSearch, StoreError> {
        validate_graph_workspace(workspace)?;
        if query.trim().is_empty() {
            return Ok(GraphSearch {
                entities: Vec::new(),
                relations: Vec::new(),
            });
        }
        let mut entities_statement = self.connection.prepare(
            "SELECT id, name, canonical_name, entity_type, aliases, workspace_id
             FROM entities
             WHERE workspace_id = ?1
               AND (instr(lower(name), lower(?2)) > 0
                    OR instr(lower(canonical_name), lower(?2)) > 0
                    OR instr(lower(aliases), lower(?2)) > 0)
             ORDER BY id",
        )?;
        let entities = entities_statement
            .query_map(params![workspace, query], map_entity)?
            .collect::<Result<Vec<_>, _>>()?;
        let entity_ids = entities.iter().map(|entity| entity.id).collect::<Vec<_>>();

        let mut relations_statement = self.connection.prepare(
            "SELECT id, subject_id, predicate, object_id, source_fact_id, workspace_id
             FROM relations
             WHERE workspace_id = ?1
               AND (instr(lower(predicate), lower(?2)) > 0
                    OR subject_id IN (
                        SELECT id FROM entities
                        WHERE workspace_id = ?1
                          AND (instr(lower(name), lower(?2)) > 0
                               OR instr(lower(canonical_name), lower(?2)) > 0)
                    )
                    OR object_id IN (
                        SELECT id FROM entities
                        WHERE workspace_id = ?1
                          AND (instr(lower(name), lower(?2)) > 0
                               OR instr(lower(canonical_name), lower(?2)) > 0)
                    ))
             ORDER BY id",
        )?;
        let relations = relations_statement
            .query_map(params![workspace, query], map_relation)?
            .collect::<Result<Vec<_>, _>>()?;
        let relations = if entity_ids.is_empty() {
            relations
        } else {
            relations
                .into_iter()
                .filter(|relation| {
                    relation
                        .predicate
                        .to_lowercase()
                        .contains(&query.to_lowercase())
                        || entity_ids.contains(&relation.subject_id)
                        || entity_ids.contains(&relation.object_id)
                })
                .collect()
        };
        Ok(GraphSearch {
            entities,
            relations,
        })
    }

    /// Return a bounded breadth-first neighborhood for the modern graph tool.
    /// The legacy search_graph query remains substring based; this adapter
    /// resolves an exact entity first and then walks only the requested
    /// number of relation hops in both directions.
    pub fn graph_neighborhood(
        &self,
        query: &str,
        depth: usize,
        limit: usize,
        workspace: &str,
    ) -> Result<GraphSearch, StoreError> {
        validate_graph_workspace(workspace)?;
        if query.trim().is_empty() {
            return Ok(GraphSearch {
                entities: Vec::new(),
                relations: Vec::new(),
            });
        }
        if !(1..=2).contains(&depth) || !(1..=200).contains(&limit) {
            return Err(StoreError::Invalid(
                "depth or limit is outside the supported range".to_owned(),
            ));
        }

        let entities = self.list_entities(workspace)?;
        let normalized_query = canonical_name(query);
        let query_lower = query.to_lowercase();
        let root = entities
            .iter()
            .find(|entity| {
                entity.canonical_name == normalized_query
                    || canonical_name(&entity.name) == normalized_query
                    || entity
                        .aliases
                        .iter()
                        .any(|alias| canonical_name(alias) == normalized_query)
            })
            .or_else(|| {
                entities.iter().find(|entity| {
                    entity.name.to_lowercase().contains(&query_lower)
                        || entity.canonical_name.contains(&normalized_query)
                        || entity
                            .aliases
                            .iter()
                            .any(|alias| alias.to_lowercase().contains(&query_lower))
                })
            });
        let Some(root) = root else {
            return Ok(GraphSearch {
                entities: Vec::new(),
                relations: Vec::new(),
            });
        };
        let root = root.clone();

        let relations = self.list_relations(workspace)?;
        let entity_by_id = entities
            .iter()
            .map(|entity| (entity.id, entity.clone()))
            .collect::<std::collections::HashMap<_, _>>();
        let mut selected_ids = std::collections::HashSet::from([root.id]);
        let mut frontier = vec![root.id];
        let mut selected_relation_ids = std::collections::HashSet::new();
        let mut selected_relations = Vec::new();

        for _ in 0..depth {
            if frontier.is_empty() {
                break;
            }
            let mut next_frontier = Vec::new();
            for relation in &relations {
                let neighbor = if frontier.contains(&relation.subject_id) {
                    Some(relation.object_id)
                } else if frontier.contains(&relation.object_id) {
                    Some(relation.subject_id)
                } else {
                    None
                };
                let Some(neighbor) = neighbor else {
                    continue;
                };
                if !entity_by_id.contains_key(&neighbor) {
                    continue;
                }
                if !selected_ids.contains(&neighbor) && selected_relations.len() >= limit {
                    continue;
                }
                if !selected_ids.contains(&neighbor) {
                    if selected_ids.len() >= limit {
                        continue;
                    }
                    selected_ids.insert(neighbor);
                    next_frontier.push(neighbor);
                }
                if selected_relation_ids.insert(relation.id) && selected_relations.len() < limit {
                    selected_relations.push(relation.clone());
                }
            }
            frontier = next_frontier;
        }

        let mut selected_entities = Vec::with_capacity(selected_ids.len());
        selected_entities.push(root.clone());
        selected_entities.extend(
            entities
                .into_iter()
                .filter(|entity| entity.id != root.id && selected_ids.contains(&entity.id))
                .take(limit.saturating_sub(1)),
        );
        Ok(GraphSearch {
            entities: selected_entities,
            relations: selected_relations,
        })
    }

    pub fn record_decision(&self, spec: &DecisionSpec) -> Result<Decision, StoreError> {
        validate_decision_spec(spec)?;
        if let Some(parent_id) = spec.parent_id {
            if self.decision_by_id(parent_id, &spec.workspace)?.is_none() {
                return Err(StoreError::Invalid(format!(
                    "parent decision not found: {parent_id}"
                )));
            }
        }
        self.connection.execute(
            "INSERT INTO decisions
                (category, subject, scenario, reasoning, outcome, confidence,
                 decision_maker, issue_ref, path, symbol, parent_id, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                spec.category,
                spec.subject,
                spec.scenario,
                spec.reasoning,
                spec.outcome,
                spec.confidence,
                spec.decision_maker,
                spec.issue_ref,
                spec.path,
                spec.symbol,
                spec.parent_id,
                spec.workspace
            ],
        )?;
        let id = self.connection.last_insert_rowid();
        self.decision_by_id(id, &spec.workspace)?.ok_or_else(|| {
            StoreError::Invalid("decision insert did not produce a readable row".to_owned())
        })
    }

    pub fn query_decisions(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<Vec<Decision>, StoreError> {
        validate_graph_workspace(workspace)?;
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let fts_query = query
            .split_whitespace()
            .map(|term| format!("\"{}\"", term.replace('"', "")))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut statement = self.connection.prepare(
            "SELECT d.id, d.category, d.subject, d.scenario, d.reasoning,
                    d.outcome, d.confidence, d.decision_maker, d.issue_ref,
                    d.path, d.symbol, d.parent_id, d.workspace_id
             FROM decisions_fts
             JOIN decisions d ON d.id = decisions_fts.rowid
             WHERE decisions_fts MATCH ?1 AND d.workspace_id = ?2
             ORDER BY d.id",
        )?;
        let rows = statement
            .query_map(params![fts_query, workspace], map_decision)?
            .collect::<Result<Vec<_>, _>>()?;
        if !rows.is_empty() {
            return Ok(rows);
        }
        let like = format!("%{}%", query);
        let mut fallback = self.connection.prepare(
            "SELECT id, category, subject, scenario, reasoning,
                    outcome, confidence, decision_maker, issue_ref,
                    path, symbol, parent_id, workspace_id
             FROM decisions
             WHERE workspace_id = ?1
               AND (category LIKE ?2 OR subject LIKE ?2 OR scenario LIKE ?2
                    OR reasoning LIKE ?2 OR outcome LIKE ?2)
             ORDER BY id",
        )?;
        let rows = fallback
            .query_map(params![workspace, like], map_decision)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)?;
        Ok(rows)
    }

    pub fn find_precedents(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<Vec<Decision>, StoreError> {
        self.query_decisions(query, workspace)
    }

    pub fn causal_chain(&self, id: i64, workspace: &str) -> Result<Vec<Decision>, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut chain = Vec::new();
        let mut current_id = Some(id);
        let mut seen = Vec::new();
        while let Some(decision_id) = current_id {
            if seen.contains(&decision_id) {
                return Err(StoreError::Invalid(
                    "decision parent chain contains a cycle".to_owned(),
                ));
            }
            seen.push(decision_id);
            let decision = self
                .decision_by_id(decision_id, workspace)?
                .ok_or_else(|| StoreError::Invalid(format!("decision not found: {decision_id}")))?;
            current_id = decision.parent_id;
            chain.push(decision);
        }
        chain.reverse();
        Ok(chain)
    }

    pub fn detect_conflicts(
        &self,
        query: &str,
        workspace: &str,
    ) -> Result<Vec<DecisionConflict>, StoreError> {
        let decisions = self.query_decisions(query, workspace)?;
        let mut grouped = std::collections::BTreeMap::<(String, String), Vec<String>>::new();
        for decision in decisions {
            let outcomes = grouped
                .entry((decision.subject, decision.scenario))
                .or_default();
            if !outcomes.contains(&decision.outcome) {
                outcomes.push(decision.outcome);
            }
        }
        Ok(grouped
            .into_iter()
            .filter_map(|((subject, scenario), outcomes)| {
                (outcomes.len() > 1).then_some(DecisionConflict {
                    subject,
                    scenario,
                    outcomes,
                })
            })
            .collect())
    }

    pub fn list_entities(&self, workspace: &str) -> Result<Vec<Entity>, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, name, canonical_name, entity_type, aliases, workspace_id
             FROM entities
             WHERE workspace_id = ?1
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_entity)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)?;
        Ok(rows)
    }

    pub fn list_relations(&self, workspace: &str) -> Result<Vec<Relation>, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, subject_id, predicate, object_id, source_fact_id, workspace_id
             FROM relations
             WHERE workspace_id = ?1
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_relation)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)?;
        Ok(rows)
    }

    pub fn list_decisions(&self, workspace: &str) -> Result<Vec<Decision>, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, category, subject, scenario, reasoning,
                    outcome, confidence, decision_maker, issue_ref,
                    path, symbol, parent_id, workspace_id
             FROM decisions
             WHERE workspace_id = ?1
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_decision)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)?;
        Ok(rows)
    }

    pub fn attach_evidence(&self, spec: &EvidenceSpec) -> Result<Evidence, StoreError> {
        validate_evidence_spec(spec)?;
        if self.fact_by_id(spec.fact_id, &spec.workspace)?.is_none() {
            return Err(StoreError::Invalid(format!(
                "fact not found: {}",
                spec.fact_id
            )));
        }
        if let Some(fetched_at) = spec.fetched_at.as_deref() {
            self.validate_timestamp(fetched_at, "evidence fetched_at")?;
        }
        let selected_text_sha256 = sha256(&spec.selected_text);
        if let Some(existing) =
            self.evidence_by_key(spec.fact_id, &spec.source_ref, &spec.workspace)?
        {
            if existing.source != spec.source
                || existing.checksum != spec.checksum
                || existing.fetched_at != spec.fetched_at
                || existing.repository_ref != spec.repository_ref
                || existing.path != spec.path
                || existing.symbol != spec.symbol
                || existing.line_start != spec.line_start
                || existing.line_end != spec.line_end
                || existing.column_start != spec.column_start
                || existing.column_end != spec.column_end
                || existing.selected_text_sha256 != selected_text_sha256
                || existing.resolution_status != spec.resolution_status
            {
                return Err(StoreError::Invalid(
                    "evidence source ref conflicts with an existing record".to_owned(),
                ));
            }
            return Ok(existing);
        }
        self.connection.execute(
            "INSERT INTO evidence
                (fact_id, source_ref, source, checksum, fetched_at, repository_ref,
                 path, symbol, line_start, line_end, column_start, column_end,
                 selected_text_sha256, resolution_status, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                spec.fact_id,
                spec.source_ref,
                spec.source,
                spec.checksum,
                spec.fetched_at,
                spec.repository_ref,
                spec.path,
                spec.symbol,
                spec.line_start,
                spec.line_end,
                spec.column_start,
                spec.column_end,
                selected_text_sha256,
                spec.resolution_status,
                spec.workspace
            ],
        )?;
        self.evidence_by_key(spec.fact_id, &spec.source_ref, &spec.workspace)?
            .ok_or_else(|| {
                StoreError::Invalid("evidence insert did not produce a readable row".to_owned())
            })
    }

    pub fn get_provenance(
        &self,
        fact_id: i64,
        workspace: &str,
    ) -> Result<Vec<Evidence>, StoreError> {
        validate_context_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, fact_id, source_ref, source, checksum, fetched_at,
                    repository_ref, path, symbol, line_start, line_end,
                    column_start, column_end, selected_text_sha256,
                    resolution_status, workspace_id, created_at
             FROM evidence
             WHERE fact_id = ?1 AND workspace_id = ?2
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![fact_id, workspace], map_evidence)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)?;
        Ok(rows)
    }

    /// Return only bounded provenance counters for retrieval policy checks.
    /// Unlike get_provenance, this accepts the shared empty workspace used by
    /// facts and applies the normal shared-pool visibility rule.
    pub fn fact_evidence_summary(
        &self,
        fact_id: i64,
        workspace: &str,
    ) -> Result<FactEvidenceSummary, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT resolution_status
             FROM evidence
             WHERE fact_id = ?1 AND (workspace_id = '' OR workspace_id = ?2)
             ORDER BY id",
        )?;
        let statuses = statement
            .query_map(params![fact_id, workspace], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)?;
        let mut summary = FactEvidenceSummary {
            total: statuses.len(),
            ..FactEvidenceSummary::default()
        };
        for status in statuses {
            match status.as_str() {
                "resolved" => summary.resolved += 1,
                "stale" => summary.stale += 1,
                _ => summary.unresolved += 1,
            }
        }
        Ok(summary)
    }

    pub fn list_evidence(&self, workspace: &str) -> Result<Vec<Evidence>, StoreError> {
        validate_context_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT id, fact_id, source_ref, source, checksum, fetched_at,
                    repository_ref, path, symbol, line_start, line_end,
                    column_start, column_end, selected_text_sha256,
                    resolution_status, workspace_id, created_at
             FROM evidence
             WHERE workspace_id = ?1
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_evidence)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)?;
        Ok(rows)
    }

    pub fn export_snapshot(&self, workspace: &str) -> Result<MemoryExport, StoreError> {
        validate_context_workspace(workspace)?;
        Ok(MemoryExport {
            facts: self.list_facts(workspace)?,
            contexts: self.list_contexts(workspace)?,
            events: self.list_events(workspace)?,
            handoffs: self.list_handoffs(workspace)?,
            entities: self.list_entities(workspace)?,
            relations: self.list_relations(workspace)?,
            decisions: self.list_decisions(workspace)?,
            evidence: self.list_evidence(workspace)?,
            categories: self.list_categories(workspace)?,
            runs: self.query_runs("", workspace)?,
            measurements: self.query_measurements("", workspace)?,
            feedback: self.query_feedback("", workspace)?,
        })
    }

    /// Export every workspace for the migration/backup tool.  The public
    /// workspace export remains scoped, while the no-argument Python
    /// `export` contract intentionally includes all fact rows, including
    /// forgotten/invalid rows retained for migration history.
    pub fn export_all(&self) -> Result<MemoryExport, StoreError> {
        let mut workspaces = Vec::new();
        let mut statement = self.connection.prepare(
            "SELECT id FROM workspaces
             UNION SELECT DISTINCT workspace_id FROM facts WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM contexts WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM lifecycle_events WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM handoffs WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM entities WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM relations WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM decisions WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM evidence WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM categories WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM runs WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM measurement_observations WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM memory_feedback WHERE workspace_id <> ''
             ORDER BY id",
        )?;
        for row in statement.query_map([], |row| row.get::<_, String>(0))? {
            let workspace = row?;
            if !workspace.is_empty() && !workspaces.contains(&workspace) {
                workspaces.push(workspace);
            }
        }

        let mut export = MemoryExport {
            facts: {
                let mut statement = self.connection.prepare(
                    "SELECT id, text, sha256, workspace_id, lifecycle,
                            source, project, domain, trust, strong, importance, category_id,
                            validity, session_id, access_count
                     FROM facts ORDER BY id",
                )?;
                let rows = statement
                    .query_map([], map_fact)?
                    .collect::<Result<Vec<_>, _>>()?;
                rows
            },
            contexts: Vec::new(),
            events: Vec::new(),
            handoffs: Vec::new(),
            entities: Vec::new(),
            relations: Vec::new(),
            decisions: Vec::new(),
            evidence: Vec::new(),
            categories: Vec::new(),
            runs: Vec::new(),
            measurements: Vec::new(),
            feedback: Vec::new(),
        };
        for workspace in workspaces {
            let snapshot = self.export_snapshot(&workspace)?;
            export.contexts.extend(snapshot.contexts);
            export.events.extend(snapshot.events);
            export.handoffs.extend(snapshot.handoffs);
            export.entities.extend(snapshot.entities);
            export.relations.extend(snapshot.relations);
            export.decisions.extend(snapshot.decisions);
            export.evidence.extend(snapshot.evidence);
            export.categories.extend(snapshot.categories);
            export.runs.extend(snapshot.runs);
            export.measurements.extend(snapshot.measurements);
            export.feedback.extend(snapshot.feedback);
        }
        Ok(export)
    }

    pub fn export_rdf(&self, workspace: &str) -> Result<String, StoreError> {
        validate_graph_workspace(workspace)?;
        let mut statement = self.connection.prepare(
            "SELECT subject.name, relations.predicate, object.name
             FROM relations
             JOIN entities subject ON subject.id = relations.subject_id
             JOIN entities object ON object.id = relations.object_id
             WHERE relations.workspace_id = ?1
             ORDER BY relations.id",
        )?;
        let rows = statement.query_map(params![workspace], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut output = String::new();
        for row in rows {
            let (subject, predicate, object) = row?;
            output.push_str(&format!(
                "<{}> <{}> <{}> .\n",
                escape_rdf(&subject),
                escape_rdf(&predicate),
                escape_rdf(&object)
            ));
        }
        Ok(output)
    }

    pub fn stats(&self) -> Result<Stats, StoreError> {
        let facts = self
            .connection
            .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))?;
        let contexts = self
            .connection
            .query_row("SELECT COUNT(*) FROM contexts", [], |row| row.get(0))?;
        Ok(Stats { facts, contexts })
    }

    pub fn forget_fact(&self, id: i64, workspace: &str) -> Result<Option<Fact>, StoreError> {
        self.update_fact_lifecycle(id, workspace, "forgotten")
    }

    pub fn restore_fact(&self, id: i64, workspace: &str) -> Result<Option<Fact>, StoreError> {
        self.update_fact_lifecycle(id, workspace, "active")
    }

    pub fn list_forgotten(&self, workspace: &str) -> Result<Vec<Fact>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, text, sha256, workspace_id, lifecycle,
                    source, project, domain, trust, strong, importance, category_id,
                    validity, session_id, access_count
             FROM facts
             WHERE (workspace_id = '' OR workspace_id = ?1)
               AND lifecycle = 'forgotten'
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_fact)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn verify_facts(&self, workspace: &str) -> Result<FactVerification, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, text, sha256
             FROM facts
             WHERE workspace_id = '' OR workspace_id = ?1
             ORDER BY id",
        )?;
        let rows = statement.query_map(params![workspace], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut checked = 0i64;
        let mut invalid_ids = Vec::new();
        for row in rows {
            let (id, text, expected_hash) = row?;
            checked += 1;
            if sha256(&text) != expected_hash {
                invalid_ids.push(id);
            }
        }
        Ok(FactVerification {
            checked,
            valid: invalid_ids.is_empty(),
            invalid_ids,
        })
    }

    pub fn chunk_fact(
        &self,
        id: i64,
        max_bytes: usize,
        workspace: &str,
    ) -> Result<Vec<FactChunk>, StoreError> {
        if max_bytes == 0 {
            return Err(StoreError::Invalid(
                "fact chunk size must be positive".to_owned(),
            ));
        }
        let fact = self
            .fact_by_id(id, workspace)?
            .ok_or_else(|| StoreError::Invalid(format!("fact not found: {id}")))?;
        let chunks = split_utf8_chunks(&fact.text, max_bytes)?;
        let total = chunks.len() as i64;
        Ok(chunks
            .into_iter()
            .enumerate()
            .map(|(index, content)| FactChunk {
                fact_id: id,
                index: index as i64,
                total,
                byte_size: content.len() as i64,
                content,
            })
            .collect())
    }

    pub fn search_semantic(&self, query: &str, workspace: &str) -> Result<Vec<Fact>, StoreError> {
        self.search_facts(query, workspace)
    }

    pub fn compose_recall(&self, query: &str, workspace: &str) -> Result<Recall, StoreError> {
        validate_context_workspace(workspace)?;
        Ok(Recall {
            facts: self.search_facts(query, workspace)?,
            contexts: self.search_contexts(query, workspace)?,
        })
    }

    pub fn search_index(&self, query: &str, workspace: &str) -> Result<Recall, StoreError> {
        self.compose_recall(query, workspace)
    }

    pub fn create_workspace(&self, id: &str) -> Result<Workspace, StoreError> {
        validate_workspace(id)?;
        self.connection.execute(
            "INSERT OR IGNORE INTO workspaces (id) VALUES (?1)",
            params![id],
        )?;
        self.workspace_by_id(id)?.ok_or_else(|| {
            StoreError::Invalid("workspace insert did not produce a readable row".to_owned())
        })
    }

    pub fn list_workspaces(&self) -> Result<Vec<Workspace>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, status FROM workspaces ORDER BY id")?;
        let rows = statement
            .query_map([], map_workspace)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Return every workspace identifier represented by the store, including
    /// legacy/imported rows that predate a row in the `workspaces` catalog.
    /// Native Redis projection and recovery must not omit those implicit
    /// workspaces when rebuilding or checkpointing the compatibility image.
    pub fn list_workspace_ids(&self) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id FROM workspaces
             UNION SELECT DISTINCT workspace_id FROM facts WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM contexts WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM lifecycle_events WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM handoffs WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM entities WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM relations WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM decisions WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM evidence WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM categories WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM runs WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM measurement_observations WHERE workspace_id <> ''
             UNION SELECT DISTINCT workspace_id FROM memory_feedback WHERE workspace_id <> ''
             ORDER BY id",
        )?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn archive_workspace(&self, id: &str) -> Result<Option<Workspace>, StoreError> {
        validate_workspace(id)?;
        self.connection.execute(
            "UPDATE workspaces
             SET status = 'archived', updated_at = CURRENT_TIMESTAMP
             WHERE id = ?1",
            params![id],
        )?;
        self.workspace_by_id(id)
    }

    pub fn reset_workspace(&self, id: &str) -> Result<Workspace, StoreError> {
        validate_workspace(id)?;
        self.connection
            .execute("DELETE FROM facts WHERE workspace_id = ?1", params![id])?;
        self.connection
            .execute("DELETE FROM contexts WHERE workspace_id = ?1", params![id])?;
        self.connection.execute(
            "INSERT INTO workspaces (id, status, updated_at) VALUES (?1, 'reset', CURRENT_TIMESTAMP)
             ON CONFLICT(id) DO UPDATE SET status = 'reset', updated_at = CURRENT_TIMESTAMP",
            params![id],
        )?;
        self.workspace_by_id(id)?.ok_or_else(|| {
            StoreError::Invalid("workspace reset did not produce a readable row".to_owned())
        })
    }

    fn update_fact_lifecycle(
        &self,
        id: i64,
        workspace: &str,
        lifecycle: &str,
    ) -> Result<Option<Fact>, StoreError> {
        if id <= 0 {
            return Err(StoreError::Invalid("fact id must be positive".to_owned()));
        }
        if !matches!(lifecycle, "active" | "degraded" | "forgotten") {
            return Err(StoreError::Invalid(
                "fact lifecycle must be active, degraded, or forgotten".to_owned(),
            ));
        }
        let Some(existing) = self.fact_by_id(id, workspace)? else {
            return Ok(None);
        };
        if existing.lifecycle == lifecycle {
            let archived = i64::from(lifecycle == "forgotten");
            self.connection.execute(
                "UPDATE facts SET archived = ?1, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?2 AND (workspace_id = '' OR workspace_id = ?3)",
                params![archived, id, workspace],
            )?;
            return Ok(Some(existing));
        }
        self.connection.execute(
            "UPDATE facts SET lifecycle = ?1,
                    archived = CASE WHEN ?1 = 'forgotten' THEN 1 ELSE 0 END,
                    updated_at = CURRENT_TIMESTAMP
             WHERE id = ?2 AND (workspace_id = '' OR workspace_id = ?3)",
            params![lifecycle, id, workspace],
        )?;
        self.record_fact_history(
            id,
            "lifecycle_changed",
            &existing.lifecycle,
            lifecycle,
            "lifecycle updated",
            &existing.workspace,
        )?;
        self.fact_by_id(id, workspace)
    }

    fn fact_by_hash(&self, hash: &str, workspace: &str) -> Result<Option<Fact>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, text, sha256, workspace_id, lifecycle,
                        source, project, domain, trust, strong, importance, category_id,
                        validity, session_id, access_count
                 FROM facts
                 WHERE sha256 = ?1 AND workspace_id = ?2",
                params![hash, workspace],
                map_fact,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn fact_by_id(&self, id: i64, workspace: &str) -> Result<Option<Fact>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, text, sha256, workspace_id, lifecycle,
                        source, project, domain, trust, strong, importance, category_id,
                        validity, session_id, access_count
                 FROM facts
                 WHERE id = ?1 AND (workspace_id = '' OR workspace_id = ?2)",
                params![id, workspace],
                map_fact,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn workspace_by_id(&self, id: &str) -> Result<Option<Workspace>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, status FROM workspaces WHERE id = ?1",
                params![id],
                map_workspace,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn entity_by_name(&self, name: &str, workspace: &str) -> Result<Option<Entity>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, name, canonical_name, entity_type, aliases, workspace_id
                 FROM entities
                 WHERE name = ?1 AND workspace_id = ?2",
                params![name, workspace],
                map_entity,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn entity_by_id(&self, id: i64, workspace: &str) -> Result<Option<Entity>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, name, canonical_name, entity_type, aliases, workspace_id
                 FROM entities
                 WHERE id = ?1 AND workspace_id = ?2",
                params![id, workspace],
                map_entity,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn entity_id_for_reference(
        &self,
        reference: &str,
        workspace: &str,
    ) -> Result<Option<i64>, StoreError> {
        if let Ok(id) = reference.parse::<i64>() {
            if let Some(entity) = self.entity_by_id(id, workspace)? {
                return Ok(Some(entity.id));
            }
        }
        if let Some(entity) = self.entity_by_name(reference, workspace)? {
            return Ok(Some(entity.id));
        }
        self.connection
            .query_row(
                "SELECT id
                 FROM entities
                 WHERE canonical_name = ?1 AND workspace_id = ?2",
                params![canonical_name(reference), workspace],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn relation_by_key(
        &self,
        subject_id: i64,
        predicate: &str,
        object_id: i64,
        workspace: &str,
    ) -> Result<Option<Relation>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, subject_id, predicate, object_id, source_fact_id, workspace_id
                 FROM relations
                 WHERE subject_id = ?1 AND predicate = ?2 AND object_id = ?3
                   AND workspace_id = ?4",
                params![subject_id, predicate, object_id, workspace],
                map_relation,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn decision_by_id(&self, id: i64, workspace: &str) -> Result<Option<Decision>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, category, subject, scenario, reasoning,
                        outcome, confidence, decision_maker, issue_ref,
                        path, symbol, parent_id, workspace_id
                 FROM decisions
                 WHERE id = ?1 AND workspace_id = ?2",
                params![id, workspace],
                map_decision,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn evidence_by_key(
        &self,
        fact_id: i64,
        source_ref: &str,
        workspace: &str,
    ) -> Result<Option<Evidence>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, fact_id, source_ref, source, checksum, fetched_at,
                        repository_ref, path, symbol, line_start, line_end,
                        column_start, column_end, selected_text_sha256,
                        resolution_status, workspace_id, created_at
                 FROM evidence
                 WHERE fact_id = ?1 AND source_ref = ?2 AND workspace_id = ?3",
                params![fact_id, source_ref, workspace],
                map_evidence,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn validate_timestamp(&self, value: &str, label: &str) -> Result<(), StoreError> {
        let parsed: Option<f64> =
            self.connection
                .query_row("SELECT julianday(?1)", params![value], |row| row.get(0))?;
        if parsed.is_none() {
            return Err(StoreError::Invalid(format!(
                "{label} is not a valid timestamp"
            )));
        }
        Ok(())
    }

    fn event_by_key(
        &self,
        idempotency_key: &str,
        workspace: &str,
    ) -> Result<Option<LifecycleEvent>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, idempotency_key, event_type, context_ref, metadata,
                        payload_sha256, payload_size, payload_truncated,
                        workspace_id, created_at
                 FROM lifecycle_events
                 WHERE idempotency_key = ?1 AND workspace_id = ?2",
                params![idempotency_key, workspace],
                map_lifecycle_event,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn event_by_context(
        &self,
        context_reference: &str,
        workspace: &str,
    ) -> Result<Option<LifecycleEvent>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, idempotency_key, event_type, context_ref, metadata,
                        payload_sha256, payload_size, payload_truncated,
                        workspace_id, created_at
                 FROM lifecycle_events
                 WHERE context_ref = ?1 AND workspace_id = ?2",
                params![context_reference, workspace],
                map_lifecycle_event,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn handoff_expiry(&self, spec: &HandoffSpec) -> Result<Option<String>, StoreError> {
        if spec.ttl_seconds.is_some() && spec.expires_at.is_some() {
            return Err(StoreError::Invalid(
                "handoff cannot specify both ttl_seconds and expires_at".to_owned(),
            ));
        }
        if let Some(expires_at) = spec.expires_at.as_deref() {
            self.validate_timestamp(expires_at, "handoff expiry")?;
            return Ok(Some(expires_at.to_owned()));
        }
        let Some(ttl_seconds) = spec.ttl_seconds else {
            return Ok(None);
        };
        if ttl_seconds < 0 {
            return Err(StoreError::Invalid(
                "handoff ttl_seconds must not be negative".to_owned(),
            ));
        }
        let modifier = format!("+{ttl_seconds} seconds");
        let expires_at: Option<String> =
            self.connection
                .query_row("SELECT datetime('now', ?1)", params![modifier], |row| {
                    row.get(0)
                })?;
        expires_at
            .ok_or_else(|| {
                StoreError::Invalid("handoff ttl_seconds produced an invalid expiry".to_owned())
            })
            .map(Some)
    }

    fn handoff_by_key(
        &self,
        idempotency_key: &str,
        workspace: &str,
    ) -> Result<Option<Handoff>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, idempotency_key, context_ref, owner, session, source,
                        workspace_id, shared, expires_at, state, accepted_at,
                        accepted_by, cancelled_at, cancelled_by, created_at
                 FROM handoffs
                 WHERE idempotency_key = ?1 AND workspace_id = ?2",
                params![idempotency_key, workspace],
                map_handoff,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn handoff_by_context(
        &self,
        context_reference: &str,
        workspace: &str,
    ) -> Result<Option<Handoff>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, idempotency_key, context_ref, owner, session, source,
                        workspace_id, shared, expires_at, state, accepted_at,
                        accepted_by, cancelled_at, cancelled_by, created_at
                 FROM handoffs
                 WHERE context_ref = ?1 AND workspace_id = ?2",
                params![context_reference, workspace],
                map_handoff,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn refresh_expired_handoffs(&self, workspace: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE handoffs
             SET state = 'expired'
             WHERE workspace_id = ?1
               AND state = 'open'
               AND expires_at IS NOT NULL
               AND julianday(expires_at) <= julianday('now')",
            params![workspace],
        )?;
        Ok(())
    }
}

pub fn default_path() -> PathBuf {
    std::env::var_os("MEMORY_MCP_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/facts.db"))
}

fn temporary_snapshot_path(kind: &str) -> PathBuf {
    let sequence = SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "memory-mcp-rust-{kind}-{}-{sequence}.db",
        std::process::id()
    ))
}

fn create_private_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn atomic_private_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| StoreError::Invalid("private file directory is missing".to_owned()))?;
    let sequence = SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        ".{}-{}-{sequence}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("backup"),
        std::process::id()
    ));
    let result = create_private_file(&temporary, bytes)
        .and_then(|_| set_private_file_mode(&temporary))
        .and_then(|_| fs::rename(&temporary, path).map_err(StoreError::from));
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

fn set_private_file_mode(path: &Path) -> Result<(), StoreError> {
    #[cfg(unix)]
    fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    Ok(())
}

fn set_private_directory_mode(path: &Path) -> Result<(), StoreError> {
    #[cfg(unix)]
    fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
    Ok(())
}

fn database_root_for_path(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(
            || PathBuf::from("databases"),
            |parent| parent.join("databases"),
        )
}

fn validate_database_name(name: &str) -> Result<(), StoreError> {
    if name.is_empty() || name.trim() != name {
        return Err(StoreError::Invalid(
            "database name must not be empty or padded with whitespace".to_owned(),
        ));
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        return Err(StoreError::Invalid(
            "database name may contain only ASCII letters, digits, '.', '_' or '-'".to_owned(),
        ));
    }
    if matches!(name, "." | "..") {
        return Err(StoreError::Invalid(
            "database name must not be a path traversal component".to_owned(),
        ));
    }
    Ok(())
}

fn archived_database_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.archived", path.to_string_lossy()))
}

fn database_path_kind(path: &Path) -> Option<bool> {
    let name = path.file_name()?.to_str()?;
    if name.ends_with(".db.archived") {
        Some(true)
    } else if name.ends_with(".db") {
        Some(false)
    } else {
        None
    }
}

fn database_name_from_path(path: &Path, archived: bool) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    let suffix = if archived { ".db.archived" } else { ".db" };
    file_name
        .strip_suffix(suffix)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
}

fn same_database_path(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn sha256(text: &str) -> String {
    encode(Sha256::digest(text.as_bytes()))
}

fn map_fact(row: &rusqlite::Row<'_>) -> rusqlite::Result<Fact> {
    Ok(Fact {
        id: row.get(0)?,
        text: row.get(1)?,
        sha256: row.get(2)?,
        workspace: row.get(3)?,
        lifecycle: row.get(4)?,
        source: row.get(5)?,
        project: row.get(6)?,
        domain: row.get(7)?,
        trust: row.get(8)?,
        strong: row.get::<_, i64>(9)? != 0,
        importance: row.get(10)?,
        category_id: row.get(11)?,
        validity: row.get(12)?,
        session_id: row.get(13)?,
        access_count: row.get(14)?,
    })
}

fn map_category(row: &rusqlite::Row<'_>) -> rusqlite::Result<Category> {
    Ok(Category {
        id: row.get(0)?,
        name: row.get(1)?,
        workspace: row.get(2)?,
        created_at: row.get(3)?,
    })
}

fn map_fact_history(row: &rusqlite::Row<'_>) -> rusqlite::Result<FactHistory> {
    Ok(FactHistory {
        id: row.get(0)?,
        fact_id: row.get(1)?,
        event: row.get(2)?,
        from_lifecycle: row.get(3)?,
        to_lifecycle: row.get(4)?,
        note: row.get(5)?,
        workspace: row.get(6)?,
        created_at: row.get(7)?,
    })
}

fn map_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<Run> {
    Ok(Run {
        id: row.get(0)?,
        run_id: row.get(1)?,
        issue_ref: row.get(2)?,
        pr_ref: row.get(3)?,
        session: row.get(4)?,
        git_ref: row.get(5)?,
        files: row.get(6)?,
        diff: row.get(7)?,
        summary: row.get(8)?,
        state: row.get(9)?,
        workspace: row.get(10)?,
        created_at: row.get(11)?,
        ended_at: row.get(12)?,
    })
}

fn map_measurement(row: &rusqlite::Row<'_>) -> rusqlite::Result<Measurement> {
    Ok(Measurement {
        id: row.get(0)?,
        measurement: row.get(1)?,
        sample: row.get(2)?,
        variant: row.get(3)?,
        value: row.get(4)?,
        baseline: row.get::<_, i64>(5)? != 0,
        workspace: row.get(6)?,
        created_at: row.get(7)?,
    })
}

fn map_feedback(row: &rusqlite::Row<'_>) -> rusqlite::Result<Feedback> {
    Ok(Feedback {
        id: row.get(0)?,
        feedback_id: row.get(1)?,
        site: row.get(2)?,
        item_type: row.get(3)?,
        item_ref: row.get(4)?,
        signal: row.get(5)?,
        query_hash: row.get(6)?,
        workspace: row.get(7)?,
        created_at: row.get(8)?,
    })
}

fn map_entity(row: &rusqlite::Row<'_>) -> rusqlite::Result<Entity> {
    let aliases_json: String = row.get(4)?;
    Ok(Entity {
        id: row.get(0)?,
        name: row.get(1)?,
        canonical_name: row.get(2)?,
        entity_type: row.get(3)?,
        aliases: serde_json::from_str(&aliases_json).unwrap_or_default(),
        workspace: row.get(5)?,
    })
}

fn map_relation(row: &rusqlite::Row<'_>) -> rusqlite::Result<Relation> {
    Ok(Relation {
        id: row.get(0)?,
        subject_id: row.get(1)?,
        predicate: row.get(2)?,
        object_id: row.get(3)?,
        source_fact_id: row.get(4)?,
        workspace: row.get(5)?,
    })
}

fn map_decision(row: &rusqlite::Row<'_>) -> rusqlite::Result<Decision> {
    Ok(Decision {
        id: row.get(0)?,
        category: row.get(1)?,
        subject: row.get(2)?,
        scenario: row.get(3)?,
        reasoning: row.get(4)?,
        outcome: row.get(5)?,
        confidence: row.get(6)?,
        decision_maker: row.get(7)?,
        issue_ref: row.get(8)?,
        path: row.get(9)?,
        symbol: row.get(10)?,
        parent_id: row.get(11)?,
        workspace: row.get(12)?,
    })
}

fn map_evidence(row: &rusqlite::Row<'_>) -> rusqlite::Result<Evidence> {
    Ok(Evidence {
        id: row.get(0)?,
        fact_id: row.get(1)?,
        source_ref: row.get(2)?,
        source: row.get(3)?,
        checksum: row.get(4)?,
        fetched_at: row.get(5)?,
        repository_ref: row.get(6)?,
        path: row.get(7)?,
        symbol: row.get(8)?,
        line_start: row.get(9)?,
        line_end: row.get(10)?,
        column_start: row.get(11)?,
        column_end: row.get(12)?,
        selected_text_sha256: row.get(13)?,
        resolution_status: row.get(14)?,
        workspace: row.get(15)?,
        created_at: row.get(16)?,
    })
}

fn map_workspace(row: &rusqlite::Row<'_>) -> rusqlite::Result<Workspace> {
    Ok(Workspace {
        id: row.get(0)?,
        status: row.get(1)?,
    })
}

fn validate_workspace(id: &str) -> Result<(), StoreError> {
    if id.trim().is_empty() {
        return Err(StoreError::Invalid(
            "workspace id must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_graph_workspace(workspace: &str) -> Result<(), StoreError> {
    if workspace.trim() != workspace {
        return Err(StoreError::Invalid(
            "graph workspace must not have surrounding whitespace".to_owned(),
        ));
    }
    Ok(())
}

fn map_context(row: &rusqlite::Row<'_>) -> rusqlite::Result<Context> {
    Ok(Context {
        reference: row.get(0)?,
        name: row.get(1)?,
        content: row.get(2)?,
        sha256: row.get(3)?,
        workspace: row.get(4)?,
        schema: row.get(5)?,
        source: row.get(6)?,
        expires_at: row.get(7)?,
        byte_size: row.get(8)?,
    })
}

fn map_context_lineage(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContextLineage> {
    Ok(ContextLineage {
        parent_reference: row.get(0)?,
        child_reference: row.get(1)?,
        relation: row.get(2)?,
        workspace: row.get(3)?,
    })
}

fn map_lifecycle_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<LifecycleEvent> {
    Ok(LifecycleEvent {
        id: row.get(0)?,
        idempotency_key: row.get(1)?,
        event_type: row.get(2)?,
        context_reference: row.get(3)?,
        metadata: row.get(4)?,
        payload_sha256: row.get(5)?,
        payload_size: row.get(6)?,
        payload_truncated: row.get::<_, i64>(7)? != 0,
        workspace: row.get(8)?,
        created_at: row.get(9)?,
    })
}

fn map_handoff(row: &rusqlite::Row<'_>) -> rusqlite::Result<Handoff> {
    Ok(Handoff {
        id: row.get(0)?,
        idempotency_key: row.get(1)?,
        context_reference: row.get(2)?,
        owner: row.get(3)?,
        session: row.get(4)?,
        source: row.get(5)?,
        workspace: row.get(6)?,
        shared: row.get::<_, i64>(7)? != 0,
        expires_at: row.get(8)?,
        state: row.get(9)?,
        accepted_at: row.get(10)?,
        accepted_by: row.get(11)?,
        cancelled_at: row.get(12)?,
        cancelled_by: row.get(13)?,
        created_at: row.get(14)?,
    })
}

fn validate_fact_metadata(metadata: &FactMetadata) -> Result<(), StoreError> {
    if !matches!(metadata.trust.as_str(), "high" | "medium" | "low") {
        return Err(StoreError::Invalid(
            "fact trust must be high, medium, or low".to_owned(),
        ));
    }
    if !metadata.importance.is_finite() || !(0.0..=1.0).contains(&metadata.importance) {
        return Err(StoreError::Invalid(
            "fact importance must be between 0 and 1".to_owned(),
        ));
    }
    Ok(())
}

fn validate_run_key(run_id: &str, workspace: &str) -> Result<(), StoreError> {
    validate_graph_workspace(workspace)?;
    if run_id.trim().is_empty() {
        return Err(StoreError::Invalid("run_id must not be empty".to_owned()));
    }
    Ok(())
}

fn validate_run_spec(spec: &RunSpec) -> Result<(), StoreError> {
    validate_run_key(&spec.run_id, &spec.workspace)
}

fn validate_measurement_spec(spec: &MeasurementSpec) -> Result<(), StoreError> {
    validate_graph_workspace(&spec.workspace)?;
    if spec.measurement.trim().is_empty() {
        return Err(StoreError::Invalid(
            "measurement name must not be empty".to_owned(),
        ));
    }
    if spec.sample.trim().is_empty() {
        return Err(StoreError::Invalid(
            "measurement sample must not be empty".to_owned(),
        ));
    }
    if !spec.value.is_finite() {
        return Err(StoreError::Invalid(
            "measurement value must be finite".to_owned(),
        ));
    }
    Ok(())
}

fn validate_feedback_spec(spec: &FeedbackSpec) -> Result<(), StoreError> {
    validate_graph_workspace(&spec.workspace)?;
    if spec.feedback_id.trim().is_empty() {
        return Err(StoreError::Invalid(
            "feedback_id must not be empty".to_owned(),
        ));
    }
    if spec.item_type.trim().is_empty() || spec.item_ref.trim().is_empty() {
        return Err(StoreError::Invalid(
            "feedback item_type and item_ref must not be empty".to_owned(),
        ));
    }
    if !matches!(
        spec.signal.as_str(),
        "helpful" | "not_helpful" | "stale" | "irrelevant" | "unsafe"
    ) {
        return Err(StoreError::Invalid(
            "feedback signal must be helpful, not_helpful, stale, irrelevant, or unsafe".to_owned(),
        ));
    }
    Ok(())
}

fn validate_fact_filters(filters: &FactFilters) -> Result<(), StoreError> {
    if let Some(trust) = filters.trust.as_deref() {
        if !matches!(trust, "high" | "medium" | "low") {
            return Err(StoreError::Invalid(
                "fact trust filter must be high, medium, or low".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_entity_spec(spec: &EntitySpec) -> Result<(), StoreError> {
    validate_graph_workspace(&spec.workspace)?;
    if spec.name.trim().is_empty() {
        return Err(StoreError::Invalid(
            "entity name must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_relation_spec(spec: &RelationSpec) -> Result<(), StoreError> {
    validate_graph_workspace(&spec.workspace)?;
    for (value, label) in [
        (&spec.subject, "relation subject"),
        (&spec.predicate, "relation predicate"),
        (&spec.object, "relation object"),
    ] {
        if value.trim().is_empty() {
            return Err(StoreError::Invalid(format!("{label} must not be empty")));
        }
    }
    if spec.source_fact_id.is_some_and(|id| id <= 0) {
        return Err(StoreError::Invalid(
            "relation source fact id must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_decision_spec(spec: &DecisionSpec) -> Result<(), StoreError> {
    validate_graph_workspace(&spec.workspace)?;
    for (value, label) in [
        (&spec.subject, "decision subject"),
        (&spec.scenario, "decision scenario"),
        (&spec.outcome, "decision outcome"),
    ] {
        if value.trim().is_empty() {
            return Err(StoreError::Invalid(format!("{label} must not be empty")));
        }
    }
    if spec
        .confidence
        .is_some_and(|confidence| !confidence.is_finite() || !(0.0..=1.0).contains(&confidence))
    {
        return Err(StoreError::Invalid(
            "decision confidence must be between 0 and 1".to_owned(),
        ));
    }
    Ok(())
}

fn canonical_name(name: &str) -> String {
    name.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn validate_evidence_spec(spec: &EvidenceSpec) -> Result<(), StoreError> {
    validate_context_workspace(&spec.workspace)?;
    if spec.fact_id <= 0 {
        return Err(StoreError::Invalid(
            "evidence fact id must be positive".to_owned(),
        ));
    }
    for (value, label) in [
        (&spec.source_ref, "evidence source ref"),
        (&spec.resolution_status, "evidence resolution status"),
    ] {
        if value.trim().is_empty() {
            return Err(StoreError::Invalid(format!("{label} must not be empty")));
        }
    }
    for (start, end, label) in [
        (spec.line_start, spec.line_end, "line"),
        (spec.column_start, spec.column_end, "column"),
    ] {
        if start.is_some_and(|value| value < 0) || end.is_some_and(|value| value < 0) {
            return Err(StoreError::Invalid(format!(
                "evidence {label} offsets must not be negative"
            )));
        }
        if let (Some(start), Some(end)) = (start, end) {
            if end < start {
                return Err(StoreError::Invalid(format!(
                    "evidence {label} end must not precede start"
                )));
            }
        }
    }
    Ok(())
}

fn escape_rdf(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn split_utf8_chunks(content: &str, max_bytes: usize) -> Result<Vec<String>, StoreError> {
    if max_bytes == 0 {
        return Err(StoreError::Invalid(
            "chunk size must be positive".to_owned(),
        ));
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_size = 0usize;
    for character in content.chars() {
        let character_size = character.len_utf8();
        if character_size > max_bytes {
            return Err(StoreError::Invalid(
                "chunk size is smaller than one UTF-8 character".to_owned(),
            ));
        }
        if current_size > 0 && current_size + character_size > max_bytes {
            chunks.push(current);
            current = String::new();
            current_size = 0;
        }
        current.push(character);
        current_size += character_size;
    }
    if !current.is_empty() || chunks.is_empty() {
        chunks.push(current);
    }
    Ok(chunks)
}

fn truncate_utf8(content: &str, max_bytes: usize) -> String {
    if content.len() <= max_bytes {
        return content.to_owned();
    }
    let end = content
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= max_bytes)
        .last()
        .unwrap_or(0);
    content[..end].to_owned()
}

fn validate_fact_text(text: &str) -> Result<(), StoreError> {
    if text.trim().is_empty() {
        return Err(StoreError::Invalid(
            "fact text must not be empty".to_owned(),
        ));
    }
    if text.chars().count() > MAX_FACT_TEXT_CHARS {
        return Err(StoreError::Invalid(format!(
            "fact text exceeds the configured limit ({MAX_FACT_TEXT_CHARS} characters)"
        )));
    }
    Ok(())
}

fn configured_context_max_bytes() -> Result<usize, StoreError> {
    let Some(value) = std::env::var_os("MEMORY_MCP_CONTEXT_MAX_BYTES") else {
        return Ok(DEFAULT_CONTEXT_MAX_BYTES);
    };
    let value = value.to_str().ok_or_else(|| {
        StoreError::Invalid("MEMORY_MCP_CONTEXT_MAX_BYTES must be valid UTF-8".to_owned())
    })?;
    let value = value.parse::<usize>().map_err(|_| {
        StoreError::Invalid("MEMORY_MCP_CONTEXT_MAX_BYTES must be a positive integer".to_owned())
    })?;
    if !(1..=MAX_CONTEXT_MAX_BYTES).contains(&value) {
        return Err(StoreError::Invalid(format!(
            "MEMORY_MCP_CONTEXT_MAX_BYTES must be between 1 and {MAX_CONTEXT_MAX_BYTES}"
        )));
    }
    Ok(value)
}

fn validate_event_spec(spec: &EventSpec) -> Result<(), StoreError> {
    validate_context_workspace(&spec.workspace)?;
    for (value, label) in [
        (&spec.idempotency_key, "event idempotency key"),
        (&spec.event_type, "event type"),
        (&spec.context_reference, "event context ref"),
    ] {
        if value.trim().is_empty() {
            return Err(StoreError::Invalid(format!("{label} must not be empty")));
        }
    }
    if spec.metadata.len() > MAX_EVENT_METADATA_BYTES {
        return Err(StoreError::Invalid(
            "event metadata exceeds the configured size limit".to_owned(),
        ));
    }
    if spec.payload.len() > MAX_EVENT_PAYLOAD_BYTES {
        return Err(StoreError::Invalid(
            "event payload exceeds the configured size limit".to_owned(),
        ));
    }
    Ok(())
}

fn validate_handoff_spec(spec: &HandoffSpec) -> Result<(), StoreError> {
    validate_context_workspace(&spec.workspace)?;
    for (value, label) in [
        (&spec.idempotency_key, "handoff idempotency key"),
        (&spec.context_reference, "handoff context ref"),
        (&spec.owner, "handoff owner"),
    ] {
        if value.trim().is_empty() {
            return Err(StoreError::Invalid(format!("{label} must not be empty")));
        }
    }
    if spec.ttl_seconds.is_some_and(|ttl| ttl < 0) {
        return Err(StoreError::Invalid(
            "handoff ttl_seconds must not be negative".to_owned(),
        ));
    }
    if spec
        .expires_at
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(StoreError::Invalid(
            "handoff expiry must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_context_workspace(workspace: &str) -> Result<(), StoreError> {
    if workspace.trim().is_empty() {
        return Err(StoreError::Invalid(
            "context operations require a non-empty workspace".to_owned(),
        ));
    }
    Ok(())
}

fn validate_context(
    reference: &str,
    name: &str,
    workspace: &str,
    expires_at: Option<&str>,
) -> Result<(), StoreError> {
    validate_context_workspace(workspace)?;
    if reference.trim().is_empty() {
        return Err(StoreError::Invalid(
            "context ref must not be empty".to_owned(),
        ));
    }
    if name.trim().is_empty() {
        return Err(StoreError::Invalid(
            "context name must not be empty".to_owned(),
        ));
    }
    if expires_at.is_some_and(|value| value.trim().is_empty()) {
        return Err(StoreError::Invalid(
            "context expiry must not be empty".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_store_round_trips_facts_and_contexts() {
        let store = Store::in_memory().expect("fresh store");
        let first = store
            .remember_fact("SQLite is the deterministic fallback", "workspace-a")
            .expect("fact");
        let duplicate = store
            .remember_fact("SQLite is the deterministic fallback", "workspace-a")
            .expect("duplicate");
        assert_eq!(first, duplicate);
        assert_eq!(
            store
                .search_facts("deterministic", "workspace-a")
                .unwrap()
                .len(),
            1
        );

        let context = store
            .put_context("ctx-1", "Contract", "stdio JSON-RPC", "workspace-a")
            .expect("context");
        assert_eq!(
            store.context("ctx-1", "workspace-a").unwrap(),
            Some(context)
        );
        assert_eq!(
            store.stats().unwrap(),
            Stats {
                facts: 1,
                contexts: 1
            }
        );
    }

    #[test]
    fn fact_and_context_sizes_are_rejected_before_persistence() {
        let store = Store::in_memory().expect("fresh store");
        let long_fact = "x".repeat(MAX_FACT_TEXT_CHARS + 1);
        assert!(store.remember_fact(&long_fact, "workspace-a").is_err());

        let long_context = "x".repeat(DEFAULT_CONTEXT_MAX_BYTES + 1);
        assert!(store
            .put_context("too-large", "Too large", &long_context, "workspace-a")
            .is_err());
        assert!(store.context("too-large", "workspace-a").unwrap().is_none());
    }

    #[test]
    fn database_snapshot_round_trips_schema_fts_and_state() {
        let source = Store::in_memory().expect("source store");
        source
            .remember_fact("snapshot fact", "workspace-a")
            .expect("fact");
        source
            .put_context("snapshot-context", "Snapshot", "state", "workspace-a")
            .expect("context");
        source.create_workspace("workspace-a").expect("workspace");

        let snapshot = source.snapshot_bytes().expect("snapshot bytes");
        assert!(!snapshot.is_empty());

        let restored = Store::in_memory().expect("restored store");
        restored
            .restore_snapshot_bytes(&snapshot)
            .expect("restore bytes");
        assert_eq!(
            restored
                .search_facts("snapshot", "workspace-a")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            restored
                .context("snapshot-context", "workspace-a")
                .unwrap()
                .expect("restored context")
                .content,
            "state"
        );
        assert_eq!(
            restored.list_workspaces().unwrap(),
            vec![Workspace {
                id: "workspace-a".to_owned(),
                status: "active".to_owned()
            }]
        );
    }

    #[test]
    fn fact_and_workspace_lifecycle_preserve_isolation() {
        let store = Store::in_memory().expect("fresh store");
        assert_eq!(
            store.create_workspace("workspace-a").unwrap(),
            Workspace {
                id: "workspace-a".to_owned(),
                status: "active".to_owned()
            }
        );
        store.create_workspace("workspace-b").unwrap();
        let fact_a = store.remember_fact("fact in a", "workspace-a").unwrap();
        let fact_b = store.remember_fact("fact in b", "workspace-b").unwrap();

        let forgotten = store
            .forget_fact(fact_a.id, "workspace-a")
            .unwrap()
            .unwrap();
        assert_eq!(forgotten.lifecycle, "forgotten");
        assert!(store
            .search_facts("fact", "workspace-a")
            .unwrap()
            .is_empty());
        assert_eq!(
            store.list_forgotten("workspace-a").unwrap(),
            vec![forgotten]
        );
        assert_eq!(
            store.list_facts("workspace-b").unwrap(),
            vec![fact_b.clone()]
        );

        let restored = store
            .restore_fact(fact_a.id, "workspace-a")
            .unwrap()
            .unwrap();
        assert_eq!(restored.lifecycle, "active");
        assert_eq!(store.list_facts("workspace-a").unwrap(), vec![restored]);

        assert_eq!(
            store.archive_workspace("workspace-a").unwrap(),
            Some(Workspace {
                id: "workspace-a".to_owned(),
                status: "archived".to_owned()
            })
        );
        store
            .put_context("ctx-a", "A", "workspace a", "workspace-a")
            .unwrap();
        let reset = store.reset_workspace("workspace-a").unwrap();
        assert_eq!(reset.status, "reset");
        assert!(store.list_facts("workspace-a").unwrap().is_empty());
        assert!(store.list_contexts("workspace-a").unwrap().is_empty());
        assert_eq!(store.list_facts("workspace-b").unwrap(), vec![fact_b]);
        assert_eq!(store.list_workspaces().unwrap().len(), 2);
    }

    #[test]
    fn context_retrieval_chunking_expiry_and_lineage_are_deterministic() {
        let store = Store::in_memory().expect("fresh store");
        let metadata = ContextMetadata {
            schema: "text/plain".to_owned(),
            source: "design-note".to_owned(),
            expires_at: Some("2999-01-01T00:00:00Z".to_owned()),
        };
        let first = store
            .put_context_with_metadata(
                "ctx-a",
                "Architecture",
                "Rust SQLite context",
                &metadata,
                "workspace-a",
            )
            .expect("first context");
        let second = store
            .put_context("ctx-b", "Operations", "Container runner", "workspace-a")
            .expect("second context");
        let expired = store
            .put_context_with_metadata(
                "ctx-expired",
                "Expired",
                "old content",
                &ContextMetadata {
                    expires_at: Some("2000-01-01T00:00:00Z".to_owned()),
                    ..ContextMetadata::default()
                },
                "workspace-a",
            )
            .expect("expired context is stored");

        let reduced_metadata = ContextMetadata {
            schema: "text/plain".to_owned(),
            source: "reducer".to_owned(),
            ..ContextMetadata::default()
        };

        assert_eq!(first.byte_size, "Rust SQLite context".len() as i64);
        assert_eq!(first.schema, "text/plain");
        assert_eq!(first.source, "design-note");
        assert_eq!(first.expires_at.as_deref(), Some("2999-01-01T00:00:00Z"));
        assert_eq!(
            store
                .resolve_context("Architecture", "workspace-a")
                .unwrap(),
            Some(first.clone())
        );
        assert_eq!(store.context("ctx-a", "workspace-b").unwrap(), None);
        assert_eq!(
            store.search_contexts("runner", "workspace-a").unwrap(),
            vec![second]
        );
        assert_eq!(store.context("ctx-expired", "workspace-a").unwrap(), None);
        assert_eq!(store.list_contexts("workspace-a").unwrap().len(), 2);
        assert_eq!(expired.content, "old content");

        let chunks = store
            .chunk_context("ctx-a", 5, "workspace-a")
            .expect("UTF-8 chunks");
        assert!(chunks.iter().all(|chunk| chunk.byte_size <= 5));
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.content.as_str())
                .collect::<String>(),
            first.content
        );
        assert!(chunks.iter().enumerate().all(
            |(index, chunk)| chunk.index == index as i64 && chunk.total == chunks.len() as i64
        ));

        let reduced = store
            .reduce_context(
                &["ctx-a".to_owned(), "ctx-b".to_owned()],
                Some("ctx-reduced"),
                "Reduced",
                &reduced_metadata,
                "workspace-a",
            )
            .expect("reduced context");
        assert_eq!(reduced.content, "Rust SQLite context\n\nContainer runner");
        let lineage = store
            .context_map(Some("ctx-reduced"), "workspace-a")
            .unwrap();
        assert_eq!(lineage.len(), 2);
        assert!(lineage.iter().all(|entry| {
            entry.child_reference == "ctx-reduced" && entry.relation == "reduced_from"
        }));
        assert_eq!(store.context_map(None, "workspace-b").unwrap(), Vec::new());
    }

    #[test]
    fn ingestion_fact_chunking_and_composed_recall_are_workspace_scoped() {
        let store = Store::in_memory().expect("fresh store");
        let absorbed = store
            .absorb(
                &[
                    "Rust memory".to_owned(),
                    "Rust memory".to_owned(),
                    "SQLite index".to_owned(),
                ],
                "workspace-a",
            )
            .expect("absorbed facts");
        assert_eq!(absorbed.len(), 3);
        assert_eq!(absorbed[0].id, absorbed[1].id);
        assert_ne!(absorbed[0].id, absorbed[2].id);

        let turn = store
            .ingest_turn("память", "workspace-a")
            .expect("ingested turn");
        let chunks = store
            .chunk_fact(turn.id, 4, "workspace-a")
            .expect("UTF-8-safe fact chunks");
        assert!(chunks.iter().all(|chunk| chunk.byte_size <= 4));
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.content.as_str())
                .collect::<String>(),
            turn.text
        );
        assert!(chunks.iter().enumerate().all(|(index, chunk)| {
            chunk.fact_id == turn.id
                && chunk.index == index as i64
                && chunk.total == chunks.len() as i64
        }));
        assert!(store.chunk_fact(turn.id, 1, "workspace-a").is_err());
        assert!(store.chunk_fact(turn.id, 4, "workspace-b").is_err());

        store
            .put_context(
                "ctx-rust",
                "Rust context",
                "Rust workspace context",
                "workspace-a",
            )
            .expect("context");
        let semantic = store
            .search_semantic("SQLite", "workspace-a")
            .expect("lexical semantic fallback");
        assert_eq!(semantic, vec![absorbed[2].clone()]);

        let recall = store
            .compose_recall("Rust", "workspace-a")
            .expect("composed recall");
        assert!(recall.facts.iter().any(|fact| fact.text == "Rust memory"));
        assert_eq!(recall.contexts.len(), 1);
        assert_eq!(recall.contexts[0].reference, "ctx-rust");
        assert_eq!(
            store.search_index("Rust", "workspace-b").unwrap(),
            Recall {
                facts: Vec::new(),
                contexts: Vec::new()
            }
        );
        assert!(store.compose_recall("Rust", "").is_err());
    }

    #[test]
    fn runs_measurements_feedback_and_categories_are_idempotent_and_scoped() {
        let store = Store::in_memory().expect("fresh store");
        let category = store
            .create_category("engineering", "workspace-a")
            .expect("category");
        assert_eq!(
            store.create_category("engineering", "workspace-a").unwrap(),
            category
        );
        let fact = store
            .remember_fact("Rust run fact", "workspace-a")
            .expect("fact");
        let categorized = store
            .categorize_pending("engineering", "Rust", "workspace-a", 10)
            .expect("categorized facts");
        assert_eq!(categorized.len(), 1);
        assert_eq!(categorized[0].id, fact.id);
        assert_eq!(categorized[0].category_id, Some(category.id));
        assert!(store
            .categorize_pending("engineering", "Rust", "workspace-a", 10)
            .unwrap()
            .is_empty());
        assert_eq!(store.list_categories("workspace-b").unwrap(), Vec::new());

        let run_spec = RunSpec {
            run_id: "run-1".to_owned(),
            issue_ref: "performance-decision".to_owned(),
            pr_ref: "1".to_owned(),
            session: "session-1".to_owned(),
            git_ref: "abc123".to_owned(),
            files: "src/store.rs".to_owned(),
            diff: "small diff".to_owned(),
            workspace: "workspace-a".to_owned(),
        };
        let run = store.begin_run(&run_spec).expect("run");
        assert_eq!(store.begin_run(&run_spec).unwrap(), run);
        assert!(store
            .begin_run(&RunSpec {
                issue_ref: "different-issue".to_owned(),
                ..run_spec.clone()
            })
            .is_err());
        let linked = store
            .link_run(
                "run-1",
                Some("performance-decision"),
                Some("fixture-pr-1"),
                None,
                None,
                "workspace-a",
            )
            .unwrap()
            .expect("linked run");
        assert!(linked.pr_ref.contains("fixture-pr-1"));
        let ended = store
            .end_run("run-1", "passed", "workspace-a")
            .unwrap()
            .expect("ended run");
        assert_eq!(ended.state, "closed");
        assert_eq!(ended.summary, "passed");
        assert_eq!(
            store.end_run("run-1", "", "workspace-a").unwrap(),
            Some(ended)
        );
        assert_eq!(
            store
                .query_runs("performance-decision", "workspace-a")
                .unwrap()
                .len(),
            1
        );
        assert!(store.query_runs("run-1", "workspace-b").unwrap().is_empty());

        let measurement_spec = MeasurementSpec {
            measurement: "latency_ms".to_owned(),
            sample: "sample-1".to_owned(),
            variant: "rust".to_owned(),
            value: 12.5,
            baseline: false,
            workspace: "workspace-a".to_owned(),
        };
        let measurement = store
            .record_measurement(&measurement_spec)
            .expect("measurement");
        assert_eq!(
            store.record_measurement(&measurement_spec).unwrap(),
            measurement
        );
        assert!(store
            .record_measurement(&MeasurementSpec {
                value: 13.0,
                ..measurement_spec.clone()
            })
            .is_err());
        assert_eq!(
            store.query_measurements("latency", "workspace-a").unwrap(),
            vec![measurement]
        );

        let feedback_spec = FeedbackSpec {
            feedback_id: "feedback-1".to_owned(),
            site: "recall".to_owned(),
            item_type: "fact".to_owned(),
            item_ref: fact.id.to_string(),
            signal: "helpful".to_owned(),
            query_hash: "query-hash".to_owned(),
            workspace: "workspace-a".to_owned(),
        };
        let feedback = store.record_feedback(&feedback_spec).expect("feedback");
        assert_eq!(store.record_feedback(&feedback_spec).unwrap(), feedback);
        assert_eq!(
            store.query_feedback("helpful", "workspace-a").unwrap(),
            vec![feedback]
        );
        assert!(store
            .record_feedback(&FeedbackSpec {
                signal: "invalid".to_owned(),
                ..feedback_spec
            })
            .is_err());
    }

    #[test]
    fn fact_review_history_sessions_and_guarded_summaries_are_deterministic() {
        let store = Store::in_memory().expect("fresh store");
        let fact = store
            .remember_fact("Rust review candidate", "workspace-a")
            .expect("fact");
        let pending = store
            .set_fact_validity(fact.id, "pending", "workspace-a")
            .unwrap()
            .expect("pending fact");
        assert_eq!(pending.validity, "pending");
        assert_eq!(store.review_pending("workspace-a").unwrap(), vec![pending]);
        let confirmed = store
            .confirm_fact(fact.id, "reviewed", "workspace-a")
            .unwrap()
            .expect("confirmed fact");
        assert_eq!(confirmed.validity, "valid");
        assert_eq!(confirmed.lifecycle, "active");
        let history = store.fact_history(fact.id, "workspace-a").unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].event, "created");
        assert_eq!(history[1].event, "validity_changed");
        assert_eq!(history[2].event, "confirmed");

        let session_fact = store
            .set_fact_session(fact.id, "session-a", "workspace-a")
            .unwrap()
            .expect("session fact");
        assert_eq!(session_fact.session_id, "session-a");
        assert_eq!(
            store.facts_for_session("session-a", "workspace-a").unwrap(),
            vec![session_fact.clone()]
        );
        assert_eq!(
            store.list_sessions("workspace-a").unwrap(),
            vec!["session-a".to_owned()]
        );
        assert!(store
            .fact_references(fact.id, "workspace-a")
            .unwrap()
            .is_empty());

        let guarded = store
            .search_guard("Rust", "workspace-a")
            .expect("guarded match");
        assert_eq!(guarded.status, "ok");
        assert_eq!(guarded.reason, "match");
        let abstained = store
            .search_guard("missing", "workspace-a")
            .expect("guarded abstention");
        assert_eq!(abstained.status, "abstain");
        assert_eq!(abstained.reason, "no_match");

        let summary = store.summarize_index("workspace-a").unwrap();
        assert_eq!(summary.facts, 1);
        assert_eq!(summary.active_facts, 1);
        assert_eq!(summary.contexts, 0);
        let prepared = store
            .prepare_summary("Rust", "workspace-a")
            .expect("prepared summary");
        assert_eq!(prepared.summary, summary);
        assert_eq!(prepared.recall.facts, vec![session_fact]);
    }

    #[test]
    fn freshness_is_audited_without_an_unbounded_path_ingest_route() {
        let store = Store::in_memory().expect("fresh store");
        let fact = store
            .remember_fact("freshness candidate", "workspace-a")
            .expect("fact");
        let degraded = store
            .sweep_freshness(0, "workspace-a")
            .expect("freshness sweep");
        assert!(degraded.iter().any(|candidate| candidate.id == fact.id));
        assert_eq!(degraded[0].lifecycle, "degraded");
        assert_eq!(store.fact_history(fact.id, "workspace-a").unwrap().len(), 2);
        let embeddings = store.embed_backfill("workspace-a").unwrap();
        assert_eq!(embeddings.status, "disabled");
        assert_eq!(embeddings.updated, 0);
    }

    #[test]
    fn anchored_queries_consolidation_and_backups_are_deterministic() {
        let store = Store::in_memory().expect("fresh store");
        let fact = store
            .remember_fact("anchored fact", "workspace-a")
            .expect("fact");
        store
            .record_decision(&DecisionSpec {
                category: "storage".to_owned(),
                subject: "memory".to_owned(),
                scenario: "anchor".to_owned(),
                reasoning: "test".to_owned(),
                outcome: "SQLite".to_owned(),
                confidence: Some(0.9),
                decision_maker: "test".to_owned(),
                issue_ref: "performance-decision".to_owned(),
                path: "src/store.rs".to_owned(),
                symbol: "Store".to_owned(),
                parent_id: None,
                workspace: "workspace-a".to_owned(),
            })
            .expect("decision");
        store
            .attach_evidence(&EvidenceSpec {
                fact_id: fact.id,
                source_ref: "src/store.rs:anchor".to_owned(),
                source: "repository".to_owned(),
                checksum: "checksum".to_owned(),
                fetched_at: None,
                repository_ref: "main".to_owned(),
                path: "src/store.rs".to_owned(),
                symbol: "Store".to_owned(),
                line_start: Some(1),
                line_end: Some(2),
                column_start: None,
                column_end: None,
                selected_text: "anchored fact".to_owned(),
                resolution_status: "resolved".to_owned(),
                workspace: "workspace-a".to_owned(),
            })
            .expect("evidence");
        let anchored = store.query_anchored("src/store.rs", "workspace-a").unwrap();
        assert_eq!(anchored.decisions.len(), 1);
        assert_eq!(anchored.evidence.len(), 1);
        let consolidated = store.consolidate("anchored", "workspace-a").unwrap();
        assert_eq!(consolidated.scanned, 1);
        assert_eq!(consolidated.consolidated, 0);
        assert_eq!(consolidated.remaining, 1);

        let backup = store
            .backup_workspace("anchored-workspace.json", "workspace-a")
            .expect("workspace backup");
        assert!(backup.bytes > 0);
        assert_eq!(backup.facts, 1);
        assert!(serde_json::to_value(&backup).unwrap().get("path").is_none());
        assert!(fs::read_to_string(&backup.path)
            .unwrap()
            .contains("anchored fact"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&backup.path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(Path::new(&backup.path).parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        let _ = fs::remove_file(&backup.path);
    }

    #[test]
    fn lifecycle_events_and_handoffs_are_idempotent_and_workspace_scoped() {
        let store = Store::in_memory().expect("fresh store");
        store
            .put_context("ctx-a", "A", "context a", "workspace-a")
            .expect("context a");
        store
            .put_context("ctx-b", "B", "context b", "workspace-b")
            .expect("context b");

        let event_spec = EventSpec {
            idempotency_key: "event-1".to_owned(),
            event_type: "captured".to_owned(),
            context_reference: "ctx-a".to_owned(),
            metadata: r#"{"source":"test"}"#.to_owned(),
            payload: r#"{"turn":1}"#.to_owned(),
            payload_truncated: false,
            workspace: "workspace-a".to_owned(),
        };
        let event = store.capture_event(&event_spec).expect("event");
        assert_eq!(event.payload_size, event_spec.payload.len() as i64);
        assert!(!event.payload_truncated);
        assert_eq!(store.capture_event(&event_spec).unwrap(), event);
        assert!(store
            .capture_event(&EventSpec {
                event_type: "different".to_owned(),
                ..event_spec.clone()
            })
            .is_err());
        assert_eq!(store.list_events("workspace-a").unwrap().len(), 1);
        assert!(store
            .read_event("missing", "workspace-a")
            .unwrap()
            .is_none());

        let handoff_spec = HandoffSpec {
            idempotency_key: "handoff-1".to_owned(),
            context_reference: "ctx-a".to_owned(),
            owner: "agent-a".to_owned(),
            session: "session-a".to_owned(),
            source: "test".to_owned(),
            workspace: "workspace-a".to_owned(),
            shared: true,
            ttl_seconds: Some(3600),
            expires_at: None,
        };
        let handoff = store.begin_handoff(&handoff_spec).expect("handoff");
        assert_eq!(handoff.state, "open");
        assert!(handoff.expires_at.is_some());
        assert_eq!(store.begin_handoff(&handoff_spec).unwrap(), handoff);
        let accepted = store
            .accept_handoff("handoff-1", "agent-b", "workspace-a")
            .unwrap()
            .expect("accepted handoff");
        assert_eq!(accepted.state, "accepted");
        assert_eq!(accepted.accepted_by.as_deref(), Some("agent-b"));
        assert_eq!(
            store
                .accept_handoff("handoff-1", "agent-c", "workspace-a")
                .unwrap(),
            Some(accepted.clone())
        );
        assert!(store
            .cancel_handoff("handoff-1", "agent-b", "workspace-a")
            .is_err());
        assert!(store.list_handoffs("workspace-b").unwrap().is_empty());

        let expired = store
            .begin_handoff(&HandoffSpec {
                idempotency_key: "handoff-expired".to_owned(),
                context_reference: "ctx-b".to_owned(),
                owner: "agent-b".to_owned(),
                session: String::new(),
                source: String::new(),
                workspace: "workspace-b".to_owned(),
                shared: false,
                ttl_seconds: None,
                expires_at: Some("2000-01-01T00:00:00Z".to_owned()),
            })
            .expect("expired handoff");
        assert_eq!(expired.state, "open");
        let listed = store.list_handoffs("workspace-b").unwrap();
        assert_eq!(listed[0].state, "expired");
        assert!(store
            .accept_handoff("handoff-expired", "agent-b", "workspace-b")
            .is_err());
    }

    #[test]
    fn fact_metadata_filters_and_hash_verification_are_deterministic() {
        let store = Store::in_memory().expect("fresh store");
        let important = store
            .remember_fact_with_metadata(
                "SQLite is the selected fallback",
                "workspace-a",
                &FactMetadata {
                    source: "design".to_owned(),
                    project: "memory".to_owned(),
                    domain: "storage".to_owned(),
                    trust: "high".to_owned(),
                    strong: true,
                    importance: 0.9,
                },
            )
            .expect("rich fact");
        let duplicate = store
            .remember_fact_with_metadata(
                "SQLite is the selected fallback",
                "workspace-a",
                &FactMetadata::default(),
            )
            .expect("deduplicated fact");
        assert_eq!(duplicate, important);
        store
            .remember_fact_with_metadata(
                "SQLite is also a test subject",
                "workspace-a",
                &FactMetadata {
                    source: "test".to_owned(),
                    ..FactMetadata::default()
                },
            )
            .expect("second fact");

        let filtered = store
            .search_facts_with_filters(
                "SQLite",
                "workspace-a",
                &FactFilters {
                    source: Some("design".to_owned()),
                    strong: Some(true),
                    ..FactFilters::default()
                },
            )
            .expect("filtered search");
        assert_eq!(filtered, vec![important.clone()]);
        assert_eq!(
            store
                .list_facts_with_filters(
                    "workspace-a",
                    &FactFilters {
                        trust: Some("low".to_owned()),
                        ..FactFilters::default()
                    },
                )
                .unwrap(),
            Vec::new()
        );
        assert_eq!(
            store.verify_facts("workspace-a").unwrap(),
            FactVerification {
                checked: 2,
                valid: true,
                invalid_ids: Vec::new()
            }
        );
        store
            .connection
            .execute(
                "UPDATE facts SET sha256 = 'corrupted' WHERE id = ?1",
                params![important.id],
            )
            .expect("tamper fixture");
        let verification = store.verify_facts("workspace-a").unwrap();
        assert!(!verification.valid);
        assert_eq!(verification.invalid_ids, vec![important.id]);
    }

    #[test]
    fn graph_and_decision_queries_preserve_workspace_scope_and_parentage() {
        let store = Store::in_memory().expect("fresh store");
        let fact = store
            .remember_fact("Graph source fact", "workspace-a")
            .expect("source fact");
        let rust = store
            .remember_entity(&EntitySpec {
                name: "Rust".to_owned(),
                entity_type: "language".to_owned(),
                aliases: vec!["rust-lang".to_owned()],
                workspace: "workspace-a".to_owned(),
            })
            .expect("rust entity");
        let sqlite = store
            .remember_entity(&EntitySpec {
                name: "SQLite".to_owned(),
                entity_type: "database".to_owned(),
                aliases: Vec::new(),
                workspace: "workspace-a".to_owned(),
            })
            .expect("sqlite entity");
        assert_eq!(
            store
                .remember_entity(&EntitySpec {
                    name: "Rust".to_owned(),
                    entity_type: "different".to_owned(),
                    aliases: Vec::new(),
                    workspace: "workspace-a".to_owned(),
                })
                .unwrap(),
            rust
        );
        let relation = store
            .remember_relation(&RelationSpec {
                subject: "Rust".to_owned(),
                predicate: "uses".to_owned(),
                object: "SQLite".to_owned(),
                source_fact_id: Some(fact.id),
                workspace: "workspace-a".to_owned(),
            })
            .expect("relation");
        assert_eq!(relation.subject_id, rust.id);
        assert_eq!(relation.object_id, sqlite.id);
        let graph = store.search_graph("rust", "workspace-a").unwrap();
        assert_eq!(graph.entities.len(), 1);
        assert_eq!(graph.relations, vec![relation]);
        assert_eq!(
            store.export_rdf("workspace-a").unwrap(),
            "<Rust> <uses> <SQLite> .\n"
        );
        assert!(store
            .search_graph("rust", "workspace-b")
            .unwrap()
            .entities
            .is_empty());

        let root = store
            .record_decision(&DecisionSpec {
                category: "storage".to_owned(),
                subject: "memory".to_owned(),
                scenario: "fallback".to_owned(),
                reasoning: "avoid external dependency".to_owned(),
                outcome: "SQLite".to_owned(),
                confidence: Some(0.9),
                decision_maker: "agent".to_owned(),
                issue_ref: "performance-decision".to_owned(),
                path: "src/store.rs".to_owned(),
                symbol: "Store".to_owned(),
                parent_id: None,
                workspace: "workspace-a".to_owned(),
            })
            .expect("root decision");
        let child = store
            .record_decision(&DecisionSpec {
                category: "storage".to_owned(),
                subject: "memory".to_owned(),
                scenario: "fallback".to_owned(),
                reasoning: "follow-up".to_owned(),
                outcome: "SQLite with FTS5".to_owned(),
                confidence: Some(0.8),
                decision_maker: "agent".to_owned(),
                issue_ref: "performance-decision".to_owned(),
                path: String::new(),
                symbol: String::new(),
                parent_id: Some(root.id),
                workspace: "workspace-a".to_owned(),
            })
            .expect("child decision");
        store
            .record_decision(&DecisionSpec {
                category: "storage".to_owned(),
                subject: "memory".to_owned(),
                scenario: "fallback".to_owned(),
                reasoning: "alternative".to_owned(),
                outcome: "Redis".to_owned(),
                confidence: Some(0.2),
                decision_maker: "reviewer".to_owned(),
                issue_ref: String::new(),
                path: String::new(),
                symbol: String::new(),
                parent_id: None,
                workspace: "workspace-a".to_owned(),
            })
            .expect("conflicting decision");
        assert_eq!(
            store.causal_chain(child.id, "workspace-a").unwrap().len(),
            2
        );
        let conflicts = store
            .detect_conflicts("fallback", "workspace-a")
            .expect("conflict query");
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].outcomes,
            vec!["SQLite", "SQLite with FTS5", "Redis"]
        );
        assert!(store
            .query_decisions("fallback", "workspace-b")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn evidence_provenance_and_workspace_export_are_deterministic() {
        let store = Store::in_memory().expect("fresh store");
        let fact = store
            .remember_fact("Evidence-backed fact", "workspace-a")
            .expect("fact");
        store
            .put_context("ctx-a", "Context", "Evidence context", "workspace-a")
            .expect("context");
        let spec = EvidenceSpec {
            fact_id: fact.id,
            source_ref: "docs/current-contract.md".to_owned(),
            source: "repository".to_owned(),
            checksum: "source-checksum".to_owned(),
            fetched_at: Some("2026-08-26T00:00:00Z".to_owned()),
            repository_ref: "main".to_owned(),
            path: "docs/current-contract.md".to_owned(),
            symbol: "contract".to_owned(),
            line_start: Some(1),
            line_end: Some(3),
            column_start: Some(1),
            column_end: Some(20),
            selected_text: "Evidence context".to_owned(),
            resolution_status: "resolved".to_owned(),
            workspace: "workspace-a".to_owned(),
        };
        let evidence = store.attach_evidence(&spec).expect("evidence");
        assert_eq!(evidence.selected_text_sha256, sha256("Evidence context"));
        assert_eq!(store.attach_evidence(&spec).unwrap(), evidence);
        assert!(store
            .attach_evidence(&EvidenceSpec {
                checksum: "different".to_owned(),
                ..spec.clone()
            })
            .is_err());
        assert_eq!(
            store.get_provenance(fact.id, "workspace-b").unwrap(),
            Vec::new()
        );
        let export = store
            .export_snapshot("workspace-a")
            .expect("workspace export");
        assert_eq!(export.facts.len(), 1);
        assert_eq!(export.contexts.len(), 1);
        assert_eq!(export.evidence, vec![evidence]);
        assert!(export.events.is_empty());
        assert!(export.handoffs.is_empty());
    }

    #[test]
    fn named_database_lifecycle_switches_isolates_and_backs_up() {
        let root =
            std::env::temp_dir().join(format!("memory-mcp-rust-databases-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let main_path = root.join("facts.db");
        let store = Store::open(&main_path).expect("file-backed store");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&main_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        assert_eq!(store.current_database().unwrap().name, "facts");
        let alpha = store.create_database("alpha").expect("alpha database");
        let beta = store.create_database("beta").expect("beta database");
        assert!(!alpha.active);
        assert!(!beta.active);
        assert!(serde_json::to_value(&alpha).unwrap().get("path").is_none());
        assert!(store
            .list_databases()
            .unwrap()
            .iter()
            .any(|database| database.name == "facts" && database.active));

        store.select_database("alpha").expect("select alpha");
        store.remember_fact("alpha fact", "workspace-a").unwrap();
        store.select_database("beta").expect("select beta");
        store.remember_fact("beta fact", "workspace-a").unwrap();
        assert_eq!(store.list_facts("workspace-a").unwrap().len(), 1);
        assert_eq!(
            store.list_facts("workspace-a").unwrap()[0].text,
            "beta fact"
        );
        assert!(store.select_database("missing").is_err());

        let backup = store
            .backup_database("current", "beta-backup.db")
            .expect("physical database backup");
        assert_eq!(backup.database, "current");
        assert!(backup.bytes > 0);
        assert!(serde_json::to_value(&backup).unwrap().get("path").is_none());
        let backup_store = Store::open(&backup.path).expect("read backup");
        assert_eq!(backup_store.list_facts("workspace-a").unwrap().len(), 1);
        drop(backup_store);

        store.reset_database("beta").expect("reset beta");
        assert!(store.list_facts("workspace-a").unwrap().is_empty());
        assert!(store.delete_database("beta").is_err());
        store.archive_database("alpha").expect("archive alpha");
        assert!(store
            .list_databases()
            .unwrap()
            .iter()
            .any(|database| database.name == "alpha" && database.archived));
        assert!(store.delete_database("alpha").unwrap());
        assert!(!store.delete_database("missing").unwrap());
        assert!(store.create_database("../unsafe").is_err());
        assert!(store.backup_database("current", "../unsafe.db").is_err());

        drop(store);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn in_memory_database_lifecycle_is_snapshot_backed() {
        let store = Store::in_memory().expect("memory store");
        store.remember_fact("main fact", "w").expect("main fact");
        store.create_database("alpha").expect("alpha database");
        store.select_database("alpha").expect("select alpha");
        store.remember_fact("alpha fact", "w").expect("alpha fact");
        store.select_database("memory").expect("select memory");
        assert_eq!(store.list_facts("w").expect("memory facts").len(), 1);
        store.select_database("alpha").expect("select alpha again");
        assert_eq!(store.list_facts("w").expect("alpha facts").len(), 1);
        let backup = store
            .backup_database("current", "memory-backup.db")
            .expect("memory database backup");
        assert!(backup.bytes > 0);
        assert!(serde_json::to_value(&backup).unwrap().get("path").is_none());
        let backup_store = Store::open(&backup.path).expect("open memory backup");
        assert_eq!(backup_store.list_facts("w").expect("backup facts").len(), 1);
        drop(backup_store);

        let snapshot = store.snapshot_bytes().expect("snapshot");
        let restored = Store::in_memory().expect("restored memory store");
        restored
            .restore_snapshot_bytes(&snapshot)
            .expect("restore snapshot");
        assert_eq!(
            restored.current_database().expect("current database").name,
            "alpha"
        );
        assert_eq!(
            restored.list_databases().expect("database catalog").len(),
            2
        );
        assert_eq!(
            restored
                .list_facts("w")
                .expect("restored alpha facts")
                .len(),
            1
        );

        store
            .select_database("memory")
            .expect("select memory again");
        store.archive_database("alpha").expect("archive alpha");
        assert!(store
            .select_database("alpha")
            .expect_err("archived database must not be selected")
            .to_string()
            .contains("archived"));
        assert!(store
            .delete_database("alpha")
            .expect("delete archived alpha"));
        let _ = fs::remove_file(&backup.path);
    }

    #[test]
    fn file_store_adopts_snapshot_backed_database_catalog_for_fallback() {
        let path = std::env::temp_dir().join(format!(
            "memory-mcp-rust-fallback-{}-{}.db",
            std::process::id(),
            SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_file(&path);
        let active = Store::in_memory().expect("active memory store");
        active
            .remember_fact("main fallback fact", "workspace-a")
            .expect("main fact");
        active.create_database("alpha").expect("alpha database");
        active.select_database("alpha").expect("select alpha");
        active
            .remember_fact("alpha fallback fact", "workspace-a")
            .expect("alpha fact");
        let snapshot = active.snapshot_bytes().expect("active snapshot");

        let fallback = Store::open(&path).expect("file-backed fallback store");
        fallback
            .restore_snapshot_bytes(&snapshot)
            .expect("restore active snapshot");
        assert_eq!(
            fallback.current_database().expect("current database").name,
            "alpha"
        );
        assert_eq!(
            fallback.list_databases().expect("database catalog").len(),
            2
        );
        assert_eq!(
            fallback
                .list_facts("workspace-a")
                .expect("alpha fallback facts")
                .iter()
                .map(|fact| fact.text.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha fallback fact"]
        );
        drop(fallback);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
    }

    #[test]
    fn legacy_facts_table_is_upgraded_before_serving_calls() {
        let path =
            std::env::temp_dir().join(format!("memory-mcp-rust-legacy-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        {
            let connection = Connection::open(&path).expect("legacy db");
            connection
                .execute_batch(
                    "CREATE TABLE facts (
                        id INTEGER PRIMARY KEY,
                        text TEXT NOT NULL,
                        sha256 TEXT NOT NULL
                    );
                    INSERT INTO facts (text, sha256) VALUES ('legacy fact', 'legacy-hash');",
                )
                .expect("legacy schema");
        }
        let store = Store::open(&path).expect("migrated db");
        assert_eq!(store.list_facts("").unwrap().len(), 1);
        assert_eq!(
            store.remember_fact("new fact", "").unwrap().text,
            "new fact"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn legacy_context_table_receives_metadata_columns_without_data_loss() {
        let path = std::env::temp_dir().join(format!(
            "memory-mcp-rust-context-legacy-{}.db",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        {
            let connection = Connection::open(&path).expect("legacy db");
            connection
                .execute_batch(
                    "CREATE TABLE contexts (
                        ref TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        content TEXT NOT NULL,
                        sha256 TEXT NOT NULL,
                        workspace_id TEXT NOT NULL
                    );
                    INSERT INTO contexts
                        (ref, name, content, sha256, workspace_id)
                    VALUES ('legacy-ctx', 'Legacy', 'old context', 'legacy-hash', 'legacy');
                    CREATE TABLE entities (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        workspace_id TEXT NOT NULL
                    );
                    INSERT INTO entities (name, workspace_id)
                    VALUES ('Legacy Entity', 'legacy');
                    CREATE TABLE relations (
                        id INTEGER PRIMARY KEY,
                        subject_id INTEGER NOT NULL,
                        predicate TEXT NOT NULL,
                        object_id INTEGER NOT NULL,
                        workspace_id TEXT NOT NULL
                    );
                    INSERT INTO relations
                        (subject_id, predicate, object_id, workspace_id)
                    VALUES (1, 'references', 1, 'legacy');
                    CREATE TABLE decisions (
                        id INTEGER PRIMARY KEY,
                        subject TEXT NOT NULL,
                        scenario TEXT NOT NULL,
                        outcome TEXT NOT NULL,
                        workspace_id TEXT NOT NULL
                    );
                    INSERT INTO decisions
                        (subject, scenario, outcome, workspace_id)
                    VALUES ('legacy subject', 'legacy scenario', 'legacy outcome', 'legacy');
                    CREATE TABLE evidence (
                        id INTEGER PRIMARY KEY,
                        fact_id INTEGER NOT NULL,
                        source_ref TEXT NOT NULL,
                        workspace_id TEXT NOT NULL
                    );
                    INSERT INTO evidence
                        (fact_id, source_ref, workspace_id)
                    VALUES (1, 'legacy-source', 'legacy');
                    CREATE TABLE lifecycle_events (
                        id INTEGER PRIMARY KEY,
                        idempotency_key TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        context_ref TEXT NOT NULL,
                        workspace_id TEXT NOT NULL
                    );
                    INSERT INTO lifecycle_events
                        (idempotency_key, event_type, context_ref, workspace_id)
                    VALUES ('legacy-event', 'captured', 'legacy-ctx', 'legacy');
                    CREATE TABLE handoffs (
                        id INTEGER PRIMARY KEY,
                        idempotency_key TEXT NOT NULL,
                        context_ref TEXT NOT NULL,
                        owner TEXT NOT NULL,
                        workspace_id TEXT NOT NULL
                    );
                    INSERT INTO handoffs
                        (idempotency_key, context_ref, owner, workspace_id)
                    VALUES ('legacy-handoff', 'legacy-ctx', 'agent', 'legacy');",
                )
                .expect("legacy context schema");
        }
        let store = Store::open(&path).expect("migrated db");
        let context = store
            .context("legacy-ctx", "legacy")
            .expect("read migrated context")
            .expect("legacy context exists");
        assert_eq!(context.content, "old context");
        assert_eq!(context.schema, "");
        assert_eq!(context.byte_size, "old context".len() as i64);
        assert!(store.context_map(None, "legacy").unwrap().is_empty());
        assert_eq!(store.list_events("legacy").unwrap().len(), 1);
        assert_eq!(store.list_handoffs("legacy").unwrap().len(), 1);
        assert_eq!(
            store
                .search_graph("legacy", "legacy")
                .unwrap()
                .entities
                .len(),
            1
        );
        assert_eq!(store.query_decisions("legacy", "legacy").unwrap().len(), 1);
        assert_eq!(store.list_evidence("legacy").unwrap().len(), 1);
        let _ = fs::remove_file(path);
    }
}
