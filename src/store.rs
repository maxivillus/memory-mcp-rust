use hex::encode;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone, Serialize, PartialEq)]
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

const MAX_EVENT_PAYLOAD_BYTES: usize = 16 * 1024;

pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    pub fn in_memory() -> Result<Self, StoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, StoreError> {
        connection.execute_batch(
            "PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;",
        )?;
        let store = Self { connection };
        store.migrate()?;
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
            "CREATE INDEX IF NOT EXISTS entities_canonical_idx
                ON entities (workspace_id, canonical_name);
             CREATE INDEX IF NOT EXISTS relations_subject_idx
                ON relations (workspace_id, subject_id);
             CREATE INDEX IF NOT EXISTS relations_object_idx
                ON relations (workspace_id, object_id);
             CREATE INDEX IF NOT EXISTS decisions_subject_idx
                ON decisions (workspace_id, subject);
             CREATE INDEX IF NOT EXISTS evidence_fact_idx
                ON evidence (workspace_id, fact_id);",
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

    pub fn remember_fact(&self, text: &str, workspace: &str) -> Result<Fact, StoreError> {
        self.remember_fact_with_metadata(text, workspace, &FactMetadata::default())
    }

    pub fn remember_fact_with_metadata(
        &self,
        text: &str,
        workspace: &str,
        metadata: &FactMetadata,
    ) -> Result<Fact, StoreError> {
        if text.trim().is_empty() {
            return Err(StoreError::Invalid(
                "fact text must not be empty".to_owned(),
            ));
        }
        validate_fact_metadata(metadata)?;
        let sha256 = sha256(text);
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
        self.fact_by_hash(&sha256, workspace)?.ok_or_else(|| {
            StoreError::Invalid("fact insert did not produce a readable row".to_owned())
        })
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
                    f.source, f.project, f.domain, f.trust, f.strong, f.importance
             FROM facts_fts
             JOIN facts f ON f.id = facts_fts.rowid
             WHERE facts_fts MATCH ?1
               AND (f.workspace_id = '' OR f.workspace_id = ?2)
               AND f.lifecycle != 'forgotten'
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
                    source, project, domain, trust, strong, importance
             FROM facts
             WHERE text LIKE ?1
               AND (workspace_id = '' OR workspace_id = ?2)
               AND lifecycle != 'forgotten'
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
                    source, project, domain, trust, strong, importance
             FROM facts
             WHERE (workspace_id = '' OR workspace_id = ?1)
               AND lifecycle != 'forgotten'
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
        let mut chunks = Vec::new();
        let mut current = String::new();
        let mut current_size = 0usize;
        for character in context.content.chars() {
            let character_size = character.len_utf8();
            if character_size > max_bytes {
                return Err(StoreError::Invalid(
                    "context chunk size is smaller than one UTF-8 character".to_owned(),
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
        let payload_truncated = spec.payload.len() > MAX_EVENT_PAYLOAD_BYTES;
        if let Some(existing) = self.event_by_key(&spec.idempotency_key, &spec.workspace)? {
            if existing.event_type != spec.event_type
                || existing.context_reference != spec.context_reference
                || existing.metadata != spec.metadata
                || existing.payload_sha256 != payload_sha256
                || existing.payload_size != payload_size
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
        })
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
                    source, project, domain, trust, strong, importance
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
        self.connection.execute(
            "UPDATE facts SET lifecycle = ?1
             WHERE id = ?2 AND (workspace_id = '' OR workspace_id = ?3)",
            params![lifecycle, id, workspace],
        )?;
        self.fact_by_id(id, workspace)
    }

    fn fact_by_hash(&self, hash: &str, workspace: &str) -> Result<Option<Fact>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, text, sha256, workspace_id, lifecycle,
                        source, project, domain, trust, strong, importance
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
                        source, project, domain, trust, strong, importance
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
                issue_ref: "NTL-722".to_owned(),
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
                issue_ref: "NTL-722".to_owned(),
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
