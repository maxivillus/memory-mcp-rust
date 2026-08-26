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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Fact {
    pub id: i64,
    pub text: String,
    pub sha256: String,
    pub workspace: String,
    pub lifecycle: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Context {
    pub reference: String,
    pub name: String,
    pub content: String,
    pub sha256: String,
    pub workspace: String,
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
                workspace_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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

    pub fn remember_fact(&self, text: &str, workspace: &str) -> Result<Fact, StoreError> {
        if text.trim().is_empty() {
            return Err(StoreError::Invalid(
                "fact text must not be empty".to_owned(),
            ));
        }
        let sha256 = sha256(text);
        self.connection.execute(
            "INSERT OR IGNORE INTO facts (text, sha256, workspace_id) VALUES (?1, ?2, ?3)",
            params![text, sha256, workspace],
        )?;
        self.fact_by_hash(&sha256, workspace)?.ok_or_else(|| {
            StoreError::Invalid("fact insert did not produce a readable row".to_owned())
        })
    }

    pub fn search_facts(&self, query: &str, workspace: &str) -> Result<Vec<Fact>, StoreError> {
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let fts_query = query
            .split_whitespace()
            .map(|term| format!("\"{}\"", term.replace('"', "")))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut statement = self.connection.prepare(
            "SELECT f.id, f.text, f.sha256, f.workspace_id, f.lifecycle
             FROM facts_fts
             JOIN facts f ON f.id = facts_fts.rowid
             WHERE facts_fts MATCH ?1
               AND (f.workspace_id = '' OR f.workspace_id = ?2)
               AND f.lifecycle != 'forgotten'
             ORDER BY f.id",
        )?;
        let rows = statement
            .query_map(params![fts_query, workspace], map_fact)?
            .collect::<Result<Vec<_>, _>>()?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        let like = format!("%{}%", query);
        let mut fallback = self.connection.prepare(
            "SELECT id, text, sha256, workspace_id, lifecycle FROM facts
             WHERE text LIKE ?1
               AND (workspace_id = '' OR workspace_id = ?2)
               AND lifecycle != 'forgotten'
             ORDER BY id",
        )?;
        let rows = fallback
            .query_map(params![like, workspace], map_fact)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn list_facts(&self, workspace: &str) -> Result<Vec<Fact>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, text, sha256, workspace_id, lifecycle FROM facts
             WHERE (workspace_id = '' OR workspace_id = ?1)
               AND lifecycle != 'forgotten'
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_fact)?
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
        let sha256 = sha256(content);
        if let Some(existing) = self.context(reference, workspace)? {
            if existing.sha256 != sha256 {
                return Err(StoreError::Invalid(format!(
                    "context ref is immutable: {reference}"
                )));
            }
            return Ok(existing);
        }
        self.connection.execute(
            "INSERT INTO contexts (ref, name, content, sha256, workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![reference, name, content, sha256, workspace],
        )?;
        self.context(reference, workspace)?.ok_or_else(|| {
            StoreError::Invalid("context insert did not produce a readable row".to_owned())
        })
    }

    pub fn context(&self, reference: &str, workspace: &str) -> Result<Option<Context>, StoreError> {
        self.connection
            .query_row(
                "SELECT ref, name, content, sha256, workspace_id FROM contexts
                 WHERE ref = ?1 AND (workspace_id = '' OR workspace_id = ?2)",
                params![reference, workspace],
                map_context,
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn list_contexts(&self, workspace: &str) -> Result<Vec<Context>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT ref, name, content, sha256, workspace_id FROM contexts
             WHERE workspace_id = '' OR workspace_id = ?1 ORDER BY ref",
        )?;
        let rows = statement
            .query_map(params![workspace], map_context)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
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
            "SELECT id, text, sha256, workspace_id, lifecycle FROM facts
             WHERE (workspace_id = '' OR workspace_id = ?1)
               AND lifecycle = 'forgotten'
             ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![workspace], map_fact)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
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
                "SELECT id, text, sha256, workspace_id, lifecycle FROM facts
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
                "SELECT id, text, sha256, workspace_id, lifecycle FROM facts
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

fn map_context(row: &rusqlite::Row<'_>) -> rusqlite::Result<Context> {
    Ok(Context {
        reference: row.get(0)?,
        name: row.get(1)?,
        content: row.get(2)?,
        sha256: row.get(3)?,
        workspace: row.get(4)?,
    })
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
}
