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
                    VALUES ('legacy-ctx', 'Legacy', 'old context', 'legacy-hash', 'legacy');",
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
        let _ = fs::remove_file(path);
    }
}
