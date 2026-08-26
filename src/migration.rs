//! Safe, copy-first migration of the legacy SQLite store.
//!
//! The migration command deliberately does not overwrite an existing target.
//! It reads the source through SQLite's read-only connection and creates a
//! private temporary copy with the online-backup API. The Rust store is then
//! opened on that copy so its additive schema migrations run before the copy
//! is published atomically.

use crate::store::{Store, StoreError};
use rusqlite::{types::ValueRef, Connection, DatabaseName, OpenFlags};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMPORARY_MIGRATION_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub enum MigrationError {
    Io(std::io::Error),
    Sql(rusqlite::Error),
    Store(StoreError),
    Invalid(String),
}

impl Display for MigrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io error: {error}"),
            Self::Sql(error) => write!(formatter, "sqlite error: {error}"),
            Self::Store(error) => write!(formatter, "store migration error: {error}"),
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for MigrationError {}

impl MigrationError {
    /// Return a diagnostic safe for a terminal or issue attachment. Detailed
    /// I/O/SQLite errors can contain the caller's private filesystem path.
    pub fn public_message(&self) -> String {
        match self {
            Self::Io(_) => "I/O operation failed".to_owned(),
            Self::Sql(_) => "SQLite operation failed".to_owned(),
            Self::Store(_) => "Rust store schema migration failed".to_owned(),
            Self::Invalid(message) => message.clone(),
        }
    }
}

impl From<std::io::Error> for MigrationError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for MigrationError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<StoreError> for MigrationError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// Non-sensitive migration evidence. Paths are intentionally not included in
/// the report because callers already know the source and target they chose.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct MigrationReport {
    pub copied: bool,
    pub source_integrity: String,
    pub target_integrity: String,
    pub source_tables: usize,
    pub target_tables: usize,
    pub source_rows: u64,
    pub target_rows: u64,
    pub source_fingerprint: String,
    pub target_fingerprint: String,
    pub data_match: bool,
}

/// Copy and migrate `source` into a new `destination`.
///
/// The source is never opened for writing. The destination must not already
/// exist; refusing overwrite preserves the Python launcher as a rollback path
/// and makes reruns idempotent at the filesystem boundary.
pub fn migrate(source: &Path, destination: &Path) -> Result<MigrationReport, MigrationError> {
    validate_paths(source, destination)?;
    let source_connection = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    source_connection.busy_timeout(std::time::Duration::from_secs(5))?;
    // Keep the preflight reads and online backup on one consistent snapshot;
    // otherwise a live writer could make counts/fingerprints describe a
    // different revision than the copied file.
    source_connection.execute_batch("BEGIN")?;
    let source_tables = table_rows(&source_connection)?;
    // SQLite's full integrity_check and database-wide quick_check validate
    // FTS5 by writing to its transient inverted index, which is not allowed on
    // a read-only source. Check every durable non-FTS table without that write;
    // the copied target receives the full check below after Rust migrations.
    let source_integrity = quick_check_tables(&source_connection, &source_tables)?;
    let source_names = source_tables
        .keys()
        .filter(|table| !is_derived_fts_table(table))
        .cloned()
        .collect::<Vec<_>>();
    let source_columns = table_columns_for_tables(&source_connection, &source_names)?;
    let source_fingerprint = fingerprint_for_tables(
        &source_connection,
        &source_names,
        &source_tables,
        &source_columns,
    )?;

    let temporary = temporary_path(destination)?;
    create_private_file(&temporary)?;
    let result = (|| {
        source_connection.backup(
            DatabaseName::Main,
            &temporary,
            None::<fn(rusqlite::backup::Progress)>,
        )?;

        // Opening the copy is the schema migration step. It can add only the
        // Rust store's compatible tables/columns; the original source remains
        // untouched until an operator explicitly performs a cutover.
        let migrated_store = Store::open(&temporary)?;
        drop(migrated_store);

        let target_connection = Connection::open(&temporary)?;
        target_connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let target_integrity = full_integrity_check(&target_connection)?;
        let target_tables = table_rows(&target_connection)?;
        let target_fingerprint =
            database_fingerprint_projection(&target_connection, &source_tables, &source_columns)?;
        let data_match = source_fingerprint == target_fingerprint;
        if !data_match {
            return Err(MigrationError::Invalid(
                "source rows changed during migration; target was not published".to_owned(),
            ));
        }

        drop(target_connection);
        // A hard link publishes the already-validated inode without the
        // overwrite behavior of rename(2). Both paths are in the destination
        // directory, so this is atomic with respect to another cutover.
        fs::hard_link(&temporary, destination)?;
        fs::remove_file(&temporary)?;
        sync_parent(destination)?;

        let target_rows = target_tables.values().copied().sum();
        Ok(MigrationReport {
            copied: true,
            source_integrity,
            target_integrity,
            source_tables: source_tables.len(),
            target_tables: target_tables.len(),
            source_rows: source_tables.values().copied().sum(),
            target_rows,
            source_fingerprint,
            target_fingerprint,
            data_match,
        })
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn validate_paths(source: &Path, destination: &Path) -> Result<(), MigrationError> {
    if source.as_os_str().is_empty() || destination.as_os_str().is_empty() {
        return Err(MigrationError::Invalid(
            "source and destination must not be empty".to_owned(),
        ));
    }
    if !source.is_file() {
        return Err(MigrationError::Invalid(
            "source database must be an existing file".to_owned(),
        ));
    }
    if destination.exists() {
        return Err(MigrationError::Invalid(
            "destination database already exists; refusing overwrite".to_owned(),
        ));
    }
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let source_canonical = fs::canonicalize(source)?;
    let destination_parent = fs::canonicalize(parent)?;
    let destination_canonical = destination_parent.join(
        destination
            .file_name()
            .ok_or_else(|| MigrationError::Invalid("destination must name a file".to_owned()))?,
    );
    if source_canonical == destination_canonical {
        return Err(MigrationError::Invalid(
            "source and destination must be different files".to_owned(),
        ));
    }
    Ok(())
}

fn temporary_path(destination: &Path) -> Result<PathBuf, MigrationError> {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let file_name = destination
        .file_name()
        .ok_or_else(|| MigrationError::Invalid("destination must name a file".to_owned()))?
        .to_string_lossy();
    let sequence = TEMPORARY_MIGRATION_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(
        ".{file_name}.migration-{}-{sequence}",
        std::process::id()
    )))
}

fn create_private_file(path: &Path) -> Result<(), MigrationError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path)?;
    file.write_all(b"")?;
    file.sync_all()?;
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), MigrationError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn quick_check_tables(
    connection: &Connection,
    tables: &BTreeMap<String, u64>,
) -> Result<String, MigrationError> {
    for table in tables.keys().filter(|table| !is_derived_fts_table(table)) {
        let escaped = table.replace('\'', "''");
        let query = format!("PRAGMA quick_check('{escaped}')");
        let result: String = connection.query_row(&query, [], |row| row.get(0))?;
        if result != "ok" {
            return Err(MigrationError::Invalid(format!(
                "SQLite quick check failed for table {table}; target was not published"
            )));
        }
    }
    Ok("ok".to_owned())
}

fn is_derived_fts_table(table: &str) -> bool {
    table.contains("_fts")
}

fn full_integrity_check(connection: &Connection) -> Result<String, MigrationError> {
    let result: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if result != "ok" {
        return Err(MigrationError::Invalid(
            "SQLite integrity check failed; target was not published".to_owned(),
        ));
    }
    Ok(result)
}

fn table_names(connection: &Connection) -> Result<Vec<String>, MigrationError> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(names)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<String>, MigrationError> {
    let pragma = format!("PRAGMA table_info({})", quote_identifier(table));
    let mut statement = connection.prepare(&pragma)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns)
}

fn table_rows(connection: &Connection) -> Result<BTreeMap<String, u64>, MigrationError> {
    let mut rows = BTreeMap::new();
    for table in table_names(connection)? {
        let query = format!("SELECT COUNT(*) FROM {}", quote_identifier(&table));
        let count: i64 = connection.query_row(&query, [], |row| row.get(0))?;
        rows.insert(
            table,
            u64::try_from(count)
                .map_err(|_| MigrationError::Invalid("SQLite row count is negative".to_owned()))?,
        );
    }
    Ok(rows)
}

fn database_fingerprint_projection(
    connection: &Connection,
    source_tables: &BTreeMap<String, u64>,
    source_columns: &BTreeMap<String, Vec<String>>,
) -> Result<String, MigrationError> {
    let selected = source_tables
        .keys()
        .filter(|table| !is_derived_fts_table(table))
        .cloned()
        .collect::<Vec<_>>();
    fingerprint_for_tables(connection, &selected, source_tables, source_columns)
}

fn table_columns_for_tables(
    connection: &Connection,
    tables: &[String],
) -> Result<BTreeMap<String, Vec<String>>, MigrationError> {
    tables
        .iter()
        .map(|table| table_columns(connection, table).map(|columns| (table.clone(), columns)))
        .collect()
}

fn fingerprint_for_tables(
    connection: &Connection,
    tables: &[String],
    expected_rows: &BTreeMap<String, u64>,
    columns_by_table: &BTreeMap<String, Vec<String>>,
) -> Result<String, MigrationError> {
    let mut database_hasher = Sha256::new();
    for table in tables {
        let columns = columns_by_table.get(table).ok_or_else(|| {
            MigrationError::Invalid(format!("missing column metadata for table {table}"))
        })?;
        database_hasher.update(table.as_bytes());
        database_hasher.update([0]);
        for column in columns {
            database_hasher.update(column.as_bytes());
            database_hasher.update([0]);
        }
        let projection = columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        let query = if projection.is_empty() {
            format!("SELECT 1 FROM {}", quote_identifier(table))
        } else {
            format!("SELECT {projection} FROM {}", quote_identifier(table))
        };
        let mut statement = connection.prepare(&query)?;
        let mut result = statement.query([])?;
        let mut row_fingerprints = Vec::new();
        while let Some(row) = result.next()? {
            let mut row_hasher = Sha256::new();
            for index in 0..columns.len() {
                hash_value(&mut row_hasher, row.get_ref(index)?)?;
            }
            row_fingerprints.push(row_hasher.finalize().to_vec());
        }
        row_fingerprints.sort();
        let actual_rows = u64::try_from(row_fingerprints.len())
            .map_err(|_| MigrationError::Invalid("SQLite row count is too large".to_owned()))?;
        if expected_rows.get(table).copied().unwrap_or_default() != actual_rows {
            return Err(MigrationError::Invalid(format!(
                "row count changed for table {table}; target was not published"
            )));
        }
        for row_fingerprint in row_fingerprints {
            database_hasher.update(row_fingerprint);
        }
    }
    Ok(hex::encode(database_hasher.finalize()))
}

fn hash_value(hasher: &mut Sha256, value: ValueRef<'_>) -> Result<(), MigrationError> {
    match value {
        ValueRef::Null => hasher.update([0]),
        ValueRef::Integer(value) => {
            hasher.update([1]);
            hasher.update(value.to_le_bytes());
        }
        ValueRef::Real(value) => {
            hasher.update([2]);
            hasher.update(value.to_le_bytes());
        }
        ValueRef::Text(value) => {
            hasher.update([3]);
            hasher.update((value.len() as u64).to_le_bytes());
            hasher.update(value);
        }
        ValueRef::Blob(value) => {
            hasher.update([4]);
            hasher.update((value.len() as u64).to_le_bytes());
            hasher.update(value);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn migration_is_copy_first_and_preserves_rows() {
        let root =
            std::env::temp_dir().join(format!("memory-mcp-rust-migration-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let source = root.join("legacy.db");
        let destination = root.join("rust.db");
        let store = Store::open(&source).unwrap();
        store
            .remember_fact("migration fact", "migration-workspace")
            .unwrap();
        drop(store);

        let report = migrate(&source, &destination).unwrap();
        assert!(report.copied);
        assert!(report.data_match);
        assert_eq!(report.source_integrity, "ok");
        assert_eq!(report.target_integrity, "ok");
        assert!(report.target_tables >= report.source_tables);
        assert!(report.target_rows >= report.source_rows);

        let source_connection =
            Connection::open_with_flags(&source, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let source_facts: i64 = source_connection
            .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(source_facts, 1);
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let migrated = Store::open(&destination).unwrap();
        assert_eq!(migrated.list_facts("migration-workspace").unwrap().len(), 1);
        drop(migrated);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn migration_refuses_to_overwrite_target() {
        let root = std::env::temp_dir().join(format!(
            "memory-mcp-rust-migration-overwrite-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let source = root.join("legacy.db");
        let destination = root.join("rust.db");
        let source_store = Store::open(&source).unwrap();
        drop(source_store);
        File::create(&destination).unwrap();

        let error = migrate(&source, &destination).unwrap_err();
        assert!(error.to_string().contains("refusing overwrite"));
        let _ = fs::remove_dir_all(root);
    }
}
