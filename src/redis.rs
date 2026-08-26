use crate::store::Fact;
use hex::encode;
use serde_json::from_slice;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

const REDIS_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_RESP_LINE_BYTES: usize = 64 * 1024;
const MAX_RESP_BULK_BYTES: usize = 32 * 1024 * 1024;
const MAX_RESP_DEPTH: usize = 16;
const MAX_SNAPSHOT_BYTES: usize = 32 * 1024 * 1024;
const IDEMPOTENCY_MARKER_TTL_SECONDS: u64 = 7 * 24 * 60 * 60;

#[derive(Debug)]
pub enum RedisError {
    Invalid(String),
    Io(std::io::Error),
    Protocol(String),
    Json(serde_json::Error),
    Conflict { expected: u64, actual: u64 },
}

impl Display for RedisError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid Redis configuration: {message}"),
            Self::Io(error) => write!(f, "Redis I/O error: {error}"),
            Self::Protocol(message) => write!(f, "Redis protocol error: {message}"),
            Self::Json(error) => write!(f, "Redis fact encoding error: {error}"),
            Self::Conflict { expected, actual } => {
                write!(
                    f,
                    "Redis state revision changed (expected {expected}, actual {actual})"
                )
            }
        }
    }
}

impl std::error::Error for RedisError {}

impl From<std::io::Error> for RedisError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for RedisError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub struct RedisAdapter {
    connection: RefCell<RedisConnection>,
    namespace: String,
}

impl RedisAdapter {
    pub fn configured() -> bool {
        std::env::var_os("MEMORY_MCP_REDIS_URL").is_some()
            || std::env::var_os("REDIS_URL").is_some()
    }

    pub fn from_env() -> Result<Option<Self>, RedisError> {
        Self::from_env_with_namespace_suffix("")
    }

    pub fn from_env_with_namespace_suffix(suffix: &str) -> Result<Option<Self>, RedisError> {
        let Some(url) =
            std::env::var_os("MEMORY_MCP_REDIS_URL").or_else(|| std::env::var_os("REDIS_URL"))
        else {
            return Ok(None);
        };
        let url = url
            .to_str()
            .ok_or_else(|| RedisError::Invalid("Redis URL must be valid UTF-8".to_owned()))?;
        let namespace =
            std::env::var("MEMORY_MCP_REDIS_NAMESPACE").unwrap_or_else(|_| "memory-mcp".to_owned());
        let namespace = if suffix.is_empty() {
            namespace
        } else {
            format!("{namespace}:{suffix}")
        };
        Self::connect(url, &namespace).map(Some)
    }

    pub fn connect(url: &str, namespace: &str) -> Result<Self, RedisError> {
        validate_namespace(namespace)?;
        let endpoint = RedisEndpoint::parse(url)?;
        let mut connection = RedisConnection::connect(&endpoint)?;
        let pong = connection.command(vec![b"PING".to_vec()])?;
        if !matches!(pong, RespValue::Simple(value) if value == b"PONG") {
            return Err(RedisError::Protocol("PING did not return PONG".to_owned()));
        }
        Ok(Self {
            connection: RefCell::new(connection),
            namespace: namespace.to_owned(),
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Check the existing connection without fetching the replicated state.
    pub fn ping(&self) -> Result<(), RedisError> {
        let pong = self.command(vec![b"PING".to_vec()])?;
        if matches!(pong, RespValue::Simple(value) if value == b"PONG") {
            Ok(())
        } else {
            Err(RedisError::Protocol("PING did not return PONG".to_owned()))
        }
    }

    /// Read the small state revision key used by the bounded watcher.
    pub fn state_revision(&self) -> Result<u64, RedisError> {
        let value = self
            .command(vec![
                b"GET".to_vec(),
                self.state_revision_key().into_bytes(),
            ])?
            .into_bulk("GET")?;
        let Some(value) = value else {
            return Ok(0);
        };
        let value = std::str::from_utf8(&value)
            .map_err(|_| RedisError::Protocol("state revision is not UTF-8".to_owned()))?;
        value
            .parse::<u64>()
            .map_err(|_| RedisError::Protocol("state revision is not an integer".to_owned()))
    }

    /// Read the complete replicated SQLite image only when the caller needs it.
    pub fn state_snapshot(&self) -> Result<Option<Vec<u8>>, RedisError> {
        let value = self
            .command(vec![
                b"GET".to_vec(),
                self.state_snapshot_key().into_bytes(),
            ])?
            .into_bulk("GET")?;
        let Some(value) = value else {
            return Ok(None);
        };
        if value.len() > MAX_SNAPSHOT_BYTES {
            return Err(RedisError::Protocol(
                "state snapshot exceeds the configured size limit".to_owned(),
            ));
        }
        Ok(Some(value))
    }

    /// Check whether a stateful operation was already committed by Redis.
    /// Markers are hashes rather than caller payloads and expire after the
    /// bounded duplicate-replay detection window.
    pub fn operation_applied(&self, idempotency_key: &str) -> Result<bool, RedisError> {
        validate_operation_key(idempotency_key)?;
        Ok(self
            .command(vec![
                b"GET".to_vec(),
                self.operation_marker_key(idempotency_key).into_bytes(),
            ])?
            .into_bulk("GET")?
            .is_some())
    }

    /// Publish one complete state image after checking the last observed
    /// revision. Redis WATCH/MULTI/EXEC makes the check and write atomic with
    /// respect to other coordinators.
    pub fn publish_state(
        &self,
        expected_revision: u64,
        snapshot: &[u8],
    ) -> Result<u64, RedisError> {
        self.publish_state_with_operations(expected_revision, snapshot, &[])
    }

    /// Publish a state image and the operation markers that produced it in one
    /// transaction. The marker TTL bounds Redis memory while covering the
    /// intended short recovery window.
    pub fn publish_state_with_operations(
        &self,
        expected_revision: u64,
        snapshot: &[u8],
        idempotency_keys: &[&str],
    ) -> Result<u64, RedisError> {
        if snapshot.is_empty() {
            return Err(RedisError::Invalid(
                "state snapshot must not be empty".to_owned(),
            ));
        }
        if snapshot.len() > MAX_SNAPSHOT_BYTES {
            return Err(RedisError::Invalid(
                "state snapshot exceeds the configured size limit".to_owned(),
            ));
        }
        for idempotency_key in idempotency_keys {
            validate_operation_key(idempotency_key)?;
        }
        let watched = self.command(vec![
            b"WATCH".to_vec(),
            self.state_snapshot_key().into_bytes(),
            self.state_revision_key().into_bytes(),
        ])?;
        if !matches!(watched, RespValue::Simple(value) if value == b"OK") {
            return Err(RedisError::Protocol("WATCH did not return OK".to_owned()));
        }
        let actual = self.state_revision()?;
        if actual != expected_revision {
            let _ = self.command(vec![b"UNWATCH".to_vec()]);
            return Err(RedisError::Conflict {
                expected: expected_revision,
                actual,
            });
        }
        let multi = self.command(vec![b"MULTI".to_vec()])?;
        if !matches!(multi, RespValue::Simple(value) if value == b"OK") {
            return Err(RedisError::Protocol("MULTI did not return OK".to_owned()));
        }
        for (command, arguments) in [
            (
                "SET",
                vec![
                    b"SET".to_vec(),
                    self.state_snapshot_key().into_bytes(),
                    snapshot.to_vec(),
                ],
            ),
            (
                "INCR",
                vec![b"INCR".to_vec(), self.state_revision_key().into_bytes()],
            ),
        ] {
            let queued = self.command(arguments)?;
            if !matches!(queued, RespValue::Simple(value) if value == b"QUEUED") {
                return Err(RedisError::Protocol(format!(
                    "{command} was not queued in the Redis transaction"
                )));
            }
        }
        for idempotency_key in idempotency_keys {
            let queued = self.command(vec![
                b"SET".to_vec(),
                self.operation_marker_key(idempotency_key).into_bytes(),
                b"1".to_vec(),
                b"EX".to_vec(),
                IDEMPOTENCY_MARKER_TTL_SECONDS.to_string().into_bytes(),
            ])?;
            if !matches!(queued, RespValue::Simple(value) if value == b"QUEUED") {
                return Err(RedisError::Protocol(
                    "idempotency marker was not queued in the Redis transaction".to_owned(),
                ));
            }
        }
        let result = self.command(vec![b"EXEC".to_vec()])?;
        let mut values = match result {
            RespValue::Array(Some(values)) => values,
            RespValue::Array(None) => {
                let actual = self.state_revision().unwrap_or(expected_revision);
                return Err(RedisError::Conflict {
                    expected: expected_revision,
                    actual,
                });
            }
            _ => {
                return Err(RedisError::Protocol(
                    "EXEC returned a non-array response".to_owned(),
                ))
            }
        };
        if values.len() != 2 + idempotency_keys.len() {
            return Err(RedisError::Protocol(
                "EXEC returned an unexpected result count".to_owned(),
            ));
        }
        let _stored = values.remove(0).into_simple("SET")?;
        let revision = values.remove(0).into_integer("INCR")?;
        for _ in idempotency_keys {
            values.remove(0).into_simple("idempotency marker SET")?;
        }
        u64::try_from(revision)
            .map_err(|_| RedisError::Protocol("state revision overflowed".to_owned()))
    }

    pub fn remember_fact(&self, text: &str, workspace: &str) -> Result<Fact, RedisError> {
        let key = self.fact_key(text, workspace);
        if let Some(existing) = self.get_fact(&key)? {
            return Ok(existing);
        }

        let id = self
            .command(vec![
                b"INCR".to_vec(),
                self.key("next-fact-id").into_bytes(),
            ])?
            .into_integer("INCR")?;
        let fact = Fact {
            id,
            text: text.to_owned(),
            sha256: digest(text),
            workspace: workspace.to_owned(),
            lifecycle: "active".to_owned(),
            source: String::new(),
            project: String::new(),
            domain: String::new(),
            trust: "medium".to_owned(),
            strong: false,
            importance: 0.5,
            category_id: None,
            validity: "valid".to_owned(),
            session_id: String::new(),
            access_count: 0,
        };
        let encoded = serde_json::to_vec(&fact)?;
        let result = self.command(vec![
            b"SET".to_vec(),
            key.as_bytes().to_vec(),
            encoded,
            b"NX".to_vec(),
        ])?;
        if matches!(result, RespValue::Simple(value) if value == b"OK") {
            self.command(vec![
                b"SADD".to_vec(),
                self.workspace_key(workspace).into_bytes(),
                key.into_bytes(),
            ])?;
            Ok(fact)
        } else {
            self.get_fact(&key)?
                .ok_or_else(|| RedisError::Protocol("SET NX lost the existing fact".to_owned()))
        }
    }

    pub fn list_facts(&self, workspace: &str) -> Result<Vec<Fact>, RedisError> {
        let keys = self
            .command(vec![
                b"SMEMBERS".to_vec(),
                self.workspace_key(workspace).into_bytes(),
            ])?
            .into_array("SMEMBERS")?;
        let mut facts = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(key) = key.into_bulk("SMEMBERS")? else {
                continue;
            };
            let Some(fact) = self.get_fact_bytes(&key)? else {
                continue;
            };
            if fact.lifecycle != "forgotten" {
                facts.push(fact);
            }
        }
        facts.sort_by_key(|fact| fact.id);
        Ok(facts)
    }

    pub fn search_facts(&self, query: &str, workspace: &str) -> Result<Vec<Fact>, RedisError> {
        let query = query.to_lowercase();
        Ok(self
            .list_facts(workspace)?
            .into_iter()
            .filter(|fact| query.is_empty() || fact.text.to_lowercase().contains(&query))
            .collect())
    }

    pub fn reset_workspace(&self, workspace: &str) -> Result<usize, RedisError> {
        let index = self
            .command(vec![
                b"SMEMBERS".to_vec(),
                self.workspace_key(workspace).into_bytes(),
            ])?
            .into_array("SMEMBERS")?;
        let mut deleted = 0;
        for key in index {
            let Some(key) = key.into_bulk("SMEMBERS")? else {
                continue;
            };
            let result = self.command(vec![b"DEL".to_vec(), key])?;
            if result.into_integer("DEL")? > 0 {
                deleted += 1;
            }
        }
        self.command(vec![
            b"DEL".to_vec(),
            self.workspace_key(workspace).into_bytes(),
        ])?;
        Ok(deleted)
    }

    fn get_fact(&self, key: &str) -> Result<Option<Fact>, RedisError> {
        self.get_fact_bytes(key.as_bytes())
    }

    fn get_fact_bytes(&self, key: &[u8]) -> Result<Option<Fact>, RedisError> {
        let value = self.command(vec![b"GET".to_vec(), key.to_vec()])?;
        let Some(value) = value.into_bulk("GET")? else {
            return Ok(None);
        };
        Ok(Some(from_slice(&value)?))
    }

    fn command(&self, arguments: Vec<Vec<u8>>) -> Result<RespValue, RedisError> {
        self.connection.borrow_mut().command(arguments)
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.namespace)
    }

    fn workspace_key(&self, workspace: &str) -> String {
        self.key(&format!("workspace:{}", digest(workspace)))
    }

    fn state_snapshot_key(&self) -> String {
        self.key("state:snapshot")
    }

    fn state_revision_key(&self) -> String {
        self.key("state:revision")
    }

    fn operation_marker_key(&self, idempotency_key: &str) -> String {
        self.key(&format!("operation:{idempotency_key}"))
    }

    fn fact_key(&self, text: &str, workspace: &str) -> String {
        self.key(&format!("fact:{}:{}", digest(workspace), digest(text)))
    }
}

fn validate_namespace(namespace: &str) -> Result<(), RedisError> {
    if namespace.is_empty()
        || !namespace.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | ':' | '.')
        })
    {
        return Err(RedisError::Invalid(
            "namespace may contain only ASCII letters, digits, '.', '_', '-' or ':'".to_owned(),
        ));
    }
    Ok(())
}

fn validate_operation_key(idempotency_key: &str) -> Result<(), RedisError> {
    if idempotency_key.len() != 64 || !idempotency_key.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(RedisError::Invalid(
            "operation idempotency key must be a SHA-256 hex digest".to_owned(),
        ));
    }
    Ok(())
}

fn digest(value: &str) -> String {
    encode(Sha256::digest(value.as_bytes()))
}

struct RedisEndpoint {
    host: String,
    port: u16,
    database: u32,
    username: Option<String>,
    password: Option<String>,
}

impl RedisEndpoint {
    fn parse(url: &str) -> Result<Self, RedisError> {
        let remainder = url.strip_prefix("redis://").ok_or_else(|| {
            RedisError::Invalid(
                "only redis:// URLs are supported by the bundled adapter".to_owned(),
            )
        })?;
        let (authority, database) = remainder.split_once('/').unwrap_or((remainder, ""));
        if authority.is_empty() || database.contains('/') || database.contains('?') {
            return Err(RedisError::Invalid(
                "Redis URL has an invalid authority or database".to_owned(),
            ));
        }
        let database = if database.is_empty() {
            0
        } else {
            database.parse::<u32>().map_err(|_| {
                RedisError::Invalid("Redis database must be a non-negative integer".to_owned())
            })?
        };

        let (credentials, host_port) = authority
            .rsplit_once('@')
            .map_or((None, authority), |(credentials, host_port)| {
                (Some(credentials), host_port)
            });
        let (username, password) = match credentials {
            None => (None, None),
            Some(credentials) => {
                let (username, password) = credentials.split_once(':').unwrap_or(("", credentials));
                let username = percent_decode(username)?;
                (
                    (!username.is_empty()).then_some(username),
                    Some(percent_decode(password)?),
                )
            }
        };

        let (host, port) = if let Some(rest) = host_port.strip_prefix('[') {
            let closing = rest.find(']').ok_or_else(|| {
                RedisError::Invalid("Redis IPv6 host is missing closing bracket".to_owned())
            })?;
            let host = &rest[..closing];
            let port = match rest[closing + 1..].strip_prefix(':') {
                Some(value) => value.parse::<u16>().map_err(|_| {
                    RedisError::Invalid("Redis port must be a valid integer".to_owned())
                })?,
                None => 6379,
            };
            (host.to_owned(), port)
        } else {
            let (host, port) = host_port
                .rsplit_once(':')
                .map_or((host_port, "6379"), |(host, port)| (host, port));
            let port = port.parse::<u16>().map_err(|_| {
                RedisError::Invalid("Redis port must be a valid integer".to_owned())
            })?;
            (host.to_owned(), port)
        };
        if host.is_empty() {
            return Err(RedisError::Invalid(
                "Redis host must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            host,
            port,
            database,
            username,
            password,
        })
    }
}

fn percent_decode(value: &str) -> Result<String, RedisError> {
    let mut decoded = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(RedisError::Invalid(
                    "Redis credentials contain an incomplete percent escape".to_owned(),
                ));
            }
            let high = hex_digit(bytes[index + 1])?;
            let low = hex_digit(bytes[index + 2])?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| RedisError::Invalid("Redis credentials must be valid UTF-8".to_owned()))
}

fn hex_digit(value: u8) -> Result<u8, RedisError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(RedisError::Invalid(
            "Redis credentials contain an invalid percent escape".to_owned(),
        )),
    }
}

struct RedisConnection {
    stream: TcpStream,
}

impl RedisConnection {
    fn connect(endpoint: &RedisEndpoint) -> Result<Self, RedisError> {
        let address = format!("{}:{}", endpoint.host, endpoint.port);
        let addresses = address.to_socket_addrs()?;
        let mut last_error = None;
        for address in addresses {
            match TcpStream::connect_timeout(&address, REDIS_TIMEOUT) {
                Ok(stream) => {
                    stream.set_read_timeout(Some(REDIS_TIMEOUT))?;
                    stream.set_write_timeout(Some(REDIS_TIMEOUT))?;
                    let mut connection = Self { stream };
                    if let Some(command) = auth_command(endpoint) {
                        connection.command(command)?;
                    }
                    if endpoint.database != 0 {
                        connection.command(vec![
                            b"SELECT".to_vec(),
                            endpoint.database.to_string().into_bytes(),
                        ])?;
                    }
                    return Ok(connection);
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(RedisError::Io(last_error.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "no Redis address")
        })))
    }

    fn command(&mut self, arguments: Vec<Vec<u8>>) -> Result<RespValue, RedisError> {
        let mut request = format!("*{}\r\n", arguments.len()).into_bytes();
        for argument in arguments {
            request.extend_from_slice(b"$");
            request.extend_from_slice(format!("{}\r\n", argument.len()).as_bytes());
            request.extend_from_slice(&argument);
            request.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&request)?;
        self.stream.flush()?;
        let response = read_resp(&mut self.stream, 0)?;
        if let RespValue::Error(message) = response {
            return Err(RedisError::Protocol(message));
        }
        Ok(response)
    }
}

fn auth_command(endpoint: &RedisEndpoint) -> Option<Vec<Vec<u8>>> {
    endpoint.password.as_ref().map(|password| {
        let mut command = vec![b"AUTH".to_vec()];
        if let Some(username) = endpoint.username.as_deref() {
            command.push(username.as_bytes().to_vec());
        }
        command.push(password.as_bytes().to_vec());
        command
    })
}

enum RespValue {
    Simple(Vec<u8>),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    fn into_simple(self, command: &str) -> Result<Vec<u8>, RedisError> {
        match self {
            Self::Simple(value) => Ok(value),
            _ => Err(RedisError::Protocol(format!(
                "{command} returned a non-simple response"
            ))),
        }
    }

    fn into_integer(self, command: &str) -> Result<i64, RedisError> {
        match self {
            Self::Integer(value) => Ok(value),
            _ => Err(RedisError::Protocol(format!(
                "{command} returned a non-integer response"
            ))),
        }
    }

    fn into_bulk(self, command: &str) -> Result<Option<Vec<u8>>, RedisError> {
        match self {
            Self::Bulk(value) => Ok(value),
            _ => Err(RedisError::Protocol(format!(
                "{command} returned a non-bulk response"
            ))),
        }
    }

    fn into_array(self, command: &str) -> Result<Vec<RespValue>, RedisError> {
        match self {
            Self::Array(Some(value)) => Ok(value),
            Self::Array(None) => Ok(Vec::new()),
            _ => Err(RedisError::Protocol(format!(
                "{command} returned a non-array response"
            ))),
        }
    }
}

fn read_resp(stream: &mut TcpStream, depth: usize) -> Result<RespValue, RedisError> {
    if depth > MAX_RESP_DEPTH {
        return Err(RedisError::Protocol("RESP nesting is too deep".to_owned()));
    }
    let prefix = read_exact_bytes(stream, 1)?[0];
    match prefix {
        b'+' => Ok(RespValue::Simple(read_resp_line(stream)?)),
        b'-' => Ok(RespValue::Error(
            String::from_utf8_lossy(&read_resp_line(stream)?).into_owned(),
        )),
        b':' => {
            let line = read_resp_line(stream)?;
            let value = String::from_utf8_lossy(&line)
                .parse::<i64>()
                .map_err(|_| RedisError::Protocol("RESP integer is invalid".to_owned()))?;
            Ok(RespValue::Integer(value))
        }
        b'$' => {
            let line = read_resp_line(stream)?;
            let length = String::from_utf8_lossy(&line)
                .parse::<i64>()
                .map_err(|_| RedisError::Protocol("RESP bulk length is invalid".to_owned()))?;
            if length < 0 {
                return Ok(RespValue::Bulk(None));
            }
            let length = usize::try_from(length).map_err(|_| {
                RedisError::Protocol("RESP bulk length does not fit usize".to_owned())
            })?;
            if length > MAX_RESP_BULK_BYTES {
                return Err(RedisError::Protocol(
                    "RESP bulk value is too large".to_owned(),
                ));
            }
            let value = read_exact_bytes(stream, length)?;
            let terminator = read_exact_bytes(stream, 2)?;
            if terminator != b"\r\n" {
                return Err(RedisError::Protocol(
                    "RESP bulk value is missing CRLF".to_owned(),
                ));
            }
            Ok(RespValue::Bulk(Some(value)))
        }
        b'*' => {
            let line = read_resp_line(stream)?;
            let length = String::from_utf8_lossy(&line)
                .parse::<i64>()
                .map_err(|_| RedisError::Protocol("RESP array length is invalid".to_owned()))?;
            if length < 0 {
                return Ok(RespValue::Array(None));
            }
            let length = usize::try_from(length).map_err(|_| {
                RedisError::Protocol("RESP array length does not fit usize".to_owned())
            })?;
            if length > MAX_RESP_BULK_BYTES {
                return Err(RedisError::Protocol("RESP array is too large".to_owned()));
            }
            let mut values = Vec::with_capacity(length);
            for _ in 0..length {
                values.push(read_resp(stream, depth + 1)?);
            }
            Ok(RespValue::Array(Some(values)))
        }
        _ => Err(RedisError::Protocol(
            "RESP response has an unknown prefix".to_owned(),
        )),
    }
}

fn read_resp_line(stream: &mut TcpStream) -> Result<Vec<u8>, RedisError> {
    let mut line = Vec::new();
    loop {
        let byte = read_exact_bytes(stream, 1)?[0];
        if byte == b'\n' {
            if line.last() != Some(&b'\r') {
                return Err(RedisError::Protocol("RESP line is missing CRLF".to_owned()));
            }
            line.pop();
            return Ok(line);
        }
        line.push(byte);
        if line.len() > MAX_RESP_LINE_BYTES {
            return Err(RedisError::Protocol("RESP line is too large".to_owned()));
        }
    }
}

fn read_exact_bytes(stream: &mut TcpStream, length: usize) -> Result<Vec<u8>, RedisError> {
    let mut value = vec![0; length];
    stream.read_exact(&mut value)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_safe_redis_urls_without_exposing_credentials() {
        let endpoint = RedisEndpoint::parse("redis://user:p%40ss@localhost:6380/3").unwrap();
        assert_eq!(endpoint.host, "localhost");
        assert_eq!(endpoint.port, 6380);
        assert_eq!(endpoint.database, 3);
        assert_eq!(endpoint.username.as_deref(), Some("user"));
        assert_eq!(endpoint.password.as_deref(), Some("p@ss"));

        let password_only = RedisEndpoint::parse("redis://:secret@localhost:6379/0").unwrap();
        assert_eq!(password_only.username, None);
        assert_eq!(password_only.password.as_deref(), Some("secret"));
        assert_eq!(
            auth_command(&password_only),
            Some(vec![b"AUTH".to_vec(), b"secret".to_vec()])
        );

        let named = RedisEndpoint::parse("redis://user:secret@localhost").unwrap();
        assert_eq!(
            auth_command(&named),
            Some(vec![b"AUTH".to_vec(), b"user".to_vec(), b"secret".to_vec()])
        );

        assert!(RedisEndpoint::parse("https://localhost").is_err());
        assert!(RedisEndpoint::parse("redis://localhost/not-a-db").is_err());
    }

    #[test]
    fn rejects_unsafe_namespaces() {
        assert!(validate_namespace("memory-mcp:bench_1").is_ok());
        assert!(validate_namespace("../secret").is_err());
        assert!(validate_namespace("").is_err());
    }

    #[test]
    fn parses_resp_scalars_and_bulk_values() {
        assert!(matches!(RespValue::Integer(3).into_integer("test"), Ok(3)));
        assert!(RespValue::Bulk(None).into_bulk("test").unwrap().is_none());
    }

    #[test]
    fn core_fact_operations_round_trip_over_resp() {
        use std::collections::{BTreeSet, HashMap};
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut values = HashMap::<Vec<u8>, Vec<u8>>::new();
            let mut sets = HashMap::<Vec<u8>, BTreeSet<Vec<u8>>>::new();
            let mut transaction: Option<Vec<Vec<Vec<u8>>>> = None;
            while let Ok(RespValue::Array(Some(arguments))) = read_resp(&mut stream, 0) {
                let arguments = arguments
                    .into_iter()
                    .map(|argument| argument.into_bulk("test").unwrap().unwrap())
                    .collect::<Vec<_>>();
                let command = String::from_utf8_lossy(&arguments[0]).to_string();
                if let Some(queue) = transaction.as_mut() {
                    if command != "EXEC" {
                        queue.push(arguments);
                        write_simple(&mut stream, b"QUEUED");
                        continue;
                    }
                }
                match command.as_str() {
                    "PING" => write_simple(&mut stream, b"PONG"),
                    "WATCH" | "UNWATCH" => write_simple(&mut stream, b"OK"),
                    "MULTI" => {
                        transaction = Some(Vec::new());
                        write_simple(&mut stream, b"OK");
                    }
                    "EXEC" => {
                        let queued = transaction.take().unwrap_or_default();
                        let result_count = queued.len();
                        let mut revision = None;
                        for arguments in queued {
                            match arguments[0].as_slice() {
                                b"SET" => {
                                    if arguments.len() == 5 {
                                        assert_eq!(arguments[3], b"EX");
                                        assert_eq!(
                                            arguments[4],
                                            IDEMPOTENCY_MARKER_TTL_SECONDS.to_string().into_bytes()
                                        );
                                    }
                                    values.insert(arguments[1].clone(), arguments[2].clone());
                                }
                                b"INCR" => {
                                    let current = values
                                        .get(&arguments[1])
                                        .map(|value| {
                                            String::from_utf8_lossy(value).parse::<i64>().unwrap()
                                        })
                                        .unwrap_or(0)
                                        + 1;
                                    values.insert(
                                        arguments[1].clone(),
                                        current.to_string().into_bytes(),
                                    );
                                    revision = Some(current);
                                }
                                _ => {}
                            }
                        }
                        write_exec_results(&mut stream, revision.unwrap_or(0), result_count);
                    }
                    "GET" => write_bulk(&mut stream, values.get(&arguments[1])),
                    "INCR" => {
                        let current = values
                            .get(&arguments[1])
                            .map(|value| String::from_utf8_lossy(value).parse::<i64>().unwrap())
                            .unwrap_or(0)
                            + 1;
                        values.insert(arguments[1].clone(), current.to_string().into_bytes());
                        write_integer(&mut stream, current);
                    }
                    "SET" => {
                        let nx = arguments.iter().any(|argument| argument == b"NX");
                        if nx && values.contains_key(&arguments[1]) {
                            write_bulk(&mut stream, None);
                        } else {
                            values.insert(arguments[1].clone(), arguments[2].clone());
                            write_simple(&mut stream, b"OK");
                        }
                    }
                    "SADD" => {
                        let inserted = sets
                            .entry(arguments[1].clone())
                            .or_default()
                            .insert(arguments[2].clone());
                        write_integer(&mut stream, i64::from(inserted));
                    }
                    "SMEMBERS" => {
                        let members = sets
                            .get(&arguments[1])
                            .into_iter()
                            .flat_map(|members| members.iter())
                            .cloned()
                            .collect::<Vec<_>>();
                        write_array(&mut stream, &members);
                    }
                    "DEL" => {
                        let mut deleted = values.remove(&arguments[1]).is_some();
                        for members in sets.values_mut() {
                            deleted |= members.remove(&arguments[1]);
                        }
                        deleted |= sets.remove(&arguments[1]).is_some();
                        write_integer(&mut stream, i64::from(deleted));
                    }
                    _ => write_error(&mut stream, b"unsupported test command"),
                }
            }
        });

        let adapter =
            RedisAdapter::connect(&format!("redis://{}", address), "test-round-trip").unwrap();
        let first = adapter
            .remember_fact("Redis fact", "workspace")
            .expect("first Redis fact");
        let duplicate = adapter
            .remember_fact("Redis fact", "workspace")
            .expect("deduplicated Redis fact");
        assert_eq!(first, duplicate);
        assert_eq!(
            adapter.search_facts("redis", "workspace").unwrap(),
            vec![first]
        );
        assert_eq!(adapter.reset_workspace("workspace").unwrap(), 1);
        assert!(adapter.list_facts("workspace").unwrap().is_empty());
        assert_eq!(adapter.state_revision().unwrap(), 0);
        assert_eq!(adapter.publish_state(0, b"sqlite-image").unwrap(), 1);
        assert_eq!(adapter.state_revision().unwrap(), 1);
        assert_eq!(
            adapter.state_snapshot().unwrap(),
            Some(b"sqlite-image".to_vec())
        );
        let idempotency_key = "a".repeat(64);
        assert!(!adapter.operation_applied(&idempotency_key).unwrap());
        assert_eq!(
            adapter
                .publish_state_with_operations(1, b"next-image", &[&idempotency_key])
                .unwrap(),
            2
        );
        assert!(adapter.operation_applied(&idempotency_key).unwrap());
        assert!(matches!(
            adapter.operation_applied("not-a-digest"),
            Err(RedisError::Invalid(_))
        ));
        assert!(matches!(
            adapter.publish_state(0, b"new-image"),
            Err(RedisError::Conflict {
                expected: 0,
                actual: 2
            })
        ));
        drop(adapter);
        server.join().unwrap();
    }

    fn write_simple(stream: &mut TcpStream, value: &[u8]) {
        stream.write_all(b"+").unwrap();
        stream.write_all(value).unwrap();
        stream.write_all(b"\r\n").unwrap();
    }

    fn write_error(stream: &mut TcpStream, value: &[u8]) {
        stream.write_all(b"-").unwrap();
        stream.write_all(value).unwrap();
        stream.write_all(b"\r\n").unwrap();
    }

    fn write_integer(stream: &mut TcpStream, value: i64) {
        stream
            .write_all(format!(":{value}\r\n").as_bytes())
            .unwrap();
    }

    fn write_bulk(stream: &mut TcpStream, value: Option<&Vec<u8>>) {
        let Some(value) = value else {
            stream.write_all(b"$-1\r\n").unwrap();
            return;
        };
        stream.write_all(b"$").unwrap();
        stream
            .write_all(format!("{}\r\n", value.len()).as_bytes())
            .unwrap();
        stream.write_all(value).unwrap();
        stream.write_all(b"\r\n").unwrap();
    }

    fn write_array(stream: &mut TcpStream, values: &[Vec<u8>]) {
        stream
            .write_all(format!("*{}\r\n", values.len()).as_bytes())
            .unwrap();
        for value in values {
            write_bulk(stream, Some(value));
        }
    }

    fn write_exec_results(stream: &mut TcpStream, revision: i64, result_count: usize) {
        stream
            .write_all(format!("*{result_count}\r\n+OK\r\n:{revision}\r\n").as_bytes())
            .unwrap();
        for _ in 2..result_count {
            write_simple(stream, b"OK");
        }
    }
}
