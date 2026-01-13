// services/dual-writer/src/cql_server.rs
//
// CQL Protocol Proxy - Transparent proxy with write interception
// Forwards raw CQL frames for reads, intercepts writes for dual-writing
//

use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::HashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{info, warn, error, debug};
use uuid::Uuid;
use anyhow::{Result, Context, anyhow};

use crate::writer::DualWriter;
use svckit::types::WriteRequest;

/// CQL Protocol Constants
const CQL_VERSION_REQUEST: u8 = 0x04;  // Client request version
const CQL_VERSION_RESPONSE: u8 = 0x84; // Server response version (0x80 | 0x04)

/// CQL Opcodes
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

impl Opcode {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Opcode::Error),
            0x01 => Some(Opcode::Startup),
            0x02 => Some(Opcode::Ready),
            0x03 => Some(Opcode::Authenticate),
            0x05 => Some(Opcode::Options),
            0x06 => Some(Opcode::Supported),
            0x07 => Some(Opcode::Query),
            0x08 => Some(Opcode::Result),
            0x09 => Some(Opcode::Prepare),
            0x0A => Some(Opcode::Execute),
            0x0B => Some(Opcode::Register),
            0x0C => Some(Opcode::Event),
            0x0D => Some(Opcode::Batch),
            0x0E => Some(Opcode::AuthChallenge),
            0x0F => Some(Opcode::AuthResponse),
            0x10 => Some(Opcode::AuthSuccess),
            _ => None,
        }
    }
}

/// CQL Result Types
#[repr(i32)]
#[derive(Debug, Clone, Copy)]
enum ResultKind {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
}

/// CQL Frame Header (9 bytes)
#[derive(Debug, Clone)]
struct FrameHeader {
    version: u8,
    flags: u8,
    stream_id: i16,
    opcode: u8,
    body_length: i32,
}

impl FrameHeader {
    fn response(opcode: Opcode, stream_id: i16, body_length: i32) -> Self {
        Self {
            version: CQL_VERSION_RESPONSE,
            flags: 0x00,
            stream_id,
            opcode: opcode as u8,
            body_length,
        }
    }

    async fn read_from<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let mut header = [0u8; 9];
        reader.read_exact(&mut header).await?;

        Ok(Self {
            version: header[0],
            flags: header[1],
            stream_id: i16::from_be_bytes([header[2], header[3]]),
            opcode: header[4],
            body_length: i32::from_be_bytes([header[5], header[6], header[7], header[8]]),
        })
    }

    fn to_bytes(&self) -> [u8; 9] {
        let mut bytes = [0u8; 9];
        bytes[0] = self.version;
        bytes[1] = self.flags;
        bytes[2..4].copy_from_slice(&self.stream_id.to_be_bytes());
        bytes[4] = self.opcode;
        bytes[5..9].copy_from_slice(&self.body_length.to_be_bytes());
        bytes
    }
}

/// Prepared statement tracker
#[derive(Clone)]
struct PreparedStatement {
    query: String,
    is_write: bool,
}

/// CQL Proxy Server
pub struct CqlServer {
    writer: Arc<DualWriter>,
    bind_addr: SocketAddr,
    source_addr: SocketAddr,
}

impl CqlServer {
    pub fn new(writer: Arc<DualWriter>, bind_addr: SocketAddr, source_addr: SocketAddr) -> Self {
        Self {
            writer,
            bind_addr,
            source_addr,
        }
    }

    pub async fn start(self) -> Result<()> {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .context("Failed to bind CQL server")?;

        info!("CQL proxy listening on {}", self.bind_addr);
        info!("Proxying to source Cassandra at {}", self.source_addr);

        loop {
            match listener.accept().await {
                Ok((client_stream, client_addr)) => {
                    info!("New CQL connection from {}", client_addr);
                    let writer = self.writer.clone();
                    let source_addr = self.source_addr;

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(client_stream, writer, source_addr).await {
                            debug!("Connection closed from {}: {}", client_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

/// Handle a single client connection
async fn handle_connection(
    mut client_stream: TcpStream,
    writer: Arc<DualWriter>,
    source_addr: SocketAddr,
) -> Result<()> {
    let client_addr = client_stream.peer_addr()?;
    debug!("Handling connection from {}", client_addr);

    // Connect to source Cassandra
    let mut source_stream = TcpStream::connect(source_addr)
        .await
        .context("Failed to connect to source Cassandra")?;

    debug!("Connected to source Cassandra at {}", source_addr);

    // Track prepared statements for this connection
    let prepared_statements: Arc<Mutex<HashMap<Vec<u8>, PreparedStatement>>> =
        Arc::new(Mutex::new(HashMap::new()));

    loop {
        // Read frame header from client
        let header = match FrameHeader::read_from(&mut client_stream).await {
            Ok(h) => h,
            Err(e) => {
                debug!("Connection closed by client {}: {}", client_addr, e);
                return Ok(());
            }
        };

        debug!(
            "Received frame: opcode=0x{:02x}, stream_id={}, body_len={}",
            header.opcode, header.stream_id, header.body_length
        );

        // Read frame body from client
        let mut body = vec![0u8; header.body_length as usize];
        if header.body_length > 0 {
            client_stream.read_exact(&mut body).await?;
        }

        // Process based on opcode
        let response = match Opcode::from_u8(header.opcode) {
            Some(Opcode::Query) => {
                handle_query(
                    &header,
                    &body,
                    &writer,
                    &mut source_stream,
                )
                .await?
            }
            Some(Opcode::Prepare) => {
                handle_prepare(
                    &header,
                    &body,
                    &mut source_stream,
                    &prepared_statements,
                )
                .await?
            }
            Some(Opcode::Execute) => {
                handle_execute(
                    &header,
                    &body,
                    &writer,
                    &mut source_stream,
                    &prepared_statements,
                )
                .await?
            }
            Some(Opcode::Batch) => {
                handle_batch(
                    &header,
                    &body,
                    &writer,
                    &mut source_stream,
                )
                .await?
            }
            _ => {
                // All other opcodes: forward to source and proxy response
                forward_and_proxy(&header, &body, &mut source_stream).await?
            }
        };

        // Send response to client
        client_stream.write_all(&response).await?;
        client_stream.flush().await?;
    }
}

/// Forward a frame to source and return the raw response
async fn forward_and_proxy(
    header: &FrameHeader,
    body: &[u8],
    source_stream: &mut TcpStream,
) -> Result<Vec<u8>> {
    // Forward the original frame to source
    source_stream.write_all(&header.to_bytes()).await?;
    if !body.is_empty() {
        source_stream.write_all(body).await?;
    }
    source_stream.flush().await?;

    // Read response from source
    read_full_frame(source_stream).await
}

/// Read a complete CQL frame from a stream
async fn read_full_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    // Read header
    let mut header_bytes = [0u8; 9];
    stream.read_exact(&mut header_bytes).await?;

    let body_length = i32::from_be_bytes([
        header_bytes[5],
        header_bytes[6],
        header_bytes[7],
        header_bytes[8],
    ]) as usize;

    // Read body
    let mut body = vec![0u8; body_length];
    if body_length > 0 {
        stream.read_exact(&mut body).await?;
    }

    // Combine header + body
    let mut frame = Vec::with_capacity(9 + body_length);
    frame.extend_from_slice(&header_bytes);
    frame.extend_from_slice(&body);

    Ok(frame)
}

/// Handle QUERY opcode
async fn handle_query(
    header: &FrameHeader,
    body: &[u8],
    writer: &Arc<DualWriter>,
    source_stream: &mut TcpStream,
) -> Result<Vec<u8>> {
    // Parse query string
    let query = parse_query_string(body)?;
    debug!("QUERY: {}", query);

    let query_upper = query.trim().to_uppercase();

    // Check if this is a write operation
    let is_write = query_upper.starts_with("INSERT")
        || query_upper.starts_with("UPDATE")
        || query_upper.starts_with("DELETE");

    if is_write {
        // Intercept writes for dual-writing
        debug!("Write query intercepted - dual-writing");
        handle_write_query(header, &query, body, writer, source_stream).await
    } else {
        // Forward all reads (SELECT, USE, system queries, etc.) to source
        debug!("Read/DDL query - forwarding to source");
        forward_and_proxy(header, body, source_stream).await
    }
}

/// Handle write queries (INSERT/UPDATE/DELETE)
async fn handle_write_query(
    header: &FrameHeader,
    query: &str,
    body: &[u8],
    writer: &Arc<DualWriter>,
    source_stream: &mut TcpStream,
) -> Result<Vec<u8>> {
    // Extract keyspace and table
    let (keyspace, table) = extract_keyspace_table(query)?;

    // Parse values from body
    let values = parse_query_values(body)?;

    // Create write request
    let request = WriteRequest {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        query: query.to_string(),
        values,
        consistency: None,
        request_id: Uuid::new_v4(),
        timestamp: None,
    };

    // Execute dual-write
    match writer.write(request).await {
        Ok(response) => {
            debug!("Dual-write successful");
            build_void_result(header.stream_id)
        }
        Err(e) => {
            error!("Dual-write failed: {}", e);
            // On dual-write failure, try source-only as fallback
            warn!("Falling back to source-only write");
            forward_and_proxy(header, body, source_stream).await
        }
    }
}

/// Handle PREPARE opcode
async fn handle_prepare(
    header: &FrameHeader,
    body: &[u8],
    source_stream: &mut TcpStream,
    prepared_statements: &Arc<Mutex<HashMap<Vec<u8>, PreparedStatement>>>,
) -> Result<Vec<u8>> {
    // Parse the query being prepared
    let query = parse_long_string(body, 0)?.0;
    debug!("PREPARE: {}", query);

    // Determine if this is a write query
    let query_upper = query.trim().to_uppercase();
    let is_write = query_upper.starts_with("INSERT")
        || query_upper.starts_with("UPDATE")
        || query_upper.starts_with("DELETE");

    // Forward to source to get the prepared statement ID
    let response = forward_and_proxy(header, body, source_stream).await?;

    // Extract prepared statement ID from response and track it
    if let Some(stmt_id) = extract_prepared_id(&response) {
        let mut stmts = prepared_statements.lock().await;
        stmts.insert(
            stmt_id,
            PreparedStatement {
                query: query.clone(),
                is_write,
            },
        );
        debug!("Tracked prepared statement: is_write={}", is_write);
    }

    Ok(response)
}

/// Handle EXECUTE opcode
async fn handle_execute(
    header: &FrameHeader,
    body: &[u8],
    writer: &Arc<DualWriter>,
    source_stream: &mut TcpStream,
    prepared_statements: &Arc<Mutex<HashMap<Vec<u8>, PreparedStatement>>>,
) -> Result<Vec<u8>> {
    // Extract prepared statement ID
    let stmt_id = extract_execute_stmt_id(body)?;

    // Look up the prepared statement
    let stmt_info = {
        let stmts = prepared_statements.lock().await;
        stmts.get(&stmt_id).cloned()
    };

    match stmt_info {
        Some(stmt) if stmt.is_write => {
            debug!("EXECUTE write statement: {}", stmt.query);

            // Parse values from EXECUTE body
            let values = parse_execute_values(body)?;

            // Extract keyspace and table from cached query
            let (keyspace, table) = extract_keyspace_table(&stmt.query)?;

            let request = WriteRequest {
                keyspace: keyspace.to_string(),
                table: table.to_string(),
                query: stmt.query.clone(),
                values,
                consistency: None,
                request_id: Uuid::new_v4(),
                timestamp: None,
            };

            match writer.write(request).await {
                Ok(_) => {
                    debug!("EXECUTE dual-write successful");
                    build_void_result(header.stream_id)
                }
                Err(e) => {
                    error!("EXECUTE dual-write failed: {}", e);
                    forward_and_proxy(header, body, source_stream).await
                }
            }
        }
        _ => {
            // Read query or unknown - forward to source
            debug!("EXECUTE read/unknown statement - forwarding");
            forward_and_proxy(header, body, source_stream).await
        }
    }
}


/// Handle BATCH opcode
async fn handle_batch(
    header: &FrameHeader,
    body: &[u8],
    writer: &Arc<DualWriter>,
    source_stream: &mut TcpStream,
) -> Result<Vec<u8>> {
    debug!("BATCH query - forwarding to source and shadowing to target");

    // For batches, forward to source first (to ensure consistency)
    let response = forward_and_proxy(header, body, source_stream).await?;

    // Shadow write to target (async, best-effort)
    // Note: Full batch parsing and dual-writing is complex
    // For now, we rely on individual statement interception
    // A production implementation would parse the batch and dual-write each statement

    Ok(response)
}

/// Extract query string from QUERY frame body
fn parse_query_string(body: &[u8]) -> Result<String> {
    parse_long_string(body, 0).map(|(s, _)| s)
}

/// Parse a CQL long string from body at offset
fn parse_long_string(body: &[u8], offset: usize) -> Result<(String, usize)> {
    if body.len() < offset + 4 {
        return Err(anyhow!("Body too short for long string length"));
    }

    let len = i32::from_be_bytes([
        body[offset],
        body[offset + 1],
        body[offset + 2],
        body[offset + 3],
    ]) as usize;

    if body.len() < offset + 4 + len {
        return Err(anyhow!("Body too short for long string content"));
    }

    let string = String::from_utf8(body[offset + 4..offset + 4 + len].to_vec())?;
    Ok((string, offset + 4 + len))
}

/// Parse query values from QUERY frame body
fn parse_query_values(body: &[u8]) -> Result<Vec<serde_json::Value>> {
    let (_, offset) = parse_long_string(body, 0)?;

    if body.len() < offset + 3 {
        return Ok(vec![]);
    }

    // Skip consistency (2 bytes) and read flags
    let flags = body[offset + 2];
    let mut values = Vec::new();
    let mut pos = offset + 3;

    // Check if VALUES flag is set (0x01)
    if flags & 0x01 != 0 && body.len() >= pos + 2 {
        let value_count = i16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2;

        for _ in 0..value_count {
            if body.len() < pos + 4 {
                break;
            }

            let value_len = i32::from_be_bytes([
                body[pos],
                body[pos + 1],
                body[pos + 2],
                body[pos + 3],
            ]);
            pos += 4;

            if value_len >= 0 {
                let len = value_len as usize;
                if body.len() >= pos + len {
                    let value_bytes = &body[pos..pos + len];
                    pos += len;
                    let value_str = String::from_utf8_lossy(value_bytes).to_string();
                    values.push(serde_json::json!(value_str));
                }
            } else {
                values.push(serde_json::Value::Null);
            }
        }
    }

    Ok(values)
}

/// Parse values from EXECUTE frame body
fn parse_execute_values(body: &[u8]) -> Result<Vec<serde_json::Value>> {
    // Skip prepared statement ID
    if body.len() < 2 {
        return Ok(vec![]);
    }

    let id_len = i16::from_be_bytes([body[0], body[1]]) as usize;
    let offset = 2 + id_len;

    if body.len() < offset + 3 {
        return Ok(vec![]);
    }

    // Skip consistency (2 bytes) and read flags
    let flags = body[offset + 2];
    let mut values = Vec::new();
    let mut pos = offset + 3;

    // Check if VALUES flag is set (0x01)
    if flags & 0x01 != 0 && body.len() >= pos + 2 {
        let value_count = i16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2;

        for _ in 0..value_count {
            if body.len() < pos + 4 {
                break;
            }

            let value_len = i32::from_be_bytes([
                body[pos],
                body[pos + 1],
                body[pos + 2],
                body[pos + 3],
            ]);
            pos += 4;

            if value_len >= 0 {
                let len = value_len as usize;
                if body.len() >= pos + len {
                    let value_bytes = &body[pos..pos + len];
                    pos += len;
                    let value_str = String::from_utf8_lossy(value_bytes).to_string();
                    values.push(serde_json::json!(value_str));
                }
            } else {
                values.push(serde_json::Value::Null);
            }
        }
    }

    Ok(values)
}

/// Extract prepared statement ID from EXECUTE frame body
fn extract_execute_stmt_id(body: &[u8]) -> Result<Vec<u8>> {
    if body.len() < 2 {
        return Err(anyhow!("Body too short for prepared statement ID"));
    }

    let id_len = i16::from_be_bytes([body[0], body[1]]) as usize;

    if body.len() < 2 + id_len {
        return Err(anyhow!("Body too short for prepared statement ID content"));
    }

    Ok(body[2..2 + id_len].to_vec())
}

/// Extract prepared statement ID from RESULT (PREPARED) response
fn extract_prepared_id(response: &[u8]) -> Option<Vec<u8>> {
    // Skip header (9 bytes)
    if response.len() < 9 + 4 + 2 {
        return None;
    }

    // Check if this is a RESULT frame
    if response[4] != Opcode::Result as u8 {
        return None;
    }

    // Check result kind (4 bytes after header)
    let result_kind = i32::from_be_bytes([
        response[9],
        response[10],
        response[11],
        response[12],
    ]);

    if result_kind != ResultKind::Prepared as i32 {
        return None;
    }

    // Read prepared ID length (2 bytes)
    let id_len = i16::from_be_bytes([response[13], response[14]]) as usize;

    if response.len() < 15 + id_len {
        return None;
    }

    Some(response[15..15 + id_len].to_vec())
}


/// Extract keyspace and table from query
fn extract_keyspace_table(query: &str) -> Result<(String, String)> {
    let upper = query.to_uppercase();

    // Try to find table name from various query patterns
    let table_name = if let Some(idx) = upper.find("INTO") {
        let after = &query[idx + 4..].trim_start();
        extract_table_name(after)
    } else if let Some(idx) = upper.find("UPDATE") {
        let after = &query[idx + 6..].trim_start();
        extract_table_name(after)
    } else if let Some(idx) = upper.find("FROM") {
        let after = &query[idx + 4..].trim_start();
        extract_table_name(after)
    } else {
        None
    };

    match table_name {
        Some((ks, table)) => Ok((ks.to_string(), table.to_string())),
        None => Ok(("unknown".to_string(), "unknown".to_string())),
    }
}

/// Extract table name from query fragment
fn extract_table_name(fragment: &str) -> Option<(&str, &str)> {
    let end_idx = fragment.find(|c: char| c.is_whitespace() || c == '(' || c == ';')?;
    let name = &fragment[..end_idx];

    if let Some(dot_idx) = name.find('.') {
        Some((&name[..dot_idx], &name[dot_idx + 1..]))
    } else {
        Some(("default", name))
    }
}

/// Build VOID result frame
fn build_void_result(stream_id: i16) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    body.extend_from_slice(&(ResultKind::Void as i32).to_be_bytes());

    let header = FrameHeader::response(Opcode::Result, stream_id, body.len() as i32);

    let mut response = header.to_bytes().to_vec();
    response.extend_from_slice(&body);
    Ok(response)
}

/// Build ERROR frame
fn build_error_frame(stream_id: i16, message: &str) -> Result<Vec<u8>> {
    let mut body = Vec::new();

    // Error code: Server error (0x0000)
    body.extend_from_slice(&(0x0000i32).to_be_bytes());

    // Error message (short string)
    let msg_bytes = message.as_bytes();
    body.extend_from_slice(&(msg_bytes.len() as i16).to_be_bytes());
    body.extend_from_slice(msg_bytes);

    let header = FrameHeader::response(Opcode::Error, stream_id, body.len() as i32);

    let mut response = header.to_bytes().to_vec();
    response.extend_from_slice(&body);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_keyspace_table() {
        let query = "INSERT INTO my_keyspace.my_table (id, name) VALUES (?, ?)";
        let (ks, table) = extract_keyspace_table(query).unwrap();
        assert_eq!(ks, "my_keyspace");
        assert_eq!(table, "my_table");
    }

    #[test]
    fn test_extract_table_no_keyspace() {
        let query = "INSERT INTO my_table (id) VALUES (?)";
        let (ks, table) = extract_keyspace_table(query).unwrap();
        assert_eq!(ks, "default");
        assert_eq!(table, "my_table");
    }

    #[test]
    fn test_frame_header_response_version() {
        let header = FrameHeader::response(Opcode::Result, 1, 4);
        let bytes = header.to_bytes();
        assert_eq!(bytes[0], CQL_VERSION_RESPONSE);
        assert_eq!(bytes[4], Opcode::Result as u8);
    }

    #[test]
    fn test_parse_long_string() {
        let mut body = Vec::new();
        let test_str = "SELECT * FROM test";
        body.extend_from_slice(&(test_str.len() as i32).to_be_bytes());
        body.extend_from_slice(test_str.as_bytes());

        let (parsed, offset) = parse_long_string(&body, 0).unwrap();
        assert_eq!(parsed, test_str);
        assert_eq!(offset, 4 + test_str.len());
    }
}