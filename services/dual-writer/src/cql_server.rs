// services/dual-writer/src/cql_server.rs
//
// CQL Protocol Server - Accepts native Cassandra driver connections
// Implements CQL binary protocol v4 for transparent dual-writing
//

use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error, debug};
use uuid::Uuid;
use anyhow::{Result, Context};

use crate::writer::DualWriter;
use svckit::types::WriteRequest;

/// CQL Protocol Version
const CQL_VERSION: u8 = 0x04; // CQL v4

/// CQL Opcodes
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum Opcode {
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

/// CQL Result Type
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
#[derive(Debug)]
struct FrameHeader {
    version: u8,
    flags: u8,
    stream_id: i16,
    opcode: u8,
    body_length: i32,
}

impl FrameHeader {
    fn new(opcode: Opcode, stream_id: i16, body_length: i32) -> Self {
        Self {
            version: CQL_VERSION,
            flags: 0x00,
            stream_id,
            opcode: opcode as u8,
            body_length,
        }
    }
    
    async fn read_from(stream: &mut TcpStream) -> Result<Self> {
        let mut header = [0u8; 9];
        stream.read_exact(&mut header).await?;
        
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

/// CQL Server
pub struct CqlServer {
    writer: Arc<DualWriter>,
    bind_addr: SocketAddr,
}

impl CqlServer {
    pub fn new(writer: Arc<DualWriter>, bind_addr: SocketAddr) -> Self {
        Self { writer, bind_addr }
    }
    
    pub async fn start(self) -> Result<()> {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .context("Failed to bind CQL server")?;
        
        info!("CQL server listening on {}", self.bind_addr);
        
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New CQL connection from {}", addr);
                    let writer = self.writer.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, writer).await {
                            error!("Connection error from {}: {}", addr, e);
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

async fn handle_connection(mut stream: TcpStream, writer: Arc<DualWriter>) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    debug!("Handling connection from {}", peer_addr);
    
    loop {
        // Read frame header
        let header = match FrameHeader::read_from(&mut stream).await {
            Ok(h) => h,
            Err(e) => {
                debug!("Connection closed by {}: {}", peer_addr, e);
                return Ok(());
            }
        };
        
        debug!("Received frame: opcode={:?}, stream_id={}, body_len={}", 
               header.opcode, header.stream_id, header.body_length);
        
        // Read frame body
        let mut body = vec![0u8; header.body_length as usize];
        if header.body_length > 0 {
            stream.read_exact(&mut body).await?;
        }
        
        // Process frame
        let response = match Opcode::from_u8(header.opcode) {
            Some(Opcode::Startup) => handle_startup(&header, &body).await?,
            Some(Opcode::Options) => handle_options(&header).await?,
            Some(Opcode::Query) => handle_query(&header, &body, &writer).await?,
            Some(Opcode::Prepare) => handle_prepare(&header, &body).await?,
            Some(Opcode::Execute) => handle_execute(&header, &body, &writer).await?,
            Some(Opcode::Register) => handle_register(&header).await?,
            _ => {
                warn!("Unsupported opcode: 0x{:02x}", header.opcode);
                build_error_frame(header.stream_id, "Unsupported opcode")?
            }
        };
        
        // Write response
        stream.write_all(&response).await?;
        stream.flush().await?;
    }
}

async fn handle_startup(header: &FrameHeader, _body: &[u8]) -> Result<Vec<u8>> {
    info!("STARTUP frame received");
    
    // Return READY frame
    let response_header = FrameHeader::new(Opcode::Ready, header.stream_id, 0);
    Ok(response_header.to_bytes().to_vec())
}

async fn handle_options(header: &FrameHeader) -> Result<Vec<u8>> {
    info!("OPTIONS frame received");
    
    // Build SUPPORTED frame
    let mut body = Vec::new();
    
    // Number of options (2)
    body.extend_from_slice(&(2u16).to_be_bytes());
    
    // CQL_VERSION
    write_string(&mut body, "CQL_VERSION");
    body.extend_from_slice(&(1u16).to_be_bytes()); // 1 version
    write_string(&mut body, "3.4.5");
    
    // COMPRESSION
    write_string(&mut body, "COMPRESSION");
    body.extend_from_slice(&(0u16).to_be_bytes()); // No compression
    
    let response_header = FrameHeader::new(Opcode::Supported, header.stream_id, body.len() as i32);
    
    let mut response = response_header.to_bytes().to_vec();
    response.extend_from_slice(&body);
    Ok(response)
}

async fn handle_query(header: &FrameHeader, body: &[u8], writer: &Arc<DualWriter>) -> Result<Vec<u8>> {
    // Parse query
    let (query, values) = parse_query_frame(body)?;
    
    debug!("QUERY: {}", query);
    
    // Extract keyspace.table from query
    let (keyspace, table) = extract_keyspace_table(&query)?;
    
    // Build WriteRequest
    let request = WriteRequest {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        query: query.clone(),
        values,
        consistency: None,
        request_id: Uuid::new_v4(),
        timestamp: None,
    };
    
    // Execute dual-write
    match writer.write(request).await {
        Ok(_) => {
            // Return VOID result
            build_void_result(header.stream_id)
        }
        Err(e) => {
            error!("Write failed: {}", e);
            build_error_frame(header.stream_id, &format!("Write failed: {}", e))
        }
    }
}

async fn handle_prepare(header: &FrameHeader, body: &[u8]) -> Result<Vec<u8>> {
    let (query, _) = read_long_string(body, 0)?;
    
    info!("PREPARE: {}", query);
    
    // Generate prepared statement ID
    let stmt_id = Uuid::new_v4().as_bytes().to_vec();
    
    // Build PREPARED result
    let mut body = Vec::new();
    
    // Result kind: PREPARED
    body.extend_from_slice(&(ResultKind::Prepared as i32).to_be_bytes());
    
    // Prepared ID (short bytes)
    body.extend_from_slice(&(stmt_id.len() as i16).to_be_bytes());
    body.extend_from_slice(&stmt_id);
    
    // Metadata (empty for now)
    body.extend_from_slice(&(0i32).to_be_bytes()); // flags
    body.extend_from_slice(&(0i32).to_be_bytes()); // column count
    
    let response_header = FrameHeader::new(Opcode::Result, header.stream_id, body.len() as i32);
    
    let mut response = response_header.to_bytes().to_vec();
    response.extend_from_slice(&body);
    Ok(response)
}

async fn handle_execute(header: &FrameHeader, body: &[u8], writer: &Arc<DualWriter>) -> Result<Vec<u8>> {
    // Parse EXECUTE frame
    let (stmt_id, values) = parse_execute_frame(body)?;
    
    debug!("EXECUTE: stmt_id={:?}, values={:?}", stmt_id, values);
    
    // For now, return VOID
    // In production, you'd cache prepared statements and execute them
    build_void_result(header.stream_id)
}

async fn handle_register(header: &FrameHeader) -> Result<Vec<u8>> {
    info!("REGISTER frame received");
    
    // Return READY frame
    let response_header = FrameHeader::new(Opcode::Ready, header.stream_id, 0);
    Ok(response_header.to_bytes().to_vec())
}

fn parse_query_frame(body: &[u8]) -> Result<(String, Vec<serde_json::Value>)> {
    let (query, offset) = read_long_string(body, 0)?;
    
    // Parse query parameters
    let consistency = i16::from_be_bytes([body[offset], body[offset + 1]]);
    let flags = body[offset + 2];
    
    let mut values = Vec::new();
    let mut pos = offset + 3;
    
    // Check if VALUES flag is set (0x01)
    if flags & 0x01 != 0 {
        let value_count = i16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
        pos += 2;
        
        for _ in 0..value_count {
            let value_len = i32::from_be_bytes([
                body[pos], body[pos + 1], body[pos + 2], body[pos + 3]
            ]);
            pos += 4;
            
            if value_len >= 0 {
                let value_bytes = &body[pos..pos + value_len as usize];
                pos += value_len as usize;
                
                // Convert to JSON value (simplified)
                let value_str = String::from_utf8_lossy(value_bytes).to_string();
                values.push(serde_json::json!(value_str));
            } else {
                // NULL value
                values.push(serde_json::Value::Null);
            }
        }
    }
    
    Ok((query, values))
}

fn parse_execute_frame(body: &[u8]) -> Result<(Vec<u8>, Vec<serde_json::Value>)> {
    // Read prepared statement ID
    let id_len = i16::from_be_bytes([body[0], body[1]]) as usize;
    let stmt_id = body[2..2 + id_len].to_vec();
    
    // For now, return empty values
    Ok((stmt_id, vec![]))
}

fn extract_keyspace_table(query: &str) -> Result<(&str, &str)> {
    let upper = query.to_uppercase();
    
    // Extract table name from INSERT/UPDATE/DELETE queries
    if let Some(idx) = upper.find("INTO") {
        let after = &query[idx + 4..].trim_start();
        if let Some(space_idx) = after.find(|c: char| c.is_whitespace() || c == '(') {
            let full_table = &after[..space_idx].trim();
            return parse_qualified_name(full_table);
        }
    } else if let Some(idx) = upper.find("UPDATE") {
        let after = &query[idx + 6..].trim_start();
        if let Some(space_idx) = after.find(|c: char| c.is_whitespace()) {
            let full_table = &after[..space_idx].trim();
            return parse_qualified_name(full_table);
        }
    } else if let Some(idx) = upper.find("FROM") {
        let after = &query[idx + 4..].trim_start();
        if let Some(space_idx) = after.find(|c: char| c.is_whitespace()) {
            let full_table = &after[..space_idx].trim();
            return parse_qualified_name(full_table);
        }
    }
    
    // Default fallback
    Ok(("unknown_keyspace", "unknown_table"))
}

fn parse_qualified_name(name: &str) -> Result<(&str, &str)> {
    if let Some(dot_idx) = name.find('.') {
        Ok((&name[..dot_idx], &name[dot_idx + 1..]))
    } else {
        Ok(("unknown_keyspace", name))
    }
}

fn build_void_result(stream_id: i16) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    body.extend_from_slice(&(ResultKind::Void as i32).to_be_bytes());
    
    let header = FrameHeader::new(Opcode::Result, stream_id, body.len() as i32);
    
    let mut response = header.to_bytes().to_vec();
    response.extend_from_slice(&body);
    Ok(response)
}

fn build_error_frame(stream_id: i16, message: &str) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    
    // Error code: Server error (0x0000)
    body.extend_from_slice(&(0x0000i32).to_be_bytes());
    
    // Error message
    write_string(&mut body, message);
    
    let header = FrameHeader::new(Opcode::Error, stream_id, body.len() as i32);
    
    let mut response = header.to_bytes().to_vec();
    response.extend_from_slice(&body);
    Ok(response)
}

fn read_long_string(data: &[u8], offset: usize) -> Result<(String, usize)> {
    let len = i32::from_be_bytes([
        data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
    ]) as usize;
    
    let string = String::from_utf8(data[offset + 4..offset + 4 + len].to_vec())?;
    Ok((string, offset + 4 + len))
}

fn write_string(buffer: &mut Vec<u8>, s: &str) {
    buffer.extend_from_slice(&(s.len() as i16).to_be_bytes());
    buffer.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_extract_keyspace_table() {
        let query = "INSERT INTO files_keyspace.files (id, name) VALUES (?, ?)";
        let (ks, table) = extract_keyspace_table(query).unwrap();
        assert_eq!(ks, "files_keyspace");
        assert_eq!(table, "files");
    }
    
    #[test]
    fn test_frame_header_serialization() {
        let header = FrameHeader::new(Opcode::Ready, 1, 0);
        let bytes = header.to_bytes();
        assert_eq!(bytes[0], CQL_VERSION);
        assert_eq!(bytes[4], Opcode::Ready as u8);
    }
}