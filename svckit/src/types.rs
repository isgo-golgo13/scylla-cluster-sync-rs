use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub columns: HashMap<String, ColumnValue>,
    pub writetime: Option<i64>,
    pub ttl: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnValue {
    Text(String),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Blob(Vec<u8>),
    Uuid(Uuid),
    Timestamp(i64),
    List(Vec<ColumnValue>),
    Map(HashMap<String, ColumnValue>),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    pub keyspace: String,
    pub table: String,
    pub query: String,
    pub values: Vec<serde_json::Value>,
    pub consistency: Option<ConsistencyLevel>,
    pub request_id: Uuid,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    LocalOne,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResponse {
    pub success: bool,
    pub request_id: Uuid,
    pub write_timestamp: i64,
    pub latency_ms: f64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub keyspace: String,
    pub table_name: String,
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<String>,
    pub regular_columns: Vec<String>,
    pub estimated_rows: u64,
}

#[derive(Debug, Clone)]
pub struct TokenRange {
    pub start_token: i64,
    pub end_token: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStatus {
    pub table: String,
    pub total_rows: u64,
    pub migrated_rows: u64,
    pub failed_rows: u64,
    pub progress_percent: f32,
    pub start_time: DateTime<Utc>,
    pub estimated_completion: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub table: String,
    pub rows_checked: u64,
    pub rows_matched: u64,
    pub discrepancies: Vec<Discrepancy>,
    pub consistency_percentage: f32,
    pub validation_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discrepancy {
    pub id: Uuid,
    pub table: String,
    pub key: HashMap<String, String>,
    pub discrepancy_type: DiscrepancyType,
    pub source_value: Option<RowData>,
    pub target_value: Option<RowData>,
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscrepancyType {
    MissingInTarget,
    MissingInSource,
    DataMismatch,
    TimestampMismatch,
    TTLMismatch,
}
