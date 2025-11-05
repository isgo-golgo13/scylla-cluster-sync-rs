use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Database error: {0}")]
    DatabaseError(String),
    
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Network error: {0}")]
    NetworkError(String),
    
    #[error("Validation error: {0}")]
    ValidationError(String),
    
    #[error("Migration error: {0}")]
    MigrationError(String),
    
    #[error("Timeout error: operation timed out after {0:?}")]
    TimeoutError(Duration),
    
    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<scylla::transport::errors::QueryError> for SyncError {
    fn from(err: scylla::transport::errors::QueryError) -> Self {
        SyncError::DatabaseError(err.to_string())
    }
}

impl From<anyhow::Error> for SyncError {
    fn from(err: anyhow::Error) -> Self {
        SyncError::Unknown(err.to_string())
    }
}
