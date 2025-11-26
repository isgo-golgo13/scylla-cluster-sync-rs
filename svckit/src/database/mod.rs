// svckit/src/database/mod.rs
//
// Database module - unified interface for ScyllaDB and Cassandra
//
// Architecture:
// - ScyllaConnection: Primary driver (works with both ScyllaDB and Cassandra 4.x via CQL)
// - CassandraClient: Native cdrs-tokio driver (for Cassandra-specific features)
// - DatabaseFactory: Runtime driver selection based on config
// - QueryBuilder: CQL query construction helpers
// - RetryPolicy: Exponential backoff retry logic

pub mod retry;
pub mod scylla;
pub mod query_builder;
pub mod factory;
pub mod cassandra;

// Re-exports for convenience
pub use scylla::{ScyllaConnection, DatabaseConnection};
pub use query_builder::QueryBuilder;
pub use retry::RetryPolicy;
pub use factory::{DatabaseFactory, DatabaseDriver, UnifiedConnection};
pub use cassandra::{CassandraClient, CassandraDatabase};