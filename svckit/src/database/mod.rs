// svckit/src/database/mod.rs
//
// Database module - unified interface for ScyllaDB and Cassandra
//
// Architecture:
// - ScyllaConnection: Primary driver (works with both ScyllaDB AND Cassandra 4.x via CQL)
// - DatabaseFactory: Runtime driver selection based on config
// - QueryBuilder: CQL query construction helpers
// - RetryPolicy: Exponential backoff retry logic
// - cassandra: Cassandra-specific services (connection via ScyllaConnection)

pub mod retry;
pub mod scylla;
pub mod query_builder;
pub mod factory;
pub mod cassandra;

// Core exports - ScyllaConnection handles both ScyllaDB and Cassandra
pub use scylla::{ScyllaConnection, DatabaseConnection};
pub use query_builder::QueryBuilder;
pub use retry::RetryPolicy;
pub use factory::{DatabaseFactory, DatabaseDriver, UnifiedConnection};

// Cassandra
pub use cassandra::{CassandraClient, calculate_cassandra_token_ranges, is_cassandra_driver};