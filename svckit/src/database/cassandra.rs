// svckit/src/database/cassandra.rs
//
// Cassandra compatibility module
//
// NOTE: For Cassandra 4.x support, we use the `scylla` crate which speaks
// CQL binary protocol - compatible with both ScyllaDB and Cassandra.
// 
// The DatabaseFactory selects the appropriate connection based on config.
// This module provides types and utilities for Cassandra-specific needs.
//
// If native cdrs-tokio support is needed in the future, implement here.
// For now, ScyllaConnection handles all Cassandra connections.

use crate::errors::SyncError;

/// Cassandra client template
/// 
/// Currently, all Cassandra connections go through ScyllaConnection
/// through the DatabaseFactory, since the scylla crate is CQL-compatible
/// with Cassandra 4.x.
/// 
/// This struct exists for:
/// 1. Future native cdrs-tokio integration if needed
/// 2. Cassandra-specific services (ScyllaDB driver already uses this)
/// 3. Type distinction in APIs that need it
pub struct CassandraClient {
    contact_points: Vec<String>,
    keyspace: String,
}

impl CassandraClient {
    /// Create a new CassandraClient reference
    /// 
    /// Note: This doesn't actually connect - use DatabaseFactory::create()
    /// with DatabaseDriver::Cassandra for actual connections.
    pub fn new(contact_points: Vec<String>, keyspace: String) -> Self {
        Self {
            contact_points,
            keyspace,
        }
    }
    
    /// Get contact points
    pub fn contact_points(&self) -> &[String] {
        &self.contact_points
    }
    
    /// Get keyspace
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }
    
    /// Connection info string
    pub fn connection_info(&self) -> String {
        format!(
            "Cassandra - Keyspace: {}, Nodes: {:?}",
            self.keyspace, self.contact_points
        )
    }
}

/// Cassandra-specific token range calculation
/// 
/// Cassandra uses Murmur3Partitioner by default (same as ScyllaDB)
/// Token range: i64::MIN to i64::MAX
pub fn calculate_cassandra_token_ranges(num_ranges: usize) -> Vec<(i64, i64)> {
    let min_token: i64 = i64::MIN;
    let max_token: i64 = i64::MAX;
    
    let range_size = (max_token as i128 - min_token as i128) / num_ranges as i128;
    let mut ranges = Vec::with_capacity(num_ranges);
    
    for i in 0..num_ranges {
        let start = min_token as i128 + (i as i128 * range_size);
        let end = if i == num_ranges - 1 {
            max_token as i128
        } else {
            start + range_size - 1
        };
        
        ranges.push((start as i64, end as i64));
    }
    
    ranges
}

/// Check if a driver string indicates Cassandra
pub fn is_cassandra_driver(driver: &str) -> bool {
    matches!(
        driver.to_lowercase().as_str(),
        "cassandra" | "cassandra4" | "cassandra3" | "cass" | "c*"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cassandra_client_info() {
        let client = CassandraClient::new(
            vec!["node1:9042".to_string(), "node2:9042".to_string()],
            "test_keyspace".to_string(),
        );
        
        assert_eq!(client.keyspace(), "test_keyspace");
        assert_eq!(client.contact_points().len(), 2);
        assert!(client.connection_info().contains("Cassandra"));
    }
    
    #[test]
    fn test_token_range_calculation() {
        let ranges = calculate_cassandra_token_ranges(4);
        
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].0, i64::MIN);
        assert_eq!(ranges[3].1, i64::MAX);
        
        // Ranges should be contiguous
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].1 < ranges[i + 1].0 || ranges[i].1 + 1 == ranges[i + 1].0);
        }
    }
    
    #[test]
    fn test_is_cassandra_driver() {
        assert!(is_cassandra_driver("cassandra"));
        assert!(is_cassandra_driver("Cassandra4"));
        assert!(is_cassandra_driver("CASSANDRA"));
        assert!(is_cassandra_driver("cass"));
        assert!(!is_cassandra_driver("scylla"));
        assert!(!is_cassandra_driver("mysql"));
    }
}