use crate::types::WriteRequest;

pub struct QueryBuilder;

impl QueryBuilder {
    /// Build an INSERT query from a WriteRequest
    pub fn build_insert_query(request: &WriteRequest) -> String {
        // If query is already provided, use it
        if !request.query.is_empty() {
            return request.query.clone();
        }

        // Otherwise, build INSERT query from keyspace/table
        // This is a simplified version - production would need column names
        format!(
            "INSERT INTO {}.{} JSON ?",
            request.keyspace,
            request.table
        )
    }

    /// Build a SELECT query with token range
    pub fn build_select_with_token_range(
        keyspace: &str,
        table: &str,
        start_token: i64,
        end_token: i64,
    ) -> String {
        format!(
            "SELECT * FROM {}.{} WHERE token(id) >= {} AND token(id) <= {}",
            keyspace, table, start_token, end_token
        )
    }

    /// Build a simple SELECT query
    pub fn build_select_query(keyspace: &str, table: &str) -> String {
        format!("SELECT * FROM {}.{}", keyspace, table)
    }

    /// Build a DELETE query
    pub fn build_delete_query(
        keyspace: &str,
        table: &str,
        where_clause: &str,
    ) -> String {
        format!("DELETE FROM {}.{} WHERE {}", keyspace, table, where_clause)
    }

    /// Build an UPDATE query
    pub fn build_update_query(
        keyspace: &str,
        table: &str,
        set_clause: &str,
        where_clause: &str,
    ) -> String {
        format!(
            "UPDATE {}.{} SET {} WHERE {}",
            keyspace, table, set_clause, where_clause
        )
    }

    /// Build a batch statement
    pub fn build_batch_statements(queries: Vec<String>) -> String {
        let mut batch = String::from("BEGIN BATCH\n");
        for query in queries {
            batch.push_str(&format!("  {};\n", query));
        }
        batch.push_str("APPLY BATCH;");
        batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::WriteRequest;
    use uuid::Uuid;

    #[test]
    fn test_build_insert_query() {
        let request = WriteRequest {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            query: String::new(),
            values: vec![],
            consistency: None,
            request_id: Uuid::new_v4(),
            timestamp: None,
        };

        let query = QueryBuilder::build_insert_query(&request);
        assert!(query.contains("INSERT INTO test_ks.test_table"));
    }

    #[test]
    fn test_build_select_with_token_range() {
        let query = QueryBuilder::build_select_with_token_range(
            "test_ks",
            "test_table",
            -9223372036854775808,
            0,
        );
        assert!(query.contains("token(id)"));
        assert!(query.contains("test_ks.test_table"));
    }

    #[test]
    fn test_build_batch_statements() {
        let queries = vec![
            "INSERT INTO ks.table1 (id, name) VALUES (1, 'test1')".to_string(),
            "INSERT INTO ks.table2 (id, value) VALUES (2, 'test2')".to_string(),
        ];
        
        let batch = QueryBuilder::build_batch_statements(queries);
        assert!(batch.contains("BEGIN BATCH"));
        assert!(batch.contains("APPLY BATCH"));
    }
}