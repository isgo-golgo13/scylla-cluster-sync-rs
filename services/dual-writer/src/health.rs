use std::sync::Arc;
use svckit::database::ScyllaConnection;

pub struct HealthChecker {
    source_conn: Arc<ScyllaConnection>,
    target_conn: Arc<ScyllaConnection>,
}

impl HealthChecker {
    pub fn new(source: Arc<ScyllaConnection>, target: Arc<ScyllaConnection>) -> Self {
        Self {
            source_conn: source,
            target_conn: target,
        }
    }
    
    pub async fn check_source(&self) -> bool {
        self.source_conn
            .get_session()
            .query("SELECT now() FROM system.local", &[])
            .await
            .is_ok()
    }
    
    pub async fn check_target(&self) -> bool {
        self.target_conn
            .get_session()
            .query("SELECT now() FROM system.local", &[])
            .await
            .is_ok()
    }
}
