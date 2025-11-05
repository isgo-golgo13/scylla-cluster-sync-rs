use lazy_static::lazy_static;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
    HistogramVec, IntCounterVec, IntGaugeVec,
};

lazy_static! {
    pub static ref DATABASE_OPERATION_DURATION: HistogramVec = register_histogram_vec!(
        "database_operation_duration_seconds",
        "Database operation duration in seconds",
        &["operation", "database", "status"]
    ).unwrap();
    
    pub static ref OPERATION_COUNTER: IntCounterVec = register_int_counter_vec!(
        "operations_total",
        "Total number of operations",
        &["operation", "status"]
    ).unwrap();
    
    pub static ref ACTIVE_CONNECTIONS: IntGaugeVec = register_int_gauge_vec!(
        "active_connections",
        "Number of active database connections",
        &["database"]
    ).unwrap();
}

pub fn record_operation(operation: &str, database: &str, success: bool, duration: f64) {
    let status = if success { "success" } else { "failure" };
    DATABASE_OPERATION_DURATION
        .with_label_values(&[operation, database, status])
        .observe(duration);
    OPERATION_COUNTER
        .with_label_values(&[operation, status])
        .inc();
}
