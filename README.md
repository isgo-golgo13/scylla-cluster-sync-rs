# ScyllaDB Cluster-to-Cluster Sync Service (Rust)
Ultra-Fast RPO/RTO Sensitive ScyllaDB (Cassandra DB Adaptive) Cluster to Cluster Tenant Data Synching Service in Rust using Shadow Write Transition Pattern.

The architecture of the ScyllaDB (or Cassandra DB source) to ScyllaDB (or Cassandra DB sink/target) involves three services to fulfill the tenant data lift-and-shift transition. These services are.

- **Dual-Writer Proxy Service** (Rust or C++20/23+) - Rust version used
- **SSTableLoader Processor Service** (Rust or C++20/23+) - Rust version used
- **Dual-Reader Data Sync Ack Service** (Rust or C++20/23+) - Rust version used

Rust or C++ is required to avoid any GC pauses that would fail 99999 SLA delivery as the data shuttling across the source to the target can NOT withstand latency delays.


## The Dual-Write Proxy Service Architecture

The following graphic shows the architectual workflow of the `Dual-Write Proxy` service deployed to Kubernetes.



![dual-write-proxy-architecture-gcp-aws](docs/Dual-Writer-Data-Synch-Architecture-K8s.png)


The `Dual-Write` Pattern is executed as follows (high-level). See the following section `The Dual-Write Proxy Service Pattern (Low-Level)` for in-depth execution of the code.

```rust
async fn write_dual_async(
    &self,
    request: WriteRequest,
    config: &ProxyConfig,
) -> Result<WriteResponse, ProxyError> {
    let query = self.build_cql_query(&request);
    
    // 1. SYNCHRONOUS write to Cassandra (PRIMARY) - This BLOCKS!
    let cassandra_result = self.execute_cassandra_query(&query).await;
    let primary_latency = timer.stop_and_record();
    
    // 2. ASYNCHRONOUS write to ScyllaDB (SHADOW) - Fire and forget!
    let scylla = self.scylla.clone();
    let query_clone = query.clone();
    
    tokio::spawn(async move {  // <-- This spawns a separate task!
        match Self::execute_scylla_query(&scylla, &query_clone).await {
            Ok(_) => {
                // Shadow write succeeded
                WRITE_COUNTER.with_label_values(&["scylla", "shadow_success"]).inc();
            }
            Err(e) => {
                // Shadow write failed, log however DON'T fail the request
                warn!("Shadow write to ScyllaDB failed: {}", e);
            }
        }
    });
    
    // 3. Return based on PRIMARY (Cassandra) result ONLY
    match cassandra_result {
        Ok(_) => {
            // Success returned immediately after Cassandra write
            // No wait for ScyllaDB
            Ok(WriteResponse {
                success: true,
                latency_ms: primary_latency * 1000.0,
                database: "cassandra_primary".to_string(),
            })
        }
        Err(e) => {
            // Only fail if Cassandra fails
            Err(ProxyError::DatabaseError(e.to_string()))
        }
    }
}
```

**The Dual-Write Flow Sequence**

The following graphic shows the `Dual-Writer-Proxy` receving the tenant data write request (to dispatch to the target ScyllaDB/CassandraDB Cluster instance) and how it returns thread control to the application calling on this service.

![dual-write-sync-async-shadow-write-flow](docs/Dual-Writer-Sync-Async-Write-Flow.png)


The critical part of the `Dual-Writer-Proxy` service is to understand the distinction of the `async` wait on the result from a sychrononous write to the source Cassandra (or even source ScyllaDB) and fire-and-forget asynchronous write to the shadow Cassandra DB or ScyllaDB sink/target database. The following codfe logic shows this workflow.


```rust
// SYNCHRONOUS (waits for result):
let cassandra_result = self.execute_cassandra_query(&query).await;
//                                                            
// This 'await': "Wait until Cassandra responds"
// The function PAUSES here untilthe result is retrieved

// ASYNCHRONOUS (fire-and-forget):
tokio::spawn(async move {
    Self::execute_scylla_query(&scylla, &query_clone).await;
});
// NO await on the spawn itself
// The function CONTINUES immediately without waiting
``` 


The `Dual-Writer-Proxy` service deploys a Kubernetes Pod co-resident to the GCP application (the streaming applicattion in the graphic) to avoid cross-cloud latency IF the Dual-Writer Proxy resided in the target cloud (AWS). The following Kubernetes `Deployment`resource shows this configuration relative to the streaming application. This shows the use of Cassandra DB as the source DB. This works identically for ScyllaDB.

```yaml
# rust-proxy-deployment.yaml - Deploys to GKE, NOT EKS 
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rust-dual-writer-proxy
  namespace: engine-vector  # Same namespace as app in GKE
spec:
  replicas: 3
  selector:
    matchLabels:
      app: rust-proxy
  template:
    metadata:
      labels:
        app: rust-proxy
    spec:
      affinity:
        podAntiAffinity:  # Spread across nodes
          requiredDuringSchedulingIgnoredDuringExecution:
          - topologyKey: kubernetes.io/hostname
      containers:
      - name: proxy
        image: gcr.io/backlight-gcp/rust-proxy:v1  # GCR, not ECR!
        ports:
        - containerPort: 9042  # CQL port
        - containerPort: 8080  # Admin API
        env:
        - name: CASSANDRA_HOSTS
          value: "cassandra-0.cassandra-svc,cassandra-1.cassandra-svc"  # Local
        - name: SCYLLA_HOSTS
          value: "10.100.0.10,10.100.0.11,10.100.0.12"  # AWS IPs via VPN
        - name: WRITE_MODE
          value: "DualWriteAsync"
```

## The Dual-Write DB Duality Functionality


![dual-writer-duality-of-service-configuration](docs/Dual-Writer-Docker-Image-ConfigMap.png)


The `dual-writer` design uses the `Factory Design Pattern` in crate `svckit/src/database/factory.rs` to instantiate the correct connection resource (Scylla to Scylla or Cassandra to Cassandra, or mixed-mode source of the DB and the target DB for the data migration). The ScyllaDB driver is 100% adaptable to Cassandra (to the latest Cassandra 4 driver). The client that requires a data migration of a ScyllaDB to ScyllaDB scenario will configure the values in the provided root directory `config/dual-writer-scylla.yaml` or `config/dual-writer-cassandra.yaml`, however these are templates and the actual configuration of each will go into a Kubernetes ConfigMap and have the ConfigMap referenced in the Kubernetes `Deployment` spec. No configuration logic for the dual-writer is hard-coded. 




## The Dual-Write Proxy Service Architecture (Alternate No-GKE, No-EKS)

The following graphic shows the architectual workflow of the `Dual-Write Proxy` service deployed directly to VMs (source VM on GCP and sink/target VM on AWS) without Kubernetes.

**INSERT NON K8s IMAGE**


## The Dual-Write Proxy Service Pattern (Low-Level)


The non-multi crate pre-prod view of the code for the Dual-Write Proxy service (NOT using Envoy Proxy).

```rust
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use cassandra_cpp::{Cluster as CassandraCluster, Session as CassandraSession};
use futures::future::join_all;
use lazy_static::lazy_static;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec,
};
use scylla::{Session as ScyllaSession, SessionBuilder};
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

// Metrics for observability
lazy_static! {
    static ref WRITE_LATENCY: HistogramVec = register_histogram_vec!(
        "proxy_write_latency_seconds",
        "Write latency in seconds",
        &["database", "status"]
    )
    .unwrap();
    
    static ref WRITE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "proxy_writes_total",
        "Total number of writes",
        &["database", "status"]
    )
    .unwrap();
    
    static ref CONSISTENCY_MISMATCHES: IntCounterVec = register_int_counter_vec!(
        "proxy_consistency_mismatches",
        "Number of consistency check failures",
        &["operation"]
    )
    .unwrap();
}

// Configuration for the proxy
#[derive(Clone, Debug, Deserialize)]
pub struct ProxyConfig {
    pub cassandra_hosts: Vec<String>,
    pub scylla_hosts: Vec<String>,
    pub write_mode: WriteMode,
    pub validation_percentage: f32, // 0.01 = 1% validation
    pub shadow_write_timeout_ms: u64,
    pub primary_write_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub enum WriteMode {
    PrimaryOnly,      // Phase 0: Only write to Cassandra
    DualWriteAsync,   // Phase 1: Write to both, don't wait for ScyllaDB
    DualWriteSync,    // Phase 2: Write to both, wait for both
    ShadowPrimary,    // Phase 3: ScyllaDB primary, Cassandra shadow
    TargetOnly,       // Phase 4: Only write to ScyllaDB (migration complete)
}

// Main proxy state
#[derive(Clone)]
pub struct MigrationProxy {
    cassandra: Arc<CassandraSession>,
    scylla: Arc<ScyllaSession>,
    config: Arc<RwLock<ProxyConfig>>,
    validator: Arc<ConsistencyValidator>,
}

impl MigrationProxy {
    pub async fn new(config: ProxyConfig) -> anyhow::Result<Self> {
        info!("Initializing Migration Proxy with config: {:?}", config);
        
        // Initialize Cassandra connection
        let mut cassandra_cluster = CassandraCluster::default();
        for host in &config.cassandra_hosts {
            cassandra_cluster.set_contact_points(host).unwrap();
        }
        cassandra_cluster.set_protocol_version(4).unwrap();
        let cassandra_session = cassandra_cluster.connect().unwrap();
        
        // Initialize ScyllaDB connection with native Rust driver
        let scylla_session = SessionBuilder::new()
            .known_nodes(&config.scylla_hosts)
            .connection_timeout(Duration::from_secs(10))
            .build()
            .await?;
        
        let validator = Arc::new(ConsistencyValidator::new());
        
        Ok(Self {
            cassandra: Arc::new(cassandra_session),
            scylla: Arc::new(scylla_session),
            config: Arc::new(RwLock::new(config)),
            validator,
        })
    }
    
    // Main write operation with dual-write logic
    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, ProxyError> {
        let start = Instant::now();
        let config = self.config.read().await;
        
        match config.write_mode {
            WriteMode::PrimaryOnly => {
                self.write_cassandra_only(request).await
            }
            WriteMode::DualWriteAsync => {
                self.write_dual_async(request, &config).await
            }
            WriteMode::DualWriteSync => {
                self.write_dual_sync(request, &config).await
            }
            WriteMode::ShadowPrimary => {
                self.write_scylla_primary(request, &config).await
            }
            WriteMode::TargetOnly => {
                self.write_scylla_only(request).await
            }
        }
    }
    
    // Write only to Cassandra (Phase 0)
    async fn write_cassandra_only(&self, request: WriteRequest) -> Result<WriteResponse, ProxyError> {
        let timer = WRITE_LATENCY.with_label_values(&["cassandra", "success"]).start_timer();
        
        let query = self.build_cql_query(&request);
        // Execute on Cassandra
        match self.execute_cassandra_query(&query).await {
            Ok(_) => {
                timer.observe_duration();
                WRITE_COUNTER.with_label_values(&["cassandra", "success"]).inc();
                Ok(WriteResponse {
                    success: true,
                    latency_ms: timer.stop_and_record() * 1000.0,
                    database: "cassandra".to_string(),
                })
            }
            Err(e) => {
                WRITE_COUNTER.with_label_values(&["cassandra", "error"]).inc();
                Err(ProxyError::DatabaseError(e.to_string()))
            }
        }
    }
    
    // Dual write with async shadow write to ScyllaDB (Phase 1)
    async fn write_dual_async(
        &self,
        request: WriteRequest,
        config: &ProxyConfig,
    ) -> Result<WriteResponse, ProxyError> {
        let query = self.build_cql_query(&request);
        let timer = WRITE_LATENCY.with_label_values(&["cassandra", "primary"]).start_timer();
        
        // Write to primary (Cassandra) - this blocks
        let cassandra_result = self.execute_cassandra_query(&query).await;
        let primary_latency = timer.stop_and_record();
        
        // Clone for async ScyllaDB write
        let scylla = self.scylla.clone();
        let query_clone = query.clone();
        let timeout = config.shadow_write_timeout_ms;
        
        // Fire-and-forget write to ScyllaDB
        tokio::spawn(async move {
            let timer = WRITE_LATENCY.with_label_values(&["scylla", "shadow"]).start_timer();
            
            match tokio::time::timeout(
                Duration::from_millis(timeout),
                Self::execute_scylla_query(&scylla, &query_clone),
            )
            .await
            {
                Ok(Ok(_)) => {
                    timer.observe_duration();
                    WRITE_COUNTER.with_label_values(&["scylla", "shadow_success"]).inc();
                }
                Ok(Err(e)) => {
                    warn!("Shadow write to ScyllaDB failed: {}", e);
                    WRITE_COUNTER.with_label_values(&["scylla", "shadow_error"]).inc();
                }
                Err(_) => {
                    warn!("Shadow write to ScyllaDB timed out");
                    WRITE_COUNTER.with_label_values(&["scylla", "shadow_timeout"]).inc();
                }
            }
        });
        
        // Return based on primary result only
        match cassandra_result {
            Ok(_) => {
                WRITE_COUNTER.with_label_values(&["cassandra", "success"]).inc();
                
                // Randomly validate consistency
                if rand::random::<f32>() < config.validation_percentage {
                    self.validator.schedule_validation(request.clone()).await;
                }
                
                Ok(WriteResponse {
                    success: true,
                    latency_ms: primary_latency * 1000.0,
                    database: "cassandra_primary".to_string(),
                })
            }
            Err(e) => {
                WRITE_COUNTER.with_label_values(&["cassandra", "error"]).inc();
                Err(ProxyError::DatabaseError(e.to_string()))
            }
        }
    }
    
    // Dual write with synchronous writes to both (Phase 2)
    async fn write_dual_sync(
        &self,
        request: WriteRequest,
        config: &ProxyConfig,
    ) -> Result<WriteResponse, ProxyError> {
        let query = self.build_cql_query(&request);
        
        // Execute both writes in parallel
        let cassandra_future = self.execute_cassandra_query(&query);
        let scylla_future = Self::execute_scylla_query(&self.scylla, &query);
        
        let (cassandra_result, scylla_result) = tokio::join!(cassandra_future, scylla_future);
        
        // Both must succeed for the write to be considered successful
        match (cassandra_result, scylla_result) {
            (Ok(_), Ok(_)) => {
                WRITE_COUNTER.with_label_values(&["both", "success"]).inc();
                Ok(WriteResponse {
                    success: true,
                    latency_ms: 0.0, // Would track both latencies in production
                    database: "both".to_string(),
                })
            }
            (Err(e), _) | (_, Err(e)) => {
                WRITE_COUNTER.with_label_values(&["both", "error"]).inc();
                Err(ProxyError::DatabaseError(format!("Dual write failed: {}", e)))
            }
        }
    }
    
    // Write to ScyllaDB as primary (Phase 3)
    async fn write_scylla_primary(
        &self,
        request: WriteRequest,
        config: &ProxyConfig,
    ) -> Result<WriteResponse, ProxyError> {
        let query = self.build_cql_query(&request);
        let timer = WRITE_LATENCY.with_label_values(&["scylla", "primary"]).start_timer();
        
        // Write to primary (ScyllaDB) - this blocks
        let scylla_result = Self::execute_scylla_query(&self.scylla, &query).await;
        let primary_latency = timer.stop_and_record();
        
        // Shadow write to Cassandra (async, best-effort)
        let cassandra = self.cassandra.clone();
        let query_clone = query.clone();
        
        tokio::spawn(async move {
            let _ = Self::execute_cassandra_shadow(&cassandra, &query_clone).await;
        });
        
        match scylla_result {
            Ok(_) => {
                WRITE_COUNTER.with_label_values(&["scylla", "success"]).inc();
                Ok(WriteResponse {
                    success: true,
                    latency_ms: primary_latency * 1000.0,
                    database: "scylla_primary".to_string(),
                })
            }
            Err(e) => {
                WRITE_COUNTER.with_label_values(&["scylla", "error"]).inc();
                Err(ProxyError::DatabaseError(e.to_string()))
            }
        }
    }
    
    // Write only to ScyllaDB (Phase 4 - Migration Complete)
    async fn write_scylla_only(&self, request: WriteRequest) -> Result<WriteResponse, ProxyError> {
        let timer = WRITE_LATENCY.with_label_values(&["scylla", "success"]).start_timer();
        
        let query = self.build_cql_query(&request);
        match Self::execute_scylla_query(&self.scylla, &query).await {
            Ok(_) => {
                timer.observe_duration();
                WRITE_COUNTER.with_label_values(&["scylla", "success"]).inc();
                Ok(WriteResponse {
                    success: true,
                    latency_ms: timer.stop_and_record() * 1000.0,
                    database: "scylla".to_string(),
                })
            }
            Err(e) => {
                WRITE_COUNTER.with_label_values(&["scylla", "error"]).inc();
                Err(ProxyError::DatabaseError(e.to_string()))
            }
        }
    }
    
    // Build CQL query from request
    fn build_cql_query(&self, request: &WriteRequest) -> String {
        match &request.operation {
            Operation::Insert { table, data } => {
                format!(
                    "INSERT INTO {} ({}) VALUES ({}) IF NOT EXISTS",
                    table,
                    data.keys().cloned().collect::<Vec<_>>().join(", "),
                    data.values()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Operation::Update { table, data, key } => {
                let set_clause = data
                    .iter()
                    .map(|(k, v)| format!("{} = '{}'", k, v))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("UPDATE {} SET {} WHERE {}", table, set_clause, key)
            }
            Operation::Delete { table, key } => {
                format!("DELETE FROM {} WHERE {}", table, key)
            }
        }
    }
    
    // Execute query on Cassandra
    async fn execute_cassandra_query(&self, query: &str) -> anyhow::Result<()> {
        // Cassandra execution logic
        // In production, this would use the cassandra-cpp async API
        Ok(())
    }
    
    async fn execute_cassandra_shadow(session: &CassandraSession, query: &str) -> anyhow::Result<()> {
        // Shadow write to Cassandra - best effort, no error propagation
        Ok(())
    }
    
    // Execute query on ScyllaDB
    async fn execute_scylla_query(session: &ScyllaSession, query: &str) -> anyhow::Result<()> {
        session.query(query, &[]).await?;
        Ok(())
    }
    
    // Update configuration on the fly (no restart needed!)
    pub async fn update_config(&self, new_config: ProxyConfig) {
        info!("Updating proxy configuration to: {:?}", new_config);
        let mut config = self.config.write().await;
        *config = new_config;
    }
    
    // Health check endpoint
    pub async fn health_check(&self) -> HealthStatus {
        let config = self.config.read().await;
        HealthStatus {
            cassandra_connected: self.check_cassandra_health().await,
            scylla_connected: self.check_scylla_health().await,
            current_mode: format!("{:?}", config.write_mode),
            validation_rate: config.validation_percentage,
        }
    }
    
    async fn check_cassandra_health(&self) -> bool {
        // Check Cassandra connection health
        true // Simplified for demo
    }
    
    async fn check_scylla_health(&self) -> bool {
        // Check ScyllaDB connection health  
        match self.scylla.query("SELECT now() FROM system.local", &[]).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

// Consistency Validator runs in background
pub struct ConsistencyValidator {
    pending_validations: Arc<RwLock<Vec<WriteRequest>>>,
}

impl ConsistencyValidator {
    pub fn new() -> Self {
        Self {
            pending_validations: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    pub async fn schedule_validation(&self, request: WriteRequest) {
        let mut pending = self.pending_validations.write().await;
        pending.push(request);
    }
    
    pub async fn run_validation_loop(
        &self,
        cassandra: Arc<CassandraSession>,
        scylla: Arc<ScyllaSession>,
    ) {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            
            let mut pending = self.pending_validations.write().await;
            if pending.is_empty() {
                continue;
            }
            
            let to_validate = pending.drain(..).collect::<Vec<_>>();
            drop(pending); // Release lock
            
            for request in to_validate {
                // Compare data between Cassandra and ScyllaDB
                match self.validate_consistency(&request, &cassandra, &scylla).await {
                    Ok(true) => {
                        info!("Consistency check passed for {:?}", request);
                    }
                    Ok(false) => {
                        warn!("Consistency mismatch detected for {:?}", request);
                        CONSISTENCY_MISMATCHES.with_label_values(&["data_mismatch"]).inc();
                    }
                    Err(e) => {
                        error!("Consistency check failed: {}", e);
                        CONSISTENCY_MISMATCHES.with_label_values(&["check_error"]).inc();
                    }
                }
            }
        }
    }
    
    async fn validate_consistency(
        &self,
        request: &WriteRequest,
        cassandra: &CassandraSession,
        scylla: &ScyllaSession,
    ) -> anyhow::Result<bool> {
        // Read from both databases and compare
        // This is trivialized - actual production code in Git would handle all data types
        Ok(true)
    }
}

// Request/Response types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteRequest {
    pub operation: Operation,
    pub consistency: Option<String>,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Insert {
        table: String,
        data: std::collections::HashMap<String, String>,
    },
    Update {
        table: String,
        data: std::collections::HashMap<String, String>,
        key: String,
    },
    Delete {
        table: String,
        key: String,
    },
}

#[derive(Debug, Serialize)]
pub struct WriteResponse {
    pub success: bool,
    pub latency_ms: f64,
    pub database: String,
}

#[derive(Debug, Serialize)]
pub struct HealthStatus {
    pub cassandra_connected: bool,
    pub scylla_connected: bool,
    pub current_mode: String,
    pub validation_rate: f32,
}

#[derive(Debug)]
pub enum ProxyError {
    DatabaseError(String),
    ValidationError(String),
    ConfigError(String),
}

impl From<ProxyError> for StatusCode {
    fn from(err: ProxyError) -> Self {
        match err {
            ProxyError::DatabaseError(_) => StatusCode::SERVICE_UNAVAILABLE,
            ProxyError::ValidationError(_) => StatusCode::CONFLICT,
            ProxyError::ConfigError(_) => StatusCode::BAD_REQUEST,
        }
    }
}

// HTTP Server setup
pub async fn create_app(proxy: MigrationProxy) -> Router {
    Router::new()
        .route("/write", post(handle_write))
        .route("/health", get(handle_health))
        .route("/config", post(handle_config_update))
        .route("/metrics", get(handle_metrics))
        .layer(TraceLayer::new_for_http())
        .with_state(proxy)
}

async fn handle_write(
    State(proxy): State<MigrationProxy>,
    Json(request): Json<WriteRequest>,
) -> Result<Json<WriteResponse>, StatusCode> {
    match proxy.write(request).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => Err(e.into()),
    }
}

async fn handle_health(State(proxy): State<MigrationProxy>) -> Json<HealthStatus> {
    Json(proxy.health_check().await)
}

async fn handle_config_update(
    State(proxy): State<MigrationProxy>,
    Json(config): Json<ProxyConfig>,
) -> StatusCode {
    proxy.update_config(config).await;
    StatusCode::OK
}

async fn handle_metrics() -> String {
    // Return Prometheus metrics
    prometheus::TextEncoder::new()
        .encode_to_string(&prometheus::gather())
        .unwrap()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    // Load configuration
    let config = ProxyConfig {
        cassandra_hosts: vec!["cassandra-1.gcp.enginevector.io".to_string()],
        scylla_hosts: vec!["scylla-1.aws.enginevector.io".to_string()],
        write_mode: WriteMode::DualWriteAsync,
        validation_percentage: 0.01, // 1% validation
        shadow_write_timeout_ms: 100,
        primary_write_timeout_ms: 1000,
    };
    
    // Create proxy
    let proxy = MigrationProxy::new(config).await?;
    
    // Start validation loop in background
    let validator = proxy.validator.clone();
    let cassandra = proxy.cassandra.clone();
    let scylla = proxy.scylla.clone();
    tokio::spawn(async move {
        validator.run_validation_loop(cassandra, scylla).await;
    });
    
    // Create and run HTTP server
    let app = create_app(proxy).await;
    
    info!("Starting Migration Proxy on port 8080");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}
```

## The Four Stages of the Dual-Write Proxy Service (To Full Data Transition End)

```shell
Phase 1 - DualWriteAsync (Current):
  Cassandra: SYNCHRONOUS (primary) ✓
  ScyllaDB:  ASYNCHRONOUS (shadow) 
  App Impact: NONE (still <1ms latency)

Phase 2 - DualWriteSync (Validation):
  Cassandra: SYNCHRONOUS (primary) ✓
  ScyllaDB:  SYNCHRONOUS (also waits) ✓
  App Impact: Slight increase in latency
  Purpose: Ensure ScyllaDB can handle load

Phase 3 - ShadowPrimary (Flip):
  Cassandra: ASYNCHRONOUS (shadow)
  ScyllaDB:  SYNCHRONOUS (primary) ✓
  App Impact: Now using ScyllaDB's performance!

Phase 4 - TargetOnly (Complete):
  Cassandra: NONE (decommissioned)
  ScyllaDB:  SYNCHRONOUS (only database) ✓
  App Impact: Full ScyllaDB benefits
```



## Client Applications Use of Dual-Write Proxy Service

```python
"""
Python client for the Rust-based Migration Proxy
"""

import asyncio
import json
import time
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, Optional, Any, List
import aiohttp
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WriteMode(Enum):
    """Migration phases - matches Rust enum exactly"""
    PRIMARY_ONLY = "PrimaryOnly"          # Phase 0: Cassandra only
    DUAL_WRITE_ASYNC = "DualWriteAsync"   # Phase 1: Shadow writes
    DUAL_WRITE_SYNC = "DualWriteSync"     # Phase 2: Synchronous dual writes
    SHADOW_PRIMARY = "ShadowPrimary"      # Phase 3: ScyllaDB primary
    TARGET_ONLY = "TargetOnly"            # Phase 4: ScyllaDB only


@dataclass
class ProxyConfig:
    """Configuration that can be updated on the fly"""
    cassandra_hosts: List[str]
    scylla_hosts: List[str]
    write_mode: WriteMode
    validation_percentage: float
    shadow_write_timeout_ms: int
    primary_write_timeout_ms: int


class MigrationProxyClient:
    """
    Python client for the Rust migration proxy.
    This shows that ANY application language can use our proxy.
    """
    
    def __init__(self, proxy_url: str = "http://localhost:8080"):
        self.proxy_url = proxy_url
        self.session = None
        self._metrics = {
            "writes": 0,
            "errors": 0,
            "total_latency_ms": 0
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        await self.health_check()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def health_check(self) -> Dict[str, Any]:
        """Check proxy health and connectivity"""
        async with self.session.get(f"{self.proxy_url}/health") as response:
            health = await response.json()
            logger.info(f"Proxy Health: {health}")
            return health
    
    async def insert(self, 
                    table: str, 
                    data: Dict[str, Any],
                    consistency: Optional[str] = None) -> Dict[str, Any]:
        """
        Insert data through the proxy
        
        Example:
            await client.insert(
                table="media_assets",
                data={
                    "asset_id": "uuid-12345",
                    "title": "Corporate Video Q4",
                    "duration": "1800",
                    "codec": "h264"
                }
            )
        """
        request = {
            "operation": {
                "Insert": {
                    "table": table,
                    "data": {str(k): str(v) for k, v in data.items()}
                }
            },
            "consistency": consistency,
            "timeout_ms": 5000
        }
        
        return await self._execute_write(request)
    
    async def update(self,
                    table: str,
                    data: Dict[str, Any],
                    key: str,
                    consistency: Optional[str] = None) -> Dict[str, Any]:
        """
        Update data through the proxy
        
        Example:
            await client.update(
                table="media_assets",
                data={"status": "processed", "updated_at": "2024-01-01"},
                key="asset_id = 'uuid-12345'"
            )
        """
        request = {
            "operation": {
                "Update": {
                    "table": table,
                    "data": {str(k): str(v) for k, v in data.items()},
                    "key": key
                }
            },
            "consistency": consistency,
            "timeout_ms": 5000
        }
        
        return await self._execute_write(request)
    
    async def delete(self,
                    table: str,
                    key: str,
                    consistency: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete data through the proxy
        
        Example:
            await client.delete(
                table="media_assets",
                key="asset_id = 'uuid-12345'"
            )
        """
        request = {
            "operation": {
                "Delete": {
                    "table": table,
                    "key": key
                }
            },
            "consistency": consistency,
            "timeout_ms": 5000
        }
        
        return await self._execute_write(request)
    
    async def _execute_write(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Execute write operation through the proxy"""
        start_time = time.time()
        
        try:
            async with self.session.post(
                f"{self.proxy_url}/write",
                json=request,
                headers={"Content-Type": "application/json"}
            ) as response:
                result = await response.json()
                
                # Track metrics
                self._metrics["writes"] += 1
                latency = (time.time() - start_time) * 1000
                self._metrics["total_latency_ms"] += latency
                
                if response.status == 200:
                    logger.debug(f"Write successful: {result}")
                    return result
                else:
                    self._metrics["errors"] += 1
                    logger.error(f"Write failed with status {response.status}: {result}")
                    raise Exception(f"Write failed: {result}")
                    
        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(f"Write operation failed: {e}")
            raise
    
    async def update_proxy_config(self, config: ProxyConfig) -> bool:
        """
        Update proxy configuration without restart
        This is how we progress through migration phases!
        """
        config_dict = {
            "cassandra_hosts": config.cassandra_hosts,
            "scylla_hosts": config.scylla_hosts,
            "write_mode": config.write_mode.value,
            "validation_percentage": config.validation_percentage,
            "shadow_write_timeout_ms": config.shadow_write_timeout_ms,
            "primary_write_timeout_ms": config.primary_write_timeout_ms
        }
        
        async with self.session.post(
            f"{self.proxy_url}/config",
            json=config_dict
        ) as response:
            if response.status == 200:
                logger.info(f"Successfully updated proxy to {config.write_mode.value} mode")
                return True
            else:
                logger.error(f"Failed to update proxy config: {response.status}")
                return False
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get Prometheus metrics from the proxy"""
        async with self.session.get(f"{self.proxy_url}/metrics") as response:
            metrics_text = await response.text()
            return self._parse_prometheus_metrics(metrics_text)
    
    def _parse_prometheus_metrics(self, metrics_text: str) -> Dict[str, Any]:
        """Parse Prometheus text format into dict"""
        metrics = {}
        for line in metrics_text.split('\n'):
            if line and not line.startswith('#'):
                parts = line.split(' ')
                if len(parts) == 2:
                    metric_name = parts[0].split('{')[0]
                    metric_value = float(parts[1])
                    metrics[metric_name] = metric_value
        return metrics
    
    def get_client_metrics(self) -> Dict[str, Any]:
        """Get client-side metrics"""
        avg_latency = 0
        if self._metrics["writes"] > 0:
            avg_latency = self._metrics["total_latency_ms"] / self._metrics["writes"]
        
        return {
            "total_writes": self._metrics["writes"],
            "total_errors": self._metrics["errors"],
            "average_latency_ms": avg_latency,
            "error_rate": self._metrics["errors"] / max(1, self._metrics["writes"])
        }


class MigrationOrchestrator:
    """
    Orchestrates the migration through different phases
    This would run as a separate control plane service
    """
    
    def __init__(self, proxy_client: MigrationProxyClient):
        self.proxy_client = proxy_client
        self.current_phase = WriteMode.PRIMARY_ONLY
        
    async def start_shadow_writes(self):
        """Phase 1: Begin shadow writes to ScyllaDB"""
        logger.info("Starting Phase 1: Shadow Writes")
        
        config = ProxyConfig(
            cassandra_hosts=["cassandra.gcp.example.com"],
            scylla_hosts=["scylla.aws.example.com"],
            write_mode=WriteMode.DUAL_WRITE_ASYNC,
            validation_percentage=0.01,  # Start with 1% validation
            shadow_write_timeout_ms=100,
            primary_write_timeout_ms=1000
        )
        
        success = await self.proxy_client.update_proxy_config(config)
        if success:
            self.current_phase = WriteMode.DUAL_WRITE_ASYNC
            logger.info("Shadow writes enabled")
        return success
    
    async def increase_validation(self, percentage: float):
        """Gradually increase validation percentage"""
        logger.info(f"Increasing validation to {percentage*100}%")
        
        config = ProxyConfig(
            cassandra_hosts=["cassandra.gcp.example.com"],
            scylla_hosts=["scylla.aws.example.com"],
            write_mode=self.current_phase,
            validation_percentage=percentage,
            shadow_write_timeout_ms=100,
            primary_write_timeout_ms=1000
        )
        
        return await self.proxy_client.update_proxy_config(config)
    
    async def enable_sync_writes(self):
        """Phase 2: Enable synchronous dual writes"""
        logger.info("Starting Phase 2: Synchronous Dual Writes")
        
        config = ProxyConfig(
            cassandra_hosts=["cassandra.gcp.example.com"],
            scylla_hosts=["scylla.aws.example.com"],
            write_mode=WriteMode.DUAL_WRITE_SYNC,
            validation_percentage=0.10,  # Increase validation
            shadow_write_timeout_ms=500,
            primary_write_timeout_ms=1000
        )
        
        success = await self.proxy_client.update_proxy_config(config)
        if success:
            self.current_phase = WriteMode.DUAL_WRITE_SYNC
            logger.info("Synchronous dual writes enabled")
        return success
    
    async def switch_primary_to_scylla(self):
        """Phase 3: Make ScyllaDB the primary"""
        logger.info("Starting Phase 3: ScyllaDB as Primary")
        
        config = ProxyConfig(
            cassandra_hosts=["cassandra.gcp.example.com"],
            scylla_hosts=["scylla.aws.example.com"],
            write_mode=WriteMode.SHADOW_PRIMARY,
            validation_percentage=0.05,
            shadow_write_timeout_ms=100,
            primary_write_timeout_ms=1000
        )
        
        success = await self.proxy_client.update_proxy_config(config)
        if success:
            self.current_phase = WriteMode.SHADOW_PRIMARY
            logger.info("ScyllaDB is now primary")
        return success
    
    async def complete_migration(self):
        """Phase 4: Complete migration to ScyllaDB only"""
        logger.info("Starting Phase 4: ScyllaDB Only")
        
        config = ProxyConfig(
            cassandra_hosts=[],  # No longer needed
            scylla_hosts=["scylla.aws.example.com"],
            write_mode=WriteMode.TARGET_ONLY,
            validation_percentage=0.0,  # No validation needed
            shadow_write_timeout_ms=0,
            primary_write_timeout_ms=1000
        )
        
        success = await self.proxy_client.update_proxy_config(config)
        if success:
            self.current_phase = WriteMode.TARGET_ONLY
            logger.info("Migration complete! Running on ScyllaDB only")
        return success
    
    async def monitor_migration(self, duration_seconds: int = 60):
        """Monitor migration metrics"""
        logger.info(f"📈 Monitoring migration for {duration_seconds} seconds...")
        
        start_time = time.time()
        while time.time() - start_time < duration_seconds:
            metrics = await self.proxy_client.get_metrics()
            client_metrics = self.proxy_client.get_client_metrics()
            
            logger.info(f"""
            Migration Metrics:
            - Phase: {self.current_phase.value}
            - Client writes: {client_metrics['total_writes']}
            - Error rate: {client_metrics['error_rate']:.2%}
            - Avg latency: {client_metrics['average_latency_ms']:.2f}ms
            """)
            
            await asyncio.sleep(10)


# Use of showing how third-party stream media application would integrate
async def enginevector_stream_application_svc():
    """
    Showcase of how the third-party media application would use the proxy
    The application code doesn't change - just the connection endpoint
    """
    
    # Instead of connecting directly to Cassandra, connect to the proxy
    async with MigrationProxyClient("http://migration-proxy:8080") as client:
        
        # Normal application operations - no code changes needed!
        
        # Insert a new media asset
        await client.insert(
            table="media_assets",
            data={
                "asset_id": "550e8400-e29b-41d4-a716-446655440000",
                "title": "Q4 2024 Earnings Call",
                "file_path": "s3://media-bucket/earnings/q4-2024.mp4",
                "duration": 3600,
                "codec": "h264",
                "bitrate": 5000000,
                "resolution": "1920x1080",
                "created_at": datetime.now().isoformat()
            }
        )
        
        # Update asset status after processing
        await client.update(
            table="media_assets",
            data={
                "status": "transcoded",
                "thumbnail_path": "s3://media-bucket/thumbnails/q4-2024.jpg",
                "updated_at": datetime.now().isoformat()
            },
            key="asset_id = '550e8400-e29b-41d4-a716-446655440000'"
        )
        
        # Bulk insert for batch operations
        for i in range(100):
            await client.insert(
                table="media_chunks",
                data={
                    "chunk_id": f"chunk-{i}",
                    "asset_id": "550e8400-e29b-41d4-a716-446655440000",
                    "sequence": i,
                    "data": f"base64_encoded_chunk_{i}",
                    "size": 1048576  # 1MB chunks
                }
            )
        
        # Get metrics to monitor performance
        metrics = client.get_client_metrics()
        logger.info(f"Application metrics: {metrics}")


async def migration_control_plane_svc():
    """
    Scenario of how the migration would be orchestrated
    This would be a separate control plane service
    """
    
    async with MigrationProxyClient("http://migration-proxy:8080") as client:
        orchestrator = MigrationOrchestrator(client)
        
        # Check initial health
        health = await client.health_check()
        logger.info(f"Initial proxy health: {health}")
        
        # Phase 1: Start shadow writes
        await orchestrator.start_shadow_writes()
        await asyncio.sleep(5)  # Let some traffic flow
        
        # Gradually increase validation
        await orchestrator.increase_validation(0.05)  # 5%
        await asyncio.sleep(5)
        await orchestrator.increase_validation(0.10)  # 10%
        
        # Monitor for period of time
        await orchestrator.monitor_migration(30)
        
        # Phase 2: Synchronous writes (if metrics look good)
        await orchestrator.enable_sync_writes()
        await orchestrator.monitor_migration(30)
        
        # Phase 3: Switch primary
        await orchestrator.switch_primary_to_scylla()
        await orchestrator.monitor_migration(30)
        
        # Phase 4: Complete migration
        await orchestrator.complete_migration()
        
        logger.info("Migration completed successfully!")


if __name__ == "__main__":
    # Run the services
    asyncio.run(enginevector_stream_application_svc())
    
    # Uncomment to run migration orchestration
    # asyncio.run(migration_control_plane_svc())
```


## The SSTableLoader Bulk Transfer Processor

The deployment location of this service is on the target cloud side. If the DB clusters involved are Cassandra or ScyllaDB on GCP (GKE, GCP Compute Instance VM) on source side and the target Cassandra DB or ScyllaDB Cluster is on AWS (EKS or EC2, Firecracker), the `sstable-loader` service will require is location on the AWS target side. 

The SSTableLoader requires its location on the target side. 

**WIP** Add Diagrams 

### The Advantages of Target Side Locationing

- **Network Locality** Loads data directly into AWS Cassandra or SycllaDB cluster
- **Network Throughput** No Cross-Cloud data transfer during bulk load
- **Cost** Avoids GCP egress charges for bulk data
- **Performance** Local disk I/O on AWS EC2



## The Dual-Reader Service

The deployment location of this service is NOT strictly enforced on source or target side. 
It is advised that this service location execute on the target side of the transfer to avoid costly egress costs from the source cloud side (if GCP to AWS), the `dual-reader` service is at its advantage if it is deployed on the AWS side (EKS, EC2, Firecracker).

**WIP** Add Diagrams 

### The Advantages of Target Side Locationing

- **Network Symmetry** If on AWS, 1 read is local, 1 is remote (vs 2 remote if on GCP)
- **Cost** Avoid GCP egress for validation reads
- **After Cutover** Dual Reader validator stays on AWS to focus AWS target on Cassandra DB



## Cassandra DB vs ScyllaDB Kubernetes Operator Cost Reduction

The high-cognitive load of configuration and JVM overhead using the Cassandra DB and its deployment using the Cassandra DB Kubernetes Operator.

```shell
# cassandra-dc1.yaml
apiVersion: cassandra.datastax.com/v1beta1
kind: CassandraDatacenter
metadata:
  name: dc1
spec:
  clusterName: enginevector-cluster
  serverVersion: "4.0.7"
  size: 12  # Requires 4x nodes vs ScyllaDB (3)
  config:
    jvm-options:
      initial_heap_size: "8G"
      max_heap_size: "8G"
      # GC Tuning cognitive-overload
      additional-jvm-opts:
        - "-XX:+UseG1GC"
        - "-XX:G1RSetUpdatingPauseTimePercent=5"
        - "-XX:MaxGCPauseMillis=300"
        # ... 20 more GC flags
```

The low-cognitive load of configuration and no-JVM overhead using the Scylla DB (uses 100% C++) and its deployment using the ScyllaDB Kubernetes Operator.

```shell
# scylla-cluster.yaml - Self-tuning (automatic):
apiVersion: scylla.scylladb.com/v1
kind: ScyllaCluster
metadata:
  name: enginevector-cluster
spec:
  version: 5.2.0
  developerMode: false  # Production mode = auto-tuning!
  cpuset: true  # CPU pinning for zero latency
  size: 3  # 3 nodes instead of 12
  resources:
    requests:
      cpu: 7  # Leaves 1 CPU for system
      memory: 30Gi  # No heap sizing
```

The applications using CassandraDB and to the switch to SycllaDB provides 100% transparent functionality with no code changes.

```rust
//Existing Cassandra DB code:
let cluster = Cluster::new(vec!["cassandra-node1", "cassandra-node2"]);
let session = cluster.connect("engine-vector");

// ScyllaDB (identical)
let cluster = Cluster::new(vec!["scylla-node1", "scylla-node2"]);
let session = cluster.connect("engine-vector");
```



```shell
# Changes with ScyllaDB Operator on EKS:

Cassandra on EKS:
- Memory: 32GB (8GB heap + 24GB off-heap)
- GC Pauses: 200-800ms (G1GC)
- Nodes needed: 12 for your workload
- Annual EC2 cost: ~$105,000 (m5.2xlarge)

ScyllaDB on EKS:  
- Memory: 32GB (ALL used efficiently, no JVM)
- GC Pauses: ZERO (C++ memory management)
- Nodes needed: 3-4 for same workload
- Annual EC2 cost: ~$35,000 (i3.2xlarge with NVMe)
- TCO Savings: ~70% ($70,000/year!)
```



## Project Structure

```shell
scylla-cluster-sync-rs
├── Cargo.toml
├── LICENSE
├── Makefile
├── README-DW-SSTLoader-Processor-DR.md
├── README.md
├── config
│   ├── dual-reader.yaml
│   ├── dual-writer.yaml
│   └── sstable-loader.yaml
├── docker-compose.yaml
├── docs
│   └── Dual-Writer-Data-Synch-Architecture.png
├── services
│   ├── dual-reader
│   │   ├── Cargo.toml
│   │   ├── Dockerfile.dual-reader
│   │   ├── Makefile
│   │   └── src
│   │       ├── api.rs
│   │       ├── config.rs
│   │       ├── main.rs
│   │       ├── reader.rs
│   │       ├── reconciliation.rs
│   │       └── validator.rs
│   ├── dual-writer
│   │   ├── Cargo.toml
│   │   ├── Dockerfile.dual-writer
│   │   ├── Makefile
│   │   └── src
│   │       ├── api.rs
│   │       ├── config.rs
│   │       ├── health.rs
│   │       ├── main.rs
│   │       └── writer.rs
│   └── sstable-loader
│       ├── Cargo.toml
│       ├── Dockerfile.sstable-loader
│       ├── Makefile
│       └── src
│           ├── api.rs
│           ├── config.rs
│           ├── loader.rs
│           ├── main.rs
│           └── token_range.rs
└── svckit
    ├── Cargo.toml
    └── src
        ├── config.rs
        ├── database
        │   ├── cassandra.rs
        │   ├── connection.rs
        │   ├── mod.rs
        │   ├── query_builder.rs
        │   ├── retry.rs
        │   └── scylla.rs
        ├── errors.rs
        ├── lib.rs
        ├── metrics.rs
        └── types.rs
```


## Compiling the Services

```shell
# From repository root
cd scylla-cluster-sync-rs

# Check workspace structure
cargo check --workspace

# Build all services
cargo build --workspace

# Build individual services
cargo build --bin dual-writer
cargo build --bin dual-reader
cargo build --bin sstable-loader
```

**Or** with the provided root Makefile.


 






## References

Cassandra DB Kubernetes Operator 
- https://docs.k8ssandra.io/components/cass-operator/

ScyllaDB Kubernetes Operator 
- https://www.scylladb.com/product/scylladb-operator-kubernetes/