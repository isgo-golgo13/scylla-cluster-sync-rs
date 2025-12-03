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
async fn write_dual_async(&self, request: &WriteRequest) -> Result<WriteResponse, SyncError> {
    let query = QueryBuilder::build_insert_query(request);
    let start = Instant::now();
    
    // 1. SYNCHRONOUS write to source (PRIMARY) - This BLOCKS!
    self.source_conn.execute(&query, request.values.clone()).await?;
    let primary_latency = start.elapsed().as_secs_f64() * 1000.0;
    
    // 2. Check FilterGovernor before shadow write (tenant/table blacklist)
    let should_write_to_target = self.filter_governor.should_write_to_target(request).await;
    
    if !should_write_to_target {
        // Tenant/table is blacklisted - skip target write entirely
        info!(
            "Skipping target write for blacklisted request: {}.{}", 
            request.keyspace, request.table
        );
        
        let mut stats = self.stats.lock();
        stats.filtered_writes += 1;
        
        return Ok(WriteResponse {
            success: true,
            request_id: request.request_id,
            write_timestamp: request.timestamp.unwrap_or(0),
            latency_ms: primary_latency,
            error: None,
        });
    }
    
    // 3. ASYNCHRONOUS write to target (SHADOW) - Fire and forget!
    let target_conn = self.target_conn.clone();
    let request_clone = request.clone();
    let query_clone = query.clone();
    let failed_writes = self.failed_writes.clone();
    let config = self.config.read().await.clone();
    
    tokio::spawn(async move {  // <-- This spawns a separate task!
        let timeout = Duration::from_millis(config.writer.shadow_timeout_ms);
        let result = tokio::time::timeout(
            timeout,
            target_conn.execute(&query_clone, request_clone.values.clone())
        ).await;
        
        match result {
            Ok(Ok(_)) => {
                // Shadow write succeeded
                info!("Shadow write successful for request {}", request_clone.request_id);
            }
            Ok(Err(e)) => {
                // Shadow write failed - queue for retry, DON'T fail the request
                warn!("Shadow write failed for request {}: {:?}", request_clone.request_id, e);
                failed_writes.insert(
                    request_clone.request_id,
                    FailedWrite {
                        request: request_clone,
                        error: e.to_string(),
                        attempts: 1,
                        first_attempt: SystemTime::now(),
                    }
                );
            }
            Err(_) => {
                // Shadow write timed out - queue for retry
                warn!("Shadow write timed out for request {}", request_clone.request_id);
                failed_writes.insert(
                    request_clone.request_id,
                    FailedWrite {
                        request: request_clone.clone(),
                        error: "Timeout".to_string(),
                        attempts: 1,
                        first_attempt: SystemTime::now(),
                    }
                );
            }
        }
    });
    
    // 4. Return based on PRIMARY (source) result ONLY
    //    Success returned immediately after source write
    //    No wait for target - shadow write is fire-and-forget
    Ok(WriteResponse {
        success: true,
        request_id: request.request_id,
        write_timestamp: request.timestamp.unwrap_or(0),
        latency_ms: primary_latency,
        error: None,
    })
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

## The Dual-Write DB Duality of Functionality


![dual-writer-duality-of-service-configuration](docs/Dual-Writer-Docker-Image-ConfigMap.png)


The `dual-writer` design uses the `Factory Design Pattern` in crate `svckit/src/database/factory.rs` to instantiate the correct connection resource (Scylla to Scylla or Cassandra to Cassandra, or mixed-mode source of the DB and the target DB for the data migration). The ScyllaDB driver is 100% adaptable to Cassandra (to the latest Cassandra 4 driver). The client that requires a data migration of a ScyllaDB to ScyllaDB scenario will configure the values in the provided root directory `config/dual-writer-scylla.yaml` or `config/dual-writer-cassandra.yaml`, however these are templates and the actual configuration of each will go into a Kubernetes ConfigMap and have the ConfigMap referenced in the Kubernetes `Deployment` spec. No configuration logic for the dual-writer is hard-coded. 




## The Dual-Write Proxy Service Architecture (Alternate No-GKE, No-EKS)

The following graphic shows the architectual workflow of the `Dual-Write Proxy` service deployed directly to VMs (source VM on GCP and sink/target VM on AWS) without Kubernetes.

![dual-write-proxy-architecture-gcp-aws](docs/Dual-Writer-Data-Synch-Architecture-NoK8s.png)


## The Dual-Write Proxy Service Pattern (Low-Level)

The non-multi crate pre-prod view of the code for the Dual-Write Proxy service (NOT using Envoy Proxy).
The `dual-writer` proxy service accepts native ScyllaDB/CassandraDB driver connections through the provided `cql_server.rs` crate module. It provides **CQL** binary protocol v4 for transparent dual-writing.

```rust
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error, debug};
use uuid::Uuid;
use anyhow::{Result, Context};

use crate::writer::DualWriter;
use svckit::types::WriteRequest;

const CQL_VERSION: u8 = 0x04; // CQL v4

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
}

#[derive(Clone)]
pub struct MigrationProxy {
    writer: Arc<DualWriter>,
    filter_governor: Arc<FilterGovernor>,
}

impl MigrationProxy {
    pub async fn new(config: DualWriterConfig) -> anyhow::Result<Self> {
        info!("Initializing CQL Migration Proxy");
        
        // Load filter configuration
        let filter_config = load_filter_config("config/filter-rules.yaml")?;
        let filter_governor = Arc::new(FilterGovernor::new(&filter_config));
        
        // Initialize dual writer
        let writer = Arc::new(DualWriter::new(config, filter_governor.clone()).await?);
        
        Ok(Self {
            writer,
            filter_governor,
        })
    }
    
    pub async fn start_cql_server(self, bind_addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .context("Failed to bind CQL server")?;
        
        info!("CQL server listening on {}", bind_addr);
        
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
                Err(e) => error!("Failed to accept connection: {}", e),
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, writer: Arc<DualWriter>) -> Result<()> {
    loop {
        // Read CQL frame header (9 bytes)
        let header = read_frame_header(&mut stream).await?;
        
        // Read frame body
        let mut body = vec![0u8; header.body_length as usize];
        if header.body_length > 0 {
            stream.read_exact(&mut body).await?;
        }
        
        // Process frame based on opcode
        let response = match Opcode::from_u8(header.opcode) {
            Some(Opcode::Startup) => handle_startup(&header).await?,
            Some(Opcode::Query) => handle_query(&header, &body, &writer).await?,
            Some(Opcode::Prepare) => handle_prepare(&header, &body).await?,
            Some(Opcode::Execute) => handle_execute(&header, &body, &writer).await?,
            _ => build_error_frame(header.stream_id, "Unsupported opcode")?,
        };
        
        // Write response
        stream.write_all(&response).await?;
        stream.flush().await?;
    }
}

async fn handle_query(
    header: &FrameHeader, 
    body: &[u8], 
    writer: &Arc<DualWriter>
) -> Result<Vec<u8>> {
    let (query, values) = parse_query_frame(body)?;
    let query_upper = query.trim().to_uppercase();
    
    if query_upper.starts_with("SELECT") {
        // SELECT: Route to source cluster only
        debug!("SELECT query - routing to source cluster");
        match writer.query_source(&query, values).await {
            Ok(rows) => build_rows_result(header.stream_id, rows),
            Err(e) => {
                error!("SELECT failed: {}", e);
                build_error_frame(header.stream_id, &format!("SELECT failed: {}", e))
            }
        }
    } else if query_upper.starts_with("INSERT") 
           || query_upper.starts_with("UPDATE") 
           || query_upper.starts_with("DELETE") {
        // Write operations: Dual-write to both clusters
        debug!("Write query - dual-writing to both clusters");
        
        let (keyspace, table) = extract_keyspace_table(&query)?;
        
        let request = WriteRequest {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            query: query.clone(),
            values,
            consistency: None,
            request_id: Uuid::new_v4(),
            timestamp: None,
        };
        
        match writer.write(request).await {
            Ok(_) => build_void_result(header.stream_id),
            Err(e) => {
                error!("Write failed: {}", e);
                build_error_frame(header.stream_id, &format!("Write failed: {}", e))
            }
        }
    } else {
        // DDL/Other queries: Route to source
        debug!("DDL/Other query - routing to source cluster");
        match writer.query_source(&query, values).await {
            Ok(_) => build_void_result(header.stream_id),
            Err(e) => build_error_frame(header.stream_id, &format!("Query failed: {}", e)),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    
    let config = load_config("config.yaml")?;
    let proxy = MigrationProxy::new(config).await?;
    
    let bind_addr: SocketAddr = "0.0.0.0:9042".parse()?;
    info!("Starting CQL Migration Proxy on {}", bind_addr);
    
    proxy.start_cql_server(bind_addr).await?;
    
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



## Dual-Writer CQL Proxy Two Separate Layers**
```
┌─────────────────────────────────────────────┐
│ LAYER 1: App → Proxy                       │
│ Protocol: CQL Binary v4                     │
│ Parsed by: cassandra-protocol crate        │
└─────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│ LAYER 2: Proxy → Database                   │
│ Protocol: CQL Binary v4 (same!)             │
│ Generated by: scylla crate                  │
│ Works with: Cassandra 4.x AND ScyllaDB      │
└─────────────────────────────────────────────┘
```


### Testing the CQL Proxy Layer (Python Client)

```python
#!/usr/bin/env python3
"""
Test client for CQL dual-writer proxy
Shows that Python cassandra-driver works transparently
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import time

def test_cql_proxy():
    """Test CQL proxy with Python cassandra-driver"""
    
    print("Connecting to CQL dual-writer proxy...")
    
    # Connect to dual-writer proxy (NOT directly to Cassandra!)
    cluster = Cluster(
        contact_points=['127.0.0.1'],
        port=9042,  # CQL proxy port
        protocol_version=4
    )
    
    session = cluster.connect()
    print("Connected to CQL proxy")
    
    # Set keyspace
    session.set_keyspace('files_keyspace')
    print("Keyspace set to files_keyspace")
    
    # Test INSERT
    file_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    
    query = """
    INSERT INTO files (system_domain_id, id, name, size, status)
    VALUES (?, ?, ?, ?, ?)
    """
    
    print(f"\nInserting test file: {file_id}")
    session.execute(query, [tenant_id, file_id, "test.jpg", 1024, "CLOSED"])
    print("INSERT successful - dual-written to GCP + AWS!")
    
    # Test SELECT
    query = "SELECT * FROM files WHERE system_domain_id = ? AND id = ?"
    print(f"\nQuerying file: {file_id}")
    rows = session.execute(query, [tenant_id, file_id])
    
    for row in rows:
        print(f"Found: {row.name} ({row.size} bytes, status={row.status})")
    
    # Test UPDATE
    query = """
    UPDATE files SET status = ? WHERE system_domain_id = ? AND id = ?
    """
    print(f"\nUpdating file status...")
    session.execute(query, ["ARCHIVED", tenant_id, file_id])
    print("UPDATE successful - dual-written to both clusters!")
    
    # Test blacklist (optional - requires tenant in blacklist)
    blacklisted_tenant = uuid.UUID("e19206ba-c584-11e8-882a-0a580a30028e")
    query = """
    INSERT INTO files (system_domain_id, id, name, size, status)
    VALUES (?, ?, ?, ?, ?)
    """
    print(f"\nTesting blacklist with tenant: {blacklisted_tenant}")
    session.execute(query, [blacklisted_tenant, uuid.uuid4(), "blocked.jpg", 2048, "CLOSED"])
    print("Request accepted (written to GCP only, AWS skipped)")
    
    cluster.shutdown()
    print("\nAll tests passed!")
    print("CQL proxy working transparently with Python cassandra-driver!")

if __name__ == "__main__":
    try:
        test_cql_proxy()
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
```




## Client Applications Use of Dual-Write Proxy Service (Using CQL Native Driver)

The following Python 3 application template that shows how to connect and call APIs to the `dual-writer` proxy service (service is deployed to Kubernetes). The `dual-writer` proxy uses native SycllaDB (Cassandra 100% compatible) drive that is transparent to the Python calling client application.

```python
"""
Python client template for the Rust developed CQL Migration Proxy
"""

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Any, List
import logging
from datetime import datetime
import uuid

# Native Cassandra driver
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel as CassConsistency

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WriteMode(Enum):
    """Migration phases - matches Rust enum exactly"""
    SOURCE_ONLY = "SourceOnly"      # Phase 0: Source (GCP/Cassandra) only
    DUAL_ASYNC = "DualAsync"        # Phase 1: Shadow writes (async to target)
    DUAL_SYNC = "DualSync"          # Phase 2: Synchronous dual writes
    TARGET_ONLY = "TargetOnly"      # Phase 3: Target (AWS/ScyllaDB) only


class ConsistencyLevel(Enum):
    """CQL consistency levels - matches Rust enum"""
    ANY = CassConsistency.ANY
    ONE = CassConsistency.ONE
    TWO = CassConsistency.TWO
    THREE = CassConsistency.THREE
    QUORUM = CassConsistency.QUORUM
    ALL = CassConsistency.ALL
    LOCAL_QUORUM = CassConsistency.LOCAL_QUORUM
    EACH_QUORUM = CassConsistency.EACH_QUORUM
    LOCAL_ONE = CassConsistency.LOCAL_ONE


@dataclass
class ClusterConfig:
    """Database cluster configuration - matches Rust ClusterConfig"""
    driver: str  # "scylla" or "cassandra"
    hosts: List[str]
    port: int
    keyspace: str
    username: Optional[str] = None
    password: Optional[str] = None
    connection_timeout_secs: int = 5
    request_timeout_secs: int = 10
    pool_size: int = 4


@dataclass
class WriterConfig:
    """Writer behavior configuration - matches Rust WriterConfig"""
    mode: WriteMode
    shadow_timeout_ms: int = 5000
    retry_interval_secs: int = 60
    max_retry_attempts: int = 3
    batch_size: int = 100


@dataclass
class DualWriterConfig:
    """Full configuration - matches Rust DualWriterConfig"""
    source: ClusterConfig
    target: ClusterConfig
    writer: WriterConfig


class MigrationProxyClient:
    """
    Python client for the Rust CQL migration proxy.
    Uses native cassandra-driver - ZERO application code changes!
    """
    
    def __init__(self, contact_points: List[str] = None, port: int = 9042):
        """
        Initialize client connecting to CQL proxy
        
        Args:
            contact_points: List of proxy hostnames (default: ['localhost'])
            port: CQL proxy port (default: 9042)
        """
        if contact_points is None:
            contact_points = ['localhost']
            
        self.contact_points = contact_points
        self.port = port
        self.cluster = None
        self.session = None
        self._metrics = {
            "writes": 0,
            "reads": 0,
            "errors": 0,
            "total_latency_ms": 0
        }
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def connect(self):
        """Connect to CQL proxy"""
        logger.info(f"Connecting to CQL proxy at {self.contact_points}:{self.port}")
        
        self.cluster = Cluster(
            contact_points=self.contact_points,
            port=self.port,
            protocol_version=4
        )
        self.session = self.cluster.connect()
        logger.info("Connected to CQL migration proxy")
    
    def close(self):
        """Close connection"""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Disconnected from CQL proxy")
    
    def set_keyspace(self, keyspace: str):
        """Set active keyspace"""
        self.session.set_keyspace(keyspace)
    
    def insert(self, 
              keyspace: str,
              table: str, 
              data: Dict[str, Any],
              consistency: Optional[ConsistencyLevel] = None) -> bool:
        """
        Insert data through the proxy
        
        Use Case Call:
            client.insert(
                keyspace="iconik_production",
                table="media_assets",
                data={
                    "asset_id": uuid.uuid4(),
                    "title": "Corporate Video Q4",
                    "duration": 1800,
                    "codec": "h264"
                }
            )
        """
        start_time = time.time()
        
        # Build CQL INSERT query
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        query = f"INSERT INTO {keyspace}.{table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        try:
            statement = SimpleStatement(query)
            if consistency:
                statement.consistency_level = consistency.value
            
            self.session.execute(statement, list(data.values()))
            
            # Track metrics
            self._metrics["writes"] += 1
            latency = (time.time() - start_time) * 1000
            self._metrics["total_latency_ms"] += latency
            
            logger.debug(f"Insert successful: {table}")
            return True
            
        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(f"Insert failed: {e}")
            raise
    
    def update(self,
              keyspace: str,
              table: str,
              data: Dict[str, Any],
              where_clause: str,
              where_values: List[Any],
              consistency: Optional[ConsistencyLevel] = None) -> bool:
        """
        Update data through the proxy
        
        Use Case Call:
            client.update(
                keyspace="iconik_production",
                table="media_assets",
                data={"status": "processed", "updated_at": datetime.now()},
                where_clause="asset_id = ?",
                where_values=[asset_uuid]
            )
        """
        start_time = time.time()
        
        # Build CQL UPDATE query
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        query = f"UPDATE {keyspace}.{table} SET {set_clause} WHERE {where_clause}"
        values = list(data.values()) + where_values
        
        try:
            statement = SimpleStatement(query)
            if consistency:
                statement.consistency_level = consistency.value
            
            self.session.execute(statement, values)
            
            # Track metrics
            self._metrics["writes"] += 1
            latency = (time.time() - start_time) * 1000
            self._metrics["total_latency_ms"] += latency
            
            logger.debug(f"Update successful: {table}")
            return True
            
        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(f"Update failed: {e}")
            raise
    
    def delete(self,
              keyspace: str,
              table: str,
              where_clause: str,
              where_values: List[Any],
              consistency: Optional[ConsistencyLevel] = None) -> bool:
        """
        Delete data through the proxy
        
        Use Case Call:
            client.delete(
                keyspace="iconik_production",
                table="media_assets",
                where_clause="asset_id = ?",
                where_values=[asset_uuid]
            )
        """
        start_time = time.time()
        
        query = f"DELETE FROM {keyspace}.{table} WHERE {where_clause}"
        
        try:
            statement = SimpleStatement(query)
            if consistency:
                statement.consistency_level = consistency.value
            
            self.session.execute(statement, where_values)
            
            # Track metrics
            self._metrics["writes"] += 1
            latency = (time.time() - start_time) * 1000
            self._metrics["total_latency_ms"] += latency
            
            logger.debug(f"Delete successful: {table}")
            return True
            
        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(f"Delete failed: {e}")
            raise
    
    def select(self,
              keyspace: str,
              table: str,
              where_clause: str,
              where_values: List[Any],
              consistency: Optional[ConsistencyLevel] = None) -> List[Any]:
        """
        Select data through the proxy
        
        Use Case Call:
            rows = client.select(
                keyspace="iconik_production",
                table="media_assets",
                where_clause="asset_id = ?",
                where_values=[asset_uuid]
            )
        """
        start_time = time.time()
        
        query = f"SELECT * FROM {keyspace}.{table} WHERE {where_clause}"
        
        try:
            statement = SimpleStatement(query)
            if consistency:
                statement.consistency_level = consistency.value
            
            rows = self.session.execute(statement, where_values)
            
            # Track metrics
            self._metrics["reads"] += 1
            latency = (time.time() - start_time) * 1000
            self._metrics["total_latency_ms"] += latency
            
            return list(rows)
            
        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(f"Select failed: {e}")
            raise
    
    def get_client_metrics(self) -> Dict[str, Any]:
        """Get client-side metrics"""
        total_ops = self._metrics["writes"] + self._metrics["reads"]
        avg_latency = 0
        if total_ops > 0:
            avg_latency = self._metrics["total_latency_ms"] / total_ops
        
        return {
            "total_writes": self._metrics["writes"],
            "total_reads": self._metrics["reads"],
            "total_errors": self._metrics["errors"],
            "average_latency_ms": avg_latency,
            "error_rate": self._metrics["errors"] / max(1, total_ops)
        }


# Showing how third-party media application would integrate
def iconik_media_application_svc():
    """
    How Iconik's application would use the proxy
    ONLY CONNECTION STRING CHANGES - everything else stays the same!
    """
    
    # OLD (before migration):
    # client = MigrationProxyClient(['cassandra-1', 'cassandra-2', 'cassandra-3'], 9042)
    
    # NEW (during migration):
    # Just point to the CQL proxy instead of Cassandra directly!
    with MigrationProxyClient(['dual-writer-proxy'], 9042) as client:
        
        client.set_keyspace("iconik_production")
        
        # Normal application operations - NO CODE CHANGES!
        
        # Insert a new media asset
        asset_id = uuid.uuid4()
        client.insert(
            keyspace="iconik_production",
            table="media_assets",
            data={
                "asset_id": asset_id,
                "title": "Q4 2024 Earnings Call",
                "file_path": "s3://media-bucket/earnings/q4-2024.mp4",
                "duration": 3600,
                "codec": "h264",
                "bitrate": 5000000,
                "resolution": "1920x1080",
                "created_at": datetime.now()
            }
        )
        logger.info(f"Inserted media asset: {asset_id}")
        
        # Update asset status after processing
        client.update(
            keyspace="iconik_production",
            table="media_assets",
            data={
                "status": "transcoded",
                "thumbnail_path": "s3://media-bucket/thumbnails/q4-2024.jpg",
                "updated_at": datetime.now()
            },
            where_clause="asset_id = ?",
            where_values=[asset_id]
        )
        logger.info(f"Updated media asset: {asset_id}")
        
        # Query the asset
        rows = client.select(
            keyspace="iconik_production",
            table="media_assets",
            where_clause="asset_id = ?",
            where_values=[asset_id]
        )
        for row in rows:
            logger.info(f"Found asset: {row.title} - {row.status}")
        
        # Bulk insert for batch operations
        for i in range(100):
            client.insert(
                keyspace="iconik_production",
                table="media_chunks",
                data={
                    "chunk_id": uuid.uuid4(),
                    "asset_id": asset_id,
                    "sequence": i,
                    "size": 1048576  # 1MB chunks
                }
            )
        logger.info("Inserted 100 media chunks")
        
        # Get metrics to monitor performance
        metrics = client.get_client_metrics()
        logger.info(f"Application metrics: {metrics}")


if __name__ == "__main__":
    # Run the application
    iconik_media_application_svc()
```



## The SSTableLoader Bulk Transfer Processor

The deployment location of this service is on the target cloud side. If the DB clusters involved are Cassandra or ScyllaDB on GCP (GKE, GCP Compute Instance VM) on source side and the target Cassandra DB or ScyllaDB Cluster is on AWS (EKS or EC2, Firecracker), the `sstable-loader` service will require is location on the AWS target side. 

The SSTableLoader requires its location on the **target** side. 


### The Advantages of Target Side Locationing

- **Network Locality** Loads data directly into AWS Cassandra or SycllaDB cluster
- **Network Throughput** No Cross-Cloud data transfer during bulk load
- **Cost** Avoids GCP egress charges for bulk data
- **Performance** Local disk I/O on AWS EC2


## The Dual-Reader Service

The deployment location of this service is **NOT** strictly enforced on source or target side. 
It is advised that this service location execute on the **target** side of the transfer to avoid costly egress costs from the source cloud side (if GCP to AWS), the `dual-reader` service is at its advantage if it is deployed on the AWS side (EKS, EC2, Firecracker).


### The Advantages of Target Side Locationing

- **Network Symmetry** If on AWS, 1 read is local, 1 is remote (vs 2 remote if on GCP)
- **Cost** Avoid GCP egress for validation reads
- **After Cutover** Dual Reader validator stays on AWS to focus AWS target on Cassandra DB



The following architectural workflow graphic shows the entire fleet of the `scylla/cassandra-cluster-sync` application of the dual-writer proxy service, sstable-loader service and dual-writer service (dual-cluster checksumming validation). This version of the entire architectural workflow shows a **Kubernetes-Native** deployment. The graphic shows CasssandraDB instance, however this 100% adapts to ScyllaDB cluster without disruptive change.

![entire-scylla-cluster-sync-workflow](docs/Full-Service-Architecture.png)


The following architectural workflow graphic shows the entire fleet of the `scylla/cassandra-cluster-sync` application of the dual-writer proxy service, sstable-loader service and dual-writer service. This version of the entire architectural workflow shows a **Cloud-Native** VM deployment (services a Docker containers on GCP Compute Instance VMs and AWS EC2 VMs). The graphic shows CasssandraDB instance, however this 100% adapts to ScyllaDB cluster without disruptive change.

![entire-scylla-cluster-sync-workflow-vm-native](docs/Full-Service-Architecture-VM-Native.png)




## Index Management (SSTableLoader Process Server)

### With Secondary Indexes (recommended for 60+ indexes):
```bash
./sstable-loader --config config/sstable-loader.yaml \
                 --indexes config/indexes.yaml \
                 --auto-start
```

### Without Secondary Indexes:
```bash
./sstable-loader --config config/sstable-loader.yaml \
                 --skip-indexes \
                 --auto-start
```

### Empty indexes.yaml (also valid):
```yaml
# indexes.yaml - no indexes to manage
[]
```


## CassandraDB vs ScyllaDB Kubernetes Operator Cost Reduction

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



# Changes with ScyllaDB Operator on EKS

```shell
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
├── Cargo.lock
├── Cargo.toml
├── LICENSE
├── Makefile
├── README.md
├── config
│   ├── deploy
│   │   └── scylla-cluster-sync
│   │       ├── Chart.yaml
│   │       ├── templates
│   │       │   ├── __helpers.tpl
│   │       │   ├── app-config.yaml
│   │       │   ├── app-gateway.yaml
│   │       │   ├── app-monitoring.yaml
│   │       │   ├── app-scaling.yaml
│   │       │   ├── app-secrets.yaml
│   │       │   └── app.yaml
│   │       ├── values-production.yaml
│   │       └── values.yaml
│   ├── dual-reader.yaml
│   ├── dual-writer-cassandra.yaml
│   ├── dual-writer-scylla.yaml
│   ├── filter-rules,yaml
│   ├── indexes.yaml
│   └── sstable-loader.yaml
├── docker-compose.yaml
├── docs
├── monitoring
│   ├── grafana
│   │   └── provisioning
│   │       └── datasources
│   │           └── prometheus.yaml
│   └── prometheus.yaml
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
│   │       ├── config.rs
│   │       ├── cql_server.rs
│   │       ├── filter.rs
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
│           ├── index_manager.rs
│           ├── loader.rs
│           ├── main.rs
│           └── token_range.rs
└── svckit
    ├── Cargo.toml
    └── src
        ├── config.rs
        ├── database
        │   ├── cassandra.rs
        │   ├── factory.rs
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
```
# =============================================================================
# Building Scylla Cluster Sync Services
# =============================================================================

# --- Native Rust Builds ---

# Build all services (debug mode - faster compilation)
make build

# Build all services (release mode - optimized binaries)
make build-release

# Build individual services (release mode)
make dual-writer
make dual-reader
make sstable-loader

# Alternative: using cargo directly
cargo build --workspace                    # debug
cargo build --workspace --release          # release
cargo build --release --bin dual-writer    # single service

# --- Docker Builds ---

# Build all Docker images
make docker-build

# Build individual Docker images
make docker-build-dual-writer
make docker-build-dual-reader
make docker-build-sstable-loader

# Alternative: using docker directly
docker build -f services/dual-writer/Dockerfile.dual-writer -t dual-writer:latest .
docker build -f services/dual-reader/Dockerfile.dual-reader -t dual-reader:latest .
docker build -f services/sstable-loader/Dockerfile.sstable-loader -t sstable-loader:latest .

# --- Docker Compose ---

# Start all services (ScyllaDB + migration services + monitoring)
make docker-up

# Start with logs attached
make docker-up-logs

# View logs
make docker-logs

# Stop all services
make docker-down

# Stop and remove volumes (clean slate)
make docker-clean

# --- Development Workflow ---

# Start only ScyllaDB instances for local development
make dev-db

# Run services locally (after make build)
make run-dual-writer      # localhost:8080
make run-dual-reader      # localhost:8082
make run-sstable-loader   # localhost:8081

# --- Code Quality ---

make fmt          # Format code
make clippy       # Run linter
make test         # Run tests
make check        # Quick syntax check
make fix          # Auto-fix warnings

# --- Service Artifacts Location ---

# Debug builds:   ./target/debug/{dual-writer,dual-reader,sstable-loader}
# Release builds: ./target/release/{dual-writer,dual-reader,sstable-loader}
```


## Kubernetes Deployment Architecture 

### Unified Kubernetes Helm Chart Structure

```shell
config/deploy/scylla-cluster-sync/
├── Chart.yaml
├── values.yaml
├── values-production.yaml
└── templates/
    ├── _helpers.tpl
    ├── app.yaml
    ├── app-config.yaml
    ├── app-secrets.yaml
    ├── app-scaling.yaml
    ├── app-gateway.yaml
    └── app-monitoring.yaml
```

### Deploying the Services using Unified Kubernetes Helm Chart

```shell
# Install all 3 services (default)
helm install migration ./config/deploy/scylla-cluster-sync \
  --namespace migration --create-namespace

# Install only dual-writer and dual-reader (skip sstable-loader)
helm install migration ./config/deploy/scylla-cluster-sync \
  --namespace migration --create-namespace \
  --set sstableLoader.enabled=false

# Install only dual-writer
helm install dual-writer ./config/deploy/scylla-cluster-sync \
  --namespace migration --create-namespace \
  --set dualReader.enabled=false \
  --set sstableLoader.enabled=false

# Production deployment
helm install migration ./config/deploy/scylla-cluster-sync \
  --namespace migration --create-namespace \
  -f ./config/deploy/scylla-cluster-sync/values-production.yaml

# Dry-run
helm template migration ./config/deploy/scylla-cluster-sync \
  --debug
```


## Scylla/Cassandra Cluster Sync Architectural Layout 

```
┌─────────────────────────────────────────────────────────────┐
│  GCP (Source)                                               │
├─────────────────────────────────────────────────────────────┤
│  • 3x GCP Compute VMs (Cassandra 4.x, self-managed)         │
│    - NO Kubernetes                                          │
│    - Manual install (no IaC)                                │
│    - RF=2                                                   │
│  • dual-writer service (GKE, 3 replicas)                    │
│    - Port 9042 (CQL proxy)                                  │
│    - Python app connects here                               │
│  • Local disk: /var/lib/cassandra/data                      │
│  • DataSync agent (maybe - client decides Monday)           │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ AWS DataSync OR aws s3 sync
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  AWS (Target)                                               │
├─────────────────────────────────────────────────────────────┤
│  • S3 bucket: iconik-migration-sstables                     │
│  • 3x EC2 instances (Cassandra 4.x, self-managed)           │
│    - NO Kubernetes                                          │
│    - Manual install (no IaC)                                │
│    - RF=3                                                   │
│  • sstable-loader service (EKS, 3 replicas)                 │
│    - Reads from S3                                          │
│    - Imports to AWS Cassandra                               │
│  • dual-reader service (EKS, 3 replicas)                    │
│    - Validates consistency                                  │
└─────────────────────────────────────────────────────────────┘
```



## Scylla/Cassandra Cluster Sync Sequence Diagram (No AWS DataSync Services)

The following sequence diagram shows the worklow of the three `scylla-cluster-sync-rs` services during the ScyllaDB/CassandraDB source to cross-cloud  ScyllaDB/CassandraDB target. 

```
┌─────────┐     ┌────────────┐     ┌───────────┐     ┌─────────────┐     ┌──────────┐
│   App   │     │dual-writer │     │ Cassandra │     │sstable-load │     │ ScyllaDB │
└────┬────┘     └─────┬──────┘     └─────┬─────┘     └──────┬──────┘     └────┬─────┘
     │                │                  │                  │                 │
     │ ═══════════════════════════ Phase 1: SourceOnly ══════════════════════════
     │                │                  │                  │                 │
     │──Write Req────▶│                  │                  │                 │
     │                │───Execute CQL───▶│                  │                 │
     │                │◀──────OK─────────│                  │                 │
     │◀───Response────│                  │                  │                 │
     │                │                  │                  │                 │
     │ ═══════════════════════════ Phase 2: DualAsync ═══════════════════════════
     │                │                  │                  │                 │
     │──Write Req────▶│                  │                  │                 │
     │                │───Execute CQL───▶│                  │                 │
     │                │◀──────OK─────────│                  │                 │
     │◀───Response────│                  │                  │                 │
     │                │                  │                  │                 │
     │                │════════════Shadow Write (async)════════════════════▶. │
     │                │                  │                  │                 │
     │                │                  │                  │                 │
     │                │                  │◀═══Bulk Read════│                  │
     │                │                  │═══════════════▶│                   │
     │                │                  │                  │───Insert Batch─▶│
     │                │                  │                  │◀──────OK────────│
     │                │                  │                  │                 │
     │ ═══════════════════════════ Phase 3: DualSync ════════════════════════════
     │                │                  │                  │                 │
     │──Write Req────▶│                  │                  │                 │
     │                │───Execute CQL───▶│                  │                 │
     │                │══════════════Execute CQL══════════════════════════▶   │
     │                │◀──────OK─────────│                  │                 │
     │                │◀════════════════════════OK═════════════════════════   │
     │◀───Response────│                  │                  │                 │
     │                │                  │                  │                 │
     │ ═══════════════════════════ Phase 4: TargetOnly ══════════════════════════
     │                │                  │                  │                 │
     │──Write Req────▶│                  │                  │                 │
     │                │══════════════Execute CQL══════════════════════════▶   │
     │                │◀════════════════════════OK═════════════════════════   │
     │◀───Response────│                  │                  │                 │
     │                │                  │                  │                 │
┌────┴────┐     ┌─────┴──────┐     ┌─────┴─────┐     ┌──────┴──────┐     ┌────┴─────┐
│   App   │     │dual-writer │     │ Cassandra │     │sstable-load │     │ ScyllaDB │
└─────────┘     └────────────┘     └───────────┘     └─────────────┘     └──────────┘
```

## Deployment Order (Chronological)

### Phase 0: Pre-Migration Setup

**Duration:** 1-2 days  
**Risk Level:** Low
```bash
# 1. Create namespaces
kubectl --context=gke-prod create namespace migration
kubectl --context=eks-prod create namespace migration

# 2. Deploy External Secrets Operator (if not present)
helm repo add external-secrets https://charts.external-secrets.io
helm install external-secrets external-secrets/external-secrets \
  --namespace external-secrets --create-namespace

# 3. Deploy ESO ClusterSecretStore CRD and Configuration with AWS SecretsManager
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: aws-secrets-manager
spec:
  provider:
    aws:
      service: SecretsManager
      region: us-east-1
      auth:
        jwt:
          serviceAccountRef:
            name: external-secrets
            namespace: external-secrets
```

```shell 
kubectl apply -f aws-secrets-store-cassandra.yaml 
```

```shell
# 4. Seed database credentials in AWS Secrets Manager
aws secretsmanager create-secret \
  --name production/iconik/cassandra-credentials \
  --secret-string '{"username":"cassandra","password":"xxx"}'

aws secretsmanager create-secret \
  --name production/iconik/scylla-credentials \
  --secret-string '{"username":"scylla","password":"xxx"}'
```

---

### Phase 1: Deploy sstable-loader (AWS EKS)

**Duration:** 30 minutes  
**Risk Level:** Low (no production impact yet)

Deploy the bulk migration service on the **target side** first. It will sit idle until triggered.
```shell
# Switch to EKS context
kubectl config use-context eks-prod

# Deploy sstable-loader only
helm install sstable-loader ./config/deploy/scylla-cluster-sync \
  --namespace migration \
  --set dualWriter.enabled=false \
  --set dualReader.enabled=false \
  --set sstableLoader.enabled=true \
  -f values-production.yaml

# Verify deployment
kubectl -n migration get pods -l app.kubernetes.io/component=sstable-loader
kubectl -n migration logs -l app.kubernetes.io/component=sstable-loader -f

# Test health endpoint
kubectl -n migration port-forward svc/sstable-loader 8081:8081 &
curl http://localhost:8081/health
```

**Expected output:**
```json
{
  "status": "healthy",
  "service": "sstable-loader",
  "index_manager_enabled": true,
  "index_count": 60
}
```

---



## References

Cassandra DB Kubernetes Operator 
- https://docs.k8ssandra.io/components/cass-operator/

ScyllaDB Kubernetes Operator 
- https://www.scylladb.com/product/scylladb-operator-kubernetes/