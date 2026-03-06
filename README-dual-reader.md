# Scylla Cluster Sync - State of Affairs

## Project Context

**Project:** scylla-cluster-sync-rs
**Client:** Iconik (Backlight acquisition)
**Migration:** 84TB+ Cassandra (GCP) to ScyllaDB/Cassandra (AWS)
**Consultant:** Presidio engagement

## Repository Structure

```
scylla-cluster-sync-rs/
├── Cargo.toml                    # Workspace root
├── Makefile                      # Build targets including tui-demo, tui-dash, tui-live
├── svckit/                       # Shared library
│   └── src/
│       ├── config.rs             # DatabaseConfig with speculative_execution support
│       ├── database/scylla.rs    # ScyllaConnection with ExecutionProfile
│       └── errors.rs             # SyncError types
├── services/
│   ├── dual-writer/              # Production - CQL proxy for dual writes
│   ├── dual-reader/              # NOT DEPLOYED - Consistency validation
│   ├── sstable-loader/           # Production - Bulk migration
│   ├── doctore-dash/             # Web dashboard (separate)
│   └── tui-dash/                 # Terminal dashboard (NEW)
└── deploy/
    └── scylla-cluster-sync/      # Helm chart with Gateway API support
```

## Service Status

| Service | Status | Notes |
|---------|--------|-------|
| **dual-writer** | PRODUCTION | 100% working, pre-optimized |
| **dual-reader** | NOT DEPLOYED | Needs FilterGovernor + client filtering spec |
| **sstable-loader** | PRODUCTION | 100% working, 120k rows/sec (client tuned) |
| **tui-dash** | READY | Demo + live mode, connects via Gateway API |

## Recent Implementations (This Session)

### 1. Single-Table Migration API
- Endpoint: `POST /migrate/:keyspace/:table`
- Auto-discovers partition keys from schema
- Returns MigrationStats on completion

### 2. Table Discovery API
- Endpoint: `GET /discover/:keyspace`
- Returns JSON list of tables in keyspace

### 3. Row-Level Retry + JSONL Logging
- Wraps INSERT in retry loop using `max_retries` and `retry_delay_secs`
- Logs failed rows to `failed_rows.jsonl` after retries exhausted
- Fixed 2.8% failure rate issue (was 183-second GC pause)

### 4. Speculative Execution
- Added to svckit DatabaseConfig
- Uses scylla-rust-driver ExecutionProfile API
- Config: `speculative_execution: true`, `speculative_delay_ms: 100`
- Masks Cassandra GC pauses by sending to backup node

### 5. TUI Dashboard (tui-dash)
- New service: `services/tui-dash/`
- Demo mode: `make tui-demo`
- Live mode: `make tui-dash` (polls /status endpoint)
- Custom URL: `make tui-live API_URL=https://...`
- Color scheme: Red/white/silver/gold on dark background

### 6. Helm Chart Updates
- Gateway API HTTPRoute for sstable-loader endpoints
- TLS certificate template (cert-manager integration)
- Correct port routing (9092 for API)

## Configuration Files

### sstable-loader.yaml (key fields)
```yaml
loader:
  batch_size: 1000
  max_concurrent_loaders: 4
  num_ranges_per_core: 16
  skip_on_error: true
  max_retries: 10
  retry_delay_secs: 5

target:
  speculative_execution: true
  speculative_delay_ms: 100
```

### filter_rules.yaml (existing)
```yaml
filter:
  mode: blacklist  # or whitelist
  tenant_id_columns:
    - system_domain_id
    - tenant_id
  blacklisted_tenants:
    - "test-tenant-1"
  blacklisted_tables:
    - "keyspace.table_to_skip"
```

## Client Performance Journey

| Stage | Throughput | Change |
|-------|------------|--------|
| Initial | 15k rows/sec | Baseline |
| Token tuning | 41.5k rows/sec | +2.75x |
| Concurrency tuning | 120k rows/sec | +8x from baseline |

**Root cause of 2.8% failures:** 183-second JVM GC pause on Cassandra node.

## Key Technical Decisions

1. **SELECT JSON / INSERT JSON** - Bypasses driver UDT deserialization issues
2. **Token-range parallelism** - Distributes load across cluster
3. **Idempotent inserts** - Safe retry (upsert semantics)
4. **Speculative execution** - Masks coordinator GC pauses
5. **FilterGovernor** - Tenant/table filtering (proven in sstable-loader)

## Files Modified This Session

| File | Changes |
|------|---------|
| `services/sstable-loader/src/loader.rs` | Row retry, JSONL logging, single-table API |
| `services/sstable-loader/src/api.rs` | /migrate/:ks/:table, /discover/:keyspace endpoints |
| `svckit/src/config.rs` | speculative_execution, speculative_delay_ms |
| `svckit/src/database/scylla.rs` | ExecutionProfile with speculative policy |
| `services/tui-dash/*` | NEW service (main.rs, api.rs, mock.rs, state.rs) |
| `deploy/scylla-cluster-sync/templates/app-gateway.yaml` | HTTPRoute for sstable-loader |
| `deploy/scylla-cluster-sync/templates/app-gateway-tls.yaml` | NEW cert-manager template |
| `deploy/scylla-cluster-sync/values.yaml` | Gateway TLS config, apiPort |
| `Makefile` | tui-demo, tui-dash, tui-live targets |

---

# OPTIMIZATION TODO LIST

## Priority 1: Prepared Statements (LOW RISK, HIGH IMPACT)

**Current:** Each INSERT parsed fresh.
```rust
target.get_session()
    .query_unpaged(insert_query.as_str(), (&json_row,))
    .await
```

**Optimized:**
```rust
let prepared = target.get_session().prepare(&insert_query).await?;
target.get_session().execute_unpaged(&prepared, (&json_row,)).await
```

**Files:** `services/sstable-loader/src/loader.rs` (process_range)
**Impact:** 10-30% improvement

---

## Priority 2: Batched Inserts (MEDIUM RISK, HIGH IMPACT)

**Use UNLOGGED BATCH only** (LOGGED uses Paxos, slower).

```rust
use scylla::batch::{Batch, BatchType};

let mut batch = Batch::new(BatchType::Unlogged);
for _ in 0..row_buffer.len() {
    batch.append_statement(prepared.clone());
}
target.get_session().batch(&batch, row_buffer).await?;
```

**Files:** `services/sstable-loader/src/loader.rs`, config.rs
**Impact:** 2-5x improvement
**Config:** Add `insert_batch_size: 100`

---

## Priority 3: Concurrent Inserts Within Range (MEDIUM RISK)

```rust
use futures::stream::{self, StreamExt};

stream::iter(rows)
    .map(|row| async { target.execute_unpaged(&prepared, (&row,)).await })
    .buffer_unordered(insert_concurrency)
    .collect::<Vec<_>>()
    .await;
```

**Files:** `services/sstable-loader/src/loader.rs`
**Impact:** 2-4x improvement
**Config:** Add `insert_concurrency: 32`

---

## Priority 4: Paged Reads (MEDIUM RISK)

**Current:** Loads entire token range into memory.

**Optimized:**
```rust
let mut stream = source.get_session()
    .query_iter(query, &[])
    .await?
    .rows_stream::<(String,)>()?;

while let Some(row) = stream.next().await { ... }
```

**Files:** `services/sstable-loader/src/loader.rs`
**Impact:** Memory efficiency, enables larger ranges

---

## Priority 5: Compression (LOW RISK, LOW IMPACT)

```rust
.compression(Some(Compression::Lz4))
```

**Files:** `svckit/src/database/scylla.rs`
**Impact:** 10-20% network reduction

---

## Priority 6: Async Read-Write Pipeline (HIGH RISK)

Decouple read and write with tokio channels. Major refactor.

---

## Feature Flag Strategy

```yaml
loader:
  optimizations:
    use_prepared_statements: true
    use_batched_inserts: false
    insert_batch_size: 100
    use_concurrent_inserts: false
    insert_concurrency: 32
    use_paged_reads: false
    use_compression: false
```

Each optimization independently toggleable. Zero regression guarantee.

---

## Estimated Throughput Ceiling

| Config | Estimate |
|--------|----------|
| Current | 120k rows/sec |
| + Prepared | 140-160k |
| + Batched | 250-400k |
| + Concurrent | 400-600k |
| + All | 500k-1M rows/sec |

Note: ScyllaDB target only. Cassandra limited by JVM regardless.

---

# DUAL-READER FILTERING LOGIC (CLIENT SPEC)

## Requirements from Iconik

### Context
Some customers already reside in AWS pre-migration. The dual-reader needs selective comparison logic.

### Requirement 1: system_domain_id Filtering

**Behavior:**
- Provide a list of `system_domain_id` values that SHOULD be compared between source and target
- For requests containing a `system_domain_id` IN this list: read from BOTH source and target, compare results
- For requests containing a `system_domain_id` NOT in this list: read from SOURCE ONLY, return data without comparison

**Config Example:**
```yaml
dual_reader:
  filter:
    # List of system_domain_ids to compare (already migrated to AWS)
    compare_system_domain_ids:
      - "domain-abc-123"
      - "domain-def-456"
      - "domain-ghi-789"
    
    # Column name to extract system_domain_id from queries
    system_domain_id_column: "system_domain_id"
```

**Logic:**
```
IF query contains system_domain_id IN compare_list:
    result_source = query(source)
    result_target = query(target)
    compare(result_source, result_target)
    log_discrepancy_if_any()
    return result_source  # or result_target based on config
ELSE:
    result_source = query(source)
    return result_source  # No comparison, source only
```

### Requirement 2: Table Comparison Whitelist

**Behavior:**
- Provide a list of `keyspace.table_name` that SHOULD be compared
- Tables NOT in this list: read from SOURCE ONLY, no comparison
- Some tables contain source-system-specific data that won't be migrated

**Config Example:**
```yaml
dual_reader:
  filter:
    # Tables to compare (will be migrated)
    compare_tables:
      - "acls_keyspace.group_acls"
      - "acls_keyspace.user_acls"
      - "assets_keyspace.assets"
      - "assets_keyspace.asset_versions"
      - "collections_keyspace.collections"
    
    # Tables NOT in this list are read from source only
    # Example excluded: "audit_keyspace.audit_logs" (source-specific)
```

**Logic:**
```
IF table IN compare_tables:
    proceed_with_comparison()  # Subject to system_domain_id filter above
ELSE:
    result_source = query(source)
    return result_source  # No comparison, source only
```

### Combined Logic Flow

```
1. Parse incoming query to extract:
   - keyspace.table_name
   - system_domain_id (if present in query/parameters)

2. Check table whitelist:
   IF table NOT IN compare_tables:
       -> Read SOURCE only, return result
   
3. Check system_domain_id:
   IF system_domain_id NOT IN compare_system_domain_ids:
       -> Read SOURCE only, return result
   
4. Both conditions met - perform dual read:
   result_source = query(source)
   result_target = query(target)
   compare_and_log(result_source, result_target)
   return result_source
```

### Implementation Notes

1. **Reuse FilterGovernor pattern** from sstable-loader
2. **Config structure** should mirror filter_rules.yaml pattern
3. **Query parsing** - extract table and system_domain_id from CQL
4. **Metrics** - track source-only reads vs dual reads vs discrepancies
5. **Logging** - log all discrepancies to file (similar to failed_rows.jsonl)

### Files to Modify

| File | Changes |
|------|---------|
| `services/dual-reader/src/config.rs` | Add DualReaderFilterConfig |
| `services/dual-reader/src/filter.rs` | NEW - FilterGovernor for dual-reader |
| `services/dual-reader/src/reader.rs` | Integrate filter logic before read |
| `services/dual-reader/src/main.rs` | Load filter config |

### Config Schema

```yaml
dual_reader:
  # Existing config
  port: 8082
  source:
    hosts: [...]
  target:
    hosts: [...]
  
  # NEW: Filtering config
  filter:
    enabled: true
    
    # Tables to compare (whitelist)
    compare_tables:
      - "acls_keyspace.group_acls"
      - "assets_keyspace.assets"
    
    # system_domain_ids to compare (whitelist)
    compare_system_domain_ids:
      - "domain-abc-123"
      - "domain-def-456"
    
    # Column name for system_domain_id extraction
    system_domain_id_column: "system_domain_id"
    
    # Discrepancy logging
    log_discrepancies: true
    discrepancy_log_path: "/var/log/dual-reader/discrepancies.jsonl"
```

### Discrepancy Log Format

```json
{
  "timestamp": "2026-02-07T12:34:56Z",
  "table": "acls_keyspace.group_acls",
  "system_domain_id": "domain-abc-123",
  "query": "SELECT * FROM acls_keyspace.group_acls WHERE ...",
  "source_row_count": 150,
  "target_row_count": 148,
  "discrepancy_type": "row_count_mismatch",
  "details": "Target missing 2 rows"
}
```

---

# SUMMARY FOR NEXT AGENT

## What's Working (DO NOT REGRESS)
- dual-writer: Production, 100% functional
- sstable-loader: Production, 120k rows/sec, retry logic, speculative execution
- tui-dash: Ready, demo + live mode
- Helm chart: Gateway API routing, TLS support

## What Needs Work
1. **Optimizations** (sstable-loader): Prepared statements, batching, concurrency
2. **dual-reader deployment**: FilterGovernor integration + client filtering spec above

## Key Principles
- Zero regression to working production code
- Feature flags for all optimizations
- Incremental, testable changes
- Rolex-quality engineering standards

## Client Contact
- Iconik team on Slack
- Currently achieving 120k rows/sec
- Fighting Cassandra JVM GC pauses (183 seconds observed)
- Pre-optimized Rust code - ceiling much higher with optimizations