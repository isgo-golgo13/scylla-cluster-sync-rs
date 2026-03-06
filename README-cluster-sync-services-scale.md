# Scylla Cluster Sync Services - Optimization Guide

## Overview

This document describes optimizations for the scylla-cluster-sync-rs services that have been identified but NOT yet implemented. The current production code is stable and working (dual-writer, sstable-loader). These optimizations represent Phase 2 enhancements to maximize throughput without regressing working functionality.

## Current State

| Service | Status | Throughput (Current) |
|---------|--------|---------------------|
| dual-writer | Production | Baseline |
| dual-reader | Not deployed | N/A |
| sstable-loader | Production | 120k rows/sec (after client tuning) |

## Optimization Opportunities

### 1. Prepared Statements (HIGH IMPACT)

**Current:** Each INSERT is parsed fresh on every execution.
```rust
// Current - query_unpaged parses SQL each time
target.get_session()
    .query_unpaged(insert_query.as_str(), (&json_row,))
    .await
```

**Optimized:** Prepare once, execute many.
```rust
// Prepare at start of range processing
let prepared = target.get_session()
    .prepare(&insert_query)
    .await?;

// Execute with prepared statement
target.get_session()
    .execute_unpaged(&prepared, (&json_row,))
    .await
```

**Impact:** 10-30% throughput improvement. Eliminates query parsing overhead.

**Files Affected:**
- `services/sstable-loader/src/loader.rs` (process_range function)

**Regression Risk:** Low. Purely additive change to existing insert path.

---

### 2. Batched Inserts (HIGH IMPACT)

**Current:** Single-row inserts with individual round-trips.
```rust
for row in rows {
    // One network round-trip per row
    target.get_session()
        .query_unpaged(insert_query.as_str(), (&json_row,))
        .await
}
```

**Optimized:** UNLOGGED BATCH for multiple rows (same partition).
```rust
use scylla::batch::Batch;

let mut batch = Batch::new(BatchType::Unlogged);
let prepared = target.get_session().prepare(&insert_query).await?;

let mut row_buffer = Vec::with_capacity(batch_size);
for row in rows {
    row_buffer.push(json_row);
    
    if row_buffer.len() >= batch_size {
        let mut batch = Batch::new(BatchType::Unlogged);
        for _ in 0..row_buffer.len() {
            batch.append_statement(prepared.clone());
        }
        target.get_session()
            .batch(&batch, row_buffer.drain(..).collect::<Vec<_>>())
            .await?;
    }
}
```

**CRITICAL:** Use UNLOGGED batch only. LOGGED batches use Paxos and are slower.

**Impact:** 2-5x throughput improvement. Amortizes network round-trip cost.

**Files Affected:**
- `services/sstable-loader/src/loader.rs` (process_range function)
- `services/sstable-loader/src/config.rs` (add insert_batch_size config)

**Regression Risk:** Medium. Changes insert path. Requires careful testing.

**Considerations:**
- Batch size tuning (50-200 rows typically optimal)
- Error handling per-batch vs per-row
- Retry logic adaptation for batch failures

---

### 3. Paged Reads for Large Token Ranges (MEDIUM IMPACT)

**Current:** Unpaged SELECT loads entire token range into memory.
```rust
let result = source.get_session()
    .query_unpaged(query.as_str(), &[])
    .await?;

// All rows loaded into memory before processing
if let Some(rows) = result.rows {
    for row in rows { ... }
}
```

**Optimized:** Streaming with paged queries.
```rust
use futures::StreamExt;

let mut rows_stream = source.get_session()
    .query_iter(query.as_str(), &[])
    .await?
    .rows_stream::<(String,)>()?;

while let Some(row_result) = rows_stream.next().await {
    let (json_row,) = row_result?;
    // Process row immediately, constant memory
}
```

**Impact:** 
- Memory: O(page_size) instead of O(rows_in_range)
- Throughput: Enables processing larger ranges without OOM
- Latency: First row available sooner (streaming)

**Files Affected:**
- `services/sstable-loader/src/loader.rs` (process_range function)
- `Cargo.toml` (ensure futures crate imported)

**Regression Risk:** Medium. Changes read path. Requires testing with large ranges.

---

### 4. Concurrent Inserts Within Range (MEDIUM IMPACT)

**Current:** Sequential inserts within each token range.
```rust
for row in rows {
    // Wait for each insert before starting next
    target.get_session().query_unpaged(...).await;
}
```

**Optimized:** Parallel insert pipeline with bounded concurrency.
```rust
use futures::stream::{self, StreamExt};

let insert_concurrency = 32; // Configurable

stream::iter(rows)
    .map(|row| async {
        target.get_session()
            .execute_unpaged(&prepared, (&row,))
            .await
    })
    .buffer_unordered(insert_concurrency)
    .for_each(|result| async {
        match result {
            Ok(_) => stats.migrated_rows.fetch_add(1, Ordering::Relaxed),
            Err(e) => stats.failed_rows.fetch_add(1, Ordering::Relaxed),
        }
    })
    .await;
```

**Impact:** 2-4x throughput improvement. Saturates network and target cluster.

**Files Affected:**
- `services/sstable-loader/src/loader.rs` (process_range function)
- `services/sstable-loader/src/config.rs` (add insert_concurrency config)

**Regression Risk:** Medium. Changes insert path. Requires backpressure testing.

---

### 5. Compression (LOW IMPACT)

**Current:** No compression enabled.

**Optimized:** Enable LZ4 compression in driver.
```rust
let session_builder = SessionBuilder::new()
    .known_nodes(&contact_points)
    .compression(Some(Compression::Lz4))  // Add this
    ...
```

**Impact:** 10-20% reduction in network I/O. More impactful for large rows.

**Files Affected:**
- `svckit/src/database/scylla.rs` (ScyllaConnection::new)
- `svckit/src/config.rs` (add compression option)

**Regression Risk:** Low. Driver handles transparently.

---

### 6. Shard-Aware Connection Pooling (SCYLLA TARGET ONLY)

**Current:** Using PerShard pool size.
```rust
.pool_size(PoolSize::PerShard(pool_size))
```

**Optimized:** Driver already shard-aware for ScyllaDB. Verify configuration.

**Verification:**
- Ensure target is ScyllaDB (not Cassandra)
- Check logs for "shard-aware port" discovery
- Confirm connections distributed across shards

**Impact:** Already enabled. Verify working correctly.

**Regression Risk:** None. Configuration verification only.

---

### 7. Async Read-Write Pipeline (HIGH COMPLEXITY)

**Current:** Read all rows, then write all rows.
```rust
// Phase 1: Read
let result = source.query_unpaged(...).await;

// Phase 2: Write (after read completes)
for row in result.rows {
    target.query_unpaged(...).await;
}
```

**Optimized:** Decouple with tokio channels.
```rust
let (tx, rx) = tokio::sync::mpsc::channel(1000);

// Reader task
let reader = tokio::spawn(async move {
    let mut stream = source.query_iter(...).await?.rows_stream()?;
    while let Some(row) = stream.next().await {
        tx.send(row?).await?;
    }
});

// Writer task(s)
let writer = tokio::spawn(async move {
    while let Some(row) = rx.recv().await {
        target.execute_unpaged(&prepared, (&row,)).await?;
    }
});

tokio::try_join!(reader, writer)?;
```

**Impact:** Overlaps read and write latency. Better utilization of both clusters.

**Files Affected:**
- `services/sstable-loader/src/loader.rs` (major refactor of process_range)

**Regression Risk:** High. Architectural change. Requires extensive testing.

---

### 8. dual-reader FilterGovernor Integration

**Current:** dual-reader exists but not deployed. Needs filtering.

**Required:**
- Integrate FilterGovernor component (already proven in sstable-loader)
- Add filter rules config support
- Implement tenant/table filtering for validation queries

**Files Affected:**
- `services/dual-reader/src/reader.rs`
- `services/dual-reader/src/config.rs`

**Regression Risk:** None (new deployment). But must match sstable-loader filtering logic.

---

## Implementation Priority

| Optimization | Impact | Risk | Effort | Priority |
|--------------|--------|------|--------|----------|
| Prepared Statements | High | Low | Low | 1 |
| Batched Inserts | High | Medium | Medium | 2 |
| Concurrent Inserts | Medium | Medium | Medium | 3 |
| Paged Reads | Medium | Medium | Medium | 4 |
| Compression | Low | Low | Low | 5 |
| Async Pipeline | High | High | High | 6 |
| dual-reader Filters | N/A | Low | Medium | Separate |

## Implementation Strategy

### Phase 2A: Low-Risk, High-Impact
1. Prepared statements
2. Compression
3. Verify shard-aware routing

### Phase 2B: Medium-Risk, High-Impact
1. Batched inserts (with configurable batch size)
2. Concurrent inserts (with configurable concurrency)

### Phase 2C: Higher-Risk Refactors
1. Paged reads
2. Async read-write pipeline

### Parallel Track: dual-reader
1. FilterGovernor integration
2. Config parity with sstable-loader

## Testing Requirements

Each optimization requires:

1. **Unit tests** - Isolated function behavior
2. **Integration tests** - Against test Cassandra/ScyllaDB clusters
3. **Performance benchmarks** - Before/after throughput comparison
4. **Regression tests** - Ensure existing functionality unchanged
5. **Load tests** - Behavior under sustained load

## Rollback Strategy

Each optimization should be:

1. **Feature-flagged** - Config option to enable/disable
2. **Incrementally deployable** - Can deploy without enabling
3. **Independently reversible** - Disable single optimization without affecting others

Example config structure:
```yaml
loader:
  # Existing
  batch_size: 1000
  max_concurrent_loaders: 4
  
  # New optimization flags
  optimizations:
    use_prepared_statements: true
    use_batched_inserts: false
    insert_batch_size: 100
    use_concurrent_inserts: false
    insert_concurrency: 32
    use_paged_reads: false
    page_size: 5000
    use_compression: false
```

## Estimated Impact

| Configuration | Throughput Estimate |
|---------------|---------------------|
| Current (tuned) | 120k rows/sec |
| + Prepared statements | 140-160k rows/sec |
| + Batched inserts | 250-400k rows/sec |
| + Concurrent inserts | 400-600k rows/sec |
| + All optimizations | 500k-1M rows/sec |

Note: Estimates assume ScyllaDB target. Cassandra targets will be limited by JVM/GC regardless of client optimizations.

## Summary

The current codebase prioritizes correctness and stability over raw performance. These optimizations represent known improvements that can be incrementally applied without disrupting working production deployments. Each optimization is isolated, testable, and reversible through configuration flags.

The goal is to achieve maximum throughput while maintaining the "zero regression" guarantee that has made this migration tooling reliable for production use.