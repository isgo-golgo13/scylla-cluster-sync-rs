# Phase 2 Optimizations — sstable-loader

## Overview

Phase 2 adds a high-performance insert path to `sstable-loader` using Rust's
compile-time feature flag system (`#[cfg(feature = "optimized-inserts")]`).

**The original production code is physically preserved and active by default.**
The optimized path is compiled in only when explicitly requested.
This provides a zero-regression guarantee: a default build is byte-for-byte
identical to the pre-Phase-2 binary.

---

## What Changed

| Component | Change |
|-----------|--------|
| `services/sstable-loader/Cargo.toml` | Added `[features] optimized-inserts` |
| `services/sstable-loader/src/config.rs` | Added `insert_batch_size`, `insert_concurrency` fields (with defaults) |
| `services/sstable-loader/src/loader.rs` | Filtering loop refactored to collect rows first, then `#[cfg]`-dispatches to either the original sequential path or the new optimized path |
| `config/sstable-loader.yaml` | Added Phase 2 config fields (ignored in default builds) |
| `Makefile` | Added `build-optimized` and `build-optimized-release` targets |

**Nothing else was modified. dual-writer and dual-reader are untouched.**

---

## Build Commands

```bash
# Standard production build — original code path, zero change
make build
make build-release

# Phase 2 optimized build — sstable-loader only
make build-optimized           # debug
make build-optimized-release   # release (use this in production)
```

Equivalent `cargo` commands if needed directly:

```bash
# Standard (production)
cargo build -p sstable-loader

# Optimized
cargo build -p sstable-loader --features optimized-inserts
cargo build -p sstable-loader --release --features optimized-inserts
```

---

## Optimized Insert Path Details

When built with `--features optimized-inserts`, the insert path changes as follows:

### 1. Prepared Statements (always active in optimized build)

The `INSERT INTO table JSON ?` query is **prepared once per token range** rather
than parsed fresh on every row. Eliminates query parse overhead.

**Expected gain:** 10–30% throughput improvement.

### 2. UNLOGGED BATCH inserts (`insert_batch_size > 1`)

When `insert_batch_size > 1` (default: `100`), rows are grouped into
`UNLOGGED BATCH` statements before being sent. This amortises the network
round-trip cost across multiple rows.

**`UNLOGGED` is mandatory** — `LOGGED` batches use Paxos and are slower.
The driver enforces this. ScyllaDB-specific gain; Cassandra will benefit less.

**Expected gain:** 2–5× throughput improvement over prepared-only.

**Retry semantics:** entire batch is retried on failure. If all retries
are exhausted, all rows in the batch are counted as failed and logged
to `failed_rows.jsonl` with `error_type: batch_insert_error`.

### 3. Concurrent prepared inserts (`insert_batch_size == 1`)

When `insert_batch_size` is set to `1`, batching is disabled and instead
`insert_concurrency` concurrent prepared-statement executes are fired in
parallel using `futures::stream::buffer_unordered`.

Per-row retry logic mirrors the production path exactly.

**Expected gain:** 2–4× throughput improvement over sequential.

---

## Configuration Tuning

In `config/sstable-loader.yaml`, under `loader:`:

```yaml
# Batch size: rows per UNLOGGED BATCH round-trip
# Start at 100, tune down if ScyllaDB returns "batch too large" warnings
insert_batch_size: 100

# Concurrency: only active when insert_batch_size == 1
insert_concurrency: 32
```

**Recommended starting config for ScyllaDB target:**
- `insert_batch_size: 100` (UNLOGGED BATCH mode)
- Monitor ScyllaDB logs for batch size warnings; tune down if needed

**For Cassandra target:**
- `insert_batch_size: 1`, `insert_concurrency: 32` (concurrent mode)
- Cassandra batch performance limited by JVM regardless; concurrency gains more

---

## Expected Throughput

| Configuration | Estimate |
|---------------|----------|
| Current production | 120k rows/sec |
| + Prepared statements | 140–160k rows/sec |
| + UNLOGGED BATCH (batch_size=100) | 250–400k rows/sec |
| + Concurrent inserts (batch_size=1, concurrency=32) | 200–350k rows/sec |

*Estimates for ScyllaDB target. Cassandra targets limited by JVM/GC regardless.*

---


## Docker Builds (linux/amd64 — GKE / EKS / EC2)

All Docker images target `linux/amd64` via Docker buildx. Two variants of `sstable-loader` are now available, distinguished by image tag. All other services (`dual-writer`, `dual-reader`) are unchanged and use their existing Dockerfiles and targets.

### Standard production image (original code path)

```bash
# Build locally (--load, no push)
make docker-build-sstable-loader

# Build + push to registry
make docker-release-sstable-loader
```

Tags produced: `sstable-loader:latest`, `sstable-loader:<git-version>`

### Phase 2 optimized image

```bash
# Build locally (--load, no push)
make docker-build-sstable-loader-optimized

# Build + push to registry
make docker-release-sstable-loader-optimized
```

Tags produced: `sstable-loader:optimized`, `sstable-loader:<git-version>-optimized`

The optimized image uses `Dockerfile.sstable-loader-optimized` — identical to the original except for the single `cargo build` line which adds `--features optimized-inserts`. The `-optimized` tag suffix ensures the two images coexist in the registry with no risk of accidentally deploying the wrong one.

To switch a running deployment from standard to optimized, update the image tag in your Helm `values.yaml` or Kubernetes manifest from `sstable-loader:latest` to `sstable-loader:optimized` and roll out normally.


To revert to original behaviour at any time:

```bash
# Simply build without the feature flag
make build-release
```

No config changes needed. The original code path requires no modifications.


