# ScyllaDB Cluster-to-Cluster Sync Service (Rust)
Ultra-Fast RPO/RTO Sensitive ScyllaDB (Cassandra DB Adaptive) Cluster to Cluster Tenant Data Synching Service in Rust using Shadow Write Transition Pattern.



## The Shadow Write Transition Pattern

```rust
WIP
```


## Cassandra DB vs ScyllaDB Kubernetes Operator Cost Reduction

```shell
# Here's what changes with ScyllaDB Operator on EKS:

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





