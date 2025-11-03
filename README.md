# ScyllaDB Cluster-to-Cluster Sync Service (Rust)
Ultra-Fast RPO/RTO Sensitive ScyllaDB (Cassandra DB Adaptive) Cluster to Cluster Tenant Data Synching Service in Rust using Shadow Write Transition Pattern.



## The Shadow Write Transition Pattern

```rust
WIP
```


## Cassandra DB vs ScyllaDB Kubernetes Operator Cost Reduction

The high-cognitive load of configuration and JVM overhead using the Cassandra DB and its deployment using the Cassandra DB Kubernetes Operator.

```shell
# cassandra-dc1.yaml - What you need to manage:
apiVersion: cassandra.datastax.com/v1beta1
kind: CassandraDatacenter
metadata:
  name: dc1
spec:
  clusterName: iconik-cluster
  serverVersion: "4.0.7"
  size: 12  # Need more nodes!
  config:
    jvm-options:
      initial_heap_size: "8G"
      max_heap_size: "8G"
      # GC Tuning nightmare begins...
      additional-jvm-opts:
        - "-XX:+UseG1GC"
        - "-XX:G1RSetUpdatingPauseTimePercent=5"
        - "-XX:MaxGCPauseMillis=300"
        # ... 20 more GC flags
```

The low-cognitive load of configuration and no-JVM overhead using the Scylla DB (uses 100% C++) and its deployment using the ScyllaDB Kubernetes Operator.

```shell
# scylla-cluster.yaml - Self-tuning magic:
apiVersion: scylla.scylladb.com/v1
kind: ScyllaCluster
metadata:
  name: iconik-media
spec:
  version: 5.2.0
  developerMode: false  # Production mode = auto-tuning!
  cpuset: true  # CPU pinning for zero latency
  size: 3  # That's it! 3 nodes instead of 12
  resources:
    requests:
      cpu: 7  # Leaves 1 CPU for system
      memory: 30Gi  # All usable, no heap sizing!
```

The applications using CassandraDB and to the switch to SycllaDB provides 100% transparent functionality with no code changes.

```pseudo
# Existing Cassandra DB code:
cluster = Cluster(['cassandra-node1', 'cassandra-node2'])
session = cluster.connect('iconik')
...

# SycllaDB code (identical)
cluster = Cluster(['scylla-node1', 'scylla-node2'])
session = cluster.connect('iconik')
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







## References

- Cassandra DB Kubernetes Operator
- ScyllaDB Kubernetes Operator