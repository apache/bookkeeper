# BP-66: support throttling for zookeeper read of rereplication

### Motivation

Each time the cluster trigger the re-replication, all replicators will read data from zookeeper. This can cause a great pressure on Zookeeper. We need to support throttling for zookeeper read of re-replication.

For example, in a Pulsar cluster, we enable auto-recovery for every bookie. There are 400 bookies in a cluster, which means there are 400 replicators in the cluster.
And there are about 3000 ledgers in each bookie, 1/3 of them are small ledgers, whose size is less than 0.1MB, that is 1000 small ledgers in each bookie.
If we decommission one bookie, the read latency of zookeeper will increase to minutes. 


### Configuration
add the following configuration:
```
replicationAcquireTaskPerSecond
```


### Proposed Changes

Add a new configuration `replicationAcquireTaskPerSecond` to control the rate of zookeeper read of re-replication.


### Compatibility, Deprecation, and Migration Plan

Full compatibility with the existing behavior. No deprecation needed.
