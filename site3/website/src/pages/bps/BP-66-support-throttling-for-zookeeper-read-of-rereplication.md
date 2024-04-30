# BP-66: support throttling for zookeeper read of rereplication

### Motivation

Each time the cluster trigger the re-replication, all replicators will read data from zookeeper. This can cause a great pressure on Zookeeper. We need to support throttling for zookeeper read of re-replication.

For example, in a Pulsar cluster, we enable auto-recovery for every bookie. There are 400 bookies in a cluster, which means there are 400 replicators in the cluster.
And there are about 3000 ledgers in each bookie, 1/3 of them are small ledgers, whose size is less than 0.1MB, that is 1000 small ledgers in each bookie.
If we decommission one bookie, the read latency of zookeeper will increase to minutes. 


### Configuration
add the following configuration:
```
zkReplicationTaskRateLimit
```
default value is 0, which means no limit.
Value greater than 0 will enable the rate limit, and the value is the number of tasks that can be acquired per second.
Decimals are allowed too, for example, 0.5 means 1 task every 2 seconds, 1 means 1 task per second, 2 means 2 tasks per second, and so on.

### Proposed Changes

Add a new configuration `zkReplicationTaskRateLimit` to control the rate of zookeeper read of re-replication.


### Compatibility, Deprecation, and Migration Plan

Full compatibility with the existing behavior. No deprecation needed.
