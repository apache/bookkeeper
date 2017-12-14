---
title: "BP-23: ledger balancer"
issue: https://github.com/apache/bookkeeper/846
state: "WIP" 
release: "x.y.z"
---

### Motivation

There are typical two use cases of _Apache BookKeeper_, one is *Messaging/Streaming/Logging* style use cases, the other one is *Storage* style use cases.

In Messaging/Streaming/Logging oriented use case (where old ledgers/segments are most likely will be deleted at some point), we don't actually need to rebalance the ledgers stored on bookies.

However,
In Storage oriented use cases (where data most likely will never be deleted), BookKeeper data might not always be placed uniformly across bookies. One common reason is addition of new bookies to an existing cluster. This proposal is proposing to provide a balancer mechanism (as an utility, also as part of AutoRecovery daemon), that analyzes ledger distributions and balances ledgers across bookies.

It replicated ledgers to new bookies (based on resource-aware placement policies) until the cluster is deemed to be balanced, which means that disk utilization of every bookie (ratio of used space on the node to the capacity of the node) differs from the utilization of the cluster (ratio of used space on the cluster to total capacity of the cluster) by no more than a given threshold percentage.

The balancer will replicate ledgers away from disk-filled bookies as first priority.

### Public Interfaces

There is not public API changes.

Potentially we might need a new command in `BookieShell` to run balancer.

### Proposed Changes

[TBD]

A couple of thoughts:

- it should be moving ledgers from filled-up bookies only.
- it should enforce resource-awareness into the ledger placement policy.
- it should provide capabilities to throttle bandwidth usage.


### Compatibility, Deprecation, and Migration Plan

N/A

### Test Plan

[TBD]

### Rejected Alternatives

Manual balancer using `Recovery` tools.

[TBD]
