---
title: Geo-replication
subtitle: Replicate data across BookKeeper clusters
---

*Geo-replication* is the replication of data across BookKeeper clusters. In order to enable geo-replication for a group of BookKeeper clusters,

## Global ZooKeeper

Setting up a global ZooKeeper quorum is a lot like setting up a cluster-specific quorum. The crucial difference is that

### Geo-replication across three clusters

Let's say that you want to set up geo-replication across clusters in regions A, B, and C. First, the BookKeeper clusters in each region must have their own local (cluster-specific) ZooKeeper quorum.

> BookKeeper clusters use global ZooKeeper only for metadata storage. Traffic from bookies to ZooKeeper should thus be fairly light in general.

The crucial difference between using cluster-specific ZooKeeper and global ZooKeeper is that {% pop bookies %} is that you need to point all bookies to use the global ZooKeeper setup.

## Region-aware placement polocy

## Autorecovery
