---
title: Apache BookKeeper 4.11.0 Release Notes
---

This is the 21th release of Apache BookKeeper!

The 4.11.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.10.0.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.11.0](../../admin/upgrade). The technical details of
this release are summarized below.

## News and noteworthy

-  Upgraded ZooKeeper version from `3.4.13` to `3.5.7` with #2112
-  BookKeeper-server depends on `org.apache.httpcomponents-httpcore-4.4.9` with #2156


### Changes

- [https://github.com/apache/bookkeeper/pull/2338] Fix bookie port conflict when using LocalBookKeeper
- [https://github.com/apache/bookkeeper/pull/2333] Handle QuorumCoverage should only count unknown nodes
- [https://github.com/apache/bookkeeper/pull/2326] Update jackson version 2.11.0
- [https://github.com/apache/bookkeeper/pull/2314] BP-38: Publish Bookie Service Info including all advertised addresses on Metadata Service and it is backward compatible
- [https://github.com/apache/bookkeeper/pull/2313] add REST API to manage auto-recovery
- [https://github.com/apache/bookkeeper/pull/2312] Support metadata decoding for list-ledger api
- [https://github.com/apache/bookkeeper/pull/2300] files: Fix TLS with with v2 protocol
- [https://github.com/apache/bookkeeper/pull/2297] Update Arquillian Cube to 1.18.2
- [https://github.com/apache/bookkeeper/pull/2291] Update Prometheus library to 0.8.1
- [https://github.com/apache/bookkeeper/pull/2205] Handle empty ledger segmant while replica-check
- [https://github.com/apache/bookkeeper/pull/2156] Add Hostname verification for bookie-mTLS
- [https://github.com/apache/bookkeeper/pull/2112] Update ZooKeeper dependency to 3.5.7



## Full list of changes

- [https://github.com/apache/bookkeeper/milestone/7](https://github.com/apache/bookkeeper/milestone/6?closed=1)