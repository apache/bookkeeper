---
title: Apache BookKeeper 4.10.0 Release Notes
---

This is the 20th release of Apache BookKeeper!

The 4.10.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.9.0.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.10.0](../../admin/upgrade). The technical details of
this release are summarized below.

## News and noteworthy

- [https://github.com/apache/bookkeeper/pull/2069] Use pure python implementation of MurmurHash
- [https://github.com/apache/bookkeeper/pull/1934] Bump Netty and GRPC version
- [https://github.com/apache/bookkeeper/pull/1907] Add new *bkctl* shell tool
- [https://github.com/apache/bookkeeper/issues/1602] Cluster Metadata Checker
- [https://github.com/apache/bookkeeper/pull/2154] Auto refresh TLS certificate at bookie-server
- [https://github.com/apache/bookkeeper/pull/2150] Improve journal throughput when journalSyncData is disabled.
- [https://github.com/apache/bookkeeper/pull/2147] Journal should respect to `flushWhenQueueEmpty` setting
- [https://github.com/apache/bookkeeper/pull/2132] Make default Bookie scripts work on JDK11+
- [https://github.com/apache/bookkeeper/pull/2128] Allow to override default SASL service name 'bookkeeper'
- [https://github.com/apache/bookkeeper/pull/2117] BookKeeper Admin API: Implement a method to get all the Bookies
- [https://github.com/apache/bookkeeper/pull/2111] Ensure getStickyReadBookieIndex returns valid bookie index


## Full list of changes

- [https://github.com/apache/bookkeeper/milestone/6](https://github.com/apache/bookkeeper/milestone/6?closed=1)
