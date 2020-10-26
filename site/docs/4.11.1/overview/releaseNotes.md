---
title: Apache BookKeeper 4.11.1 Release Notes
---

Apache BookKeeper users are encouraged to upgrade to 4.11.1. The technical details of this release are summarized
below.

## Highlights

- Upgrade Netty,Vertx and RocksDB
- Better error reporting in case of ZooKeeper related errors
- Fix error that prevents Garbage Collections in case of corrupted EntryLogger file
- Support for Apache ZooKeeper 3.6.x

### Changes

- [https://github.com/apache/bookkeeper/pull/2410] Upgrade the `vertx` version to 3.5.3

- [https://github.com/apache/bookkeeper/pull/2390] Issue #2385: NullPointerException in Zookeeper multiple operations execution with 3.6.1

- [https://github.com/apache/bookkeeper/pull/2389] Issue #2197: bkctl binary distribution needs a 'logs' directory

- [https://github.com/apache/bookkeeper/pull/2384] Track ZooKeeper errors as causes of ZKException

- [https://github.com/apache/bookkeeper/pull/2383] fix fillReadAheadCache stat bug

- [https://github.com/apache/bookkeeper/pull/2381] The latency of BenchThroughputLatency may be wrong due to Integer overflow when we do a large scale benchmark test

- [https://github.com/apache/bookkeeper/pull/2380] NP check for print BookieSocketAddress and a better format

- [https://github.com/apache/bookkeeper/pull/2379] Updated netty,netty-boringssl and rocksdb

- [https://github.com/apache/bookkeeper/pull/2373] Issue 2264: Bookie cannot perform Garbage Collection in case of corrupted EntryLogger file

- [https://github.com/apache/bookkeeper/pull/2327] Bookie Client add quarantine ratio when error count exceed threshold

- [https://github.com/apache/bookkeeper/pull/2415] Spammy log when one bookie of ensemble is down

## Compatiblity

This is a point release and it does not bring API changes.

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.11.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.11.1+is%3Aclosed)
