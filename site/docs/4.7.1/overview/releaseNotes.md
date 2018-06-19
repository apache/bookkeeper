---
title: Apache BookKeeper 4.7.1 Release Notes
---

This is the eleventh release of Apache BookKeeper!

The 4.7.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.0.

Apache BookKeeper users who are using 4.7.0 are encouraged to upgrade to 4.7.1. The technical details of this release are summarized
below.

## Highlights

- Performance enhancement on eliminating bytes copying in `AddEntry` code path, see [apache/bookkeeper#1361](https://github.com/apache/bookkeeper/pull/1361)

- Introduce Fast and Garbage-Free Statistics Timers in Codahale Stats Provider, see [apache/bookkeeper#1364](https://github.com/apache/bookkeeper/pull/1364)

- Fix OrderedScheduler handling null key, see [apache/bookkeeper#1372](https://github.com/apache/bookkeeper/pull/1372)

- Fix zookeeper ledger manager on handling no ledger exists, see [apache/bookkeeper#1382](https://github.com/apache/bookkeeper/pull/1382)

- Fix long poll reads when ensemble size is larger than write quorum size, see [apache/bookkeeper#1404](https://github.com/apache/bookkeeper/pull/1404)

- Fix IllegalReferenceCount on filling readahead cache for DbLedgerStorage, see [apache/bookkeeper#1487](https://github.com/apache/bookkeeper/issues/1487)

- Fix LedgerEntry recycling issue on long poll speculative reads, see [apache/bookkeeper#1509](https://github.com/apache/bookkeeper/pull/1509)

- Various bug fixes and improvements around bookkeeper table service, see changes under [apache/bookkeeper#release/4.7.1](https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=is%3Aclosed+label%3Aarea%2Ftableservice+label%3Arelease%2F4.7.1)

### Dependencies Upgrade

Here is a list of dependencies changed in 4.7.1:

- [Grpc](https://grpc.io/) is upgraded from `1.5.0` to `1.12.0`. See [apache/bookkeeper#1441](https://github.com/apache/bookkeeper/pull/1441)
- [Netty](http://netty.io/) is upgraded from `4.1.12` to `4.1.22`. See [apache/bookkeeper#1441](https://github.com/apache/bookkeeper/pull/1441)
- [Protobuf](https://developers.google.com/protocol-buffers/) is upgraded from `3.4.0` to `3.5.1`. See [apache/bookkeeper#1466](https://github.com/apache/bookkeeper/pull/1466)
- [RocksDB](http://rocksdb.org/) is upgraded from `5.8.6` to `5.13.1`. See [apache/bookkeeper#1466](https://github.com/apache/bookkeeper/pull/1466)

`Reflective setAccessible(true)` is disabled by default in Netty while using java9+. This might result in performance degradation. Consider reenabling `Reflective setAccessible(true)` by setting
environment value `io.netty.tryReflectionSetAccessible` to `true`. See [netty/netty#7650](https://github.com/netty/netty/pull/7650) for more details.

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.1+is%3Aclosed)
