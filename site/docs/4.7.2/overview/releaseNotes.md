---
title: Apache BookKeeper 4.7.2 Release Notes
---

This is the 12th release of Apache BookKeeper!

The 4.7.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.1.

Apache BookKeeper users who are using 4.7.1 are encouraged to upgrade to 4.7.2. The technical details of this release are summarized
below.

## Highlights

- Fix high cpu usage issue in DbLedgerStorage by avoiding using RocksDD#deleteRange, see [apache/bookkeeper#1620](https://github.com/apache/bookkeeper/pull/1620)

- Fix deadlock in Auditor blocking zookeeper thread, see [apache/bookkeeper#1619](https://github.com/apache/bookkeeper/pull/1619)

- Fix ArrayIndexOutOfBoundsException on ConcurrentLongHashMap, see [apache/bookkeeper#1606](https://github.com/apache/bookkeeper/pull/1606)

- Fix deferred failure handling causes data loss, see [apache/bookkeeper#1591](https://github.com/apache/bookkeeper/pull/1591)

- Fix ConcurrentModificationException using nonblocking logReader#readNext, see [apache/bookkeeper#1544](https://github.com/apache/bookkeeper/pull/1544)

- Fix Bookie shutdown fails to exit, see [apache/bookkeeper#1543](https://github.com/apache/bookkeeper/issues/1543)

- Fix race conditions on accessing guava multimap in PCBC when using v2 protocol, see [apache/bookkeeper#1618](https://github.com/apache/bookkeeper/pull/1618)

### Dependency Changes

In 4.7.2, [Zookeeper](https://zookeeper.apache.org/) version is downgraded from `3.5.3-beta` to `3.4.13` to avoid having a `beta` dependency and address maturity concerns.
The downgrade is safe and smooth. No extra actions are required from switching bookkeeper 4.7.1 to 4.7.2.

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.2+is%3Aclosed)
