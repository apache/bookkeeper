---
title: Apache BookKeeper 4.8.1 Release Notes
---

This is the 14th release of Apache BookKeeper!

The 4.8.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.8.0.

Apache BookKeeper users who are using 4.8.0 are encouraged to upgrade to 4.8.1. The technical details of this release are summarized
below.

## Highlights

- Use default metrics registry in Prometheus exporter, see [apache/bookkeeper#1765](https://github.com/apache/bookkeeper/pull/1765)

- Don't cache Bookie hostname DNS resolution forever, see [apache/bookkeeper#1762](https://github.com/apache/bookkeeper/pull/1762)

- Reduce stack traces in logs for common cases, see [apache/bookkeeper#1762](https://github.com/apache/bookkeeper/pull/1776)

- Ledger deletion racing with flush can cause a ledger index to be resurrected, see [apache/bookkeeper#1757](https://github.com/apache/bookkeeper/pull/1757)

- EntryMemTable.newEntry retains reference to passed ByteBuffer array, can cause corruption on journal replay, see [apache/bookkeeper#1737](https://github.com/apache/bookkeeper/pull/1737)


### Dependency Changes

There is no dependecy upgrade from 4.8.0.

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.8.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.8.1+is%3Aclosed)
