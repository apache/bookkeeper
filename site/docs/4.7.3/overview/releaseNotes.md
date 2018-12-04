---
title: Apache BookKeeper 4.7.3 Release Notes
---

This is the 16th release of Apache BookKeeper!

The 4.7.3 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.2.

Apache BookKeeper users who are using 4.7.2 are encouraged to upgrade to 4.7.3. The technical details of this release are summarized
below.

## Highlights

- Cancel Scheduled SpeculativeReads, see [apache/bookkeeper#1665](https://github.com/apache/bookkeeper/pull/1665)

- IllegalReferenceCountException at closing EntryLogManagerForSingleEntryLog, see [apache/bookkeeper#1703](https://github.com/apache/bookkeeper/issues/1703)

- EntryMemTable.newEntry retains reference to passed ByteBuffer array can cause corruption on journal replay, see [apache/bookkeeper#1737](https://github.com/apache/bookkeeper/issues/1737)

- Ledger deletion racing with flush can cause a ledger index to be resurrected, see [apache/bookkeeper#1757](https://github.com/apache/bookkeeper/issues/1757)

- Don't cache Bookie hostname DNS resolution forever, see [apache/bookkeeper#1762](https://github.com/apache/bookkeeper/pull/1762)

- Use default metric registry in Prometheus export, see [apache/bookkeeper#1765](https://github.com/apache/bookkeeper/pull/1765)

- Fix Auth with v2 protocol, see [apache/bookkeeper#1805](https://github.com/apache/bookkeeper/pull/1805)

- Remove MathUtils.now to address compaction scheduling deplay issues, see [apache/bookkeeper#1837](https://github.com/apache/bookkeeper/pull/1837)

- DbLedgerStorage should do periodical flush, see [apache/bookkeeper#1843](https://github.com/apache/bookkeeper/pull/1843)

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.3+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.3+is%3Aclosed)
