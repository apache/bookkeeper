---
title: Apache BookKeeper 4.9.2 Release Notes
---

The 4.9.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.9.1.

Apache BookKeeper users who are using 4.9.1 are encouraged to upgrade to 4.9.2. The technical details of this release are summarized
below.

## Highlights

 - Added HTTP handler to expose bookie state [apache/bookkeeper#1995](https://github.com/apache/bookkeeper/pull/1995)
 - Fix DbLedgerStorage encountering unexpected entry id [apache/bookkeeper#2002](https://github.com/apache/bookkeeper/pull/2002)
 - Close db properly to avoid open RocksDB failure at the second time [apache/bookkeeper#2022](https://github.com/apache/bookkeeper/pull/2022)
 - Cancel LAC watch when longpoll LAC times out [apache/bookkeeper#2051](https://github.com/apache/bookkeeper/pull/2051)
 - Wait for LAC update even if ledger fenced [apache/bookkeeper#2052](https://github.com/apache/bookkeeper/pull/2052)
 - Bugfix for Percentile Calculation in FastCodahale Timer Implementation [apache/bookkeeper#2054](https://github.com/apache/bookkeeper/pull/2054)
 - Use pure python implementation of MurmurHash [apache/bookkeeper#2069](https://github.com/apache/bookkeeper/pull/2069)
 - Bookieshell lastmark command isn't functional, always returning 0-0 [apache/bookkeeper#2076](https://github.com/apache/bookkeeper/pull/2076)


## Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.9.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.9.2+is%3Aclosed)
