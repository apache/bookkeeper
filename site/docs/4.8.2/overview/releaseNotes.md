---
title: Apache BookKeeper 4.8.2 Release Notes
---

This is the 17th release of Apache BookKeeper!

The 4.8.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.8.1.

Apache BookKeeper users who are using 4.8.1 are encouraged to upgrade to 4.8.2. The technical details of this release are summarized
below.

## Highlights

- [DLOG] Avoid double read in readahead, see [apache/bookkeeper#1973](https://github.com/apache/bookkeeper/pull/1973)

- Small fix wrong nodesUninitialized count when checkCovered, see [apache/bookkeeper#1900](https://github.com/apache/bookkeeper/pull/1900)

- Handle double bookie failures, see [apache/bookkeeper#1886](https://github.com/apache/bookkeeper/pull/1886)

- dir_\*_usage stats are reported as 0, see [apache/bookkeeper#1884](https://github.com/apache/bookkeeper/pull/1884)

- Fix selectFromNetworkLocation in RackawareEnsemblePlacementPolicyImpl, see [apache/bookkeeper#1862](https://github.com/apache/bookkeeper/pull/1862)

- DbLedgerStorage should do periodical flush, see [apache/bookkeeper#1842](https://github.com/apache/bookkeeper/pull/1842)

- Add rest endpoint trigger_gc to trigger GC on Bookie, see [apache/bookkeeper#1838](https://github.com/apache/bookkeeper/pull/1838)

- Fix sorted ledger storage rotating entry log files too frequent, see [apache/bookkeeper#1807](https://github.com/apache/bookkeeper/pull/1807)

- Fixed Auth with v2 protocol, see [apache/bookkeeper#1805](https://github.com/apache/bookkeeper/pull/1805)

- [tools] add cookie related commands, see [apache/bookkeeper#1974](https://github.com/apache/bookkeeper/pull/1794)

- [tools] improve bkctl help message, see [apache/bookkeeper#1793](https://github.com/apache/bookkeeper/pull/1793)

- Read Submission should bypass OSE Threads, see [apache/bookkeeper#1791](https://github.com/apache/bookkeeper/pull/1791)

- Cache InetSocketAddress if hostname is IPAddress, see [apache/bookkeeper#1789](https://github.com/apache/bookkeeper/pull/1789)

- Fix bugs in DefaultEnsemblePlacementPolicy, see [apache/bookkeeper#1788](https://github.com/apache/bookkeeper/pull/1788)

### Dependency Changes

There is no dependency upgrade from 4.8.1.

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.8.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.8.2+is%3Aclosed)

