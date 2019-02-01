---
title: Apache BookKeeper 4.9.0 Release Notes
---

This is the 16th release of Apache BookKeeper!

The 4.9.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.8.0,
which was released four months ago. It is a new milestone in Apache BookKeeper community.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.9.0](../../admin/upgrade). The technical details of
this release are summarized below.

## Highlights

The main features in 4.9.0 cover are around following areas:

- Dependencies Changes
- Public API
- Configuration
- Metadata
- Table Service
- Operations
- Builds & Testing
- Enhancements
- Bug Fixes

### Dependencies Changes

Here is a list of dependencies changed in 4.9.0:

- Upgrade [Jackson](http://fasterxml.com/) from `2.8.9` to `2.9.7`.
- Upgrade [Jline](https://jline.github.io/) to `2.11`.
- Upgrade [Netty](https://netty.io/) from `4.1.22` to `4.1.31`.
- Upgrade [TestContainers](https://www.testcontainers.org/) from `1.7.0` to `1.8.3`.

### Public API

There are multiple new client features introduced in 4.9.0. Here are two highlighted features:

- LedgerHandleAdv exposes `asyncAddEntry` variant that takes ByteBuf

### Configuration

There are bunch of new settings introduced in both bookie and client in 4.9.0. Here are those settings:

#### Bookie

- `serverNumIOThreads`: configures the number of IO threads for bookies
  (see [#1612](https://github.com/apache/bookkeeper/pull/1612))
- The default value of `fileInfoFormatVersionToWrite` is bumped from `0` to `1`.
  (see [#1689](https://github.com/apache/bookkeeper/pull/1689))
- The default value of `journalFormatVersionToWrite` is bumped from `5` to `6`.
  (see [#1689](https://github.com/apache/bookkeeper/pull/1689))

#### Client

- `numIOThreads`: configures the number of IO threads for client
  (see [#1612](https://github.com/apache/bookkeeper/pull/1612))

### Metadata

There are a few big changes around metadata in 4.9.0. They are:

- Refactor ledger metadata in LedgerHandle to make ledger metadata instance immutable (see [#281](https://github.com/apache/bookkeeper/issues/281))
- Store ledger metadata in binary protobuf format (see details at [#723](https://github.com/apache/bookkeeper/issues/723))
- Etcd based metadata driver implementation is in BETA release (see details at [#1639](https://github.com/apache/bookkeeper/issues/1639))

Additionally, there are bunch of new interfaces introduced in the metadata driver API.

- [Issue #1619: Provide async version of markLedgerUnderreplicated for LedgerUnderreplicationManager](https://github.com/apache/bookkeeper/pull/1619)

### Table Service

There are a lot of improvements and features introduced into the table service. The maturity of table service is moving from alpha to beta,
and has started to be used as the state storage for Pulsar Functions. More table service usage will come in Pulsar's future releases.

Starting from 4.9.0, bookkeeper will release a python client for table service. See details at [#1691](https://github.com/apache/bookkeeper/pull/1691)

### Operations

#### HTTP Admin REST Endpoint

- `/api/v1/bookie/gc_details` is introduced to retrieve the GC details.
- `/api/v1/bookie/gc` is introduced to trigger GC through HTTP REST endpoint.

#### BookieShell

There are are multiple new commands are added in BookieShell. Here are a few highlighted:

- `regenerate-interleaved-storage-index-file` command is introduced for rebuilding the index files for interleaved based ledger storage. ([#1642](https://github.com/apache/bookkeeper/pull/1642))
- `ledgermetadata` command now supports dumping/restoring ledger metadata to/from file.
- `localconsistencycheck` command is introduce for running consistency check on bookies locally. ([#1819](https://github.com/apache/bookkeeper/pull/1819))
- a new `bk-perf` script is introduced for running performance benchmark on bookkeeper. ([1697](https://github.com/apache/bookkeeper/pull/1697))

A new BookKeeper CLI package is released as `bkctl`. This `bkctl` package includes both the existing bookie shell and the new `bkctl` tool.

#### MDC

Mapped Diagnostic Context (MDC) is now supported at both bookie and client sides. Application request context can be passed as context
and being logged through slf4j/log4j. This simplifies throubleshooting of request-level failures/errors. See details at [#1672](https://github.com/apache/bookkeeper/pull/1672).

#### Stats Annotation

`StatsDoc` annotation is introduced in [BP-36](https://github.com/apache/bookkeeper/pull/1786). The `StatsDoc` annotation is
used for documenting stats added across the project.

### Builds & Testing

- Java 11 is supported for building bookkeeper.

### Enhancements

- [Issue 1791: Read Submission should bypass OSE Threads](https://github.com/apache/bookkeeper/pull/1792)
- A new module is introduced for enabling CPU affinity [#1641](https://github.com/apache/bookkeeper/pull/1641)
- [Issue 1682: Added BlockingQueue implementation based on JCtools](https://github.com/apache/bookkeeper/pull/1682)
- [Issue 1813: Set default sizes of DbLedgerStorage read and write cache to be proportional to JVM direct memory](https://github.com/apache/bookkeeper/pull/1813)
- [Issue 1808: Allow to configure sticky reads](https://github.com/apache/bookkeeper/pull/1808)
- [Issue 1754: Netty allocator wrapper](https://github.com/apache/bookkeeper/pull/1754)

### Bug Fixes

#### Bookie

- [Issue #1414: Ensure BufferedChannel instance is properly closed](https://github.com/apache/bookkeeper/pull/1414)
- [Issue #1805: Fixed Auth with V2 protocol](https://github.com/apache/bookkeeper/pull/1805)
- [Issue #1769: prevent race between flush and delete from recreating index](https://github.com/apache/bookkeeper/pull/1769)
- [Issue #1807: Fix sorted ledger storage rotating entry log files too frequent](https://github.com/apache/bookkeeper/pull/1807)
- [Issue #1843: DbLedgerStorage should do periodical flush](https://github.com/apache/bookkeeper/pull/1843)

#### AutoRecovery

- [Issue #1578: Fixed deadlock in auditor blocking ZK thread](https://github.com/apache/bookkeeper/pull/1608)
- [Issue #1834: Only publish suspect ledgers if they have missing fragments](https://github.com/apache/bookkeeper/pull/1834)

#### Client

- [Issue #1762: Don't cache Bookie hostname DNS resolution forever](https://github.com/apache/bookkeeper/pull/1762)
- [Issue #1788: Fix bugs in DefaultEnsemblePlacementPolicy](https://github.com/apache/bookkeeper/pull/1788)
- [Issue #1862: Fix selectFromNetworkLocation in RackawareEnsemblePlacementPolicyImpl](https://github.com/apache/bookkeeper/pull/1862)
- [Issue #1857: changingEnsemble should be negated before calling unset success](https://github.com/apache/bookkeeper/pull/1857)

## Full list of changes

- [https://github.com/apache/bookkeeper/milestone/5](https://github.com/apache/bookkeeper/milestone/5?closed=1)
