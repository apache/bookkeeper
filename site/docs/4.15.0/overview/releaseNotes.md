---
title: Apache BookKeeper 4.15.0 Release Notes
---

Release 4.15 includes many upgrades to third party libraries marked with CVEs, 
adds more configuration options, extends REST API, 
adds an option to run without journal, improves memory utilization and stability, and more!

Apache BookKeeper users are encouraged to upgrade to 4.15.0. The technical details of this release are summarized
below.

## Breaking Changes

* `BookieServer` API changed and the code that creates its instances will require addition
of the `UncleanShutdownDetection` parameter.
See [PR 2936](https://github.com/apache/bookkeeper/pull/2936) for details and examples.

* `Bookie` class now is an interface with implementation in `BookieImpl`.
Code that uses it may need changes.
For details please refer to [PR 2717](https://github.com/apache/bookkeeper/pull/2717).

* `LedgerUnderreplicationManager` interface added a new method.
Code that implements the interface will need changes.
See [PR 2805](https://github.com/apache/bookkeeper/pull/2805) for details.

* `MetadataBookieDriver` interface added a new method and removed an old one.
`RegistrationManager` interface added a new method.
`ByteBufAllocatorWithOomHandler` interface is added and used instead of
the `ByteBufAllocator` in multiple places.
Code that implements the interfaces will need changes.
See [PR 2901](https://github.com/apache/bookkeeper/pull/2901) for details.

## Highlights

### Configuration

* [ledgerMetadataVersion](https://github.com/apache/bookkeeper/pull/2708): 
  BookKeeper-Client config to write ledger metadata with configured version.
* [clientTcpUserTimeoutMillis](https://github.com/apache/bookkeeper/pull/2761): 
  Added TCP_USER_TIMEOUT to Epoll channel config. 
* [auditorMaxNumberOfConcurrentOpenLedgerOperations and auditorAcquireConcurrentOpenLedgerOperationsTimeOutMSec](https://github.com/apache/bookkeeper/pull/2802)
  Add auditor get ledger throttle to avoid auto recovery zk session.
* [dbStorage_rocksDB_format_version](https://github.com/apache/bookkeeper/pull/2824)
  make rocksdb format version configurable.

### Features

* Running without journal. See [BP-46](https://github.com/apache/bookkeeper/pull/2706) for details.
* A REST API to get or update bookie readOnly state. [Details](https://github.com/apache/bookkeeper/pull/2799)
* Separate config files for Rocks DB. [Details](https://github.com/apache/bookkeeper/pull/3056/)

### Improvements

* Build and tests work on JDK 17
* CLI: listunderreplicated command has an option to return count without printing all ledgers https://github.com/apache/bookkeeper/pull/3228
* Stream Storage: support an optional time to live (TTL) on a per table basis https://github.com/apache/bookkeeper/pull/2775
* Added dDb ledger index rebuild operation and CLI commands https://github.com/apache/bookkeeper/pull/2774
* Support multi ledger directories for rocksdb backend entryMetadataMap https://github.com/apache/bookkeeper/pull/2965
* Improved memory utilization
    * support shrink for ConcurrentLong map or set https://github.com/apache/bookkeeper/pull/3074
    * reduce unnecessary expansions for ConcurrentLong map and set https://github.com/apache/bookkeeper/pull/3072
* read speed rate limiter for scanning entry log file in entryMetadataMap rebuild https://github.com/apache/bookkeeper/pull/2963
* Other improvements in areas such as test, documentation, CI, metrics, logging, and CLI tools. 

### Notable fixes

* Bookkeeper client might not close the channel for several minutes after a Bookie crashes https://github.com/apache/bookkeeper/issues/2482
* Stream storage: Ensure progress while restoring from checkpoint. https://github.com/apache/bookkeeper/pull/2764
* Entry Log GC may get blocked when using entryLogPerLedgerEnabled option https://github.com/apache/bookkeeper/pull/2779
* Fix region aware placement policy use disk weight https://github.com/apache/bookkeeper/pull/2981
* Some cases that could cause RocksDB segfault
* DistributedLogManager can skip over a segment on read. https://github.com/apache/bookkeeper/pull/3064
* Backpressure: check all bookies of writeset are writable https://github.com/apache/bookkeeper/pull/3055
* Fix Journal.ForceWriteThread.forceWriteRequests.put deadlock https://github.com/apache/bookkeeper/pull/2962
* PendingReadOp: Fix ledgerEntryImpl reuse problem https://github.com/apache/bookkeeper/pull/3110
* Region/rack aware placement policy: replace bookie bug https://github.com/apache/bookkeeper/pull/2642
* ReplicationWorker: numLedgersReplicated metric does not update https://github.com/apache/bookkeeper/pull/3218
* Force GC doesn't work under forceAllowCompaction when disk is full https://github.com/apache/bookkeeper/pull/3205

### Dependencies changes

Upgraded dependencies to address CVEs include:
* vertx
* freebuilder
* libthrift
* netty
* bouncycastle
* commonsIO
* jetty
* log4j
* grpc
* protobuf
* snakeyaml
* RocksDB
* jackson
* jackson-databind
* Zookeeper
* http-core
* dropwizard metrics

Dependency on log4j v.1 is removed.

## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.15.0


