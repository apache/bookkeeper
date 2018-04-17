---
title: Apache BookKeeper 4.7.0 Release Notes
---

This is the tenth release of Apache BookKeeper!

The 4.7.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.6.0,
which was released four months ago. It is a big milestone in Apache BookKeeper community - Yahoo branch is fully merged
back to upstream, and Apache Pulsar (incubating) starts using official BookKeeper release for its upcoming 2.0 release.

It is also the first release of Apache DistributedLog after it is merged as sub modules of Apache BookKeeper.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.7.0](../../admin/upgrade). The technical details of
this release are summarized below.

## Highlights

The main features in 4.7.0 cover are around following areas:

- Dependencies Changes
- Public API
- Security
- DbLedgerStorage
- Metadata API
- Performance
- Operations
- Builds & Testing
- Bug Fixes

### Dependencies Changes

Here is a list of dependencies changed in 4.7.0:

- [JCommander](http://jcommander.org/) 1.48 is added as a dependency of bookkeeper-server module.
- [RocksDB](http://rocksdb.org/) 5.8.6 is introduced as part of `DbLedgerStorage` as a dependency of bookkeeper-server module.
- [DataSketches](https://datasketches.github.io/) 0.8.3 is introduced as a dependency of prometheus-metrics-provider module.
- Upgrade [Guava](https://github.com/google/guava) from `20.0` to `21.0`.

### Public API

There are multiple new client features introduced in 4.7.0. Here are two highlighted features:

#### Fluent API

The new fluent style APi is evolving in 4.7.0. All the methods in handlers are now having both async and sync methods.
See [#1288](https://github.com/apache/bookkeeper/pull/1288) for more details

#### CRC32C

`circe-checksum` module is ported from Apache Pulsar to Apache BookKeeper, and CRC32C digest type is added as one digest type option.
The JNI based CRC32C in `circe-checksum` module provides excellent performance than existing CRC32 digest type. Users are encouraged
to start use CRC32C digest type.

### Security

- New PEM format `X.509` certificates are introduced for TLS authentication. See [#965](https://github.com/apache/bookkeeper/pull/965) for more details.
- TLS related settings are converged into same settings as bookie server. See [Upgrade Guide](../../admin/upgrade) for more details.

### DbLedgerStorage

`DbLedgerStorage` is a new ledger storage that introduced by Yahoo and now fully merged into Apache BookKeeper. It is fully compatible for both v2 and v3
protocols and also support long polling. It uses [RocksDB](http://rocksdb.org/) to store ledger index, which eliminates the needed of ledger index files and
reduces the number of open file descriptors and the amount of random IOs can occurs during flushing ledger index.

### Metadata API

New serviceUri based metadata API is introduced as [BP-29](http://bookkeeper.apache.org/bps/BP-29-metadata-store-api-module). This metadata API provides the metadata
abstraction over ledger manager, registration service, allowing plugin different type of data stores as the metadata service.

### Performance

There are a lot for performance related bug fixes and improvements in 4.7.0. Some of the changes are highlighted as below:

- Leverage netty object recycler to reduce object allocations
- A bunch of contentions around locking are removed. E.g. [#1321](https://github.com/apache/bookkeeper/pull/1321) [#1292](https://github.com/apache/bookkeeper/pull/1292) [#1258](https://github.com/apache/bookkeeper/pull/1258)
- Introduce priority thread pool for accepting high priority reads/writes. This allows high priority reads/writes such as ledger recovery operations can
  succeed even bookies are overwhelmed. [#898](https://github.com/apache/bookkeeper/pull/898)
- Reorder slow bookies in read sequence. [#883](https://github.com/apache/bookkeeper/pull/883)
- Use atomic field updater and long adder to replace AtomicInteger/AtomicLong/AtomicReference in Dlog. [#1299](https://github.com/apache/bookkeeper/pull/1299)
- DataSketches library is used for implementing prometheus provider. [#1245](https://github.com/apache/bookkeeper/pull/1245)

### Operations

### BookieShell

There are are multiple new commands are added in BookieShell. Here are a few highlighted:

- `metaformat` is deprecated with two new commands `initnewcluster` and `nukeexistingcluster`. This separation provides better operability and reduces mistakes.
- `initbookie` command is introduced for initializing a new bookie. `bookieformat` keeps serving as the purpose of reformatting a bookie.

A new BookKeeper CLI is proposed in [BP-27](http://bookkeeper.apache.org/bps/BP-27-new-bookkeeper-cli). Some commands are already ported to new bookkeeper CLI.
The full list of shell commands will be fully ported to new bookkeeper CLI in next release.

### ReadOnly Mode Support

Operations are improved around readonly mode for handling bookkeeper outage situation. New settings are introduce allow entry log creation, high priority writes
even when bookies are readonly. See [Upgrade Guide](../../admin/upgrade) to learn all newly added settings.


### Builds & Testing

- [Arquillian](http://arquillian.org/) framework is introduced in 4.7.0 for backward compatibility and integration tests. 
- Both Java8 and Java9 are now supported for running bookkeeper.

## Full list of changes

- [https://github.com/apache/bookkeeper/milestone/3](https://github.com/apache/bookkeeper/milestone/3?closed=1)
