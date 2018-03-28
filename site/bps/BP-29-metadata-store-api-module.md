---
title: "BP-29: Metadata API module"
issue: https://github.com/apache/bookkeeper/<issue-number>
state: 'Accepted'
release: "N/A"
---

### Motivation

We have already abstracted all the metadata operations into interfaces. And all the bookkeeper implementations only reply on metadata interfaces,
rather than depending on zookeeper. This proposal is to organize the metadata interfaces and its implementations in a separate module and make
bookkeeper implementation only depends on metadata interfaces, not depends on zookeeper. This would a few benefits:

- It allows supporting different metadata storages, without bringing in dependencies of metadata store implementation directly into
  bookkeeper-server module. The development of different metadata storage can be done without interleaving with each other.
- It would define a clean module dependency between bookkeeper implementation and metadata api, and how bookkeeper load a different metadata
  implementation.

### Public Interfaces

A more generic setting `metadataServiceUri` is introduced for replacing implementation specific settings `zkServers` and `zkLedgersRootPath`.

A metadata service uri defines the location of a metadata storage. In zookeeper based implementation, the metadata service url will be
`zk://<zkServers>/<zkLedgersRootPath>`.

This new setting in bookie configuration will be like as below:

```
metadataServiceUri=zk://127.0.0.1/ledgers
```

If we eventually support Etcd as one of the metadata storages. Then the setting in bookie configuration to use Etcd will be:

```
metadataServiceUri=etcd://<etcd_servers>/<etcd_key_prefix>
```

### Proposed Changes

#### Configuration

This BP proposes introducing a more generic metadata setting `metadataServiceUri` to replace implementation specific settings
`zkServers` and `zkLedgersRootPath`. All implementation specific settings should be considered moving to implementation itself.

The `metadataServiceUri` can also be used for replacing the need of configuring `ledgerManagerFactoryClass`, `registrationClientClass` and
`registrationManagerClass`. It is unnecessarily complicated to configure multiple settings to load a specific metadata implementation.
We can just use the `scheme` field in `metadataServiceUri` to resolve which metadata implementation to use. Using uri to resolve
different driver or implementation is commonly seen at java world, for example, jdbc to support different database drivers. Also, distributedlog
uses this pattern to load different metadata driver.

So in zookeeper based metadata implementation, the metadata service uri can be:

- `zk+flat://127.0.0.1/ledgers`: the scheme is "zk+flat". it means a zookeeper base metadata implementation and it uses flat ledger manager.
- `zk+hierarchical://127.0.0.1/ledgers`: the scheme is "zk+hierarchical". it means a zookeeper base metadata implementation and it
  uses hierarchical ledger manager.
- `zk+longhierarchical://127.0.0.1/ledgers`: the scheme is "zk+longhierarchical". it means a zookeeper base metadata implementation and it
  uses long hierarchical ledger manager.

#### Metadata Stores

Introduce a new directory called `metadata-stores` for storing all the metadata related modules. Under this directory, it will have following modules:

- `api`: it is the metadata api module `metadata-store-api`. It contains all the files defining the metadata interfaces.
- `zookeeper`: it is the zookeeper implementation module `metadata-store-zookeeper`. It contains all the files that implementing the metadata interfaces
  using zookeeper.

If a new metadata implementation is added, a new directory will be created under `metadata-stores` to contain the implementation. For example, if we
are adding `Etcd` as the metadata store. A `Etcd` directory will be created under `metadata-stores/etcd` to store the files that implement metadata
interfaces using etcd client. And its module is named as `metadata-store-etcd`.

We then change bookkeeper-server to depend on `metadata-store-api` only.

This approach is same as other pluggable modules `stats-api` and `http-server`.

### Compatibility, Deprecation, and Migration Plan

No compatibility concern at this moment. New setting is introduced and old settings will still continue to work.
No immediate deprecation.
No migration is needed.

### Test Plan

This proposal is mostly around refactor. So existing test cases would cover this.

### Rejected Alternatives

N/A
