---
title: Apache BookKeeper 4.6.0 Release Notes
---

This is the seventh release of BookKeeper as an Apache Top Level Project!

The 4.6.0 release incorporates new fixes, improvements, and features since previous major release 4.5.0.

Apache BookKeeper users are encouraged to upgrade to 4.6.0. The technical details of this release are summarized
below.

## Highlights

The main features in 4.6.0 cover are around following areas:
- Dependencies Upgrade
- Bookie enhancement
- BookKeeper Admin REST API
- New BookKeeper API
- Performance improvement
- Deployment or Ease of use 

### Dependencies Upgrade

- Upgrade Protobuf to `3.4`.

### Bookie enhancement

- Persistable bookie status.
  - Prior to this release, bookie status was transient. It is a bit hard for management tooling. This feature adds persistable bookies status. See [Issue-265](https://github.com/apache/bookkeeper/issues/265) for more details.

- Introduce Bookie Discovery Interface.  Prior to this release, bookkeeper client only provides interfaces for ledger metadata management. It doesn't provide any interface for service discovery part. This feature introduces bookie discovery interface, so it allows plugging in different service discovery backends for bookkeeper.
  - Introduce Bookie Registration Manager for bookie server, see [Issue-662](https://github.com/apache/bookkeeper/issues/662) for more details.
  - Introduce registration client for bookkeeper client, see [Issue-666](https://github.com/apache/bookkeeper/issues/666) for more details.

- Lifecycle components for managing components in bookie server.
  - Introduce lifecycle component for each service component, which includes "stats provider", "auto recovery", "http endpoint", and "bookie server(both storage and netty server)", to run these components in a clear way. See [Issue-508](https://github.com/apache/bookkeeper/issues/508) and [Issue-547](https://github.com/apache/bookkeeper/issues/547) for more details.

- Make bookie recovery work with recovering multiple bookies. 
  - Make recovery tool work with multiple bookies, so that one call could recover multiple bookies. See [Issue-612](https://github.com/apache/bookkeeper/issues/612) for more details.

### BookKeeper Admin REST API

- Introduce a bookkeeper admin endpoint for operations to interact and administer the bookkeeper cluster using REST API. see [PR-278](https://github.com/apache/bookkeeper/pull/278), [Issue-520](https://github.com/apache/bookkeeper/issues/520), and [Issue-674](https://github.com/apache/bookkeeper/issues/674) for more details.

### New BookKeeper API

- New Fluent Style API.
  - A brand new API to manage ledgers using the Builder pattern, and new interfaces to make it clear operations on ledgers, like WriteHandle and ReadHandle, are provided in this release. See [Issue-506](https://github.com/apache/bookkeeper/issues/506), [Issue-673](https://github.com/apache/bookkeeper/issues/673) and [Issue-550](https://github.com/apache/bookkeeper/issues/550) for more details

### Performance improvement
- Use ByteBuf in multiple places to avoid unnecessary memory allocation and reduce the garbage produced in JVM. See [PR-640](https://github.com/apache/bookkeeper/pull/640) for more details.

- Separate the FileInfo cache into write and read cache. It avoids catchup reads impact tailing reads and writes. See [PR-513](https://github.com/apache/bookkeeper/pull/513) for more details.

### Deployment or Ease of use
- Deployment BookKeeper on K8s. 
  - Provide yaml files to run BookKeeper on Kubernetes using both StatefulSets and DaemonSet. See [Issue-337](https://github.com/apache/bookkeeper/issues/337) and [Issue-681](https://github.com/apache/bookkeeper/issues/681)for more details.

## Existing API changes

- BookKeeper constructor now throws BKException instead of KeeperException.
- The signatures of `reorderReadSequence` and `reorderReadLACSequence` are changed in EnsemblePlacementPolicy.

## Full list of changes

- [https://github.com/apache/bookkeeper/milestone/2?closed=1](https://github.com/apache/bookkeeper/milestone/2?closed=1)