---
title: Apache BookKeeper 4.8.0 Release Notes
---

This is the 13th release of Apache BookKeeper!

The 4.8.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.7.0.
It is a new big milestone in Apache BookKeeper community,
this release include great new features, like Relaxed Durability, Stream Storage service and Multiple active Entrylogs.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.8.0](../../admin/upgrade). The technical details of
this release are summarized below.

## Highlights

The main features in 4.8.0 are around following areas:

- Durability
- ExplicitLAC feature
- New Table Storage Service
- Bug Fixes


### New WriteFlag DEFERRED_SYNC

The writer may ask for temporary relaxed durability writes, that is to receive early acknowledge from Bookies, before an fsync() on Journal.
Together with this new flag we introduced the new WriteHandle#force() API, this this API the writer is able to request an explicit guarantee of durability to the Bookies
it is mostly like and explicit fsync() on a file system.

See [`DEFERRED_SYNC`](../javadoc/org/apache/bookkeeper/client/api/WriteFlag) and [force()](../javadoc/org/apache/bookkeeper/client/api/ForceableHandle) for reference

### New behaviour for Netty ByteBuf reference count management

All the client side APIs which take ByteBufs now will have the net effect of decrementing by 1 the refcount.
This is consistent with general contract of Netty.
It is expected that the client passes the ownership of the ByteBuf to BookKeeper client.

### Multiple Active Entrylogs

It is now possible on the Bookie to have multiple active entry loggers,
this new feature will help with compaction performance and some specific workloads.

See [Multiple active entrylogs](https://github.com/apache/bookkeeper/issues/570)

### Table Storage Service

From this version we are providing the a table (key/value) service embedded in Bookies.
 
See [BP-30: BookKeeper Table Service](https://github.com/apache/bookkeeper/issues/1205)

### Make ExplicitLAC persistent

ExplicitLAC was contributed from Salesforce in 4.5.0 release, but in the first release
it was a beft-effort in-memory mechanism. Now you can configure Bookies to store durably ExplicitLAC.

See [Make ExplicitLAC persistent](https://github.com/apache/bookkeeper/issues/1527)

### Ensemble change on Delayed Write Failure

We are handling more gracefully the case of a failure of a Bookie in spite of a succeeded write.
If you are writing with Ack Quorum = 2 and Write Quorum = 3, writes will succeeed even if 1 of 3 Bookies fail,
now BookKeeper will trigger an *ensemble change* and replace the failed bookie earlier.

See [Ensemble change on Delayed Write Failure](https://github.com/apache/bookkeeper/issues/1390)

## Full list of changes

- [https://github.com/apache/bookkeeper/milestone/4](https://github.com/apache/bookkeeper/milestone/4?closed=1)
