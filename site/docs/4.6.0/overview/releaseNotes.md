---
title: Apache BookKeeper 4.6.0 Release Notes
---

This is the seventh release of BookKeeper as an Apache Top Level Project!

The 4.6.0 release incorporates new fixes, improvements, and features since previous major release 4.5.0.

Apache BookKeeper users are encouraged to upgrade to 4.6.0. The technical details of this release are summarized
below.

## Highlights

The main features in 4.6.0 cover are around following areas:

- Persistable bookie status
- BookKeeper Admin REST API
- New CreateLedger API
- lifecycle components for managing components in bookie server
- Introduce write FileInfo cache and read FileInfo cache
- Use ByteBuf for entrylogger reads
- Introduce Bookie Registration Manager for bookie server
- Make bookie recovery work with recovering multiple bookies
- Refine LedgerEntry interface and provide LedgerEntries interface

### Persistable bookie status

Prior to this release, bookie status was transient. It is a bit hard for management tooling. This feature adds persistable bookies status.

### BookKeeper Admin REST API

[to be continue]

## Full list of changes

- [https://github.com/apache/bookkeeper/milestone/2](https://github.com/apache/bookkeeper/milestone/2)