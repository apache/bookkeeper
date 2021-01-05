---
title: Apache BookKeeper 4.12.0 Release Notes
---

This is the 23rd release of Apache BookKeeper, it is a great milestone for the project, and we are introducing a few breaking changes on the API.
There are not changes on the wire protocol, on metadata and on persisted data on disks by default, so the new version is totally compatible with the previous ones.
With BookKeeper 4.12.0 we are making a step toward better deployment on environments with dynamic network addresses with BP-41.
We are also enhancing the new Client API by adding features that were still missing, like the ability of queryng for ledger metadata.

Apache BookKeeper users are encouraged to [upgrade to 4.12.0](../../admin/upgrade). The technical details of this release are summarized
below.

## News and noteworthy

- [https://github.com/apache/bookkeeper/pull/1901] Enable ExplicitLAC but default on the reader side and in the New org.apache.bookkeeper.client.api.ReadHandle API
- [https://github.com/apache/bookkeeper/issues/2396] BP-41 Bookie Network Address Change Tracking + BookieId
- [https://github.com/apache/bookkeeper/issues/2422] BP-42 List and Access LedgerMetadata on the new API
- [https://github.com/apache/bookkeeper/pull/2433] Support Java 11 and switch to Java 11 default Docker images
- [https://github.com/apache/bookkeeper/pull/2455] BP-40 clean up output for tools
- [https://github.com/apache/bookkeeper/pull/2429] Certificate role based authorization 

## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.12.0+
