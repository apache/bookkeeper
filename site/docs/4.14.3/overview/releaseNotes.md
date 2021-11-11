---
title: Apache BookKeeper 4.14.3 Release Notes
---

Release 4.14.3 includes multiple stability, performance, and security fixes along with the fix for Prometheus metrics.

Apache BookKeeper users are encouraged to upgrade to 4.14.3. 
The technical details of this release are summarized below.

## Highlights

### Improvements

- [https://github.com/apache/bookkeeper/pull/2768] Add metrics and internal command for AutoRecovery
- [https://github.com/apache/bookkeeper/pull/2788] Fix npe when pulsar ZkBookieRackAffinityMapping getBookieAddressResolver
- [https://github.com/apache/bookkeeper/pull/2794] Heap memory leak problem when ledger replication failed
- [https://github.com/apache/bookkeeper/pull/2802] Add auditor get ledger throttle to avoid auto recovery zk session expire
- [https://github.com/apache/bookkeeper/pull/2813] Add ensemble check to over-replicated ledger GC
- [https://github.com/apache/bookkeeper/pull/2832] Fix semaphore leak when EntryMemTable#addEntry accepts the same entries
- [https://github.com/apache/bookkeeper/pull/2833] Eliminate direct ZK access in ScanAndCompareGarbageCollector
- [https://github.com/apache/bookkeeper/pull/2842] Remove direct ZK access for Auditor
- [https://github.com/apache/bookkeeper/pull/2844] Add error handling to readLedgerMetadata in over-replicated ledger GC
- [https://github.com/apache/bookkeeper/pull/2845] A empty implmentation of newLedgerAuditorManager in EtcdLedgerManagerFactory to fix build

### Dependency updates

- [https://github.com/apache/bookkeeper/pull/2792] Upgraded dependencies with CVEs
- [https://github.com/apache/bookkeeper/pull/2793] Upgrade httpclient from 4.5.5 to 4.5.13 to address CVE-2020-13956
- [https://github.com/apache/bookkeeper/pull/2811] Upgrade Netty to 4.1.68.Final

## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.3
