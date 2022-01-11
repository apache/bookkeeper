---
title: Apache BookKeeper 4.14.4 Release Notes
---

Release 4.14.4 includes multiple stability, performance, and security fixes.

Apache BookKeeper users are encouraged to upgrade to 4.14.4. 
The technical details of this release are summarized below.

## Highlights

### Improvements

- [https://github.com/apache/bookkeeper/pull/2952] Allow to easily override zkServers with metadataServiceUri
- [https://github.com/apache/bookkeeper/pull/2935] ReplicationWorker should not try to create a ZK based LedgerManagerFactory
- [https://github.com/apache/bookkeeper/pull/2870] Add skip unrecoverable ledger option for bookkeeper shell recover command
- [https://github.com/apache/bookkeeper/pull/2847] ISSUE #2846 Allow to run on java 17

### Dependency updates

- [https://github.com/apache/bookkeeper/pull/2934] Upgrade to Grpc 1.42.1

## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.4
