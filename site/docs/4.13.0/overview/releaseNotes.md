---
title: Apache BookKeeper 4.13.0 Release Notes
---

Release 4.13 improves reliability of the Stream Storage, 
brings additional configuration options for the Stream Storage and Prometheus HTTP Server, 
fixes multiple bugs and brings critical dependencies up-to-date.

Apache BookKeeper users are encouraged to upgrade to 4.13.0. The technical details of this release are summarized
below.

## Highlights

### Table Service (stream storage) reliability improvements

- [https://github.com/apache/pulsar/pull/9481] Rocksdb DLCheckpoint SST file corruption in statestore
- [https://github.com/apache/bookkeeper/pull/2564] Fix SST file corruption
- [https://github.com/apache/bookkeeper/pull/2566] Handling checkpoint corruption in case of bookie crash
- [https://github.com/apache/bookkeeper/issues/2567] Save latest revision information in statestore
- [https://github.com/apache/bookkeeper/pull/2568] Save last revision in rocksdb

### Other improvements

- [https://github.com/apache/bookkeeper/pull/2560] Allow stream storage to use hostname instead of IP address
- [https://github.com/apache/bookkeeper/pull/2597] Skip unavailable bookies during verifyLedgerFragment
- [https://github.com/apache/bookkeeper/pull/2543] Allow to configure Prometheus HTTP Server bind address
- various fixes of the tests, documentation, etc.

### Dependency updates

- [https://github.com/apache/bookkeeper/pull/2580] Upgrade protobuf to 3.14.0
- [https://github.com/apache/bookkeeper/pull/2582] Upgrading GRPC version to 1.33, Netty to 4.1.50Final and ETCD client driver
- [https://github.com/apache/bookkeeper/pull/2602] Upgrading dropwizard to 3.2.5

## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.13.0

