---
title: Apache BookKeeper 4.14.0 Release Notes
---

Release 4.14 adds FIPS compliance, improves compaction logic and the Stream Storage, 
improves data reliability in the recovery scenarios,
fixes multiple bugs and brings critical dependencies up-to-date.

Apache BookKeeper users are encouraged to upgrade to 4.14.0. 
The technical details of this release are summarized below.

## Highlights

### Bookkeeper is FIPS compliant by default now

- [https://github.com/apache/bookkeeper/pull/2631] Make Bookkeeper FIPS compliant by default

  FIPS is 'Federal Information Processing Standard'. 
  It's a set of guidelines for security functions such as encryption/decryption/RNG etc. 
  Applications running in FIPS mode are said to be more secure as they adhere to more stringent standards.

### Data reliability

- [https://github.com/apache/bookkeeper/pull/2616] Add fencing to recovery reads to avoid data loss issue

### Table Service (stream storage) reliability improvements

- [https://github.com/apache/bookkeeper/pull/2686] Improved handling of RocksDB tombstones
- [https://github.com/apache/bookkeeper/pull/2641] Checksum validation for SST files
- [https://github.com/apache/bookkeeper/pull/2635] Better handling of corrupted checkpoints
- [https://github.com/apache/bookkeeper/pull/2643] Adjusted default rocksDbBlockCache size to 10%/numberOfLedgers of direct memory
- [https://github.com/apache/bookkeeper/pull/2698] RocksDB log path is configurable now

### Compaction logic improvements

- [https://github.com/apache/bookkeeper/pull/2675] forceAllowCompaction to run only when force is set or configured interval
- [https://github.com/apache/bookkeeper/pull/2670] Allow a customer to set a limit on the duration of the major and minor compaction runs
- [https://github.com/apache/bookkeeper/pull/2645] Fix: The compaction status report is off by 1
- [https://github.com/apache/bookkeeper/pull/2626] Allow force compact entry log when entry log compaction is disabled
- [https://github.com/apache/bookkeeper/pull/2627] Allow DBLedgerStorage to force GC by disk listener

### Dependency updates

- [https://github.com/apache/bookkeeper/pull/2696] SECURITY: Upgraded Netty to 4.1.63.Final
- [https://github.com/apache/bookkeeper/pull/2701] SECURITY: Removed jackson-mapper-asl dependency to resolve multiple CVEs
- [https://github.com/apache/bookkeeper/pull/2697] Upgraded Lombok to 1.18.20 (required for Java 16 support)
- [https://github.com/apache/bookkeeper/pull/2686] Upgraded rocksdb to 6.16.4

### Other improvements and fixes

- [https://github.com/apache/bookkeeper/pull/2658] Fix: always select the same region set bug for RegionAwareEnsemblePlacementPolicy
- [https://github.com/apache/bookkeeper/pull/2650] Allow to attach labels to metrics
- [https://github.com/apache/bookkeeper/pull/2401] Allow to bypass journal for writes
- [https://github.com/apache/bookkeeper/pull/2710] Imposed a memory limit on the bookie journal
- [https://github.com/apache/bookkeeper/pull/2664] Bookkeeper client throttling logic is based upon entryId instead of ledgerId
- [https://github.com/apache/bookkeeper/pull/2694] Performance: unnecessary copy to heap from CompositeByteBuf
- [https://github.com/apache/bookkeeper/pull/2654] Ensure that only entries of the current ensemble are included in the ledger recovery process
- [https://github.com/apache/bookkeeper/pull/2646] Auto-throttle read operations
- [https://github.com/apache/bookkeeper/pull/2647] Limit read-ahead bytes to the size of the read cache
- [https://github.com/apache/bookkeeper/pull/2632] Fixed NetworkTopologyImpl#getLeaves returning set with null value in case of non existing scope

### Other

Documentation, build, CI, tests improvements

## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.0
