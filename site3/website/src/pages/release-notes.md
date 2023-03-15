<!-- markdown-link-check-disable -->
# Release notes

## 4.15.4

Release 4.15.4 includes multiple bug fixes and some dependencies CVE fixes.

Apache BookKeeper users are encouraged to upgrade to 4.15.4.
The technical details of this release are summarized below.

### Highlights

#### Bugs

* Fix QueueEntry recycle problem [PR #3747](https://github.com/apache/bookkeeper/pull/3747)
* Fix memory leak when the Bookie is in read only mode [PR #3746](https://github.com/apache/bookkeeper/pull/3746)
* Fix RegionAwareEnsemblePlacementPolicy.newEnsemble sometimes failed problem [PR #3725](https://github.com/apache/bookkeeper/pull/3725)
* Not wrap IOException twice form checkpoint [PR #3683](https://github.com/apache/bookkeeper/pull/3683)
* Make `jvm_memory_direct_bytes_used` metrics compatible with jdk8 [PR #3677](https://github.com/apache/bookkeeper/pull/3677)
* Fix memory leak when closeRecovered,failed on clearing newEnsemblesFromRecovery [PR #3672](https://github.com/apache/bookkeeper/pull/3672)
* Fix RegionAwareEnsemblePlacementPolicy update rack info problem [PR #3666](https://github.com/apache/bookkeeper/pull/3666)
* Exit bookkeeper shell correctly even if fails to run for some reason [PR #3663](https://github.com/apache/bookkeeper/pull/3663)
* Fix memory leak when operating ledger metadata [PR #3662](https://github.com/apache/bookkeeper/pull/3662)
* Check client if closed when complete callback [PR #3661](https://github.com/apache/bookkeeper/pull/3661)
* Fix issue where checkAllLedgers could get stuck when read throttling is enabled [PR #3655](https://github.com/apache/bookkeeper/pull/3655)
* Fix GetBookieInfo failed event stats [PR #3622](https://github.com/apache/bookkeeper/pull/3622)
* Apply recycle logic during add entry creation but ledger close to LedgerHandleAdv [PR #3621](https://github.com/apache/bookkeeper/pull/3621)
* BookieImpl remove wait until journal quits [PR #3603](https://github.com/apache/bookkeeper/pull/3603)

#### Improvements

* Support build in the aarch64 linux platform [PR #3828](https://github.com/apache/bookkeeper/pull/3828)
* Use ReferenceCountUtil.release() instead of ReferenceCountUtil.safeRelease() [PR #3797](https://github.com/apache/bookkeeper/pull/3797)
* Added api/v1/bookie/cluster_info REST API [PR #3713](https://github.com/apache/bookkeeper/pull/3713)
* Expose registrationClient in DefaultBookieAddressResolver [PR #3724](https://github.com/apache/bookkeeper/pull/3724)
* New ensemble choose different rack first [PR #3721](https://github.com/apache/bookkeeper/pull/3721)
* Improve bk_server.conf docs [PR #3715](https://github.com/apache/bookkeeper/pull/3715)
* Show result of isFenced in log [PR #3678](https://github.com/apache/bookkeeper/pull/3678)
* Include bkperf into bk all package [PR #3632](https://github.com/apache/bookkeeper/pull/3632)
* Add journal file path that caused failure in multi-journal config [PR #3623](https://github.com/apache/bookkeeper/pull/3623)
* Avoid extra buffer to prepend frame size [PR #3560](https://github.com/apache/bookkeeper/pull/3560)
* Using a separate thread pool to execute openWithMetadata [PR #3548](https://github.com/apache/bookkeeper/pull/3548)
* LedgerHandle: do not complete metadata operation on the ZookKeeper/Metadata callback thread [PR #3516](https://github.com/apache/bookkeeper/pull/3516)
* ledgerFragment check and results keep order [PR #3504](https://github.com/apache/bookkeeper/pull/3504)
* Simplified No network topology script is found default log stack output [PR #3496](https://github.com/apache/bookkeeper/pull/3496)
* Support apple m1 build [PR #3175](https://github.com/apache/bookkeeper/pull/3175)

#### Dependency updates

* Bump vertx-web from 4.3.2 to 4.3.8 [PR #3775](https://github.com/apache/bookkeeper/pull/3775)
* Upgrade docker image version to fix CVEs [PR #3640](https://github.com/apache/bookkeeper/pull/3640)
* Bump jcommander from 1.78 to 1.82 [PR #3476](https://github.com/apache/bookkeeper/pull/3476)

### Details
https://github.com/apache/bookkeeper/pulls?page=1&q=is%3Apr+label%3Arelease%2F4.15.4+is%3Aclosed

## 4.14.7

Release 4.14.7 includes multiple bug fixes.

Apache BookKeeper users are encouraged to upgrade to 4.14.7.
The technical details of this release are summarized below.

### Highlights

#### Bugs

* Revert PR#3653 and make delete entries batch size configurable [PR #3768](https://github.com/apache/bookkeeper/pull/3768)
* Fix memory leak when the Bookie is in read only mode [PR #3746](https://github.com/apache/bookkeeper/pull/3746)
* Fix QueueEntry recycle problem. [PR #3747](https://github.com/apache/bookkeeper/pull/3747)
* Expose registrationClient in DefaultBookieAddressResolver. [PR #3724](https://github.com/apache/bookkeeper/pull/3724)
* Added api/v1/bookie/cluster_info REST API [PR #3714](https://github.com/apache/bookkeeper/pull/3714)
* Check if channel closed before processing read request [PR #3486](https://github.com/apache/bookkeeper/pull/3486)
* Add missed call to onReadRequestFinish() when read request rejected [PR #3482](https://github.com/apache/bookkeeper/pull/3482)
* Avoid extra buffer to prepend frame size [PR #3560](https://github.com/apache/bookkeeper/pull/3560)

### Details
https://github.com/apache/bookkeeper/pulls?q=is%3Apr+label%3Arelease%2F4.14.7+is%3Aclosed

## 4.14.6

Release 4.14.6 includes multiple bug fixes and some dependencies CVE fixes.

Apache BookKeeper users are encouraged to upgrade to 4.14.6.
The technical details of this release are summarized below.

### Highlights

#### Bugs

* Fix memory leak when reading entry but the connection disconnected. [PR #3528](https://github.com/apache/bookkeeper/pull/3528)
* When call openLedgerOp, make the timeout ex is a separate error code [PR #3562](https://github.com/apache/bookkeeper/pull/3562)
* Apply recycle logic during add entry creation but ledger close to LedgerHandleAdv [PR #3621](https://github.com/apache/bookkeeper/pull/3621)
* Fix autoRecovery memory leak. [PR #3361](https://github.com/apache/bookkeeper/pull/3361)
* Fix potential memory leak. [PR #3530](https://github.com/apache/bookkeeper/pull/3530)
* Fix the V2 AddRequest object leak issue [PR #3323](https://github.com/apache/bookkeeper/pull/3323)
* Fix ByteBuf memory leak problem when setExplicitLac [PR #3617](https://github.com/apache/bookkeeper/pull/3617)
* Fix the problem that the abnormal file causes the bookie GC fail [PR #3611](https://github.com/apache/bookkeeper/pull/3611)
* Fix the deadlock when only using io thread to handle request [PR #3480](https://github.com/apache/bookkeeper/pull/3480)
* Fix memory leak when closeRecovered,failed on clearing newEnsemblesFromRecovery [PR #3672](https://github.com/apache/bookkeeper/pull/3672)
* Fix memory leak when operating ledger metadata [PR #3662](https://github.com/apache/bookkeeper/pull/3662)
* LedgerHandle: do not complete metadata operation on the ZookKeeper/Metadata callback thread [PR #3516](https://github.com/apache/bookkeeper/pull/3516)
* Ledger replicate supports throttle [PR #2778](https://github.com/apache/bookkeeper/pull/2778)
* CheckAllLedgers in Auditor supports read throttle [PR #2973](https://github.com/apache/bookkeeper/pull/2973)
* Rename success with writableResult and update final writableResult about wait writeSet [PR #3505](https://github.com/apache/bookkeeper/pull/3505)
* LedgerFragment check and results keep order [PR #3504](https://github.com/apache/bookkeeper/pull/3504)
* Improve the throttle function [PR #2991](https://github.com/apache/bookkeeper/pull/2991)
* Fix close ledgerAuditorManager repeatedly [PR #3503](https://github.com/apache/bookkeeper/pull/3503)
* AutoRecovery - Do not call shutdown() on the main ZookKeeper client thread [PR #3487](https://github.com/apache/bookkeeper/pull/3487)
* WriteLacResponse should be processed in the same thread as other requests for the same ledgerId [PR #3452](https://github.com/apache/bookkeeper/pull/3452)
* Shutdown ReplicationWorker and Auditor on non-recoverable ZK error [PR #3374](https://github.com/apache/bookkeeper/pull/3374)
* Enhance future sync wait. [PR #3336](https://github.com/apache/bookkeeper/pull/3336)
* Pre break loop when self create layoutZNode succeed. [PR #3335](https://github.com/apache/bookkeeper/pull/3335)
* Fix the PendingAddOp is not recycled when LedgerHandler closed [PR #3321](https://github.com/apache/bookkeeper/pull/3321)
* Fix autorecovery does not process underreplicated empty ledgers [PR #3239](https://github.com/apache/bookkeeper/pull/3239)
* Fix RegionAwareEnsemblePlacementPolicy update rack info problem. [PR #3666](https://github.com/apache/bookkeeper/pull/3666)
* Fix bug where checkAllLedgers gets stuck when read throttling is enabled [PR #3214](https://github.com/apache/bookkeeper/pull/3214)
* Fix checkAllLedgersDuration compute [PR #2970](https://github.com/apache/bookkeeper/pull/2970)
* Zk client config update and bugfix for ZKMetadataClientDriver [PR #2958](https://github.com/apache/bookkeeper/pull/2958)
* Make sure the LedgerHandle close callback can be completed when encounter exception [PR #2913](https://github.com/apache/bookkeeper/pull/2913)
* Make `jvm_memory_direct_bytes_used` metrics compatible with jdk8. [PR #3677](https://github.com/apache/bookkeeper/pull/3677)
* Reorder the sequence of the bookkeeper server shutdown so that there are no read/ write ops while shutting down the bookie [PR #2888](https://github.com/apache/bookkeeper/pull/2888)
* Fix readlogmetadata failed bug and export entrylog file usage to output [PR #2349](https://github.com/apache/bookkeeper/pull/2349)
* MinorCompactionInterval should be greater than gcWaitTime [PR #2116](https://github.com/apache/bookkeeper/pull/2116)
* Skipping placementPolicyCheck when ledger replication disabled [PR #3561](https://github.com/apache/bookkeeper/pull/3561)
* Skip replicasCheck when replication disabled [PR #3563](https://github.com/apache/bookkeeper/pull/3563)
* Fix GetBookieInfo failed event stats [PR #3622](https://github.com/apache/bookkeeper/pull/3622)
* Add journal file path that caused failure in multi-journal config [PR #3634](https://github.com/apache/bookkeeper/pull/3634)
* Include bkperf into bk all package [PR #3632](https://github.com/apache/bookkeeper/pull/3632)
* Fix maven javadoc generate issues [PR #3615](https://github.com/apache/bookkeeper/pull/3615)
* Switch to rely on SslEngine for Hostname Verification [PR #3310](https://github.com/apache/bookkeeper/pull/3310)
* Simplified No network topology script is found default log stack output [PR #3496](https://github.com/apache/bookkeeper/pull/3496)
* Fix jvm_memory_direct_bytes_used metrics when using jdk11+ [PR #3252](https://github.com/apache/bookkeeper/pull/3252)
* Fix the 3144 revert issue [PR #3283](https://github.com/apache/bookkeeper/pull/3283)
* Avoid init WriteSet when waitForWriteSetMs < 0. [PR #3325](https://github.com/apache/bookkeeper/pull/3325)
* Tuning PendingReadOp.java seq [PR #3330](https://github.com/apache/bookkeeper/pull/3330)
* Optimize log for failed to write entry [PR #3463](https://github.com/apache/bookkeeper/pull/3463)
* Reduce unnecessary loop in removeIf if map is empty [PR #3512](https://github.com/apache/bookkeeper/pull/3512)
* Deduplicate error log for SSLException [PR #3320](https://github.com/apache/bookkeeper/pull/3320)
* Fix underReplicatedLedgerTotalSize calculate problem. [PR #3337](https://github.com/apache/bookkeeper/pull/3337)
* Fix wrong ledger id parse radix for index relocation file in IndexPersistenceMgr [PR #2944](https://github.com/apache/bookkeeper/pull/2944)
* Fix bookie CI test not run [PR #3367](https://github.com/apache/bookkeeper/pull/3367)
* Update doc about flushInterval config [PR #3601](https://github.com/apache/bookkeeper/pull/3601)
* Show content of map [PR #3538](https://github.com/apache/bookkeeper/pull/3538)
* Add example for multiple server list in metadataServiceUri configuration [PR #3580](https://github.com/apache/bookkeeper/pull/3580)
* Replace sleep with await to avoid flaky test in SlowBookieTest [PR #3581](https://github.com/apache/bookkeeper/pull/3581)
* Add javadoc comments for test classes [PR #3587](https://github.com/apache/bookkeeper/pull/3587)
* Correct link class name [PR #3594](https://github.com/apache/bookkeeper/pull/3594)
* Fix typo in bk_server.conf [PR #3574](https://github.com/apache/bookkeeper/pull/3574)
* Fix flaky test testAutoRecoverySessionLoss [PR #3576](https://github.com/apache/bookkeeper/pull/3576)
* Fix flaky tests in AuditorReplicasCheckTest [PR #3551](https://github.com/apache/bookkeeper/pull/3551)
* Fix the tls failed test [PR #3448](https://github.com/apache/bookkeeper/pull/3448)
* Backport BookieBackpressureForV2Test to branch-4.14 [PR #3443](https://github.com/apache/bookkeeper/pull/3443)
* Fix Flaky-test: testBookieContinueWritingIfMultipleLedgersPresent [PR #3421](https://github.com/apache/bookkeeper/pull/3421)
* BookieAutoRecoveryTest.testEmptyLedgerLosesQuorumEventually fix flaky test, ensure that the Auditor is alive [PR #3149](https://github.com/apache/bookkeeper/pull/3149)
* Close journal channel in testJunkEndedJournal [PR #3307](https://github.com/apache/bookkeeper/pull/3307)
* Fix stream storage flaky tests, statelib test times out [PR #2883](https://github.com/apache/bookkeeper/pull/2883)
* Fix flaky AutoRecoveryMainTest [PR #2881](https://github.com/apache/bookkeeper/pull/2881)
* Support apple m1 build [PR #3175](https://github.com/apache/bookkeeper/pull/3175)
* Refactor ByteBuf release method in module distributedlog-core/distributedlog-protocol [PR #3693](https://github.com/apache/bookkeeper/pull/3693)
* Refactor ByteBuf release method in module distributedlog-core [PR #3691](https://github.com/apache/bookkeeper/pull/3691)
* Refactor ByteBuf release method in  tools [PR #3687](https://github.com/apache/bookkeeper/pull/3687)
* Refactor ByteBuf release method in stream/statelib [PR #3689](https://github.com/apache/bookkeeper/pull/3689)
* Refactor ByteBuf release method in DefaultEntryLogger [PR #3673](https://github.com/apache/bookkeeper/pull/3673)
* Refactor ByteBuf release method in InterleavedLedgerStorage [PR #3674](https://github.com/apache/bookkeeper/pull/3674)

#### Improvements

* Bring back deleteRange for RocksDB to improve location delete performance [PR #3653](https://github.com/apache/bookkeeper/pull/3653)
* Consolidate Netty channel flushes to mitigate syscall overhead [PR #3383](https://github.com/apache/bookkeeper/pull/3383)
* Using a separate thread pool to execute openWithMetadata [PR #3548](https://github.com/apache/bookkeeper/pull/3548)
* Replace unsafe NoEntryException with IOException [PR #2909](https://github.com/apache/bookkeeper/pull/2909)
* Update default value of allocatorPoolingConcurrency [PR #3001](https://github.com/apache/bookkeeper/pull/3001)
* Apply the backpressure changes on the V2 requests [PR #3324](https://github.com/apache/bookkeeper/pull/3324)
* LedgerOpenOp: Do not call blocking close() in the callback [PR #3513](https://github.com/apache/bookkeeper/pull/3513)

#### Metrics changes

* Add rocksDB read latency and read from storage latency for entry reading [PR #3647](https://github.com/apache/bookkeeper/pull/3647)
* Add latency stats for entry location index lookup so that possible RocksDB bottleneck can be detected [PR #3444](https://github.com/apache/bookkeeper/pull/3444)
* Add stats for throttled-write [PR #3102](https://github.com/apache/bookkeeper/pull/3102)
* Add writeThreadQueuedLatency [PR #3363](https://github.com/apache/bookkeeper/pull/3363)

#### Dependency updates

* Upgrade docker image version to fix CVEs [PR #3640](https://github.com/apache/bookkeeper/pull/3640)
* Upgrade dependencies for CVE-2022-3171 and CVE-2022-42003 [PR #3579](https://github.com/apache/bookkeeper/pull/3579)
* Bump jcommander from 1.78 to 1.82 [PR #3476](https://github.com/apache/bookkeeper/pull/3476)
* Upgrade hadoop version to 3.2.4 [PR #3485](https://github.com/apache/bookkeeper/pull/3485)
* Upgrade Jetty to 9.4.48.v20220622 to get rid of CVE-2022-2047 [PR #3404](https://github.com/apache/bookkeeper/pull/3404)

### Details
https://github.com/apache/bookkeeper/pulls?q=is%3Apr+label%3Arelease%2F4.14.6+is%3Aclosed

## 4.15.3

Release 4.15.3 includes multiple bug fixes and some dependencies CVE fixes.

Apache BookKeeper users are encouraged to upgrade to 4.15.3.
The technical details of this release are summarized below.

### Notice

* 4.15.0 introduced a breaking change in the RocksDB configuration in 4.15.0. Release 4.15.3 allows users to safely upgrade from 4.14.x without losing the RocksDB runtime configuration
  For more details, refer to https://lists.apache.org/thread/drh4p5prxbcs8gszhxnd1xsv0g48vvbt
  See [PR #3523](https://github.com/apache/bookkeeper/pull/3523)

* Timeout exceptions are now handled in a better way during reads.
  See [PR #3562](https://github.com/apache/bookkeeper/pull/3562)

### Highlights

#### Dependency updates

* Upgrade dependencies for CVE-2022-3171 and CVE-2022-42003. [PR #3579](https://github.com/apache/bookkeeper/pull/3579)
* Bump jackson version to 2.13.4 . [PR #3518](https://github.com/apache/bookkeeper/pull/3518)

#### Bugs

* Fix ByteBuf memory leak problem when setExplicitLac. [PR #3557](https://github.com/apache/bookkeeper/pull/3577)
* Flush time started moved to after lock. [PR #3570](https://github.com/apache/bookkeeper/pull/3570)
* Skip replicasCheck when replication disabled. [PR #3563](https://github.com/apache/bookkeeper/pull/3563)
* Skipping placementPolicyCheck when ledger replication disabled. [PR #3561](https://github.com/apache/bookkeeper/pull/3561)
* Fix the deadlock when only using io thread to handle request. [PR #3480](https://github.com/apache/bookkeeper/pull/3480)
* Fix memory leak when reading entry but the connection disconnected. [PR #3528](https://github.com/apache/bookkeeper/pull/3528)
* Fix byteBuf potential memory leak problem. [PR #3525](https://github.com/apache/bookkeeper/pull/3525)
* LedgerOpenOp: Do not call blocking close() in the callback. [PR #3513](https://github.com/apache/bookkeeper/pull/3513)
* Rename success with writableResult and update final writableResult about wait writeSet. [PR #3505](https://github.com/apache/bookkeeper/pull/3505)
* Avoid closing the ledgerAuditorManager twice in the close method. [PR #3503](https://github.com/apache/bookkeeper/pull/3503)

#### Improvements

* When call openLedgerOp, make the timeout ex is a separate error code. [PR #3562](https://github.com/apache/bookkeeper/pull/3562)
* Make the rocksDB configuration compatible with previous versions. [PR #3523](https://github.com/apache/bookkeeper/pull/3523)
* Reduce unnecessary loop in removeIf if map is empty. [PR #3512](https://github.com/apache/bookkeeper/pull/3512)
* Update default value of allocatorPoolingConcurrency. [PR #3001](https://github.com/apache/bookkeeper/pull/3001)


## 4.15.2

Release 4.15.2 contains various bug fixes and some dependencies CVE fixes.

Apache BookKeeper users are encouraged to upgrade to 4.15.2.
The technical details of this release are summarized below.

### Highlights

#### Dependency updates

* Bump snakeyaml from 1.31 to 1.32 to solve CVE-2022-38752. [PR #3491](https://github.com/apache/bookkeeper/pull/3491)
* upgrade hadoop version to 3.2.4. [PR #3485](https://github.com/apache/bookkeeper/pull/3485)
* [security] Upgrade Jetty to 9.4.48.v20220622 to get rid of CVE-2022-2047. [PR #3404](https://github.com/apache/bookkeeper/pull/3404)
* Bump dependency check to 7.1.2 to avoid FP. [PR #3470](https://github.com/apache/bookkeeper/pull/3470)
* Bump snakeyaml from 1.30 to 1.31 to solve CVE-2022-25857. [PR #3469](https://github.com/apache/bookkeeper/pull/3469)

#### Bugs

* AutoRecovery - Do not call shutdown() on the main ZookKeeper client thread. [PR #3487](https://github.com/apache/bookkeeper/pull/3487)
* Check if channel closed before processing read request. [PR #3486](https://github.com/apache/bookkeeper/pull/3486)
* Add missed call to onReadRequestFinish() when read request rejected. [PR #3482](https://github.com/apache/bookkeeper/pull/3482)
* Fix the deadlock when only using io thread to handle request. [PR #3480](https://github.com/apache/bookkeeper/pull/3480)

#### Metrics changes

* Add latency stats for entry location index lookup so that possible RocksDB bottleneck can be detected. [PR #3444](https://github.com/apache/bookkeeper/pull/3444)

## 4.15.1

Release 4.15.1 contains performance and stability improvements, new metrics, and various bug fixes.

Apache BookKeeper users are encouraged to upgrade to 4.15.1.
The technical details of this release are summarized below.

### Highlights

#### Bugs

* Fix AutoRecovery memory leak. [PR #3361](https://github.com/apache/bookkeeper/pull/3361)
* Fix: NPE in RackawareEnsemblePlacementPolicyImpl logged by AutoRecovery. [PR #3350](https://github.com/apache/bookkeeper/pull/3350)
* Fix underReplicatedLedgerTotalSize calculate problem. [PR #3337](https://github.com/apache/bookkeeper/pull/3337)
* Fix JVM exited when running localbookie with jdk17. [PR #3334](https://github.com/apache/bookkeeper/pull/3334)
* Fix the V2 AddRequest object leak issue. [PR #3323](https://github.com/apache/bookkeeper/pull/3323)
* Fix the PendingAddOp is not recycled when LedgerHandler closed. [PR #3321](https://github.com/apache/bookkeeper/pull/3321)
* Bookie can't start after rebooting due the cookie mistmatch. [PR #3308](https://github.com/apache/bookkeeper/pull/3308)
* Close journal channel in testJunkEndedJournal. [PR #3307](https://github.com/apache/bookkeeper/pull/3307)
* Fix jvm_memory_direct_bytes_used metrics when using jdk11+. [PR #3252](https://github.com/apache/bookkeeper/pull/3252)
* AutoRecovery does not process underreplicated empty ledgers. [PR #3239](https://github.com/apache/bookkeeper/pull/3239)
* Fix wrong ledger id parse radix for index relocation file in IndexPersistenceMgr. [PR #2944](https://github.com/apache/bookkeeper/pull/2944)
* Fix the infinite waiting for shutdown due to throttler limit. [PR #2942](https://github.com/apache/bookkeeper/pull/2942)
* Make sure the LedgerHandle close callback can be completed when encounter exception. [PR #2913](https://github.com/apache/bookkeeper/pull/2913)

#### Improvements

* If ensembleList is empty, return PlacementPolicyAdherence.FAIL. [PR #3369](https://github.com/apache/bookkeeper/pull/3369)
* reduce unnecessary checkpoints. [PR #3341](https://github.com/apache/bookkeeper/pull/3341)
* Pre break loop when self create layoutZNode succeed. [PR #3335](https://github.com/apache/bookkeeper/pull/3335)
* Avoid init WriteSet when waitForWriteSetMs < 0. [PR #3325](https://github.com/apache/bookkeeper/pull/3325)
* Tuning PendingReadOp.java seq. [PR #3330](https://github.com/apache/bookkeeper/pull/3330)
* Apply the backpressure changes on the V2 requests. [PR #3324](https://github.com/apache/bookkeeper/pull/3324)
* Deduplicate error log for SSLException. [PR #3320](https://github.com/apache/bookkeeper/pull/3320)
* Switch to rely on SslEngine for Hostname Verification. [PR #3310](https://github.com/apache/bookkeeper/pull/3310)
* Try to use Java9 CRC32C when JNI based CRC is not available. [PR #3309](https://github.com/apache/bookkeeper/pull/3309)
* Enhance future sync wait. [PR #3336](https://github.com/apache/bookkeeper/pull/3336)
* validate diskUsageThreshold and diskUsageLwmThreshold. [PR #3285](https://github.com/apache/bookkeeper/pull/3285)
* Replace unsafe NoEntryException with IOException. [PR #2909](https://github.com/apache/bookkeeper/pull/2909)
* let bookie quit if journal thread exit. [PR #2887](https://github.com/apache/bookkeeper/pull/2887)
* Ledger replicate supports throttle. [PR #2778](https://github.com/apache/bookkeeper/pull/2778)
* minorCompactionInterval should be greater than gcWaitTime. [PR #2116](https://github.com/apache/bookkeeper/pull/2116)
* Optimize concurrent collection's shrink logic. [PR #3417](https://github.com/apache/bookkeeper/pull/3417)
* Make BookieFileChannel interface public. [PR #3396](https://github.com/apache/bookkeeper/pull/3396)
* Prioritize compaction of entry logs with the lowest amount of remaining usable data. [PR #3390](https://github.com/apache/bookkeeper/pull/3390)
* Switch back ordered executor to LinkedBlockingQueue. [PR #3384](https://github.com/apache/bookkeeper/pull/3384)
* Consolidate Netty channel flushes to mitigate syscall overhead. [PR #3383](https://github.com/apache/bookkeeper/pull/3383)
* Shut down ReplicationWorker and Auditor on non-recoverable ZK error. [PR #3374](https://github.com/apache/bookkeeper/pull/3374)
* BP-41 Add flag to enable/disable BookieAddressResolver. [PR #3356](https://github.com/apache/bookkeeper/pull/3356)
* release the bookie from QuarantinedBookies when health check is disabled. [PR #3349](https://github.com/apache/bookkeeper/pull/3349)

#### Metrics changes

* add journalCbQueueLatency. [PR #3364](https://github.com/apache/bookkeeper/pull/3364)
* add writeThreadQueuedLatency. [PR #3363](https://github.com/apache/bookkeeper/pull/3363)
* add metric cbThreadPoolQueueSize. [PR #3424](https://github.com/apache/bookkeeper/pull/3424)

#### Dependency updates

* Upgrade log4j2 to 2.18.0. [PR #3434](https://github.com/apache/bookkeeper/pull/3434)
* upgrade groovy from 2.5.17 to 3.0.11 to fix CVE-2019-11358(7.5). [PR #3346](https://github.com/apache/bookkeeper/pull/3346)

### Details

https://github.com/apache/bookkeeper/pulls?q=is%3Apr+label%3Arelease%2F4.15.1+is%3Aclosed+is%3Amerged

## 4.15.0

Release 4.15 includes many upgrades to third party libraries marked with CVEs,
adds more configuration options, extends REST API,
adds an option to run without journal, improves memory utilization and stability, and more!

Apache BookKeeper users are encouraged to upgrade to 4.15.0. The technical details of this release are summarized
below.

### Breaking Changes

* `BookieServer` API changed and the code that creates its instances will require addition
  of the `UncleanShutdownDetection` parameter.
  See [PR 2936](https://github.com/apache/bookkeeper/pull/2936) for details and examples.

* `Bookie` class now is an interface with implementation in `BookieImpl`.
  Code that uses it may need changes.
  For details please refer to [PR 2717](https://github.com/apache/bookkeeper/pull/2717).

* `LedgerUnderreplicationManager` interface added a new method.
  Code that implements the interface will need changes.
  See [PR 2805](https://github.com/apache/bookkeeper/pull/2805) for details.

* `MetadataBookieDriver` interface added a new method and removed an old one.
  `RegistrationManager` interface added a new method.
  `ByteBufAllocatorWithOomHandler` interface is added and used instead of
  the `ByteBufAllocator` in multiple places.
  Code that implements the interfaces will need changes.
  See [PR 2901](https://github.com/apache/bookkeeper/pull/2901) for details.

* RocksDB configuration moves to the independent configuration files.
  We used to place the RocksDB configuration properties in the `bk_server.conf`. Now it moved to the independent files
  `entry_location_rocksdb.conf` and `ledger_metadata_rocksdb.conf`. 
  The existing configuration in the `bk_server.conf` for the RocksDB will invalidate.
  See [PR 3056](https://github.com/apache/bookkeeper/pull/3056) for details.

### Highlights

#### Configuration

* [ledgerMetadataVersion](https://github.com/apache/bookkeeper/pull/2708):
  BookKeeper-Client config to write ledger metadata with configured version.
* [clientTcpUserTimeoutMillis](https://github.com/apache/bookkeeper/pull/2761):
  Added TCP_USER_TIMEOUT to Epoll channel config.
* [auditorMaxNumberOfConcurrentOpenLedgerOperations and auditorAcquireConcurrentOpenLedgerOperationsTimeOutMSec](https://github.com/apache/bookkeeper/pull/2802)
  Add auditor get ledger throttle to avoid auto recovery zk session.
* [dbStorage_rocksDB_format_version](https://github.com/apache/bookkeeper/pull/2824)
  make rocksdb format version configurable.

#### Features

* Running without journal. See [BP-46](https://github.com/apache/bookkeeper/pull/2706) for details.
* A REST API to get or update bookie readOnly state. [Details](https://github.com/apache/bookkeeper/pull/2799)
* Separate config files for Rocks DB. [Details](https://github.com/apache/bookkeeper/pull/3056/)

#### Improvements

* Build and tests work on JDK 17
* CLI: listunderreplicated command has an option to return count without printing all ledgers https://github.com/apache/bookkeeper/pull/3228
* Stream Storage: support an optional time to live (TTL) on a per table basis https://github.com/apache/bookkeeper/pull/2775
* Added dDb ledger index rebuild operation and CLI commands https://github.com/apache/bookkeeper/pull/2774
* Support multi ledger directories for rocksdb backend entryMetadataMap https://github.com/apache/bookkeeper/pull/2965
* Improved memory utilization
  * support shrink for ConcurrentLong map or set https://github.com/apache/bookkeeper/pull/3074
  * reduce unnecessary expansions for ConcurrentLong map and set https://github.com/apache/bookkeeper/pull/3072
* read speed rate limiter for scanning entry log file in entryMetadataMap rebuild https://github.com/apache/bookkeeper/pull/2963
* Other improvements in areas such as test, documentation, CI, metrics, logging, and CLI tools.

#### Notable fixes

* Bookkeeper client might not close the channel for several minutes after a Bookie crashes https://github.com/apache/bookkeeper/issues/2482
* Stream storage: Ensure progress while restoring from checkpoint. https://github.com/apache/bookkeeper/pull/2764
* Entry Log GC may get blocked when using entryLogPerLedgerEnabled option https://github.com/apache/bookkeeper/pull/2779
* Fix region aware placement policy use disk weight https://github.com/apache/bookkeeper/pull/2981
* Some cases that could cause RocksDB segfault
* DistributedLogManager can skip over a segment on read. https://github.com/apache/bookkeeper/pull/3064
* Backpressure: check all bookies of writeset are writable https://github.com/apache/bookkeeper/pull/3055
* Fix Journal.ForceWriteThread.forceWriteRequests.put deadlock https://github.com/apache/bookkeeper/pull/2962
* PendingReadOp: Fix ledgerEntryImpl reuse problem https://github.com/apache/bookkeeper/pull/3110
* Region/rack aware placement policy: replace bookie bug https://github.com/apache/bookkeeper/pull/2642
* ReplicationWorker: numLedgersReplicated metric does not update https://github.com/apache/bookkeeper/pull/3218
* Force GC doesn't work under forceAllowCompaction when disk is full https://github.com/apache/bookkeeper/pull/3205

#### Dependencies changes

Upgraded dependencies to address CVEs include:
* vertx
* freebuilder
* libthrift
* netty
* bouncycastle
* commonsIO
* jetty
* log4j
* grpc
* protobuf
* snakeyaml
* RocksDB
* jackson
* jackson-databind
* Zookeeper
* http-core
* dropwizard metrics

Dependency on log4j v.1 is removed.

### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.15.0

## 4.14.5

Release 4.14.5 includes multiple stability, performance, and security fixes.

Apache BookKeeper users are encouraged to upgrade to 4.14.5.
The technical details of this release are summarized below.

### Highlights

#### Bugs

- [https://github.com/apache/bookkeeper/pull/3110] PendingReadOp: Fix ledgerEntryImpl reuse problem
- [https://github.com/apache/bookkeeper/pull/3060] Catch onBookieRackChange exception
- [https://github.com/apache/bookkeeper/pull/2981] Fix region aware placement policy use disk weight not work
- [https://github.com/apache/bookkeeper/pull/2642] fix region/rack aware placement police replace bookie bug
- [https://github.com/apache/bookkeeper/pull/3011] Auditor should get the LegdgerManagerFactory from the client instance


#### Improvements

- [https://github.com/apache/bookkeeper/pull/3144] Revert rocksdb compaction on checkpoint to reduce cpu intensive
- [https://github.com/apache/bookkeeper/pull/3117] Log NoLedgerException at debug level
- [https://github.com/apache/bookkeeper/pull/2799] Add a REST API to get or update bookie readOnly state
- [https://github.com/apache/bookkeeper/pull/2790] only update topology when bookie rack changed
- [https://github.com/apache/bookkeeper/pull/2769] Support specifying bookie http port as a command argument
- [https://github.com/apache/bookkeeper/pull/2757] Change log level from error to warn in getReadLacResponse
- [https://github.com/apache/bookkeeper/pull/3096] Set BOOKIE_HTTP_PORT to make it optional in docker run

#### Dependency updates

- [https://github.com/apache/bookkeeper/pull/3167] Replace Log4J with Reload4J


### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.5

## 4.14.4

Release 4.14.4 includes multiple stability, performance, and security fixes.

Apache BookKeeper users are encouraged to upgrade to 4.14.4. 
The technical details of this release are summarized below.

### Highlights

#### Improvements

- [https://github.com/apache/bookkeeper/pull/2952] Allow to easily override zkServers with metadataServiceUri
- [https://github.com/apache/bookkeeper/pull/2935] ReplicationWorker should not try to create a ZK based LedgerManagerFactory
- [https://github.com/apache/bookkeeper/pull/2870] Add skip unrecoverable ledger option for bookkeeper shell recover command
- [https://github.com/apache/bookkeeper/pull/2847] ISSUE #2846 Allow to run on java 17

#### Dependency updates

- [https://github.com/apache/bookkeeper/pull/2934] Upgrade to Grpc 1.42.1

### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.4

## 4.14.3

Release 4.14.3 includes multiple stability, performance, and security fixes along with the fix for Prometheus metrics.

Apache BookKeeper users are encouraged to upgrade to 4.14.3. 
The technical details of this release are summarized below.

### Highlights

#### Improvements

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

#### Dependency updates

- [https://github.com/apache/bookkeeper/pull/2792] Upgraded dependencies with CVEs
- [https://github.com/apache/bookkeeper/pull/2793] Upgrade httpclient from 4.5.5 to 4.5.13 to address CVE-2020-13956
- [https://github.com/apache/bookkeeper/pull/2811] Upgrade Netty to 4.1.68.Final

### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.3

## 4.14.2

Release 4.14.2 fixes an issue with Prometheus metrics that was
found in 4.14.0.

Apache BookKeeper users are encouraged to upgrade to 4.14.2. 
The technical details of this release are summarized below.

#### Bugs

- [https://github.com/apache/bookkeeper/pull/2740] Fix Bouncy Castle fips incompatible issue

  In #2631, the default BouncyCastle was changed from non-fips into fips version. But the default version of BouncyCastle in Pulsar is the non-fips one(aimed to make it compatible with the old version of Pulsar).

  Bouncy Castle provides both FIPS and non-FIPS versions, but in a JVM, it can not include both of the 2 versions(non-Fips and Fips), and we have to exclude the current version before including the other. This makes the backward compatible a little hard, and that's why Pulsar has to involve an individual module for Bouncy Castle.

- [https://github.com/apache/bookkeeper/pull/2762] Upgrade libthrift to 0.14.2 to address multiple CVEs

  The current libthrift version 0.12.0 has multiple vulnerabilities: CVE-2019-0205 , CVE-2019-0210 , CVE-2020-13949

- [https://github.com/apache/bookkeeper/pull/2735] Exclude grpc-okhttp dependency

  The okhttp dependency version 2.7.4 is old and vulnerable. This dependency isn't needed and it causes Bookkeeper to be flagged for security vulnerabilities.

- [https://github.com/apache/bookkeeper/pull/2734] Upgrade Freebuilder version and fix the dependency

  - Freebuilder 1.14.9 contains an outdate jquery js file which causes the library to be flagged as vulnerable with the highest threat level in Sonatype IQ vulnerability scanner. This also flags Bookkeeper as vulnerable with the highest threat level although it is a false positive and not an actual threat.

  - Freebuilder shouldn't be exposed as a transitive dependency
    - it's an annotation processor which should be defined
      - [optional in maven](https://github.com/inferred/FreeBuilder#maven)
      - [compileOnly in gradle](https://github.com/inferred/FreeBuilder#gradle)

- [https://github.com/apache/bookkeeper/pull/2693] Upgrade vertx to 4.3.2, addresses CVE-2018-12541

  The current vertx version is 3.5.3 which has a vulnerability, CVE-2018-12541 .

### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.2

## 4.14.1

Release 4.14.1 fixes an issue with Prometheus metrics that was
found in 4.14.0.

Apache BookKeeper users are encouraged to upgrade to 4.14.1. 
The technical details of this release are summarized below.

#### Bugs

- [https://github.com/apache/bookkeeper/pull/2718] Fix prometheus metric provider bug and add test to cover label scope

  After add label for prometheus metric by #2650, it will cause prometheus metric format check failed when no label specified for a statsLogger. The metric list as follow.


### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.1

## 4.14.0

Release 4.14 adds FIPS compliance, improves compaction logic and the Stream Storage, 
improves data reliability in the recovery scenarios,
fixes multiple bugs and brings critical dependencies up-to-date.

Apache BookKeeper users are encouraged to upgrade to 4.14.0. 
The technical details of this release are summarized below.

### Highlights

#### Bookkeeper is FIPS compliant by default now

- [https://github.com/apache/bookkeeper/pull/2631] Make Bookkeeper FIPS compliant by default

  FIPS is 'Federal Information Processing Standard'. 
  It's a set of guidelines for security functions such as encryption/decryption/RNG etc. 
  Applications running in FIPS mode are said to be more secure as they adhere to more stringent standards.

#### Data reliability

- [https://github.com/apache/bookkeeper/pull/2616] Add fencing to recovery reads to avoid data loss issue

#### Table Service (stream storage) reliability improvements

- [https://github.com/apache/bookkeeper/pull/2686] Improved handling of RocksDB tombstones
- [https://github.com/apache/bookkeeper/pull/2641] Checksum validation for SST files
- [https://github.com/apache/bookkeeper/pull/2635] Better handling of corrupted checkpoints
- [https://github.com/apache/bookkeeper/pull/2643] Adjusted default rocksDbBlockCache size to 10%/numberOfLedgers of direct memory
- [https://github.com/apache/bookkeeper/pull/2698] RocksDB log path is configurable now

#### Compaction logic improvements

- [https://github.com/apache/bookkeeper/pull/2675] forceAllowCompaction to run only when force is set or configured interval
- [https://github.com/apache/bookkeeper/pull/2670] Allow a customer to set a limit on the duration of the major and minor compaction runs
- [https://github.com/apache/bookkeeper/pull/2645] Fix: The compaction status report is off by 1
- [https://github.com/apache/bookkeeper/pull/2626] Allow force compact entry log when entry log compaction is disabled
- [https://github.com/apache/bookkeeper/pull/2627] Allow DBLedgerStorage to force GC by disk listener

#### Dependency updates

- [https://github.com/apache/bookkeeper/pull/2696] SECURITY: Upgraded Netty to 4.1.63.Final
- [https://github.com/apache/bookkeeper/pull/2701] SECURITY: Removed jackson-mapper-asl dependency to resolve multiple CVEs
- [https://github.com/apache/bookkeeper/pull/2697] Upgraded Lombok to 1.18.20 (required for Java 16 support)
- [https://github.com/apache/bookkeeper/pull/2686] Upgraded rocksdb to 6.16.4

#### Other improvements and fixes

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

#### Other

Documentation, build, CI, tests improvements

### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.0

## 4.13.0

Release 4.13 improves reliability of the Stream Storage, 
brings additional configuration options for the Stream Storage and Prometheus HTTP Server, 
fixes multiple bugs and brings critical dependencies up-to-date.

Apache BookKeeper users are encouraged to upgrade to 4.13.0. The technical details of this release are summarized
below.

### Highlights

#### Table Service (stream storage) reliability improvements

- [https://github.com/apache/pulsar/pull/9481] Rocksdb DLCheckpoint SST file corruption in statestore
- [https://github.com/apache/bookkeeper/pull/2564] Fix SST file corruption
- [https://github.com/apache/bookkeeper/pull/2566] Handling checkpoint corruption in case of bookie crash
- [https://github.com/apache/bookkeeper/issues/2567] Save latest revision information in statestore
- [https://github.com/apache/bookkeeper/pull/2568] Save last revision in rocksdb

#### Other improvements

- [https://github.com/apache/bookkeeper/pull/2560] Allow stream storage to use hostname instead of IP address
- [https://github.com/apache/bookkeeper/pull/2597] Skip unavailable bookies during verifyLedgerFragment
- [https://github.com/apache/bookkeeper/pull/2543] Allow to configure Prometheus HTTP Server bind address
- various fixes of the tests, documentation, etc.

#### Dependency updates

- [https://github.com/apache/bookkeeper/pull/2580] Upgrade protobuf to 3.14.0
- [https://github.com/apache/bookkeeper/pull/2582] Upgrading GRPC version to 1.33, Netty to 4.1.50Final and ETCD client driver
- [https://github.com/apache/bookkeeper/pull/2602] Upgrading dropwizard to 3.2.5

### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.13.0


## 4.12.1

This is the 24th release of Apache BookKeeper, it contains a few bugfixes, new features and dependency upgrades

Apache BookKeeper users are encouraged to [upgrade to 4.12.1](docs/admin/upgrade). The technical details of this release are summarized
below.

### News and noteworthy
- [https://github.com/apache/bookkeeper/pull/2519] Allow DNSToSwitchMapping to access BookieAddressResolver
- [https://github.com/apache/bookkeeper/pull/2493] Opportunistic Striping
- [https://github.com/apache/bookkeeper/pull/2398] getBookieInfo is stuck if no bookie is up
- [https://github.com/apache/bookkeeper/pull/2491] Upgrade to Curator 5.1
- [https://github.com/apache/bookkeeper/pull/2523] Update jcommander from 1.48 to 1.78

### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.12.1+

## 4.12.0

This is the 23rd release of Apache BookKeeper, it is a great milestone for the project, and we are introducing a few breaking changes on the API.
There are not changes on the wire protocol, on metadata and on persisted data on disks by default, so the new version is totally compatible with the previous ones.
With BookKeeper 4.12.0 we are making a step toward better deployment on environments with dynamic network addresses with BP-41.
We are also enhancing the new Client API by adding features that were still missing, like the ability of queryng for ledger metadata.

Apache BookKeeper users are encouraged to [upgrade to 4.12.0](docs/admin/upgrade). The technical details of this release are summarized
below.

### News and noteworthy

- [https://github.com/apache/bookkeeper/pull/1901] Enable ExplicitLAC but default on the reader side and in the New org.apache.bookkeeper.client.api.ReadHandle API
- [https://github.com/apache/bookkeeper/issues/2396] BP-41 Bookie Network Address Change Tracking + BookieId
- [https://github.com/apache/bookkeeper/issues/2422] BP-42 List and Access LedgerMetadata on the new API
- [https://github.com/apache/bookkeeper/pull/2433] Support Java 11 and switch to Java 11 default Docker images
- [https://github.com/apache/bookkeeper/pull/2455] BP-40 clean up output for tools
- [https://github.com/apache/bookkeeper/pull/2429] Certificate role based authorization 

### Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.12.0+

## 4.11.1

Apache BookKeeper users are encouraged to upgrade to 4.11.1. The technical details of this release are summarized
below.

### Highlights

- Upgrade Netty,Vertx and RocksDB
- Better error reporting in case of ZooKeeper related errors
- Fix error that prevents Garbage Collections in case of corrupted EntryLogger file
- Support for Apache ZooKeeper 3.6.x

#### Changes

- [https://github.com/apache/bookkeeper/pull/2410] Upgrade the `vertx` version to 3.5.3

- [https://github.com/apache/bookkeeper/pull/2390] Issue #2385: NullPointerException in Zookeeper multiple operations execution with 3.6.1

- [https://github.com/apache/bookkeeper/pull/2389] Issue #2197: bkctl binary distribution needs a 'logs' directory

- [https://github.com/apache/bookkeeper/pull/2384] Track ZooKeeper errors as causes of ZKException

- [https://github.com/apache/bookkeeper/pull/2383] fix fillReadAheadCache stat bug

- [https://github.com/apache/bookkeeper/pull/2381] The latency of BenchThroughputLatency may be wrong due to Integer overflow when we do a large scale benchmark test

- [https://github.com/apache/bookkeeper/pull/2380] NP check for print BookieSocketAddress and a better format

- [https://github.com/apache/bookkeeper/pull/2379] Updated netty,netty-boringssl and rocksdb

- [https://github.com/apache/bookkeeper/pull/2373] Issue 2264: Bookie cannot perform Garbage Collection in case of corrupted EntryLogger file

- [https://github.com/apache/bookkeeper/pull/2327] Bookie Client add quarantine ratio when error count exceed threshold

- [https://github.com/apache/bookkeeper/pull/2415] Spammy log when one bookie of ensemble is down

### Compatibility

This is a point release and it does not bring API changes.

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.11.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.11.1+is%3Aclosed)

## 4.11.0

This is the 21th release of Apache BookKeeper!

The 4.11.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.10.0.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.11.0](docs/admin/upgrade). The technical details of
this release are summarized below.

### News and noteworthy

-  Upgraded ZooKeeper version from `3.4.13` to `3.5.7` with #2112
-  BookKeeper-server depends on `org.apache.httpcomponents-httpcore-4.4.9` with #2156


#### Changes

- [https://github.com/apache/bookkeeper/pull/2338] Fix bookie port conflict when using LocalBookKeeper
- [https://github.com/apache/bookkeeper/pull/2333] Handle QuorumCoverage should only count unknown nodes
- [https://github.com/apache/bookkeeper/pull/2326] Update jackson version 2.11.0
- [https://github.com/apache/bookkeeper/pull/2314] BP-38: Publish Bookie Service Info including all advertised addresses on Metadata Service and it is backward compatible
- [https://github.com/apache/bookkeeper/pull/2313] add REST API to manage auto-recovery
- [https://github.com/apache/bookkeeper/pull/2312] Support metadata decoding for list-ledger api
- [https://github.com/apache/bookkeeper/pull/2300] files: Fix TLS with with v2 protocol
- [https://github.com/apache/bookkeeper/pull/2297] Update Arquillian Cube to 1.18.2
- [https://github.com/apache/bookkeeper/pull/2291] Update Prometheus library to 0.8.1
- [https://github.com/apache/bookkeeper/pull/2205] Handle empty ledger segmant while replica-check
- [https://github.com/apache/bookkeeper/pull/2156] Add Hostname verification for bookie-mTLS
- [https://github.com/apache/bookkeeper/pull/2112] Update ZooKeeper dependency to 3.5.7



### Full list of changes

- [https://github.com/apache/bookkeeper/milestone/7](https://github.com/apache/bookkeeper/milestone/6?closed=1)
## 4.10.0

This is the 20th release of Apache BookKeeper!

The 4.10.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.9.0.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.10.0](docs/admin/upgrade). The technical details of
this release are summarized below.

### News and noteworthy

- [https://github.com/apache/bookkeeper/pull/2069] Use pure python implementation of MurmurHash
- [https://github.com/apache/bookkeeper/pull/1934] Bump Netty and GRPC version
- [https://github.com/apache/bookkeeper/pull/1907] Add new *bkctl* shell tool
- [https://github.com/apache/bookkeeper/issues/1602] Cluster Metadata Checker
- [https://github.com/apache/bookkeeper/pull/2154] Auto refresh TLS certificate at bookie-server
- [https://github.com/apache/bookkeeper/pull/2150] Improve journal throughput when journalSyncData is disabled.
- [https://github.com/apache/bookkeeper/pull/2147] Journal should respect to `flushWhenQueueEmpty` setting
- [https://github.com/apache/bookkeeper/pull/2132] Make default Bookie scripts work on JDK11+
- [https://github.com/apache/bookkeeper/pull/2128] Allow to override default SASL service name 'bookkeeper'
- [https://github.com/apache/bookkeeper/pull/2117] BookKeeper Admin API: Implement a method to get all the Bookies
- [https://github.com/apache/bookkeeper/pull/2111] Ensure getStickyReadBookieIndex returns valid bookie index


### Full list of changes

- [https://github.com/apache/bookkeeper/milestone/6](https://github.com/apache/bookkeeper/milestone/6?closed=1)

## 4.9.2

This is the 18th release of Apache BookKeeper!

The 4.9.2 release incorporates a few critical bug fixes, since previous major release, 4.9.0.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.9.2](docs/admin/upgrade). The technical details of
this release are summarized below.

#### Dependencies Changes

No dependency change.

#### Bug Fixes

- [Issue #1973: [DLOG] Avoid double read in readahead](https://github.com/apache/bookkeeper/pull/1973)
- [Issue #1952: Filter empty string for networkTopologyScriptFileName](https://github.com/apache/bookkeeper/pull/1952)
- [Issue #1950: putEntryOffset translate FileInfoDeletedException](https://github.com/apache/bookkeeper/pull/1950)

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.9.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.9.1+is%3Aclosed)

## 4.9.1

This is the 18th release of Apache BookKeeper!

The 4.9.1 release incorporates a few critical bug fixes, since previous major release, 4.9.0.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.9.1](docs/admin/upgrade). The technical details of
this release are summarized below.

#### Dependencies Changes

No dependency change.

#### Bug Fixes

- [Issue #1973: [DLOG] Avoid double read in readahead](https://github.com/apache/bookkeeper/pull/1973)
- [Issue #1952: Filter empty string for networkTopologyScriptFileName](https://github.com/apache/bookkeeper/pull/1952)
- [Issue #1950: putEntryOffset translate FileInfoDeletedException](https://github.com/apache/bookkeeper/pull/1950)

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.9.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.9.1+is%3Aclosed)

## 4.9.0

This is the 16th release of Apache BookKeeper!

The 4.9.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.8.0,
which was released four months ago. It is a new milestone in Apache BookKeeper community.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.9.0](docs/admin/upgrade). The technical details of
this release are summarized below.

### Highlights

The main features in 4.9.0 cover are around following areas:

- Dependencies Changes
- Public API
- Configuration
- Metadata
- Table Service
- Operations
- Builds & Testing
- Enhancements
- Bug Fixes

#### Dependencies Changes

Here is a list of dependencies changed in 4.9.0:

- Upgrade [Jackson](http://fasterxml.com/) from `2.8.9` to `2.9.7`.
- Upgrade [Jline](https://jline.github.io/) to `2.11`.
- Upgrade [Netty](https://netty.io/) from `4.1.22` to `4.1.31`.
- Upgrade [TestContainers](https://www.testcontainers.org/) from `1.7.0` to `1.8.3`.

#### Public API

There are multiple new client features introduced in 4.9.0. Here are two highlighted features:

- LedgerHandleAdv exposes `asyncAddEntry` variant that takes ByteBuf

#### Configuration

There are bunch of new settings introduced in both bookie and client in 4.9.0. Here are those settings:

##### Bookie

- `serverNumIOThreads`: configures the number of IO threads for bookies
  (see [#1612](https://github.com/apache/bookkeeper/pull/1612))
- The default value of `fileInfoFormatVersionToWrite` is bumped from `0` to `1`.
  (see [#1689](https://github.com/apache/bookkeeper/pull/1689))
- The default value of `journalFormatVersionToWrite` is bumped from `5` to `6`.
  (see [#1689](https://github.com/apache/bookkeeper/pull/1689))

##### Client

- `numIOThreads`: configures the number of IO threads for client
  (see [#1612](https://github.com/apache/bookkeeper/pull/1612))

#### Metadata

There are a few big changes around metadata in 4.9.0. They are:

- Refactor ledger metadata in LedgerHandle to make ledger metadata instance immutable (see [#281](https://github.com/apache/bookkeeper/issues/281))
- Store ledger metadata in binary protobuf format (see details at [#723](https://github.com/apache/bookkeeper/issues/723))
- Etcd based metadata driver implementation is in BETA release (see details at [#1639](https://github.com/apache/bookkeeper/issues/1639))

Additionally, there are bunch of new interfaces introduced in the metadata driver API.

- [Issue #1619: Provide async version of markLedgerUnderreplicated for LedgerUnderreplicationManager](https://github.com/apache/bookkeeper/pull/1619)

#### Table Service

There are a lot of improvements and features introduced into the table service. The maturity of table service is moving from alpha to beta,
and has started to be used as the state storage for Pulsar Functions. More table service usage will come in Pulsar's future releases.

Starting from 4.9.0, bookkeeper will release a python client for table service. See details at [#1691](https://github.com/apache/bookkeeper/pull/1691)

#### Operations

##### HTTP Admin REST Endpoint

- `/api/v1/bookie/gc_details` is introduced to retrieve the GC details.
- `/api/v1/bookie/gc` is introduced to trigger GC through HTTP REST endpoint.

##### BookieShell

There are are multiple new commands are added in BookieShell. Here are a few highlighted:

- `regenerate-interleaved-storage-index-file` command is introduced for rebuilding the index files for interleaved based ledger storage. ([#1642](https://github.com/apache/bookkeeper/pull/1642))
- `ledgermetadata` command now supports dumping/restoring ledger metadata to/from file.
- `localconsistencycheck` command is introduce for running consistency check on bookies locally. ([#1819](https://github.com/apache/bookkeeper/pull/1819))
- a new `bk-perf` script is introduced for running performance benchmark on bookkeeper. ([1697](https://github.com/apache/bookkeeper/pull/1697))

A new BookKeeper CLI package is released as `bkctl`. This `bkctl` package includes both the existing bookie shell and the new `bkctl` tool.

##### MDC

Mapped Diagnostic Context (MDC) is now supported at both bookie and client sides. Application request context can be passed as context
and being logged through slf4j/log4j. This simplifies throubleshooting of request-level failures/errors. See details at [#1672](https://github.com/apache/bookkeeper/pull/1672).

##### Stats Annotation

`StatsDoc` annotation is introduced in [BP-36](https://github.com/apache/bookkeeper/pull/1786). The `StatsDoc` annotation is
used for documenting stats added across the project.

#### Builds & Testing

- Java 11 is supported for building bookkeeper.

#### Enhancements

- [Issue 1791: Read Submission should bypass OSE Threads](https://github.com/apache/bookkeeper/pull/1792)
- A new module is introduced for enabling CPU affinity [#1641](https://github.com/apache/bookkeeper/pull/1641)
- [Issue 1682: Added BlockingQueue implementation based on JCtools](https://github.com/apache/bookkeeper/pull/1682)
- [Issue 1813: Set default sizes of DbLedgerStorage read and write cache to be proportional to JVM direct memory](https://github.com/apache/bookkeeper/pull/1813)
- [Issue 1808: Allow to configure sticky reads](https://github.com/apache/bookkeeper/pull/1808)
- [Issue 1754: Netty allocator wrapper](https://github.com/apache/bookkeeper/pull/1754)

#### Bug Fixes

##### Bookie

- [Issue #1414: Ensure BufferedChannel instance is properly closed](https://github.com/apache/bookkeeper/pull/1414)
- [Issue #1805: Fixed Auth with V2 protocol](https://github.com/apache/bookkeeper/pull/1805)
- [Issue #1769: prevent race between flush and delete from recreating index](https://github.com/apache/bookkeeper/pull/1769)
- [Issue #1807: Fix sorted ledger storage rotating entry log files too frequent](https://github.com/apache/bookkeeper/pull/1807)
- [Issue #1843: DbLedgerStorage should do periodical flush](https://github.com/apache/bookkeeper/pull/1843)

##### AutoRecovery

- [Issue #1578: Fixed deadlock in auditor blocking ZK thread](https://github.com/apache/bookkeeper/pull/1608)
- [Issue #1834: Only publish suspect ledgers if they have missing fragments](https://github.com/apache/bookkeeper/pull/1834)

##### Client

- [Issue #1762: Don't cache Bookie hostname DNS resolution forever](https://github.com/apache/bookkeeper/pull/1762)
- [Issue #1788: Fix bugs in DefaultEnsemblePlacementPolicy](https://github.com/apache/bookkeeper/pull/1788)
- [Issue #1862: Fix selectFromNetworkLocation in RackawareEnsemblePlacementPolicyImpl](https://github.com/apache/bookkeeper/pull/1862)
- [Issue #1857: changingEnsemble should be negated before calling unset success](https://github.com/apache/bookkeeper/pull/1857)

### Full list of changes

- [https://github.com/apache/bookkeeper/milestone/5](https://github.com/apache/bookkeeper/milestone/5?closed=1)

## 4.8.2

This is the 17th release of Apache BookKeeper!

The 4.8.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.8.1.

Apache BookKeeper users who are using 4.8.1 are encouraged to upgrade to 4.8.2. The technical details of this release are summarized
below.

### Highlights

- [DLOG] Avoid double read in readahead, see [apache/bookkeeper#1973](https://github.com/apache/bookkeeper/pull/1973)

- Small fix wrong nodesUninitialized count when checkCovered, see [apache/bookkeeper#1900](https://github.com/apache/bookkeeper/pull/1900)

- Handle double bookie failures, see [apache/bookkeeper#1886](https://github.com/apache/bookkeeper/pull/1886)

- dir_\*_usage stats are reported as 0, see [apache/bookkeeper#1884](https://github.com/apache/bookkeeper/pull/1884)

- Fix selectFromNetworkLocation in RackawareEnsemblePlacementPolicyImpl, see [apache/bookkeeper#1862](https://github.com/apache/bookkeeper/pull/1862)

- DbLedgerStorage should do periodical flush, see [apache/bookkeeper#1842](https://github.com/apache/bookkeeper/pull/1842)

- Add rest endpoint trigger_gc to trigger GC on Bookie, see [apache/bookkeeper#1838](https://github.com/apache/bookkeeper/pull/1838)

- Fix sorted ledger storage rotating entry log files too frequent, see [apache/bookkeeper#1807](https://github.com/apache/bookkeeper/pull/1807)

- Fixed Auth with v2 protocol, see [apache/bookkeeper#1805](https://github.com/apache/bookkeeper/pull/1805)

- [tools] add cookie related commands, see [apache/bookkeeper#1974](https://github.com/apache/bookkeeper/pull/1794)

- [tools] improve bkctl help message, see [apache/bookkeeper#1793](https://github.com/apache/bookkeeper/pull/1793)

- Read Submission should bypass OSE Threads, see [apache/bookkeeper#1791](https://github.com/apache/bookkeeper/pull/1791)

- Cache InetSocketAddress if hostname is IPAddress, see [apache/bookkeeper#1789](https://github.com/apache/bookkeeper/pull/1789)

- Fix bugs in DefaultEnsemblePlacementPolicy, see [apache/bookkeeper#1788](https://github.com/apache/bookkeeper/pull/1788)

#### Dependency Changes

There is no dependency upgrade from 4.8.1.

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.8.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.8.2+is%3Aclosed)


## 4.8.1

This is the 14th release of Apache BookKeeper!

The 4.8.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.8.0.

Apache BookKeeper users who are using 4.8.0 are encouraged to upgrade to 4.8.1. The technical details of this release are summarized
below.

### Highlights

- Use default metrics registry in Prometheus exporter, see [apache/bookkeeper#1765](https://github.com/apache/bookkeeper/pull/1765)

- Don't cache Bookie hostname DNS resolution forever, see [apache/bookkeeper#1762](https://github.com/apache/bookkeeper/pull/1762)

- Reduce stack traces in logs for common cases, see [apache/bookkeeper#1762](https://github.com/apache/bookkeeper/pull/1776)

- Ledger deletion racing with flush can cause a ledger index to be resurrected, see [apache/bookkeeper#1757](https://github.com/apache/bookkeeper/pull/1757)

- EntryMemTable.newEntry retains reference to passed ByteBuffer array, can cause corruption on journal replay, see [apache/bookkeeper#1737](https://github.com/apache/bookkeeper/pull/1737)


#### Dependency Changes

There is no dependecy upgrade from 4.8.0.

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.8.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.8.1+is%3Aclosed)

## 4.8.0

This is the 13th release of Apache BookKeeper!

The 4.8.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.7.0.
It is a new big milestone in Apache BookKeeper community,
this release include great new features, like Relaxed Durability, Stream Storage service and Multiple active Entrylogs.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.8.0](docs/admin/upgrade). The technical details of
this release are summarized below.

### Highlights

The main features in 4.8.0 are around following areas:

- Durability
- ExplicitLAC feature
- New Table Storage Service
- Bug Fixes


#### New WriteFlag DEFERRED_SYNC

The writer may ask for temporary relaxed durability writes, that is to receive early acknowledge from Bookies, before an fsync() on Journal.
Together with this new flag we introduced the new WriteHandle#force() API, this this API the writer is able to request an explicit guarantee of durability to the Bookies
it is mostly like and explicit fsync() on a file system.

See [`DEFERRED_SYNC`]({{ site.javadoc_base_url }}/javadoc/org/apache/bookkeeper/client/api/WriteFlag) and [force()]({{ site.javadoc_base_url }}/javadoc/org/apache/bookkeeper/client/api/ForceableHandle) for reference

#### New behaviour for Netty ByteBuf reference count management

All the client side APIs which take ByteBufs now will have the net effect of decrementing by 1 the refcount.
This is consistent with general contract of Netty.
It is expected that the client passes the ownership of the ByteBuf to BookKeeper client.

#### Multiple Active Entrylogs

It is now possible on the Bookie to have multiple active entry loggers,
this new feature will help with compaction performance and some specific workloads.

See [Multiple active entrylogs](https://github.com/apache/bookkeeper/issues/570)

#### Table Storage Service

From this version we are providing the a table (key/value) service embedded in Bookies.
 
See [BP-30: BookKeeper Table Service](https://github.com/apache/bookkeeper/issues/1205)

#### Make ExplicitLAC persistent

ExplicitLAC was contributed from Salesforce in 4.5.0 release, but in the first release
it was a beft-effort in-memory mechanism. Now you can configure Bookies to store durably ExplicitLAC.

See [Make ExplicitLAC persistent](https://github.com/apache/bookkeeper/issues/1527)

#### Ensemble change on Delayed Write Failure

We are handling more gracefully the case of a failure of a Bookie in spite of a succeeded write.
If you are writing with Ack Quorum = 2 and Write Quorum = 3, writes will succeeed even if 1 of 3 Bookies fail,
now BookKeeper will trigger an *ensemble change* and replace the failed bookie earlier.

See [Ensemble change on Delayed Write Failure](https://github.com/apache/bookkeeper/issues/1390)

### Full list of changes

- [https://github.com/apache/bookkeeper/milestone/4](https://github.com/apache/bookkeeper/milestone/4?closed=1)

## 4.7.3

This is the 16th release of Apache BookKeeper!

The 4.7.3 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.2.

Apache BookKeeper users who are using 4.7.2 are encouraged to upgrade to 4.7.3. The technical details of this release are summarized
below.

### Highlights

- Cancel Scheduled SpeculativeReads, see [apache/bookkeeper#1665](https://github.com/apache/bookkeeper/pull/1665)

- IllegalReferenceCountException at closing EntryLogManagerForSingleEntryLog, see [apache/bookkeeper#1703](https://github.com/apache/bookkeeper/issues/1703)

- EntryMemTable.newEntry retains reference to passed ByteBuffer array can cause corruption on journal replay, see [apache/bookkeeper#1737](https://github.com/apache/bookkeeper/issues/1737)

- Ledger deletion racing with flush can cause a ledger index to be resurrected, see [apache/bookkeeper#1757](https://github.com/apache/bookkeeper/issues/1757)

- Don't cache Bookie hostname DNS resolution forever, see [apache/bookkeeper#1762](https://github.com/apache/bookkeeper/pull/1762)

- Use default metric registry in Prometheus export, see [apache/bookkeeper#1765](https://github.com/apache/bookkeeper/pull/1765)

- Fix Auth with v2 protocol, see [apache/bookkeeper#1805](https://github.com/apache/bookkeeper/pull/1805)

- Remove MathUtils.now to address compaction scheduling deplay issues, see [apache/bookkeeper#1837](https://github.com/apache/bookkeeper/pull/1837)

- DbLedgerStorage should do periodical flush, see [apache/bookkeeper#1843](https://github.com/apache/bookkeeper/pull/1843)

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.3+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.3+is%3Aclosed)

## 4.7.2

This is the 12th release of Apache BookKeeper!

The 4.7.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.1.

Apache BookKeeper users who are using 4.7.1 are encouraged to upgrade to 4.7.2. The technical details of this release are summarized
below.

### Highlights

- Fix high cpu usage issue in DbLedgerStorage by avoiding using RocksDD#deleteRange, see [apache/bookkeeper#1620](https://github.com/apache/bookkeeper/pull/1620)

- Fix deadlock in Auditor blocking zookeeper thread, see [apache/bookkeeper#1619](https://github.com/apache/bookkeeper/pull/1619)

- Fix ArrayIndexOutOfBoundsException on ConcurrentLongHashMap, see [apache/bookkeeper#1606](https://github.com/apache/bookkeeper/pull/1606)

- Fix deferred failure handling causes data loss, see [apache/bookkeeper#1591](https://github.com/apache/bookkeeper/pull/1591)

- Fix ConcurrentModificationException using nonblocking logReader#readNext, see [apache/bookkeeper#1544](https://github.com/apache/bookkeeper/pull/1544)

- Fix Bookie shutdown fails to exit, see [apache/bookkeeper#1543](https://github.com/apache/bookkeeper/issues/1543)

- Fix race conditions on accessing guava multimap in PCBC when using v2 protocol, see [apache/bookkeeper#1618](https://github.com/apache/bookkeeper/pull/1618)

#### Dependency Changes

In 4.7.2, [Zookeeper](https://zookeeper.apache.org/) version is downgraded from `3.5.3-beta` to `3.4.13` to avoid having a `beta` dependency and address maturity concerns.
The downgrade is safe and smooth. No extra actions are required from switching bookkeeper 4.7.1 to 4.7.2.

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.2+is%3Aclosed)

## 4.7.1

This is the eleventh release of Apache BookKeeper!

The 4.7.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.0.

Apache BookKeeper users who are using 4.7.0 are encouraged to upgrade to 4.7.1. The technical details of this release are summarized
below.

### Highlights

- Performance enhancement on eliminating bytes copying in `AddEntry` code path, see [apache/bookkeeper#1361](https://github.com/apache/bookkeeper/pull/1361)

- Introduce Fast and Garbage-Free Statistics Timers in Codahale Stats Provider, see [apache/bookkeeper#1364](https://github.com/apache/bookkeeper/pull/1364)

- Fix OrderedScheduler handling null key, see [apache/bookkeeper#1372](https://github.com/apache/bookkeeper/pull/1372)

- Fix zookeeper ledger manager on handling no ledger exists, see [apache/bookkeeper#1382](https://github.com/apache/bookkeeper/pull/1382)

- Fix long poll reads when ensemble size is larger than write quorum size, see [apache/bookkeeper#1404](https://github.com/apache/bookkeeper/pull/1404)

- Fix IllegalReferenceCount on filling readahead cache for DbLedgerStorage, see [apache/bookkeeper#1487](https://github.com/apache/bookkeeper/issues/1487)

- Fix LedgerEntry recycling issue on long poll speculative reads, see [apache/bookkeeper#1509](https://github.com/apache/bookkeeper/pull/1509)

- Various bug fixes and improvements around bookkeeper table service, see changes under [apache/bookkeeper#release/4.7.1](https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=is%3Aclosed+label%3Aarea%2Ftableservice+label%3Arelease%2F4.7.1)

#### Dependencies Upgrade

Here is a list of dependencies changed in 4.7.1:

- [Grpc](https://grpc.io/) is upgraded from `1.5.0` to `1.12.0`. See [apache/bookkeeper#1441](https://github.com/apache/bookkeeper/pull/1441)
- [Netty](http://netty.io/) is upgraded from `4.1.12` to `4.1.22`. See [apache/bookkeeper#1441](https://github.com/apache/bookkeeper/pull/1441)
- [Protobuf](https://developers.google.com/protocol-buffers/) is upgraded from `3.4.0` to `3.5.1`. See [apache/bookkeeper#1466](https://github.com/apache/bookkeeper/pull/1466)
- [RocksDB](http://rocksdb.org/) is upgraded from `5.8.6` to `5.13.1`. See [apache/bookkeeper#1466](https://github.com/apache/bookkeeper/pull/1466)

`Reflective setAccessible(true)` is disabled by default in Netty while using java9+. This might result in performance degradation. Consider reenabling `Reflective setAccessible(true)` by setting
environment value `io.netty.tryReflectionSetAccessible` to `true`. See [netty/netty#7650](https://github.com/netty/netty/pull/7650) for more details.

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.7.1+is%3Aclosed)

## 4.7.0

This is the tenth release of Apache BookKeeper!

The 4.7.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.6.0,
which was released four months ago. It is a big milestone in Apache BookKeeper community - Yahoo branch is fully merged
back to upstream, and Apache Pulsar (incubating) starts using official BookKeeper release for its upcoming 2.0 release.

It is also the first release of Apache DistributedLog after it is merged as sub modules of Apache BookKeeper.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.7.0](docs/admin/upgrade). The technical details of
this release are summarized below.

### Highlights

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

#### Dependencies Changes

Here is a list of dependencies changed in 4.7.0:

- [JCommander](http://jcommander.org/) 1.48 is added as a dependency of bookkeeper-server module.
- [RocksDB](http://rocksdb.org/) 5.8.6 is introduced as part of `DbLedgerStorage` as a dependency of bookkeeper-server module.
- [DataSketches](https://datasketches.github.io/) 0.8.3 is introduced as a dependency of prometheus-metrics-provider module.
- Upgrade [Guava](https://github.com/google/guava) from `20.0` to `21.0`.

#### Public API

There are multiple new client features introduced in 4.7.0. Here are two highlighted features:

##### Fluent API

The new fluent style APi is evolving in 4.7.0. All the methods in handlers are now having both async and sync methods.
See [#1288](https://github.com/apache/bookkeeper/pull/1288) for more details

##### CRC32C

`circe-checksum` module is ported from Apache Pulsar to Apache BookKeeper, and CRC32C digest type is added as one digest type option.
The JNI based CRC32C in `circe-checksum` module provides excellent performance than existing CRC32 digest type. Users are encouraged
to start use CRC32C digest type.

#### Security

- New PEM format `X.509` certificates are introduced for TLS authentication. See [#965](https://github.com/apache/bookkeeper/pull/965) for more details.
- TLS related settings are converged into same settings as bookie server. See [Upgrade Guide](docs/admin/upgrade) for more details.

#### DbLedgerStorage

`DbLedgerStorage` is a new ledger storage that introduced by Yahoo and now fully merged into Apache BookKeeper. It is fully compatible for both v2 and v3
protocols and also support long polling. It uses [RocksDB](http://rocksdb.org/) to store ledger index, which eliminates the needed of ledger index files and
reduces the number of open file descriptors and the amount of random IOs can occurs during flushing ledger index.

#### Metadata API

New serviceUri based metadata API is introduced as [BP-29](http://bookkeeper.apache.org/bps/BP-29-metadata-store-api-module). This metadata API provides the metadata
abstraction over ledger manager, registration service, allowing plugin different type of data stores as the metadata service.

#### Performance

There are a lot for performance related bug fixes and improvements in 4.7.0. Some of the changes are highlighted as below:

- Leverage netty object recycler to reduce object allocations
- A bunch of contentions around locking are removed. E.g. [#1321](https://github.com/apache/bookkeeper/pull/1321) [#1292](https://github.com/apache/bookkeeper/pull/1292) [#1258](https://github.com/apache/bookkeeper/pull/1258)
- Introduce priority thread pool for accepting high priority reads/writes. This allows high priority reads/writes such as ledger recovery operations can
  succeed even bookies are overwhelmed. [#898](https://github.com/apache/bookkeeper/pull/898)
- Reorder slow bookies in read sequence. [#883](https://github.com/apache/bookkeeper/pull/883)
- Use atomic field updater and long adder to replace AtomicInteger/AtomicLong/AtomicReference in Dlog. [#1299](https://github.com/apache/bookkeeper/pull/1299)
- DataSketches library is used for implementing prometheus provider. [#1245](https://github.com/apache/bookkeeper/pull/1245)

#### Operations

#### BookieShell

There are are multiple new commands are added in BookieShell. Here are a few highlighted:

- `metaformat` is deprecated with two new commands `initnewcluster` and `nukeexistingcluster`. This separation provides better operability and reduces mistakes.
- `initbookie` command is introduced for initializing a new bookie. `bookieformat` keeps serving as the purpose of reformatting a bookie.

A new BookKeeper CLI is proposed in [BP-27](http://bookkeeper.apache.org/bps/BP-27-new-bookkeeper-cli). Some commands are already ported to new bookkeeper CLI.
The full list of shell commands will be fully ported to new bookkeeper CLI in next release.

#### ReadOnly Mode Support

Operations are improved around readonly mode for handling bookkeeper outage situation. New settings are introduce allow entry log creation, high priority writes
even when bookies are readonly. See [Upgrade Guide](docs/admin/upgrade) to learn all newly added settings.


#### Builds & Testing

- [Arquillian](http://arquillian.org/) framework is introduced in 4.7.0 for backward compatibility and integration tests. 
- Both Java8 and Java9 are now supported for running bookkeeper.

### Full list of changes

- [https://github.com/apache/bookkeeper/milestone/3](https://github.com/apache/bookkeeper/milestone/3?closed=1)

## 4.6.2

This is the ninth release of BookKeeper as an Apache Top Level Project!

The 4.6.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.6.1.

Apache BookKeeper users are encouraged to upgrade to 4.6.2. The technical details of this release are summarized
below.

### Highlights

- Fix performance regression is using Netty > 4.1.12, see [https://github.com/apache/bookkeeper/pull/1108](https://github.com/apache/bookkeeper/pull/1108)

- Enhance performances on Prometheus stats provider, see [https://github.com/apache/bookkeeper/pull/1081](https://github.com/apache/bookkeeper/pull/1081)

- Save memory resources on client by retaining for less time references to data to write, see [https://github.com/apache/bookkeeper/issues/1063](https://github.com/apache/bookkeeper/issues/1063)

- Fix a problem on Java 9/10 with the 'shaded' artifacts, due to a bug in Maven Shade Plugin, see [https://github.com/apache/bookkeeper/pull/1144](https://github.com/apache/bookkeeper/pull/1144)

- Fix Journal stats names, see [https://github.com/apache/bookkeeper/pull/1250](https://github.com/apache/bookkeeper/pull/1250)

#### Dependencies Upgrade

There is no dependency upgrade since 4.6.0, and since 4.6.1 we distribute a 'shaded' version of main artifacts, see [Ledger API](docs/4.6.2/api/ledger-api)

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.6.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.6.2+is%3Aclosed)

## 4.6.1

This is the eighth release of BookKeeper as an Apache Top Level Project!

The 4.6.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.6.0.

Apache BookKeeper users are encouraged to upgrade to 4.6.1. The technical details of this release are summarized
below.

### Highlights

- Fix critical bug on index persistence manager, see [https://github.com/apache/bookkeeper/pull/913](https://github.com/apache/bookkeeper/pull/913)

- Fix critical bug to allow using versions of Netty newer than 4.1.2 on classpath, see [https://github.com/apache/bookkeeper/pull/996](https://github.com/apache/bookkeeper/pull/996)

- Enhance Java 9 compatibility, see [https://github.com/apache/bookkeeper/issues/326](https://github.com/apache/bookkeeper/issues/326)

- New option to track task execution time, see [https://github.com/apache/bookkeeper/issues/931](https://github.com/apache/bookkeeper/issues/931)

- Distribute a version of BookKeeper which embeds and relocates Guava and Protobuf, see [https://github.com/apache/bookkeeper/issues/922](https://github.com/apache/bookkeeper/issues/922)

- Add description for the new error code "Too many requests", see [https://github.com/apache/bookkeeper/pull/921](https://github.com/apache/bookkeeper/pull/921)

#### Dependencies Upgrade

There is no dependency upgrade since 4.6.0, but now we distribute a 'shaded' version of main artifacts, see [Ledger API](docs/api/ledger-api)

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.6.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.6.1+is%3Aclosed)

## 4.6.0

This is the seventh release of BookKeeper as an Apache Top Level Project!

The 4.6.0 release incorporates new fixes, improvements, and features since previous major release 4.5.0.

Apache BookKeeper users are encouraged to upgrade to 4.6.0. The technical details of this release are summarized
below.

### Highlights

The main features in 4.6.0 cover are around following areas:
- Dependencies Upgrade
- Bookie enhancement
- BookKeeper Admin REST API
- New BookKeeper API
- Performance improvement
- Deployment or Ease of use 

#### Dependencies Upgrade

- Upgrade Protobuf to `3.4`.

#### Bookie enhancement

- Persistable bookie status.
  - Prior to this release, bookie status was transient. It is a bit hard for management tooling. This feature adds persistable bookies status. See [Issue-265](https://github.com/apache/bookkeeper/issues/265) for more details.

- Introduce Bookie Discovery Interface.  Prior to this release, bookkeeper client only provides interfaces for ledger metadata management. It doesn't provide any interface for service discovery part. This feature introduces bookie discovery interface, so it allows plugging in different service discovery backends for bookkeeper.
  - Introduce Bookie Registration Manager for bookie server, see [Issue-662](https://github.com/apache/bookkeeper/issues/662) for more details.
  - Introduce registration client for bookkeeper client, see [Issue-666](https://github.com/apache/bookkeeper/issues/666) for more details.

- Lifecycle components for managing components in bookie server.
  - Introduce lifecycle component for each service component, which includes "stats provider", "auto recovery", "http endpoint", and "bookie server(both storage and netty server)", to run these components in a clear way. See [Issue-508](https://github.com/apache/bookkeeper/issues/508) and [Issue-547](https://github.com/apache/bookkeeper/issues/547) for more details.

- Make bookie recovery work with recovering multiple bookies. 
  - Make recovery tool work with multiple bookies, so that one call could recover multiple bookies. See [Issue-612](https://github.com/apache/bookkeeper/issues/612) for more details.

#### BookKeeper Admin REST API

- Introduce a bookkeeper admin endpoint for operations to interact and administer the bookkeeper cluster using REST API. see [PR-278](https://github.com/apache/bookkeeper/pull/278), [Issue-520](https://github.com/apache/bookkeeper/issues/520), and [Issue-674](https://github.com/apache/bookkeeper/issues/674) for more details.

#### New BookKeeper API

- New Fluent Style API.
  - A brand new API to manage ledgers using the Builder pattern, and new interfaces to make it clear operations on ledgers, like WriteHandle and ReadHandle, are provided in this release. See [Issue-506](https://github.com/apache/bookkeeper/issues/506), [Issue-673](https://github.com/apache/bookkeeper/issues/673) and [Issue-550](https://github.com/apache/bookkeeper/issues/550) for more details

#### Performance improvement
- Use ByteBuf in multiple places to avoid unnecessary memory allocation and reduce the garbage produced in JVM. See [PR-640](https://github.com/apache/bookkeeper/pull/640) for more details.

- Separate the FileInfo cache into write and read cache. It avoids catchup reads impact tailing reads and writes. See [PR-513](https://github.com/apache/bookkeeper/pull/513) for more details.

#### Deployment or Ease of use
- Deployment BookKeeper on K8s. 
  - Provide yaml files to run BookKeeper on Kubernetes using both StatefulSets and DaemonSet. See [Issue-337](https://github.com/apache/bookkeeper/issues/337) and [Issue-681](https://github.com/apache/bookkeeper/issues/681)for more details.

### Existing API changes

- BookKeeper constructor now throws BKException instead of KeeperException.
- The signatures of `reorderReadSequence` and `reorderReadLACSequence` are changed in EnsemblePlacementPolicy.

### Full list of changes

- [https://github.com/apache/bookkeeper/milestone/2?closed=1](https://github.com/apache/bookkeeper/milestone/2?closed=1)
## 4.5.1

This is the sixth release of BookKeeper as an Apache Top Level Project!

The 4.5.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.5.0.

Apache BookKeeper users are encouraged to upgrade to 4.5.1. The technical details of this release are summarized
below.

### Highlights

- Fix critical bug on Parallel Recovery, see [https://github.com/apache/bookkeeper/issues/343](https://github.com/apache/bookkeeper/issues/343)

- Fix critical bug on Prometheus stats provider, see [https://github.com/apache/bookkeeper/pull/535](https://github.com/apache/bookkeeper/pull/535)

- Fix critical bug ledger length for LedgerHandleAdv, see [https://github.com/apache/bookkeeper/issues/683](https://github.com/apache/bookkeeper/issues/683)

- Fix critical bug on RackAwarePolicy, see [https://github.com/apache/bookkeeper/issues/551](https://github.com/apache/bookkeeper/issues/551)

#### Dependencies Upgrade

There is no dependency upgrade since 4.5.0.

### Full list of changes

- [https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.5.1%20](https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.5.1%20)

## 4.5.0

This is the fifth release of BookKeeper as an Apache Top Level Project!

The 4.5.0 release incorporates hundreds of new fixes, improvements, and features since previous major release, 4.4.0,
which was released over a year ago. It is a big milestone in Apache BookKeeper community, converging from three
main branches (Salesforce, Twitter and Yahoo).

Apache BookKeeper users are encouraged to upgrade to 4.5.0. The technical details of this release are summarized
below.

### Highlights

The main features in 4.5.0 cover are around following areas:

- Dependencies Upgrade
- Security
- Public API
- Performance
- Operations

#### Dependencies Upgrade

Here is a list of dependencies upgraded in 4.5.0:

- Moved the developement from Java 7 to Java 8.
- Upgrade Protobuf to `2.6`.
- Upgrade ZooKeeper from `3.4` to `3.5`.
- Upgrade Netty to `4.1`.
- Upgrade Guava to `20.0`.
- Upgrade SLF4J to `1.7.25`.
- Upgrade Codahale to `3.1.0`.

#### Security

Prior to this release, Apache BookKeeper only supports simple `DIGEST-MD5` type authentication.

With this release of Apache BookKeeper, a number of feature are introduced that can be used, together of separately,
to secure a BookKeeper cluster.

The following security features are currently supported.

- Authentication of connections to bookies from clients, using either `TLS` or `SASL (Kerberos).
- Authentication of connections from clients, bookies, autorecovery daemons to `ZooKeeper`, when using zookeeper
    based ledger managers.
- Encryption of data transferred between bookies and clients, between bookies and autorecovery daemons using `TLS`.

It's worth noting that those security features are optional - non-secured clusters are supported, as well as a mix
of authenticated, unauthenticated, encrypted and non-encrypted clients.

For more details, have a look at [BookKeeper Security](docs/security/overview).

#### Public API

There are multiple new client features introduced in 4.5.0.

##### LedgerHandleAdv

The [Ledger API] is the low level API provides by BookKeeper for interacting with `ledgers` in a bookkeeper cluster.
It is simple but not flexible on ledger id or entry id generation. Apache BookKeeper introduces `LedgerHandleAdv`
as an extension of existing `LedgerHandle` for advanced usage. The new `LedgerHandleAdv` allows applications providing
its own `ledger-id` and assigning `entry-id` on adding entries.

See [Ledger Advanced API](docs/api/ledger-adv-api) for more details.

##### Long Poll

`Long Poll` is a main feature that [DistributedLog](https://distributedlog.io) uses to achieve low-latency tailing.
This big feature has been merged back in 4.5.0 and available to BookKeeper users.

This feature includes two main changes, one is `LastAddConfirmed` piggyback, while the other one is a new `long poll` read API.

The first change piggyback the latest `LastAddConfirm` along with the read response, so your `LastAddConfirmed` will be automatically advanced
when your read traffic continues. It significantly reduces the traffic to explicitly polling `LastAddConfirmed` and hence reduces the end-to-end latency.

The second change provides a new `long poll` read API, allowing tailing-reads without polling `LastAddConfirmed` everytime after readers exhaust known entries.
Although `long poll` API brings great latency improvements on tailing reads, it is still a very low-level primitive.
It is still recommended to use high level API (e.g. [DistributedLog API](docs/api/distributedlog-api)) for tailing and streaming use cases.

See [Streaming Reads](https://bookkeeper.apache.org/docs/next/api/distributedlog-api) for more details.

##### Explicit LAC

Prior to 4.5.0, the `LAC` is only advanced when subsequent entries are added. If there is no subsequent entries added,
the last entry written will not be visible to readers until the ledger is closed. High-level client (e.g. DistributedLog) or applications
has to work around this by writing some sort of `control records` to advance `LAC`.

In 4.5.0, a new `explicit lac` feature is introduced to periodically advance `LAC` if there are not subsequent entries added. This feature
can be enabled by setting `explicitLacInterval` to a positive value.

#### Performance

There are a lot for performance related bug fixes and improvements in 4.5.0. These changes includes:

- Upgraded netty from 3.x to 4.x to leverage buffer pooling and reduce memory copies.
- Moved developement from Java 7 to Java 8 to take advantage of Java 8 features.
- A lot of improvements around scheduling and threading on `bookies`.
- Delay ensemble change to improve tail latency.
- Parallel ledger recovery to improve the recovery speed.
- ...

We outlined following four changes as below. For a complete list of performance improvements, please checkout the `full list of changes` at the end.

##### Netty 4 Upgrade

The major performance improvement introduced in 4.5.0, is upgrading netty from 3.x to [4.x](http://netty.io/wiki/new-and-noteworthy-in-4.0.html).

For more details, please read [upgrade guide](docs/admin/upgrade) about the netty related tips when upgrading bookkeeper from 4.4.0 to 4.5.0.

##### Delay Ensemble Change

`Ensemble Change` is a feature that Apache BookKeeper uses to achieve high availability. However it is an expensive metadata operation.
Especially when Apache BookKeeper is deployed in a multiple data-centers environment, losing a data center will cause churn of metadata
operations due to ensemble changes. `Delay Ensemble Change` is introduced in 4.5.0 to overcome this problem. Enabling this feature means
an `Ensemble Change` will only occur when clients can't receive enough valid responses to satisfy `ack-quorum` constraint. This feature
improves the tail latency.

To enable this feature, please set `delayEnsembleChange` to `true` on your clients.

##### Parallel Ledger Recovery

BookKeeper clients recovers entries one-by-one during ledger recovery. If a ledger has very large volumn of traffic, it will have
large number of entries to recover when client failures occur. BookKeeper introduces `parallel ledger recovery` in 4.5.0 to allow
batch recovery to improve ledger recovery speed.

To enable this feature, please set `enableParallelRecoveryRead` to `true` on your clients. You can also set `recoveryReadBatchSize`
to control the batch size of recovery read.

##### Multiple Journals

Prior to 4.5.0, bookies are only allowed to configure one journal device. If you want to have high write bandwidth, you can raid multiple
disks into one device and mount that device for jouranl directory. However because there is only one journal thread, this approach doesn't
actually improve the write bandwidth.

BookKeeper introduces multiple journal directories support in 4.5.0. Users can configure multiple devices for journal directories.

To enable this feature, please use `journalDirectories` rather than `journalDirectory`.

#### Operations

##### LongHierarchicalLedgerManager

Apache BookKeeper supports pluggable metadata store. By default, it uses Apache ZooKeeper as its metadata store. Among the zookeeper-based
ledger manager implementations, `HierarchicalLedgerManager` is the most popular and widely adopted ledger manager. However it has a major
limitation, which it assumes `ledger-id` is a 32-bits integer. It limits the number of ledgers to `2^32`.

`LongHierarchicalLedgerManager` is introduced to overcome this limitation.

See [Ledger Manager](docs/getting-started/concepts#ledger-manager) for more details.

##### Weight-based placement policy

`Rack-Aware` and `Region-Aware` placement polices are the two available placement policies in BookKeeper client. It places ensembles based
on users' configured network topology. However they both assume that all nodes are equal. `weight-based` placement is introduced in 4.5.0 to
improve the existing placement polices. `weight-based` placement was not built as separated polices. It is built in the existing placement policies.
If you are using `Rack-Aware` or `Region-Aware`, you can simply enable `weight-based` placement by setting `diskWeightBasedPlacementEnabled` to `true`.

##### Customized Ledger Metadata

A `Map<String, byte[]>` is introduced in ledger metadata in 4.5.0. Clients now are allowed to pass in a key/value map when creating ledgers.
This customized ledger metadata can be later on used by user defined placement policy. This extends the flexibility of bookkeeper API.

##### Add Prometheus stats provider

A new [Prometheus](https://prometheus.io/) [stats provider](https://github.com/apache/bookkeeper/tree/master/bookkeeper-stats-providers/prometheus-metrics-provider)
is introduce in 4.5.0. It simplies the metric collection when running bookkeeper on [kubernetes](https://kubernetes.io/).

##### Add more tools in BookieShell

`BookieShell` is the tool provided by Apache BooKeeper to operate clusters. There are multiple importants tools introduced in 4.5.0, for example, `decommissionbookie`,
`expandstorage`, `lostbookierecoverydelay`, `triggeraudit`.

For the complete list of commands in `BookieShell`, please read [BookKeeper CLI tool reference](docs/reference/cli).

### Full list of changes

#### JIRA

##### Sub-task
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-552'>BOOKKEEPER-552</a>] -         64 Bits Ledger ID Generation
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-553'>BOOKKEEPER-553</a>] -         New LedgerManager for 64 Bits Ledger ID Management in ZooKeeper
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-588'>BOOKKEEPER-588</a>] -         SSL support
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-873'>BOOKKEEPER-873</a>] -         Enhance CreatedLedger API to accept ledgerId as input
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-949'>BOOKKEEPER-949</a>] -         Allow entryLog creation even when bookie is in RO mode for compaction
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-965'>BOOKKEEPER-965</a>] -         Long Poll: Changes to the Write Path
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-997'>BOOKKEEPER-997</a>] -         Wire protocol change for supporting long poll
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1017'>BOOKKEEPER-1017</a>] -         Create documentation for ZooKeeper ACLs
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1086'>BOOKKEEPER-1086</a>] -         Ledger Recovery - Refactor PendingReadOp
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1087'>BOOKKEEPER-1087</a>] -         Ledger Recovery - Add a parallel reading request in PendingReadOp
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1088'>BOOKKEEPER-1088</a>] -         Ledger Recovery - Add a ReadEntryListener to callback on individual request
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1089'>BOOKKEEPER-1089</a>] -         Ledger Recovery - allow batch reads in ledger recovery
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1092'>BOOKKEEPER-1092</a>] -         Ledger Recovery - Add Test Case for Parallel Ledger Recovery
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1093'>BOOKKEEPER-1093</a>] -         Piggyback LAC on ReadResponse
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1094'>BOOKKEEPER-1094</a>] -         Long Poll - Server and Client Side Changes
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1095'>BOOKKEEPER-1095</a>] -         Long Poll - Client side changes
</li>
</ul>
                            
<b>Bug</b>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-852'>BOOKKEEPER-852</a>] -         Release LedgerDescriptor and master-key objects when not used anymore
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-903'>BOOKKEEPER-903</a>] -         MetaFormat BookieShell Command is not deleting UnderReplicatedLedgers list from the ZooKeeper
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-907'>BOOKKEEPER-907</a>] -         for ReadLedgerEntriesCmd, EntryFormatter should be configurable and HexDumpEntryFormatter should be one of them
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-908'>BOOKKEEPER-908</a>] -         Case to handle BKLedgerExistException
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-924'>BOOKKEEPER-924</a>] -         addEntry() is susceptible to spurious wakeups
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-927'>BOOKKEEPER-927</a>] -         Extend BOOKKEEPER-886 to LedgerHandleAdv too (BOOKKEEPER-886: Allow to disable ledgers operation throttling)
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-933'>BOOKKEEPER-933</a>] -         ClientConfiguration always inherits System properties
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-938'>BOOKKEEPER-938</a>] -         LedgerOpenOp should use digestType from metadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-939'>BOOKKEEPER-939</a>] -         Fix typo in bk-merge-pr.py
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-940'>BOOKKEEPER-940</a>] -         Fix findbugs warnings after bumping to java 8
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-952'>BOOKKEEPER-952</a>] -         Fix RegionAwarePlacementPolicy
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-955'>BOOKKEEPER-955</a>] -         in BookKeeperAdmin listLedgers method currentRange variable is not getting updated to next iterator when it has run out of elements
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-956'>BOOKKEEPER-956</a>] -         HierarchicalLedgerManager doesn&#39;t work for ledgerid of length 9 and 10 because of order issue in HierarchicalLedgerRangeIterator
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-958'>BOOKKEEPER-958</a>] -         ZeroBuffer readOnlyBuffer returns ByteBuffer with 0 remaining bytes for length &gt; 64k
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-959'>BOOKKEEPER-959</a>] -         ClientAuthProvider and BookieAuthProvider Public API used Protobuf Shaded classes
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-976'>BOOKKEEPER-976</a>] -         Fix license headers with &quot;Copyright 2016 The Apache Software Foundation&quot;
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-980'>BOOKKEEPER-980</a>] -         BookKeeper Tools doesn&#39;t process the argument correctly
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-981'>BOOKKEEPER-981</a>] -         NullPointerException in RackawareEnsemblePlacementPolicy while running in Docker Container
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-984'>BOOKKEEPER-984</a>] -          BookieClientTest.testWriteGaps tested
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-986'>BOOKKEEPER-986</a>] -         Handle Memtable flush failure
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-987'>BOOKKEEPER-987</a>] -         BookKeeper build is broken due to the shade plugin for commit ecbb053e6e
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-988'>BOOKKEEPER-988</a>] -         Missing license headers
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-989'>BOOKKEEPER-989</a>] -         Enable travis CI for bookkeeper git
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-999'>BOOKKEEPER-999</a>] -         BookKeeper client can leak threads
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1013'>BOOKKEEPER-1013</a>] -         Fix findbugs errors on latest master
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1018'>BOOKKEEPER-1018</a>] -         Allow client to select older V2 protocol (no protobuf)
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1020'>BOOKKEEPER-1020</a>] -         Fix Explicit LAC tests on master
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1021'>BOOKKEEPER-1021</a>] -         Improve the merge script to handle github reviews api
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1031'>BOOKKEEPER-1031</a>] -         ReplicationWorker.rereplicate fails to call close() on ReadOnlyLedgerHandle
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1044'>BOOKKEEPER-1044</a>] -         Entrylogger is not readding rolled logs back to the logChannelsToFlush list when exception happens while trying to flush rolled logs
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1047'>BOOKKEEPER-1047</a>] -         Add missing error code in ZK setData return path
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1058'>BOOKKEEPER-1058</a>] -         Ignore already deleted ledger on replication audit
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1061'>BOOKKEEPER-1061</a>] -         BookieWatcher should not do ZK blocking operations from ZK async callback thread
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1065'>BOOKKEEPER-1065</a>] -         OrderedSafeExecutor should only have 1 thread per bucket
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1071'>BOOKKEEPER-1071</a>] -         BookieRecoveryTest is failing due to a Netty4 IllegalReferenceCountException
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1072'>BOOKKEEPER-1072</a>] -         CompactionTest is flaky when disks are almost full
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1073'>BOOKKEEPER-1073</a>] -         Several stats provider related changes.
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1074'>BOOKKEEPER-1074</a>] -         Remove JMX Bean 
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1075'>BOOKKEEPER-1075</a>] -         BK LedgerMetadata: more memory-efficient parsing of configs
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1076'>BOOKKEEPER-1076</a>] -         BookieShell should be able to read the &#39;FENCE&#39; entry in the log
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1077'>BOOKKEEPER-1077</a>] -         BookKeeper: Local Bookie Journal and ledger paths
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1079'>BOOKKEEPER-1079</a>] -         shell lastMark throws NPE
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1098'>BOOKKEEPER-1098</a>] -         ZkUnderreplicationManager can build up an unbounded number of watchers
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1101'>BOOKKEEPER-1101</a>] -         BookKeeper website menus not working under https
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1102'>BOOKKEEPER-1102</a>] -         org.apache.bookkeeper.client.BookKeeperDiskSpaceWeightedLedgerPlacementTest.testDiskSpaceWeightedBookieSelectionWithBookiesBeingAdded is unreliable
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1103'>BOOKKEEPER-1103</a>] -         LedgerMetadataCreateTest bug in ledger id generation causes intermittent hang
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1104'>BOOKKEEPER-1104</a>] -         BookieInitializationTest.testWithDiskFullAndAbilityToCreateNewIndexFile testcase is unreliable
</li>
</ul>
                            
<b>Improvement</b>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-612'>BOOKKEEPER-612</a>] -         RegionAwarePlacement Policy
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-748'>BOOKKEEPER-748</a>] -         Move fence requests out of read threads
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-757'>BOOKKEEPER-757</a>] -         Ledger Recovery Improvement
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-759'>BOOKKEEPER-759</a>] -         bookkeeper: delay ensemble change if it doesn&#39;t break ack quorum requirement
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-772'>BOOKKEEPER-772</a>] -         Reorder read sequnce 
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-874'>BOOKKEEPER-874</a>] -         Explict LAC from Writer to Bookies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-881'>BOOKKEEPER-881</a>] -         upgrade surefire plugin to 2.19
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-887'>BOOKKEEPER-887</a>] -         Allow to use multiple bookie journals
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-922'>BOOKKEEPER-922</a>] -         Create a generic (K,V) map to store ledger metadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-935'>BOOKKEEPER-935</a>] -         Publish sources and javadocs to Maven Central
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-937'>BOOKKEEPER-937</a>] -         Upgrade protobuf to 2.6
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-944'>BOOKKEEPER-944</a>] -         Multiple issues and improvements to BK Compaction.
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-945'>BOOKKEEPER-945</a>] -         Add counters to track the activity of auditor and replication workers
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-946'>BOOKKEEPER-946</a>] -         Provide an option to delay auto recovery of lost bookies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-961'>BOOKKEEPER-961</a>] -         Assing read/write request for same ledger to a single thread
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-962'>BOOKKEEPER-962</a>] -         Add more journal timing stats
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-963'>BOOKKEEPER-963</a>] -         Allow to use multiple journals in bookie
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-964'>BOOKKEEPER-964</a>] -         Add concurrent maps and sets for primitive types
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-966'>BOOKKEEPER-966</a>] -         change the bookieServer cmdline to make conf-file and option co-exist
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-968'>BOOKKEEPER-968</a>] -         Entry log flushes happen on log rotation and cause long spikes in IO utilization
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-970'>BOOKKEEPER-970</a>] -         Bump zookeeper version to 3.5
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-971'>BOOKKEEPER-971</a>] -         update bk codahale stats provider version
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-998'>BOOKKEEPER-998</a>] -         Increased the max entry size to 5MB
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1001'>BOOKKEEPER-1001</a>] -         Make LocalBookiesRegistry.isLocalBookie() public
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1002'>BOOKKEEPER-1002</a>] -         BookieRecoveryTest can run out of file descriptors
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1003'>BOOKKEEPER-1003</a>] -         Fix TestDiskChecker so it can be used on /dev/shm
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1004'>BOOKKEEPER-1004</a>] -         Allow bookie garbage collection to be triggered manually from tests
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1007'>BOOKKEEPER-1007</a>] -         Explicit LAC: make the interval configurable in milliseconds instead of seconds
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1008'>BOOKKEEPER-1008</a>] -         Move to netty4
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1010'>BOOKKEEPER-1010</a>] -         Bump up Guava version to 20.0
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1022'>BOOKKEEPER-1022</a>] -         Make BookKeeperAdmin implement AutoCloseable
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1039'>BOOKKEEPER-1039</a>] -         bk-merge-pr.py ask to run findbugs and rat before merge
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1046'>BOOKKEEPER-1046</a>] -         Avoid long to Long conversion in OrderedSafeExecutor task submit
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1048'>BOOKKEEPER-1048</a>] -         Use ByteBuf in LedgerStorageInterface
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1050'>BOOKKEEPER-1050</a>] -         Cache journalFormatVersionToWrite when starting Journal
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1051'>BOOKKEEPER-1051</a>] -         Fast shutdown for GarbageCollectorThread
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1052'>BOOKKEEPER-1052</a>] -         Print autorecovery enabled or not in bookie shell
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1053'>BOOKKEEPER-1053</a>] -         Upgrade RAT maven version to 0.12 and ignore Eclipse project files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1055'>BOOKKEEPER-1055</a>] -         Optimize handling of masterKey in case it is empty
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1056'>BOOKKEEPER-1056</a>] -         Removed PacketHeader serialization/deserialization allocation
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1063'>BOOKKEEPER-1063</a>] -         Use executure.execute() instead of submit() to avoid creation of unused FutureTask
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1066'>BOOKKEEPER-1066</a>] -         Introduce GrowableArrayBlockingQueue
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1068'>BOOKKEEPER-1068</a>] -         Expose ByteBuf in LedgerEntry to avoid data copy
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1069'>BOOKKEEPER-1069</a>] -         If client uses V2 proto, set the connection to always decode V2 messages
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1083'>BOOKKEEPER-1083</a>] -         Improvements on OrderedSafeExecutor
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1084'>BOOKKEEPER-1084</a>] -         Make variables finale if necessary
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1085'>BOOKKEEPER-1085</a>] -         Introduce the AlertStatsLogger
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1090'>BOOKKEEPER-1090</a>] -         Use LOG.isDebugEnabled() to avoid unexpected allocations
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1096'>BOOKKEEPER-1096</a>] -         When ledger is deleted, along with leaf node all the eligible branch nodes also should be deleted in ZooKeeper.
</li>
</ul>
                
<b>New Feature</b>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-390'>BOOKKEEPER-390</a>] -         Provide support for ZooKeeper authentication
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-391'>BOOKKEEPER-391</a>] -         Support Kerberos authentication of bookkeeper
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-575'>BOOKKEEPER-575</a>] -         Bookie SSL support
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-670'>BOOKKEEPER-670</a>] -         Longpoll Read &amp; Piggyback Support
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-912'>BOOKKEEPER-912</a>] -         Allow EnsemblePlacementPolicy to choose bookies using ledger custom data (multitenancy support)
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-928'>BOOKKEEPER-928</a>] -         Add custom client supplied metadata field to LedgerMetadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-930'>BOOKKEEPER-930</a>] -         Option to disable Bookie networking
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-941'>BOOKKEEPER-941</a>] -         Introduce Feature Switches For controlling client and server behavior
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-948'>BOOKKEEPER-948</a>] -         Provide an option to add more ledger/index directories to a bookie
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-950'>BOOKKEEPER-950</a>] -         Ledger placement policy to accomodate different storage capacity of bookies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-969'>BOOKKEEPER-969</a>] -         Security Support
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-983'>BOOKKEEPER-983</a>] -         BookieShell Command for LedgerDelete
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-991'>BOOKKEEPER-991</a>] -         bk shell - Get a list of all on disk files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-992'>BOOKKEEPER-992</a>] -         ReadLog Command Enhancement
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1019'>BOOKKEEPER-1019</a>] -         Support for reading entries after LAC (causal consistency driven by out-of-band communications)
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1034'>BOOKKEEPER-1034</a>] -         When all disks are full, start Bookie in RO mode if RO mode is enabled 
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1067'>BOOKKEEPER-1067</a>] -         Add Prometheus stats provider
</li>
</ul>
                                            
<b>Story</b>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-932'>BOOKKEEPER-932</a>] -         Move to JDK 8
</li>
</ul>
                
<b>Task</b>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-931'>BOOKKEEPER-931</a>] -         Update the committers list on website
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-996'>BOOKKEEPER-996</a>] -         Apache Rat Check Failures
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1012'>BOOKKEEPER-1012</a>] -         Shade and relocate Guava
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1027'>BOOKKEEPER-1027</a>] -         Cleanup main README and main website page
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1038'>BOOKKEEPER-1038</a>] -         Fix findbugs warnings and upgrade to 3.0.4
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1043'>BOOKKEEPER-1043</a>] -         Upgrade Apache Parent Pom Reference to latest version
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1054'>BOOKKEEPER-1054</a>] -         Add gitignore file
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1059'>BOOKKEEPER-1059</a>] -         Upgrade to SLF4J-1.7.25
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1060'>BOOKKEEPER-1060</a>] -         Add utility to use SafeRunnable from Java8 Lambda
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1070'>BOOKKEEPER-1070</a>] -         bk-merge-pr.py use apache-rat:check goal instead of rat:rat
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1091'>BOOKKEEPER-1091</a>] -         Remove Hedwig from BookKeeper website page
</li>
</ul>
            
<b>Test</b>

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-967'>BOOKKEEPER-967</a>] -         Create new testsuite for testing RackAwareEnsemblePlacementPolicy using ScriptBasedMapping.
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1045'>BOOKKEEPER-1045</a>] -         Execute tests in different JVM processes
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1064'>BOOKKEEPER-1064</a>] -         ConcurrentModificationException in AuditorLedgerCheckerTest
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1078'>BOOKKEEPER-1078</a>] -         Local BookKeeper enhancements for testability
</li>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-1097'>BOOKKEEPER-1097</a>] -         GC test when no WritableDirs
</li>
</ul>
        
<b>Wish</b>
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-943'>BOOKKEEPER-943</a>] -         Reduce log level of AbstractZkLedgerManager for register/unregister ReadOnlyLedgerHandle
</li>
</ul>

#### Github

- [https://github.com/apache/bookkeeper/milestone/1](https://github.com/apache/bookkeeper/milestone/1)

