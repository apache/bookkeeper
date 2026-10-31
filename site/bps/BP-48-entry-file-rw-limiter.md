---
title: "BP-48: Add R/W speed limit mechanism for entry files"
issue: https://github.com/apache/bookkeeper/issues/2992
state: "Under Discussion"
release: "n/a"
---

### Motivation

Under the current design of Bookkeeper, the entry data will eventually be persisted to the entry log file. The new entry data will continuously update the cache, causing the original data to be swapped out. Therefore, those old entry data can only be read through the disk-read operation. In the scenario of continuous high-throughput writing, the catch-up read will trigger the bookie process to perform disk read, making the disk enter a state of mixed read and write. The mixed read and write state will affect the writing of new comming entries and the reading of old entries at the same time, which will significantly reduce the inflow and outflow throughput of the entire cluster.

By combining the cluster maintenance experience in the production environment, we have summarized several locations where disk performance bottlenecks are prone to occur:

- Compaction
- Replication
- LedgerChecker

#### Compaction

`LedgerFragmentReplicator` is the low-level tool class that realizes the copying of fragments between bookies. All operations that need to complete ledger copying will eventually be subordinated to this method, especially the replication process mentioned here. For example, when a bookie node in the cluster exits unexpectedly, the auditor will later find all ledgers on the bookie and publish a series of replication tasks. Then the worker will find the existing replicas of these ledgers in the cluster and replicate them one by one, so that the number of replicas will eventually be restored to the write quorum. In this mode, if the data stored in the disconnected bookie is large, the amount of ledgers to copy will also be large. 

In addition to this, there is another consideration. Since the workers will actively compete for the replication tasks, all the workers will immediately start replicating the ledger when the auditor finishes publishing these tasks. This behavior mode has an obvious impact on the cluster performance. The sudden increase in R/W requests due to replication will affect the ordinary R/W requests at the same time. From the monitoring point of view, the read and write delays will increase. If we want to reduce the impact due to replication, we'd better add a speed limit here.

#### Replication

`EntryLogger#scanEntryLog` can help us scan an entry file and find the entry that meets the requirements. In the compaction process, this method is crucial, because all target entry files will be scanned once to find those entries that have not been deleted, and append these entries to the latest entry file to complete the compaction process. If we look closely, we can find that there are already some parameters to control the write rate of the entry, i.e. `isThrottleByBytes`, `compactionRateByEntries` and `compactionRateByBytes`. However only limiting the write rate does not limit the entire compaction process. Take a scenario as an example, when 80% of the entries in an entry file have expired and only 20% of the entries need to be retained, it means that compaction takes a lot of time to read from the disk. During disk reading, the write speed limit for the remaining 20% ​​of the data is somewhat trivial. Therefore, in order to limit the progress of the entire compaction, we also need to add a speed limit for scanning entry files.

There is another advantage to adding speed limit to entry file scanning. As we all know, entry is composed of meta information and payload data. When we need to count ledger information such as the number of entries and space occupied like `EntryLogger#getEntryLogMetadata`, we also need to traverse the all entries again. But the sudden increase of a large number of read requests to the disk can easily make ioutil reach 100%, so that real-time writes will be affected. After we increase the speed limit, the problems caused by this will also be solved.

#### LedgerChecker

The `LedgerChecker` utility class provides operations for checking whether a `LedgerFragment` object is readable. This method is mainly used in two scenarios:

- After the worker obtains the replication task, the worker will first check the ledger to confirm whether a real copy is necessary.
- The auditor will periodically check the full amount of ledgers to see if any fragments are missing and publish replication tasks.

Specifically to determine whether a fragment is readable, in fact, the underlying logic will also convert to whether the entries on the fragment can be read, so it eventually evolves into calling `BookieClient#readEntry`. In our production practice, the `Auditor#checkAllLedgers` operation has a very significant impact on cluster performance, because this operation needs to read all fragments of the entire cluster at least once. For this periodic check, it should not affect the ordinary R/W operations of the cluster. It can be executed quietly as a background thread. Therefore, it is necessary to add a speed limit in the check process.

### Public Interfaces

- Add new server configurations for rate limitation.
- Add new metrics to observe the performance changes while limiting speed.

### Proposed Changes

Add rate limiters and corresponding monitoring metrics in the main process at the location discussed above.

### Compatibility, Deprecation, and Migration Plan

Since this part of the modification is brand new, it does not affect the original logical semantics, and we will not turn on the speed limit by default, so there will be no side effects during the rolling upgrade. 

When rolling back from a higher version with the speed limit function enabled to a lower version, it is equivalent to losing the corresponding speed limit function, which has no impact on the availability and stability of the cluster.

### Test Plan

These changes have been extensively tested and applied on our production clusters to ensure that rate limiting features and monitoring metrics are accurate and useful.

### Rejected Alternatives

None