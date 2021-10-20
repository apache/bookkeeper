---
title: "BP-44: USE metrics"
issue: https://github.com/apache/bookkeeper/issues/2834
state: "Under Discussion"
release: "N/A"
---

### Motivation
Based on our experience (at Splunk) running many BookKeeper clusters in production, from very small to very large deployments (in terms of number of bookies, size of VMs and load) we have identified a number of short-comings with the current BookKeeper metrics that make it harder than it should be to identify bottlenecks in performance. The [USE method](https://www.brendangregg.com/usemethod.html) (Utilization, Saturation, Errors) is an effective strategy for diagnosing where bottlenecks in a system lie but the current metrics do not always expose metrics related to utilization and saturation. Also, even if you have a good mental model for how BookKeeper works internally, there are blindspots in the metrics that make it difficult to know what is happening at times.

Finally, many of the metrics are aggregated, such as journal and DbLedgerStorage. When these components are configured with multiple directories, it is currently not possible to inspect the metrics of only a single journal or DbLedgerStorage instance. One bad volume can be hard to identify.

This BP proposes a number of improvements to help identify performance issues in production. The objective is for a human operator to rapidly see where a bottleneck exists and where it exists by using utilization and saturation metrics.

#### Utilization
Ultimately all work in a bookie is performed by threads. Some operations are performed by only a single thread while others are spread out over multiple threads. Some threads play a core role in the critical path of reads and writes and when fully utilized indicate a bottleneck. We propose that each thread reports the time that it is busy which will allow time utilization to be calculated. Because all file i/o is synchronous, thread utilization is a very close approximation of resource utilization for a bookie. For a thread whose sole role is file i/o, if it is 100% utilized then it can take little to no more work.

For example, if the Force Write thread is reporting 100% time utilization we’ll know that it has become a bottleneck and to look further into the force write metrics.

Likewise, some operations are in the critical path that are executed across multiple threads. For example, with DbLedgerStorage, flushes are in the critical path for writes and the work is performed across at least two threads (Sync thread, DbStorage thread). Knowing the time utilization of the Sync Thread and the DbStorage thread is useful, but knowing the time utilization of flushes as a whole (regardless of thread) is even more useful in this particular case. Once flushes are being performed 100% of the time we know that we have a write bottleneck in DbLedgerStorage.

So utilization metrics of both threads and operations can be extremely useful and often combining the two give even more insight.

#### Saturation
Metrics that indicate saturation are things like rejected requests, full queues/caches. We need to ensure that there are no blindspots for saturation metrics.

Once we start getting saturation indicators we have strong signals of a bottleneck existing. It may be a high level metric (rejected requests) which only tells us we have a problem, or a component level metric (journal queue length) that tells us the locality of a bottleneck. We also need to know things like the queue size bounds in order to detect when a component is fully saturated, so emitting metrics for certain configurations is useful.

#### Component and Thread Labels
Work is parallelized by thread pools and by using multiple directories that allow for multiple journal instances and multiple ledger storage instances. We have seen in production cases where a single bad volume has a large negative impact on BookKeeper performance. Being able to pick apart metrics by the thread and component instance can be extremely helpful in diagnosing issues that only affect one journal/ledger storage instance or a subset of threads.

#### Sub-Operation Time
In some cases a single operation can consist of multiple smaller operations. For example, a DbLedgerStorage flush involves flushing two RocksDB instances and entry log files. The only way to currently confirm whether RocksDB has gotten into a bad state is by profiling on the machine/pod.

Adding extra metrics to cover sub-operations can help to diagnose or rule-out an issue quickly.

#### Read Cache Thrashing
Read cache thrashing can have a huge negative impact on bookie performance. We have most of the metrics already for detecting when it happens except for one problem: we don’t report read and write cache hits/misses separately.

When we separate out cache hits/misses, we can start calculating the actual read cache hit-to-miss rate and compare that to the readahead batch sizes and calculate that the miss rate is too high (indicating thrashing).

### The Current State of Affairs

#### Utilization
The OpsStatsLogger gives us visibility into some time utilization metrics by using the “_sum” suffix metric for latency based OpStatsLogger metrics. We can’t see the utilization of each thread, just the thread pool as a whole. Knowing the number of threads in the pool allows us to calculate the % time utilization (given that each cluster may have a different size of thread pool this is inconvenient).

Operations such as journal flushes, journal force writes and DbLedgerStorage already record latencies with OpStatsLoggers which allows us to calculate utilization of IO operations as long as we know the number of journal/ledger storage directories (again, inconvenient).

Blindspots:
- Write/read/high priority thread pool
- Utilization of each thread
- Journal
  - Drill-down by journal instance
  - Utilization of the threads (they do other work such as taking work from queues and placing work on queues)
- Ledger Storage
  - Drill-down by ledger storage instance
  - Where is time spent on reads? Locations index, read from entry log, readahead?
  - Where is time spent on flushes? Ledger index, locations index, entry log files?

#### Saturation
There are some existing indicators of saturation, such as the thread pool task queue lengths, journal queue length, force write queue length, DbLedgerStorage rejected writes. But there are some blindspots:

- Requests rejected due to full thread pool task queues are logged as request failures. More helpful is a specific metric for rejected read and write requests
- Queue lengths are in aggregate, need to be able to drill-down by thread/journal/ledger storage instance.
- We don’t have metrics for things like maximum queue/cache sizes so we can’t calculate when a queue or a cache is full.
- Netty can become saturated, tracking the number of unwritable channels, bytes for unwritable can help diagnose Netty saturation


### Public Interfaces
N/A
### Proposed Changes

#### Thread scoped OpStatsLogger and Counter
Add a new “thread scoped” variant of the OpStatsLogger and the Counter where each adds labels:
- threadPool (or just the thread name for lone threads such as a journal thread)
- thread (the thread ordinal, for example a read thread pool with four threads, they are numbers 0-3)

The following methods are added to the StatsLogger interface:
```
OpStatsLogger getThreadScopedOpStatsLogger(String name)
Counter getThreadScopedCounter(String name)
```

The OpStatsLogger and Counter interfaces remain unchanged.

For each thread that mutates a thread scoped OpStatsLogger, internally it will end up registering one OpStatsLogger per thread. For example, with four read threads, then the following metrics for successful task executions would be created:

```
bookkeeper_server_BookieReadThreadPool_task_execution_sum{success="true", thread="0", threadPool="BookieReadThreadPool"}
bookkeeper_server_BookieReadThreadPool_task_execution_sum{success="true", thread="1", threadPool="BookieReadThreadPool"}
bookkeeper_server_BookieReadThreadPool_task_execution_sum{success="true", thread="2", threadPool="BookieReadThreadPool"}
bookkeeper_server_BookieReadThreadPool_task_execution_sum{success="true", thread="3", threadPool="BookieReadThreadPool"}
```

In order for thread scoped metrics to know which thread pool and thread ordinal the metrics of each thread should be labelled with, a thread registry is maintained. After each thread is started, the first work it performs is registering itself with a thread registry object that maps thread id -> {threadPool, thread}. In some cases this work can be performed outside of the thread itself, such as in a thread factory.

Each thread scoped metric registers per-thread metrics lazily. For example, when a thread scoped counter is incremented by a given thread, the first time that occurs on that given thread the thread labels are retrieved from the thread registry and a counter for that thread is created with the right labels and registered with the provider. In the case that the calling thread is unregistered, a default metric is used. Per thread OpStatsLoggers/Counters are not visible externally and are stored inside the thread scoped metric using a thread-local variable.

With these new variants, we can now replace calls to `getCounter` with `getThreadScopedCounter` and `getOpsStatsLogger` with `getThreadScopedOpsStatsLogger` for the metrics where we want to be able to drill down by thread pool and thread.

#### How to Report Time Spent (For utilization calculation)
We want to be able to report time spent by each thread as a whole, but also on operations and sub-operations. The simplest way to do this is simply measure elapsed time by taking a System.nanoTime() before the work and then logging the elapsed time via an OpStatsLogger or a Counter.

One challenge regarding this simple method is when operations take a long time, like a journal fsync or a ledger flush on a heavily overloaded bookie. Long execution times can cause per second calculations to go to 0 then to spike to very high levels (above 100% utilization). This can be mitigated by using larger windows (like 1 minute windows rather than 1 second windows) and heavily loaded systems don’t tend to fluctuate too much from second to second.

##### Discarded time measurement alternatives:
Use a sampling technique: too costly and complex.
Use proc filesystem: not portable, only thread level utilization metrics.

#### Per Component Instance labels
All journal metrics get the additional label of `journalIndex`.
All DbLedgerStorage metrics get the additional label of `ledgerDir`.

This does not prevent aggregating metrics of the journal or DbLedgerStorage as a whole but does allow for drill-down.

### New Metrics List

The following new metrics to be added to cover saturation/utilization or blindspots.

Saturation:
- bookkeeper_server_ADD_ENTRY_REJECTED (thread scoped counter)
- bookkeeper_server_READ_ENTRY_REJECTED (thread scoped counter)

Thread time utilization:
- bookie_sync_thread_time (thread scoped counter)
- bookie_journal_journal_thread_time  (thread scoped counter, journalIndex label)
- bookie_journal_force_write_thread_time (thread scoped counter, journalIndex label)
- bookie_journal_callback_thread_time (thread scoped counter, journalIndex label)
- bookie_db_storage_thread_time (thread scoped counter, ledgerDir label)

Operation time spent. Counters for high frequency ops and OpStatsLoggers for less frequent ops:
- bookie_read_locations_index_time (thread scoped counter, ledgerDir label)
- bookie_read_entrylog_time (thread scoped counter, ledgerDir label)
- bookie_readahead_time (thread scoped counter, ledgerDir label)
- bookie_flush_entrylog (thread scoped OpStatsLogger, ledgerDir label)
- bookie_flush_locations_index(thread scoped OpStatsLogger, ledgerDir label)
- bookie_flush_ledger_index(thread scoped OpStatsLogger, ledgerDir label)

Queue/cache max sizes, number of dirs, thread counts (useful for calculations such as current queue length vs max size and useful for operators to see on dashboards). All gauges based on config values or calculated values such as write cache being 25% of direct memory:
- bookkeeper_server_<OrderedExecutor name>_threads
- bookkeeper_server_<OrderedExecutor name>_max_queue_size
- bookie_JOURNAL_DIRS
- bookie_ledger_num_dirs
- bookie_JOURNAL_QUEUE_MAX_SIZE
- bookie_write_cache_max_size

Other:
- bookie_write_cache_hits (thread scoped counter, ledgerDir label)
- bookie_write_cache_misses (thread scoped counter, ledgerDir label)

#### Breaking Changes
Some proposed changes are breaking:
1. Remove thread ordinal from OrderedExecutor based metric names and use the label “thread” instead. This makes it easier to work with when using modern observability tooling and inline with the rest of the proposed changes.
2. Replace cache hit and cache miss OpStatsLoggers with Counters. OpStatsLoggers are relatively expensive and cost far more CPU cycle than the actual accesses to the caches themselves. Latency for cache hits/misses is extremely low and recording these latencies is not worth the cost. Counters are far cheaper.

### Compatibility, Deprecation, and Migration Plan
Two changes are breaking and will cause existing dashboards or alerts based on the affected metrics to stop working. The affected release should document this changes in order to warn operators of this impact.

### Test Plan
These changes have been tested extensively using various sized deployments (including multi journal and ledger storage configurations) and loads to ensure that the utilization and saturation metrics are accurate and useful.

### Rejected Alternatives

Time reporting alternatives discussed above.