---
title: Apache BookKeeper 4.5.0 Release Notes
---

This is the fifth release of BookKeeper as an Apache Top Level Project!

The 4.5.0 release incorporates hundreds of new fixes, improvements, and features since previous major release, 4.4.0,
which was released over a year ago. It is a big milestone in Apache BookKeeper community, converging from three
main branches (Salesforce, Twitter and Yahoo).

Apache BookKeeper users are encouraged to upgrade to 4.5.0. The technical details of this release are summarized
below.

## Highlights

The main features in 4.5.0 cover are around following areas:

- Dependencies Upgrade
- Security
- Public API
- Performance
- Operations

### Dependencies Upgrade

Here is a list of dependencies upgraded in 4.5.0:

- Moved the developement from Java 7 to Java 8.
- Upgrade Protobuf to `2.6`.
- Upgrade ZooKeeper from `3.4` to `3.5`.
- Upgrade Netty to `4.1`.
- Upgrade Guava to `20.0`.
- Upgrade SLF4J to `1.7.25`.
- Upgrade Codahale to `3.1.0`.

### Security

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

For more details, have a look at [BookKeeper Security](../../security/overview).

### Public API

There are multiple new client features introduced in 4.5.0.

#### LedgerHandleAdv

The [Ledger API] is the low level API provides by BookKeeper for interacting with `ledgers` in a bookkeeper cluster.
It is simple but not flexible on ledger id or entry id generation. Apache BookKeeper introduces `LedgerHandleAdv`
as an extension of existing `LedgerHandle` for advanced usage. The new `LedgerHandleAdv` allows applications providing
its own `ledger-id` and assigning `entry-id` on adding entries.

See [Ledger Advanced API](../../api/ledger-adv-api) for more details.

#### Long Poll

`Long Poll` is a main feature that [DistributedLog](https://distributedlog.io) uses to achieve low-latency tailing.
This big feature has been merged back in 4.5.0 and available to BookKeeper users.

This feature includes two main changes, one is `LastAddConfirmed` piggyback, while the other one is a new `long poll` read API.

The first change piggyback the latest `LastAddConfirm` along with the read response, so your `LastAddConfirmed` will be automatically advanced
when your read traffic continues. It significantly reduces the traffic to explicitly polling `LastAddConfirmed` and hence reduces the end-to-end latency.

The second change provides a new `long poll` read API, allowing tailing-reads without polling `LastAddConfirmed` everytime after readers exhaust known entries.
Although `long poll` API brings great latency improvements on tailing reads, it is still a very low-level primitive.
It is still recommended to use high level API (e.g. [DistributedLog API](../../api/distributedlog-api)) for tailing and streaming use cases.

See [Streaming Reads](https://bookkeeper.apache.org/distributedlog/docs/latest/user_guide/design/main.html#streaming-reads) for more details.

#### Explicit LAC

Prior to 4.5.0, the `LAC` is only advanced when subsequent entries are added. If there is no subsequent entries added,
the last entry written will not be visible to readers until the ledger is closed. High-level client (e.g. DistributedLog) or applications
has to work around this by writing some sort of `control records` to advance `LAC`.

In 4.5.0, a new `explicit lac` feature is introduced to periodically advance `LAC` if there are not subsequent entries added. This feature
can be enabled by setting `explicitLacInterval` to a positive value.

### Performance

There are a lot for performance related bug fixes and improvements in 4.5.0. These changes includes:

- Upgraded netty from 3.x to 4.x to leverage buffer pooling and reduce memory copies.
- Moved developement from Java 7 to Java 8 to take advantage of Java 8 features.
- A lot of improvements around scheduling and threading on `bookies`.
- Delay ensemble change to improve tail latency.
- Parallel ledger recovery to improve the recovery speed.
- ...

We outlined following four changes as below. For a complete list of performance improvements, please checkout the `full list of changes` at the end.

#### Netty 4 Upgrade

The major performance improvement introduced in 4.5.0, is upgrading netty from 3.x to [4.x](http://netty.io/wiki/new-and-noteworthy-in-4.0.html).

For more details, please read [upgrade guide](../../admin/upgrade) about the netty related tips when upgrading bookkeeper from 4.4.0 to 4.5.0.

#### Delay Ensemble Change

`Ensemble Change` is a feature that Apache BookKeeper uses to achieve high availability. However it is an expensive metadata operation.
Especially when Apache BookKeeper is deployed in a multiple data-centers environment, losing a data center will cause churn of metadata
operations due to ensemble changes. `Delay Ensemble Change` is introduced in 4.5.0 to overcome this problem. Enabling this feature means
an `Ensemble Change` will only occur when clients can't receive enough valid responses to satisfy `ack-quorum` constraint. This feature
improves the tail latency.

To enable this feature, please set `delayEnsembleChange` to `true` on your clients.

#### Parallel Ledger Recovery

BookKeeper clients recovers entries one-by-one during ledger recovery. If a ledger has very large volumn of traffic, it will have
large number of entries to recover when client failures occur. BookKeeper introduces `parallel ledger recovery` in 4.5.0 to allow
batch recovery to improve ledger recovery speed.

To enable this feature, please set `enableParallelRecoveryRead` to `true` on your clients. You can also set `recoveryReadBatchSize`
to control the batch size of recovery read.

#### Multiple Journals

Prior to 4.5.0, bookies are only allowed to configure one journal device. If you want to have high write bandwidth, you can raid multiple
disks into one device and mount that device for jouranl directory. However because there is only one journal thread, this approach doesn't
actually improve the write bandwidth.

BookKeeper introduces multiple journal directories support in 4.5.0. Users can configure multiple devices for journal directories.

To enable this feature, please use `journalDirectories` rather than `journalDirectory`.

### Operations

#### LongHierarchicalLedgerManager

Apache BookKeeper supports pluggable metadata store. By default, it uses Apache ZooKeeper as its metadata store. Among the zookeeper-based
ledger manager implementations, `HierarchicalLedgerManager` is the most popular and widely adopted ledger manager. However it has a major
limitation, which it assumes `ledger-id` is a 32-bits integer. It limits the number of ledgers to `2^32`.

`LongHierarchicalLedgerManager` is introduced to overcome this limitation.

See [Ledger Manager](../../getting-started/concepts/#ledger-manager) for more details.

#### Weight-based placement policy

`Rack-Aware` and `Region-Aware` placement polices are the two available placement policies in BookKeeper client. It places ensembles based
on users' configured network topology. However they both assume that all nodes are equal. `weight-based` placement is introduced in 4.5.0 to
improve the existing placement polices. `weight-based` placement was not built as separated polices. It is built in the existing placement policies.
If you are using `Rack-Aware` or `Region-Aware`, you can simply enable `weight-based` placement by setting `diskWeightBasedPlacementEnabled` to `true`.

#### Customized Ledger Metadata

A `Map<String, byte[]>` is introduced in ledger metadata in 4.5.0. Clients now are allowed to pass in a key/value map when creating ledgers.
This customized ledger metadata can be later on used by user defined placement policy. This extends the flexibility of bookkeeper API.

#### Add Prometheus stats provider

A new [Prometheus](https://prometheus.io/) [stats provider](https://github.com/apache/bookkeeper/tree/master/bookkeeper-stats-providers/prometheus-metrics-provider)
is introduce in 4.5.0. It simplies the metric collection when running bookkeeper on [kubernetes](https://kubernetes.io/).

#### Add more tools in BookieShell

`BookieShell` is the tool provided by Apache BooKeeper to operate clusters. There are multiple importants tools introduced in 4.5.0, for example, `decommissionbookie`,
`expandstorage`, `lostbookierecoverydelay`, `triggeraudit`.

For the complete list of commands in `BookieShell`, please read [BookKeeper CLI tool reference](../../reference/cli).

## Full list of changes

### JIRA

#### Sub-task
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
                            
#### Bug
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
                            
#### Improvement
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
                
#### New Feature
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
                                            
#### Story
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-932'>BOOKKEEPER-932</a>] -         Move to JDK 8
</li>
</ul>
                
#### Task
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
            
#### Test
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
        
#### Wish
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/BOOKKEEPER-943'>BOOKKEEPER-943</a>] -         Reduce log level of AbstractZkLedgerManager for register/unregister ReadOnlyLedgerHandle
</li>
</ul>

### Github

- [https://github.com/apache/bookkeeper/milestone/1](https://github.com/apache/bookkeeper/milestone/1)
