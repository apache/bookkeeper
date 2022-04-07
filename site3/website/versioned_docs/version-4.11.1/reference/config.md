---
id: config
title: BookKeeper configuration
---

The table below lists parameters that you can set to configure bookies. All configuration takes place in the `bk_server.conf` file in the `bookkeeper-server/conf` directory of your [BookKeeper installation](../getting-started/installation).



## Server parameters

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| bookiePort | The port that the bookie server listens on. | 3181 | 
| allowMultipleDirsUnderSameDiskPartition | Configure the bookie to allow/disallow multiple ledger/index/journal directories in the same filesystem disk partition |  | 
| listeningInterface | The network interface that the bookie should listen on. If not set, the bookie will listen on all interfaces. | eth0 | 
| advertisedAddress | Configure a specific hostname or IP address that the bookie should use to advertise itself to<br />clients. If not set, bookie will advertised its own IP address or hostname, depending on the<br />`listeningInterface` and `useHostNameAsBookieID` settings.<br /> | eth0 | 
| allowLoopback | Whether the bookie is allowed to use a loopback interface as its primary<br />interface (the interface it uses to establish its identity). By default, loopback interfaces are *not* allowed as the primary interface.<br /><br />Using a loopback interface as the primary interface usually indicates a configuration error. It's fairly common in some VPS setups, for example, to not configure a hostname or to have the hostname resolve to 127.0.0.1. If this is the case, then all bookies in the cluster will establish their identities as 127.0.0.1:3181, and only one will be able to join the cluster. For VPSs configured like this, you should explicitly set the listening interface.<br /> | false | 
| useHostNameAsBookieID | Whether the bookie should use its hostname to register with the ZooKeeper coordination service. When `false`, the bookie will use its IP address for the registration. | false | 
| useShortHostName | Whether the bookie should use short hostname or [FQDN](https://en.wikipedia.org/wiki/Fully_qualified_domain_name) hostname for registration and ledger metadata when `useHostNameAsBookieID` is enabled. | false | 
| allowEphemeralPorts | Whether the bookie is allowed to use an ephemeral port (port 0) as its server port. By default, an ephemeral port is not allowed. Using an ephemeral port as the service port usually indicates a configuration error. However, in unit tests, using an ephemeral port will address port conflict problems and allow running tests in parallel. | false | 
| enableLocalTransport | Whether allow the bookie to listen for BookKeeper clients executed on the local JVM. | false | 
| disableServerSocketBind | Whether allow the bookie to disable bind on network interfaces, this bookie will be available only to BookKeeper clients executed on the local JVM. | false | 
| bookieDeathWatchInterval | Interval to watch whether bookie is dead or not, in milliseconds. | 1000 | 
| extraServerComponents | Configure a list of extra server components to enable and load on a bookie server. This provides a plugin mechanism to run extra server components along with a bookie server. |  | 
| ignoreExtraServerComponentsStartupFailures | Whether the bookie should ignore startup failures on loading server components specified by `extraServerComponents`. | false | 


## Worker thread settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| numAddWorkerThreads | The number of threads that handle write requests. if zero, writes are handled by [Netty threads](//netty.io/wiki/thread-model.html) directly. | 1 | 
| numReadWorkerThreads | The number of threads that handle read requests. If zero, reads are handled by [Netty threads](//netty.io/wiki/thread-model.html) directly. | 8 | 
| numLongPollWorkerThreads | The number of threads that handle long poll requests. If zero, long poll requests are handled by [Netty threads](//netty.io/wiki/thread-model.html) directly. |  | 
| numJournalCallbackThreads | The number of threads that handle journal callbacks. If zero, journal callbacks are executed directly on force write threads. | 1 | 
| numHighPriorityWorkerThreads | The number of threads that should be used for high priority requests (i.e. recovery reads and adds, and fencing). If zero, reads are handled by [Netty threads](//netty.io/wiki/thread-model.html) directly. | 8 | 
| maxPendingAddRequestsPerThread | If read worker threads are enabled, limit the number of pending requests, to avoid the executor queue to grow indefinitely. If zero or negative, the number of pending requests is unlimited. | 10000 | 
| maxPendingReadRequestsPerThread | If add worker threads are enabled, limit the number of pending requests, to avoid the executor queue to grow indefinitely. If zero or negative, the number of pending requests is unlimited. | 10000 | 
| enableBusyWait | Option to enable busy-wait settings. Default is false.<br />WARNING: This option will enable spin-waiting on executors and IO threads in order to reduce latency during<br />context switches. The spinning will consume 100% CPU even when bookie is not doing any work. It is recommended to<br />reduce the number of threads in the main workers pool and Netty event loop to only have few CPU cores busy.<br /> |  | 


## Long poll settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| requestTimerTickDurationMs | The tick duration for long poll request timer, in milliseconds. See [HashedWheelTimer](//netty.io/4.1/api/io/netty/util/HashedWheelTimer.html) for more details. | 10 | 
| requestTimerNumTicks | The number of ticks per wheel for long poll request timer. See [HashedWheelTimer](//netty.io/4.1/api/io/netty/util/HashedWheelTimer.html) for more details. | 1024 | 


## Read-only mode support

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| readOnlyModeEnabled | If all ledger directories configured are full, then support only read requests for clients. If "readOnlyModeEnabled=true" then on all ledger disks full, bookie will be converted to read-only mode and serve only read requests. Otherwise the bookie will be shutdown. By default this will be disabled. | true | 
| forceReadOnlyBookie | Whether the bookie is force started in read only mode or not. | false | 
| persistBookieStatusEnabled | Persist the bookie status locally on the disks. So the bookies can keep their status upon restarts. | false | 


## Netty server settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| serverTcpNoDelay | This settings is used to enabled/disabled Nagle's algorithm, which is a means of improving the efficiency of TCP/IP networks by reducing the number of packets that need to be sent over the network.<br /><br />If you are sending many small messages, such that more than one can fit in a single IP packet, setting server.tcpnodelay to false to enable Nagle algorithm can provide better performance.<br /> | true | 
| serverSockKeepalive | This setting is used to send keep-alive messages on connection-oriented sockets. | true | 
| serverTcpLinger | The socket linger timeout on close. When enabled, a close or shutdown will not return until all queued messages for the socket have been successfully sent or the linger timeout has been reached. Otherwise, the call returns immediately and the closing is done in the background. |  | 
| byteBufAllocatorSizeInitial | The Recv ByteBuf allocator initial buf size. | 65536 | 
| byteBufAllocatorSizeMin | The Recv ByteBuf allocator min buf size. | 65536 | 
| byteBufAllocatorSizeMax | The Recv ByteBuf allocator max buf size. | 1048576 | 
| nettyMaxFrameSizeBytes | The maximum netty frame size in bytes. Any message received larger than this will be rejected, so when the client-side attempt to send more than the default size bytes, it should set up the corresponding parameter `setNettyMaxFrameSizeBytes(int maxSize)`, pay attention to the parameter should be less than the value of server-side. | 5242880 | 


## Http server settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| httpServerEnabled | The flag enables/disables starting the admin http server. | false | 
| httpServerPort | The http server port to listen on if `httpServerEnabled` is set to true. | 8080 | 
| httpServerHost | The http server host to listen on if `httpServerEnabled` is set to true. | 0.0.0.0 | 


## Security settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| bookieAuthProviderFactoryClass | The bookie authentication provider factory class name. If this is null, no authentication will take place. |  | 
| permittedStartupUsers | The list of users are permitted to run the bookie process. Any users can run the bookie process if it is not set.<br /><br />Example settings - "permittedStartupUsers=user1,user2,user3"<br /> |  | 


## TLS settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| tlsProvider | TLS Provider (JDK or OpenSSL) | OpenSSL | 
| tlsProviderFactoryClass | The path to the class that provides security. | org.apache.bookkeeper.tls.TLSContextFactory | 
| tlsClientAuthentication | Type of security used by server. | true | 
| tlsKeyStoreType | Bookie Keystore type. | JKS | 
| tlsKeyStore | Bookie Keystore location (path). |  | 
| tlsKeyStore | Bookie Keystore location (path). |  | 
| tlsKeyStorePasswordPath | Bookie Keystore password path, if the keystore is protected by a password. |  | 
| tlsTrustStoreType | Bookie Truststore type. |  | 
| tlsTrustStore | Bookie Truststore location (path). |  | 
| tlsTrustStorePasswordPath | Bookie Truststore password path, if the truststore is protected by a password. |  | 
| tlsCertificatePath | Bookie TLS certificate path. |  | 


## Journal settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| journalDirectories | The directories to which Bookkeeper outputs its write-ahead log (WAL). Could define multi directories to store write head logs, separated by ','.<br />For example:<br /> journalDirectories=/tmp/bk-journal1,/tmp/bk-journal2<br />If journalDirectories is set, bookies will skip journalDirectory and use this setting directory.<br /> | /tmp/bk-journal | 
| journalDirectory | @Deprecated since 4.5.0, in favor of using `journalDirectories`.<br /><br />The directory to which Bookkeeper outputs its write-ahead log (WAL).<br /> | /tmp/bk-txn | 
| journalFormatVersionToWrite | The journal format version to write.<br />Available formats are 1-5:<br /> 1: no header<br /> 2: a header section was added<br /> 3: ledger key was introduced<br /> 4: fencing key was introduced<br /> 5: expanding header to 512 and padding writes to align sector size configured by `journalAlignmentSize`<br /> 6: persisting explicitLac is introduced<br /><br />By default, it is `6`.<br />If you'd like to disable persisting ExplicitLac, you can set this config to < `6` and also fileInfoFormatVersionToWrite should be set to 0. If there is mismatch then the serverconfig is considered invalid.<br />You can disable `padding-writes` by setting journal version back to `4`. This feature is available in 4.5.0 and onward versions.<br /> | 6 | 
| journalMaxSizeMB | Max file size of journal file, in mega bytes. A new journal file will be created when the old one reaches the file size limitation. | 2048 | 
| journalMaxBackups | Max number of old journal file to kept. Keep a number of old journal files would help data recovery in specia case. | 5 | 
| journalPreAllocSizeMB | How much space should we pre-allocate at a time in the journal. | 16 | 
| journalWriteBufferSizeKB | Size of the write buffers used for the journal. | 64 | 
| journalRemoveFromPageCache | Should we remove pages from page cache after force write | true | 
| journalSyncData | Should the data be fsynced on journal before acknowledgment.<br />By default, data sync is enabled to guarantee durability of writes. Beware - when disabling data sync in the bookie journal<br />might improve the bookie write performance, it will also introduce the possibility of data loss. With no fsync, the journal<br />entries are written in the OS page cache but not flushed to disk. In case of power failure, the affected bookie might lose<br />the unflushed data. If the ledger is replicated to multiple bookies, the chances of data loss are reduced though still present.<br /> | true | 
| journalAdaptiveGroupWrites | Should we group journal force writes, which optimize group commit for higher throughput. | true | 
| journalMaxGroupWaitMSec | Maximum latency to impose on a journal write to achieve grouping. | 2 | 
| journalBufferedWritesThreshold | Maximum writes to buffer to achieve grouping. | 524288 | 
| journalFlushWhenQueueEmpty | If we should flush the journal when journal queue is empty. | false | 
| journalAlignmentSize | All the journal writes and commits should be aligned to given size. If not, zeros will be padded to align to given size. | 512 | 
| journalBufferedEntriesThreshold | Maximum entries to buffer to impose on a journal write to achieve grouping. |  | 
| journalFlushWhenQueueEmpty | If we should flush the journal when journal queue is empty. | false | 
| journalQueueSize | Set the size of the journal queue. | 10000 | 


## Ledger storage settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| ledgerStorageClass | Ledger storage implementation class<br /><br />Options:<br /> - org.apache.bookkeeper.bookie.InterleavedLedgerStorage<br /> - org.apache.bookkeeper.bookie.SortedLedgerStorage<br /> - org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage<br /> | org.apache.bookkeeper.bookie.SortedLedgerStorage | 
| sortedLedgerStorageEnabled | @Deprecated in favor of using `ledgerStorageClass`<br /><br />Whether sorted-ledger storage enabled (default true)<br /> | true | 
| ledgerDirectories | The directory to which Bookkeeper outputs ledger snapshots. You can define multiple directories to store snapshots separated by a comma, for example `/tmp/data-dir1,/tmp/data-dir2`. | /tmp/bk-data | 
| indexDirectories | The directories in which index files are stored. If not specified, the value of [`ledgerDirectories`](#ledgerDirectories) will be used. | /tmp/bk-data | 
| minUsableSizeForIndexFileCreation | Minimum safe usable size to be available in index directory for bookie to create index file while replaying journal at the time of bookie start in readonly mode (in bytes) | 1073741824 | 
| minUsableSizeForEntryLogCreation | Minimum safe usable size to be available in ledger directory for bookie to create entry log files (in bytes).<br />This parameter allows creating entry log files when there are enough disk spaces, even when<br />the bookie is running at readonly mode because of the disk usage is exceeding `diskUsageThreshold`.<br />Because compaction, journal replays can still write data to disks when a bookie is readonly.<br /> | 1.2 * `logSizeLimit` | 
| minUsableSizeForHighPriorityWrites | Minimum safe usable size to be available in ledger directory for bookie to accept high priority writes even it is in readonly mode.<br /> | 1.2 * `logSizeLimit` | 
| flushInterval | When entryLogPerLedgerEnabled is enabled, checkpoint doesn't happens when a new active entrylog is created / previous one is rolled over. Instead SyncThread checkpoints periodically with 'flushInterval' delay (in milliseconds) in between executions. Checkpoint flushes both ledger entryLogs and ledger index pages to disk. Flushing entrylog and index files will introduce much random disk I/O. If separating journal dir and ledger dirs each on different devices, flushing would not affect performance. But if putting journal dir and ledger dirs on same device, performance degrade significantly on too frequent flushing. You can consider increment flush interval to get better performance, but you need to pay more time on bookie server restart after failure. This config is used only when entryLogPerLedgerEnabled is enabled. | 10000 | 
| allowStorageExpansion | Allow the expansion of bookie storage capacity. Newly added ledger and index directories must be empty. | false | 


## Entry log settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| logSizeLimit | Max file size of entry logger, in bytes. A new entry log file will be created when the old one reaches the file size limitation. | 2147483648 | 
| entryLogFilePreallocationEnabled | Enable/Disable entry logger preallocation | true | 
| flushEntrylogBytes | Entry log flush interval, in bytes. Setting this to 0 or less disables this feature and makes flush happen on log rotation. Flushing in smaller chunks but more frequently reduces spikes in disk I/O. Flushing too frequently may negatively affect performance. |  | 
| readBufferSizeBytes | The capacity allocated for [`BufferedReadChannel`]({{ site.javadoc_base_url }}/org/apache/bookkeeper/bookie/BufferedReadChannel)s, in bytes. | 512 | 
| writeBufferSizeBytes | The number of bytes used as capacity for the write buffer. | 65536 | 
| entryLogPerLedgerEnabled | Specifies if entryLog per ledger is enabled/disabled. If it is enabled, then there would be a active entrylog for each ledger. It would be ideal to enable this feature if the underlying storage device has multiple DiskPartitions or SSD and if in a given moment, entries of fewer number of active ledgers are written to the bookie. |  | 
| entrylogMapAccessExpiryTimeInSeconds | config specifying if the entrylog per ledger is enabled, then the amount of time EntryLogManagerForEntryLogPerLedger should wait for closing the entrylog file after the last addEntry call for that ledger, if explicit writeclose for that ledger is not received. | 300 | 
| maximumNumberOfActiveEntryLogs | in entryLogPerLedger feature, this specifies the maximum number of entrylogs that can be active at a given point in time. If there are more number of active entryLogs then the maximumNumberOfActiveEntryLogs then the entrylog will be evicted from the cache. | 500 | 
| entryLogPerLedgerCounterLimitsMultFactor | in EntryLogManagerForEntryLogPerLedger, this config value specifies the metrics cache size limits in multiples of entrylogMap cache size limits. | 10 | 


## Entry log compaction settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| compactionRate | The rate at which compaction will read entries. The unit is adds per second. | 1000 | 
| minorCompactionThreshold | Threshold of minor compaction. For those entry log files whose remaining size percentage reaches below this threshold will be compacted in a minor compaction. If it is set to less than zero, the minor compaction is disabled. | 0.2 | 
| minorCompactionInterval | Interval to run minor compaction, in seconds. If it is set to less than zero, the minor compaction is disabled. | 3600 | 
| compactionMaxOutstandingRequests | Set the maximum number of entries which can be compacted without flushing. When compacting, the entries are written to the entrylog and the new offsets are cached in memory. Once the entrylog is flushed the index is updated with the new offsets. This parameter controls the number of entries added to the entrylog before a flush is forced. A higher value for this parameter means more memory will be used for offsets. Each offset consists of 3 longs. This parameter should *not* be modified unless you know what you're doing. | 100000 | 
| majorCompactionThreshold | Threshold of major compaction. For those entry log files whose remaining size percentage reaches below this threshold will be compacted in a major compaction. Those entry log files whose remaining size percentage is still higher than the threshold will never be compacted. If it is set to less than zero, the minor compaction is disabled. | 0.8 | 
| majorCompactionInterval | Interval to run major compaction, in seconds. If it is set to less than zero, the major compaction is disabled. | 86400 | 
| isThrottleByBytes | Throttle compaction by bytes or by entries. | false | 
| compactionRateByEntries | Set the rate at which compaction will read entries. The unit is adds per second. | 1000 | 
| compactionRateByBytes | Set the rate at which compaction will read entries. The unit is bytes added per second. | 1000000 | 
| useTransactionalCompaction | Flag to enable/disable transactional compaction. If it is set to true, it will use transactional compaction, which uses<br />new entry log files to store entries after compaction; otherwise, it will use normal compaction, which shares same entry<br />log file with normal add operations.<br /> | false | 


## Garbage collection settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| gcWaitTime | How long the interval to trigger next garbage collection, in milliseconds. Since garbage collection is running in background, too frequent gc will heart performance. It is better to give a higher number of gc interval if there is enough disk capacity. | 1000 | 
| gcOverreplicatedLedgerWaitTime | How long the interval to trigger next garbage collection of overreplicated ledgers, in milliseconds. This should not be run very frequently since we read the metadata for all the ledgers on the bookie from zk. | 86400000 | 
| gcOverreplicatedLedgerMaxConcurrentRequests | Max number of concurrent requests in garbage collection of overreplicated ledgers. | 1000 | 
| isForceGCAllowWhenNoSpace | Whether force compaction is allowed when the disk is full or almost full. Forcing GC may get some space back, but may also fill up disk space more quickly. This is because new log files are created before GC, while old garbage log files are deleted after GC. | false | 
| verifyMetadataOnGC | Whether the bookie should double check if a ledger exists in metadata service prior to gc. | false | 


## Disk utilization

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| diskUsageThreshold | For each ledger dir, maximum disk space which can be used. Default is 0.95f. i.e. 95% of disk can be used at most after which nothing will be written to that partition. If all ledger dir partions are full, then bookie will turn to readonly mode if 'readOnlyModeEnabled=true' is set, else it will shutdown. Valid values should be in between 0 and 1 (exclusive).<br /> | 0.95 | 
| diskUsageWarnThreshold | The disk free space low water mark threshold. Disk is considered full when usage threshold is exceeded. Disk returns back to non-full state when usage is below low water mark threshold. This prevents it from going back and forth between these states frequently when concurrent writes and compaction are happening. This also prevent bookie from switching frequently between read-only and read-writes states in the same cases. | 0.95 | 
| diskUsageLwmThreshold | Set the disk free space low water mark threshold. Disk is considered full when usage threshold is exceeded. Disk returns back to non-full state when usage is below low water mark threshold. This prevents it from going back and forth between these states frequently when concurrent writes and compaction are happening. This also prevent bookie from switching frequently between read-only and read-writes states in the same cases.<br /> | 0.9 | 
| diskCheckInterval | Disk check interval in milliseconds. Interval to check the ledger dirs usage. | 10000 | 


## Sorted Ledger Storage Settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| skipListSizeLimit | The skip list data size limitation (default 64MB) in EntryMemTable | 67108864 | 
| skipListArenaChunkSize | The number of bytes we should use as chunk allocation for org.apache.bookkeeper.bookie.SkipListArena | 4194304 | 
| skipListArenaMaxAllocSize | The max size we should allocate from the skiplist arena. Allocations larger than this should be allocated directly by the VM to avoid fragmentation. | 131072 | 
| openFileLimit | Max number of ledger index files could be opened in bookie server. If number of ledger index files reaches this limitation, bookie server started to swap some ledgers from memory to disk. Too frequent swap will affect performance. You can tune this number to gain performance according your requirements.<br /> | 20000 | 
| fileInfoCacheInitialCapacity | The minimum total size of the internal file info cache table. Providing a large enough estimate at construction time avoids the need for expensive resizing operations later,<br />but setting this value unnecessarily high wastes memory. The default value is `1/4` of `openFileLimit` if openFileLimit is positive, otherwise it is 64.<br /> |  | 
| fileInfoMaxIdleTime | The max idle time allowed for an open file info existed in the file info cache. If the file info is idle for a long time, exceed the given time period. The file info will be<br />evicted and closed. If the value is zero or negative, the file info is evicted only when opened files reached `openFileLimit`.<br /> |  | 
| fileInfoFormatVersionToWrite | The fileinfo format version to write.<br />Available formats are 0-1:<br /> 0: Initial version<br /> 1: persisting explicitLac is introduced<br /><br />By default, it is `1`. If you'd like to disable persisting ExplicitLac, you can set this config to 0 and also journalFormatVersionToWrite should be set to < 6. If there is mismatch then the serverconfig is considered invalid.<br /> | 1 | 
| pageSize | Size of a index page in ledger cache, in bytes. A larger index page can improve performance writing page to disk, which is efficent when you have small number of ledgers and these ledgers have similar number of entries. If you have large number of ledgers and each ledger has fewer entries, smaller index page would improve memory usage.<br /> | 8192 | 
| pageLimit | How many index pages provided in ledger cache. If number of index pages reaches this limitation, bookie server starts to swap some ledgers from memory to disk. You can increment this value when you found swap became more frequent. But make sure pageLimit*pageSize should not more than JVM max memory limitation, otherwise you would got OutOfMemoryException. In general, incrementing pageLimit, using smaller index page would gain bettern performance in lager number of ledgers with fewer entries case. If pageLimit is -1, bookie server will use 1/3 of JVM memory to compute the limitation of number of index pages.<br /> | -1 | 
| numOfMemtableFlushThreads | When entryLogPerLedger is enabled SortedLedgerStorage flushes entries from memTable using OrderedExecutor having numOfMemtableFlushThreads number of threads.<br /> | 8 | 


## DB Ledger Storage Settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| dbStorage_writeCacheMaxSizeMb | Size of write cache. Memory is allocated from JVM direct memory. Write cache is used for buffer entries before flushing into the entry log. For good performance, it should be big enough to hold a substantial amount of entries in the flush interval. | 25% of the available direct memory | 
| dbStorage_readAheadCacheMaxSizeMb | Size of read cache. Memory is allocated from JVM direct memory. The read cache is pre-filled doing read-ahead whenever a cache miss happens. | 25% of the available direct memroy | 
| dbStorage_readAheadCacheBatchSize | How many entries to pre-fill in cache after a read cache miss | 100 | 
| dbStorage_rocksDB_blockSize | Size of RocksDB block-cache. RocksDB is used for storing ledger indexes.<br />For best performance, this cache should be big enough to hold a significant portion of the index database which can reach ~2GB in some cases.<br /> | 268435456 | 
| dbStorage_rocksDB_writeBufferSizeMB | Size of RocksDB write buffer. RocksDB is used for storing ledger indexes.<br /> | 64 | 
| dbStorage_rocksDB_sstSizeInMB | Size of RocksDB sst file size in MB. RocksDB is used for storing ledger indexes.<br /> | 64 | 
| dbStorage_rocksDB_blockSize |  | 65536 | 
| dbStorage_rocksDB_bloomFilterBitsPerKey |  | 10 | 
| dbStorage_rocksDB_numLevels |  | -1 | 
| dbStorage_rocksDB_numFilesInLevel0 |  | 10 | 
| dbStorage_rocksDB_maxSizeInLevel1MB |  | 256 | 


## Metadata Service Settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| metadataServiceUri | metadata service uri that bookkeeper is used for loading corresponding metadata driver and resolving its metadata service location. | zk+hierarchical://localhost:2181/ledgers | 
| ledgerManagerFactoryClass | @Deprecated in favor of using `metadataServiceUri`<br /><br />The ledger manager factory class, which defines how ledgers are stored, managed, and garbage collected. See the [Ledger Manager](../getting-started/concepts#ledger-manager) guide for more details.<br /> | hierarchical | 
| allowShadedLedgerManagerFactoryClass | Sometimes the bookkeeper server classes are shaded. The ledger manager factory classes might be relocated to be under other packages.<br />This would fail the clients using shaded factory classes since the factory classes are stored in cookies and used for verification.<br />Users can enable this flag to allow using shaded ledger manager factory classes to connect to a bookkeeper cluster.<br /> | false | 
| shadedLedgerManagerFactoryClassPrefix | The shaded ledger manager factory prefix. This is used when `allowShadedLedgerManagerFactoryClass` is set to true. | dlshade. | 


## ZooKeeper Metadata Service Settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| zkLedgersRootPath | @Deprecated in favor of using `metadataServiceUri`<br /><br />Root Zookeeper path to store ledger metadata. This parameter is used by zookeeper-based ledger manager as a root znode to store all ledgers.<br /> | /ledgers | 
| zkServers | @Deprecated in favor of using `metadataServiceUri`<br /><br />A list of one of more servers on which Zookeeper is running. The server list can be comma separated values, for example `zkServers=zk1:2181,zk2:2181,zk3:2181`.<br /> | localhost:2181 | 
| zkTimeout | ZooKeeper client session timeout in milliseconds. Bookie server will exit if it received SESSION_EXPIRED because it was partitioned off from ZooKeeper for more than the session timeout JVM garbage collection, disk I/O will cause SESSION_EXPIRED. Increment this value could help avoiding this issue. | 10000 | 
| zkRetryBackoffStartMs | The Zookeeper client backoff retry start time in millis. | 1000 | 
| zkRetryBackoffMaxMs | The Zookeeper client backoff retry max time in millis. | 10000 | 
| zkRequestRateLimit | The Zookeeper request limit. It is only enabled when setting a postivie value. |  | 
| zkEnableSecurity | Set ACLs on every node written on ZooKeeper, this way only allowed users will be able to read and write BookKeeper metadata stored on ZooKeeper. In order to make ACLs work you need to setup ZooKeeper JAAS authentication all the bookies and Client need to share the same user, and this is usually done using Kerberos authentication. See ZooKeeper documentation | false | 


## Statistics

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| enableStatistics | Whether statistics are enabled for the bookie. | true | 
| statsProviderClass | Stats provider class.<br />Options:<br /> - Prometheus    : org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider<br /> - Codahale     : org.apache.bookkeeper.stats.codahale.CodahaleMetricsProvider<br /> - Twitter Finagle  : org.apache.bookkeeper.stats.twitter.finagle.FinagleStatsProvider<br /> - Twitter Ostrich  : org.apache.bookkeeper.stats.twitter.ostrich.OstrichProvider<br /> - Twitter Science  : org.apache.bookkeeper.stats.twitter.science.TwitterStatsProvider<br /> | org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider | 
| limitStatsLogging | option to limit stats logging | false | 


## Prometheus Metrics Provider Settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| prometheusStatsHttpAddress | default bind address for Prometheus metrics exporter | 0.0.0.0 | 
| prometheusStatsHttpPort | default port for prometheus metrics exporter | 8000 | 
| prometheusStatsLatencyRolloverSeconds | latency stats rollover interval, in seconds | 60 | 


## Codahale Metrics Provider Settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| codahaleStatsPrefix | metric name prefix, default is empty. |  | 
| codahaleStatsOutputFrequencySeconds | the frequency that stats reporters report stats, in seconds. | 60 | 
| codahaleStatsGraphiteEndpoint | the graphite endpoint for reporting stats. see [graphite reporter](//metrics.dropwizard.io/3.1.0/manual/graphite/) for more details. | null | 
| codahaleStatsCSVEndpoint | the directory for reporting stats in csv format. see [csv reporter](//metrics.dropwizard.io/3.1.0/manual/core/#csv) for more details. | null | 
| codahaleStatsSlf4jEndpoint | the slf4j endpoint for reporting stats. see [slf4j reporter](//metrics.dropwizard.io/3.1.0/manual/core/#slf4j) for more details. | null | 
| codahaleStatsJmxEndpoint | the jmx endpoint for reporting stats. see [jmx reporter](//metrics.dropwizard.io/3.1.0/manual/core/#jmx) for more details. |  | 


## Twitter Ostrich Metrics Provider

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| statsExport | Flag to control whether to expose ostrich metrics via a http endpoint configured by `statsHttpPort`. | false | 
| statsHttpPort | The http port of exposing ostrich stats if `statsExport` is set to true | 9002 | 


## Twitter Science Metrics Provider

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| statsExport | Flag to control whether to expose metrics via a http endpoint configured by `statsHttpPort`. | false | 
| statsHttpPort | The http port of exposing stats if `statsExport` is set to true | 9002 | 


## AutoRecovery general settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| autoRecoveryDaemonEnabled | Whether the bookie itself can start auto-recovery service also or not. |  | 
| digestType | The default digest type used for opening ledgers. | CRC32 | 
| passwd | The default password used for opening ledgers. Default value is empty string. |  | 
| enableDigestTypeAutodetection | The flag to enable/disable digest type auto-detection. If it is enabled, the bookkeeper client will ignore the provided digest type provided at `digestType` and the provided passwd provided at `passwd`. | true | 


## AutoRecovery placement settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| ensemblePlacementPolicy | The ensemble placement policy used for finding bookie for re-replicating entries.<br /><br />Options:<br /> - org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy<br /> - org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy<br /> | org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy | 
| reppDnsResolverClass | The DNS resolver class used for resolving network locations for bookies. The setting is used<br />when using either RackawareEnsemblePlacementPolicy and RegionAwareEnsemblePlacementPolicy.<br /> | org.apache.bookkeeper.net.ScriptBasedMapping | 
| networkTopologyScriptFileName | The bash script used by `ScriptBasedMapping` DNS resolver for resolving bookies' network locations.<br /> |  | 
| networkTopologyScriptNumberArgs | The max number of args used in the script provided at `networkTopologyScriptFileName`.<br /> |  | 
| minNumRacksPerWriteQuorum | minimum number of racks per write quorum. RackawareEnsemblePlacementPolicy will try to get bookies from atleast 'minNumRacksPerWriteQuorum' racks for a writeQuorum.<br /> |  | 
| enforceMinNumRacksPerWriteQuorum | 'enforceMinNumRacksPerWriteQuorum' enforces RackawareEnsemblePlacementPolicy to pick bookies from 'minNumRacksPerWriteQuorum' racks for a writeQuorum. If it cann't find bookie then it would throw BKNotEnoughBookiesException instead of picking random one.<br /> |  | 
| ignoreLocalNodeInPlacementPolicy | 'ignoreLocalNodeInPlacementPolicy' specifies whether to ignore localnode in the internal logic of placement policy. If it is not possible or useful to use Bookkeeper client node's (or AutoReplicator) rack/region info. for placement policy then it is better to ignore localnode instead of false alarming with log lines and metrics.<br /> |  | 
| enforceMinNumFaultDomainsForWrite | 'enforceMinNumFaultDomainsForWrite' enforces EnsemblePlacementPolicy to check if a write has made it to bookies in 'minNumRacksPerWriteQuorum' number of fault domains, before acknowledging the write back.<br /> |  | 
| minNumZonesPerWriteQuorum | minimum number of zones per write quorum in ZoneawareEnsemblePlacementPolicy. ZoneawareEnsemblePlacementPolicy would get bookies from atleast 'minNumZonesPerWriteQuorum' racks for a writeQuorum.<br /> | 2 | 
| desiredNumZonesPerWriteQuorum | desired number of zones per write quorum in ZoneawareEnsemblePlacementPolicy. ZoneawareEnsemblePlacementPolicy will try to get bookies from 'desiredNumZonesPerWriteQuorum' zones for a writeQuorum.<br /> | 3 | 
| enforceStrictZoneawarePlacement | in ZoneawareEnsemblePlacementPolicy if strict placement is enabled then minZones/desiredZones in writeQuorum would be maintained otherwise it will pick nodes randomly.<br /> | true | 


## AutoRecovery auditor settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| auditorPeriodicBookieCheckInterval | The time interval between auditor bookie checks, in seconds. The auditor bookie check checks ledger metadata to see which bookies should contain entries for each ledger. If a bookie that should contain entries is unavailable, then the ledger containing that entry is marked for recovery. Setting this to 0 disables the periodic check. Bookie checks will still run when a bookie fails. The default is once per day. | 86400 | 
| auditorPeriodicCheckInterval | The time interval, in seconds, at which the auditor will check all ledgers in the cluster. By default this runs once a week.<br /><br />Set this to 0 to disable the periodic check completely. Note that periodic checking will put extra load on the cluster, so it should not be run more frequently than once a day.<br /> | 604800 | 
| auditorPeriodicPlacementPolicyCheckInterval | The time interval between auditor placement policy checks, in seconds. The auditor placement policy check validates if the ensemble of segments of all the closed ledgers is adhering to the placement policy. It is just monitoring scrutiny but doesn't take any corrective measure other than logging error and reporting metrics. By default it is disabled. |  | 
| auditorLedgerVerificationPercentage | The percentage of a ledger (fragment)'s entries will be verified before claiming a fragment as missing. If it is 0, it only verifies the first and last entries of a given fragment.<br /> |  | 
| lostBookieRecoveryDelay | How long to wait, in seconds, before starting autorecovery of a lost bookie. |  | 
| storeSystemTimeAsLedgerUnderreplicatedMarkTime | Enable the Auditor to use system time as underreplicated ledger mark time. If this is enabled, Auditor will write a ctime field into the underreplicated ledger znode. | true | 
| underreplicatedLedgerRecoveryGracePeriod | The grace period (in seconds) for underreplicated ledgers recovery. If ledger is marked underreplicated for more than this period then it will be reported by placementPolicyCheck in Auditor. Setting this to 0 will disable this check. |  | 
| auditorReplicasCheckInterval | Sets the regularity/interval at which the auditor will run a replicas check of all ledgers, which are closed. This should not be run very often since it validates availability of replicas of all ledgers by querying bookies. Setting this to 0 will completely disable the periodic replicas check. By default it is disabled. |  | 


## AutoRecovery replication worker settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| rereplicationEntryBatchSize | The number of entries that a replication will rereplicate in parallel. | 10 | 
| openLedgerRereplicationGracePeriod | The grace period, in milliseconds, that the replication worker waits before fencing and replicating a ledger fragment that's still being written to upon bookie failure. | 30000 | 
| lockReleaseOfFailedLedgerGracePeriod | Set the grace period, in milliseconds, which the replication worker has to wait before releasing the lock after it failed to replicate a ledger. For the first ReplicationWorker.NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS failures it will do exponential backoff then it will bound at lockReleaseOfFailedLedgerGracePeriod. | 300000 | 
| rwRereplicateBackoffMs | The time to backoff when replication worker encounters exceptions on replicating a ledger, in milliseconds. | 5000 | 


## Memory allocator settings

| Parameter | Description | Default
| --------- | ----------- | ------- | 
| allocatorPoolingPolicy | Define the memory pooling policy.<br /><br />Available options are:<br /> - PooledDirect: Use Direct memory for all buffers and pool the memory.<br />         Direct memory will avoid the overhead of JVM GC and most<br />         memory copies when reading and writing to socket channel.<br />         Pooling will add memory space overhead due to the fact that<br />         there will be fragmentation in the allocator and that threads<br />         will keep a portion of memory as thread-local to avoid<br />         contention when possible.<br /> - UnpooledHeap: Allocate memory from JVM heap without any pooling.<br />         This option has the least overhead in terms of memory usage<br />         since the memory will be automatically reclaimed by the<br />         JVM GC but might impose a performance penalty at high<br />         throughput.<br /> | PooledDirect | 
| allocatorPoolingConcurrency | Controls the amount of concurrency for the memory pool.<br />Default is to have a number of allocator arenas equals to 2 * CPUS.<br />Decreasing this number will reduce the amount of memory overhead, at the<br />expense of increased allocation contention.<br /> | 2 * CPUS | 
| allocatorOutOfMemoryPolicy | Define the memory allocator out of memory policy.<br /><br />Available options are:<br /> - FallbackToHeap: If it's not possible to allocate a buffer from direct memory,<br />          fallback to allocate an unpooled buffer from JVM heap.<br />          This will help absorb memory allocation spikes because the heap<br />          allocations will naturally slow down the process and will result<br />          if full GC cleanup if the Heap itself is full.<br /> - ThrowException: Throw regular OOM exception without taking addition actions.<br /> | FallbackToHeap | 
| allocatorLeakDetectionPolicy | Define the memory allocator leak detection policy.<br /><br />Available options are:<br /> - Disabled: No leak detection and no overhead.<br /> - Simple: Instruments 1% of the allocated buffer to track for leaks.<br /> - Advanced: Instruments 1% of the allocated buffer to track for leaks, reporting<br />       stack traces of places where the buffer was used.<br /> - Paranoid: Instruments 100% of the allocated buffer to track for leaks, reporting<br />       stack traces of places where the buffer was used. Introduce very<br />       significant overhead.<br /> | Disabled | 
