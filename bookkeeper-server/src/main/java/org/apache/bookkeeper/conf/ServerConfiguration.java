/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.conf;

import static org.apache.bookkeeper.util.BookKeeperConstants.MAX_LOG_SIZE_LIMIT;

import com.google.common.annotations.Beta;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.conf.ConfigDef;
import org.apache.bookkeeper.common.conf.ConfigException;
import org.apache.bookkeeper.common.conf.ConfigKey;
import org.apache.bookkeeper.common.conf.ConfigKeyGroup;
import org.apache.bookkeeper.common.conf.Type;
import org.apache.bookkeeper.common.conf.validators.ClassValidator;
import org.apache.bookkeeper.common.conf.validators.RangeValidator;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;

/**
 * Configuration manages server-side settings.
 */
public class ServerConfiguration extends AbstractConfiguration<ServerConfiguration> {

    // Ledger Storage Settings

    private static final ConfigKeyGroup GROUP_LEDGER_STORAGE = ConfigKeyGroup.builder("ledgerstorage")
        .description("Ledger Storage related settings")
        .order(10) // place a place holder here
        .build();

    protected static final String LEDGER_STORAGE_CLASS = "ledgerStorageClass";
    protected static final ConfigKey LEDGER_STORAGE_CLASS_KEY = ConfigKey.builder(LEDGER_STORAGE_CLASS)
        .type(Type.CLASS)
        .description("Ledger storage implementation class")
        .defaultValue(SortedLedgerStorage.class)
        .optionValues(Lists.newArrayList(
            InterleavedLedgerStorage.class.getName(),
            SortedLedgerStorage.class.getName(),
            DbLedgerStorage.class.getName()
        ))
        .validator(ClassValidator.of(LedgerStorage.class))
        .group(GROUP_LEDGER_STORAGE)
        .build();

    // Entry Log Parameters

    private static final ConfigKeyGroup GROUP_LEDGER_STORAGE_ENTRY_LOGGER = ConfigKeyGroup.builder("entrylogger")
        .description("EntryLogger related settings")
        .order(11)
        .build();

    protected static final String ENTRY_LOG_SIZE_LIMIT = "logSizeLimit";
    protected static final ConfigKey ENTRY_LOG_SIZE_LIMIT_KEY = ConfigKey.builder(ENTRY_LOG_SIZE_LIMIT)
        .type(Type.LONG)
        .description("Max file size of entry logger, in bytes")
        .documentation("A new entry log file will be created when the old one reaches this file size limitation")
        .defaultValue(MAX_LOG_SIZE_LIMIT)
        .validator(RangeValidator.between(0, MAX_LOG_SIZE_LIMIT))
        .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
        .build();

    protected static final String ENTRY_LOG_FILE_PREALLOCATION_ENABLED = "entryLogFilePreallocationEnabled";


    protected static final String MINOR_COMPACTION_INTERVAL = "minorCompactionInterval";
    protected static final String MINOR_COMPACTION_THRESHOLD = "minorCompactionThreshold";
    protected static final String MAJOR_COMPACTION_INTERVAL = "majorCompactionInterval";
    protected static final String MAJOR_COMPACTION_THRESHOLD = "majorCompactionThreshold";
    protected static final String IS_THROTTLE_BY_BYTES = "isThrottleByBytes";
    protected static final String COMPACTION_MAX_OUTSTANDING_REQUESTS = "compactionMaxOutstandingRequests";
    protected static final String COMPACTION_RATE = "compactionRate";
    protected static final String COMPACTION_RATE_BY_ENTRIES = "compactionRateByEntries";
    protected static final String COMPACTION_RATE_BY_BYTES = "compactionRateByBytes";

    // Gc Parameters
    protected static final String GC_WAIT_TIME = "gcWaitTime";
    protected static final String IS_FORCE_GC_ALLOW_WHEN_NO_SPACE = "isForceGCAllowWhenNoSpace";
    protected static final String GC_OVERREPLICATED_LEDGER_WAIT_TIME = "gcOverreplicatedLedgerWaitTime";
    protected static final String USE_TRANSACTIONAL_COMPACTION = "useTransactionalCompaction";
    protected static final String VERIFY_METADATA_ON_GC = "verifyMetadataOnGC";
    // Scrub Parameters
    protected static final String LOCAL_SCRUB_PERIOD = "localScrubInterval";
    protected static final String LOCAL_SCRUB_RATE_LIMIT = "localScrubRateLimit";
    // Sync Parameters
    protected static final String FLUSH_INTERVAL = "flushInterval";
    protected static final String FLUSH_ENTRYLOG_INTERVAL_BYTES = "flushEntrylogBytes";
    // Bookie death watch interval
    protected static final String DEATH_WATCH_INTERVAL = "bookieDeathWatchInterval";
    // Ledger Cache Parameters
    protected static final String OPEN_FILE_LIMIT = "openFileLimit";
    protected static final String PAGE_LIMIT = "pageLimit";
    protected static final String PAGE_SIZE = "pageSize";
    protected static final String FILEINFO_CACHE_INITIAL_CAPACITY = "fileInfoCacheInitialCapacity";
    protected static final String FILEINFO_MAX_IDLE_TIME = "fileInfoMaxIdleTime";
    protected static final String FILEINFO_FORMAT_VERSION_TO_WRITE = "fileInfoFormatVersionToWrite";
    // Journal Parameters
    protected static final String MAX_JOURNAL_SIZE = "journalMaxSizeMB";
    protected static final String MAX_BACKUP_JOURNALS = "journalMaxBackups";
    protected static final String JOURNAL_SYNC_DATA = "journalSyncData";
    protected static final String JOURNAL_ADAPTIVE_GROUP_WRITES = "journalAdaptiveGroupWrites";
    protected static final String JOURNAL_MAX_GROUP_WAIT_MSEC = "journalMaxGroupWaitMSec";
    protected static final String JOURNAL_BUFFERED_WRITES_THRESHOLD = "journalBufferedWritesThreshold";
    protected static final String JOURNAL_BUFFERED_ENTRIES_THRESHOLD = "journalBufferedEntriesThreshold";
    protected static final String JOURNAL_FLUSH_WHEN_QUEUE_EMPTY = "journalFlushWhenQueueEmpty";
    protected static final String JOURNAL_REMOVE_FROM_PAGE_CACHE = "journalRemoveFromPageCache";
    protected static final String JOURNAL_PRE_ALLOC_SIZE = "journalPreAllocSizeMB";
    protected static final String JOURNAL_WRITE_BUFFER_SIZE = "journalWriteBufferSizeKB";
    protected static final String JOURNAL_ALIGNMENT_SIZE = "journalAlignmentSize";
    protected static final String NUM_JOURNAL_CALLBACK_THREADS = "numJournalCallbackThreads";
    protected static final String JOURNAL_FORMAT_VERSION_TO_WRITE = "journalFormatVersionToWrite";
    protected static final String JOURNAL_QUEUE_SIZE = "journalQueueSize";
    protected static final String JOURNAL_PAGECACHE_FLUSH_INTERVAL_MSEC = "journalPageCacheFlushIntervalMSec";
    // backpressure control
    protected static final String MAX_ADDS_IN_PROGRESS_LIMIT = "maxAddsInProgressLimit";
    protected static final String MAX_READS_IN_PROGRESS_LIMIT = "maxReadsInProgressLimit";
    protected static final String CLOSE_CHANNEL_ON_RESPONSE_TIMEOUT = "closeChannelOnResponseTimeout";
    protected static final String WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE = "waitTimeoutOnResponseBackpressureMs";

    // Bookie Parameters
    protected static final String BOOKIE_PORT = "bookiePort";
    protected static final String LISTENING_INTERFACE = "listeningInterface";
    protected static final String ALLOW_LOOPBACK = "allowLoopback";
    protected static final String ADVERTISED_ADDRESS = "advertisedAddress";
    protected static final String ALLOW_EPHEMERAL_PORTS = "allowEphemeralPorts";

    protected static final String JOURNAL_DIR = "journalDirectory";
    protected static final String JOURNAL_DIRS = "journalDirectories";
    protected static final String LEDGER_DIRS = "ledgerDirectories";
    protected static final String INDEX_DIRS = "indexDirectories";
    protected static final String ALLOW_STORAGE_EXPANSION = "allowStorageExpansion";
    // NIO and Netty Parameters
    protected static final String SERVER_TCP_NODELAY = "serverTcpNoDelay";
    protected static final String SERVER_SOCK_KEEPALIVE = "serverSockKeepalive";
    protected static final String SERVER_SOCK_LINGER = "serverTcpLinger";
    protected static final String SERVER_WRITEBUFFER_LOW_WATER_MARK = "serverWriteBufferLowWaterMark";
    protected static final String SERVER_WRITEBUFFER_HIGH_WATER_MARK = "serverWriteBufferHighWaterMark";
    protected static final String SERVER_NUM_IO_THREADS = "serverNumIOThreads";

    // Zookeeper Parameters
    protected static final String ZK_RETRY_BACKOFF_START_MS = "zkRetryBackoffStartMs";
    protected static final String ZK_RETRY_BACKOFF_MAX_MS = "zkRetryBackoffMaxMs";
    protected static final String OPEN_LEDGER_REREPLICATION_GRACE_PERIOD = "openLedgerRereplicationGracePeriod";
    protected static final String LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD = "lockReleaseOfFailedLedgerGracePeriod";
    //ReadOnly mode support on all disk full
    protected static final String READ_ONLY_MODE_ENABLED = "readOnlyModeEnabled";
    //Whether the bookie is force started in ReadOnly mode
    protected static final String FORCE_READ_ONLY_BOOKIE = "forceReadOnlyBookie";
    //Whether to persist the bookie status
    protected static final String PERSIST_BOOKIE_STATUS_ENABLED = "persistBookieStatusEnabled";
    //Disk utilization
    protected static final String DISK_USAGE_THRESHOLD = "diskUsageThreshold";
    protected static final String DISK_USAGE_WARN_THRESHOLD = "diskUsageWarnThreshold";
    protected static final String DISK_USAGE_LWM_THRESHOLD = "diskUsageLwmThreshold";
    protected static final String DISK_CHECK_INTERVAL = "diskCheckInterval";

    // Replication parameters
    protected static final String AUDITOR_PERIODIC_CHECK_INTERVAL = "auditorPeriodicCheckInterval";
    protected static final String AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL = "auditorPeriodicBookieCheckInterval";
    protected static final String AUDITOR_PERIODIC_PLACEMENT_POLICY_CHECK_INTERVAL =
                                                                "auditorPeriodicPlacementPolicyCheckInterval";
    protected static final String AUDITOR_LEDGER_VERIFICATION_PERCENTAGE = "auditorLedgerVerificationPercentage";
    protected static final String AUTO_RECOVERY_DAEMON_ENABLED = "autoRecoveryDaemonEnabled";
    protected static final String LOST_BOOKIE_RECOVERY_DELAY = "lostBookieRecoveryDelay";
    protected static final String RW_REREPLICATE_BACKOFF_MS = "rwRereplicateBackoffMs";
    protected static final String UNDERREPLICATED_LEDGER_RECOVERY_GRACE_PERIOD =
            "underreplicatedLedgerRecoveryGracePeriod";
    protected static final String AUDITOR_REPLICAS_CHECK_INTERVAL = "auditorReplicasCheckInterval";

    // Worker Thread parameters.
    protected static final String NUM_ADD_WORKER_THREADS = "numAddWorkerThreads";
    protected static final String NUM_READ_WORKER_THREADS = "numReadWorkerThreads";
    protected static final String MAX_PENDING_READ_REQUESTS_PER_THREAD = "maxPendingReadRequestsPerThread";
    protected static final String MAX_PENDING_ADD_REQUESTS_PER_THREAD = "maxPendingAddRequestsPerThread";
    protected static final String NUM_LONG_POLL_WORKER_THREADS = "numLongPollWorkerThreads";
    protected static final String NUM_HIGH_PRIORITY_WORKER_THREADS = "numHighPriorityWorkerThreads";

    // Long poll parameters
    protected static final String REQUEST_TIMER_TICK_DURATION_MILLISEC = "requestTimerTickDurationMs";
    protected static final String REQUEST_TIMER_NO_OF_TICKS = "requestTimerNumTicks";

    protected static final String READ_BUFFER_SIZE = "readBufferSizeBytes";
    protected static final String WRITE_BUFFER_SIZE = "writeBufferSizeBytes";
    // Whether the bookie should use its hostname or ipaddress for the
    // registration.
    protected static final String USE_HOST_NAME_AS_BOOKIE_ID = "useHostNameAsBookieID";
    protected static final String USE_SHORT_HOST_NAME = "useShortHostName";
    protected static final String ENABLE_LOCAL_TRANSPORT = "enableLocalTransport";
    protected static final String DISABLE_SERVER_SOCKET_BIND = "disableServerSocketBind";

    protected static final String SORTED_LEDGER_STORAGE_ENABLED = "sortedLedgerStorageEnabled";
    protected static final String SKIP_LIST_SIZE_LIMIT = "skipListSizeLimit";
    protected static final String SKIP_LIST_CHUNK_SIZE_ENTRY = "skipListArenaChunkSize";
    protected static final String SKIP_LIST_MAX_ALLOC_ENTRY = "skipListArenaMaxAllocSize";

    // Statistics Parameters
    protected static final String ENABLE_STATISTICS = "enableStatistics";
    protected static final String STATS_PROVIDER_CLASS = "statsProviderClass";


    // Rx adaptive ByteBuf allocator parameters
    protected static final String BYTEBUF_ALLOCATOR_SIZE_INITIAL = "byteBufAllocatorSizeInitial";
    protected static final String BYTEBUF_ALLOCATOR_SIZE_MIN = "byteBufAllocatorSizeMin";
    protected static final String BYTEBUF_ALLOCATOR_SIZE_MAX = "byteBufAllocatorSizeMax";

    // Bookie auth provider factory class name
    protected static final String BOOKIE_AUTH_PROVIDER_FACTORY_CLASS = "bookieAuthProviderFactoryClass";

    protected static final String MIN_USABLESIZE_FOR_INDEXFILE_CREATION = "minUsableSizeForIndexFileCreation";
    protected static final String MIN_USABLESIZE_FOR_ENTRYLOG_CREATION = "minUsableSizeForEntryLogCreation";
    protected static final String MIN_USABLESIZE_FOR_HIGH_PRIORITY_WRITES = "minUsableSizeForHighPriorityWrites";

    protected static final String ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION =
        "allowMultipleDirsUnderSameDiskPartition";

    // Http Server parameters
    protected static final String HTTP_SERVER_ENABLED = "httpServerEnabled";
    protected static final String HTTP_SERVER_PORT = "httpServerPort";

    // Lifecycle Components
    protected static final String EXTRA_SERVER_COMPONENTS = "extraServerComponents";
    protected static final String IGNORE_EXTRA_SERVER_COMPONENTS_STARTUP_FAILURES =
        "ignoreExtraServerComponentsStartupFailures";

    // Registration
    protected static final String REGISTRATION_MANAGER_CLASS = "registrationManagerClass";

    // Stats
    protected static final String ENABLE_TASK_EXECUTION_STATS = "enableTaskExecutionStats";

    /*
     * config specifying if the entrylog per ledger is enabled or not.
     */
    protected static final String ENTRY_LOG_PER_LEDGER_ENABLED = "entryLogPerLedgerEnabled";
    // In the case of multipleentrylogs, multiple threads can be used to flush the memtable parallelly.
    protected static final String NUMBER_OF_MEMTABLE_FLUSH_THREADS = "numOfMemtableFlushThreads";


    /*
     * config specifying if the entrylog per ledger is enabled, then the amount
     * of time EntryLogManagerForEntryLogPerLedger should wait for closing the
     * entrylog file after the last addEntry call for that ledger, if explicit
     * writeclose for that ledger is not received.
     */
    protected static final String ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS = "entrylogMapAccessExpiryTimeInSeconds";

    /*
     * in entryLogPerLedger feature, this specifies the maximum number of
     * entrylogs that can be active at a given point in time. If there are more
     * number of active entryLogs then the maximumNumberOfActiveEntryLogs then
     * the entrylog will be evicted from the cache.
     */
    protected static final String MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS = "maximumNumberOfActiveEntryLogs";

    /*
     * in EntryLogManagerForEntryLogPerLedger, this config value specifies the
     * metrics cache size limits in multiples of entrylogMap cache size limits.
     */
    protected static final String ENTRY_LOG_PER_LEDGER_COUNTER_LIMITS_MULT_FACTOR =
            "entryLogPerLedgerCounterLimitsMultFactor";

    // Perform local consistency check on bookie startup
    protected static final String LOCAL_CONSISTENCY_CHECK_ON_STARTUP = "localConsistencyCheckOnStartup";

    /**
     * Construct a default configuration object.
     */
    public ServerConfiguration() {
        super();
    }

    /**
     * Construct a configuration based on other configuration.
     *
     * @param conf
     *          Other configuration
     */
    public ServerConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }

    /**
     * Get entry logger size limitation.
     *
     * @return entry logger size limitation
     */
    public long getEntryLogSizeLimit() {
        return ENTRY_LOG_SIZE_LIMIT_KEY.getLong(this);
    }

    /**
     * Set entry logger size limitation.
     *
     * @param logSizeLimit
     *          new log size limitation
     */
    public ServerConfiguration setEntryLogSizeLimit(long logSizeLimit) {
        ENTRY_LOG_SIZE_LIMIT_KEY.set(this, logSizeLimit);
        return this;
    }

    /**
     * Is entry log file preallocation enabled.
     *
     * @return whether entry log file preallocation is enabled or not.
     */
    public boolean isEntryLogFilePreAllocationEnabled() {
        return this.getBoolean(ENTRY_LOG_FILE_PREALLOCATION_ENABLED, true);
    }

    /**
     * Enable/disable entry log file preallocation.
     *
     * @param enabled
     *          enable/disable entry log file preallocation.
     * @return server configuration object.
     */
    public ServerConfiguration setEntryLogFilePreAllocationEnabled(boolean enabled) {
        this.setProperty(ENTRY_LOG_FILE_PREALLOCATION_ENABLED, enabled);
        return this;
    }

    /**
     * Get Garbage collection wait time. Default value is 10 minutes.
     * The guideline is not to set a too low value for this, if using zookeeper based
     * ledger manager. And it would be nice to align with the average lifecyle time of
     * ledgers in the system.
     *
     * @return gc wait time
     */
    public long getGcWaitTime() {
        return this.getLong(GC_WAIT_TIME, 600000);
    }

    /**
     * Set garbage collection wait time.
     *
     * @param gcWaitTime
     *          gc wait time
     * @return server configuration
     */
    public ServerConfiguration setGcWaitTime(long gcWaitTime) {
        this.setProperty(GC_WAIT_TIME, Long.toString(gcWaitTime));
        return this;
    }

    /**
     * Get wait time in millis for garbage collection of overreplicated ledgers.
     *
     * @return gc wait time
     */
    public long getGcOverreplicatedLedgerWaitTimeMillis() {
        return this.getLong(GC_OVERREPLICATED_LEDGER_WAIT_TIME, TimeUnit.DAYS.toMillis(1));
    }

    /**
     * Set wait time for garbage collection of overreplicated ledgers. Default: 1 day
     *
     * <p>A ledger can be overreplicated under the following circumstances:
     * 1. The ledger with few entries has bk1 and bk2 as its ensemble.
     * 2. bk1 crashes.
     * 3. bk3 replicates the ledger from bk2 and updates the ensemble to bk2 and bk3.
     * 4. bk1 comes back up.
     * 5. Now there are 3 copies of the ledger.
     *
     * @param gcWaitTime
     * @return server configuration
     */
    public ServerConfiguration setGcOverreplicatedLedgerWaitTime(long gcWaitTime, TimeUnit unit) {
        this.setProperty(GC_OVERREPLICATED_LEDGER_WAIT_TIME, Long.toString(unit.toMillis(gcWaitTime)));
        return this;
    }

    /**
     * Get whether to use transactional compaction and using a separate log for compaction or not.
     *
     * @return use transactional compaction
     */
    public boolean getUseTransactionalCompaction() {
        return this.getBoolean(USE_TRANSACTIONAL_COMPACTION, false);
    }

    /**
     * Set whether to use transactional compaction and using a separate log for compaction or not.
     * @param useTransactionalCompaction
     * @return server configuration
     */
    public ServerConfiguration setUseTransactionalCompaction(boolean useTransactionalCompaction) {
        this.setProperty(USE_TRANSACTIONAL_COMPACTION, useTransactionalCompaction);
        return this;
    }

    /**
     * Get whether the bookie is configured to double check prior to gc.
     *
     * @return use transactional compaction
     */
    public boolean getVerifyMetadataOnGC() {
        return this.getBoolean(VERIFY_METADATA_ON_GC, false);
    }

    /**
     * Set whether the bookie is configured to double check prior to gc.
     * @param verifyMetadataOnGC
     * @return server configuration
     */
    public ServerConfiguration setVerifyMetadataOnGc(boolean verifyMetadataOnGC) {
        this.setProperty(VERIFY_METADATA_ON_GC, verifyMetadataOnGC);
        return this;
    }

    /**
     * Get whether local scrub is enabled.
     *
     * @return Whether local scrub is enabled.
     */
    public boolean isLocalScrubEnabled() {
        return this.getLocalScrubPeriod() > 0;
    }

    /**
     * Get local scrub interval.
     *
     * @return Number of seconds between scrubs, <= 0 for disabled.
     */
    public long getLocalScrubPeriod() {
        return this.getLong(LOCAL_SCRUB_PERIOD, 0);
    }

    /**
     * Set local scrub period in seconds (<= 0 for disabled). Scrub will be scheduled at delays
     * chosen from the interval (.5 * interval, 1.5 * interval)
     */
    public void setLocalScrubPeriod(long period) {
        this.setProperty(LOCAL_SCRUB_PERIOD, period);
    }

    /**
     * Get local scrub rate limit (entries/second).
     *
     * @return Max number of entries to scrub per second, 0 for disabled.
     */
    public double getLocalScrubRateLimit() {
        return this.getDouble(LOCAL_SCRUB_RATE_LIMIT, 60);
    }

    /**
     * Get local scrub rate limit (entries/second).
     *
     * @param scrubRateLimit Max number of entries per second to scan.
     */
    public void setLocalScrubRateLimit(double scrubRateLimit) {
        this.setProperty(LOCAL_SCRUB_RATE_LIMIT, scrubRateLimit);
    }

    /**
     * Get flush interval. Default value is 10 second. It isn't useful to decrease
     * this value, since ledger storage only checkpoints when an entry logger file
     * is rolled.
     *
     * @return flush interval
     */
    public int getFlushInterval() {
        return this.getInt(FLUSH_INTERVAL, 10000);
    }

    /**
     * Set flush interval.
     *
     * @param flushInterval
     *          Flush Interval
     * @return server configuration
     */
    public ServerConfiguration setFlushInterval(int flushInterval) {
        this.setProperty(FLUSH_INTERVAL, Integer.toString(flushInterval));
        return this;
    }

    /**
     * Set entry log flush interval in bytes.
     *
     * <p>Default is 0. 0 or less disables this feature and effectively flush
     * happens on log rotation.
     *
     * <p>Flushing in smaller chunks but more frequently reduces spikes in disk
     * I/O. Flushing too frequently may also affect performance negatively.
     *
     * @return Entry log flush interval in bytes
     */
    public long getFlushIntervalInBytes() {
        return this.getLong(FLUSH_ENTRYLOG_INTERVAL_BYTES, 0);
    }

    /**
     * Set entry log flush interval in bytes.
     *
     * @param flushInterval in bytes
     * @return server configuration
     */
    public ServerConfiguration setFlushIntervalInBytes(long flushInterval) {
        this.setProperty(FLUSH_ENTRYLOG_INTERVAL_BYTES, Long.toString(flushInterval));
        return this;
    }


    /**
     * Get bookie death watch interval.
     *
     * @return watch interval
     */
    public int getDeathWatchInterval() {
        return this.getInt(DEATH_WATCH_INTERVAL, 1000);
    }

    /**
     * Get open file limit. Default value is 20000.
     *
     * @return max number of files to open
     */
    public int getOpenFileLimit() {
        return this.getInt(OPEN_FILE_LIMIT, 20000);
    }

    /**
     * Set limitation of number of open files.
     *
     * @param fileLimit
     *          Limitation of number of open files.
     * @return server configuration
     */
    public ServerConfiguration setOpenFileLimit(int fileLimit) {
        setProperty(OPEN_FILE_LIMIT, fileLimit);
        return this;
    }

    /**
     * Get limitation number of index pages in ledger cache.
     *
     * @return max number of index pages in ledger cache
     */
    public int getPageLimit() {
        return this.getInt(PAGE_LIMIT, -1);
    }

    /**
     * Set limitation number of index pages in ledger cache.
     *
     * @param pageLimit
     *          Limitation of number of index pages in ledger cache.
     * @return server configuration
     */
    public ServerConfiguration setPageLimit(int pageLimit) {
        this.setProperty(PAGE_LIMIT, pageLimit);
        return this;
    }

    /**
     * Get page size.
     *
     * @return page size in ledger cache
     */
    public int getPageSize() {
        return this.getInt(PAGE_SIZE, 8192);
    }

    /**
     * Set page size.
     *
     * @see #getPageSize()
     *
     * @param pageSize
     *          Page Size
     * @return Server Configuration
     */
    public ServerConfiguration setPageSize(int pageSize) {
        this.setProperty(PAGE_SIZE, pageSize);
        return this;
    }

    /**
     * Get the minimum total size for the internal file info cache tables.
     * Providing a large enough estimate at construction time avoids the need for
     * expensive resizing operations later, but setting this value unnecessarily high
     * wastes memory.
     *
     * @return minimum size of initial file info cache.
     */
    public int getFileInfoCacheInitialCapacity() {
        return getInt(FILEINFO_CACHE_INITIAL_CAPACITY, Math.max(getOpenFileLimit() / 4, 64));
    }

    /**
     * Set the minimum total size for the internal file info cache tables for initialization.
     *
     * @param initialCapacity
     *          Initial capacity of file info cache table.
     * @return server configuration instance.
     */
    public ServerConfiguration setFileInfoCacheInitialCapacity(int initialCapacity) {
        setProperty(FILEINFO_CACHE_INITIAL_CAPACITY, initialCapacity);
        return this;
    }

    /**
     * Get the max idle time allowed for a open file info existed in file info cache.
     * If the file info is idle for a long time, exceed the given time period. The file
     * info will be evicted and closed. If the value is zero, the file info is evicted
     * only when opened files reached openFileLimit.
     *
     * @see #getOpenFileLimit
     * @return max idle time of a file info in the file info cache.
     */
    public long getFileInfoMaxIdleTime() {
        return this.getLong(FILEINFO_MAX_IDLE_TIME, 0L);
    }

    /**
     * Set the max idle time allowed for a open file info existed in file info cache.
     *
     * @param idleTime
     *          Idle time, in seconds.
     * @see #getFileInfoMaxIdleTime
     * @return server configuration object.
     */
    public ServerConfiguration setFileInfoMaxIdleTime(long idleTime) {
        setProperty(FILEINFO_MAX_IDLE_TIME, idleTime);
        return this;
    }

    /**
     * Get fileinfo format version to write.
     *
     * @return fileinfo format version to write.
     */
    public int getFileInfoFormatVersionToWrite() {
        return this.getInt(FILEINFO_FORMAT_VERSION_TO_WRITE, 1);
    }

    /**
     * Set fileinfo format version to write.
     *
     * @param version
     *            fileinfo format version to write.
     * @return server configuration.
     */
    public ServerConfiguration setFileInfoFormatVersionToWrite(int version) {
        this.setProperty(FILEINFO_FORMAT_VERSION_TO_WRITE, version);
        return this;
    }

    /**
     * Max journal file size.
     *
     * @return max journal file size
     */
    public long getMaxJournalSizeMB() {
        return this.getLong(MAX_JOURNAL_SIZE, 2 * 1024);
    }

    /**
     * Set new max journal file size.
     *
     * @param maxJournalSize
     *          new max journal file size
     * @return server configuration
     */
    public ServerConfiguration setMaxJournalSizeMB(long maxJournalSize) {
        this.setProperty(MAX_JOURNAL_SIZE, Long.toString(maxJournalSize));
        return this;
    }

    /**
     * How much space should we pre-allocate at a time in the journal.
     *
     * @return journal pre-allocation size in MB
     */
    public int getJournalPreAllocSizeMB() {
        return this.getInt(JOURNAL_PRE_ALLOC_SIZE, 16);
    }

    /**
     * Size of the write buffers used for the journal.
     *
     * @return journal write buffer size in KB
     */
    public int getJournalWriteBufferSizeKB() {
        return this.getInt(JOURNAL_WRITE_BUFFER_SIZE, 64);
    }

    /**
     * Set the size of the write buffers used for the journal.
     *
     * @param bufferSizeKB the size of the write buffer used for the journal, in KB.
     * @return server configuration
     */
    public ServerConfiguration setJournalWriteBufferSizeKB(int bufferSizeKB) {
        setProperty(JOURNAL_WRITE_BUFFER_SIZE, bufferSizeKB);
        return this;
    }

    /**
     * Max number of older journal files kept.
     *
     * @return max number of older journal files to kept
     */
    public int getMaxBackupJournals() {
        return this.getInt(MAX_BACKUP_JOURNALS, 5);
    }

    /**
     * Set max number of older journal files to kept.
     *
     * @param maxBackupJournals
     *          Max number of older journal files
     * @return server configuration
     */
    public ServerConfiguration setMaxBackupJournals(int maxBackupJournals) {
        this.setProperty(MAX_BACKUP_JOURNALS, Integer.toString(maxBackupJournals));
        return this;
    }

    /**
     * All the journal writes and commits should be aligned to given size. If not,
     * zeros will be padded to align to given size.
     *
     * @return journal alignment size
     */
    public int getJournalAlignmentSize() {
        return this.getInt(JOURNAL_ALIGNMENT_SIZE, 512);
    }

    /**
     * Set journal alignment size.
     *
     * @param size
     *          journal alignment size.
     * @return server configuration.
     */
    public ServerConfiguration setJournalAlignmentSize(int size) {
        this.setProperty(JOURNAL_ALIGNMENT_SIZE, size);
        return this;
    }

    /**
     * Get journal format version to write.
     *
     * @return journal format version to write.
     */
    public int getJournalFormatVersionToWrite() {
        return this.getInt(JOURNAL_FORMAT_VERSION_TO_WRITE, 6);
    }

    /**
     * Set journal format version to write.
     *
     * @param version
     *          journal format version to write.
     * @return server configuration.
     */
    public ServerConfiguration setJournalFormatVersionToWrite(int version) {
        this.setProperty(JOURNAL_FORMAT_VERSION_TO_WRITE, version);
        return this;
    }

    /**
     * Set the size of the journal queue.
     *
     * @param journalQueueSize
     *            the max size of journal queue
     * @return server configuration.
     */
    public ServerConfiguration setJournalQueueSize(int journalQueueSize) {
        this.setProperty(JOURNAL_QUEUE_SIZE, journalQueueSize);
        return this;
    }

    /**
     * Get size of journal queue.
     *
     * @return the max size of journal queue.
     */
    public int getJournalQueueSize() {
        return this.getInt(JOURNAL_QUEUE_SIZE, 10_000);
    }

    /**
     * Set PageCache flush interval in second.
     *
     * @Param journalPageCacheFlushInterval
     *          journal pageCache flush interval when journalSyncData closed
     * @return server configuration.
     */
    public ServerConfiguration setJournalPageCacheFlushIntervalMSec(long journalPageCacheFlushIntervalMSec) {
        this.setProperty(JOURNAL_PAGECACHE_FLUSH_INTERVAL_MSEC, journalPageCacheFlushIntervalMSec);
        return this;
    }

    /**
     * Get journal pageCache flush interval.
     *
     * @return journal pageCache flush interval.
     */
    public long getJournalPageCacheFlushIntervalMSec() {
        return this.getLong(JOURNAL_PAGECACHE_FLUSH_INTERVAL_MSEC, 1000);
    }

    /**
     * Get max number of adds in progress. 0 == unlimited.
     *
     * @return Max number of adds in progress.
     */
    public int getMaxAddsInProgressLimit() {
        return this.getInt(MAX_ADDS_IN_PROGRESS_LIMIT, 0);
    }

    /**
     * Set max number of adds in progress. 0 == unlimited.
     *
     * @param value
     *          max number of adds in progress.
     * @return server configuration.
     */
    public ServerConfiguration setMaxAddsInProgressLimit(int value) {
        this.setProperty(MAX_ADDS_IN_PROGRESS_LIMIT, value);
        return this;
    }

    /**
     * Get max number of reads in progress. 0 == unlimited.
     *
     * @return Max number of reads in progress.
     */
    public int getMaxReadsInProgressLimit() {
        return this.getInt(MAX_READS_IN_PROGRESS_LIMIT, 0);
    }

    /**
     * Set max number of reads in progress. 0 == unlimited.
     *
     * @param value
     *          max number of reads in progress.
     * @return server configuration.
     */
    public ServerConfiguration setMaxReadsInProgressLimit(int value) {
        this.setProperty(MAX_READS_IN_PROGRESS_LIMIT, value);
        return this;
    }

    /**
     * Configures action in case if server timed out sending response to the client.
     * true == close the channel and drop response
     * false == drop response
     * Requires waitTimeoutOnBackpressureMs >= 0 otherwise ignored.
     *
     * @return value indicating if channel should be closed.
     */
    public boolean getCloseChannelOnResponseTimeout(){
        return this.getBoolean(CLOSE_CHANNEL_ON_RESPONSE_TIMEOUT, false);
    }

    /**
     * Configures action in case if server timed out sending response to the client.
     * true == close the channel and drop response
     * false == drop response
     * Requires waitTimeoutOnBackpressureMs >= 0 otherwise ignored.
     *
     * @param value
     * @return server configuration.
     */
    public ServerConfiguration setCloseChannelOnResponseTimeout(boolean value) {
        this.setProperty(CLOSE_CHANNEL_ON_RESPONSE_TIMEOUT, value);
        return this;
    }

    /**
     * Timeout controlling wait on response send in case of unresponsive client
     * (i.e. client in long GC etc.)
     *
     * @return timeout value
     *        negative value disables the feature
     *        0 to allow request to fail immediately
     *        Default is -1 (disabled)
     */
    public long getWaitTimeoutOnResponseBackpressureMillis() {
        return getLong(WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE, -1);
    }

    /**
     * Timeout controlling wait on response send in case of unresponsive client
     * (i.e. client in long GC etc.)
     *
     * @param value
     *        negative value disables the feature
     *        0 to allow request to fail immediately
     *        Default is -1 (disabled)
     * @return client configuration.
     */
    public ServerConfiguration setWaitTimeoutOnResponseBackpressureMillis(long value) {
        setProperty(WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE, value);
        return this;
    }

    /**
     * Get bookie port that bookie server listen on.
     *
     * @return bookie port
     */
    public int getBookiePort() {
        return this.getInt(BOOKIE_PORT, 3181);
    }

    /**
     * Set new bookie port that bookie server listen on.
     *
     * @param port
     *          Port to listen on
     * @return server configuration
     */
    public ServerConfiguration setBookiePort(int port) {
        this.setProperty(BOOKIE_PORT, Integer.toString(port));
        return this;
    }

    /**
     * Get the network interface that the bookie should
     * listen for connections on. If this is null, then the bookie
     * will listen for connections on all interfaces.
     *
     * @return the network interface to listen on, e.g. eth0, or
     *         null if none is specified
     */
    public String getListeningInterface() {
        return this.getString(LISTENING_INTERFACE);
    }

    /**
     * Set the network interface that the bookie should listen on.
     * If not set, the bookie will listen on all interfaces.
     *
     * @param iface the interface to listen on
     */
    public ServerConfiguration setListeningInterface(String iface) {
        this.setProperty(LISTENING_INTERFACE, iface);
        return this;
    }

    /**
     * Is the bookie allowed to use a loopback interface as its primary
     * interface(i.e. the interface it uses to establish its identity)?
     *
     * <p>By default, loopback interfaces are not allowed as the primary
     * interface.
     *
     * <p>Using a loopback interface as the primary interface usually indicates
     * a configuration error. For example, its fairly common in some VPS setups
     * to not configure a hostname, or to have the hostname resolve to
     * 127.0.0.1. If this is the case, then all bookies in the cluster will
     * establish their identities as 127.0.0.1:3181, and only one will be able
     * to join the cluster. For VPSs configured like this, you should explicitly
     * set the listening interface.
     *
     * @see #setListeningInterface(String)
     * @return whether a loopback interface can be used as the primary interface
     */
    public boolean getAllowLoopback() {
        return this.getBoolean(ALLOW_LOOPBACK, false);
    }

    /**
     * Configure the bookie to allow loopback interfaces to be used
     * as the primary bookie interface.
     *
     * @see #getAllowLoopback
     * @param allow whether to allow loopback interfaces
     * @return server configuration
     */
    public ServerConfiguration setAllowLoopback(boolean allow) {
        this.setProperty(ALLOW_LOOPBACK, allow);
        return this;
    }

    /**
     * Get the configured advertised address for the bookie.
     *
     * <p>If present, this setting will take precedence over the
     * {@link #setListeningInterface(String)} and
     * {@link #setUseHostNameAsBookieID(boolean)}.
     *
     * @see #setAdvertisedAddress(String)
     * @return the configure address to be advertised
     */
    public String getAdvertisedAddress() {
        return this.getString(ADVERTISED_ADDRESS, null);
    }

    /**
     * Configure the bookie to advertise a specific address.
     *
     * <p>By default, a bookie will advertise either its own IP or hostname,
     * depending on the {@link getUseHostNameAsBookieID()} setting.
     *
     * <p>When the advertised is set to a non-empty string, the bookie will
     * register and advertise using this address.
     *
     * <p>If present, this setting will take precedence over the
     * {@link #setListeningInterface(String)} and
     * {@link #setUseHostNameAsBookieID(boolean)}.
     *
     * @see #getAdvertisedAddress()
     * @param advertisedAddress
     *            whether to allow loopback interfaces
     * @return server configuration
     */
    public ServerConfiguration setAdvertisedAddress(String advertisedAddress) {
        this.setProperty(ADVERTISED_ADDRESS, advertisedAddress);
        return this;
    }

    /**
     * Is the bookie allowed to use an ephemeral port (port 0) as its server port.
     *
     * <p>By default, an ephemeral port is not allowed. Using an ephemeral port
     * as the service port usually indicates a configuration error. However, in unit
     * tests, using ephemeral port will address port conflicts problem and allow
     * running tests in parallel.
     *
     * @return whether is allowed to use an ephemeral port.
     */
    public boolean getAllowEphemeralPorts() {
        return this.getBoolean(ALLOW_EPHEMERAL_PORTS, false);
    }

    /**
     * Configure the bookie to allow using an ephemeral port.
     *
     * @param allow whether to allow using an ephemeral port.
     * @return server configuration
     */
    public ServerConfiguration setAllowEphemeralPorts(boolean allow) {
        this.setProperty(ALLOW_EPHEMERAL_PORTS, allow);
        return this;
    }

    /**
     * Return whether we should allow addition of ledger/index dirs to an existing bookie.
     *
     * @return true if the addition is allowed; false otherwise
     */
    public boolean getAllowStorageExpansion() {
        return this.getBoolean(ALLOW_STORAGE_EXPANSION, false);
    }

    /**
     * Change the setting of whether or not we should allow ledger/index
     * dirs to be added to the current set of dirs.
     *
     * @param val - true if new ledger/index dirs can be added; false otherwise.
     *
     * @return server configuration
     */
    public ServerConfiguration setAllowStorageExpansion(boolean val) {
        this.setProperty(ALLOW_STORAGE_EXPANSION, val);
        return this;
    }

    /**
     * Get dir names to store journal files.
     *
     * @return journal dir name
     */
    public String[] getJournalDirNames() {
        String[] journalDirs = this.getStringArray(JOURNAL_DIRS);
        if (journalDirs == null || journalDirs.length == 0
                || (journalDirs.length == 1 && StringUtils.isEmpty(journalDirs[0]))) {
            return new String[] { getJournalDirName() };
        }
        return journalDirs;
    }

    /**
     * Get dir name to store journal files.
     *
     * @return journal dir name
     */
    @Deprecated
    public String getJournalDirName() {
        return this.getString(JOURNAL_DIR, "/tmp/bk-txn");
    }

    /**
     * Get dir name to store journal files.
     *
     * @return journal dir name
     */
    public String getJournalDirNameWithoutDefault() {
        return this.getString(JOURNAL_DIR);
    }


    /**
     * Set dir name to store journal files.
     *
     * @param journalDir
     *          Dir to store journal files
     * @return server configuration
     */
    public ServerConfiguration setJournalDirName(String journalDir) {
        this.setProperty(JOURNAL_DIRS, new String[] {journalDir});
        return this;
    }

    /**
     * Set dir names to store journal files.
     *
     * @param journalDirs
     *          Dir to store journal files
     * @return server configuration
     */
    public ServerConfiguration setJournalDirsName(String[] journalDirs) {
        this.setProperty(JOURNAL_DIRS, journalDirs);
        return this;
    }

    /**
     * Get dirs to store journal files.
     *
     * @return journal dirs, if no journal dir provided return null
     */
    public File[] getJournalDirs() {
        String[] journalDirNames = getJournalDirNames();
        File[] journalDirs = new File[journalDirNames.length];
        for (int i = 0; i < journalDirNames.length; i++) {
            journalDirs[i] = new File(journalDirNames[i]);
        }
        return journalDirs;
    }

    /**
     * Get dir names to store ledger data.
     *
     * @return ledger dir names, if not provided return null
     */
    public String[] getLedgerDirWithoutDefault() {
        return this.getStringArray(LEDGER_DIRS);
    }

    /**
     * Get dir names to store ledger data.
     *
     * @return ledger dir names, if not provided return null
     */
    public String[] getLedgerDirNames() {
        String[] ledgerDirs = this.getStringArray(LEDGER_DIRS);
        if ((null == ledgerDirs) || (0 == ledgerDirs.length)) {
            return new String[] { "/tmp/bk-data" };
        }
        return ledgerDirs;
    }

    /**
     * Set dir names to store ledger data.
     *
     * @param ledgerDirs
     *          Dir names to store ledger data
     * @return server configuration
     */
    public ServerConfiguration setLedgerDirNames(String[] ledgerDirs) {
        if (null == ledgerDirs) {
            return this;
        }
        this.setProperty(LEDGER_DIRS, ledgerDirs);
        return this;
    }

    /**
     * Get dirs that stores ledger data.
     *
     * @return ledger dirs
     */
    public File[] getLedgerDirs() {
        String[] ledgerDirNames = getLedgerDirNames();

        File[] ledgerDirs = new File[ledgerDirNames.length];
        for (int i = 0; i < ledgerDirNames.length; i++) {
            ledgerDirs[i] = new File(ledgerDirNames[i]);
        }
        return ledgerDirs;
    }

    /**
     * Get dir name to store index files.
     *
     * @return ledger index dir name, if no index dirs provided return null
     */
    public String[] getIndexDirNames() {
        if (!this.containsKey(INDEX_DIRS)) {
            return null;
        }
        return this.getStringArray(INDEX_DIRS);
    }

    /**
     * Set dir name to store index files.
     *
     * @param indexDirs
     *          Index dir names
     * @return server configuration.
     */
    public ServerConfiguration setIndexDirName(String[] indexDirs) {
        this.setProperty(INDEX_DIRS, indexDirs);
        return this;
    }

    /**
     * Get index dir to store ledger index files.
     *
     * @return index dirs, if no index dirs provided return null
     */
    public File[] getIndexDirs() {
        String[] idxDirNames = getIndexDirNames();
        if (null == idxDirNames) {
            return null;
        }
        File[] idxDirs = new File[idxDirNames.length];
        for (int i = 0; i < idxDirNames.length; i++) {
            idxDirs[i] = new File(idxDirNames[i]);
        }
        return idxDirs;
    }

    /**
     * Is tcp connection no delay.
     *
     * @return tcp socket nodelay setting
     */
    public boolean getServerTcpNoDelay() {
        return getBoolean(SERVER_TCP_NODELAY, true);
    }

    /**
     * Set socket nodelay setting.
     *
     * @param noDelay
     *          NoDelay setting
     * @return server configuration
     */
    public ServerConfiguration setServerTcpNoDelay(boolean noDelay) {
        setProperty(SERVER_TCP_NODELAY, Boolean.toString(noDelay));
        return this;
    }

    /**
     * Get the number of IO threads. This is the number of
     * threads used by Netty to handle TCP connections.
     *
     * @return the number of IO threads
     */
    public int getServerNumIOThreads() {
        return getInt(SERVER_NUM_IO_THREADS, 2 * Runtime.getRuntime().availableProcessors());
    }

    /**
     * Set the number of IO threads.
     *
     * <p>
     * This is the number of threads used by Netty to handle TCP connections.
     * </p>
     *
     * @see #getNumIOThreads()
     * @param numThreads number of IO threads used for bookkeeper
     * @return client configuration
     */
    public ServerConfiguration setServerNumIOThreads(int numThreads) {
        setProperty(SERVER_NUM_IO_THREADS, Integer.toString(numThreads));
        return this;
    }

    /**
     * Timeout to drain the socket on close.
     *
     * @return socket linger setting
     */
    public int getServerSockLinger() {
        return getInt(SERVER_SOCK_LINGER, 0);
    }

    /**
     * Set socket linger timeout on close.
     *
     * <p>When enabled, a close or shutdown will not return until all queued messages for the socket have been
     * successfully sent or the linger timeout has been reached. Otherwise, the call returns immediately and the
     * closing is done in the background.
     *
     * @param linger
     *            NoDelay setting
     * @return server configuration
     */
    public ServerConfiguration setServerSockLinger(int linger) {
        setProperty(SERVER_SOCK_LINGER, Integer.toString(linger));
        return this;
    }

    /**
     * Get socket keepalive.
     *
     * @return socket keepalive setting
     */
    public boolean getServerSockKeepalive() {
        return getBoolean(SERVER_SOCK_KEEPALIVE, true);
    }

    /**
     * Set socket keepalive setting.
     *
     * <p>This setting is used to send keep-alive messages on connection-oriented sockets.
     *
     * @param keepalive
     *            KeepAlive setting
     * @return server configuration
     */
    public ServerConfiguration setServerSockKeepalive(boolean keepalive) {
        setProperty(SERVER_SOCK_KEEPALIVE, Boolean.toString(keepalive));
        return this;
    }

    /**
     * Get zookeeper client backoff retry start time in millis.
     *
     * @return zk backoff retry start time in millis.
     */
    public int getZkRetryBackoffStartMs() {
        return getInt(ZK_RETRY_BACKOFF_START_MS, getZkTimeout());
    }

    /**
     * Set zookeeper client backoff retry start time in millis.
     *
     * @param retryMs
     *          backoff retry start time in millis.
     * @return server configuration.
     */
    public ServerConfiguration setZkRetryBackoffStartMs(int retryMs) {
        setProperty(ZK_RETRY_BACKOFF_START_MS, retryMs);
        return this;
    }

    /**
     * Get zookeeper client backoff retry max time in millis.
     *
     * @return zk backoff retry max time in millis.
     */
    public int getZkRetryBackoffMaxMs() {
        return getInt(ZK_RETRY_BACKOFF_MAX_MS, getZkTimeout());
    }

    /**
     * Set zookeeper client backoff retry max time in millis.
     *
     * @param retryMs
     *          backoff retry max time in millis.
     * @return server configuration.
     */
    public ServerConfiguration setZkRetryBackoffMaxMs(int retryMs) {
        setProperty(ZK_RETRY_BACKOFF_MAX_MS, retryMs);
        return this;
    }

    /**
     * Is statistics enabled.
     *
     * @return is statistics enabled
     */
    public boolean isStatisticsEnabled() {
        return getBoolean(ENABLE_STATISTICS, true);
    }

    /**
     * Turn on/off statistics.
     *
     * @param enabled
     *          Whether statistics enabled or not.
     * @return server configuration
     */
    public ServerConfiguration setStatisticsEnabled(boolean enabled) {
        setProperty(ENABLE_STATISTICS, Boolean.toString(enabled));
        return this;
    }

    /**
     * Get threshold of minor compaction.
     *
     * <p>For those entry log files whose remaining size percentage reaches below
     * this threshold  will be compacted in a minor compaction.
     *
     * <p>If it is set to less than zero, the minor compaction is disabled.
     *
     * @return threshold of minor compaction
     */
    public double getMinorCompactionThreshold() {
        return getDouble(MINOR_COMPACTION_THRESHOLD, 0.2f);
    }

    /**
     * Set threshold of minor compaction.
     *
     * @see #getMinorCompactionThreshold()
     *
     * @param threshold
     *          Threshold for minor compaction
     * @return server configuration
     */
    public ServerConfiguration setMinorCompactionThreshold(double threshold) {
        setProperty(MINOR_COMPACTION_THRESHOLD, threshold);
        return this;
    }

    /**
     * Get threshold of major compaction.
     *
     * <p>For those entry log files whose remaining size percentage reaches below
     * this threshold  will be compacted in a major compaction.
     *
     * <p>If it is set to less than zero, the major compaction is disabled.
     *
     * @return threshold of major compaction
     */
    public double getMajorCompactionThreshold() {
        return getDouble(MAJOR_COMPACTION_THRESHOLD, 0.8f);
    }

    /**
     * Set threshold of major compaction.
     *
     * @see #getMajorCompactionThreshold()
     *
     * @param threshold
     *          Threshold of major compaction
     * @return server configuration
     */
    public ServerConfiguration setMajorCompactionThreshold(double threshold) {
        setProperty(MAJOR_COMPACTION_THRESHOLD, threshold);
        return this;
    }

    /**
     * Get interval to run minor compaction, in seconds.
     *
     * <p>If it is set to less than zero, the minor compaction is disabled.
     *
     * @return threshold of minor compaction
     */
    public long getMinorCompactionInterval() {
        return getLong(MINOR_COMPACTION_INTERVAL, 3600);
    }

    /**
     * Set interval to run minor compaction.
     *
     * @see #getMinorCompactionInterval()
     *
     * @param interval
     *          Interval to run minor compaction
     * @return server configuration
     */
    public ServerConfiguration setMinorCompactionInterval(long interval) {
        setProperty(MINOR_COMPACTION_INTERVAL, interval);
        return this;
    }

    /**
     * Get interval to run major compaction, in seconds.
     *
     * <p>If it is set to less than zero, the major compaction is disabled.
     *
     * @return high water mark
     */
    public long getMajorCompactionInterval() {
        return getLong(MAJOR_COMPACTION_INTERVAL, 86400);
    }

    /**
     * Set interval to run major compaction.
     *
     * @see #getMajorCompactionInterval()
     *
     * @param interval
     *          Interval to run major compaction
     * @return server configuration
     */
    public ServerConfiguration setMajorCompactionInterval(long interval) {
        setProperty(MAJOR_COMPACTION_INTERVAL, interval);
        return this;
    }

    /**
     * Get whether force compaction is allowed when disk full or almost full.
     *
     * <p>Force GC may get some space back, but may also fill up disk space more
     * quickly. This is because new log files are created before GC, while old
     * garbage log files deleted after GC.
     *
     * @return true  - do force GC when disk full,
     *         false - suspend GC when disk full.
     */
    public boolean getIsForceGCAllowWhenNoSpace() {
        return getBoolean(IS_FORCE_GC_ALLOW_WHEN_NO_SPACE, false);
    }

    /**
     * Set whether force GC is allowed when disk full or almost full.
     *
     * @param force true to allow force GC; false to suspend GC
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setIsForceGCAllowWhenNoSpace(boolean force) {
        setProperty(IS_FORCE_GC_ALLOW_WHEN_NO_SPACE, force);
        return this;
    }

    /**
     * Set the grace period which the rereplication worker will wait before
     * fencing and rereplicating a ledger fragment which is still being written
     * to, on bookie failure.
     *
     * <p>The grace period allows the writer to detect the bookie failure, and and
     * start writing to another ledger fragment. If the writer writes nothing
     * during the grace period, the rereplication worker assumes that it has
     * crashed and therefore fences the ledger, preventing any further writes to
     * that ledger.
     *
     * @see org.apache.bookkeeper.client.BookKeeper#openLedger
     *
     * @param waitTime time to wait before replicating ledger fragment
     */
    public void setOpenLedgerRereplicationGracePeriod(String waitTime) {
        setProperty(OPEN_LEDGER_REREPLICATION_GRACE_PERIOD, waitTime);
    }

    /**
     * Get the grace period which the rereplication worker to wait before
     * fencing and rereplicating a ledger fragment which is still being written
     * to, on bookie failure.
     *
     * @return long
     */
    public long getOpenLedgerRereplicationGracePeriod() {
        return getLong(OPEN_LEDGER_REREPLICATION_GRACE_PERIOD, 30000);
    }

    /**
     * Set the grace period, in milliseconds, which the replication worker has
     * to wait before releasing the lock after it failed to replicate a ledger.
     * For the first ReplicationWorker.NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS
     * failures it will do exponential backoff then it will bound at
     * LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD.
     *
     * <p>On replication failure, instead of releasing the lock immediately
     * after failed attempt, it will hold under replicated ledger lock for the
     * grace period and then it will release the lock.
     *
     * @param waitTime
     */
    public void setLockReleaseOfFailedLedgerGracePeriod(String waitTime) {
        setProperty(LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD, waitTime);
    }

    /**
     * Get the grace period, in milliseconds, which the replication worker has
     * to wait before releasing the lock after it failed to replicate a ledger.
     * For the first ReplicationWorker.NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS
     * failures it will do exponential backoff then it will bound at
     * LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD.
     *
     * @return
     */
    public long getLockReleaseOfFailedLedgerGracePeriod() {
        return getLong(LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD, 300000);
    }

    /**
     * Get the number of bytes we should use as capacity for
     * org.apache.bookkeeper.bookie.BufferedReadChannel.
     * Default is 512 bytes
     * @return read buffer size
     */
    public int getReadBufferBytes() {
        return getInt(READ_BUFFER_SIZE, 512);
    }

    /**
     * Set the number of bytes we should use as capacity for
     * org.apache.bookkeeper.bookie.BufferedReadChannel.
     *
     * @param readBufferSize
     *          Read Buffer Size
     * @return server configuration
     */
    public ServerConfiguration setReadBufferBytes(int readBufferSize) {
        setProperty(READ_BUFFER_SIZE, readBufferSize);
        return this;
    }

    /**
     * Set the number of threads that would handle write requests.
     *
     * @param numThreads
     *          number of threads to handle write requests.
     * @return server configuration
     */
    public ServerConfiguration setNumAddWorkerThreads(int numThreads) {
        setProperty(NUM_ADD_WORKER_THREADS, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should handle write requests.
     *
     * @return the number of threads that handle write requests.
     */
    public int getNumAddWorkerThreads() {
        return getInt(NUM_ADD_WORKER_THREADS, 1);
    }

    /**
     * Set the number of threads that should handle long poll requests.
     *
     * @param numThreads
     *          number of threads to handle long poll requests.
     * @return server configuration
     */
    public ServerConfiguration setNumLongPollWorkerThreads(int numThreads) {
        setProperty(NUM_LONG_POLL_WORKER_THREADS, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should handle long poll requests.
     *
     * <p>If the number of threads is zero or negative, bookie will fallback to
     * use read threads. If there is no read threads used, it will create a thread pool
     * with {@link Runtime#availableProcessors()} threads.
     *
     * @return the number of threads that should handle long poll requests, default value is 0.
     */
    public int getNumLongPollWorkerThreads() {
        return getInt(NUM_LONG_POLL_WORKER_THREADS, 0);
    }

    /**
     * Set the number of threads that should be used for high priority requests
     * (i.e. recovery reads and adds, and fencing)
     *
     * @param numThreads
     *          number of threads to handle high priority requests.
     * @return server configuration
     */
    public ServerConfiguration setNumHighPriorityWorkerThreads(int numThreads) {
        setProperty(NUM_HIGH_PRIORITY_WORKER_THREADS, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should be used for high priority requests
     * (i.e. recovery reads and adds, and fencing)
     * @return
     */
    public int getNumHighPriorityWorkerThreads() {
        return getInt(NUM_HIGH_PRIORITY_WORKER_THREADS, 8);
    }


    /**
     * Set the number of threads that would handle read requests.
     *
     * @param numThreads
     *          Number of threads to handle read requests.
     * @return server configuration
     */
    public ServerConfiguration setNumReadWorkerThreads(int numThreads) {
        setProperty(NUM_READ_WORKER_THREADS, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should handle read requests.
     */
    public int getNumReadWorkerThreads() {
        return getInt(NUM_READ_WORKER_THREADS, 8);
    }

    /**
     * Set the tick duration in milliseconds.
     *
     * @param tickDuration
     *          tick duration in milliseconds.
     * @return server configuration
     */
    public ServerConfiguration setRequestTimerTickDurationMs(int tickDuration) {
        setProperty(REQUEST_TIMER_TICK_DURATION_MILLISEC, tickDuration);
        return this;
    }

    /**
     * Set the max number of pending read requests for each read worker thread. After the quota is reached,
     * new requests will be failed immediately.
     *
     * @param maxPendingReadRequestsPerThread
     * @return server configuration
     */
    public ServerConfiguration setMaxPendingReadRequestPerThread(int maxPendingReadRequestsPerThread) {
        setProperty(MAX_PENDING_READ_REQUESTS_PER_THREAD, maxPendingReadRequestsPerThread);
        return this;
    }

    /**
     * If read workers threads are enabled, limit the number of pending requests, to avoid the executor queue to grow
     * indefinitely (default: 10000 entries).
     */
    public int getMaxPendingReadRequestPerThread() {
        return getInt(MAX_PENDING_READ_REQUESTS_PER_THREAD, 10000);
    }

    /**
     * Set the max number of pending add requests for each add worker thread. After the quota is reached, new requests
     * will be failed immediately.
     *
     * @param maxPendingAddRequestsPerThread
     * @return server configuration
     */
    public ServerConfiguration setMaxPendingAddRequestPerThread(int maxPendingAddRequestsPerThread) {
        setProperty(MAX_PENDING_ADD_REQUESTS_PER_THREAD, maxPendingAddRequestsPerThread);
        return this;
    }

    /**
     * If add workers threads are enabled, limit the number of pending requests, to avoid the executor queue to grow
     * indefinitely (default: 10000 entries).
     */
    public int getMaxPendingAddRequestPerThread() {
        return getInt(MAX_PENDING_ADD_REQUESTS_PER_THREAD, 10000);
    }



    /**
     * Get the tick duration in milliseconds.
     * @return
     */
    public int getRequestTimerTickDurationMs() {
        return getInt(REQUEST_TIMER_TICK_DURATION_MILLISEC, 10);
    }

    /**
     * Set the number of ticks per wheel for the request timer.
     *
     * @param tickCount
     *          number of ticks per wheel for the request timer.
     * @return server configuration
     */
    public ServerConfiguration setRequestTimerNumTicks(int tickCount) {
        setProperty(REQUEST_TIMER_NO_OF_TICKS, tickCount);
        return this;
    }

    /**
     * Get the number of ticks per wheel for the request timer.
     * @return
     */
    public int getRequestTimerNumTicks() {
        return getInt(REQUEST_TIMER_NO_OF_TICKS, 1024);
    }

    /**
     * Get the number of bytes used as capacity for the write buffer. Default is
     * 64KB.
     * NOTE: Make sure this value is greater than the maximum message size.
     * @return the size of the write buffer in bytes
     */
    public int getWriteBufferBytes() {
        return getInt(WRITE_BUFFER_SIZE, 65536);
    }

    /**
     * Set the number of bytes used as capacity for the write buffer.
     *
     * @param writeBufferBytes
     *          Write Buffer Bytes
     * @return server configuration
     */
    public ServerConfiguration setWriteBufferBytes(int writeBufferBytes) {
        setProperty(WRITE_BUFFER_SIZE, writeBufferBytes);
        return this;
    }

    /**
     * Set the number of threads that would handle journal callbacks.
     *
     * @param numThreads
     *          number of threads to handle journal callbacks.
     * @return server configuration
     */
    public ServerConfiguration setNumJournalCallbackThreads(int numThreads) {
        setProperty(NUM_JOURNAL_CALLBACK_THREADS, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should handle journal callbacks.
     *
     * @return the number of threads that handle journal callbacks.
     */
    public int getNumJournalCallbackThreads() {
        return getInt(NUM_JOURNAL_CALLBACK_THREADS, 1);
    }

    /**
     * Set sorted-ledger storage enabled or not.
     *
     * @deprecated Use {@link #setLedgerStorageClass(String)} to configure the implementation class
     * @param enabled
     */
    public ServerConfiguration setSortedLedgerStorageEnabled(boolean enabled) {
        this.setProperty(SORTED_LEDGER_STORAGE_ENABLED, enabled);
        return this;
    }

    /**
     * Check if sorted-ledger storage enabled (default true).
     *
     * @return true if sorted ledger storage is enabled, false otherwise
     */
    public boolean getSortedLedgerStorageEnabled() {
        return this.getBoolean(SORTED_LEDGER_STORAGE_ENABLED, true);
    }

    /**
     * Get skip list data size limitation (default 64MB).
     * Max value is 1,073,741,823
     *
     * @return skip list data size limitation
     */
    public long getSkipListSizeLimit() {
        return this.getLong(SKIP_LIST_SIZE_LIMIT, 64 * 1024 * 1024L);
    }

    /**
     * Set skip list size limit.
     *
     * @param size skip list size limit.
     * @return server configuration object.
     */
    public ServerConfiguration setSkipListSizeLimit(int size) {
        if (size > (Integer.MAX_VALUE - 1) / 2) {
            // gives max of 2*1023MB for mem table (one being checkpointed and still writable).
            throw new IllegalArgumentException("skiplist size over " + ((Integer.MAX_VALUE - 1) / 2));
        }
        setProperty(SKIP_LIST_SIZE_LIMIT, size);
        return this;
    }

    /**
     * Get the number of bytes we should use as chunk allocation for
     * org.apache.bookkeeper.bookie.SkipListArena.
     * Default is 4 MB
     * @return the number of bytes to use for each chunk in the skiplist arena
     */
    public int getSkipListArenaChunkSize() {
        return getInt(SKIP_LIST_CHUNK_SIZE_ENTRY, 4096 * 1024);
    }

    /**
     * Set the number of bytes we used as chunk allocation for
     * org.apache.bookkeeper.bookie.SkipListArena.
     *
     * @param size chunk size.
     * @return server configuration object.
     */
    public ServerConfiguration setSkipListArenaChunkSize(int size) {
        setProperty(SKIP_LIST_CHUNK_SIZE_ENTRY, size);
        return this;
    }

    /**
     * Get the max size we should allocate from the skiplist arena. Allocations
     * larger than this should be allocated directly by the VM to avoid fragmentation.
     *
     * @return max size allocatable from the skiplist arena (Default is 128 KB)
     */
    public int getSkipListArenaMaxAllocSize() {
        return getInt(SKIP_LIST_MAX_ALLOC_ENTRY, 128 * 1024);
    }

    /**
     * Set the max size we should allocate from the skiplist arena. Allocations
     * larger than this should be allocated directly by the VM to avoid fragmentation.
     *
     * @param size max alloc size.
     * @return server configuration object.
     */
    public ServerConfiguration setSkipListArenaMaxAllocSize(int size) {
        setProperty(SKIP_LIST_MAX_ALLOC_ENTRY, size);
        return this;
    }

    /**
     * Should the data be fsynced on journal before acknowledgment.
     *
     * <p>Default is true
     *
     * @return
     */
    public boolean getJournalSyncData() {
        return getBoolean(JOURNAL_SYNC_DATA, true);
    }

    /**
     * Enable or disable journal syncs.
     *
     * <p>By default, data sync is enabled to guarantee durability of writes.
     *
     * <p>Beware: while disabling data sync in the Bookie journal might improve the bookie write performance, it will
     * also introduce the possibility of data loss. With no sync, the journal entries are written in the OS page cache
     * but not flushed to disk. In case of power failure, the affected bookie might lose the unflushed data. If the
     * ledger is replicated to multiple bookies, the chances of data loss are reduced though still present.
     *
     * @param syncData
     *            whether to sync data on disk before acknowledgement
     * @return server configuration object
     */
    public ServerConfiguration setJournalSyncData(boolean syncData) {
        setProperty(JOURNAL_SYNC_DATA, syncData);
        return this;
    }

    /**
     * Should we group journal force writes.
     *
     * @return group journal force writes
     */
    public boolean getJournalAdaptiveGroupWrites() {
        return getBoolean(JOURNAL_ADAPTIVE_GROUP_WRITES, true);
    }

    /**
     * Enable/disable group journal force writes.
     *
     * @param enabled flag to enable/disable group journal force writes
     */
    public ServerConfiguration setJournalAdaptiveGroupWrites(boolean enabled) {
        setProperty(JOURNAL_ADAPTIVE_GROUP_WRITES, enabled);
        return this;
    }

    /**
     * Maximum latency to impose on a journal write to achieve grouping. Default is 2ms.
     *
     * @return max wait for grouping
     */
    public long getJournalMaxGroupWaitMSec() {
        return getLong(JOURNAL_MAX_GROUP_WAIT_MSEC, 2);
    }

    /**
     * Sets the maximum latency to impose on a journal write to achieve grouping.
     *
     * @param journalMaxGroupWaitMSec
     *          maximum time to wait in milliseconds.
     * @return server configuration.
     */
    public ServerConfiguration setJournalMaxGroupWaitMSec(long journalMaxGroupWaitMSec) {
        setProperty(JOURNAL_MAX_GROUP_WAIT_MSEC, journalMaxGroupWaitMSec);
        return this;
    }

    /**
     * Maximum bytes to buffer to impose on a journal write to achieve grouping.
     *
     * @return max bytes to buffer
     */
    public long getJournalBufferedWritesThreshold() {
        return getLong(JOURNAL_BUFFERED_WRITES_THRESHOLD, 512 * 1024);
    }

    /**
     * Set maximum bytes to buffer to impose on a journal write to achieve grouping.
     *
     * @param maxBytes maximum bytes to buffer to impose on a journal write
     * @return max bytes to buffer
     */
    public ServerConfiguration setJournalBufferedWritesThreshold(long maxBytes) {
        setProperty(JOURNAL_BUFFERED_WRITES_THRESHOLD, maxBytes);
        return this;
    }

    /**
     * Maximum entries to buffer to impose on a journal write to achieve grouping.
     * Use {@link #getJournalBufferedWritesThreshold()} if this is set to zero or
     * less than zero.
     *
     * @return max entries to buffer.
     */
    public long getJournalBufferedEntriesThreshold() {
        return getLong(JOURNAL_BUFFERED_ENTRIES_THRESHOLD, 0);
    }

    /**
     * Set maximum entries to buffer to impose on a journal write to achieve grouping.
     * Use {@link #getJournalBufferedWritesThreshold()} set this to zero or less than
     * zero.
     *
     * @param maxEntries
     *          maximum entries to buffer.
     * @return server configuration.
     */
    public ServerConfiguration setJournalBufferedEntriesThreshold(int maxEntries) {
        setProperty(JOURNAL_BUFFERED_ENTRIES_THRESHOLD, maxEntries);
        return this;
    }

    /**
     * Set if we should flush the journal when queue is empty.
     */
    public ServerConfiguration setJournalFlushWhenQueueEmpty(boolean enabled) {
        setProperty(JOURNAL_FLUSH_WHEN_QUEUE_EMPTY, enabled);
        return this;
    }

    /**
     * Should we flush the journal when queue is empty.
     *
     * @return flush when queue is empty
     */
    public boolean getJournalFlushWhenQueueEmpty() {
        return getBoolean(JOURNAL_FLUSH_WHEN_QUEUE_EMPTY, false);
    }

    /**
     * Set whether the bookie is able to go into read-only mode.
     * If this is set to false, the bookie will shutdown on encountering
     * an error condition.
     *
     * @param enabled whether to enable read-only mode.
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setReadOnlyModeEnabled(boolean enabled) {
        setProperty(READ_ONLY_MODE_ENABLED, enabled);
        return this;
    }

    /**
     * Get whether read-only mode is enabled. The default is true.
     *
     * @return boolean
     */
    public boolean isReadOnlyModeEnabled() {
        return getBoolean(READ_ONLY_MODE_ENABLED, true);
    }

    /**
     * Set the warning threshold for disk usage.
     *
     * @param threshold warning threshold to force gc.
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setDiskUsageWarnThreshold(float threshold) {
        setProperty(DISK_USAGE_WARN_THRESHOLD, threshold);
        return this;
    }

    /**
     * Returns the warning threshold for disk usage. If disk usage
     * goes beyond this, a garbage collection cycle will be forced.
     * @return the percentage at which a disk usage warning will trigger
     */
    public float getDiskUsageWarnThreshold() {
        return getFloat(DISK_USAGE_WARN_THRESHOLD, 0.90f);
    }

    /**
     * Whether to persist the bookie status so that when bookie server restarts,
     * it will continue using the previous status.
     *
     * @param enabled
     *            - true if persist the bookie status. Otherwise false.
     * @return ServerConfiguration
     */
    public ServerConfiguration setPersistBookieStatusEnabled(boolean enabled) {
        setProperty(PERSIST_BOOKIE_STATUS_ENABLED, enabled);
        return this;
    }

    /**
     * Get whether to persist the bookie status so that when bookie server restarts,
     * it will continue using the previous status.
     *
     * @return true - if need to start a bookie in read only mode. Otherwise false.
     */
    public boolean isPersistBookieStatusEnabled() {
        return getBoolean(PERSIST_BOOKIE_STATUS_ENABLED, false);
    }

    /**
     * Set the Disk free space threshold as a fraction of the total
     * after which disk will be considered as full during disk check.
     *
     * @param threshold threshold to declare a disk full
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setDiskUsageThreshold(float threshold) {
        setProperty(DISK_USAGE_THRESHOLD, threshold);
        return this;
    }

    /**
     * Returns disk free space threshold. By default it is 0.95.
     *
     * @return the percentage at which a disk will be considered full
     */
    public float getDiskUsageThreshold() {
        return getFloat(DISK_USAGE_THRESHOLD, 0.95f);
    }

    /**
     * Set the disk free space low water mark threshold.
     * Disk is considered full when usage threshold is exceeded.
     * Disk returns back to non-full state when usage is below low water mark threshold.
     * This prevents it from going back and forth between these states frequently
     * when concurrent writes and compaction are happening. This also prevent bookie from
     * switching frequently between read-only and read-writes states in the same cases.
     *
     * @param threshold threshold to declare a disk full
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setDiskLowWaterMarkUsageThreshold(float threshold) {
        setProperty(DISK_USAGE_LWM_THRESHOLD, threshold);
        return this;
    }

    /**
     * Returns disk free space low water mark threshold. By default it is the
     * same as usage threshold (for backwards-compatibility).
     *
     * @return the percentage below which a disk will NOT be considered full
     */
    public float getDiskLowWaterMarkUsageThreshold() {
        return getFloat(DISK_USAGE_LWM_THRESHOLD, getDiskUsageThreshold());
    }

    /**
     * Set the disk checker interval to monitor ledger disk space.
     *
     * @param interval interval between disk checks for space.
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setDiskCheckInterval(int interval) {
        setProperty(DISK_CHECK_INTERVAL, interval);
        return this;
    }

    /**
     * Get the disk checker interval.
     *
     * @return int
     */
    public int getDiskCheckInterval() {
        return getInt(DISK_CHECK_INTERVAL, 10 * 1000);
    }

    /**
     * Set the regularity at which the auditor will run a check
     * of all ledgers. This should not be run very often, and at most,
     * once a day. Setting this to 0 will completely disable the periodic
     * check.
     *
     * @param interval The interval in seconds. e.g. 86400 = 1 day, 604800 = 1 week
     */
    public void setAuditorPeriodicCheckInterval(long interval) {
        setProperty(AUDITOR_PERIODIC_CHECK_INTERVAL, interval);
    }

    /**
     * Get the regularity at which the auditor checks all ledgers.
     * @return The interval in seconds. Default is 604800 (1 week).
     */
    public long getAuditorPeriodicCheckInterval() {
        return getLong(AUDITOR_PERIODIC_CHECK_INTERVAL, 604800);
    }

    /**
     * Set the interval between auditor bookie checks.
     * The auditor bookie check, checks ledger metadata to see which bookies
     * contain entries for each ledger. If a bookie which should contain entries
     * is unavailable, then the ledger containing that entry is marked for recovery.
     * Setting this to 0 disabled the periodic check. Bookie checks will still
     * run when a bookie fails.
     *
     * @param interval The period in seconds.
     */
    public void setAuditorPeriodicBookieCheckInterval(long interval) {
        setProperty(AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL, interval);
    }

    /**
     * Get the interval between auditor bookie check runs.
     * @see #setAuditorPeriodicBookieCheckInterval(long)
     * @return the interval between bookie check runs, in seconds. Default is 86400 (= 1 day)
     */
    public long getAuditorPeriodicBookieCheckInterval() {
        return getLong(AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL, 86400);
    }

    /**
     * Sets the regularity/interval at which the auditor will run a placement
     * policy check of all ledgers, which are closed. This should not be run
     * very often, and should be run at most once a day. Setting this to 0 will
     * completely disable the periodic metadata check.
     *
     * @param interval
     *            The interval in seconds. e.g. 86400 = 1 day, 604800 = 1 week
     */
    public void setAuditorPeriodicPlacementPolicyCheckInterval(long interval) {
        setProperty(AUDITOR_PERIODIC_PLACEMENT_POLICY_CHECK_INTERVAL, interval);
    }

    /**
     * Get the regularity at which the auditor does placement policy check of
     * all ledgers, which are closed.
     *
     * @return The interval in seconds. By default it is disabled.
     */
    public long getAuditorPeriodicPlacementPolicyCheckInterval() {
        return getLong(AUDITOR_PERIODIC_PLACEMENT_POLICY_CHECK_INTERVAL, 0);
    }

    /**
     * Sets the grace period (in seconds) for underreplicated ledgers recovery.
     * If ledger is marked underreplicated for more than this period then it
     * will be reported by placementPolicyCheck in Auditor. Setting this to 0
     * will disable this check.
     *
     * @param gracePeriod
     *            The interval in seconds. e.g. 3600 = 1 hour
     */
    public void setUnderreplicatedLedgerRecoveryGracePeriod(long gracePeriod) {
        setProperty(UNDERREPLICATED_LEDGER_RECOVERY_GRACE_PERIOD, gracePeriod);
    }

    /**
     * Gets the grace period (in seconds) for underreplicated ledgers recovery.
     * If ledger is marked underreplicated for more than this period then it
     * will be reported by placementPolicyCheck in Auditor. Setting this to 0
     * will disable this check.
     *
     * @return The interval in seconds. By default it is disabled.
     */
    public long getUnderreplicatedLedgerRecoveryGracePeriod() {
        return getLong(UNDERREPLICATED_LEDGER_RECOVERY_GRACE_PERIOD, 0);
    }

    /**
     * Sets the interval at which the auditor will run a replicas check of all
     * ledgers. This should not be run very often since it validates
     * availability of replicas of all ledgers by querying bookies. Setting this
     * to 0 will disable the periodic replicas check.
     *
     * @param interval
     *            The interval in seconds. e.g. 86400 = 1 day, 604800 = 1 week
     */
    public void setAuditorPeriodicReplicasCheckInterval(long interval) {
        setProperty(AUDITOR_REPLICAS_CHECK_INTERVAL, interval);
    }

    /**
     * Get the interval at which the auditor does replicas check of all ledgers.
     *
     * @return The interval in seconds. By default it is disabled.
     */
    public long getAuditorPeriodicReplicasCheckInterval() {
        return getLong(AUDITOR_REPLICAS_CHECK_INTERVAL, 0);
    }

    /**
     * Set what percentage of a ledger (fragment)'s entries will be verified.
     * 0 - only the first and last entry of each ledger fragment would be verified
     * 100 - the entire ledger fragment would be verified
     * anything else - randomly picked entries from over the fragment would be verifiec
     * @param auditorLedgerVerificationPercentage The verification proportion as a percentage
     * @return ServerConfiguration
     */
    public ServerConfiguration setAuditorLedgerVerificationPercentage(long auditorLedgerVerificationPercentage) {
        setProperty(AUDITOR_LEDGER_VERIFICATION_PERCENTAGE, auditorLedgerVerificationPercentage);
        return this;
    }

    /**
     * Get what percentage of a ledger (fragment)'s entries will be verified.
     * @see #setAuditorLedgerVerificationPercentage(long)
     * @return percentage of a ledger (fragment)'s entries will be verified. Default is 0.
     */
    public long getAuditorLedgerVerificationPercentage() {
        return getLong(AUDITOR_LEDGER_VERIFICATION_PERCENTAGE, 0);
    }

    /**
     * Sets that whether the auto-recovery service can start along with Bookie
     * server itself or not.
     *
     * @param enabled
     *            - true if need to start auto-recovery service. Otherwise
     *            false.
     * @return ServerConfiguration
     */
    public ServerConfiguration setAutoRecoveryDaemonEnabled(boolean enabled) {
        setProperty(AUTO_RECOVERY_DAEMON_ENABLED, enabled);
        return this;
    }

    /**
     * Get whether the Bookie itself can start auto-recovery service also or not.
     *
     * @return true - if Bookie should start auto-recovery service along with
     *         it. false otherwise.
     */
    public boolean isAutoRecoveryDaemonEnabled() {
        return getBoolean(AUTO_RECOVERY_DAEMON_ENABLED, false);
    }

    /**
     * Get how long to delay the recovery of ledgers of a lost bookie.
     *
     * @return delay interval in seconds
     */
    public int getLostBookieRecoveryDelay() {
        return getInt(LOST_BOOKIE_RECOVERY_DELAY, 0);
    }

    /**
     * Set the delay interval for starting recovery of a lost bookie.
     */
    public void setLostBookieRecoveryDelay(int interval) {
        setProperty(LOST_BOOKIE_RECOVERY_DELAY, interval);
    }

    /**
     * Get how long to backoff when encountering exception on rereplicating a ledger.
     *
     * @return backoff time in milliseconds
     */
    public int getRwRereplicateBackoffMs() {
        return getInt(RW_REREPLICATE_BACKOFF_MS, 5000);
    }

    /**
     * Set how long to backoff when encountering exception on rereplicating a ledger.
     *
     * @param backoffMs backoff time in milliseconds
     */
    public void setRwRereplicateBackoffMs(int backoffMs) {
        setProperty(RW_REREPLICATE_BACKOFF_MS, backoffMs);
    }

    /**
     * Sets that whether force start a bookie in readonly mode.
     *
     * @param enabled
     *            - true if need to start a bookie in read only mode. Otherwise
     *            false.
     * @return ServerConfiguration
     */
    public ServerConfiguration setForceReadOnlyBookie(boolean enabled) {
        setProperty(FORCE_READ_ONLY_BOOKIE, enabled);
        return this;
    }

    /**
     * Get whether the Bookie is force started in read only mode or not.
     *
     * @return true - if need to start a bookie in read only mode. Otherwise
     *         false.
     */
    public boolean isForceReadOnlyBookie() {
        return getBoolean(FORCE_READ_ONLY_BOOKIE, false);
    }

    /**
     * Get whether use bytes to throttle garbage collector compaction or not.
     *
     * @return true  - use Bytes,
     *         false - use Entries.
     */
    public boolean getIsThrottleByBytes() {
        return getBoolean(IS_THROTTLE_BY_BYTES, false);
    }

    /**
     * Set whether use bytes to throttle garbage collector compaction or not.
     *
     * @param byBytes true to use by bytes; false to use by entries
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setIsThrottleByBytes(boolean byBytes) {
        setProperty(IS_THROTTLE_BY_BYTES, byBytes);
        return this;
    }

    /**
     * Get the maximum number of entries which can be compacted without flushing.
     * Default is 100,000.
     *
     * @return the maximum number of unflushed entries
     */
    public int getCompactionMaxOutstandingRequests() {
        return getInt(COMPACTION_MAX_OUTSTANDING_REQUESTS, 100000);
    }

    /**
     * Set the maximum number of entries which can be compacted without flushing.
     *
     * <p>When compacting, the entries are written to the entrylog and the new offsets
     * are cached in memory. Once the entrylog is flushed the index is updated with
     * the new offsets. This parameter controls the number of entries added to the
     * entrylog before a flush is forced. A higher value for this parameter means
     * more memory will be used for offsets. Each offset consists of 3 longs.
     *
     * <p>This parameter should _not_ be modified unless you know what you're doing.
     * The default is 100,000.
     *
     * @param maxOutstandingRequests number of entries to compact before flushing
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setCompactionMaxOutstandingRequests(int maxOutstandingRequests) {
        setProperty(COMPACTION_MAX_OUTSTANDING_REQUESTS, maxOutstandingRequests);
        return this;
    }

    /**
     * Get the rate of compaction adds. Default is 1,000.
     *
     * @return rate of compaction (adds per second)
     * @deprecated  replaced by {@link #getCompactionRateByEntries()}
     */
    @Deprecated
    public int getCompactionRate() {
        return getInt(COMPACTION_RATE, 1000);
    }

    /**
     * Set the rate of compaction adds.
     *
     * @param rate rate of compaction adds (adds entries per second)
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setCompactionRate(int rate) {
        setProperty(COMPACTION_RATE, rate);
        return this;
    }

    /**
     * Get the rate of compaction adds. Default is 1,000.
     *
     * @return rate of compaction (adds entries per second)
     */
    public int getCompactionRateByEntries() {
        return getInt(COMPACTION_RATE_BY_ENTRIES, getCompactionRate());
    }

    /**
     * Set the rate of compaction adds.
     *
     * @param rate rate of compaction adds (adds entries per second)
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setCompactionRateByEntries(int rate) {
        setProperty(COMPACTION_RATE_BY_ENTRIES, rate);
        return this;
    }

    /**
     * Get the rate of compaction adds. Default is 1,000,000.
     *
     * @return rate of compaction (adds bytes per second)
     */
    public int getCompactionRateByBytes() {
        return getInt(COMPACTION_RATE_BY_BYTES, 1000000);
    }

    /**
     * Set the rate of compaction adds.
     *
     * @param rate rate of compaction adds (adds bytes per second)
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setCompactionRateByBytes(int rate) {
        setProperty(COMPACTION_RATE_BY_BYTES, rate);
        return this;
    }

    /**
     * Should we remove pages from page cache after force write.
     *
     * @return remove pages from cache
     */
    @Beta
    public boolean getJournalRemovePagesFromCache() {
        return getBoolean(JOURNAL_REMOVE_FROM_PAGE_CACHE, true);
    }

    /**
     * Sets that whether should we remove pages from page cache after force write.
     *
     * @param enabled
     *            - true if we need to remove pages from page cache. otherwise, false
     * @return ServerConfiguration
     */
    public ServerConfiguration setJournalRemovePagesFromCache(boolean enabled) {
        setProperty(JOURNAL_REMOVE_FROM_PAGE_CACHE, enabled);
        return this;
    }

    /*
     * Get the {@link LedgerStorage} implementation class name.
     *
     * @return the class name
     */
    public String getLedgerStorageClass() {
        String ledgerStorageClass = LEDGER_STORAGE_CLASS_KEY.getString(this);
        if (ledgerStorageClass.equals(SortedLedgerStorage.class.getName())
                && !getSortedLedgerStorageEnabled()) {
            // This is to retain compatibility with BK-4.3 configuration
            // In BK-4.3, the ledger storage is configured through the "sortedLedgerStorageEnabled" flag :
            // sortedLedgerStorageEnabled == true (default) ---> use SortedLedgerStorage
            // sortedLedgerStorageEnabled == false ---> use InterleavedLedgerStorage
            //
            // Since BK-4.4, one can specify the implementation class, but if it was using InterleavedLedgerStorage it
            // should continue to use that with the same configuration
            ledgerStorageClass = InterleavedLedgerStorage.class.getName();
        }

        return ledgerStorageClass;
    }

    /**
     * Set the {@link LedgerStorage} implementation class name.
     *
     * @param ledgerStorageClass the class name
     * @return ServerConfiguration
     */
    public ServerConfiguration setLedgerStorageClass(String ledgerStorageClass) {
        LEDGER_STORAGE_CLASS_KEY.set(this, ledgerStorageClass);
        return this;
    }

    /**
     * Get whether bookie is using hostname for registration and in ledger
     * metadata. Defaults to false.
     *
     * @return true, then bookie will be registered with its hostname and
     *         hostname will be used in ledger metadata. Otherwise bookie will
     *         use its ipaddress
     */
    public boolean getUseHostNameAsBookieID() {
        return getBoolean(USE_HOST_NAME_AS_BOOKIE_ID, false);
    }

    /**
     * Configure the bookie to use its hostname to register with the
     * co-ordination service(eg: zookeeper) and in ledger metadata.
     *
     * @see #getUseHostNameAsBookieID
     * @param useHostName
     *            whether to use hostname for registration and in ledgermetadata
     * @return server configuration
     */
    public ServerConfiguration setUseHostNameAsBookieID(boolean useHostName) {
        setProperty(USE_HOST_NAME_AS_BOOKIE_ID, useHostName);
        return this;
    }

    /**
     * If bookie is using hostname for registration and in ledger metadata then
     * whether to use short hostname or FQDN hostname. Defaults to false.
     *
     * @return true, then bookie will be registered with its short hostname and
     *         short hostname will be used in ledger metadata. Otherwise bookie
     *         will use its FQDN hostname
     */
    public boolean getUseShortHostName() {
        return getBoolean(USE_SHORT_HOST_NAME, false);
    }

    /**
     * Configure the bookie to use its short hostname or FQDN hostname to
     * register with the co-ordination service(eg: zookeeper) and in ledger
     * metadata.
     *
     * @see #getUseShortHostName
     * @param useShortHostName
     *            whether to use short hostname for registration and in
     *            ledgermetadata
     * @return server configuration
     */
    public ServerConfiguration setUseShortHostName(boolean useShortHostName) {
        setProperty(USE_SHORT_HOST_NAME, useShortHostName);
        return this;
    }

    /**
     * Get whether to listen for local JVM clients. Defaults to false.
     *
     * @return true, then bookie will be listen for local JVM clients
     */
    public boolean isEnableLocalTransport() {
        return getBoolean(ENABLE_LOCAL_TRANSPORT, false);
    }

    /**
     * Configure the bookie to listen for BookKeeper clients executed on the local JVM.
     *
     * @see #isEnableLocalTransport
     * @param enableLocalTransport
     *            whether to use listen for local JVM clients
     * @return server configuration
     */
    public ServerConfiguration setEnableLocalTransport(boolean enableLocalTransport) {
        setProperty(ENABLE_LOCAL_TRANSPORT, enableLocalTransport);
        return this;
    }

    /**
     * Get whether to disable bind of server-side sockets. Defaults to false.
     *
     * @return true, then bookie will not listen for network connections
     */
    public boolean isDisableServerSocketBind() {
        return getBoolean(DISABLE_SERVER_SOCKET_BIND, false);
    }

    /**
     * Configure the bookie to disable bind on network interfaces,
     * this bookie will be available only to BookKeeper clients executed on the local JVM.
     *
     * @see #isDisableServerSocketBind
     * @param disableServerSocketBind
     *            whether to disable binding on network interfaces
     * @return server configuration
     */
    public ServerConfiguration setDisableServerSocketBind(boolean disableServerSocketBind) {
        setProperty(DISABLE_SERVER_SOCKET_BIND, disableServerSocketBind);
        return this;
    }

    /**
     * Get the stats provider used by bookie.
     *
     * @return stats provider class
     * @throws ConfigurationException
     */
    public Class<? extends StatsProvider> getStatsProviderClass()
        throws ConfigurationException {
        return ReflectionUtils.getClass(this, STATS_PROVIDER_CLASS,
                                        NullStatsProvider.class, StatsProvider.class,
                                        DEFAULT_LOADER);
    }

    /**
     * Set the stats provider used by bookie.
     *
     * @param providerClass
     *          stats provider class
     * @return server configuration
     */
    public ServerConfiguration setStatsProviderClass(Class<? extends StatsProvider> providerClass) {
        setProperty(STATS_PROVIDER_CLASS, providerClass.getName());
        return this;
    }

    /**
     * Validate the configuration.
     * @throws ConfigurationException
     */
    public void validate() throws ConfigurationException {
        // generate config def
        ConfigDef configDef = ConfigDef.of(ServerConfiguration.class);
        try {
            configDef.validate(this);
        } catch (ConfigException e) {
            throw new ConfigurationException(e.getMessage(), e.getCause());
        }

        if (getSkipListArenaChunkSize() < getSkipListArenaMaxAllocSize()) {
            throw new ConfigurationException("Arena max allocation size should be smaller than the chunk size.");
        }
        if (getJournalAlignmentSize() < 512 || getJournalAlignmentSize() % 512 != 0) {
            throw new ConfigurationException("Invalid journal alignment size : " + getJournalAlignmentSize());
        }
        if (getJournalAlignmentSize() > getJournalPreAllocSizeMB() * 1024 * 1024) {
            throw new ConfigurationException("Invalid preallocation size : " + getJournalPreAllocSizeMB() + " MB");
        }
        if (0 == getBookiePort() && !getAllowEphemeralPorts()) {
            throw new ConfigurationException("Invalid port specified, using ephemeral ports accidentally?");
        }
        if (isEntryLogPerLedgerEnabled() && getUseTransactionalCompaction()) {
            throw new ConfigurationException(
                    "When entryLogPerLedger is enabled , it is unnecessary to use transactional compaction");
        }
        if ((getJournalFormatVersionToWrite() >= 6) ^ (getFileInfoFormatVersionToWrite() >= 1)) {
            throw new ConfigurationException("For persisiting explicitLac, journalFormatVersionToWrite should be >= 6"
                    + "and FileInfoFormatVersionToWrite should be >= 1");
        }
    }

    /**
     * Get Recv ByteBuf allocator initial buf size.
     *
     * @return initial byteBuf size
     */
    public int getRecvByteBufAllocatorSizeInitial() {
        return getInt(BYTEBUF_ALLOCATOR_SIZE_INITIAL, 64 * 1024);
    }

    /**
     * Set Recv ByteBuf allocator initial buf size.
     *
     * @param size
     *            buffer size
     */
    public void setRecvByteBufAllocatorSizeInitial(int size) {
        setProperty(BYTEBUF_ALLOCATOR_SIZE_INITIAL, size);
    }

    /**
     * Get Recv ByteBuf allocator min buf size.
     *
     * @return min byteBuf size
     */
    public int getRecvByteBufAllocatorSizeMin() {
        return getInt(BYTEBUF_ALLOCATOR_SIZE_MIN, 64 * 1024);
    }

    /**
     * Set Recv ByteBuf allocator min buf size.
     *
     * @param size
     *            buffer size
     */
    public void setRecvByteBufAllocatorSizeMin(int size) {
        setProperty(BYTEBUF_ALLOCATOR_SIZE_MIN, size);
    }

    /**
     * Get Recv ByteBuf allocator max buf size.
     *
     * @return max byteBuf size
     */
    public int getRecvByteBufAllocatorSizeMax() {
        return getInt(BYTEBUF_ALLOCATOR_SIZE_MAX, 1 * 1024 * 1024);
    }

    /**
     * Set Recv ByteBuf allocator max buf size.
     *
     * @param size
     *            buffer size
     */
    public void setRecvByteBufAllocatorSizeMax(int size) {
        setProperty(BYTEBUF_ALLOCATOR_SIZE_MAX, size);
    }

    /**
     * Set the bookie authentication provider factory class name.
     * If this is not set, no authentication will be used.
     *
     * @param factoryClass
     *          the bookie authentication provider factory class name
     */
    public void setBookieAuthProviderFactoryClass(String factoryClass) {
        setProperty(BOOKIE_AUTH_PROVIDER_FACTORY_CLASS, factoryClass);
    }

    /**
     * Get the bookie authentication provider factory class name.
     * If this returns null, no authentication will take place.
     *
     * @return the bookie authentication provider factory class name or null.
     */
    public String getBookieAuthProviderFactoryClass() {
        return getString(BOOKIE_AUTH_PROVIDER_FACTORY_CLASS, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ServerConfiguration setNettyMaxFrameSizeBytes(int maxSize) {
        super.setNettyMaxFrameSizeBytes(maxSize);
        return this;
    }

    /**
     * Get the truststore type for client. Default is JKS.
     *
     * @return
     */
    public String getTLSTrustStoreType() {
        return getString(TLS_TRUSTSTORE_TYPE, "JKS");
    }

    /**
     * Set the keystore type for client.
     *
     * @return
     */
    public ServerConfiguration setTLSKeyStoreType(String arg) {
        setProperty(TLS_KEYSTORE_TYPE, arg);
        return this;
    }

    /**
     * Get the keystore path for the client.
     *
     * @return
     */
    public String getTLSKeyStore() {
        return getString(TLS_KEYSTORE, null);
    }

    /**
     * Set the keystore path for the client.
     *
     * @return
     */
    public ServerConfiguration setTLSKeyStore(String arg) {
        setProperty(TLS_KEYSTORE, arg);
        return this;
    }

    /**
     * Get the path to file containing keystore password if the client keystore is password protected. Default is null.
     *
     * @return
     */
    public String getTLSKeyStorePasswordPath() {
        return getString(TLS_KEYSTORE_PASSWORD_PATH, null);
    }

    /**
     * Set the path to file containing keystore password, if the client keystore is password protected.
     *
     * @return
     */
    public ServerConfiguration setTLSKeyStorePasswordPath(String arg) {
        setProperty(TLS_KEYSTORE_PASSWORD_PATH, arg);
        return this;
    }

    /**
     * Get the keystore type for client. Default is JKS.
     *
     * @return
     */
    public String getTLSKeyStoreType() {
        return getString(TLS_KEYSTORE_TYPE, "JKS");
    }

    /**
     * Set the truststore type for client.
     *
     * @return
     */
    public ServerConfiguration setTLSTrustStoreType(String arg) {
        setProperty(TLS_TRUSTSTORE_TYPE, arg);
        return this;
    }

    /**
     * Get the truststore path for the client.
     *
     * @return
     */
    public String getTLSTrustStore() {
        return getString(TLS_TRUSTSTORE, null);
    }

    /**
     * Set the truststore path for the client.
     *
     * @return
     */
    public ServerConfiguration setTLSTrustStore(String arg) {
        setProperty(TLS_TRUSTSTORE, arg);
        return this;
    }

    /**
     * Get the path to file containing truststore password if the client truststore is password protected. Default is
     * null.
     *
     * @return
     */
    public String getTLSTrustStorePasswordPath() {
        return getString(TLS_TRUSTSTORE_PASSWORD_PATH, null);
    }

    /**
     * Set the path to file containing truststore password, if the client truststore is password protected.
     *
     * @return
     */
    public ServerConfiguration setTLSTrustStorePasswordPath(String arg) {
        setProperty(TLS_TRUSTSTORE_PASSWORD_PATH, arg);
        return this;
    }

    /**
     * Get the path to file containing TLS Certificate.
     *
     * @return
     */
    public String getTLSCertificatePath() {
        return getString(TLS_CERTIFICATE_PATH, null);
    }

    /**
     * Set the path to file containing TLS Certificate.
     *
     * @return
     */
    public ServerConfiguration setTLSCertificatePath(String arg) {
        setProperty(TLS_CERTIFICATE_PATH, arg);
        return this;
    }


    /**
     * Whether to enable recording task execution stats.
     *
     * @return flag to enable/disable recording task execution stats.
     */
    public boolean getEnableTaskExecutionStats() {
        return getBoolean(ENABLE_TASK_EXECUTION_STATS, false);
    }

    /**
     * Enable/Disable recording task execution stats.
     *
     * @param enabled
     *          flag to enable/disable recording task execution stats.
     * @return client configuration.
     */
    public ServerConfiguration setEnableTaskExecutionStats(boolean enabled) {
        setProperty(ENABLE_TASK_EXECUTION_STATS, enabled);
        return this;
    }

    /**
     * Gets the minimum safe Usable size to be available in index directory for Bookie to create Index File while
     * replaying journal at the time of Bookie Start in Readonly Mode (in bytes).
     *
     * @return minimum safe usable size to be available in index directory for bookie to create index files.
     * @see #setMinUsableSizeForIndexFileCreation(long)
     */
    public long getMinUsableSizeForIndexFileCreation() {
        return this.getLong(MIN_USABLESIZE_FOR_INDEXFILE_CREATION, 100 * 1024 * 1024L);
    }

    /**
     * Sets the minimum safe Usable size to be available in index directory for Bookie to create Index File while
     * replaying journal at the time of Bookie Start in Readonly Mode (in bytes).
     *
     * <p>This parameter allows creating index files when there are enough disk spaces, even when the bookie
     * is running at readonly mode because of the disk usage is exceeding {@link #getDiskUsageThreshold()}. Because
     * compaction, journal replays can still write index files to disks when a bookie is readonly.
     *
     * @param minUsableSizeForIndexFileCreation min usable size for index file creation
     * @return server configuration
     */
    public ServerConfiguration setMinUsableSizeForIndexFileCreation(long minUsableSizeForIndexFileCreation) {
        this.setProperty(MIN_USABLESIZE_FOR_INDEXFILE_CREATION, minUsableSizeForIndexFileCreation);
        return this;
    }

    /**
     * Gets the minimum safe usable size to be available in ledger directory for Bookie to create entry log files.
     *
     * @return minimum safe usable size to be available in ledger directory for entry log file creation.
     * @see #setMinUsableSizeForEntryLogCreation(long)
     */
    public long getMinUsableSizeForEntryLogCreation() {
        return this.getLong(MIN_USABLESIZE_FOR_ENTRYLOG_CREATION, (long) 1.2 * getEntryLogSizeLimit());
    }

    /**
     * Sets the minimum safe usable size to be available in ledger directory for Bookie to create entry log files.
     *
     * <p>This parameter allows creating entry log files when there are enough disk spaces, even when the bookie
     * is running at readonly mode because of the disk usage is exceeding {@link #getDiskUsageThreshold()}. Because
     * compaction, journal replays can still write data to disks when a bookie is readonly.
     *
     * @param minUsableSizeForEntryLogCreation minimum safe usable size to be available in ledger directory
     * @return server configuration
     */
    public ServerConfiguration setMinUsableSizeForEntryLogCreation(long minUsableSizeForEntryLogCreation) {
        this.setProperty(MIN_USABLESIZE_FOR_ENTRYLOG_CREATION, minUsableSizeForEntryLogCreation);
        return this;
    }

    /**
     * Gets the minimum safe usable size to be available in ledger directory for Bookie to accept high priority writes.
     *
     * <p>If not set, it is the value of {@link #getMinUsableSizeForEntryLogCreation()}.
     *
     * @return the minimum safe usable size per ledger directory for bookie to accept high priority writes.
     */
    public long getMinUsableSizeForHighPriorityWrites() {
        return this.getLong(MIN_USABLESIZE_FOR_HIGH_PRIORITY_WRITES, getMinUsableSizeForEntryLogCreation());
    }

    /**
     * Sets the minimum safe usable size to be available in ledger directory for Bookie to accept high priority writes.
     *
     * @param minUsableSizeForHighPriorityWrites minimum safe usable size per ledger directory for Bookie to accept
     *                                           high priority writes
     * @return server configuration.
     */
    public ServerConfiguration setMinUsableSizeForHighPriorityWrites(long minUsableSizeForHighPriorityWrites) {
        this.setProperty(MIN_USABLESIZE_FOR_HIGH_PRIORITY_WRITES, minUsableSizeForHighPriorityWrites);
        return this;
    }

    /**
     * returns whether it is allowed to have multiple ledger/index/journal
     * Directories in the same filesystem diskpartition.
     *
     * @return
     */
    public boolean isAllowMultipleDirsUnderSameDiskPartition() {
        return this.getBoolean(ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION, true);
    }

    /**
     * Configure the Bookie to allow/disallow multiple ledger/index/journal
     * directories in the same filesystem diskpartition.
     *
     * @param allow
     *
     * @return server configuration object.
     */
    public ServerConfiguration setAllowMultipleDirsUnderSameDiskPartition(boolean allow) {
        this.setProperty(ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION, allow);
        return this;
    }

    /**
     * Get whether to start the http server or not.
     *
     * @return true - if http server should start
     */
    public boolean isHttpServerEnabled() {
        return getBoolean(HTTP_SERVER_ENABLED, false);
    }

    /**
     * Set whether to start the http server or not.
     *
     * @param enabled
     *            - true if we should start http server
     * @return ServerConfiguration
     */
    public ServerConfiguration setHttpServerEnabled(boolean enabled) {
        setProperty(HTTP_SERVER_ENABLED, enabled);
        return this;
    }

    /**
     * Get the http server port.
     *
     * @return http server port
     */
    public int getHttpServerPort() {
        return getInt(HTTP_SERVER_PORT, 8080);
    }

    /**
     * Set Http server port listening on.
     *
     * @param port
     *          Port to listen on
     * @return server configuration
     */
    public ServerConfiguration setHttpServerPort(int port) {
        setProperty(HTTP_SERVER_PORT, port);
        return this;
    }

    /**
     * Get the extra list of server lifecycle components to enable on a bookie server.
     *
     * @return the extra list of server lifecycle components to enable on a bookie server.
     */
    public String[] getExtraServerComponents() {
        String extraServerComponentsStr = getString(EXTRA_SERVER_COMPONENTS);
        if (Strings.isNullOrEmpty(extraServerComponentsStr)) {
            return null;
        }
        return this.getStringArray(EXTRA_SERVER_COMPONENTS);
    }

    /**
     * Set the extra list of server lifecycle components to enable on a bookie server.
     *
     * @param componentClasses
     *          the list of server lifecycle components to enable on a bookie server.
     * @return server configuration.
     */
    public ServerConfiguration setExtraServerComponents(String[] componentClasses) {
        this.setProperty(EXTRA_SERVER_COMPONENTS, componentClasses);
        return this;
    }

    /**
     * Return the flag whether to ignore startup failures on loading server components specified at
     * {@link #getExtraServerComponents()}.
     *
     * @return the flag whether to ignore startup failures on loading server components specified at
     * {@link #getExtraServerComponents()}. The default value is <tt>false</tt>.
     */
    public boolean getIgnoreExtraServerComponentsStartupFailures() {
        return getBoolean(IGNORE_EXTRA_SERVER_COMPONENTS_STARTUP_FAILURES, false);
    }

    /**
     * Set the flag whether to ignore startup failures on loading server components specified at
     * {@link #getExtraServerComponents()}.
     *
     * @param enabled flag to enable/disable ignoring startup failures on loading server components.
     * @return server configuration.
     */
    public ServerConfiguration setIgnoreExtraServerComponentsStartupFailures(boolean enabled) {
        setProperty(IGNORE_EXTRA_SERVER_COMPONENTS_STARTUP_FAILURES, enabled);
        return this;
    }

    /**
     * Get server netty channel write buffer low water mark.
     *
     * @return netty channel write buffer low water mark.
     */
    public int getServerWriteBufferLowWaterMark() {
        return getInt(SERVER_WRITEBUFFER_LOW_WATER_MARK, 384 * 1024);
    }

    /**
     * Set server netty channel write buffer low water mark.
     *
     * @param waterMark
     *          netty channel write buffer low water mark.
     * @return client configuration.
     */
    public ServerConfiguration setServerWriteBufferLowWaterMark(int waterMark) {
        setProperty(SERVER_WRITEBUFFER_LOW_WATER_MARK, waterMark);
        return this;
    }

    /**
     * Get server netty channel write buffer high water mark.
     *
     * @return netty channel write buffer high water mark.
     */
    public int getServerWriteBufferHighWaterMark() {
        return getInt(SERVER_WRITEBUFFER_HIGH_WATER_MARK, 512 * 1024);
    }

    /**
     * Set server netty channel write buffer high water mark.
     *
     * @param waterMark
     *          netty channel write buffer high water mark.
     * @return client configuration.
     */
    public ServerConfiguration setServerWriteBufferHighWaterMark(int waterMark) {
        setProperty(SERVER_WRITEBUFFER_HIGH_WATER_MARK, waterMark);
        return this;
    }
    /**
     * Set registration manager class.
     *
     * @param regManagerClass
     *            ManagerClass
     * @deprecated since 4.7.0, in favor of using {@link #setMetadataServiceUri(String)}
     */
    @Deprecated
    public void setRegistrationManagerClass(
            Class<? extends RegistrationManager> regManagerClass) {
        setProperty(REGISTRATION_MANAGER_CLASS, regManagerClass);
    }

    /**
     * Get Registration Manager Class.
     *
     * @return registration manager class.
     * @deprecated since 4.7.0, in favor of using {@link #getMetadataServiceUri()}
     */
    @Deprecated
    public Class<? extends RegistrationManager> getRegistrationManagerClass()
            throws ConfigurationException {
        return ReflectionUtils.getClass(this, REGISTRATION_MANAGER_CLASS,
                ZKRegistrationManager.class, RegistrationManager.class,
                DEFAULT_LOADER);
    }

    @Override
    protected ServerConfiguration getThis() {
        return this;
    }

    /*
     * specifies if entryLog per ledger is enabled. If it is enabled, then there
     * would be a active entrylog for each ledger
     */
    public boolean isEntryLogPerLedgerEnabled() {
        return this.getBoolean(ENTRY_LOG_PER_LEDGER_ENABLED, false);
    }

    /*
     * enables/disables entrylog per ledger feature.
     *
     */
    public ServerConfiguration setEntryLogPerLedgerEnabled(boolean entryLogPerLedgerEnabled) {
        this.setProperty(ENTRY_LOG_PER_LEDGER_ENABLED, Boolean.toString(entryLogPerLedgerEnabled));
        return this;
    }

    /*
     * In the case of multipleentrylogs, multiple threads can be used to flush the memtable.
     *
     * Gets the number of threads used to flush entrymemtable
     */
    public int getNumOfMemtableFlushThreads() {
        return this.getInt(NUMBER_OF_MEMTABLE_FLUSH_THREADS, 8);
    }

    /*
     * Sets the number of threads used to flush entrymemtable, in the case of multiple entrylogs
     *
     */
    public ServerConfiguration setNumOfMemtableFlushThreads(int numOfMemtableFlushThreads) {
        this.setProperty(NUMBER_OF_MEMTABLE_FLUSH_THREADS, Integer.toString(numOfMemtableFlushThreads));
        return this;
    }

    /*
     * in entryLogPerLedger feature, this specifies the time, once this duration
     * has elapsed after the entry's last access, that entry should be
     * automatically removed from the cache
     */
    public int getEntrylogMapAccessExpiryTimeInSeconds() {
        return this.getInt(ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS, 5 * 60);
    }

    /*
     * sets the time duration for entrylogMapAccessExpiryTimeInSeconds, which will be used for cache eviction
     * policy, in entrylogperledger feature.
     */
    public ServerConfiguration setEntrylogMapAccessExpiryTimeInSeconds(int entrylogMapAccessExpiryTimeInSeconds) {
        this.setProperty(ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS,
                Integer.toString(entrylogMapAccessExpiryTimeInSeconds));
        return this;
    }

    /*
     * get the maximum number of entrylogs that can be active at a given point
     * in time.
     */
    public int getMaximumNumberOfActiveEntryLogs() {
        return this.getInt(MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS, 500);
    }

    /*
     * sets the maximum number of entrylogs that can be active at a given point
     * in time.
     */
    public ServerConfiguration setMaximumNumberOfActiveEntryLogs(int maximumNumberOfActiveEntryLogs) {
        this.setProperty(MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS,
                Integer.toString(maximumNumberOfActiveEntryLogs));
        return this;
    }

    /*
     * in EntryLogManagerForEntryLogPerLedger, this config value specifies the
     * metrics cache size limits in multiples of entrylogMap cache size limits.
     */
    public int getEntryLogPerLedgerCounterLimitsMultFactor() {
        return this.getInt(ENTRY_LOG_PER_LEDGER_COUNTER_LIMITS_MULT_FACTOR, 10);
    }

    /*
     * in EntryLogManagerForEntryLogPerLedger, this config value specifies the
     * metrics cache size limits in multiples of entrylogMap cache size limits.
     */
    public ServerConfiguration setEntryLogPerLedgerCounterLimitsMultFactor(
            int entryLogPerLedgerCounterLimitsMultFactor) {
        this.setProperty(ENTRY_LOG_PER_LEDGER_COUNTER_LIMITS_MULT_FACTOR,
                Integer.toString(entryLogPerLedgerCounterLimitsMultFactor));
        return this;
    }

    /**
     * True if a local consistency check should be performed on startup.
     */
    public boolean isLocalConsistencyCheckOnStartup() {
        return this.getBoolean(LOCAL_CONSISTENCY_CHECK_ON_STARTUP, false);
    }
}
