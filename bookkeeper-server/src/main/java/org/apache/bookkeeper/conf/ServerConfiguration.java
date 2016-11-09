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

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.Beta;

import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.commons.lang.StringUtils;

/**
 * Configuration manages server-side settings
 */
public class ServerConfiguration extends AbstractConfiguration {
    // Entry Log Parameters
    protected final static String ENTRY_LOG_SIZE_LIMIT = "logSizeLimit";
    protected final static String ENTRY_LOG_FILE_PREALLOCATION_ENABLED = "entryLogFilePreallocationEnabled";
    protected final static String MINOR_COMPACTION_INTERVAL = "minorCompactionInterval";
    protected final static String MINOR_COMPACTION_THRESHOLD = "minorCompactionThreshold";
    protected final static String MAJOR_COMPACTION_INTERVAL = "majorCompactionInterval";
    protected final static String MAJOR_COMPACTION_THRESHOLD = "majorCompactionThreshold";
    protected final static String IS_THROTTLE_BY_BYTES = "isThrottleByBytes";
    protected final static String COMPACTION_MAX_OUTSTANDING_REQUESTS
        = "compactionMaxOutstandingRequests";
    protected final static String COMPACTION_RATE = "compactionRate";
    protected final static String COMPACTION_RATE_BY_ENTRIES = "compactionRateByEntries";
    protected final static String COMPACTION_RATE_BY_BYTES = "compactionRateByBytes";

    // Gc Parameters
    protected final static String GC_WAIT_TIME = "gcWaitTime";
    protected final static String IS_FORCE_GC_ALLOW_WHEN_NO_SPACE = "isForceGCAllowWhenNoSpace";
    protected final static String GC_OVERREPLICATED_LEDGER_WAIT_TIME = "gcOverreplicatedLedgerWaitTime";
    // Sync Parameters
    protected final static String FLUSH_INTERVAL = "flushInterval";
    // Bookie death watch interval
    protected final static String DEATH_WATCH_INTERVAL = "bookieDeathWatchInterval";
    // Ledger Cache Parameters
    protected final static String OPEN_FILE_LIMIT = "openFileLimit";
    protected final static String PAGE_LIMIT = "pageLimit";
    protected final static String PAGE_SIZE = "pageSize";
    // Journal Parameters
    protected final static String MAX_JOURNAL_SIZE = "journalMaxSizeMB";
    protected final static String MAX_BACKUP_JOURNALS = "journalMaxBackups";
    protected final static String JOURNAL_ADAPTIVE_GROUP_WRITES = "journalAdaptiveGroupWrites";
    protected final static String JOURNAL_MAX_GROUP_WAIT_MSEC = "journalMaxGroupWaitMSec";
    protected final static String JOURNAL_BUFFERED_WRITES_THRESHOLD = "journalBufferedWritesThreshold";
    protected final static String JOURNAL_BUFFERED_ENTRIES_THRESHOLD = "journalBufferedEntriesThreshold";
    protected final static String JOURNAL_FLUSH_WHEN_QUEUE_EMPTY = "journalFlushWhenQueueEmpty";
    protected final static String JOURNAL_REMOVE_FROM_PAGE_CACHE = "journalRemoveFromPageCache";
    protected final static String JOURNAL_PRE_ALLOC_SIZE = "journalPreAllocSizeMB";
    protected final static String JOURNAL_WRITE_BUFFER_SIZE = "journalWriteBufferSizeKB";
    protected final static String JOURNAL_ALIGNMENT_SIZE = "journalAlignmentSize";
    protected final static String NUM_JOURNAL_CALLBACK_THREADS = "numJournalCallbackThreads";
    protected final static String JOURNAL_FORMAT_VERSION_TO_WRITE = "journalFormatVersionToWrite";
    // Bookie Parameters
    protected final static String BOOKIE_PORT = "bookiePort";
    protected final static String LISTENING_INTERFACE = "listeningInterface";
    protected final static String ALLOW_LOOPBACK = "allowLoopback";

    protected final static String JOURNAL_DIR = "journalDirectory";
    protected final static String LEDGER_DIRS = "ledgerDirectories";
    protected final static String INDEX_DIRS = "indexDirectories";
    // NIO Parameters
    protected final static String SERVER_TCP_NODELAY = "serverTcpNoDelay";
    // Zookeeper Parameters
    protected final static String ZK_TIMEOUT = "zkTimeout";
    protected final static String ZK_SERVERS = "zkServers";
    protected final static String ZK_RETRY_BACKOFF_START_MS = "zkRetryBackoffStartMs";
    protected final static String ZK_RETRY_BACKOFF_MAX_MS = "zkRetryBackoffMaxMs";
    protected final static String OPEN_LEDGER_REREPLICATION_GRACE_PERIOD = "openLedgerRereplicationGracePeriod";
    //ReadOnly mode support on all disk full
    protected final static String READ_ONLY_MODE_ENABLED = "readOnlyModeEnabled";
    //Whether the bookie is force started in ReadOnly mode
    protected final static String FORCE_READ_ONLY_BOOKIE = "forceReadOnlyBookie";
    //Disk utilization
    protected final static String DISK_USAGE_THRESHOLD = "diskUsageThreshold";
    protected final static String DISK_USAGE_WARN_THRESHOLD = "diskUsageWarnThreshold";
    protected final static String DISK_CHECK_INTERVAL = "diskCheckInterval";
    protected final static String AUDITOR_PERIODIC_CHECK_INTERVAL = "auditorPeriodicCheckInterval";
    protected final static String AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL = "auditorPeriodicBookieCheckInterval";
    protected final static String AUTO_RECOVERY_DAEMON_ENABLED = "autoRecoveryDaemonEnabled";
    protected final static String LOST_BOOKIE_RECOVERY_DELAY = "lostBookieRecoveryDelay";

    // Worker Thread parameters.
    protected final static String NUM_ADD_WORKER_THREADS = "numAddWorkerThreads";
    protected final static String NUM_READ_WORKER_THREADS = "numReadWorkerThreads";

    protected final static String READ_BUFFER_SIZE = "readBufferSizeBytes";
    protected final static String WRITE_BUFFER_SIZE = "writeBufferSizeBytes";
    // Whether the bookie should use its hostname or ipaddress for the
    // registration.
    protected final static String USE_HOST_NAME_AS_BOOKIE_ID = "useHostNameAsBookieID";
    protected final static String ENABLE_LOCAL_TRANSPORT = "enableLocalTransport";
    protected final static String DISABLE_SERVER_SOCKET_BIND = "disableServerSocketBind";

    protected final static String SORTED_LEDGER_STORAGE_ENABLED = "sortedLedgerStorageEnabled";
    protected final static String SKIP_LIST_SIZE_LIMIT = "skipListSizeLimit";
    protected final static String SKIP_LIST_CHUNK_SIZE_ENTRY = "skipListArenaChunkSize";
    protected final static String SKIP_LIST_MAX_ALLOC_ENTRY = "skipListArenaMaxAllocSize";

    // Statistics Parameters
    protected final static String ENABLE_STATISTICS = "enableStatistics";
    protected final static String STATS_PROVIDER_CLASS = "statsProviderClass";

    protected final static String LEDGER_STORAGE_CLASS = "ledgerStorageClass";

    // Bookie auth provider factory class name
    protected final static String BOOKIE_AUTH_PROVIDER_FACTORY_CLASS = "bookieAuthProviderFactoryClass";

    /**
     * Construct a default configuration object
     */
    public ServerConfiguration() {
        super();
    }

    /**
     * Construct a configuration based on other configuration
     *
     * @param conf
     *          Other configuration
     */
    public ServerConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }

    /**
     * Get entry logger size limitation
     *
     * @return entry logger size limitation
     */
    public long getEntryLogSizeLimit() {
        return this.getLong(ENTRY_LOG_SIZE_LIMIT, 1 * 1024 * 1024 * 1024L);
    }

    /**
     * Set entry logger size limitation
     *
     * @param logSizeLimit
     *          new log size limitation
     */
    public ServerConfiguration setEntryLogSizeLimit(long logSizeLimit) {
        this.setProperty(ENTRY_LOG_SIZE_LIMIT, Long.toString(logSizeLimit));
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
     * Set garbage collection wait time
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
     * Get wait time in millis for garbage collection of overreplicated ledgers
     * 
     * @return gc wait time
     */
    public long getGcOverreplicatedLedgerWaitTimeMillis() {
        return this.getLong(GC_OVERREPLICATED_LEDGER_WAIT_TIME, TimeUnit.DAYS.toMillis(1));
    }

    /**
     * Set wait time for garbage collection of overreplicated ledgers. Default: 1 day
     * 
     * A ledger can be overreplicated under the following circumstances:
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
     * Set flush interval
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
     * Get bookie death watch interval
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
     * Get limitation number of index pages in ledger cache
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
     * Get page size
     *
     * @return page size in ledger cache
     */
    public int getPageSize() {
        return this.getInt(PAGE_SIZE, 8192);
    }

    /**
     * Set page size
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
     * Max journal file size
     *
     * @return max journal file size
     */
    public long getMaxJournalSizeMB() {
        return this.getLong(MAX_JOURNAL_SIZE, 2 * 1024);
    }

    /**
     * Set new max journal file size
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
     * How much space should we pre-allocate at a time in the journal
     *
     * @return journal pre-allocation size in MB
     */
    public int getJournalPreAllocSizeMB() {
        return this.getInt(JOURNAL_PRE_ALLOC_SIZE, 16);
    }

    /**
     * Size of the write buffers used for the journal
     *
     * @return journal write buffer size in KB
     */
    public int getJournalWriteBufferSizeKB() {
        return this.getInt(JOURNAL_WRITE_BUFFER_SIZE, 64);
    }

    /**
     * Max number of older journal files kept
     *
     * @return max number of older journal files to kept
     */
    public int getMaxBackupJournals() {
        return this.getInt(MAX_BACKUP_JOURNALS, 5);
    }

    /**
     * Set max number of older journal files to kept
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
        return this.getInt(JOURNAL_FORMAT_VERSION_TO_WRITE, 4);
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
     * Get bookie port that bookie server listen on
     *
     * @return bookie port
     */
    public int getBookiePort() {
        return this.getInt(BOOKIE_PORT, 3181);
    }

    /**
     * Set new bookie port that bookie server listen on
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
     * By default, loopback interfaces are not allowed as the primary
     * interface.
     *
     * Using a loopback interface as the primary interface usually indicates
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
     * Get dir name to store journal files
     *
     * @return journal dir name
     */
    public String getJournalDirName() {
        return this.getString(JOURNAL_DIR, "/tmp/bk-txn");
    }

    /**
     * Set dir name to store journal files
     *
     * @param journalDir
     *          Dir to store journal files
     * @return server configuration
     */
    public ServerConfiguration setJournalDirName(String journalDir) {
        this.setProperty(JOURNAL_DIR, journalDir);
        return this;
    }

    /**
     * Get dir to store journal files
     *
     * @return journal dir, if no journal dir provided return null
     */
    public File getJournalDir() {
        String journalDirName = getJournalDirName();
        if (null == journalDirName) {
            return null;
        }
        return new File(journalDirName);
    }

    /**
     * Get dir names to store ledger data
     *
     * @return ledger dir names, if not provided return null
     */
    public String[] getLedgerDirNames() {
        String[] ledgerDirs = this.getStringArray(LEDGER_DIRS);
        if (null == ledgerDirs) {
            return new String[] { "/tmp/bk-data" };
        }
        return ledgerDirs;
    }

    /**
     * Set dir names to store ledger data
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
     * Get dirs that stores ledger data
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
        for (int i=0; i<idxDirNames.length; i++) {
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
     * Set socket nodelay setting
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
     * Get zookeeper servers to connect
     *
     * @return zookeeper servers
     */
    public String getZkServers() {
        List servers = getList(ZK_SERVERS, null);
        if (null == servers || 0 == servers.size()) {
            return null;
        }
        return StringUtils.join(servers, ",");
    }

    /**
     * Set zookeeper servers to connect
     *
     * @param zkServers
     *          ZooKeeper servers to connect
     */
    public ServerConfiguration setZkServers(String zkServers) {
        setProperty(ZK_SERVERS, zkServers);
        return this;
    }

    /**
     * Get zookeeper timeout
     *
     * @return zookeeper server timeout
     */
    public int getZkTimeout() {
        return getInt(ZK_TIMEOUT, 10000);
    }

    /**
     * Set zookeeper timeout
     *
     * @param zkTimeout
     *          ZooKeeper server timeout
     * @return server configuration
     */
    public ServerConfiguration setZkTimeout(int zkTimeout) {
        setProperty(ZK_TIMEOUT, Integer.toString(zkTimeout));
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
     * Is statistics enabled
     *
     * @return is statistics enabled
     */
    public boolean isStatisticsEnabled() {
        return getBoolean(ENABLE_STATISTICS, true);
    }

    /**
     * Turn on/off statistics
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
     * For those entry log files whose remaining size percentage reaches below
     * this threshold  will be compacted in a minor compaction.
     *
     * If it is set to less than zero, the minor compaction is disabled.
     *
     * @return threshold of minor compaction
     */
    public double getMinorCompactionThreshold() {
        return getDouble(MINOR_COMPACTION_THRESHOLD, 0.2f);
    }

    /**
     * Set threshold of minor compaction
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
     * For those entry log files whose remaining size percentage reaches below
     * this threshold  will be compacted in a major compaction.
     *
     * If it is set to less than zero, the major compaction is disabled.
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
     * If it is set to less than zero, the minor compaction is disabled.
     *
     * @return threshold of minor compaction
     */
    public long getMinorCompactionInterval() {
        return getLong(MINOR_COMPACTION_INTERVAL, 3600);
    }

    /**
     * Set interval to run minor compaction
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
     * If it is set to less than zero, the major compaction is disabled.
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
     * Force GC may get some space back, but may also fill up disk space more
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
     * The grace period allows the writer to detect the bookie failure, and and
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
     * Get the number of bytes we should use as capacity for
     * org.apache.bookkeeper.bookie.BufferedReadChannel
     * Default is 512 bytes
     * @return read buffer size
     */
    public int getReadBufferBytes() {
        return getInt(READ_BUFFER_SIZE, 512);
    }

    /**
     * Set the number of bytes we should use as capacity for
     * org.apache.bookkeeper.bookie.BufferedReadChannel
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
     * Set sorted-ledger storage enabled or not
     *
     * @deprecated Use {@link #setLedgerStorageClass(String)} to configure the implementation class
     * @param enabled
     */
    public ServerConfiguration setSortedLedgerStorageEnabled(boolean enabled) {
        this.setProperty(SORTED_LEDGER_STORAGE_ENABLED, enabled);
        return this;
    }

    /**
     * Check if sorted-ledger storage enabled (default true)
     *
     * @return true if sorted ledger storage is enabled, false otherwise
     */
    public boolean getSortedLedgerStorageEnabled() {
        return this.getBoolean(SORTED_LEDGER_STORAGE_ENABLED, true);
    }

    /**
     * Get skip list data size limitation (default 64MB)
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
        setProperty(SKIP_LIST_SIZE_LIMIT, size);
        return this;
    }

    /**
     * Get the number of bytes we should use as chunk allocation for
     * org.apache.bookkeeper.bookie.SkipListArena
     * Default is 4 MB
     * @return the number of bytes to use for each chunk in the skiplist arena
     */
    public int getSkipListArenaChunkSize() {
        return getInt(SKIP_LIST_CHUNK_SIZE_ENTRY, 4096 * 1024);
    }

    /**
     * Set the number of bytes w used as chunk allocation for
     * org.apache.bookkeeper.bookie.SkipListArena
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
     * Should we group journal force writes
     *
     * @return group journal force writes
     */
    public boolean getJournalAdaptiveGroupWrites() {
        return getBoolean(JOURNAL_ADAPTIVE_GROUP_WRITES, true);
    }

    /**
     * Enable/disable group journal force writes
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
     * Maximum bytes to buffer to impose on a journal write to achieve grouping
     *
     * @return max bytes to buffer
     */
    public long getJournalBufferedWritesThreshold() {
        return getLong(JOURNAL_BUFFERED_WRITES_THRESHOLD, 512 * 1024);
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
     * Set if we should flush the journal when queue is empty
     */
    public ServerConfiguration setJournalFlushWhenQueueEmpty(boolean enabled) {
        setProperty(JOURNAL_FLUSH_WHEN_QUEUE_EMPTY, enabled);
        return this;
    }

    /**
     * Should we flush the journal when queue is empty
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
     * Set the disk checker interval to monitor ledger disk space
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
     * Get the disk checker interval
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
     * Sets that whether the auto-recovery service can start along with Bookie
     * server itself or not
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
     * Get whether the Bookie itself can start auto-recovery service also or not
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
     * Sets that whether force start a bookie in readonly mode
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
     * Get whether the Bookie is force started in read only mode or not
     *
     * @return true - if need to start a bookie in read only mode. Otherwise
     *         false.
     */
    public boolean isForceReadOnlyBookie() {
        return getBoolean(FORCE_READ_ONLY_BOOKIE, false);
    }

    /**
     * Get whether use bytes to throttle garbage collector compaction or not
     *
     * @return true  - use Bytes,
     *         false - use Entries.
     */
    public boolean getIsThrottleByBytes() {
        return getBoolean(IS_THROTTLE_BY_BYTES, false);
    }

    /**
     * Set whether use bytes to throttle garbage collector compaction or not
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
     * When compacting, the entries are written to the entrylog and the new offsets
     * are cached in memory. Once the entrylog is flushed the index is updated with
     * the new offsets. This parameter controls the number of entries added to the
     * entrylog before a flush is forced. A higher value for this parameter means
     * more memory will be used for offsets. Each offset consists of 3 longs.
     *
     * This parameter should _not_ be modified unless you know what you're doing.
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
     * Should we remove pages from page cache after force write
     *
     * @return remove pages from cache
     */
    @Beta
    public boolean getJournalRemovePagesFromCache() {
        return getBoolean(JOURNAL_REMOVE_FROM_PAGE_CACHE, false);
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
     * Get the {@link LedgerStorage} implementation class name
     *
     * @return the class name
     */
    public String getLedgerStorageClass() {
        String ledgerStorageClass = getString(LEDGER_STORAGE_CLASS, SortedLedgerStorage.class.getName());
        if (ledgerStorageClass.equals(SortedLedgerStorage.class.getName())
                && getSortedLedgerStorageEnabled() == false) {
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
     * Set the {@link LedgerStorage} implementation class name
     *
     * @param ledgerStorageClass the class name
     * @return ServerConfiguration
     */
    public ServerConfiguration setLedgerStorageClass(String ledgerStorageClass) {
        setProperty(LEDGER_STORAGE_CLASS, ledgerStorageClass);
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
     * co-ordination service(eg: zookeeper) and in ledger metadata
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
     * Get whether to listen for local JVM clients. Defaults to false.
     *
     * @return true, then bookie will be listen for local JVM clients
     */
    public boolean isEnableLocalTransport() {
        return getBoolean(ENABLE_LOCAL_TRANSPORT, false);
    }

    /**
     * Configure the bookie to listen for BookKeeper clients executed on the local JVM
     *
     * @see #getEnableLocalTransport
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
     * this bookie will be available only to BookKeeper clients executed on the local JVM
     *
     * @see #getDisableServerSocketBind
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
                                        defaultLoader);
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
        if (getSkipListArenaChunkSize() < getSkipListArenaMaxAllocSize()) {
            throw new ConfigurationException("Arena max allocation size should be smaller than the chunk size.");
        }
        if (getJournalAlignmentSize() < 512 || getJournalAlignmentSize() % 512 != 0) {
            throw new ConfigurationException("Invalid journal alignment size : " + getJournalAlignmentSize());
        }
        if (getJournalAlignmentSize() > getJournalPreAllocSizeMB() * 1024 * 1024) {
            throw new ConfigurationException("Invalid preallocation size : " + getJournalPreAllocSizeMB() + " MB");
        }
        if (getEntryLogSizeLimit() > BookKeeperConstants.MAX_LOG_SIZE_LIMIT) {
            throw new ConfigurationException("Entry log file size should not be larger than "
                    + BookKeeperConstants.MAX_LOG_SIZE_LIMIT);
        }
    }

    /*
     * Set the bookie authentication provider factory class name.
     * If this is not set, no authentication will be used
     *
     * @param factoryClass
     *          the bookie authentication provider factory class name
     * @return void
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
}
