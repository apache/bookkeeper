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

import org.apache.commons.lang.StringUtils;

/**
 * Configuration manages server-side settings
 */
public class ServerConfiguration extends AbstractConfiguration {
    // Entry Log Parameters
    protected final static String ENTRY_LOG_SIZE_LIMIT = "logSizeLimit";

    // Gc Parameters
    protected final static String GC_WAIT_TIME = "gcWaitTime";
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
    // Bookie Parameters
    protected final static String BOOKIE_PORT = "bookiePort";
    protected final static String JOURNAL_DIR = "journalDirectory";
    protected final static String LEDGER_DIRS = "ledgerDirectories";
    // NIO Parameters
    protected final static String SERVER_TCP_NODELAY = "serverTcpNoDelay";
    // Zookeeper Parameters
    protected final static String ZK_TIMEOUT = "zkTimeout";
    protected final static String ZK_SERVERS = "zkServers";

    // separator for ledger dir
    protected final static String SEP = ",";

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
        return this.getLong(ENTRY_LOG_SIZE_LIMIT, 2 * 1024 * 1024 * 1024L);
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
     * Get Garbage collection wait time
     *
     * @return gc wait time
     */
    public long getGcWaitTime() {
        return this.getLong(GC_WAIT_TIME, 1000);
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
     * Get flush interval
     *
     * @return flush interval
     */
    public int getFlushInterval() {
        return this.getInt(FLUSH_INTERVAL, 100);
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
     * Get open file limit
     *
     * @return max number of files to open
     */
    public int getOpenFileLimit() {
        return this.getInt(OPEN_FILE_LIMIT, 900);
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
     * Get page size
     *
     * @return page size in ledger cache
     */
    public int getPageSize() {
        return this.getInt(PAGE_SIZE, 8192);
    }

    /**
     * Max journal file size
     *
     * @return max journal file size
     */
    public long getMaxJournalSize() {
        return this.getLong(MAX_JOURNAL_SIZE, 2 * 1024);
    }

    /**
     * Set new max journal file size
     *
     * @param maxJournalSize
     *          new max journal file size
     * @return server configuration
     */
    public ServerConfiguration setMaxJournalSize(long maxJournalSize) {
        this.setProperty(MAX_JOURNAL_SIZE, Long.toString(maxJournalSize));
        return this;
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
        String ledgerDirs = this.getString(LEDGER_DIRS, "/tmp/bk-data");
        if (null == ledgerDirs) {
            return null;
        }
        return ledgerDirs.split(SEP);
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
        this.setProperty(LEDGER_DIRS, StringUtils.join(ledgerDirs, SEP));
        return this;
    }

    /**
     * Get dirs that stores ledger data
     *
     * @return ledger dirs
     */
    public File[] getLedgerDirs() {
        String[] ledgerDirNames = getLedgerDirNames();
        if (null == ledgerDirNames) {
            return null;
        }
        File[] ledgerDirs = new File[ledgerDirNames.length];
        for (int i = 0; i < ledgerDirNames.length; i++) {
            ledgerDirs[i] = new File(ledgerDirNames[i]);
        }
        return ledgerDirs;
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
        return getString(ZK_SERVERS, null);
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
        setProperty(ZK_SERVERS, Integer.toString(zkTimeout));
        return this;
    }

}
