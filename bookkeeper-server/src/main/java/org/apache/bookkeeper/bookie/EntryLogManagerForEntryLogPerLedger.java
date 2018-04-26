/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.lang.mutable.MutableInt;

@Slf4j
class EntryLogManagerForEntryLogPerLedger extends EntryLogManagerBase {

    static class EntryLogAndLockTuple {
        private final Lock ledgerLock;
        private BufferedLogChannel entryLog;

        public EntryLogAndLockTuple() {
            ledgerLock = new ReentrantLock();
        }

        public Lock getLedgerLock() {
            return ledgerLock;
        }

        public BufferedLogChannel getEntryLog() {
            return entryLog;
        }

        public void setEntryLog(BufferedLogChannel entryLog) {
            this.entryLog = entryLog;
        }
    }

    private LoadingCache<Long, EntryLogAndLockTuple> ledgerIdEntryLogMap;
    /*
     * every time active logChannel is accessed from ledgerIdEntryLogMap
     * cache, the accesstime of that entry is updated. But for certain
     * operations we dont want to impact accessTime of the entries (like
     * periodic flush of current active logChannels), and those operations
     * can use this copy of references.
     */
    private final ConcurrentHashMap<Long, BufferedLogChannel> replicaOfCurrentLogChannels;
    private final CacheLoader<Long, EntryLogAndLockTuple> entryLogAndLockTupleCacheLoader;
    private EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;
    private final int entrylogMapAccessExpiryTimeInSeconds;
    private final int maximumNumberOfActiveEntryLogs;

    EntryLogManagerForEntryLogPerLedger(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
            EntryLoggerAllocator entryLoggerAllocator, List<EntryLogger.EntryLogListener> listeners,
            EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus) throws IOException {
        super(conf, ledgerDirsManager, entryLoggerAllocator, listeners);
        this.recentlyCreatedEntryLogsStatus = recentlyCreatedEntryLogsStatus;
        this.rotatedLogChannels = new CopyOnWriteArrayList<BufferedLogChannel>();
        this.replicaOfCurrentLogChannels = new ConcurrentHashMap<Long, BufferedLogChannel>();
        this.entrylogMapAccessExpiryTimeInSeconds = conf.getEntrylogMapAccessExpiryTimeInSeconds();
        this.maximumNumberOfActiveEntryLogs = conf.getMaximumNumberOfActiveEntryLogs();
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        this.entryLogAndLockTupleCacheLoader = new CacheLoader<Long, EntryLogAndLockTuple>() {
            @Override
            public EntryLogAndLockTuple load(Long key) throws Exception {
                return new EntryLogAndLockTuple();
            }
        };
        /*
         * Currently we are relying on access time based eviction policy for
         * removal of EntryLogAndLockTuple, so if the EntryLogAndLockTuple of
         * the ledger is not accessed in
         * entrylogMapAccessExpiryTimeInSeconds period, it will be removed
         * from the cache.
         *
         * We are going to introduce explicit advisory writeClose call, with
         * that explicit call EntryLogAndLockTuple of the ledger will be
         * removed from the cache. But still timebased eviciton policy is
         * needed because it is not guaranteed that Bookie/EntryLogger would
         * receive successfully write close call in all the cases.
         */
        ledgerIdEntryLogMap = CacheBuilder.newBuilder()
                .expireAfterAccess(entrylogMapAccessExpiryTimeInSeconds, TimeUnit.SECONDS)
                .maximumSize(maximumNumberOfActiveEntryLogs)
                .removalListener(new RemovalListener<Long, EntryLogAndLockTuple>() {
                    @Override
                    public void onRemoval(
                            RemovalNotification<Long, EntryLogAndLockTuple> expiredLedgerEntryLogMapEntry) {
                        removalOnExpiry(expiredLedgerEntryLogMapEntry);
                    }
                }).build(entryLogAndLockTupleCacheLoader);
    }

    /*
     * This method is called when access time of that ledger has elapsed
     * entrylogMapAccessExpiryTimeInSeconds period and the entry for that
     * ledger is removed from cache. Since the entrylog of this ledger is
     * not active anymore it has to be removed from
     * replicaOfCurrentLogChannels and added to rotatedLogChannels.
     *
     * Because of performance/optimizations concerns the cleanup maintenance
     * operations wont happen automatically, for more info on eviction
     * cleanup maintenance tasks -
     * https://google.github.io/guava/releases/19.0/api/docs/com/google/
     * common/cache/CacheBuilder.html
     *
     */
    private void removalOnExpiry(RemovalNotification<Long, EntryLogAndLockTuple> expiredLedgerEntryLogMapEntry) {
        Long ledgerId = expiredLedgerEntryLogMapEntry.getKey();
        log.debug("LedgerId {} is not accessed for entrylogMapAccessExpiryTimeInSeconds"
                + " period so it is being evicted from the cache map", ledgerId);
        EntryLogAndLockTuple entryLogAndLockTuple = expiredLedgerEntryLogMapEntry.getValue();
        Lock lock = entryLogAndLockTuple.ledgerLock;
        BufferedLogChannel logChannel = entryLogAndLockTuple.entryLog;
        lock.lock();
        try {
            replicaOfCurrentLogChannels.remove(logChannel.getLogId());
            rotatedLogChannels.add(logChannel);
        } finally {
            lock.unlock();
        }
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                Set<BufferedLogChannel> copyOfCurrentLogs = getCopyOfCurrentLogs();
                for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
                    if (disk.equals(currentLog.getLogFile().getParentFile())) {
                        currentLog.setLedgerDirFull(true);
                    }
                }
            }

            @Override
            public void diskWritable(File disk) {
                Set<BufferedLogChannel> copyOfCurrentLogs = getCopyOfCurrentLogs();
                for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
                    if (disk.equals(currentLog.getLogFile().getParentFile())) {
                        currentLog.setLedgerDirFull(false);
                    }
                }
            }
        };
    }

    public void acquireLock(Long ledgerId) throws IOException {
        try {
            ledgerIdEntryLogMap.get(ledgerId).getLedgerLock().lock();
        } catch (Exception e) {
            log.error("Received unexpected exception while fetching lock to acquire", e);
            throw new IOException("Received unexpected exception while fetching lock to acquire", e);
        }
    }

    public void releaseLock(Long ledgerId) throws IOException {
        try {
            ledgerIdEntryLogMap.get(ledgerId).getLedgerLock().unlock();
        } catch (Exception e) {
            log.error("Received unexpected exception while fetching lock to release", e);
            throw new IOException("Received unexpected exception while fetching lock to release", e);
        }
    }

    /*
     * sets the logChannel for the given ledgerId. It will add the new
     * logchannel to replicaOfCurrentLogChannels, and the previous one will
     * be removed from replicaOfCurrentLogChannels. Previous logChannel will
     * be added to rotatedLogChannels in both the cases.
     */
    @Override
    public void setCurrentLogForLedgerAndAddToRotate(long ledgerId, BufferedLogChannel logChannel) throws IOException {
        acquireLock(ledgerId);
        try {
            BufferedLogChannel hasToRotateLogChannel = getCurrentLogForLedger(ledgerId);
            logChannel.setLedgerIdAssigned(ledgerId);
            ledgerIdEntryLogMap.get(ledgerId).setEntryLog(logChannel);
            replicaOfCurrentLogChannels.put(logChannel.getLogId(), logChannel);
            if (hasToRotateLogChannel != null) {
                replicaOfCurrentLogChannels.remove(hasToRotateLogChannel.getLogId());
                rotatedLogChannels.add(hasToRotateLogChannel);
            }
        } catch (Exception e) {
            log.error("Received unexpected exception while fetching entry from map", e);
            throw new IOException("Received unexpected exception while fetching entry from map", e);
        } finally {
            releaseLock(ledgerId);
        }
    }

    @Override
    public BufferedLogChannel getCurrentLogForLedger(long ledgerId) throws IOException {
        acquireLock(ledgerId);
        try {
            EntryLogAndLockTuple entryLogAndLockTuple = ledgerIdEntryLogMap.get(ledgerId);
            return entryLogAndLockTuple.getEntryLog();
        } catch (Exception e) {
            log.error("Received unexpected exception while fetching entry from map", e);
            throw new IOException("Received unexpected exception while fetching entry from map", e);
        } finally {
            releaseLock(ledgerId);
        }
    }

    public Set<BufferedLogChannel> getCopyOfCurrentLogs() {
        return new HashSet<BufferedLogChannel>(replicaOfCurrentLogChannels.values());
    }

    @Override
    public BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
        return replicaOfCurrentLogChannels.get(entryLogId);
    }

    @Override
    public void checkpoint() throws IOException {
        /*
         * In the case of entryLogPerLedgerEnabled we need to flush
         * both rotatedlogs and currentlogs. This is needed because
         * syncThread periodically does checkpoint and at this time
         * all the logs should be flushed.
         *
         */
        super.flush();
    }

    @Override
    public void prepareSortedLedgerStorageCheckpoint(long numBytesFlushed) throws IOException {
        // do nothing
        /*
         * prepareSortedLedgerStorageCheckpoint is required for
         * singleentrylog scenario, but it is not needed for
         * entrylogperledger scenario, since entries of a ledger go
         * to a entrylog (even during compaction) and SyncThread
         * drives periodic checkpoint logic.
         */

    }

    @Override
    public void prepareEntryMemTableFlush() {
        // do nothing
    }

    @Override
    public boolean commitEntryMemTableFlush() throws IOException {
        // lock it only if there is new data
        // so that cache accesstime is not changed
        Set<BufferedLogChannel> copyOfCurrentLogs = getCopyOfCurrentLogs();
        for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
            if (reachEntryLogLimit(currentLog, 0L)) {
                Long ledgerId = currentLog.getLedgerIdAssigned();
                acquireLock(ledgerId);
                try {
                    if (reachEntryLogLimit(currentLog, 0L)) {
                        log.info("Rolling entry logger since it reached size limitation for ledger: {}", ledgerId);
                        createNewLog(ledgerId);
                    }
                } finally {
                    releaseLock(ledgerId);
                }
            }
        }
        /*
         * in the case of entrylogperledger, SyncThread drives
         * checkpoint logic for every flushInterval. So
         * EntryMemtable doesn't need to call checkpoint in the case
         * of entrylogperledger.
         */
        return false;
    }

    /*
     * this is for testing purpose only. guava's cache doesnt cleanup
     * completely (including calling expiry removal listener) automatically
     * when access timeout elapses.
     *
     * https://google.github.io/guava/releases/19.0/api/docs/com/google/
     * common/cache/CacheBuilder.html
     *
     * If expireAfterWrite or expireAfterAccess is requested entries may be
     * evicted on each cache modification, on occasional cache accesses, or
     * on calls to Cache.cleanUp(). Expired entries may be counted by
     * Cache.size(), but will never be visible to read or write operations.
     *
     * Certain cache configurations will result in the accrual of periodic
     * maintenance tasks which will be performed during write operations, or
     * during occasional read operations in the absence of writes. The
     * Cache.cleanUp() method of the returned cache will also perform
     * maintenance, but calling it should not be necessary with a high
     * throughput cache. Only caches built with removalListener,
     * expireAfterWrite, expireAfterAccess, weakKeys, weakValues, or
     * softValues perform periodic maintenance.
     */
    @VisibleForTesting
    void doEntryLogMapCleanup() {
        ledgerIdEntryLogMap.cleanUp();
    }

    @VisibleForTesting
    ConcurrentMap<Long, EntryLogAndLockTuple> getCacheAsMap() {
        return ledgerIdEntryLogMap.asMap();
    }
    /*
     * Returns writable ledger dir with least number of current active
     * entrylogs.
     */
    @Override
    public File getDirForNextEntryLog(List<File> writableLedgerDirs) {
        Map<File, MutableInt> writableLedgerDirFrequency = new HashMap<File, MutableInt>();
        writableLedgerDirs.stream()
                .forEach((ledgerDir) -> writableLedgerDirFrequency.put(ledgerDir, new MutableInt()));
        for (BufferedLogChannel logChannel : replicaOfCurrentLogChannels.values()) {
            File parentDirOfCurrentLogChannel = logChannel.getLogFile().getParentFile();
            if (writableLedgerDirFrequency.containsKey(parentDirOfCurrentLogChannel)) {
                writableLedgerDirFrequency.get(parentDirOfCurrentLogChannel).increment();
            }
        }
        @SuppressWarnings("unchecked")
        Optional<Entry<File, MutableInt>> ledgerDirWithLeastNumofCurrentLogs = writableLedgerDirFrequency.entrySet()
                .stream().min(Map.Entry.comparingByValue());
        return ledgerDirWithLeastNumofCurrentLogs.get().getKey();
    }

    @Override
    public void close() throws IOException {
        Set<BufferedLogChannel> copyOfCurrentLogs = getCopyOfCurrentLogs();
        for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
            EntryLogger.closeFileChannel(currentLog);
        }
    }

    @Override
    public void forceClose() {
        Set<BufferedLogChannel> copyOfCurrentLogs = getCopyOfCurrentLogs();
        for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
            EntryLogger.forceCloseFileChannel(currentLog);
        }
    }

    @Override
    void flushCurrentLogs() throws IOException {
        Set<BufferedLogChannel> copyOfCurrentLogs = getCopyOfCurrentLogs();
        for (BufferedLogChannel logChannel : copyOfCurrentLogs) {
            /**
             * flushCurrentLogs method is called during checkpoint, so metadata
             * of the file also should be force written.
             */
            flushLogChannel(logChannel, true);
        }
    }

    @Override
    public BufferedLogChannel createNewLogForCompaction() throws IOException {
        throw new UnsupportedOperationException(
                "When entryLogPerLedger is enabled, transactional compaction should have been disabled");
    }

    @Override
    public long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
        acquireLock(ledger);
        try {
            return super.addEntry(ledger, entry, rollLog);
        } finally {
            releaseLock(ledger);
        }
    }

    @Override
    void createNewLog(long ledgerId) throws IOException {
        acquireLock(ledgerId);
        try {
            super.createNewLog(ledgerId);
        } finally {
            releaseLock(ledgerId);
        }
    }


    @Override
    BufferedLogChannel getCurrentLogForLedgerForAddEntry(long ledgerId, int entrySize, boolean rollLog)
            throws IOException {
        acquireLock(ledgerId);
        try {
            BufferedLogChannel logChannel = getCurrentLogForLedger(ledgerId);
            boolean reachEntryLogLimit = rollLog ? reachEntryLogLimit(logChannel, entrySize)
                    : readEntryLogHardLimit(logChannel, entrySize);
            // Create new log if logSizeLimit reached or current disk is full
            boolean diskFull = (logChannel == null) ? false : logChannel.isLedgerDirFull();
            boolean allDisksFull = !ledgerDirsManager.hasWritableLedgerDirs();

            /**
             * if disk of the logChannel is full or if the entrylog limit is
             * reached of if the logchannel is not initialized, then
             * createNewLog. If allDisks are full then proceed with the current
             * logChannel, since Bookie must have turned to readonly mode and
             * the addEntry traffic would be from GC and it is ok to proceed in
             * this case.
             */
            if ((diskFull && (!allDisksFull)) || reachEntryLogLimit || (logChannel == null)) {
                if (logChannel != null) {
                    logChannel.flushAndForceWriteIfRegularFlush(false);
                }
                createNewLog(ledgerId);
            }

            return getCurrentLogForLedger(ledgerId);
        } finally {
            releaseLock(ledgerId);
        }
    }

    @Override
    public void flushRotatedLogs() throws IOException {
        for (BufferedLogChannel channel : rotatedLogChannels) {
            channel.flushAndForceWrite(true);
            // since this channel is only used for writing, after flushing the channel,
            // we had to close the underlying file channel. Otherwise, we might end up
            // leaking fds which cause the disk spaces could not be reclaimed.
            EntryLogger.closeFileChannel(channel);
            recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(channel.getLogId());
            rotatedLogChannels.remove(channel);
            log.info("Synced entry logger {} to disk.", channel.getLogId());
        }
    }
}
