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
package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
//CHECKSTYLE.OFF: IllegalImport
import io.netty.util.internal.PlatformDependent;
//CHECKSTYLE.ON: IllegalImport

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.bookie.storage.ldb.SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.lang3.StringUtils;


/**
 * Implementation of LedgerStorage that uses RocksDB to keep the indexes for entries stored in EntryLogs.
 */
@Slf4j
public class DbLedgerStorage implements LedgerStorage {

    public static final String WRITE_CACHE_MAX_SIZE_MB = "dbStorage_writeCacheMaxSizeMb";

    public static final String READ_AHEAD_CACHE_MAX_SIZE_MB = "dbStorage_readAheadCacheMaxSizeMb";

    static final String MAX_THROTTLE_TIME_MILLIS = "dbStorage_maxThrottleTimeMs";

    private static final int MB = 1024 * 1024;

    private static final long DEFAULT_WRITE_CACHE_MAX_SIZE_MB = (long) (0.25 * PlatformDependent.maxDirectMemory())
            / MB;
    private static final long DEFAULT_READ_CACHE_MAX_SIZE_MB = (long) (0.25 * PlatformDependent.maxDirectMemory())
            / MB;
    private int numberOfDirs;
    private List<SingleDirectoryDbLedgerStorage> ledgerStorageList;

    // Keep 1 single Bookie GC thread so the the compactions from multiple individual directories are serialized
    private ScheduledExecutorService gcExecutor;
    private DbLedgerStorageStats stats;

    protected ByteBufAllocator allocator;

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager,
            LedgerDirsManager indexDirsManager, StateManager stateManager, CheckpointSource checkpointSource,
            Checkpointer checkpointer, StatsLogger statsLogger, ByteBufAllocator allocator) throws IOException {
        long writeCacheMaxSize = getLongVariableOrDefault(conf, WRITE_CACHE_MAX_SIZE_MB,
                DEFAULT_WRITE_CACHE_MAX_SIZE_MB) * MB;
        long readCacheMaxSize = getLongVariableOrDefault(conf, READ_AHEAD_CACHE_MAX_SIZE_MB,
                DEFAULT_READ_CACHE_MAX_SIZE_MB) * MB;

        this.allocator = allocator;
        this.numberOfDirs = ledgerDirsManager.getAllLedgerDirs().size();

        log.info("Started Db Ledger Storage");
        log.info(" - Number of directories: {}", numberOfDirs);
        log.info(" - Write cache size: {} MB", writeCacheMaxSize / MB);
        log.info(" - Read Cache: {} MB", readCacheMaxSize / MB);

        if (readCacheMaxSize + writeCacheMaxSize > PlatformDependent.maxDirectMemory()) {
            throw new IOException("Read and write cache sizes exceed the configured max direct memory size");
        }

        long perDirectoryWriteCacheSize = writeCacheMaxSize / numberOfDirs;
        long perDirectoryReadCacheSize = readCacheMaxSize / numberOfDirs;

        gcExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("GarbageCollector"));

        ledgerStorageList = Lists.newArrayList();
        for (File ledgerDir : ledgerDirsManager.getAllLedgerDirs()) {
            // Create a ledger dirs manager for the single directory
            File[] dirs = new File[1];
            // Remove the `/current` suffix which will be appended again by LedgersDirManager
            dirs[0] = ledgerDir.getParentFile();
            LedgerDirsManager ldm = new LedgerDirsManager(conf, dirs, ledgerDirsManager.getDiskChecker(), statsLogger);
            ledgerStorageList.add(newSingleDirectoryDbLedgerStorage(conf, ledgerManager, ldm, indexDirsManager,
                    stateManager, checkpointSource, checkpointer, statsLogger, gcExecutor, perDirectoryWriteCacheSize,
                    perDirectoryReadCacheSize));
        }

        this.stats = new DbLedgerStorageStats(
            statsLogger,
            () -> ledgerStorageList.stream().mapToLong(SingleDirectoryDbLedgerStorage::getWriteCacheSize).sum(),
            () -> ledgerStorageList.stream().mapToLong(SingleDirectoryDbLedgerStorage::getWriteCacheCount).sum(),
            () -> ledgerStorageList.stream().mapToLong(SingleDirectoryDbLedgerStorage::getReadCacheSize).sum(),
            () -> ledgerStorageList.stream().mapToLong(SingleDirectoryDbLedgerStorage::getReadCacheCount).sum()
        );
    }

    @VisibleForTesting
    protected SingleDirectoryDbLedgerStorage newSingleDirectoryDbLedgerStorage(ServerConfiguration conf,
            LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
            StateManager stateManager, CheckpointSource checkpointSource, Checkpointer checkpointer,
            StatsLogger statsLogger, ScheduledExecutorService gcExecutor, long writeCacheSize, long readCacheSize)
            throws IOException {
        return new SingleDirectoryDbLedgerStorage(conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                stateManager, checkpointSource, checkpointer, statsLogger, allocator, gcExecutor, writeCacheSize,
                readCacheSize);
    }

    @Override
    public void start() {
        ledgerStorageList.forEach(LedgerStorage::start);
    }

    @Override
    public void shutdown() throws InterruptedException {
        for (LedgerStorage ls : ledgerStorageList) {
            ls.shutdown();
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return getLedgerSorage(ledgerId).ledgerExists(ledgerId);
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return getLedgerSorage(ledgerId).setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return getLedgerSorage(ledgerId).isFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        getLedgerSorage(ledgerId).setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return getLedgerSorage(ledgerId).readMasterKey(ledgerId);
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException, BookieException {
        long ledgerId = entry.getLong(entry.readerIndex());
        return getLedgerSorage(ledgerId).addEntry(entry);
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException {
        return getLedgerSorage(ledgerId).getEntry(ledgerId, entryId);
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException {
        return getLedgerSorage(ledgerId).getLastAddConfirmed(ledgerId);
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId, long previousLAC,
            Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        return getLedgerSorage(ledgerId).waitForLastAddConfirmedUpdate(ledgerId, previousLAC, watcher);
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        getLedgerSorage(ledgerId).cancelWaitForLastAddConfirmedUpdate(ledgerId, watcher);
    }

    @Override
    public void flush() throws IOException {
        for (LedgerStorage ls : ledgerStorageList) {
            ls.flush();
        }
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws IOException {
        for (LedgerStorage ls : ledgerStorageList) {
            ls.checkpoint(checkpoint);
        }
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        getLedgerSorage(ledgerId).deleteLedger(ledgerId);
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        ledgerStorageList.forEach(ls -> ls.registerLedgerDeletionListener(listener));
    }

    @Override
    public void setExplicitlac(long ledgerId, ByteBuf lac) throws IOException {
        getLedgerSorage(ledgerId).setExplicitlac(ledgerId, lac);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) {
        return getLedgerSorage(ledgerId).getExplicitLac(ledgerId);
    }

    public long addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
                                 LedgerCache.PageEntriesIterable pages) throws Exception {
        return getLedgerSorage(ledgerId).addLedgerToIndex(ledgerId, isFenced, masterKey, pages);
    }

    public long getLastEntryInLedger(long ledgerId) throws IOException {
        return getLedgerSorage(ledgerId).getEntryLocationIndex().getLastEntryInLedger(ledgerId);
    }

    public long getLocation(long ledgerId, long entryId) throws IOException {
        return getLedgerSorage(ledgerId).getEntryLocationIndex().getLocation(ledgerId, entryId);
    }

    private SingleDirectoryDbLedgerStorage getLedgerSorage(long ledgerId) {
        return ledgerStorageList.get(MathUtils.signSafeMod(ledgerId, numberOfDirs));
    }

    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        List<Iterable<Long>> listIt = new ArrayList<>(numberOfDirs);
        for (SingleDirectoryDbLedgerStorage ls : ledgerStorageList) {
            listIt.add(ls.getActiveLedgersInRange(firstLedgerId, lastLedgerId));
        }

        return Iterables.concat(listIt);
    }

    public ByteBuf getLastEntry(long ledgerId) throws IOException {
        return getLedgerSorage(ledgerId).getLastEntry(ledgerId);
    }

    @VisibleForTesting
    boolean isFlushRequired() {
        return ledgerStorageList.stream().allMatch(SingleDirectoryDbLedgerStorage::isFlushRequired);
    }

    @VisibleForTesting
    List<SingleDirectoryDbLedgerStorage> getLedgerStorageList() {
        return ledgerStorageList;
    }

    /**
     * Reads ledger index entries to get list of entry-logger that contains given ledgerId.
     *
     * @param ledgerId
     * @param serverConf
     * @param processor
     * @throws IOException
     */
    public static void readLedgerIndexEntries(long ledgerId, ServerConfiguration serverConf,
            LedgerLoggerProcessor processor) throws IOException {

        checkNotNull(serverConf, "ServerConfiguration can't be null");
        checkNotNull(processor, "LedgerLoggger info processor can't null");

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(serverConf, serverConf.getLedgerDirs(),
                new DiskChecker(serverConf.getDiskUsageThreshold(), serverConf.getDiskUsageWarnThreshold()));
        List<File> ledgerDirs = ledgerDirsManager.getAllLedgerDirs();

        int dirIndex = MathUtils.signSafeMod(ledgerId, ledgerDirs.size());
        String ledgerBasePath = ledgerDirs.get(dirIndex).toString();

        EntryLocationIndex entryLocationIndex = new EntryLocationIndex(serverConf,
                (path, dbConfigType, conf1) -> new KeyValueStorageRocksDB(path, DbConfigType.Small, conf1, true),
                ledgerBasePath, NullStatsLogger.INSTANCE);
        try {
            long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
            for (long currentEntry = 0; currentEntry <= lastEntryId; currentEntry++) {
                long offset = entryLocationIndex.getLocation(ledgerId, currentEntry);
                if (offset <= 0) {
                    // entry not found in this bookie
                    continue;
                }
                long entryLogId = offset >> 32L;
                long position = offset & 0xffffffffL;
                processor.process(currentEntry, entryLogId, position);
            }
        } finally {
            entryLocationIndex.close();
        }
    }

    @Override
    public void forceGC() {
        ledgerStorageList.stream().forEach(SingleDirectoryDbLedgerStorage::forceGC);
    }

    @Override
    public boolean isInForceGC() {
        return ledgerStorageList.stream().anyMatch(SingleDirectoryDbLedgerStorage::isInForceGC);
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        return ledgerStorageList.stream()
            .map(single -> single.getGarbageCollectionStatus().get(0)).collect(Collectors.toList());
    }

    static long getLongVariableOrDefault(ServerConfiguration conf, String keyName, long defaultValue) {
        Object obj = conf.getProperty(keyName);
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        } else if (obj == null) {
            return defaultValue;
        } else if (StringUtils.isEmpty(conf.getString(keyName))) {
            return defaultValue;
        } else {
            return conf.getLong(keyName);
        }
    }

    @Override
    public OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        // check Issue #2078
        throw new UnsupportedOperationException(
                "getListOfEntriesOfLedger method is currently unsupported for DbLedgerStorage");
    }
}
