/*
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

// CHECKSTYLE.OFF: IllegalImport
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.DefaultEntryLogger;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.directentrylogger.DirectEntryLogger;
import org.apache.bookkeeper.bookie.storage.directentrylogger.EntryLogIdsImpl;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.bookie.storage.ldb.SingleDirectoryDbLedgerStorage.LedgerLoggerProcessor;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.slogger.slf4j.Slf4jSlogger;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.lang3.StringUtils;
// CHECKSTYLE.ON: IllegalImport


/**
 * Implementation of LedgerStorage that uses RocksDB to keep the indexes for entries stored in EntryLogs.
 */
@Slf4j
public class DbLedgerStorage implements LedgerStorage {

    public static final String WRITE_CACHE_MAX_SIZE_MB = "dbStorage_writeCacheMaxSizeMb";
    public static final String READ_AHEAD_CACHE_MAX_SIZE_MB = "dbStorage_readAheadCacheMaxSizeMb";
    public static final String DIRECT_IO_ENTRYLOGGER = "dbStorage_directIOEntryLogger";
    public static final String DIRECT_IO_ENTRYLOGGER_TOTAL_WRITEBUFFER_SIZE_MB =
        "dbStorage_directIOEntryLoggerTotalWriteBufferSizeMB";
    public static final String DIRECT_IO_ENTRYLOGGER_TOTAL_READBUFFER_SIZE_MB =
        "dbStorage_directIOEntryLoggerTotalReadBufferSizeMB";
    public static final String DIRECT_IO_ENTRYLOGGER_READBUFFER_SIZE_MB =
        "dbStorage_directIOEntryLoggerReadBufferSizeMB";
    public static final String DIRECT_IO_ENTRYLOGGER_MAX_FD_CACHE_TIME_SECONDS =
        "dbStorage_directIOEntryLoggerMaxFdCacheTimeSeconds";

    static final String MAX_THROTTLE_TIME_MILLIS = "dbStorage_maxThrottleTimeMs";

    private static final int MB = 1024 * 1024;

    private static final long DEFAULT_WRITE_CACHE_MAX_SIZE_MB =
        (long) (0.25 * PlatformDependent.estimateMaxDirectMemory()) / MB;
    private static final long DEFAULT_READ_CACHE_MAX_SIZE_MB =
        (long) (0.25 * PlatformDependent.estimateMaxDirectMemory()) / MB;

    static final String READ_AHEAD_CACHE_BATCH_SIZE = "dbStorage_readAheadCacheBatchSize";
    static final String READ_AHEAD_CACHE_BATCH_BYTES_SIZE = "dbStorage_readAheadCacheBatchBytesSize";
    private static final int DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE = 100;
    // the default value is -1. this feature(limit of read ahead bytes) is disabled
    private static final int DEFAULT_READ_AHEAD_CACHE_BATCH_BYTES_SIZE = -1;

    private static final long DEFAULT_DIRECT_IO_TOTAL_WRITEBUFFER_SIZE_MB =
        (long) (0.125 * PlatformDependent.estimateMaxDirectMemory())
            / MB;
    private static final long DEFAULT_DIRECT_IO_TOTAL_READBUFFER_SIZE_MB =
        (long) (0.125 * PlatformDependent.estimateMaxDirectMemory())
            / MB;
    private static final long DEFAULT_DIRECT_IO_READBUFFER_SIZE_MB = 8;

    private static final int DEFAULT_DIRECT_IO_MAX_FD_CACHE_TIME_SECONDS = 300;

    // use the storage assigned to ledger 0 for flags.
    // if the storage configuration changes, the flags may be lost
    // but in that case data integrity should kick off anyhow.
    private static final long STORAGE_FLAGS_KEY = 0L;
    private int numberOfDirs;
    private List<SingleDirectoryDbLedgerStorage> ledgerStorageList;

    private ExecutorService entryLoggerWriteExecutor = null;
    private ExecutorService entryLoggerFlushExecutor = null;

    protected ByteBufAllocator allocator;

    // parent DbLedgerStorage stats (not per directory)
    private static final String MAX_READAHEAD_BATCH_SIZE = "readahead-max-batch-size";
    private static final String MAX_WRITE_CACHE_SIZE = "write-cache-max-size";

    @StatsDoc(
            name = MAX_READAHEAD_BATCH_SIZE,
            help = "the configured readahead batch size"
    )
    private Gauge<Integer> readaheadBatchSizeGauge;

    @StatsDoc(
            name = MAX_WRITE_CACHE_SIZE,
            help = "the configured write cache size"
    )
    private Gauge<Long> writeCacheSizeGauge;

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager,
                           LedgerDirsManager indexDirsManager, StatsLogger statsLogger, ByteBufAllocator allocator)
            throws IOException {
        long writeCacheMaxSize = getLongVariableOrDefault(conf, WRITE_CACHE_MAX_SIZE_MB,
                DEFAULT_WRITE_CACHE_MAX_SIZE_MB) * MB;
        long readCacheMaxSize = getLongVariableOrDefault(conf, READ_AHEAD_CACHE_MAX_SIZE_MB,
                DEFAULT_READ_CACHE_MAX_SIZE_MB) * MB;
        boolean directIOEntryLogger = getBooleanVariableOrDefault(conf, DIRECT_IO_ENTRYLOGGER, false);

        this.allocator = allocator;
        this.numberOfDirs = ledgerDirsManager.getAllLedgerDirs().size();

        log.info("Started Db Ledger Storage");
        log.info(" - Number of directories: {}", numberOfDirs);
        log.info(" - Write cache size: {} MB", writeCacheMaxSize / MB);
        log.info(" - Read Cache: {} MB", readCacheMaxSize / MB);

        if (readCacheMaxSize + writeCacheMaxSize > PlatformDependent.estimateMaxDirectMemory()) {
            throw new IOException("Read and write cache sizes exceed the configured max direct memory size");
        }

        if (ledgerDirsManager.getAllLedgerDirs().size() != indexDirsManager.getAllLedgerDirs().size()) {
            throw new IOException("ledger and index dirs size not matched");
        }

        long perDirectoryWriteCacheSize = writeCacheMaxSize / numberOfDirs;
        long perDirectoryReadCacheSize = readCacheMaxSize / numberOfDirs;
        int readAheadCacheBatchSize = conf.getInt(READ_AHEAD_CACHE_BATCH_SIZE, DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE);
        long readAheadCacheBatchBytesSize = conf.getInt(READ_AHEAD_CACHE_BATCH_BYTES_SIZE,
                DEFAULT_READ_AHEAD_CACHE_BATCH_BYTES_SIZE);

        ledgerStorageList = Lists.newArrayList();
        for (int i = 0; i < ledgerDirsManager.getAllLedgerDirs().size(); i++) {
            File ledgerDir = ledgerDirsManager.getAllLedgerDirs().get(i);
            File indexDir = indexDirsManager.getAllLedgerDirs().get(i);
            // Create a ledger dirs manager for the single directory
            File[] lDirs = new File[1];
            // Remove the `/current` suffix which will be appended again by LedgersDirManager
            lDirs[0] = ledgerDir.getParentFile();
            LedgerDirsManager ldm = new LedgerDirsManager(conf, lDirs, ledgerDirsManager.getDiskChecker(),
                    NullStatsLogger.INSTANCE);

            // Create a index dirs manager for the single directory
            File[] iDirs = new File[1];
            // Remove the `/current` suffix which will be appended again by LedgersDirManager
            iDirs[0] = indexDir.getParentFile();
            LedgerDirsManager idm = new LedgerDirsManager(conf, iDirs, indexDirsManager.getDiskChecker(),
                    NullStatsLogger.INSTANCE);

            EntryLogger entrylogger;
            if (directIOEntryLogger) {
                long perDirectoryTotalWriteBufferSize = MB * getLongVariableOrDefault(
                    conf,
                    DIRECT_IO_ENTRYLOGGER_TOTAL_WRITEBUFFER_SIZE_MB,
                    DEFAULT_DIRECT_IO_TOTAL_WRITEBUFFER_SIZE_MB) / numberOfDirs;
                long perDirectoryTotalReadBufferSize = MB * getLongVariableOrDefault(
                    conf,
                    DIRECT_IO_ENTRYLOGGER_TOTAL_READBUFFER_SIZE_MB,
                    DEFAULT_DIRECT_IO_TOTAL_READBUFFER_SIZE_MB) / numberOfDirs;
                int readBufferSize = MB * (int) getLongVariableOrDefault(
                    conf,
                    DIRECT_IO_ENTRYLOGGER_READBUFFER_SIZE_MB,
                    DEFAULT_DIRECT_IO_READBUFFER_SIZE_MB);
                int maxFdCacheTimeSeconds = (int) getLongVariableOrDefault(
                    conf,
                    DIRECT_IO_ENTRYLOGGER_MAX_FD_CACHE_TIME_SECONDS,
                    DEFAULT_DIRECT_IO_MAX_FD_CACHE_TIME_SECONDS);
                Slf4jSlogger slog = new Slf4jSlogger(DbLedgerStorage.class);
                entryLoggerWriteExecutor = Executors.newSingleThreadExecutor(
                    new DefaultThreadFactory("EntryLoggerWrite"));
                entryLoggerFlushExecutor = Executors.newSingleThreadExecutor(
                    new DefaultThreadFactory("EntryLoggerFlush"));

                int numReadThreads = conf.getNumReadWorkerThreads();
                if (numReadThreads == 0) {
                    numReadThreads = conf.getServerNumIOThreads();
                }

                entrylogger = new DirectEntryLogger(ledgerDir, new EntryLogIdsImpl(ldm, slog),
                    new NativeIOImpl(),
                    allocator, entryLoggerWriteExecutor, entryLoggerFlushExecutor,
                    conf.getEntryLogSizeLimit(),
                    conf.getNettyMaxFrameSizeBytes() - 500,
                    perDirectoryTotalWriteBufferSize,
                    perDirectoryTotalReadBufferSize,
                    readBufferSize,
                    numReadThreads,
                    maxFdCacheTimeSeconds,
                    slog, statsLogger);
            } else {
                entrylogger = new DefaultEntryLogger(conf, ldm, null, statsLogger, allocator);
            }
            ledgerStorageList.add(newSingleDirectoryDbLedgerStorage(conf, ledgerManager, ldm,
                idm, entrylogger,
                statsLogger, perDirectoryWriteCacheSize,
                perDirectoryReadCacheSize,
                readAheadCacheBatchSize, readAheadCacheBatchBytesSize));
            ldm.getListeners().forEach(ledgerDirsManager::addLedgerDirsListener);
            if (!lDirs[0].getPath().equals(iDirs[0].getPath())) {
                idm.getListeners().forEach(indexDirsManager::addLedgerDirsListener);
            }
        }

        // parent DbLedgerStorage stats (not per directory)
        readaheadBatchSizeGauge = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return readAheadCacheBatchSize;
            }

            @Override
            public Integer getSample() {
                return readAheadCacheBatchSize;
            }
        };
        statsLogger.registerGauge(MAX_READAHEAD_BATCH_SIZE, readaheadBatchSizeGauge);

        writeCacheSizeGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return perDirectoryWriteCacheSize;
            }

            @Override
            public Long getSample() {
                return perDirectoryWriteCacheSize;
            }
        };
        statsLogger.registerGauge(MAX_WRITE_CACHE_SIZE, writeCacheSizeGauge);
    }

    @VisibleForTesting
    protected SingleDirectoryDbLedgerStorage newSingleDirectoryDbLedgerStorage(ServerConfiguration conf,
            LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
            EntryLogger entryLogger, StatsLogger statsLogger, long writeCacheSize, long readCacheSize,
            int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize)
            throws IOException {
        return new SingleDirectoryDbLedgerStorage(conf, ledgerManager, ledgerDirsManager, indexDirsManager, entryLogger,
                                                  statsLogger, allocator, writeCacheSize, readCacheSize,
                                                  readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
    }

    @Override
    public void setStateManager(StateManager stateManager) {
        ledgerStorageList.forEach(s -> s.setStateManager(stateManager));
    }
    @Override
    public void setCheckpointSource(CheckpointSource checkpointSource) {
        ledgerStorageList.forEach(s -> s.setCheckpointSource(checkpointSource));
    }
    @Override
    public void setCheckpointer(Checkpointer checkpointer) {
        ledgerStorageList.forEach(s -> s.setCheckpointer(checkpointer));
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

        if (entryLoggerWriteExecutor != null) {
            entryLoggerWriteExecutor.shutdown();
        }
        if (entryLoggerFlushExecutor != null) {
            entryLoggerFlushExecutor.shutdown();
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).ledgerExists(ledgerId);
    }

    @Override
    public boolean entryExists(long ledgerId, long entryId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).entryExists(ledgerId, entryId);
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).isFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        getLedgerStorage(ledgerId).setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).readMasterKey(ledgerId);
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException, BookieException {
        long ledgerId = entry.getLong(entry.readerIndex());
        return getLedgerStorage(ledgerId).addEntry(entry);
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).getEntry(ledgerId, entryId);
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).getLastAddConfirmed(ledgerId);
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId, long previousLAC,
            Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        return getLedgerStorage(ledgerId).waitForLastAddConfirmedUpdate(ledgerId, previousLAC, watcher);
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        getLedgerStorage(ledgerId).cancelWaitForLastAddConfirmedUpdate(ledgerId, watcher);
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
        getLedgerStorage(ledgerId).deleteLedger(ledgerId);
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        ledgerStorageList.forEach(ls -> ls.registerLedgerDeletionListener(listener));
    }

    @Override
    public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        getLedgerStorage(ledgerId).setExplicitLac(ledgerId, lac);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).getExplicitLac(ledgerId);
    }

    public long addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
                                 LedgerCache.PageEntriesIterable pages) throws Exception {
        return getLedgerStorage(ledgerId).addLedgerToIndex(ledgerId, isFenced, masterKey, pages);
    }

    public long getLastEntryInLedger(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).getEntryLocationIndex().getLastEntryInLedger(ledgerId);
    }

    public long getLocation(long ledgerId, long entryId) throws IOException {
        return getLedgerStorage(ledgerId).getEntryLocationIndex().getLocation(ledgerId, entryId);
    }

    private SingleDirectoryDbLedgerStorage getLedgerStorage(long ledgerId) {
        return ledgerStorageList.get(MathUtils.signSafeMod(ledgerId, numberOfDirs));
    }

    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        List<Iterable<Long>> listIt = new ArrayList<>(numberOfDirs);
        for (SingleDirectoryDbLedgerStorage ls : ledgerStorageList) {
            listIt.add(ls.getActiveLedgersInRange(firstLedgerId, lastLedgerId));
        }

        return Iterables.concat(listIt);
    }

    public ByteBuf getLastEntry(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).getLastEntry(ledgerId);
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

        DiskChecker diskChecker = new DiskChecker(serverConf.getDiskUsageThreshold(),
                serverConf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(serverConf,
                serverConf.getLedgerDirs(), diskChecker);
        LedgerDirsManager indexDirsManager = ledgerDirsManager;
        File[] idxDirs = serverConf.getIndexDirs();
        if (null != idxDirs) {
            indexDirsManager = new LedgerDirsManager(serverConf, idxDirs, diskChecker);
        }
        List<File> ledgerDirs = ledgerDirsManager.getAllLedgerDirs();
        List<File> indexDirs = indexDirsManager.getAllLedgerDirs();
        if (ledgerDirs.size() != indexDirs.size()) {
            throw new IOException("ledger and index dirs size not matched");
        }
        int dirIndex = MathUtils.signSafeMod(ledgerId, ledgerDirs.size());
        String indexBasePath = indexDirs.get(dirIndex).toString();

        EntryLocationIndex entryLocationIndex = new EntryLocationIndex(serverConf,
                (basePath, subPath, dbConfigType, conf1) ->
                        new KeyValueStorageRocksDB(basePath, subPath, DbConfigType.Default, conf1, true),
                indexBasePath, NullStatsLogger.INSTANCE);
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
    public void forceGC(boolean forceMajor, boolean forceMinor) {
        ledgerStorageList.stream().forEach(s -> s.forceGC(forceMajor, forceMinor));
    }

    @Override
    public boolean isInForceGC() {
        return ledgerStorageList.stream().anyMatch(SingleDirectoryDbLedgerStorage::isInForceGC);
    }

    @Override
    public void suspendMinorGC() {
        ledgerStorageList.stream().forEach(SingleDirectoryDbLedgerStorage::suspendMinorGC);
    }

    @Override
    public void suspendMajorGC() {
        ledgerStorageList.stream().forEach(SingleDirectoryDbLedgerStorage::suspendMajorGC);
    }

    @Override
    public void resumeMinorGC() {
        ledgerStorageList.stream().forEach(SingleDirectoryDbLedgerStorage::resumeMinorGC);
    }

    @Override
    public void resumeMajorGC() {
        ledgerStorageList.stream().forEach(SingleDirectoryDbLedgerStorage::resumeMajorGC);
    }

    @Override
    public boolean isMajorGcSuspended() {
        return ledgerStorageList.stream().allMatch(SingleDirectoryDbLedgerStorage::isMajorGcSuspended);
    }

    @Override
    public boolean isMinorGcSuspended() {
        return ledgerStorageList.stream().allMatch(SingleDirectoryDbLedgerStorage::isMinorGcSuspended);
    }

    @Override
    public void entryLocationCompact() {
        ledgerStorageList.forEach(SingleDirectoryDbLedgerStorage::entryLocationCompact);
    }

    @Override
    public void entryLocationCompact(List<String> locations) {
        for (SingleDirectoryDbLedgerStorage ledgerStorage : ledgerStorageList) {
            String entryLocation = ledgerStorage.getEntryLocationDBPath().get(0);
            if (locations.contains(entryLocation)) {
                ledgerStorage.entryLocationCompact();
            }
        }
    }

    @Override
    public boolean isEntryLocationCompacting() {
        return ledgerStorageList.stream().anyMatch(SingleDirectoryDbLedgerStorage::isEntryLocationCompacting);
    }

    @Override
    public Map<String, Boolean> isEntryLocationCompacting(List<String> locations) {
        HashMap<String, Boolean> isCompacting = Maps.newHashMap();
        for (SingleDirectoryDbLedgerStorage ledgerStorage : ledgerStorageList) {
            String entryLocation = ledgerStorage.getEntryLocationDBPath().get(0);
            if (locations.contains(entryLocation)) {
                isCompacting.put(entryLocation, ledgerStorage.isEntryLocationCompacting());
            }
        }
        return isCompacting;
    }

    @Override
    public List<String> getEntryLocationDBPath() {
        List<String> allEntryLocationDBPath = Lists.newArrayList();
        for (SingleDirectoryDbLedgerStorage ledgerStorage : ledgerStorageList) {
            allEntryLocationDBPath.addAll(ledgerStorage.getEntryLocationDBPath());
        }
        return allEntryLocationDBPath;
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

    static boolean getBooleanVariableOrDefault(ServerConfiguration conf, String keyName, boolean defaultValue) {
        Object obj = conf.getProperty(keyName);
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj == null) {
            return defaultValue;
        } else if (StringUtils.isEmpty(conf.getString(keyName))) {
            return defaultValue;
        } else {
            return conf.getBoolean(keyName);
        }
    }

    @Override
    public OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        // check Issue #2078
        throw new UnsupportedOperationException(
                "getListOfEntriesOfLedger method is currently unsupported for DbLedgerStorage");
    }

    @Override
    public void setLimboState(long ledgerId) throws IOException {
        getLedgerStorage(ledgerId).setLimboState(ledgerId);
    }

    @Override
    public boolean hasLimboState(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).hasLimboState(ledgerId);
    }

    @Override
    public void clearLimboState(long ledgerId) throws IOException {
        getLedgerStorage(ledgerId).clearLimboState(ledgerId);
    }

    @Override
    public EnumSet<StorageState> getStorageStateFlags() throws IOException {
        return getLedgerStorage(STORAGE_FLAGS_KEY).getStorageStateFlags();
    }

    @Override
    public void setStorageStateFlag(StorageState flag) throws IOException {
        getLedgerStorage(STORAGE_FLAGS_KEY).setStorageStateFlag(flag);
    }

    @Override
    public void clearStorageStateFlag(StorageState flag) throws IOException {
        getLedgerStorage(STORAGE_FLAGS_KEY).clearStorageStateFlag(flag);
    }
}
