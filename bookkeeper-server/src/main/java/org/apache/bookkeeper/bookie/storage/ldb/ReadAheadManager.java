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

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A management tool that supports asynchronous read-ahead operations for {@link SingleDirectoryDbLedgerStorage}.
 **/
public class ReadAheadManager {

    private static final Logger log = LoggerFactory.getLogger(ReadAheadManager.class);

    public static final String READ_AHEAD_MAX_MESSAGES = "dbStorage_readAheadMaxMessages";
    public static final String READ_AHEAD_MAX_BYTES = "dbStorage_readAheadMaxBytes";
    public static final String READ_AHEAD_PRE_TRIGGER_RATIO = "dbStorage_readAheadPreTriggerRatio";
    public static final String READ_AHEAD_TASK_EXPIRED_TIME_MS = "dbStorage_readAheadTaskExpiredTimeMs";
    public static final String READ_AHEAD_TIMEOUT_MS = "dbStorage_readAheadTimeoutMs";

    // operation behavior indicator
    public static final String SUBMIT_READ_AHEAD_TASK_IMMEDIATELY = "dbStorage_submitReadAheadTaskImmediately";
    public static final String READ_AHEAD_TASK_POOL_SIZE = "dbStorage_readAheadTaskPoolSize";

    private static final int DEFAULT_READ_AHEAD_MESSAGES = 1000;
    private static final int DEFAULT_READ_AHEAD_BYTES = 256 * 1024;
    private static final double DEFAULT_PRE_TRIGGER_READ_AHEAD_RATIO = 0.75;

    private static final long DEFAULT_READ_AHEAD_TASK_EXPIRED_TIME_MS = 60 * 1000;
    private static final long DEFAULT_READ_AHEAD_TIMEOUT_MS = 30 * 1000;

    private static final boolean DEFAULT_SUBMIT_READ_AHEAD_TASK_IMMEDIATELY = false;
    private static final int DEFAULT_READ_AHEAD_TASK_POOL_SIZE = 8;

    private static final class LedgerEntryPosition {

        private long ledgerId;
        private long entryId;

        public LedgerEntryPosition(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LedgerEntryPosition that = (LedgerEntryPosition) o;
            return ledgerId == that.ledgerId
                    && entryId == that.entryId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ledgerId, entryId);
        }
    }

    private static final class ReadAheadPos {

        private final long ledgerId;
        private final long entryId;
        private final long location;
        private final long createTimeMs;

        private long readAheadTaskExpiredTimeMs;

        public ReadAheadPos(long ledgerId, long entryId, long location, long readAheadTaskExpiredTimeMs) {

            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.location = location;

            this.createTimeMs = System.currentTimeMillis();
            this.readAheadTaskExpiredTimeMs = readAheadTaskExpiredTimeMs;
        }

        public boolean isExpired() {
            return (System.currentTimeMillis() - createTimeMs) > readAheadTaskExpiredTimeMs;
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public long getEntryId() {
            return entryId;
        }

        public long getLocation() {
            return location;
        }
    }

    private final ExecutorService readAheadExecutor;
    private final ScheduledExecutorService cleanupExecutor;

    private final ConcurrentHashMap<LedgerEntryPosition, ReadAheadPos> pendingReadAheadPositions;
    private final ConcurrentLinkedQueue<LedgerEntryPosition> pendingDeletePositions;

    private final ConcurrentHashMap<Long, NavigableMap<Long, ReadAheadTaskStatus>> inProgressReadAheadTaskStatuses;
    private final ConcurrentLinkedQueue<ReadAheadTaskStatus> pendingDeleteReadAheadTaskStatuses;

    private final EntryLogger entryLogger;
    private final EntryLocationIndex entryLocationIndex;
    private final ReadCache cache;

    private final DbLedgerStorageStats dbLedgerStorageStats;

    private final boolean submitReadAheadTaskImmediately;
    private final int readAheadMessages;
    private final int readAheadBytes;
    private final double preTriggerReadAheadRatio;

    private final long readAheadTaskExpiredTimeMs;
    private final long readAheadTimeoutMs;

    /**
     * Entrance for test cases.
     *
     * @param entryLogger
     * @param entryLocationIndex
     * @param cache
     * @param dbLedgerStorageStats
     */
    public ReadAheadManager(EntryLogger entryLogger, EntryLocationIndex entryLocationIndex,
                            ReadCache cache, DbLedgerStorageStats dbLedgerStorageStats) {
        this(entryLogger, entryLocationIndex, cache, dbLedgerStorageStats,
                DEFAULT_SUBMIT_READ_AHEAD_TASK_IMMEDIATELY, DEFAULT_READ_AHEAD_TASK_POOL_SIZE,
                DEFAULT_READ_AHEAD_MESSAGES, DEFAULT_READ_AHEAD_BYTES, DEFAULT_PRE_TRIGGER_READ_AHEAD_RATIO,
                DEFAULT_READ_AHEAD_TASK_EXPIRED_TIME_MS, DEFAULT_READ_AHEAD_TIMEOUT_MS);
    }

    /**
     * Entrance for normal use.
     *
     * @param entryLogger
     * @param entryLocationIndex
     * @param cache
     * @param dbLedgerStorageStats
     * @param conf
     */
    public ReadAheadManager(EntryLogger entryLogger, EntryLocationIndex entryLocationIndex,
                            ReadCache cache, DbLedgerStorageStats dbLedgerStorageStats, ServerConfiguration conf) {
        this(entryLogger, entryLocationIndex, cache, dbLedgerStorageStats,
                conf.getBoolean(SUBMIT_READ_AHEAD_TASK_IMMEDIATELY, DEFAULT_SUBMIT_READ_AHEAD_TASK_IMMEDIATELY),
                conf.getInt(READ_AHEAD_TASK_POOL_SIZE, DEFAULT_READ_AHEAD_TASK_POOL_SIZE),
                conf.getInt(READ_AHEAD_MAX_MESSAGES, DEFAULT_READ_AHEAD_MESSAGES),
                conf.getInt(READ_AHEAD_MAX_BYTES, DEFAULT_READ_AHEAD_BYTES),
                conf.getDouble(READ_AHEAD_PRE_TRIGGER_RATIO, DEFAULT_PRE_TRIGGER_READ_AHEAD_RATIO),
                conf.getLong(READ_AHEAD_TASK_EXPIRED_TIME_MS, DEFAULT_READ_AHEAD_TASK_EXPIRED_TIME_MS),
                conf.getLong(READ_AHEAD_TIMEOUT_MS, DEFAULT_READ_AHEAD_TIMEOUT_MS));
    }

    public ReadAheadManager(EntryLogger entryLogger, EntryLocationIndex entryLocationIndex,
                            ReadCache cache, DbLedgerStorageStats dbLedgerStorageStats,
                            boolean submitReadAheadTaskImmediately, int readAheadTaskPoolSize,
                            int readAheadMessages, int readAheadBytes, double preTriggerReadAheadRatio,
                            long readAheadTaskExpiredTimeMs, long readAheadTimeoutMs) {
        // core components initialization
        readAheadExecutor = Executors.newFixedThreadPool(
                readAheadTaskPoolSize, new DefaultThreadFactory("read-ahead"));
        cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("read-ahead-cleanup"));

        pendingReadAheadPositions = new ConcurrentHashMap<>();
        pendingDeletePositions = new ConcurrentLinkedQueue<>();

        inProgressReadAheadTaskStatuses = new ConcurrentHashMap<>();
        pendingDeleteReadAheadTaskStatuses = new ConcurrentLinkedQueue<>();

        // external assistant components assignment
        this.entryLogger = entryLogger;
        this.entryLocationIndex = entryLocationIndex;
        this.cache = cache;

        // metrics
        this.dbLedgerStorageStats = dbLedgerStorageStats;

        // configurable arguments
        this.submitReadAheadTaskImmediately = submitReadAheadTaskImmediately;
        this.readAheadMessages = readAheadMessages;
        this.readAheadBytes = readAheadBytes;
        this.preTriggerReadAheadRatio = preTriggerReadAheadRatio;

        this.readAheadTaskExpiredTimeMs = readAheadTaskExpiredTimeMs;
        this.readAheadTimeoutMs = readAheadTimeoutMs;

        cleanupExecutor.scheduleAtFixedRate(
                SafeRunnable.safeRun(this::removeExpiredReadAheadTasks), 30, 30, TimeUnit.SECONDS);
    }

    public void shutdown() {
        this.readAheadExecutor.shutdown();
        this.cleanupExecutor.shutdown();
    }

    private static void recordStatsInNano(OpStatsLogger logger, long startTimeNanos) {
        logger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    private static void recordStatsInNano(Counter counter, long startTimeNanos) {
        counter.add(MathUtils.elapsedNanos(startTimeNanos));
    }

    public void addNextReadPosition(long expectedLedgerId, long expectedEntryId,
                                    long actualStartLedgerId, long actualStartEntryId, long location) {
        LedgerEntryPosition lep = new LedgerEntryPosition(expectedLedgerId, expectedEntryId);
        pendingReadAheadPositions.put(lep, new ReadAheadPos(
                actualStartLedgerId, actualStartEntryId, location, readAheadTaskExpiredTimeMs));
        pendingDeletePositions.add(lep);
    }

    protected ReadAheadTaskStatus getNearestTask(long ledgerId, long entryId) {
        NavigableMap<Long, ReadAheadTaskStatus> ledgerReadAheadTaskStatuses =
                inProgressReadAheadTaskStatuses.get(ledgerId);
        if (ledgerReadAheadTaskStatuses != null) {
            Map.Entry<Long, ReadAheadTaskStatus> floorEntry = ledgerReadAheadTaskStatuses.floorEntry(entryId);
            if (floorEntry != null) {
                return floorEntry.getValue();
            }
        }
        return null;
    }

    /**
     * Remove those read-ahead tasks which have already exceeded expired time.
     * NOTE: this method is NOT thread-safe, thus it should be kept in a single thread.
     */
    private void removeExpiredReadAheadTasks() {
        // cleanup read-ahead pos
        int reclaimedPositions = 0;
        while (!pendingDeletePositions.isEmpty()) {
            ReadAheadPos pos = pendingReadAheadPositions.computeIfPresent(
                    pendingDeletePositions.peek(),
                    (lep, rap) -> {
                        if (rap.isExpired()) {
                            return null;
                        }
                        return rap;
                    });
            if (pos == null) {
                pendingDeletePositions.poll();
                reclaimedPositions++;
            } else {
                break;
            }
        }

        // cleanup read-ahead task
        int reclaimedTasks = 0;
        while (!pendingDeleteReadAheadTaskStatuses.isEmpty()
                && pendingDeleteReadAheadTaskStatuses.peek().isExpired()) {
            ReadAheadTaskStatus readAheadTaskStatus = pendingDeleteReadAheadTaskStatuses.poll();
            reclaimedTasks++;
            inProgressReadAheadTaskStatuses.computeIfPresent(
                    readAheadTaskStatus.ledgerId,
                    (lid, ledgerReadAheadTaskStatuses) -> {
                       ledgerReadAheadTaskStatuses.remove(readAheadTaskStatus.startEntryId);
                       return ledgerReadAheadTaskStatuses.isEmpty() ? null : ledgerReadAheadTaskStatuses;
                    });
        }

        if (log.isDebugEnabled()) {
            log.debug("Pending position map reclaimed {} positions, now is {}. "
                            + "Read-ahead task map reclaimed {} tasks, now is {}",
                    reclaimedPositions, pendingDeletePositions.size(),
                    reclaimedTasks, pendingDeleteReadAheadTaskStatuses.size());
        }
    }

    /**
     * This method could be invoked frequently. Please make it short and simple.
     */
    public boolean hitInReadAheadPositions(long ledgerId, long entryId) {
        AtomicBoolean isHit = new AtomicBoolean(false);
        pendingReadAheadPositions.computeIfPresent(
                new LedgerEntryPosition(ledgerId, entryId),
                (lep, rap) -> {
                    isHit.set(true);
                    readAheadAsync(rap.getLedgerId(), rap.getEntryId(), rap.getLocation(),
                            readAheadMessages, readAheadBytes);

                    if (log.isDebugEnabled()) {
                        log.debug("Submitted read-ahead task. Info: hit-pos=[L{} E{}] / actual-start-pos=[L{} E{}]",
                                ledgerId, entryId, rap.getLedgerId(), rap.getEntryId());
                    }
                    return null;
                });
        return isHit.get();
    }

    private ByteBuf readAndAddNextReadAheadPosition(long ledgerId, long entryId) throws IOException {
        ByteBuf entry;
        long entryLocation;

        long getLocationIndexStartNanos = MathUtils.nowInNano();
        try {
            entryLocation = entryLocationIndex.getLocation(ledgerId, entryId);
            if (entryLocation == 0) {
                throw new Bookie.NoEntryException(ledgerId, entryId);
            }
        } catch (Bookie.NoEntryException e) {
            log.warn("[L{} E{}] Entry not found", ledgerId, entryId);
            throw e;
        } finally {
            recordStatsInNano(dbLedgerStorageStats.getReadFromLocationIndexTime(), getLocationIndexStartNanos);
        }

        long readEntryStartNanos = MathUtils.nowInNano();
        try {
            entry = entryLogger.readEntry(ledgerId, entryId, entryLocation);
        } finally {
            recordStatsInNano(dbLedgerStorageStats.getReadFromEntryLogTime(), readEntryStartNanos);
        }

        cache.put(ledgerId, entryId, entry);
        // init position
        if (submitReadAheadTaskImmediately) {
            // submit the read-ahead task immediately
            readAheadAsync(ledgerId, entryId + 1, entryLocation + 4 + entry.readableBytes(),
                    readAheadMessages, readAheadBytes);
        } else {
            // actually execute read-ahead task after hitting this position next time
            addNextReadPosition(ledgerId, entryId + 1,
                    ledgerId, entryId + 1, entryLocation + 4 + entry.readableBytes());
        }

        if (log.isDebugEnabled()) {
            log.debug("[L{} E{}] Read {} bytes from local storage, and put L{} E{} to the pending map"
                            + " or submit task immediately according to submitReadAheadTaskImmediately={}.",
                    ledgerId, entryId, entry.readableBytes(), ledgerId, entryId + 1, submitReadAheadTaskImmediately);
        }
        return entry;
    }

    /**
     * Read an entry or wait until it is ready to be read.
     *
     * @param ledgerId
     * @param entryId
     * @return
     * @throws IOException
     */
    public ByteBuf readEntryOrWait(long ledgerId, long entryId) throws IOException {
        // check if we need to read ahead
        boolean isHit = hitInReadAheadPositions(ledgerId, entryId);
        if (log.isDebugEnabled()) {
            if (isHit) {
                log.debug("[L{} E{}] Trigger read-ahead task", ledgerId, entryId);
            } else {
                log.debug("[L{} E{}] Not found in pending map", ledgerId, entryId);
            }
        }

        // hit in the cache
        ByteBuf entry = cache.get(ledgerId, entryId);
        if (entry != null) {
            if (log.isDebugEnabled()) {
                log.debug("[L{} E{}] Hit cache directly", ledgerId, entryId);
            }
            dbLedgerStorageStats.getReadCacheHitCounter().inc();
            return entry;
        }
        dbLedgerStorageStats.getReadCacheMissCounter().inc();

        // search entry in the read-ahead index
        ReadAheadTaskStatus readAheadTaskStatus = getNearestTask(ledgerId, entryId);
        if (readAheadTaskStatus != null && readAheadTaskStatus.hasEntry(entryId)) {
            if (log.isDebugEnabled()) {
                log.debug("[L{} E{}] Block until data is ready", ledgerId, entryId);
            }
            readAheadTaskStatus.waitUntilReadCompleted(dbLedgerStorageStats);

            entry = cache.get(ledgerId, entryId);
            if (entry != null) {
                if (log.isDebugEnabled()) {
                    log.debug("[L{} E{}] Hit cache after read-ahead", ledgerId, entryId);
                }
                return entry;
            }
        }

        // read the current pos entry, and add the next pos into read-ahead set
        if (log.isDebugEnabled()) {
            log.debug("[L{} E{}] Read from storage layer", ledgerId, entryId);
        }
        return readAndAddNextReadAheadPosition(ledgerId, entryId);
    }

    private void internalReadAhead(long ledgerId, long entryId, long location,
                                   int maxMessages, int maxBytes, long submitStartNanos) {
        // record queue time
        recordStatsInNano(dbLedgerStorageStats.getReadAheadAsyncQueueTime(), submitStartNanos);
        long readAheadStartNanos = MathUtils.nowInNano();

        // obtain the read-ahead info
        ReadAheadTaskStatus readAheadTaskStatus = getNearestTask(ledgerId, entryId);
        if (readAheadTaskStatus == null) {
            return;
        }

        // record all the positions of these entries
        List<Long> entriesPos = new ArrayList<>();

        // read from fc
        int messages = 0;
        int bytes = 0;

        // flag to determine whether keeps reading ahead
        boolean earlyExit = false;

        // flag to indicate whether reads from cache since last time
        boolean readFromCacheBefore = false;

        try {
            while (messages <= maxMessages && bytes <= maxBytes) {

                ByteBuf entry = cache.get(ledgerId, entryId);
                if (entry != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("[L{} E{}] Entry already exists.", ledgerId, entryId);
                    }
                    readFromCacheBefore = true;
                    entryId++;
                    location += (4 + entry.readableBytes());
                    entry.release();
                    continue;
                }

                if (readFromCacheBefore) {
                    try {
                        entry = entryLogger.readEntry(ledgerId, entryId, location);
                        readFromCacheBefore = false;
                    } catch (Exception e) {
                        if (log.isDebugEnabled()) {
                            log.debug("[L{} E{}] Failed to locate entry with location:{}", ledgerId, entryId, location);
                        }
                        break;
                    }
                } else {
                    entry = entryLogger.internalReadEntry(ledgerId, entryId, location, false);
                }

                try {
                    long currentEntryLedgerId = entry.getLong(0);
                    long currentEntryId = entry.getLong(8);

                    if (currentEntryLedgerId != ledgerId) {
                        earlyExit = true;
                        break;
                    }

                    // add to cache
                    cache.put(ledgerId, currentEntryId, entry);
                    entriesPos.add(currentEntryId);

                    if (log.isDebugEnabled()) {
                        log.debug("[L{} E{}] Entry loaded with {} bytes",
                                ledgerId, entryId, entry.readableBytes());
                    }

                    // update stats
                    messages++;
                    bytes += entry.readableBytes();

                    entryId++;
                    location += (4 + entry.readableBytes());
                } finally {
                    entry.release();
                }
            }
        } catch (Exception e) {
            log.error("Exception during reading ahead for L{} E{}", ledgerId, entryId, e);
            return;
        } finally {
            // update the actual range
            if (messages >= 1) {
                readAheadTaskStatus.updateEndEntry(entriesPos.get(messages - 1));
            }

            // notify all waiting threads
            readAheadTaskStatus.readCompleted();
            pendingDeleteReadAheadTaskStatuses.add(readAheadTaskStatus);
        }

        // set next read-ahead pos
        if (preTriggerReadAheadRatio > 0 && !earlyExit && messages >= 1) {
            int index = Math.min(messages - 1, (int) (preTriggerReadAheadRatio * messages));
            long nextHitEntryId = entriesPos.get(index);
            addNextReadPosition(ledgerId, nextHitEntryId, ledgerId, entryId, location);
        }

        if (log.isDebugEnabled()) {
            log.debug("Read-ahead task [L{} E{} - E{} len={}] eventually collected {} messages and {} bytes in {}ms",
                    readAheadTaskStatus.ledgerId, readAheadTaskStatus.startEntryId, readAheadTaskStatus.endEntryId,
                    readAheadTaskStatus.readAheadEntries, messages, bytes,
                    TimeUnit.NANOSECONDS.toMillis(MathUtils.elapsedNanos(readAheadStartNanos)));
        }

        // record exec time
        dbLedgerStorageStats.getReadAheadBatchCountCounter().add(messages);
        dbLedgerStorageStats.getReadAheadBatchSizeCounter().add(bytes);
        recordStatsInNano(dbLedgerStorageStats.getReadAheadAsyncTotalTime(), submitStartNanos);
    }

    public void readAheadAsync(long ledgerId, long entryId, long location, int maxMessages, int maxBytes) {
        // add a new ReadAheadTaskStatus before actually read-ahead
        inProgressReadAheadTaskStatuses.computeIfAbsent(ledgerId, lid -> new TreeMap<>());
        inProgressReadAheadTaskStatuses.computeIfPresent(ledgerId, (lid, ledgerReadAheadTaskStatuses) -> {
            // ensure that the read-ahead task is unique
            if (ledgerReadAheadTaskStatuses.containsKey(entryId)) {
                if (log.isDebugEnabled()) {
                    log.debug("Read-ahead task of L{} E{} is redundant", ledgerId, entryId);
                }
            } else {
                ledgerReadAheadTaskStatuses.put(entryId, new ReadAheadTaskStatus(
                        ledgerId, entryId, maxMessages, readAheadTaskExpiredTimeMs, readAheadTimeoutMs));
            }
            return ledgerReadAheadTaskStatuses;
        });

        // submit the read-ahead task async
        readAheadExecutor.submit(SafeRunnable.safeRun(
                () -> internalReadAhead(ledgerId, entryId, location, maxMessages, maxBytes, MathUtils.nowInNano())));
    }

    /**
     * A structure that records the transitions of the task status.
     */
    public static class ReadAheadTaskStatus {
        private long ledgerId;
        private long startEntryId;
        private long endEntryId = -1;
        private long readAheadEntries;

        private final long readAheadTaskExpiredTimeMs;
        private final long readAheadTimeoutMs;
        private long readCompletedTimeMs = -1;

        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private volatile boolean readCompleted = false;

        public ReadAheadTaskStatus(long ledgerId, long startEntryId, int readAheadEntries,
                                   long readAheadTaskExpiredTimeMs, long readAheadTimeoutMs) {
            this.ledgerId = ledgerId;
            this.startEntryId = startEntryId;
            this.readAheadEntries = readAheadEntries;

            this.readAheadTaskExpiredTimeMs = readAheadTaskExpiredTimeMs;
            this.readAheadTimeoutMs = readAheadTimeoutMs;
        }

        public boolean hasEntry(long entryId) {
            if (!readCompleted) {
                // the max-estimate is return before the read-ahead task is completed
                return entryId >= startEntryId && entryId <= startEntryId + readAheadEntries;
            } else {
                return entryId >= startEntryId && entryId <= endEntryId;
            }
        }

        public void updateEndEntry(long endEntryId) {
            this.endEntryId = endEntryId;
        }

        public long getEndEntryId() {
            return endEntryId;
        }

        public boolean isExpired() {
            return readCompletedTimeMs > 0
                    && (System.currentTimeMillis() - readCompletedTimeMs) > readAheadTaskExpiredTimeMs;
        }

        public void waitUntilReadCompleted(DbLedgerStorageStats dbLedgerStorageStats) {
            long blockStartNanos = MathUtils.nowInNano();
            lock.lock();
            try {
                while (!readCompleted) {
                    try {
                        boolean signalled = condition.await(readAheadTimeoutMs, TimeUnit.MILLISECONDS);
                        if (!signalled) {
                            log.warn("Failed to read entries={} ahead from L{} E{} due to timeout={}ms",
                                    readAheadEntries, ledgerId, startEntryId, readAheadTimeoutMs);
                            readCompleted();
                        }
                    } catch (InterruptedException e) {
                        log.warn("Failed to read entries={} ahead from L{} E{} due to the interruption of the thread",
                                readAheadEntries, ledgerId, startEntryId, e);
                        readCompleted();
                    }
                }
            } finally {
                lock.unlock();

                // record request blocking time
                recordStatsInNano(dbLedgerStorageStats.getReadAheadAsyncBlockTime(), blockStartNanos);
            }
        }

        public void readCompleted() {
            lock.lock();
            try {
                readCompleted = true;
                condition.signalAll();
                readCompletedTimeMs = System.currentTimeMillis();

                if (log.isDebugEnabled()) {
                    log.debug("[start:[L{} E{}]][len:{}] Completed reading data ahead",
                            ledgerId, startEntryId, readAheadEntries);
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
