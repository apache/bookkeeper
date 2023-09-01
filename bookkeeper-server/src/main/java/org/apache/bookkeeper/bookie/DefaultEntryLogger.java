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

package org.apache.bookkeeper.bookie;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import org.apache.bookkeeper.bookie.storage.CompactionEntryLog;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.HardLink;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LedgerDirUtil;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap.BiConsumerLong;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the writing of the bookkeeper entries. All the new
 * entries are written to a common log. The LedgerCache will have pointers
 * into files created by this class with offsets into the files to find
 * the actual ledger entry. The entry log files created by this class are
 * identified by a long.
 */
public class DefaultEntryLogger implements EntryLogger {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultEntryLogger.class);

    @VisibleForTesting
    static final int UNINITIALIZED_LOG_ID = -0xDEAD;

    static class BufferedLogChannel extends BufferedChannel {
        private final long logId;
        private final EntryLogMetadata entryLogMetadata;
        private final File logFile;
        private long ledgerIdAssigned = UNASSIGNED_LEDGERID;

        public BufferedLogChannel(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity,
                long logId, File logFile, long unpersistedBytesBound) throws IOException {
            super(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
            this.logId = logId;
            this.entryLogMetadata = new EntryLogMetadata(logId);
            this.logFile = logFile;
        }
        public long getLogId() {
            return logId;
        }

        public File getLogFile() {
            return logFile;
        }

        public void registerWrittenEntry(long ledgerId, long entrySize) {
            entryLogMetadata.addLedgerSize(ledgerId, entrySize);
        }

        public ConcurrentLongLongHashMap getLedgersMap() {
            return entryLogMetadata.getLedgersMap();
        }

        public Long getLedgerIdAssigned() {
            return ledgerIdAssigned;
        }

        public void setLedgerIdAssigned(Long ledgerId) {
            this.ledgerIdAssigned = ledgerId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(BufferedChannel.class)
                .add("logId", logId)
                .add("logFile", logFile)
                .add("ledgerIdAssigned", ledgerIdAssigned)
                .toString();
        }

        /**
         * Append the ledger map at the end of the entry log.
         * Updates the entry log file header with the offset and size of the map.
         */
        void appendLedgersMap() throws IOException {

            long ledgerMapOffset = this.position();

            ConcurrentLongLongHashMap ledgersMap = this.getLedgersMap();
            int numberOfLedgers = (int) ledgersMap.size();

            // Write the ledgers map into several batches

            final int maxMapSize = LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * LEDGERS_MAP_MAX_BATCH_SIZE;
            final ByteBuf serializedMap = ByteBufAllocator.DEFAULT.buffer(maxMapSize);

            try {
                ledgersMap.forEach(new BiConsumerLong() {
                    int remainingLedgers = numberOfLedgers;
                    boolean startNewBatch = true;
                    int remainingInBatch = 0;

                    @Override
                    public void accept(long ledgerId, long size) {
                        if (startNewBatch) {
                            int batchSize = Math.min(remainingLedgers, LEDGERS_MAP_MAX_BATCH_SIZE);
                            int ledgerMapSize = LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * batchSize;

                            serializedMap.clear();
                            serializedMap.writeInt(ledgerMapSize - 4);
                            serializedMap.writeLong(INVALID_LID);
                            serializedMap.writeLong(LEDGERS_MAP_ENTRY_ID);
                            serializedMap.writeInt(batchSize);

                            startNewBatch = false;
                            remainingInBatch = batchSize;
                        }
                        // Dump the ledger in the current batch
                        serializedMap.writeLong(ledgerId);
                        serializedMap.writeLong(size);
                        --remainingLedgers;

                        if (--remainingInBatch == 0) {
                            // Close current batch
                            try {
                                write(serializedMap);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            startNewBatch = true;
                        }
                    }
                });
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                } else {
                    throw e;
                }
            } finally {
                ReferenceCountUtil.release(serializedMap);
            }
            // Flush the ledger's map out before we write the header.
            // Otherwise the header might point to something that is not fully
            // written
            super.flush();

            // Update the headers with the map offset and count of ledgers
            ByteBuffer mapInfo = ByteBuffer.allocate(8 + 4);
            mapInfo.putLong(ledgerMapOffset);
            mapInfo.putInt(numberOfLedgers);
            mapInfo.flip();
            this.fileChannel.write(mapInfo, LEDGERS_MAP_OFFSET_POSITION);
        }
    }

    private final LedgerDirsManager ledgerDirsManager;
    private final boolean entryLogPerLedgerEnabled;

    final RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;

    /**
     * locks for compaction log.
     */
    private final Object compactionLogLock = new Object();

    private volatile BufferedLogChannel compactionLogChannel;

    final EntryLoggerAllocator entryLoggerAllocator;
    private final EntryLogManager entryLogManager;

    private final CopyOnWriteArrayList<EntryLogListener> listeners = new CopyOnWriteArrayList<EntryLogListener>();

    private static final int HEADER_V0 = 0; // Old log file format (no ledgers map index)
    private static final int HEADER_V1 = 1; // Introduced ledger map index
    static final int HEADER_CURRENT_VERSION = HEADER_V1;

    private static class Header {
        final int version;
        final long ledgersMapOffset;
        final int ledgersCount;

        Header(int version, long ledgersMapOffset, int ledgersCount) {
            this.version = version;
            this.ledgersMapOffset = ledgersMapOffset;
            this.ledgersCount = ledgersCount;
        }
    }

    /**
     * The 1K block at the head of the entry logger file
     * that contains the fingerprint and meta-data.
     *
     * <pre>
     * Header is composed of:
     * Fingerprint: 4 bytes "BKLO"
     * Log file HeaderVersion enum: 4 bytes
     * Ledger map offset: 8 bytes
     * Ledgers Count: 4 bytes
     * </pre>
     */
    static final int LOGFILE_HEADER_SIZE = 1024;
    final ByteBuf logfileHeader = Unpooled.buffer(LOGFILE_HEADER_SIZE);

    static final int HEADER_VERSION_POSITION = 4;
    static final int LEDGERS_MAP_OFFSET_POSITION = HEADER_VERSION_POSITION + 4;

    /**
     * Ledgers map is composed of multiple parts that can be split into separated entries. Each of them is composed of:
     *
     * <pre>
     * length: (4 bytes) [0-3]
     * ledger id (-1): (8 bytes) [4 - 11]
     * entry id: (8 bytes) [12-19]
     * num ledgers stored in current metadata entry: (4 bytes) [20 - 23]
     * ledger entries: sequence of (ledgerid, size) (8 + 8 bytes each) [24..]
     * </pre>
     */
    static final int LEDGERS_MAP_HEADER_SIZE = 4 + 8 + 8 + 4;
    static final int LEDGERS_MAP_ENTRY_SIZE = 8 + 8;

    // Break the ledgers map into multiple batches, each of which can contain up to 10K ledgers
    static final int LEDGERS_MAP_MAX_BATCH_SIZE = 10000;

    static final long INVALID_LID = -1L;

    // EntryId used to mark an entry (belonging to INVALID_ID) as a component of the serialized ledgers map
    static final long LEDGERS_MAP_ENTRY_ID = -2L;

    static final int MIN_SANE_ENTRY_SIZE = 8 + 8;
    static final long MB = 1024 * 1024;

    private final int maxSaneEntrySize;

    private final ByteBufAllocator allocator;

    final ServerConfiguration conf;

    /**
     * Entry Log Listener.
     */
    interface EntryLogListener {
        /**
         * Rotate a new entry log to write.
         */
        void onRotateEntryLog();
    }

    public DefaultEntryLogger(ServerConfiguration conf) throws IOException {
        this(conf, new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())));
    }

    /**
     * Create an EntryLogger that stores it's log files in the given directories.
     */
    public DefaultEntryLogger(ServerConfiguration conf,
                              LedgerDirsManager ledgerDirsManager) throws IOException {
        this(conf, ledgerDirsManager, null, NullStatsLogger.INSTANCE, PooledByteBufAllocator.DEFAULT);
    }

    public DefaultEntryLogger(ServerConfiguration conf,
                              LedgerDirsManager ledgerDirsManager, EntryLogListener listener, StatsLogger statsLogger,
                              ByteBufAllocator allocator) throws IOException {
        //We reserve 500 bytes as overhead for the protocol.  This is not 100% accurate
        // but the protocol varies so an exact value is difficult to determine
        this.maxSaneEntrySize = conf.getNettyMaxFrameSizeBytes() - 500;
        this.allocator = allocator;
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        entryLogPerLedgerEnabled = conf.isEntryLogPerLedgerEnabled();
        if (listener != null) {
            addListener(listener);
        }

        // Initialize the entry log header buffer. This cannot be a static object
        // since in our unit tests, we run multiple Bookies and thus EntryLoggers
        // within the same JVM. All of these Bookie instances access this header
        // so there can be race conditions when entry logs are rolled over and
        // this header buffer is cleared before writing it into the new logChannel.
        logfileHeader.writeBytes("BKLO".getBytes(UTF_8));
        logfileHeader.writeInt(HEADER_CURRENT_VERSION);
        logfileHeader.writerIndex(LOGFILE_HEADER_SIZE);

        // Find the largest logId
        long logId = INVALID_LID;
        for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
            if (!dir.exists()) {
                throw new FileNotFoundException(
                        "Entry log directory '" + dir + "' does not exist");
            }
            long lastLogId;
            long lastLogFileFromFile = getLastLogIdFromFile(dir);
            long lastLogIdInDir = getLastLogIdInDir(dir);
            if (lastLogFileFromFile < lastLogIdInDir) {
                LOG.info("The lastLogFileFromFile is {}, the lastLogIdInDir is {}, "
                        + "use lastLogIdInDir as the lastLogId.", lastLogFileFromFile, lastLogIdInDir);
                lastLogId = lastLogIdInDir;
            } else {
                lastLogId = lastLogFileFromFile;
            }

            if (lastLogId > logId) {
                logId = lastLogId;
            }
        }
        this.recentlyCreatedEntryLogsStatus = new RecentEntryLogsStatus(logId + 1);
        this.entryLoggerAllocator = new EntryLoggerAllocator(conf, ledgerDirsManager, recentlyCreatedEntryLogsStatus,
                logId, allocator);
        if (entryLogPerLedgerEnabled) {
            this.entryLogManager = new EntryLogManagerForEntryLogPerLedger(conf, ledgerDirsManager,
                    entryLoggerAllocator, listeners, recentlyCreatedEntryLogsStatus, statsLogger);
        } else {
            this.entryLogManager = new EntryLogManagerForSingleEntryLog(conf, ledgerDirsManager, entryLoggerAllocator,
                    listeners, recentlyCreatedEntryLogsStatus);
        }
    }

    EntryLogManager getEntryLogManager() {
        return entryLogManager;
    }

    void addListener(EntryLogListener listener) {
        if (null != listener) {
            listeners.add(listener);
        }
    }

    /**
     * If the log id of current writable channel is the same as entryLogId and the position
     * we want to read might end up reading from a position in the write buffer of the
     * buffered channel, route this read to the current logChannel. Else,
     * read from the BufferedReadChannel that is provided.
     * @param entryLogId
     * @param channel
     * @param buff remaining() on this bytebuffer tells us the last position that we
     *             expect to read.
     * @param pos The starting position from where we want to read.
     * @return
     */
    private int readFromLogChannel(long entryLogId, BufferedReadChannel channel, ByteBuf buff, long pos)
            throws IOException {
        BufferedLogChannel bc = entryLogManager.getCurrentLogIfPresent(entryLogId);
        if (null != bc) {
            synchronized (bc) {
                if (pos + buff.writableBytes() >= bc.getFileChannelPosition()) {
                    return bc.read(buff, pos);
                }
            }
        }
        return channel.read(buff, pos);
    }

    /**
     * A thread-local variable that wraps a mapping of log ids to bufferedchannels
     * These channels should be used only for reading. logChannel is the one
     * that is used for writes.
     */
    private final ThreadLocal<Map<Long, BufferedReadChannel>> logid2Channel =
            new ThreadLocal<Map<Long, BufferedReadChannel>>() {
        @Override
        public Map<Long, BufferedReadChannel> initialValue() {
            // Since this is thread local there only one modifier
            // We dont really need the concurrency, but we need to use
            // the weak values. Therefore using the concurrency level of 1
            return new MapMaker().concurrencyLevel(1)
                .weakValues()
                .makeMap();
        }
    };

    /**
     * Each thread local buffered read channel can share the same file handle because reads are not relative
     * and don't cause a change in the channel's position. We use this map to store the file channels. Each
     * file channel is mapped to a log id which represents an open log file.
     */
    private final ConcurrentMap<Long, FileChannel> logid2FileChannel = new ConcurrentHashMap<Long, FileChannel>();

    /**
     * Put the logId, bc pair in the map responsible for the current thread.
     * @param logId
     * @param bc
     */
    public BufferedReadChannel putInReadChannels(long logId, BufferedReadChannel bc) {
        Map<Long, BufferedReadChannel> threadMap = logid2Channel.get();
        return threadMap.put(logId, bc);
    }

    /**
     * Remove all entries for this log file in each thread's cache.
     * @param logId
     */
    public void removeFromChannelsAndClose(long logId) {
        FileChannel fileChannel = logid2FileChannel.remove(logId);
        if (null != fileChannel) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                LOG.warn("Exception while closing channel for log file:" + logId);
            }
        }
    }

    public BufferedReadChannel getFromChannels(long logId) {
        return logid2Channel.get().get(logId);
    }

    @VisibleForTesting
    long getLeastUnflushedLogId() {
        return recentlyCreatedEntryLogsStatus.getLeastUnflushedLogId();
    }

    @Override
    public Set<Long> getFlushedLogIds() {
        Set<Long> logIds = new HashSet<>();
        synchronized (recentlyCreatedEntryLogsStatus) {
            for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
                if (dir.exists() && dir.isDirectory()) {
                    File[] files = dir.listFiles(file -> file.getName().endsWith(".log"));
                    if (files != null && files.length > 0) {
                        for (File f : files) {
                            long logId = fileName2LogId(f.getName());
                            if (recentlyCreatedEntryLogsStatus.isFlushedLogId(logId)) {
                                logIds.add(logId);
                            }
                        }
                    }
                }
            }
        }
        return logIds;
    }

    long getPreviousAllocatedEntryLogId() {
        return entryLoggerAllocator.getPreallocatedLogId();
    }

    /**
     * Get the current log file for compaction.
     */
    private File getCurCompactionLogFile() {
        synchronized (compactionLogLock) {
            if (compactionLogChannel == null) {
                return null;
            }
            return compactionLogChannel.getLogFile();
        }
    }

    void prepareSortedLedgerStorageCheckpoint(long numBytesFlushed) throws IOException {
        entryLogManager.prepareSortedLedgerStorageCheckpoint(numBytesFlushed);
    }

    void prepareEntryMemTableFlush() {
        entryLogManager.prepareEntryMemTableFlush();
    }

    boolean commitEntryMemTableFlush() throws IOException {
        return entryLogManager.commitEntryMemTableFlush();
    }

    /**
     * get EntryLoggerAllocator, Just for tests.
     */
    EntryLoggerAllocator getEntryLoggerAllocator() {
        return entryLoggerAllocator;
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    @Override
    public boolean removeEntryLog(long entryLogId) {
        removeFromChannelsAndClose(entryLogId);
        File entryLogFile;
        try {
            entryLogFile = findFile(entryLogId);
        } catch (FileNotFoundException e) {
            LOG.error("Trying to delete an entryLog file that could not be found: "
                    + entryLogId + ".log");
            return true;
        }
        if (!entryLogFile.delete()) {
            LOG.warn("Could not delete entry log file {}", entryLogFile);
            return false;
        }
        return true;
    }

    private long getLastLogIdFromFile(File dir) {
        long id = readLastLogId(dir);
        // read success
        if (id > 0) {
            return id;
        }
        // read failed, scan the ledger directories to find biggest log id
        File[] logFiles = dir.listFiles(file -> file.getName().endsWith(".log"));
        List<Long> logs = new ArrayList<Long>();
        if (logFiles != null) {
            for (File lf : logFiles) {
                long logId = fileName2LogId(lf.getName());
                logs.add(logId);
            }
        }
        // no log file found in this directory
        if (0 == logs.size()) {
            return INVALID_LID;
        }
        // order the collections
        Collections.sort(logs);
        return logs.get(logs.size() - 1);
    }

    private long getLastLogIdInDir(File dir) {
        List<Integer> currentIds = new ArrayList<Integer>();
        currentIds.addAll(LedgerDirUtil.logIdsInDirectory(dir));
        currentIds.addAll(LedgerDirUtil.compactedLogIdsInDirectory(dir));
        if (currentIds.isEmpty()) {
            return -1;
        }
        Pair<Integer, Integer> largestGap = LedgerDirUtil.findLargestGap(currentIds);
        return largestGap.getLeft() - 1;
    }

    /**
     * reads id from the "lastId" file in the given directory.
     */
    private long readLastLogId(File f) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(new File(f, "lastId"));
        } catch (FileNotFoundException e) {
            return INVALID_LID;
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fis, UTF_8))) {
            String lastIdString = br.readLine();
            return Long.parseLong(lastIdString, 16);
        } catch (IOException | NumberFormatException e) {
            return INVALID_LID;
        }
    }

    /**
     * Flushes all rotated log channels. After log channels are flushed,
     * move leastUnflushedLogId ptr to current logId.
     */
    void checkpoint() throws IOException {
        entryLogManager.checkpoint();
    }

    @Override
    public void flush() throws IOException {
        entryLogManager.flush();
    }

    long addEntry(long ledger, ByteBuffer entry) throws IOException {
        return entryLogManager.addEntry(ledger, Unpooled.wrappedBuffer(entry), true);
    }

    long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
        return entryLogManager.addEntry(ledger, entry, rollLog);
    }

    @Override
    public long addEntry(long ledger, ByteBuf entry) throws IOException {
        return entryLogManager.addEntry(ledger, entry, true);
    }

    private final FastThreadLocal<ByteBuf> sizeBuffer = new FastThreadLocal<ByteBuf>() {
        @Override
        protected ByteBuf initialValue() throws Exception {
            // Max usage is size (4 bytes) + ledgerId (8 bytes) + entryid (8 bytes)
            return Unpooled.buffer(4 + 8 + 8);
        }
    };

    private long addEntryForCompaction(long ledgerId, ByteBuf entry) throws IOException {
        synchronized (compactionLogLock) {
            int entrySize = entry.readableBytes() + 4;
            if (compactionLogChannel == null) {
                createNewCompactionLog();
            }

            ByteBuf sizeBuffer = this.sizeBuffer.get();
            sizeBuffer.clear();
            sizeBuffer.writeInt(entry.readableBytes());
            compactionLogChannel.write(sizeBuffer);

            long pos = compactionLogChannel.position();
            compactionLogChannel.write(entry);
            compactionLogChannel.registerWrittenEntry(ledgerId, entrySize);
            return (compactionLogChannel.getLogId() << 32L) | pos;
        }
    }

    private void flushCompactionLog() throws IOException {
        synchronized (compactionLogLock) {
            if (compactionLogChannel != null) {
                compactionLogChannel.appendLedgersMap();
                compactionLogChannel.flushAndForceWrite(false);
                LOG.info("Flushed compaction log file {} with logId {}.",
                    compactionLogChannel.getLogFile(),
                    compactionLogChannel.getLogId());
                // since this channel is only used for writing, after flushing the channel,
                // we had to close the underlying file channel. Otherwise, we might end up
                // leaking fds which cause the disk spaces could not be reclaimed.
                compactionLogChannel.close();
            } else {
                throw new IOException("Failed to flush compaction log which has already been removed.");
            }
        }
    }

    private void createNewCompactionLog() throws IOException {
        synchronized (compactionLogLock) {
            if (compactionLogChannel == null) {
                compactionLogChannel = entryLogManager.createNewLogForCompaction();
            }
        }
    }

    /**
     * Remove the current compaction log, usually invoked when compaction failed and
     * we need to do some clean up to remove the compaction log file.
     */
    private void removeCurCompactionLog() {
        synchronized (compactionLogLock) {
            if (compactionLogChannel != null) {
                if (!compactionLogChannel.getLogFile().delete()) {
                    LOG.warn("Could not delete compaction log file {}", compactionLogChannel.getLogFile());
                }

                try {
                    compactionLogChannel.close();
                } catch (IOException e) {
                    LOG.error("Failed to close file channel for compaction log {}", compactionLogChannel.getLogId(),
                            e);
                }
                compactionLogChannel = null;
            }
        }
    }

    static long logIdForOffset(long offset) {
        return offset >> 32L;
    }


    static long posForOffset(long location) {
        return location & 0xffffffffL;
    }


    /**
     * Exception type for representing lookup errors.  Useful for disambiguating different error
     * conditions for reporting purposes.
     */
    static class EntryLookupException extends Exception {
        EntryLookupException(String message) {
            super(message);
        }

        /**
         * Represents case where log file is missing.
         */
        static class MissingLogFileException extends EntryLookupException {
            MissingLogFileException(long ledgerId, long entryId, long entryLogId, long pos) {
                super(String.format("Missing entryLog %d for ledgerId %d, entry %d at offset %d",
                        entryLogId,
                        ledgerId,
                        entryId,
                        pos));
            }
        }

        /**
         * Represents case where entry log is present, but does not contain the specified entry.
         */
        static class MissingEntryException extends EntryLookupException {
            MissingEntryException(long ledgerId, long entryId, long entryLogId, long pos) {
                super(String.format("pos %d (entry %d for ledgerId %d) past end of entryLog %d",
                        pos,
                        entryId,
                        ledgerId,
                        entryLogId));
            }
        }

        /**
         * Represents case where log is present, but encoded entry length header is invalid.
         */
        static class InvalidEntryLengthException extends EntryLookupException {
            InvalidEntryLengthException(long ledgerId, long entryId, long entryLogId, long pos) {
                super(String.format("Invalid entry length at pos %d (entry %d for ledgerId %d) for entryLog %d",
                        pos,
                        entryId,
                        ledgerId,
                        entryLogId));
            }
        }

        /**
         * Represents case where the entry at pos is wrong.
         */
        static class WrongEntryException extends EntryLookupException {
            WrongEntryException(long foundEntryId, long foundLedgerId, long ledgerId,
                                long entryId, long entryLogId, long pos) {
                super(String.format(
                        "Found entry %d, ledger %d at pos %d entryLog %d, should have found entry %d for ledgerId %d",
                        foundEntryId,
                        foundLedgerId,
                        pos,
                        entryLogId,
                        entryId,
                        ledgerId));
            }
        }
    }

    private BufferedReadChannel getFCForEntryInternal(
            long ledgerId, long entryId, long entryLogId, long pos)
            throws EntryLookupException, IOException {
        try {
            return getChannelForLogId(entryLogId);
        } catch (FileNotFoundException e) {
            throw new EntryLookupException.MissingLogFileException(ledgerId, entryId, entryLogId, pos);
        }
    }

    private ByteBuf readEntrySize(long ledgerId, long entryId, long entryLogId, long pos, BufferedReadChannel fc)
            throws EntryLookupException, IOException {
        ByteBuf sizeBuff = sizeBuffer.get();
        sizeBuff.clear();

        long entrySizePos = pos - 4; // we want to get the entrySize as well as the ledgerId and entryId

        try {
            if (readFromLogChannel(entryLogId, fc, sizeBuff, entrySizePos) != sizeBuff.capacity()) {
                throw new EntryLookupException.MissingEntryException(ledgerId, entryId, entryLogId, entrySizePos);
            }
        } catch (BufferedChannelBase.BufferedChannelClosedException | AsynchronousCloseException e) {
            throw new EntryLookupException.MissingLogFileException(ledgerId, entryId, entryLogId, entrySizePos);
        }
        return sizeBuff;
    }

    void checkEntry(long ledgerId, long entryId, long location) throws EntryLookupException, IOException {
        long entryLogId = logIdForOffset(location);
        long pos = posForOffset(location);
        BufferedReadChannel fc = getFCForEntryInternal(ledgerId, entryId, entryLogId, pos);
        ByteBuf sizeBuf = readEntrySize(ledgerId, entryId, entryLogId, pos, fc);
        validateEntry(ledgerId, entryId, entryLogId, pos, sizeBuf);
    }

    private void validateEntry(long ledgerId, long entryId, long entryLogId, long pos, ByteBuf sizeBuff)
            throws IOException, EntryLookupException {
        int entrySize = sizeBuff.readInt();

        // entrySize does not include the ledgerId
        if (entrySize > maxSaneEntrySize) {
            LOG.warn("Sanity check failed for entry size of " + entrySize + " at location " + pos + " in "
                    + entryLogId);
        }
        if (entrySize < MIN_SANE_ENTRY_SIZE) {
            LOG.error("Read invalid entry length {}", entrySize);
            throw new EntryLookupException.InvalidEntryLengthException(ledgerId, entryId, entryLogId, pos);
        }

        long thisLedgerId = sizeBuff.getLong(4);
        long thisEntryId = sizeBuff.getLong(12);
        if (thisLedgerId != ledgerId || thisEntryId != entryId) {
            throw new EntryLookupException.WrongEntryException(
                    thisEntryId, thisLedgerId, ledgerId, entryId, entryLogId, pos);
        }
    }

    @Override
    public ByteBuf readEntry(long ledgerId, long entryId, long entryLocation)
            throws IOException, Bookie.NoEntryException {
        return internalReadEntry(ledgerId, entryId, entryLocation, true /* validateEntry */);
    }

    @Override
    public ByteBuf readEntry(long location) throws IOException, Bookie.NoEntryException {
        return internalReadEntry(-1L, -1L, location, false /* validateEntry */);
    }


    private ByteBuf internalReadEntry(long ledgerId, long entryId, long location, boolean validateEntry)
            throws IOException, Bookie.NoEntryException {
        long entryLogId = logIdForOffset(location);
        long pos = posForOffset(location);


        BufferedReadChannel fc = null;
        int entrySize = -1;
        try {
            fc = getFCForEntryInternal(ledgerId, entryId, entryLogId, pos);

            ByteBuf sizeBuff = readEntrySize(ledgerId, entryId, entryLogId, pos, fc);
            entrySize = sizeBuff.getInt(0);
            if (validateEntry) {
                validateEntry(ledgerId, entryId, entryLogId, pos, sizeBuff);
            }
        } catch (EntryLookupException e) {
            throw new IOException("Bad entry read from log file id: " + entryLogId, e);
        }

        ByteBuf data = allocator.buffer(entrySize, entrySize);
        int rc = readFromLogChannel(entryLogId, fc, data, pos);
        if (rc != entrySize) {
            ReferenceCountUtil.release(data);
            throw new IOException("Bad entry read from log file id: " + entryLogId,
                    new EntryLookupException("Short read for " + ledgerId + "@"
                                              + entryId + " in " + entryLogId + "@"
                                              + pos + "(" + rc + "!=" + entrySize + ")"));
        }
        data.writerIndex(entrySize);

        return data;
    }

    /**
     * Read the header of an entry log.
     */
    private Header getHeaderForLogId(long entryLogId) throws IOException {
        BufferedReadChannel bc = getChannelForLogId(entryLogId);

        // Allocate buffer to read (version, ledgersMapOffset, ledgerCount)
        ByteBuf headers = allocator.directBuffer(LOGFILE_HEADER_SIZE);
        try {
            bc.read(headers, 0);

            // Skip marker string "BKLO"
            headers.readInt();

            int headerVersion = headers.readInt();
            if (headerVersion < HEADER_V0 || headerVersion > HEADER_CURRENT_VERSION) {
                LOG.info("Unknown entry log header version for log {}: {}", entryLogId, headerVersion);
            }

            long ledgersMapOffset = headers.readLong();
            int ledgersCount = headers.readInt();
            return new Header(headerVersion, ledgersMapOffset, ledgersCount);
        } finally {
            ReferenceCountUtil.release(headers);
        }
    }

    private BufferedReadChannel getChannelForLogId(long entryLogId) throws IOException {
        BufferedReadChannel fc = getFromChannels(entryLogId);
        if (fc != null) {
            return fc;
        }
        File file = findFile(entryLogId);
        // get channel is used to open an existing entry log file
        // it would be better to open using read mode
        FileChannel newFc = new RandomAccessFile(file, "r").getChannel();
        FileChannel oldFc = logid2FileChannel.putIfAbsent(entryLogId, newFc);
        if (null != oldFc) {
            newFc.close();
            newFc = oldFc;
        }
        // We set the position of the write buffer of this buffered channel to Long.MAX_VALUE
        // so that there are no overlaps with the write buffer while reading
        fc = new BufferedReadChannel(newFc, conf.getReadBufferBytes());
        putInReadChannels(entryLogId, fc);
        return fc;
    }

    /**
     * Whether the log file exists or not.
     */
    @Override
    public boolean logExists(long logId) {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId) + ".log");
            if (f.exists()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a set with the ids of all the entry log files.
     *
     * @throws IOException
     */
    public Set<Long> getEntryLogsSet() throws IOException {
        Set<Long> entryLogs = Sets.newTreeSet();

        final FilenameFilter logFileFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".log");
            }
        };

        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File[] files = d.listFiles(logFileFilter);
            if (files == null) {
                throw new IOException("Failed to get list of files in directory " + d);
            }

            for (File f : files) {
                Long entryLogId = Long.parseLong(f.getName().split(".log")[0], 16);
                entryLogs.add(entryLogId);
            }
        }
        return entryLogs;
    }

    private File findFile(long logId) throws FileNotFoundException {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId) + ".log");
            if (f.exists()) {
                return f;
            }
        }
        throw new FileNotFoundException("No file for log " + Long.toHexString(logId));
    }

    /**
     * Scan entry log.
     *
     * @param entryLogId Entry Log Id
     * @param scanner Entry Log Scanner
     * @throws IOException
     */
    @Override
    public void scanEntryLog(long entryLogId, EntryLogScanner scanner) throws IOException {
        // Buffer where to read the entrySize (4 bytes) and the ledgerId (8 bytes)
        ByteBuf headerBuffer = Unpooled.buffer(4 + 8);
        BufferedReadChannel bc;
        // Get the BufferedChannel for the current entry log file
        try {
            bc = getChannelForLogId(entryLogId);
        } catch (IOException e) {
            LOG.warn("Failed to get channel to scan entry log: " + entryLogId + ".log");
            throw e;
        }
        // Start the read position in the current entry log file to be after
        // the header where all of the ledger entries are.
        long pos = LOGFILE_HEADER_SIZE;

        // Start with a reasonably sized buffer size
        ByteBuf data = allocator.directBuffer(1024 * 1024);

        try {

            // Read through the entry log file and extract the ledger ID's.
            while (true) {
                // Check if we've finished reading the entry log file.
                if (pos >= bc.size()) {
                    break;
                }
                if (readFromLogChannel(entryLogId, bc, headerBuffer, pos) != headerBuffer.capacity()) {
                    LOG.warn("Short read for entry size from entrylog {}", entryLogId);
                    return;
                }
                long offset = pos;

                int entrySize = headerBuffer.readInt();
                if (entrySize <= 0) { // hitting padding
                    pos++;
                    headerBuffer.clear();
                    continue;
                }
                long ledgerId = headerBuffer.readLong();
                headerBuffer.clear();

                pos += 4;
                if (ledgerId == INVALID_LID || !scanner.accept(ledgerId)) {
                    // skip this entry
                    pos += entrySize;
                    continue;
                }
                // read the entry
                data.clear();
                data.capacity(entrySize);
                int rc = readFromLogChannel(entryLogId, bc, data, pos);
                if (rc != entrySize) {
                    LOG.warn("Short read for ledger entry from entryLog {}@{} ({} != {})",
                            entryLogId, pos, rc, entrySize);
                    return;
                }
                // process the entry
                scanner.process(ledgerId, offset, data);

                // Advance position to the next entry
                pos += entrySize;
            }
        } finally {
            ReferenceCountUtil.release(data);
        }
    }

    public EntryLogMetadata getEntryLogMetadata(long entryLogId, AbstractLogCompactor.Throttler throttler)
        throws IOException {
        // First try to extract the EntryLogMetadata from the index, if there's no index then fallback to scanning the
        // entry log
        try {
            return extractEntryLogMetadataFromIndex(entryLogId);
        } catch (FileNotFoundException fne) {
            LOG.warn("Cannot find entry log file {}.log : {}", Long.toHexString(entryLogId), fne.getMessage());
            throw fne;
        } catch (Exception e) {
            LOG.info("Failed to get ledgers map index from: {}.log : {}", entryLogId, e.getMessage());

            // Fall-back to scanning
            return extractEntryLogMetadataByScanning(entryLogId, throttler);
        }
    }

    EntryLogMetadata extractEntryLogMetadataFromIndex(long entryLogId) throws IOException {
        Header header = getHeaderForLogId(entryLogId);

        if (header.version < HEADER_V1) {
            throw new IOException("Old log file header without ledgers map on entryLogId " + entryLogId);
        }

        if (header.ledgersMapOffset == 0L) {
            // The index was not stored in the log file (possibly because the bookie crashed before flushing it)
            throw new IOException("No ledgers map index found on entryLogId " + entryLogId);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering ledgers maps for log {} at offset: {}", entryLogId, header.ledgersMapOffset);
        }

        BufferedReadChannel bc = getChannelForLogId(entryLogId);

        // There can be multiple entries containing the various components of the serialized ledgers map
        long offset = header.ledgersMapOffset;
        EntryLogMetadata meta = new EntryLogMetadata(entryLogId);

        final int maxMapSize = LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * LEDGERS_MAP_MAX_BATCH_SIZE;
        ByteBuf ledgersMap = allocator.directBuffer(maxMapSize);

        try {
            while (offset < bc.size()) {
                // Read ledgers map size
                sizeBuffer.get().clear();
                bc.read(sizeBuffer.get(), offset);

                int ledgersMapSize = sizeBuffer.get().readInt();
                if (ledgersMapSize <= 0) {
                    break;
                }
                // Read the index into a buffer
                ledgersMap.clear();
                bc.read(ledgersMap, offset + 4, ledgersMapSize);

                // Discard ledgerId and entryId
                long lid = ledgersMap.readLong();
                if (lid != INVALID_LID) {
                    throw new IOException("Cannot deserialize ledgers map from ledger " + lid);
                }

                long entryId = ledgersMap.readLong();
                if (entryId != LEDGERS_MAP_ENTRY_ID) {
                    throw new IOException("Cannot deserialize ledgers map from entryId " + entryId);
                }

                // Read the number of ledgers in the current entry batch
                int ledgersCount = ledgersMap.readInt();

                // Extract all (ledger,size) tuples from buffer
                for (int i = 0; i < ledgersCount; i++) {
                    long ledgerId = ledgersMap.readLong();
                    long size = ledgersMap.readLong();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Recovering ledgers maps for log {} -- Found ledger: {} with size: {}",
                                entryLogId, ledgerId, size);
                    }
                    meta.addLedgerSize(ledgerId, size);
                }
                if (ledgersMap.isReadable()) {
                    throw new IOException("Invalid entry size when reading ledgers map");
                }

                // Move to next entry, if any
                offset += ledgersMapSize + 4;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        } finally {
            ReferenceCountUtil.release(ledgersMap);
        }

        if (meta.getLedgersMap().size() != header.ledgersCount) {
            throw new IOException("Not all ledgers were found in ledgers map index. expected: " + header.ledgersCount
                    + " -- found: " + meta.getLedgersMap().size() + " -- entryLogId: " + entryLogId);
        }

        return meta;
    }

    private EntryLogMetadata extractEntryLogMetadataByScanning(long entryLogId,
                                                               AbstractLogCompactor.Throttler throttler)
        throws IOException {
        final EntryLogMetadata meta = new EntryLogMetadata(entryLogId);

        // Read through the entry log file and extract the entry log meta
        scanEntryLog(entryLogId, new EntryLogScanner() {
            @Override
            public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                if (throttler != null) {
                    throttler.acquire(entry.readableBytes());
                }
                // add new entry size of a ledger to entry log meta
                meta.addLedgerSize(ledgerId, entry.readableBytes() + 4);
            }

            @Override
            public boolean accept(long ledgerId) {
                return ledgerId >= 0;
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("Retrieved entry log meta data entryLogId: {}, meta: {}", entryLogId, meta);
        }
        return meta;
    }

    /**
     * Shutdown method to gracefully stop entry logger.
     */
    @Override
    public void close() {
        // since logChannel is buffered channel, do flush when shutting down
        LOG.info("Stopping EntryLogger");
        try {
            flush();
            for (FileChannel fc : logid2FileChannel.values()) {
                fc.close();
            }
            // clear the mapping, so we don't need to go through the channels again in finally block in normal case.
            logid2FileChannel.clear();
            entryLogManager.close();
            synchronized (compactionLogLock) {
                if (compactionLogChannel != null) {
                    compactionLogChannel.close();
                    compactionLogChannel = null;
                }
            }
        } catch (IOException ie) {
            // we have no idea how to avoid io exception during shutting down, so just ignore it
            LOG.error("Error flush entry log during shutting down, which may cause entry log corrupted.", ie);
        } finally {
            for (FileChannel fc : logid2FileChannel.values()) {
                IOUtils.close(LOG, fc);
            }

            entryLogManager.forceClose();
            synchronized (compactionLogLock) {
                IOUtils.close(LOG, compactionLogChannel);
            }
        }
        // shutdown the pre-allocation thread
        entryLoggerAllocator.stop();
    }

    protected LedgerDirsManager getLedgerDirsManager() {
        return ledgerDirsManager;
    }

    /**
     * Convert log filename (hex format with suffix) to logId in long.
     */
    static long fileName2LogId(String fileName) {
        if (fileName != null && fileName.contains(".")) {
            fileName = fileName.split("\\.")[0];
        }
        try {
            return Long.parseLong(fileName, 16);
        } catch (Exception nfe) {
            LOG.error("Invalid log file name {} found when trying to convert to logId.", fileName, nfe);
        }
        return INVALID_LID;
    }

    /**
     * Convert log Id to hex string.
     */
    static String logId2HexString(long logId) {
        return Long.toHexString(logId);
    }

    /**
     * Datastructure which maintains the status of logchannels. When a
     * logChannel is created entry of < entryLogId, false > will be made to this
     * sortedmap and when logChannel is rotated and flushed then the entry is
     * updated to < entryLogId, true > and all the lowest entries with
     * < entryLogId, true > status will be removed from the sortedmap. So that way
     * we could get least unflushed LogId.
     *
     */
    static class RecentEntryLogsStatus {
        private final SortedMap<Long, Boolean> entryLogsStatusMap;
        private long leastUnflushedLogId;

        RecentEntryLogsStatus(long leastUnflushedLogId) {
            entryLogsStatusMap = new TreeMap<>();
            this.leastUnflushedLogId = leastUnflushedLogId;
        }

        synchronized void createdEntryLog(Long entryLogId) {
            entryLogsStatusMap.put(entryLogId, false);
        }

        synchronized void flushRotatedEntryLog(Long entryLogId) {
            entryLogsStatusMap.replace(entryLogId, true);
            while ((!entryLogsStatusMap.isEmpty()) && (entryLogsStatusMap.get(entryLogsStatusMap.firstKey()))) {
                long leastFlushedLogId = entryLogsStatusMap.firstKey();
                entryLogsStatusMap.remove(leastFlushedLogId);
                leastUnflushedLogId = leastFlushedLogId + 1;
            }
        }

        synchronized long getLeastUnflushedLogId() {
            return leastUnflushedLogId;
        }

        synchronized boolean isFlushedLogId(long entryLogId) {
            return entryLogsStatusMap.getOrDefault(entryLogId, Boolean.FALSE) || entryLogId < leastUnflushedLogId;
        }
    }

    @Override
    public CompactionEntryLog newCompactionLog(long logToCompact) throws IOException {
        createNewCompactionLog();

        File compactingLogFile = getCurCompactionLogFile();
        long compactionLogId = fileName2LogId(compactingLogFile.getName());
        File compactedLogFile = compactedLogFileFromCompacting(compactingLogFile, logToCompact);
        File finalLogFile = new File(compactingLogFile.getParentFile(),
                                     compactingLogFile.getName().substring(0,
                                             compactingLogFile.getName().indexOf(".log") + 4));
        return new EntryLoggerCompactionEntryLog(
                compactionLogId, logToCompact, compactingLogFile, compactedLogFile, finalLogFile);

    }

    private class EntryLoggerCompactionEntryLog implements CompactionEntryLog {
        private final long compactionLogId;
        private final long logIdToCompact;
        private final File compactingLogFile;
        private final File compactedLogFile;
        private final File finalLogFile;

        EntryLoggerCompactionEntryLog(long compactionLogId, long logIdToCompact,
                                      File compactingLogFile,
                                      File compactedLogFile,
                                      File finalLogFile) {
            this.compactionLogId = compactionLogId;
            this.logIdToCompact = logIdToCompact;
            this.compactingLogFile = compactingLogFile;
            this.compactedLogFile = compactedLogFile;
            this.finalLogFile = finalLogFile;
        }

        @Override
        public long addEntry(long ledgerId, ByteBuf entry) throws IOException {
            return addEntryForCompaction(ledgerId, entry);
        }
        @Override
        public void scan(EntryLogScanner scanner) throws IOException {
            scanEntryLog(compactionLogId, scanner);
        }
        @Override
        public void flush() throws IOException {
            flushCompactionLog();
        }
        @Override
        public void abort() {
            removeCurCompactionLog();
            if (compactedLogFile.exists()) {
                if (!compactedLogFile.delete()) {
                    LOG.warn("Could not delete file: {}", compactedLogFile);
                }
            }
        }

        @Override
        public void markCompacted() throws IOException {
            if (compactingLogFile.exists()) {
                if (!compactedLogFile.exists()) {
                    HardLink.createHardLink(compactingLogFile, compactedLogFile);
                }
            } else {
                throw new IOException("Compaction log doesn't exist any more after flush: " + compactingLogFile);
            }
            removeCurCompactionLog();
        }

        @Override
        public void makeAvailable() throws IOException {
            if (!finalLogFile.exists()) {
                HardLink.createHardLink(compactedLogFile, finalLogFile);
            }
        }
        @Override
        public void finalizeAndCleanup() {
            if (compactedLogFile.exists()) {
                if (!compactedLogFile.delete()) {
                    LOG.warn("Could not delete file: {}", compactedLogFile);
                }
            }
            if (compactingLogFile.exists()) {
                if (!compactingLogFile.delete()) {
                    LOG.warn("Could not delete file: {}", compactingLogFile);
                }
            }
        }

        @Override
        public long getDstLogId() {
            return compactionLogId;
        }
        @Override
        public long getSrcLogId() {
            return logIdToCompact;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("logId", compactionLogId)
                .add("compactedLogId", logIdToCompact)
                .add("compactingLogFile", compactingLogFile)
                .add("compactedLogFile", compactedLogFile)
                .add("finalLogFile", finalLogFile)
                .toString();
        }
    }

    @Override
    public Collection<CompactionEntryLog> incompleteCompactionLogs() {
        List<File> ledgerDirs = ledgerDirsManager.getAllLedgerDirs();
        List<CompactionEntryLog> compactionLogs = new ArrayList<>();

        for (File dir : ledgerDirs) {
            File[] compactingPhaseFiles = dir.listFiles(
                    file -> file.getName().endsWith(TransactionalEntryLogCompactor.COMPACTING_SUFFIX));
            if (compactingPhaseFiles != null) {
                for (File file : compactingPhaseFiles) {
                    if (file.delete()) {
                        LOG.info("Deleted failed compaction file {}", file);
                    }
                }
            }
            File[] compactedPhaseFiles = dir.listFiles(
                    file -> file.getName().endsWith(TransactionalEntryLogCompactor.COMPACTED_SUFFIX));
            if (compactedPhaseFiles != null) {
                for (File compactedFile : compactedPhaseFiles) {
                    LOG.info("Found compacted log file {} has partially flushed index, recovering index.",
                             compactedFile);

                    File compactingLogFile = new File(compactedFile.getParentFile(), "doesntexist");
                    long compactionLogId = -1L;
                    long compactedLogId = -1L;
                    String[] parts = compactedFile.getName().split(Pattern.quote("."));
                    boolean valid = true;
                    if (parts.length != 4) {
                        valid = false;
                    } else {
                        try {
                            compactionLogId = Long.parseLong(parts[0], 16);
                            compactedLogId = Long.parseLong(parts[2], 16);
                        } catch (NumberFormatException nfe) {
                            valid = false;
                        }
                    }

                    if (!valid) {
                        LOG.info("Invalid compacted file found ({}), deleting", compactedFile);
                        if (!compactedFile.delete()) {
                            LOG.warn("Couldn't delete invalid compacted file ({})", compactedFile);
                        }
                        continue;
                    }
                    File finalLogFile = new File(compactedFile.getParentFile(), compactionLogId + ".log");

                    compactionLogs.add(
                            new EntryLoggerCompactionEntryLog(compactionLogId, compactedLogId,
                                                              compactingLogFile, compactedFile, finalLogFile));
                }
            }
        }
        return compactionLogs;
    }

    private static File compactedLogFileFromCompacting(File compactionLogFile, long compactingLogId) {
        File dir = compactionLogFile.getParentFile();
        String filename = compactionLogFile.getName();
        String newSuffix = ".log." + DefaultEntryLogger.logId2HexString(compactingLogId)
            + TransactionalEntryLogCompactor.COMPACTED_SUFFIX;
        String hardLinkFilename = filename.replace(TransactionalEntryLogCompactor.COMPACTING_SUFFIX, newSuffix);
        return new File(dir, hardLinkFilename);
    }
}
