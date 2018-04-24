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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTING_SUFFIX;
import static org.apache.bookkeeper.util.BookKeeperConstants.MAX_LOG_SIZE_LIMIT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap.BiConsumerLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the writing of the bookkeeper entries. All the new
 * entries are written to a common log. The LedgerCache will have pointers
 * into files created by this class with offsets into the files to find
 * the actual ledger entry. The entry log files created by this class are
 * identified by a long.
 */
public class EntryLogger {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogger.class);
    static final long UNASSIGNED_LEDGERID = -1L;
    // log file suffix
    private static final String LOG_FILE_SUFFIX = ".log";

    @VisibleForTesting
    static final int UNINITIALIZED_LOG_ID = -0xDEAD;

    static class BufferedLogChannel extends BufferedChannel {
        private final long logId;
        private final EntryLogMetadata entryLogMetadata;
        private final File logFile;
        private long ledgerIdAssigned = UNASSIGNED_LEDGERID;

        public BufferedLogChannel(FileChannel fc, int writeCapacity, int readCapacity, long logId, File logFile,
                long unpersistedBytesBound) throws IOException {
            super(fc, writeCapacity, readCapacity, unpersistedBytesBound);
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
        private void appendLedgersMap() throws IOException {

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
                serializedMap.release();
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

    /**
     * The maximum size of a entry logger file.
     */
    final long logSizeLimit;
    private volatile BufferedLogChannel compactionLogChannel;

    final EntryLoggerAllocator entryLoggerAllocator;
    private final EntryLogManager entryLogManager;

    private final boolean entryLogPreAllocationEnabled;
    private final CopyOnWriteArrayList<EntryLogListener> listeners = new CopyOnWriteArrayList<EntryLogListener>();

    private static final int HEADER_V0 = 0; // Old log file format (no ledgers map index)
    private static final int HEADER_V1 = 1; // Introduced ledger map index
    private static final int HEADER_CURRENT_VERSION = HEADER_V1;

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

    final ServerConfiguration conf;
    /**
     * Scan entries in a entry log file.
     */
    public interface EntryLogScanner {
        /**
         * Tests whether or not the entries belongs to the specified ledger
         * should be processed.
         *
         * @param ledgerId
         *          Ledger ID.
         * @return true if and only the entries of the ledger should be scanned.
         */
        boolean accept(long ledgerId);

        /**
         * Process an entry.
         *
         * @param ledgerId
         *          Ledger ID.
         * @param offset
         *          File offset of this entry.
         * @param entry
         *          Entry ByteBuf
         * @throws IOException
         */
        void process(long ledgerId, long offset, ByteBuf entry) throws IOException;
    }

    /**
     * Entry Log Listener.
     */
    interface EntryLogListener {
        /**
         * Rotate a new entry log to write.
         */
        void onRotateEntryLog();
    }

    /**
     * Create an EntryLogger that stores it's log files in the given directories.
     */
    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) throws IOException {
        this(conf, ledgerDirsManager, null);
    }

    public EntryLogger(ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, EntryLogListener listener)
                    throws IOException {
        //We reserve 500 bytes as overhead for the protocol.  This is not 100% accurate
        // but the protocol varies so an exact value is difficult to determine
        this.maxSaneEntrySize = conf.getNettyMaxFrameSizeBytes() - 500;
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        entryLogPerLedgerEnabled = conf.isEntryLogPerLedgerEnabled();
        if (listener != null) {
            addListener(listener);
        }
        // log size limit
        this.logSizeLimit = Math.min(conf.getEntryLogSizeLimit(), MAX_LOG_SIZE_LIMIT);
        this.entryLogPreAllocationEnabled = conf.isEntryLogFilePreAllocationEnabled();

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
            long lastLogId = getLastLogId(dir);
            if (lastLogId > logId) {
                logId = lastLogId;
            }
        }
        this.recentlyCreatedEntryLogsStatus = new RecentEntryLogsStatus(logId + 1);
        this.entryLoggerAllocator = new EntryLoggerAllocator(logId);
        if (entryLogPerLedgerEnabled) {
            this.entryLogManager = new EntryLogManagerForSingleEntryLog(ledgerDirsManager) {
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
                    Set<BufferedLogChannel> copyOfCurrentLogs = new HashSet<BufferedLogChannel>(
                            Arrays.asList(super.getCurrentLogForLedger(EntryLogger.UNASSIGNED_LEDGERID)));
                    for (BufferedLogChannel currentLog : copyOfCurrentLogs) {
                        if (reachEntryLogLimit(currentLog, 0L)) {
                            synchronized (this) {
                                if (reachEntryLogLimit(currentLog, 0L)) {
                                    LOG.info("Rolling entry logger since it reached size limitation");
                                    createNewLog(EntryLogger.UNASSIGNED_LEDGERID);
                                }
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
            };
        } else {
            this.entryLogManager = new EntryLogManagerForSingleEntryLog(ledgerDirsManager);
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

    /**
     * Get the least unflushed log id. Garbage collector thread should not process
     * unflushed entry log file.
     *
     * @return least unflushed log id.
     */
    long getLeastUnflushedLogId() {
        return recentlyCreatedEntryLogsStatus.getLeastUnflushedLogId();
    }

    long getPreviousAllocatedEntryLogId() {
        return entryLoggerAllocator.getPreallocatedLogId();
    }

    /**
     * Get the current log file for compaction.
     */
    File getCurCompactionLogFile() {
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
     * An allocator pre-allocates entry log files.
     */
    class EntryLoggerAllocator {

        private long preallocatedLogId;
        private Future<BufferedLogChannel> preallocation = null;
        private ExecutorService allocatorExecutor;
        private final Object createEntryLogLock = new Object();
        private final Object createCompactionLogLock = new Object();

        EntryLoggerAllocator(long logId) {
            preallocatedLogId = logId;
            allocatorExecutor = Executors.newSingleThreadExecutor();
        }

        synchronized long getPreallocatedLogId() {
            return preallocatedLogId;
        }

        BufferedLogChannel createNewLog() throws IOException {
            synchronized (createEntryLogLock) {
                BufferedLogChannel bc;
                if (!entryLogPreAllocationEnabled){
                    // create a new log directly
                    bc = allocateNewLog();
                    return bc;
                } else {
                    // allocate directly to response request
                    if (null == preallocation){
                        bc = allocateNewLog();
                    } else {
                        // has a preallocated entry log
                        try {
                            bc = preallocation.get();
                        } catch (ExecutionException ee) {
                            if (ee.getCause() instanceof IOException) {
                                throw (IOException) (ee.getCause());
                            } else {
                                throw new IOException("Error to execute entry log allocation.", ee);
                            }
                        } catch (CancellationException ce) {
                            throw new IOException("Task to allocate a new entry log is cancelled.", ce);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Intrrupted when waiting a new entry log to be allocated.", ie);
                        }
                    }
                    // preallocate a new log in background upon every call
                    preallocation = allocatorExecutor.submit(() -> allocateNewLog());
                    return bc;
                }
            }
        }

        BufferedLogChannel createNewLogForCompaction() throws IOException {
            synchronized (createCompactionLogLock) {
                return allocateNewLog(COMPACTING_SUFFIX);
            }
        }

        private synchronized BufferedLogChannel allocateNewLog() throws IOException {
            return allocateNewLog(".log");
        }

        /**
         * Allocate a new log file.
         */
        private synchronized BufferedLogChannel allocateNewLog(String suffix) throws IOException {
            List<File> list = ledgerDirsManager.getWritableLedgerDirsForNewLog();
            File dirForNextEntryLog = entryLogManager.getDirForNextEntryLog(list);

            List<File> ledgersDirs = ledgerDirsManager.getAllLedgerDirs();
            String logFileName;
            // It would better not to overwrite existing entry log files
            File testLogFile = null;
            do {
                if (preallocatedLogId >= Integer.MAX_VALUE) {
                    preallocatedLogId = 0;
                } else {
                    ++preallocatedLogId;
                }
                logFileName = Long.toHexString(preallocatedLogId) + suffix;
                for (File dir : ledgersDirs) {
                    testLogFile = new File(dir, logFileName);
                    if (testLogFile.exists()) {
                        LOG.warn("Found existed entry log " + testLogFile
                               + " when trying to create it as a new log.");
                        testLogFile = null;
                        break;
                    }
                }
            } while (testLogFile == null);

            File newLogFile = new File(dirForNextEntryLog, logFileName);
            FileChannel channel = new RandomAccessFile(newLogFile, "rw").getChannel();

            BufferedLogChannel logChannel = new BufferedLogChannel(channel, conf.getWriteBufferBytes(),
                    conf.getReadBufferBytes(), preallocatedLogId, newLogFile, conf.getFlushIntervalInBytes());
            logfileHeader.readerIndex(0);
            logChannel.write(logfileHeader);

            for (File f : list) {
                setLastLogId(f, preallocatedLogId);
            }

            if (suffix.equals(LOG_FILE_SUFFIX)) {
                recentlyCreatedEntryLogsStatus.createdEntryLog(preallocatedLogId);
            }

            LOG.info("Created new entry log file {} for logId {}.", newLogFile, preallocatedLogId);
            return logChannel;
        }

        /**
         * Stop the allocator.
         */
        void stop() {
            // wait until the preallocation finished.
            allocatorExecutor.shutdown();
            LOG.info("Stopped entry logger preallocator.");
        }

        /**
         * get the preallocation for tests.
         */
        Future<BufferedLogChannel> getPreallocationFuture(){
            return preallocation;
        }
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    protected boolean removeEntryLog(long entryLogId) {
        removeFromChannelsAndClose(entryLogId);
        File entryLogFile;
        try {
            entryLogFile = findFile(entryLogId);
        } catch (FileNotFoundException e) {
            LOG.error("Trying to delete an entryLog file that could not be found: "
                    + entryLogId + ".log");
            return false;
        }
        if (!entryLogFile.delete()) {
            LOG.warn("Could not delete entry log file {}", entryLogFile);
        }
        return true;
    }

    /**
     * writes the given id to the "lastId" file in the given directory.
     */
    private void setLastLogId(File dir, long logId) throws IOException {
        FileOutputStream fos;
        fos = new FileOutputStream(new File(dir, "lastId"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
        try {
            bw.write(Long.toHexString(logId) + "\n");
            bw.flush();
        } catch (IOException e) {
            LOG.warn("Failed write lastId file");
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
                LOG.error("Could not close lastId file in {}", dir.getPath());
            }
        }
    }

    private long getLastLogId(File dir) {
        long id = readLastLogId(dir);
        // read success
        if (id > 0) {
            return id;
        }
        // read failed, scan the ledger directories to find biggest log id
        File[] logFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().endsWith(".log");
            }
        });
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
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, UTF_8));
        try {
            String lastIdString = br.readLine();
            return Long.parseLong(lastIdString, 16);
        } catch (IOException e) {
            return INVALID_LID;
        } catch (NumberFormatException e) {
            return INVALID_LID;
        } finally {
            try {
                br.close();
            } catch (IOException e) {
            }
        }
    }

    interface EntryLogManager {

        /*
         * add entry to the corresponding entrylog and return the position of
         * the entry in the entrylog
         */
        long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException;

        /*
         * gets the active logChannel with the given entryLogId. null if it is
         * not existing.
         */
        BufferedLogChannel getCurrentLogIfPresent(long entryLogId);

        /*
         * Returns eligible writable ledger dir for the creation next entrylog
         */
        File getDirForNextEntryLog(List<File> writableLedgerDirs);

        /*
         * Do the operations required for checkpoint.
         */
        void checkpoint() throws IOException;

        /*
         * flush both current and rotated logs.
         */
        void flush() throws IOException;

        /*
         * close current logs.
         */
        void close() throws IOException;

        /*
         * force close current logs.
         */
        void forceClose();

        /*
         *
         */
        void prepareSortedLedgerStorageCheckpoint(long numBytesFlushed) throws IOException;

        /*
         * this method should be called before doing entrymemtable flush, it
         * would save the state of the entrylogger before entrymemtable flush
         * and commitEntryMemTableFlush would take appropriate action after
         * entrymemtable flush.
         */
        void prepareEntryMemTableFlush();

        /*
         * this method should be called after doing entrymemtable flush,it would
         * take appropriate action after entrymemtable flush depending on the
         * current state of the entrylogger and the state of the entrylogger
         * during prepareEntryMemTableFlush.
         *
         * It is assumed that there would be corresponding
         * prepareEntryMemTableFlush for every commitEntryMemTableFlush and both
         * would be called from the same thread.
         *
         * returns boolean value indicating whether EntryMemTable should do checkpoint
         * after this commit method.
         */
        boolean commitEntryMemTableFlush() throws IOException;
    }

    abstract class EntryLogManagerBase implements EntryLogManager {
        volatile List<BufferedLogChannel> rotatedLogChannels;

        private final FastThreadLocal<ByteBuf> sizeBufferForAdd = new FastThreadLocal<ByteBuf>() {
            @Override
            protected ByteBuf initialValue() throws Exception {
                return Unpooled.buffer(4);
            }
        };

        /*
         * This method should be guarded by a lock, so callers of this method
         * should be in the right scope of the lock.
         */
        @Override
        public long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
            int entrySize = entry.readableBytes() + 4; // Adding 4 bytes to prepend the size
            BufferedLogChannel logChannel = getCurrentLogForLedgerForAddEntry(ledger, entrySize, rollLog);
            ByteBuf sizeBuffer = sizeBufferForAdd.get();
            sizeBuffer.clear();
            sizeBuffer.writeInt(entry.readableBytes());
            logChannel.write(sizeBuffer);

            long pos = logChannel.position();
            logChannel.write(entry);
            logChannel.registerWrittenEntry(ledger, entrySize);

            return (logChannel.getLogId() << 32L) | pos;
        }

        boolean reachEntryLogLimit(BufferedLogChannel logChannel, long size) {
            if (logChannel == null) {
                return false;
            }
            return logChannel.position() + size > logSizeLimit;
        }

        boolean readEntryLogHardLimit(BufferedLogChannel logChannel, long size) {
            if (logChannel == null) {
                return false;
            }
            return logChannel.position() + size > Integer.MAX_VALUE;
        }

        abstract BufferedLogChannel getCurrentLogForLedger(long ledgerId);

        abstract BufferedLogChannel getCurrentLogForLedgerForAddEntry(long ledgerId, int entrySize, boolean rollLog)
                throws IOException;

        abstract void setCurrentLogForLedgerAndAddToRotate(long ledgerId, BufferedLogChannel logChannel);

        /*
         * flush current logs.
         */
        abstract void flushCurrentLogs() throws IOException;

        /*
         * flush rotated logs.
         */
        abstract void flushRotatedLogs() throws IOException;

        List<BufferedLogChannel> getRotatedLogChannels() {
            return rotatedLogChannels;
        }

        @Override
        public void flush() throws IOException {
            flushRotatedLogs();
            flushCurrentLogs();
        }

        void flushLogChannel(BufferedLogChannel logChannel, boolean forceMetadata) throws IOException {
            if (logChannel != null) {
                logChannel.flushAndForceWrite(forceMetadata);
                LOG.debug("Flush and sync current entry logger {}", logChannel.getLogId());
            }
        }

        /*
         * Creates a new log file. This method should be guarded by a lock,
         * so callers of this method should be in right scope of the lock.
         */
        void createNewLog(long ledgerId) throws IOException {
            BufferedLogChannel logChannel = getCurrentLogForLedger(ledgerId);
            // first tried to create a new log channel. add current log channel to ToFlush list only when
            // there is a new log channel. it would prevent that a log channel is referenced by both
            // *logChannel* and *ToFlush* list.
            if (null != logChannel) {

                // flush the internal buffer back to filesystem but not sync disk
                logChannel.flush();

                // Append ledgers map at the end of entry log
                logChannel.appendLedgersMap();

                BufferedLogChannel newLogChannel = entryLoggerAllocator.createNewLog();
                setCurrentLogForLedgerAndAddToRotate(ledgerId, newLogChannel);
                LOG.info("Flushing entry logger {} back to filesystem, pending for syncing entry loggers : {}.",
                        logChannel.getLogId(), rotatedLogChannels);
                for (EntryLogListener listener : listeners) {
                    listener.onRotateEntryLog();
                }
            } else {
                setCurrentLogForLedgerAndAddToRotate(ledgerId, entryLoggerAllocator.createNewLog());
            }
        }
    }

    class EntryLogManagerForSingleEntryLog extends EntryLogManagerBase {

        private volatile BufferedLogChannel activeLogChannel;
        private long logIdBeforeFlush = INVALID_LID;
        private final AtomicBoolean shouldCreateNewEntryLog = new AtomicBoolean(false);

        EntryLogManagerForSingleEntryLog(LedgerDirsManager ledgerDirsManager) {
            this.rotatedLogChannels = new LinkedList<BufferedLogChannel>();
            // Register listener for disk full notifications.
            ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        }

        private LedgerDirsListener getLedgerDirsListener() {
            return new LedgerDirsListener() {
                @Override
                public void diskFull(File disk) {
                    // If the current entry log disk is full, then create new
                    // entry log.
                    BufferedLogChannel currentActiveLogChannel = activeLogChannel;
                    if (currentActiveLogChannel != null
                            && currentActiveLogChannel.getLogFile().getParentFile().equals(disk)) {
                        shouldCreateNewEntryLog.set(true);
                    }
                }

                @Override
                public void diskAlmostFull(File disk) {
                    // If the current entry log disk is almost full, then create new entry
                    // log.
                    BufferedLogChannel currentActiveLogChannel = activeLogChannel;
                    if (currentActiveLogChannel != null
                            && currentActiveLogChannel.getLogFile().getParentFile().equals(disk)) {
                        shouldCreateNewEntryLog.set(true);
                    }
                }
            };
        }

        @Override
        public synchronized long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
            return super.addEntry(ledger, entry, rollLog);
        }

        @Override
        synchronized BufferedLogChannel getCurrentLogForLedgerForAddEntry(long ledgerId, int entrySize,
                boolean rollLog) throws IOException {
            if (null == activeLogChannel) {
                // log channel can be null because the file is deferred to be created
                createNewLog(UNASSIGNED_LEDGERID);
            }

            boolean reachEntryLogLimit = rollLog ? reachEntryLogLimit(activeLogChannel, entrySize)
                    : readEntryLogHardLimit(activeLogChannel, entrySize);
            // Create new log if logSizeLimit reached or current disk is full
            boolean createNewLog = shouldCreateNewEntryLog.get();
            if (createNewLog || reachEntryLogLimit) {
                if (activeLogChannel != null) {
                    activeLogChannel.flushAndForceWriteIfRegularFlush(false);
                }
                createNewLog(UNASSIGNED_LEDGERID);
                // Reset the flag
                if (createNewLog) {
                    shouldCreateNewEntryLog.set(false);
                }
            }
            return activeLogChannel;
        }

        @Override
        synchronized void createNewLog(long ledgerId) throws IOException {
            super.createNewLog(ledgerId);
        }

        @Override
        public synchronized void setCurrentLogForLedgerAndAddToRotate(long ledgerId, BufferedLogChannel logChannel) {
            BufferedLogChannel hasToRotateLogChannel = activeLogChannel;
            activeLogChannel = logChannel;
            if (hasToRotateLogChannel != null) {
                rotatedLogChannels.add(hasToRotateLogChannel);
            }
        }

        @Override
        public BufferedLogChannel getCurrentLogForLedger(long ledgerId) {
            return activeLogChannel;
        }

        @Override
        public BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
            BufferedLogChannel activeLogChannelTemp = activeLogChannel;
            if ((activeLogChannelTemp != null) && (activeLogChannelTemp.getLogId() == entryLogId)) {
                return activeLogChannelTemp;
            }
            return null;
        }

        @Override
        public File getDirForNextEntryLog(List<File> writableLedgerDirs) {
            Collections.shuffle(writableLedgerDirs);
            return writableLedgerDirs.get(0);
        }

        @Override
        public void checkpoint() throws IOException {
            flushRotatedLogs();
        }

        public long getCurrentLogId() {
            BufferedLogChannel currentActiveLogChannel = activeLogChannel;
            if (currentActiveLogChannel != null) {
                return currentActiveLogChannel.getLogId();
            } else {
                return EntryLogger.UNINITIALIZED_LOG_ID;
            }
        }

        @Override
        public void flushCurrentLogs() throws IOException {
            BufferedLogChannel currentActiveLogChannel = activeLogChannel;
            if (currentActiveLogChannel != null) {
                /**
                 * flushCurrentLogs method is called during checkpoint, so
                 * metadata of the file also should be force written.
                 */
                flushLogChannel(currentActiveLogChannel, true);
            }
        }

        @Override
        void flushRotatedLogs() throws IOException {
            List<BufferedLogChannel> channels = null;
            synchronized (this) {
                channels = rotatedLogChannels;
                rotatedLogChannels = new LinkedList<BufferedLogChannel>();
            }
            if (null == channels) {
                return;
            }
            Iterator<BufferedLogChannel> chIter = channels.iterator();
            while (chIter.hasNext()) {
                BufferedLogChannel channel = chIter.next();
                try {
                    channel.flushAndForceWrite(true);
                } catch (IOException ioe) {
                    // rescue from flush exception, add unflushed channels back
                    synchronized (this) {
                        if (null == rotatedLogChannels) {
                            rotatedLogChannels = channels;
                        } else {
                            rotatedLogChannels.addAll(0, channels);
                        }
                    }
                    throw ioe;
                }
                // remove the channel from the list after it is successfully flushed
                chIter.remove();
                // since this channel is only used for writing, after flushing the channel,
                // we had to close the underlying file channel. Otherwise, we might end up
                // leaking fds which cause the disk spaces could not be reclaimed.
                closeFileChannel(channel);
                recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(channel.getLogId());
                LOG.info("Synced entry logger {} to disk.", channel.getLogId());
            }
        }

        @Override
        public void close() throws IOException {
            if (activeLogChannel != null) {
                closeFileChannel(activeLogChannel);
            }
        }

        @Override
        public void forceClose() {
            if (activeLogChannel != null) {
                forceCloseFileChannel(activeLogChannel);
            }
        }

        @Override
        public void prepareEntryMemTableFlush() {
            logIdBeforeFlush = getCurrentLogId();
        }

        @Override
        public boolean commitEntryMemTableFlush() throws IOException {
            long logIdAfterFlush = getCurrentLogId();
            /*
             * in any case that an entry log reaches the limit, we roll the log
             * and start checkpointing. if a memory table is flushed spanning
             * over two entry log files, we also roll log. this is for
             * performance consideration: since we don't wanna checkpoint a new
             * log file that ledger storage is writing to.
             */
            if (reachEntryLogLimit(activeLogChannel, 0L) || logIdAfterFlush != logIdBeforeFlush) {
                LOG.info("Rolling entry logger since it reached size limitation");
                createNewLog(UNASSIGNED_LEDGERID);
                return true;
            }
            return false;
        }

        @Override
        public void prepareSortedLedgerStorageCheckpoint(long numBytesFlushed) throws IOException{
            if (numBytesFlushed > 0) {
                // if bytes are added between previous flush and this checkpoint,
                // it means bytes might live at current active entry log, we need
                // roll current entry log and then issue checkpoint to underlying
                // interleaved ledger storage.
                createNewLog(UNASSIGNED_LEDGERID);
            }
        }
    }

    /**
     * Flushes all rotated log channels. After log channels are flushed,
     * move leastUnflushedLogId ptr to current logId.
     */
    void checkpoint() throws IOException {
        entryLogManager.checkpoint();
    }

    public void flush() throws IOException {
        entryLogManager.flush();
    }

    long addEntry(long ledger, ByteBuffer entry) throws IOException {
        return entryLogManager.addEntry(ledger, Unpooled.wrappedBuffer(entry), true);
    }

    long addEntry(long ledger, ByteBuf entry) throws IOException {
        return entryLogManager.addEntry(ledger, entry, true);
    }

    public long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
        return entryLogManager.addEntry(ledger, entry, rollLog);
    }

    private final FastThreadLocal<ByteBuf> sizeBuffer = new FastThreadLocal<ByteBuf>() {
        @Override
        protected ByteBuf initialValue() throws Exception {
            return Unpooled.buffer(4);
        }
    };

    long addEntryForCompaction(long ledgerId, ByteBuf entry) throws IOException {
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

    void flushCompactionLog() throws IOException {
        synchronized (compactionLogLock) {
            if (compactionLogChannel != null) {
                compactionLogChannel.appendLedgersMap();
                compactionLogChannel.flushAndForceWrite(false);
                LOG.info("Flushed compaction log file {} with logId.",
                    compactionLogChannel.getLogFile(),
                    compactionLogChannel.getLogId());
                // since this channel is only used for writing, after flushing the channel,
                // we had to close the underlying file channel. Otherwise, we might end up
                // leaking fds which cause the disk spaces could not be reclaimed.
                closeFileChannel(compactionLogChannel);
            } else {
                throw new IOException("Failed to flush compaction log which has already been removed.");
            }
        }
    }

    void createNewCompactionLog() throws IOException {
        synchronized (compactionLogLock) {
            if (compactionLogChannel == null) {
                compactionLogChannel = entryLoggerAllocator.createNewLogForCompaction();
            }
        }
    }

    /**
     * Remove the current compaction log, usually invoked when compaction failed and
     * we need to do some clean up to remove the compaction log file.
     */
    void removeCurCompactionLog() {
        synchronized (compactionLogLock) {
            if (compactionLogChannel != null) {
                if (!compactionLogChannel.getLogFile().delete()) {
                    LOG.warn("Could not delete compaction log file {}", compactionLogChannel.getLogFile());
                }
                try {
                    closeFileChannel(compactionLogChannel);
                } catch (IOException e) {
                    LOG.error("Failed to close file channel for compaction log {}", compactionLogChannel.getLogId());
                }
                compactionLogChannel = null;
            }
        }
    }

    static long logIdForOffset(long offset) {
        return offset >> 32L;
    }



    public ByteBuf internalReadEntry(long ledgerId, long entryId, long location)
            throws IOException, Bookie.NoEntryException {
        long entryLogId = logIdForOffset(location);
        long pos = location & 0xffffffffL;
        ByteBuf sizeBuff = sizeBuffer.get();
        sizeBuff.clear();
        pos -= 4; // we want to get the ledgerId and length to check
        BufferedReadChannel fc;
        try {
            fc = getChannelForLogId(entryLogId);
        } catch (FileNotFoundException e) {
            FileNotFoundException newe = new FileNotFoundException(e.getMessage() + " for " + ledgerId
                    + " with location " + location);
            newe.setStackTrace(e.getStackTrace());
            throw newe;
        }

        if (readFromLogChannel(entryLogId, fc, sizeBuff, pos) != sizeBuff.capacity()) {
            throw new Bookie.NoEntryException("Short read from entrylog " + entryLogId,
                                              ledgerId, entryId);
        }
        pos += 4;
        int entrySize = sizeBuff.readInt();

        // entrySize does not include the ledgerId
        if (entrySize > maxSaneEntrySize) {
            LOG.warn("Sanity check failed for entry size of " + entrySize + " at location " + pos + " in "
                    + entryLogId);
        }
        if (entrySize < MIN_SANE_ENTRY_SIZE) {
            LOG.error("Read invalid entry length {}", entrySize);
            throw new IOException("Invalid entry length " + entrySize);
        }

        ByteBuf data = PooledByteBufAllocator.DEFAULT.directBuffer(entrySize, entrySize);
        int rc = readFromLogChannel(entryLogId, fc, data, pos);
        if (rc != entrySize) {
            // Note that throwing NoEntryException here instead of IOException is not
            // without risk. If all bookies in a quorum throw this same exception
            // the client will assume that it has reached the end of the ledger.
            // However, this may not be the case, as a very specific error condition
            // could have occurred, where the length of the entry was corrupted on all
            // replicas. However, the chance of this happening is very very low, so
            // returning NoEntryException is mostly safe.
            data.release();
            throw new Bookie.NoEntryException("Short read for " + ledgerId + "@"
                                              + entryId + " in " + entryLogId + "@"
                                              + pos + "(" + rc + "!=" + entrySize + ")", ledgerId, entryId);
        }
        data.writerIndex(entrySize);

        return data;
    }

    public ByteBuf readEntry(long ledgerId, long entryId, long location) throws IOException, Bookie.NoEntryException {
        long entryLogId = logIdForOffset(location);
        long pos = location & 0xffffffffL;

        ByteBuf data = internalReadEntry(ledgerId, entryId, location);
        long thisLedgerId = data.getLong(0);
        if (thisLedgerId != ledgerId) {
            data.release();
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos
                    + " entry belongs to " + thisLedgerId + " not " + ledgerId);
        }
        long thisEntryId = data.getLong(8);
        if (thisEntryId != entryId) {
            data.release();
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos
                    + " entry is " + thisEntryId + " not " + entryId);
        }

        return data;
    }

    /**
     * Read the header of an entry log.
     */
    private Header getHeaderForLogId(long entryLogId) throws IOException {
        BufferedReadChannel bc = getChannelForLogId(entryLogId);

        // Allocate buffer to read (version, ledgersMapOffset, ledgerCount)
        ByteBuf headers = PooledByteBufAllocator.DEFAULT.directBuffer(LOGFILE_HEADER_SIZE);
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
            headers.release();
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
    boolean logExists(long logId) {
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
        ByteBuf data = PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 1024);

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
                pos += 4;
                int entrySize = headerBuffer.readInt();
                long ledgerId = headerBuffer.readLong();
                headerBuffer.clear();

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
            data.release();
        }
    }

    public EntryLogMetadata getEntryLogMetadata(long entryLogId) throws IOException {
        // First try to extract the EntryLogMetada from the index, if there's no index then fallback to scanning the
        // entry log
        try {
            return extractEntryLogMetadataFromIndex(entryLogId);
        } catch (Exception e) {
            LOG.info("Failed to get ledgers map index from: {}.log : {}", entryLogId, e.getMessage());

            // Fall-back to scanning
            return extractEntryLogMetadataByScanning(entryLogId);
        }
    }

    EntryLogMetadata extractEntryLogMetadataFromIndex(long entryLogId) throws IOException {
        Header header = getHeaderForLogId(entryLogId);

        if (header.version < HEADER_V1) {
            throw new IOException("Old log file header without ledgers map on entryLogId " + entryLogId);
        }

        if (header.ledgersMapOffset == 0L) {
            // The index was not stored in the log file (possibly because the bookie crashed before flushing it)
            throw new IOException("No ledgers map index found on entryLogId" + entryLogId);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering ledgers maps for log {} at offset: {}", entryLogId, header.ledgersMapOffset);
        }

        BufferedReadChannel bc = getChannelForLogId(entryLogId);

        // There can be multiple entries containing the various components of the serialized ledgers map
        long offset = header.ledgersMapOffset;
        EntryLogMetadata meta = new EntryLogMetadata(entryLogId);

        final int maxMapSize = LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * LEDGERS_MAP_MAX_BATCH_SIZE;
        ByteBuf ledgersMap = ByteBufAllocator.DEFAULT.directBuffer(maxMapSize);

        try {
            while (offset < bc.size()) {
                // Read ledgers map size
                sizeBuffer.get().clear();
                bc.read(sizeBuffer.get(), offset);

                int ledgersMapSize = sizeBuffer.get().readInt();

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
            ledgersMap.release();
        }

        if (meta.getLedgersMap().size() != header.ledgersCount) {
            throw new IOException("Not all ledgers were found in ledgers map index. expected: " + header.ledgersCount
                    + " -- found: " + meta.getLedgersMap().size() + " -- entryLogId: " + entryLogId);
        }

        return meta;
    }

    private EntryLogMetadata extractEntryLogMetadataByScanning(long entryLogId) throws IOException {
        final EntryLogMetadata meta = new EntryLogMetadata(entryLogId);

        // Read through the entry log file and extract the entry log meta
        scanEntryLog(entryLogId, new EntryLogScanner() {
            @Override
            public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
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
    public void shutdown() {
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
                closeFileChannel(compactionLogChannel);
                compactionLogChannel = null;
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
                forceCloseFileChannel(compactionLogChannel);
            }
        }
        // shutdown the pre-allocation thread
        entryLoggerAllocator.stop();
    }

    private static void closeFileChannel(BufferedChannelBase channel) throws IOException {
        if (null == channel) {
            return;
        }

        FileChannel fileChannel = channel.getFileChannel();
        if (null != fileChannel) {
            fileChannel.close();
        }
    }

    private static void forceCloseFileChannel(BufferedChannelBase channel) {
        if (null == channel) {
            return;
        }
        FileChannel fileChannel = channel.getFileChannel();
        if (null != fileChannel) {
            IOUtils.close(LOG, fileChannel);
        }
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
            entryLogsStatusMap = new TreeMap<Long, Boolean>();
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
    }
}