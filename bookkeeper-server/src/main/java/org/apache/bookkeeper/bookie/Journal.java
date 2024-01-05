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

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.stats.JournalStats;
import org.apache.bookkeeper.common.collections.BatchedArrayBlockingQueue;
import org.apache.bookkeeper.common.collections.BatchedBlockingQueue;
import org.apache.bookkeeper.common.collections.BlockingMpscQueue;
import org.apache.bookkeeper.common.collections.RecyclableArrayList;
import org.apache.bookkeeper.common.util.MemoryLimitController;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieRequestHandler;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide journal related management.
 */
public class Journal extends BookieCriticalThread implements CheckpointSource {

    private static final Logger LOG = LoggerFactory.getLogger(Journal.class);

    private static final RecyclableArrayList.Recycler<QueueEntry> entryListRecycler =
        new RecyclableArrayList.Recycler<QueueEntry>();

    /**
     * Filter to pickup journals.
     */
    public interface JournalIdFilter {
        boolean accept(long journalId);
    }

    /**
     * For testability.
     */
    @FunctionalInterface
    public interface BufferedChannelBuilder {
        BufferedChannelBuilder DEFAULT_BCBUILDER = (FileChannel fc,
                int capacity) -> new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity);

        BufferedChannel create(FileChannel fc, int capacity) throws IOException;
    }



    /**
     * List all journal ids by a specified journal id filer.
     *
     * @param journalDir journal dir
     * @param filter journal id filter
     * @return list of filtered ids
     */
    public static List<Long> listJournalIds(File journalDir, JournalIdFilter filter) {
        File[] logFiles = journalDir.listFiles();
        if (logFiles == null || logFiles.length == 0) {
            return Collections.emptyList();
        }
        List<Long> logs = new ArrayList<Long>();
        for (File f: logFiles) {
            String name = f.getName();
            if (!name.endsWith(".txn")) {
                continue;
            }
            String idString = name.split("\\.")[0];
            long id = Long.parseLong(idString, 16);
            if (filter != null) {
                if (filter.accept(id)) {
                    logs.add(id);
                }
            } else {
                logs.add(id);
            }
        }
        Collections.sort(logs);
        return logs;
    }

    /**
     * A wrapper over log mark to provide a checkpoint for users of journal
     * to do checkpointing.
     */
    private static class LogMarkCheckpoint implements Checkpoint {
        final LastLogMark mark;

        public LogMarkCheckpoint(LastLogMark checkpoint) {
            this.mark = checkpoint;
        }

        @Override
        public int compareTo(Checkpoint o) {
            if (o == Checkpoint.MAX) {
                return -1;
            } else if (o == Checkpoint.MIN) {
                return 1;
            }
            return mark.getCurMark().compare(((LogMarkCheckpoint) o).mark.getCurMark());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LogMarkCheckpoint)) {
                return false;
            }
            return 0 == compareTo((LogMarkCheckpoint) o);
        }

        @Override
        public int hashCode() {
            return mark.hashCode();
        }

        @Override
        public String toString() {
            return mark.toString();
        }
    }

    /**
     * Last Log Mark.
     */
    public class LastLogMark {
        private final LogMark curMark;

        LastLogMark(long logId, long logPosition) {
            this.curMark = new LogMark(logId, logPosition);
        }

        void setCurLogMark(long logId, long logPosition) {
            curMark.setLogMark(logId, logPosition);
        }

        LastLogMark markLog() {
            return new LastLogMark(curMark.getLogFileId(), curMark.getLogFileOffset());
        }

        public LogMark getCurMark() {
            return curMark;
        }

        void rollLog(LastLogMark lastMark) throws NoWritableLedgerDirException {
            byte[] buff = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            lastMark.getCurMark().writeLogMark(bb);

            if (LOG.isDebugEnabled()) {
                LOG.debug("RollLog to persist last marked log : {}", lastMark.getCurMark());
            }

            List<File> writableLedgerDirs = ledgerDirsManager
                    .getWritableLedgerDirsForNewLog();
            for (File dir : writableLedgerDirs) {
                File file = new File(dir, lastMarkFileName);
                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                    fos = null;
                } catch (IOException e) {
                    LOG.error("Problems writing to " + file, e);
                } finally {
                    // if stream already closed in try block successfully,
                    // stream might have nullified, in such case below
                    // call will simply returns
                    IOUtils.close(LOG, fos);
                }
            }
        }

        /**
         * Read last mark from lastMark file.
         * The last mark should first be max journal log id,
         * and then max log position in max journal log.
         */
        public void readLog() {
            byte[] buff = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            LogMark mark = new LogMark();
            for (File dir: ledgerDirsManager.getAllLedgerDirs()) {
                File file = new File(dir, lastMarkFileName);
                try {
                    try (FileInputStream fis = new FileInputStream(file)) {
                        int bytesRead = fis.read(buff);
                        if (bytesRead != 16) {
                            throw new IOException("Couldn't read enough bytes from lastMark."
                                                  + " Wanted " + 16 + ", got " + bytesRead);
                        }
                    }
                    bb.clear();
                    mark.readLogMark(bb);
                    if (curMark.compare(mark) < 0) {
                        curMark.setLogMark(mark.getLogFileId(), mark.getLogFileOffset());
                    }
                } catch (IOException e) {
                    LOG.error("Problems reading from " + file + " (this is okay if it is the first time starting this "
                            + "bookie");
                }
            }
        }

        @Override
        public String toString() {
            return curMark.toString();
        }
    }

    /**
     * Filter to return list of journals for rolling.
     */
    private static class JournalRollingFilter implements JournalIdFilter {

        final LastLogMark lastMark;

        JournalRollingFilter(LastLogMark lastMark) {
            this.lastMark = lastMark;
        }

        @Override
        public boolean accept(long journalId) {
            return journalId < lastMark.getCurMark().getLogFileId();
        }
    }

    /**
     * Scanner used to scan a journal.
     */
    public interface JournalScanner {
        /**
         * Process a journal entry.
         *
         * @param journalVersion Journal Version
         * @param offset File offset of the journal entry
         * @param entry Journal Entry
         * @throws IOException
         */
        void process(int journalVersion, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * Journal Entry to Record.
     */
    static class QueueEntry implements Runnable {
        ByteBuf entry;
        long ledgerId;
        long entryId;
        WriteCallback cb;
        Object ctx;
        long enqueueTime;
        boolean ackBeforeSync;

        OpStatsLogger journalAddEntryStats;
        Counter callbackTime;

        static QueueEntry create(ByteBuf entry, boolean ackBeforeSync, long ledgerId, long entryId,
                WriteCallback cb, Object ctx, long enqueueTime, OpStatsLogger journalAddEntryStats,
                Counter callbackTime) {
            QueueEntry qe = RECYCLER.get();
            qe.entry = entry;
            qe.ackBeforeSync = ackBeforeSync;
            qe.cb = cb;
            qe.ctx = ctx;
            qe.ledgerId = ledgerId;
            qe.entryId = entryId;
            qe.enqueueTime = enqueueTime;
            qe.journalAddEntryStats = journalAddEntryStats;
            qe.callbackTime = callbackTime;
            return qe;
        }

        @Override
        public void run() {
            long startTime = System.nanoTime();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Acknowledge Ledger: {}, Entry: {}", ledgerId, entryId);
            }
            journalAddEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueTime), TimeUnit.NANOSECONDS);
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
            callbackTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            recycle();
        }

        private Object getCtx() {
            return ctx;
        }

        private final Handle<QueueEntry> recyclerHandle;

        private QueueEntry(Handle<QueueEntry> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<QueueEntry> RECYCLER = new Recycler<QueueEntry>() {
            @Override
            protected QueueEntry newObject(Recycler.Handle<QueueEntry> handle) {
                return new QueueEntry(handle);
            }
        };

        private void recycle() {
            this.entry = null;
            this.cb = null;
            this.ctx = null;
            this.journalAddEntryStats = null;
            this.callbackTime = null;
            recyclerHandle.recycle(this);
        }
    }

    /**
     * Token which represents the need to force a write to the Journal.
     */
    @VisibleForTesting
    public static class ForceWriteRequest {
        private JournalChannel logFile;
        private RecyclableArrayList<QueueEntry> forceWriteWaiters;
        private boolean shouldClose;
        private long lastFlushedPosition;
        private long logId;
        private boolean flushed;

        public int process(ObjectHashSet<BookieRequestHandler> writeHandlers) {
            closeFileIfNecessary();

            // Notify the waiters that the force write succeeded
            for (int i = 0; i < forceWriteWaiters.size(); i++) {
                QueueEntry qe = forceWriteWaiters.get(i);
                if (qe != null) {
                    if (qe.getCtx() instanceof BookieRequestHandler
                            && qe.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                        writeHandlers.add((BookieRequestHandler) qe.getCtx());
                    }
                    qe.run();
                }
            }

            return forceWriteWaiters.size();
        }

        private void flushFileToDisk() throws IOException {
            if (!flushed) {
                logFile.forceWrite(false);
                flushed = true;
            }
        }

        public void closeFileIfNecessary() {
            // Close if shouldClose is set
            if (shouldClose) {
                // We should guard against exceptions so its
                // safe to call in catch blocks
                try {
                    flushFileToDisk();
                    logFile.close();
                    // Call close only once
                    shouldClose = false;
                } catch (IOException ioe) {
                    LOG.error("I/O exception while closing file", ioe);
                }
            }
        }

        private final Handle<ForceWriteRequest> recyclerHandle;

        private ForceWriteRequest(Handle<ForceWriteRequest> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private void recycle() {
            logFile = null;
            flushed = false;
            if (forceWriteWaiters != null) {
                forceWriteWaiters.recycle();
                forceWriteWaiters = null;
            }
            recyclerHandle.recycle(this);
        }
    }

    private ForceWriteRequest createForceWriteRequest(JournalChannel logFile,
                          long logId,
                          long lastFlushedPosition,
                          RecyclableArrayList<QueueEntry> forceWriteWaiters,
                          boolean shouldClose) {
        ForceWriteRequest req = forceWriteRequestsRecycler.get();
        req.forceWriteWaiters = forceWriteWaiters;
        req.logFile = logFile;
        req.logId = logId;
        req.lastFlushedPosition = lastFlushedPosition;
        req.shouldClose = shouldClose;
        journalStats.getForceWriteQueueSize().inc();
        return req;
    }

    private static final Recycler<ForceWriteRequest> forceWriteRequestsRecycler = new Recycler<ForceWriteRequest>() {
                @Override
                protected ForceWriteRequest newObject(
                        Recycler.Handle<ForceWriteRequest> handle) {
                    return new ForceWriteRequest(handle);
                }
            };

    /**
     * ForceWriteThread is a background thread which makes the journal durable periodically.
     *
     */
    private class ForceWriteThread extends BookieCriticalThread {
        volatile boolean running = true;
        // This holds the queue entries that should be notified after a
        // successful force write
        Thread threadToNotifyOnEx;

        // should we group force writes
        private final boolean enableGroupForceWrites;
        private final Counter forceWriteThreadTime;

        public ForceWriteThread(Thread threadToNotifyOnEx,
                                boolean enableGroupForceWrites,
                                StatsLogger statsLogger) {
            super("ForceWriteThread");
            this.threadToNotifyOnEx = threadToNotifyOnEx;
            this.enableGroupForceWrites = enableGroupForceWrites;
            this.forceWriteThreadTime = statsLogger.getThreadScopedCounter("force-write-thread-time");
        }
        @Override
        public void run() {
            LOG.info("ForceWrite Thread started");
            ThreadRegistry.register(super.getName(), 0);

            if (conf.isBusyWaitEnabled()) {
                try {
                    CpuAffinity.acquireCore();
                } catch (Exception e) {
                    LOG.warn("Unable to acquire CPU core for Journal ForceWrite thread: {}", e.getMessage(), e);
                }
            }

            final ObjectHashSet<BookieRequestHandler> writeHandlers = new ObjectHashSet<>();
            final ForceWriteRequest[] localRequests = new ForceWriteRequest[conf.getJournalQueueSize()];

            while (running) {
                try {
                    int numEntriesInLastForceWrite = 0;

                    int requestsCount = forceWriteRequests.takeAll(localRequests);

                    journalStats.getForceWriteQueueSize().addCount(-requestsCount);

                    // Sync and mark the journal up to the position of the last entry in the batch
                    ForceWriteRequest lastRequest = localRequests[requestsCount - 1];
                    syncJournal(lastRequest);

                    // All the requests in the batch are now fully-synced. We can trigger sending the
                    // responses
                    for (int i = 0; i < requestsCount; i++) {
                        ForceWriteRequest req = localRequests[i];
                        numEntriesInLastForceWrite += req.process(writeHandlers);
                        localRequests[i] = null;
                        req.recycle();
                    }

                    journalStats.getForceWriteGroupingCountStats()
                            .registerSuccessfulValue(numEntriesInLastForceWrite);
                    writeHandlers.forEach(
                            (ObjectProcedure<? super BookieRequestHandler>)
                                    BookieRequestHandler::flushPendingResponse);
                    writeHandlers.clear();
                } catch (IOException ioe) {
                    LOG.error("I/O exception in ForceWrite thread", ioe);
                    running = false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("ForceWrite thread interrupted");
                    running = false;
                }
            }
            // Regardless of what caused us to exit, we should notify the
            // the parent thread as it should either exit or be in the process
            // of exiting else we will have write requests hang
            threadToNotifyOnEx.interrupt();
        }

        private void syncJournal(ForceWriteRequest lastRequest) throws IOException {
            long fsyncStartTime = MathUtils.nowInNano();
            try {
                lastRequest.flushFileToDisk();
                journalStats.getJournalSyncStats().registerSuccessfulEvent(MathUtils.elapsedNanos(fsyncStartTime),
                        TimeUnit.NANOSECONDS);
                lastLogMark.setCurLogMark(lastRequest.logId, lastRequest.lastFlushedPosition);
            } catch (IOException ioe) {
                journalStats.getJournalSyncStats()
                        .registerFailedEvent(MathUtils.elapsedNanos(fsyncStartTime), TimeUnit.NANOSECONDS);
                throw ioe;
            }
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }
    }

    static final int PADDING_MASK = -0x100;

    static void writePaddingBytes(JournalChannel jc, ByteBuf paddingBuffer, int journalAlignSize)
            throws IOException {
        int bytesToAlign = (int) (jc.bc.position() % journalAlignSize);
        if (0 != bytesToAlign) {
            int paddingBytes = journalAlignSize - bytesToAlign;
            if (paddingBytes < 8) {
                paddingBytes = journalAlignSize - (8 - paddingBytes);
            } else {
                paddingBytes -= 8;
            }
            paddingBuffer.clear();
            // padding mask
            paddingBuffer.writeInt(PADDING_MASK);
            // padding len
            paddingBuffer.writeInt(paddingBytes);
            // padding bytes
            paddingBuffer.writerIndex(paddingBuffer.writerIndex() + paddingBytes);

            jc.preAllocIfNeeded(paddingBuffer.readableBytes());
            // write padding bytes
            jc.bc.write(paddingBuffer);
        }
    }

    static final long MB = 1024 * 1024L;
    static final int KB = 1024;
    // max journal file size
    final long maxJournalSize;
    // pre-allocation size for the journal files
    final long journalPreAllocSize;
    // write buffer size for the journal files
    final int journalWriteBufferSize;
    // number journal files kept before marked journal
    final int maxBackupJournals;

    final File journalDirectory;
    final ServerConfiguration conf;
    final ForceWriteThread forceWriteThread;
    final FileChannelProvider fileChannelProvider;

    // Time after which we will stop grouping and issue the flush
    private final long maxGroupWaitInNanos;
    // Threshold after which we flush any buffered journal entries
    private final long bufferedEntriesThreshold;
    // Threshold after which we flush any buffered journal writes
    private final long bufferedWritesThreshold;
    // should we flush if the queue is empty
    private final boolean flushWhenQueueEmpty;
    // should we hint the filesystem to remove pages from cache after force write
    private final boolean removePagesFromCache;
    private final int journalFormatVersionToWrite;
    private final int journalAlignmentSize;
    // control PageCache flush interval when syncData disabled to reduce disk io util
    private final long journalPageCacheFlushIntervalMSec;
    // Whether reuse journal files, it will use maxBackupJournal as the journal file pool.
    private final boolean journalReuseFiles;

    // Should data be fsynced on disk before triggering the callback
    private final boolean syncData;

    private final LastLogMark lastLogMark = new LastLogMark(0, 0);

    private static final String LAST_MARK_DEFAULT_NAME = "lastMark";

    private final String lastMarkFileName;

    private final Counter callbackTime;
    private final Counter journalTime;
    private static final String journalThreadName = "BookieJournal";

    // journal entry queue to commit
    final BatchedBlockingQueue<QueueEntry> queue;
    final BatchedBlockingQueue<ForceWriteRequest> forceWriteRequests;

    volatile boolean running = true;
    private final LedgerDirsManager ledgerDirsManager;
    private final ByteBufAllocator allocator;
    private final MemoryLimitController memoryLimitController;

    // Expose Stats
    private final JournalStats journalStats;

    private JournalAliveListener journalAliveListener;

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, NullStatsLogger.INSTANCE,
                UnpooledByteBufAllocator.DEFAULT);
    }

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger, ByteBufAllocator allocator) {
        super(journalThreadName + "-" + conf.getBookiePort());
        this.allocator = allocator;

        StatsLogger journalStatsLogger = statsLogger.scopeLabel("journalIndex", String.valueOf(journalIndex));

        if (conf.isBusyWaitEnabled()) {
            // To achieve lower latency, use busy-wait blocking queue implementation
            queue = new BlockingMpscQueue<>(conf.getJournalQueueSize());
            forceWriteRequests = new BlockingMpscQueue<>(conf.getJournalQueueSize());
        } else {
            queue = new BatchedArrayBlockingQueue<>(conf.getJournalQueueSize());
            forceWriteRequests = new BatchedArrayBlockingQueue<>(conf.getJournalQueueSize());
        }

        // Adjust the journal max memory in case there are multiple journals configured.
        long journalMaxMemory = conf.getJournalMaxMemorySizeMb() / conf.getJournalDirNames().length * 1024 * 1024;
        this.memoryLimitController = new MemoryLimitController(journalMaxMemory);
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        this.journalDirectory = journalDirectory;
        this.maxJournalSize = conf.getMaxJournalSizeMB() * MB;
        this.journalPreAllocSize = conf.getJournalPreAllocSizeMB() * MB;
        this.journalWriteBufferSize = conf.getJournalWriteBufferSizeKB() * KB;
        this.syncData = conf.getJournalSyncData();
        this.maxBackupJournals = conf.getMaxBackupJournals();
        this.forceWriteThread = new ForceWriteThread(this, conf.getJournalAdaptiveGroupWrites(),
                journalStatsLogger);
        this.maxGroupWaitInNanos = TimeUnit.MILLISECONDS.toNanos(conf.getJournalMaxGroupWaitMSec());
        this.bufferedWritesThreshold = conf.getJournalBufferedWritesThreshold();
        this.bufferedEntriesThreshold = conf.getJournalBufferedEntriesThreshold();
        this.journalFormatVersionToWrite = conf.getJournalFormatVersionToWrite();
        this.journalAlignmentSize = conf.getJournalAlignmentSize();
        this.journalPageCacheFlushIntervalMSec = conf.getJournalPageCacheFlushIntervalMSec();
        this.journalReuseFiles = conf.getJournalReuseFiles();
        this.callbackTime = journalStatsLogger.getThreadScopedCounter("callback-time");

        this.journalTime = journalStatsLogger.getThreadScopedCounter("journal-thread-time");

        // Unless there is a cap on the max wait (which requires group force writes)
        // we cannot skip flushing for queue empty
        this.flushWhenQueueEmpty = maxGroupWaitInNanos <= 0 || conf.getJournalFlushWhenQueueEmpty();

        this.removePagesFromCache = conf.getJournalRemovePagesFromCache();
        // read last log mark
        if (conf.getJournalDirs().length == 1) {
            lastMarkFileName = LAST_MARK_DEFAULT_NAME;
        } else {
            lastMarkFileName = LAST_MARK_DEFAULT_NAME + "." + journalIndex;
        }
        lastLogMark.readLog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Last Log Mark : {}", lastLogMark.getCurMark());
        }

        try {
            this.fileChannelProvider = FileChannelProvider.newProvider(conf.getJournalChannelProvider());
        } catch (IOException e) {
            LOG.error("Failed to initiate file channel provider: {}", conf.getJournalChannelProvider());
            throw new RuntimeException(e);
        }

        // Expose Stats
        this.journalStats = new JournalStats(journalStatsLogger, journalMaxMemory,
                () -> memoryLimitController.currentUsage());
    }

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
                   LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger,
                   ByteBufAllocator allocator, JournalAliveListener journalAliveListener) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator);
        this.journalAliveListener = journalAliveListener;
    }

    JournalStats getJournalStats() {
        return this.journalStats;
    }

    public File getJournalDirectory() {
        return journalDirectory;
    }

    public LastLogMark getLastLogMark() {
        return lastLogMark;
    }

    /**
     * Update lastLogMark of the journal
     * Indicates that the file has been processed.
     * @param id
     * @param scanOffset
     */
    void setLastLogMark(Long id, long scanOffset) {
        lastLogMark.setCurLogMark(id, scanOffset);
    }

    /**
     * Application tried to schedule a checkpoint. After all the txns added
     * before checkpoint are persisted, a <i>checkpoint</i> will be returned
     * to application. Application could use <i>checkpoint</i> to do its logic.
     */
    @Override
    public Checkpoint newCheckpoint() {
        return new LogMarkCheckpoint(lastLogMark.markLog());
    }

    /**
     * Telling journal a checkpoint is finished.
     *
     * @throws IOException
     */
    @Override
    public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        if (!(checkpoint instanceof LogMarkCheckpoint)) {
            return; // we didn't create this checkpoint, so dont do anything with it
        }
        LogMarkCheckpoint lmcheckpoint = (LogMarkCheckpoint) checkpoint;
        LastLogMark mark = lmcheckpoint.mark;

        mark.rollLog(mark);
        if (compact) {
            // list the journals that have been marked
            List<Long> logs = listJournalIds(journalDirectory, new JournalRollingFilter(mark));
            // keep MAX_BACKUP_JOURNALS journal files before marked journal
            if (logs.size() >= maxBackupJournals) {
                int maxIdx = logs.size() - maxBackupJournals;
                for (int i = 0; i < maxIdx; i++) {
                    long id = logs.get(i);
                    // make sure the journal id is smaller than marked journal id
                    if (id < mark.getCurMark().getLogFileId()) {
                        File journalFile = new File(journalDirectory, Long.toHexString(id) + ".txn");
                        if (!journalFile.delete()) {
                            LOG.warn("Could not delete old journal file {}", journalFile);
                        }
                        LOG.info("garbage collected journal " + journalFile.getName());
                    }
                }
            }
        }
    }

    /**
     * Scan the journal.
     *
     * @param journalId         Journal Log Id
     * @param journalPos        Offset to start scanning
     * @param scanner           Scanner to handle entries
     * @param skipInvalidRecord when invalid record,should we skip it or not
     * @return scanOffset - represents the byte till which journal was read
     * @throws IOException
     */
    public long scanJournal(long journalId, long journalPos, JournalScanner scanner, boolean skipInvalidRecord)
        throws IOException {
        JournalChannel recLog;
        if (journalPos <= 0) {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize,
                conf, fileChannelProvider);
        } else {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize,
                    journalPos, conf, fileChannelProvider);
        }
        int journalVersion = recLog.getFormatVersion();
        try {
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            ByteBuffer recBuff = ByteBuffer.allocate(64 * 1024);
            while (true) {
                // entry start offset
                long offset = recLog.fc.position();
                // start reading entry
                lenBuff.clear();
                fullRead(recLog, lenBuff);
                if (lenBuff.remaining() != 0) {
                    break;
                }
                lenBuff.flip();
                int len = lenBuff.getInt();
                if (len == 0) {
                    break;
                }
                boolean isPaddingRecord = false;
                if (len < 0) {
                    if (len == PADDING_MASK && journalVersion >= JournalChannel.V5) {
                        // skip padding bytes
                        lenBuff.clear();
                        fullRead(recLog, lenBuff);
                        if (lenBuff.remaining() != 0) {
                            break;
                        }
                        lenBuff.flip();
                        len = lenBuff.getInt();
                        if (len == 0) {
                            continue;
                        }
                        isPaddingRecord = true;
                    } else {
                        LOG.error("Invalid record found with negative length: {}", len);
                        throw new IOException("Invalid record found with negative length " + len);
                    }
                }
                recBuff.clear();
                if (recBuff.remaining() < len) {
                    recBuff = ByteBuffer.allocate(len);
                }
                recBuff.limit(len);
                if (fullRead(recLog, recBuff) != len) {
                    // This seems scary, but it just means that this is where we
                    // left off writing
                    break;
                }
                recBuff.flip();
                if (!isPaddingRecord) {
                    scanner.process(journalVersion, offset, recBuff);
                }
            }
            return recLog.fc.position();
        } catch (IOException e) {
            if (skipInvalidRecord) {
                LOG.warn("Failed to parse journal file, and skipInvalidRecord is true, skip this journal file reply");
            } else {
                throw e;
            }
            return recLog.fc.position();
        } finally {
            recLog.close();
        }
    }

    /**
     * record an add entry operation in journal.
     */
    public void logAddEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        logAddEntry(ledgerId, entryId, entry, ackBeforeSync, cb, ctx);
    }

    @VisibleForTesting
    public void logAddEntry(long ledgerId, long entryId, ByteBuf entry,
                            boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        // Retain entry until it gets written to journal
        entry.retain();

        journalStats.getJournalQueueSize().inc();

        memoryLimitController.reserveMemory(entry.readableBytes());

        queue.put(QueueEntry.create(
                entry, ackBeforeSync, ledgerId, entryId, cb, ctx, MathUtils.nowInNano(),
                journalStats.getJournalAddEntryStats(),
                callbackTime));
    }

    void forceLedger(long ledgerId, WriteCallback cb, Object ctx) {
        queue.add(QueueEntry.create(
                null, false /* ackBeforeSync */, ledgerId,
                BookieImpl.METAENTRY_ID_FORCE_LEDGER, cb, ctx, MathUtils.nowInNano(),
                journalStats.getJournalForceLedgerStats(),
                callbackTime));
        // Increment afterwards because the add operation could fail.
        journalStats.getJournalQueueSize().inc();
    }

    /**
     * Get the length of journal entries queue.
     *
     * @return length of journal entry queue.
     */
    public int getJournalQueueLength() {
        return queue.size();
    }

    /**
     * A thread used for persisting journal entries to journal files.
     *
     * <p>
     * Besides persisting journal entries, it also takes responsibility of
     * rolling journal files when a journal file reaches journal file size
     * limitation.
     * </p>
     * <p>
     * During journal rolling, it first closes the writing journal, generates
     * new journal file using current timestamp, and continue persistence logic.
     * Those journals will be garbage collected in SyncThread.
     * </p>
     * @see org.apache.bookkeeper.bookie.SyncThread
     */
    @Override
    public void run() {
        LOG.info("Starting journal on {}", journalDirectory);
        ThreadRegistry.register(journalThreadName, 0);

        if (conf.isBusyWaitEnabled()) {
            try {
                CpuAffinity.acquireCore();
            } catch (Exception e) {
                LOG.warn("Unable to acquire CPU core for Journal thread: {}", e.getMessage(), e);
            }
        }

        RecyclableArrayList<QueueEntry> toFlush = entryListRecycler.newInstance();
        int numEntriesToFlush = 0;
        ByteBuf lenBuff = Unpooled.buffer(4);
        ByteBuf paddingBuff = Unpooled.buffer(2 * conf.getJournalAlignmentSize());
        paddingBuff.writeZero(paddingBuff.capacity());

        BufferedChannel bc = null;
        JournalChannel logFile = null;
        forceWriteThread.start();
        Stopwatch journalCreationWatcher = Stopwatch.createUnstarted();
        Stopwatch journalFlushWatcher = Stopwatch.createUnstarted();
        long batchSize = 0;
        try {
            List<Long> journalIds = listJournalIds(journalDirectory, null);
            // Should not use MathUtils.now(), which use System.nanoTime() and
            // could only be used to measure elapsed time.
            // http://docs.oracle.com/javase/1.5.0/docs/api/java/lang/System.html#nanoTime%28%29
            long logId = journalIds.isEmpty() ? System.currentTimeMillis() : journalIds.get(journalIds.size() - 1);
            long lastFlushPosition = 0;
            boolean groupWhenTimeout = false;

            long dequeueStartTime = 0L;
            long lastFlushTimeMs = System.currentTimeMillis();

            final ObjectHashSet<BookieRequestHandler> writeHandlers = new ObjectHashSet<>();
            QueueEntry[] localQueueEntries = new QueueEntry[conf.getJournalQueueSize()];
            int localQueueEntriesIdx = 0;
            int localQueueEntriesLen = 0;
            QueueEntry qe = null;
            while (true) {
                // new journal file to write
                if (null == logFile) {
                    logId = logId + 1;
                    journalIds = listJournalIds(journalDirectory, null);
                    Long replaceLogId = fileChannelProvider.supportReuseFile() && journalReuseFiles
                        && journalIds.size() >= maxBackupJournals
                        && journalIds.get(0) < lastLogMark.getCurMark().getLogFileId()
                        ? journalIds.get(0) : null;

                    journalCreationWatcher.reset().start();
                    logFile = new JournalChannel(journalDirectory, logId, journalPreAllocSize, journalWriteBufferSize,
                                        journalAlignmentSize, removePagesFromCache,
                                        journalFormatVersionToWrite, getBufferedChannelBuilder(),
                                        conf, fileChannelProvider, replaceLogId);

                    journalStats.getJournalCreationStats().registerSuccessfulEvent(
                            journalCreationWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = bc.position();
                }

                if (qe == null) {
                    if (dequeueStartTime != 0) {
                        journalStats.getJournalProcessTimeStats()
                            .registerSuccessfulEvent(MathUtils.elapsedNanos(dequeueStartTime), TimeUnit.NANOSECONDS);
                    }

                    // At this point the local queue will always be empty, otherwise we would have
                    // advanced to the next `qe` at the end of the loop
                    localQueueEntriesIdx = 0;
                    if (numEntriesToFlush == 0) {
                        // There are no entries pending. We can wait indefinitely until the next
                        // one is available
                        localQueueEntriesLen = queue.takeAll(localQueueEntries);
                    } else {
                        // There are already some entries pending. We must adjust
                        // the waiting time to the remaining groupWait time
                        long pollWaitTimeNanos = maxGroupWaitInNanos
                                - MathUtils.elapsedNanos(toFlush.get(0).enqueueTime);
                        if (flushWhenQueueEmpty || pollWaitTimeNanos < 0) {
                            pollWaitTimeNanos = 0;
                        }

                        localQueueEntriesLen = queue.pollAll(localQueueEntries,
                                pollWaitTimeNanos, TimeUnit.NANOSECONDS);
                    }

                    dequeueStartTime = MathUtils.nowInNano();

                    if (localQueueEntriesLen > 0) {
                        qe = localQueueEntries[localQueueEntriesIdx];
                        localQueueEntries[localQueueEntriesIdx++] = null;
                    }
                }

                if (numEntriesToFlush > 0) {
                    boolean shouldFlush = false;
                    // We should issue a forceWrite if any of the three conditions below holds good
                    // 1. If the oldest pending entry has been pending for longer than the max wait time
                    if (maxGroupWaitInNanos > 0 && !groupWhenTimeout && (MathUtils
                            .elapsedNanos(toFlush.get(0).enqueueTime) > maxGroupWaitInNanos)) {
                        groupWhenTimeout = true;
                    } else if (maxGroupWaitInNanos > 0 && groupWhenTimeout
                        && (qe == null // no entry to group
                            || MathUtils.elapsedNanos(qe.enqueueTime) < maxGroupWaitInNanos)) {
                        // when group timeout, it would be better to look forward, as there might be lots of
                        // entries already timeout
                        // due to a previous slow write (writing to filesystem which impacted by force write).
                        // Group those entries in the queue
                        // a) already timeout
                        // b) limit the number of entries to group
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushMaxWaitCounter().inc();
                    } else if (qe != null
                            && ((bufferedEntriesThreshold > 0 && toFlush.size() > bufferedEntriesThreshold)
                            || (bc.position() > lastFlushPosition + bufferedWritesThreshold))) {
                        // 2. If we have buffered more than the buffWriteThreshold or bufferedEntriesThreshold
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushMaxOutstandingBytesCounter().inc();
                    } else if (qe == null && flushWhenQueueEmpty) {
                        // We should get here only if we flushWhenQueueEmpty is true else we would wait
                        // for timeout that would put is past the maxWait threshold
                        // 3. If the queue is empty i.e. no benefit of grouping. This happens when we have one
                        // publish at a time - common case in tests.
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushEmptyQueueCounter().inc();
                    }

                    // toFlush is non null and not empty so should be safe to access getFirst
                    if (shouldFlush) {
                        if (journalFormatVersionToWrite >= JournalChannel.V5) {
                            writePaddingBytes(logFile, paddingBuff, journalAlignmentSize);
                        }
                        journalFlushWatcher.reset().start();
                        bc.flush();

                        for (int i = 0; i < toFlush.size(); i++) {
                            QueueEntry entry = toFlush.get(i);
                            if (entry != null && (!syncData || entry.ackBeforeSync)) {
                                toFlush.set(i, null);
                                numEntriesToFlush--;
                                if (entry.getCtx() instanceof BookieRequestHandler
                                        && entry.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                                    writeHandlers.add((BookieRequestHandler) entry.getCtx());
                                }
                                entry.run();
                            }
                        }
                        writeHandlers.forEach(
                                (ObjectProcedure<? super BookieRequestHandler>)
                                        BookieRequestHandler::flushPendingResponse);
                        writeHandlers.clear();

                        lastFlushPosition = bc.position();
                        journalStats.getJournalFlushStats().registerSuccessfulEvent(
                                journalFlushWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                        // Trace the lifetime of entries through persistence
                        if (LOG.isDebugEnabled()) {
                            for (QueueEntry e : toFlush) {
                                if (e != null && LOG.isDebugEnabled()) {
                                    LOG.debug("Written and queuing for flush Ledger: {}  Entry: {}",
                                              e.ledgerId, e.entryId);
                                }
                            }
                        }

                        journalStats.getForceWriteBatchEntriesStats()
                            .registerSuccessfulValue(numEntriesToFlush);
                        journalStats.getForceWriteBatchBytesStats()
                            .registerSuccessfulValue(batchSize);
                        boolean shouldRolloverJournal = (lastFlushPosition > maxJournalSize);
                        // Trigger data sync to disk in the "Force-Write" thread.
                        // Trigger data sync to disk has three situations:
                        // 1. journalSyncData enabled, usually for SSD used as journal storage
                        // 2. shouldRolloverJournal is true, that is the journal file reaches maxJournalSize
                        // 3. if journalSyncData disabled and shouldRolloverJournal is false, we can use
                        //   journalPageCacheFlushIntervalMSec to control sync frequency, preventing disk
                        //   synchronize frequently, which will increase disk io util.
                        //   when flush interval reaches journalPageCacheFlushIntervalMSec (default: 1s),
                        //   it will trigger data sync to disk
                        if (syncData
                                || shouldRolloverJournal
                                || (System.currentTimeMillis() - lastFlushTimeMs
                                >= journalPageCacheFlushIntervalMSec)) {
                            forceWriteRequests.put(createForceWriteRequest(logFile, logId, lastFlushPosition,
                                    toFlush, shouldRolloverJournal));
                            lastFlushTimeMs = System.currentTimeMillis();
                        }
                        toFlush = entryListRecycler.newInstance();
                        numEntriesToFlush = 0;

                        batchSize = 0L;
                        // check whether journal file is over file limit
                        if (shouldRolloverJournal) {
                            // if the journal file is rolled over, the journal file will be closed after last
                            // entry is force written to disk.
                            logFile = null;
                            continue;
                        }
                    }
                }

                if (!running) {
                    LOG.info("Journal Manager is asked to shut down, quit.");
                    break;
                }

                if (qe == null) { // no more queue entry
                    continue;
                }

                journalStats.getJournalQueueSize().dec();
                journalStats.getJournalQueueStats()
                        .registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime), TimeUnit.NANOSECONDS);

                if ((qe.entryId == BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC)
                        && (journalFormatVersionToWrite < JournalChannel.V6)) {
                    /*
                     * this means we are using new code which supports
                     * persisting explicitLac, but "journalFormatVersionToWrite"
                     * is set to some older value (< V6). In this case we
                     * shouldn't write this special entry
                     * (METAENTRY_ID_LEDGER_EXPLICITLAC) to Journal.
                     */
                    memoryLimitController.releaseMemory(qe.entry.readableBytes());
                    ReferenceCountUtil.release(qe.entry);
                } else if (qe.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                    int entrySize = qe.entry.readableBytes();
                    journalStats.getJournalWriteBytes().addCount(entrySize);

                    batchSize += (4 + entrySize);

                    lenBuff.clear();
                    lenBuff.writeInt(entrySize);

                    // preAlloc based on size
                    logFile.preAllocIfNeeded(4 + entrySize);

                    bc.write(lenBuff);
                    bc.write(qe.entry);
                    memoryLimitController.releaseMemory(qe.entry.readableBytes());
                    ReferenceCountUtil.release(qe.entry);
                }

                toFlush.add(qe);
                numEntriesToFlush++;

                if (localQueueEntriesIdx < localQueueEntriesLen) {
                    qe = localQueueEntries[localQueueEntriesIdx];
                    localQueueEntries[localQueueEntriesIdx++] = null;
                } else {
                    qe = null;
                }
            }
        } catch (IOException ioe) {
            LOG.error("I/O exception in Journal thread!", ioe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("Journal exits when shutting down");
        } finally {
            // There could be packets queued for forceWrite on this logFile
            // That is fine as this exception is going to anyway take down
            // the bookie. If we execute this as a part of graceful shutdown,
            // close will flush the file system cache making any previous
            // cached writes durable so this is fine as well.
            IOUtils.close(LOG, bc);
            if (journalAliveListener != null) {
                journalAliveListener.onJournalExit();
            }
        }
        LOG.info("Journal exited loop!");
    }

    public BufferedChannelBuilder getBufferedChannelBuilder() {
        return (FileChannel fc, int capacity) -> new BufferedChannel(allocator, fc, capacity);
    }

    /**
     * Shuts down the journal.
     */
    public synchronized void shutdown() {
        try {
            if (!running) {
                return;
            }
            LOG.info("Shutting down Journal");
            if (fileChannelProvider != null) {
                fileChannelProvider.close();
            }

            forceWriteThread.shutdown();

            running = false;
            this.interrupt();
            this.join();
            LOG.info("Finished Shutting down Journal thread");
        } catch (IOException | InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted during shutting down journal : ", ie);
        }
    }

    private static int fullRead(JournalChannel fc, ByteBuffer bb) throws IOException {
        int total = 0;
        while (bb.remaining() > 0) {
            int rc = fc.read(bb);
            if (rc <= 0) {
                return total;
            }
            total += rc;
        }
        return total;
    }

    /**
     * Wait for the Journal thread to exit.
     * This is method is needed in order to mock the journal, we can't mock final method of java.lang.Thread class
     *
     * @throws InterruptedException
     */
    @VisibleForTesting
    public void joinThread() throws InterruptedException {
        join();
    }

    long getMemoryUsage() {
        return memoryLimitController.currentUsage();
    }
}
