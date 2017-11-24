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

import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DaemonThreadFactory;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ZeroBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide journal related management.
 */
public class Journal extends BookieCriticalThread implements CheckpointSource {

    private static final Logger LOG = LoggerFactory.getLogger(Journal.class);

    /**
     * Filter to pickup journals.
     */
    private interface JournalIdFilter {
        boolean accept(long journalId);
    }

    /**
     * List all journal ids by a specified journal id filer.
     *
     * @param journalDir journal dir
     * @param filter journal id filter
     * @return list of filtered ids
     */
    private static List<Long> listJournalIds(File journalDir, JournalIdFilter filter) {
        File logFiles[] = journalDir.listFiles();
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
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            lastMark.getCurMark().writeLogMark(bb);
            if (LOG.isDebugEnabled()) {
                LOG.debug("RollLog to persist last marked log : {}", lastMark.getCurMark());
            }
            List<File> writableLedgerDirs = ledgerDirsManager
                    .getWritableLedgerDirs();
            for (File dir : writableLedgerDirs) {
                File file = new File(dir, "lastMark");
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
        void readLog() {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            LogMark mark = new LogMark();
            for (File dir: ledgerDirsManager.getAllLedgerDirs()) {
                File file = new File(dir, "lastMark");
                try {
                    FileInputStream fis = new FileInputStream(file);
                    try {
                        int bytesRead = fis.read(buff);
                        if (bytesRead != 16) {
                            throw new IOException("Couldn't read enough bytes from lastMark."
                                                  + " Wanted " + 16 + ", got " + bytesRead);
                        }
                    } finally {
                        fis.close();
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
            if (journalId < lastMark.getCurMark().getLogFileId()) {
                return true;
            } else {
                return false;
            }
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
    private class QueueEntry implements Runnable {
        ByteBuf entry;
        long ledgerId;
        long entryId;
        WriteCallback cb;
        Object ctx;
        long enqueueTime;

        QueueEntry(ByteBuf entry, long ledgerId, long entryId, WriteCallback cb, Object ctx, long enqueueTime) {
            this.entry = entry.duplicate();
            this.cb = cb;
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.enqueueTime = enqueueTime;
        }

        @Override
        public void run() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Acknowledge Ledger: {}, Entry: {}", ledgerId, entryId);
            }
            journalAddEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueTime), TimeUnit.NANOSECONDS);
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
        }
    }

    private class ForceWriteRequest {
        private final JournalChannel logFile;
        private final LinkedList<QueueEntry> forceWriteWaiters;
        private boolean shouldClose;
        private final boolean isMarker;
        private final long lastFlushedPosition;
        private final long logId;

        private ForceWriteRequest(JournalChannel logFile,
                          long logId,
                          long lastFlushedPosition,
                          LinkedList<QueueEntry> forceWriteWaiters,
                          boolean shouldClose,
                          boolean isMarker) {
            this.forceWriteWaiters = forceWriteWaiters;
            this.logFile = logFile;
            this.logId = logId;
            this.lastFlushedPosition = lastFlushedPosition;
            this.shouldClose = shouldClose;
            this.isMarker = isMarker;
            forceWriteQueueSize.inc();
        }

        public int process(boolean shouldForceWrite) throws IOException {
            forceWriteQueueSize.dec();
            if (isMarker) {
                return 0;
            }

            try {
                if (shouldForceWrite) {
                    long startTime = MathUtils.nowInNano();
                    this.logFile.forceWrite(false);
                    journalSyncStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                }
                lastLogMark.setCurLogMark(this.logId, this.lastFlushedPosition);

                // Notify the waiters that the force write succeeded
                for (QueueEntry e : this.forceWriteWaiters) {
                    cbThreadPool.execute(e);
                }

                return this.forceWriteWaiters.size();
            } finally {
                closeFileIfNecessary();
            }
        }

        public void closeFileIfNecessary() {
            // Close if shouldClose is set
            if (shouldClose) {
                // We should guard against exceptions so its
                // safe to call in catch blocks
                try {
                    logFile.close();
                    // Call close only once
                    shouldClose = false;
                } catch (IOException ioe) {
                    LOG.error("I/O exception while closing file", ioe);
                }
            }
        }
    }

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
        // make flush interval as a parameter
        public ForceWriteThread(Thread threadToNotifyOnEx, boolean enableGroupForceWrites) {
            super("ForceWriteThread");
            this.threadToNotifyOnEx = threadToNotifyOnEx;
            this.enableGroupForceWrites = enableGroupForceWrites;
        }
        @Override
        public void run() {
            LOG.info("ForceWrite Thread started");
            boolean shouldForceWrite = true;
            int numReqInLastForceWrite = 0;
            while (running) {
                ForceWriteRequest req = null;
                try {
                    req = forceWriteRequests.take();

                    // Force write the file and then notify the write completions
                    //
                    if (!req.isMarker) {
                        if (shouldForceWrite) {
                            // if we are going to force write, any request that is already in the
                            // queue will benefit from this force write - post a marker prior to issuing
                            // the flush so until this marker is encountered we can skip the force write
                            if (enableGroupForceWrites) {
                                forceWriteRequests.put(new ForceWriteRequest(req.logFile, 0, 0, null, false, true));
                            }

                            // If we are about to issue a write, record the number of requests in
                            // the last force write and then reset the counter so we can accumulate
                            // requests in the write we are about to issue
                            if (numReqInLastForceWrite > 0) {
                                forceWriteGroupingCountStats.registerSuccessfulValue(numReqInLastForceWrite);
                                numReqInLastForceWrite = 0;
                            }
                        }
                        numReqInLastForceWrite += req.process(shouldForceWrite);
                    }

                    if (enableGroupForceWrites
                            // if its a marker we should switch back to flushing
                            && !req.isMarker
                            // This indicates that this is the last request in a given file
                            // so subsequent requests will go to a different file so we should
                            // flush on the next request
                            && !req.shouldClose) {
                        shouldForceWrite = false;
                    } else {
                        shouldForceWrite = true;
                    }
                } catch (IOException ioe) {
                    LOG.error("I/O exception in ForceWrite thread", ioe);
                    running = false;
                } catch (InterruptedException e) {
                    LOG.error("ForceWrite thread interrupted", e);
                    // close is idempotent
                    if (null != req) {
                        req.shouldClose = true;
                        req.closeFileIfNecessary();
                    }
                    running = false;
                }
            }
            // Regardless of what caused us to exit, we should notify the
            // the parent thread as it should either exit or be in the process
            // of exiting else we will have write requests hang
            threadToNotifyOnEx.interrupt();
        }
        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }
    }

    static final int PADDING_MASK = -0x100;

    static void writePaddingBytes(JournalChannel jc, ByteBuffer paddingBuffer, int journalAlignSize)
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
            paddingBuffer.putInt(PADDING_MASK);
            // padding len
            paddingBuffer.putInt(paddingBytes);
            // padding bytes
            paddingBuffer.position(8 + paddingBytes);

            paddingBuffer.flip();
            jc.preAllocIfNeeded(paddingBuffer.limit());
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

    // Should data be fsynced on disk before triggering the callback
    private final boolean syncData;

    private final LastLogMark lastLogMark = new LastLogMark(0, 0);

    /**
     * The thread pool used to handle callback.
     */
    private final ExecutorService cbThreadPool;

    // journal entry queue to commit
    final LinkedBlockingQueue<QueueEntry> queue = new LinkedBlockingQueue<QueueEntry>();
    final LinkedBlockingQueue<ForceWriteRequest> forceWriteRequests = new LinkedBlockingQueue<ForceWriteRequest>();

    volatile boolean running = true;
    private final LedgerDirsManager ledgerDirsManager;

    // Expose Stats
    private final OpStatsLogger journalAddEntryStats;
    private final OpStatsLogger journalSyncStats;
    private final OpStatsLogger journalCreationStats;
    private final OpStatsLogger journalFlushStats;
    private final OpStatsLogger journalProcessTimeStats;
    private final OpStatsLogger journalQueueStats;
    private final OpStatsLogger forceWriteGroupingCountStats;
    private final OpStatsLogger forceWriteBatchEntriesStats;
    private final OpStatsLogger forceWriteBatchBytesStats;
    private final Counter journalQueueSize;
    private final Counter forceWriteQueueSize;
    private final Counter flushMaxWaitCounter;
    private final Counter flushMaxOutstandingBytesCounter;
    private final Counter flushEmptyQueueCounter;
    private final Counter journalWriteBytes;

    public Journal(File journalDirectory, ServerConfiguration conf, LedgerDirsManager ledgerDirsManager) {
        this(journalDirectory, conf, ledgerDirsManager, NullStatsLogger.INSTANCE);
    }

    public Journal(File journalDirectory, ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
                   StatsLogger statsLogger) {
        super("BookieJournal-" + conf.getBookiePort());
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        this.journalDirectory = journalDirectory;
        this.maxJournalSize = conf.getMaxJournalSizeMB() * MB;
        this.journalPreAllocSize = conf.getJournalPreAllocSizeMB() * MB;
        this.journalWriteBufferSize = conf.getJournalWriteBufferSizeKB() * KB;
        this.syncData = conf.getJournalSyncData();
        this.maxBackupJournals = conf.getMaxBackupJournals();
        this.forceWriteThread = new ForceWriteThread(this, conf.getJournalAdaptiveGroupWrites());
        this.maxGroupWaitInNanos = TimeUnit.MILLISECONDS.toNanos(conf.getJournalMaxGroupWaitMSec());
        this.bufferedWritesThreshold = conf.getJournalBufferedWritesThreshold();
        this.bufferedEntriesThreshold = conf.getJournalBufferedEntriesThreshold();
        this.cbThreadPool = Executors.newFixedThreadPool(conf.getNumJournalCallbackThreads(),
                                                         new DaemonThreadFactory());

        // Unless there is a cap on the max wait (which requires group force writes)
        // we cannot skip flushing for queue empty
        this.flushWhenQueueEmpty = maxGroupWaitInNanos <= 0 || conf.getJournalFlushWhenQueueEmpty();

        this.removePagesFromCache = conf.getJournalRemovePagesFromCache();
        // read last log mark
        lastLogMark.readLog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Last Log Mark : {}", lastLogMark.getCurMark());
        }

        // Expose Stats
        journalAddEntryStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_ADD_ENTRY);
        journalSyncStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_SYNC);
        journalCreationStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_CREATION_LATENCY);
        journalFlushStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FLUSH_LATENCY);
        journalQueueStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_QUEUE_LATENCY);
        journalProcessTimeStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_PROCESS_TIME_LATENCY);
        forceWriteGroupingCountStats =
                statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FORCE_WRITE_GROUPING_COUNT);
        forceWriteBatchEntriesStats =
                statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FORCE_WRITE_BATCH_ENTRIES);
        forceWriteBatchBytesStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FORCE_WRITE_BATCH_BYTES);
        journalQueueSize = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_QUEUE_SIZE);
        forceWriteQueueSize = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_FORCE_WRITE_QUEUE_SIZE);
        flushMaxWaitCounter = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_NUM_FLUSH_MAX_WAIT);
        flushMaxOutstandingBytesCounter =
                statsLogger.getCounter(BookKeeperServerStats.JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES);
        flushEmptyQueueCounter = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_NUM_FLUSH_EMPTY_QUEUE);
        journalWriteBytes = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_WRITE_BYTES);
    }

    public File getJournalDirectory() {
        return journalDirectory;
    }

    public LastLogMark getLastLogMark() {
        return lastLogMark;
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
     * @param journalId Journal Log Id
     * @param journalPos Offset to start scanning
     * @param scanner Scanner to handle entries
     * @throws IOException
     */
    public void scanJournal(long journalId, long journalPos, JournalScanner scanner)
        throws IOException {
        JournalChannel recLog;
        if (journalPos <= 0) {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize);
        } else {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize,
                    journalPos);
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
                if (len == PADDING_MASK) {
                    if (journalVersion >= JournalChannel.V5) {
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
                        throw new IOException("Invalid record found with negative length : " + len);
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
        } finally {
            recLog.close();
        }
    }

    /**
     * Replay journal files.
     *
     * @param scanner Scanner to process replayed entries.
     * @throws IOException
     */
    public void replay(JournalScanner scanner) throws IOException {
        final LogMark markedLog = lastLogMark.getCurMark();
        List<Long> logs = listJournalIds(journalDirectory, new JournalIdFilter() {
            @Override
            public boolean accept(long journalId) {
                if (journalId < markedLog.getLogFileId()) {
                    return false;
                }
                return true;
            }
        });
        // last log mark may be missed due to no sync up before
        // validate filtered log ids only when we have markedLogId
        if (markedLog.getLogFileId() > 0) {
            if (logs.size() == 0 || logs.get(0) != markedLog.getLogFileId()) {
                throw new IOException("Recovery log " + markedLog.getLogFileId() + " is missing");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Try to relay journal logs : {}", logs);
        }
        // TODO: When reading in the journal logs that need to be synced, we
        // should use BufferedChannels instead to minimize the amount of
        // system calls done.
        for (Long id: logs) {
            long logPosition = 0L;
            if (id == markedLog.getLogFileId()) {
                logPosition = markedLog.getLogFileOffset();
            }
            LOG.info("Replaying journal {} from position {}", id, logPosition);
            scanJournal(id, logPosition, scanner);
        }
    }

    public void logAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx) {
        logAddEntry(Unpooled.wrappedBuffer(entry), cb, ctx);
    }

    /**
     * record an add entry operation in journal.
     */
    public void logAddEntry(ByteBuf entry, WriteCallback cb, Object ctx) {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        journalQueueSize.inc();

        //Retain entry until it gets written to journal
        entry.retain();
        queue.add(new QueueEntry(entry, ledgerId, entryId, cb, ctx, MathUtils.nowInNano()));
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
        LinkedList<QueueEntry> toFlush = new LinkedList<QueueEntry>();
        ByteBuffer lenBuff = ByteBuffer.allocate(4);
        ByteBuffer paddingBuff = ByteBuffer.allocate(2 * conf.getJournalAlignmentSize());
        ZeroBuffer.put(paddingBuff);
        final int journalFormatVersionToWrite = conf.getJournalFormatVersionToWrite();
        final int journalAlignmentSize = conf.getJournalAlignmentSize();
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
            BufferedChannel bc = null;
            long lastFlushPosition = 0;
            boolean groupWhenTimeout = false;

            long dequeueStartTime = 0L;

            QueueEntry qe = null;
            while (true) {
                // new journal file to write
                if (null == logFile) {
                    logId = logId + 1;

                    journalCreationWatcher.reset().start();
                    logFile = new JournalChannel(journalDirectory, logId, journalPreAllocSize, journalWriteBufferSize,
                                        journalAlignmentSize, removePagesFromCache, journalFormatVersionToWrite);
                    journalCreationStats.registerSuccessfulEvent(
                            journalCreationWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = bc.position();
                }

                if (qe == null) {
                    if (dequeueStartTime != 0) {
                        journalProcessTimeStats.registerSuccessfulEvent(MathUtils.elapsedNanos(dequeueStartTime),
                                TimeUnit.NANOSECONDS);
                    }

                    if (toFlush.isEmpty()) {
                        qe = queue.take();
                        dequeueStartTime = MathUtils.nowInNano();
                        journalQueueStats.registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime),
                                TimeUnit.NANOSECONDS);
                    } else {
                        long pollWaitTimeNanos =
                                maxGroupWaitInNanos - MathUtils.elapsedNanos(toFlush.get(0).enqueueTime);
                        if (flushWhenQueueEmpty || pollWaitTimeNanos < 0) {
                            pollWaitTimeNanos = 0;
                        }
                        qe = queue.poll(pollWaitTimeNanos, TimeUnit.NANOSECONDS);
                        dequeueStartTime = MathUtils.nowInNano();

                        if (qe != null) {
                            journalQueueStats.registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime),
                                    TimeUnit.NANOSECONDS);
                        }

                        boolean shouldFlush = false;
                        // We should issue a forceWrite if any of the three conditions below holds good
                        // 1. If the oldest pending entry has been pending for longer than the max wait time
                        if (maxGroupWaitInNanos > 0 && !groupWhenTimeout
                                && (MathUtils.elapsedNanos(toFlush.get(0).enqueueTime) > maxGroupWaitInNanos)) {
                            groupWhenTimeout = true;
                        } else if (maxGroupWaitInNanos > 0 && groupWhenTimeout && qe != null
                                && MathUtils.elapsedNanos(qe.enqueueTime) < maxGroupWaitInNanos) {
                            // when group timeout, it would be better to look forward, as there might be lots of
                            // entries already timeout
                            // due to a previous slow write (writing to filesystem which impacted by force write).
                            // Group those entries in the queue
                            // a) already timeout
                            // b) limit the number of entries to group
                            groupWhenTimeout = false;
                            shouldFlush = true;
                            flushMaxWaitCounter.inc();
                        } else if (qe != null
                                && ((bufferedEntriesThreshold > 0 && toFlush.size() > bufferedEntriesThreshold)
                                || (bc.position() > lastFlushPosition + bufferedWritesThreshold))) {
                            // 2. If we have buffered more than the buffWriteThreshold or bufferedEntriesThreshold
                            shouldFlush = true;
                            flushMaxOutstandingBytesCounter.inc();
                        } else if (qe == null) {
                            // We should get here only if we flushWhenQueueEmpty is true else we would wait
                            // for timeout that would put is past the maxWait threshold
                            // 3. If the queue is empty i.e. no benefit of grouping. This happens when we have one
                            // publish at a time - common case in tests.
                            shouldFlush = true;
                            flushEmptyQueueCounter.inc();
                        }

                        // toFlush is non null and not empty so should be safe to access getFirst
                        if (shouldFlush) {
                            if (journalFormatVersionToWrite >= JournalChannel.V5) {
                                writePaddingBytes(logFile, paddingBuff, journalAlignmentSize);
                            }
                            journalFlushWatcher.reset().start();
                            bc.flush(false);
                            lastFlushPosition = bc.position();
                            journalFlushStats.registerSuccessfulEvent(
                                    journalFlushWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                            // Trace the lifetime of entries through persistence
                            if (LOG.isDebugEnabled()) {
                                for (QueueEntry e : toFlush) {
                                    LOG.debug("Written and queuing for flush Ledger:" + e.ledgerId + " Entry:"
                                            + e.entryId);
                                }
                            }

                            forceWriteBatchEntriesStats.registerSuccessfulValue(toFlush.size());
                            forceWriteBatchBytesStats.registerSuccessfulValue(batchSize);

                            boolean shouldRolloverJournal = (lastFlushPosition > maxJournalSize);
                            if (syncData) {
                                // Trigger data sync to disk in the "Force-Write" thread. Callback will be triggered after data is committed to disk
                                forceWriteRequests.put(new ForceWriteRequest(logFile, logId, lastFlushPosition, toFlush, shouldRolloverJournal, false));
                                toFlush = new LinkedList<QueueEntry>();
                            } else {
                                // Data is already written on the file (though it might still be in the OS page-cache)
                                lastLogMark.setCurLogMark(logId, lastFlushPosition);
                                for (int i = 0; i < toFlush.size(); i++) {
                                    cbThreadPool.execute((QueueEntry) toFlush.get(i));
                                }

                                toFlush.clear();
                                if (shouldRolloverJournal) {
                                    forceWriteRequests.put(new ForceWriteRequest(logFile, logId, lastFlushPosition,
                                            new LinkedList<>(), shouldRolloverJournal, false));
                                }
                            }

                            batchSize = 0L;
                            // check whether journal file is over file limit
                            if (bc.position() > maxJournalSize) {
                                logFile = null;
                                continue;
                            }
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

                journalWriteBytes.add(qe.entry.readableBytes());
                journalQueueSize.dec();

                batchSize += (4 + qe.entry.readableBytes());

                lenBuff.clear();
                lenBuff.putInt(qe.entry.readableBytes());
                lenBuff.flip();

                // preAlloc based on size
                logFile.preAllocIfNeeded(4 + qe.entry.readableBytes());

                //
                // we should be doing the following, but then we run out of
                // direct byte buffers
                // logFile.write(new ByteBuffer[] { lenBuff, qe.entry });
                bc.write(lenBuff);
                bc.write(qe.entry.nioBuffer());
                qe.entry.release();

                toFlush.add(qe);
                qe = null;
            }
            logFile.close();
            logFile = null;
        } catch (IOException ioe) {
            LOG.error("I/O exception in Journal thread!", ioe);
        } catch (InterruptedException ie) {
            LOG.warn("Journal exits when shutting down", ie);
        } finally {
            // There could be packets queued for forceWrite on this logFile
            // That is fine as this exception is going to anyway take down the
            // the bookie. If we execute this as a part of graceful shutdown,
            // close will flush the file system cache making any previous
            // cached writes durable so this is fine as well.
            IOUtils.close(LOG, logFile);
        }
        LOG.info("Journal exited loop!");
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
            forceWriteThread.shutdown();
            cbThreadPool.shutdown();
            if (!cbThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Couldn't shutdown journal callback thread gracefully. Forcing");
            }
            cbThreadPool.shutdownNow();

            running = false;
            this.interrupt();
            this.join();
            LOG.info("Finished Shutting down Journal thread");
        } catch (InterruptedException ie) {
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
}
