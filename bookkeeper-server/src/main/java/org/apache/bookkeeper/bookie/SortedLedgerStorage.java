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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code SortedLedgerStorage} is an extension of {@link InterleavedLedgerStorage}. It
 * is comprised of two {@code MemTable}s and a {@code InterleavedLedgerStorage}. All the
 * entries will be first added into a {@code MemTable}, and then be flushed back to the
 * {@code InterleavedLedgerStorage} when the {@code MemTable} becomes full.
 */
public class SortedLedgerStorage extends InterleavedLedgerStorage
        implements LedgerStorage, CacheCallback, SkipListFlusher {
    private static final Logger LOG = LoggerFactory.getLogger(SortedLedgerStorage.class);

    EntryMemTable memTable;
    private ScheduledExecutorService scheduler;

    public SortedLedgerStorage() {
        super();
    }

    @Override
    public void initialize(ServerConfiguration conf,
                           LedgerManager ledgerManager,
                           LedgerDirsManager ledgerDirsManager,
                           LedgerDirsManager indexDirsManager,
                           CheckpointSource checkpointSource,
                           Checkpointer checkpointer,
                           StatsLogger statsLogger)
            throws IOException {
        super.initialize(
            conf,
            ledgerManager,
            ledgerDirsManager,
            indexDirsManager,
            checkpointSource,
            checkpointer,
            statsLogger);
        this.memTable = new EntryMemTable(conf, checkpointSource, statsLogger);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setNameFormat("SortedLedgerStorage-%d")
                .setPriority((Thread.NORM_PRIORITY + Thread.MAX_PRIORITY) / 2).build());
    }

    @VisibleForTesting
    ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    @Override
    public void start() {
        try {
            flush();
        } catch (IOException e) {
            LOG.error("Exception thrown while flushing ledger cache.", e);
        }
        super.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // Wait for any jobs currently scheduled to be completed and then shut down.
        scheduler.shutdown();
        if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }
        super.shutdown();
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        // Done this way because checking the skip list is an O(logN) operation compared to
        // the O(1) for the ledgerCache.
        if (!super.ledgerExists(ledgerId)) {
            EntryKeyValue kv = memTable.getLastEntry(ledgerId);
            if (null == kv) {
                return super.ledgerExists(ledgerId);
            }
        }
        return true;
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        long lac = entry.getLong(entry.readerIndex() + 16);

        memTable.addEntry(ledgerId, entryId, entry.nioBuffer(), this);
        ledgerCache.updateLastAddConfirmed(ledgerId, lac);
        return entryId;
    }

    /**
     * Get the last entry id for a particular ledger.
     * @param ledgerId
     * @return
     */
    private ByteBuf getLastEntryId(long ledgerId) throws IOException {
        EntryKeyValue kv = memTable.getLastEntry(ledgerId);
        if (null != kv) {
            return kv.getValueAsByteBuffer();
        }
        // If it doesn't exist in the skip list, then fallback to the ledger cache+index.
        return super.getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntryId(ledgerId);
        }
        ByteBuf buffToRet;
        try {
            buffToRet = super.getEntry(ledgerId, entryId);
        } catch (Bookie.NoEntryException nee) {
            EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
            if (null == kv) {
                // The entry might have been flushed since we last checked, so query the ledger cache again.
                // If the entry truly doesn't exist, then this will throw a NoEntryException
                buffToRet = super.getEntry(ledgerId, entryId);
            } else {
                buffToRet = kv.getValueAsByteBuffer();
            }
        }
        // buffToRet will not be null when we reach here.
        return buffToRet;
    }

    @Override
    public void checkpoint(final Checkpoint checkpoint) throws IOException {
        long numBytesFlushed = memTable.flush(this, checkpoint);
        if (numBytesFlushed > 0) {
            // if bytes are added between previous flush and this checkpoint,
            // it means bytes might live at current active entry log, we need
            // roll current entry log and then issue checkpoint to underlying
            // interleaved ledger storage.
            entryLogger.rollLog();
        }
        super.checkpoint(checkpoint);
    }

    @Override
    public void process(long ledgerId, long entryId,
                        ByteBuffer buffer) throws IOException {
        processEntry(ledgerId, entryId, buffer, false);
    }

    @Override
    public void flush() throws IOException {
        memTable.flush(this, Checkpoint.MAX);
        super.flush();
    }

    // CacheCallback functions.
    @Override
    public void onSizeLimitReached(final Checkpoint cp) throws IOException {
        LOG.info("Reached size {}", cp);
        // when size limit reached, we get the previous checkpoint from snapshot mem-table.
        // at this point, we are safer to schedule a checkpoint, since the entries added before
        // this checkpoint already written to entry logger.
        // but it would be better not to let mem-table flush to different entry log files,
        // so we roll entry log files in SortedLedgerStorage itself.
        // After that, we could make the process writing data to entry logger file not bound with checkpoint.
        // otherwise, it hurts add performance.
        //
        // The only exception for the size limitation is if a file grows to be more than hard limit 2GB,
        // we have to force rolling log, which it might cause slight performance effects
        scheduler.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Started flushing mem table.");
                    long logIdBeforeFlush = entryLogger.getCurrentLogId();
                    memTable.flush(SortedLedgerStorage.this);
                    long logIdAfterFlush = entryLogger.getCurrentLogId();
                    // in any case that an entry log reaches the limit, we roll the log and start checkpointing.
                    // if a memory table is flushed spanning over two entry log files, we also roll log. this is
                    // for performance consideration: since we don't wanna checkpoint a new log file that ledger
                    // storage is writing to.
                    if (entryLogger.reachEntryLogLimit(0) || logIdAfterFlush != logIdBeforeFlush) {
                        LOG.info("Rolling entry logger since it reached size limitation");
                        entryLogger.rollLog();
                        checkpointer.startCheckpoint(cp);
                    }
                } catch (IOException e) {
                    // TODO: if we failed to flush data, we should switch the bookie back to readonly mode
                    //       or shutdown it. {@link https://github.com/apache/bookkeeper/issues/280}
                    LOG.error("Exception thrown while flushing skip list cache.", e);
                }
            }
        });
    }

    @Override
    public void onRotateEntryLog() {
        // override the behavior at interleaved ledger storage.
        // we don't trigger any checkpoint logic when an entry log file is rotated, because entry log file rotation
        // can happen because compaction. in a sorted ledger storage, checkpoint should happen after the data is
        // flushed to the entry log file.
    }
}
