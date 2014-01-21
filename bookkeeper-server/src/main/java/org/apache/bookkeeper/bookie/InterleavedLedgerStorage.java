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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.util.SnapshotMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interleave ledger storage
 * This ledger storage implementation stores all entries in a single
 * file and maintains an index file for each ledger.
 */
class InterleavedLedgerStorage implements LedgerStorage, EntryLogListener {
    private final static Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorage.class);

    // Hold the last checkpoint
    static class CheckpointHolder {
        Checkpoint lastCheckpoint = Checkpoint.MAX;

        synchronized void setNextCheckpoint(Checkpoint cp) {
            if (Checkpoint.MAX.equals(lastCheckpoint) || lastCheckpoint.compareTo(cp) < 0) {
                lastCheckpoint = cp;
            }
        }

        synchronized void clearLastCheckpoint(Checkpoint done) {
            if (0 == lastCheckpoint.compareTo(done)) {
                lastCheckpoint = Checkpoint.MAX;
            }
        }

        synchronized Checkpoint getLastCheckpoint() {
            return lastCheckpoint;
        }
    }

    EntryLogger entryLogger;
    LedgerCache ledgerCache;
    private final CheckpointSource checkpointSource;
    private final CheckpointHolder checkpointHolder = new CheckpointHolder();

    // A sorted map to stored all active ledger ids
    protected final SnapshotMap<Long, Boolean> activeLedgers;

    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    final GarbageCollectorThread gcThread;

    // this indicates that a write has happened since the last flush
    private volatile boolean somethingWritten = false;

    InterleavedLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                             LedgerDirsManager ledgerDirsManager, CheckpointSource checkpointSource)
            throws IOException {
        this(conf, ledgerManager, ledgerDirsManager, ledgerDirsManager, checkpointSource);
    }

    InterleavedLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                             LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
                             CheckpointSource checkpointSource)
            throws IOException {
        activeLedgers = new SnapshotMap<Long, Boolean>();
        this.checkpointSource = checkpointSource;
        entryLogger = new EntryLogger(conf, ledgerDirsManager, this);
        ledgerCache = new LedgerCacheImpl(conf, activeLedgers,
                null == indexDirsManager ? ledgerDirsManager : indexDirsManager);
        gcThread = new GarbageCollectorThread(conf, ledgerCache, entryLogger,
                activeLedgers, ledgerManager);
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFailed(File disk) {
                // do nothing.
            }

            @Override
            public void diskAlmostFull(File disk) {
                gcThread.enableForceGC();
            }

            @Override
            public void diskFull(File disk) {
                gcThread.enableForceGC();
            }

            @Override
            public void allDisksFull() {
                gcThread.enableForceGC();
            }

            @Override
            public void fatalError() {
                // do nothing.
            }

            @Override
            public void diskWritable(File disk) {
                // we have enough space now, disable force gc.
                gcThread.disableForceGC();
            }

            @Override
            public void diskJustWritable(File disk) {
                // if a disk is just writable, we still need force gc.
                gcThread.enableForceGC();
            }
        };
    }

    @Override
    public void start() {
        gcThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // shut down gc thread, which depends on zookeeper client
        // also compaction will write entries again to entry log file
        LOG.info("Shutting down InterleavedLedgerStorage");
        gcThread.shutdown();
        entryLogger.shutdown();
        try {
            ledgerCache.close();
        } catch (IOException e) {
            LOG.error("Error while closing the ledger cache", e);
        }
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return ledgerCache.setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return ledgerCache.isFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        ledgerCache.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return ledgerCache.readMasterKey(ledgerId);
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return ledgerCache.ledgerExists(ledgerId);
    }

    @Override
    synchronized public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();

        processEntry(ledgerId, entryId, entry);

        return entryId;
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        long offset;
        /*
         * If entryId is BookieProtocol.LAST_ADD_CONFIRMED, then return the last written.
         */
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            entryId = ledgerCache.getLastEntry(ledgerId);
        }

        offset = ledgerCache.getEntryOffset(ledgerId, entryId);
        if (offset == 0) {
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }
        return ByteBuffer.wrap(entryLogger.readEntry(ledgerId, entryId, offset));
    }

    private void flushOrCheckpoint(boolean isCheckpointFlush)
            throws IOException {

        boolean flushFailed = false;
        try {
            ledgerCache.flushLedger(true);
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger cache", ioe);
            flushFailed = true;
        }

        try {
            // if it is just a checkpoint flush, we just flush rotated entry log files
            // in entry logger.
            if (isCheckpointFlush) {
                entryLogger.checkpoint();
            } else {
                entryLogger.flush();
            }
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", ioe);
            flushFailed = true;
        }
        if (flushFailed) {
            throw new IOException("Flushing to storage failed, check logs");
        }
    }

    @Override
    public Checkpoint checkpoint(Checkpoint checkpoint) throws IOException {
        Checkpoint lastCheckpoint = checkpointHolder.getLastCheckpoint();
        // if checkpoint is less than last checkpoint, we don't need to do checkpoint again.
        if (lastCheckpoint.compareTo(checkpoint) > 0) {
            return lastCheckpoint;
        }
        // we don't need to check somethingwritten since checkpoint
        // is scheduled when rotate an entry logger file. and we could
        // not set somethingWritten to false after checkpoint, since
        // current entry logger file isn't flushed yet.
        flushOrCheckpoint(true);
        // after the ledger storage finished checkpointing, try to clear the done checkpoint

        checkpointHolder.clearLastCheckpoint(lastCheckpoint);
        return lastCheckpoint;
    }

    @Override
    synchronized public void flush() throws IOException {
        if (!somethingWritten) {
            return;
        }
        somethingWritten = false;
        flushOrCheckpoint(false);
    }

    @Override
    public BKMBeanInfo getJMXBean() {
        return ledgerCache.getJMXBean();
    }

    protected void processEntry(long ledgerId, long entryId, ByteBuffer entry) throws IOException {
        processEntry(ledgerId, entryId, entry, true);
    }

    synchronized protected void processEntry(long ledgerId, long entryId, ByteBuffer entry, boolean rollLog)
            throws IOException {
        /*
         * Touch dirty flag
         */
        somethingWritten = true;

        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(ledgerId, entry, rollLog);

        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);
    }

    @Override
    public void onRotateEntryLog() {
        // for interleaved ledger storage, we request a checkpoint when rotating a entry log file.
        // the checkpoint represent the point that all the entries added before this point are already
        // in ledger storage and ready to be synced to disk.
        // TODO: we could consider remove checkpointSource and checkpointSouce#newCheckpoint
        // later if we provide kind of LSN (Log/Journal Squeuence Number)
        // mechanism when adding entry.
        checkpointHolder.setNextCheckpoint(checkpointSource.newCheckpoint());
    }
}
