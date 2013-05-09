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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;

import com.google.common.annotations.VisibleForTesting;

import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SyncThread is a background thread which help checkpointing ledger storage
 * when a checkpoint is requested. After a ledger storage is checkpointed,
 * the journal files added before checkpoint will be garbage collected.
 * <p>
 * After all data has been persisted to ledger index files and entry
 * loggers, it is safe to complete a checkpoint by persisting the log marker
 * to disk. If bookie failed after persist log mark, bookie is able to relay
 * journal entries started from last log mark without losing any entries.
 * </p>
 * <p>
 * Those journal files whose id are less than the log id in last log mark,
 * could be removed safely after persisting last log mark. We provide a
 * setting to let user keeping number of old journal files which may be used
 * for manual recovery in critical disaster.
 * </p>
 */
class SyncThread extends Thread {
    static Logger LOG = LoggerFactory.getLogger(SyncThread.class);

    volatile boolean running = true;
    // flag to ensure sync thread will not be interrupted during flush
    final AtomicBoolean flushing = new AtomicBoolean(false);
    final int flushInterval;
    final LedgerStorage ledgerStorage;
    final LedgerDirsListener dirsListener;
    final CheckpointSource checkpointSource;

    public SyncThread(ServerConfiguration conf,
                      LedgerDirsListener dirsListener,
                      LedgerStorage ledgerStorage,
                      CheckpointSource checkpointSource) {
        super("SyncThread");
        this.dirsListener = dirsListener;
        this.ledgerStorage = ledgerStorage;
        this.checkpointSource = checkpointSource;

        flushInterval = conf.getFlushInterval();
        LOG.debug("Flush Interval : {}", flushInterval);
    }

    /**
     * flush data up to given logMark and roll log if success
     * @param checkpoint
     */
    @VisibleForTesting
    public void checkpoint(Checkpoint checkpoint) {
        boolean flushFailed = false;
        try {
            if (running) {
                checkpoint = ledgerStorage.checkpoint(checkpoint);
            } else {
                ledgerStorage.flush();
            }
        } catch (NoWritableLedgerDirException e) {
            LOG.error("No writeable ledger directories");
            flushFailed = true;
            flushing.set(false);
            dirsListener.allDisksFull();
        } catch (IOException e) {
            LOG.error("Exception flushing Ledger", e);
            flushFailed = true;
        }

        // if flush failed, we should not roll last mark, otherwise we would
        // have some ledgers are not flushed and their journal entries were lost
        if (!flushFailed) {
            try {
                checkpointSource.checkpointComplete(checkpoint, running);
            } catch (IOException e) {
                flushing.set(false);
                LOG.error("Marking checkpoint as complete failed", e);
                dirsListener.allDisksFull();
            }
        }
    }

    private Object suspensionLock = new Object();
    private boolean suspended = false;

    /**
     * Suspend sync thread. (for testing)
     */
    @VisibleForTesting
    public void suspendSync() {
        synchronized(suspensionLock) {
            suspended = true;
        }
    }

    /**
     * Resume sync thread. (for testing)
     */
    @VisibleForTesting
    public void resumeSync() {
        synchronized(suspensionLock) {
            suspended = false;
            suspensionLock.notify();
        }
    }

    @Override
    public void run() {
        try {
            while(running) {
                synchronized (this) {
                    try {
                        wait(flushInterval);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        continue;
                    }
                }

                synchronized (suspensionLock) {
                    while (suspended) {
                        try {
                            suspensionLock.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            continue;
                        }
                    }
                }

                // try to mark flushing flag to check if interrupted
                if (!flushing.compareAndSet(false, true)) {
                    // set flushing flag failed, means flushing is true now
                    // indicates another thread wants to interrupt sync thread to exit
                    break;
                }
                checkpoint(checkpointSource.newCheckpoint());

                flushing.set(false);
            }
        } catch (Throwable t) {
            LOG.error("Exception in SyncThread", t);
            flushing.set(false);
            dirsListener.fatalError();
        }
    }

    // shutdown sync thread
    void shutdown() throws InterruptedException {
        // Wake up and finish sync thread
        running = false;
        // make a checkpoint when shutdown
        if (flushing.compareAndSet(false, true)) {
            // it is safe to interrupt itself now
            this.interrupt();
        }
        this.join();
    }
}
