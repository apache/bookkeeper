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

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;

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
@Slf4j
class SyncThread implements Checkpointer {

    @Getter(AccessLevel.PACKAGE)
    final ScheduledExecutorService executor;
    final LedgerStorage ledgerStorage;
    final LedgerDirsListener dirsListener;
    final CheckpointSource checkpointSource;

    private final Object suspensionLock = new Object();
    private boolean suspended = false;
    private boolean disableCheckpoint = false;

    public SyncThread(ServerConfiguration conf,
                      LedgerDirsListener dirsListener,
                      LedgerStorage ledgerStorage,
                      CheckpointSource checkpointSource) {
        this.dirsListener = dirsListener;
        this.ledgerStorage = ledgerStorage;
        this.checkpointSource = checkpointSource;
        this.executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("SyncThread"));
    }

    @Override
    public void startCheckpoint(Checkpoint checkpoint) {
        doCheckpoint(checkpoint);
    }

    protected void doCheckpoint(Checkpoint checkpoint) {
        executor.submit(() -> {
            try {
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
                if (!disableCheckpoint) {
                    checkpoint(checkpoint);
                }
            } catch (Throwable t) {
                log.error("Exception in SyncThread", t);
                dirsListener.fatalError();
            }
        });
    }

    public Future requestFlush() {
        return executor.submit(() -> {
            try {
                flush();
            } catch (Throwable t) {
                log.error("Exception flushing ledgers ", t);
            }
        });
    }

    private void flush() {
        Checkpoint checkpoint = checkpointSource.newCheckpoint();
        try {
            ledgerStorage.flush();
        } catch (NoWritableLedgerDirException e) {
            log.error("No writeable ledger directories", e);
            dirsListener.allDisksFull(true);
            return;
        } catch (IOException e) {
            log.error("Exception flushing ledgers", e);
            return;
        }

        if (disableCheckpoint) {
            return;
        }

        log.info("Flush ledger storage at checkpoint {}.", checkpoint);
        try {
            checkpointSource.checkpointComplete(checkpoint, false);
        } catch (IOException e) {
            log.error("Exception marking checkpoint as complete", e);
            dirsListener.allDisksFull(true);
        }
    }

    @VisibleForTesting
    public void checkpoint(Checkpoint checkpoint) {
        if (null == checkpoint) {
            // do nothing if checkpoint is null
            return;
        }

        try {
            ledgerStorage.checkpoint(checkpoint);
        } catch (NoWritableLedgerDirException e) {
            log.error("No writeable ledger directories", e);
            dirsListener.allDisksFull(true);
            return;
        } catch (IOException e) {
            log.error("Exception flushing ledgers", e);
            return;
        }

        try {
            checkpointSource.checkpointComplete(checkpoint, true);
        } catch (IOException e) {
            log.error("Exception marking checkpoint as complete", e);
            dirsListener.allDisksFull(true);
        }
    }

    @Override
    public void start() {
        // no-op
    }

    /**
     * Suspend sync thread. (for testing)
     */
    @VisibleForTesting
    public void suspendSync() {
        synchronized (suspensionLock) {
            suspended = true;
        }
    }

    /**
     * Resume sync thread. (for testing)
     */
    @VisibleForTesting
    public void resumeSync() {
        synchronized (suspensionLock) {
            suspended = false;
            suspensionLock.notify();
        }
    }

    @VisibleForTesting
    public void disableCheckpoint() {
        disableCheckpoint = true;
    }

    // shutdown sync thread
    void shutdown() throws InterruptedException {
        log.info("Shutting down SyncThread");
        requestFlush();

        executor.shutdown();
        long start = System.currentTimeMillis();
        while (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
            long now = System.currentTimeMillis();
            log.info("SyncThread taking a long time to shutdown. Has taken {}"
                    + " milliseconds so far", now - start);
        }
    }
}
