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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a synchronization thread.
 */
public class TestSyncThread {
    private static final Logger LOG = LoggerFactory.getLogger(TestSyncThread.class);

    ExecutorService executor = null;

    @Before
    public void setupExecutor() {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void teardownExecutor() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    /**
     * Test that if a flush is taking a long time,
     * the sync thread will not shutdown until it
     * has finished.
     */
    @Test
    public void testSyncThreadLongShutdown() throws Exception {
        int flushInterval = 100;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setFlushInterval(flushInterval);
        CheckpointSource checkpointSource = new DummyCheckpointSource();
        LedgerDirsListener listener = new LedgerDirsListener() {};

        final CountDownLatch checkpointCalledLatch = new CountDownLatch(1);
        final CountDownLatch checkpointLatch = new CountDownLatch(1);

        final CountDownLatch flushCalledLatch = new CountDownLatch(1);
        final CountDownLatch flushLatch = new CountDownLatch(1);
        final AtomicBoolean failedSomewhere = new AtomicBoolean(false);
        LedgerStorage storage = new DummyLedgerStorage() {
                @Override
                public void flush() throws IOException {
                    flushCalledLatch.countDown();
                    try {
                        flushLatch.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.error("Interrupted in flush thread", ie);
                        failedSomewhere.set(true);
                    }
                }

                @Override
                public void checkpoint(Checkpoint checkpoint)
                        throws IOException {
                    checkpointCalledLatch.countDown();
                    try {
                        checkpointLatch.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.error("Interrupted in checkpoint thread", ie);
                        failedSomewhere.set(true);
                    }
                }
            };

        final SyncThread t = new SyncThread(conf, listener, storage, checkpointSource);
        t.startCheckpoint(Checkpoint.MAX);
        assertTrue("Checkpoint should have been called",
                   checkpointCalledLatch.await(10, TimeUnit.SECONDS));
        Future<Boolean> done = executor.submit(() -> {
            try {
                t.shutdown();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted shutting down sync thread", ie);
                failedSomewhere.set(true);
                return false;
            }
            return true;
        });
        checkpointLatch.countDown();
        assertFalse("Shutdown shouldn't have finished", done.isDone());
        assertTrue("Flush should have been called",
                   flushCalledLatch.await(10, TimeUnit.SECONDS));

        assertFalse("Shutdown shouldn't have finished", done.isDone());
        flushLatch.countDown();

        assertTrue("Shutdown should have finished successfully", done.get(10, TimeUnit.SECONDS));
        assertFalse("Shouldn't have failed anywhere", failedSomewhere.get());
    }

    /**
     * Test that sync thread suspension works.
     * i.e. when we suspend the syncthread, nothing
     * will be synced.
     */
    @Test
    public void testSyncThreadSuspension() throws Exception {
        int flushInterval = 100;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setFlushInterval(flushInterval);
        CheckpointSource checkpointSource = new DummyCheckpointSource();
        LedgerDirsListener listener = new LedgerDirsListener() {};

        final AtomicInteger checkpointCount = new AtomicInteger(0);
        LedgerStorage storage = new DummyLedgerStorage() {
                @Override
                public void checkpoint(Checkpoint checkpoint)
                        throws IOException {
                    checkpointCount.incrementAndGet();
                }
            };
        final SyncThread t = new SyncThread(conf, listener, storage, checkpointSource);
        t.startCheckpoint(Checkpoint.MAX);
        while (checkpointCount.get() == 0) {
            Thread.sleep(flushInterval);
        }
        t.suspendSync();
        Thread.sleep(flushInterval);
        int count = checkpointCount.get();
        for (int i = 0; i < 10; i++) {
            t.startCheckpoint(Checkpoint.MAX);
            assertEquals("Checkpoint count shouldn't change", count, checkpointCount.get());
        }
        t.resumeSync();
        int i = 0;
        while (checkpointCount.get() == count) {
            Thread.sleep(flushInterval);
            i++;
            if (i > 100) {
                fail("Checkpointing never resumed");
            }
        }
        t.shutdown();
    }

    /**
     * Test that if the ledger storage throws a
     * runtime exception, the bookie will be told
     * to shutdown.
     */
    @Test
    public void testSyncThreadShutdownOnError() throws Exception {
        int flushInterval = 100;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setFlushInterval(flushInterval);
        CheckpointSource checkpointSource = new DummyCheckpointSource();
        final CountDownLatch fatalLatch = new CountDownLatch(1);
        LedgerDirsListener listener = new LedgerDirsListener() {
                @Override
                public void fatalError() {
                    fatalLatch.countDown();
                }
            };

        LedgerStorage storage = new DummyLedgerStorage() {
                @Override
                public void checkpoint(Checkpoint checkpoint)
                        throws IOException {
                    throw new RuntimeException("Fatal error in sync thread");
                }
            };
        final SyncThread t = new SyncThread(conf, listener, storage, checkpointSource);
        t.startCheckpoint(Checkpoint.MAX);
        assertTrue("Should have called fatal error", fatalLatch.await(10, TimeUnit.SECONDS));
        t.shutdown();
    }

    /**
     * Test that if the ledger storage throws
     * a disk full exception, the owner of the sync
     * thread will be notified.
     */
    @Test
    public void testSyncThreadDisksFull() throws Exception {
        int flushInterval = 100;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setFlushInterval(flushInterval);
        CheckpointSource checkpointSource = new DummyCheckpointSource();
        final CountDownLatch diskFullLatch = new CountDownLatch(1);
        LedgerDirsListener listener = new LedgerDirsListener() {
                @Override
                public void allDisksFull(boolean highPriorityWritesAllowed) {
                    diskFullLatch.countDown();
                }
            };

        LedgerStorage storage = new DummyLedgerStorage() {
                @Override
                public void checkpoint(Checkpoint checkpoint)
                        throws IOException {
                    throw new NoWritableLedgerDirException("Disk full error in sync thread");
                }
            };
        final SyncThread t = new SyncThread(conf, listener, storage, checkpointSource);
        t.startCheckpoint(Checkpoint.MAX);
        assertTrue("Should have disk full error", diskFullLatch.await(10, TimeUnit.SECONDS));
        t.shutdown();
    }

    private static class DummyCheckpointSource implements CheckpointSource {
        @Override
        public Checkpoint newCheckpoint() {
            return Checkpoint.MAX;
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                throws IOException {
        }
    }

    private static class DummyLedgerStorage implements LedgerStorage {
        @Override
        public void initialize(
            ServerConfiguration conf,
            LedgerManager ledgerManager,
            LedgerDirsManager ledgerDirsManager,
            LedgerDirsManager indexDirsManager,
            StateManager stateManager,
            CheckpointSource checkpointSource,
            Checkpointer checkpointer,
            StatsLogger statsLogger,
            ByteBufAllocator allocator)
                throws IOException {
        }

        @Override
        public void deleteLedger(long ledgerId) throws IOException {
        }

        @Override
        public void start() {
        }

        @Override
        public void shutdown() throws InterruptedException {
        }

        @Override
        public boolean ledgerExists(long ledgerId) throws IOException {
            return true;
        }

        @Override
        public boolean setFenced(long ledgerId) throws IOException {
            return true;
        }

        @Override
        public boolean isFenced(long ledgerId) throws IOException {
            return false;
        }

        @Override
        public void setMasterKey(long ledgerId, byte[] masterKey)
                throws IOException {
        }

        @Override
        public byte[] readMasterKey(long ledgerId)
                throws IOException, BookieException {
            return new byte[0];
        }

        @Override
        public long addEntry(ByteBuf entry) throws IOException {
            return 1L;
        }

        @Override
        public ByteBuf getEntry(long ledgerId, long entryId)
                throws IOException {
            return null;
        }

        @Override
        public long getLastAddConfirmed(long ledgerId) throws IOException {
            return 0;
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void setExplicitlac(long ledgerId, ByteBuf lac) {
        }

        @Override
        public ByteBuf getExplicitLac(long ledgerId) {
            return null;
        }

        @Override
        public boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                                     long previousLAC,
                                                     Watcher<LastAddConfirmedUpdateNotification> watcher)
                throws IOException {
            return false;
        }

        @Override
        public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                        Watcher<LastAddConfirmedUpdateNotification> watcher)
                throws IOException {
        }

        @Override
        public void checkpoint(Checkpoint checkpoint)
                throws IOException {
        }

        @Override
        public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        }
    }

}
