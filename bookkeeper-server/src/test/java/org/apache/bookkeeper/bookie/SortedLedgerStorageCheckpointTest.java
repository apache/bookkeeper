/*
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
 */

package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link SortedLedgerStorage}.
 */
@Slf4j
public class SortedLedgerStorageCheckpointTest extends LedgerStorageTestBase {

    @Data
    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    private static class TestCheckpoint implements Checkpoint {

        private final long offset;

        @Override
        public int compareTo(Checkpoint o) {
            if (Checkpoint.MAX == o) {
                return -1;
            }

            TestCheckpoint other = (TestCheckpoint) o;
            return Long.compare(offset, other.offset);
        }

    }

    @RequiredArgsConstructor
    private static class TestCheckpointSource implements CheckpointSource {

        private long currentOffset = 0;

        void advanceOffset(long numBytes) {
            currentOffset += numBytes;
        }

        @Override
        public Checkpoint newCheckpoint() {
            TestCheckpoint cp = new TestCheckpoint(currentOffset);
            log.info("New checkpoint : {}", cp);
            return cp;
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact)
            throws IOException {
            log.info("Complete checkpoint : {}", checkpoint);
        }
    }

    private SortedLedgerStorage storage;
    private Checkpointer checkpointer;
    private final LinkedBlockingQueue<Checkpoint> checkpoints;
    private final TestCheckpointSource checkpointSrc = new TestCheckpointSource();

    public SortedLedgerStorageCheckpointTest() {
        super();
        conf.setEntryLogSizeLimit(1024);
        conf.setEntryLogFilePreAllocationEnabled(false);
        this.checkpoints = new LinkedBlockingQueue<>();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        // initial checkpoint

        this.storage = new SortedLedgerStorage();
        this.checkpointer = new Checkpointer() {
            @Override
            public void startCheckpoint(Checkpoint checkpoint) {
                storage.getScheduler().submit(() -> {
                    log.info("Checkpoint the storage at {}", checkpoint);
                    try {
                        storage.checkpoint(checkpoint);
                        checkpoints.add(checkpoint);
                    } catch (IOException e) {
                        log.error("Failed to checkpoint at {}", checkpoint, e);
                    }
                });
            }

            @Override
            public void start() {
                // no-op
            }
        };

        // if the SortedLedgerStorage need not to change bookie's state, pass StateManager==null is ok
        this.storage.initialize(
            conf,
            mock(LedgerManager.class),
            ledgerDirsManager,
            ledgerDirsManager,
            null,
            checkpointSrc,
            checkpointer,
            NullStatsLogger.INSTANCE);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (null != storage) {
            storage.shutdown();
        }
        super.tearDown();
    }

    ByteBuf prepareEntry(long ledgerId, long entryId) {
        ByteBuf entry = Unpooled.buffer(4 * Long.BYTES);
        // ledger id, entry id, lac
        entry.writeLong(ledgerId);
        entry.writeLong(entryId);
        entry.writeLong(entryId - 1);
        // data
        entry.writeLong(entryId);
        return entry;
    }

    @Test
    public void testCheckpoint() throws Exception {
        // memory table holds the first checkpoint, but it is not completed yet.
        Checkpoint memtableCp = storage.memTable.kvmap.cp;
        assertEquals(new TestCheckpoint(0), memtableCp);

        // write entries into ledger storage
        long lid = System.currentTimeMillis();
        storage.setMasterKey(lid, new byte[0]);
        for (int i = 0; i < 20; i++) {
            storage.addEntry(prepareEntry(lid, i));
        }
        // simulate journal persists the entries in journal;
        checkpointSrc.advanceOffset(100);

        // memory table holds the first checkpoint, but it is not completed yet.
        memtableCp = storage.memTable.kvmap.cp;
        assertEquals(new TestCheckpoint(0), memtableCp);

        // trigger a memtable flush
        Assert.assertNotNull("snapshot shouldn't have returned null", storage.memTable.snapshot());
        storage.onSizeLimitReached(checkpointSrc.newCheckpoint());
        // wait for checkpoint to complete
        checkpoints.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertEquals(new TestCheckpoint(100), storage.memTable.kvmap.cp);
        assertEquals(0, storage.memTable.kvmap.size());
    }

    @Test
    public void testCheckpointAfterEntryLogRotated() throws Exception {
        // memory table holds the first checkpoint, but it is not completed yet.
        Checkpoint memtableCp = storage.memTable.kvmap.cp;
        assertEquals(new TestCheckpoint(0), memtableCp);

        // write entries into ledger storage
        long lid = System.currentTimeMillis();
        storage.setMasterKey(lid, new byte[0]);
        for (int i = 0; i < 20; i++) {
            storage.addEntry(prepareEntry(lid, i));
        }
        // simulate journal persists the entries in journal;
        checkpointSrc.advanceOffset(100);

        // memory table holds the first checkpoint, but it is not completed yet.
        memtableCp = storage.memTable.kvmap.cp;
        assertEquals(new TestCheckpoint(0), memtableCp);
        assertEquals(20, storage.memTable.kvmap.size());

        final CountDownLatch readyLatch = new CountDownLatch(1);
        storage.getScheduler().submit(() -> {
            try {
                readyLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // simulate entry log is rotated (due to compaction)
        EntryLogManagerForSingleEntryLog entryLogManager = (EntryLogManagerForSingleEntryLog) storage.getEntryLogger()
                .getEntryLogManager();
        entryLogManager.createNewLog(EntryLogger.UNASSIGNED_LEDGERID);
        long leastUnflushedLogId = storage.getEntryLogger().getLeastUnflushedLogId();
        long currentLogId = entryLogManager.getCurrentLogId();
        log.info("Least unflushed entry log : current = {}, leastUnflushed = {}", currentLogId, leastUnflushedLogId);

        readyLatch.countDown();
        assertNull(checkpoints.poll());
        assertEquals(new TestCheckpoint(0), storage.memTable.kvmap.cp);
        assertEquals(20, storage.memTable.kvmap.size());

        // trigger a memtable flush
        Assert.assertNotNull("snapshot shouldn't have returned null", storage.memTable.snapshot());
        storage.onSizeLimitReached(checkpointSrc.newCheckpoint());
        assertEquals(new TestCheckpoint(100), checkpoints.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS));

        // all the entries are flushed out
        assertEquals(new TestCheckpoint(100), storage.memTable.kvmap.cp);
        assertEquals(0, storage.memTable.kvmap.size());
        assertTrue(
            "current log " + currentLogId + " contains entries added from memtable should be forced to disk"
            + " but least unflushed log is " + storage.getEntryLogger().getLeastUnflushedLogId(),
            storage.getEntryLogger().getLeastUnflushedLogId() > currentLogId);
    }

}
