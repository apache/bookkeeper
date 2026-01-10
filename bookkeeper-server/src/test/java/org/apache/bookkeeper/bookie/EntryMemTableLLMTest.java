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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * LLM high-value, reduced tests for EntryMemTable.
 */
@DisplayName("EntryMemTable - LLM minimal tests")
class EntryMemTableLLMTest {

    private EntryMemTable memTable;
    private TestCheckpointSource checkpointSource;

    @BeforeEach
    void setUp() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setSkipListSizeLimit(16 * 1024 * 1024);
        checkpointSource = new TestCheckpointSource();
        memTable = new EntryMemTable(conf, checkpointSource, NullStatsLogger.INSTANCE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (memTable != null) {
            memTable.close();
        }
    }

    @Test
    @DisplayName("addEntry stores and getEntry retrieves same ledger/entry")
    void testAddAndGetEntry() throws Exception {
        long ledgerId = 1L;
        long entryId = 7L;
        ByteBuf data = Unpooled.wrappedBuffer(randomBuffer(64));

        long added = memTable.addEntry(ledgerId, entryId, data.nioBuffer(), null);
        data.release();

        assertThat(added, greaterThanOrEqualTo(1L));
        EntryKeyValue retrieved = memTable.getEntry(ledgerId, entryId);
        assertNotNull(retrieved);
        assertEquals(ledgerId, retrieved.getLedgerId());
        assertEquals(entryId, retrieved.getEntryId());
    }

    @Test
    @DisplayName("duplicate add returns zero-size path and keeps original")
    void testDuplicateAddReturnsZeroSize() throws Exception {
        ByteBuf first = Unpooled.wrappedBuffer(randomBuffer(32));
        long r1 = memTable.addEntry(2L, 0L, first.nioBuffer(), null);
        first.release();
        assertThat(r1, greaterThanOrEqualTo(1L));

        ByteBuf second = Unpooled.wrappedBuffer(randomBuffer(32));
        long r2 = memTable.addEntry(2L, 0L, second.nioBuffer(), null);
        second.release();
        assertEquals(0L, r2);

        EntryKeyValue entry = memTable.getEntry(2L, 0L);
        assertNotNull(entry);
    }

    @Test
    @DisplayName("zero-size entry still releases semaphore and allows further adds")
    void testZeroSizeEntryAllowsSubsequentAdds() throws Exception {
        ByteBuf empty = Unpooled.wrappedBuffer(new byte[0]);
        long zeroSize = memTable.addEntry(3L, 0L, empty.nioBuffer(), null);
        empty.release();
        assertThat(zeroSize, greaterThanOrEqualTo(0L));

        ByteBuf normal = Unpooled.wrappedBuffer(randomBuffer(48));
        long added = memTable.addEntry(3L, 1L, normal.nioBuffer(), null);
        normal.release();
        assertThat(added, greaterThanOrEqualTo(1L));
    }

    @Test
    @DisplayName("getLastEntry returns correct ledger and null for missing")
    void testGetLastEntryLedgerValidation() throws Exception {
        for (long ledger = 1; ledger <= 2; ledger++) {
            for (int i = 0; i < 3; i++) {
                ByteBuf data = Unpooled.wrappedBuffer(randomBuffer(24));
                memTable.addEntry(ledger, i, data.nioBuffer(), null);
                data.release();
            }
        }

        EntryKeyValue last1 = memTable.getLastEntry(1L);
        assertNotNull(last1);
        assertEquals(1L, last1.getLedgerId());
        assertEquals(2L, last1.getEntryId());

        EntryKeyValue last2 = memTable.getLastEntry(2L);
        assertNotNull(last2);
        assertEquals(2L, last2.getLedgerId());
        assertEquals(2L, last2.getEntryId());

        assertNull(memTable.getLastEntry(99L));
    }

    @Test
    @DisplayName("snapshot then flush keeps memtable usable")
    void testSnapshotAndFlushFlow() throws Exception {
        for (int i = 0; i < 5; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(randomBuffer(40));
            memTable.addEntry(10L, i, data.nioBuffer(), null);
            data.release();
        }

        Checkpoint cp = memTable.snapshot();
        assertNotNull(cp);

        MockSkipListFlusher flusher = new MockSkipListFlusher();
        long flushedSize = memTable.flush(flusher);
        assertThat(flushedSize, greaterThanOrEqualTo(0L));

        ByteBuf data = Unpooled.wrappedBuffer(randomBuffer(32));
        long added = memTable.addEntry(10L, 99L, data.nioBuffer(), null);
        data.release();
        assertThat(added, greaterThanOrEqualTo(1L));
    }

    private ByteBuffer randomBuffer(int len) {
        byte[] arr = new byte[len];
        for (int i = 0; i < len; i++) {
            arr[i] = (byte) (i % 128);
        }
        return ByteBuffer.wrap(arr);
    }

    static class MockSkipListFlusher implements SkipListFlusher {
        private long processCount = 0;

        @Override
        public void process(long ledgerId, long entryId, ByteBuf entry) throws IOException {
            processCount++;
        }

        long getProcessCount() {
            return processCount;
        }
    }

    static class TestCheckpointSource implements CheckpointSource {
        private long counter = 0;

        @Override
        public Checkpoint newCheckpoint() {
            return new TestCheckpoint(counter++);
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact) {
            // No-op for testing
        }

        static class TestCheckpoint implements Checkpoint {
            private final long id;

            TestCheckpoint(long id) {
                this.id = id;
            }

            @Override
            public int compareTo(Checkpoint o) {
                if (o == Checkpoint.MAX) {
                    return -1;
                }
                if (o == Checkpoint.MIN) {
                    return 1;
                }
                if (o instanceof TestCheckpoint) {
                    return Long.compare(id, ((TestCheckpoint) o).id);
                }
                return 0;
            }
        }
    }
}
