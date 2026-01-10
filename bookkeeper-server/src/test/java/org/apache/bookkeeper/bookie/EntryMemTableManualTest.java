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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Manual-style minimal tests for EntryMemTable.
 */
@DisplayName("EntryMemTable - Manual Tests (lean)")
class EntryMemTableManualTest {

    private EntryMemTable memTable;
    private ServerConfiguration conf;
    private TestCheckpointSource checkpointSource;

    @BeforeEach
    void setUp() {
        conf = new ServerConfiguration();
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
    @DisplayName("Add single entry")
    void testAddSingleEntry() throws Exception {
        long ledgerId = 1L;
        long entryId = 0L;
        ByteBuf data = Unpooled.wrappedBuffer("test-data".getBytes());
        
        long bytesAdded = memTable.addEntry(ledgerId, entryId, data.nioBuffer(), null);
        
        assertTrue(bytesAdded >= 0);
        data.release();
    }

    @Test
    @DisplayName("Add multiple entries and snapshot")
    void testAddMultipleEntriesAndSnapshot() throws Exception {
        long ledgerId = 2L;
        
        for (int i = 0; i < 10; i++) {
            ByteBuf data = Unpooled.wrappedBuffer(("entry-" + i).getBytes());
            memTable.addEntry(ledgerId, i, data.nioBuffer(), null);
            data.release();
        }
        
        Checkpoint cp = memTable.snapshot();
        assertNotNull(cp, "Checkpoint should not be null");
    }

    @Test
    @DisplayName("Large entry handling")
    void testLargeEntry() throws Exception {
        long ledgerId = 999L;
        long entryId = 1L;
        
        byte[] largeData = new byte[1024 * 1024]; // 1MB
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        
        ByteBuf data = Unpooled.wrappedBuffer(largeData);
        long bytesAdded = memTable.addEntry(ledgerId, entryId, data.nioBuffer(), null);
        
        assertTrue(bytesAdded > 0, "Size should reflect added entry");
        data.release();
    }

    /**
     * Test checkpoint source implementation.
     */
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
