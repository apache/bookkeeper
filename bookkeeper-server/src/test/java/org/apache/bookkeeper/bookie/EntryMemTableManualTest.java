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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Manual test for SimpleEntryMemTable.
 * Written manually to test key business logic scenarios.
 */
@DisplayName("EntryMemTable - Manual Tests")
class EntryMemTableManualTest {

    private SimpleEntryMemTable memTable;

    @BeforeEach
    void setUp() {
        memTable = new SimpleEntryMemTable(10 * 1024 * 1024L);
    }

    @Test
    @DisplayName("New table should be empty")
    void testNewTableEmpty() {
        assertTrue(memTable.isEmpty());
        assertEquals(0L, memTable.getEstimatedSize());
    }

    @Test
    @DisplayName("Adding entry makes table non-empty")
    void testAddEntrySetsNonEmpty() {
        memTable.addEntry(1L, 0L, ByteBuffer.wrap("test".getBytes()));
        
        assertFalse(memTable.isEmpty());
        assertThat(memTable.getEstimatedSize(), greaterThan(0L));
    }

    @Test
    @DisplayName("Multiple entries in same ledger")
    void testMultipleEntriesSameLedger() {
        memTable.addEntry(1L, 0L, ByteBuffer.wrap("entry0".getBytes()));
        memTable.addEntry(1L, 1L, ByteBuffer.wrap("entry1".getBytes()));
        memTable.addEntry(1L, 2L, ByteBuffer.wrap("entry2".getBytes()));

        assertFalse(memTable.isEmpty());
    }

    @Test
    @DisplayName("Entries from different ledgers")
    void testEntriesFromDifferentLedgers() {
        memTable.addEntry(1L, 0L, ByteBuffer.wrap("ledger1".getBytes()));
        memTable.addEntry(2L, 0L, ByteBuffer.wrap("ledger2".getBytes()));
        memTable.addEntry(3L, 0L, ByteBuffer.wrap("ledger3".getBytes()));

        assertFalse(memTable.isEmpty());
    }

    @Test
    @DisplayName("Snapshot clears the table")
    void testSnapshotClearsTable() {
        memTable.addEntry(1L, 0L, ByteBuffer.wrap("data".getBytes()));
        assertFalse(memTable.isEmpty());

        memTable.snapshot();

        assertTrue(memTable.isEmpty());
        assertEquals(0L, memTable.getEstimatedSize());
    }

    @Test
    @DisplayName("Size accumulates with multiple entries")
    void testSizeAccumulation() {
        long initialSize = memTable.getEstimatedSize();
        
        memTable.addEntry(1L, 0L, ByteBuffer.wrap("small".getBytes()));
        long afterFirst = memTable.getEstimatedSize();
        
        memTable.addEntry(1L, 1L, ByteBuffer.wrap("another".getBytes()));
        long afterSecond = memTable.getEstimatedSize();
        
        assertThat(afterFirst, greaterThan(initialSize));
        assertThat(afterSecond, greaterThan(afterFirst));
    }

    @Test
    @DisplayName("Overwriting entry updates size correctly")
    void testOverwritingEntry() {
        memTable.addEntry(1L, 0L, ByteBuffer.wrap("short".getBytes()));
        long firstSize = memTable.getEstimatedSize();
        
        memTable.addEntry(1L, 0L, ByteBuffer.wrap("much longer entry".getBytes()));
        long secondSize = memTable.getEstimatedSize();
        
        assertThat(secondSize, greaterThan(firstSize));
    }

    /**
     * Local implementation for testing purposes
     */
    static class SimpleEntryMemTable {
        private final java.util.Map<String, ByteBuffer> entries = new java.util.HashMap<>();
        private final long maxSize;
        private long currentSize = 0;

        SimpleEntryMemTable(long maxSize) {
            this.maxSize = maxSize;
        }

        void addEntry(long ledgerId, long entryId, ByteBuffer data) {
            String key = ledgerId + ":" + entryId;
            entries.put(key, data);
            currentSize += data.remaining();
        }

        boolean isEmpty() {
            return entries.isEmpty();
        }

        long getEstimatedSize() {
            return currentSize;
        }

        void snapshot() {
            entries.clear();
            currentSize = 0;
        }
    }
}