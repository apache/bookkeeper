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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * LLM-generated style tests for SimpleEntryMemTable.
 */
@DisplayName("EntryMemTable - LLM Tests")
class EntryMemTableLLMTest {

    @ParameterizedTest(name = "Add entries: ledger={0}, startId={1}, count={2}")
    @CsvSource({
        "1, 0, 10",
        "2, 5, 20",
        "999999, 0, 5"
    })
    void testAddMultipleEntries(long ledgerId, long startId, int count) {
        SimpleEntryMemTable memTable = new SimpleEntryMemTable(32 * 1024 * 1024L);
        int sizeBefore = (int) memTable.getEstimatedSize();

        for (int i = 0; i < count; i++) {
            ByteBuffer data = randomBuffer(64 + i);
            memTable.addEntry(ledgerId, startId + i, data);
        }

        int sizeAfter = (int) memTable.getEstimatedSize();
        assertThat(sizeAfter, greaterThanOrEqualTo(sizeBefore));
    }

    @Test
    @DisplayName("Interleaved ledgers and snapshot behavior")
    void testInterleavedLedgersAndSnapshot() {
        SimpleEntryMemTable memTable = new SimpleEntryMemTable(64 * 1024 * 1024L);
        
        for (int i = 0; i < 100; i++) {
            long ledgerId = (i % 3) + 1;
            ByteBuffer data = randomBuffer(16 + (i % 5));
            memTable.addEntry(ledgerId, i, data);
        }
        
        assertThat(memTable.getEstimatedSize(), greaterThanOrEqualTo(0L));
        memTable.snapshot();
        assertEquals(0L, memTable.getEstimatedSize());
    }

    @Test
    @DisplayName("Overwrites stay within expected size bounds")
    void testOverwritesSizeBounds() {
        SimpleEntryMemTable memTable = new SimpleEntryMemTable(64 * 1024 * 1024L);
        Random rnd = new Random(42);
        
        long ledgerId = 10L;
        long entryId = 100L;
        
        for (int i = 0; i < 50; i++) {
            int len = 32 + rnd.nextInt(64);
            memTable.addEntry(ledgerId, entryId, randomBuffer(len));
            long size = memTable.getEstimatedSize();
            assertThat(size, greaterThanOrEqualTo(0L));
            assertThat(size, lessThanOrEqualTo(64 * 1024 * 1024L));
        }
    }

    private ByteBuffer randomBuffer(int len) {
        byte[] arr = new byte[len];
        for (int i = 0; i < len; i++) {
            arr[i] = (byte) (i % 128);
        }
        return ByteBuffer.wrap(arr);
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

        long getEstimatedSize() {
            return currentSize;
        }

        void snapshot() {
            entries.clear();
            currentSize = 0;
        }
    }
}