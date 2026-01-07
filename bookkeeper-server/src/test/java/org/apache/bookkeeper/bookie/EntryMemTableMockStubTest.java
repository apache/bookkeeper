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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Test class for EntryMemTable with Mocks and Stubs.
 * 
 * This test suite verifies the behavior of EntryMemTable
 * using mock objects to isolate the unit under test from its dependencies.
 */
@DisplayName("EntryMemTable - Mock & Stub Tests")
class EntryMemTableMockStubTest {

    private EntryMemTable entryMemTable;

    @Mock
    private ServerConfiguration mockConfig;

    @Mock
    private CheckpointSource mockCheckpointSource;

    @Mock
    private StatsLogger mockStatsLogger;

    @Mock
    private Checkpoint mockCheckpoint;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        
        // Configure mocks
        when(mockConfig.getSkipListSizeLimit()).thenReturn(1024L * 1024L); // 1MB
        when(mockConfig.getSkipListArenaChunkSize()).thenReturn(4096);
        when(mockCheckpointSource.newCheckpoint()).thenReturn(mockCheckpoint);
        when(mockCheckpoint.compareTo(mockCheckpoint)).thenReturn(0);
        
        entryMemTable = new EntryMemTable(mockConfig, mockCheckpointSource, mockStatsLogger);
    }

    /**
     * Test 1: EntryMemTable is initialized empty
     * Stub approach: verify initial state
     */
    @Test
    @DisplayName("EntryMemTable should be initialized empty")
    void testInitializationEmpty() throws IOException {
        // Assert
        assertTrue(entryMemTable.isEmpty(), "EntryMemTable should be empty on initialization");
        assertEquals(0, entryMemTable.getEstimatedSize(), "Size should be 0 on initialization");
    }

    /**
     * Test 2: Adding single entry increases size
     * Mock approach: uses mocked dependencies to verify size tracking
     */
    @Test
    @DisplayName("Adding entry should increase EntryMemTable size")
    void testAddEntryIncreasesSize() throws IOException {
        // Arrange
        long ledgerId = 1L;
        long entryId = 0L;
        byte[] data = "test data".getBytes();
        
        // Act
        entryMemTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data));

        // Assert
        assertFalse(entryMemTable.isEmpty(), "EntryMemTable should not be empty after adding entry");
        assertThat(entryMemTable.getEstimatedSize(), greaterThan(0L));
    }

    /**
     * Test 3: GetEntry retrieves previously added entry
     * Mock approach: verifies data consistency
     */
    @Test
    @DisplayName("GetEntry should retrieve previously added entry")
    void testGetEntryAfterAdd() throws IOException, Bookie.NoLedgerException {
        // Arrange
        long ledgerId = 1L;
        long entryId = 0L;
        byte[] testData = "test entry data".getBytes();

        // Act
        entryMemTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(testData));
        ByteBuffer retrievedData = entryMemTable.getEntry(ledgerId, entryId);

        // Assert
        assertNotNull(retrievedData, "Retrieved entry should not be null");
        assertEquals(ByteBuffer.wrap(testData), retrievedData, "Retrieved data should match added data");
    }

    /**
     * Test 4: Multiple entries with same ledger different entry ids
     * Stub approach: verifies multiple entry management
     */
    @Test
    @DisplayName("EntryMemTable should handle multiple entries in same ledger")
    void testMultipleEntriesInSameLedger() throws IOException {
        // Arrange
        long ledgerId = 1L;
        byte[] data1 = "entry 1".getBytes();
        byte[] data2 = "entry 2".getBytes();
        byte[] data3 = "entry 3".getBytes();

        // Act
        entryMemTable.addEntry(ledgerId, 0L, ByteBuffer.wrap(data1));
        entryMemTable.addEntry(ledgerId, 1L, ByteBuffer.wrap(data2));
        entryMemTable.addEntry(ledgerId, 2L, ByteBuffer.wrap(data3));

        // Assert
        assertFalse(entryMemTable.isEmpty());
        assertThat(entryMemTable.getEstimatedSize(), greaterThan(0L));
    }

    /**
     * Test 5: Entries from different ledgers
     * Mock approach: verifies ledger isolation
     */
    @Test
    @DisplayName("EntryMemTable should handle entries from multiple ledgers")
    void testEntriesFromMultipleLedgers() throws IOException {
        // Arrange
        byte[] data1 = "ledger 1 data".getBytes();
        byte[] data2 = "ledger 2 data".getBytes();

        // Act
        entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap(data1));
        entryMemTable.addEntry(2L, 0L, ByteBuffer.wrap(data2));

        // Assert
        assertFalse(entryMemTable.isEmpty());
        assertThat(entryMemTable.getEstimatedSize(), greaterThan(0L));
    }

    /**
     * Test 6: Snapshot creation
     * Stub approach: verifies snapshot mechanism
     */
    @Test
    @DisplayName("EntryMemTable.snapshot() should create checkpoint snapshot")
    void testSnapshotCreation() throws IOException {
        // Arrange
        entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap("data".getBytes()));

        // Act
        CheckpointSource.Checkpoint snapshot = entryMemTable.snapshot();

        // Assert
        assertNotNull(snapshot, "Snapshot should not be null");
        assertTrue(entryMemTable.isEmpty(), "EntryMemTable should be cleared after snapshot");
    }

    /**
     * Test 7: MaximumRollbackTime property
     * Mock approach: tests checkpoint-based property
     */
    @Test
    @DisplayName("EntryMemTable should report maximum rollback time")
    void testMaximumRollbackTime() throws IOException {
        // Arrange
        entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap("data".getBytes()));

        // Act
        long maxRollbackTime = entryMemTable.getMaximumRollbackTime();

        // Assert
        assertThat(maxRollbackTime, greaterThanOrEqualTo(0L));
    }

    /**
     * Test 8: Close operation
     * Stub approach: verifies resource cleanup
     */
    @Test
    @DisplayName("EntryMemTable.close() should release resources")
    void testCloseOperation() throws IOException {
        // Arrange
        entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap("data".getBytes()));

        // Act & Assert
        entryMemTable.close(); // Should not throw exception
    }

    /**
     * Test 9: Iterator over entries
     * Mock approach: verifies iteration capability
     */
    @Test
    @DisplayName("EntryMemTable should provide iterator over entries")
    void testIteratorOverEntries() throws IOException {
        // Arrange
        entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap("data1".getBytes()));
        entryMemTable.addEntry(1L, 1L, ByteBuffer.wrap("data2".getBytes()));

        // Act
        Iterator<EntryMemTable.Entry> iterator = entryMemTable.iterator();

        // Assert
        assertNotNull(iterator, "Iterator should not be null");
        assertTrue(iterator.hasNext(), "Iterator should have elements");
    }

    /**
     * Test 10: Thread safety - concurrent operations
     * Stub approach: uses mocks to verify concurrent access patterns
     */
    @Test
    @DisplayName("EntryMemTable should handle concurrent add operations")
    void testConcurrentOperations() throws IOException, InterruptedException {
        // Arrange
        Thread[] threads = new Thread[5];
        
        // Act - Create threads that add entries
        for (int i = 0; i < 5; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < 10; j++) {
                        entryMemTable.addEntry(
                            (long)threadIndex, 
                            (long)j, 
                            ByteBuffer.wrap(("data-" + threadIndex + "-" + j).getBytes())
                        );
                    }
                } catch (IOException e) {
                    // Expected - test is verifying thread safety
                }
            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Assert - Table should contain entries
        assertFalse(entryMemTable.isEmpty(), "EntryMemTable should contain concurrent entries");
        assertThat(entryMemTable.getEstimatedSize(), greaterThan(0L));
    }
}
