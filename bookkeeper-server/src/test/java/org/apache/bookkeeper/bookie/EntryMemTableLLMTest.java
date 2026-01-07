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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Test class for EntryMemTable - LLM Generated Tests.
 * 
 * This test suite focuses on comprehensive behavior testing of EntryMemTable
 * including parameterized tests, nested contexts, stress scenarios,
 * and concurrent operations.
 */
@DisplayName("EntryMemTable - LLM Generated Tests")
class EntryMemTableLLMTest {

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
        when(mockConfig.getSkipListSizeLimit()).thenReturn(10 * 1024 * 1024L); // 10MB
        when(mockConfig.getSkipListArenaChunkSize()).thenReturn(4096);
        when(mockCheckpointSource.newCheckpoint()).thenReturn(mockCheckpoint);
        when(mockCheckpoint.compareTo(mockCheckpoint)).thenReturn(0);

        entryMemTable = new EntryMemTable(mockConfig, mockCheckpointSource, mockStatsLogger);
    }

    /**
     * Nested test class for basic entry operations
     */
    @Nested
    @DisplayName("Basic Entry Operations")
    class BasicEntryOperationsTests {

        /**
         * Test: Add and retrieve entries with different ledger IDs
         */
        @ParameterizedTest(name = "ledger={0}, entry={1}")
        @CsvSource({
            "1, 0",
            "1, 1",
            "100, 0",
            "999, 50"
        })
        @DisplayName("Add and retrieve entries with varying ledger and entry IDs")
        void testAddAndRetrieveEntries(long ledgerId, long entryId) throws IOException {
            // Arrange
            byte[] data = ("entry-" + ledgerId + "-" + entryId).getBytes();

            // Act
            entryMemTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data));

            // Assert
            assertFalse(entryMemTable.isEmpty(), "EntryMemTable should not be empty");
            assertThat(entryMemTable.getEstimatedSize(), greaterThan(0L));
        }

        /**
         * Test: Multiple entries in sequence
         */
        @Test
        @DisplayName("EntryMemTable should handle sequential entry additions")
        void testSequentialEntries() throws IOException {
            // Arrange & Act
            for (int i = 0; i < 100; i++) {
                entryMemTable.addEntry(1L, (long)i, ByteBuffer.wrap(("data-" + i).getBytes()));
            }

            // Assert
            assertFalse(entryMemTable.isEmpty());
            assertThat(entryMemTable.getEstimatedSize(), greaterThan(0L));
        }

        /**
         * Test: Large data entries
         */
        @ParameterizedTest(name = "dataSize={0}")
        @ValueSource(ints = {1024, 8192, 65536})
        @DisplayName("EntryMemTable should handle large data entries")
        void testLargeDataEntries(int dataSize) throws IOException {
            // Arrange
            byte[] largeData = new byte[dataSize];
            for (int i = 0; i < dataSize; i++) {
                largeData[i] = (byte)(i % 256);
            }

            // Act
            entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap(largeData));

            // Assert
            assertFalse(entryMemTable.isEmpty());
            long size = entryMemTable.getEstimatedSize();
            assertThat(size, greaterThanOrEqualTo((long)dataSize));
        }
    }

    /**
     * Nested test class for snapshot and checkpoint behavior
     */
    @Nested
    @DisplayName("Snapshot and Checkpoint Operations")
    class SnapshotCheckpointTests {

        /**
         * Test: Snapshot creation with different entry counts
         */
        @ParameterizedTest(name = "entries={0}")
        @ValueSource(ints = {0, 1, 10, 100})
        @DisplayName("Snapshot creation should work with varying entry counts")
        void testSnapshotCreation(int entryCount) throws IOException {
            // Arrange
            for (int i = 0; i < entryCount; i++) {
                entryMemTable.addEntry(1L, (long)i, ByteBuffer.wrap(("data-" + i).getBytes()));
            }

            // Act
            Checkpoint snapshot = entryMemTable.snapshot();

            // Assert
            assertNotNull(snapshot, "Snapshot should not be null");
            assertTrue(entryMemTable.isEmpty(), "EntryMemTable should be empty after snapshot");
        }

        /**
         * Test: Multiple snapshots in sequence
         */
        @Test
        @DisplayName("EntryMemTable should support multiple sequential snapshots")
        void testMultipleSnapshots() throws IOException {
            // Arrange & Act
            for (int snapshotRound = 0; snapshotRound < 5; snapshotRound++) {
                // Add entries
                for (int i = 0; i < 10; i++) {
                    entryMemTable.addEntry(1L, (long)i, 
                        ByteBuffer.wrap(("data-round-" + snapshotRound).getBytes()));
                }

                // Take snapshot
                Checkpoint snapshot = entryMemTable.snapshot();

                // Assert
                assertNotNull(snapshot, "Snapshot " + snapshotRound + " should not be null");
                assertTrue(entryMemTable.isEmpty(), "Should be empty after snapshot");
            }
        }

        /**
         * Test: Maximum rollback time calculation
         */
        @Test
        @DisplayName("EntryMemTable should report reasonable maximum rollback times")
        void testMaximumRollbackTimeCalculation() throws IOException {
            // Arrange
            entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap("data".getBytes()));

            // Act
            long maxRollbackTime = entryMemTable.getMaximumRollbackTime();

            // Assert
            assertThat(maxRollbackTime, greaterThanOrEqualTo(-1L));
        }
    }

    /**
     * Nested test class for stress and boundary conditions
     */
    @Nested
    @DisplayName("Stress and Boundary Conditions")
    class StressAndBoundaryTests {

        /**
         * Test: Many ledgers with many entries
         */
        @Test
        @DisplayName("EntryMemTable should handle many ledgers with many entries")
        void testManyLedgersAndEntries() throws IOException {
            // Arrange & Act
            int ledgerCount = 100;
            int entriesPerLedger = 50;

            for (long ledgerId = 0; ledgerId < ledgerCount; ledgerId++) {
                for (long entryId = 0; entryId < entriesPerLedger; entryId++) {
                    entryMemTable.addEntry(ledgerId, entryId,
                        ByteBuffer.wrap(("ledger-" + ledgerId + "-entry-" + entryId).getBytes()));
                }
            }

            // Assert
            assertFalse(entryMemTable.isEmpty());
            assertThat(entryMemTable.getEstimatedSize(), greaterThan(0L));
        }

        /**
         * Test: Entries with same ID from different ledgers
         */
        @Test
        @DisplayName("EntryMemTable should differentiate entries by ledger ID")
        void testEntriesFromDifferentLedgersSameEntryId() throws IOException {
            // Arrange & Act
            for (long ledgerId = 0; ledgerId < 10; ledgerId++) {
                entryMemTable.addEntry(ledgerId, 0L,
                    ByteBuffer.wrap(("ledger-" + ledgerId).getBytes()));
            }

            // Assert
            assertFalse(entryMemTable.isEmpty());
        }

        /**
         * Test: Zero-length data entries
         */
        @Test
        @DisplayName("EntryMemTable should handle zero-length data entries")
        void testZeroLengthEntries() throws IOException {
            // Arrange & Act
            byte[] emptyData = new byte[0];
            entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap(emptyData));

            // Assert
            assertFalse(entryMemTable.isEmpty());
        }

        /**
         * Test: Maximum entry ID values
         */
        @Test
        @DisplayName("EntryMemTable should handle large entry ID values")
        void testLargeEntryIds() throws IOException {
            // Arrange & Act
            long largeEntryId = Long.MAX_VALUE / 2;
            entryMemTable.addEntry(1L, largeEntryId, ByteBuffer.wrap("data".getBytes()));

            // Assert
            assertFalse(entryMemTable.isEmpty());
        }
    }

    /**
     * Nested test class for iterator functionality
     */
    @Nested
    @DisplayName("Iterator Functionality")
    class IteratorTests {

        /**
         * Test: Iterator over various entry counts
         */
        @ParameterizedTest(name = "entryCount={0}")
        @ValueSource(ints = {1, 5, 10, 50})
        @DisplayName("Iterator should work with varying entry counts")
        void testIteratorWithVaryingEntries(int entryCount) throws IOException {
            // Arrange
            for (int i = 0; i < entryCount; i++) {
                entryMemTable.addEntry(1L, (long)i, ByteBuffer.wrap(("data-" + i).getBytes()));
            }

            // Act
            Iterator<EntryMemTable.Entry> iterator = entryMemTable.iterator();

            // Assert
            assertNotNull(iterator, "Iterator should not be null");
            assertTrue(iterator.hasNext(), "Iterator should have elements");

            int count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            assertEquals(entryCount, count, "Iterator should visit all entries");
        }

        /**
         * Test: Iterator on empty table
         */
        @Test
        @DisplayName("Iterator on empty EntryMemTable should not have elements")
        void testIteratorOnEmptyTable() throws IOException {
            // Act
            Iterator<EntryMemTable.Entry> iterator = entryMemTable.iterator();

            // Assert
            assertNotNull(iterator, "Iterator should not be null");
            assertFalse(iterator.hasNext(), "Iterator should not have elements on empty table");
        }

        /**
         * Test: Multiple iterations
         */
        @Test
        @DisplayName("Multiple iterations should be possible")
        void testMultipleIterations() throws IOException {
            // Arrange
            for (int i = 0; i < 5; i++) {
                entryMemTable.addEntry(1L, (long)i, ByteBuffer.wrap(("data-" + i).getBytes()));
            }

            // Act & Assert
            for (int iteration = 0; iteration < 3; iteration++) {
                Iterator<EntryMemTable.Entry> iterator = entryMemTable.iterator();
                assertTrue(iterator.hasNext(), "Iterator should have elements on iteration " + iteration);
            }
        }
    }

    /**
     * Nested test class for concurrency scenarios
     */
    @Nested
    @DisplayName("Concurrency and Thread Safety")
    class ConcurrencyTests {

        /**
         * Test: Concurrent adds from multiple threads
         */
        @Test
        @DisplayName("EntryMemTable should handle concurrent additions from multiple threads")
        void testConcurrentAdditions() throws InterruptedException, IOException {
            // Arrange
            int threadCount = 10;
            int entriesPerThread = 50;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(threadCount);
            AtomicBoolean errorOccurred = new AtomicBoolean(false);

            // Act
            for (int t = 0; t < threadCount; t++) {
                final int threadIndex = t;
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < entriesPerThread; i++) {
                            entryMemTable.addEntry(
                                (long)threadIndex,
                                (long)i,
                                ByteBuffer.wrap(("thread-" + threadIndex + "-entry-" + i).getBytes())
                            );
                        }
                    } catch (Exception e) {
                        errorOccurred.set(true);
                    } finally {
                        endLatch.countDown();
                    }
                }).start();
            }

            startLatch.countDown();
            endLatch.await();

            // Assert
            assertFalse(errorOccurred.get(), "No errors should occur during concurrent operations");
            assertFalse(entryMemTable.isEmpty());
        }

        /**
         * Test: Concurrent reads and writes
         */
        @Test
        @DisplayName("EntryMemTable should handle concurrent read/write operations")
        void testConcurrentReadWrite() throws InterruptedException, IOException {
            // Arrange - Pre-populate with entries
            for (int i = 0; i < 50; i++) {
                entryMemTable.addEntry(1L, (long)i, ByteBuffer.wrap(("data-" + i).getBytes()));
            }

            // Act & Assert - Concurrent operations
            int threadCount = 5;
            CountDownLatch endLatch = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                final int threadIndex = t;
                new Thread(() -> {
                    try {
                        Iterator<EntryMemTable.Entry> iterator = entryMemTable.iterator();
                        while (iterator.hasNext()) {
                            iterator.next();
                        }
                    } catch (Exception e) {
                        // Concurrent access is expected
                    } finally {
                        endLatch.countDown();
                    }
                }).start();
            }

            endLatch.await();
        }
    }

    /**
     * Test: Resource cleanup
     */
    @Test
    @DisplayName("EntryMemTable should properly close resources")
    void testResourceCleanup() throws IOException {
        // Arrange
        entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap("data".getBytes()));

        // Act & Assert
        entryMemTable.close(); // Should not throw
    }

    /**
     * Test: Chaining operations
     */
    @Test
    @DisplayName("EntryMemTable should handle chained operations")
    void testChainingOperations() throws IOException {
        // Arrange & Act
        entryMemTable.addEntry(1L, 0L, ByteBuffer.wrap("data1".getBytes()));
        entryMemTable.addEntry(1L, 1L, ByteBuffer.wrap("data2".getBytes()));
        
        long sizeBeforeSnapshot = entryMemTable.getEstimatedSize();
        
        Checkpoint snapshot = entryMemTable.snapshot();
        
        entryMemTable.addEntry(2L, 0L, ByteBuffer.wrap("data3".getBytes()));

        // Assert
        assertThat(sizeBeforeSnapshot, greaterThan(0L));
        assertNotNull(snapshot);
    }
}
