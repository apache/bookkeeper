package org.apache.bookkeeper.bookie;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Demo test for EntryMemTable.
 * This is a simplified standalone test that demonstrates testing patterns.
 */
@DisplayName("EntryMemTable - Demo Tests")
class EntryMemTableDemoTest {

    private SimpleEntryMemTable entryTable;

    @BeforeEach
    void setUp() {
        entryTable = new SimpleEntryMemTable(10 * 1024 * 1024L);
    }

    @Test
    @DisplayName("New table should be empty")
    void testInitializationEmpty() {
        assertTrue(entryTable.isEmpty(), "Table should be empty initially");
        assertEquals(0, entryTable.getEstimatedSize(), "Size should be 0 initially");
    }

    @Test
    @DisplayName("Adding entry should increase size")
    void testAddEntryIncreasesSize() {
        entryTable.addEntry(1L, 0L, ByteBuffer.wrap("test data".getBytes()));
        
        assertFalse(entryTable.isEmpty(), "Table should not be empty after adding entry");
        assertThat(entryTable.getEstimatedSize(), greaterThan(0L));
    }

    @Test
    @DisplayName("Multiple entries in same ledger")
    void testMultipleEntriesSameLedger() {
        entryTable.addEntry(1L, 0L, ByteBuffer.wrap("entry0".getBytes()));
        entryTable.addEntry(1L, 1L, ByteBuffer.wrap("entry1".getBytes()));
        entryTable.addEntry(1L, 2L, ByteBuffer.wrap("entry2".getBytes()));

        assertFalse(entryTable.isEmpty());
        assertThat(entryTable.getEstimatedSize(), greaterThan(0L));
    }

    @Test
    @DisplayName("Entries from different ledgers")
    void testEntriesDifferentLedgers() {
        for (int i = 1; i <= 5; i++) {
            entryTable.addEntry((long)i, 0L, ByteBuffer.wrap(("ledger-" + i).getBytes()));
        }

        assertFalse(entryTable.isEmpty());
    }

    @Test
    @DisplayName("Snapshot creation clears table")
    void testSnapshotCreation() {
        entryTable.addEntry(1L, 0L, ByteBuffer.wrap("data".getBytes()));
        assertFalse(entryTable.isEmpty());

        entryTable.snapshot();

        assertTrue(entryTable.isEmpty(), "Table should be empty after snapshot");
    }

    /**
     * Simple in-memory entry table for testing
     */
    static class SimpleEntryMemTable {
        private final Map<String, ByteBuffer> entries = new HashMap<>();
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
