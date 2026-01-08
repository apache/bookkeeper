package org.apache.bookkeeper.bookie;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple in-memory entry table for testing purposes.
 */
public class SimpleEntryMemTable {
    private final Map<String, ByteBuffer> entries = new HashMap<>();
    private final long maxSize;
    private long currentSize = 0;

    public SimpleEntryMemTable(long maxSize) {
        this.maxSize = maxSize;
    }

    public void addEntry(long ledgerId, long entryId, ByteBuffer data) {
        String key = ledgerId + ":" + entryId;
        entries.put(key, data);
        currentSize += data.remaining();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public long getEstimatedSize() {
        return currentSize;
    }

    public void snapshot() {
        entries.clear();
        currentSize = 0;
    }
}
