package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.TreeMap;

public class MockLedgerData {
    final long ledgerId;
    boolean isFenced;
    private TreeMap<Long, ByteBuf> entries = new TreeMap<>();
    MockLedgerData(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    boolean isFenced() { return isFenced; }
    void fence() { isFenced = true; }

    void addEntry(long entryId, ByteBuf entry) {
        entries.put(entryId, entry);
    }

    ByteBuf getEntry(long entryId) {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            Map.Entry<Long, ByteBuf> lastEntry = entries.lastEntry();
            if (lastEntry != null) {
                return lastEntry.getValue();
            } else {
                return null;
            }
        } else {
            return entries.get(entryId);
        }
    }
}
