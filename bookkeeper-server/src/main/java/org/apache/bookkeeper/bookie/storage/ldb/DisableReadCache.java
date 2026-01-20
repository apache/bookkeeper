package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Disable read cache.
 */
public class DisableReadCache extends ReadCache {
    public DisableReadCache(ByteBufAllocator allocator, long maxCacheSize) {
        super(allocator, 0);
    }

    @Override
    public void put(long ledgerId, long entryId, ByteBuf entry) {
        // do nothing
    }

    @Override
    public ByteBuf get(long ledgerId, long entryId) {
        // do nothing
        return null;
    }

    @Override
    public boolean hasEntry(long ledgerId, long entryId) {
        // do nothing
        return false;
    }

    @Override
    public long size() {
        // do nothing
        return 0;
    }

    @Override
    public long count() {
        // do nothing
        return 0;
    }

    @Override
    public void close() {
        super.close();
    }
}
