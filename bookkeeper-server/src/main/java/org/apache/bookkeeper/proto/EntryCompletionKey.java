package org.apache.bookkeeper.proto;

import io.netty.util.Recycler;

class EntryCompletionKey extends CompletionKey {
    private final Recycler.Handle<EntryCompletionKey> recyclerHandle;
    long ledgerId;
    long entryId;

    static EntryCompletionKey acquireV2Key(long ledgerId, long entryId,
                                           BookkeeperProtocol.OperationType operationType) {
        EntryCompletionKey key = v2KeyRecycler.get();
        key.reset(ledgerId, entryId, operationType);
        return key;
    }

    private EntryCompletionKey(Recycler.Handle<EntryCompletionKey> handle) {
        super(null);
        this.recyclerHandle = handle;
    }

    void reset(long ledgerId, long entryId, BookkeeperProtocol.OperationType operationType) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.operationType = operationType;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof EntryCompletionKey)) {
            return  false;
        }
        EntryCompletionKey that = (EntryCompletionKey) object;
        return this.entryId == that.entryId
                && this.ledgerId == that.ledgerId
                && this.operationType == that.operationType;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(ledgerId) * 31 + Long.hashCode(entryId);
    }

    @Override
    public String toString() {
        return String.format("%d:%d %s", ledgerId, entryId, operationType);
    }

    public void release() {
        recyclerHandle.recycle(this);
    }

    private static final Recycler<EntryCompletionKey> v2KeyRecycler =
            new Recycler<EntryCompletionKey>() {
                @Override
                protected EntryCompletionKey newObject(
                        Recycler.Handle<EntryCompletionKey> handle) {
                    return new EntryCompletionKey(handle);
                }
            };
}
