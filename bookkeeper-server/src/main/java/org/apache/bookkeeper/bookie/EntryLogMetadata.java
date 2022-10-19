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

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.function.LongPredicate;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;

/**
 * Records the total size, remaining size and the set of ledgers that comprise a
 * entry log.
 */
public class EntryLogMetadata {
    protected long entryLogId;
    protected long totalSize;
    protected long remainingSize;
    protected final ConcurrentLongLongHashMap ledgersMap;
    private static final short DEFAULT_SERIALIZATION_VERSION = 0;

    protected EntryLogMetadata() {
        ledgersMap = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(256)
                .concurrencyLevel(1)
                .build();
    }

    public EntryLogMetadata(long logId) {
        this();
        this.entryLogId = logId;

        totalSize = remainingSize = 0;
    }

    public void addLedgerSize(long ledgerId, long size) {
        totalSize += size;
        remainingSize += size;
        ledgersMap.addAndGet(ledgerId, size);
    }

    public boolean containsLedger(long ledgerId) {
        return ledgersMap.containsKey(ledgerId);
    }

    public double getUsage() {
        if (totalSize == 0L) {
            return 0.0f;
        }
        return (double) remainingSize / totalSize;
    }

    public boolean isEmpty() {
        return ledgersMap.isEmpty();
    }

    public long getEntryLogId() {
        return entryLogId;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getRemainingSize() {
        return remainingSize;
    }

    public ConcurrentLongLongHashMap getLedgersMap() {
        return ledgersMap;
    }

    public void removeLedgerIf(LongPredicate predicate) {
        ledgersMap.removeIf((ledgerId, size) -> {
            boolean shouldRemove = predicate.test(ledgerId);
            if (shouldRemove) {
                remainingSize -= size;
            }
            return shouldRemove;
        });
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{totalSize = ").append(totalSize).append(", remainingSize = ").append(remainingSize)
                .append(", ledgersMap = ").append(ledgersMap.toString()).append("}");
        return sb.toString();
    }

    /**
     * Serializes {@link EntryLogMetadata} and writes to
     * {@link DataOutputStream}.
     * <pre>
     * schema:
     * 2-bytes: schema-version
     * 8-bytes: entrylog-entryLogId
     * 8-bytes: entrylog-totalSize
     * 8-bytes: entrylog-remainingSize
     * 8-bytes: total number of ledgers
     * ledgers-map
     * [repeat]: (8-bytes::ledgerId, 8-bytes::size-of-ledger)
     * </pre>
     * @param out
     * @throws IOException
     *             throws if it couldn't serialize metadata-fields
     * @throws IllegalStateException
     *             throws if it couldn't serialize ledger-map
     */
    public void serialize(DataOutputStream out) throws IOException, IllegalStateException {
        out.writeShort(DEFAULT_SERIALIZATION_VERSION);
        out.writeLong(entryLogId);
        out.writeLong(totalSize);
        out.writeLong(remainingSize);
        out.writeLong(ledgersMap.size());
        ledgersMap.forEach((ledgerId, size) -> {
            try {
                out.writeLong(ledgerId);
                out.writeLong(size);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialize entryLogMetadata", e);
            }
        });
        out.flush();
    }

    /**
     * Deserializes {@link EntryLogMetadataRecyclable} from given {@link DataInputStream}.
     * Caller has to recycle returned {@link EntryLogMetadataRecyclable}.
     * @param in
     * @return
     * @throws IOException
     */
    public static EntryLogMetadataRecyclable deserialize(DataInputStream in) throws IOException {
        EntryLogMetadataRecyclable metadata = EntryLogMetadataRecyclable.get();
        try {
            short serVersion = in.readShort();
            if ((serVersion != DEFAULT_SERIALIZATION_VERSION)) {
                throw new IOException(String.format("%s. expected =%d, found=%d", "serialization version doesn't match",
                        DEFAULT_SERIALIZATION_VERSION, serVersion));
            }
            metadata.entryLogId = in.readLong();
            metadata.totalSize = in.readLong();
            metadata.remainingSize = in.readLong();
            long ledgersMapSize = in.readLong();
            for (int i = 0; i < ledgersMapSize; i++) {
                long ledgerId = in.readLong();
                long entryId = in.readLong();
                metadata.ledgersMap.put(ledgerId, entryId);
            }
            return metadata;
        } catch (IOException e) {
            metadata.recycle();
            throw e;
        } catch (Exception e) {
            metadata.recycle();
            throw new IOException(e);
        }
    }

    public void clear() {
        entryLogId = -1L;
        totalSize = -1L;
        remainingSize = -1L;
        ledgersMap.clear();
    }

    /**
     * Recyclable {@link EntryLogMetadata} class.
     *
     */
    public static class EntryLogMetadataRecyclable extends EntryLogMetadata {

        private final Handle<EntryLogMetadataRecyclable> recyclerHandle;

        private EntryLogMetadataRecyclable(Handle<EntryLogMetadataRecyclable> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<EntryLogMetadataRecyclable> RECYCLER =
                new Recycler<EntryLogMetadataRecyclable>() {
            protected EntryLogMetadataRecyclable newObject(Recycler.Handle<EntryLogMetadataRecyclable> handle) {
                return new EntryLogMetadataRecyclable(handle);
            }
        };

        public static EntryLogMetadataRecyclable get() {
            EntryLogMetadataRecyclable metadata = RECYCLER.get();
            return metadata;
        }

        public void recycle() {
            clear();
            recyclerHandle.recycle(this);
        }

    }
}
