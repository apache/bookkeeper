/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.util.BookKeeperConstants.DEAD_ID;

import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCounted;

/**
 * An entry Key/Value.
 * EntryKeyValue wraps a byte array and takes offsets and lengths into the array to
 * interpret the content as entry blob.
 */
class EntryKeyValue extends AbstractReferenceCounted implements EntryKey {

    private static final Recycler<EntryKeyValue> RECYCLER = new Recycler<EntryKeyValue>() {
        @Override
        protected EntryKeyValue newObject(Handle<EntryKeyValue> handle) {
            return new EntryKeyValue(handle);
        }
    };

    /**
     * Creates a EntryKeyValue from the start of the specified byte array.
     * Presumes <code>bytes</code> content contains the value portion of a EntryKeyValue.
     * @param bytes byte array
     */
    public static EntryKeyValue of(long ledgerId, long entryId, final byte [] bytes) {
        return of(ledgerId, entryId, bytes, 0, bytes.length);
    }

    /**
     * Creates a EntryKeyValue from the start of the specified byte array.
     * Presumes <code>bytes</code> content contains the value portion of a EntryKeyValue.
     * @param bytes byte array
     * @param offset offset in bytes as start of blob
     * @param length of blob
     */
    public static EntryKeyValue of(long ledgerId, long entryId, final byte [] bytes, int offset, int length) {
        EntryKeyValue kv = RECYCLER.get();
        kv.setRefCnt(1);

        kv.ledgerId = ledgerId;
        kv.entryId = entryId;
        kv.bytes = bytes;
        kv.offset = offset;
        kv.length = length;

        return kv;
    }

    private final Handle<EntryKeyValue> handle;
    private long ledgerId;
    private long entryId;
    private byte [] bytes;
    private int offset = 0; // start offset of entry blob
    private int length = 0; // length of entry blob

    private EntryKeyValue(Handle<EntryKeyValue> handle) {
        this.handle = handle;
    }

    /**
    * @return The byte array backing this EntryKeyValue.
    */
    public byte [] getBuffer() {
        return this.bytes;
    }

    /**
    * @return Offset into {@link #getBuffer()} at which the EntryKeyValue starts.
    */
    public int getOffset() {
        return this.offset;
    }

    /**
    * @return Length of bytes this EntryKeyValue occupies in {@link #getBuffer()}.
    */
    public int getLength() {
        return this.length;
    }

    /**
    * Returns the blob wrapped in a new <code>ByteBuffer</code>.
    *
    * @return the value
    */
    public ByteBuf getValueAsByteBuffer() {
        return Unpooled.wrappedBuffer(getBuffer(), getOffset(), getLength());
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EntryKey)) {
          return false;
        }
        EntryKey key = (EntryKey) other;
        return getLedgerId() == key.getLedgerId() && getEntryId() == key.getEntryId();
    }

    @Override
    public int hashCode() {
        return (int) (getLedgerId() * 13 ^ getEntryId() * 17);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(EntryKey.class)
            .add("lid", getLedgerId())
            .add("eid", getEntryId())
            .toString();
    }

    @Override
    protected void deallocate() {
        this.ledgerId = DEAD_ID;
        this.entryId = DEAD_ID;
        handle.recycle(this);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }
}
