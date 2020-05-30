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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * An entry Key/Value.
 * EntryKeyValue wraps a byte array and takes offsets and lengths into the array to
 * interpret the content as entry blob.
 */
public class EntryKeyValue extends EntryKey {
    private final byte [] bytes;
    private int offset = 0; // start offset of entry blob
    private int length = 0; // length of entry blob

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
     * Creates a EntryKeyValue from the start of the specified byte array.
     * Presumes <code>bytes</code> content contains the value portion of a EntryKeyValue.
     * @param bytes byte array
     */
    public EntryKeyValue(long ledgerId, long entryId, final byte [] bytes) {
        this(ledgerId, entryId, bytes, 0, bytes.length);
    }

    /**
     * Creates a EntryKeyValue from the start of the specified byte array.
     * Presumes <code>bytes</code> content contains the value portion of a EntryKeyValue.
     * @param bytes byte array
     * @param offset offset in bytes as start of blob
     * @param length of blob
     */
    public EntryKeyValue(long ledgerId, long entryId, final byte [] bytes, int offset, int length) {
        super(ledgerId, entryId);
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    /**
    * Returns the blob wrapped in a new <code>ByteBuffer</code>.
    *
    * @return the value
    */
    public ByteBuf getValueAsByteBuffer() {
        return Unpooled.wrappedBuffer(getBuffer(), getOffset(), getLength());
    }

    /**
    * Write EntryKeyValue blob into the provided byte buffer.
    *
    * @param dst the bytes buffer to use
    *
    * @return The number of useful bytes in the buffer.
    *
    * @throws IllegalArgumentException an illegal value was passed or there is insufficient space
    * remaining in the buffer
    */
    int writeToByteBuffer(ByteBuffer dst) {
        if (dst.remaining() < getLength()) {
            throw new IllegalArgumentException("Buffer size " + dst.remaining() + " < " + getLength());
        }

        dst.put(getBuffer(), getOffset(), getLength());
        return getLength();
    }

    /**
    * String representation.
    */
    @Override
    public String toString() {
        return ledgerId + ":" + entryId;
    }

    @Override
    public boolean equals(Object other) {
        // since this entry is identified by (lid, eid)
        // so just use {@link org.apache.bookkeeper.bookie.EntryKey#equals}.
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        // since this entry is identified by (lid, eid)
        // so just use {@link org.apache.bookkeeper.bookie.EntryKey#hashCode} as the hash code.
        return super.hashCode();
    }

}
