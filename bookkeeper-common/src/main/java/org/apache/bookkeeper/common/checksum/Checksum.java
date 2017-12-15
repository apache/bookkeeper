/*
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
 */
package org.apache.bookkeeper.common.checksum;

import java.nio.ByteBuffer;

/**
 * An interface representing a data checksum.
 *
 * <p>The existence of this class is for adopting crc32 and crc32c in java8 and java9.
 */
public interface Checksum {

    /**
     * Updates the current checksum with the specified byte.
     *
     * @param b the byte to update the checksum with
     */
    void update(int b);

    /**
     * Updates the current checksum with the specified array of bytes.
     *
     * @implSpec This default implementation is equal to calling
     * {@code update(b, 0, b.length)}.
     *
     * @param b the array of bytes to update the checksum with
     *
     * @throws NullPointerException
     *         if {@code b} is {@code null}
     *
     * @since 9
     */
    default void update(byte[] b) {
        update(b, 0, b.length);
    }

    /**
     * Updates the current checksum with the specified array of bytes.
     *
     * @param b the byte array to update the checksum with
     * @param off the start offset of the data
     * @param len the number of bytes to use for the update
     */
    void update(byte[] b, int off, int len);

    /**
     * Updates the current checksum with the bytes from the specified buffer.
     *
     * <p>The checksum is updated with the remaining bytes in the buffer, starting
     * at the buffer's position. Upon return, the buffer's position will be
     * updated to its limit; its limit will not have been changed.
     *
     * @apiNote For best performance with DirectByteBuffer and other ByteBuffer
     * implementations without a backing array implementers of this interface
     * should override this method.
     *
     * @implSpec The default implementation has the following behavior.<br>
     * For ByteBuffers backed by an accessible byte array.
     * <pre>{@code
     * update(buffer.array(),
     *        buffer.position() + buffer.arrayOffset(),
     *        buffer.remaining());
     * }</pre>
     * For ByteBuffers not backed by an accessible byte array.
     * <pre>{@code
     * byte[] b = new byte[Math.min(buffer.remaining(), 4096)];
     * while (buffer.hasRemaining()) {
     *     int length = Math.min(buffer.remaining(), b.length);
     *     buffer.get(b, 0, length);
     *     update(b, 0, length);
     * }
     * }</pre>
     *
     * @param buffer the ByteBuffer to update the checksum with
     *
     * @throws NullPointerException
     *         if {@code buffer} is {@code null}
     *
     * @since 9
     */
    default void update(ByteBuffer buffer) {
        int pos = buffer.position();
        int limit = buffer.limit();
        assert (pos <= limit);
        int rem = limit - pos;
        if (rem <= 0) {
            return;
        }
        if (buffer.hasArray()) {
            update(buffer.array(), pos + buffer.arrayOffset(), rem);
        } else {
            byte[] b = new byte[Math.min(buffer.remaining(), 4096)];
            while (buffer.hasRemaining()) {
                int length = Math.min(buffer.remaining(), b.length);
                buffer.get(b, 0, length);
                update(b, 0, length);
            }
        }
        buffer.position(limit);
    }

    /**
     * Returns the current checksum value.
     *
     * @return the current checksum value
     */
    long getValue();

    /**
     * Resets the checksum to its initial value.
     */
    void reset();
}
