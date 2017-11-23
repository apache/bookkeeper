/**
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
package org.apache.bookkeeper.client.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * Provide write access to a ledger. Using WriteAdvHandler the writer MUST explictly set an entryId. Beware that the
 * write for a given entryId will be acknowledged if and only if all entries up to entryId - 1 have been acknowledged
 * too (expected from entryId 0)
 *
 * @see WriteHandle
 *
 * @since 4.6
 */
@Public
@Unstable
public interface WriteAdvHandle extends ReadHandle {

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param entryId entryId to be added
     * @param data array of bytes to be written
     * @return an handle to the result, in case of success it will return the same value of param entryId.
     */
    default CompletableFuture<Long> write(final long entryId, final ByteBuffer data) {
        return write(entryId, Unpooled.wrappedBuffer(data));
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param entryId entryId to be added.
     * @param data array of bytes to be written
     * @return an handle to the result, in case of success it will return the same value of param {@code entryId}.
     */
    default CompletableFuture<Long> write(final long entryId, final byte[] data) {
        return write(entryId, Unpooled.wrappedBuffer(data));
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param entryId entryId to  be added.
     * @param data array of bytes to be written
     * @param offset the offset of the bytes array
     * @param length the length to data to write
     * @return an handle to the result, in case of success it will return the same value of param {@code entryId}.
     */
    default CompletableFuture<Long> write(final long entryId, final byte[] data, int offset, int length) {
        return write(entryId, Unpooled.wrappedBuffer(data, offset, length));
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param entryId entryId to be added
     * @param data array of bytes to be written
     * @return an handle to the result, in case of success it will return the same value of param entryId
     */
    CompletableFuture<Long> write(final long entryId, final ByteBuf data);

}
