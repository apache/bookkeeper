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
 * Provide write access to a ledger.
 *
 * @see WriteAdvHandle
 *
 * @since 4.6
 */
@Public
@Unstable
public interface WriteHandle extends ReadHandle {

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data a bytebuf to be written. The bytebuf's reference count will be decremented by 1 after the
     *             completable future is returned
     * @return an handle to the result, in case of success it will return the id of the newly appended entry
     */
    CompletableFuture<Long> append(ByteBuf data);

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written
     * @return an handle to the result, in case of success it will return the id of the newly appended entry
     */
    default CompletableFuture<Long> append(ByteBuffer data) {
        return append(Unpooled.wrappedBuffer(data));
    }

    /**
     * Add an entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written
     * @return a completable future represents the add result, in case of success the future returns the entry id
     *         of this newly appended entry
     */
    default CompletableFuture<Long> append(byte[] data) {
        return append(Unpooled.wrappedBuffer(data));
    }

    /**
     * Add an entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written
     * @param offset the offset in the bytes array
     * @param length the length of the bytes to be appended
     * @return a completable future represents the add result, in case of success the future returns the entry id
     *         of this newly appended entry
     */
    default CompletableFuture<Long> append(byte[] data, int offset, int length) {
        return append(Unpooled.wrappedBuffer(data, offset, length));
    }

    /**
     * Get the entry id of the last entry that has been enqueued for addition (but
     * may not have possibly been persisted to the ledger).
     *
     * @return the entry id of the last entry pushed or -1 if no entry has been pushed
     */
    long getLastAddPushed();

}
