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
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Provide write access to a ledger.
 *
 * @see WriteAdvHandle
 *
 * @since 4.6
 */
@Public
@Unstable
public interface WriteHandle extends ReadHandle, ForceableHandle {

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data a bytebuf to be written. The bytebuf's reference count will be decremented by 1 after the
     *             completable future is returned
     *             do not reuse the buffer, bk-client will release it appropriately.
     * @return an handle to the result, in case of success it will return the id of the newly appended entry
     */
    CompletableFuture<Long> appendAsync(ByteBuf data);

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data a bytebuf to be written. The bytebuf's reference count will be decremented by 1 after the
     *             call completes.
     *             do not reuse the buffer, bk-client will release it appropriately.
     * @return the id of the newly appended entry
     */
    default long append(ByteBuf data) throws BKException, InterruptedException {
        return FutureUtils.<Long, BKException>result(appendAsync(data), BKException.HANDLER);
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written
     *            do not reuse the buffer, bk-client will release it appropriately.
     * @return an handle to the result, in case of success it will return the id of the newly appended entry
     */
    default CompletableFuture<Long> appendAsync(ByteBuffer data) {
        return appendAsync(Unpooled.wrappedBuffer(data));
    }

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data array of bytes to be written
     *             do not reuse the buffer, bk-client will release it appropriately.
     * @return the id of the newly appended entry
     */
    default long append(ByteBuffer data) throws BKException, InterruptedException {
        return append(Unpooled.wrappedBuffer(data));
    }

    /**
     * Add an entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written
     *             do not reuse the buffer, bk-client will release it appropriately.
     * @return a completable future represents the add result, in case of success the future returns the entry id
     *         of this newly appended entry
     */
    default CompletableFuture<Long> appendAsync(byte[] data) {
        return appendAsync(Unpooled.wrappedBuffer(data));
    }

    /**
     * Add an entry synchronously to an open ledger.
     *
     * @param data array of bytes to be written
     *             do not reuse the buffer, bk-client will release it appropriately.
     * @return the entry id of this newly appended entry
     */
    default long append(byte[] data) throws BKException, InterruptedException {
        return append(Unpooled.wrappedBuffer(data));
    }

    /**
     * Add an entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written
     *             do not reuse the buffer, bk-client will release it appropriately.
     * @param offset the offset in the bytes array
     * @param length the length of the bytes to be appended
     * @return a completable future represents the add result, in case of success the future returns the entry id
     *         of this newly appended entry
     */
    default CompletableFuture<Long> appendAsync(byte[] data, int offset, int length) {
        return appendAsync(Unpooled.wrappedBuffer(data, offset, length));
    }

    /**
     * Add an entry synchronously to an open ledger.
     *
     * @param data array of bytes to be written
     *             do not reuse the buffer, bk-client will release it appropriately.
     * @param offset the offset in the bytes array
     * @param length the length of the bytes to be appended
     * @return the entry id of this newly appended entry
     */
    default long append(byte[] data, int offset, int length) throws BKException, InterruptedException {
        return append(Unpooled.wrappedBuffer(data, offset, length));
    }

    /**
     * Get the entry id of the last entry that has been enqueued for addition (but
     * may not have possibly been persisted to the ledger).
     *
     * @return the entry id of the last entry pushed or -1 if no entry has been pushed
     */
    long getLastAddPushed();

    /**
     * Asynchronous close the write handle, any adds in flight will return errors.
     *
     * <p>Closing a ledger will ensure that all clients agree on what the last
     * entry of the ledger is. Once the ledger has been closed, all reads from the
     * ledger will return the same set of entries.
     *
     * <p>The close operation can error if it finds conflicting metadata when it
     * tries to write to the metadata store. On close, the metadata state is set to
     * closed and lastEntry and length of the ledger are fixed in the metadata. A
     * conflict occurs if the metadata in the metadata store has a different value for
     * the lastEntry or length. If another process has updated the metadata, setting it
     * to closed, but have fixed the lastEntry and length to the same values as this
     * process is trying to write, the operation completes successfully.
     *
     * @return an handle to access the result of the operation
     */
    @Override
    CompletableFuture<Void> closeAsync();

    /**
     * Synchronous close the write handle, any adds in flight will return errors.
     *
     * <p>Closing a ledger will ensure that all clients agree on what the last
     * entry of the ledger is. Once the ledger has been closed, all reads from the
     * ledger will return the same set of entries.
     *
     * <p>The close operation can error if it finds conflicting metadata when it
     * tries to write to the metadata store. On close, the metadata state is set to
     * closed and lastEntry and length of the ledger are fixed in the metadata. A
     * conflict occurs if the metadata in the metadata store has a different value for
     * the lastEntry or length. If another process has updated the metadata, setting it
     * to closed, but have fixed the lastEntry and length to the same values as this
     * process is trying to write, the operation completes successfully.
     */
    @Override
    default void close() throws BKException, InterruptedException {
        FutureUtils.<Void, BKException>result(closeAsync(), BKException.HANDLER);
    }
}
