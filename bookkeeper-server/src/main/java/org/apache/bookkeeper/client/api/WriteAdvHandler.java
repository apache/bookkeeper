/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.client.api;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandleAdv;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;

/**
 * Provide write access to a ledger. Using WriteAdvHandler the writer MUST explictly set an entryId
 *
 * @see WriteHandler
 */
public interface WriteAdvHandler extends ReadHandler {

    /**
     * Add entry synchronously to an open ledger
     *
     * @param entryId entryId to be added
     * @param data array of bytes to be written to the ledger
     * @return the entryId of the new inserted entry
     */
    public long addEntry(final long entryId, byte[] data) throws InterruptedException, BKException;

    /**
     * Add entry synchronously to an open ledger. This can be used only with {@link LedgerHandleAdv} returned through
     * ledgers created with {@link
     * BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId entryId to be added.
     * @param data array of bytes to be written to the ledger
     * @param offset offset from which to take bytes from data
     * @param length number of bytes to take from data
     * @return entryId
     */
    public long addEntry(final long entryId, byte[] data, int offset, int length) throws InterruptedException,
        BKException;

    /**
     * Add entry asynchronously to an open ledger. This can be used only with {@link LedgerHandleAdv} returned through
     * ledgers created with {@link
     * BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId entryId to be added
     * @param data array of bytes to be written
     * @param cb object implementing callbackinterface
     * @param ctx some control object
     */
    public void asyncAddEntry(final long entryId, final byte[] data, final AsyncCallback.AddCallback cb, final Object ctx);

    /**
     * Add entry asynchronously to an open ledger, using an offset and range. This can be used only with
     * {@link LedgerHandleAdv} returned through ledgers created with
     * {@link BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId entryId of the entry to add.
     * @param data array of bytes to be written
     * @param offset offset from which to take bytes from data
     * @param length number of bytes to take from data
     * @param cb object implementing callbackinterface
     * @param ctx some control object
     * @throws ArrayIndexOutOfBoundsException if offset or length is negative or offset and length sum to a value higher
     * than the length of data.
     */
    public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length,
        final AsyncCallback.AddCallback cb, final Object ctx) throws BKException;

    /**
     * Add entry asynchronously to an open ledger. This can be used only with {@link LedgerHandleAdv} returned through
     * ledgers created with {@link
     * BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId entryId to be added
     * @param data array of bytes to be written
     * @param cb object implementing callbackinterface
     * @param ctx some control object
     */
    public void asyncAddEntry(final long entryId, final ByteBuf data, final AsyncCallback.AddCallback cb, final Object ctx);

}
