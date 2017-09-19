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
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandleAdv;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;

/**
 * Provide write access to a ledger. Using WriteAdvHandler the writer MUST explictly set an entryId
 *
 * @see WriteHandler
 */
public interface WriteAdvHandler extends ReadHandler {

    /**
     * Add entry asynchronously to an open ledger
     *
     * @param entryId entryId to be added
     * @param data array of bytes to be written to the ledger
     * @return an handle to the result, in case of success it will return the same value of param entryId
     */
    public CompletableFuture<Long> write(final long entryId, byte[] data);

    /**
     * Add entry asynchronously to an open ledger
     *
     * @param entryId entryId to be added.
     * @param data array of bytes to be written to the ledger
     * @param offset offset from which to take bytes from data
     * @param length number of bytes to take from data
     * @return an handle to the result, in case of success it will return the same value of param entryId
     */
    public CompletableFuture<Long> write(final long entryId, byte[] data, int offset, int length);

    /**
     * Add entry asynchronously to an open ledger. This can be used only with {@link LedgerHandleAdv} returned through
     * ledgers created with {@link
     * BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId entryId to be added
     * @param data array of bytes to be written
     * @return an handle to the result, in case of success it will return the same value of param entryId
     */
    public CompletableFuture<Long> write(final long entryId, final ByteBuf data);

}
