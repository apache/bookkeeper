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

package org.apache.bookkeeper.proto;

import static org.apache.bookkeeper.client.LedgerHandle.INVALID_ENTRY_ID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.apache.bookkeeper.client.BKException;

class ReadCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.ReadEntryCallback cb;

    public ReadCompletion(final CompletionKey key,
                          final BookkeeperInternalCallbacks.ReadEntryCallback originalCallback,
                          final Object originalCtx,
                          long ledgerId, final long entryId,
                          PerChannelBookieClient perChannelBookieClient) {
        super("Read", originalCtx, ledgerId, entryId, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.readEntryOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.readTimeoutOpLogger;
        this.cb = (rc, ledgerId1, entryId1, buffer, ctx) -> {
            logOpResult(rc);
            originalCallback.readEntryComplete(rc,
                    ledgerId1, entryId1,
                    buffer, originalCtx);
            key.release();
        };
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        errorOutAndRunCallback(
                () -> cb.readEntryComplete(rc, ledgerId,
                        entryId, null, ctx));
    }

    @Override
    public void setOutstanding() {
        perChannelBookieClient.readEntryOutstanding.inc();
    }

    @Override
    public void handleV2Response(long ledgerId, long entryId,
                                 BookkeeperProtocol.StatusCode status,
                                 BookieProtocol.Response response) {
        perChannelBookieClient.readEntryOutstanding.dec();
        if (!(response instanceof BookieProtocol.ReadResponse)) {
            return;
        }
        BookieProtocol.ReadResponse readResponse = (BookieProtocol.ReadResponse) response;
        handleReadResponse(ledgerId, entryId, status, readResponse.getData(),
                INVALID_ENTRY_ID, -1L);
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        perChannelBookieClient.readEntryOutstanding.dec();
        BookkeeperProtocol.ReadResponse readResponse = response.getReadResponse();
        BookkeeperProtocol.StatusCode status = response.getStatus() == BookkeeperProtocol.StatusCode.EOK
                ? readResponse.getStatus() : response.getStatus();
        ByteBuf buffer = Unpooled.EMPTY_BUFFER;
        if (readResponse.hasBody()) {
            buffer = Unpooled.wrappedBuffer(readResponse.getBody().asReadOnlyByteBuffer());
        }
        long maxLAC = INVALID_ENTRY_ID;
        if (readResponse.hasMaxLAC()) {
            maxLAC = readResponse.getMaxLAC();
        }
        long lacUpdateTimestamp = -1L;
        if (readResponse.hasLacUpdateTimestamp()) {
            lacUpdateTimestamp = readResponse.getLacUpdateTimestamp();
        }
        handleReadResponse(readResponse.getLedgerId(),
                readResponse.getEntryId(),
                status, buffer, maxLAC, lacUpdateTimestamp);
        ReferenceCountUtil.release(
                buffer); // meaningless using unpooled, but client may expect to hold the last reference
    }

    private void handleReadResponse(long ledgerId,
                                    long entryId,
                                    BookkeeperProtocol.StatusCode status,
                                    ByteBuf buffer,
                                    long maxLAC, // max known lac piggy-back from bookies
                                    long lacUpdateTimestamp) { // the timestamp when the lac is updated.
        int readableBytes = buffer.readableBytes();
        if (LOG.isDebugEnabled()) {
            logResponse(status, "ledger", ledgerId, "entry", entryId, "entryLength", readableBytes);
        }

        int rc = convertStatus(status, BKException.Code.ReadException);

        if (maxLAC > INVALID_ENTRY_ID && (ctx instanceof BookkeeperInternalCallbacks.ReadEntryCallbackCtx)) {
            ((BookkeeperInternalCallbacks.ReadEntryCallbackCtx) ctx).setLastAddConfirmed(maxLAC);
        }
        if (lacUpdateTimestamp > -1L && (ctx instanceof ReadLastConfirmedAndEntryContext)) {
            ((ReadLastConfirmedAndEntryContext) ctx).setLacUpdateTimestamp(lacUpdateTimestamp);
        }
        cb.readEntryComplete(rc, ledgerId, entryId, buffer.slice(), ctx);
    }
}
