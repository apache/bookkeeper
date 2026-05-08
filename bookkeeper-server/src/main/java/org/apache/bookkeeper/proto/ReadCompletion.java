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
                                 StatusCode status,
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
    public void handleV3Response(Response response) {
        perChannelBookieClient.readEntryOutstanding.dec();
        long respLedgerId = ledgerId;
        long respEntryId = entryId;
        StatusCode status;
        ByteBuf buffer = Unpooled.EMPTY_BUFFER;
        long maxLAC = INVALID_ENTRY_ID;
        long lacUpdateTimestamp = -1L;
        if (response.getStatus() == StatusCode.EOK && response.hasReadResponse()) {
            ReadResponse readResponse = response.getReadResponse();
            status = readResponse.getStatus();
            // For long-poll reads the request entryId is LAST_ADD_CONFIRMED
            // and the server fills in the actual entry id alongside the body.
            if (readResponse.hasLedgerId()) {
                respLedgerId = readResponse.getLedgerId();
            }
            if (readResponse.hasEntryId()) {
                respEntryId = readResponse.getEntryId();
            }
            if (readResponse.hasBody()) {
                buffer = readResponse.getBodySlice();
            }
            if (readResponse.hasMaxLAC()) {
                maxLAC = readResponse.getMaxLAC();
            }
            if (readResponse.hasLacUpdateTimestamp()) {
                lacUpdateTimestamp = readResponse.getLacUpdateTimestamp();
            }
        } else {
            // Error responses may not carry a populated ReadResponse;
            // fall back to the request's recorded ledgerId/entryId.
            status = response.getStatus();
        }
        handleReadResponse(respLedgerId, respEntryId, status, buffer, maxLAC, lacUpdateTimestamp);
        ReferenceCountUtil.release(
                buffer); // meaningless using unpooled, but client may expect to hold the last reference
    }

    private void handleReadResponse(long ledgerId,
                                    long entryId,
                                    StatusCode status,
                                    ByteBuf buffer,
                                    long maxLAC, // max known lac piggy-back from bookies
                                    long lacUpdateTimestamp) { // the timestamp when the lac is updated.
        logEvent(status)
                .attr("entryLength", buffer.readableBytes())
                .log("Got response from bookie");

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
