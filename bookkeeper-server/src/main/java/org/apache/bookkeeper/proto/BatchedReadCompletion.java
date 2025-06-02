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
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.util.ByteBufList;

class BatchedReadCompletion extends CompletionValue {

    final BookkeeperInternalCallbacks.BatchedReadEntryCallback cb;

    public BatchedReadCompletion(final CompletionKey key,
                                 final BookkeeperInternalCallbacks.BatchedReadEntryCallback originalCallback,
                                 final Object originalCtx,
                                 long ledgerId, final long entryId,
                                 PerChannelBookieClient perChannelBookieClient) {
        super("BatchedRead", originalCtx, ledgerId, entryId, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.readEntryOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.readTimeoutOpLogger;
        this.cb = (rc, ledgerId1, startEntryId, bufList, ctx) -> {
            logOpResult(rc);
            originalCallback.readEntriesComplete(rc,
                    ledgerId1, entryId,
                    bufList, originalCtx);
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
                () -> cb.readEntriesComplete(rc, ledgerId,
                        entryId, null, ctx));
    }

    @Override
    public void handleV2Response(long ledgerId,
                                 long entryId,
                                 BookkeeperProtocol.StatusCode status,
                                 BookieProtocol.Response response) {

        perChannelBookieClient.readEntryOutstanding.dec();
        if (!(response instanceof BookieProtocol.BatchedReadResponse)) {
            return;
        }
        BookieProtocol.BatchedReadResponse readResponse = (BookieProtocol.BatchedReadResponse) response;
        handleBatchedReadResponse(ledgerId, entryId, status, readResponse.getData(),
                INVALID_ENTRY_ID, -1L);
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        // V3 protocol haven't supported batched read yet.
    }

    private void handleBatchedReadResponse(long ledgerId,
                                           long entryId,
                                           BookkeeperProtocol.StatusCode status,
                                           ByteBufList buffers,
                                           long maxLAC, // max known lac piggy-back from bookies
                                           long lacUpdateTimestamp) { // the timestamp when the lac is updated.
        int rc = convertStatus(status, BKException.Code.ReadException);

        if (maxLAC > INVALID_ENTRY_ID && (ctx instanceof BookkeeperInternalCallbacks.ReadEntryCallbackCtx)) {
            ((BookkeeperInternalCallbacks.ReadEntryCallbackCtx) ctx).setLastAddConfirmed(maxLAC);
        }
        if (lacUpdateTimestamp > -1L && (ctx instanceof ReadLastConfirmedAndEntryContext)) {
            ((ReadLastConfirmedAndEntryContext) ctx).setLacUpdateTimestamp(lacUpdateTimestamp);
        }
        cb.readEntriesComplete(rc, ledgerId, entryId, buffers, ctx);
    }
}
