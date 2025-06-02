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

import static org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetListOfEntriesOfLedgerCallback;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;

class GetListOfEntriesOfLedgerCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.GetListOfEntriesOfLedgerCallback cb;

    public GetListOfEntriesOfLedgerCompletion(final CompletionKey key,
                                              final GetListOfEntriesOfLedgerCallback origCallback,
                                              final long ledgerId,
                                              PerChannelBookieClient perChannelBookieClient) {
        super("GetListOfEntriesOfLedger", null, ledgerId, 0L, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.getListOfEntriesOfLedgerCompletionOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.getListOfEntriesOfLedgerCompletionTimeoutOpLogger;
        this.cb = (rc, ledgerId1, availabilityOfEntriesOfLedger) -> {
            logOpResult(rc);
            origCallback.getListOfEntriesOfLedgerComplete(rc, ledgerId1, availabilityOfEntriesOfLedger);
            key.release();
        };
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        errorOutAndRunCallback(() -> cb.getListOfEntriesOfLedgerComplete(rc, ledgerId, null));
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.GetListOfEntriesOfLedgerResponse getListOfEntriesOfLedgerResponse = response
                .getGetListOfEntriesOfLedgerResponse();
        ByteBuf availabilityOfEntriesOfLedgerBuffer = Unpooled.EMPTY_BUFFER;
        BookkeeperProtocol.StatusCode status =
                response.getStatus() == BookkeeperProtocol.StatusCode.EOK ? getListOfEntriesOfLedgerResponse.getStatus()
                        : response.getStatus();

        if (getListOfEntriesOfLedgerResponse.hasAvailabilityOfEntriesOfLedger()) {
            availabilityOfEntriesOfLedgerBuffer = Unpooled.wrappedBuffer(
                    getListOfEntriesOfLedgerResponse.getAvailabilityOfEntriesOfLedger().asReadOnlyByteBuffer());
        }

        if (LOG.isDebugEnabled()) {
            logResponse(status, "ledgerId", ledgerId);
        }

        int rc = convertStatus(status, BKException.Code.ReadException);
        AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = null;
        if (rc == BKException.Code.OK) {
            availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    availabilityOfEntriesOfLedgerBuffer.slice());
        }
        cb.getListOfEntriesOfLedgerComplete(rc, ledgerId, availabilityOfEntriesOfLedger);
    }
}
