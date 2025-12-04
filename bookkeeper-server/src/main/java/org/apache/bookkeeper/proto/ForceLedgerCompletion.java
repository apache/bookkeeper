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

import org.apache.bookkeeper.client.BKException;

class ForceLedgerCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.ForceLedgerCallback cb;

    public ForceLedgerCompletion(final CompletionKey key,
                                 final BookkeeperInternalCallbacks.ForceLedgerCallback originalCallback,
                                 final Object originalCtx,
                                 final long ledgerId,
                                 PerChannelBookieClient perChannelBookieClient) {
        super("ForceLedger",
                originalCtx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.forceLedgerOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.forceLedgerTimeoutOpLogger;
        this.cb = (rc, ledgerId1, addr, ctx) -> {
            logOpResult(rc);
            originalCallback.forceLedgerComplete(rc, ledgerId1,
                    addr, originalCtx);
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
                () -> cb.forceLedgerComplete(rc, ledgerId, perChannelBookieClient.bookieId, ctx));
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.ForceLedgerResponse forceLedgerResponse = response.getForceLedgerResponse();
        BookkeeperProtocol.StatusCode status = response.getStatus() == BookkeeperProtocol.StatusCode.EOK
                ? forceLedgerResponse.getStatus() : response.getStatus();
        long ledgerId = forceLedgerResponse.getLedgerId();

        if (LOG.isDebugEnabled()) {
            logResponse(status, "ledger", ledgerId);
        }
        int rc = convertStatus(status, BKException.Code.WriteException);
        cb.forceLedgerComplete(rc, ledgerId, perChannelBookieClient.bookieId, ctx);
    }
}
