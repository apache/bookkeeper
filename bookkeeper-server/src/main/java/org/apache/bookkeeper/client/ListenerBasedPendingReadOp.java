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
package org.apache.bookkeeper.client;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.apache.bookkeeper.util.MathUtils;

@Slf4j
class ListenerBasedPendingReadOp extends PendingReadOp {

    final ReadEntryListener listener;
    final Object ctx;

    ListenerBasedPendingReadOp(LedgerHandle lh,
                               ClientContext clientCtx,
                               long startEntryId,
                               long endEntryId,
                               ReadEntryListener listener,
                               Object ctx,
                               boolean isRecoveryRead) {
        super(lh, clientCtx, startEntryId, endEntryId, isRecoveryRead);
        this.listener = listener;
        this.ctx = ctx;
    }

    @Override
    protected void submitCallback(int code) {
        LedgerEntryRequest request;
        while (!seq.isEmpty() && (request = seq.get(0)) != null) {
            if (!request.isComplete()) {
                return;
            }
            seq.remove(0);
            long latencyNanos = MathUtils.elapsedNanos(requestTimeNanos);
            LedgerEntry entry;
            if (BKException.Code.OK == request.getRc()) {
                clientCtx.getClientStats().getReadOpLogger()
                    .registerSuccessfulEvent(latencyNanos, TimeUnit.NANOSECONDS);
                // callback with completed entry
                entry = new LedgerEntry(request.entryImpl);
            } else {
                clientCtx.getClientStats().getReadOpLogger()
                    .registerFailedEvent(latencyNanos, TimeUnit.NANOSECONDS);
                entry = null;
            }
            request.close();
            listener.onEntryComplete(request.getRc(), lh, entry, ctx);
        }
        // if all entries are already completed.
        cancelSpeculativeTask(true);
    }

}
