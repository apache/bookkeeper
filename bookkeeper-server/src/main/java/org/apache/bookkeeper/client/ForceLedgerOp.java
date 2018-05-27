/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a request to sync the ledger on every bookie.
 */
class ForceLedgerOp extends SafeRunnable implements ForceLedgerCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ForceLedgerOp.class);
    final CompletableFuture<Void> cb;
    BitSet receivedResponseSet;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;
    int lastSeenError = BKException.Code.WriteException;
    ArrayList<BookieSocketAddress> currentEnsemble;

    long currentNonDurableLastAddConfirmed = LedgerHandle.INVALID_ENTRY_ID;

    final LedgerHandle lh;

    ForceLedgerOp(LedgerHandle lh, CompletableFuture<Void> cb) {
        this.lh = lh;
        this.cb = cb;
    }

    void sendForceLedgerRequest(int bookieIndex) {
        lh.bk.getBookieClient().forceLedger(currentEnsemble.get(bookieIndex), lh.ledgerId, this, bookieIndex);
    }

    @Override
    public void safeRun() {
        initiate();
    }

    void initiate() {

        // capture currentNonDurableLastAddConfirmed
        // remember that we are inside OrderedExecutor, this induces a strict ordering
        // on the sequence of events
        this.currentNonDurableLastAddConfirmed = lh.pendingAddsSequenceHead;
        LOG.debug("start force() on ledger {} capturing {} ", lh.ledgerId, currentNonDurableLastAddConfirmed);

        // we need to send the request to every bookie in the ensamble
        this.currentEnsemble = lh.metadata.currentEnsemble;
        this.ackSet = lh.distributionSchedule.getAckSetForForceLedger();
        this.receivedResponseSet = new BitSet(
                lh.getLedgerMetadata().getEnsembleSize());
        this.receivedResponseSet.set(0,
                lh.getLedgerMetadata().getEnsembleSize());

        DistributionSchedule.WriteSet writeSet = lh.distributionSchedule.getWriteSetForForceLedger();
        try {
            for (int i = 0; i < writeSet.size(); i++) {
                sendForceLedgerRequest(writeSet.get(i));
            }
        } finally {
            writeSet.recycle();
        }
    }

    @Override
    public void forceLedgerComplete(int rc, long ledgerId, BookieSocketAddress addr, Object ctx) {
        int bookieIndex = (Integer) ctx;

        if (completed) {
            return;
        }

        if (BKException.Code.OK != rc) {
            lastSeenError = rc;
        }

        // We got response.
        receivedResponseSet.clear(bookieIndex);

        if (rc == BKException.Code.OK) {
            if (ackSet.completeBookieAndCheck(bookieIndex) && !completed) {
                completed = true;
                // we are able to say that every bookie sync'd its own journal
                // for every ackknowledged entry before issuing the force() call
                LOG.debug("After force on ledger {} updating LastAddConfirmed to {} ",
                        ledgerId, currentNonDurableLastAddConfirmed);
                lh.lastAddConfirmed = currentNonDurableLastAddConfirmed;
                FutureUtils.complete(cb, null);
                return;
            }
        } else {
            LOG.warn("ForceLedger did not succeed: Ledger {} on {}", new Object[]{ledgerId, addr});
        }

        if (receivedResponseSet.isEmpty()) {
            completed = true;
            FutureUtils.completeExceptionally(cb, BKException.create(lastSeenError));
        }
    }
}
