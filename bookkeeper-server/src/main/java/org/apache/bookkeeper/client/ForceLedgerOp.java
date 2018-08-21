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

import static com.google.common.base.Preconditions.checkState;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
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

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;
    boolean errored = false;
    int lastSeenError = BKException.Code.WriteException;
    final List<BookieSocketAddress> currentEnsemble;

    long currentNonDurableLastAddConfirmed = LedgerHandle.INVALID_ENTRY_ID;

    final LedgerHandle lh;
    final BookieClient bookieClient;

    ForceLedgerOp(LedgerHandle lh, BookieClient bookieClient,
                  List<BookieSocketAddress> ensemble,
                  CompletableFuture<Void> cb) {
        this.lh = lh;
        this.bookieClient = bookieClient;
        this.cb = cb;
        this.currentEnsemble = ensemble;
    }

    void sendForceLedgerRequest(int bookieIndex) {
        bookieClient.forceLedger(currentEnsemble.get(bookieIndex), lh.ledgerId, this, bookieIndex);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("force {} clientNonDurableLac {}", lh.ledgerId, currentNonDurableLastAddConfirmed);
        }
        // we need to send the request to every bookie in the ensamble
        this.ackSet = lh.distributionSchedule.getEnsembleAckSet();

        DistributionSchedule.WriteSet writeSet = lh.getDistributionSchedule()
                                                   .getEnsembleSet(currentNonDurableLastAddConfirmed);
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

        checkState(!completed, "We are waiting for all the bookies, it is not expected an early exit");

        if (errored) {
            // already failed, do not fire error callbacks twice
            return;
        }

        if (BKException.Code.OK != rc) {
            lastSeenError = rc;
        }

        if (rc == BKException.Code.OK) {
            if (ackSet.completeBookieAndCheck(bookieIndex)) {
                completed = true;
                // we are able to say that every bookie sync'd its own journal
                // for every ackknowledged entry before issuing the force() call
                if (LOG.isDebugEnabled()) {
                    LOG.debug("After force on ledger {} updating LastAddConfirmed to {} ",
                              ledgerId, currentNonDurableLastAddConfirmed);
                }
                lh.updateLastConfirmed(currentNonDurableLastAddConfirmed, lh.getLength());
                FutureUtils.complete(cb, null);
            }
        } else {
            // at least one bookie failed, as we are waiting for all the bookies
            // we can fail immediately
            LOG.info("ForceLedger did not succeed: Ledger {} on {}", ledgerId, addr);
            errored = true;

            // notify the failure
            FutureUtils.completeExceptionally(cb, BKException.create(lastSeenError));
        }

    }
}
