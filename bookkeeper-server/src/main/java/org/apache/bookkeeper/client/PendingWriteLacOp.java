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

import java.util.BitSet;
import java.util.List;

import org.apache.bookkeeper.client.AsyncCallback.AddLacCallback;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.util.ByteBufList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a pending WriteLac operation. When it has got
 * success from Ack Quorum bookies, sends success back to the application,
 * otherwise failure is sent back to the caller.
 *
 * <p>This is an optional protocol operations to facilitate tailing readers
 * to be up to date with the writer. This is best effort to get latest LAC
 * from bookies, and doesn't affect the correctness of the protocol.
 */
class PendingWriteLacOp implements WriteLacCallback {
    private static final Logger LOG = LoggerFactory.getLogger(PendingWriteLacOp.class);
    ByteBufList toSend;
    AddLacCallback cb;
    long lac;
    Object ctx;
    BitSet receivedResponseSet;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;
    int lastSeenError = BKException.Code.WriteException;

    LedgerHandle lh;
    ClientContext clientCtx;

    final List<BookieSocketAddress> currentEnsemble;

    PendingWriteLacOp(LedgerHandle lh, ClientContext clientCtx, List<BookieSocketAddress> ensemble,
                      AddLacCallback cb, Object ctx) {
        this.lh = lh;
        this.clientCtx = clientCtx;
        this.cb = cb;
        this.ctx = ctx;
        this.lac = LedgerHandle.INVALID_ENTRY_ID;
        ackSet = lh.distributionSchedule.getAckSet();
        currentEnsemble = ensemble;
    }

    void setLac(long lac) {
        this.lac = lac;

        this.receivedResponseSet = new BitSet(
                lh.getLedgerMetadata().getWriteQuorumSize());
        this.receivedResponseSet.set(0,
                lh.getLedgerMetadata().getWriteQuorumSize());
    }

    void sendWriteLacRequest(int bookieIndex) {
        clientCtx.getBookieClient().writeLac(currentEnsemble.get(bookieIndex),
                                             lh.ledgerId, lh.ledgerKey, lac, toSend, this, bookieIndex);
    }

    void initiate(ByteBufList toSend) {
        this.toSend = toSend;
        DistributionSchedule.WriteSet writeSet = lh.distributionSchedule.getWriteSet(lac);
        try {
            for (int i = 0; i < writeSet.size(); i++) {
                sendWriteLacRequest(writeSet.get(i));
            }
        } finally {
            writeSet.recycle();
        }
    }

    @Override
    public void writeLacComplete(int rc, long ledgerId, BookieSocketAddress addr, Object ctx) {
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
                cb.addLacComplete(rc, lh, ctx);
                return;
            }
        } else {
            LOG.warn("WriteLac did not succeed: Ledger {} on {}", ledgerId, addr);
        }

        if (receivedResponseSet.isEmpty()){
            completed = true;
            cb.addLacComplete(lastSeenError, lh, ctx);
        }
    }
}
