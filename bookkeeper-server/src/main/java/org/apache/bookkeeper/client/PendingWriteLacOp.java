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


import java.util.HashSet;
import java.util.Set;

import org.apache.bookkeeper.client.AsyncCallback.AddLacCallback;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

/**
 * This represents a pending WriteLac operation. When it has got
 * success from Ack Quorum bookies, sends success back to the application,
 * otherwise failure is sent back to the caller.
 *
 * This is an optional protocol operations to facilitate tailing readers
 * to be up to date with the writer. This is best effort to get latest LAC
 * from bookies, and doesn't affect the correctness of the protocol.
 */
class PendingWriteLacOp implements WriteLacCallback {
    private final static Logger LOG = LoggerFactory.getLogger(PendingWriteLacOp.class);
    ByteBuf toSend;
    AddLacCallback cb;
    long lac;
    Object ctx;
    Set<Integer> writeSet;
    Set<Integer> receivedResponseSet;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;
    int lastSeenError = BKException.Code.WriteException;

    LedgerHandle lh;
    OpStatsLogger putLacOpLogger;

    PendingWriteLacOp(LedgerHandle lh, AddLacCallback cb, Object ctx) {
        this.lh = lh;
        this.cb = cb;
        this.ctx = ctx;
        this.lac = LedgerHandle.INVALID_ENTRY_ID;
        ackSet = lh.distributionSchedule.getAckSet();
        putLacOpLogger = lh.bk.getWriteLacOpLogger();
    }

    void setLac(long lac) {
        this.lac = lac;
        this.writeSet = new HashSet<Integer>(lh.distributionSchedule.getWriteSet(lac));
        this.receivedResponseSet = new HashSet<Integer>(writeSet);
    }

    void sendWriteLacRequest(int bookieIndex) {
        lh.bk.bookieClient.writeLac(lh.metadata.currentEnsemble.get(bookieIndex), lh.ledgerId, lh.ledgerKey,
                lac, toSend, this, bookieIndex);
    }

    void initiate(ByteBuf toSend) {
        this.toSend = toSend;
        for (int bookieIndex: writeSet) {
            sendWriteLacRequest(bookieIndex);
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
        receivedResponseSet.remove(bookieIndex);

        if (rc == BKException.Code.OK) {
            if (ackSet.addBookieAndCheck(bookieIndex) && !completed) {
                completed = true;
                cb.addLacComplete(rc, lh, ctx);
                return;
            }
        } else {
            LOG.warn("WriteLac did not succeed: Ledger {} on {}", new Object[] { ledgerId, addr });
        }
        
        if(receivedResponseSet.isEmpty()){
            completed = true;
            cb.addLacComplete(lastSeenError, lh, ctx);
        }
    }
}
