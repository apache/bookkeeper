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

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.client.AsyncCallback.SyncCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;

/**
 * This represents a pending Sync operation. When it has got
 * success from Ack Quorum bookies, sends success back to the application,
 * otherwise failure is sent back to the caller.
 *
 */
class PendingSyncOp implements BookkeeperInternalCallbacks.SyncCallback {
    private final static Logger LOG = LoggerFactory.getLogger(PendingSyncOp.class);
    SyncCallback cb;
    Set<Integer> writeSet;
    Set<Integer> receivedResponseSet;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;
    int lastSeenError = BKException.Code.WriteException;
    final long lastAddPushed;

    LedgerHandle lh;
    OpStatsLogger syncOpLogger;

    PendingSyncOp(LedgerHandle lh, SyncCallback cb) {
        this.lh = lh;
        this.cb = cb;
        this.lastAddPushed = lh.getLastAddPushed();
        ackSet = lh.distributionSchedule.getAckSet();
        syncOpLogger = lh.bk.getSyncOpLogger();
        this.writeSet = new HashSet<>(lh.distributionSchedule.getWriteSet(lastAddPushed));
        this.receivedResponseSet = new HashSet<>(writeSet);
    }

    void sendSyncRequest(int bookieIndex) {
        lh.bk.getBookieClient().sync(lh.metadata.currentEnsemble.get(bookieIndex), lh.ledgerId, lh.ledgerKey,
                lastAddPushed, this, bookieIndex);
    }

    void initiate() {
        if (lastAddPushed == -1) {
            cb.syncComplete(BKException.Code.OK, -1);
            return;
        }
        for (int bookieIndex: writeSet) {
            sendSyncRequest(bookieIndex);
        }
    }

    @Override
    public void syncComplete(int rc, long ledgerId, long lastSyncedEntryId, BookieSocketAddress addr, Object ctx) {
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
            if (ackSet.completeBookieAndCheck(bookieIndex, lastSyncedEntryId) && !completed) {
                completed = true;
                long estimatedLastAddConfirmed = ackSet.calculateCurrentLastAddSynced();
                lh.syncCompleted(estimatedLastAddConfirmed);
                cb.syncComplete(rc, estimatedLastAddConfirmed);
                return;
            }
        } else {
            LOG.warn("Sync did not succeed: Ledger {} on {}", new Object[] { ledgerId, addr });
        }

        if (receivedResponseSet.isEmpty()){
            completed = true;
            cb.syncComplete(lastSeenError, BookieProtocol.INVALID_ENTRY_ID);
        }
    }
}