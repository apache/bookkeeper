package org.apache.bookkeeper.client;

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

import java.net.InetSocketAddress;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * This represents a pending add operation. When it has got success from all
 * bookies, it sees if its at the head of the pending adds queue, and if yes,
 * sends ack back to the application. If a bookie fails, a replacement is made
 * and placed at the same position in the ensemble. The pending adds are then
 * rereplicated.
 *
 *
 */
class PendingAddOp implements WriteCallback {
    final static Logger LOG = LoggerFactory.getLogger(PendingAddOp.class);

    ChannelBuffer toSend;
    AddCallback cb;
    Object ctx;
    long entryId;
    boolean[] successesSoFar;
    int numResponsesPending;
    LedgerHandle lh;
    boolean isRecoveryAdd = false;

    PendingAddOp(LedgerHandle lh, AddCallback cb, Object ctx) {
        this.lh = lh;
        this.cb = cb;
        this.ctx = ctx;
        this.entryId = LedgerHandle.INVALID_ENTRY_ID;
        
        successesSoFar = new boolean[lh.metadata.getQuorumSize()];
        numResponsesPending = successesSoFar.length;
    }

    /** 
     * Enable the recovery add flag for this operation.
     * @see LedgerHandle#asyncRecoveryAddEntry
     */
    PendingAddOp enableRecoveryAdd() {
        isRecoveryAdd = true;
        return this;
    }

    void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    void sendWriteRequest(int bookieIndex, int arrayIndex) {
        int flags = isRecoveryAdd ? BookieProtocol.FLAG_RECOVERY_ADD : BookieProtocol.FLAG_NONE;

        lh.bk.bookieClient.addEntry(lh.metadata.currentEnsemble.get(bookieIndex), lh.ledgerId, lh.ledgerKey, entryId, toSend,
                this, arrayIndex, flags);
    }

    void unsetSuccessAndSendWriteRequest(int bookieIndex) {
        if (toSend == null) {
            // this addOp hasn't yet had its mac computed. When the mac is
            // computed, its write requests will be sent, so no need to send it
            // now
            return;
        }

        int replicaIndex = lh.distributionSchedule.getReplicaIndex(entryId, bookieIndex);
        if (replicaIndex < 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Leaving unchanged, ledger: " + lh.ledgerId + " entry: " + entryId + " bookie index: "
                          + bookieIndex);
            }
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unsetting success for ledger: " + lh.ledgerId + " entry: " + entryId + " bookie index: "
                      + bookieIndex);
        }

        // if we had already heard a success from this array index, need to
        // increment our number of responses that are pending, since we are
        // going to unset this success
        if (successesSoFar[replicaIndex]) {
            successesSoFar[replicaIndex] = false;
            numResponsesPending++;
        }

        sendWriteRequest(bookieIndex, replicaIndex);
    }

    void initiate(ChannelBuffer toSend) {
        this.toSend = toSend;
        for (int i = 0; i < successesSoFar.length; i++) {
            int bookieIndex = lh.distributionSchedule.getBookieIndex(entryId, i);
            sendWriteRequest(bookieIndex, i);
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {

        Integer replicaIndex = (Integer) ctx;
        int bookieIndex = lh.distributionSchedule.getBookieIndex(entryId, replicaIndex);

        if (!lh.metadata.currentEnsemble.get(bookieIndex).equals(addr)) {
            // ensemble has already changed, failure of this addr is immaterial
            LOG.warn("Write did not succeed: " + ledgerId + ", " + entryId + ". But we have already fixed it.");
            return;
        }

        switch (rc) {
        case BKException.Code.OK:
            // continue
            break;
        case BKException.Code.LedgerFencedException:
            LOG.warn("Fencing exception on write: " + ledgerId + ", " + entryId);
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        case BKException.Code.UnauthorizedAccessException:
            LOG.warn("Unauthorized access exception on write: " + ledgerId + ", " + entryId);
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        default:
            LOG.warn("Write did not succeed: " + ledgerId + ", " + entryId);
            lh.handleBookieFailure(addr, bookieIndex);
            return;
        }


        if (!successesSoFar[replicaIndex]) {
            successesSoFar[replicaIndex] = true;
            numResponsesPending--;

            // do some quick checks to see if some adds may have finished. All
            // this will be checked under locks again
            if (numResponsesPending == 0 && lh.pendingAddOps.peek() == this) {
                lh.sendAddSuccessCallbacks();
            }
        }
    }

    void submitCallback(final int rc) {
        cb.addComplete(rc, lh, entryId, ctx);
        lh.opCounterSem.release();
    }

}
