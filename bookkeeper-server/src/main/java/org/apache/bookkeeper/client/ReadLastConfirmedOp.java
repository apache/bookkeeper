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

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.DigestManager.RecoveryData;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * This class encapsulated the read last confirmed operation.
 *
 */
class ReadLastConfirmedOp implements ReadEntryCallback {
    static final Logger LOG = LoggerFactory.getLogger(ReadLastConfirmedOp.class);
    LedgerHandle lh;
    int numResponsesPending;
    RecoveryData maxRecoveredData;
    volatile boolean completed = false;

    LastConfirmedDataCallback cb;
    final DistributionSchedule.QuorumCoverageSet coverageSet;

    /**
     * Wrapper to get all recovered data from the request
     */
    interface LastConfirmedDataCallback {
        public void readLastConfirmedDataComplete(int rc, RecoveryData data);
    }

    public ReadLastConfirmedOp(LedgerHandle lh, LastConfirmedDataCallback cb) {
        this.cb = cb;
        this.maxRecoveredData = new RecoveryData(LedgerHandle.INVALID_ENTRY_ID, 0);
        this.lh = lh;
        this.numResponsesPending = lh.metadata.getEnsembleSize();
        this.coverageSet = lh.distributionSchedule.getCoverageSet();
    }

    public void initiate() {
        for (int i = 0; i < lh.metadata.currentEnsemble.size(); i++) {
            lh.bk.bookieClient.readEntry(lh.metadata.currentEnsemble.get(i),
                                         lh.ledgerId,
                                         BookieProtocol.LAST_ADD_CONFIRMED,
                                         this, i);
        }
    }

    public void initiateWithFencing() {
        for (int i = 0; i < lh.metadata.currentEnsemble.size(); i++) {
            lh.bk.bookieClient.readEntryAndFenceLedger(lh.metadata.currentEnsemble.get(i),
                                                       lh.ledgerId,
                                                       lh.ledgerKey,
                                                       BookieProtocol.LAST_ADD_CONFIRMED,
                                                       this, i);
        }
    }

    public synchronized void readEntryComplete(final int rc, final long ledgerId, final long entryId,
            final ChannelBuffer buffer, final Object ctx) {
        int bookieIndex = (Integer) ctx;

        numResponsesPending--;
        boolean heardValidResponse = false;
        if (rc == BKException.Code.OK) {
            try {
                RecoveryData recoveryData = lh.macManager.verifyDigestAndReturnLastConfirmed(buffer);
                if (recoveryData.lastAddConfirmed > maxRecoveredData.lastAddConfirmed) {
                    maxRecoveredData = recoveryData;
                }
                heardValidResponse = true;
            } catch (BKDigestMatchException e) {
                // Too bad, this bookie didn't give us a valid answer, we
                // still might be able to recover though so continue
                LOG.error("Mac mismatch for ledger: " + ledgerId + ", entry: " + entryId
                          + " while reading last entry from bookie: "
                          + lh.metadata.currentEnsemble.get(bookieIndex));
            }
        }

        if (rc == BKException.Code.NoSuchLedgerExistsException || rc == BKException.Code.NoSuchEntryException) {
            // this still counts as a valid response, e.g., if the client crashed without writing any entry
            heardValidResponse = true;
        }

        if (rc == BKException.Code.UnauthorizedAccessException  && !completed) {
            cb.readLastConfirmedDataComplete(rc, maxRecoveredData);
            completed = true;
        }
        // other return codes dont count as valid responses
        if (heardValidResponse
            && coverageSet.addBookieAndCheckCovered(bookieIndex)
            && !completed) {
            completed = true;
            LOG.debug("Read Complete with enough validResponses for ledger: {}, entry: {}",
                ledgerId, entryId);

            cb.readLastConfirmedDataComplete(BKException.Code.OK, maxRecoveredData);
            return;
        }

        if (numResponsesPending == 0 && !completed) {
            // Have got all responses back but was still not enough, just fail the operation
            LOG.error("While readLastConfirmed ledger: " + ledgerId + " did not hear success responses from all quorums");
            cb.readLastConfirmedDataComplete(BKException.Code.LedgerRecoveryException, maxRecoveredData);
        }

    }
}
