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

import io.netty.buffer.ByteBuf;
import java.util.List;

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager.RecoveryData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulated the read last confirmed operation.
 *
 */
class ReadLastConfirmedOp implements ReadEntryCallback {
    static final Logger LOG = LoggerFactory.getLogger(ReadLastConfirmedOp.class);
    LedgerHandle lh;
    BookieClient bookieClient;
    int numResponsesPending;
    int numSuccessfulResponse;
    RecoveryData maxRecoveredData;
    volatile boolean completed = false;
    int lastSeenError = BKException.Code.ReadException;

    LastConfirmedDataCallback cb;
    final DistributionSchedule.QuorumCoverageSet coverageSet;
    final List<BookieSocketAddress> currentEnsemble;

    /**
     * Wrapper to get all recovered data from the request.
     */
    interface LastConfirmedDataCallback {
        void readLastConfirmedDataComplete(int rc, RecoveryData data);
    }

    public ReadLastConfirmedOp(LedgerHandle lh, BookieClient bookieClient,
                               List<BookieSocketAddress> ensemble, LastConfirmedDataCallback cb) {
        this.cb = cb;
        this.bookieClient = bookieClient;
        this.maxRecoveredData = new RecoveryData(LedgerHandle.INVALID_ENTRY_ID, 0);
        this.lh = lh;
        this.numSuccessfulResponse = 0;
        this.numResponsesPending = lh.getLedgerMetadata().getEnsembleSize();
        this.coverageSet = lh.distributionSchedule.getCoverageSet();
        this.currentEnsemble = ensemble;
    }

    public void initiate() {
        for (int i = 0; i < currentEnsemble.size(); i++) {
            bookieClient.readEntry(currentEnsemble.get(i),
                                   lh.ledgerId,
                                   BookieProtocol.LAST_ADD_CONFIRMED,
                                   this, i, BookieProtocol.FLAG_NONE);
        }
    }

    public void initiateWithFencing() {
        for (int i = 0; i < currentEnsemble.size(); i++) {
            bookieClient.readEntry(currentEnsemble.get(i),
                                   lh.ledgerId,
                                   BookieProtocol.LAST_ADD_CONFIRMED,
                                   this, i, BookieProtocol.FLAG_DO_FENCING,
                                   lh.ledgerKey);
        }
    }

    public synchronized void readEntryComplete(final int rc, final long ledgerId, final long entryId,
            final ByteBuf buffer, final Object ctx) {
        int bookieIndex = (Integer) ctx;

        // add the response to coverage set
        coverageSet.addBookie(bookieIndex, rc);

        numResponsesPending--;
        boolean heardValidResponse = false;
        if (rc == BKException.Code.OK) {
            try {
                RecoveryData recoveryData = lh.macManager.verifyDigestAndReturnLastConfirmed(buffer);
                if (recoveryData.getLastAddConfirmed() > maxRecoveredData.getLastAddConfirmed()) {
                    maxRecoveredData = recoveryData;
                }
                heardValidResponse = true;
                numSuccessfulResponse++;
            } catch (BKDigestMatchException e) {
                // Too bad, this bookie didn't give us a valid answer, we
                // still might be able to recover though so continue
                LOG.error("Mac mismatch for ledger: " + ledgerId + ", entry: " + entryId
                          + " while reading last entry from bookie: "
                          + currentEnsemble.get(bookieIndex));
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

        if (!heardValidResponse && BKException.Code.OK != rc) {
            lastSeenError = rc;
        }

        // other return codes dont count as valid responses
        if (heardValidResponse
            && coverageSet.checkCovered()
            && !completed) {
            completed = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Read Complete with enough validResponses for ledger: {}, entry: {}",
                        ledgerId, entryId);
            }

            cb.readLastConfirmedDataComplete(BKException.Code.OK, maxRecoveredData);
            return;
        }

        if (numResponsesPending == 0 && !completed) {
            int totalExepctedResponse = lh.getLedgerMetadata().getWriteQuorumSize()
                    - lh.getLedgerMetadata().getAckQuorumSize() + 1;
            if (numSuccessfulResponse >= totalExepctedResponse) {
                cb.readLastConfirmedDataComplete(BKException.Code.OK, maxRecoveredData);
                return;
            }
            // Have got all responses back but was still not enough, just fail the operation
            LOG.error("While readLastConfirmed ledger: {} did not hear success responses from all quorums {}", ledgerId,
                    lastSeenError);
            cb.readLastConfirmedDataComplete(lastSeenError, maxRecoveredData);
        }

    }
}
