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

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import java.util.List;

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.checksum.DigestManager.RecoveryData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulated the read last confirmed operation.
 *
 */
class ReadLastConfirmedOp implements ReadEntryCallback {
    static final Logger LOG = LoggerFactory.getLogger(ReadLastConfirmedOp.class);
    private final long ledgerId;
    private final byte[] ledgerKey;
    private final BookieClient bookieClient;
    private final DigestManager digestManager;
    private int numResponsesPending;
    private RecoveryData maxRecoveredData;
    private volatile boolean completed = false;
    private int lastSeenError = BKException.Code.ReadException;

    private final LastConfirmedDataCallback cb;
    private final DistributionSchedule.QuorumCoverageSet coverageSet;
    private final List<BookieSocketAddress> currentEnsemble;

    /**
     * Wrapper to get all recovered data from the request.
     */
    interface LastConfirmedDataCallback {
        void readLastConfirmedDataComplete(int rc, RecoveryData data);
    }

    public ReadLastConfirmedOp(BookieClient bookieClient,
                               DistributionSchedule schedule,
                               DigestManager digestManager,
                               long ledgerId,
                               List<BookieSocketAddress> ensemble,
                               byte[] ledgerKey,
                               LastConfirmedDataCallback cb) {
        this.cb = cb;
        this.bookieClient = bookieClient;
        this.maxRecoveredData = new RecoveryData(LedgerHandle.INVALID_ENTRY_ID, 0);
        this.numResponsesPending = ensemble.size();
        this.coverageSet = schedule.getCoverageSet();
        this.currentEnsemble = ensemble;
        this.ledgerId = ledgerId;
        this.ledgerKey = ledgerKey;
        this.digestManager = digestManager;
    }

    public void initiate() {
        for (int i = 0; i < currentEnsemble.size(); i++) {
            bookieClient.readEntry(currentEnsemble.get(i),
                                   ledgerId,
                                   BookieProtocol.LAST_ADD_CONFIRMED,
                                   this, i, BookieProtocol.FLAG_NONE);
        }
    }

    public void initiateWithFencing() {
        for (int i = 0; i < currentEnsemble.size(); i++) {
            bookieClient.readEntry(currentEnsemble.get(i),
                                   ledgerId,
                                   BookieProtocol.LAST_ADD_CONFIRMED,
                                   this, i, BookieProtocol.FLAG_DO_FENCING,
                                   ledgerKey);
        }
    }

    @Override
    public synchronized void readEntryComplete(final int rc, final long ledgerId, final long entryId,
            final ByteBuf buffer, final Object ctx) {
        int bookieIndex = (Integer) ctx;

        // add the response to coverage set
        coverageSet.addBookie(bookieIndex, rc);

        numResponsesPending--;
        boolean heardValidResponse = false;
        if (rc == BKException.Code.OK) {
            try {
                RecoveryData recoveryData = digestManager.verifyDigestAndReturnLastConfirmed(buffer);
                if (recoveryData.getLastAddConfirmed() > maxRecoveredData.getLastAddConfirmed()) {
                    maxRecoveredData = recoveryData;
                }
                heardValidResponse = true;
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
            LOG.error("While readLastConfirmed ledger: {} did not hear success responses from all quorums, {}",
                      ledgerId, coverageSet);
            cb.readLastConfirmedDataComplete(lastSeenError, maxRecoveredData);
        }

    }

    @VisibleForTesting
    synchronized int getNumResponsesPending() {
        return numResponsesPending;
    }
}
