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
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager.RecoveryData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a pending ReadLac operation.
 *
 * <p>LAC is stored in two places on bookies.
 * 1. WriteLac operation sends Explicit LAC and is stored in memory on each bookie.
 * 2. Each AddEntry operation piggy-backs LAC which is stored on bookie's disk.
 *
 * <p>This operation returns both of those entries and we pick the latest LAC out of
 * available answers.
 *
 * <p>This is an optional protocol operations to facilitate tailing readers
 * to be up to date with the writer. This is best effort to get latest LAC
 * from bookies, and doesn't affect the correctness of the protocol.
 */

class PendingReadLacOp implements ReadLacCallback {
    static final Logger LOG = LoggerFactory.getLogger(PendingReadLacOp.class);
    LedgerHandle lh;
    BookieClient bookieClient;
    LacCallback cb;
    int numResponsesPending;
    volatile boolean completed = false;
    int lastSeenError = BKException.Code.ReadException;
    final DistributionSchedule.QuorumCoverageSet coverageSet;
    long maxLac = LedgerHandle.INVALID_ENTRY_ID;
    final List<BookieSocketAddress> currentEnsemble;

    /*
     * Wrapper to get Lac from the request
     */
    interface LacCallback {
        void getLacComplete(int rc, long lac);
    }

    PendingReadLacOp(LedgerHandle lh, BookieClient bookieClient, List<BookieSocketAddress> ensemble, LacCallback cb) {
        this.lh = lh;
        this.bookieClient = bookieClient;
        this.cb = cb;
        this.numResponsesPending = ensemble.size();
        this.coverageSet = lh.distributionSchedule.getCoverageSet();
        this.currentEnsemble = ensemble;
    }

    public void initiate() {
        for (int i = 0; i < currentEnsemble.size(); i++) {
            bookieClient.readLac(currentEnsemble.get(i), lh.ledgerId, this, i);
        }
    }

    @Override
    public void readLacComplete(int rc, long ledgerId, final ByteBuf lacBuffer, final ByteBuf lastEntryBuffer,
            Object ctx) {
        int bookieIndex = (Integer) ctx;

        // add the response to coverage set
        coverageSet.addBookie(bookieIndex, rc);

        numResponsesPending--;
        boolean heardValidResponse = false;

        if (completed) {
            return;
        }

        if (rc == BKException.Code.OK) {
            try {
                // Each bookie may have two store LAC in two places.
                // One is in-memory copy in FileInfo and other is
                // piggy-backed LAC on the last entry.
                // This routine picks both of them and compares to return
                // the latest Lac.

                // lacBuffer and lastEntryBuffer are optional in the protocol.
                // So check if they exist before processing them.

                // Extract lac from FileInfo on the ledger.
                if (lacBuffer != null && lacBuffer.readableBytes() > 0) {
                    long lac = lh.macManager.verifyDigestAndReturnLac(lacBuffer);
                    if (lac > maxLac) {
                        maxLac = lac;
                    }
                }
                // Extract lac from last entry on the disk
                if (lastEntryBuffer != null && lastEntryBuffer.readableBytes() > 0) {
                    RecoveryData recoveryData = lh.macManager.verifyDigestAndReturnLastConfirmed(lastEntryBuffer);
                    long recoveredLac = recoveryData.getLastAddConfirmed();
                    if (recoveredLac > maxLac) {
                        maxLac = recoveredLac;
                    }
                }
                heardValidResponse = true;
            } catch (BKDigestMatchException e) {
                // Too bad, this bookie did not give us a valid answer, we
                // still might be able to recover. So, continue
                LOG.error("Mac mismatch while reading  ledger: " + ledgerId + " LAC from bookie: "
                        + currentEnsemble.get(bookieIndex));
                rc = BKException.Code.DigestMatchException;
            }
        }

        if (rc == BKException.Code.NoSuchLedgerExistsException || rc == BKException.Code.NoSuchEntryException) {
            heardValidResponse = true;
        }

        if (rc == BKException.Code.UnauthorizedAccessException && !completed) {
            cb.getLacComplete(rc, maxLac);
            completed = true;
            return;
        }

        if (!heardValidResponse && BKException.Code.OK != rc) {
            lastSeenError = rc;
        }

        // We don't consider a success until we have coverage set responses.
        if (heardValidResponse
                && coverageSet.checkCovered()
                && !completed) {
            completed = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Read LAC complete with enough validResponse for ledger: {} LAC: {}", ledgerId, maxLac);
            }
            cb.getLacComplete(BKException.Code.OK, maxLac);
            return;
        }

        if (numResponsesPending == 0 && !completed) {
            LOG.info("While readLac ledger: " + ledgerId + " did not hear success responses from all of ensemble");
            cb.getLacComplete(lastSeenError, maxLac);
        }
    }
}
