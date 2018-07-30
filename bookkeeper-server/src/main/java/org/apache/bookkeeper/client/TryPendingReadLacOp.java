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

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager.RecoveryData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This op is try to read last confirmed without involving quorum coverage checking.
 * Use {@link PendingLacOp} if you need quorum coverage checking.
 */

class TryPendingReadLacOp implements ReadLacCallback {
    static final Logger LOG = LoggerFactory.getLogger(TryPendingReadLacOp.class);
    LedgerHandle lh;
    LacCallback cb;
    int numResponsesPending;
    volatile boolean completed = false;
    volatile boolean hasValidResponse = false;
    int lastSeenError = BKException.Code.ReadException;
    RecoveryData maxRecoveredData;
    long maxLac;

    /*
     * Wrapper to get Lac from the request
     */
    interface LacCallback {
        void getLacComplete(int rc, long lac);
    }

    TryPendingReadLacOp(LedgerHandle lh, LacCallback cb) {
        this.lh = lh;
        this.cb = cb;
        this.maxLac = lh.getLastAddConfirmed();
        this.numResponsesPending = lh.metadata.getEnsembleSize();
        this.maxRecoveredData = new RecoveryData(maxLac, 0);
    }

    public void initiate() {
        for (int i = 0; i < lh.metadata.currentEnsemble.size(); i++) {
            lh.bk.getBookieClient().readLac(lh.metadata.currentEnsemble.get(i),
                    lh.ledgerId, this, i);
        }
    }

    @Override
    public void readLacComplete(int rc, long ledgerId, final ByteBuf lacBuffer, final ByteBuf lastEntryBuffer,
            Object ctx) {
        int bookieIndex = (Integer) ctx;
        LOG.info("readLacComplete "+rc+", "+ledgerId+" index "+bookieIndex+", numResponsesPending "+numResponsesPending+", completed:"+completed);

        numResponsesPending--;

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
                long newLac = LedgerHandle.INVALID_ENTRY_ID;
                // Extract lac from FileInfo on the ledger.
                if (lacBuffer != null && lacBuffer.readableBytes() > 0) {
                    long lac = lh.macManager.verifyDigestAndReturnLac(lacBuffer);
                    if (lac > maxLac) {
                        newLac = lac;
                    }
                }
                // Extract lac from last entry on the disk
                if (lastEntryBuffer != null && lastEntryBuffer.readableBytes() > 0) {
                    RecoveryData recoveryData = lh.macManager.verifyDigestAndReturnLastConfirmed(lastEntryBuffer);
                    long recoveredLac = recoveryData.getLastAddConfirmed();
                    if (recoveredLac > maxLac) {
                        newLac = recoveredLac;
                    }
                }
                if (newLac > maxLac) {
                    // as for TryReadLastConfirmedOp we will call the callback as soon as possible
                    cb.getLacComplete(rc, newLac);
                    completed = true;
                }
                maxLac = newLac;

                hasValidResponse = true;
            } catch (BKDigestMatchException e) {
                // Too bad, this bookie did not give us a valid answer, we
                // still might be able to recover. So, continue
                LOG.error("Mac mismatch while reading  ledger: " + ledgerId + " LAC from bookie: "
                        + lh.metadata.currentEnsemble.get(bookieIndex));
                rc = BKException.Code.DigestMatchException;
            }
        }

        if (rc == BKException.Code.NoSuchLedgerExistsException || rc == BKException.Code.NoSuchEntryException) {
            hasValidResponse = true;
        }

        if (rc == BKException.Code.UnauthorizedAccessException && !completed) {
            cb.getLacComplete(rc, maxLac);
            completed = true;
            return;
        }

        if (!hasValidResponse && BKException.Code.OK != rc) {
            lastSeenError = rc;
        }

        if (numResponsesPending == 0 && !completed) {
            if (!hasValidResponse) {
                cb.getLacComplete(lastSeenError, maxLac);
            } else {
                cb.getLacComplete(BKException.Code.OK, maxLac);
            }
            completed = true;
        }
    }
}
