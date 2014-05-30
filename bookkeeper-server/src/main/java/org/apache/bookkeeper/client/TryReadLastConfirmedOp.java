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

import org.apache.bookkeeper.client.DigestManager.RecoveryData;
import org.apache.bookkeeper.client.ReadLastConfirmedOp.LastConfirmedDataCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This op is try to read last confirmed without involving quorum coverage checking.
 * Use {@link ReadLastConfirmedOp} if you need quorum coverage checking.
 */
class TryReadLastConfirmedOp implements ReadEntryCallback {

    static final Logger LOG = LoggerFactory.getLogger(TryReadLastConfirmedOp.class);

    final LedgerHandle lh;
    final LastConfirmedDataCallback cb;

    int numResponsesPending;
    volatile boolean hasValidResponse = false;
    volatile boolean completed = false;
    RecoveryData maxRecoveredData;

    TryReadLastConfirmedOp(LedgerHandle lh, LastConfirmedDataCallback cb, long lac) {
        this.lh = lh;
        this.cb = cb;
        this.maxRecoveredData = new RecoveryData(lac, 0);
        this.numResponsesPending = lh.metadata.getEnsembleSize();
    }

    public void initiate() {
        for (int i = 0; i < lh.metadata.currentEnsemble.size(); i++) {
            lh.bk.bookieClient.readEntry(lh.metadata.currentEnsemble.get(i),
                                         lh.ledgerId,
                                         BookieProtocol.LAST_ADD_CONFIRMED,
                                         this, i);
        }
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("TryReadLastConfirmed received response for (lid={}, eid={}) : {}",
                    new Object[] { ledgerId, entryId, rc });
        }

        int bookieIndex = (Integer) ctx;
        numResponsesPending--;
        if (BKException.Code.OK == rc) {
            try {
                RecoveryData recoveryData = lh.macManager.verifyDigestAndReturnLastConfirmed(buffer);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Received lastAddConfirmed (lac={}, length={}) from bookie({}) for (lid={}).",
                            new Object[] { recoveryData.lastAddConfirmed, recoveryData.length, bookieIndex, ledgerId });
                }
                if (recoveryData.lastAddConfirmed > maxRecoveredData.lastAddConfirmed) {
                    maxRecoveredData = recoveryData;
                    // callback immediately
                    cb.readLastConfirmedDataComplete(BKException.Code.OK, maxRecoveredData);
                }
                hasValidResponse = true;
            } catch (BKException.BKDigestMatchException e) {
                LOG.error("Mac mismatch for ledger: " + ledgerId + ", entry: " + entryId
                          + " while reading last entry from bookie: "
                          + lh.metadata.currentEnsemble.get(bookieIndex));
            }
        } else if (BKException.Code.UnauthorizedAccessException == rc && !completed) {
            cb.readLastConfirmedDataComplete(rc, maxRecoveredData);
            completed = true;
        } else if (BKException.Code.NoSuchLedgerExistsException == rc ||
                   BKException.Code.NoSuchEntryException == rc) {
            hasValidResponse = true;
        }
        if (numResponsesPending == 0 && !completed) {
            if (!hasValidResponse) {
                // no success called
                cb.readLastConfirmedDataComplete(BKException.Code.LedgerRecoveryException, maxRecoveredData);
            } else {
                // callback
                cb.readLastConfirmedDataComplete(BKException.Code.OK, maxRecoveredData);
            }
            completed = true;
        }
    }
}
