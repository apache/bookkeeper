package org.apache.bookkeeper.client;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;

/**
 * Sequence of entries of a ledger that represents a pending read operation.
 * When all the data read has come back, the application callback is called.
 * This class could be improved because we could start pushing data to the
 * application as soon as it arrives rather than waiting for the whole thing.
 *
 */
class PendingReadOp implements Enumeration<LedgerEntry>, ReadEntryCallback {
    Logger LOG = LoggerFactory.getLogger(PendingReadOp.class);

    Queue<LedgerEntryRequest> seq;
    ReadCallback cb;
    Object ctx;
    LedgerHandle lh;
    long numPendingEntries;
    long startEntryId;
    long endEntryId;
    final int maxMissedReadsAllowed;

    private class LedgerEntryRequest extends LedgerEntry {
        int nextReplicaIndexToReadFrom = 0;
        AtomicBoolean complete = new AtomicBoolean(false);

        int firstError = BKException.Code.OK;
        int numMissedEntryReads = 0;

        final ArrayList<InetSocketAddress> ensemble;

        LedgerEntryRequest(ArrayList<InetSocketAddress> ensemble, long lId, long eId) {
            super(lId, eId);

            this.ensemble = ensemble;
        }

        void sendNextRead() {
            if (nextReplicaIndexToReadFrom >= lh.metadata.getWriteQuorumSize()) {
                // we are done, the read has failed from all replicas, just fail the
                // read

                // Do it a bit perssimistically, only when finished trying all replicas
                // to check whether we received more missed reads than maxMissedReadsAllowed
                if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                    numMissedEntryReads > maxMissedReadsAllowed) {
                    firstError = BKException.Code.NoSuchEntryException;
                }

                submitCallback(firstError);
                return;
            }

            int bookieIndex = lh.distributionSchedule.getWriteSet(entryId).get(nextReplicaIndexToReadFrom);
            nextReplicaIndexToReadFrom++;

            try {
                sendReadTo(ensemble.get(bookieIndex), this);
            } catch (InterruptedException ie) {
                LOG.error("Interrupted reading entry " + this, ie);
                Thread.currentThread().interrupt();
                submitCallback(BKException.Code.ReadException);
            }
        }

        void logErrorAndReattemptRead(String errMsg, int rc) {
            if (BKException.Code.OK == firstError ||
                BKException.Code.NoSuchEntryException == firstError) {
                firstError = rc;
            } else if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                       BKException.Code.NoSuchEntryException != rc) {
                // if other exception rather than NoSuchEntryException is returned
                // we need to update firstError to indicate that it might be a valid read but just failed.
                firstError = rc;
            }
            if (BKException.Code.NoSuchEntryException == rc) {
                ++numMissedEntryReads;
            }

            int bookieIndex = lh.distributionSchedule.getWriteSet(entryId).get(nextReplicaIndexToReadFrom - 1);
            LOG.error(errMsg + " while reading entry: " + entryId + " ledgerId: " + lh.ledgerId + " from bookie: "
                      + ensemble.get(bookieIndex));

            sendNextRead();
        }

        // return true if we managed to complete the entry
        boolean complete(final ChannelBuffer buffer) {
            ChannelBufferInputStream is;
            try {
                is = lh.macManager.verifyDigestAndReturnData(entryId, buffer);
            } catch (BKDigestMatchException e) {
                logErrorAndReattemptRead("Mac mismatch", BKException.Code.DigestMatchException);
                return false;
            }

            if (!complete.getAndSet(true)) {
                entryDataStream = is;

                /*
                 * The length is a long and it is the last field of the metadata of an entry.
                 * Consequently, we have to subtract 8 from METADATA_LENGTH to get the length.
                 */
                length = buffer.getLong(DigestManager.METADATA_LENGTH - 8);
                return true;
            } else {
                return false;
            }
        }

        boolean isComplete() {
            return complete.get();
        }

        public String toString() {
            return String.format("L%d-E%d", ledgerId, entryId);
        }
    }

    PendingReadOp(LedgerHandle lh, long startEntryId, long endEntryId, ReadCallback cb, Object ctx) {

        seq = new ArrayDeque<LedgerEntryRequest>((int) (endEntryId - startEntryId));
        this.cb = cb;
        this.ctx = ctx;
        this.lh = lh;
        this.startEntryId = startEntryId;
        this.endEntryId = endEntryId;
        numPendingEntries = endEntryId - startEntryId + 1;
        maxMissedReadsAllowed = lh.metadata.getWriteQuorumSize() - lh.metadata.getAckQuorumSize();
    }

    public void initiate() throws InterruptedException {
        long nextEnsembleChange = startEntryId, i = startEntryId;

        ArrayList<InetSocketAddress> ensemble = null;
        do {
            LOG.debug("Acquiring lock: {}", i);

            if (i == nextEnsembleChange) {
                ensemble = lh.metadata.getEnsemble(i);
                nextEnsembleChange = lh.metadata.getNextEnsembleChange(i);
            }
            LedgerEntryRequest entry = new LedgerEntryRequest(ensemble, lh.ledgerId, i);
            seq.add(entry);
            i++;

            entry.sendNextRead();
        } while (i <= endEntryId);
    }

    void sendReadTo(InetSocketAddress to, LedgerEntryRequest entry) throws InterruptedException {
        lh.opCounterSem.acquire();

        lh.bk.bookieClient.readEntry(to, lh.ledgerId, entry.entryId, 
                                     this, entry);
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, final long entryId, final ChannelBuffer buffer, Object ctx) {
        final LedgerEntryRequest entry = (LedgerEntryRequest) ctx;

        lh.opCounterSem.release();

        if (rc != BKException.Code.OK) {
            entry.logErrorAndReattemptRead("Error: " + BKException.getMessage(rc), rc);
            return;
        }

        if (entry.complete(buffer)) {
            numPendingEntries--;
        }

        if (numPendingEntries == 0) {
            submitCallback(BKException.Code.OK);
        }

        if(numPendingEntries < 0)
            LOG.error("Read too many values");
    }

    private void submitCallback(int code) {
        cb.readComplete(code, lh, PendingReadOp.this, PendingReadOp.this.ctx);
    }
    public boolean hasMoreElements() {
        return !seq.isEmpty();
    }

    public LedgerEntry nextElement() throws NoSuchElementException {
        return seq.remove();
    }

    public int size() {
        return seq.size();
    }
}
