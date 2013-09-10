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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sequence of entries of a ledger that represents a pending read operation.
 * When all the data read has come back, the application callback is called.
 * This class could be improved because we could start pushing data to the
 * application as soon as it arrives rather than waiting for the whole thing.
 *
 */
class PendingReadOp implements Enumeration<LedgerEntry>, ReadEntryCallback {
    Logger LOG = LoggerFactory.getLogger(PendingReadOp.class);

    final int speculativeReadTimeout;
    final private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> speculativeTask = null;
    Queue<LedgerEntryRequest> seq;
    Set<InetSocketAddress> heardFromHosts;
    ReadCallback cb;
    Object ctx;
    LedgerHandle lh;
    long numPendingEntries;
    long startEntryId;
    long endEntryId;
    final int maxMissedReadsAllowed;

    class LedgerEntryRequest extends LedgerEntry {
        final static int NOT_FOUND = -1;
        int nextReplicaIndexToReadFrom = 0;
        AtomicBoolean complete = new AtomicBoolean(false);

        int firstError = BKException.Code.OK;
        int numMissedEntryReads = 0;

        final ArrayList<InetSocketAddress> ensemble;
        final List<Integer> writeSet;
        final BitSet sentReplicas;
        final BitSet erroredReplicas;

        LedgerEntryRequest(ArrayList<InetSocketAddress> ensemble, long lId, long eId) {
            super(lId, eId);

            this.ensemble = ensemble;
            this.writeSet = lh.distributionSchedule.getWriteSet(entryId);
            this.sentReplicas = new BitSet(lh.getLedgerMetadata().getWriteQuorumSize());
            this.erroredReplicas = new BitSet(lh.getLedgerMetadata().getWriteQuorumSize());
        }

        private int getReplicaIndex(InetSocketAddress host) {
            int bookieIndex = ensemble.indexOf(host);
            if (bookieIndex == -1) {
                return NOT_FOUND;
            }
            return writeSet.indexOf(bookieIndex);
        }

        private BitSet getSentToBitSet() {
            BitSet b = new BitSet(ensemble.size());

            for (int i = 0; i < sentReplicas.length(); i++) {
                if (sentReplicas.get(i)) {
                    b.set(writeSet.get(i));
                }
            }
            return b;
        }

        private BitSet getHeardFromBitSet(Set<InetSocketAddress> heardFromHosts) {
            BitSet b = new BitSet(ensemble.size());
            for (InetSocketAddress i : heardFromHosts) {
                int index = ensemble.indexOf(i);
                if (index != -1) {
                    b.set(index);
                }
            }
            return b;
        }

        private boolean readsOutstanding() {
            return (sentReplicas.cardinality() - erroredReplicas.cardinality()) > 0;
        }

        /**
         * Send to next replica speculatively, if required and possible.
         * This returns the host we may have sent to for unit testing.
         * @return host we sent to if we sent. null otherwise.
         */
        synchronized InetSocketAddress maybeSendSpeculativeRead(Set<InetSocketAddress> heardFromHosts) {
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getWriteQuorumSize()) {
                return null;
            }

            BitSet sentTo = getSentToBitSet();
            BitSet heardFrom = getHeardFromBitSet(heardFromHosts);
            sentTo.and(heardFrom);

            // only send another read, if we have had no response at all (even for other entries)
            // from any of the other bookies we have sent the request to
            if (sentTo.cardinality() == 0) {
                return sendNextRead();
            } else {
                return null;
            }
        }

        synchronized InetSocketAddress sendNextRead() {
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getWriteQuorumSize()) {
                // we are done, the read has failed from all replicas, just fail the
                // read

                // Do it a bit pessimistically, only when finished trying all replicas
                // to check whether we received more missed reads than maxMissedReadsAllowed
                if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                    numMissedEntryReads > maxMissedReadsAllowed) {
                    firstError = BKException.Code.NoSuchEntryException;
                }

                submitCallback(firstError);
                return null;
            }

            int replica = nextReplicaIndexToReadFrom;
            int bookieIndex = lh.distributionSchedule.getWriteSet(entryId).get(nextReplicaIndexToReadFrom);
            nextReplicaIndexToReadFrom++;

            try {
                InetSocketAddress to = ensemble.get(bookieIndex);
                sendReadTo(to, this);
                sentReplicas.set(replica);
                return to;
            } catch (InterruptedException ie) {
                LOG.error("Interrupted reading entry " + this, ie);
                Thread.currentThread().interrupt();
                submitCallback(BKException.Code.ReadException);
                return null;
            }
        }

        synchronized void logErrorAndReattemptRead(InetSocketAddress host, String errMsg, int rc) {
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
                LOG.debug("No such entry found on bookie.  L{} E{} bookie: {}",
                        new Object[] { lh.ledgerId, entryId, host });
            } else {
                LOG.debug(errMsg + " while reading L{} E{} from bookie: {}",
                          new Object[] { lh.ledgerId, entryId, host });
            }

            int replica = getReplicaIndex(host);
            if (replica == NOT_FOUND) {
                LOG.error("Received error from a host which is not in the ensemble {} {}.", host, ensemble);
                return;
            }
            erroredReplicas.set(replica);

            if (!readsOutstanding()) {
                sendNextRead();
            }
        }

        // return true if we managed to complete the entry
        // return false if the read entry is not complete or it is already completed before
        boolean complete(InetSocketAddress host, final ChannelBuffer buffer) {
            ChannelBufferInputStream is;
            try {
                is = lh.macManager.verifyDigestAndReturnData(entryId, buffer);
            } catch (BKDigestMatchException e) {
                logErrorAndReattemptRead(host, "Mac mismatch", BKException.Code.DigestMatchException);
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

    PendingReadOp(LedgerHandle lh, ScheduledExecutorService scheduler,
                  long startEntryId, long endEntryId, ReadCallback cb, Object ctx) {
        seq = new ArrayBlockingQueue<LedgerEntryRequest>((int) ((endEntryId + 1) - startEntryId));
        this.cb = cb;
        this.ctx = ctx;
        this.lh = lh;
        this.startEntryId = startEntryId;
        this.endEntryId = endEntryId;
        this.scheduler = scheduler;
        numPendingEntries = endEntryId - startEntryId + 1;
        maxMissedReadsAllowed = getLedgerMetadata().getWriteQuorumSize()
                - getLedgerMetadata().getAckQuorumSize();
        speculativeReadTimeout = lh.bk.getConf().getSpeculativeReadTimeout();
        heardFromHosts = new HashSet<InetSocketAddress>();
    }

    protected LedgerMetadata getLedgerMetadata() {
        return lh.metadata;
    }

    public void initiate() throws InterruptedException {
        long nextEnsembleChange = startEntryId, i = startEntryId;

        ArrayList<InetSocketAddress> ensemble = null;

        if (speculativeReadTimeout > 0) {
            speculativeTask = scheduler.scheduleWithFixedDelay(new Runnable() {
                    public void run() {
                        int x = 0;
                        for (LedgerEntryRequest r : seq) {
                            if (!r.isComplete()) {
                                if (null != r.maybeSendSpeculativeRead(heardFromHosts)) {
                                    LOG.debug("Send speculative read for {}. Hosts heard are {}.",
                                              r, heardFromHosts);
                                    ++x;
                                }
                            }
                        }
                        if (x > 0) {
                            LOG.debug("Send {} speculative reads for ledger {} ({}, {}). Hosts heard are {}.",
                                      new Object[] { x, lh.getId(), startEntryId, endEntryId, heardFromHosts });
                        }
                    }
                }, speculativeReadTimeout, speculativeReadTimeout, TimeUnit.MILLISECONDS);
        }

        do {
            if (i == nextEnsembleChange) {
                ensemble = getLedgerMetadata().getEnsemble(i);
                nextEnsembleChange = getLedgerMetadata().getNextEnsembleChange(i);
            }
            LedgerEntryRequest entry = new LedgerEntryRequest(ensemble, lh.ledgerId, i);
            seq.add(entry);
            i++;

            entry.sendNextRead();
        } while (i <= endEntryId);
    }

    private static class ReadContext {
        final InetSocketAddress to;
        final LedgerEntryRequest entry;

        ReadContext(InetSocketAddress to, LedgerEntryRequest entry) {
            this.to = to;
            this.entry = entry;
        }
    }

    void sendReadTo(InetSocketAddress to, LedgerEntryRequest entry) throws InterruptedException {
        lh.throttler.acquire();

        lh.bk.bookieClient.readEntry(to, lh.ledgerId, entry.entryId, 
                                     this, new ReadContext(to, entry));
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, final long entryId, final ChannelBuffer buffer, Object ctx) {
        final ReadContext rctx = (ReadContext)ctx;
        final LedgerEntryRequest entry = rctx.entry;

        if (rc != BKException.Code.OK) {
            entry.logErrorAndReattemptRead(rctx.to, "Error: " + BKException.getMessage(rc), rc);
            return;
        }

        heardFromHosts.add(rctx.to);

        if (entry.complete(rctx.to, buffer)) {
            numPendingEntries--;
            if (numPendingEntries == 0) {
                submitCallback(BKException.Code.OK);
            }
        }

        if(numPendingEntries < 0)
            LOG.error("Read too many values");
    }

    private void submitCallback(int code) {
        if (speculativeTask != null) {
            speculativeTask.cancel(true);
            speculativeTask = null;
        }
        if (code != BKException.Code.OK) {
            long firstUnread = LedgerHandle.INVALID_ENTRY_ID;
            for (LedgerEntryRequest req : seq) {
                if (!req.isComplete()) {
                    firstUnread = req.getEntryId();
                    break;
                }
            }
            LOG.error("Read of ledger entry failed: L{} E{}-E{}, Heard from {}. First unread entry is {}",
                    new Object[] { lh.getId(), startEntryId, endEntryId, heardFromHosts, firstUnread });
        }
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
