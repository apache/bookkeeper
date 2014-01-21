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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
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
    private static final Logger LOG = LoggerFactory.getLogger(PendingReadOp.class);

    final int speculativeReadTimeout;
    final private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> speculativeTask = null;
    Queue<LedgerEntryRequest> seq;
    Set<BookieSocketAddress> heardFromHosts;
    ReadCallback cb;
    Object ctx;
    LedgerHandle lh;
    long numPendingEntries;
    long startEntryId;
    long endEntryId;
    long requestTimeNanos;
    OpStatsLogger readOpLogger;

    final int maxMissedReadsAllowed;
    boolean parallelRead = false;
    final AtomicBoolean complete = new AtomicBoolean(false);

    abstract class LedgerEntryRequest extends LedgerEntry {

        final AtomicBoolean complete = new AtomicBoolean(false);

        int rc = BKException.Code.OK;
        int firstError = BKException.Code.OK;
        int numMissedEntryReads = 0;

        final ArrayList<BookieSocketAddress> ensemble;
        final List<Integer> writeSet;

        LedgerEntryRequest(ArrayList<BookieSocketAddress> ensemble, long lId, long eId) {
            super(lId, eId);

            this.ensemble = ensemble;
            this.writeSet = lh.distributionSchedule.getWriteSet(entryId);
        }

        /**
         * Execute the read request.
         */
        abstract void read();

        /**
         * Complete the read request from <i>host</i>.
         *
         * @param host
         *          host that respond the read
         * @param buffer
         *          the data buffer
         * @return return true if we managed to complete the entry;
         *         otherwise return false if the read entry is not complete or it is already completed before
         */
        boolean complete(BookieSocketAddress host, final ByteBuf buffer) {
            ByteBuf content;
            try {
                content = lh.macManager.verifyDigestAndReturnData(entryId, buffer);
            } catch (BKDigestMatchException e) {
                logErrorAndReattemptRead(host, "Mac mismatch", BKException.Code.DigestMatchException);
                buffer.release();
                return false;
            }

            if (!complete.getAndSet(true)) {
                rc = BKException.Code.OK;
                /*
                 * The length is a long and it is the last field of the metadata of an entry.
                 * Consequently, we have to subtract 8 from METADATA_LENGTH to get the length.
                 */
                length = buffer.getLong(DigestManager.METADATA_LENGTH - 8);
                data = content;
                return true;
            } else {
                buffer.release();
                return false;
            }
        }

        /**
         * Fail the request with given result code <i>rc</i>.
         *
         * @param rc
         *          result code to fail the request.
         * @return true if we managed to fail the entry; otherwise return false if it already failed or completed.
         */
        boolean fail(int rc) {
            if (complete.compareAndSet(false, true)) {
                this.rc = rc;
                submitCallback(rc);
                return true;
            } else {
                return false;
            }
        }

        /**
         * Log error <i>errMsg</i> and reattempt read from <i>host</i>.
         *
         * @param host
         *          host that just respond
         * @param errMsg
         *          error msg to log
         * @param rc
         *          read result code
         */
        synchronized void logErrorAndReattemptRead(BookieSocketAddress host, String errMsg, int rc) {
            if (BKException.Code.OK == firstError ||
                BKException.Code.NoSuchEntryException == firstError ||
                BKException.Code.NoSuchLedgerExistsException == firstError) {
                firstError = rc;
            } else if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                       BKException.Code.NoSuchEntryException != rc &&
                       BKException.Code.NoSuchLedgerExistsException != rc) {
                // if other exception rather than NoSuchEntryException or NoSuchLedgerExistsException is
                // returned we need to update firstError to indicate that it might be a valid read but just
                // failed.
                firstError = rc;
            }
            if (BKException.Code.NoSuchEntryException == rc ||
                BKException.Code.NoSuchLedgerExistsException == rc) {
                ++numMissedEntryReads;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No such entry found on bookie.  L{} E{} bookie: {}",
                        new Object[] { lh.ledgerId, entryId, host });
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(errMsg + " while reading L{} E{} from bookie: {}",
                        new Object[]{lh.ledgerId, entryId, host});
                }
            }
        }

        /**
         * Send to next replica speculatively, if required and possible.
         * This returns the host we may have sent to for unit testing.
         *
         * @param heardFromHosts
         *      the set of hosts that we already received responses.
         * @return host we sent to if we sent. null otherwise.
         */
        abstract BookieSocketAddress maybeSendSpeculativeRead(Set<BookieSocketAddress> heardFromHosts);

        /**
         * Whether the read request completed.
         *
         * @return true if the read request is completed.
         */
        boolean isComplete() {
            return complete.get();
        }

        /**
         * Get result code of this entry.
         *
         * @return result code.
         */
        int getRc() {
            return rc;
        }

        @Override
        public String toString() {
            return String.format("L%d-E%d", ledgerId, entryId);
        }
    }

    class ParallelReadRequest extends LedgerEntryRequest {

        int numPendings;

        ParallelReadRequest(ArrayList<BookieSocketAddress> ensemble, long lId, long eId) {
            super(ensemble, lId, eId);
            numPendings = writeSet.size();
        }

        @Override
        void read() {
            for (int bookieIndex : writeSet) {
                BookieSocketAddress to = ensemble.get(bookieIndex);
                try {
                    sendReadTo(to, this);
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted reading entry {} : ", this, ie);
                    Thread.currentThread().interrupt();
                    fail(BKException.Code.InterruptedException);
                    return;
                }
            }
        }

        @Override
        synchronized void logErrorAndReattemptRead(BookieSocketAddress host, String errMsg, int rc) {
            super.logErrorAndReattemptRead(host, errMsg, rc);
            --numPendings;
            // if received all responses or this entry doesn't meet quorum write, complete the request.
            if (numMissedEntryReads > maxMissedReadsAllowed || numPendings == 0) {
                if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                    numMissedEntryReads > maxMissedReadsAllowed) {
                    firstError = BKException.Code.NoSuchEntryException;
                }

                fail(firstError);
            }
        }

        @Override
        BookieSocketAddress maybeSendSpeculativeRead(Set<BookieSocketAddress> heardFromHosts) {
            // no speculative read
            return null;
        }
    }

    class SequenceReadRequest extends LedgerEntryRequest {
        final static int NOT_FOUND = -1;
        int nextReplicaIndexToReadFrom = 0;

        final BitSet sentReplicas;
        final BitSet erroredReplicas;

        SequenceReadRequest(ArrayList<BookieSocketAddress> ensemble, long lId, long eId) {
            super(ensemble, lId, eId);

            this.sentReplicas = new BitSet(lh.getLedgerMetadata().getWriteQuorumSize());
            this.erroredReplicas = new BitSet(lh.getLedgerMetadata().getWriteQuorumSize());
        }

        private int getReplicaIndex(BookieSocketAddress host) {
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

        private BitSet getHeardFromBitSet(Set<BookieSocketAddress> heardFromHosts) {
            BitSet b = new BitSet(ensemble.size());
            for (BookieSocketAddress i : heardFromHosts) {
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
        @Override
        synchronized BookieSocketAddress maybeSendSpeculativeRead(Set<BookieSocketAddress> heardFromHosts) {
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

        @Override
        void read() {
            sendNextRead();
        }

        synchronized BookieSocketAddress sendNextRead() {
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getWriteQuorumSize()) {
                // we are done, the read has failed from all replicas, just fail the
                // read

                // Do it a bit pessimistically, only when finished trying all replicas
                // to check whether we received more missed reads than maxMissedReadsAllowed
                if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                    numMissedEntryReads > maxMissedReadsAllowed) {
                    firstError = BKException.Code.NoSuchEntryException;
                }

                fail(firstError);
                return null;
            }

            int replica = nextReplicaIndexToReadFrom;
            int bookieIndex = lh.distributionSchedule.getWriteSet(entryId).get(nextReplicaIndexToReadFrom);
            nextReplicaIndexToReadFrom++;

            try {
                BookieSocketAddress to = ensemble.get(bookieIndex);
                sendReadTo(to, this);
                sentReplicas.set(replica);
                return to;
            } catch (InterruptedException ie) {
                LOG.error("Interrupted reading entry " + this, ie);
                Thread.currentThread().interrupt();
                fail(BKException.Code.InterruptedException);
                return null;
            }
        }

        @Override
        synchronized void logErrorAndReattemptRead(BookieSocketAddress host, String errMsg, int rc) {
            super.logErrorAndReattemptRead(host, errMsg, rc);

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
        heardFromHosts = new HashSet<BookieSocketAddress>();

        readOpLogger = lh.bk.getReadOpLogger();
    }

    protected LedgerMetadata getLedgerMetadata() {
        return lh.metadata;
    }

    protected void cancelSpeculativeTask(boolean mayInterruptIfRunning) {
        if (speculativeTask != null) {
            speculativeTask.cancel(mayInterruptIfRunning);
            speculativeTask = null;
        }
    }

    PendingReadOp parallelRead(boolean enabled) {
        this.parallelRead = enabled;
        return this;
    }

    public void initiate() throws InterruptedException {
        long nextEnsembleChange = startEntryId, i = startEntryId;
        this.requestTimeNanos = MathUtils.nowInNano();
        ArrayList<BookieSocketAddress> ensemble = null;

        if (speculativeReadTimeout > 0 && !parallelRead) {
            Runnable readTask = new Runnable() {
                public void run() {
                    int x = 0;
                    for (LedgerEntryRequest r : seq) {
                        if (!r.isComplete()) {
                            if (null == r.maybeSendSpeculativeRead(heardFromHosts)) {
                                // Subsequent speculative read will not materialize anyway
                                cancelSpeculativeTask(false);
                            } else {
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
            };
            try {
                speculativeTask = scheduler.scheduleWithFixedDelay(readTask,
                        speculativeReadTimeout, speculativeReadTimeout, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException re) {
                LOG.debug("Failed to schedule speculative reads for ledger {} ({}, {}) : ",
                    new Object[] { lh.getId(), startEntryId, endEntryId, re });
            }
        }

        do {
            if (i == nextEnsembleChange) {
                ensemble = getLedgerMetadata().getEnsemble(i);
                nextEnsembleChange = getLedgerMetadata().getNextEnsembleChange(i);
            }
            LedgerEntryRequest entry;
            if (parallelRead) {
                entry = new ParallelReadRequest(ensemble, lh.ledgerId, i);
            } else {
                entry = new SequenceReadRequest(ensemble, lh.ledgerId, i);
            }
            seq.add(entry);
            i++;
        } while (i <= endEntryId);
        // read the entries.
        for (LedgerEntryRequest entry : seq) {
            entry.read();
        }
    }

    private static class ReadContext {
        final BookieSocketAddress to;
        final LedgerEntryRequest entry;

        ReadContext(BookieSocketAddress to, LedgerEntryRequest entry) {
            this.to = to;
            this.entry = entry;
        }
    }

    void sendReadTo(BookieSocketAddress to, LedgerEntryRequest entry) throws InterruptedException {
        if (lh.throttler != null) {
            lh.throttler.acquire();
        }

        lh.bk.bookieClient.readEntry(to, lh.ledgerId, entry.entryId,
                                     this, new ReadContext(to, entry));
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, final long entryId, final ByteBuf buffer, Object ctx) {
        final ReadContext rctx = (ReadContext)ctx;
        final LedgerEntryRequest entry = rctx.entry;

        if (rc != BKException.Code.OK) {
            entry.logErrorAndReattemptRead(rctx.to, "Error: " + BKException.getMessage(rc), rc);
            return;
        }

        heardFromHosts.add(rctx.to);

        if (entry.complete(rctx.to, buffer)) {
            submitCallback(BKException.Code.OK);
        }

        if(numPendingEntries < 0)
            LOG.error("Read too many values");
    }

    protected void submitCallback(int code) {
        if (BKException.Code.OK == code) {
            numPendingEntries--;
            if (numPendingEntries != 0) {
                return;
            }
        }

        // ensure callback once
        if (!complete.compareAndSet(false, true)) {
            return;
        }

        long latencyNanos = MathUtils.elapsedNanos(requestTimeNanos);
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
            readOpLogger.registerFailedEvent(latencyNanos, TimeUnit.NANOSECONDS);
        } else {
            readOpLogger.registerSuccessfulEvent(latencyNanos, TimeUnit.NANOSECONDS);
        }
        cancelSpeculativeTask(true);
        cb.readComplete(code, lh, PendingReadOp.this, PendingReadOp.this.ctx);
        cb = null;
    }

    @Override
    public boolean hasMoreElements() {
        return !seq.isEmpty();
    }

    @Override
    public LedgerEntry nextElement() throws NoSuchElementException {
        return seq.remove();
    }

    public int size() {
        return seq.size();
    }
}
