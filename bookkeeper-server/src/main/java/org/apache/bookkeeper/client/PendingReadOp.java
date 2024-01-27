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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager;
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
class PendingReadOp extends ReadOpBase implements ReadEntryCallback  {
    private static final Logger LOG = LoggerFactory.getLogger(PendingReadOp.class);

    private ScheduledFuture<?> speculativeTask = null;
    protected final LinkedList<SingleLedgerEntryRequest> seq;

    PendingReadOp(LedgerHandle lh,
                  ClientContext clientCtx,
                  long startEntryId,
                  long endEntryId,
                  boolean isRecoveryRead) {
        super(lh, clientCtx, startEntryId, endEntryId, isRecoveryRead);
        this.seq = new LinkedList<>();
        numPendingEntries = endEntryId - startEntryId + 1;
    }

    protected void cancelSpeculativeTask(boolean mayInterruptIfRunning) {
        if (speculativeTask != null) {
            speculativeTask.cancel(mayInterruptIfRunning);
            speculativeTask = null;
        }
    }

    public ScheduledFuture<?> getSpeculativeTask() {
        return speculativeTask;
    }

    PendingReadOp parallelRead(boolean enabled) {
        this.parallelRead = enabled;
        return this;
    }

    void initiate() {
        long nextEnsembleChange = startEntryId, i = startEntryId;
        this.requestTimeNanos = MathUtils.nowInNano();
        List<BookieId> ensemble = null;
        do {
            if (i == nextEnsembleChange) {
                ensemble = getLedgerMetadata().getEnsembleAt(i);
                nextEnsembleChange = LedgerMetadataUtils.getNextEnsembleChange(getLedgerMetadata(), i);
            }
            SingleLedgerEntryRequest entry;
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
            if (!parallelRead && clientCtx.getConf().readSpeculativeRequestPolicy.isPresent()) {
                speculativeTask = clientCtx.getConf().readSpeculativeRequestPolicy.get()
                    .initiateSpeculativeRequest(clientCtx.getScheduler(), entry);
            }
        }
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, final long entryId, final ByteBuf buffer, Object ctx) {
        final ReadContext rctx = (ReadContext) ctx;
        final SingleLedgerEntryRequest entry = (SingleLedgerEntryRequest) rctx.entry;

        if (rc != BKException.Code.OK) {
            entry.logErrorAndReattemptRead(rctx.bookieIndex, rctx.to, "Error: " + BKException.getMessage(rc), rc);
            return;
        }

        heardFromHosts.add(rctx.to);
        heardFromHostsBitSet.set(rctx.bookieIndex, true);

        buffer.retain();
        // if entry has completed don't handle twice
        if (entry.complete(rctx.bookieIndex, rctx.to, buffer)) {
            if (!isRecoveryRead) {
                // do not advance LastAddConfirmed for recovery reads
                lh.updateLastConfirmed(rctx.getLastAddConfirmed(), 0L);
            }
            submitCallback(BKException.Code.OK);
        } else {
            buffer.release();
        }

        if (numPendingEntries < 0) {
            LOG.error("Read too many values for ledger {} : [{}, {}].",
                    ledgerId, startEntryId, endEntryId);
        }

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

        cancelSpeculativeTask(true);

        long latencyNanos = MathUtils.elapsedNanos(requestTimeNanos);
        if (code != BKException.Code.OK) {
            long firstUnread = LedgerHandle.INVALID_ENTRY_ID;
            Integer firstRc = null;
            for (LedgerEntryRequest req : seq) {
                if (!req.isComplete()) {
                    firstUnread = req.eId;
                    firstRc = req.rc;
                    break;
                }
            }
            LOG.error(
                    "Read of ledger entry failed: L{} E{}-E{}, Sent to {}, "
                            + "Heard from {} : bitset = {}, Error = '{}'. First unread entry is ({}, rc = {})",
                    lh.getId(), startEntryId, endEntryId, sentToHosts, heardFromHosts, heardFromHostsBitSet,
                    BKException.getMessage(code), firstUnread, firstRc);
            clientCtx.getClientStats().getReadOpLogger().registerFailedEvent(latencyNanos, TimeUnit.NANOSECONDS);
            // release the entries
            seq.forEach(LedgerEntryRequest::close);
            future.completeExceptionally(BKException.create(code));
        } else {
            clientCtx.getClientStats().getReadOpLogger().registerSuccessfulEvent(latencyNanos, TimeUnit.NANOSECONDS);
            future.complete(LedgerEntriesImpl.create(Lists.transform(seq, input -> input.entryImpl)));
        }
    }

    void sendReadTo(int bookieIndex, BookieId to, SingleLedgerEntryRequest entry) throws InterruptedException {
        if (lh.throttler != null) {
            lh.throttler.acquire();
        }

        if (isRecoveryRead) {
            int flags = BookieProtocol.FLAG_HIGH_PRIORITY | BookieProtocol.FLAG_DO_FENCING;
            clientCtx.getBookieClient().readEntry(to, lh.ledgerId, entry.eId,
                    this, new ReadContext(bookieIndex, to, entry), flags, lh.ledgerKey);
        } else {
            clientCtx.getBookieClient().readEntry(to, lh.ledgerId, entry.eId,
                    this, new ReadContext(bookieIndex, to, entry), BookieProtocol.FLAG_NONE);
        }
    }

    abstract class SingleLedgerEntryRequest extends LedgerEntryRequest {
        final LedgerEntryImpl entryImpl;

        SingleLedgerEntryRequest(List<BookieId> ensemble, long lId, long eId) {
            super(ensemble, eId);
            this.entryImpl = LedgerEntryImpl.create(lId, eId);
        }

        @Override
        public void close() {
            super.close();
            entryImpl.close();
        }

        /**
         * Complete the read request from <i>host</i>.
         *
         * @param bookieIndex bookie index
         * @param host        host that respond the read
         * @param buffer      the data buffer
         * @return return true if we managed to complete the entry;
         * otherwise return false if the read entry is not complete or it is already completed before
         */
        boolean complete(int bookieIndex, BookieId host, final ByteBuf buffer) {
            ByteBuf content;
            if (isComplete()) {
                return false;
            }
            try {
                content = lh.macManager.verifyDigestAndReturnData(eId, buffer);
            } catch (BKException.BKDigestMatchException e) {
                clientCtx.getClientStats().getReadOpDmCounter().inc();
                logErrorAndReattemptRead(bookieIndex, host, "Mac mismatch", BKException.Code.DigestMatchException);
                return false;
            }

            if (!complete.getAndSet(true)) {
                rc = BKException.Code.OK;
                /*
                 * The length is a long and it is the last field of the metadata of an entry.
                 * Consequently, we have to subtract 8 from METADATA_LENGTH to get the length.
                 */
                entryImpl.setLength(buffer.getLong(DigestManager.METADATA_LENGTH - 8));
                entryImpl.setEntryBuf(content);
                writeSet.recycle();
                return true;
            } else {
                return false;
            }
        }
    }

    class ParallelReadRequest extends SingleLedgerEntryRequest {

        int numPendings;

        ParallelReadRequest(List<BookieId> ensemble, long lId, long eId) {
            super(ensemble, lId, eId);
            numPendings = writeSet.size();
        }

        @Override
        void read() {
            for (int i = 0; i < writeSet.size(); i++) {
                BookieId to = ensemble.get(writeSet.get(i));
                try {
                    sendReadTo(writeSet.get(i), to, this);
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted reading entry {} : ", this, ie);
                    Thread.currentThread().interrupt();
                    fail(BKException.Code.InterruptedException);
                    return;
                }
            }
        }

        @Override
        synchronized void logErrorAndReattemptRead(int bookieIndex, BookieId host, String errMsg, int rc) {
            super.logErrorAndReattemptRead(bookieIndex, host, errMsg, rc);
            // if received all responses or this entry doesn't meet quorum write, complete the request.

            --numPendings;
            if (isRecoveryRead && numBookiesMissingEntry >= requiredBookiesMissingEntryForRecovery) {
                /* For recovery, report NoSuchEntry as soon as wQ-aQ+1 bookies report that they do not
                 * have the entry */
                fail(BKException.Code.NoSuchEntryException);
            } else if (numPendings == 0) {
                // if received all responses, complete the request.
                fail(firstError);
            }
        }

        @Override
        BookieId maybeSendSpeculativeRead(BitSet heardFromHostsBitSet) {
            // no speculative read
            return null;
        }
    }

    class SequenceReadRequest extends SingleLedgerEntryRequest {
        static final int NOT_FOUND = -1;
        int nextReplicaIndexToReadFrom = 0;

        final BitSet sentReplicas;
        final BitSet erroredReplicas;

        SequenceReadRequest(List<BookieId> ensemble, long lId, long eId) {
            super(ensemble, lId, eId);

            this.sentReplicas = new BitSet(lh.getLedgerMetadata().getWriteQuorumSize());
            this.erroredReplicas = new BitSet(lh.getLedgerMetadata().getWriteQuorumSize());
        }

        private synchronized int getNextReplicaIndexToReadFrom() {
            return nextReplicaIndexToReadFrom;
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

        private boolean readsOutstanding() {
            return (sentReplicas.cardinality() - erroredReplicas.cardinality()) > 0;
        }

        /**
         * Send to next replica speculatively, if required and possible.
         * This returns the host we may have sent to for unit testing.
         * @return host we sent to if we sent. null otherwise.
         */
        @Override
        synchronized BookieId maybeSendSpeculativeRead(BitSet heardFrom) {
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getWriteQuorumSize()) {
                return null;
            }

            BitSet sentTo = getSentToBitSet();
            sentTo.and(heardFrom);

            // only send another read if we have had no successful response at all
            // (even for other entries) from any of the other bookies we have sent the
            // request to
            if (sentTo.cardinality() == 0) {
                clientCtx.getClientStats().getSpeculativeReadCounter().inc();
                return sendNextRead();
            } else {
                return null;
            }
        }

        @Override
        void read() {
            sendNextRead();
        }

        synchronized BookieId sendNextRead() {
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getWriteQuorumSize()) {
                // we are done, the read has failed from all replicas, just fail the
                // read
                fail(firstError);
                return null;
            }

            // ToDo: pick replica with writable PCBC. ISSUE #1239
            // https://github.com/apache/bookkeeper/issues/1239
            int replica = nextReplicaIndexToReadFrom;
            int bookieIndex = writeSet.get(nextReplicaIndexToReadFrom);
            nextReplicaIndexToReadFrom++;

            try {
                BookieId to = ensemble.get(bookieIndex);
                sendReadTo(bookieIndex, to, this);
                sentToHosts.add(to);
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
        synchronized void logErrorAndReattemptRead(int bookieIndex, BookieId host, String errMsg, int rc) {
            super.logErrorAndReattemptRead(bookieIndex, host, errMsg, rc);

            int replica = writeSet.indexOf(bookieIndex);
            if (replica == NOT_FOUND) {
                LOG.error("Received error from a host which is not in the ensemble {} {}.", host, ensemble);
                return;
            }
            erroredReplicas.set(replica);

            if (isRecoveryRead && (numBookiesMissingEntry >= requiredBookiesMissingEntryForRecovery)) {
                /* For recovery, report NoSuchEntry as soon as wQ-aQ+1 bookies report that they do not
                 * have the entry */
                fail(BKException.Code.NoSuchEntryException);
                return;
            }

            if (!readsOutstanding()) {
                sendNextRead();
            }
        }

        @Override
        boolean complete(int bookieIndex, BookieId host, ByteBuf buffer) {
            boolean completed = super.complete(bookieIndex, host, buffer);
            if (completed) {
                int numReplicasTried = getNextReplicaIndexToReadFrom();
                // Check if any speculative reads were issued and mark any slow bookies before
                // the first successful speculative read as "slow"
                for (int i = 0; i < numReplicasTried - 1; i++) {
                    int slowBookieIndex = writeSet.get(i);
                    BookieId slowBookieSocketAddress = ensemble.get(slowBookieIndex);
                    clientCtx.getPlacementPolicy().registerSlowBookie(slowBookieSocketAddress, eId);
                }
            }
            return completed;
        }
    }
}
