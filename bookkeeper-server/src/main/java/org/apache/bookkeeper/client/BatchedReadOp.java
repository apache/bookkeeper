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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchedReadOp extends ReadOpBase implements BatchedReadEntryCallback {

    private static final Logger LOG = LoggerFactory.getLogger(BatchedReadOp.class);

    final int maxCount;
    final long maxSize;

    BatchedLedgerEntryRequest request;

    BatchedReadOp(LedgerHandle lh,
                  ClientContext clientCtx,
                  long startEntryId,
                  int maxCount,
                  long maxSize,
                  boolean isRecoveryRead) {
        super(lh, clientCtx, startEntryId, -1L, isRecoveryRead);
        this.maxCount = maxCount;
        this.maxSize = maxSize;
    }

    @Override
    void initiate() {
        this.requestTimeNanos = MathUtils.nowInNano();
        List<BookieId> ensemble = getLedgerMetadata().getEnsembleAt(startEntryId);
        request = new SequenceReadRequest(ensemble, lh.ledgerId, startEntryId, maxCount, maxSize);
        request.read();
        if (clientCtx.getConf().readSpeculativeRequestPolicy.isPresent()) {
            speculativeTask = clientCtx.getConf().readSpeculativeRequestPolicy.get()
                    .initiateSpeculativeRequest(clientCtx.getScheduler(), request);
        }
    }

    @Override
    protected void submitCallback(int code) {
        // ensure callback once
        if (!complete.compareAndSet(false, true)) {
            return;
        }

        cancelSpeculativeTask(true);

        long latencyNanos = MathUtils.elapsedNanos(requestTimeNanos);
        if (code != BKException.Code.OK) {
            LOG.error(
                    "Batch read of ledger entry failed: L{} E{}-E{}, Sent to {}, "
                            + "Heard from {} : bitset = {}, Error = '{}'. First unread entry is ({}, rc = {})",
                    lh.getId(), startEntryId, startEntryId + maxCount - 1, sentToHosts, heardFromHosts,
                    heardFromHostsBitSet, BKException.getMessage(code), startEntryId, code);
            clientCtx.getClientStats().getReadOpLogger().registerFailedEvent(latencyNanos, TimeUnit.NANOSECONDS);
            // release the entries

            request.close();
            future.completeExceptionally(BKException.create(code));
        } else {
            clientCtx.getClientStats().getReadOpLogger().registerSuccessfulEvent(latencyNanos, TimeUnit.NANOSECONDS);
            future.complete(LedgerEntriesImpl.create(request.entries));
        }
    }

    @Override
    public void readEntriesComplete(int rc, long ledgerId, long startEntryId, ByteBufList bufList, Object ctx) {
        final ReadContext rctx = (ReadContext) ctx;
        final BatchedLedgerEntryRequest entry = (BatchedLedgerEntryRequest) rctx.entry;

        if (rc != BKException.Code.OK) {
            entry.logErrorAndReattemptRead(rctx.bookieIndex, rctx.to, "Error: " + BKException.getMessage(rc), rc);
            return;
        }

        heardFromHosts.add(rctx.to);
        heardFromHostsBitSet.set(rctx.bookieIndex, true);

        bufList.retain();
        // if entry has completed don't handle twice
        if (entry.complete(rctx.bookieIndex, rctx.to, bufList)) {
            if (!isRecoveryRead) {
                // do not advance LastAddConfirmed for recovery reads
                lh.updateLastConfirmed(rctx.getLastAddConfirmed(), 0L);
            }
            submitCallback(BKException.Code.OK);
        } else {
            bufList.release();
        }
    }

    void sendReadTo(int bookieIndex, BookieId to, BatchedLedgerEntryRequest entry) throws InterruptedException {
        if (lh.throttler != null) {
            lh.throttler.acquire();
        }
        if (isRecoveryRead) {
            int flags = BookieProtocol.FLAG_HIGH_PRIORITY | BookieProtocol.FLAG_DO_FENCING;
            clientCtx.getBookieClient().batchReadEntries(to, lh.ledgerId, entry.eId,
                    maxCount, maxSize, this, new ReadContext(bookieIndex, to, entry), flags, lh.ledgerKey);
        } else {
            clientCtx.getBookieClient().batchReadEntries(to, lh.ledgerId, entry.eId, maxCount, maxSize,
                    this, new ReadContext(bookieIndex, to, entry), BookieProtocol.FLAG_NONE);
        }
    }

    abstract class BatchedLedgerEntryRequest extends LedgerEntryRequest {

        //Indicate which ledger the BatchedLedgerEntryRequest is reading.
        final long lId;
        final int maxCount;
        final long maxSize;

        final List<LedgerEntry> entries;

        BatchedLedgerEntryRequest(List<BookieId> ensemble, long lId, long eId, int maxCount, long maxSize) {
            super(ensemble, eId);
            this.lId = lId;
            this.maxCount = maxCount;
            this.maxSize = maxSize;
            this.entries = new ArrayList<>(maxCount);
        }

        boolean complete(int bookieIndex, BookieId host, final ByteBufList bufList) {
            if (isComplete()) {
                return false;
            }
            if (!complete.getAndSet(true)) {
                for (int i = 0; i < bufList.size(); i++) {
                    ByteBuf buffer = bufList.getBuffer(i);
                    ByteBuf content;
                    try {
                        content = lh.macManager.verifyDigestAndReturnData(eId + i, buffer);
                    } catch (BKException.BKDigestMatchException e) {
                        clientCtx.getClientStats().getReadOpDmCounter().inc();
                        logErrorAndReattemptRead(bookieIndex, host, "Mac mismatch",
                                BKException.Code.DigestMatchException);
                        return false;
                    }
                    rc = BKException.Code.OK;
                    /*
                     * The length is a long and it is the last field of the metadata of an entry.
                     * Consequently, we have to subtract 8 from METADATA_LENGTH to get the length.
                     */
                    LedgerEntryImpl entryImpl =  LedgerEntryImpl.create(lh.ledgerId, startEntryId + i);
                    entryImpl.setLength(buffer.getLong(DigestManager.METADATA_LENGTH - 8));
                    entryImpl.setEntryBuf(content);
                    entries.add(entryImpl);
                }
                writeSet.recycle();
                return true;
            } else {
                writeSet.recycle();
                return false;
            }
        }

        @Override
        public String toString() {
            return String.format("L%d-E%d~%d s-%d", lh.getId(), eId, eId + maxCount, maxSize);
        }
    }

    class SequenceReadRequest extends BatchedLedgerEntryRequest {

        static final int NOT_FOUND = -1;
        int nextReplicaIndexToReadFrom = 0;
        final BitSet sentReplicas;
        final BitSet erroredReplicas;
        SequenceReadRequest(List<BookieId> ensemble,
                            long lId,
                            long eId,
                            int maxCount,
                            long maxSize) {
            super(ensemble, lId, eId, maxCount, maxSize);
            this.sentReplicas = new BitSet(lh.getLedgerMetadata().getWriteQuorumSize());
            this.erroredReplicas = new BitSet(lh.getLedgerMetadata().getWriteQuorumSize());
        }

        @Override
        void read() {
            sendNextRead();
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
        boolean complete(int bookieIndex, BookieId host, final ByteBufList bufList) {
            boolean completed = super.complete(bookieIndex, host, bufList);
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