/**
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

import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.ByteBuf;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.ReadLastConfirmedAndEntryContext;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Long poll read operation.
 */
class ReadLastConfirmedAndEntryOp implements BookkeeperInternalCallbacks.ReadEntryCallback,
                                             SpeculativeRequestExecutor {

    static final Logger LOG = LoggerFactory.getLogger(ReadLastConfirmedAndEntryOp.class);

    ReadLACAndEntryRequest request;
    final BitSet heardFromHostsBitSet;
    final BitSet emptyResponsesFromHostsBitSet;
    final int maxMissedReadsAllowed;
    boolean parallelRead = false;
    final AtomicBoolean requestComplete = new AtomicBoolean(false);

    final long requestTimeNano;
    private final LedgerHandle lh;
    private final ClientContext clientCtx;
    private final LastConfirmedAndEntryCallback cb;

    private int numResponsesPending;
    private final int numEmptyResponsesAllowed;
    private volatile boolean hasValidResponse = false;
    private final long prevEntryId;
    private long lastAddConfirmed;
    private long timeOutInMillis;
    private final List<BookieSocketAddress> currentEnsemble;
    private ScheduledFuture<?> speculativeTask = null;

    abstract class ReadLACAndEntryRequest implements AutoCloseable {

        final AtomicBoolean complete = new AtomicBoolean(false);

        int rc = BKException.Code.OK;
        int firstError = BKException.Code.OK;
        int numMissedEntryReads = 0;

        final List<BookieSocketAddress> ensemble;
        final DistributionSchedule.WriteSet writeSet;
        final DistributionSchedule.WriteSet orderedEnsemble;
        final LedgerEntryImpl entryImpl;

        ReadLACAndEntryRequest(List<BookieSocketAddress> ensemble, long lId, long eId) {
            this.entryImpl = LedgerEntryImpl.create(lId, eId);
            this.ensemble = ensemble;
            this.writeSet = lh.getDistributionSchedule().getEnsembleSet(eId);
            if (clientCtx.getConf().enableReorderReadSequence) {
                this.orderedEnsemble = clientCtx.getPlacementPolicy().reorderReadLACSequence(ensemble,
                        lh.getBookiesHealthInfo(), writeSet.copy());
            } else {
                this.orderedEnsemble = writeSet.copy();
            }
        }

        @Override
        public void close() {
            entryImpl.close();
        }

        synchronized int getFirstError() {
            return firstError;
        }

        /**
         * Execute the read request.
         */
        abstract void read();

        /**
         * Complete the read request from <i>host</i>.
         *
         * @param bookieIndex
         *          bookie index
         * @param host
         *          host that respond the read
         * @param buffer
         *          the data buffer
         * @return return true if we managed to complete the entry;
         *         otherwise return false if the read entry is not complete or it is already completed before
         */
        boolean complete(int bookieIndex, BookieSocketAddress host, final ByteBuf buffer, long entryId) {
            ByteBuf content;
            try {
                content = lh.getDigestManager().verifyDigestAndReturnData(entryId, buffer);
            } catch (BKException.BKDigestMatchException e) {
                logErrorAndReattemptRead(bookieIndex, host, "Mac mismatch", BKException.Code.DigestMatchException);
                return false;
            }

            if (!complete.getAndSet(true)) {
                writeSet.recycle();
                orderedEnsemble.recycle();
                rc = BKException.Code.OK;
                /*
                 * The length is a long and it is the last field of the metadata of an entry.
                 * Consequently, we have to subtract 8 from METADATA_LENGTH to get the length.
                 */
                entryImpl.setLength(buffer.getLong(DigestManager.METADATA_LENGTH - 8));
                entryImpl.setEntryBuf(content);
                return true;
            } else {
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
                writeSet.recycle();
                orderedEnsemble.recycle();
                this.rc = rc;
                translateAndSetFirstError(rc);
                completeRequest();
                return true;
            } else {
                return false;
            }
        }

        private synchronized void translateAndSetFirstError(int rc) {
            if (BKException.Code.OK == firstError
                || BKException.Code.NoSuchEntryException == firstError
                || BKException.Code.NoSuchLedgerExistsException == firstError) {
                firstError = rc;
            } else if (BKException.Code.BookieHandleNotAvailableException == firstError
                && BKException.Code.NoSuchEntryException != rc
                && BKException.Code.NoSuchLedgerExistsException != rc) {
                // if other exception rather than NoSuchEntryException is returned
                // we need to update firstError to indicate that it might be a valid read but just failed.
                firstError = rc;
            }
        }

        /**
         * Log error <i>errMsg</i> and reattempt read from <i>host</i>.
         *
         * @param bookieIndex
         *          bookie index
         * @param host
         *          host that just respond
         * @param errMsg
         *          error msg to log
         * @param rc
         *          read result code
         */
        synchronized void logErrorAndReattemptRead(int bookieIndex, BookieSocketAddress host, String errMsg, int rc) {
            translateAndSetFirstError(rc);

            if (BKException.Code.NoSuchEntryException == rc || BKException.Code.NoSuchLedgerExistsException == rc) {
                // Since we send all long poll requests to every available node, we should only
                // treat these errors as failures if the node from which we received this is part of
                // the writeSet
                if (this.writeSet.contains(bookieIndex)) {
                    lh.registerOperationFailureOnBookie(host, entryImpl.getEntryId());
                }
                ++numMissedEntryReads;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("{} while reading entry: {} ledgerId: {} from bookie: {}", errMsg, entryImpl.getEntryId(),
                        lh.getId(), host);
            }
        }

        /**
         * Send to next replica speculatively, if required and possible.
         * This returns the host we may have sent to for unit testing.
         *
         * @param heardFromHostsBitSet
         *      the set of hosts that we already received responses.
         * @return host we sent to if we sent. null otherwise.
         */
        abstract BookieSocketAddress maybeSendSpeculativeRead(BitSet heardFromHostsBitSet);

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
            return String.format("L%d-E%d", entryImpl.getLedgerId(), entryImpl.getEntryId());
        }
    }

    class ParallelReadRequest extends ReadLACAndEntryRequest {

        int numPendings;

        ParallelReadRequest(List<BookieSocketAddress> ensemble, long lId, long eId) {
            super(ensemble, lId, eId);
            numPendings = orderedEnsemble.size();
        }

        @Override
        void read() {
            for (int i = 0; i < orderedEnsemble.size(); i++) {
                BookieSocketAddress to = ensemble.get(orderedEnsemble.get(i));
                try {
                    sendReadTo(orderedEnsemble.get(i), to, this);
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted reading entry {} : ", this, ie);
                    Thread.currentThread().interrupt();
                    fail(BKException.Code.InterruptedException);
                    return;
                }
            }
        }

        @Override
        synchronized void logErrorAndReattemptRead(int bookieIndex, BookieSocketAddress host, String errMsg, int rc) {
            super.logErrorAndReattemptRead(bookieIndex, host, errMsg, rc);
            --numPendings;
            // if received all responses or this entry doesn't meet quorum write, complete the request.
            if (numMissedEntryReads > maxMissedReadsAllowed || numPendings == 0) {
                if (BKException.Code.BookieHandleNotAvailableException == firstError
                        && numMissedEntryReads > maxMissedReadsAllowed) {
                    firstError = BKException.Code.NoSuchEntryException;
                }

                fail(firstError);
            }
        }

        @Override
        BookieSocketAddress maybeSendSpeculativeRead(BitSet heardFromHostsBitSet) {
            // no speculative read
            return null;
        }
    }

    class SequenceReadRequest extends ReadLACAndEntryRequest {
        static final int NOT_FOUND = -1;
        int nextReplicaIndexToReadFrom = 0;

        final BitSet sentReplicas;
        final BitSet erroredReplicas;
        final BitSet emptyResponseReplicas;

        SequenceReadRequest(List<BookieSocketAddress> ensemble, long lId, long eId) {
            super(ensemble, lId, eId);

            this.sentReplicas = new BitSet(orderedEnsemble.size());
            this.erroredReplicas = new BitSet(orderedEnsemble.size());
            this.emptyResponseReplicas = new BitSet(orderedEnsemble.size());
        }

        private synchronized int getNextReplicaIndexToReadFrom() {
            return nextReplicaIndexToReadFrom;
        }

        private int getReplicaIndex(int bookieIndex) {
            return orderedEnsemble.indexOf(bookieIndex);
        }

        private BitSet getSentToBitSet() {
            BitSet b = new BitSet(ensemble.size());

            for (int i = 0; i < sentReplicas.length(); i++) {
                if (sentReplicas.get(i)) {
                    b.set(orderedEnsemble.get(i));
                }
            }
            return b;
        }

        private boolean readsOutstanding() {
            return (sentReplicas.cardinality() - erroredReplicas.cardinality()
                    - emptyResponseReplicas.cardinality()) > 0;
        }

        /**
         * Send to next replica speculatively, if required and possible.
         * This returns the host we may have sent to for unit testing.
         * @return host we sent to if we sent. null otherwise.
         */
        @Override
        synchronized BookieSocketAddress maybeSendSpeculativeRead(BitSet heardFrom) {
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getEnsembleSize()) {
                return null;
            }

            BitSet sentTo = getSentToBitSet();
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
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getEnsembleSize()) {
                // we are done, the read has failed from all replicas, just fail the
                // read

                // Do it a bit pessimistically, only when finished trying all replicas
                // to check whether we received more missed reads than requiredBookiesMissingEntryForRecovery
                if (BKException.Code.BookieHandleNotAvailableException == firstError
                        && numMissedEntryReads > maxMissedReadsAllowed) {
                    firstError = BKException.Code.NoSuchEntryException;
                }

                fail(firstError);
                return null;
            }

            int replica = nextReplicaIndexToReadFrom;
            int bookieIndex = orderedEnsemble.get(nextReplicaIndexToReadFrom);
            nextReplicaIndexToReadFrom++;

            try {
                BookieSocketAddress to = ensemble.get(bookieIndex);
                sendReadTo(bookieIndex, to, this);
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
        synchronized void logErrorAndReattemptRead(int bookieIndex, BookieSocketAddress host, String errMsg, int rc) {
            super.logErrorAndReattemptRead(bookieIndex, host, errMsg, rc);

            int replica = getReplicaIndex(bookieIndex);
            if (replica == NOT_FOUND) {
                LOG.error("Received error from a host which is not in the ensemble {} {}.", host, ensemble);
                return;
            }

            if (BKException.Code.OK == rc) {
                emptyResponseReplicas.set(replica);
            } else {
                erroredReplicas.set(replica);
            }

            if (!readsOutstanding()) {
                sendNextRead();
            }
        }

        @Override
        boolean complete(int bookieIndex, BookieSocketAddress host, ByteBuf buffer, long entryId) {
            boolean completed = super.complete(bookieIndex, host, buffer, entryId);
            if (completed) {
                int numReplicasTried = getNextReplicaIndexToReadFrom();
                // Check if any speculative reads were issued and mark any bookies before the
                // first speculative read as slow
                for (int i = 0; i < numReplicasTried; i++) {
                    int slowBookieIndex = orderedEnsemble.get(i);
                    BookieSocketAddress slowBookieSocketAddress = ensemble.get(slowBookieIndex);
                    clientCtx.getPlacementPolicy().registerSlowBookie(slowBookieSocketAddress, entryId);
                }
            }
            return completed;
        }
    }

    ReadLastConfirmedAndEntryOp(LedgerHandle lh,
                                ClientContext clientCtx,
                                List<BookieSocketAddress> ensemble,
                                LastConfirmedAndEntryCallback cb,
                                long prevEntryId,
                                long timeOutInMillis) {
        this.lh = lh;
        this.clientCtx = clientCtx;
        this.cb = cb;
        this.prevEntryId = prevEntryId;
        this.lastAddConfirmed = lh.getLastAddConfirmed();
        this.timeOutInMillis = timeOutInMillis;
        this.numResponsesPending = 0;

        this.currentEnsemble = ensemble;
        // since long poll is effectively reading lac with waits, lac can be potentially
        // be advanced in different write quorums, so we need to make sure to cover enough
        // bookies before claiming lac is not advanced.
        this.numEmptyResponsesAllowed = getLedgerMetadata().getEnsembleSize()
                - getLedgerMetadata().getAckQuorumSize() + 1;
        this.requestTimeNano = MathUtils.nowInNano();

        maxMissedReadsAllowed = getLedgerMetadata().getEnsembleSize()
            - getLedgerMetadata().getAckQuorumSize();
        heardFromHostsBitSet = new BitSet(getLedgerMetadata().getEnsembleSize());
        emptyResponsesFromHostsBitSet = new BitSet(getLedgerMetadata().getEnsembleSize());
    }

    protected LedgerMetadata getLedgerMetadata() {
        return lh.getLedgerMetadata();
    }

    ReadLastConfirmedAndEntryOp parallelRead(boolean enabled) {
        this.parallelRead = enabled;
        return this;
    }

    protected void cancelSpeculativeTask(boolean mayInterruptIfRunning) {
        if (speculativeTask != null) {
            speculativeTask.cancel(mayInterruptIfRunning);
            speculativeTask = null;
        }
    }
    /**
     * Speculative Read Logic.
     */
    @Override
    public ListenableFuture<Boolean> issueSpeculativeRequest() {
        return clientCtx.getMainWorkerPool().submitOrdered(lh.getId(), new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                if (!requestComplete.get() && !request.isComplete()
                        && (null != request.maybeSendSpeculativeRead(heardFromHostsBitSet))) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Send speculative ReadLAC {} for ledger {} (previousLAC: {}). Hosts heard are {}.",
                                request, lh.getId(), lastAddConfirmed, heardFromHostsBitSet);
                    }
                    return true;
                }
                return false;
            }
        });
    }

    public void initiate() {
        if (parallelRead) {
            request = new ParallelReadRequest(currentEnsemble, lh.getId(), prevEntryId + 1);
        } else {
            request = new SequenceReadRequest(currentEnsemble, lh.getId(), prevEntryId + 1);
        }
        request.read();

        if (!parallelRead && clientCtx.getConf().readLACSpeculativeRequestPolicy.isPresent()) {
            speculativeTask = clientCtx.getConf().readLACSpeculativeRequestPolicy.get()
                .initiateSpeculativeRequest(clientCtx.getScheduler(), this);
        }
    }

    void sendReadTo(int bookieIndex, BookieSocketAddress to, ReadLACAndEntryRequest entry) throws InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Calling Read LAC and Entry with {} and long polling interval {} on Bookie {} - Parallel {}",
                    prevEntryId, timeOutInMillis, to, parallelRead);
        }
        clientCtx.getBookieClient().readEntryWaitForLACUpdate(to,
            lh.getId(),
            BookieProtocol.LAST_ADD_CONFIRMED,
            prevEntryId,
            timeOutInMillis,
            true,
            this, new ReadLastConfirmedAndEntryContext(bookieIndex, to));
        this.numResponsesPending++;
    }

    /**
     * Wrapper to get all recovered data from the request.
     */
    interface LastConfirmedAndEntryCallback {
        void readLastConfirmedAndEntryComplete(int rc, long lastAddConfirmed, LedgerEntry entry);
    }

    private void submitCallback(int rc) {
        long latencyMicros = MathUtils.elapsedMicroSec(requestTimeNano);
        LedgerEntry entry;
        cancelSpeculativeTask(true);
        if (BKException.Code.OK != rc) {
            clientCtx.getClientStats().getReadLacAndEntryOpLogger()
                .registerFailedEvent(latencyMicros, TimeUnit.MICROSECONDS);
            entry = null;
        } else {
            // could received advanced lac, with no entry
            clientCtx.getClientStats().getReadLacAndEntryOpLogger()
                .registerSuccessfulEvent(latencyMicros, TimeUnit.MICROSECONDS);
            if (request.entryImpl.getEntryBuffer() != null) {
                entry = new LedgerEntry(request.entryImpl);
            } else {
                entry = null;
            }
        }
        request.close();
        cb.readLastConfirmedAndEntryComplete(rc, lastAddConfirmed, entry);
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} received response for (lid={}, eid={}) : {}",
                    getClass().getName(), ledgerId, entryId, rc);
        }
        ReadLastConfirmedAndEntryContext rCtx = (ReadLastConfirmedAndEntryContext) ctx;
        BookieSocketAddress bookie = rCtx.getBookieAddress();
        numResponsesPending--;
        if (BKException.Code.OK == rc) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received lastAddConfirmed (lac={}) from bookie({}) for (lid={}).",
                        rCtx.getLastAddConfirmed(), bookie, ledgerId);
            }

            if (rCtx.getLastAddConfirmed() > lastAddConfirmed) {
                lastAddConfirmed = rCtx.getLastAddConfirmed();
                lh.updateLastConfirmed(rCtx.getLastAddConfirmed(), 0L);
            }

            hasValidResponse = true;

            if (entryId != BookieProtocol.LAST_ADD_CONFIRMED) {
                buffer.retain();
                if (!requestComplete.get() && request.complete(rCtx.getBookieIndex(), bookie, buffer, entryId)) {
                    // callback immediately
                    if (rCtx.getLacUpdateTimestamp().isPresent()) {
                        long elapsedMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()
                                - rCtx.getLacUpdateTimestamp().get());
                        elapsedMicros = Math.max(elapsedMicros, 0);
                        clientCtx.getClientStats().getReadLacAndEntryRespLogger()
                                .registerSuccessfulEvent(elapsedMicros, TimeUnit.MICROSECONDS);
                    }

                    // if the request has already completed, the buffer is not going to be used anymore, release it.
                    if (!completeRequest()) {
                        buffer.release();
                    }
                    heardFromHostsBitSet.set(rCtx.getBookieIndex(), true);
                } else {
                    buffer.release();
                }
            } else {
                emptyResponsesFromHostsBitSet.set(rCtx.getBookieIndex(), true);
                if (lastAddConfirmed > prevEntryId) {
                    // received advanced lac
                    completeRequest();
                } else if (emptyResponsesFromHostsBitSet.cardinality() >= numEmptyResponsesAllowed) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Completed readLACAndEntry(lid = {}, previousEntryId = {}) "
                                + "after received {} empty responses ('{}').",
                                ledgerId, prevEntryId, emptyResponsesFromHostsBitSet.cardinality(),
                                emptyResponsesFromHostsBitSet);
                    }
                    completeRequest();
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Received empty response for readLACAndEntry(lid = {}, previousEntryId = {}) from"
                                        + " bookie {} @ {}, reattempting reading next bookie : lac = {}",
                                ledgerId, prevEntryId, rCtx.getBookieAddress(),
                                rCtx.getBookieAddress(), lastAddConfirmed);
                    }
                    request.logErrorAndReattemptRead(rCtx.getBookieIndex(), bookie, "Empty Response", rc);
                }
                return;
            }
        } else if (BKException.Code.UnauthorizedAccessException == rc && !requestComplete.get()) {
            submitCallback(rc);
            requestComplete.set(true);
        } else {
            request.logErrorAndReattemptRead(rCtx.getBookieIndex(), bookie, "Error: " + BKException.getMessage(rc), rc);
            return;
        }

        if (numResponsesPending <= 0) {
            completeRequest();
        }
    }

    private boolean completeRequest() {
        boolean requestCompleted = requestComplete.compareAndSet(false, true);
        if (requestCompleted) {
            if (!hasValidResponse) {
                // no success called
                submitCallback(request.getFirstError());
            } else {
                // callback
                submitCallback(BKException.Code.OK);
            }
        }
        return requestCompleted;
    }

    @Override
    public String toString() {
        return String.format("ReadLastConfirmedAndEntryOp(lid=%d, prevEntryId=%d])", lh.getId(), prevEntryId);
    }

}
