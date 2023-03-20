/*
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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.proto.BookieProtocol.FLAG_HIGH_PRIORITY;
import static org.apache.bookkeeper.proto.BookieProtocol.FLAG_NONE;
import static org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.AsyncCallback.AddCallbackWithLatency;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a pending add operation. When it has got success from all
 * bookies, it sees if its at the head of the pending adds queue, and if yes,
 * sends ack back to the application. If a bookie fails, a replacement is made
 * and placed at the same position in the ensemble. The pending adds are then
 * rereplicated.
 *
 *
 */
class PendingAddOp implements WriteCallback {
    private static final Logger LOG = LoggerFactory.getLogger(PendingAddOp.class);

    ByteBuf payload;
    ReferenceCounted toSend;
    AddCallbackWithLatency cb;
    Object ctx;
    long entryId;
    int entryLength;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;

    LedgerHandle lh;
    ClientContext clientCtx;
    boolean isRecoveryAdd = false;
    volatile long requestTimeNanos;
    long qwcLatency; // Quorum Write Completion Latency after response from quorum bookies.
    Set<BookieId> addEntrySuccessBookies;
    long writeDelayedStartTime; // min fault domains completion latency after response from ack quorum bookies

    long currentLedgerLength;
    int pendingWriteRequests;
    boolean callbackTriggered;
    boolean hasRun;
    EnumSet<WriteFlag> writeFlags;
    boolean allowFailFast = false;
    List<BookieId> ensemble;

    @SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
    static PendingAddOp create(LedgerHandle lh, ClientContext clientCtx,
                               List<BookieId> ensemble,
                               ByteBuf payload, EnumSet<WriteFlag> writeFlags,
                               AddCallbackWithLatency cb, Object ctx) {
        PendingAddOp op = RECYCLER.get();
        op.lh = lh;
        op.clientCtx = clientCtx;
        op.isRecoveryAdd = false;
        op.cb = cb;
        op.ctx = ctx;
        op.entryId = LedgerHandle.INVALID_ENTRY_ID;
        op.currentLedgerLength = -1;
        op.payload = payload;
        op.entryLength = payload.readableBytes();

        op.completed = false;
        op.ensemble = ensemble;
        op.ackSet = lh.getDistributionSchedule().getAckSet();
        op.pendingWriteRequests = 0;
        op.callbackTriggered = false;
        op.hasRun = false;
        op.requestTimeNanos = Long.MAX_VALUE;
        op.allowFailFast = false;
        op.qwcLatency = 0;
        op.writeFlags = writeFlags;

        if (op.addEntrySuccessBookies == null) {
            op.addEntrySuccessBookies = new HashSet<>();
        } else {
            op.addEntrySuccessBookies.clear();
        }
        op.writeDelayedStartTime = -1;

        return op;
    }

    /**
     * Enable the recovery add flag for this operation.
     * @see LedgerHandle#asyncRecoveryAddEntry
     */
    PendingAddOp enableRecoveryAdd() {
        isRecoveryAdd = true;
        return this;
    }

    PendingAddOp allowFailFastOnUnwritableChannel() {
        allowFailFast = true;
        return this;
    }

    void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    void setLedgerLength(long ledgerLength) {
        this.currentLedgerLength = ledgerLength;
    }

    long getEntryId() {
        return this.entryId;
    }

    private void sendWriteRequest(List<BookieId> ensemble, int bookieIndex) {
        int flags = isRecoveryAdd ? FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY : FLAG_NONE;

        clientCtx.getBookieClient().addEntry(ensemble.get(bookieIndex),
                                             lh.ledgerId, lh.ledgerKey, entryId, toSend, this, bookieIndex,
                                             flags, allowFailFast, lh.writeFlags);
        ++pendingWriteRequests;
    }

    boolean maybeTimeout() {
        if (MathUtils.elapsedNanos(requestTimeNanos) >= clientCtx.getConf().addEntryQuorumTimeoutNanos) {
            timeoutQuorumWait();
            return true;
        }
        return false;
    }

    synchronized void timeoutQuorumWait() {
        if (completed) {
            return;
        }

        if (addEntrySuccessBookies.size() >= lh.getLedgerMetadata().getAckQuorumSize()) {
            // If ackQuorum number of bookies have acknowledged the write but still not complete, indicates
            // failures due to not having been written to enough fault domains. Increment corresponding
            // counter.
            clientCtx.getClientStats().getWriteTimedOutDueToNotEnoughFaultDomains().inc();
        }

        lh.handleUnrecoverableErrorDuringAdd(BKException.Code.AddEntryQuorumTimeoutException);
    }

    synchronized void unsetSuccessAndSendWriteRequest(List<BookieId> ensemble, int bookieIndex) {
        // update the ensemble
        this.ensemble = ensemble;

        if (toSend == null) {
            // this addOp hasn't yet had its mac computed. When the mac is
            // computed, its write requests will be sent, so no need to send it
            // now
            return;
        }
        // Suppose that unset doesn't happen on the write set of an entry. In this
        // case we don't need to resend the write request upon an ensemble change.
        // We do need to invoke #sendAddSuccessCallbacks() for such entries because
        // they may have already completed, but they are just waiting for the ensemble
        // to change.
        // E.g.
        // ensemble (A, B, C, D), entry k is written to (A, B, D). An ensemble change
        // happens to replace C with E. Entry k does not complete until C is
        // replaced with E successfully. When the ensemble change completes, it tries
        // to unset entry k. C however is not in k's write set, so no entry is written
        // again, and no one triggers #sendAddSuccessCallbacks. Consequently, k never
        // completes.
        //
        // We call sendAddSuccessCallback when unsetting t cover this case.
        if (!lh.distributionSchedule.hasEntry(entryId, bookieIndex)) {
            lh.sendAddSuccessCallbacks();
            return;
        }

        if (callbackTriggered) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unsetting success for ledger: " + lh.ledgerId + " entry: " + entryId + " bookie index: "
                      + bookieIndex);
        }

        // if we had already heard a success from this array index, need to
        // increment our number of responses that are pending, since we are
        // going to unset this success
        if (!ackSet.removeBookieAndCheck(bookieIndex)) {
            // unset completed if this results in loss of ack quorum
            completed = false;
        }

        sendWriteRequest(ensemble, bookieIndex);
    }

    /**
     * Initiate the add operation.
     */
    public synchronized void initiate() {
        hasRun = true;
        if (callbackTriggered) {
            // this should only be true if the request was failed due
            // to another request ahead in the pending queue,
            // so we can just ignore this request
            maybeRecycle();
            return;
        }

        this.requestTimeNanos = MathUtils.nowInNano();
        checkNotNull(lh);
        checkNotNull(lh.macManager);

        int flags = isRecoveryAdd ? FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY : FLAG_NONE;
        this.toSend = lh.macManager.computeDigestAndPackageForSending(
                entryId, lh.lastAddConfirmed, currentLedgerLength,
                payload, lh.ledgerKey, flags);
        // ownership of RefCounted ByteBuf was passed to computeDigestAndPackageForSending
        payload = null;

        // We are about to send. Check if we need to make an ensemble change
        // because of delayed write errors
        lh.maybeHandleDelayedWriteBookieFailure();

        // Iterate over set and trigger the sendWriteRequests
        for (int i = 0; i < lh.distributionSchedule.getWriteQuorumSize(); i++) {
            sendWriteRequest(ensemble, lh.distributionSchedule.getWriteSetBookieIndex(entryId, i));
        }
    }

    @Override
    public synchronized void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
        int bookieIndex = (Integer) ctx;
        --pendingWriteRequests;

        if (!ensemble.get(bookieIndex).equals(addr)) {
            // ensemble has already changed, failure of this addr is immaterial
            if (LOG.isDebugEnabled()) {
                LOG.debug("Write did not succeed: " + ledgerId + ", " + entryId + ". But we have already fixed it.");
            }
            return;
        }

        // must record all acks, even if complete (completion can be undone by an ensemble change)
        boolean ackQuorum = false;
        if (BKException.Code.OK == rc) {
            ackQuorum = ackSet.completeBookieAndCheck(bookieIndex);
            addEntrySuccessBookies.add(ensemble.get(bookieIndex));
        }

        if (completed) {
            if (rc != BKException.Code.OK) {
                // Got an error after satisfying AQ. This means we are under replicated at the create itself.
                // Update the stat to reflect it.
                clientCtx.getClientStats().getAddOpUrCounter().inc();
                if (!clientCtx.getConf().disableEnsembleChangeFeature.isAvailable()
                        && !clientCtx.getConf().delayEnsembleChange) {
                    lh.notifyWriteFailed(bookieIndex, addr);
                }
            }
            // even the add operation is completed, but because we don't reset completed flag back to false when
            // #unsetSuccessAndSendWriteRequest doesn't break ack quorum constraint. we still have current pending
            // add op is completed but never callback. so do a check here to complete again.
            //
            // E.g. entry x is going to complete.
            //
            // 1) entry x + k hits a failure. lh.handleBookieFailure increases blockAddCompletions to 1, for ensemble
            //    change
            // 2) entry x receives all responses, sets completed to true but fails to send success callback because
            //    blockAddCompletions is 1
            // 3) ensemble change completed. lh unset success starting from x to x+k, but since the unset doesn't break
            //    ackSet constraint. #removeBookieAndCheck doesn't set completed back to false.
            // 4) so when the retry request on new bookie completes, it finds the pending op is already completed.
            //    we have to trigger #sendAddSuccessCallbacks
            //
            sendAddSuccessCallbacks();
            // I am already finished, ignore incoming responses.
            // otherwise, we might hit the following error handling logic, which might cause bad things.
            maybeRecycle();
            return;
        }

        switch (rc) {
        case BKException.Code.OK:
            // continue
            break;
        case BKException.Code.ClientClosedException:
            // bookie client is closed.
            lh.errorOutPendingAdds(rc);
            return;
        case BKException.Code.IllegalOpException:
            // illegal operation requested, like using unsupported feature in v2 protocol
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        case BKException.Code.LedgerFencedException:
            LOG.warn("Fencing exception on write: L{} E{} on {}",
                    ledgerId, entryId, addr);
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        case BKException.Code.UnauthorizedAccessException:
            LOG.warn("Unauthorized access exception on write: L{} E{} on {}",
                    ledgerId, entryId, addr);
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        default:
            if (clientCtx.getConf().delayEnsembleChange) {
                if (ackSet.failBookieAndCheck(bookieIndex, addr)
                        || rc == BKException.Code.WriteOnReadOnlyBookieException) {
                    Map<Integer, BookieId> failedBookies = ackSet.getFailedBookies();
                    LOG.warn("Failed to write entry ({}, {}) to bookies {}, handling failures.",
                            ledgerId, entryId, failedBookies);
                    // we can't meet ack quorum requirement, trigger ensemble change.
                    lh.handleBookieFailure(failedBookies);
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to write entry ({}, {}) to bookie ({}, {}),"
                                    + " but it didn't break ack quorum, delaying ensemble change : {}",
                            ledgerId, entryId, bookieIndex, addr, BKException.getMessage(rc));
                }
            } else {
                LOG.warn("Failed to write entry ({}, {}) to bookie ({}, {}): {}",
                        ledgerId, entryId, bookieIndex, addr, BKException.getMessage(rc));
                lh.handleBookieFailure(ImmutableMap.of(bookieIndex, addr));
            }
            return;
        }

        if (ackQuorum && !completed) {
            if (clientCtx.getConf().enforceMinNumFaultDomainsForWrite
                && !(clientCtx.getPlacementPolicy()
                              .areAckedBookiesAdheringToPlacementPolicy(addEntrySuccessBookies,
                                                                        lh.getLedgerMetadata().getWriteQuorumSize(),
                                                                        lh.getLedgerMetadata().getAckQuorumSize()))) {
                LOG.warn("Write success for entry ID {} delayed, not acknowledged by bookies in enough fault domains",
                         entryId);
                // Increment to indicate write did not complete due to not enough fault domains
                clientCtx.getClientStats().getWriteDelayedDueToNotEnoughFaultDomains().inc();

                // Only do this for the first time.
                if (writeDelayedStartTime == -1) {
                    writeDelayedStartTime = MathUtils.nowInNano();
                }
            } else {
                completed = true;
                this.qwcLatency = MathUtils.elapsedNanos(requestTimeNanos);

                if (writeDelayedStartTime != -1) {
                    clientCtx.getClientStats()
                             .getWriteDelayedDueToNotEnoughFaultDomainsLatency()
                             .registerSuccessfulEvent(MathUtils.elapsedNanos(writeDelayedStartTime),
                                                      TimeUnit.NANOSECONDS);
                }

                sendAddSuccessCallbacks();
            }
        }
    }

    void sendAddSuccessCallbacks() {
        lh.sendAddSuccessCallbacks();
    }

    synchronized void submitCallback(final int rc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Submit callback (lid:{}, eid: {}). rc:{}", lh.getId(), entryId, rc);
        }

        long latencyNanos = MathUtils.elapsedNanos(requestTimeNanos);
        if (rc != BKException.Code.OK) {
            clientCtx.getClientStats().getAddOpLogger().registerFailedEvent(latencyNanos, TimeUnit.NANOSECONDS);
            LOG.error("Write of ledger entry to quorum failed: L{} E{}",
                      lh.getId(), entryId);
        } else {
            clientCtx.getClientStats().getAddOpLogger().registerSuccessfulEvent(latencyNanos, TimeUnit.NANOSECONDS);
        }
        cb.addCompleteWithLatency(rc, lh, entryId, qwcLatency, ctx);
        callbackTriggered = true;

        maybeRecycle();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PendingAddOp(lid:").append(lh.ledgerId)
          .append(", eid:").append(entryId).append(", completed:")
          .append(completed).append(")");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return (int) entryId;
    }

    @Override
    public boolean equals(Object o) {
       if (o instanceof PendingAddOp) {
           return (this.entryId == ((PendingAddOp) o).entryId);
       }
       return (this == o);
    }

    private final Handle<PendingAddOp> recyclerHandle;
    private static final Recycler<PendingAddOp> RECYCLER = new Recycler<PendingAddOp>() {
        @Override
        protected PendingAddOp newObject(Recycler.Handle<PendingAddOp> handle) {
            return new PendingAddOp(handle);
        }
    };

    private PendingAddOp(Handle<PendingAddOp> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }


    private synchronized void maybeRecycle() {
        /**
         * We have opportunity to recycle two objects here.
         * PendingAddOp#toSend and LedgerHandle#pendingAddOp
         *
         * A. LedgerHandle#pendingAddOp: This can be released after all 3 conditions are met.
         *    - After the callback is run
         *    - After safeRun finished by the executor
         *    - Write responses are returned from all bookies
         *      as BookieClient Holds a reference from the point the addEntry requests are sent.
         *
         * B. ByteBuf can be released after 2 of the conditions are met.
         *    - After the callback is run as we will not retry the write after callback
         *    - After safeRun finished by the executor
         * BookieClient takes and releases on this buffer immediately after sending the data.
         *
         * The object can only be recycled after the above conditions are met
         * otherwise we could end up recycling twice and all
         * joy that goes along with that.
         */
        if (hasRun && callbackTriggered) {
            ReferenceCountUtil.release(toSend);
            toSend = null;
        }
        // only recycle a pending add op after it has been run.
        if (hasRun && toSend == null && pendingWriteRequests == 0) {
            recyclePendAddOpObject();
        }
    }

    public synchronized void recyclePendAddOpObject() {
        entryId = LedgerHandle.INVALID_ENTRY_ID;
        currentLedgerLength = -1;
        if (payload != null) {
            ReferenceCountUtil.release(payload);
            payload = null;
        }
        cb = null;
        ctx = null;
        ensemble = null;
        ackSet.recycle();
        ackSet = null;
        lh = null;
        clientCtx = null;
        isRecoveryAdd = false;
        completed = false;
        pendingWriteRequests = 0;
        callbackTriggered = false;
        hasRun = false;
        allowFailFast = false;
        writeFlags = null;
        addEntrySuccessBookies.clear();
        writeDelayedStartTime = -1;

        recyclerHandle.recycle(this);
    }
}
