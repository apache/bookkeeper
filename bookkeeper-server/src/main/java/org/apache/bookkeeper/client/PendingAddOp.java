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

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.RejectedExecutionException;

/**
 * This represents a pending add operation. When it has got success from all
 * bookies, it sees if its at the head of the pending adds queue, and if yes,
 * sends ack back to the application. If a bookie fails, a replacement is made
 * and placed at the same position in the ensemble. The pending adds are then
 * rereplicated.
 *
 *
 */
class PendingAddOp extends SafeRunnable implements WriteCallback, TimerTask {
    private final static Logger LOG = LoggerFactory.getLogger(PendingAddOp.class);

    ByteBuf payload;
    ByteBuf toSend;
    AddCallback cb;
    Object ctx;
    long entryId;
    int entryLength;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;

    LedgerHandle lh;
    boolean isRecoveryAdd = false;
    long requestTimeNanos;

    int timeoutSec;
    Timeout timeout = null;

    OpStatsLogger addOpLogger;
    long currentLedgerLength;
    int pendingWriteRequests;
    boolean callbackTriggered;
    boolean hasRun;

    static PendingAddOp create(LedgerHandle lh, ByteBuf payload, AddCallback cb, Object ctx) {
        PendingAddOp op = RECYCLER.get();
        op.lh = lh;
        op.isRecoveryAdd = false;
        op.cb = cb;
        op.ctx = ctx;
        op.entryId = LedgerHandle.INVALID_ENTRY_ID;
        op.currentLedgerLength = -1;
        op.payload = payload;
        op.entryLength = payload.readableBytes();

        op.completed = false;
        op.ackSet = lh.distributionSchedule.getAckSet();
        op.addOpLogger = lh.bk.getAddOpLogger();
        if (op.timeout != null) {
            op.timeout.cancel();
        }
        op.timeout = null;
        op.timeoutSec = lh.bk.getConf().getAddEntryQuorumTimeout();
        op.pendingWriteRequests = 0;
        op.callbackTriggered = false;
        op.hasRun = false;
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

    void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    void setLedgerLength(long ledgerLength) {
        this.currentLedgerLength = ledgerLength;
    }

    long getEntryId() {
        return this.entryId;
    }

    void sendWriteRequest(int bookieIndex) {
        int flags = isRecoveryAdd ? BookieProtocol.FLAG_RECOVERY_ADD : BookieProtocol.FLAG_NONE;

        lh.bk.getBookieClient().addEntry(lh.metadata.currentEnsemble.get(bookieIndex), lh.ledgerId, lh.ledgerKey, entryId, toSend,
                this, bookieIndex, flags);
        ++pendingWriteRequests;
    }

    @Override
    public void run(Timeout timeout) {
        timeoutQuorumWait();
    }

    void timeoutQuorumWait() {
        try {
            lh.bk.getMainWorkerPool().submitOrdered(lh.ledgerId, new SafeRunnable() {
                @Override
                public void safeRun() {
                    if (completed) {
                        return;
                    }
                    lh.handleUnrecoverableErrorDuringAdd(BKException.Code.AddEntryQuorumTimeoutException);
                }
                @Override
                public String toString() {
                    return String.format("AddEntryQuorumTimeout(lid=%d, eid=%d)", lh.ledgerId, entryId);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.warn("Timeout add entry quorum wait failed {} entry: {}", lh.ledgerId, entryId);
        }
    }

    void unsetSuccessAndSendWriteRequest(int bookieIndex) {
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
        DistributionSchedule.WriteSet writeSet
            = lh.distributionSchedule.getWriteSet(entryId);
        try {
            if (!writeSet.contains(bookieIndex)) {
                lh.sendAddSuccessCallbacks();
                return;
            }
        } finally {
            writeSet.recycle();
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

        sendWriteRequest(bookieIndex);
    }

    /**
     * Initiate the add operation
     */
    public void safeRun() {
        hasRun = true;
        if (callbackTriggered) {
            // this should only be true if the request was failed due
            // to another request ahead in the pending queue,
            // so we can just ignore this request
            maybeRecycle();
            return;
        }

        if (timeoutSec > -1) {
            this.timeout = lh.bk.getBookieClient().scheduleTimeout(
                    this, timeoutSec, TimeUnit.SECONDS);
        }

        this.requestTimeNanos = MathUtils.nowInNano();
        checkNotNull(lh);
        checkNotNull(lh.macManager);

        this.toSend = lh.macManager.computeDigestAndPackageForSending(
                entryId, lh.lastAddConfirmed, currentLedgerLength,
                payload);

        // Iterate over set and trigger the sendWriteRequests
        DistributionSchedule.WriteSet writeSet
            = lh.distributionSchedule.getWriteSet(entryId);
        try {
            for (int i = 0; i < writeSet.size(); i++) {
                sendWriteRequest(writeSet.get(i));
            }
        } finally {
            writeSet.recycle();
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
        int bookieIndex = (Integer) ctx;
        --pendingWriteRequests;

        if (!lh.metadata.currentEnsemble.get(bookieIndex).equals(addr)) {
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
        }

        if (completed) {
            // even the add operation is completed, but because we don't reset completed flag back to false when
            // #unsetSuccessAndSendWriteRequest doesn't break ack quorum constraint. we still have current pending
            // add op is completed but never callback. so do a check here to complete again.
            //
            // E.g. entry x is going to complete.
            //
            // 1) entry x + k hits a failure. lh.handleBookieFailure increases blockAddCompletions to 1, for ensemble change
            // 2) entry x receives all responses, sets completed to true but fails to send success callback because
            //    blockAddCompletions is 1
            // 3) ensemble change completed. lh unset success starting from x to x+k, but since the unset doesn't break ackSet
            //    constraint. #removeBookieAndCheck doesn't set completed back to false.
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
        case BKException.Code.LedgerFencedException:
            LOG.warn("Fencing exception on write: L{} E{} on {}",
                     new Object[] { ledgerId, entryId, addr });
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        case BKException.Code.UnauthorizedAccessException:
            LOG.warn("Unauthorized access exception on write: L{} E{} on {}",
                     new Object[] { ledgerId, entryId, addr });
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        default:
            if (lh.bk.delayEnsembleChange) {
                if (ackSet.failBookieAndCheck(bookieIndex, addr) || rc == BKException.Code.WriteOnReadOnlyBookieException) {
                    Map<Integer, BookieSocketAddress> failedBookies = ackSet.getFailedBookies();
                    LOG.warn("Failed to write entry ({}, {}) to bookies {}, handling failures.",
                             new Object[] { ledgerId, entryId, failedBookies });
                    // we can't meet ack quorum requirement, trigger ensemble change.
                    lh.handleBookieFailure(failedBookies);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Failed to write entry ({}, {}) to bookie ({}, {})," +
                                  " but it didn't break ack quorum, delaying ensemble change : {}",
                                  new Object[] { ledgerId, entryId, bookieIndex, addr, BKException.getMessage(rc) });
                    }
                }
            } else {
                LOG.warn("Failed to write entry ({}, {}): {}",
                         new Object[] { ledgerId, entryId, BKException.getMessage(rc) });
                lh.handleBookieFailure(ImmutableMap.of(bookieIndex, addr));
            }
            return;
        }

        if (ackQuorum && !completed) {
            completed = true;

            sendAddSuccessCallbacks();
        }
    }

    void sendAddSuccessCallbacks() {
        lh.sendAddSuccessCallbacks();
    }

    void submitCallback(final int rc) {
        if (null != timeout) {
            timeout.cancel();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Submit callback (lid:{}, eid: {}). rc:{}", lh.getId(), entryId, rc);
        }

        long latencyNanos = MathUtils.elapsedNanos(requestTimeNanos);
        if (rc != BKException.Code.OK) {
            addOpLogger.registerFailedEvent(latencyNanos, TimeUnit.NANOSECONDS);
            LOG.error("Write of ledger entry to quorum failed: L{} E{}",
                      lh.getId(), entryId);
        } else {
            addOpLogger.registerSuccessfulEvent(latencyNanos, TimeUnit.NANOSECONDS);
        }
        cb.addComplete(rc, lh, entryId, ctx);
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
           return (this.entryId == ((PendingAddOp)o).entryId);
       }
       return (this == o);
    }

    private final Handle<PendingAddOp> recyclerHandle;
    private static final Recycler<PendingAddOp> RECYCLER = new Recycler<PendingAddOp>() {
        protected PendingAddOp newObject(Recycler.Handle<PendingAddOp> handle) {
            return new PendingAddOp(handle);
        }
    };

    private PendingAddOp(Handle<PendingAddOp> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private void maybeRecycle() {
        // The reference to PendingAddOp can be held in 3 places
        // - LedgerHandle#pendingAddOp
        //   This reference is released when the callback is run
        // - The executor
        //   Released after safeRun finishes
        // - BookieClient
        //   Holds a reference from the point the addEntry requests are
        //   sent.
        // The object can only be recycled after all references are
        // released, otherwise we could end up recycling twice and all
        // joy that goes along with that.
        if (hasRun && callbackTriggered && pendingWriteRequests == 0) {
            recycle();
        }
    }

    private void recycle() {
        entryId = LedgerHandle.INVALID_ENTRY_ID;
        currentLedgerLength = -1;
        ReferenceCountUtil.release(toSend);
        payload = null;
        toSend = null;
        cb = null;
        ctx = null;
        ackSet.recycle();
        ackSet = null;
        lh = null;
        isRecoveryAdd = false;
        addOpLogger = null;
        completed = false;
        pendingWriteRequests = 0;
        callbackTriggered = false;
        hasRun = false;
        if (timeout != null) {
            timeout.cancel();
        }
        timeout = null;

        recyclerHandle.recycle(this);
    }
}
