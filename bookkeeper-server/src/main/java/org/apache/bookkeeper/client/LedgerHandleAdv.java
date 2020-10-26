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
import io.netty.buffer.Unpooled;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.AddCallbackWithLatency;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncAddCallback;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Ledger Advanced handle extends {@link LedgerHandle} to provide API to add entries with
 * user supplied entryIds. Through this interface Ledger Length may not be accurate while the
 * ledger being written.
 */
public class LedgerHandleAdv extends LedgerHandle implements WriteAdvHandle {
    static final Logger LOG = LoggerFactory.getLogger(LedgerHandleAdv.class);

    static class PendingOpsComparator implements Comparator<PendingAddOp>, Serializable {
        @Override
        public int compare(PendingAddOp o1, PendingAddOp o2) {
            return Long.compare(o1.entryId, o2.entryId);
        }
    }

    LedgerHandleAdv(ClientContext clientCtx,
                    long ledgerId, Versioned<LedgerMetadata> metadata,
                    BookKeeper.DigestType digestType, byte[] password, EnumSet<WriteFlag> writeFlags)
            throws GeneralSecurityException, NumberFormatException {
        super(clientCtx, ledgerId, metadata, digestType, password, writeFlags);
        pendingAddOps = new PriorityBlockingQueue<PendingAddOp>(10, new PendingOpsComparator());
    }


    /**
     * Add entry synchronously to an open ledger.
     *
     * @param entryId
     *            entryId of the entry to add
     * @param data
     *            array of bytes to be written to the ledger
     *            do not reuse the buffer, bk-client will release it appropriately
     * @return
     *            entryId that is just created.
     */
    @Override
    public long addEntry(final long entryId, byte[] data) throws InterruptedException, BKException {

        return addEntry(entryId, data, 0, data.length);

    }

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param entryId
     *            entryId of the entry to add
     * @param data
     *            array of bytes to be written to the ledger
     *            do not reuse the buffer, bk-client will release it appropriately
     * @param offset
     *            offset from which to take bytes from data
     * @param length
     *            number of bytes to take from data
     * @return The entryId of newly inserted entry.
     */
    @Override
    public long addEntry(final long entryId, byte[] data, int offset, int length) throws InterruptedException,
            BKException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding entry {}", data);
        }

        SyncAddCallback callback = new SyncAddCallback();
        asyncAddEntry(entryId, data, offset, length, callback, null);

        try {
            return callback.get();
        } catch (ExecutionException err) {
            throw (BKException) err.getCause();
        }
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param entryId
     *            entryId of the entry to add
     * @param data
     *            array of bytes to be written
     *            do not reuse the buffer, bk-client will release it appropriately
     * @param cb
     *            object implementing callbackinterface
     * @param ctx
     *            some control object
     */
    @Override
    public void asyncAddEntry(long entryId, byte[] data, AddCallback cb, Object ctx) {
        asyncAddEntry(entryId, data, 0, data.length, cb, ctx);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     *
     * @param entryId
     *            entryId of the entry to add
     * @param data
     *            array of bytes to be written
     *            do not reuse the buffer, bk-client will release it appropriately
     * @param offset
     *            offset from which to take bytes from data
     * @param length
     *            number of bytes to take from data
     * @param cb
     *            object implementing callbackinterface
     * @param ctx
     *            some control object
     * @throws ArrayIndexOutOfBoundsException
     *             if offset or length is negative or offset and length sum to a
     *             value higher than the length of data.
     */
    @Override
    public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length,
            final AddCallback cb, final Object ctx) {
        asyncAddEntry(entryId, Unpooled.wrappedBuffer(data, offset, length), cb, ctx);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     *
     * @param entryId
     *            entryId of the entry to add
     * @param data
     *            array of bytes to be written
     *            do not reuse the buffer, bk-client will release it appropriately
     * @param offset
     *            offset from which to take bytes from data
     * @param length
     *            number of bytes to take from data
     * @param cb
     *            object implementing callbackinterface
     * @param ctx
     *            some control object
     * @throws ArrayIndexOutOfBoundsException
     *             if offset or length is negative or offset and length sum to a
     *             value higher than the length of data.
     */
    @Override
    public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length,
                              final AddCallbackWithLatency cb, final Object ctx) {
        asyncAddEntry(entryId, Unpooled.wrappedBuffer(data, offset, length), cb, ctx);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     * This can be used only with {@link LedgerHandleAdv} returned through
     * ledgers created with {@link createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId
     *            entryId of the entry to add.
     * @param data
     *            io.netty.buffer.ByteBuf of bytes to be written
     *            do not reuse the buffer, bk-client will release it appropriately
     * @param cb
     *            object implementing callbackinterface
     * @param ctx
     *            some control object
     */
    @Override
    public void asyncAddEntry(final long entryId, ByteBuf data,
                              final AddCallbackWithLatency cb, final Object ctx) {
        PendingAddOp op = PendingAddOp.create(this, clientCtx, getCurrentEnsemble(), data, writeFlags, cb, ctx);
        op.setEntryId(entryId);

        if ((entryId <= this.lastAddConfirmed) || pendingAddOps.contains(op)) {
            LOG.error("Trying to re-add duplicate entryid:{}", entryId);
            op.submitCallback(BKException.Code.DuplicateEntryIdException);
            return;
        }
        doAsyncAddEntry(op);
    }

    /**
     * Overriding part is mostly around setting entryId.
     * Though there may be some code duplication, Choose to have the override routine so the control flow is
     * unaltered in the base class.
     */
    @Override
    protected void doAsyncAddEntry(final PendingAddOp op) {
        if (throttler != null) {
            throttler.acquire();
        }

        boolean wasClosed = false;
        synchronized (this) {
            // synchronized on this to ensure that
            // the ledger isn't closed between checking and
            // updating lastAddPushed
            if (isHandleWritable()) {
                long currentLength = addToLength(op.payload.readableBytes());
                op.setLedgerLength(currentLength);
                pendingAddOps.add(op);
            } else {
                wasClosed = true;
            }
        }

        if (wasClosed) {
            // make sure the callback is triggered in main worker pool
            try {
                clientCtx.getMainWorkerPool().submit(new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        LOG.warn("Attempt to add to closed ledger: {}", ledgerId);
                        op.cb.addCompleteWithLatency(BKException.Code.LedgerClosedException,
                                LedgerHandleAdv.this, op.getEntryId(), 0, op.ctx);
                    }
                    @Override
                    public String toString() {
                        return String.format("AsyncAddEntryToClosedLedger(lid=%d)", ledgerId);
                    }
                });
            } catch (RejectedExecutionException e) {
                op.cb.addCompleteWithLatency(BookKeeper.getReturnRc(clientCtx.getBookieClient(),
                                                                    BKException.Code.InterruptedException),
                        LedgerHandleAdv.this, op.getEntryId(), 0, op.ctx);
            }
            return;
        }

        if (!waitForWritable(distributionSchedule.getWriteSet(op.getEntryId()),
                    op.getEntryId(), 0, clientCtx.getConf().waitForWriteSetMs)) {
            op.allowFailFastOnUnwritableChannel();
        }

        try {
            clientCtx.getMainWorkerPool().executeOrdered(ledgerId, op);
        } catch (RejectedExecutionException e) {
            op.cb.addCompleteWithLatency(BookKeeper.getReturnRc(clientCtx.getBookieClient(),
                                                                BKException.Code.InterruptedException),
                              LedgerHandleAdv.this, op.getEntryId(), 0, op.ctx);
        }
    }

    @Override
    public CompletableFuture<Long> writeAsync(long entryId, ByteBuf data) {
        SyncAddCallback callback = new SyncAddCallback();
        asyncAddEntry(entryId, data, callback, data);
        return callback;
    }

    /**
     * LedgerHandleAdv will not allow addEntry without providing an entryId.
     */
    @Override
    public void asyncAddEntry(ByteBuf data, AddCallback cb, Object ctx) {
        cb.addCompleteWithLatency(BKException.Code.IllegalOpException, this, LedgerHandle.INVALID_ENTRY_ID, 0, ctx);
    }

    /**
     * LedgerHandleAdv will not allow addEntry without providing an entryId.
     */
    @Override
    public void asyncAddEntry(final byte[] data, final int offset, final int length,
                              final AddCallback cb, final Object ctx) {
        cb.addComplete(BKException.Code.IllegalOpException, this, LedgerHandle.INVALID_ENTRY_ID, ctx);
    }

}
