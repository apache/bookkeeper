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

import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger Advanced handle extends {@link LedgerHandle} to provide API to add entries with
 * user supplied entryIds. Through this interface Ledger Length may not be accurate wile the
 * ledger being written.
 */
public class LedgerHandleAdv extends LedgerHandle {
    final static Logger LOG = LoggerFactory.getLogger(LedgerHandleAdv.class);

    static class PendingOpsComparator implements Comparator<PendingAddOp>, Serializable {
        public int compare(PendingAddOp o1, PendingAddOp o2) {
            return Long.compare(o1.entryId, o2.entryId);
        }
    }

    LedgerHandleAdv(BookKeeper bk, long ledgerId, LedgerMetadata metadata, DigestType digestType, byte[] password)
            throws GeneralSecurityException, NumberFormatException {
        super(bk, ledgerId, metadata, digestType, password);
        pendingAddOps = new PriorityBlockingQueue<PendingAddOp>(10, new PendingOpsComparator());
    }


    /**
     * Add entry synchronously to an open ledger.
     *
     * @param entryId
     *            entryId of the entry to add
     * @param data
     *            array of bytes to be written to the ledger
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
     * @param offset
     *            offset from which to take bytes from data
     * @param length
     *            number of bytes to take from data
     * @return The entryId of newly inserted entry.
     */
    @Override
    public long addEntry(final long entryId, byte[] data, int offset, int length) throws InterruptedException,
            BKException {
        LOG.debug("Adding entry {}", data);

        SyncCounter counter = new SyncCounter();

        SyncAddCallback callback = new SyncAddCallback();
        asyncAddEntry(entryId, data, offset, length, callback, counter);

        counter.block();

        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }
        return callback.entryId;
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param entryId
     *            entryId of the entry to add
     * @param data
     *            array of bytes to be written
     * @param cb
     *            object implementing callbackinterface
     * @param ctx
     *            some control object
     */
    @Override
    public void asyncAddEntry(long entryId, byte[] data, AddCallback cb, Object ctx) throws BKException {
        asyncAddEntry(entryId, data, 0, data.length, cb, ctx);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     *
     * @param entryId
     *            entryId of the entry to add
     * @param data
     *            array of bytes to be written
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

    public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length,
            final AddCallback cb, final Object ctx) {
        PendingAddOp op = new PendingAddOp(this, cb, ctx);
        op.setEntryId(entryId);
        if ((entryId <= this.lastAddConfirmed) || pendingAddOps.contains(op)) {
            LOG.error("Trying to re-add duplicate entryid:{}", entryId);
            cb.addComplete(BKException.Code.DuplicateEntryIdException,
                    LedgerHandleAdv.this, entryId, ctx);
            return;
        }
        pendingAddOps.add(op);

        doAsyncAddEntry(op, data, offset, length, cb, ctx);
    }

    /**
     * Overriding part is mostly around setting entryId.
     * Though there may be some code duplication, Choose to have the override routine so the control flow is
     * unaltered in the base class.
     */
    @Override
    void doAsyncAddEntry(final PendingAddOp op, final byte[] data, final int offset, final int length,
            final AddCallback cb, final Object ctx) {
        if (offset < 0 || length < 0
                || (offset + length) > data.length) {
            throw new ArrayIndexOutOfBoundsException(
                "Invalid values for offset("+offset
                +") or length("+length+")");
        }
        throttler.acquire();

        if (metadata.isClosed()) {
            // make sure the callback is triggered in main worker pool
            try {
                bk.mainWorkerPool.submit(new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        LOG.warn("Attempt to add to closed ledger: {}", ledgerId);
                        cb.addComplete(BKException.Code.LedgerClosedException,
                                LedgerHandleAdv.this, op.getEntryId(), ctx);
                    }
                    @Override
                    public String toString() {
                        return String.format("AsyncAddEntryToClosedLedger(lid=%d)", ledgerId);
                    }
                });
            } catch (RejectedExecutionException e) {
                cb.addComplete(bk.getReturnRc(BKException.Code.InterruptedException),
                        LedgerHandleAdv.this, op.getEntryId(), ctx);
            }
            return;
        }

        try {
            final long currentLength = addToLength(length);

            bk.mainWorkerPool.submit(new SafeRunnable() {
                @Override
                public void safeRun() {
                    ChannelBuffer toSend = macManager.computeDigestAndPackageForSending(
                                               op.getEntryId(), lastAddConfirmed, currentLength, data, offset, length);
                    op.initiate(toSend, length);
                }
            });
        } catch (RejectedExecutionException e) {
            cb.addComplete(bk.getReturnRc(BKException.Code.InterruptedException),
                    LedgerHandleAdv.this, op.getEntryId(), ctx);
        }
    }

}
