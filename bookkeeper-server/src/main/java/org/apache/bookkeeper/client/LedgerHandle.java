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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Queue;
import java.util.concurrent.Semaphore;

import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.util.SafeRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Ledger handle contains ledger metadata and is used to access the read and
 * write operations to a ledger.
 */
public class LedgerHandle {
    final static Logger LOG = LoggerFactory.getLogger(LedgerHandle.class);

    final byte[] ledgerKey;
    LedgerMetadata metadata;
    final BookKeeper bk;
    final long ledgerId;
    long lastAddPushed;
    long lastAddConfirmed;
    long length;
    final DigestManager macManager;
    final DistributionSchedule distributionSchedule;

    final Semaphore opCounterSem;
    private final Integer throttling;

    final Queue<PendingAddOp> pendingAddOps = new ArrayDeque<PendingAddOp>();

    LedgerHandle(BookKeeper bk, long ledgerId, LedgerMetadata metadata,
                 DigestType digestType, byte[] password)
            throws GeneralSecurityException, NumberFormatException {
        this.bk = bk;
        this.metadata = metadata;

        if (metadata.isClosed()) {
            lastAddConfirmed = lastAddPushed = metadata.close;
            length = metadata.length;
        } else {
            lastAddConfirmed = lastAddPushed = -1;
            length = 0;
        }

        this.ledgerId = ledgerId;

        this.throttling = bk.getConf().getThrottleValue();
        this.opCounterSem = new Semaphore(throttling);

        macManager = DigestManager.instantiate(ledgerId, password, digestType);
        this.ledgerKey = MacDigestManager.genDigest("ledger", password);
        distributionSchedule = new RoundRobinDistributionSchedule(
            metadata.quorumSize, metadata.ensembleSize);
    }

    /**
     * Get the id of the current ledger
     *
     * @return the id of the ledger
     */
    public long getId() {
        return ledgerId;
    }

    /**
     * Get the last confirmed entry id on this ledger. It reads
     * the local state of the ledger handle, which is different
     * from the readLastConfirmed call. In the case the ledger
     * is not closed and the client is a reader, it is necessary
     * to call readLastConfirmed to obtain an estimate of the
     * last add operation that has been confirmed.  
     * 
     * @see #readLastConfirmed()
     *
     * @return the last confirmed entry id
     */
    public long getLastAddConfirmed() {
        return lastAddConfirmed;
    }

    /**
     * Get the entry id of the last entry that has been enqueued for addition (but
     * may not have possibly been persited to the ledger)
     *
     * @return the id of the last entry pushed
     */
    public long getLastAddPushed() {
        return lastAddPushed;
    }

    /**
     * Get the Ledger's key/password.
     *
     * @return byte array for the ledger's key/password.
     */
    public byte[] getLedgerKey() {
        return ledgerKey;
    }

    /**
     * Get the LedgerMetadata
     *
     * @return LedgerMetadata for the LedgerHandle
     */
    LedgerMetadata getLedgerMetadata() {
        return metadata;
    }

    /**
     * Get the DigestManager
     *
     * @return DigestManager for the LedgerHandle
     */
    DigestManager getDigestManager() {
        return macManager;
    }

    /**
     * Return total number of available slots.
     *
     * @return int    available slots
     */
    Semaphore getAvailablePermits() {
        return this.opCounterSem;
    }

    /**
     *  Add to the length of the ledger in bytes.
     *
     * @param delta
     * @return
     */
    long addToLength(long delta) {
        this.length += delta;
        return this.length;
    }

    /**
     * Returns the length of the ledger in bytes.
     *
     * @return the length of the ledger in bytes
     */
    public long getLength() {
        return this.length;
    }

    /**
     * Get the Distribution Schedule
     *
     * @return DistributionSchedule for the LedgerHandle
     */
    DistributionSchedule getDistributionSchedule() {
        return distributionSchedule;
    }

    void writeLedgerConfig(StatCallback callback, Object ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Writing metadata to ZooKeeper: " + this.ledgerId + ", " + metadata.getZnodeVersion());
        }

        bk.getZkHandle().setData(bk.getLedgerManager().getLedgerPath(ledgerId),
                                 metadata.serialize(), metadata.getZnodeVersion(),
                                 callback, ctx);
    }

    /**
     * Close this ledger synchronously.
     * @see #asyncClose
     */
    public void close() 
            throws InterruptedException, BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();

        asyncClose(new SyncCloseCallback(), counter);

        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }
    }

    /**
     * Asynchronous close, any adds in flight will return errors.
     * 
     * Closing a ledger will ensure that all clients agree on what the last entry 
     * of the ledger is. This ensures that, once the ledger has been closed, all 
     * reads from the ledger will return the same set of entries. 
     * 
     * @param cb
     *          callback implementation
     * @param ctx
     *          control object
     * @throws InterruptedException
     */
    public void asyncClose(CloseCallback cb, Object ctx) {
        asyncCloseInternal(cb, ctx, BKException.Code.LedgerClosedException);
    }

    /**
     * Same as public version of asyncClose except that this one takes an
     * additional parameter which is the return code to hand to all the pending
     * add ops
     *
     * @param cb
     * @param ctx
     * @param rc
     */
    void asyncCloseInternal(final CloseCallback cb, final Object ctx, final int rc) {
        bk.mainWorkerPool.submitOrdered(ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                synchronized(LedgerHandle.this) {
                    // synchronized on LedgerHandle.this to ensure that 
                    // lastAddPushed can not be updated after the metadata 
                    // is closed. 
                    metadata.length = length;

                    // Close operation is idempotent, so no need to check if we are
                    // already closed

                    metadata.close(lastAddConfirmed);
                    errorOutPendingAdds(rc);
                    lastAddPushed = lastAddConfirmed;
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Closing ledger: " + ledgerId + " at entryId: "
                              + metadata.close + " with this many bytes: " + metadata.length);
                }

                writeLedgerConfig(new StatCallback() {
                    @Override
                    public void processResult(int rc, String path, Object subctx,
                                              Stat stat) {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            LOG.warn("Conditional write failed: " + KeeperException.Code.get(rc));
                            cb.closeComplete(BKException.Code.ZKException, LedgerHandle.this,
                                             ctx);
                        } else {
                            metadata.updateZnodeStatus(stat);
                            cb.closeComplete(BKException.Code.OK, LedgerHandle.this, ctx);
                        }
                    }
                }, null);

            }
        });
    }

    /**
     * Read a sequence of entries synchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence (included)
     * @param lastEntry
     *          id of last entry of sequence (included)
     *
     */
    public Enumeration<LedgerEntry> readEntries(long firstEntry, long lastEntry)
            throws InterruptedException, BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();

        asyncReadEntries(firstEntry, lastEntry, new SyncReadCallback(), counter);

        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return counter.getSequence();
    }

    /**
     * Read a sequence of entries asynchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     * @param cb
     *          object implementing read callback interface
     * @param ctx
     *          control object
     */
    public void asyncReadEntries(long firstEntry, long lastEntry,
                                 ReadCallback cb, Object ctx) {
        // Little sanity check
        if (firstEntry < 0 || lastEntry > lastAddConfirmed
                || firstEntry > lastEntry) {
            cb.readComplete(BKException.Code.ReadException, this, null, ctx);
            return;
        }

        try {
            new PendingReadOp(this, firstEntry, lastEntry, cb, ctx).initiate();
        } catch (InterruptedException e) {
            cb.readComplete(BKException.Code.InterruptedException, this, null, ctx);
        }
    }

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data
     *         array of bytes to be written to the ledger
     */
    public void addEntry(byte[] data) throws InterruptedException, BKException {
        addEntry(data, 0, data.length);
    }

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data
     *         array of bytes to be written to the ledger
     * @param offset
     *          offset from which to take bytes from data
     * @param length
     *          number of bytes to take from data
     */
    public void addEntry(byte[] data, int offset, int length)
            throws InterruptedException, BKException {
        LOG.debug("Adding entry " + data);
        SyncCounter counter = new SyncCounter();
        counter.inc();

        asyncAddEntry(data, offset, length, new SyncAddCallback(), counter);
        counter.block(0);
        
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        if(counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data
     *          array of bytes to be written
     * @param cb
     *          object implementing callbackinterface
     * @param ctx
     *          some control object
     */
    public void asyncAddEntry(final byte[] data, final AddCallback cb,
                              final Object ctx) {
        asyncAddEntry(data, 0, data.length, cb, ctx);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     *
     * @param data
     *          array of bytes to be written
     * @param offset
     *          offset from which to take bytes from data
     * @param length
     *          number of bytes to take from data
     * @param cb
     *          object implementing callbackinterface
     * @param ctx
     *          some control object
     * @throws ArrayIndexOutOfBoundsException if offset or length is negative or
     *          offset and length sum to a value higher than the length of data.
     */
    public void asyncAddEntry(final byte[] data, final int offset, final int length,
                              final AddCallback cb, final Object ctx) {
        PendingAddOp op = new PendingAddOp(LedgerHandle.this, cb, ctx);
        doAsyncAddEntry(op, data, offset, length, cb, ctx);
    }

    /**
     * Make a recovery add entry request. Recovery adds can add to a ledger even if
     * it has been fenced.
     *
     * This is only valid for bookie and ledger recovery, which may need to replicate
     * entries to a quorum of bookies to ensure data safety.
     *
     * Normal client should never call this method.
     */
    void asyncRecoveryAddEntry(final byte[] data, final int offset, final int length,
                               final AddCallback cb, final Object ctx) {
        PendingAddOp op = new PendingAddOp(LedgerHandle.this, cb, ctx).enableRecoveryAdd();
        doAsyncAddEntry(op, data, offset, length, cb, ctx);
    }

    private void doAsyncAddEntry(final PendingAddOp op, final byte[] data, final int offset, final int length,
                                 final AddCallback cb, final Object ctx) {
        if (offset < 0 || length < 0
                || (offset + length) > data.length) {
            throw new ArrayIndexOutOfBoundsException(
                "Invalid values for offset("+offset
                +") or length("+length+")");
        }
        try {
            opCounterSem.acquire();
        } catch (InterruptedException e) {
            cb.addComplete(BKException.Code.InterruptedException,
                           LedgerHandle.this, -1, ctx);
        }

        final long entryId;
        final long currentLength;
        synchronized(this) {
            // synchronized on this to ensure that
            // the ledger isn't closed between checking and 
            // updating lastAddPushed
            if (metadata.isClosed()) {
                LOG.warn("Attempt to add to closed ledger: " + ledgerId);
                LedgerHandle.this.opCounterSem.release();
                cb.addComplete(BKException.Code.LedgerClosedException,
                               LedgerHandle.this, -1, ctx);
                return;
            }

            entryId = ++lastAddPushed;
            currentLength = addToLength(length);
            op.setEntryId(entryId);
            pendingAddOps.add(op);
        }

        try {
            bk.mainWorkerPool.submit(new SafeRunnable() {
                @Override
                public void safeRun() {
                    ChannelBuffer toSend = macManager.computeDigestAndPackageForSending(
                                               entryId, lastAddConfirmed, currentLength, data, offset, length);
                    op.initiate(toSend);
                }
            });
        } catch (RuntimeException e) {
            opCounterSem.release();
            throw e;
        }
    }

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies. This 
     * call obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may 
     * return a different value than getLastAddConfirmed, which returns the local value 
     * of the ledger handle.
     * 
     * @see #getLastAddConfirmed()
     *
     * @param cb
     * @param ctx
     */

    public void asyncReadLastConfirmed(ReadLastConfirmedCallback cb, Object ctx) {
        new ReadLastConfirmedOp(this, cb, ctx).initiate();
    }


    /**
     * Context objects for synchronous call to read last confirmed.
     */
    class LastConfirmedCtx {
        long response;
        int rc;

        LastConfirmedCtx() {
            this.response = -10;
        }

        void setLastConfirmed(long lastConfirmed) {
            this.response = lastConfirmed;
        }

        long getlastConfirmed() {
            return this.response;
        }

        void setRC(int rc) {
            this.rc = rc;
        }

        int getRC() {
            return this.rc;
        }

        boolean ready() {
            return (this.response != -10);
        }
    }

    /**
     * Obtains synchronously the last confirmed write from a quorum of bookies. This call
     * obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may 
     * return a different value than getLastAddConfirmed, which returns the local value 
     * of the ledger handle.
     * 
     * @see #getLastAddConfirmed()
     * 
     * @return
     * @throws InterruptedException
     * @throws BKException
     */
    
    public long readLastConfirmed()
            throws InterruptedException, BKException {
        LastConfirmedCtx ctx = new LastConfirmedCtx();
        asyncReadLastConfirmed(new SyncReadLastConfirmedCallback(), ctx);
        synchronized(ctx) {
            while(!ctx.ready()) {
                ctx.wait();
            }
        }

        if(ctx.getRC() != BKException.Code.OK) throw BKException.create(ctx.getRC());
        return ctx.getlastConfirmed();
    }

    // close the ledger and send fails to all the adds in the pipeline
    void handleUnrecoverableErrorDuringAdd(int rc) {
        if (metadata.isInRecovery()) {
            // we should not close ledger if ledger is recovery mode
            // otherwise we may lose entry.
            errorOutPendingAdds(rc);
            return;
        }
        asyncCloseInternal(NoopCloseCallback.instance, null, rc);
    }

    void errorOutPendingAdds(int rc) {
        PendingAddOp pendingAddOp;
        while ((pendingAddOp = pendingAddOps.poll()) != null) {
            pendingAddOp.submitCallback(rc);
        }
    }

    void sendAddSuccessCallbacks() {
        // Start from the head of the queue and proceed while there are
        // entries that have had all their responses come back
        PendingAddOp pendingAddOp;
        while ((pendingAddOp = pendingAddOps.peek()) != null) {
            if (pendingAddOp.numResponsesPending != 0) {
                return;
            }
            pendingAddOps.remove();
            lastAddConfirmed = pendingAddOp.entryId;
            pendingAddOp.submitCallback(BKException.Code.OK);
        }

    }

    void handleBookieFailure(InetSocketAddress addr, final int bookieIndex) {
        InetSocketAddress newBookie;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Handling failure of bookie: " + addr + " index: "
                      + bookieIndex);
        }

        try {
            newBookie = bk.bookieWatcher
                        .getAdditionalBookie(metadata.currentEnsemble);
        } catch (BKNotEnoughBookiesException e) {
            LOG
            .error("Could not get additional bookie to remake ensemble, closing ledger: "
                   + ledgerId);
            handleUnrecoverableErrorDuringAdd(e.getCode());
            return;
        }

        final ArrayList<InetSocketAddress> newEnsemble = new ArrayList<InetSocketAddress>(
            metadata.currentEnsemble);
        newEnsemble.set(bookieIndex, newBookie);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Changing ensemble from: " + metadata.currentEnsemble + " to: "
                      + newEnsemble + " for ledger: " + ledgerId + " starting at entry: "
                      + (lastAddConfirmed + 1));
        }

        metadata.addEnsemble(lastAddConfirmed + 1, newEnsemble);

        writeLedgerConfig(new StatCallback() {
            @Override
            public void processResult(final int rc, String path, Object ctx, final Stat stat) {

                bk.mainWorkerPool.submitOrdered(ledgerId, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            LOG
                            .error("Could not persist ledger metadata while changing ensemble to: "
                                   + newEnsemble + " , closing ledger");
                            handleUnrecoverableErrorDuringAdd(BKException.Code.ZKException);
                            return;
                        }

                        metadata.updateZnodeStatus(stat);
                        for (PendingAddOp pendingAddOp : pendingAddOps) {
                            pendingAddOp.unsetSuccessAndSendWriteRequest(bookieIndex);
                        }
                    }
                });

            }
        }, null);

    }

    void rereadMetadata(final GenericCallback<Void> cb) {
        bk.getZkHandle().getData(bk.getLedgerManager().getLedgerPath(ledgerId), false,
                new DataCallback() {
                    public void processResult(int rc, String path,
                                              Object ctx, byte[] data, Stat stat) {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            LOG.error("Error reading metadata from ledger, code =" + rc);
                            cb.operationComplete(BKException.Code.ZKException, null);
                            return;
                        }
                        
                        try {
                            metadata = LedgerMetadata.parseConfig(data, stat.getVersion());
                        } catch (IOException e) {
                            LOG.error("Error parsing ledger metadata for ledger", e);
                            cb.operationComplete(BKException.Code.ZKException, null);
                        }
                        cb.operationComplete(BKException.Code.OK, null);
                    }
                }, null);
    }

    void recover(final GenericCallback<Void> cb) {
        if (metadata.isClosed()) {
            lastAddConfirmed = lastAddPushed = metadata.close;
            length = metadata.length;

            // We are already closed, nothing to do
            cb.operationComplete(BKException.Code.OK, null);
            return;
        }

        metadata.markLedgerInRecovery();

        writeLedgerConfig(new StatCallback() {
            @Override
            public void processResult(final int rc, String path, Object ctx, Stat stat) {
                if (rc == KeeperException.Code.BadVersion) {
                    rereadMetadata(new GenericCallback<Void>() {
                            @Override
                            public void operationComplete(int rc, Void result) {
                                if (rc != BKException.Code.OK) {
                                    cb.operationComplete(rc, null);
                                } else {
                                    recover(cb);
                                }
                            }
                        });
                } else if (rc == KeeperException.Code.OK.intValue()) {
                    metadata.znodeVersion = stat.getVersion();
                    new LedgerRecoveryOp(LedgerHandle.this, cb).initiate();
                } else {
                    LOG.error("Error writing ledger config " +  rc 
                              + " path = " + path);
                    cb.operationComplete(BKException.Code.ZKException, null);
                }
            }
        }, null);
    }

    static class NoopCloseCallback implements CloseCallback {
        static NoopCloseCallback instance = new NoopCloseCallback();

        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            if (rc != KeeperException.Code.OK.intValue()) {
                LOG.warn("Close failed: " + BKException.getMessage(rc));
            }
            // noop
        }
    }
    
    private static class SyncReadCallback implements ReadCallback {
        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param leder
         *          ledger identifier
         * @param seq
         *          sequence of entries
         * @param ctx
         *          control object
         */
        public void readComplete(int rc, LedgerHandle lh,
                                 Enumeration<LedgerEntry> seq, Object ctx) {
            
            SyncCounter counter = (SyncCounter) ctx;
            synchronized (counter) {
                counter.setSequence(seq);
                counter.setrc(rc);
                counter.dec();
                counter.notify();
            }
        }
    }

    private static class SyncAddCallback implements AddCallback {
        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param leder
         *          ledger identifier
         * @param entry
         *          entry identifier
         * @param ctx
         *          control object
         */
        public void addComplete(int rc, LedgerHandle lh, long entry, Object ctx) {
            SyncCounter counter = (SyncCounter) ctx;
            
            counter.setrc(rc);
            counter.dec();
        }
    }

    private static class SyncReadLastConfirmedCallback implements ReadLastConfirmedCallback {
        /**
         * Implementation of  callback interface for synchronous read last confirmed method.
         */
        public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
            LastConfirmedCtx lcCtx = (LastConfirmedCtx) ctx;
            
            synchronized(lcCtx) {
                lcCtx.setRC(rc);
                lcCtx.setLastConfirmed(lastConfirmed);
                lcCtx.notify();
            }
        }
    }

    private static class SyncCloseCallback implements CloseCallback {
        /**
         * Close callback method
         *
         * @param rc
         * @param lh
         * @param ctx
         */
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            SyncCounter counter = (SyncCounter) ctx;
            counter.setrc(rc);
            synchronized (counter) {
                counter.dec();
                counter.notify();
            }
        }
    }
}
