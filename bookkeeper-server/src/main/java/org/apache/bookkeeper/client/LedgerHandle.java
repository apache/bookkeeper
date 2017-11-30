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

import static org.apache.bookkeeper.client.api.BKException.Code.ClientClosedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BKException.BKIncorrectParameterException;
import org.apache.bookkeeper.client.BKException.BKReadException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.SyncCallbackUtils.FutureReadLastConfirmed;
import org.apache.bookkeeper.client.SyncCallbackUtils.FutureReadLastConfirmedAndEntry;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncAddCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncCloseCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncReadCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncReadLastConfirmedCallback;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.TimedGenericCallback;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.State;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.util.OrderedSafeExecutor.OrderedSafeGenericCallback;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger handle contains ledger metadata and is used to access the read and
 * write operations to a ledger.
 */
public class LedgerHandle implements WriteHandle {
    final static Logger LOG = LoggerFactory.getLogger(LedgerHandle.class);

    final byte[] ledgerKey;
    LedgerMetadata metadata;
    final BookKeeper bk;
    final long ledgerId;
    long lastAddPushed;
    volatile long lastAddConfirmed;

    long length;
    final DigestManager macManager;
    final DistributionSchedule distributionSchedule;
    final RateLimiter throttler;
    final LoadingCache<BookieSocketAddress, Long> bookieFailureHistory;
    final boolean enableParallelRecoveryRead;
    final int recoveryReadBatchSize;

    /**
     * Invalid entry id. This value is returned from methods which
     * should return an entry id but there is no valid entry available.
     */
    final static public long INVALID_ENTRY_ID = BookieProtocol.INVALID_ENTRY_ID;

    final AtomicInteger blockAddCompletions = new AtomicInteger(0);
    final AtomicInteger numEnsembleChanges = new AtomicInteger(0);
    Queue<PendingAddOp> pendingAddOps;
    ExplicitLacFlushPolicy explicitLacFlushPolicy;

    final Counter ensembleChangeCounter;
    final Counter lacUpdateHitsCounter;
    final Counter lacUpdateMissesCounter;

    // This empty master key is used when an empty password is provided which is the hash of an empty string
    private final static byte[] emptyLedgerKey;
    static {
        try {
            emptyLedgerKey = MacDigestManager.genDigest("ledger", new byte[0]);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    LedgerHandle(BookKeeper bk, long ledgerId, LedgerMetadata metadata,
                 DigestType digestType, byte[] password)
            throws GeneralSecurityException, NumberFormatException {
        this.bk = bk;
        this.metadata = metadata;
        this.pendingAddOps = new ConcurrentLinkedQueue<PendingAddOp>();
        this.enableParallelRecoveryRead = bk.getConf().getEnableParallelRecoveryRead();
        this.recoveryReadBatchSize = bk.getConf().getRecoveryReadBatchSize();

        if (metadata.isClosed()) {
            lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
            length = metadata.getLength();
        } else {
            lastAddConfirmed = lastAddPushed = INVALID_ENTRY_ID;
            length = 0;
        }

        this.ledgerId = ledgerId;

        if (bk.getConf().getThrottleValue() > 0) {
            this.throttler = RateLimiter.create(bk.getConf().getThrottleValue());
        } else {
            this.throttler = null;
        }

        macManager = DigestManager.instantiate(ledgerId, password, digestType);

        // If the password is empty, pass the same random ledger key which is generated by the hash of the empty
        // password, so that the bookie can avoid processing the keys for each entry
        this.ledgerKey = password.length > 0 ? MacDigestManager.genDigest("ledger", password) : emptyLedgerKey;
        distributionSchedule = new RoundRobinDistributionSchedule(
                metadata.getWriteQuorumSize(), metadata.getAckQuorumSize(), metadata.getEnsembleSize());
        this.bookieFailureHistory = CacheBuilder.newBuilder()
            .expireAfterWrite(bk.getConf().getBookieFailureHistoryExpirationMSec(), TimeUnit.MILLISECONDS)
            .build(new CacheLoader<BookieSocketAddress, Long>() {
            public Long load(BookieSocketAddress key) {
                return -1L;
            }
        });

        ensembleChangeCounter = bk.getStatsLogger().getCounter(BookKeeperClientStats.ENSEMBLE_CHANGES);
        lacUpdateHitsCounter = bk.getStatsLogger().getCounter(BookKeeperClientStats.LAC_UPDATE_HITS);
        lacUpdateMissesCounter = bk.getStatsLogger().getCounter(BookKeeperClientStats.LAC_UPDATE_MISSES);
        bk.getStatsLogger().registerGauge(BookKeeperClientStats.PENDING_ADDS,
                                          new Gauge<Integer>() {
                                              public Integer getDefaultValue() {
                                                  return 0;
                                              }
                                              public Integer getSample() {
                                                  return pendingAddOps.size();
                                              }
                                          });
        initializeExplicitLacFlushPolicy();
    }

    protected void initializeExplicitLacFlushPolicy() {
        if (!metadata.isClosed() && bk.getExplicitLacInterval() > 0) {
            explicitLacFlushPolicy = new ExplicitLacFlushPolicy.ExplicitLacFlushPolicyImpl(this);
        } else {
            explicitLacFlushPolicy = ExplicitLacFlushPolicy.VOID_EXPLICITLAC_FLUSH_POLICY;
        }
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
     * {@inheritDoc}
     */
    @Override
    public synchronized long getLastAddConfirmed() {
        return lastAddConfirmed;
    }

    synchronized void setLastAddConfirmed(long lac) {
        this.lastAddConfirmed = lac;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long getLastAddPushed() {
        return lastAddPushed;
    }

    /**
     * Get the Ledger's key/password.
     *
     * @return byte array for the ledger's key/password.
     */
    public byte[] getLedgerKey() {
        return Arrays.copyOf(ledgerKey, ledgerKey.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LedgerMetadata getLedgerMetadata() {
        return metadata;
    }

    /**
     * Get this ledger's customMetadata map.
     *
     * @return map containing user provided customMetadata.
     */
    public Map<String, byte[]> getCustomMetadata() {
        return metadata.getCustomMetadata();
    }

    /**
     * Get the number of fragments that makeup this ledger
     *
     * @return the count of fragments
     */
    synchronized public long getNumFragments() {
        return metadata.getEnsembles().size();
    }

    /**
     * Get the count of unique bookies that own part of this ledger
     * by going over all the fragments of the ledger.
     *
     * @return count of unique bookies
     */
    synchronized public long getNumBookies() {
        Map<Long, ArrayList<BookieSocketAddress>> m = metadata.getEnsembles();
        Set<BookieSocketAddress> s = Sets.newHashSet();
        for (ArrayList<BookieSocketAddress> aList : m.values()) {
            s.addAll(aList);
        }
        return s.size();
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
     *  Add to the length of the ledger in bytes.
     *
     * @param delta
     * @return the length of the ledger after the addition
     */
    synchronized long addToLength(long delta) {
        this.length += delta;
        return this.length;
    }

    /**
     * Returns the length of the ledger in bytes.
     *
     * @return the length of the ledger in bytes
     */
    synchronized public long getLength() {
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

    void writeLedgerConfig(GenericCallback<Void> writeCb) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Writing metadata to ledger manager: {}, {}", this.ledgerId, metadata.getVersion());
        }

        bk.getLedgerManager().writeLedgerMetadata(ledgerId, metadata, writeCb);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void close()
            throws InterruptedException, BKException {
        SyncCallbackUtils.waitForResult(asyncClose());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public CompletableFuture<Void> asyncClose() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        SyncCloseCallback callback = new SyncCloseCallback(result);
        asyncClose(callback, null);
        explicitLacFlushPolicy.stopExplicitLacFlush();
        return result;
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
     */
    public void asyncClose(CloseCallback cb, Object ctx) {
        asyncCloseInternal(cb, ctx, BKException.Code.LedgerClosedException);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean isClosed() {
        return metadata.isClosed();
    }

    void asyncCloseInternal(final CloseCallback cb, final Object ctx, final int rc) {
        try {
            doAsyncCloseInternal(cb, ctx, rc);
        } catch (RejectedExecutionException re) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to close ledger {} : ", ledgerId, re);
            }
            errorOutPendingAdds(bk.getReturnRc(rc));
            cb.closeComplete(bk.getReturnRc(BKException.Code.InterruptedException), this, ctx);
        }
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
    void doAsyncCloseInternal(final CloseCallback cb, final Object ctx, final int rc) {
        bk.getMainWorkerPool().submitOrdered(ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                final long prevLastEntryId;
                final long prevLength;
                final State prevState;
                List<PendingAddOp> pendingAdds;

                if (isClosed()) {
                    // TODO: make ledger metadata immutable {@link https://github.com/apache/bookkeeper/issues/281}
                    // Although the metadata is already closed, we don't need to proceed zookeeper metadata update, but
                    // we still need to error out the pending add ops.
                    //
                    // There is a race condition a pending add op is enqueued, after a close op reset ledger metadata state
                    // to unclosed to resolve metadata conflicts. If we don't error out these pending add ops, they would be
                    // leak and never callback.
                    //
                    // The race condition happen in following sequence:
                    // a) ledger L is fenced
                    // b) write entry E encountered LedgerFencedException, trigger ledger close procedure
                    // c) ledger close encountered metadata version exception and set ledger metadata back to open
                    // d) writer tries to write entry E+1, since ledger metadata is still open (reset by c))
                    // e) the close procedure in c) resolved the metadata conflicts and set ledger metadata to closed
                    // f) writing entry E+1 encountered LedgerFencedException which will enter ledger close procedure
                    // g) it would find that ledger metadata is closed, then it callbacks immediately without erroring out any pendings
                    synchronized (LedgerHandle.this) {
                        pendingAdds = drainPendingAddsToErrorOut();
                    }
                    errorOutPendingAdds(rc, pendingAdds);
                    cb.closeComplete(BKException.Code.OK, LedgerHandle.this, ctx);
                    return;
                }

                synchronized(LedgerHandle.this) {
                    prevState = metadata.getState();
                    prevLastEntryId = metadata.getLastEntryId();
                    prevLength = metadata.getLength();

                    // drain pending adds first
                    pendingAdds = drainPendingAddsToErrorOut();

                    // synchronized on LedgerHandle.this to ensure that
                    // lastAddPushed can not be updated after the metadata
                    // is closed.
                    metadata.setLength(length);
                    metadata.close(lastAddConfirmed);
                    lastAddPushed = lastAddConfirmed;
                }

                // error out all pending adds during closing, the callbacks shouldn't be
                // running under any bk locks.
                errorOutPendingAdds(rc, pendingAdds);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Closing ledger: " + ledgerId + " at entryId: "
                              + metadata.getLastEntryId() + " with this many bytes: " + metadata.getLength());
                }

                final class CloseCb extends OrderedSafeGenericCallback<Void> {
                    CloseCb() {
                        super(bk.getMainWorkerPool(), ledgerId);
                    }

                    @Override
                    public void safeOperationComplete(final int rc, Void result) {
                        if (rc == BKException.Code.MetadataVersionException) {
                            rereadMetadata(new OrderedSafeGenericCallback<LedgerMetadata>(bk.getMainWorkerPool(),
                                                                                          ledgerId) {
                                @Override
                                public void safeOperationComplete(int newrc, LedgerMetadata newMeta) {
                                    if (newrc != BKException.Code.OK) {
                                        LOG.error("Error reading new metadata from ledger {} when closing, code={}",
                                                ledgerId, newrc);
                                        cb.closeComplete(rc, LedgerHandle.this, ctx);
                                    } else {
                                        metadata.setState(prevState);
                                        if (prevState.equals(State.CLOSED)) {
                                            metadata.close(prevLastEntryId);
                                        }

                                        metadata.setLength(prevLength);
                                        if (!metadata.isNewerThan(newMeta)
                                                && !metadata.isConflictWith(newMeta)) {
                                            // use the new metadata's ensemble, in case re-replication already
                                            // replaced some bookies in the ensemble.
                                            metadata.setEnsembles(newMeta.getEnsembles());
                                            metadata.setVersion(newMeta.version);
                                            metadata.setLength(length);
                                            metadata.close(getLastAddConfirmed());
                                            writeLedgerConfig(new CloseCb());
                                            return;
                                        } else {
                                            metadata.setLength(length);
                                            metadata.close(getLastAddConfirmed());
                                            LOG.warn("Conditional update ledger metadata for ledger {} failed.", ledgerId);
                                            cb.closeComplete(rc, LedgerHandle.this, ctx);
                                        }
                                    }
                                }

                                @Override
                                public String toString() {
                                    return String.format("ReReadMetadataForClose(%d)", ledgerId);
                                }
                            });
                        } else if (rc != BKException.Code.OK) {
                            LOG.error("Error update ledger metadata for ledger {} : {}", ledgerId, rc);
                            cb.closeComplete(rc, LedgerHandle.this, ctx);
                        } else {
                            cb.closeComplete(BKException.Code.OK, LedgerHandle.this, ctx);
                        }
                    }

                    @Override
                    public String toString() {
                        return String.format("WriteLedgerConfigForClose(%d)", ledgerId);
                    }
                }

                writeLedgerConfig(new CloseCb());

            }

            @Override
            public String toString() {
                return String.format("CloseLedgerHandle(%d)", ledgerId);
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
     * @see #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object)
     */
    public Enumeration<LedgerEntry> readEntries(long firstEntry, long lastEntry)
            throws InterruptedException, BKException {
        CompletableFuture<Enumeration<LedgerEntry>> result = new CompletableFuture<>();

        asyncReadEntries(firstEntry, lastEntry, new SyncReadCallback(result), null);

        return SyncCallbackUtils.waitForResult(result);
    }

    /**
     * Read a sequence of entries synchronously, allowing to read after the LastAddConfirmed range.<br>
     * This is the same of
     * {@link #asyncReadUnconfirmedEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object) }
     *
     * @param firstEntry
     *          id of first entry of sequence (included)
     * @param lastEntry
     *          id of last entry of sequence (included)
     *
     * @see #readEntries(long, long)
     * @see #asyncReadUnconfirmedEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object)
     * @see #asyncReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, java.lang.Object)
     */
    public Enumeration<LedgerEntry> readUnconfirmedEntries(long firstEntry, long lastEntry)
            throws InterruptedException, BKException {
        CompletableFuture<Enumeration<LedgerEntry>> result = new CompletableFuture<>();

        asyncReadUnconfirmedEntries(firstEntry, lastEntry, new SyncReadCallback(result), null);

        return SyncCallbackUtils.waitForResult(result);
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
    public void asyncReadEntries(long firstEntry, long lastEntry, ReadCallback cb, Object ctx) {
        // Little sanity check
        if (firstEntry < 0 || firstEntry > lastEntry) {
            LOG.error("IncorrectParameterException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    new Object[] { ledgerId, firstEntry, lastEntry });
            cb.readComplete(BKException.Code.IncorrectParameterException, this, null, ctx);
            return;
        }

        if (lastEntry > lastAddConfirmed) {
            LOG.error("ReadException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    new Object[] { ledgerId, firstEntry, lastEntry });
            cb.readComplete(BKException.Code.ReadException, this, null, ctx);
            return;
        }

        asyncReadEntriesInternal(firstEntry, lastEntry, cb, ctx);
    }

    /**
     * Read a sequence of entries asynchronously, allowing to read after the LastAddConfirmed range.
     * <br>This is the same of
     * {@link #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object) }
     * but it lets the client read without checking the local value of LastAddConfirmed, so that it is possibile to
     * read entries for which the writer has not received the acknowledge yet. <br>
     * For entries which are within the range 0..LastAddConfirmed BookKeeper guarantees that the writer has successfully
     * received the acknowledge.<br>
     * For entries outside that range it is possible that the writer never received the acknowledge
     * and so there is the risk that the reader is seeing entries before the writer and this could result in a consistency
     * issue in some cases.<br>
     * With this method you can even read entries before the LastAddConfirmed and entries after it with one call,
     * the expected consistency will be as described above for each subrange of ids.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     * @param cb
     *          object implementing read callback interface
     * @param ctx
     *          control object
     *
     * @see #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object)
     * @see #asyncReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, java.lang.Object)
     * @see #readUnconfirmedEntries(long, long)
     */
    public void asyncReadUnconfirmedEntries(long firstEntry, long lastEntry, ReadCallback cb, Object ctx) {
        // Little sanity check
        if (firstEntry < 0 || firstEntry > lastEntry) {
            LOG.error("IncorrectParameterException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    new Object[] { ledgerId, firstEntry, lastEntry });
            cb.readComplete(BKException.Code.IncorrectParameterException, this, null, ctx);
            return;
        }

        asyncReadEntriesInternal(firstEntry, lastEntry, cb, ctx);
    }

    /**
     * Read a sequence of entries asynchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     */
    @Override
    public CompletableFuture<LedgerEntries> read(long firstEntry, long lastEntry) {
        // Little sanity check
        if (firstEntry < 0 || firstEntry > lastEntry) {
            LOG.error("IncorrectParameterException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    new Object[] { ledgerId, firstEntry, lastEntry });
            return FutureUtils.exception(new BKIncorrectParameterException());
        }

        if (lastEntry > lastAddConfirmed) {
            LOG.error("ReadException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    new Object[] { ledgerId, firstEntry, lastEntry });
            return FutureUtils.exception(new BKReadException());
        }

        return readEntriesInternalAsync(firstEntry, lastEntry);
    }

    /**
     * Read a sequence of entries asynchronously, allowing to read after the LastAddConfirmed range.
     * <br>This is the same of
     * {@link #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object) }
     * but it lets the client read without checking the local value of LastAddConfirmed, so that it is possibile to
     * read entries for which the writer has not received the acknowledge yet. <br>
     * For entries which are within the range 0..LastAddConfirmed BookKeeper guarantees that the writer has successfully
     * received the acknowledge.<br>
     * For entries outside that range it is possible that the writer never received the acknowledge
     * and so there is the risk that the reader is seeing entries before the writer and this could result in a consistency
     * issue in some cases.<br>
     * With this method you can even read entries before the LastAddConfirmed and entries after it with one call,
     * the expected consistency will be as described above for each subrange of ids.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     *
     * @see #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object)
     * @see #asyncReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, java.lang.Object)
     * @see #readUnconfirmedEntries(long, long)
     */
    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmed(long firstEntry, long lastEntry) {
        // Little sanity check
        if (firstEntry < 0 || firstEntry > lastEntry) {
            LOG.error("IncorrectParameterException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    new Object[] { ledgerId, firstEntry, lastEntry });
            return FutureUtils.exception(new BKIncorrectParameterException());
        }

        return readEntriesInternalAsync(firstEntry, lastEntry);
    }

    void asyncReadEntriesInternal(long firstEntry, long lastEntry, ReadCallback cb, Object ctx) {
        if(!bk.isClosed()) {
            readEntriesInternalAsync(firstEntry, lastEntry)
                .whenCompleteAsync(new FutureEventListener<LedgerEntries>() {
                    @Override
                    public void onSuccess(LedgerEntries entries) {
                        cb.readComplete(
                            Code.OK,
                            LedgerHandle.this,
                            IteratorUtils.asEnumeration(
                                Iterators.transform(entries.iterator(), le -> {
                                    LedgerEntry entry = new LedgerEntry((LedgerEntryImpl) le);
                                    le.close();
                                    return entry;
                                })),
                            ctx);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        if (cause instanceof BKException) {
                            BKException bke = (BKException) cause;
                            cb.readComplete(bke.getCode(), LedgerHandle.this, null, ctx);
                        } else {
                            cb.readComplete(Code.UnexpectedConditionException, LedgerHandle.this, null, ctx);
                        }
                    }
                }, bk.getMainWorkerPool().chooseThread(ledgerId));
        } else {
            cb.readComplete(Code.ClientClosedException, LedgerHandle.this, null, ctx);
        }
    }

    CompletableFuture<LedgerEntries> readEntriesInternalAsync(long firstEntry,
                                                              long lastEntry) {
        PendingReadOp op = new PendingReadOp(this, bk.getScheduler(), firstEntry, lastEntry);
        if(!bk.isClosed()) {
            bk.getMainWorkerPool().submitOrdered(ledgerId, op);
        } else {
            op.future().completeExceptionally(BKException.create(ClientClosedException));
        }
        return op.future();
    }

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data
     *         array of bytes to be written to the ledger
     * @return the entryId of the new inserted entry
     */
    public long addEntry(byte[] data) throws InterruptedException, BKException {
        return addEntry(data, 0, data.length);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public CompletableFuture<Long> append(ByteBuf data) {
        SyncAddCallback callback = new SyncAddCallback();
        asyncAddEntry(data, callback, null);
        return callback;
    }

    /**
     * Add entry synchronously to an open ledger. This can be used only with
     * {@link LedgerHandleAdv} returned through ledgers created with {@link
     * BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     *
     * @param entryId
     *            entryId to be added
     * @param data
     *            array of bytes to be written to the ledger
     * @return the entryId of the new inserted entry
     */
    public long addEntry(final long entryId, byte[] data) throws InterruptedException, BKException {
        LOG.error("To use this feature Ledger must be created with createLedgerAdv interface.");
        throw BKException.create(BKException.Code.IllegalOpException);
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
     * @return the entryId of the new inserted entry
     */
    public long addEntry(byte[] data, int offset, int length)
            throws InterruptedException, BKException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding entry {}", data);
        }

        SyncAddCallback callback = new SyncAddCallback();
        asyncAddEntry(data, offset, length, callback, null);

        return SyncCallbackUtils.waitForResult(callback);
    }

    /**
     * Add entry synchronously to an open ledger. This can be used only with
     * {@link LedgerHandleAdv} returned through ledgers created with {@link
     * BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId
     *            entryId to be added.
     * @param data
     *            array of bytes to be written to the ledger
     * @param offset
     *            offset from which to take bytes from data
     * @param length
     *            number of bytes to take from data
     * @return entryId
     */
    public long addEntry(final long entryId, byte[] data, int offset, int length) throws InterruptedException,
            BKException {
        LOG.error("To use this feature Ledger must be created with createLedgerAdv() interface.");
        throw BKException.create(BKException.Code.IllegalOpException);
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
     * Add entry asynchronously to an open ledger. This can be used only with
     * {@link LedgerHandleAdv} returned through ledgers created with {@link
     * BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId
     *            entryId to be added
     * @param data
     *            array of bytes to be written
     * @param cb
     *            object implementing callbackinterface
     * @param ctx
     *            some control object
     */
    public void asyncAddEntry(final long entryId, final byte[] data, final AddCallback cb, final Object ctx) {
        LOG.error("To use this feature Ledger must be created with createLedgerAdv() interface.");
        cb.addComplete(BKException.Code.IllegalOpException, LedgerHandle.this, entryId, ctx);
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
        if (offset < 0 || length < 0
                || (offset + length) > data.length) {
            throw new ArrayIndexOutOfBoundsException(
                    "Invalid values for offset("+offset
                    +") or length("+length+")");
        }

        asyncAddEntry(Unpooled.wrappedBuffer(data, offset, length), cb, ctx);
    }

    public void asyncAddEntry(ByteBuf data, final AddCallback cb, final Object ctx) {
        data.retain();
        PendingAddOp op = PendingAddOp.create(this, data, cb, ctx);
        doAsyncAddEntry(op);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     * This can be used only with {@link LedgerHandleAdv} returned through
     * ledgers created with {@link BookKeeper#createLedgerAdv(int, int, int, DigestType, byte[])}.
     *
     * @param entryId
     *            entryId of the entry to add.
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
            final AddCallback cb, final Object ctx) throws BKException {
        LOG.error("To use this feature Ledger must be created with createLedgerAdv() interface.");
        cb.addComplete(BKException.Code.IllegalOpException, LedgerHandle.this, entryId, ctx);
    }

    /**
     * Make a recovery add entry request. Recovery adds can add to a ledger even
     * if it has been fenced.
     *
     * This is only valid for bookie and ledger recovery, which may need to replicate
     * entries to a quorum of bookies to ensure data safety.
     *
     * Normal client should never call this method.
     */
    void asyncRecoveryAddEntry(final byte[] data, final int offset, final int length,
                               final AddCallback cb, final Object ctx) {
        PendingAddOp op = PendingAddOp.create(this, Unpooled.wrappedBuffer(data, offset, length), cb, ctx)
                .enableRecoveryAdd();
        doAsyncAddEntry(op);
    }

    protected void doAsyncAddEntry(final PendingAddOp op) {
        if (throttler != null) {
            throttler.acquire();
        }

        boolean wasClosed = false;
        synchronized(this) {
            // synchronized on this to ensure that
            // the ledger isn't closed between checking and
            // updating lastAddPushed
            if (metadata.isClosed()) {
                wasClosed = true;
            } else {
                long entryId = ++lastAddPushed;
                long currentLedgerLength = addToLength(op.payload.readableBytes());
                op.setEntryId(entryId);
                op.setLedgerLength(currentLedgerLength);
                pendingAddOps.add(op);
            }
        }

        if (wasClosed) {
            // make sure the callback is triggered in main worker pool
            try {
                bk.getMainWorkerPool().submit(new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        LOG.warn("Attempt to add to closed ledger: {}", ledgerId);
                        op.cb.addComplete(BKException.Code.LedgerClosedException,
                                LedgerHandle.this, INVALID_ENTRY_ID, op.ctx);
                    }

                    @Override
                    public String toString() {
                        return String.format("AsyncAddEntryToClosedLedger(lid=%d)", ledgerId);
                    }
                });
            } catch (RejectedExecutionException e) {
                op.cb.addComplete(bk.getReturnRc(BKException.Code.InterruptedException),
                        LedgerHandle.this, INVALID_ENTRY_ID, op.ctx);
            }
            return;
        }

        try {
            bk.getMainWorkerPool().submitOrdered(ledgerId, op);
        } catch (RejectedExecutionException e) {
            op.cb.addComplete(bk.getReturnRc(BKException.Code.InterruptedException),
                    LedgerHandle.this, INVALID_ENTRY_ID, op.ctx);
        }
    }

    synchronized void updateLastConfirmed(long lac, long len) {
        if (lac > lastAddConfirmed) {
            lastAddConfirmed = lac;
            lacUpdateHitsCounter.inc();
        } else {
            lacUpdateMissesCounter.inc();
        }
        lastAddPushed = Math.max(lastAddPushed, lac);
        length = Math.max(length, len);
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

    public void asyncReadLastConfirmed(final ReadLastConfirmedCallback cb, final Object ctx) {
        boolean isClosed;
        long lastEntryId;
        synchronized (this) {
            isClosed = metadata.isClosed();
            lastEntryId = metadata.getLastEntryId();
        }
        if (isClosed) {
            cb.readLastConfirmedComplete(BKException.Code.OK, lastEntryId, ctx);
            return;
        }
        ReadLastConfirmedOp.LastConfirmedDataCallback innercb = new ReadLastConfirmedOp.LastConfirmedDataCallback() {
                @Override
                public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                    if (rc == BKException.Code.OK) {
                        updateLastConfirmed(data.lastAddConfirmed, data.length);
                        cb.readLastConfirmedComplete(rc, data.lastAddConfirmed, ctx);
                    } else {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            };
        new ReadLastConfirmedOp(this, innercb).initiate();
    }

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies.
     * It is similar as
     * {@link #asyncReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, Object)},
     * but it doesn't wait all the responses from the quorum. It would callback
     * immediately if it received a LAC which is larger than current LAC.
     *
     * @see #asyncTryReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, Object)
     *
     * @param cb
     *          callback to return read last confirmed
     * @param ctx
     *          callback context
     */
    public void asyncTryReadLastConfirmed(final ReadLastConfirmedCallback cb, final Object ctx) {
        boolean isClosed;
        long lastEntryId;
        synchronized (this) {
            isClosed = metadata.isClosed();
            lastEntryId = metadata.getLastEntryId();
        }
        if (isClosed) {
            cb.readLastConfirmedComplete(BKException.Code.OK, lastEntryId, ctx);
            return;
        }
        ReadLastConfirmedOp.LastConfirmedDataCallback innercb = new ReadLastConfirmedOp.LastConfirmedDataCallback() {
            AtomicBoolean completed = new AtomicBoolean(false);

            @Override
            public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                if (rc == BKException.Code.OK) {
                    updateLastConfirmed(data.lastAddConfirmed, data.length);
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, data.lastAddConfirmed, ctx);
                    }
                } else {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            }
        };
        new TryReadLastConfirmedOp(this, innercb, getLastAddConfirmed()).initiate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmed() {
        FutureReadLastConfirmed result = new FutureReadLastConfirmed();
        asyncTryReadLastConfirmed(result, null);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Long> readLastAddConfirmed() {
        FutureReadLastConfirmed result = new FutureReadLastConfirmed();
        asyncReadLastConfirmed(result, null);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntry(long entryId,
                                                                                 long timeOutInMillis,
                                                                                 boolean parallel) {
        FutureReadLastConfirmedAndEntry result = new FutureReadLastConfirmedAndEntry();
        asyncReadLastConfirmedAndEntry(entryId, timeOutInMillis, parallel, result, null);
        return result;
    }

    /**
     * Asynchronous read next entry and the latest last add confirmed.
     * If the next entryId is less than known last add confirmed, the call will read next entry directly.
     * If the next entryId is ahead of known last add confirmed, the call will issue a long poll read
     * to wait for the next entry <i>entryId</i>.
     *
     * The callback will return the latest last add confirmed and next entry if it is available within timeout period <i>timeOutInMillis</i>.
     *
     * @param entryId
     *          next entry id to read
     * @param timeOutInMillis
     *          timeout period to wait for the entry id to be available (for long poll only)
     * @param parallel
     *          whether to issue the long poll reads in parallel
     * @param cb
     *          callback to return the result
     * @param ctx
     *          callback context
     */
    public void asyncReadLastConfirmedAndEntry(final long entryId,
                                               final long timeOutInMillis,
                                               final boolean parallel,
                                               final AsyncCallback.ReadLastConfirmedAndEntryCallback cb,
                                               final Object ctx) {
        boolean isClosed;
        long lac;
        synchronized (this) {
            isClosed = metadata.isClosed();
            lac = metadata.getLastEntryId();
        }
        if (isClosed) {
            if (entryId > lac) {
                cb.readLastConfirmedAndEntryComplete(BKException.Code.OK, lac, null, ctx);
                return;
            }
        } else {
            lac = getLastAddConfirmed();
        }
        if (entryId <= lac) {
            asyncReadEntries(entryId, entryId, new ReadCallback() {
                @Override
                public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                    if (BKException.Code.OK == rc) {
                        if (seq.hasMoreElements()) {
                            cb.readLastConfirmedAndEntryComplete(rc, getLastAddConfirmed(), seq.nextElement(), ctx);
                        } else {
                            cb.readLastConfirmedAndEntryComplete(rc, getLastAddConfirmed(), null, ctx);
                        }
                    } else {
                        cb.readLastConfirmedAndEntryComplete(rc, INVALID_ENTRY_ID, null, ctx);
                    }
                }
            }, ctx);
            return;
        }
        // wait for entry <i>entryId</i>
        ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback innercb =
            new ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback() {
            AtomicBoolean completed = new AtomicBoolean(false);
            @Override
            public void readLastConfirmedAndEntryComplete(int rc, long lastAddConfirmed, LedgerEntry entry) {
                if (rc == BKException.Code.OK) {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedAndEntryComplete(rc, lastAddConfirmed, entry, ctx);
                    }
                } else {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedAndEntryComplete(rc, INVALID_ENTRY_ID, null, ctx);
                    }
                }
            }
        };
        new ReadLastConfirmedAndEntryOp(this,
            innercb,
            entryId - 1,
            timeOutInMillis,
            bk.getScheduler())
            .parallelRead(parallel)
            .initiate();
    }

    /**
     * Context objects for synchronous call to read last confirmed.
     */
    static class LastConfirmedCtx {
        final static long ENTRY_ID_PENDING = -10;
        long response;
        int rc;

        LastConfirmedCtx() {
            this.response = ENTRY_ID_PENDING;
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
            return (this.response != ENTRY_ID_PENDING);
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
     * @return The entry id of the last confirmed write or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}
     *         if no entry has been confirmed
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

    /**
     * Obtains synchronously the last confirmed write from a quorum of bookies.
     * It is similar as {@link #readLastConfirmed()}, but it doesn't wait all the responses
     * from the quorum. It would callback immediately if it received a LAC which is larger
     * than current LAC.
     *
     * @see #readLastConfirmed()
     *
     * @return The entry id of the last confirmed write or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}
     *         if no entry has been confirmed
     * @throws InterruptedException
     * @throws BKException
     */
    public long tryReadLastConfirmed() throws InterruptedException, BKException {
        LastConfirmedCtx ctx = new LastConfirmedCtx();
        asyncTryReadLastConfirmed(new SyncReadLastConfirmedCallback(), ctx);
        synchronized (ctx) {
            while (!ctx.ready()) {
                ctx.wait();
            }
        }
        if (ctx.getRC() != BKException.Code.OK) throw BKException.create(ctx.getRC());
        return ctx.getlastConfirmed();
    }

    /**
     * Obtains asynchronously the explicit last add confirmed from a quorum of
     * bookies. This call obtains the the explicit last add confirmed each
     * bookie has received for this ledger and returns the maximum. If in the
     * write LedgerHandle, explicitLAC feature is not enabled then this will
     * return {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}. If the read explicit
     * lastaddconfirmed is greater than getLastAddConfirmed, then it updates the
     * lastAddConfirmed of this ledgerhandle. If the ledger has been closed, it
     * returns the value of the last add confirmed from the metadata.
     *
     * @see #getLastAddConfirmed()
     *
     * @param cb
     *          callback to return read explicit last confirmed
     * @param ctx
     *          callback context
     */
    public void asyncReadExplicitLastConfirmed(final ReadLastConfirmedCallback cb, final Object ctx) {
        boolean isClosed;
        synchronized (this) {
            isClosed = metadata.isClosed();
            if (isClosed) {
                lastAddConfirmed = metadata.getLastEntryId();
                length = metadata.getLength();
            }
        }
        if (isClosed) {
            cb.readLastConfirmedComplete(BKException.Code.OK, lastAddConfirmed, ctx);
            return;
        }

        PendingReadLacOp.LacCallback innercb = new PendingReadLacOp.LacCallback() {

            @Override
            public void getLacComplete(int rc, long lac) {
                if (rc == BKException.Code.OK) {
                    // here we are trying to update lac only but not length
                    updateLastConfirmed(lac, 0);
                    cb.readLastConfirmedComplete(rc, lac, ctx);
                } else {
                    cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                }
            }
        };
        new PendingReadLacOp(this, innercb).initiate();
    }

    /**
     * Obtains synchronously the explicit last add confirmed from a quorum of
     * bookies. This call obtains the the explicit last add confirmed each
     * bookie has received for this ledger and returns the maximum. If in the
     * write LedgerHandle, explicitLAC feature is not enabled then this will
     * return {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}. If the read explicit
     * lastaddconfirmed is greater than getLastAddConfirmed, then it updates the
     * lastAddConfirmed of this ledgerhandle. If the ledger has been closed, it
     * returns the value of the last add confirmed from the metadata.
     *
     * @see #getLastAddConfirmed()
     *
     * @return The entry id of the explicit last confirmed write or
     *         {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID} if no entry has been
     *         confirmed or if explicitLAC feature is not enabled in write
     *         LedgerHandle.
     * @throws InterruptedException
     * @throws BKException
     */
    public long readExplicitLastConfirmed() throws InterruptedException, BKException {
        LastConfirmedCtx ctx = new LastConfirmedCtx();
        asyncReadExplicitLastConfirmed(new SyncReadLastConfirmedCallback(), ctx);
        synchronized (ctx) {
            while (!ctx.ready()) {
                ctx.wait();
            }
        }
        if (ctx.getRC() != BKException.Code.OK) {
            throw BKException.create(ctx.getRC());
        }
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
        LOG.error("Closing ledger {} due to error {}", ledgerId, rc);
        asyncCloseInternal(NoopCloseCallback.instance, null, rc);
    }

    void errorOutPendingAdds(int rc) {
        errorOutPendingAdds(rc, drainPendingAddsToErrorOut());
    }

    synchronized List<PendingAddOp> drainPendingAddsToErrorOut() {
        PendingAddOp pendingAddOp;
        List<PendingAddOp> opsDrained = new ArrayList<PendingAddOp>(pendingAddOps.size());
        while ((pendingAddOp = pendingAddOps.poll()) != null) {
            addToLength(-pendingAddOp.entryLength);
            opsDrained.add(pendingAddOp);
        }
        return opsDrained;
    }

    void errorOutPendingAdds(int rc, List<PendingAddOp> ops) {
        for (PendingAddOp op : ops) {
            op.submitCallback(rc);
        }
    }

    void sendAddSuccessCallbacks() {
        // Start from the head of the queue and proceed while there are
        // entries that have had all their responses come back
        PendingAddOp pendingAddOp;

        while ((pendingAddOp = pendingAddOps.peek()) != null
               && blockAddCompletions.get() == 0) {
            if (!pendingAddOp.completed) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("pending add not completed: {}", pendingAddOp);
                }
                return;
            }
            // Check if it is the next entry in the sequence.
            if (pendingAddOp.entryId != 0 && pendingAddOp.entryId != lastAddConfirmed + 1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Head of the queue entryId: {} is not lac: {} + 1", pendingAddOp.entryId,
                            lastAddConfirmed);
                }
                return;
            }

            pendingAddOps.remove();
            explicitLacFlushPolicy.updatePiggyBackedLac(lastAddConfirmed);
            lastAddConfirmed = pendingAddOp.entryId;

            pendingAddOp.submitCallback(BKException.Code.OK);
        }

    }

    EnsembleInfo replaceBookieInMetadata(final Map<Integer, BookieSocketAddress> failedBookies,
                                         int ensembleChangeIdx)
            throws BKException.BKNotEnoughBookiesException {
        final ArrayList<BookieSocketAddress> newEnsemble = new ArrayList<BookieSocketAddress>();
        final long newEnsembleStartEntry = getLastAddConfirmed() + 1;
        final HashSet<Integer> replacedBookies = new HashSet<Integer>();
        synchronized (metadata) {
            newEnsemble.addAll(metadata.currentEnsemble);
            for (Map.Entry<Integer, BookieSocketAddress> entry : failedBookies.entrySet()) {
                int idx = entry.getKey();
                BookieSocketAddress addr = entry.getValue();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[EnsembleChange-L{}-{}] : replacing bookie: {} index: {}",
                        new Object[]{getId(), ensembleChangeIdx, addr, idx});
                }
                if (!newEnsemble.get(idx).equals(addr)) {
                    // ensemble has already changed, failure of this addr is immaterial
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Write did not succeed to {}, bookieIndex {}, but we have already fixed it.",
                                  addr, idx);
                    }
                    continue;
                }
                try {
                    BookieSocketAddress newBookie = bk.bookieWatcher.replaceBookie(
                        metadata.getEnsembleSize(),
                        metadata.getWriteQuorumSize(),
                        metadata.getAckQuorumSize(),
                        metadata.getCustomMetadata(),
                        newEnsemble,
                        idx,
                        new HashSet<BookieSocketAddress>(failedBookies.values()));
                    newEnsemble.set(idx, newBookie);
                    replacedBookies.add(idx);
                } catch (BKException.BKNotEnoughBookiesException e) {
                    // if there is no bookie replaced, we throw not enough bookie exception
                    if (replacedBookies.size() <= 0) {
                        throw e;
                    } else {
                        break;
                    }
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("[EnsembleChange-L{}-{}] : changing ensemble from: {} to: {} starting at entry: {}," +
                    " failed bookies: {}, replaced bookies: {}",
                      new Object[] { ledgerId, ensembleChangeIdx, metadata.currentEnsemble, newEnsemble,
                              (getLastAddConfirmed() + 1), failedBookies, replacedBookies });
            }
            metadata.addEnsemble(newEnsembleStartEntry, newEnsemble);
        }
        return new EnsembleInfo(newEnsemble, failedBookies, replacedBookies);
    }

    void handleBookieFailure(final Map<Integer, BookieSocketAddress> failedBookies) {
        int curBlockAddCompletions = blockAddCompletions.incrementAndGet();

        if (bk.disableEnsembleChangeFeature.isAvailable()) {
            blockAddCompletions.decrementAndGet();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ensemble change is disabled. Retry sending to failed bookies {} for ledger {}.",
                    failedBookies, ledgerId);
            }
            unsetSuccessAndSendWriteRequest(failedBookies.keySet());
            return;
        }

        int curNumEnsembleChanges = numEnsembleChanges.incrementAndGet();

        synchronized (metadata) {
            try {
                EnsembleInfo ensembleInfo = replaceBookieInMetadata(failedBookies, curNumEnsembleChanges);
                if (ensembleInfo.replacedBookies.isEmpty()) {
                    blockAddCompletions.decrementAndGet();
                    return;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[EnsembleChange-L{}-{}] : writing new ensemble info = {}, block add completions = {}",
                        new Object[]{getId(), curNumEnsembleChanges, ensembleInfo, curBlockAddCompletions});
                }
                writeLedgerConfig(new ChangeEnsembleCb(ensembleInfo, curBlockAddCompletions, curNumEnsembleChanges));
            } catch (BKException.BKNotEnoughBookiesException e) {
                LOG.error("Could not get additional bookie to remake ensemble, closing ledger: {}", ledgerId);
                handleUnrecoverableErrorDuringAdd(e.getCode());
                return;
            }
        }
    }

    // Contains newly reformed ensemble, bookieIndex, failedBookieAddress
    static final class EnsembleInfo {
        private final ArrayList<BookieSocketAddress> newEnsemble;
        private final Map<Integer, BookieSocketAddress> failedBookies;
        final Set<Integer> replacedBookies;

        public EnsembleInfo(ArrayList<BookieSocketAddress> newEnsemble,
                            Map<Integer, BookieSocketAddress> failedBookies,
                            Set<Integer> replacedBookies) {
            this.newEnsemble = newEnsemble;
            this.failedBookies = failedBookies;
            this.replacedBookies = replacedBookies;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Ensemble Info : failed bookies = ").append(failedBookies)
                    .append(", replaced bookies = ").append(replacedBookies)
                    .append(", new ensemble = ").append(newEnsemble);
            return sb.toString();
        }
    }

    /**
     * Callback which is updating the ledgerMetadata in zk with the newly
     * reformed ensemble. On MetadataVersionException, will reread latest
     * ledgerMetadata and act upon.
     */
    private final class ChangeEnsembleCb extends OrderedSafeGenericCallback<Void> {
        private final EnsembleInfo ensembleInfo;
        private final int curBlockAddCompletions;
        private final int ensembleChangeIdx;

        ChangeEnsembleCb(EnsembleInfo ensembleInfo,
                         int curBlockAddCompletions,
                         int ensembleChangeIdx) {
            super(bk.getMainWorkerPool(), ledgerId);
            this.ensembleInfo = ensembleInfo;
            this.curBlockAddCompletions = curBlockAddCompletions;
            this.ensembleChangeIdx = ensembleChangeIdx;
        }

        @Override
        public void safeOperationComplete(final int rc, Void result) {
            if (rc == BKException.Code.MetadataVersionException) {
                // We changed the ensemble, but got a version exception. We
                // should still consider this as an ensemble change
                ensembleChangeCounter.inc();

                if (LOG.isDebugEnabled()) {
                    LOG.info("[EnsembleChange-L{}-{}] : encountered version conflicts, re-read ledger metadata.",
                        getId(), ensembleChangeIdx);
                }

                rereadMetadata(new ReReadLedgerMetadataCb(rc,
                                       ensembleInfo, curBlockAddCompletions, ensembleChangeIdx));
                return;
            } else if (rc != BKException.Code.OK) {
                LOG.error("[EnsembleChange-L{}-{}] : could not persist ledger metadata : info = {}, closing ledger : {}.",
                        new Object[] { getId(), ensembleChangeIdx, ensembleInfo, rc });
                handleUnrecoverableErrorDuringAdd(rc);
                return;
            }
            int newBlockAddCompletions = blockAddCompletions.decrementAndGet();

            if (LOG.isDebugEnabled()) {
                LOG.info("[EnsembleChange-L{}-{}] : completed ensemble change, block add completion {} => {}",
                    new Object[]{getId(), ensembleChangeIdx, curBlockAddCompletions, newBlockAddCompletions});
            }

            // We've successfully changed an ensemble
            ensembleChangeCounter.inc();
            // the failed bookie has been replaced
            unsetSuccessAndSendWriteRequest(ensembleInfo.replacedBookies);
        }

        @Override
        public String toString() {
            return String.format("ChangeEnsemble(%d)", ledgerId);
        }
    }

    /**
     * Callback which is reading the ledgerMetadata present in zk. This will try
     * to resolve the version conflicts.
     */
    private final class ReReadLedgerMetadataCb extends OrderedSafeGenericCallback<LedgerMetadata> {
        private final int rc;
        private final EnsembleInfo ensembleInfo;
        private final int curBlockAddCompletions;
        private final int ensembleChangeIdx;

        ReReadLedgerMetadataCb(int rc,
                               EnsembleInfo ensembleInfo,
                               int curBlockAddCompletions,
                               int ensembleChangeIdx) {
            super(bk.getMainWorkerPool(), ledgerId);
            this.rc = rc;
            this.ensembleInfo = ensembleInfo;
            this.curBlockAddCompletions = curBlockAddCompletions;
            this.ensembleChangeIdx = ensembleChangeIdx;
        }

        @Override
        public void safeOperationComplete(int newrc, LedgerMetadata newMeta) {
            if (newrc != BKException.Code.OK) {
                LOG.error("[EnsembleChange-L{}-{}] : error re-reading metadata to address ensemble change conflicts," +
                        " code=", new Object[] { ledgerId, ensembleChangeIdx, newrc });
                handleUnrecoverableErrorDuringAdd(rc);
            } else {
                if (!resolveConflict(newMeta)) {
                    LOG.error("[EnsembleChange-L{}-{}] : could not resolve ledger metadata conflict" +
                            " while changing ensemble to: {}, local meta data is \n {} \n," +
                            " zk meta data is \n {} \n, closing ledger",
                            new Object[] { ledgerId, ensembleChangeIdx, ensembleInfo.newEnsemble, metadata, newMeta });
                    handleUnrecoverableErrorDuringAdd(rc);
                }
            }
        }

        /**
         * Specific resolve conflicts happened when multiple bookies failures in same ensemble.
         * <p>
         * Resolving the version conflicts between local ledgerMetadata and zk
         * ledgerMetadata. This will do the following:
         * <ul>
         * <li>
         * check whether ledgerMetadata state matches of local and zk</li>
         * <li>
         * if the zk ledgerMetadata still contains the failed bookie, then
         * update zookeeper with the newBookie otherwise send write request</li>
         * </ul>
         * </p>
         */
        private boolean resolveConflict(LedgerMetadata newMeta) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[EnsembleChange-L{}-{}] : resolving conflicts - local metadata = \n {} \n," +
                    " zk metadata = \n {} \n", new Object[]{ledgerId, ensembleChangeIdx, metadata, newMeta});
            }
            // make sure the ledger isn't closed by other ones.
            if (metadata.getState() != newMeta.getState()) {
                if (LOG.isDebugEnabled()) {
                    LOG.info("[EnsembleChange-L{}-{}] : resolving conflicts but state changed," +
                            " local metadata = \n {} \n, zk metadata = \n {} \n",
                        new Object[]{ledgerId, ensembleChangeIdx, metadata, newMeta});
                }
                return false;
            }

            // We should check number of ensembles since there are two kinds of metadata conflicts:
            // - Case 1: Multiple bookies involved in ensemble change.
            //           Number of ensembles should be same in this case.
            // - Case 2: Recovery (Auto/Manually) replaced ensemble and ensemble changed.
            //           The metadata changed due to ensemble change would have one more ensemble
            //           than the metadata changed by recovery.
            int diff = newMeta.getEnsembles().size() - metadata.getEnsembles().size();
            if (0 != diff) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[EnsembleChange-L{}-{}] : resolving conflicts but ensembles have {} differences," +
                            " local metadata = \n {} \n, zk metadata = \n {} \n",
                        new Object[]{ledgerId, ensembleChangeIdx, diff, metadata, newMeta});
                }
                if (-1 == diff) {
                    // Case 1: metadata is changed by other ones (e.g. Recovery)
                    return updateMetadataIfPossible(newMeta);
                }
                return false;
            }

            //
            // Case 2:
            //
            // If the failed the bookie is still existed in the metadata (in zookeeper), it means that
            // the ensemble change of the failed bookie is failed due to metadata conflicts. so try to
            // update the ensemble change metadata again. Otherwise, it means that the ensemble change
            // is already succeed, unset the success and re-adding entries.
            if (!areFailedBookiesReplaced(newMeta, ensembleInfo)) {
                // If the in-memory data doesn't contains the failed bookie, it means the ensemble change
                // didn't finish, so try to resolve conflicts with the metadata read from zookeeper and
                // update ensemble changed metadata again.
                if (areFailedBookiesReplaced(metadata, ensembleInfo)) {
                    return updateMetadataIfPossible(newMeta);
                }
            } else {
                ensembleChangeCounter.inc();
                // We've successfully changed an ensemble
                // the failed bookie has been replaced
                int newBlockAddCompletions = blockAddCompletions.decrementAndGet();
                unsetSuccessAndSendWriteRequest(ensembleInfo.replacedBookies);
                if (LOG.isDebugEnabled()) {
                    LOG.info("[EnsembleChange-L{}-{}] : resolved conflicts, block add complectiosn {} => {}.",
                        new Object[]{ledgerId, ensembleChangeIdx, curBlockAddCompletions, newBlockAddCompletions});
                }
            }
            return true;
        }

        /**
         * Check whether all the failed bookies are replaced.
         *
         * @param newMeta
         *          new ledger metadata
         * @param ensembleInfo
         *          ensemble info used for ensemble change.
         * @return true if all failed bookies are replaced, false otherwise
         */
        private boolean areFailedBookiesReplaced(LedgerMetadata newMeta, EnsembleInfo ensembleInfo) {
            boolean replaced = true;
            for (Integer replacedBookieIdx : ensembleInfo.replacedBookies) {
                BookieSocketAddress failedBookieAddr = ensembleInfo.failedBookies.get(replacedBookieIdx);
                BookieSocketAddress replacedBookieAddr = newMeta.currentEnsemble.get(replacedBookieIdx);
                replaced &= !Objects.equal(replacedBookieAddr, failedBookieAddr);
            }
            return replaced;
        }

        private boolean updateMetadataIfPossible(LedgerMetadata newMeta) {
            // if the local metadata is newer than zookeeper metadata, it means that metadata is updated
            // again when it was trying re-reading the metatada, re-kick the reread again
            if (metadata.isNewerThan(newMeta)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[EnsembleChange-L{}-{}] : reread metadata because local metadata is newer.",
                        new Object[]{ledgerId, ensembleChangeIdx});
                }
                rereadMetadata(this);
                return true;
            }
            // make sure the metadata doesn't changed by other ones.
            if (metadata.isConflictWith(newMeta)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[EnsembleChange-L{}-{}] : metadata is conflicted, local metadata = \n {} \n," +
                        " zk metadata = \n {} \n", new Object[]{ledgerId, ensembleChangeIdx, metadata, newMeta});
                }
                return false;
            }
            if (LOG.isDebugEnabled()) {
                LOG.info("[EnsembleChange-L{}-{}] : resolved ledger metadata conflict and writing to zookeeper,"
                        + " local meta data is \n {} \n, zk meta data is \n {}.",
                    new Object[]{ledgerId, ensembleChangeIdx, metadata, newMeta});
            }
            // update znode version
            metadata.setVersion(newMeta.getVersion());
            // merge ensemble infos from new meta except last ensemble
            // since they might be modified by recovery tool.
            metadata.mergeEnsembles(newMeta.getEnsembles());
            writeLedgerConfig(new ChangeEnsembleCb(ensembleInfo, curBlockAddCompletions, ensembleChangeIdx));
            return true;
        }

        @Override
        public String toString() {
            return String.format("ReReadLedgerMetadata(%d)", ledgerId);
        }
    }

    void unsetSuccessAndSendWriteRequest(final Set<Integer> bookies) {
        for (PendingAddOp pendingAddOp : pendingAddOps) {
            for (Integer bookieIndex: bookies) {
                pendingAddOp.unsetSuccessAndSendWriteRequest(bookieIndex);
            }
        }
    }

    void rereadMetadata(final GenericCallback<LedgerMetadata> cb) {
        bk.getLedgerManager().readLedgerMetadata(ledgerId, cb);
    }

    void registerOperationFailureOnBookie(BookieSocketAddress bookie, long entryId) {
        if (bk.getConf().getEnableBookieFailureTracking()) {
            bookieFailureHistory.put(bookie, entryId);
        }
    }


    void recover(GenericCallback<Void> finalCb) {
        recover(finalCb, null, false);
    }

    /**
     * Recover the ledger.
     *
     * @param finalCb
     *          callback after recovery is done.
     * @param listener
     *          read entry listener on recovery reads.
     * @param forceRecovery
     *          force the recovery procedure even the ledger metadata shows the ledger is closed.
     */
    void recover(GenericCallback<Void> finalCb,
                 final @VisibleForTesting BookkeeperInternalCallbacks.ReadEntryListener listener,
                 final boolean forceRecovery) {
        final GenericCallback<Void> cb = new TimedGenericCallback<Void>(
            finalCb,
            BKException.Code.OK,
            bk.getRecoverOpLogger());
        boolean wasClosed = false;
        boolean wasInRecovery = false;

        synchronized (this) {
            if (metadata.isClosed()) {
                if (forceRecovery) {
                    wasClosed = false;
                    // mark the ledger back to in recovery state, so it would proceed ledger recovery again.
                    wasInRecovery = false;
                    metadata.markLedgerInRecovery();
                } else {
                    lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
                    length = metadata.getLength();
                    wasClosed = true;
                }
            } else {
                wasClosed = false;
                if (metadata.isInRecovery()) {
                    wasInRecovery = true;
                } else {
                    wasInRecovery = false;
                    metadata.markLedgerInRecovery();
                }
            }
        }

        if (wasClosed) {
            // We are already closed, nothing to do
            cb.operationComplete(BKException.Code.OK, null);
            return;
        }

        if (wasInRecovery) {
            // if metadata is already in recover, dont try to write again,
            // just do the recovery from the starting point
            new LedgerRecoveryOp(LedgerHandle.this, cb)
                    .parallelRead(enableParallelRecoveryRead)
                    .readBatchSize(recoveryReadBatchSize)
                    .setEntryListener(listener)
                    .initiate();
            return;
        }

        writeLedgerConfig(new OrderedSafeGenericCallback<Void>(bk.getMainWorkerPool(), ledgerId) {
            @Override
            public void safeOperationComplete(final int rc, Void result) {
                if (rc == BKException.Code.MetadataVersionException) {
                    rereadMetadata(new OrderedSafeGenericCallback<LedgerMetadata>(bk.getMainWorkerPool(),
                                                                                  ledgerId) {
                        @Override
                        public void safeOperationComplete(int rc, LedgerMetadata newMeta) {
                            if (rc != BKException.Code.OK) {
                                cb.operationComplete(rc, null);
                            } else {
                                metadata = newMeta;
                                recover(cb, listener, forceRecovery);
                            }
                        }

                        @Override
                        public String toString() {
                            return String.format("ReReadMetadataForRecover(%d)", ledgerId);
                        }
                    });
                } else if (rc == BKException.Code.OK) {
                    // we only could issue recovery operation after we successfully update the ledger state to in recovery
                    // otherwise, it couldn't prevent us advancing last confirmed while the other writer is closing the ledger,
                    // which will cause inconsistent last add confirmed on bookies & zookeeper metadata.
                    new LedgerRecoveryOp(LedgerHandle.this, cb)
                        .parallelRead(enableParallelRecoveryRead)
                        .readBatchSize(recoveryReadBatchSize)
                        .setEntryListener(listener)
                        .initiate();
                } else {
                    LOG.error("Error writing ledger config {} of ledger {}", rc, ledgerId);
                    cb.operationComplete(rc, null);
                }
            }

            @Override
            public String toString() {
                return String.format("WriteLedgerConfigForRecover(%d)", ledgerId);
            }
        });
    }

    static class NoopCloseCallback implements CloseCallback {
        static NoopCloseCallback instance = new NoopCloseCallback();

        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            if (rc != BKException.Code.OK) {
                LOG.warn("Close failed: " + BKException.getMessage(rc));
            }
            // noop
        }
    }

}
