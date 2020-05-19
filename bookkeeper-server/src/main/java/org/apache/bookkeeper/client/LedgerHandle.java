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

import static com.google.common.base.Preconditions.checkState;

import static org.apache.bookkeeper.client.api.BKException.Code.ClientClosedException;
import static org.apache.bookkeeper.client.api.BKException.Code.WriteException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.AddCallbackWithLatency;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BKException.BKIncorrectParameterException;
import org.apache.bookkeeper.client.BKException.BKReadException;
import org.apache.bookkeeper.client.DistributionSchedule.WriteSet;
import org.apache.bookkeeper.client.SyncCallbackUtils.FutureReadLastConfirmed;
import org.apache.bookkeeper.client.SyncCallbackUtils.FutureReadLastConfirmedAndEntry;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncAddCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncCloseCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncReadCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncReadLastConfirmedCallback;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger handle contains ledger metadata and is used to access the read and
 * write operations to a ledger.
 */
public class LedgerHandle implements WriteHandle {
    static final Logger LOG = LoggerFactory.getLogger(LedgerHandle.class);

    private static final int STICKY_READ_BOOKIE_INDEX_UNSET = -1;

    final ClientContext clientCtx;

    final byte[] ledgerKey;
    private Versioned<LedgerMetadata> versionedMetadata;
    final long ledgerId;
    long lastAddPushed;

    private enum HandleState {
        OPEN,
        CLOSED
    }

    private HandleState handleState = HandleState.OPEN;
    private final CompletableFuture<Void> closePromise = new CompletableFuture<>();

    /**
      * Last entryId which has been confirmed to be written durably to the bookies.
      * This value is used by readers, the the LAC protocol
      */
    volatile long lastAddConfirmed;

     /**
      * Next entryId which is expected to move forward during {@link #sendAddSuccessCallbacks() }. This is important
      * in order to have an ordered sequence of addEntry ackknowledged to the writer
      */
    volatile long pendingAddsSequenceHead;

    /**
     * If bookie sticky reads are enabled, this will contain the index of the bookie
     * selected as "sticky" for this ledger. The bookie is chosen at random when the
     * LedgerHandle is created.
     *
     * <p>In case of failures, the bookie index will be updated (to the next bookie in
     * the ensemble) to avoid continuing to attempt to read from a failed bookie.
     *
     * <p>If the index is -1, it means the sticky reads are disabled.
     */
    private int stickyBookieIndex;

    long length;
    final DigestManager macManager;
    final DistributionSchedule distributionSchedule;
    final RateLimiter throttler;
    final LoadingCache<BookieSocketAddress, Long> bookieFailureHistory;
    final BookiesHealthInfo bookiesHealthInfo;
    final EnumSet<WriteFlag> writeFlags;

    ScheduledFuture<?> timeoutFuture = null;

    @VisibleForTesting
    final Map<Integer, BookieSocketAddress> delayedWriteFailedBookies =
        new HashMap<Integer, BookieSocketAddress>();

    /**
     * Invalid entry id. This value is returned from methods which
     * should return an entry id but there is no valid entry available.
     */
    public static final long INVALID_ENTRY_ID = BookieProtocol.INVALID_ENTRY_ID;

    /**
     * Invalid ledger id. Ledger IDs must be greater than or equal to 0.
     * Large negative used to make it easy to spot in logs if erroneously used.
     */
    public static final long INVALID_LEDGER_ID = -0xABCDABCDL;

    final Object metadataLock = new Object();
    boolean changingEnsemble = false;
    final AtomicInteger numEnsembleChanges = new AtomicInteger(0);
    Queue<PendingAddOp> pendingAddOps;
    ExplicitLacFlushPolicy explicitLacFlushPolicy;

    final Counter ensembleChangeCounter;
    final Counter lacUpdateHitsCounter;
    final Counter lacUpdateMissesCounter;
    private final OpStatsLogger clientChannelWriteWaitStats;

    LedgerHandle(ClientContext clientCtx,
                 long ledgerId, Versioned<LedgerMetadata> versionedMetadata,
                 BookKeeper.DigestType digestType, byte[] password,
                 EnumSet<WriteFlag> writeFlags)
            throws GeneralSecurityException, NumberFormatException {
        this.clientCtx = clientCtx;

        this.versionedMetadata = versionedMetadata;
        this.pendingAddOps = new ConcurrentLinkedQueue<PendingAddOp>();
        this.writeFlags = writeFlags;

        LedgerMetadata metadata = versionedMetadata.getValue();
        if (metadata.isClosed()) {
            lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
            length = metadata.getLength();
        } else {
            lastAddConfirmed = lastAddPushed = INVALID_ENTRY_ID;
            length = 0;
        }

        this.pendingAddsSequenceHead = lastAddConfirmed;

        this.ledgerId = ledgerId;

        if (clientCtx.getConf().enableStickyReads
                && getLedgerMetadata().getEnsembleSize() == getLedgerMetadata().getWriteQuorumSize()) {
            stickyBookieIndex = clientCtx.getPlacementPolicy().getStickyReadBookieIndex(metadata, Optional.empty());
        } else {
            stickyBookieIndex = STICKY_READ_BOOKIE_INDEX_UNSET;
        }

        if (clientCtx.getConf().throttleValue > 0) {
            this.throttler = RateLimiter.create(clientCtx.getConf().throttleValue);
        } else {
            this.throttler = null;
        }

        macManager = DigestManager.instantiate(ledgerId, password, BookKeeper.DigestType.toProtoDigestType(digestType),
                                               clientCtx.getByteBufAllocator(), clientCtx.getConf().useV2WireProtocol);

        // If the password is empty, pass the same random ledger key which is generated by the hash of the empty
        // password, so that the bookie can avoid processing the keys for each entry
        this.ledgerKey = DigestManager.generateMasterKey(password);
        distributionSchedule = new RoundRobinDistributionSchedule(
                metadata.getWriteQuorumSize(),
                metadata.getAckQuorumSize(),
                metadata.getEnsembleSize());
        this.bookieFailureHistory = CacheBuilder.newBuilder()
            .expireAfterWrite(clientCtx.getConf().bookieFailureHistoryExpirationMSec, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<BookieSocketAddress, Long>() {
            @Override
            public Long load(BookieSocketAddress key) {
                return -1L;
            }
        });
        this.bookiesHealthInfo = new BookiesHealthInfo() {
            @Override
            public long getBookieFailureHistory(BookieSocketAddress bookieSocketAddress) {
                Long lastFailure = bookieFailureHistory.getIfPresent(bookieSocketAddress);
                return lastFailure == null ? -1L : lastFailure;
            }

            @Override
            public long getBookiePendingRequests(BookieSocketAddress bookieSocketAddress) {
                return clientCtx.getBookieClient().getNumPendingRequests(bookieSocketAddress, ledgerId);
            }
        };

        ensembleChangeCounter = clientCtx.getClientStats().getEnsembleChangeCounter();
        lacUpdateHitsCounter = clientCtx.getClientStats().getLacUpdateHitsCounter();
        lacUpdateMissesCounter = clientCtx.getClientStats().getLacUpdateMissesCounter();
        clientChannelWriteWaitStats = clientCtx.getClientStats().getClientChannelWriteWaitLogger();

        clientCtx.getClientStats().registerPendingAddsGauge(new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pendingAddOps.size();
                }
            });

        initializeWriteHandleState();
    }

    /**
     * Notify the LedgerHandle that a read operation was failed on a particular bookie.
     */
    void recordReadErrorOnBookie(int bookieIndex) {
        // If sticky bookie reads are enabled, switch the sticky bookie to the
        // next bookie in the ensemble so that we avoid to keep reading from the
        // same failed bookie
        if (stickyBookieIndex != STICKY_READ_BOOKIE_INDEX_UNSET) {
            // This will be idempotent when we have multiple read errors on the
            // same bookie. The net result is that we just go to the next bookie
            stickyBookieIndex = clientCtx.getPlacementPolicy().getStickyReadBookieIndex(getLedgerMetadata(),
                    Optional.of(bookieIndex));
        }
    }

    protected void initializeWriteHandleState() {
        if (clientCtx.getConf().explicitLacInterval > 0) {
            explicitLacFlushPolicy = new ExplicitLacFlushPolicy.ExplicitLacFlushPolicyImpl(
                    this, clientCtx);
        } else {
            explicitLacFlushPolicy = ExplicitLacFlushPolicy.VOID_EXPLICITLAC_FLUSH_POLICY;
        }

        if (clientCtx.getConf().addEntryQuorumTimeoutNanos > 0) {
            SafeRunnable monitor = new SafeRunnable() {
                @Override
                public void safeRun() {
                    monitorPendingAddOps();
                }
            };
            this.timeoutFuture = clientCtx.getScheduler().scheduleAtFixedRate(
                    monitor,
                    clientCtx.getConf().timeoutMonitorIntervalSec,
                    clientCtx.getConf().timeoutMonitorIntervalSec,
                    TimeUnit.SECONDS);
        }
    }

    private void tearDownWriteHandleState() {
        explicitLacFlushPolicy.stopExplicitLacFlush();
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
    }

    /**
     * Get the id of the current ledger.
     *
     * @return the id of the ledger
     */
    @Override
    public long getId() {
        return ledgerId;
    }

    @VisibleForTesting
    public EnumSet<WriteFlag> getWriteFlags() {
        return writeFlags;
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
        return versionedMetadata.getValue();
    }

    Versioned<LedgerMetadata> getVersionedLedgerMetadata() {
        return versionedMetadata;
    }

    boolean setLedgerMetadata(Versioned<LedgerMetadata> expected, Versioned<LedgerMetadata> newMetadata) {
        synchronized (this) {
            // ensure that we only update the metadata if it is the object we expect it to be
            if (versionedMetadata == expected) {
                versionedMetadata = newMetadata;
                LedgerMetadata metadata = versionedMetadata.getValue();
                if (metadata.isClosed()) {
                    lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
                    length = metadata.getLength();
                }
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Get this ledger's customMetadata map.
     *
     * @return map containing user provided customMetadata.
     */
    public Map<String, byte[]> getCustomMetadata() {
        return getLedgerMetadata().getCustomMetadata();
    }

    /**
     * Get the number of fragments that makeup this ledger.
     *
     * @return the count of fragments
     */
    public synchronized long getNumFragments() {
        return getLedgerMetadata().getAllEnsembles().size();
    }

    /**
     * Get the count of unique bookies that own part of this ledger
     * by going over all the fragments of the ledger.
     *
     * @return count of unique bookies
     */
    public synchronized long getNumBookies() {
        Map<Long, ? extends List<BookieSocketAddress>> m = getLedgerMetadata().getAllEnsembles();
        Set<BookieSocketAddress> s = Sets.newHashSet();
        for (List<BookieSocketAddress> aList : m.values()) {
            s.addAll(aList);
        }
        return s.size();
    }

    /**
     * Get the DigestManager.
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
    @Override
    public synchronized long getLength() {
        return this.length;
    }

    /**
     * Returns the ledger creation time.
     *
     * @return the ledger creation time
     */
    public long getCtime() {
        return getLedgerMetadata().getCtime();
    }

    /**
     * Get the Distribution Schedule.
     *
     * @return DistributionSchedule for the LedgerHandle
     */
    DistributionSchedule getDistributionSchedule() {
        return distributionSchedule;
    }

    /**
     * Get the health info for bookies for this ledger.
     *
     * @return BookiesHealthInfo for every bookie in the write set.
     */
    BookiesHealthInfo getBookiesHealthInfo() {
        return bookiesHealthInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
            throws InterruptedException, BKException {
        SyncCallbackUtils.waitForResult(closeAsync());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        SyncCloseCallback callback = new SyncCloseCallback(result);
        asyncClose(callback, null);
        return result;
    }

    /**
     * Asynchronous close, any adds in flight will return errors.
     *
     * <p>Closing a ledger will ensure that all clients agree on what the last entry
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
        return getLedgerMetadata().isClosed();
    }

    boolean isHandleWritable() {
        return !getLedgerMetadata().isClosed() && handleState == HandleState.OPEN;
    }

    void asyncCloseInternal(final CloseCallback cb, final Object ctx, final int rc) {
        try {
            doAsyncCloseInternal(cb, ctx, rc);
        } catch (RejectedExecutionException re) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to close ledger {} : ", ledgerId, re);
            }
            errorOutPendingAdds(BookKeeper.getReturnRc(clientCtx.getBookieClient(), rc));
            cb.closeComplete(BookKeeper.getReturnRc(clientCtx.getBookieClient(), BKException.Code.InterruptedException),
                             this, ctx);
        }
    }

    /**
     * Same as public version of asyncClose except that this one takes an
     * additional parameter which is the return code to hand to all the pending
     * add ops.
     *
     * @param cb
     * @param ctx
     * @param rc
     */
    void doAsyncCloseInternal(final CloseCallback cb, final Object ctx, final int rc) {
        clientCtx.getMainWorkerPool().executeOrdered(ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                final HandleState prevHandleState;
                final List<PendingAddOp> pendingAdds;
                final long lastEntry;
                final long finalLength;

                closePromise.whenComplete((ignore, ex) -> {
                        if (ex != null) {
                            cb.closeComplete(
                                    BKException.getExceptionCode(ex, BKException.Code.UnexpectedConditionException),
                                    LedgerHandle.this, ctx);
                        } else {
                            cb.closeComplete(BKException.Code.OK, LedgerHandle.this, ctx);
                        }
                    });

                synchronized (LedgerHandle.this) {
                    prevHandleState = handleState;

                    // drain pending adds first
                    pendingAdds = drainPendingAddsAndAdjustLength();

                    // taking the length must occur after draining, as draining changes the length
                    lastEntry = lastAddPushed = LedgerHandle.this.lastAddConfirmed;
                    finalLength = LedgerHandle.this.length;
                    handleState = HandleState.CLOSED;
                }

                // error out all pending adds during closing, the callbacks shouldn't be
                // running under any bk locks.
                errorOutPendingAdds(rc, pendingAdds);

                if (prevHandleState != HandleState.CLOSED) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Closing ledger: {} at entryId {} with {} bytes", getId(), lastEntry, finalLength);
                    }

                    tearDownWriteHandleState();
                    new MetadataUpdateLoop(
                            clientCtx.getLedgerManager(), getId(),
                            LedgerHandle.this::getVersionedLedgerMetadata,
                            (metadata) -> {
                                if (metadata.isClosed()) {
                                    /* If the ledger has been closed with the same lastEntry
                                     * and length that we planned to close with, we have nothing to do,
                                     * so just return success */
                                    if (lastEntry == metadata.getLastEntryId()
                                        && finalLength == metadata.getLength()) {
                                        return false;
                                    } else {
                                        LOG.error("Metadata conflict when closing ledger {}."
                                                  + " Another client may have recovered the ledger while there"
                                                  + " were writes outstanding. (local lastEntry:{} length:{}) "
                                                  + " (metadata lastEntry:{} length:{})",
                                                  getId(), lastEntry, finalLength,
                                                  metadata.getLastEntryId(), metadata.getLength());
                                        throw new BKException.BKMetadataVersionException();
                                    }
                                } else {
                                    return true;
                                }
                            },
                            (metadata) -> {
                                return LedgerMetadataBuilder.from(metadata)
                                    .withClosedState().withLastEntryId(lastEntry)
                                    .withLength(finalLength).build();
                            },
                            LedgerHandle.this::setLedgerMetadata)
                        .run().whenComplete((metadata, ex) -> {
                                if (ex != null) {
                                    closePromise.completeExceptionally(ex);
                                } else {
                                    FutureUtils.complete(closePromise, null);
                                }
                        });
                }
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
     * @see #asyncReadEntries(long, long, ReadCallback, Object)
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
     * {@link #asyncReadUnconfirmedEntries(long, long, ReadCallback, Object) }
     *
     * @param firstEntry
     *          id of first entry of sequence (included)
     * @param lastEntry
     *          id of last entry of sequence (included)
     *
     * @see #readEntries(long, long)
     * @see #asyncReadUnconfirmedEntries(long, long, ReadCallback, java.lang.Object)
     * @see #asyncReadLastConfirmed(ReadLastConfirmedCallback, java.lang.Object)
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
                    ledgerId, firstEntry, lastEntry);
            cb.readComplete(BKException.Code.IncorrectParameterException, this, null, ctx);
            return;
        }

        if (lastEntry > lastAddConfirmed) {
            LOG.error("ReadException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    ledgerId, firstEntry, lastEntry);
            cb.readComplete(BKException.Code.ReadException, this, null, ctx);
            return;
        }

        asyncReadEntriesInternal(firstEntry, lastEntry, cb, ctx, false);
    }

    /**
     * Read a sequence of entries asynchronously, allowing to read after the LastAddConfirmed range.
     * <br>This is the same of
     * {@link #asyncReadEntries(long, long, ReadCallback, Object) }
     * but it lets the client read without checking the local value of LastAddConfirmed, so that it is possibile to
     * read entries for which the writer has not received the acknowledge yet. <br>
     * For entries which are within the range 0..LastAddConfirmed BookKeeper guarantees that the writer has successfully
     * received the acknowledge.<br>
     * For entries outside that range it is possible that the writer never received the acknowledge
     * and so there is the risk that the reader is seeing entries before the writer and this could result in
     * a consistency issue in some cases.<br>
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
     * @see #asyncReadEntries(long, long, ReadCallback, Object)
     * @see #asyncReadLastConfirmed(ReadLastConfirmedCallback, Object)
     * @see #readUnconfirmedEntries(long, long)
     */
    public void asyncReadUnconfirmedEntries(long firstEntry, long lastEntry, ReadCallback cb, Object ctx) {
        // Little sanity check
        if (firstEntry < 0 || firstEntry > lastEntry) {
            LOG.error("IncorrectParameterException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    ledgerId, firstEntry, lastEntry);
            cb.readComplete(BKException.Code.IncorrectParameterException, this, null, ctx);
            return;
        }

        asyncReadEntriesInternal(firstEntry, lastEntry, cb, ctx, false);
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
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        // Little sanity check
        if (firstEntry < 0 || firstEntry > lastEntry) {
            LOG.error("IncorrectParameterException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    ledgerId, firstEntry, lastEntry);
            return FutureUtils.exception(new BKIncorrectParameterException());
        }

        if (lastEntry > lastAddConfirmed) {
            LOG.error("ReadException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    ledgerId, firstEntry, lastEntry);
            return FutureUtils.exception(new BKReadException());
        }

        return readEntriesInternalAsync(firstEntry, lastEntry, false);
    }

    /**
     * Read a sequence of entries asynchronously, allowing to read after the LastAddConfirmed range.
     * <br>This is the same of
     * {@link #asyncReadEntries(long, long, ReadCallback, Object) }
     * but it lets the client read without checking the local value of LastAddConfirmed, so that it is possibile to
     * read entries for which the writer has not received the acknowledge yet. <br>
     * For entries which are within the range 0..LastAddConfirmed BookKeeper guarantees that the writer has successfully
     * received the acknowledge.<br>
     * For entries outside that range it is possible that the writer never received the acknowledge
     * and so there is the risk that the reader is seeing entries before the writer and this could result in
     * a consistency issue in some cases.<br>
     * With this method you can even read entries before the LastAddConfirmed and entries after it with one call,
     * the expected consistency will be as described above for each subrange of ids.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     *
     * @see #asyncReadEntries(long, long, ReadCallback, Object)
     * @see #asyncReadLastConfirmed(ReadLastConfirmedCallback, Object)
     * @see #readUnconfirmedEntries(long, long)
     */
    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        // Little sanity check
        if (firstEntry < 0 || firstEntry > lastEntry) {
            LOG.error("IncorrectParameterException on ledgerId:{} firstEntry:{} lastEntry:{}",
                    ledgerId, firstEntry, lastEntry);
            return FutureUtils.exception(new BKIncorrectParameterException());
        }

        return readEntriesInternalAsync(firstEntry, lastEntry, false);
    }

    void asyncReadEntriesInternal(long firstEntry, long lastEntry, ReadCallback cb,
                                  Object ctx, boolean isRecoveryRead) {
        if (!clientCtx.isClientClosed()) {
            readEntriesInternalAsync(firstEntry, lastEntry, isRecoveryRead)
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
                    }, clientCtx.getMainWorkerPool().chooseThread(ledgerId));
        } else {
            cb.readComplete(Code.ClientClosedException, LedgerHandle.this, null, ctx);
        }
    }

    /*
     * Read the last entry in the ledger
     *
     * @param cb
     *            object implementing read callback interface
     * @param ctx
     *            control object
     */
    public void asyncReadLastEntry(ReadCallback cb, Object ctx) {
        long lastEntryId = getLastAddConfirmed();
        if (lastEntryId < 0) {
            // Ledger was empty, so there is no last entry to read
            cb.readComplete(BKException.Code.NoSuchEntryException, this, null, ctx);
        } else {
            asyncReadEntriesInternal(lastEntryId, lastEntryId, cb, ctx, false);
        }
    }

    public LedgerEntry readLastEntry()
        throws InterruptedException, BKException {
        long lastEntryId = getLastAddConfirmed();
        if (lastEntryId < 0) {
            // Ledger was empty, so there is no last entry to read
            throw new BKException.BKNoSuchEntryException();
        } else {
            CompletableFuture<Enumeration<LedgerEntry>> result = new CompletableFuture<>();
            asyncReadEntries(lastEntryId, lastEntryId, new SyncReadCallback(result), null);

            return SyncCallbackUtils.waitForResult(result).nextElement();
        }
    }

    CompletableFuture<LedgerEntries> readEntriesInternalAsync(long firstEntry,
                                                              long lastEntry,
                                                              boolean isRecoveryRead) {
        PendingReadOp op = new PendingReadOp(this, clientCtx,
                                             firstEntry, lastEntry, isRecoveryRead);
        if (!clientCtx.isClientClosed()) {
            // Waiting on the first one.
            // This is not very helpful if there are multiple ensembles or if bookie goes into unresponsive
            // state later after N requests sent.
            // Unfortunately it seems that alternatives are:
            // - send reads one-by-one (up to the app)
            // - rework LedgerHandle to send requests one-by-one (maybe later, potential perf impact)
            // - block worker pool (not good)
            // Even with this implementation one should be more concerned about OOME when all read responses arrive
            // or about overloading bookies with these requests then about submission of many small requests.
            // Naturally one of the solutions would be to submit smaller batches and in this case
            // current implementation will prevent next batch from starting when bookie is
            // unresponsive thus helpful enough.
            DistributionSchedule.WriteSet ws = distributionSchedule.getWriteSet(firstEntry);
            try {
                if (!waitForWritable(ws, firstEntry, ws.size() - 1, clientCtx.getConf().waitForWriteSetMs)) {
                    op.allowFailFastOnUnwritableChannel();
                }
            } finally {
                ws.recycle();
            }

            if (isHandleWritable()) {
                // Ledger handle in read/write mode: submit to OSE for ordered execution.
                clientCtx.getMainWorkerPool().executeOrdered(ledgerId, op);
            } else {
                // Read-only ledger handle: bypass OSE and execute read directly in client thread.
                // This avoids a context-switch to OSE thread and thus reduces latency.
                op.run();
            }
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
     *         do not reuse the buffer, bk-client will release it appropriately
     * @return the entryId of the new inserted entry
     */
    public long addEntry(byte[] data) throws InterruptedException, BKException {
        return addEntry(data, 0, data.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Long> appendAsync(ByteBuf data) {
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
     *            do not reuse the buffer, bk-client will release it appropriately
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
     *         do not reuse the buffer, bk-client will release it appropriately
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
     *            do not reuse the buffer, bk-client will release it appropriately
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
     *          do not reuse the buffer, bk-client will release it appropriately
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
     *            do not reuse the buffer, bk-client will release it appropriately
     * @param cb
     *            object implementing callbackinterface
     * @param ctx
     *            some control object
     */
    public void asyncAddEntry(final long entryId, final byte[] data, final AddCallback cb, final Object ctx) {
        LOG.error("To use this feature Ledger must be created with createLedgerAdv() interface.");
        cb.addCompleteWithLatency(BKException.Code.IllegalOpException, LedgerHandle.this, entryId, 0, ctx);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     *
     * @param data
     *          array of bytes to be written
     *          do not reuse the buffer, bk-client will release it appropriately
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
        if (offset < 0 || length < 0 || (offset + length) > data.length) {
            throw new ArrayIndexOutOfBoundsException(
                    "Invalid values for offset(" + offset
                    + ") or length(" + length + ")");
        }

        asyncAddEntry(Unpooled.wrappedBuffer(data, offset, length), cb, ctx);
    }

    public void asyncAddEntry(ByteBuf data, final AddCallback cb, final Object ctx) {
        PendingAddOp op = PendingAddOp.create(this, clientCtx, getCurrentEnsemble(), data, writeFlags, cb, ctx);
        doAsyncAddEntry(op);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     * This can be used only with {@link LedgerHandleAdv} returned through
     * ledgers created with
     * {@link BookKeeper#createLedgerAdv(int, int, int, org.apache.bookkeeper.client.BookKeeper.DigestType, byte[])}.
     *
     * @param entryId
     *            entryId of the entry to add.
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
    public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length,
            final AddCallback cb, final Object ctx) {
        LOG.error("To use this feature Ledger must be created with createLedgerAdv() interface.");
        cb.addCompleteWithLatency(BKException.Code.IllegalOpException, LedgerHandle.this, entryId, 0, ctx);
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
    public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length,
                              final AddCallbackWithLatency cb, final Object ctx) {
        LOG.error("To use this feature Ledger must be created with createLedgerAdv() interface.");
        cb.addCompleteWithLatency(BKException.Code.IllegalOpException, LedgerHandle.this, entryId, 0, ctx);
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
    public void asyncAddEntry(final long entryId, ByteBuf data,
                              final AddCallbackWithLatency cb, final Object ctx) {
        LOG.error("To use this feature Ledger must be created with createLedgerAdv() interface.");
        cb.addCompleteWithLatency(BKException.Code.IllegalOpException, LedgerHandle.this, entryId, 0, ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> force() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ForceLedgerOp op = new ForceLedgerOp(this, clientCtx.getBookieClient(), getCurrentEnsemble(), result);
        boolean wasClosed = false;
        synchronized (this) {
            // synchronized on this to ensure that
            // the ledger isn't closed between checking and
            // updating lastAddPushed
            if (!isHandleWritable()) {
                wasClosed = true;
            }
        }

        if (wasClosed) {
            // make sure the callback is triggered in main worker pool
            try {
                clientCtx.getMainWorkerPool().executeOrdered(ledgerId, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        LOG.warn("Force() attempted on a closed ledger: {}", ledgerId);
                        result.completeExceptionally(new BKException.BKLedgerClosedException());
                    }

                    @Override
                    public String toString() {
                        return String.format("force(lid=%d)", ledgerId);
                    }
                });
            } catch (RejectedExecutionException e) {
                result.completeExceptionally(new BKException.BKInterruptedException());
            }
            return result;
        }

        // early exit: no write has been issued yet
        if (pendingAddsSequenceHead == INVALID_ENTRY_ID) {
            clientCtx.getMainWorkerPool().executeOrdered(ledgerId, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        FutureUtils.complete(result, null);
                    }

                    @Override
                    public String toString() {
                        return String.format("force(lid=%d)", ledgerId);
                    }
                });
            return result;
        }

        try {
            clientCtx.getMainWorkerPool().executeOrdered(ledgerId, op);
        } catch (RejectedExecutionException e) {
            result.completeExceptionally(new BKException.BKInterruptedException());
        }
        return result;
    }

    /**
     * Make a recovery add entry request. Recovery adds can add to a ledger even
     * if it has been fenced.
     *
     * <p>This is only valid for bookie and ledger recovery, which may need to replicate
     * entries to a quorum of bookies to ensure data safety.
     *
     * <p>Normal client should never call this method.
     */
    void asyncRecoveryAddEntry(final byte[] data, final int offset, final int length,
                               final AddCallback cb, final Object ctx) {
        PendingAddOp op = PendingAddOp.create(this, clientCtx, getCurrentEnsemble(),
                                              Unpooled.wrappedBuffer(data, offset, length),
                                              writeFlags, cb, ctx)
                .enableRecoveryAdd();
        doAsyncAddEntry(op);
    }

    private boolean isWritesetWritable(DistributionSchedule.WriteSet writeSet,
                                       long key, int allowedNonWritableCount) {
        if (allowedNonWritableCount < 0) {
            allowedNonWritableCount = 0;
        }

        final int sz = writeSet.size();
        final int requiredWritable = sz - allowedNonWritableCount;

        int nonWritableCount = 0;
        List<BookieSocketAddress> currentEnsemble = getCurrentEnsemble();
        for (int i = 0; i < sz; i++) {
            if (!clientCtx.getBookieClient().isWritable(currentEnsemble.get(i), key)) {
                nonWritableCount++;
                if (nonWritableCount >= allowedNonWritableCount) {
                    return false;
                }
            } else {
                final int knownWritable = i - nonWritableCount;
                if (knownWritable >= requiredWritable) {
                    return true;
                }
            }
        }
        return true;
    }

    protected boolean waitForWritable(DistributionSchedule.WriteSet writeSet, long key,
                                    int allowedNonWritableCount, long durationMs) {
        if (durationMs < 0) {
            return true;
        }

        final long startTime = MathUtils.nowInNano();
        boolean success = isWritesetWritable(writeSet, key, allowedNonWritableCount);

        if (!success && durationMs > 0) {
            int backoff = 1;
            final int maxBackoff = 4;
            final long deadline = startTime + TimeUnit.MILLISECONDS.toNanos(durationMs);

            while (!isWritesetWritable(writeSet, key, allowedNonWritableCount)) {
                if (MathUtils.nowInNano() < deadline) {
                    long maxSleep = MathUtils.elapsedMSec(startTime);
                    if (maxSleep < 0) {
                        maxSleep = 1;
                    }
                    long sleepMs = Math.min(backoff, maxSleep);

                    try {
                        TimeUnit.MILLISECONDS.sleep(sleepMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        success = isWritesetWritable(writeSet, key, allowedNonWritableCount);
                        break;
                    }
                    if (backoff <= maxBackoff) {
                        backoff++;
                    }
                } else {
                    success = false;
                    break;
                }
            }
            if (backoff > 1) {
                LOG.info("Spent {} ms waiting for {} writable channels",
                        MathUtils.elapsedMSec(startTime),
                        writeSet.size() - allowedNonWritableCount);
            }
        }

        if (success) {
            clientChannelWriteWaitStats.registerSuccessfulEvent(
                    MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        } else {
            clientChannelWriteWaitStats.registerFailedEvent(
                    MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        }
        return success;
    }

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
                long entryId = ++lastAddPushed;
                long currentLedgerLength = addToLength(op.payload.readableBytes());
                op.setEntryId(entryId);
                op.setLedgerLength(currentLedgerLength);
                pendingAddOps.add(op);
            } else {
                wasClosed = true;
            }
        }

        if (wasClosed) {
            // make sure the callback is triggered in main worker pool
            try {
                clientCtx.getMainWorkerPool().executeOrdered(ledgerId, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        LOG.warn("Attempt to add to closed ledger: {}", ledgerId);
                        op.cb.addCompleteWithLatency(BKException.Code.LedgerClosedException,
                                LedgerHandle.this, INVALID_ENTRY_ID, 0, op.ctx);
                    }

                    @Override
                    public String toString() {
                        return String.format("AsyncAddEntryToClosedLedger(lid=%d)", ledgerId);
                    }
                });
            } catch (RejectedExecutionException e) {
                op.cb.addCompleteWithLatency(BookKeeper.getReturnRc(clientCtx.getBookieClient(),
                                                                    BKException.Code.InterruptedException),
                        LedgerHandle.this, INVALID_ENTRY_ID, 0, op.ctx);
            }
            return;
        }

        DistributionSchedule.WriteSet ws = distributionSchedule.getWriteSet(op.getEntryId());
        try {
            if (!waitForWritable(ws, op.getEntryId(), 0, clientCtx.getConf().waitForWriteSetMs)) {
                op.allowFailFastOnUnwritableChannel();
            }
        } finally {
            ws.recycle();
        }

        try {
            clientCtx.getMainWorkerPool().executeOrdered(ledgerId, op);
        } catch (RejectedExecutionException e) {
            op.cb.addCompleteWithLatency(
                    BookKeeper.getReturnRc(clientCtx.getBookieClient(), BKException.Code.InterruptedException),
                    LedgerHandle.this, INVALID_ENTRY_ID, 0, op.ctx);
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
            LedgerMetadata metadata = getLedgerMetadata();
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
                        updateLastConfirmed(data.getLastAddConfirmed(), data.getLength());
                        cb.readLastConfirmedComplete(rc, data.getLastAddConfirmed(), ctx);
                    } else {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            };

        new ReadLastConfirmedOp(clientCtx.getBookieClient(),
                                distributionSchedule,
                                macManager,
                                ledgerId,
                                getCurrentEnsemble(),
                                ledgerKey,
                                innercb).initiate();
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
            LedgerMetadata metadata = getLedgerMetadata();
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
                    updateLastConfirmed(data.getLastAddConfirmed(), data.getLength());
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, data.getLastAddConfirmed(), ctx);
                    }
                } else {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            }
        };
        new TryReadLastConfirmedOp(this, clientCtx.getBookieClient(), getCurrentEnsemble(),
                                   innercb, getLastAddConfirmed()).initiate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        FutureReadLastConfirmed result = new FutureReadLastConfirmed();
        asyncTryReadLastConfirmed(result, null);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        FutureReadLastConfirmed result = new FutureReadLastConfirmed();
        asyncReadLastConfirmed(result, null);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
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
     * <p>The callback will return the latest last add confirmed and next entry if it is available within timeout
     * period <i>timeOutInMillis</i>.
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
            LedgerMetadata metadata = getLedgerMetadata();
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
        new ReadLastConfirmedAndEntryOp(this, clientCtx, getCurrentEnsemble(), innercb, entryId - 1, timeOutInMillis)
            .parallelRead(parallel)
            .initiate();
    }

    /**
     * Context objects for synchronous call to read last confirmed.
     */
    static class LastConfirmedCtx {
        static final long ENTRY_ID_PENDING = -10;
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
        if (ctx.getRC() != BKException.Code.OK) {
            throw BKException.create(ctx.getRC());
        }
        return ctx.getlastConfirmed();
    }

    /**
     * Obtains asynchronously the explicit last add confirmed from a quorum of
     * bookies. This call obtains Explicit LAC value and piggy-backed LAC value (just like
     * {@link #asyncReadLastConfirmed(ReadLastConfirmedCallback, Object)}) from each
     * bookie in the ensemble and returns the maximum.
     * If in the write LedgerHandle, explicitLAC feature is not enabled then this call behavior
     * will be similar to {@link #asyncReadLastConfirmed(ReadLastConfirmedCallback, Object)}.
     * If the read explicit lastaddconfirmed is greater than getLastAddConfirmed, then it updates the
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
            LedgerMetadata metadata = getLedgerMetadata();
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
        new PendingReadLacOp(this, clientCtx.getBookieClient(), getCurrentEnsemble(), innercb).initiate();
    }

    /*
     * Obtains synchronously the explicit last add confirmed from a quorum of
     * bookies. This call obtains Explicit LAC value and piggy-backed LAC value (just like
     * {@Link #readLastAddConfirmed()) from each bookie in the ensemble and returns the maximum.
     * If in the write LedgerHandle, explicitLAC feature is not enabled then this call behavior
     * will be similar to {@Link #readLastAddConfirmed()}.
     * If the read explicit lastaddconfirmed is greater than getLastAddConfirmed, then it updates the
     * lastAddConfirmed of this ledgerhandle. If the ledger has been closed, it
     * returns the value of the last add confirmed from the metadata.
     *
     * @see #getLastAddConfirmed()
     *
     * @return The entry id of the explicit last confirmed write or
     *         {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID} if no entry has been
     *         confirmed.
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
        if (getLedgerMetadata().getState() == LedgerMetadata.State.IN_RECOVERY) {
            // we should not close ledger if ledger is recovery mode
            // otherwise we may lose entry.
            errorOutPendingAdds(rc);
            return;
        }
        LOG.error("Closing ledger {} due to {}", ledgerId, BKException.codeLogger(rc));
        asyncCloseInternal(NoopCloseCallback.instance, null, rc);
    }

    private void monitorPendingAddOps() {
        int timedOut = 0;
        for (PendingAddOp op : pendingAddOps) {
            if (op.maybeTimeout()) {
                timedOut++;
            }
        }
        if (timedOut > 0) {
            LOG.info("Timed out {} add ops", timedOut);
        }
    }

    void errorOutPendingAdds(int rc) {
        errorOutPendingAdds(rc, drainPendingAddsAndAdjustLength());
    }

    synchronized List<PendingAddOp> drainPendingAddsAndAdjustLength() {
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
               && !changingEnsemble) {
            if (!pendingAddOp.completed) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("pending add not completed: {}", pendingAddOp);
                }
                return;
            }
            // Check if it is the next entry in the sequence.
            if (pendingAddOp.entryId != 0 && pendingAddOp.entryId != pendingAddsSequenceHead + 1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Head of the queue entryId: {} is not the expected value: {}", pendingAddOp.entryId,
                               pendingAddsSequenceHead + 1);
                }
                return;
            }

            pendingAddOps.remove();
            explicitLacFlushPolicy.updatePiggyBackedLac(lastAddConfirmed);
            pendingAddsSequenceHead = pendingAddOp.entryId;
            if (!writeFlags.contains(WriteFlag.DEFERRED_SYNC)) {
                this.lastAddConfirmed = pendingAddsSequenceHead;
            }

            pendingAddOp.submitCallback(BKException.Code.OK);
        }

    }

    @VisibleForTesting
    boolean hasDelayedWriteFailedBookies() {
        return !delayedWriteFailedBookies.isEmpty();
    }

    void notifyWriteFailed(int index, BookieSocketAddress addr) {
        synchronized (metadataLock) {
            delayedWriteFailedBookies.put(index, addr);
        }
    }

    void maybeHandleDelayedWriteBookieFailure() {
        synchronized (metadataLock) {
            if (delayedWriteFailedBookies.isEmpty()) {
                return;
            }
            Map<Integer, BookieSocketAddress> toReplace = new HashMap<>(delayedWriteFailedBookies);
            delayedWriteFailedBookies.clear();

            // Original intent of this change is to do a best-effort ensemble change.
            // But this is not possible until the local metadata is completely immutable.
            // Until the feature "Make LedgerMetadata Immutable #610" Is complete we will use
            // handleBookieFailure() to handle delayed writes as regular bookie failures.
            handleBookieFailure(toReplace);
        }
    }

    void handleBookieFailure(final Map<Integer, BookieSocketAddress> failedBookies) {
        if (clientCtx.getConf().disableEnsembleChangeFeature.isAvailable()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ensemble change is disabled. Retry sending to failed bookies {} for ledger {}.",
                    failedBookies, ledgerId);
            }
            unsetSuccessAndSendWriteRequest(getCurrentEnsemble(), failedBookies.keySet());
            return;
        }

        if (writeFlags.contains(WriteFlag.DEFERRED_SYNC)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot perform ensemble change with write flags {}. "
                        + "Failed bookies {} for ledger {}.",
                    writeFlags, failedBookies, ledgerId);
            }
            handleUnrecoverableErrorDuringAdd(WriteException);
            return;
        }


        boolean triggerLoop = false;
        Map<Integer, BookieSocketAddress> toReplace = null;
        List<BookieSocketAddress> origEnsemble = null;
        synchronized (metadataLock) {
            if (changingEnsemble) {
                delayedWriteFailedBookies.putAll(failedBookies);
            } else {
                changingEnsemble = true;
                triggerLoop = true;

                toReplace = new HashMap<>(delayedWriteFailedBookies);
                delayedWriteFailedBookies.clear();
                toReplace.putAll(failedBookies);

                origEnsemble = getCurrentEnsemble();
            }
        }
        if (triggerLoop) {
            ensembleChangeLoop(origEnsemble, toReplace);
        }
    }

    void ensembleChangeLoop(List<BookieSocketAddress> origEnsemble, Map<Integer, BookieSocketAddress> failedBookies) {
        int ensembleChangeId = numEnsembleChanges.incrementAndGet();
        String logContext = String.format("[EnsembleChange(ledger:%d, change-id:%010d)]", ledgerId, ensembleChangeId);

        // when the ensemble changes are too frequent, close handle
        if (ensembleChangeId > clientCtx.getConf().maxAllowedEnsembleChanges) {
            LOG.info("{} reaches max allowed ensemble change number {}",
                     logContext, clientCtx.getConf().maxAllowedEnsembleChanges);
            handleUnrecoverableErrorDuringAdd(WriteException);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} Replacing {} in {}", logContext, failedBookies, origEnsemble);
        }

        AtomicInteger attempts = new AtomicInteger(0);
        new MetadataUpdateLoop(
                clientCtx.getLedgerManager(), getId(),
                this::getVersionedLedgerMetadata,
                (metadata) -> metadata.getState() == LedgerMetadata.State.OPEN
                        && failedBookies.entrySet().stream().anyMatch(
                                e -> LedgerMetadataUtils.getLastEnsembleValue(metadata)
                                             .get(e.getKey()).equals(e.getValue())),
                (metadata) -> {
                    attempts.incrementAndGet();

                    List<BookieSocketAddress> currentEnsemble = getCurrentEnsemble();
                    List<BookieSocketAddress> newEnsemble = EnsembleUtils.replaceBookiesInEnsemble(
                            clientCtx.getBookieWatcher(), metadata, currentEnsemble, failedBookies, logContext);
                    Long lastEnsembleKey = LedgerMetadataUtils.getLastEnsembleKey(metadata);
                    LedgerMetadataBuilder builder = LedgerMetadataBuilder.from(metadata);
                    long newEnsembleStartEntry = getLastAddConfirmed() + 1;
                    checkState(lastEnsembleKey <= newEnsembleStartEntry,
                               "New ensemble must either replace the last ensemble, or add a new one");
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{}[attempt:{}] changing ensemble from: {} to: {} starting at entry: {}",
                                  logContext, attempts.get(), currentEnsemble, newEnsemble, newEnsembleStartEntry);
                    }

                    if (lastEnsembleKey.equals(newEnsembleStartEntry)) {
                        return builder.replaceEnsembleEntry(newEnsembleStartEntry, newEnsemble).build();
                    } else {
                        return builder.newEnsembleEntry(newEnsembleStartEntry, newEnsemble).build();
                    }
                },
                this::setLedgerMetadata)
            .run().whenCompleteAsync((metadata, ex) -> {
                    if (ex != null) {
                        LOG.warn("{}[attempt:{}] Exception changing ensemble", logContext, attempts.get(), ex);
                        handleUnrecoverableErrorDuringAdd(BKException.getExceptionCode(ex, WriteException));
                    } else if (metadata.getValue().isClosed()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("{}[attempt:{}] Metadata closed during attempt to replace bookie."
                                      + " Another client must have recovered the ledger.", logContext, attempts.get());
                        }
                        handleUnrecoverableErrorDuringAdd(BKException.Code.LedgerClosedException);
                    } else if (metadata.getValue().getState() == LedgerMetadata.State.IN_RECOVERY) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("{}[attempt:{}] Metadata marked as in-recovery during attempt to replace bookie."
                                      + " Another client must be recovering the ledger.", logContext, attempts.get());
                        }

                        handleUnrecoverableErrorDuringAdd(BKException.Code.LedgerFencedException);
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("{}[attempt:{}] Success updating metadata.", logContext, attempts.get());
                        }

                        List<BookieSocketAddress> newEnsemble = null;
                        Set<Integer> replaced = null;
                        synchronized (metadataLock) {
                            if (!delayedWriteFailedBookies.isEmpty()) {
                                Map<Integer, BookieSocketAddress> toReplace = new HashMap<>(delayedWriteFailedBookies);
                                delayedWriteFailedBookies.clear();

                                ensembleChangeLoop(origEnsemble, toReplace);
                            } else {
                                newEnsemble = getCurrentEnsemble();
                                replaced = EnsembleUtils.diffEnsemble(origEnsemble, newEnsemble);
                                LOG.info("New Ensemble: {} for ledger: {}", newEnsemble, ledgerId);

                                changingEnsemble = false;
                            }
                        }
                        if (newEnsemble != null) { // unsetSuccess outside of lock
                            unsetSuccessAndSendWriteRequest(newEnsemble, replaced);
                        }
                    }
            }, clientCtx.getMainWorkerPool().chooseThread(ledgerId));
    }

    void unsetSuccessAndSendWriteRequest(List<BookieSocketAddress> ensemble, final Set<Integer> bookies) {
        for (PendingAddOp pendingAddOp : pendingAddOps) {
            for (Integer bookieIndex: bookies) {
                pendingAddOp.unsetSuccessAndSendWriteRequest(ensemble, bookieIndex);
            }
        }
    }

    void registerOperationFailureOnBookie(BookieSocketAddress bookie, long entryId) {
        if (clientCtx.getConf().enableBookieFailureTracking) {
            bookieFailureHistory.put(bookie, entryId);
        }
    }

    static class NoopCloseCallback implements CloseCallback {
        static NoopCloseCallback instance = new NoopCloseCallback();

        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            if (rc != BKException.Code.OK) {
                LOG.warn("Close failed: {}", BKException.codeLogger(rc));
            }
            // noop
        }
    }

    /**
     * Get the current ensemble from the ensemble list. The current ensemble
     * is the last ensemble in the list. The ledger handle uses this ensemble when
     * triggering operations which work on the end of the ledger, such as adding new
     * entries or reading the last add confirmed.
     *
     * <p>This method is also used by ReadOnlyLedgerHandle during recovery, and when
     * tailing a ledger.
     *
     * <p>Generally, this method should only be called by LedgerHandle and not by the
     * operations themselves, to avoid adding more dependencies between the classes.
     * There are too many already.
     */
    List<BookieSocketAddress> getCurrentEnsemble() {
        // Getting current ensemble from the metadata is only a temporary
        // thing until metadata is immutable. At that point, current ensemble
        // becomes a property of the LedgerHandle itself.
        return LedgerMetadataUtils.getCurrentEnsemble(versionedMetadata.getValue());
    }

    /**
     * Return a {@link WriteSet} suitable for reading a particular entry.
     * This will include all bookies that are cotna
     */
    WriteSet getWriteSetForReadOperation(long entryId) {
        if (stickyBookieIndex != STICKY_READ_BOOKIE_INDEX_UNSET) {
            // When sticky reads are enabled we want to make sure to take
            // advantage of read-ahead (or, anyway, from efficiencies in
            // reading sequential data from disk through the page cache).
            // For this, all the entries that a given bookie prefetches,
            // should read from that bookie.
            // For example, with e=2, w=2, a=2 we would have
            // B-1 B-2
            // e-0 X X
            // e-1 X X
            // e-2 X X
            //
            // In this case we want all the requests to be issued to B-1 (by
            // preference), so that cache hits will be maximized.
            //
            // We can only enable sticky reads if the ensemble==writeQuorum
            // otherwise the same bookie will not have all the entries
            // stored
            return distributionSchedule.getWriteSet(stickyBookieIndex);
        } else {
            return distributionSchedule.getWriteSet(entryId);
        }
    }
}
