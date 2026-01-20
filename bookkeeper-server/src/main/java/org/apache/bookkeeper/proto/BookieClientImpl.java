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
package org.apache.bookkeeper.proto;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ExtensionRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.FutureGetListOfEntriesOfLedger;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.ByteBufList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the client-side part of the BookKeeper protocol.
 *
 */
public class BookieClientImpl implements BookieClient, PerChannelBookieClientFactory {
    static final Logger LOG = LoggerFactory.getLogger(BookieClientImpl.class);

    private final OrderedExecutor executor;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> timeoutFuture;

    private final EventLoopGroup eventLoopGroup;
    private final ByteBufAllocator allocator;
    final ConcurrentHashMap<BookieId, PerChannelBookieClientPool> channels =
            new ConcurrentHashMap<BookieId, PerChannelBookieClientPool>();

    private final ClientAuthProvider.Factory authProviderFactory;
    private final ExtensionRegistry registry;

    private final ClientConfiguration conf;
    private final ClientConfiguration v3Conf;
    private final boolean useV3Enforced;
    private volatile boolean closed;
    private final ReentrantReadWriteLock closeLock;
    private final StatsLogger statsLogger;
    private final int numConnectionsPerBookie;
    private final BookieAddressResolver bookieAddressResolver;

    private final long bookieErrorThresholdPerInterval;

    public BookieClientImpl(ClientConfiguration conf, EventLoopGroup eventLoopGroup,
                            ByteBufAllocator allocator,
                            OrderedExecutor executor, ScheduledExecutorService scheduler,
                            StatsLogger statsLogger, BookieAddressResolver bookieAddressResolver) throws IOException {
        this.conf = conf;
        this.v3Conf = new ClientConfiguration(conf);
        this.v3Conf.setUseV2WireProtocol(false);
        this.useV3Enforced = conf.getUseV2WireProtocol();
        this.eventLoopGroup = eventLoopGroup;
        this.allocator = allocator;
        this.executor = executor;
        this.closed = false;
        this.closeLock = new ReentrantReadWriteLock();
        this.bookieAddressResolver = bookieAddressResolver;
        this.registry = ExtensionRegistry.newInstance();
        this.authProviderFactory = AuthProviderFactoryFactory.newClientAuthProviderFactory(conf);

        this.statsLogger = statsLogger;
        this.numConnectionsPerBookie = conf.getNumChannelsPerBookie();
        this.bookieErrorThresholdPerInterval = conf.getBookieErrorThresholdPerInterval();

        this.scheduler = scheduler;
        if (conf.getAddEntryTimeout() > 0 || conf.getReadEntryTimeout() > 0) {
            this.timeoutFuture = this.scheduler.scheduleAtFixedRate(
                    () -> monitorPendingOperations(),
                    conf.getTimeoutMonitorIntervalSec(),
                    conf.getTimeoutMonitorIntervalSec(),
                    TimeUnit.SECONDS);
        } else {
            this.timeoutFuture = null;
        }
    }

    private int getRc(int rc) {
        if (BKException.Code.OK == rc) {
            return rc;
        } else {
            if (closed) {
                return BKException.Code.ClientClosedException;
            } else {
                return rc;
            }
        }
    }

    @Override
    public List<BookieId> getFaultyBookies() {
        List<BookieId> faultyBookies = Lists.newArrayList();
        for (PerChannelBookieClientPool channelPool : channels.values()) {
            if (channelPool instanceof DefaultPerChannelBookieClientPool) {
                DefaultPerChannelBookieClientPool pool = (DefaultPerChannelBookieClientPool) channelPool;
                if (pool.errorCounter.getAndSet(0) >= bookieErrorThresholdPerInterval) {
                    faultyBookies.add(pool.address);
                }
            }
        }
        return faultyBookies;
    }

    @Override
    public boolean isWritable(BookieId address, long key) {
        final PerChannelBookieClientPool pcbcPool = lookupClient(address);
        // if null, let the write initiate connect of fail with whatever error it produces
        return pcbcPool == null || pcbcPool.isWritable(key);
    }

    @Override
    public long getNumPendingRequests(BookieId address, long ledgerId) {
        PerChannelBookieClientPool pcbcPool = lookupClient(address);
        if (pcbcPool == null) {
            return 0;
        } else if (pcbcPool.isWritable(ledgerId)) {
            return pcbcPool.getNumPendingCompletionRequests();
        } else {
            return pcbcPool.getNumPendingCompletionRequests() | PENDINGREQ_NOTWRITABLE_MASK;
        }
    }

    @Override
    public PerChannelBookieClient create(BookieId address, PerChannelBookieClientPool pcbcPool,
            SecurityHandlerFactory shFactory, boolean forceUseV3) throws SecurityException {
        StatsLogger statsLoggerForPCBC = statsLogger;
        if (conf.getLimitStatsLogging()) {
            statsLoggerForPCBC = NullStatsLogger.INSTANCE;
        }
        ClientConfiguration clientConfiguration = conf;
        if (forceUseV3) {
            clientConfiguration = v3Conf;
        }
        return new PerChannelBookieClient(clientConfiguration, executor, eventLoopGroup, allocator, address,
                                   statsLoggerForPCBC, authProviderFactory, registry, pcbcPool,
                                   shFactory, bookieAddressResolver);
    }

    public PerChannelBookieClientPool lookupClient(BookieId addr) {
        PerChannelBookieClientPool clientPool = channels.get(addr);
        if (null == clientPool) {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    return null;
                }
                PerChannelBookieClientPool newClientPool =
                    new DefaultPerChannelBookieClientPool(conf, this, addr, numConnectionsPerBookie);
                PerChannelBookieClientPool oldClientPool = channels.putIfAbsent(addr, newClientPool);
                if (null == oldClientPool) {
                    clientPool = newClientPool;
                    // initialize the pool only after we put the pool into the map
                    clientPool.initialize();
                } else {
                    clientPool = oldClientPool;
                    newClientPool.close(false);
                }
            } catch (SecurityException e) {
                LOG.error("Security Exception in creating new default PCBC pool: ", e);
                return null;
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return clientPool;
    }

    @Override
    public void forceLedger(final BookieId addr, final long ledgerId,
            final ForceLedgerCallback cb, final Object ctx) {
        final PerChannelBookieClientPool client = lookupClient(addr);
        if (client == null) {
            cb.forceLedgerComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                              ledgerId, addr, ctx);
            return;
        }

        client.obtain((rc, pcbc) -> {
            if (rc != BKException.Code.OK) {
                try {
                    executor.executeOrdered(ledgerId,
                            () -> cb.forceLedgerComplete(rc, ledgerId, addr, ctx));
                } catch (RejectedExecutionException re) {
                    cb.forceLedgerComplete(getRc(BKException.Code.InterruptedException), ledgerId, addr, ctx);
                }
            } else {
                pcbc.forceLedger(ledgerId, cb, ctx);
            }
        }, ledgerId);
    }

    @Override
    public void writeLac(final BookieId addr, final long ledgerId, final byte[] masterKey,
            final long lac, final ByteBufList toSend, final WriteLacCallback cb, final Object ctx) {
        final PerChannelBookieClientPool client = lookupClient(addr);
        if (client == null) {
            cb.writeLacComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                              ledgerId, addr, ctx);
            return;
        }

        toSend.retain();
        client.obtain((rc, pcbc) -> {
            try {
                if (rc != BKException.Code.OK) {
                    try {
                        executor.executeOrdered(ledgerId,
                                () -> cb.writeLacComplete(rc, ledgerId, addr, ctx));
                    } catch (RejectedExecutionException re) {
                        cb.writeLacComplete(getRc(BKException.Code.InterruptedException), ledgerId, addr, ctx);
                    }
                } else {
                    pcbc.writeLac(ledgerId, masterKey, lac, toSend, cb, ctx);
                }
            } finally {
                ReferenceCountUtil.release(toSend);
            }
        }, ledgerId, useV3Enforced);
    }

    private void completeAdd(final int rc,
                             final long ledgerId,
                             final long entryId,
                             final BookieId addr,
                             final WriteCallback cb,
                             final Object ctx) {
        executor.executeOrdered(ledgerId, () -> cb.writeComplete(rc, ledgerId, entryId, addr, ctx));
    }

    @Override
    public void addEntry(final BookieId addr,
                         final long ledgerId,
                         final byte[] masterKey,
                         final long entryId,
                         final ReferenceCounted toSend,
                         final WriteCallback cb,
                         final Object ctx,
                         final int options,
                         final boolean allowFastFail,
                         final EnumSet<WriteFlag> writeFlags) {
        final PerChannelBookieClientPool client = lookupClient(addr);
        if (client == null) {
            completeAdd(getRc(BKException.Code.BookieHandleNotAvailableException),
                        ledgerId, entryId, addr, cb, ctx);
            return;
        }

        // Retain the buffer, since the connection could be obtained after
        // the PendingApp might have already failed
        toSend.retain();

        client.obtain(ChannelReadyForAddEntryCallback.create(
                              this, toSend, ledgerId, entryId, addr,
                                  ctx, cb, options, masterKey, allowFastFail, writeFlags),
                      ledgerId);
    }

    @Override
    public CompletableFuture<AvailabilityOfEntriesOfLedger> getListOfEntriesOfLedger(BookieId address,
            long ledgerId) {
        FutureGetListOfEntriesOfLedger futureResult = new FutureGetListOfEntriesOfLedger(ledgerId);
        final PerChannelBookieClientPool client = lookupClient(address);
        if (client == null) {
            futureResult.getListOfEntriesOfLedgerComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                    ledgerId, null);
            return futureResult;
        }
        client.obtain((rc, pcbc) -> {
            if (rc != BKException.Code.OK) {
                try {
                    executor.executeOrdered(ledgerId, () ->
                            futureResult.getListOfEntriesOfLedgerComplete(rc, ledgerId, null)
                    );
                } catch (RejectedExecutionException re) {
                    futureResult.getListOfEntriesOfLedgerComplete(getRc(BKException.Code.InterruptedException),
                            ledgerId, null);
                }
            } else {
                pcbc.getListOfEntriesOfLedger(ledgerId, futureResult);
            }
        }, ledgerId);
        return futureResult;
    }

    private void completeRead(final int rc,
                              final long ledgerId,
                              final long entryId,
                              final ByteBuf entry,
                              final ReadEntryCallback cb,
                              final Object ctx) {
        try {
            executor.executeOrdered(ledgerId, () -> cb.readEntryComplete(rc, ledgerId, entryId, entry, ctx));
        } catch (RejectedExecutionException ree) {
            cb.readEntryComplete(getRc(BKException.Code.InterruptedException),
                                 ledgerId, entryId, entry, ctx);
        }
    }

    private void completeBatchRead(final int rc,
            final long ledgerId,
            final long startEntryId,
            final ByteBufList bufList,
            final BatchedReadEntryCallback cb,
            final Object ctx) {
        try {
            executor.executeOrdered(ledgerId, () -> cb.readEntriesComplete(rc, ledgerId, startEntryId, bufList, ctx));
        } catch (RejectedExecutionException ree) {
            cb.readEntriesComplete(getRc(BKException.Code.InterruptedException),
                    ledgerId, startEntryId, bufList, ctx);
        }
    }

    // Without test, this class should be modifier with "private".
    @VisibleForTesting
    static class ChannelReadyForAddEntryCallback
        implements GenericCallback<PerChannelBookieClient> {
        private final Handle<ChannelReadyForAddEntryCallback> recyclerHandle;

        private BookieClientImpl bookieClient;
        private ReferenceCounted toSend;
        private long ledgerId;
        private long entryId;
        private BookieId addr;
        private Object ctx;
        // Without test, this class should be modifier with "private".
        @VisibleForTesting
        WriteCallback cb;
        private int options;
        private byte[] masterKey;
        private boolean allowFastFail;
        private EnumSet<WriteFlag> writeFlags;

        static ChannelReadyForAddEntryCallback create(
                BookieClientImpl bookieClient, ReferenceCounted toSend, long ledgerId,
                long entryId, BookieId addr, Object ctx,
                WriteCallback cb, int options, byte[] masterKey, boolean allowFastFail,
                EnumSet<WriteFlag> writeFlags) {
            ChannelReadyForAddEntryCallback callback = RECYCLER.get();
            callback.bookieClient = bookieClient;
            callback.toSend = toSend;
            callback.ledgerId = ledgerId;
            callback.entryId = entryId;
            callback.addr = addr;
            callback.ctx = ctx;
            callback.cb = cb;
            callback.options = options;
            callback.masterKey = masterKey;
            callback.allowFastFail = allowFastFail;
            callback.writeFlags = writeFlags;
            return callback;
        }

        @Override
        public void operationComplete(final int rc,
                                      PerChannelBookieClient pcbc) {
            if (rc != BKException.Code.OK) {
                bookieClient.executor.executeOrdered(ledgerId, () -> {
                    try {
                        bookieClient.completeAdd(rc, ledgerId, entryId, addr, cb, ctx);
                    } finally {
                        ReferenceCountUtil.release(toSend);
                    }
                    recycle();
                });
            } else {
                try {
                    pcbc.addEntry(ledgerId, masterKey, entryId,
                            toSend, cb, ctx, options, allowFastFail, writeFlags);
                } finally {
                    ReferenceCountUtil.release(toSend);
                }
                recycle();
            }
        }

        private ChannelReadyForAddEntryCallback(
                Handle<ChannelReadyForAddEntryCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<ChannelReadyForAddEntryCallback> RECYCLER =
            new Recycler<ChannelReadyForAddEntryCallback>() {
                    @Override
                    protected ChannelReadyForAddEntryCallback newObject(
                            Recycler.Handle<ChannelReadyForAddEntryCallback> recyclerHandle) {
                        return new ChannelReadyForAddEntryCallback(recyclerHandle);
                    }
                };

        public void recycle() {
            bookieClient = null;
            toSend = null;
            ledgerId = -1;
            entryId = -1;
            addr = null;
            ctx = null;
            cb = null;
            options = -1;
            masterKey = null;
            allowFastFail = false;
            writeFlags = null;
            recyclerHandle.recycle(this);
        }
    }

    @Override
    public void readLac(final BookieId addr, final long ledgerId, final ReadLacCallback cb,
            final Object ctx) {
        final PerChannelBookieClientPool client = lookupClient(addr);
        if (client == null) {
            cb.readLacComplete(getRc(BKException.Code.BookieHandleNotAvailableException), ledgerId, null, null,
                    ctx);
            return;
        }
        client.obtain((rc, pcbc) -> {
            if (rc != BKException.Code.OK) {
                try {
                    executor.executeOrdered(ledgerId,
                            () -> cb.readLacComplete(rc, ledgerId, null, null, ctx));
                } catch (RejectedExecutionException re) {
                    cb.readLacComplete(getRc(BKException.Code.InterruptedException),
                            ledgerId, null, null, ctx);
                }
            } else {
                pcbc.readLac(ledgerId, cb, ctx);
            }
        }, ledgerId, useV3Enforced);
    }

    @Override
    public void readEntry(BookieId addr, long ledgerId, long entryId,
                          ReadEntryCallback cb, Object ctx, int flags) {
        readEntry(addr, ledgerId, entryId, cb, ctx, flags, null);
    }

    @Override
    public void readEntry(final BookieId addr, final long ledgerId, final long entryId,
                          final ReadEntryCallback cb, final Object ctx, int flags, byte[] masterKey) {
        readEntry(addr, ledgerId, entryId, cb, ctx, flags, masterKey, false);
    }

    @Override
    public void readEntry(final BookieId addr, final long ledgerId, final long entryId,
                          final ReadEntryCallback cb, final Object ctx, int flags, byte[] masterKey,
                          final boolean allowFastFail) {
        final PerChannelBookieClientPool client = lookupClient(addr);
        if (client == null) {
            cb.readEntryComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                                 ledgerId, entryId, null, ctx);
            return;
        }

        client.obtain((rc, pcbc) -> {
            if (rc != BKException.Code.OK) {
                completeRead(rc, ledgerId, entryId, null, cb, ctx);
            } else {
                pcbc.readEntry(ledgerId, entryId, cb, ctx, flags, masterKey, allowFastFail);
            }
        }, ledgerId);
    }

    @Override
    public void batchReadEntries(final BookieId address, final long ledgerId, final long startEntryId,
            final int maxCount, final long maxSize, final BatchedReadEntryCallback cb, final Object ctx,
            final int flags, final byte[] masterKey, final boolean allowFastFail) {
        final PerChannelBookieClientPool client = lookupClient(address);
        if (client == null) {
            cb.readEntriesComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                    ledgerId, startEntryId, null, ctx);
            return;
        }

        client.obtain((rc, pcbc) -> {
            if (rc != BKException.Code.OK) {
                completeBatchRead(rc, ledgerId, startEntryId, null, cb, ctx);
            } else {
                pcbc.batchReadEntries(ledgerId, startEntryId, maxCount, maxSize, cb, ctx, flags, masterKey,
                        allowFastFail);
            }
        }, ledgerId);
    }

    @Override
    public void readEntryWaitForLACUpdate(final BookieId addr,
                                          final long ledgerId,
                                          final long entryId,
                                          final long previousLAC,
                                          final long timeOutInMillis,
                                          final boolean piggyBackEntry,
                                          final ReadEntryCallback cb,
                                          final Object ctx) {
        final PerChannelBookieClientPool client = lookupClient(addr);
        if (client == null) {
            completeRead(BKException.Code.BookieHandleNotAvailableException,
                    ledgerId, entryId, null, cb, ctx);
            return;
        }

        client.obtain((rc, pcbc) -> {
            if (rc != BKException.Code.OK) {
                completeRead(rc, ledgerId, entryId, null, cb, ctx);
            } else {
                pcbc.readEntryWaitForLACUpdate(ledgerId, entryId, previousLAC, timeOutInMillis, piggyBackEntry, cb,
                        ctx);
            }
        }, ledgerId);
    }

    @Override
    public void getBookieInfo(final BookieId addr, final long requested, final GetBookieInfoCallback cb,
            final Object ctx) {
        final PerChannelBookieClientPool client = lookupClient(addr);
        if (client == null) {
            cb.getBookieInfoComplete(getRc(BKException.Code.BookieHandleNotAvailableException), new BookieInfo(),
                    ctx);
            return;
        }
        client.obtain((rc, pcbc) -> {
            if (rc != BKException.Code.OK) {
                try {
                    executor.execute(() -> cb.getBookieInfoComplete(rc, new BookieInfo(), ctx));
                } catch (RejectedExecutionException re) {
                    cb.getBookieInfoComplete(getRc(BKException.Code.InterruptedException),
                            new BookieInfo(), ctx);
                }
            } else {
                pcbc.getBookieInfo(requested, cb, ctx);
            }
        }, requested, useV3Enforced);
    }

    private void monitorPendingOperations() {
        for (PerChannelBookieClientPool clientPool : channels.values()) {
            clientPool.checkTimeoutOnPendingOperations();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closeLock.writeLock().lock();
        try {
            closed = true;
            for (PerChannelBookieClientPool pool : channels.values()) {
                pool.close(true);
            }
            channels.clear();
            authProviderFactory.close();

            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
        } finally {
            closeLock.writeLock().unlock();
        }
    }

    private static class Counter {
        int i;
        int total;

        synchronized void inc() {
            i++;
            total++;
        }

        synchronized void dec() {
            i--;
            notifyAll();
        }

        synchronized void wait(int limit) throws InterruptedException {
            while (i > limit) {
                wait();
            }
        }

        synchronized int total() {
            return total;
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws NumberFormatException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws NumberFormatException, IOException, InterruptedException {
        if (args.length != 3) {
            System.err.println("USAGE: BookieClient bookieHost port ledger#");
            return;
        }
        WriteCallback cb = new WriteCallback() {

            @Override
            public void writeComplete(int rc, long ledger, long entry, BookieId addr, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                if (rc != 0) {
                    System.out.println("rc = " + rc + " for " + entry + "@" + ledger);
                }
            }
        };
        Counter counter = new Counter();
        byte[] hello = "hello".getBytes(UTF_8);
        long ledger = Long.parseLong(args[2]);
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        OrderedExecutor executor = OrderedExecutor.newBuilder()
                .name("BookieClientWorker")
                .numThreads(1)
                .build();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));
        BookieClientImpl bc = new BookieClientImpl(new ClientConfiguration(), eventLoopGroup,
                null, executor, scheduler, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        BookieId addr = new BookieSocketAddress(args[0], Integer.parseInt(args[1])).toBookieId();

        for (int i = 0; i < 100000; i++) {
            counter.inc();
            bc.addEntry(addr, ledger, new byte[0], i,
                    ByteBufList.get(Unpooled.wrappedBuffer(hello)), cb, counter, 0, false,
                    WriteFlag.NONE);
        }
        counter.wait(0);
        System.out.println("Total = " + counter.total());
        scheduler.shutdown();
        eventLoopGroup.shutdownGracefully();
        executor.shutdown();
    }
}
