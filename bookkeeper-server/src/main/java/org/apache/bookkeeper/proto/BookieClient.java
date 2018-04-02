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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.util.SafeRunnable.safeRun;

import com.google.common.collect.Lists;
import com.google.protobuf.ExtensionRegistry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
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
import org.apache.bookkeeper.util.ByteBufList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the client-side part of the BookKeeper protocol.
 *
 */
public class BookieClient implements PerChannelBookieClientFactory {
    static final Logger LOG = LoggerFactory.getLogger(BookieClient.class);

    // This is global state that should be across all BookieClients
    AtomicLong totalBytesOutstanding = new AtomicLong();

    OrderedExecutor executor;
    ScheduledExecutorService scheduler;
    ScheduledFuture<?> timeoutFuture;

    EventLoopGroup eventLoopGroup;
    final ConcurrentHashMap<BookieSocketAddress, PerChannelBookieClientPool> channels =
            new ConcurrentHashMap<BookieSocketAddress, PerChannelBookieClientPool>();

    private final ClientAuthProvider.Factory authProviderFactory;
    private final ExtensionRegistry registry;

    private final ClientConfiguration conf;
    private volatile boolean closed;
    private final ReentrantReadWriteLock closeLock;
    private final StatsLogger statsLogger;
    private final int numConnectionsPerBookie;

    private final long bookieErrorThresholdPerInterval;

    public BookieClient(ClientConfiguration conf, EventLoopGroup eventLoopGroup,
                        OrderedExecutor executor, ScheduledExecutorService scheduler,
                        StatsLogger statsLogger) throws IOException {
        this.conf = conf;
        this.eventLoopGroup = eventLoopGroup;
        this.executor = executor;
        this.closed = false;
        this.closeLock = new ReentrantReadWriteLock();

        this.registry = ExtensionRegistry.newInstance();
        this.authProviderFactory = AuthProviderFactoryFactory.newClientAuthProviderFactory(conf);

        this.statsLogger = statsLogger;
        this.numConnectionsPerBookie = conf.getNumChannelsPerBookie();
        this.bookieErrorThresholdPerInterval = conf.getBookieErrorThresholdPerInterval();

        this.scheduler = scheduler;
        if (conf.getAddEntryTimeout() > 0 || conf.getReadEntryTimeout() > 0) {
            SafeRunnable monitor = safeRun(() -> {
                monitorPendingOperations();
            });
            this.timeoutFuture = this.scheduler.scheduleAtFixedRate(monitor,
                                                                    conf.getTimeoutMonitorIntervalSec(),
                                                                    conf.getTimeoutMonitorIntervalSec(),
                                                                    TimeUnit.SECONDS);
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

    public List<BookieSocketAddress> getFaultyBookies() {
        List<BookieSocketAddress> faultyBookies = Lists.newArrayList();
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
    public PerChannelBookieClient create(BookieSocketAddress address, PerChannelBookieClientPool pcbcPool,
            SecurityHandlerFactory shFactory) throws SecurityException {
        return new PerChannelBookieClient(conf, executor, eventLoopGroup, address, statsLogger,
                                          authProviderFactory, registry, pcbcPool, shFactory);
    }

    public PerChannelBookieClientPool lookupClient(BookieSocketAddress addr) {
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
                    clientPool.intialize();
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

    public void writeLac(final BookieSocketAddress addr, final long ledgerId, final byte[] masterKey,
            final long lac, final ByteBufList toSend, final WriteLacCallback cb, final Object ctx) {
        final PerChannelBookieClientPool client = lookupClient(addr);
        if (client == null) {
            cb.writeLacComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                              ledgerId, addr, ctx);
            return;
        }

        toSend.retain();
        client.obtain((rc, pcbc) -> {
            if (rc != BKException.Code.OK) {
                try {
                    executor.executeOrdered(ledgerId, safeRun(() -> {
                        cb.writeLacComplete(rc, ledgerId, addr, ctx);
                    }));
                } catch (RejectedExecutionException re) {
                    cb.writeLacComplete(getRc(BKException.Code.InterruptedException), ledgerId, addr, ctx);
                }
            } else {
                pcbc.writeLac(ledgerId, masterKey, lac, toSend, cb, ctx);
            }

            toSend.release();
        }, ledgerId);
    }

    private void completeAdd(final int rc,
                             final long ledgerId,
                             final long entryId,
                             final BookieSocketAddress addr,
                             final WriteCallback cb,
                             final Object ctx) {
        try {
            executor.executeOrdered(ledgerId, new SafeRunnable() {
                @Override
                public void safeRun() {
                    cb.writeComplete(rc, ledgerId, entryId, addr, ctx);
                }
                @Override
                public String toString() {
                    return String.format("CompleteWrite(ledgerId=%d, entryId=%d, addr=%s)", ledgerId, entryId, addr);
                }
            });
        } catch (RejectedExecutionException ree) {
            cb.writeComplete(getRc(BKException.Code.InterruptedException), ledgerId, entryId, addr, ctx);
        }
    }

    public void addEntry(final BookieSocketAddress addr,
                         final long ledgerId,
                         final byte[] masterKey,
                         final long entryId,
                         final ByteBufList toSend,
                         final WriteCallback cb,
                         final Object ctx,
                         final int options) {
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
                              ctx, cb, options, masterKey),
                      ledgerId);
    }

    private void completeRead(final int rc,
                              final long ledgerId,
                              final long entryId,
                              final ByteBuf entry,
                              final ReadEntryCallback cb,
                              final Object ctx) {
        try {
            executor.executeOrdered(ledgerId, new SafeRunnable() {
                @Override
                public void safeRun() {
                    cb.readEntryComplete(rc, ledgerId, entryId, entry, ctx);
                }
            });
        } catch (RejectedExecutionException ree) {
            cb.readEntryComplete(getRc(BKException.Code.InterruptedException),
                                 ledgerId, entryId, entry, ctx);
        }
    }

    private static class ChannelReadyForAddEntryCallback
        implements GenericCallback<PerChannelBookieClient> {
        private final Handle<ChannelReadyForAddEntryCallback> recyclerHandle;

        private BookieClient bookieClient;
        private ByteBufList toSend;
        private long ledgerId;
        private long entryId;
        private BookieSocketAddress addr;
        private Object ctx;
        private WriteCallback cb;
        private int options;
        private byte[] masterKey;

        static ChannelReadyForAddEntryCallback create(
                BookieClient bookieClient, ByteBufList toSend, long ledgerId,
                long entryId, BookieSocketAddress addr, Object ctx,
                WriteCallback cb, int options, byte[] masterKey) {
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
            return callback;
        }

        @Override
        public void operationComplete(final int rc,
                                      PerChannelBookieClient pcbc) {
            if (rc != BKException.Code.OK) {
                bookieClient.completeAdd(rc, ledgerId, entryId, addr, cb, ctx);
            } else {
                pcbc.addEntry(ledgerId, masterKey, entryId,
                              toSend, cb, ctx, options);
            }

            toSend.release();
            recycle();
        }

        private ChannelReadyForAddEntryCallback(
                Handle<ChannelReadyForAddEntryCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<ChannelReadyForAddEntryCallback> RECYCLER =
            new Recycler<ChannelReadyForAddEntryCallback>() {
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

            recyclerHandle.recycle(this);
        }
    }

    public void readLac(final BookieSocketAddress addr, final long ledgerId, final ReadLacCallback cb,
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
                    executor.executeOrdered(ledgerId, safeRun(() -> {
                        cb.readLacComplete(rc, ledgerId, null, null, ctx);
                    }));
                } catch (RejectedExecutionException re) {
                    cb.readLacComplete(getRc(BKException.Code.InterruptedException),
                            ledgerId, null, null, ctx);
                }
            } else {
                pcbc.readLac(ledgerId, cb, ctx);
            }
        }, ledgerId);
    }

    public void readEntry(BookieSocketAddress addr, long ledgerId, long entryId,
                          ReadEntryCallback cb, Object ctx, int flags) {
        readEntry(addr, ledgerId, entryId, cb, ctx, flags, null);
    }

    public void readEntry(final BookieSocketAddress addr, final long ledgerId, final long entryId,
                          final ReadEntryCallback cb, final Object ctx, int flags, byte[] masterKey) {
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
                pcbc.readEntry(ledgerId, entryId, cb, ctx, flags, masterKey);
            }
        }, ledgerId);
    }


    public void readEntryWaitForLACUpdate(final BookieSocketAddress addr,
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

    public void getBookieInfo(final BookieSocketAddress addr, final long requested, final GetBookieInfoCallback cb,
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
                    executor.submit(safeRun(() -> {
                        cb.getBookieInfoComplete(rc, new BookieInfo(), ctx);
                    }));
                } catch (RejectedExecutionException re) {
                    cb.getBookieInfoComplete(getRc(BKException.Code.InterruptedException),
                            new BookieInfo(), ctx);
                }
            } else {
                pcbc.getBookieInfo(requested, cb, ctx);
            }
        }, requested);
    }

    private void monitorPendingOperations() {
        for (PerChannelBookieClientPool clientPool : channels.values()) {
            clientPool.checkTimeoutOnPendingOperations();
        }
    }

    public boolean isClosed() {
        return closed;
    }

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

            public void writeComplete(int rc, long ledger, long entry, BookieSocketAddress addr, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                if (rc != 0) {
                    System.out.println("rc = " + rc + " for " + entry + "@" + ledger);
                }
            }
        };
        Counter counter = new Counter();
        byte hello[] = "hello".getBytes(UTF_8);
        long ledger = Long.parseLong(args[2]);
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        OrderedExecutor executor = OrderedExecutor.newBuilder()
                .name("BookieClientWorker")
                .numThreads(1)
                .build();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));
        BookieClient bc = new BookieClient(new ClientConfiguration(), eventLoopGroup, executor,
                                           scheduler, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr = new BookieSocketAddress(args[0], Integer.parseInt(args[1]));

        for (int i = 0; i < 100000; i++) {
            counter.inc();
            bc.addEntry(addr, ledger, new byte[0], i, ByteBufList.get(Unpooled.wrappedBuffer(hello)), cb, counter, 0);
        }
        counter.wait(0);
        System.out.println("Total = " + counter.total());
        scheduler.shutdown();
        eventLoopGroup.shutdownGracefully();
        executor.shutdown();
    }
}
