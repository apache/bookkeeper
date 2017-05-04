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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ExtensionRegistry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

/**
 * Implements the client-side part of the BookKeeper protocol.
 *
 */
public class BookieClient implements PerChannelBookieClientFactory {
    static final Logger LOG = LoggerFactory.getLogger(BookieClient.class);

    // This is global state that should be across all BookieClients
    AtomicLong totalBytesOutstanding = new AtomicLong();

    OrderedSafeExecutor executor;
    EventLoopGroup eventLoopGroup;
    final ConcurrentHashMap<BookieSocketAddress, PerChannelBookieClientPool> channels =
            new ConcurrentHashMap<BookieSocketAddress, PerChannelBookieClientPool>();
    final HashedWheelTimer requestTimer;

    final private ClientAuthProvider.Factory authProviderFactory;
    final private ExtensionRegistry registry;

    private final ClientConfiguration conf;
    private volatile boolean closed;
    private final ReentrantReadWriteLock closeLock;
    private final StatsLogger statsLogger;
    private final int numConnectionsPerBookie;

    private final long bookieErrorThresholdPerInterval;

    public BookieClient(ClientConfiguration conf, EventLoopGroup eventLoopGroup,
            OrderedSafeExecutor executor) throws IOException {
        this(conf, eventLoopGroup, executor, NullStatsLogger.INSTANCE);
    }

    public BookieClient(ClientConfiguration conf, EventLoopGroup eventLoopGroup,
                        OrderedSafeExecutor executor, StatsLogger statsLogger) throws IOException {
        this.conf = conf;
        this.eventLoopGroup = eventLoopGroup;
        this.executor = executor;
        this.closed = false;
        this.closeLock = new ReentrantReadWriteLock();

        this.registry = ExtensionRegistry.newInstance();
        this.authProviderFactory = AuthProviderFactoryFactory.newClientAuthProviderFactory(conf);

        this.statsLogger = statsLogger;
        this.numConnectionsPerBookie = conf.getNumChannelsPerBookie();
        this.requestTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("BookieClientTimer-%d").build(),
                conf.getPCBCTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getPCBCTimeoutTimerNumTicks());
        this.bookieErrorThresholdPerInterval = conf.getBookieErrorThresholdPerInterval();
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
    public PerChannelBookieClient create(BookieSocketAddress address, PerChannelBookieClientPool pcbcPool) {
        return new PerChannelBookieClient(conf, executor, eventLoopGroup, address, requestTimer, statsLogger,
                authProviderFactory, registry, pcbcPool);
    }

    private PerChannelBookieClientPool lookupClient(BookieSocketAddress addr, Object key) {
        PerChannelBookieClientPool clientPool = channels.get(addr);
        if (null == clientPool) {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    return null;
                }
                PerChannelBookieClientPool newClientPool =
                    new DefaultPerChannelBookieClientPool(this, addr, numConnectionsPerBookie);
                PerChannelBookieClientPool oldClientPool = channels.putIfAbsent(addr, newClientPool);
                if (null == oldClientPool) {
                    clientPool = newClientPool;
                    // initialize the pool only after we put the pool into the map
                    clientPool.intialize();
                } else {
                    clientPool = oldClientPool;
                    newClientPool.close(false);
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return clientPool;
    }

    public void writeLac(final BookieSocketAddress addr, final long ledgerId, final byte[] masterKey,
            final long lac, final ByteBuf toSend, final WriteLacCallback cb, final Object ctx) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, lac);
            if (client == null) {
                cb.writeLacComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                                  ledgerId, addr, ctx);
                return;
            }

            toSend.retain();
            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {
                    if (rc != BKException.Code.OK) {
                        try {
                            executor.submitOrdered(ledgerId, new SafeRunnable() {
                                @Override
                                public void safeRun() {
                                    cb.writeLacComplete(rc, ledgerId, addr, ctx);
                                }
                            });
                        } catch (RejectedExecutionException re) {
                            cb.writeLacComplete(getRc(BKException.Code.InterruptedException), ledgerId, addr, ctx);
                        }
                    } else {
                        pcbc.writeLac(ledgerId, masterKey, lac, toSend, cb, ctx);
                    }

                    toSend.release();
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public void addEntry(final BookieSocketAddress addr, final long ledgerId, final byte[] masterKey,
            final long entryId, final ByteBuf toSend, final WriteCallback cb, final Object ctx, final int options) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, entryId);
            if (client == null) {
                cb.writeComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                                 ledgerId, entryId, addr, ctx);
                return;
            }

            // Retain the buffer, since the connection could be obtained after the PendingApp might have already
            // failed
            toSend.retain();

            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {
                    if (rc != BKException.Code.OK) {
                        try {
                            executor.submitOrdered(ledgerId, new SafeRunnable() {
                                @Override
                                public void safeRun() {
                                    cb.writeComplete(rc, ledgerId, entryId, addr, ctx);
                                }
                            });
                        } catch (RejectedExecutionException re) {
                            cb.writeComplete(getRc(BKException.Code.InterruptedException),
                                    ledgerId, entryId, addr, ctx);
                        }
                    } else {
                        pcbc.addEntry(ledgerId, masterKey, entryId, toSend, cb, ctx, options);
                    }
                    toSend.release();
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public void readEntryAndFenceLedger(final BookieSocketAddress addr,
                                        final long ledgerId,
                                        final byte[] masterKey,
                                        final long entryId,
                                        final ReadEntryCallback cb,
                                        final Object ctx) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, entryId);
            if (client == null) {
                cb.readEntryComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                                     ledgerId, entryId, null, ctx);
                return;
            }

            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {
                    if (rc != BKException.Code.OK) {
                        try {
                            executor.submitOrdered(ledgerId, new SafeRunnable() {
                                @Override
                                public void safeRun() {
                                    cb.readEntryComplete(rc, ledgerId, entryId, null, ctx);
                                }
                            });
                        } catch (RejectedExecutionException re) {
                            cb.readEntryComplete(getRc(BKException.Code.InterruptedException),
                                    ledgerId, entryId, null, ctx);
                        }
                        return;
                    }
                    pcbc.readEntryAndFenceLedger(ledgerId, masterKey, entryId, cb, ctx);
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public void readLac(final BookieSocketAddress addr, final long ledgerId, final ReadLacCallback cb, final Object ctx) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, BookieProtocol.LAST_ADD_CONFIRMED);
            if (client == null) {
                cb.readLacComplete(getRc(BKException.Code.BookieHandleNotAvailableException), ledgerId, null, null, ctx);
                return;
            }
            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc,PerChannelBookieClient pcbc) {
                    if (rc != BKException.Code.OK) {
                        try {
                            executor.submitOrdered(ledgerId, new SafeRunnable() {
                                @Override
                                public void safeRun() {
                                    cb.readLacComplete(rc, ledgerId, null, null, ctx);
                                }
                            });
                        } catch (RejectedExecutionException re) {
                            cb.readLacComplete(getRc(BKException.Code.InterruptedException),
                                    ledgerId, null, null, ctx);
                        }
                        return;
                    }
                    pcbc.readLac(ledgerId, cb, ctx);
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public void readEntry(final BookieSocketAddress addr, final long ledgerId, final long entryId,
                          final ReadEntryCallback cb, final Object ctx) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, entryId);
            if (client == null) {
                cb.readEntryComplete(getRc(BKException.Code.BookieHandleNotAvailableException),
                                     ledgerId, entryId, null, ctx);
                return;
            }

            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {
                    if (rc != BKException.Code.OK) {
                        try {
                            executor.submitOrdered(ledgerId, new SafeRunnable() {
                                @Override
                                public void safeRun() {
                                    cb.readEntryComplete(rc, ledgerId, entryId, null, ctx);
                                }
                            });
                        } catch (RejectedExecutionException re) {
                            cb.readEntryComplete(getRc(BKException.Code.InterruptedException),
                                    ledgerId, entryId, null, ctx);
                        }
                        return;
                    }
                    pcbc.readEntry(ledgerId, entryId, cb, ctx);
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public void getBookieInfo(final BookieSocketAddress addr, final long requested, final GetBookieInfoCallback cb, final Object ctx) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, BookkeeperProtocol.OperationType.GET_BOOKIE_INFO);
            if (client == null) {
                cb.getBookieInfoComplete(getRc(BKException.Code.BookieHandleNotAvailableException), new BookieInfo(), ctx);
                return;
            }
            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {
                    if (rc != BKException.Code.OK) {
                        try {
                            executor.submit(new SafeRunnable() {
                                @Override
                                public void safeRun() {
                                    cb.getBookieInfoComplete(rc, new BookieInfo(), ctx);
                                }
                            });
                        } catch (RejectedExecutionException re) {
                            cb.getBookieInfoComplete(getRc(BKException.Code.InterruptedException),
                                    new BookieInfo(), ctx);
                        }
                        return;
                    }
                    pcbc.getBookieInfo(requested, cb, ctx);
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public Timeout scheduleTimeout(TimerTask task, long timeoutSec, TimeUnit timeUnit) {
        return requestTimer.newTimeout(task, timeoutSec, timeUnit);
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
        } finally {
            closeLock.writeLock().unlock();
        }
        // Shut down the timeout executor.
        this.requestTimer.stop();
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
        OrderedSafeExecutor executor = OrderedSafeExecutor.newBuilder()
                .name("BookieClientWorker")
                .numThreads(1)
                .build();
        BookieClient bc = new BookieClient(new ClientConfiguration(), eventLoopGroup, executor);
        BookieSocketAddress addr = new BookieSocketAddress(args[0], Integer.parseInt(args[1]));

        for (int i = 0; i < 100000; i++) {
            counter.inc();
            bc.addEntry(addr, ledger, new byte[0], i, Unpooled.wrappedBuffer(hello), cb, counter, 0);
        }
        counter.wait(0);
        System.out.println("Total = " + counter.total());
        eventLoopGroup.shutdownGracefully();
        executor.shutdown();
    }
}
