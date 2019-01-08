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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.util.function.BooleanSupplier;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.MockRegistrationClient;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MockLedgerManager;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.MockBookieClient;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.mockito.Mockito;

/**
 * Mock client context to allow testing client functionality with no external dependencies.
 * The client context can be created with defaults, copied from another context or constructed from scratch.
 */
public class MockClientContext implements ClientContext {
    private ClientInternalConf internalConf;
    private LedgerManager ledgerManager;
    private BookieWatcher bookieWatcher;
    private EnsemblePlacementPolicy placementPolicy;
    private BookieClient bookieClient;
    private OrderedExecutor mainWorkerPool;
    private OrderedScheduler scheduler;
    private BookKeeperClientStats clientStats;
    private BooleanSupplier isClientClosed;
    private MockRegistrationClient regClient;
    private ByteBufAllocator allocator;

    static MockClientContext create() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder().name("mock-executor").numThreads(1).build();
        MockRegistrationClient regClient = new MockRegistrationClient();
        EnsemblePlacementPolicy placementPolicy = new DefaultEnsemblePlacementPolicy();
        BookieWatcherImpl bookieWatcherImpl = new BookieWatcherImpl(conf, placementPolicy,
                                                                    regClient, NullStatsLogger.INSTANCE);
        bookieWatcherImpl.initialBlockingBookieRead();

        return new MockClientContext()
            .setConf(ClientInternalConf.fromConfig(conf))
            .setLedgerManager(new MockLedgerManager())
            .setBookieWatcher(bookieWatcherImpl)
            .setPlacementPolicy(placementPolicy)
            .setRegistrationClient(regClient)
            .setBookieClient(new MockBookieClient(scheduler))
            .setByteBufAllocator(UnpooledByteBufAllocator.DEFAULT)
            .setMainWorkerPool(scheduler)
            .setScheduler(scheduler)
            .setClientStats(BookKeeperClientStats.newInstance(NullStatsLogger.INSTANCE))
            .setIsClientClosed(() -> false);
    }

    static MockClientContext copyOf(ClientContext other) {
        return new MockClientContext()
            .setConf(other.getConf())
            .setLedgerManager(other.getLedgerManager())
            .setBookieWatcher(other.getBookieWatcher())
            .setPlacementPolicy(other.getPlacementPolicy())
            .setBookieClient(other.getBookieClient())
            .setMainWorkerPool(other.getMainWorkerPool())
            .setScheduler(other.getScheduler())
            .setClientStats(other.getClientStats())
            .setByteBufAllocator(other.getByteBufAllocator())
            .setIsClientClosed(other::isClientClosed);
    }

    public MockRegistrationClient getMockRegistrationClient() {
        checkState(regClient != null);
        return regClient;
    }

    public MockLedgerManager getMockLedgerManager() {
        checkState(ledgerManager instanceof MockLedgerManager);
        return (MockLedgerManager) ledgerManager;
    }

    public MockBookieClient getMockBookieClient() {
        checkState(bookieClient instanceof MockBookieClient);
        return (MockBookieClient) bookieClient;
    }

    public MockClientContext setConf(ClientInternalConf internalConf) {
        this.internalConf = maybeSpy(internalConf);
        return this;
    }

    public MockClientContext setLedgerManager(LedgerManager ledgerManager) {
        this.ledgerManager = maybeSpy(ledgerManager);
        return this;
    }

    public MockClientContext setBookieWatcher(BookieWatcher bookieWatcher) {
        this.bookieWatcher = maybeSpy(bookieWatcher);
        return this;
    }

    public MockClientContext setPlacementPolicy(EnsemblePlacementPolicy placementPolicy) {
        this.placementPolicy = maybeSpy(placementPolicy);
        return this;
    }

    public MockClientContext setBookieClient(BookieClient bookieClient) {
        this.bookieClient = maybeSpy(bookieClient);
        return this;
    }

    public MockClientContext setMainWorkerPool(OrderedExecutor mainWorkerPool) {
        this.mainWorkerPool = maybeSpy(mainWorkerPool);
        return this;
    }

    public MockClientContext setScheduler(OrderedScheduler scheduler) {
        this.scheduler = maybeSpy(scheduler);
        return this;
    }

    public MockClientContext setClientStats(BookKeeperClientStats clientStats) {
        this.clientStats = clientStats;
        return this;
    }

    public MockClientContext setIsClientClosed(BooleanSupplier isClientClosed) {
        this.isClientClosed = isClientClosed;
        return this;
    }

    public MockClientContext setRegistrationClient(MockRegistrationClient regClient) {
        this.regClient = maybeSpy(regClient);
        return this;
    }

    public MockClientContext setByteBufAllocator(ByteBufAllocator allocator) {
        this.allocator = allocator;
        return this;
    }

    private static <T> T maybeSpy(T orig) {
        if (Mockito.mockingDetails(orig).isSpy()) {
            return orig;
        } else {
            return Mockito.spy(orig);
        }
    }

    @Override
    public ClientInternalConf getConf() {
        return this.internalConf;
    }

    @Override
    public LedgerManager getLedgerManager() {
        return this.ledgerManager;
    }

    @Override
    public BookieWatcher getBookieWatcher() {
        return this.bookieWatcher;
    }

    @Override
    public EnsemblePlacementPolicy getPlacementPolicy() {
        return this.placementPolicy;
    }

    @Override
    public BookieClient getBookieClient() {
        return this.bookieClient;
    }

    @Override
    public OrderedExecutor getMainWorkerPool() {
        return this.mainWorkerPool;
    }

    @Override
    public OrderedScheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public BookKeeperClientStats getClientStats() {
        return clientStats;
    }

    @Override
    public boolean isClientClosed() {
        return isClientClosed.getAsBoolean();
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return allocator;
    }
}
