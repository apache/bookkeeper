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

import java.util.function.BooleanSupplier;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieClient;

class MockClientContext implements ClientContext {
    private ClientInternalConf conf;
    private LedgerManager ledgerManager;
    private BookieWatcher bookieWatcher;
    private EnsemblePlacementPolicy placementPolicy;
    private BookieClient bookieClient;
    private OrderedExecutor mainWorkerPool;
    private OrderedScheduler scheduler;
    private BookKeeperClientStats clientStats;
    private BooleanSupplier isClientClosed;

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
            .setIsClientClosed(other::isClientClosed);
    }

    MockClientContext setConf(ClientInternalConf conf) {
        this.conf = conf;
        return this;
    }

    MockClientContext setLedgerManager(LedgerManager ledgerManager) {
        this.ledgerManager = ledgerManager;
        return this;
    }

    MockClientContext setBookieWatcher(BookieWatcher bookieWatcher) {
        this.bookieWatcher = bookieWatcher;
        return this;
    }

    MockClientContext setPlacementPolicy(EnsemblePlacementPolicy placementPolicy) {
        this.placementPolicy = placementPolicy;
        return this;
    }

    MockClientContext setBookieClient(BookieClient bookieClient) {
        this.bookieClient = bookieClient;
        return this;
    }

    MockClientContext setMainWorkerPool(OrderedExecutor mainWorkerPool) {
        this.mainWorkerPool = mainWorkerPool;
        return this;
    }

    MockClientContext setScheduler(OrderedScheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    MockClientContext setClientStats(BookKeeperClientStats clientStats) {
        this.clientStats = clientStats;
        return this;
    }

    MockClientContext setIsClientClosed(BooleanSupplier isClientClosed) {
        this.isClientClosed = isClientClosed;
        return this;
    }

    @Override
    public ClientInternalConf getConf() {
        return this.conf;
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

}
