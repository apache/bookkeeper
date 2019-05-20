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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.SecurityProviderFactoryFactory;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Provide a simple round-robin style channel pool. We could improve it later to do more
 *  fantastic things.
 */
class DefaultPerChannelBookieClientPool implements PerChannelBookieClientPool,
        GenericCallback<PerChannelBookieClient> {

    static final Logger LOG = LoggerFactory.getLogger(DefaultPerChannelBookieClientPool.class);

    final PerChannelBookieClientFactory factory;
    final BookieSocketAddress address;

    final PerChannelBookieClient[] clients;
    final PerChannelBookieClient[] clientsV3Enforced;

    final ClientConfiguration conf;
    SecurityHandlerFactory shFactory;

    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicLong errorCounter = new AtomicLong(0);

    DefaultPerChannelBookieClientPool(ClientConfiguration conf, PerChannelBookieClientFactory factory,
                                      BookieSocketAddress address,
                                      int coreSize) throws SecurityException {
        checkArgument(coreSize > 0);
        this.factory = factory;
        this.address = address;
        this.conf = conf;

        this.shFactory = SecurityProviderFactoryFactory.getSecurityProviderFactory(conf.getTLSProviderFactoryClass());

        this.clients = new PerChannelBookieClient[coreSize];
        for (int i = 0; i < coreSize; i++) {
            this.clients[i] = factory.create(address, this, shFactory, false);
        }

        if (conf.getUseV2WireProtocol()) {
            this.clientsV3Enforced = new PerChannelBookieClient[coreSize];
            for (int i = 0; i < coreSize; i++) {
                this.clientsV3Enforced[i] = factory.create(address, this, shFactory, true);
            }
        } else {
            this.clientsV3Enforced = this.clients;
        }
    }

    @Override
    public void operationComplete(int rc, PerChannelBookieClient pcbc) {
        // nop
    }

    @Override
    public void intialize() {
        for (PerChannelBookieClient pcbc : this.clients) {
            pcbc.connectIfNeededAndDoOp(this);
        }
    }

    private PerChannelBookieClient getClient(long key) {
        return getClient(key, false);
    }

    private PerChannelBookieClient getClient(long key, PerChannelBookieClient[] pcbc) {
        if (1 == pcbc.length) {
            return pcbc[0];
        }
        int idx = MathUtils.signSafeMod(key, pcbc.length);
        return pcbc[idx];
    }
    private PerChannelBookieClient getClient(long key, boolean forceUseV3) {
        if (forceUseV3) {
            return getClient(key, clientsV3Enforced);
        }
        return getClient(key, clients);
    }

    @Override
    public void obtain(GenericCallback<PerChannelBookieClient> callback, long key) {
        obtain(callback, key, false);
    }

    @Override
    public void obtain(GenericCallback<PerChannelBookieClient> callback, long key, boolean forceUseV3) {
        getClient(key, forceUseV3).connectIfNeededAndDoOp(callback);
    }

    @Override
    public boolean isWritable(long key) {
        return getClient(key).isWritable();
    }

    @Override
    public void checkTimeoutOnPendingOperations() {
        for (int i = 0; i < clients.length; i++) {
            clients[i].checkTimeoutOnPendingOperations();
            if (clients != clientsV3Enforced) {
                clientsV3Enforced[i].checkTimeoutOnPendingOperations();
            }
        }
    }

    @Override
    public void recordError() {
        errorCounter.incrementAndGet();
    }

    @Override
    public void disconnect(boolean wait) {
        for (int i = 0; i < clients.length; i++) {
            clients[i].disconnect();
            if (clients != clientsV3Enforced) {
                clientsV3Enforced[i].disconnect();
            }
        }
    }

    @Override
    public void close(boolean wait) {
        for (int i = 0; i < clients.length; i++) {
            clients[i].close(wait);
            if (clients != clientsV3Enforced) {
                clientsV3Enforced[i].close(wait);
            }
        }
    }

    @Override
    public long getNumPendingCompletionRequests() {
        long numPending = 0;
        for (PerChannelBookieClient pcbc : clients) {
            numPending += pcbc.getNumPendingCompletionRequests();
        }
        if (clients != clientsV3Enforced) {
            for (PerChannelBookieClient pcbc : clientsV3Enforced) {
                numPending += pcbc.getNumPendingCompletionRequests();
            }
        }
        return numPending;
    }
}
