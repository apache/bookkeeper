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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.EventLoopUtil;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class ClientSocketDisconnectTest extends BookKeeperClusterTestCase {

    public ClientSocketDisconnectTest() {
        super(1);
        this.useUUIDasBookieId = true;
    }

    public static class PerChannelBookieClientDecorator extends PerChannelBookieClient {

        private final ThreadCounter threadCounter;
        private final AtomicInteger failurePredicate = new AtomicInteger();

        public PerChannelBookieClientDecorator(PerChannelBookieClient client, BookieId addr, ThreadCounter tCounter)
                throws SecurityException {
            super(client.executor, client.eventLoopGroup, addr, client.bookieAddressResolver);
            this.threadCounter = tCounter;
        }

        // Inject a disconnection per two connections.
        protected void addChannelListeners(ChannelFuture future, long connectStartTime) {
            future.addListener((ChannelFutureListener) future1 -> {
                if (failurePredicate.incrementAndGet() % 2 == 1) {
                    future1.channel().close();
                }
            });
            super.addChannelListeners(future, connectStartTime);
        }

        // Records the thread who running "PendingAddOp.writeComplete".
        @Override
        protected void connectIfNeededAndDoOp(BookkeeperInternalCallbacks.GenericCallback<PerChannelBookieClient> op) {
            BookieClientImpl.ChannelReadyForAddEntryCallback callback =
                    (BookieClientImpl.ChannelReadyForAddEntryCallback) op;
            BookkeeperInternalCallbacks.WriteCallback originalCallback = callback.cb;
            callback.cb = (rc, ledgerId, entryId, addr, ctx) -> {
                threadCounter.record();
                originalCallback.writeComplete(rc, ledgerId, entryId, addr, ctx);
            };
            super.connectIfNeededAndDoOp(op);
        }
    }

    private static class ThreadCounter {

        private final Map<Thread, AtomicInteger> records = new ConcurrentHashMap<>();

        public void record() {
            Thread currentThread = Thread.currentThread();
            records.computeIfAbsent(currentThread, k -> new AtomicInteger());
            records.get(currentThread).incrementAndGet();
        }
    }

    @Test
    public void testAddEntriesCallbackWithBKClientThread() throws Exception {
        //setUp();
        // Create BKC and a ledger handle.
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        org.apache.bookkeeper.client.BookKeeper bkc =
                (org.apache.bookkeeper.client.BookKeeper) BookKeeper.newBuilder(conf)
                .eventLoopGroup(
                        EventLoopUtil.getClientEventLoopGroup(conf, new DefaultThreadFactory("test-io")))
                .build();
        final BookieClientImpl bookieClient = (BookieClientImpl) bkc.getClientCtx().getBookieClient();
        LedgerHandle lh = (LedgerHandle) bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute().join();

        // Inject two operations.
        // 1. Inject a disconnection when connecting successfully.
        // 2. Records the thread who running "PendingAddOp.writeComplete".
        final ThreadCounter callbackThreadRecorder = new ThreadCounter();
        List<BookieId> ensemble = lh.getLedgerMetadata()
                .getAllEnsembles().entrySet().iterator().next().getValue();
        DefaultPerChannelBookieClientPool clientPool =
                (DefaultPerChannelBookieClientPool) bookieClient.lookupClient(ensemble.get(0));
        PerChannelBookieClient[] clients = clientPool.clients;

        // Write 100 entries and wait for finishing.
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new PerChannelBookieClientDecorator(clients[i], ensemble.get(0), callbackThreadRecorder);
        }
        int addCount = 1000;
        CountDownLatch countDownLatch = new CountDownLatch(addCount);
        for (int i = 0; i < addCount; i++) {
            lh.asyncAddEntry(new byte[]{1}, (rc, lh1, entryId, ctx) -> {
                countDownLatch.countDown();
            }, i);
        }
        countDownLatch.await();

        // Verify: all callback will run in the "BookKeeperClientWorker" thread.
        for (Thread callbackThread : callbackThreadRecorder.records.keySet()) {
            Assert.assertTrue(callbackThread.getName(), callbackThread.getName().startsWith("BookKeeperClientWorker"));
        }
    }
}