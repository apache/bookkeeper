/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.delivery;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.filter.PipelineFilter;
import org.apache.hedwig.server.subscriptions.AllToAllTopologyFilter;

import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.exceptions.PubSubException;

import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.server.persistence.StubPersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.PersistRequest;

public class TestFIFODeliveryManager {
    static Logger logger = LoggerFactory.getLogger(TestFIFODeliveryManager.class);

    static class TestCallback implements Callback<MessageSeqId> {
        AtomicBoolean success = new AtomicBoolean(false);
        final CountDownLatch latch;
        MessageSeqId msgid = null;

        TestCallback(CountDownLatch l) {
            this.latch = l;
        }
        public void operationFailed(Object ctx, PubSubException exception) {
            logger.error("Persist operation failed", exception);
            latch.countDown();
        }

        public void operationFinished(Object ctx, MessageSeqId resultOfOperation) {
            msgid = resultOfOperation;
            success.set(true);
            latch.countDown();
        }

        MessageSeqId getId() {
            assertTrue("Persist operation failed", success.get());
            return msgid;
        }
    }

    /**
     * Delivery endpoint which puts all responses on a queue
     */
    static class ExecutorDeliveryEndPointWithQueue implements DeliveryEndPoint {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger numResponses = new AtomicInteger(0);
        ConcurrentLinkedQueue<PubSubResponse> queue = new ConcurrentLinkedQueue<PubSubResponse>();

        public void send(final PubSubResponse response, final DeliveryCallback callback) {
            logger.info("Received response {}", response);
            queue.add(response);
            numResponses.incrementAndGet();
            executor.submit(new Runnable() {
                    public void run() {
                        callback.sendingFinished();
                    }
                });
        }

        public void close() {
            executor.shutdown();
        }

        PubSubResponse getNextResponse() {
            return queue.poll();
        }

        int getNumResponses() {
            return numResponses.get();
        }
    }

    /**
     * Test that the FIFO delivery manager executes stopServing and startServing
     * in the correct order
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-539}
     */
    @Test
    public void testFIFODeliverySubCloseSubRace() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        ByteString topic = ByteString.copyFromUtf8("subRaceTopic");
        ByteString subscriber = ByteString.copyFromUtf8("subRaceSubscriber");

        PersistenceManager pm = new StubPersistenceManager();
        FIFODeliveryManager fdm = new FIFODeliveryManager(pm, conf);
        ExecutorDeliveryEndPointWithQueue dep = new ExecutorDeliveryEndPointWithQueue();
        SubscriptionPreferences prefs = SubscriptionPreferences.newBuilder().build();

        PipelineFilter filter = new PipelineFilter();
        filter.addLast(new AllToAllTopologyFilter());
        filter.initialize(conf.getConf());
        filter.setSubscriptionPreferences(topic, subscriber, prefs);
        MessageSeqId startId = MessageSeqId.newBuilder().build();

        CountDownLatch l = new CountDownLatch(1);
        Message m = Message.newBuilder().setBody(ByteString.copyFromUtf8(String.valueOf(1))).build();
        TestCallback cb = new TestCallback(l);
        pm.persistMessage(new PersistRequest(topic, m, cb, null));
        assertTrue("Persistence never finished", l.await(10, TimeUnit.SECONDS));

        final CountDownLatch oplatch = new CountDownLatch(3);
        fdm.start();
        fdm.startServingSubscription(topic, subscriber, prefs, startId, dep, filter,
                new Callback<Void>() {
                     @Override
                     public void operationFinished(Object ctx, Void result) {
                         oplatch.countDown();
                     }
                     @Override
                     public void operationFailed(Object ctx, PubSubException exception) {
                         oplatch.countDown();
                     }
                }, null);
        fdm.stopServingSubscriber(topic, subscriber, null,
                new Callback<Void>() {
                     @Override
                     public void operationFinished(Object ctx, Void result) {
                         oplatch.countDown();
                     }
                     @Override
                     public void operationFailed(Object ctx, PubSubException exception) {
                         oplatch.countDown();
                     }
                }, null);
        fdm.startServingSubscription(topic, subscriber, prefs, startId, dep, filter,
                new Callback<Void>() {
                     @Override
                     public void operationFinished(Object ctx, Void result) {
                         oplatch.countDown();
                     }
                     @Override
                     public void operationFailed(Object ctx, PubSubException exception) {
                         oplatch.countDown();
                     }
                }, null);

        assertTrue("Ops never finished", oplatch.await(10, TimeUnit.SECONDS));
        int seconds = 5;
        while (dep.getNumResponses() < 2) {
            if (seconds-- == 0) {
                break;
            }
            Thread.sleep(1000);
        }
        PubSubResponse r = dep.getNextResponse();
        assertNotNull("There should be a response", r);
        assertTrue("Response should contain a message", r.hasMessage());
        r = dep.getNextResponse();
        assertNotNull("There should be a response", r);
        assertTrue("Response should contain a message", r.hasMessage());
        r = dep.getNextResponse();
        assertNull("There should only be 2 responses", r);
    }
}
