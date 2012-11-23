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
package org.apache.hedwig.server.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import com.google.protobuf.ByteString;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRange;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;

import org.apache.hedwig.client.api.Client;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.util.Callback;

import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;

public class MessageBoundedPersistenceTest extends HedwigHubTestBase {
    protected static Logger logger = LoggerFactory.getLogger(MessageBoundedPersistenceTest.class);

    protected class SmallReadAheadServerConfiguration
        extends HedwigHubTestBase.HubServerConfiguration {
        SmallReadAheadServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }
        public long getMaximumCacheSize() {
            return 1;
        }

        public int getReadAheadCount() {
            return 1;
        }

        public int getMessagesConsumedThreadRunInterval() {
            return 1000; // run every second
        }
    }

    protected ServerConfiguration getServerConfiguration(int serverPort, int sslServerPort) {
        return new SmallReadAheadServerConfiguration(serverPort, sslServerPort);
    }

    private class MessageBoundClientConfiguration extends HubClientConfiguration {
        final int messageBound;

        public MessageBoundClientConfiguration(int bound) {
            this.messageBound = bound;
        }

        public MessageBoundClientConfiguration() {
            this(5);
        }

        public int getSubscriptionMessageBound() {
            return messageBound;
        }
    }

    private void sendXExpectLastY(Publisher pub, Subscriber sub,
                                  ByteString topic, ByteString subid,
                                  final int X, final int Y) throws Exception {
        for (int i = 0; i < X; i++) {
            pub.publish(topic, Message.newBuilder().setBody(
                                ByteString.copyFromUtf8(String.valueOf(i))).build());
        }
        sub.subscribe(topic, subid, CreateOrAttach.ATTACH);

        final AtomicInteger expected = new AtomicInteger(X - Y);
        final CountDownLatch latch = new CountDownLatch(1);
        sub.startDelivery(topic, subid, new MessageHandler () {
                synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                    Message msg, Callback<Void> callback,
                                    Object context) {
                    try {
                        int value = Integer.valueOf(msg.getBody().toStringUtf8());

                        if (value == expected.get()) {
                            expected.incrementAndGet();
                        } else {
                            // error condition
                            logger.error("Did not receive expected value, expected {}, got {}",
                                         expected.get(), value);
                            expected.set(0);
                            latch.countDown();
                        }
                        if (expected.get() == X) {
                            latch.countDown();
                        }
                        callback.operationFinished(context, null);
                    } catch (Exception e) {
                        logger.error("Received bad message", e);
                        latch.countDown();// will error on match
                    }
                }
            });
        assertTrue("Timed out waiting for messages Y is " + Y
                + " expected is currently " + expected.get(), latch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected message with " + X, X, expected.get());

        sub.stopDelivery(topic, subid);
        sub.closeSubscription(topic, subid);
    }

    @Test
    public void testBasicBounding() throws Exception {
        Client client = new HedwigClient(new MessageBoundClientConfiguration(5));
        Publisher pub = client.getPublisher();
        Subscriber sub = client.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("basicBoundingTopic");
        ByteString subid = ByteString.copyFromUtf8("basicBoundingSubId");
        sub.subscribe(topic, subid, CreateOrAttach.CREATE);
        sub.closeSubscription(topic, subid);

        sendXExpectLastY(pub, sub, topic, subid, 1000, 5);

        client.close();
    }

    @Test
    public void testMultipleSubscribers() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("multiSubTopic");

        Client client = new HedwigClient(new HubClientConfiguration());
        Publisher pub = client.getPublisher();
        Subscriber sub = client.getSubscriber();

        SubscriptionOptions options5 = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE).setMessageBound(5).build();
        SubscriptionOptions options20 = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE).setMessageBound(20).build();
        SubscriptionOptions optionsUnbounded = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE).build();

        ByteString subid5 = ByteString.copyFromUtf8("bound5SubId");
        ByteString subid20 = ByteString.copyFromUtf8("bound20SubId");
        ByteString subidUnbounded = ByteString.copyFromUtf8("noboundSubId");

        sub.subscribe(topic, subid5, options5);
        sub.closeSubscription(topic, subid5);
        sendXExpectLastY(pub, sub, topic, subid5, 1000, 5);

        sub.subscribe(topic, subid20, options20);
        sub.closeSubscription(topic, subid20);
        sendXExpectLastY(pub, sub, topic, subid20, 1000, 20);

        sub.subscribe(topic, subidUnbounded, optionsUnbounded);
        sub.closeSubscription(topic, subidUnbounded);

        sendXExpectLastY(pub, sub, topic, subidUnbounded, 10000, 10000);
        sub.unsubscribe(topic, subidUnbounded);

        sendXExpectLastY(pub, sub, topic, subid20, 1000, 20);
        sub.unsubscribe(topic, subid20);

        sendXExpectLastY(pub, sub, topic, subid5, 1000, 5);
        sub.unsubscribe(topic, subid5);

        client.close();
    }

    @Test
    public void testUpdateMessageBound() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("UpdateMessageBound");

        Client client = new HedwigClient(new HubClientConfiguration());
        Publisher pub = client.getPublisher();
        Subscriber sub = client.getSubscriber();

        SubscriptionOptions options5 = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageBound(5).build();
        SubscriptionOptions options20 = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageBound(20).build();
        SubscriptionOptions options10 = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageBound(10).build();

        ByteString subid = ByteString.copyFromUtf8("updateSubId");

        sub.subscribe(topic, subid, options5);
        sub.closeSubscription(topic, subid);
        sendXExpectLastY(pub, sub, topic, subid, 50, 5);

        // update bound to 20
        sub.subscribe(topic, subid, options20);
        sub.closeSubscription(topic, subid);
        sendXExpectLastY(pub, sub, topic, subid, 50, 20);

        // update bound to 10
        sub.subscribe(topic, subid, options10);
        sub.closeSubscription(topic, subid);
        sendXExpectLastY(pub, sub, topic, subid, 50, 10);

        // message bound is not provided, no update
        sub.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        sub.closeSubscription(topic, subid);
        sendXExpectLastY(pub, sub, topic, subid, 50, 10);

        client.close();
    }

    @Test
    public void testLedgerGC() throws Exception {
        Client client = new HedwigClient(new MessageBoundClientConfiguration());
        Publisher pub = client.getPublisher();
        Subscriber sub = client.getSubscriber();

        String ledgersPath = "/hedwig/standalone/topics/testGCTopic/ledgers";
        ByteString topic = ByteString.copyFromUtf8("testGCTopic");
        ByteString subid = ByteString.copyFromUtf8("testGCSubId");
        sub.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        sub.closeSubscription(topic, subid);

        for (int i = 1; i <= 100; i++) {
            pub.publish(topic, Message.newBuilder().setBody(
                                ByteString.copyFromUtf8(String.valueOf(i))).build());
        }
        LedgerRanges r = LedgerRanges.parseFrom(bktb.getZooKeeperClient().getData(ledgersPath, false, null));
        assertEquals("Should only have 1 ledger yet", 1, r.getRangesList().size());
        long firstLedger = r.getRangesList().get(0).getLedgerId();

        stopHubServers();
        startHubServers();

        pub.publish(topic, Message.newBuilder().setBody(
                            ByteString.copyFromUtf8(String.valueOf(0xdeadbeef))).build());

        r = LedgerRanges.parseFrom(bktb.getZooKeeperClient().getData(ledgersPath, false, null));
        assertEquals("Should have 2 ledgers after restart", 2, r.getRangesList().size());

        for (int i = 100; i <= 200; i++) {
            pub.publish(topic, Message.newBuilder().setBody(
                                ByteString.copyFromUtf8(String.valueOf(i))).build());
        }
        Thread.sleep(5000); // give GC a chance to happen

        r = LedgerRanges.parseFrom(bktb.getZooKeeperClient().getData(ledgersPath, false, null));
        long secondLedger = r.getRangesList().get(0).getLedgerId();

        assertEquals("Should only have 1 ledger after GC", 1, r.getRangesList().size());

        // ensure original ledger doesn't exist
        String firstLedgerPath = String.format("/ledgers/L%010d", firstLedger);
        String secondLedgerPath = String.format("/ledgers/L%010d", secondLedger);
        assertNull("Ledger should not exist", bktb.getZooKeeperClient().exists(firstLedgerPath, false));
        assertNotNull("Ledger should exist", bktb.getZooKeeperClient().exists(secondLedgerPath, false));

        client.close();
    }
}
