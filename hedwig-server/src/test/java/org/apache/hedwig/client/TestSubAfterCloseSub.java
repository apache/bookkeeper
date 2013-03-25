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
package org.apache.hedwig.client;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.delivery.FIFODeliveryManager;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.util.Callback;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestSubAfterCloseSub extends HedwigHubTestBase {

    class TestClientConfiguration extends HubClientConfiguration {

        boolean isSubscriptionChannelSharingEnabled;

        TestClientConfiguration(boolean isSubscriptionChannelSharingEnabled) {
            this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
        }

        @Override
        public boolean isSubscriptionChannelSharingEnabled() {
            return isSubscriptionChannelSharingEnabled;
        }
    }

    private void sleepDeliveryManager(final CountDownLatch wakeupLatch)
            throws IOException {
        PubSubServer server = serversList.get(0);
        assertNotNull("There should be at least one pubsub server", server);
        DeliveryManager dm = server.getDeliveryManager();
        assertNotNull("Delivery manager should not be null once server has started", dm);
        assertTrue("Delivery manager is wrong type", dm instanceof FIFODeliveryManager);
        final FIFODeliveryManager fdm = (FIFODeliveryManager)dm;

        Thread sleeper = new Thread() {
                @Override
                public void run() {
                    try {
                        fdm.suspendProcessing();
                        wakeupLatch.await();
                        fdm.resumeProcessing();
                    } catch (Exception e) {
                        logger.error("Error suspending delivery manager", e);
                    }
                }
            };
        sleeper.start();
    }

    /**
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-507}
     */
    @Test(timeout=15000)
    public void testSubAfterCloseSubForSimpleClient() throws Exception {
        runSubAfterCloseSubTest(false);
    }

    /**
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-507}
     */
    @Test(timeout=15000)
    public void testSubAfterCloseSubForMultiplexClient() throws Exception {
        runSubAfterCloseSubTest(true);
    }

    private void runSubAfterCloseSubTest(boolean sharedSubscriptionChannel) throws Exception {
        HedwigClient client = new HedwigClient(new TestClientConfiguration(sharedSubscriptionChannel));
        Publisher publisher = client.getPublisher();
        final Subscriber subscriber = client.getSubscriber();

        final ByteString topic = ByteString.copyFromUtf8("TestSubAfterCloseSub-" + sharedSubscriptionChannel);
        final ByteString subid = ByteString.copyFromUtf8("mysub");

        final CountDownLatch wakeupLatch = new CountDownLatch(1);
        final CountDownLatch closeLatch = new CountDownLatch(1);
        final CountDownLatch subLatch = new CountDownLatch(1);
        final CountDownLatch deliverLatch = new CountDownLatch(1);

        try {
            SubscriptionOptions opts = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).build();
            subscriber.subscribe(topic, subid, opts);
            sleepDeliveryManager(wakeupLatch);
            subscriber.asyncCloseSubscription(topic, subid, new Callback<Void>() {
                @Override
                public void operationFinished(Object ctx, Void resultOfOperation) {
                    closeLatch.countDown();
                }
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    logger.error("Closesub failed : ", exception);
                }
            }, null);

            SubscriptionOptions optsAttach = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.ATTACH).build();
            subscriber.asyncSubscribe(topic, subid, optsAttach, new Callback<Void>() {
                @Override
                public void operationFinished(Object ctx, Void resultOfOperation) {
                    try {
                        subscriber.startDelivery(topic, subid, new MessageHandler() {
                            @Override
                            public void deliver(ByteString topic, ByteString subid, Message msg,
                                                Callback<Void> callback, Object context) {
                                deliverLatch.countDown();
                            }
                        });
                    } catch (Exception cnse) {
                        logger.error("Failed to start delivery : ", cnse);
                    }
                    subLatch.countDown();
                }
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    logger.error("Failed to subscriber : ", exception);
                }
            }, null);
            // Make the delivery manager thread sleep for a while.
            // Before {@link https://issues.apache.org/jira/browse/BOOKKEEPER-507},
            // subscribe would succeed before closesub, while closesub would clear
            // a successful subscription w/o notifying the client.
            TimeUnit.SECONDS.sleep(2);
            // wake up fifo delivery thread
            wakeupLatch.countDown();
            // wait close sub to succeed
            assertTrue("Async close subscription should succeed.",
                       closeLatch.await(5, TimeUnit.SECONDS));
            assertTrue("Subscribe should succeed.",
                       subLatch.await(5, TimeUnit.SECONDS));
            // publish a message
            publisher.publish(topic, Message.newBuilder().setBody(topic).build());
            // wait for seconds to receive message
            assertTrue("Message should be received through successful subscription.",
                       deliverLatch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
        }
    }

    /**
     * Test that if we close a subscription and open again immediately, we don't
     * get a TOPIC_BUSY. This race existed because the simple client simply closed
     * the connection when closing a subscription, and another client could try to
     * attach to the subscription before the channel disconnected event occurs.
     *
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-513}
     */
    @Test(timeout=15000)
    public void testSimpleClientDoesntGetTopicBusy() throws Exception {
        // run ten times to increase chance of hitting race
        for (int i = 0; i < 10; i++) {
            HedwigClient client1 = new HedwigClient(new TestClientConfiguration(false));
            Subscriber subscriber1 = client1.getSubscriber();
            HedwigClient client2 = new HedwigClient(new TestClientConfiguration(false));
            Subscriber subscriber2 = client2.getSubscriber();

            final ByteString topic = ByteString.copyFromUtf8("TestSimpleClientTopicBusy");
            final ByteString subid = ByteString.copyFromUtf8("mysub");

            SubscriptionOptions opts1 = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).build();
            subscriber1.subscribe(topic, subid, opts1);
            subscriber1.closeSubscription(topic, subid);

            SubscriptionOptions opts2 = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.ATTACH).build();
            subscriber2.subscribe(topic, subid, opts2);
            subscriber2.closeSubscription(topic, subid);

            client1.close();
            client2.close();
        }
    }
}

