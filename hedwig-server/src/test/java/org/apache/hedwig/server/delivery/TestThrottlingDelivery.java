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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;

@RunWith(Parameterized.class)
public class TestThrottlingDelivery extends HedwigHubTestBase {

    private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 10;

    protected class ThrottleDeliveryServerConfiguration extends HubServerConfiguration {

        ThrottleDeliveryServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }

        @Override
        public int getDefaultMessageWindowSize() {
            return TestThrottlingDelivery.this.DEFAULT_MESSAGE_WINDOW_SIZE;
        }
    }

    protected class ThrottleDeliveryClientConfiguration extends HubClientConfiguration {

        int messageWindowSize;

        ThrottleDeliveryClientConfiguration() {
            this(TestThrottlingDelivery.this.DEFAULT_MESSAGE_WINDOW_SIZE);
        }

        ThrottleDeliveryClientConfiguration(int messageWindowSize) {
            this.messageWindowSize = messageWindowSize;
        }

        @Override
        public int getMaximumOutstandingMessages() {
            return messageWindowSize;
        }

        void setMessageWindowSize(int messageWindowSize) {
            this.messageWindowSize = messageWindowSize;
        }

        @Override
        public boolean isAutoSendConsumeMessageEnabled() {
            return false;
        }

        @Override
        public boolean isSubscriptionChannelSharingEnabled() {
            return isSubscriptionChannelSharingEnabled;
        }
    }

    private void throttleX(Publisher pub, final Subscriber sub,
                           ByteString topic, ByteString subid,
                           final int X) throws Exception {
        for (int i=1; i<=3*X; i++) {
            pub.publish(topic, Message.newBuilder().setBody(
                               ByteString.copyFromUtf8(String.valueOf(i))).build());
        }
        SubscriptionOptions opts = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.ATTACH).build();
        sub.subscribe(topic, subid, opts);

        final AtomicInteger expected = new AtomicInteger(1);
        final CountDownLatch throttleLatch = new CountDownLatch(1);
        final CountDownLatch nonThrottleLatch = new CountDownLatch(1);
        sub.startDelivery(topic, subid, new MessageHandler() {
            @Override
            public synchronized void deliver(ByteString topic, ByteString subscriberId,
                                             Message msg,
                                             Callback<Void> callback, Object context) {
                try {
                    int value = Integer.valueOf(msg.getBody().toStringUtf8());
                    logger.debug("Received message {},", value);

                    if (value == expected.get()) {
                        expected.incrementAndGet();
                    } else {
                        // error condition
                        logger.error("Did not receive expected value, expected {}, got {}",
                                     expected.get(), value);
                        expected.set(0);
                        throttleLatch.countDown();
                        nonThrottleLatch.countDown();
                    }
                    if (expected.get() > X+1) {
                        throttleLatch.countDown();
                    }
                    if (expected.get() == (3 * X + 1)) {
                        nonThrottleLatch.countDown();
                    }
                    callback.operationFinished(context, null);
                    if (expected.get() > X + 1) {
                        sub.consume(topic, subscriberId, msg.getMsgId());
                    }      
                } catch (Exception e) {
                    logger.error("Received bad message", e);
                    throttleLatch.countDown();
                    nonThrottleLatch.countDown();
                }
            }
        });
        assertFalse("Received more messages than throttle value " + X,
                    throttleLatch.await(3, TimeUnit.SECONDS));
        assertEquals("Should be expected messages with only " + (X+1), X+1, expected.get());

        // consume messages to not throttle it
        for (int i=1; i<=X; i++) {
            sub.consume(topic, subid,
                        MessageSeqId.newBuilder().setLocalComponent(i).build());
        }

        assertTrue("Timed out waiting for messages " + (3*X + 1),
                   nonThrottleLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected message with " + (3*X + 1),
                     3*X + 1, expected.get());

        sub.stopDelivery(topic, subid);
        sub.closeSubscription(topic, subid);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    protected boolean isSubscriptionChannelSharingEnabled;

    public TestThrottlingDelivery(boolean isSubscriptionChannelSharingEnabled) {
        super(1);
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }


    @Override
    protected ServerConfiguration getServerConfiguration(int port, int sslPort) {
        return new ThrottleDeliveryServerConfiguration(port, sslPort);
    }

    @Test(timeout=60000)
    public void testServerSideThrottle() throws Exception {
        int messageWindowSize = DEFAULT_MESSAGE_WINDOW_SIZE;
        ThrottleDeliveryClientConfiguration conf =
            new ThrottleDeliveryClientConfiguration();
        HedwigClient client = new HedwigClient(conf);
        Publisher pub = client.getPublisher();
        Subscriber sub = client.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testServerSideThrottle"); 
        ByteString subid = ByteString.copyFromUtf8("serverThrottleSub");
        SubscriptionOptions opts = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE).build();
        sub.subscribe(topic, subid, opts);
        sub.closeSubscription(topic, subid);

        // throttle with hub server's setting
        throttleX(pub, sub, topic, subid, DEFAULT_MESSAGE_WINDOW_SIZE);

        messageWindowSize = DEFAULT_MESSAGE_WINDOW_SIZE / 2;
        // throttle with a lower value than hub server's setting
        SubscriptionOptions.Builder optionsBuilder = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE)
            .setMessageWindowSize(messageWindowSize);
        topic = ByteString.copyFromUtf8("testServerSideThrottleWithLowerValue");
        sub.subscribe(topic, subid, optionsBuilder.build());
        sub.closeSubscription(topic, subid);
        throttleX(pub, sub, topic, subid, messageWindowSize);

        messageWindowSize = DEFAULT_MESSAGE_WINDOW_SIZE + 5;
        // throttle with a higher value than hub server's setting
        optionsBuilder = SubscriptionOptions.newBuilder()
                         .setCreateOrAttach(CreateOrAttach.CREATE)
                         .setMessageWindowSize(messageWindowSize);
        topic = ByteString.copyFromUtf8("testServerSideThrottleWithHigherValue");
        sub.subscribe(topic, subid, optionsBuilder.build());
        sub.closeSubscription(topic, subid);
        throttleX(pub, sub, topic, subid, messageWindowSize);

        client.close();
    }

}
