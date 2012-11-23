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
package org.apache.hedwig.client.netty;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;

public class TestMultiplexing extends HedwigHubTestBase {

    private static final int DEFAULT_MSG_WINDOW_SIZE = 10;

    protected class TestServerConfiguration extends HubServerConfiguration {
        TestServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }
        @Override
        public int getDefaultMessageWindowSize() {
            return DEFAULT_MSG_WINDOW_SIZE;
        }
    }

    class TestMessageHandler implements MessageHandler {

        int expected;
        final int numMsgsAtFirstRun;
        final int numMsgsAtSecondRun;
        final CountDownLatch firstLatch;
        final CountDownLatch secondLatch;
        final boolean receiveSecondRun;

        public TestMessageHandler(int start, int numMsgsAtFirstRun,
                                  boolean receiveSecondRun,
                                  int numMsgsAtSecondRun) {
            expected = start;
            this.numMsgsAtFirstRun = numMsgsAtFirstRun;
            this.numMsgsAtSecondRun = numMsgsAtSecondRun;
            this.receiveSecondRun = receiveSecondRun;
            firstLatch = new CountDownLatch(1);
            secondLatch = new CountDownLatch(1);
        }

        @Override
        public synchronized void deliver(ByteString topic, ByteString subscriberId,
                                         Message msg,
                                         Callback<Void> callback, Object context) {
            try {
                int value = Integer.valueOf(msg.getBody().toStringUtf8());
                logger.debug("Received message {}.", value);

                if (value == expected) {
                    ++expected;
                } else {
                    // error condition
                    logger.error("Did not receive expected value, expected {}, got {}",
                                 expected, value);
                    expected = 0;
                    firstLatch.countDown();
                    secondLatch.countDown();
                }
                if (numMsgsAtFirstRun + 1 == expected) {
                    firstLatch.countDown();
                }
                if (receiveSecondRun) {
                    if (numMsgsAtFirstRun + numMsgsAtSecondRun + 1 == expected) {
                        secondLatch.countDown();
                    }
                } else {
                    if (numMsgsAtFirstRun + 1 < expected) {
                        secondLatch.countDown();
                    }
                }
                callback.operationFinished(context, null);
                subscriber.consume(topic, subscriberId, msg.getMsgId());
            } catch (Throwable t) {
                logger.error("Received bad message.", t);
                firstLatch.countDown();
                secondLatch.countDown();
            }
        }

        public void checkFirstRun() throws Exception {
            assertTrue("Timed out waiting for messages " + (numMsgsAtFirstRun + 1),
                       firstLatch.await(10, TimeUnit.SECONDS));
            assertEquals("Should be expected messages with " + (numMsgsAtFirstRun + 1),
                         numMsgsAtFirstRun + 1, expected);
        }

        public void checkSecondRun() throws Exception {
            if (receiveSecondRun) {
                assertTrue("Timed out waiting for messages "
                           + (numMsgsAtFirstRun + numMsgsAtSecondRun + 1),
                           secondLatch.await(10, TimeUnit.SECONDS));
                assertEquals("Should be expected messages with "
                             + (numMsgsAtFirstRun + numMsgsAtSecondRun + 1),
                             numMsgsAtFirstRun + numMsgsAtSecondRun + 1, expected);
            } else {
                assertFalse("Receive more messages than " + numMsgsAtFirstRun,
                            secondLatch.await(3, TimeUnit.SECONDS));
                assertEquals("Should be expected messages with ony " + (numMsgsAtFirstRun + 1),
                             numMsgsAtFirstRun + 1, expected);
            }
        }
    }

    class ThrottleMessageHandler implements MessageHandler {

        int expected;
        final int numMsgs;
        final int numMsgsThrottle;
        final CountDownLatch throttleLatch;
        final CountDownLatch nonThrottleLatch;
        final boolean enableThrottle;

        public ThrottleMessageHandler(int start, int numMsgs,
                                      boolean enableThrottle,
                                      int numMsgsThrottle) {
            expected = start;
            this.numMsgs = numMsgs;
            this.enableThrottle = enableThrottle;
            this.numMsgsThrottle = numMsgsThrottle;
            throttleLatch = new CountDownLatch(1);
            nonThrottleLatch = new CountDownLatch(1);
        }

        @Override
        public synchronized void deliver(ByteString topic, ByteString subscriberId,
                                         Message msg,
                                         Callback<Void> callback, Object context) {
            try {
                int value = Integer.valueOf(msg.getBody().toStringUtf8());
                logger.debug("Received message {}.", value);

                if (value == expected) {
                    ++expected;
                } else {
                    // error condition
                    logger.error("Did not receive expected value, expected {}, got {}",
                                 expected, value);
                    expected = 0;
                    throttleLatch.countDown();
                    nonThrottleLatch.countDown();
                }
                if (expected == numMsgsThrottle + 2) {
                    throttleLatch.countDown();
                }
                if (expected == numMsgs + 1) {
                    nonThrottleLatch.countDown();
                }
                callback.operationFinished(context, null);
                if (enableThrottle) {
                    if (expected > numMsgsThrottle + 1) {
                        subscriber.consume(topic, subscriberId, msg.getMsgId());
                    }
                } else {
                    subscriber.consume(topic, subscriberId, msg.getMsgId());
                }
            } catch (Throwable t) {
                logger.error("Received bad message.", t);
                throttleLatch.countDown();
                nonThrottleLatch.countDown();
            }
        }

        public void checkThrottle() throws Exception {
            if (enableThrottle) {
                assertFalse("Received more messages than throttle value " + numMsgsThrottle,
                            throttleLatch.await(3, TimeUnit.SECONDS));
                assertEquals("Should be expected messages with only " + (numMsgsThrottle + 1),
                             numMsgsThrottle + 1, expected);
            } else {
                assertTrue("Should not be throttled.", throttleLatch.await(10, TimeUnit.SECONDS));
                assertTrue("Timed out waiting for messages " + (numMsgs + 1),
                           nonThrottleLatch.await(10, TimeUnit.SECONDS));
                assertEquals("Should be expected messages with " + (numMsgs + 1),
                             numMsgs + 1, expected);
            }
        }

        public void checkAfterThrottle() throws Exception {
            if (enableThrottle) {
                assertTrue("Timed out waiting for messages " + (numMsgs + 1),
                           nonThrottleLatch.await(10, TimeUnit.SECONDS));
                assertEquals("Should be expected messages with " + (numMsgs + 1),
                             numMsgs + 1, expected);
            }
        }
    }

    HedwigClient client;
    Publisher publisher;
    Subscriber subscriber;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = new HedwigClient(new HubClientConfiguration() {
            @Override
            public boolean isSubscriptionChannelSharingEnabled() {
                return true;
            }
            @Override
            public boolean isAutoSendConsumeMessageEnabled() {
                return false;
            }
        });
        publisher = client.getPublisher();
        subscriber = client.getSubscriber();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        client.close();
        super.tearDown();
    }

    @Override
    protected ServerConfiguration getServerConfiguration(int port, int sslPort) {
        return new TestServerConfiguration(port, sslPort);
    }

    @Test
    public void testStopDelivery() throws Exception {
        ByteString topic1 = ByteString.copyFromUtf8("testStopDelivery-1");
        ByteString topic2 = ByteString.copyFromUtf8("testStopDelivery-2");
        ByteString subid1 = ByteString.copyFromUtf8("mysubid-1");
        ByteString subid2 = ByteString.copyFromUtf8("mysubid-2");

        final int X = 20;

        TestMessageHandler csHandler11 =
            new TestMessageHandler(1, X, true, X);
        TestMessageHandler csHandler12 =
            new TestMessageHandler(1, X, false, 0);
        TestMessageHandler csHandler21 =
            new TestMessageHandler(1, X, false, 0);
        TestMessageHandler csHandler22 =
            new TestMessageHandler(1, X, true, X);

        subscriber.subscribe(topic1, subid1, CreateOrAttach.CREATE);
        subscriber.subscribe(topic1, subid2, CreateOrAttach.CREATE);
        subscriber.subscribe(topic2, subid1, CreateOrAttach.CREATE);
        subscriber.subscribe(topic2, subid2, CreateOrAttach.CREATE);

        // start deliveries
        subscriber.startDelivery(topic1, subid1, csHandler11);
        subscriber.startDelivery(topic1, subid2, csHandler12);
        subscriber.startDelivery(topic2, subid1, csHandler21);
        subscriber.startDelivery(topic2, subid2, csHandler22);

        // first publish
        for (int i = 1; i<=X; i++) {
            publisher.publish(topic1, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
            publisher.publish(topic2, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
        }

        csHandler11.checkFirstRun();
        csHandler12.checkFirstRun();
        csHandler21.checkFirstRun();
        csHandler22.checkFirstRun();

        // stop delivery for <topic1, subscriber2> and <topic2, subscriber1>
        subscriber.stopDelivery(topic1, subid2);
        subscriber.stopDelivery(topic2, subid1);

        // second publish
        for (int i = X+1; i<=2*X; i++) {
            publisher.publish(topic1, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
            publisher.publish(topic2, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
        }

        csHandler11.checkSecondRun();
        csHandler22.checkSecondRun();
        csHandler12.checkSecondRun();
        csHandler21.checkSecondRun();
    }

    @Test
    public void testCloseSubscription() throws Exception {
        ByteString topic1 = ByteString.copyFromUtf8("testCloseSubscription-1");
        ByteString topic2 = ByteString.copyFromUtf8("testCloseSubscription-2");
        ByteString subid1 = ByteString.copyFromUtf8("mysubid-1");
        ByteString subid2 = ByteString.copyFromUtf8("mysubid-2");

        final int X = 20;

        TestMessageHandler csHandler11 =
            new TestMessageHandler(1, X, true, X);
        TestMessageHandler csHandler12 =
            new TestMessageHandler(1, X, false, 0);
        TestMessageHandler csHandler21 =
            new TestMessageHandler(1, X, false, 0);
        TestMessageHandler csHandler22 =
            new TestMessageHandler(1, X, true, X);

        subscriber.subscribe(topic1, subid1, CreateOrAttach.CREATE);
        subscriber.subscribe(topic1, subid2, CreateOrAttach.CREATE);
        subscriber.subscribe(topic2, subid1, CreateOrAttach.CREATE);
        subscriber.subscribe(topic2, subid2, CreateOrAttach.CREATE);

        // start deliveries
        subscriber.startDelivery(topic1, subid1, csHandler11);
        subscriber.startDelivery(topic1, subid2, csHandler12);
        subscriber.startDelivery(topic2, subid1, csHandler21);
        subscriber.startDelivery(topic2, subid2, csHandler22);

        // first publish
        for (int i = 1; i<=X; i++) {
            publisher.publish(topic1, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
            publisher.publish(topic2, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
        }

        csHandler11.checkFirstRun();
        csHandler12.checkFirstRun();
        csHandler21.checkFirstRun();
        csHandler22.checkFirstRun();

        // close subscription for <topic1, subscriber2> and <topic2, subscriber1>
        subscriber.closeSubscription(topic1, subid2);
        subscriber.closeSubscription(topic2, subid1);

        // second publish
        for (int i = X+1; i<=2*X; i++) {
            publisher.publish(topic1, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
            publisher.publish(topic2, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
        }

        csHandler11.checkSecondRun();
        csHandler22.checkSecondRun();
        csHandler12.checkSecondRun();
        csHandler21.checkSecondRun();
    }

    @Test
    public void testThrottle() throws Exception {
        ByteString topic1 = ByteString.copyFromUtf8("testThrottle-1");
        ByteString topic2 = ByteString.copyFromUtf8("testThrottle-2");
        ByteString subid1 = ByteString.copyFromUtf8("mysubid-1");
        ByteString subid2 = ByteString.copyFromUtf8("mysubid-2");

        final int X = DEFAULT_MSG_WINDOW_SIZE;

        ThrottleMessageHandler csHandler11 =
            new ThrottleMessageHandler(1, 3*X, false, X);
        ThrottleMessageHandler csHandler12 =
            new ThrottleMessageHandler(1, 3*X, true, X);
        ThrottleMessageHandler csHandler21 =
            new ThrottleMessageHandler(1, 3*X, true, X);
        ThrottleMessageHandler csHandler22 =
            new ThrottleMessageHandler(1, 3*X, false, X);

        subscriber.subscribe(topic1, subid1, CreateOrAttach.CREATE);
        subscriber.subscribe(topic1, subid2, CreateOrAttach.CREATE);
        subscriber.subscribe(topic2, subid1, CreateOrAttach.CREATE);
        subscriber.subscribe(topic2, subid2, CreateOrAttach.CREATE);

        // start deliveries
        subscriber.startDelivery(topic1, subid1, csHandler11);
        subscriber.startDelivery(topic1, subid2, csHandler12);
        subscriber.startDelivery(topic2, subid1, csHandler21);
        subscriber.startDelivery(topic2, subid2, csHandler22);

        // publish
        for (int i = 1; i<=3*X; i++) {
            publisher.publish(topic1, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
            publisher.publish(topic2, Message.newBuilder().setBody(
                                      ByteString.copyFromUtf8(String.valueOf(i))).build());
        }

        csHandler11.checkThrottle();
        csHandler12.checkThrottle();
        csHandler21.checkThrottle();
        csHandler22.checkThrottle();

        // consume messages to not throttle them
        for (int i=1; i<=X; i++) {
            MessageSeqId seqId =
                MessageSeqId.newBuilder().setLocalComponent(i).build();
            subscriber.consume(topic1, subid2, seqId);
            subscriber.consume(topic2, subid1, seqId);
        }

        csHandler11.checkAfterThrottle();
        csHandler22.checkAfterThrottle();
        csHandler12.checkAfterThrottle();
        csHandler21.checkAfterThrottle();
    }
}
