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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.PubSubServerStandAloneTestBase;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.SubscriptionListener;
import org.apache.hedwig.util.HedwigSocketAddress;

@RunWith(Parameterized.class)
public class TestPubSubClient extends PubSubServerStandAloneTestBase {

    private static final int RETENTION_SECS_VALUE = 10;

    // Client side variables
    protected HedwigClient client;
    protected Publisher publisher;
    protected Subscriber subscriber;

    protected class RetentionServerConfiguration extends StandAloneServerConfiguration {
        @Override
        public boolean isStandalone() {
            return true;
        }

        @Override
        public int getRetentionSecs() {
            return RETENTION_SECS_VALUE;
        }
    }

    // SynchronousQueues to verify async calls
    private final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
    private final SynchronousQueue<Boolean> consumeQueue = new SynchronousQueue<Boolean>();
    private final SynchronousQueue<SubscriptionEvent> eventQueue =
        new SynchronousQueue<SubscriptionEvent>();

    class TestSubscriptionListener implements SubscriptionListener {
        SynchronousQueue<SubscriptionEvent> eventQueue;
        public TestSubscriptionListener() {
            this.eventQueue = TestPubSubClient.this.eventQueue;
        }
        public TestSubscriptionListener(SynchronousQueue<SubscriptionEvent> queue) {
            this.eventQueue = queue;
        }
        @Override
        public void processEvent(final ByteString topic, final ByteString subscriberId,
                                 final SubscriptionEvent event) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    logger.debug("Event {} received for subscription(topic:{}, subscriber:{})",
                                 new Object[] { event, topic.toStringUtf8(), subscriberId.toStringUtf8() });
                    ConcurrencyUtils.put(TestSubscriptionListener.this.eventQueue, event);
                }
            }).start();
        }
    }

    // Test implementation of Callback for async client actions.
    class TestCallback implements Callback<Void> {

        @Override
        public void operationFinished(Object ctx, Void resultOfOperation) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    if (logger.isDebugEnabled())
                        logger.debug("Operation finished!");
                    ConcurrencyUtils.put(queue, true);
                }
            }).start();
        }

        @Override
        public void operationFailed(Object ctx, final PubSubException exception) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    logger.error("Operation failed!", exception);
                    ConcurrencyUtils.put(queue, false);
                }
            }).start();
        }
    }

    // Test implementation of subscriber's message handler.
    class TestMessageHandler implements MessageHandler {

        private final SynchronousQueue<Boolean> consumeQueue;

        public TestMessageHandler() {
            this.consumeQueue = TestPubSubClient.this.consumeQueue;
        }

        public TestMessageHandler(SynchronousQueue<Boolean> consumeQueue) {
            this.consumeQueue = consumeQueue;
        }

        public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                            Object context) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    if (logger.isDebugEnabled())
                        logger.debug("Consume operation finished successfully!");
                    ConcurrencyUtils.put(TestMessageHandler.this.consumeQueue, true);
                }
            }).start();
            callback.operationFinished(context, null);
        }
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    protected boolean isSubscriptionChannelSharingEnabled;

    public TestPubSubClient(boolean isSubscriptionChannelSharingEnabled) {
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = new HedwigClient(new ClientConfiguration() {
            @Override
            public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
                return getDefaultHedwigAddress();
            }

            @Override
            public boolean isSubscriptionChannelSharingEnabled() {
                return TestPubSubClient.this.isSubscriptionChannelSharingEnabled;
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

    @Test(timeout=60000)
    public void testSyncPublish() throws Exception {
        boolean publishSuccess = true;
        try {
            publisher.publish(ByteString.copyFromUtf8("mySyncTopic"), Message.newBuilder().setBody(
                                  ByteString.copyFromUtf8("Hello Sync World!")).build());
        } catch (Exception e) {
            publishSuccess = false;
        }
        assertTrue(publishSuccess);
    }

    @Test(timeout=60000)
    public void testSyncPublishWithResponse() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("testSyncPublishWithResponse");
        ByteString subid = ByteString.copyFromUtf8("mysubid");

        final String prefix = "SyncMessage-";
        final int numMessages = 30;

        final Map<String, MessageSeqId> publishedMsgs =
            new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> receivedMsgs =
            new HashMap<String, MessageSeqId>();

        subscriber.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        subscriber.startDelivery(topic, subid, new MessageHandler() {
            synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                             Message msg, Callback<Void> callback,
                                             Object context) {
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.put(str, msg.getMsgId()); 
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        for (int i=0; i<numMessages; i++) {
            String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            PublishResponse response = publisher.publish(topic, msg);
            assertNotNull(response);
            publishedMsgs.put(str, response.getPublishedMsgId());
        }

        assertTrue("Timed out waiting on callback for messages.",
                   receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.",
                     numMessages, numReceived.get());
        assertEquals("Should be expected " + numMessages + " messages in map.",
                     numMessages, receivedMsgs.size());

        for (int i=0; i<numMessages; i++) {
            final String str = prefix + i;
            MessageSeqId pubId = publishedMsgs.get(str);
            MessageSeqId revId = receivedMsgs.get(str);
            assertTrue("Doesn't receive same message seq id for " + str,
                       pubId.equals(revId));
        }
    }

    @Test(timeout=60000)
    public void testAsyncPublish() throws Exception {
        publisher.asyncPublish(ByteString.copyFromUtf8("myAsyncTopic"), Message.newBuilder().setBody(
                                   ByteString.copyFromUtf8("Hello Async World!")).build(), new TestCallback(), null);
        assertTrue(queue.take());
    }

    @Test(timeout=60000)
    public void testAsyncPublishWithResponse() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("testAsyncPublishWithResponse");
        ByteString subid = ByteString.copyFromUtf8("mysubid");

        final String prefix = "AsyncMessage-";
        final int numMessages = 30;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs =
            new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> receivedMsgs =
            new HashMap<String, MessageSeqId>();

        subscriber.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        subscriber.startDelivery(topic, subid, new MessageHandler() {
            synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                             Message msg, Callback<Void> callback,
                                             Object context) {
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.put(str, msg.getMsgId()); 
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        for (int i=0; i<numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }
                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);
        }
        assertTrue("Timed out waiting on callback for publish requests.",
                   publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.",
                     numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publishe responses.",
                     numMessages, publishedMsgs.size());

        assertTrue("Timed out waiting on callback for messages.",
                   receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.",
                     numMessages, numReceived.get());
        assertEquals("Should be expected " + numMessages + " messages in map.",
                     numMessages, receivedMsgs.size());

        for (int i=0; i<numMessages; i++) {
            final String str = prefix + i;
            MessageSeqId pubId = publishedMsgs.get(str);
            MessageSeqId revId = receivedMsgs.get(str);
            assertTrue("Doesn't receive same message seq id for " + str,
                       pubId.equals(revId));
        }
    }

    @Test(timeout=60000)
    public void testMultipleAsyncPublish() throws Exception {
        ByteString topic1 = ByteString.copyFromUtf8("myFirstTopic");
        ByteString topic2 = ByteString.copyFromUtf8("myNewTopic");

        publisher.asyncPublish(topic1, Message.newBuilder().setBody(ByteString.copyFromUtf8("Hello World!")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        publisher.asyncPublish(topic2, Message.newBuilder().setBody(ByteString.copyFromUtf8("Hello on new topic!"))
                               .build(), new TestCallback(), null);
        assertTrue(queue.take());
        publisher.asyncPublish(topic1, Message.newBuilder().setBody(
                                   ByteString.copyFromUtf8("Hello Again on old topic!")).build(), new TestCallback(), null);
        assertTrue(queue.take());
    }

    @Test(timeout=60000)
    public void testSyncSubscribe() throws Exception {
        boolean subscribeSuccess = true;
        try {
            subscriber.subscribe(ByteString.copyFromUtf8("mySyncSubscribeTopic"), ByteString.copyFromUtf8("1"), CreateOrAttach.CREATE_OR_ATTACH);
        } catch (Exception e) {
            subscribeSuccess = false;
        }
        assertTrue(subscribeSuccess);
    }

    @Test(timeout=60000)
    public void testAsyncSubscribe() throws Exception {
        subscriber.asyncSubscribe(ByteString.copyFromUtf8("myAsyncSubscribeTopic"), ByteString.copyFromUtf8("1"),
                                  CreateOrAttach.CREATE_OR_ATTACH, new TestCallback(), null);
        assertTrue(queue.take());
    }

    @Test(timeout=60000)
    public void testStartDeliveryAfterCloseSub() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("testStartDeliveryAfterCloseSub");
        ByteString subid = ByteString.copyFromUtf8("mysubid");
        subscriber.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);

        // Start delivery for the subscriber
        subscriber.startDelivery(topic, subid, new TestMessageHandler());

        // Now publish some messages for the topic to be consumed by the
        // subscriber.
        publisher.publish(topic, Message.newBuilder()
                                .setBody(ByteString.copyFromUtf8("Message #1")).build());
        assertTrue(consumeQueue.take());

        // Close subscriber for the subscriber
        subscriber.closeSubscription(topic, subid);

        // subscribe again
        subscriber.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        subscriber.startDelivery(topic, subid, new TestMessageHandler());

        publisher.publish(topic, Message.newBuilder()
                                .setBody(ByteString.copyFromUtf8("Message #2")).build());
        assertTrue(consumeQueue.take());
    }

    @Test(timeout=60000)
    public void testSubscribeAndConsume() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("myConsumeTopic");
        ByteString subscriberId = ByteString.copyFromUtf8("1");
        subscriber.asyncSubscribe(topic, subscriberId, CreateOrAttach.CREATE_OR_ATTACH, new TestCallback(), null);
        assertTrue(queue.take());

        // Start delivery for the subscriber
        subscriber.startDelivery(topic, subscriberId, new TestMessageHandler());

        // Now publish some messages for the topic to be consumed by the
        // subscriber.
        publisher.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #1")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        assertTrue(consumeQueue.take());
        publisher.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #2")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        assertTrue(consumeQueue.take());
        publisher.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #3")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        assertTrue(consumeQueue.take());
        publisher.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #4")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        assertTrue(consumeQueue.take());
        publisher.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #5")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        assertTrue(consumeQueue.take());
    }

    @Test(timeout=60000)
    public void testAsyncSubscribeAndUnsubscribe() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("myAsyncUnsubTopic");
        ByteString subscriberId = ByteString.copyFromUtf8("1");
        subscriber.asyncSubscribe(topic, subscriberId, CreateOrAttach.CREATE_OR_ATTACH, new TestCallback(), null);
        assertTrue(queue.take());
        subscriber.asyncUnsubscribe(topic, subscriberId, new TestCallback(), null);
        assertTrue(queue.take());
    }

    @Test(timeout=60000)
    public void testSyncUnsubscribeWithoutSubscription() throws Exception {
        boolean unsubscribeSuccess = false;
        try {
            subscriber.unsubscribe(ByteString.copyFromUtf8("mySyncUnsubTopic"), ByteString.copyFromUtf8("1"));
        } catch (ClientNotSubscribedException e) {
            unsubscribeSuccess = true;
        } catch (Exception ex) {
            unsubscribeSuccess = false;
        }
        assertTrue(unsubscribeSuccess);
    }

    @Test(timeout=60000)
    public void testAsyncSubscribeAndCloseSubscription() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("myAsyncSubAndCloseSubTopic");
        ByteString subscriberId = ByteString.copyFromUtf8("1");
        subscriber.asyncSubscribe(topic, subscriberId, CreateOrAttach.CREATE_OR_ATTACH, new TestCallback(), null);
        assertTrue(queue.take());
        subscriber.closeSubscription(topic, subscriberId);
        assertTrue(true);
    }

    @Test(timeout=60000)
    public void testSubClosesubAndPublish() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("mySubClosesubAndPublish");
        ByteString subid = ByteString.copyFromUtf8("mysub");
        // to populate startServing/stopServing sequeuence
        for (int i=0; i<5; i++) {
            subscriber.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
            subscriber.closeSubscription(topic, subid);
        }
        subscriber.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        subscriber.startDelivery(topic, subid, new TestMessageHandler());
        for (int i=0; i<3; i++) {
            publisher.asyncPublish(topic,
                Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #" + i)).build(),
                new TestCallback(), null);
            assertTrue(queue.take());
            assertTrue(consumeQueue.take());
        }
    }

    @Test(timeout=60000)
    public void testSyncSubscribeWithListener() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("mySyncSubscribeWithListener");
        ByteString subscriberId = ByteString.copyFromUtf8("mysub");
        subscriber.addSubscriptionListener(new TestSubscriptionListener());
        try {
            SubscriptionOptions options =
                SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
                .setEnableResubscribe(false).build();
            subscriber.subscribe(topic, subscriberId, options);
        } catch (PubSubException.ServiceDownException e) {
            fail("Should not reach here!");
        }
        subscriber.startDelivery(topic, subscriberId, new TestMessageHandler());
        tearDownHubServer();
        assertEquals(SubscriptionEvent.TOPIC_MOVED, eventQueue.take());
    }

    @Test(timeout=60000)
    public void testAsyncSubscribeWithListener() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("myAsyncSubscribeWithListener");
        ByteString subscriberId = ByteString.copyFromUtf8("mysub");
        subscriber.addSubscriptionListener(new TestSubscriptionListener());
        SubscriptionOptions options =
            SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
            .setEnableResubscribe(false).build();
        subscriber.asyncSubscribe(topic, subscriberId, options,
                                  new TestCallback(), null);
        assertTrue(queue.take());
        subscriber.startDelivery(topic, subscriberId, new TestMessageHandler());
        tearDownHubServer();
        assertEquals(SubscriptionEvent.TOPIC_MOVED, eventQueue.take());
    }

    @Test(timeout=60000)
    public void testSyncSubscribeForceAttach() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("mySyncSubscribeForceAttach");
        ByteString subscriberId = ByteString.copyFromUtf8("mysub");
        subscriber.addSubscriptionListener(new TestSubscriptionListener());
        SubscriptionOptions options =
            SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
            .setForceAttach(true).setEnableResubscribe(false).build();
        try {
            subscriber.subscribe(topic, subscriberId, options);
        } catch (PubSubException.ServiceDownException e) {
            fail("Should not reach here!");
        }
        subscriber.startDelivery(topic, subscriberId, new TestMessageHandler());

        // new a client
        HedwigClient client2 = new HedwigClient(new ClientConfiguration() {
                @Override
                public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
                    return getDefaultHedwigAddress();
                }
            });
        Subscriber subscriber2 = client2.getSubscriber();
        Publisher publisher2 = client2.getPublisher();
        SynchronousQueue<SubscriptionEvent> eventQueue2 =
            new SynchronousQueue<SubscriptionEvent>();
        subscriber2.addSubscriptionListener(new TestSubscriptionListener(eventQueue2));
        try {
            subscriber2.subscribe(topic, subscriberId, options);
        } catch (PubSubException.ServiceDownException e) {
            fail("Should not reach here!");
        }

        SynchronousQueue<Boolean> consumeQueue2 = new SynchronousQueue<Boolean>();
        subscriber2.startDelivery(topic, subscriberId, new TestMessageHandler(consumeQueue2));

        assertEquals(SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED, eventQueue.take());
        assertTrue(eventQueue2.isEmpty());

        // Now publish some messages for the topic to be consumed by the
        // subscriber.
        publisher.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #1")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        assertTrue(consumeQueue2.take());
        assertTrue(consumeQueue.isEmpty());

        publisher2.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #2")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        assertTrue(consumeQueue2.take());
        assertTrue(consumeQueue.isEmpty());

        client2.close();
    }

    @Test(timeout=60000)
    public void testSyncSubscribeWithListenerWhenReleasingTopic() throws Exception {
        client.close();

        tearDownHubServer();
        startHubServer(new RetentionServerConfiguration());
        client = new HedwigClient(new ClientConfiguration() {
            @Override
            public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
                return getDefaultHedwigAddress();
            }

            @Override
            public boolean isSubscriptionChannelSharingEnabled() {
                return TestPubSubClient.this.isSubscriptionChannelSharingEnabled;
            }
        });
        publisher = client.getPublisher();
        subscriber = client.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("mySyncSubscribeWithListenerWhenReleasingTopic");
        ByteString subscriberId = ByteString.copyFromUtf8("mysub");
        subscriber.addSubscriptionListener(new TestSubscriptionListener());
        SubscriptionOptions options =
            SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
            .setForceAttach(false).setEnableResubscribe(false).build();
        try {
            subscriber.subscribe(topic, subscriberId, options);
        } catch (PubSubException.ServiceDownException e) {
            fail("Should not reach here!");
        }
        subscriber.startDelivery(topic, subscriberId, new TestMessageHandler());

        publisher.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #1")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());
        assertTrue(consumeQueue.take());

        Thread.sleep(RETENTION_SECS_VALUE * 2);
        assertEquals(SubscriptionEvent.TOPIC_MOVED, eventQueue.take());
    }

    @Test
    public void testCloseSubscribeDuringResubscribe() throws Exception {
        client.close();

        final long reconnectWaitTime = 2000L;
        client = new HedwigClient(new ClientConfiguration() {
            @Override
            public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
                return getDefaultHedwigAddress();
            }

            @Override
            public boolean isSubscriptionChannelSharingEnabled() {
                return TestPubSubClient.this.isSubscriptionChannelSharingEnabled;
            }

            @Override
            public long getSubscribeReconnectRetryWaitTime() {
                return reconnectWaitTime;
            }
        });

        publisher = client.getPublisher();
        subscriber = client.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testCloseSubscribeDuringResubscribe");
        ByteString subscriberId = ByteString.copyFromUtf8("mysub");
        subscriber.addSubscriptionListener(new TestSubscriptionListener());
        SubscriptionOptions options =
            SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
            .setForceAttach(false).setEnableResubscribe(true).build();
        subscriber.subscribe(topic, subscriberId, options);
        logger.info("Subscribed topic {}, subscriber {}.", topic.toStringUtf8(),
                    subscriberId.toStringUtf8());
        subscriber.startDelivery(topic, subscriberId, new TestMessageHandler());

        // tear down the hub server to let subscribe enter
        tearDownHubServer();
        logger.info("Tear down the hub server");

        // wait for client enter to resubscribe logic
        Thread.sleep(reconnectWaitTime / 2);

        // close sub
        subscriber.closeSubscription(topic, subscriberId);

        // start the hub server again
        startHubServer(conf);

        // publish a new message
        publisher.asyncPublish(topic, Message.newBuilder().setBody(ByteString.copyFromUtf8("Message #1")).build(),
                               new TestCallback(), null);
        assertTrue(queue.take());

        // wait for another reconnect time period
        assertNull("Should not receive any messages since the subscription has already been closed.",
                   consumeQueue.poll(reconnectWaitTime + reconnectWaitTime / 2, TimeUnit.MILLISECONDS));
    }

}
