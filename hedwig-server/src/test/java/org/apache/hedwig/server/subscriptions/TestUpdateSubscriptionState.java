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
package org.apache.hedwig.server.subscriptions;

import java.util.concurrent.SynchronousQueue;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestUpdateSubscriptionState extends HedwigHubTestBase {

    private static final int RETENTION_SECS_VALUE = 100;

    // Client side variables
    protected HedwigClient client;
    protected Publisher publisher;
    protected Subscriber subscriber;

    // SynchronousQueues to verify async calls
    private final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();

    // Test implementation of subscriber's message handler
    class OrderCheckingMessageHandler implements MessageHandler {

        ByteString topic;
        ByteString subscriberId;
        int startMsgId;
        int numMsgs;
        int endMsgId;
        boolean inOrder = true;

        OrderCheckingMessageHandler(ByteString topic, ByteString subscriberId,
                                    int startMsgId, int numMsgs) {
            this.topic = topic;
            this.subscriberId = subscriberId;
            this.startMsgId = startMsgId;
            this.numMsgs = numMsgs;
            this.endMsgId = startMsgId + numMsgs - 1;
        }

        @Override
        public void deliver(ByteString thisTopic, ByteString thisSubscriberId,
                            Message msg, Callback<Void> callback, Object context) {
            if (!topic.equals(thisTopic) ||
                !subscriberId.equals(thisSubscriberId)) {
                return;
            }
            // check order
            int msgId = Integer.parseInt(msg.getBody().toStringUtf8());
            if (logger.isDebugEnabled()) {
                logger.debug("Received message : " + msgId);
            }

            if (inOrder) {
                if (startMsgId != msgId) {
                    logger.error("Expected message " + startMsgId + ", but received message " + msgId);
                    inOrder = false;
                } else {
                    ++startMsgId;
                }
            }
            callback.operationFinished(context, null);
            if (msgId == endMsgId) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Deliver finished!");
                        }
                        ConcurrencyUtils.put(queue, true);
                    }
                }).start();
            }
        }

        public boolean isInOrder() {
            return inOrder;
        }
    }

    public TestUpdateSubscriptionState() {
        super(1);
    }

    protected class NewHubServerConfiguration extends HubServerConfiguration {

        public NewHubServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }

        @Override
        public int getRetentionSecs() {
            return RETENTION_SECS_VALUE;
        }

    }

    @Override
    protected ServerConfiguration getServerConfiguration(int serverPort, int sslServerPort) {
        return new NewHubServerConfiguration(serverPort, sslServerPort);
    }

    protected class TestClientConfiguration extends HubClientConfiguration {
        @Override
        public boolean isAutoSendConsumeMessageEnabled() {
            return true;
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = new HedwigClient(new TestClientConfiguration());
        publisher = client.getPublisher();
        subscriber = client.getSubscriber();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        client.close();
        super.tearDown();
    }

    @Test
    public void testConsumeWhenTopicRelease() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestConsumeWhenTopicRelease");
        ByteString subId = ByteString.copyFromUtf8("mysub");

        int startMsgId = 0;
        int numMsgs = 10;
        // subscriber in client
        subscriber.subscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH);
        // start delivery
        OrderCheckingMessageHandler ocm = new OrderCheckingMessageHandler(
                topic, subId, startMsgId, numMsgs);
        subscriber.startDelivery(topic, subId, ocm);
        for (int i=0; i<numMsgs; i++) {
            Message msg = Message.newBuilder().setBody(
                    ByteString.copyFromUtf8(Integer.toString(startMsgId + i))).build();
            publisher.publish(topic, msg);
        }
        logger.info("Publish finished.");
        queue.take();
        logger.info("Deliver finished.");
        // check messages received in order
        assertTrue(ocm.isInOrder());

        // wait for retention secs
        Thread.sleep((RETENTION_SECS_VALUE + 2) * 1000);

        subscriber.stopDelivery(topic, subId);
        subscriber.closeSubscription(topic, subId);

        startMsgId = 20;
        // reconnect it again
        subscriber.subscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH);
        ocm = new OrderCheckingMessageHandler(topic, subId, startMsgId, numMsgs);
        subscriber.startDelivery(topic, subId, ocm);
        for (int i=0; i<numMsgs; i++) {
            Message msg = Message.newBuilder().setBody(
                    ByteString.copyFromUtf8(Integer.toString(startMsgId + i))).build();
            publisher.publish(topic, msg);
        }
        queue.take();
        // check messages received in order
        assertTrue(ocm.isInOrder());
    }

    @Test
    public void testConsumeWhenHubShutdown() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestConsumeWhenHubShutdown");
        ByteString subId = ByteString.copyFromUtf8("mysub");

        int startMsgId = 0;
        int numMsgs = 10;
        // subscriber in client
        subscriber.subscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH);
        // start delivery
        OrderCheckingMessageHandler ocm = new OrderCheckingMessageHandler(
                topic, subId, startMsgId, numMsgs);
        subscriber.startDelivery(topic, subId, ocm);
        for (int i=0; i<numMsgs; i++) {
            Message msg = Message.newBuilder().setBody(
                    ByteString.copyFromUtf8(Integer.toString(startMsgId + i))).build();
            publisher.publish(topic, msg);
        }
        logger.info("Publish finished.");
        queue.take();
        logger.info("Deliver finished.");
        // check messages received in order
        assertTrue(ocm.isInOrder());
        // make sure consume request sent to hub server before shut down
        Thread.sleep(2000);
        subscriber.stopDelivery(topic, subId);
        subscriber.closeSubscription(topic, subId);

        stopHubServers();
        Thread.sleep(1000);
        startHubServers();

        startMsgId = 20;
        // reconnect it again
        subscriber.subscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH);
        ocm = new OrderCheckingMessageHandler(topic, subId, startMsgId, numMsgs);
        subscriber.startDelivery(topic, subId, ocm);
        for (int i=0; i<numMsgs; i++) {
            Message msg = Message.newBuilder().setBody(
                    ByteString.copyFromUtf8(Integer.toString(startMsgId + i))).build();
            publisher.publish(topic, msg);
        }
        queue.take();
        // check messages received in order
        assertTrue(ocm.isInOrder());
    }
}
