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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;

public class TestDeadlock extends HedwigHubTestBase {

    protected static Logger logger = LoggerFactory.getLogger(TestDeadlock.class);

    // Client side variables
    protected HedwigClient client;
    protected Publisher publisher;
    protected Subscriber subscriber;

    ByteString topic = ByteString.copyFromUtf8("DeadLockTopic");
    ByteString subscriberId = ByteString.copyFromUtf8("dl");

    public TestDeadlock() {
        super(1);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        numBookies = 1;
        readDelay = 1000L; // 1s
        super.setUp();
        client = new HedwigClient(new HubClientConfiguration());
        publisher = client.getPublisher();
        subscriber = client.getSubscriber();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        client.close();
        super.tearDown();
    }

    // Test implementation of Callback for async client actions.
    static class TestCallback implements Callback<Void> {
        private final SynchronousQueue<Boolean> queue;

        public TestCallback(SynchronousQueue<Boolean> queue) {
            this.queue = queue;
        }

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
        boolean doAdd = false;

        public TestMessageHandler(SynchronousQueue<Boolean> consumeQueue) {
            this.consumeQueue = consumeQueue;
        }

        public void deliver(ByteString t, ByteString sub, final Message msg, Callback<Void> callback,
                            Object context) {
            if (!doAdd) {
                // after receiving first message, we send a publish
                // to obtain permit of second ledger
                doAdd = true;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // publish messages again to obtain permits
                        logger.info("Start publishing message to obtain permit");
                        // it obtains the permit and wait for a response,
                        // but the response is delayed and readEntries is called
                        // in the readComplete callback to read entries of the
                        // same ledger. since there is no permit, it blocks
                        try {
                            CountDownLatch latch = new CountDownLatch(1);
                            sleepBookies(8, latch);
                            latch.await();
                            SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
                            for (int i=0; i<3; i++) {
                                publisher.asyncPublish(topic, getMsg(9999), new TestCallback(queue), null);
                            }
                            for (int i=0; i<3; i++) {
                                if (!queue.take()) {
                                    logger.error("Error publishing to topic {}", topic);
                                    ConcurrencyUtils.put(consumeQueue, false);
                                }
                            }
                        } catch (Exception e) {
                            logger.error("Failed to publish message to obtain permit.");
                        }
                    }
                }).start();
            }

            new Thread(new Runnable() {
                @Override
                public void run() {
                    ConcurrencyUtils.put(consumeQueue, true);
                }
            }).start();
            callback.operationFinished(context, null);
        }
    }

    // Helper function to generate Messages
    protected Message getMsg(int msgNum) {
        return Message.newBuilder().setBody(ByteString.copyFromUtf8("Message" + msgNum)).build();
    }

    // Helper function to generate Topics
    protected ByteString getTopic(int topicNum) {
        return ByteString.copyFromUtf8("DeadLockTopic" + topicNum);
    }

    class TestServerConfiguration extends HubServerConfiguration {
        public TestServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }
        @Override
        public int getBkEnsembleSize() {
            return 1;
        }
        @Override
        public int getBkWriteQuorumSize() {
            return 1;
        }
        @Override
        public int getBkAckQuorumSize() {
            return 1;
        }
        @Override
        public int getReadAheadCount() {
            return 4;
        }
        @Override
        public long getMaximumCacheSize() {
            return 32;
        }
    }

    @Override
    protected ServerConfiguration getServerConfiguration(int serverPort, int sslServerPort) {
        ServerConfiguration serverConf = new TestServerConfiguration(serverPort, sslServerPort);

        org.apache.bookkeeper.conf.ClientConfiguration bkClientConf =
            new org.apache.bookkeeper.conf.ClientConfiguration();
        bkClientConf.setNumWorkerThreads(1).setReadTimeout(9999)
                    .setThrottleValue(3);
        try {
            serverConf.addConf(bkClientConf);
        } catch (Exception e) {
        }
        return serverConf;
    }

    @Test(timeout=60000)
    public void testDeadlock() throws Exception {
        int numMessages = 5;

        SynchronousQueue<Boolean> consumeQueue = new SynchronousQueue<Boolean>();

        // subscribe to topic
        logger.info("Setup subscriptions");
        SubscriptionOptions opts = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).build();
        subscriber.subscribe(topic, subscriberId, opts);
        subscriber.closeSubscription(topic, subscriberId);

        // publish 5 messages to form first ledger
        for (int i=0; i<numMessages; i++) {
            logger.info("Start publishing message {}", i);
            publisher.publish(topic, getMsg(i));
        }

        stopHubServers();
        Thread.sleep(1000);
        startHubServers();

        logger.info("Start publishing messages");
        // publish enough messages to second ledger
        // so a scan request need to scan over two ledgers, which
        // cause readEntries executed in the previous readEntries
        for (int i=0; i<numMessages; i++) {
            logger.info("Start publishing message {}", i+5);
            publisher.publish(topic, getMsg(i));
        }

        logger.info("Start subscribe topics again and receive messages");
        // subscribe to topic
        subscriber.subscribe(topic, subscriberId, opts);
        subscriber.startDelivery(topic, subscriberId,
                                 new TestMessageHandler(consumeQueue));
        for (int i=0; i<(2*numMessages+3); i++) {
            assertTrue(consumeQueue.take());
        }
    }

    protected void sleepBookies(final int seconds, final CountDownLatch l)
            throws InterruptedException, IOException {
        Thread sleeper = new Thread() {
                public void run() {
                    try {
                        bktb.suspendAllBookieServers();
                        l.countDown();
                        Thread.sleep(seconds * 1000);
                        bktb.resumeAllBookieServers();
                    } catch (Exception e) {
                        logger.error("Error suspending thread", e);
                    }
                }
            };
        sleeper.start();
    }
}
