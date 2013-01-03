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
package org.apache.hedwig.server.integration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Client;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.StartDeliveryRequest;
import org.apache.hedwig.protocol.PubSubProtocol.StopDeliveryRequest;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.netty.WriteRecordingChannel;
import org.apache.hedwig.server.proxy.HedwigProxy;
import org.apache.hedwig.server.proxy.ProxyConfiguration;
import org.apache.hedwig.server.regions.HedwigHubClient;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.bookkeeper.test.PortManager;
import org.apache.hedwig.server.LoggingExceptionHandler;

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

    private void sleepThread(final String name, final CountDownLatch wakeupLatch)
    throws IOException {
        Thread[] allthreads = new Thread[Thread.activeCount()];
        Thread.enumerate(allthreads);
        for (final Thread t : allthreads) {
            if (t.getName().equals(name)) {
                Thread sleeper = new Thread() {
                    @Override
                    public void run() {
                        try {
                            t.suspend();
                            wakeupLatch.await();
                            t.resume();
                        } catch (Exception e) {
                            logger.error("Error suspending thread " + name + " : ", e);
                        }
                    }
                };
                sleeper.start();
                return;
            }
        }
        throw new IOException("Could not find thread " + name);
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
            subscriber.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
            sleepThread("DeliveryManagerThread", wakeupLatch);
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
            subscriber.asyncSubscribe(topic, subid, CreateOrAttach.ATTACH, new Callback<Void>() {
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

}

