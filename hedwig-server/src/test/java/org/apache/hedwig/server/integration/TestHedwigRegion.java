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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.server.HedwigRegionTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.integration.TestHedwigHub.TestCallback;
import org.apache.hedwig.server.integration.TestHedwigHub.TestMessageHandler;
import org.apache.hedwig.util.HedwigSocketAddress;

@RunWith(Parameterized.class)
public class TestHedwigRegion extends HedwigRegionTestBase {

    // SynchronousQueues to verify async calls
    private final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
    private final SynchronousQueue<Boolean> consumeQueue = new SynchronousQueue<Boolean>();

    private static final int TEST_RETRY_REMOTE_SUBSCRIBE_INTERVAL_VALUE = 3000;

    protected class NewRegionServerConfiguration extends RegionServerConfiguration {

        public NewRegionServerConfiguration(int serverPort, int sslServerPort,
                String regionName) {
            super(serverPort, sslServerPort, regionName);
        }

        @Override
        public int getRetryRemoteSubscribeThreadRunInterval() {
            return TEST_RETRY_REMOTE_SUBSCRIBE_INTERVAL_VALUE;
        }

    }

    protected class NewRegionClientConfiguration extends ClientConfiguration {
        @Override
        public boolean isSubscriptionChannelSharingEnabled() {
            return isSubscriptionChannelSharingEnabled;
        }
        @Override
        public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
            return regionHubAddresses.get(0).get(0);
        }
    }

    protected ServerConfiguration getServerConfiguration(int serverPort, int sslServerPort, String regionName) {
        return new NewRegionServerConfiguration(serverPort, sslServerPort, regionName);
    }

    protected ClientConfiguration getRegionClientConfiguration() {
        return new NewRegionClientConfiguration();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    protected boolean isSubscriptionChannelSharingEnabled;

    public TestHedwigRegion(boolean isSubscriptionChannelSharingEnabled) {
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        numRegions = 3;
        numServersPerRegion = 4;
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test(timeout=60000)
    public void testMultiRegionSubscribeAndConsume() throws Exception {
        int batchSize = 10;
        // Subscribe to topics for clients in all regions
        for (HedwigClient client : regionClientsMap.values()) {
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertTrue(queue.take());
            }
        }

        // Start delivery for the local subscribers in all regions
        for (HedwigClient client : regionClientsMap.values()) {
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().startDelivery(ByteString.copyFromUtf8("Topic" + i),
                                                     ByteString.copyFromUtf8("LocalSubscriber"), new TestMessageHandler(consumeQueue));
            }
        }

        // Now start publishing messages for the subscribed topics in one of the
        // regions and verify that it gets delivered and consumed in all of the
        // other ones.
        Publisher publisher = regionClientsMap.values().iterator().next().getPublisher();
        for (int i = 0; i < batchSize; i++) {
            publisher.asyncPublish(ByteString.copyFromUtf8("Topic" + i), Message.newBuilder().setBody(
                                       ByteString.copyFromUtf8("Message" + i)).build(), new TestCallback(queue), null);
            assertTrue(queue.take());
        }
        // Make sure each region consumes the same set of published messages.
        for (int i = 0; i < regionClientsMap.size(); i++) {
            for (int j = 0; j < batchSize; j++) {
                assertTrue(consumeQueue.take());
            }
        }
    }

    /**
     * Test region shuts down when first subscription.
     *
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testSubscribeAndConsumeWhenARegionDown() throws Exception {
        int batchSize = 10;

        // first shut down a region
        Random r = new Random();
        int regionId = r.nextInt(numRegions);
        stopRegion(regionId);
        // subscribe to topics when a region shuts down
        for (HedwigClient client : regionClientsMap.values()) {
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertFalse(queue.take());
            }
        }

        // start region gain
        startRegion(regionId);

        // sub it again
        for (Map.Entry<String, HedwigClient> entry : regionClientsMap.entrySet()) {
            HedwigClient client = entry.getValue();
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertTrue(queue.take());
            }
        }

        // Start delivery for local subscribers in all regions
        for (Map.Entry<String, HedwigClient> entry : regionClientsMap.entrySet()) {
            HedwigClient client = entry.getValue();
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().startDelivery(ByteString.copyFromUtf8("Topic" + i),
                                                     ByteString.copyFromUtf8("LocalSubscriber"), new TestMessageHandler(consumeQueue));
            }
        }

        // Now start publishing messages for the subscribed topics in one of the
        // regions and verify that it gets delivered and consumed in all of the
        // other ones.
        int rid = r.nextInt(numRegions);
        String regionName = REGION_PREFIX + rid;
        Publisher publisher = regionClientsMap.get(regionName).getPublisher();
        for (int i = 0; i < batchSize; i++) {
            publisher.asyncPublish(ByteString.copyFromUtf8("Topic" + i), Message.newBuilder().setBody(
                                   ByteString.copyFromUtf8(regionName + "-Message" + i)).build(), new TestCallback(queue), null);
            assertTrue(queue.take());
        }
        // Make sure each region consumes the same set of published messages.
        for (int i = 0; i < regionClientsMap.size(); i++) {
            for (int j = 0; j < batchSize; j++) {
                assertTrue(consumeQueue.take());
            }
        }
    }

    /**
     * Test region shuts down when attaching existing subscriptions.
     *
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testAttachExistingSubscriptionsWhenARegionDown() throws Exception {
        int batchSize = 10;
        
        // sub it remotely to make subscriptions existed
        for (Map.Entry<String, HedwigClient> entry : regionClientsMap.entrySet()) {
            HedwigClient client = entry.getValue();
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertTrue(queue.take());
            }
        }

        // stop regions
        for (int i=0; i<numRegions; i++) {
            stopRegion(i);
        }
        // start regions again
        for (int i=0; i<numRegions; i++) {
            startRegion(i);
        }

        // first shut down a region
        Random r = new Random();
        int regionId = r.nextInt(numRegions);
        stopRegion(regionId);
        // subscribe to topics when a region shuts down
        // it should succeed since the subscriptions existed before
        for (HedwigClient client : regionClientsMap.values()) {
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertTrue(queue.take());
            }
        }

        // Start delivery for local subscribers in all regions
        for (Map.Entry<String, HedwigClient> entry : regionClientsMap.entrySet()) {
            HedwigClient client = entry.getValue();
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().startDelivery(ByteString.copyFromUtf8("Topic" + i),
                                                     ByteString.copyFromUtf8("LocalSubscriber"), new TestMessageHandler(consumeQueue));
            }
        }

        // start region again
        startRegion(regionId);
        // wait for retry
        Thread.sleep(3 * TEST_RETRY_REMOTE_SUBSCRIBE_INTERVAL_VALUE);

        String regionName = REGION_PREFIX + regionId;
        HedwigClient client = regionClientsMap.get(regionName);
        for (int i = 0; i < batchSize; i++) {
            client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                  ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                  new TestCallback(queue), null);
            assertTrue(queue.take());
            client.getSubscriber().startDelivery(ByteString.copyFromUtf8("Topic" + i),
                    ByteString.copyFromUtf8("LocalSubscriber"), new TestMessageHandler(consumeQueue));
        }

        // Now start publishing messages for the subscribed topics in one of the
        // regions and verify that it gets delivered and consumed in all of the
        // other ones.        
        Publisher publisher = client.getPublisher();
        for (int i = 0; i < batchSize; i++) {
            publisher.asyncPublish(ByteString.copyFromUtf8("Topic" + i), Message.newBuilder().setBody(
                                   ByteString.copyFromUtf8(regionName + "-Message" + i)).build(), new TestCallback(queue), null);
            assertTrue(queue.take());
        }
        // Make sure each region consumes the same set of published messages.
        for (int i = 0; i < regionClientsMap.size(); i++) {
            for (int j = 0; j < batchSize; j++) {
                assertTrue(consumeQueue.take());
            }
        }
    }
}
