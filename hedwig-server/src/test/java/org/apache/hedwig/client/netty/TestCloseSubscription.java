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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.util.Callback;

/**
 * TODO: it is a temp test for close subscription request. after
 * multiplexing channel manager is implemented, remove this test.
 */
public class TestCloseSubscription extends HedwigHubTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        numServers = 1;
        super.setUp();
    }

    @Test
    public void testCloseSubscriptionRequest() throws Exception {
        HedwigClientImpl client = new HedwigClientImpl(new ClientConfiguration());
        Publisher pub = client.getPublisher();
        Subscriber sub = client.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testCloseSubscriptionRequest"); 
        ByteString subid = ByteString.copyFromUtf8("mysubid");
        sub.subscribe(topic, subid, CreateOrAttach.CREATE);

        final int X = 20;
        final AtomicInteger expected = new AtomicInteger(1);
        final CountDownLatch firstLatch = new CountDownLatch(1);
        final CountDownLatch secondLatch = new CountDownLatch(1);
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
                        firstLatch.countDown();
                        secondLatch.countDown();
                    }
                    if (expected.get() == X+1) {
                        firstLatch.countDown();
                    }
                    if (expected.get() > X+1) {
                        secondLatch.countDown();
                    }
                    callback.operationFinished(context, null);
                } catch (Exception e) {
                    logger.error("Received bad message", e);
                    firstLatch.countDown();
                    secondLatch.countDown();
                }
            }
        });

        // first publish 
        for (int i=1; i<=X; i++) {
            pub.publish(topic, Message.newBuilder().setBody(
                               ByteString.copyFromUtf8(String.valueOf(i))).build());
        }

        assertTrue("Timed out waiting for messages " + (X+1),
                   firstLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected messages with only " + (X+1), X+1, expected.get());

        final CountDownLatch closeSubLatch = new CountDownLatch(1);
        Callback<ResponseBody> closeCb = new Callback<ResponseBody>() {
            @Override
            public void operationFinished(Object ctx, ResponseBody respBody) {
                closeSubLatch.countDown();
            }
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                closeSubLatch.countDown();
            }
        };

        PubSubData pubSubData = new PubSubData(topic, null, subid,
                                               OperationType.CLOSESUBSCRIPTION,
                                               SubscriptionOptions.newBuilder().build(),
                                               closeCb, null);
        client.getHChannelManager().submitOp(pubSubData);
        closeSubLatch.await(10, TimeUnit.SECONDS);

        // second publish
        for (int i=X+1; i<=2*X; i++) {
            pub.publish(topic, Message.newBuilder().setBody(
                               ByteString.copyFromUtf8(String.valueOf(i))).build());
        }

        assertFalse("Receive more messages than " + X,
                    secondLatch.await(3, TimeUnit.SECONDS));
        assertEquals("Should be expected message with " + (X + 1),
                     X + 1, expected.get());

        sub.stopDelivery(topic, subid);
        sub.closeSubscription(topic, subid);

        client.close();
    }

    

}
