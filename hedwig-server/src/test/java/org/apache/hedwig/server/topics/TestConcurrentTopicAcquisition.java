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
package org.apache.hedwig.server.topics;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestConcurrentTopicAcquisition extends HedwigHubTestBase {

    // Client variables
    protected HedwigClient client;
    protected Publisher publisher;
    protected Subscriber subscriber;
    
    final LinkedBlockingQueue<ByteString> subscribers =
            new LinkedBlockingQueue<ByteString>();
    final ByteString topic = ByteString.copyFromUtf8("concurrent-topic");
    final int numSubscribers = 300;
    final AtomicInteger numDone = new AtomicInteger(0);

    // SynchronousQueues to verify async calls
    private final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
    
    class SubCallback implements Callback<Void> {
        
        ByteString subId;
        
        public SubCallback(ByteString subId) {
            this.subId = subId;
        }

        @Override
        public void operationFinished(Object ctx,
                Void resultOfOperation) {
            if (logger.isDebugEnabled()) {
                logger.debug("subscriber " + subId.toStringUtf8() + " succeed.");
            }
            int done = numDone.incrementAndGet();
            if (done == numSubscribers) {
                ConcurrencyUtils.put(queue, false);
            }
        }

        @Override
        public void operationFailed(Object ctx,
                PubSubException exception) {
            if (logger.isDebugEnabled()) {
                logger.debug("subscriber " + subId.toStringUtf8() + " failed : ", exception);
            }
            ConcurrencyUtils.put(subscribers, subId);
            // ConcurrencyUtils.put(queue, false);
        }
    }
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = new HedwigClient(new HubClientConfiguration());

        publisher = client.getPublisher();
        subscriber = client.getSubscriber();
    }

    @Override
    public void tearDown() throws Exception {
        // sub.interrupt();
        // sub.join();
        
        client.close();
        super.tearDown();
    }
    
    @Test
    public void testTopicAcquistion() throws Exception {
        logger.info("Start concurrent topic acquistion test.");
        
        // let one bookie down to cause not enough bookie exception
        logger.info("Tear down one bookie server.");
        bktb.tearDownOneBookieServer();
        
        // In current implementation, the first several subscriptions will succeed to put topic in topic manager set,
        // because the tear down bookie server's zk node need time to disappear
        // some subscriptions will create ledger successfully, then other subscriptions will fail.
        // the race condition will be: topic manager own topic but persistence manager doesn't
        
        // 300 subscribers subscribe to a same topic
        final AtomicBoolean inRedirectLoop = new AtomicBoolean(false);
        numDone.set(0);
        for (int i=0; i<numSubscribers; i++) {
            ByteString subId = ByteString.copyFromUtf8("sub-" + i);
            if (logger.isDebugEnabled()) {
                logger.debug("subscriber " + subId.toStringUtf8() + " subscribes topic " + topic.toStringUtf8());
            }
            subscriber.asyncSubscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH,
                new Callback<Void>() {
                
                    private void tick() {
                        if (numDone.incrementAndGet() == numSubscribers) {
                            ConcurrencyUtils.put(queue, true);
                        }
                    }

                    @Override
                    public void operationFinished(Object ctx,
                            Void resultOfOperation) {
                        tick();
                    }

                    @Override
                    public void operationFailed(Object ctx,
                            PubSubException exception) {
                        if (exception instanceof PubSubException.ServiceDownException) {
                            String msg = exception.getMessage();
                            if (msg.indexOf("ServerRedirectLoopException") > 0) {
                                inRedirectLoop.set(true);
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug("Operation failed : ", exception);
                            }
                        }
                        tick(); 
                    }
                
                },
            null);
        }
        
        queue.take();
        
        // TODO: remove comment after we fix the issue
        // Assert.assertEquals(false, inRedirectLoop.get());
        
        // start a thread to send subscriptions
        numDone.set(0);
        Thread sub = new Thread(new Runnable() {

            @Override
            public void run() {
                logger.info("sub thread started");
                try {
                    // 100 subscribers subscribe to a same topic
                    for (int i=0; i<numSubscribers; i++) {
                        ByteString subscriberId = ByteString.copyFromUtf8("sub-" + i);
                        subscribers.put(subscriberId);
                    }
                    
                    ByteString subId;
                    while (true) {
                        subId = subscribers.take();
                        
                        if (logger.isDebugEnabled()) {
                            logger.debug("subscriber " + subId.toStringUtf8() + " subscribes topic " + topic.toStringUtf8());
                        }
                        subscriber.asyncSubscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH,
                            new SubCallback(subId), null);
                    }
                    // subscriber.asyncSubscribe(topic, subscriberId, mode, callback, context)
                } catch (InterruptedException ie) {
                    // break
                    logger.warn("Interrupted : ", ie);
                }
            }

        });
        sub.start();
        Thread.sleep(2000);
        
        // start a new bookie server
        logger.info("start new bookie server");
        bktb.startUpNewBookieServer();
        
        // hope that all the subscriptions will be OK
        queue.take();
    }

}
