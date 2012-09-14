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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

import com.google.protobuf.ByteString;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.persistence.LocalDBPersistenceManager;
import org.apache.hedwig.server.topics.TrivialOwnAllTopicManager;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.zookeeper.ZooKeeperTestBase;

public class TestMMSubscriptionManager extends ZooKeeperTestBase {
    MetadataManagerFactory mm;
    MMSubscriptionManager sm;
    ServerConfiguration cfg = new ServerConfiguration();
    SynchronousQueue<Either<SubscriptionData, PubSubException>> subDataCallbackQueue = new SynchronousQueue<Either<SubscriptionData, PubSubException>>();
    SynchronousQueue<Either<Boolean, PubSubException>> BooleanCallbackQueue = new SynchronousQueue<Either<Boolean, PubSubException>>();

    Callback<Void> voidCallback;
    Callback<SubscriptionData> subDataCallback;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        cfg = new ServerConfiguration();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        mm = MetadataManagerFactory.newMetadataManagerFactory(cfg, zk);
        sm = new MMSubscriptionManager(cfg, mm, new TrivialOwnAllTopicManager(cfg, scheduler),
                                       LocalDBPersistenceManager.instance(), null, scheduler);
        subDataCallback = new Callback<SubscriptionData>() {
            @Override
            public void operationFailed(Object ctx, final PubSubException exception) {
                scheduler.execute(new Runnable() {
                    public void run() {
                        ConcurrencyUtils.put(subDataCallbackQueue, Either.of((SubscriptionData) null, exception));
                    }
                });
            }

            @Override
            public void operationFinished(Object ctx, final SubscriptionData resultOfOperation) {
                scheduler.execute(new Runnable() {
                    public void run() {
                        ConcurrencyUtils.put(subDataCallbackQueue, Either.of(resultOfOperation, (PubSubException) null));
                    }
                });
            }
        };

        voidCallback = new Callback<Void>() {
            @Override
            public void operationFailed(Object ctx, final PubSubException exception) {
                scheduler.execute(new Runnable() {
                    public void run() {
                        ConcurrencyUtils.put(BooleanCallbackQueue, Either.of((Boolean) null, exception));
                    }
                });
            }

            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                scheduler.execute(new Runnable() {
                    public void run() {
                        ConcurrencyUtils.put(BooleanCallbackQueue, Either.of(true, (PubSubException) null));
                    }
                });
            }
        };

    }

    @Test
    public void testBasics() throws Exception {

        ByteString topic1 = ByteString.copyFromUtf8("topic1");
        ByteString sub1 = ByteString.copyFromUtf8("sub1");

        //
        // No topics acquired.
        //
        SubscribeRequest subRequest = SubscribeRequest.newBuilder().setSubscriberId(sub1).build();
        MessageSeqId msgId = MessageSeqId.newBuilder().setLocalComponent(100).build();

        sm.serveSubscribeRequest(topic1, subRequest, msgId, subDataCallback, null);

        Assert.assertEquals(ConcurrencyUtils.take(subDataCallbackQueue).right().getClass(),
                            PubSubException.ServerNotResponsibleForTopicException.class);

        sm.unsubscribe(topic1, sub1, voidCallback, null);

        Assert.assertEquals(ConcurrencyUtils.take(BooleanCallbackQueue).right().getClass(),
                            PubSubException.ServerNotResponsibleForTopicException.class);

        //
        // Acquire topic.
        //

        sm.acquiredTopic(topic1, voidCallback, null);
        Assert.assertTrue(BooleanCallbackQueue.take().left());

        Assert.assertTrue(sm.top2sub2seq.containsKey(topic1));
        Assert.assertEquals(0, sm.top2sub2seq.get(topic1).size());

        sm.unsubscribe(topic1, sub1, voidCallback, null);
        Assert.assertEquals(ConcurrencyUtils.take(BooleanCallbackQueue).right().getClass(),
                            PubSubException.ClientNotSubscribedException.class);

        //
        // Try to attach to a subscription.
        subRequest = SubscribeRequest.newBuilder().setCreateOrAttach(CreateOrAttach.ATTACH).setSubscriberId(sub1)
                     .build();

        sm.serveSubscribeRequest(topic1, subRequest, msgId, subDataCallback, null);
        Assert.assertEquals(ConcurrencyUtils.take(subDataCallbackQueue).right().getClass(),
                            PubSubException.ClientNotSubscribedException.class);

        // now create
        subRequest = SubscribeRequest.newBuilder().setCreateOrAttach(CreateOrAttach.CREATE).setSubscriberId(sub1)
                     .build();
        sm.serveSubscribeRequest(topic1, subRequest, msgId, subDataCallback, null);
        Assert.assertEquals(msgId.getLocalComponent(), ConcurrencyUtils.take(subDataCallbackQueue).left().getState().getMsgId().getLocalComponent());
        Assert.assertEquals(msgId.getLocalComponent(), sm.top2sub2seq.get(topic1).get(sub1).getLastConsumeSeqId()
                            .getLocalComponent());

        // try to create again
        sm.serveSubscribeRequest(topic1, subRequest, msgId, subDataCallback, null);
        Assert.assertEquals(ConcurrencyUtils.take(subDataCallbackQueue).right().getClass(),
                            PubSubException.ClientAlreadySubscribedException.class);
        Assert.assertEquals(msgId.getLocalComponent(), sm.top2sub2seq.get(topic1).get(sub1).getLastConsumeSeqId()
                            .getLocalComponent());

        sm.lostTopic(topic1);
        sm.acquiredTopic(topic1, voidCallback, null);
        Assert.assertTrue(BooleanCallbackQueue.take().left());

        // try to attach
        subRequest = SubscribeRequest.newBuilder().setCreateOrAttach(CreateOrAttach.ATTACH).setSubscriberId(sub1)
                     .build();
        MessageSeqId msgId1 = MessageSeqId.newBuilder().setLocalComponent(msgId.getLocalComponent() + 10).build();
        sm.serveSubscribeRequest(topic1, subRequest, msgId1, subDataCallback, null);
        Assert.assertEquals(msgId.getLocalComponent(), subDataCallbackQueue.take().left().getState().getMsgId().getLocalComponent());
        Assert.assertEquals(msgId.getLocalComponent(), sm.top2sub2seq.get(topic1).get(sub1).getLastConsumeSeqId()
                            .getLocalComponent());

        // now manipulate the consume ptrs
        // dont give it enough to have it persist to ZK
        MessageSeqId msgId2 = MessageSeqId.newBuilder().setLocalComponent(
                                  msgId.getLocalComponent() + cfg.getConsumeInterval() - 1).build();
        sm.setConsumeSeqIdForSubscriber(topic1, sub1, msgId2, voidCallback, null);
        Assert.assertTrue(BooleanCallbackQueue.take().left());
        Assert.assertEquals(msgId2.getLocalComponent(), sm.top2sub2seq.get(topic1).get(sub1).getLastConsumeSeqId()
                            .getLocalComponent());
        Assert.assertEquals(msgId.getLocalComponent(), sm.top2sub2seq.get(topic1).get(sub1).getSubscriptionState().getMsgId()
                            .getLocalComponent());

        // give it more so that it will write to ZK
        MessageSeqId msgId3 = MessageSeqId.newBuilder().setLocalComponent(
                                  msgId.getLocalComponent() + cfg.getConsumeInterval() + 1).build();
        sm.setConsumeSeqIdForSubscriber(topic1, sub1, msgId3, voidCallback, null);
        Assert.assertTrue(BooleanCallbackQueue.take().left());

        sm.lostTopic(topic1);
        sm.acquiredTopic(topic1, voidCallback, null);
        Assert.assertTrue(BooleanCallbackQueue.take().left());

        Assert.assertEquals(msgId3.getLocalComponent(), sm.top2sub2seq.get(topic1).get(sub1).getLastConsumeSeqId()
                            .getLocalComponent());
        Assert.assertEquals(msgId3.getLocalComponent(), sm.top2sub2seq.get(topic1).get(sub1).getSubscriptionState().getMsgId()
                            .getLocalComponent());

        // finally unsubscribe
        sm.unsubscribe(topic1, sub1, voidCallback, null);
        Assert.assertTrue(BooleanCallbackQueue.take().left());

        sm.lostTopic(topic1);
        sm.acquiredTopic(topic1, voidCallback, null);
        Assert.assertTrue(BooleanCallbackQueue.take().left());
        Assert.assertFalse(sm.top2sub2seq.get(topic1).containsKey(sub1));

    }

}
