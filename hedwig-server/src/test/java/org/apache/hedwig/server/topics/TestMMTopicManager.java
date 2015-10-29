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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import org.apache.hedwig.StubCallback;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.CompositeException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.MetadataManagerFactoryTestCase;
import org.apache.hedwig.server.meta.TopicOwnershipManager;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.hedwig.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMMTopicManager extends MetadataManagerFactoryTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestMMTopicManager.class);

    protected MMTopicManager tm;
    protected TopicOwnershipManager tom;

    protected class CallbackQueue<T> implements Callback<T> {
        SynchronousQueue<Either<T, Exception>> q = new SynchronousQueue<Either<T, Exception>>();

        public SynchronousQueue<Either<T, Exception>> getQueue() {
            return q;
        }

        public Either<T, Exception> take() throws InterruptedException {
            return q.take();
        }

        @Override
        public void operationFailed(Object ctx, final PubSubException exception) {
            LOG.error("got exception: " + exception);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ConcurrencyUtils.put(q, Either.of((T) null, (Exception) exception));
                }
            }).start();
        }

        @Override
        public void operationFinished(Object ctx, final T resultOfOperation) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ConcurrencyUtils.put(q, Either.of(resultOfOperation, (Exception) null));
                }
            }).start();
        }
    }

    protected CallbackQueue<HedwigSocketAddress> addrCbq = new CallbackQueue<HedwigSocketAddress>();
    protected CallbackQueue<ByteString> bsCbq = new CallbackQueue<ByteString>();
    protected CallbackQueue<Void> voidCbq = new CallbackQueue<Void>();

    protected ByteString topic = ByteString.copyFromUtf8("topic");
    protected HedwigSocketAddress me;
    protected ScheduledExecutorService scheduler;

    public TestMMTopicManager(String metaManagerCls) {
        super(metaManagerCls);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        me = conf.getServerAddr();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        tom = metadataManagerFactory.newTopicOwnershipManager();
        tm = new MMTopicManager(conf, zk, metadataManagerFactory, scheduler);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        tom.close();
        tm.stop();
        super.tearDown();
    }

    @Test(timeout=60000)
    public void testGetOwnerSingle() throws Exception {
        tm.getOwner(topic, false, addrCbq, null);
        Assert.assertEquals(me, check(addrCbq.take()));
    }

    protected ByteString mkTopic(int i) {
        return ByteString.copyFromUtf8(topic.toStringUtf8() + i);
    }

    protected <T> T check(Either<T, Exception> ex) throws Exception {
        if (ex.left() == null)
            throw ex.right();
        else
            return ex.left();
    }

    public static class CustomServerConfiguration extends ServerConfiguration {
        int port;

        public CustomServerConfiguration(int port) {
            this.port = port;
        }

        @Override
        public int getServerPort() {
            return port;
        }
    }

    @Test(timeout=60000)
    public void testGetOwnerMulti() throws Exception {
        ServerConfiguration conf1 = new CustomServerConfiguration(conf.getServerPort() + 1),
                            conf2 = new CustomServerConfiguration(conf.getServerPort() + 2);
        MMTopicManager tm1 = new MMTopicManager(conf1, zk, metadataManagerFactory, scheduler),
                       tm2 = new MMTopicManager(conf2, zk, metadataManagerFactory, scheduler);

        tm.getOwner(topic, false, addrCbq, null);
        HedwigSocketAddress owner = check(addrCbq.take());

        for (int i = 0; i < 100; ++i) {
            tm.getOwner(topic, false, addrCbq, null);
            Assert.assertEquals(owner, check(addrCbq.take()));

            tm1.getOwner(topic, false, addrCbq, null);
            Assert.assertEquals(owner, check(addrCbq.take()));

            tm2.getOwner(topic, false, addrCbq, null);
            Assert.assertEquals(owner, check(addrCbq.take()));
        }

        for (int i = 0; i < 100; ++i) {
            if (!owner.equals(me))
                break;
            tm.getOwner(mkTopic(i), false, addrCbq, null);
            owner = check(addrCbq.take());
            if (i == 99)
                Assert.fail("Never chose another owner");
        }

        tm1.stop();
        tm2.stop();
    }

    @Test(timeout=60000)
    public void testLoadBalancing() throws Exception {
        tm.getOwner(topic, false, addrCbq, null);

        Assert.assertEquals(me, check(addrCbq.take()));

        ServerConfiguration conf1 = new CustomServerConfiguration(conf.getServerPort() + 1);
        TopicManager tm1 = new MMTopicManager(conf1, zk, metadataManagerFactory, scheduler);

        ByteString topic1 = mkTopic(1);
        tm.getOwner(topic1, false, addrCbq, null);
        Assert.assertEquals(conf1.getServerAddr(), check(addrCbq.take()));

        tm1.stop();
    }

    class StubOwnershipChangeListener implements TopicOwnershipChangeListener {
        boolean failure;
        SynchronousQueue<Pair<ByteString, Boolean>> bsQueue;

        public StubOwnershipChangeListener(SynchronousQueue<Pair<ByteString, Boolean>> bsQueue) {
            this.bsQueue = bsQueue;
        }

        public void setFailure(boolean failure) {
            this.failure = failure;
        }

        @Override
        public void lostTopic(final ByteString topic) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ConcurrencyUtils.put(bsQueue, Pair.of(topic, false));
                }
            }).start();
        }

        public void acquiredTopic(final ByteString topic, final Callback<Void> callback, final Object ctx) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ConcurrencyUtils.put(bsQueue, Pair.of(topic, true));
                    if (failure) {
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException("Asked to fail"));
                    } else {
                        callback.operationFinished(ctx, null);
                    }
                }
            }).start();
        }
    }

    @Test(timeout=60000)
    public void testOwnershipChange() throws Exception {
        SynchronousQueue<Pair<ByteString, Boolean>> bsQueue = new SynchronousQueue<Pair<ByteString, Boolean>>();

        StubOwnershipChangeListener listener = new StubOwnershipChangeListener(bsQueue);

        tm.addTopicOwnershipChangeListener(listener);

        // regular acquire
        tm.getOwner(topic, true, addrCbq, null);
        Pair<ByteString, Boolean> pair = bsQueue.take();
        Assert.assertEquals(topic, pair.first());
        Assert.assertTrue(pair.second());
        Assert.assertEquals(me, check(addrCbq.take()));
        assertOwnershipNodeExists();

        // topic that I already own
        tm.getOwner(topic, true, addrCbq, null);
        Assert.assertEquals(me, check(addrCbq.take()));
        Assert.assertTrue(bsQueue.isEmpty());
        assertOwnershipNodeExists();

        // regular release
        tm.releaseTopic(topic, cb, null);
        pair = bsQueue.take();
        Assert.assertEquals(topic, pair.first());
        Assert.assertFalse(pair.second());
        Assert.assertTrue(queue.take());
        assertOwnershipNodeDoesntExist();

        // releasing topic that I don't own
        tm.releaseTopic(mkTopic(0), cb, null);
        Assert.assertTrue(queue.take());
        Assert.assertTrue(bsQueue.isEmpty());

        // set listener to return error
        listener.setFailure(true);

        tm.getOwner(topic, true, addrCbq, null);
        pair = bsQueue.take();
        Assert.assertEquals(topic, pair.first());
        Assert.assertTrue(pair.second());
        Assert.assertEquals(PubSubException.ServiceDownException.class, ((CompositeException) addrCbq.take().right())
                            .getExceptions().iterator().next().getClass());
        Assert.assertFalse(null != tm.topics.getIfPresent(topic));
        Thread.sleep(100);
        assertOwnershipNodeDoesntExist();

    }

    public void assertOwnershipNodeExists() throws Exception {
        StubCallback<Versioned<HubInfo>> callback = new StubCallback<Versioned<HubInfo>>();
        tom.readOwnerInfo(topic, callback, null);
        Versioned<HubInfo> hubInfo = callback.queue.take().left();
        Assert.assertEquals(tm.addr, hubInfo.getValue().getAddress());
    }

    public void assertOwnershipNodeDoesntExist() throws Exception {
        StubCallback<Versioned<HubInfo>> callback = new StubCallback<Versioned<HubInfo>>();
        tom.readOwnerInfo(topic, callback, null);
        Versioned<HubInfo> hubInfo = callback.queue.take().left();
        Assert.assertEquals(null, hubInfo);
    }

    @Test(timeout=60000)
    public void testZKClientDisconnected() throws Exception {
        // First assert ownership of the topic
        tm.getOwner(topic, true, addrCbq, null);
        Assert.assertEquals(me, check(addrCbq.take()));

        // Suspend the ZKTopicManager and make sure calls to getOwner error out
        tm.isSuspended = true;
        tm.getOwner(topic, true, addrCbq, null);
        Assert.assertEquals(PubSubException.ServiceDownException.class, addrCbq.take().right().getClass());
        // Release the topic. This should not error out even if suspended.
        tm.releaseTopic(topic, cb, null);
        Assert.assertTrue(queue.take());
        assertOwnershipNodeDoesntExist();

        // Restart the ZKTopicManager and make sure calls to getOwner are okay
        tm.isSuspended = false;
        tm.getOwner(topic, true, addrCbq, null);
        Assert.assertEquals(me, check(addrCbq.take()));
        assertOwnershipNodeExists();
    }

    @Test(timeout=60000)
    public void testRetentionAfterAccess() throws Exception {
        conf.getConf().setProperty("retention_secs_after_access", "5");
        MMTopicManager tm1 = new MMTopicManager(conf, zk, metadataManagerFactory, scheduler);
        tm1.getOwner(topic, true, addrCbq, null);
        Assert.assertEquals(me, check(addrCbq.take()));
        Thread.sleep(6000L);
        tm1.topics.cleanUp();
        Thread.sleep(2000L);
        assertOwnershipNodeDoesntExist();
        tm1.getOwner(topic, true, addrCbq, null);
        Assert.assertEquals(me, check(addrCbq.take()));
        Thread.sleep(1000L);
        tm1.topics.cleanUp();
        Thread.sleep(2000L);
        assertOwnershipNodeExists();

        tm1.stop();
    }

    @Test(timeout=60000)
    public void testMaxNumTopics() throws Exception {
        conf.getConf().setProperty("max_num_topics", "1");
        MMTopicManager tm1 = new MMTopicManager(conf, zk, metadataManagerFactory, scheduler);
        tm1.getOwner(topic, true, addrCbq, null);
        Assert.assertEquals(me, check(addrCbq.take()));
        assertOwnershipNodeExists();
        tm1.getOwner(ByteString.copyFromUtf8("MaxNumTopic"),
                     true, addrCbq, null);
        Assert.assertEquals(me, check(addrCbq.take()));
        Thread.sleep(2000L);
        assertOwnershipNodeDoesntExist();
        tm1.stop();
    }


}
