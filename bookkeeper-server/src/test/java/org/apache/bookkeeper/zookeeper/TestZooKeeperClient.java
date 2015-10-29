/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import junit.framework.TestCase;

/**
 * Test the wrapper of {@link org.apache.zookeeper.ZooKeeper} client.
 */
public class TestZooKeeperClient extends TestCase {

    static final Logger logger = LoggerFactory.getLogger(TestZooKeeperClient.class);

    // ZooKeeper related variables
    protected ZooKeeperUtil zkUtil = new ZooKeeperUtil();

    @Before
    @Override
    public void setUp() throws Exception {
        logger.info("Setting up test {}.", getName());
        zkUtil.startServer();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        zkUtil.killServer();
        logger.info("Teared down test {}.", getName());
    }

    private void expireZooKeeperSession(ZooKeeper zk, int timeout)
            throws IOException, InterruptedException, KeeperException {
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper newZk = new ZooKeeper(zkUtil.getZooKeeperConnectString(), timeout,
                new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None &&
                        event.getState() == KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }

        }, zk.getSessionId(), zk.getSessionPasswd());
        if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }
        newZk.close();
    }

    /**
     * Shutdown Zk Server when client received an expire event.
     * So the client issue recreation client task but it would not succeed
     * until we start the zookeeper server again.
     */
    class ShutdownZkServerClient extends ZooKeeperClient {

        ShutdownZkServerClient(String connectString, int sessionTimeoutMs,
                ZooKeeperWatcherBase watcher, RetryPolicy operationRetryPolicy)throws IOException {
            super(connectString, sessionTimeoutMs, watcher, operationRetryPolicy, null, NullStatsLogger.INSTANCE, 1, 0);
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == EventType.None &&
                    event.getState() == KeeperState.Expired) {
                try {
                    zkUtil.stopServer();
                } catch (Exception e) {
                    logger.error("Failed to stop zookeeper server : ", e);
                }
            }
            super.process(event);
        }

    }

    @Test(timeout=12000)
    public void testReconnectAfterExipred() throws Exception {
        final CountDownLatch expireLatch = new CountDownLatch(1);
        Watcher testWatcher = new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None &&
                        event.getState() == KeeperState.Expired) {
                    expireLatch.countDown();
                }
            }

        };
        final int timeout = 2000;
        ZooKeeperWatcherBase watcherManager =
                new ZooKeeperWatcherBase(timeout).addChildWatcher(testWatcher);
        List<Watcher> watchers = new ArrayList<Watcher>(1);
        watchers.add(testWatcher);
        ZooKeeperClient client = new ShutdownZkServerClient(
                zkUtil.getZooKeeperConnectString(), timeout, watcherManager,
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, 0)
                );
        client.waitForConnection();
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.",
                client.getState().isConnected());
        logger.info("Expire zookeeper client");
        expireZooKeeperSession(client, timeout);

        // wait until session expire
        Assert.assertTrue("Client registered watcher should receive expire event.",
                expireLatch.await(2 * timeout, TimeUnit.MILLISECONDS));

        Assert.assertFalse("Client doesn't receive expire event from ZooKeeper.",
                client.getState().isConnected());
        
        try {
            client.exists("/tmp", false);
            Assert.fail("Should fail due to connection loss.");
        } catch (KeeperException.ConnectionLossException cle) {
            // expected
        } catch (KeeperException.SessionExpiredException cle) {
            // expected
        }

        zkUtil.restartServer();

        // wait for a reconnect cycle
        Thread.sleep(2*timeout);
        Assert.assertTrue("Client failed to connect zookeeper even it was back.",
                client.getState().isConnected());
        try {
            client.exists("/tmp", false);
        } catch (KeeperException.ConnectionLossException cle) {
            Assert.fail("Should not throw ConnectionLossException");
        } catch (KeeperException.SessionExpiredException cle) {
            Assert.fail("Should not throw SessionExpiredException");
        }
    }

    @Test(timeout=60000)
    public void testRetrySyncOperations() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(
                zkUtil.getZooKeeperConnectString(), timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE)
                );
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.",
                client.getState().isConnected());

        String path = "/a";
        byte[] data = "test".getBytes();

        expireZooKeeperSession(client, timeout);
        logger.info("Create znode " + path);
        client.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        logger.info("Created znode " + path);

        expireZooKeeperSession(client, timeout);
        logger.info("Exists znode " + path);
        Stat stat = client.exists(path, false);
        Assert.assertNotNull("znode doesn't existed", stat);

        expireZooKeeperSession(client, timeout);
        logger.info("Get data from znode " + path);
        Stat newStat = new Stat();
        client.getData(path, false, newStat);
        Assert.assertEquals(stat, newStat);

        expireZooKeeperSession(client, timeout);
        logger.info("Create children under znode " + path);
        client.create(path + "/children", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        expireZooKeeperSession(client, timeout);
        List<String> children = client.getChildren(path, false, newStat);
        Assert.assertEquals(1, children.size());
        Assert.assertEquals("children", children.get(0));
        logger.info("Get children under znode " + path);

        expireZooKeeperSession(client, timeout);
        client.delete(path + "/children", -1);
        logger.info("Delete children from znode " + path);
    }

    @Test(timeout=60000)
    public void testRetryOnCreatingEphemeralZnode() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(
                zkUtil.getZooKeeperConnectString(), timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE)
                );
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.",
                client.getState().isConnected());

        String path = "/a";
        byte[] data = "test".getBytes();

        logger.info("Create znode " + path);
        client.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        logger.info("Created znode " + path);

        expireZooKeeperSession(client, timeout);
        logger.info("Create znode w/ new session : " + path);
        client.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        logger.info("Created znode w/ new session : " + path);
    }

    @Test(timeout=60000)
    public void testRetryAsyncOperations() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(
                zkUtil.getZooKeeperConnectString(), timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE)
                );
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.",
                client.getState().isConnected());

        String path = "/a";
        byte[] data = "test".getBytes();

        expireZooKeeperSession(client, timeout);
        logger.info("Create znode " + path);
        final CountDownLatch createLatch = new CountDownLatch(1);
        client.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new StringCallback() {

            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    createLatch.countDown();
                }
            }

        }, null);
        createLatch.await();
        logger.info("Created znode " + path);

        expireZooKeeperSession(client, timeout);
        logger.info("Exists znode " + path);
        final CountDownLatch existsLatch = new CountDownLatch(1);
        client.exists(path, false, new StatCallback() {

            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    existsLatch.countDown();
                }
            }

        }, null);
        existsLatch.await();

        expireZooKeeperSession(client, timeout);
        final CountDownLatch getLatch = new CountDownLatch(1);
        logger.info("Get data from znode " + path);
        client.getData(path, false, new DataCallback() {

            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    getLatch.countDown();
                }
            }

        }, null);
        getLatch.await();

        expireZooKeeperSession(client, timeout);
        logger.info("Create children under znode " + path);
        final CountDownLatch createChildLatch = new CountDownLatch(1);
        client.create(path + "/children", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new StringCallback() {

            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    createChildLatch.countDown();
                }
            }

        }, null);
        createChildLatch.await();

        expireZooKeeperSession(client, timeout);
        final CountDownLatch getChildLatch = new CountDownLatch(1);
        final AtomicReference<List<String>> children =
                new AtomicReference<List<String>>();
        client.getChildren(path, false, new Children2Callback() {

            @Override
            public void processResult(int rc, String path, Object ctx, List<String> childList, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    children.set(childList);
                    getChildLatch.countDown();
                }
            }

        }, null);
        getChildLatch.await();
        Assert.assertNotNull(children.get());
        Assert.assertEquals(1, children.get().size());
        Assert.assertEquals("children", children.get().get(0));
        logger.info("Get children under znode " + path);

        expireZooKeeperSession(client, timeout);
        final CountDownLatch deleteChildLatch = new CountDownLatch(1);
        client.delete(path + "/children", -1, new VoidCallback() {

            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    deleteChildLatch.countDown();
                }
            }

        }, null);
        deleteChildLatch.await();
        logger.info("Delete children from znode " + path);
    }

}
