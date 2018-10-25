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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperClusterUtil;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
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
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the wrapper of {@link org.apache.zookeeper.ZooKeeper} client.
 */
@RunWith(Parameterized.class)
public class TestZooKeeperClient extends TestCase {

    static {
        ZooKeeperClusterUtil.enableZookeeperTestEnvVariables();
    }

    private static final Logger logger = LoggerFactory.getLogger(TestZooKeeperClient.class);

    // ZooKeeper related variables
    protected ZooKeeperCluster zkUtil;

    @Parameters
    public static Collection<Object[]> zooKeeperUtilClass() {
        return Arrays.asList(new Object[][] { { ZooKeeperUtil.class }, { ZooKeeperClusterUtil.class } });
    }

    public TestZooKeeperClient(Class<? extends ZooKeeperCluster> zooKeeperUtilClass)
            throws IOException, KeeperException, InterruptedException {
        if (zooKeeperUtilClass.equals(ZooKeeperUtil.class)) {
            zkUtil = new ZooKeeperUtil();
        } else {
            zkUtil = new ZooKeeperClusterUtil(3);
        }
    }

    @Before
    @Override
    public void setUp() throws Exception {
        logger.info("Setting up test {}.", getName());
        zkUtil.startCluster();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        zkUtil.killCluster();
        logger.info("Teared down test {}.", getName());
    }

    private void expireZooKeeperSession(ZooKeeper zk, int timeout)
            throws IOException, InterruptedException, KeeperException {
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper newZk = new ZooKeeper(zkUtil.getZooKeeperConnectString(), timeout,
                new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None && event.getState() == KeeperState.SyncConnected) {
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
                ZooKeeperWatcherBase watcher, RetryPolicy operationRetryPolicy)
                throws IOException {
            super(connectString, sessionTimeoutMs, watcher,
                    new BoundExponentialBackoffRetryPolicy(sessionTimeoutMs, sessionTimeoutMs, Integer.MAX_VALUE),
                    operationRetryPolicy,
                    NullStatsLogger.INSTANCE, 1, 0, false);
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == EventType.None && event.getState() == KeeperState.Expired) {
                try {
                    zkUtil.stopCluster();
                } catch (Exception e) {
                    logger.error("Failed to stop zookeeper server : ", e);
                }
            }
            super.process(event);
        }

    }

    @Test
    public void testReconnectAfterExipred() throws Exception {
        final CountDownLatch expireLatch = new CountDownLatch(1);
        Watcher testWatcher = new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None && event.getState() == KeeperState.Expired) {
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

        zkUtil.restartCluster();

        // wait for a reconnect cycle
        Thread.sleep(2 * timeout);
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

    @Test
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
        logger.info("Create children under znode " + path);
        client.create(path + "/children2", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        expireZooKeeperSession(client, timeout);
        List<String> children = client.getChildren(path, false, newStat);
        Assert.assertEquals(2, children.size());
        Assert.assertTrue(children.contains("children"));
        Assert.assertTrue(children.contains("children2"));
        logger.info("Get children under znode " + path);

        expireZooKeeperSession(client, timeout);
        client.delete(path + "/children", -1);
        logger.info("Delete children from znode " + path);
    }

    @Test
    public void testSyncAfterSessionExpiry() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(zkUtil.getZooKeeperConnectString(),
                timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE));
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.", client.getState().isConnected());

        String path = "/testSyncAfterSessionExpiry";
        byte[] data = "test".getBytes();

        // create a node
        logger.info("Create znode " + path);
        List<ACL> setACLList = new ArrayList<ACL>();
        setACLList.addAll(Ids.OPEN_ACL_UNSAFE);
        client.create(path, data, setACLList, CreateMode.PERSISTENT);

        // expire the ZKClient session
        expireZooKeeperSession(client, timeout);

        // the current Client connection should be in connected state even after the session expiry
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.", client.getState().isConnected());

        // even after the previous session expiry client should be able to sync
        CountDownLatch latch = new CountDownLatch(1);
        final int[] rcArray = { -1 };
        client.sync(path, new VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                CountDownLatch cdLatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                cdLatch.countDown();
            }
        }, latch);
        Assert.assertTrue("client.sync operation should have completed successfully",
                latch.await(6000, TimeUnit.MILLISECONDS));
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail("Sync failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }

        // delete the node
        client.delete(path, -1);
    }

    @Test
    public void testACLSetAndGet() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(zkUtil.getZooKeeperConnectString(),
                timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE));
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.", client.getState().isConnected());

        String path = "/testACLSetAndGet";
        byte[] data = "test".getBytes();

        // create a node and call getACL to verify the received ACL
        logger.info("Create znode " + path);
        List<ACL> setACLList = new ArrayList<ACL>();
        setACLList.addAll(Ids.OPEN_ACL_UNSAFE);
        client.create(path, data, setACLList, CreateMode.PERSISTENT);
        Stat status = new Stat();
        List<ACL> receivedACLList = client.getACL(path, status);
        Assert.assertEquals("Test1 - ACLs are expected to match", setACLList, receivedACLList);

        // update node's ACL and call getACL to verify the received ACL
        setACLList.clear();
        setACLList.addAll(Ids.OPEN_ACL_UNSAFE);
        setACLList.addAll(Ids.READ_ACL_UNSAFE);
        status = client.setACL(path, setACLList, status.getAversion());
        receivedACLList = client.getACL(path, status);
        Assert.assertEquals("Test2 - ACLs are expected to match", setACLList, receivedACLList);

        // update node's ACL by calling async setACL and call async getACL to verify the received ACL
        setACLList.clear();
        setACLList.addAll(Ids.OPEN_ACL_UNSAFE);
        CountDownLatch latch = new CountDownLatch(1);
        final Stat[] statArray = { null };
        final int[] rcArray = { -1 };
        client.setACL(path, setACLList, status.getAversion(), new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                CountDownLatch cdLatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                statArray[0] = stat;
                cdLatch.countDown();
            }
        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail("Test3 - SetACL call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        status = statArray[0];
        latch = new CountDownLatch(1);
        rcArray[0] = 0;
        statArray[0] = null;
        final List<List<ACL>> aclListArray = new ArrayList<List<ACL>>(1);
        client.getACL(path, status, new ACLCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<ACL> acl, Stat stat) {
                CountDownLatch cdLatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                statArray[0] = stat;
                aclListArray.add(acl);
                cdLatch.countDown();
            }

        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail("Test4 - GetACL call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        status = statArray[0];
        receivedACLList = aclListArray.get(0);
        Assert.assertEquals("Test5 - ACLs are expected to match", setACLList, receivedACLList);

        // delete the node
        client.delete(path, status.getVersion());
    }

    @Test
    public void testACLSetAndGetAfterSessionExpiry() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(zkUtil.getZooKeeperConnectString(),
                timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE));
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.", client.getState().isConnected());

        String path = "/testACLSetAndGetAfterSessionExpiry";
        byte[] data = "test".getBytes();

        // create a node
        logger.info("Create znode " + path);
        List<ACL> setACLList = new ArrayList<ACL>();
        setACLList.addAll(Ids.OPEN_ACL_UNSAFE);
        client.create(path, data, setACLList, CreateMode.PERSISTENT);

        // expire the ZKClient session
        expireZooKeeperSession(client, timeout);

        // call getACL and verify if it returns the previously set ACL
        Stat status = new Stat();
        List<ACL> receivedACLList = client.getACL(path, status);
        Assert.assertEquals("Test1 - ACLs are expected to match", setACLList, receivedACLList);

        // update ACL of that node
        setACLList.clear();
        setACLList.addAll(Ids.OPEN_ACL_UNSAFE);
        setACLList.addAll(Ids.READ_ACL_UNSAFE);
        status = client.setACL(path, setACLList, status.getAversion());

        // expire the ZKClient session
        expireZooKeeperSession(client, timeout);

        // call getACL and verify if it returns the previously set ACL
        receivedACLList = client.getACL(path, status);
        Assert.assertEquals("Test2 - ACLs are expected to match", setACLList, receivedACLList);

        // update the ACL of node by calling async setACL
        setACLList.clear();
        setACLList.addAll(Ids.OPEN_ACL_UNSAFE);
        CountDownLatch latch = new CountDownLatch(1);
        final Stat[] statArray = { null };
        final int[] rcArray = { -1 };
        client.setACL(path, setACLList, status.getAversion(), new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                CountDownLatch cdLatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                statArray[0] = stat;
                cdLatch.countDown();
            }
        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail("Test3 - SetACL call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        status = statArray[0];

        // expire the ZKClient session
        expireZooKeeperSession(client, timeout);

        // call async getACL and verify if it returns the previously set ACL
        latch = new CountDownLatch(1);
        rcArray[0] = 0;
        statArray[0] = null;
        final List<List<ACL>> aclListArray = new ArrayList<List<ACL>>(1);
        client.getACL(path, status, new ACLCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<ACL> acl, Stat stat) {
                CountDownLatch cdLatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                statArray[0] = stat;
                aclListArray.add(acl);
                cdLatch.countDown();
            }

        }, latch);
        Assert.assertTrue("getACL operation should have completed successfully",
                latch.await(6000, TimeUnit.MILLISECONDS));
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail("Test4 - GetACL call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        status = statArray[0];
        receivedACLList = aclListArray.get(0);
        Assert.assertEquals("Test5 - ACLs are expected to match", setACLList, receivedACLList);

        client.delete(path, status.getVersion());
    }

    @Test
    public void testZnodeExists() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(zkUtil.getZooKeeperConnectString(),
                timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE));
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.", client.getState().isConnected());

        String path = "/testZnodeExists";
        byte[] data = "test".getBytes();

        // create a node
        logger.info("Create znode " + path);
        client.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // expire the ZKClient session and then call exists method for the path
        expireZooKeeperSession(client, timeout);
        final AtomicBoolean isDeleted = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        Stat stat = client.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    isDeleted.set(true);
                    latch.countDown();
                }
            }
        });
        Assert.assertNotNull("node with path " + path + " should exists", stat);

        // now delete the znode and verify if the Watcher is called
        client.delete(path, stat.getVersion());
        latch.await(5000, TimeUnit.MILLISECONDS);
        Assert.assertTrue("The watcher on the node should have been called", isDeleted.get());

        // calling async exists method and verifying the return values
        CountDownLatch latch2 = new CountDownLatch(1);
        final int[] rcArray = { -1 };
        final boolean[] statIsnull = { false };
        client.exists(path, null, new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                CountDownLatch cdlatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                statIsnull[0] = (stat == null);
                cdlatch.countDown();
            }
        }, latch2);
        latch2.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.NONODE.intValue()) {
            Assert.fail("exists call is supposed to return NONODE rcvalue, but it returned - "
                    + KeeperException.Code.get(rcArray[0]));
        }
        Assert.assertTrue("exists is supposed to return null for Stat,"
                + " since the node is already deleted", statIsnull[0]);
    }

    @Test
    public void testGetSetData() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(zkUtil.getZooKeeperConnectString(),
                timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE));
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.", client.getState().isConnected());

        String path = "/testGetSetData";
        byte[] data = "test".getBytes();

        // create a node and call async getData method and verify its return value
        logger.info("Create znode " + path);
        client.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        CountDownLatch latch = new CountDownLatch(1);
        final Stat[] statArray = { null };
        final int[] rcArray = { -1 };
        final byte[][] dataArray = { {} };
        client.getData(path, true, new DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                CountDownLatch cdLatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                statArray[0] = stat;
                dataArray[0] = data;
                cdLatch.countDown();
            }
        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail("Test1 - getData call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        Assert.assertArrayEquals("Test1 - getData output - ", data, dataArray[0]);
        Stat stat = statArray[0];

        // expire the ZKClient session and then call async setData with new data
        expireZooKeeperSession(client, timeout);
        latch = new CountDownLatch(1);
        data = "newtest".getBytes();
        client.setData(path, data, stat.getVersion(), new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                CountDownLatch cdlatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                statArray[0] = stat;
                cdlatch.countDown();
            }
        }, latch);
        Assert.assertTrue("setData operation should have completed successfully",
                latch.await(6000, TimeUnit.MILLISECONDS));
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail("Test2 - setData call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        stat = statArray[0];

        // call getData
        byte[] getDataRet = client.getData(path, null, stat);
        Assert.assertArrayEquals("Test3 - getData output - ", data, getDataRet);

        // call setdata and then async getData call
        data = "newesttest".getBytes();
        stat = client.setData(path, data, stat.getVersion());
        latch = new CountDownLatch(1);
        client.getData(path, null, new DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                CountDownLatch cdLatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                statArray[0] = stat;
                dataArray[0] = data;
                cdLatch.countDown();
            }
        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail("Test4 - getData call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        Assert.assertArrayEquals("Test4 - getData output - ", data, dataArray[0]);
        stat = statArray[0];

        client.delete(path, stat.getVersion());
    }

    @Test
    public void testGetChildren() throws Exception {
        final int timeout = 2000;
        ZooKeeperClient client = ZooKeeperClient.createConnectedZooKeeperClient(zkUtil.getZooKeeperConnectString(),
                timeout, new HashSet<Watcher>(),
                new BoundExponentialBackoffRetryPolicy(timeout, timeout, Integer.MAX_VALUE));
        Assert.assertTrue("Client failed to connect an alive ZooKeeper.", client.getState().isConnected());

        // create a root node
        String root = "/testGetChildren";
        byte[] rootData = "root".getBytes();
        logger.info("Create znode " + root);
        client.create(root, rootData, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // create a child1 node
        String child1 = root + "/" + "child1";
        logger.info("Create znode " + child1);
        byte[] child1Data = "child1".getBytes();
        client.create(child1, child1Data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // create a child2 node
        String child2 = root + "/" + "child2";
        logger.info("Create znode " + child2);
        byte[] child2Data = "child2".getBytes();
        client.create(child2, child2Data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // call getChildren method and verify its return value
        Stat rootStat = new Stat();
        List<String> children = client.getChildren(root, null, rootStat);
        Assert.assertEquals("Test1 - children size", 2, children.size());

        // create a child3 node
        String child3 = root + "/" + "child3";
        logger.info("Create znode " + child3);
        byte[] child3Data = "child3".getBytes();
        client.create(child3, child3Data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // call getChildren method and verify its return value
        children = client.getChildren(root, true, rootStat);
        Assert.assertEquals("Test2 - children size", 3, children.size());

        // call async getChildren method and verify its return value
        CountDownLatch latch = new CountDownLatch(1);
        final Stat[] statArray = { null };
        final int[] rcArray = { -1 };
        final int[] childrenCount = { 0 };
        client.getChildren(root, null, new Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                CountDownLatch cdlatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                childrenCount[0] = children.size();
                statArray[0] = stat;
                cdlatch.countDown();
            }
        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail(
                    "Test3 - getChildren call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        Assert.assertEquals("Test3 - children size", 3, childrenCount[0]);
        rootStat = statArray[0];

        // call async getChildren method and verify its return value
        latch = new CountDownLatch(1);
        client.getChildren(root, true, new Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                CountDownLatch cdlatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                childrenCount[0] = children.size();
                statArray[0] = stat;
                cdlatch.countDown();
            }
        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail(
                    "Test4 - getChildren call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        Assert.assertEquals("Test4 - children size", 3, childrenCount[0]);
        rootStat = statArray[0];

        // expire the ZKClient session and then call async setData with new data
        expireZooKeeperSession(client, timeout);

        // this is after previous session expiry. call getChildren method and verify its return value
        children = client.getChildren(root, null, rootStat);
        Assert.assertEquals("Test5 - children size", 3, children.size());

        // create a child4 node
        String child4 = root + "/" + "child4";
        logger.info("Create znode " + child4);
        byte[] child4Data = "child4".getBytes();
        client.create(child4, child4Data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // call async getChildren method and verify its return value
        latch = new CountDownLatch(1);
        client.getChildren(root, null, new Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                CountDownLatch cdlatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                childrenCount[0] = children.size();
                statArray[0] = stat;
                cdlatch.countDown();
            }
        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail(
                    "Test6 - getChildren call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        Assert.assertEquals("Test6 - children size", 4, childrenCount[0]);
        rootStat = statArray[0];

        // expire the ZKClient session and then call async setData with new data
        expireZooKeeperSession(client, timeout);

        // call getChildren method and verify its return value
        children = client.getChildren(root, null);
        Assert.assertEquals("Test7 - children size", 4, children.size());

        // call getChildren method and verify its return value
        children = client.getChildren(root, true);
        Assert.assertEquals("Test8 - children size", 4, children.size());

        // call async getChildren method and verify its return value
        latch = new CountDownLatch(1);
        client.getChildren(root, true, new AsyncCallback.ChildrenCallback() {

            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                CountDownLatch cdlatch = (CountDownLatch) ctx;
                rcArray[0] = rc;
                childrenCount[0] = children.size();
                cdlatch.countDown();
            }
        }, latch);
        latch.await(3000, TimeUnit.MILLISECONDS);
        if (rcArray[0] != KeeperException.Code.OK.intValue()) {
            Assert.fail(
                    "Test9 - getChildren call failed because of exception - " + KeeperException.Code.get(rcArray[0]));
        }
        Assert.assertEquals("Test9 - children size", 4, childrenCount[0]);
    }

    @Test
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

    @Test
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
        logger.info("Create znode " + path);
        final CountDownLatch create2Latch = new CountDownLatch(1);
        client.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (rc, path1, ctx, name) -> {
            if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                create2Latch.countDown();
            }
        }, null);
        create2Latch.await();
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
