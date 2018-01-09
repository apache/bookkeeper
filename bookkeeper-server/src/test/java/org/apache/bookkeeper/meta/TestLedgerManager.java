/*
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
package org.apache.bookkeeper.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the ledger manager.
 */
public class TestLedgerManager extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestLedgerManager.class);

    public TestLedgerManager() {
        super(0);
    }

    private void writeLedgerLayout(String ledgersRootPath,
                                   String managerType,
                                   int managerVersion, int layoutVersion)
        throws Exception {
        LedgerLayout layout = new LedgerLayout(managerType, managerVersion);

        Field f = LedgerLayout.class.getDeclaredField("layoutFormatVersion");
        f.setAccessible(true);
        f.set(layout, layoutVersion);

        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(zkc, ledgersRootPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        zkLayoutManager.storeLedgerLayout(layout);
    }

    /**
     * Test bad client configuration.
     */
    @Test
    public void testBadConf() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();

        // success case
        String root0 = "/goodconf0";
        zkc.create(root0, new byte[0],
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root0);

        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(
            zkc,
            conf.getZkLedgersRootPath(),
            ZkUtils.getACLs(conf));

        LedgerManagerFactory m = LedgerManagerFactory.newLedgerManagerFactory(
            conf,
            zkLayoutManager);
        assertTrue("Ledger manager is unexpected type",
                   (m instanceof FlatLedgerManagerFactory));
        m.uninitialize();

        // mismatching conf
        conf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);
        try {
            LedgerManagerFactory.newLedgerManagerFactory(conf, zkLayoutManager);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            LOG.error("Received exception", e);
            assertTrue("Invalid exception",
                       e.getMessage().contains("does not match existing layout"));
        }

        // invalid ledger manager
        String root1 = "/badconf1";
        zkc.create(root1, new byte[0],
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root1);

        conf.setLedgerManagerFactoryClassName("DoesNotExist");
        try {
            LedgerManagerFactory f = LedgerManagerFactory.newLedgerManagerFactory(conf, zkLayoutManager);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            LOG.error("Received exception", e);
            assertTrue("Invalid exception",
                    e.getMessage().contains("Failed to get ledger manager factory class from configuration"));
        }
    }

    /**
     * Test bad client configuration.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testBadConfV1() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();

        String root0 = "/goodconf0";
        zkc.create(root0, new byte[0],
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root0);
        // write v1 layout
        writeLedgerLayout(root0, FlatLedgerManagerFactory.NAME,
                          FlatLedgerManagerFactory.CUR_VERSION, 1);

        conf.setLedgerManagerFactoryClass(FlatLedgerManagerFactory.class);

        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(
            zkc,
            conf.getZkLedgersRootPath(),
            ZkUtils.getACLs(conf));

        LedgerManagerFactory m = LedgerManagerFactory.newLedgerManagerFactory(
            conf,
            zkLayoutManager);

        assertTrue("Ledger manager is unexpected type",
                   (m instanceof FlatLedgerManagerFactory));
        m.uninitialize();

        // v2 setting doesn't effect v1
        conf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);
        m = LedgerManagerFactory.newLedgerManagerFactory(conf, zkLayoutManager);
        assertTrue("Ledger manager is unexpected type",
                   (m instanceof FlatLedgerManagerFactory));
        m.uninitialize();

        // mismatching conf
        conf.setLedgerManagerType(HierarchicalLedgerManagerFactory.NAME);
        try {
            LedgerManagerFactory.newLedgerManagerFactory(conf, zkLayoutManager);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            LOG.error("Received exception", e);
            assertTrue("Invalid exception",
                       e.getMessage().contains("does not match existing layout"));
        }
    }

    /**
     * Test bad zk configuration.
     */
    @Test
    public void testBadZkContents() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();

        // bad type in zookeeper
        String root0 = "/badzk0";
        zkc.create(root0, new byte[0],
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root0);

        LedgerLayout layout = new LedgerLayout("DoesNotExist",
                         0xdeadbeef);

        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(zkc, root0, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        zkLayoutManager.storeLedgerLayout(layout);

        try {
            LedgerManagerFactory.newLedgerManagerFactory(conf, zkLayoutManager);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            LOG.error("Received exception", e);
            assertTrue("Invalid exception",
                    e.getMessage().contains("Failed to instantiate ledger manager factory"));
        }

        // bad version in zookeeper
        String root1 = "/badzk1";
        zkc.create(root1, new byte[0],
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root1);

        LedgerLayout layout1 = new LedgerLayout(FlatLedgerManagerFactory.class.getName(),
                         0xdeadbeef);
        ZkLayoutManager zkLayoutManager1 = new ZkLayoutManager(zkc, root1, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        zkLayoutManager1.storeLedgerLayout(layout1);

        try {
            LedgerManagerFactory.newLedgerManagerFactory(conf, zkLayoutManager1);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            LOG.error("Received exception", e);
            assertTrue("Invalid exception",
                    e.getMessage().contains("Incompatible layout version found"));
        }
    }

    private static class CreateLMThread extends Thread {
        private boolean success = false;
        private final String factoryCls;
        private final String root;
        private final CyclicBarrier barrier;
        private ZooKeeper zkc;

        CreateLMThread(String zkConnectString, String root,
                       String factoryCls, CyclicBarrier barrier) throws Exception {
            this.factoryCls = factoryCls;
            this.barrier = barrier;
            this.root = root;
            zkc = ZooKeeperClient.newBuilder()
                    .connectString(zkConnectString)
                    .build();
        }

        public void run() {
            ClientConfiguration conf = new ClientConfiguration();
            conf.setLedgerManagerFactoryClassName(factoryCls);

            try {
                barrier.await();
                LedgerManagerFactory factory = LedgerManagerFactory.newLedgerManagerFactory(
                    conf,
                    new ZkLayoutManager(
                        zkc,
                        conf.getZkLedgersRootPath(),
                        ZkUtils.getACLs(conf)));
                factory.uninitialize();

                success = true;
            } catch (Exception e) {
                LOG.error("Failed to create ledger manager", e);
            }
        }

        public boolean isSuccessful() {
            return success;
        }

        public void close() throws Exception {
            zkc.close();
        }
    }

    // test concurrent
    @Test
    public void testConcurrent1() throws Exception {
        /// everyone creates the same
        int numThreads = 50;

        // bad version in zookeeper
        String root0 = "/lmroot0";
        zkc.create(root0, new byte[0],
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        List<CreateLMThread> threads = new ArrayList<CreateLMThread>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            CreateLMThread t = new CreateLMThread(zkUtil.getZooKeeperConnectString(),
                    root0, FlatLedgerManagerFactory.class.getName(), barrier);
            t.start();
            threads.add(t);
        }

        barrier.await();

        boolean success = true;
        for (CreateLMThread t : threads) {
            t.join();
            t.close();
            success = t.isSuccessful() && success;
        }
        assertTrue("Not all ledger managers created", success);
    }

    @Test
    public void testConcurrent2() throws Exception {
        /// odd create different
        int numThreadsEach = 25;

        // bad version in zookeeper
        String root0 = "/lmroot0";
        zkc.create(root0, new byte[0],
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        CyclicBarrier barrier = new CyclicBarrier(numThreadsEach * 2 + 1);
        List<CreateLMThread> threadsA = new ArrayList<CreateLMThread>(numThreadsEach);
        for (int i = 0; i < numThreadsEach; i++) {
            CreateLMThread t = new CreateLMThread(zkUtil.getZooKeeperConnectString(),
                    root0, FlatLedgerManagerFactory.class.getName(), barrier);
            t.start();
            threadsA.add(t);
        }
        List<CreateLMThread> threadsB = new ArrayList<CreateLMThread>(numThreadsEach);
        for (int i = 0; i < numThreadsEach; i++) {
            CreateLMThread t = new CreateLMThread(zkUtil.getZooKeeperConnectString(),
                    root0, HierarchicalLedgerManagerFactory.class.getName(), barrier);
            t.start();
            threadsB.add(t);
        }

        barrier.await();

        int numSuccess = 0;
        int numFails = 0;
        for (CreateLMThread t : threadsA) {
            t.join();
            t.close();
            if (t.isSuccessful()) {
                numSuccess++;
            } else {
                numFails++;
            }
        }

        for (CreateLMThread t : threadsB) {
            t.join();
            t.close();
            if (t.isSuccessful()) {
                numSuccess++;
            } else {
                numFails++;
            }
        }
        assertEquals("Incorrect number of successes", numThreadsEach, numSuccess);
        assertEquals("Incorrect number of failures", numThreadsEach, numFails);
    }
}
