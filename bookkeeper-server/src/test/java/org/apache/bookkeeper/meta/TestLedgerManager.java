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

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;

import java.util.List;
import java.util.ArrayList;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLedgerManager extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestLedgerManager.class);

    public TestLedgerManager() {
        super(0);
    }

    /** 
     * Test bad client configuration
     */
    @Test
    public void testBadConf() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        
        // success case
        String root0 = "/goodconf0";
        zkc.create(root0, new byte[0], 
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root0);

        LedgerManager m = LedgerManagerFactory.newLedgerManager(conf, zkc);
        assertTrue("Ledger manager is unexpected type", 
                   (m instanceof FlatLedgerManager));

        // mismatching conf
        conf.setLedgerManagerType(HierarchicalLedgerManager.NAME);
        try {
            LedgerManagerFactory.newLedgerManager(conf, zkc);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            assertTrue("Invalid exception", 
                       e.getMessage().contains("does not match existing layout"));
        }

        // invalid ledger manager
        String root1 = "/badconf1";
        zkc.create(root1, new byte[0], 
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root1);

        conf.setLedgerManagerType("DoesNotExist");
        try {
            LedgerManagerFactory.newLedgerManager(conf, zkc);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            assertTrue("Invalid exception", 
                    e.getMessage().contains("Unknown ledger manager type "));
        }
    }

    /**
     * Test bad zk configuration
     */
    @Test
    public void testBadZkContents() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        
        // bad type in zookeeper
        String root0 = "/badzk0";
        zkc.create(root0, new byte[0], 
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root0);
        
        new LedgerLayout("DoesNotExist",
                         0xdeadbeef).store(zkc, root0);
        
        try {
            LedgerManagerFactory.newLedgerManager(conf, zkc);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            assertTrue("Invalid exception", 
                    e.getMessage().contains("Unknown ledger manager type"));
        }

        // bad version in zookeeper
        String root1 = "/badzk1";
        zkc.create(root1, new byte[0], 
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        conf.setZkLedgersRootPath(root1);
        
        new LedgerLayout(FlatLedgerManager.NAME,
                         0xdeadbeef).store(zkc, root1);
        
        try {
            LedgerManagerFactory.newLedgerManager(conf, zkc);
            fail("Shouldn't reach here");
        } catch (Exception e) {
            assertTrue("Invalid exception", 
                    e.getMessage().contains("Incompatible layout version found"));
        }
    }

    private static class CreateLMThread extends Thread {
        private boolean success = false;
        private final String type;
        private final String root;
        private final CyclicBarrier barrier;
        private ZooKeeper zkc;
        
        CreateLMThread(String root, String type, CyclicBarrier barrier) throws Exception {
            this.type = type;
            this.barrier = barrier;
            this.root = root;
            final CountDownLatch latch = new CountDownLatch(1);
            zkc = new ZooKeeper("127.0.0.1", 10000, new Watcher() {
                    public void process(WatchedEvent event) {
                        latch.countDown();
                    }
                });
            latch.await();
        }

        public void run() {
            ClientConfiguration conf = new ClientConfiguration();
            conf.setLedgerManagerType(type);

            try {
                barrier.await();
                LedgerManagerFactory.newLedgerManager(conf, zkc);
                
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

        CyclicBarrier barrier = new CyclicBarrier(numThreads+1);
        List<CreateLMThread> threads = new ArrayList<CreateLMThread>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            CreateLMThread t = new CreateLMThread(root0, FlatLedgerManager.NAME, barrier);
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

        CyclicBarrier barrier = new CyclicBarrier(numThreadsEach*2+1);
        List<CreateLMThread> threadsA = new ArrayList<CreateLMThread>(numThreadsEach);
        for (int i = 0; i < numThreadsEach; i++) {
            CreateLMThread t = new CreateLMThread(root0, FlatLedgerManager.NAME, barrier);
            t.start();
            threadsA.add(t);
        }
        List<CreateLMThread> threadsB = new ArrayList<CreateLMThread>(numThreadsEach);
        for (int i = 0; i < numThreadsEach; i++) {
            CreateLMThread t = new CreateLMThread(root0, 
                    HierarchicalLedgerManager.NAME, barrier);
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
