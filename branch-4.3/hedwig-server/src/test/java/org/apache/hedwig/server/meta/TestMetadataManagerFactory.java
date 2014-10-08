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
package org.apache.hedwig.server.meta;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.hedwig.protocol.PubSubProtocol.ManagerMeta;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.zookeeper.ZooKeeperTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMetadataManagerFactory extends ZooKeeperTestBase {
    static Logger LOG = LoggerFactory.getLogger(TestMetadataManagerFactory.class);

    static class TestServerConfiguration extends ServerConfiguration {
        String hedwigPrefix = "/hedwig";

        @Override
        public String getZkPrefix() {
            return hedwigPrefix;
        }

        public void setZkPrefix(String prefix) {
            this.hedwigPrefix = prefix;
        }
    }

    static class DummyMetadataManagerFactory extends MetadataManagerFactory {
        static int VERSION = 10;

        public int getCurrentVersion() { return VERSION; }


        public MetadataManagerFactory initialize(ServerConfiguration cfg,
                                                 ZooKeeper zk,
                                                 int version)
        throws IOException {
            if (version != VERSION) {
                throw new IOException("unmatched manager version");
            }
            // do nothing
            return this;
        }

        public void shutdown() {}

        public Iterator<ByteString> getTopics() {
            return null;
        }

        public TopicPersistenceManager newTopicPersistenceManager() {
            return null;
        }

        public SubscriptionDataManager newSubscriptionDataManager() {
            return null;
        }

        public TopicOwnershipManager newTopicOwnershipManager() {
            return null;
        }

        public void format(ServerConfiguration cfg, ZooKeeper zk) throws IOException {
            // do nothing
        }
    }

    private void writeFactoryLayout(ServerConfiguration conf,
                                    String factoryCls,
                                    int factoryVersion)
        throws Exception {
        ManagerMeta meta = ManagerMeta.newBuilder()
                                      .setManagerImpl(factoryCls)
                                      .setManagerVersion(factoryVersion).build();
        new FactoryLayout(meta).store(zk, conf);
    }

    /**
     * Test bad server configuration
     */
    @Test(timeout=60000)
    public void testBadConf() throws Exception {
        TestServerConfiguration conf = new TestServerConfiguration();

        String root0 = "/goodconf";
        conf.setZkPrefix(root0);

        MetadataManagerFactory m =
            MetadataManagerFactory.newMetadataManagerFactory(conf, zk);
        Assert.assertTrue("MetadataManagerFactory is unexpected type",
                          (m instanceof ZkMetadataManagerFactory));

        // mismatching conf
        conf.setMetadataManagerFactoryName(DummyMetadataManagerFactory.class.getName());
        try {
            MetadataManagerFactory.newMetadataManagerFactory(conf, zk);
            Assert.fail("Shouldn't reach here");
        } catch (Exception e) {
            Assert.assertTrue("Invalid exception",
                              e.getMessage().contains("does not match existing factory"));
        }

        // invalid metadata manager
        String root1 = "/badconf1";
        conf.setZkPrefix(root1);
        conf.setMetadataManagerFactoryName("DoesNotExist");
        try {
            MetadataManagerFactory.newMetadataManagerFactory(conf, zk);
            Assert.fail("Shouldn't reach here");
        } catch (Exception e) {
            Assert.assertTrue("Invalid exception",
                              e.getMessage().contains("Failed to get metadata manager factory class from configuration"));
        }
    }

    /**
     * Test bad zk configuration
     */
    @Test(timeout=60000)
    public void testBadZkContents() throws Exception {
        TestServerConfiguration conf = new TestServerConfiguration();

        // bad type in zookeeper
        String root0 = "/badzk0";
        conf.setZkPrefix(root0);

        writeFactoryLayout(conf, "DoesNotExist", 0xdeadbeef);
        try {
            MetadataManagerFactory.newMetadataManagerFactory(conf, zk);
            Assert.fail("Shouldn't reach here");
        } catch (Exception e) {
            Assert.assertTrue("Invalid exception",
                              e.getMessage().contains("No class found to instantiate metadata manager factory"));
        }

        // bad version in zookeeper
        String root1 = "/badzk1";
        conf.setZkPrefix(root1);

        writeFactoryLayout(conf, ZkMetadataManagerFactory.class.getName(), 0xdeadbeef);
        try {
            MetadataManagerFactory.newMetadataManagerFactory(conf, zk);
            Assert.fail("Shouldn't reach here");
        } catch (Exception e) {
            Assert.assertTrue("Invalid exception",
                              e.getMessage().contains("Incompatible ZkMetadataManagerFactory version"));
        }
    }

    private class CreateMMThread extends Thread {
        private boolean success = false;
        private final String factoryCls;
        private final String root;
        private final CyclicBarrier barrier;
        private ZooKeeper zkc;

        CreateMMThread(String root, String factoryCls, CyclicBarrier barrier) throws Exception {
            this.factoryCls = factoryCls;
            this.barrier = barrier;
            this.root = root;
            final CountDownLatch latch = new CountDownLatch(1);
            zkc = new ZooKeeper(hostPort, 10000, new Watcher() {
                public void process(WatchedEvent event) {
                    latch.countDown();
                }
            });
            latch.await();
        }

        public void run() {
            TestServerConfiguration conf = new TestServerConfiguration();
            conf.setZkPrefix(root);
            conf.setMetadataManagerFactoryName(factoryCls);

            try {
                barrier.await();
                MetadataManagerFactory.newMetadataManagerFactory(conf, zkc);
                success = true;
            } catch (Exception e) {
                LOG.error("Failed to create metadata manager factory", e);
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
    @Test(timeout=60000)
    public void testConcurrent1() throws Exception {
        /// everyone creates the same
        int numThreads = 50;

        // bad version in zookeeper
        String root0 = "/lmroot0";

        CyclicBarrier barrier = new CyclicBarrier(numThreads+1);
        List<CreateMMThread> threads = new ArrayList<CreateMMThread>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            CreateMMThread t = new CreateMMThread(root0, ZkMetadataManagerFactory.class.getName(), barrier);
            t.start();
            threads.add(t);
        }

        barrier.await();

        boolean success = true;
        for (CreateMMThread t : threads) {
            t.join();
            t.close();
            success = t.isSuccessful() && success;
        }
        Assert.assertTrue("Not all metadata manager factories created", success);
    }

    @Test(timeout=60000)
    public void testConcurrent2() throws Exception {
        /// odd create different
        int numThreadsEach = 25;

        // bad version in zookeeper
        String root0 = "/lmroot0";

        CyclicBarrier barrier = new CyclicBarrier(numThreadsEach*2+1);
        List<CreateMMThread> threadsA = new ArrayList<CreateMMThread>(numThreadsEach);
        for (int i = 0; i < numThreadsEach; i++) {
            CreateMMThread t = new CreateMMThread(root0, ZkMetadataManagerFactory.class.getName(), barrier);
            t.start();
            threadsA.add(t);
        }
        List<CreateMMThread> threadsB = new ArrayList<CreateMMThread>(numThreadsEach);
        for (int i = 0; i < numThreadsEach; i++) {
            CreateMMThread t = new CreateMMThread(root0, DummyMetadataManagerFactory.class.getName(), barrier);
            t.start();
            threadsB.add(t);
        }

        barrier.await();

        int numSuccess = 0;
        int numFails = 0;
        for (CreateMMThread t : threadsA) {
            t.join();
            t.close();
            if (t.isSuccessful()) {
                numSuccess++;
            } else {
                numFails++;
            }
        }

        for (CreateMMThread t : threadsB) {
            t.join();
            t.close();
            if (t.isSuccessful()) {
                numSuccess++;
            } else {
                numFails++;
            }
        }
        Assert.assertEquals("Incorrect number of successes", numThreadsEach, numSuccess);
        Assert.assertEquals("Incorrect number of failures", numThreadsEach, numFails);
    }
}
