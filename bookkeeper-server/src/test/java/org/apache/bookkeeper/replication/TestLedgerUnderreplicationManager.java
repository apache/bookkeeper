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

package org.apache.bookkeeper.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.proto.DataFormats.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat;

/**
 * Test the zookeeper implementation of the ledger replication manager
 */
public class TestLedgerUnderreplicationManager {
    static final Logger LOG = LoggerFactory.getLogger(TestLedgerUnderreplicationManager.class);

    ZooKeeperUtil zkUtil = null;

    ServerConfiguration conf = null;
    ExecutorService executor = null;
    LedgerManagerFactory lmf1 = null;
    LedgerManagerFactory lmf2 = null;
    ZooKeeper zkc1 = null;
    ZooKeeper zkc2 = null;

    String basePath;
    String urLedgerPath;

    @Before
    public void setupZooKeeper() throws Exception {
        zkUtil = new ZooKeeperUtil();
        zkUtil.startServer();

        conf = new ServerConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());

        executor = Executors.newCachedThreadPool();

        zkc1 = ZkUtils.createConnectedZookeeperClient(
                zkUtil.getZooKeeperConnectString(), 10000);
        zkc2 = ZkUtils.createConnectedZookeeperClient(
                zkUtil.getZooKeeperConnectString(), 10000);
        lmf1 = LedgerManagerFactory.newLedgerManagerFactory(conf, zkc1);
        lmf2 = LedgerManagerFactory.newLedgerManagerFactory(conf, zkc2);
        basePath = conf.getZkLedgersRootPath() + '/'
                + ZkLedgerUnderreplicationManager.UNDER_REPLICATION_NODE;
        urLedgerPath = basePath + "/ledgers";
    }

    @After
    public void teardownZooKeeper() throws Exception {
        if (zkUtil != null) {
            zkUtil.killServer();
            zkUtil = null;
        }
        if (executor != null) {
            executor = null;
        }
        if (zkc1 != null) {
            zkc1.close();
            zkc1 = null;
        }
        if (zkc2 != null) {
            zkc2.close();
            zkc2 = null;
        }
        if (lmf1 != null) {
            lmf1.uninitialize();
            lmf1 = null;
        }
        if (lmf2 != null) {
            lmf2.uninitialize();
            lmf2 = null;
        }
    }

    private Future<Long> getLedgerToReplicate(final LedgerUnderreplicationManager m) {
        return executor.submit(new Callable<Long>() {
                public Long call() {
                    try {
                        return m.getLedgerToRereplicate();
                    } catch (Exception e) {
                        LOG.error("Error getting ledger id", e);
                        return -1L;
                    }
                }
            });
    }

    /**
     * Test basic interactions with the ledger underreplication
     * manager.
     * Mark some ledgers as underreplicated.
     * Ensure that getLedgerToReplicate will block until it a ledger
     * becomes available.
     */
    @Test
    public void testBasicInteraction() throws Exception {
        Set<Long> ledgers = new HashSet<Long>();
        ledgers.add(0xdeadbeefL);
        ledgers.add(0xbeefcafeL);
        ledgers.add(0xffffbeefL);
        ledgers.add(0xfacebeefL);
        String missingReplica = "localhost:3181";

        int count = 0;
        LedgerUnderreplicationManager m = lmf1.newLedgerUnderreplicationManager();
        Iterator<Long> iter = ledgers.iterator();
        while (iter.hasNext()) {
            m.markLedgerUnderreplicated(iter.next(), missingReplica);
            count++;
        }

        List<Future<Long>> futures = new ArrayList<Future<Long>>();
        for (int i = 0; i < count; i++) {
            futures.add(getLedgerToReplicate(m));
        }

        for (Future<Long> f : futures) {
            Long l = f.get(5, TimeUnit.SECONDS);
            assertTrue(ledgers.remove(l));
        }

        Future<Long> f = getLedgerToReplicate(m);
        try {
            f.get(5, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        Long newl = 0xfefefefefefeL;
        m.markLedgerUnderreplicated(newl, missingReplica);
        assertEquals("Should have got the one just added", newl, f.get(5, TimeUnit.SECONDS));
    }

    /**
     * Test locking for ledger unreplication manager.
     * If there's only one ledger marked for rereplication,
     * and one client has it, it should be locked; another
     * client shouldn't be able to get it. If the first client dies
     * however, the second client should be able to get it.
     */
    @Test
    public void testLocking() throws Exception {
        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf2.newLedgerUnderreplicationManager();

        Long ledger = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledger, missingReplica);
        Future<Long> f = getLedgerToReplicate(m1);
        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I just marked", ledger, l);

        f = getLedgerToReplicate(m2);
        try {
            f.get(5, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        zkc1.close(); // should kill the lock
        zkc1 = null;

        l = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I marked", ledger, l);
    }


    /**
     * Test that when a ledger has been marked as replicated, it
     * will not be offered to anther client.
     * This test checked that by marking two ledgers, and acquiring
     * them on a single client. It marks one as replicated and then
     * the client is killed. We then check that another client can
     * acquire a ledger, and that it's not the one that was previously
     * marked as replicated.
     */
    @Test
    public void testMarkingAsReplicated() throws Exception {
        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf2.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        Long ledgerB = 0xdefadebL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);
        m1.markLedgerUnderreplicated(ledgerB, missingReplica);

        Future<Long> fA = getLedgerToReplicate(m1);
        Future<Long> fB = getLedgerToReplicate(m1);

        Long lA = fA.get(5, TimeUnit.SECONDS);
        Long lB = fB.get(5, TimeUnit.SECONDS);

        assertTrue("Should be the ledgers I just marked",
                   (lA.equals(ledgerA) && lB.equals(ledgerB))
                   || (lA.equals(ledgerB) && lB.equals(ledgerA)));

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(5, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        m1.markLedgerReplicated(lA);
        zkc1.close(); // should kill the lock
        zkc1 = null;

        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I marked", lB, l);
    }

    /**
     * Test releasing of a ledger
     * A ledger is released when a client decides it does not want
     * to replicate it (or cannot at the moment).
     * When a client releases a previously acquired ledger, another
     * client should then be able to acquire it.
     */
    @Test
    public void testRelease() throws Exception {
        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf2.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        Long ledgerB = 0xdefadebL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);
        m1.markLedgerUnderreplicated(ledgerB, missingReplica);

        Future<Long> fA = getLedgerToReplicate(m1);
        Future<Long> fB = getLedgerToReplicate(m1);

        Long lA = fA.get(5, TimeUnit.SECONDS);
        Long lB = fB.get(5, TimeUnit.SECONDS);

        assertTrue("Should be the ledgers I just marked",
                   (lA.equals(ledgerA) && lB.equals(ledgerB))
                   || (lA.equals(ledgerB) && lB.equals(ledgerA)));

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(5, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        m1.markLedgerReplicated(lA);
        m1.releaseUnderreplicatedLedger(lB);

        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I marked", lB, l);
    }

    /**
     * Test that when a failure occurs on a ledger, while the ledger
     * is already being rereplicated, the ledger will still be in the
     * under replicated ledger list when first rereplicating client marks
     * it as replicated.
     */
    @Test
    public void testManyFailures() throws Exception {
        String missingReplica1 = "localhost:3181";
        String missingReplica2 = "localhost:3182";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);

        Future<Long> fA = getLedgerToReplicate(m1);
        Long lA = fA.get(5, TimeUnit.SECONDS);

        m1.markLedgerUnderreplicated(ledgerA, missingReplica2);

        assertEquals("Should be the ledger I just marked",
                     lA, ledgerA);
        m1.markLedgerReplicated(lA);

        Future<Long> f = getLedgerToReplicate(m1);
        lA = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I had marked previously",
                     lA, ledgerA);
    }

    /**
     * Test that when a ledger is marked as underreplicated with
     * the same missing replica twice, only marking as replicated
     * will be enough to remove it from the list.
     */
    @Test
    public void test2reportSame() throws Exception {
        String missingReplica1 = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf1.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf2.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        m2.markLedgerUnderreplicated(ledgerA, missingReplica1);

        // verify duplicate missing replica
        UnderreplicatedLedgerFormat.Builder builderA = UnderreplicatedLedgerFormat
                .newBuilder();
        String znode = getUrLedgerZnode(ledgerA);
        byte[] data = zkc1.getData(znode, false, null);
        TextFormat.merge(new String(data, Charset.forName("UTF-8")), builderA);
        List<String> replicaList = builderA.getReplicaList();
        assertEquals("Published duplicate missing replica : " + replicaList, 1,
                replicaList.size());
        assertTrue("Published duplicate missing replica : " + replicaList,
                replicaList.contains(missingReplica1));

        Future<Long> fA = getLedgerToReplicate(m1);
        Long lA = fA.get(5, TimeUnit.SECONDS);

        assertEquals("Should be the ledger I just marked",
                     lA, ledgerA);
        m1.markLedgerReplicated(lA);

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(5, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
    }

    /**
     * Test that multiple LedgerUnderreplicationManagers should be able to take
     * lock and release for same ledger
     */
    @Test(timeout = 30000)
    public void testMultipleManagersShouldBeAbleToTakeAndReleaseLock()
            throws Exception {
        String missingReplica1 = "localhost:3181";
        final LedgerUnderreplicationManager m1 = lmf1
                .newLedgerUnderreplicationManager();
        final LedgerUnderreplicationManager m2 = lmf2
                .newLedgerUnderreplicationManager();
        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        final int iterationCount = 100;
        final CountDownLatch latch1 = new CountDownLatch(iterationCount);
        final CountDownLatch latch2 = new CountDownLatch(iterationCount);
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                takeLedgerAndRelease(m1, latch1, iterationCount);
            }
        };

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                takeLedgerAndRelease(m2, latch2, iterationCount);
            }
        };
        thread1.start();
        thread2.start();

        // wait until at least one thread completed
        while (!latch1.await(50, TimeUnit.MILLISECONDS)
                && !latch2.await(50, TimeUnit.MILLISECONDS)) {
            Thread.sleep(50);
        }

        m1.close();
        m2.close();

        // After completing 'lock acquire,release' job, it should notify below
        // wait
        latch1.await();
        latch2.await();
    }

    /**
     * Test verifies failures of bookies which are resembling each other.
     *
     * BK servers named like*********************************************
     * 1.cluster.com, 2.cluster.com, 11.cluster.com, 12.cluster.com
     * *******************************************************************
     *
     * BKserver IP:HOST like*********************************************
     * localhost:3181, localhost:318, localhost:31812
     * *******************************************************************
     */
    @Test
    public void testMarkSimilarMissingReplica() throws Exception {
        List<String> missingReplica = new ArrayList<String>();
        missingReplica.add("localhost:3181");
        missingReplica.add("localhost:318");
        missingReplica.add("localhost:31812");
        missingReplica.add("1.cluster.com");
        missingReplica.add("2.cluster.com");
        missingReplica.add("11.cluster.com");
        missingReplica.add("12.cluster.com");
        verifyMarkLedgerUnderreplicated(missingReplica);
    }

    /**
     * Test multiple bookie failures for a ledger and marked as underreplicated
     * one after another.
     */
    @Test
    public void testManyFailuresInAnEnsemble() throws Exception {
        List<String> missingReplica = new ArrayList<String>();
        missingReplica.add("localhost:3181");
        missingReplica.add("localhost:3182");
        verifyMarkLedgerUnderreplicated(missingReplica);
    }

    private void verifyMarkLedgerUnderreplicated(Collection<String> missingReplica)
            throws KeeperException, InterruptedException,
            CompatibilityException, UnavailableException {
        Long ledgerA = 0xfeadeefdacL;
        String znodeA = getUrLedgerZnode(ledgerA);
        LedgerUnderreplicationManager replicaMgr = lmf1
                .newLedgerUnderreplicationManager();
        for (String replica : missingReplica) {
            replicaMgr.markLedgerUnderreplicated(ledgerA, replica);
        }

        String urLedgerA = getData(znodeA);
        UnderreplicatedLedgerFormat.Builder builderA = UnderreplicatedLedgerFormat
                .newBuilder();
        for (String replica : missingReplica) {
            builderA.addReplica(replica);
        }
        List<String> replicaList = builderA.getReplicaList();

        for (String replica : missingReplica) {
            assertTrue("UrLedger:" + urLedgerA
                    + " doesn't contain failed bookie :" + replica, replicaList
                    .contains(replica));
        }
    }

    private String getData(String znode) {
        try {
            byte[] data = zkc1.getData(znode, false, null);
            return new String(data);
        } catch (KeeperException e) {
            LOG.error("Exception while reading data from znode :" + znode);
        } catch (InterruptedException e) {
            LOG.error("Exception while reading data from znode :" + znode);
        }
        return "";

    }

    private String getParentZnodePath(String base, long ledgerId) {
        String subdir1 = String.format("%04x", ledgerId >> 48 & 0xffff);
        String subdir2 = String.format("%04x", ledgerId >> 32 & 0xffff);
        String subdir3 = String.format("%04x", ledgerId >> 16 & 0xffff);
        String subdir4 = String.format("%04x", ledgerId & 0xffff);

        return String.format("%s/%s/%s/%s/%s", base, subdir1, subdir2, subdir3,
                subdir4);
    }

    private String getUrLedgerZnode(long ledgerId) {
        return String.format("%s/urL%010d", getParentZnodePath(urLedgerPath,
                ledgerId), ledgerId);
    }

    private void takeLedgerAndRelease(final LedgerUnderreplicationManager m,
            final CountDownLatch latch, int numberOfIterations) {
        for (int i = 0; i < numberOfIterations; i++) {
            try {
                long ledgerToRereplicate = m.getLedgerToRereplicate();
                m.releaseUnderreplicatedLedger(ledgerToRereplicate);
            } catch (UnavailableException e) {
                LOG.error("UnavailableException when "
                        + "taking or releasing lock", e);
            }
            latch.countDown();
        }
    }
}
