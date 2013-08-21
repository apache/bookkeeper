/**
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdapter;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.MultiLedgerManagerTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the ReplicationWroker, where it has to replicate the fragments from
 * failed Bookies to given target Bookie.
 */
public class TestReplicationWorker extends MultiLedgerManagerTestCase {

    private static final byte[] TESTPASSWD = "testpasswd".getBytes();
    private static final Logger LOG = LoggerFactory
            .getLogger(TestReplicationWorker.class);
    private String basePath = "";
    private LedgerManagerFactory mFactory;
    private LedgerUnderreplicationManager underReplicationManager;
    private static byte[] data = "TestReplicationWorker".getBytes();

    public TestReplicationWorker(String ledgerManagerFactory) {
        super(3);
        LOG.info("Running test case using ledger manager : "
                + ledgerManagerFactory);
        // set ledger manager name
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        basePath = baseClientConf.getZkLedgersRootPath() + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
        baseConf.setRereplicationEntryBatchSize(3);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // initialize urReplicationManager
        mFactory = LedgerManagerFactory.newLedgerManagerFactory(baseClientConf,
                zkc);
        underReplicationManager = mFactory.newLedgerUnderreplicationManager();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if(null != mFactory){
            mFactory.uninitialize();
            mFactory = null;
        }
        if(null != underReplicationManager){
            underReplicationManager.close();
            underReplicationManager = null;
        }
    }
    
    /**
     * Tests that replication worker should replicate the failed bookie
     * fragments to target bookie given to the worker.
     */
    @Test(timeout = 30000)
    public void testRWShouldReplicateFragmentsToTargetBookie() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        InetSocketAddress replicaToKill = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        int startNewBookie = startNewBookie();
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);
        LOG.info("New Bookie addr :" + newBkAddr);

        ReplicationWorker rw = new ReplicationWorker(zkc, baseConf, newBkAddr);

        rw.start();
        try {

            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());

            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }

            killAllBookies(lh, newBkAddr);

            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
        } finally {
            rw.shutdown();
        }
    }

    /**
     * Tests that replication worker should retry for replication until enough
     * bookies available for replication
     */
    @Test(timeout = 60000)
    public void testRWShouldRetryUntilThereAreEnoughBksAvailableForReplication()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        InetSocketAddress replicaToKill = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie", replicaToKill);
        ServerConfiguration killedBookieConfig = killBookie(replicaToKill);

        int startNewBookie = startNewBookie();
        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);
        LOG.info("New Bookie addr :" + newBkAddr);

        killAllBookies(lh, newBkAddr);
        ReplicationWorker rw = new ReplicationWorker(zkc, baseConf, newBkAddr);

        rw.start();
        try {
            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            int counter = 100;
            while (counter-- > 0) {
                assertTrue("Expecting that replication should not complete",
                        ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                                .getId(), basePath));
                Thread.sleep(100);
            }
            // restart killed bookie
            bs.add(startBookie(killedBookieConfig));
            bsConfs.add(killedBookieConfig);
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }
            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
        } finally {
            rw.shutdown();
        }
    }

    /**
     * Tests that replication worker1 should take one fragment replication and
     * other replication worker also should compete for the replication.
     */
    @Test(timeout = 90000)
    public void test2RWsShouldCompeteForReplicationOf2FragmentsAndCompleteReplication()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        InetSocketAddress replicaToKill = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie", replicaToKill);
        ServerConfiguration killedBookieConfig = killBookie(replicaToKill);

        killAllBookies(lh, null);
        // Starte RW1
        int startNewBookie1 = startNewBookie();
        InetSocketAddress newBkAddr1 = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie1);
        LOG.info("New Bookie addr :" + newBkAddr1);
        ReplicationWorker rw1 = new ReplicationWorker(zkc, baseConf, newBkAddr1);

        // Starte RW2
        int startNewBookie2 = startNewBookie();
        InetSocketAddress newBkAddr2 = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie2);
        LOG.info("New Bookie addr :" + newBkAddr2);
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(10000);
        ZooKeeper zkc1 = ZkUtils.createConnectedZookeeperClient(
                zkUtil.getZooKeeperConnectString(), w);
        ReplicationWorker rw2 = new ReplicationWorker(zkc1, baseConf,
                newBkAddr2);
        rw1.start();
        rw2.start();

        try {
            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            int counter = 10;
            while (counter-- > 0) {
                assertTrue("Expecting that replication should not complete",
                        ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                                .getId(), basePath));
                Thread.sleep(100);
            }
            // restart killed bookie
            bs.add(startBookie(killedBookieConfig));
            bsConfs.add(killedBookieConfig);
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }
            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
        } finally {
            rw1.shutdown();
            rw2.shutdown();
            zkc1.close();
        }
    }

    /**
     * Tests that Replication worker should clean the leadger under replication
     * node of the ledger already deleted
     */
    @Test(timeout = 3000)
    public void testRWShouldCleanTheLedgerFromUnderReplicationIfLedgerAlreadyDeleted()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        InetSocketAddress replicaToKill = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        int startNewBookie = startNewBookie();
        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);
        LOG.info("New Bookie addr :" + newBkAddr);
        ReplicationWorker rw = new ReplicationWorker(zkc, baseConf, newBkAddr);
        rw.start();

        try {
            bkc.deleteLedger(lh.getId()); // Deleting the ledger
            // Also mark ledger as in UnderReplication
            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }
        } finally {
            rw.shutdown();
        }

    }

    @Test(timeout = 60000)
    public void testMultipleLedgerReplicationWithReplicationWorker()
            throws Exception {
        // Ledger1
        LedgerHandle lh1 = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh1.addEntry(data);
        }
        InetSocketAddress replicaToKillFromFirstLedger = LedgerHandleAdapter
                .getLedgerMetadata(lh1).getEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKillFromFirstLedger);

        // Ledger2
        LedgerHandle lh2 = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh2.addEntry(data);
        }
        InetSocketAddress replicaToKillFromSecondLedger = LedgerHandleAdapter
                .getLedgerMetadata(lh2).getEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKillFromSecondLedger);

        // Kill ledger1
        killBookie(replicaToKillFromFirstLedger);
        lh1.close();
        // Kill ledger2
        killBookie(replicaToKillFromFirstLedger);
        lh2.close();

        int startNewBookie = startNewBookie();

        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);
        LOG.info("New Bookie addr :" + newBkAddr);

        ReplicationWorker rw = new ReplicationWorker(zkc, baseConf, newBkAddr);

        rw.start();
        try {

            // Mark ledger1 and 2 as underreplicated
            underReplicationManager.markLedgerUnderreplicated(lh1.getId(),
                    replicaToKillFromFirstLedger.toString());
            underReplicationManager.markLedgerUnderreplicated(lh2.getId(),
                    replicaToKillFromSecondLedger.toString());

            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh1
                    .getId(), basePath)) {
                Thread.sleep(100);
            }

            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh2
                    .getId(), basePath)) {
                Thread.sleep(100);
            }

            killAllBookies(lh1, newBkAddr);

            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh1, 0, 9);
            verifyRecoveredLedgers(lh2, 0, 9);
        } finally {
            rw.shutdown();
        }

    }
    
    /**
     * Tests that ReplicationWorker should fence the ledger and release ledger
     * lock after timeout. Then replication should happen normally.
     */
    @Test(timeout = 60000)
    public void testRWShouldReplicateTheLedgersAfterTimeoutIfLastFragmentIsUR()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        InetSocketAddress replicaToKill = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        int startNewBookie = startNewBookie();

        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);
        LOG.info("New Bookie addr :" + newBkAddr);

        ReplicationWorker rw = new ReplicationWorker(zkc, baseConf, newBkAddr);

        LedgerManagerFactory mFactory = LedgerManagerFactory
                .newLedgerManagerFactory(baseClientConf, zkc);
        LedgerUnderreplicationManager underReplicationManager = mFactory
                .newLedgerUnderreplicationManager();
        rw.start();
        try {

            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }
            killAllBookies(lh, newBkAddr);
            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
            lh = bkc.openLedgerNoRecovery(lh.getId(),
                    BookKeeper.DigestType.CRC32, TESTPASSWD);
            assertFalse("Ledger must have been closed by RW", ClientUtil
                    .isLedgerOpen(lh));
        } finally {
            rw.shutdown();
            underReplicationManager.close();
        }

    }

    /**
     * Tests that ReplicationWorker should not have identified for postponing
     * the replication if ledger is in open state and lastFragment is not in
     * underReplication state. Note that RW should not fence such ledgers.
     */
    @Test(timeout = 30000)
    public void testRWShouldReplicateTheLedgersAfterTimeoutIfLastFragmentIsNotUR()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        InetSocketAddress replicaToKill = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        int startNewBookie = startNewBookie();

        // Reform ensemble...Making sure that last fragment is not in
        // under-replication
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);
        LOG.info("New Bookie addr :" + newBkAddr);

        ReplicationWorker rw = new ReplicationWorker(zkc, baseConf, newBkAddr);

        LedgerManagerFactory mFactory = LedgerManagerFactory
                .newLedgerManagerFactory(baseClientConf, zkc);
        LedgerUnderreplicationManager underReplicationManager = mFactory
                .newLedgerUnderreplicationManager();

        rw.start();
        try {

            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }

            killAllBookies(lh, newBkAddr);

            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
            lh = bkc.openLedgerNoRecovery(lh.getId(),
                    BookKeeper.DigestType.CRC32, TESTPASSWD);

            // Ledger should be still in open state
            assertTrue("Ledger must have been closed by RW", ClientUtil
                    .isLedgerOpen(lh));
        } finally {
            rw.shutdown();
            underReplicationManager.close();
        }

    }

    /**
     * Test that if the local bookie turns out to be readonly, then no point in running RW. So RW should shutdown.
     */
    @Test(timeout = 20000)
    public void testRWShutdownOnLocalBookieReadonlyTransition() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32, TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        InetSocketAddress replicaToKill = LedgerHandleAdapter.getLedgerMetadata(lh).getEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        int newBkPort = startNewBookie();
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), newBkPort);
        LOG.info("New Bookie addr :" + newBkAddr);

        ReplicationWorker rw = new ReplicationWorker(zkc, baseConf, newBkAddr);

        rw.start();
        try {
            BookieServer newBk = bs.get(bs.size() - 1);
            bsConfs.get(bsConfs.size() - 1).setReadOnlyModeEnabled(true);
            newBk.getBookie().transitionToReadOnlyMode();
            underReplicationManager.markLedgerUnderreplicated(lh.getId(), replicaToKill.toString());
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath) && rw.isRunning()) {
                Thread.sleep(100);
            }
            assertFalse("RW should shutdown if the bookie is readonly", rw.isRunning());
        } finally {
            rw.shutdown();
        }
    }

    /**
     * Test that the replication worker will shutdown if it lose its zookeeper session
     */
    @Test(timeout=30000)
    public void testRWZKSessionLost() throws Exception {
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(10000);
        ZooKeeper zk = ZkUtils.createConnectedZookeeperClient(
                zkUtil.getZooKeeperConnectString(), w);

        try {
            ReplicationWorker rw = new ReplicationWorker(zk, baseConf, getBookie(0));
            rw.start();
            for (int i = 0; i < 10; i++) {
                if (rw.isRunning()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertTrue("Replication worker should be running", rw.isRunning());
            stopZKCluster();

            for (int i = 0; i < 10; i++) {
                if (!rw.isRunning()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertFalse("Replication worker should have shut down", rw.isRunning());
        } finally {
            zk.close();
        }
    }

    private void killAllBookies(LedgerHandle lh, InetSocketAddress excludeBK)
            throws Exception {
        // Killing all bookies except newly replicated bookie
        Set<Entry<Long, ArrayList<InetSocketAddress>>> entrySet = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().entrySet();
        for (Entry<Long, ArrayList<InetSocketAddress>> entry : entrySet) {
            ArrayList<InetSocketAddress> bookies = entry.getValue();
            for (InetSocketAddress bookie : bookies) {
                if (bookie.equals(excludeBK)) {
                    continue;
                }
                killBookie(bookie);
            }
        }
    }

    private void verifyRecoveredLedgers(LedgerHandle lh, long startEntryId,
            long endEntryId) throws BKException, InterruptedException {
        LedgerHandle lhs = bkc.openLedgerNoRecovery(lh.getId(),
                BookKeeper.DigestType.CRC32, TESTPASSWD);
        Enumeration<LedgerEntry> entries = lhs.readEntries(startEntryId,
                endEntryId);
        assertTrue("Should have the elements", entries.hasMoreElements());
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("TestReplicationWorker", new String(entry.getEntry()));
        }
    }

}
