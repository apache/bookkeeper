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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Cleanup;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the ReplicationWroker, where it has to replicate the fragments from
 * failed Bookies to given target Bookie.
 */
public class TestReplicationWorker extends BookKeeperClusterTestCase {

    private static final byte[] TESTPASSWD = "testpasswd".getBytes();
    private static final Logger LOG = LoggerFactory
            .getLogger(TestReplicationWorker.class);
    private String basePath = "";
    private String baseLockPath = "";
    private MetadataBookieDriver driver;
    private LedgerManagerFactory mFactory;
    private LedgerUnderreplicationManager underReplicationManager;
    private static byte[] data = "TestReplicationWorker".getBytes();
    private OrderedScheduler scheduler;

    public TestReplicationWorker() {
        this("org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory");
    }

    TestReplicationWorker(String ledgerManagerFactory) {
        super(3);
        LOG.info("Running test case using ledger manager : "
                + ledgerManagerFactory);
        // set ledger manager name
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseConf.setRereplicationEntryBatchSize(3);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        String zkLedgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseClientConf);
        basePath = zkLedgersRootPath + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
        baseLockPath = zkLedgersRootPath + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE
                + "/locks";

        this.scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-scheduler")
            .numThreads(1)
            .build();

        this.driver = MetadataDrivers.getBookieDriver(
            URI.create(baseConf.getMetadataServiceUri()));
        this.driver.initialize(
            baseConf,
            () -> {},
            NullStatsLogger.INSTANCE);
        // initialize urReplicationManager
        mFactory = driver.getLedgerManagerFactory();
        underReplicationManager = mFactory.newLedgerUnderreplicationManager();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (null != underReplicationManager){
            underReplicationManager.close();
            underReplicationManager = null;
        }
        if (null != driver) {
            driver.close();
        }
    }

    /**
     * Tests that replication worker should replicate the failed bookie
     * fragments to target bookie given to the worker.
     */
    @Test
    public void testRWShouldReplicateFragmentsToTargetBookie() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        BookieSocketAddress replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        BookieSocketAddress newBkAddr = startNewBookieAndReturnAddress();
        LOG.info("New Bookie addr : {}", newBkAddr);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        ReplicationWorker rw = new ReplicationWorker(baseConf);

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
     * bookies available for replication.
     */
    @Test
    public void testRWShouldRetryUntilThereAreEnoughBksAvailableForReplication()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        BookieSocketAddress replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie", replicaToKill);
        ServerConfiguration killedBookieConfig = killBookie(replicaToKill);

        BookieSocketAddress newBkAddr = startNewBookieAndReturnAddress();
        LOG.info("New Bookie addr :" + newBkAddr);

        killAllBookies(lh, newBkAddr);
        ReplicationWorker rw = new ReplicationWorker(baseConf);

        rw.start();
        try {
            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            int counter = 30;
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
    @Test
    public void test2RWsShouldCompeteForReplicationOf2FragmentsAndCompleteReplication()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        BookieSocketAddress replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie", replicaToKill);
        ServerConfiguration killedBookieConfig = killBookie(replicaToKill);

        killAllBookies(lh, null);
        // Starte RW1
        BookieSocketAddress newBkAddr1 = startNewBookieAndReturnAddress();
        LOG.info("New Bookie addr : {}", newBkAddr1);
        ReplicationWorker rw1 = new ReplicationWorker(baseConf);

        // Starte RW2
        BookieSocketAddress newBkAddr2 = startNewBookieAndReturnAddress();
        LOG.info("New Bookie addr : {}", newBkAddr2);
        ReplicationWorker rw2 = new ReplicationWorker(baseConf);
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
        }
    }

    /**
     * Tests that Replication worker should clean the leadger under replication
     * node of the ledger already deleted.
     */
    @Test
    public void testRWShouldCleanTheLedgerFromUnderReplicationIfLedgerAlreadyDeleted()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        BookieSocketAddress replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        BookieSocketAddress newBkAddr = startNewBookieAndReturnAddress();
        LOG.info("New Bookie addr : {}", newBkAddr);
        ReplicationWorker rw = new ReplicationWorker(baseConf);
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

    @Test
    public void testMultipleLedgerReplicationWithReplicationWorker()
            throws Exception {
        // Ledger1
        LedgerHandle lh1 = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh1.addEntry(data);
        }
        BookieSocketAddress replicaToKillFromFirstLedger = lh1.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKillFromFirstLedger);

        // Ledger2
        LedgerHandle lh2 = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh2.addEntry(data);
        }
        BookieSocketAddress replicaToKillFromSecondLedger = lh2.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKillFromSecondLedger);

        // Kill ledger1
        killBookie(replicaToKillFromFirstLedger);
        lh1.close();
        // Kill ledger2
        killBookie(replicaToKillFromFirstLedger);
        lh2.close();

        BookieSocketAddress newBkAddr = startNewBookieAndReturnAddress();
        LOG.info("New Bookie addr : {}", newBkAddr);

        ReplicationWorker rw = new ReplicationWorker(baseConf);

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
    @Test
    public void testRWShouldReplicateTheLedgersAfterTimeoutIfLastFragmentIsUR()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        BookieSocketAddress replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        BookieSocketAddress newBkAddr = startNewBookieAndReturnAddress();
        LOG.info("New Bookie addr : {}", newBkAddr);

        // set to 3s instead of default 30s
        baseConf.setOpenLedgerRereplicationGracePeriod("3000");
        ReplicationWorker rw = new ReplicationWorker(baseConf);

        @Cleanup MetadataClientDriver clientDriver = MetadataDrivers.getClientDriver(
            URI.create(baseClientConf.getMetadataServiceUri()));
        clientDriver.initialize(baseClientConf, scheduler, NullStatsLogger.INSTANCE, Optional.empty());

        LedgerManagerFactory mFactory = clientDriver.getLedgerManagerFactory();

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
     * Tests that ReplicationWorker will not make more than
     * ReplicationWorker.MAXNUMBER_REPLICATION_FAILURES_ALLOWED_BEFORE_DEFERRING
     * number of replication failure attempts and if it fails more these many
     * number of times then it will defer lock release by
     * lockReleaseOfFailedLedgerGracePeriod.
     *
     * @throws Exception
     */
    @Test
    public void testBookiesNotAvailableScenarioForReplicationWorker() throws Exception {
        int ensembleSize = 3;
        LedgerHandle lh = bkc.createLedger(ensembleSize, ensembleSize, BookKeeper.DigestType.CRC32, TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();

        BookieSocketAddress[] bookiesKilled = new BookieSocketAddress[ensembleSize];
        ServerConfiguration[] killedBookiesConfig = new ServerConfiguration[ensembleSize];

        // kill all bookies
        for (int i = 0; i < ensembleSize; i++) {
            bookiesKilled[i] = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(i);
            killedBookiesConfig[i] = getBkConf(bookiesKilled[i]);
            LOG.info("Killing Bookie", bookiesKilled[i]);
            killBookie(bookiesKilled[i]);
        }

        // start new bookiesToKill number of bookies
        for (int i = 0; i < ensembleSize; i++) {
            BookieSocketAddress newBkAddr = startNewBookieAndReturnAddress();
        }

        // create couple of replicationworkers
        baseConf.setLockReleaseOfFailedLedgerGracePeriod("500");
        ReplicationWorker rw1 = new ReplicationWorker(baseConf);
        ReplicationWorker rw2 = new ReplicationWorker(baseConf);

        @Cleanup
        MetadataClientDriver clientDriver = MetadataDrivers
                .getClientDriver(URI.create(baseClientConf.getMetadataServiceUri()));
        clientDriver.initialize(baseClientConf, scheduler, NullStatsLogger.INSTANCE, Optional.empty());

        LedgerManagerFactory mFactory = clientDriver.getLedgerManagerFactory();

        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        try {
            for (int i = 0; i < bookiesKilled.length; i++) {
                underReplicationManager.markLedgerUnderreplicated(lh.getId(), bookiesKilled[i].toString());
            }
            while (!ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath)) {
                Thread.sleep(100);
            }
            rw1.start();
            rw2.start();

            AtomicBoolean isBookieRestarted = new AtomicBoolean(false);

            (new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(4000);
                        isBookieRestarted.set(true);
                        /*
                         * after sleeping for 4000 msecs, restart one of the
                         * bookie, so that replication can succeed.
                         */
                        startBookie(killedBookiesConfig[0]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            })).start();

            while (!isBookieRestarted.get()) {
                /*
                 * since all the bookies containing the ledger entries are down
                 * replication wouldnt have succeeded.
                 */
                assertTrue("Ledger: " + lh.getId() + " should be underreplicated",
                        ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath));
                /*
                 * check for both the replicationworkders number of failed
                 * attempts should be less than ReplicationWorker.
                 * MAXNUMBER_REPLICATION_FAILURES_ALLOWED_BEFORE_DEFERRING
                 */
                int failedAttempts = rw1.replicationFailedLedgers.get(lh.getId()).get();
                assertTrue(
                        "The number of failed attempts should be less than "
                                + "ReplicationWorker.MAXNUMBER_REPLICATION_FAILURES_ALLOWED_BEFORE_DEFERRING, "
                                + "but it is "
                                + failedAttempts,
                        failedAttempts <= ReplicationWorker.MAXNUMBER_REPLICATION_FAILURES_ALLOWED_BEFORE_DEFERRING);

                failedAttempts = rw2.replicationFailedLedgers.get(lh.getId()).get();
                assertTrue(
                        "The number of failed attempts should be less than "
                                + "ReplicationWorker.MAXNUMBER_REPLICATION_FAILURES_ALLOWED_BEFORE_DEFERRING, "
                                + "but it is "
                                + failedAttempts,
                        failedAttempts <= ReplicationWorker.MAXNUMBER_REPLICATION_FAILURES_ALLOWED_BEFORE_DEFERRING);

                Thread.sleep(50);
            }

            /**
             * since one of the killed bookie is restarted, replicationworker
             * should succeed in replicating this under replicated ledger and it
             * shouldn't be under replicated anymore.
             */
            int timeToWaitForReplicationToComplete = 2000;
            int timeWaited = 0;
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath)) {
                Thread.sleep(100);
                timeWaited += 100;
                if (timeWaited == timeToWaitForReplicationToComplete) {
                    fail("Ledger should be replicated by now");
                }
            }
        } finally {
            rw1.shutdown();
            rw2.shutdown();
            underReplicationManager.close();
        }
    }

    /**
     * Tests that ReplicationWorker should not have identified for postponing
     * the replication if ledger is in open state and lastFragment is not in
     * underReplication state. Note that RW should not fence such ledgers.
     */
    @Test
    public void testRWShouldReplicateTheLedgersAfterTimeoutIfLastFragmentIsNotUR()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        BookieSocketAddress replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        BookieSocketAddress newBkAddr = startNewBookieAndReturnAddress();
        LOG.info("New Bookie addr : {}", newBkAddr);

        // Reform ensemble...Making sure that last fragment is not in
        // under-replication
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        ReplicationWorker rw = new ReplicationWorker(baseConf);

        baseClientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        @Cleanup MetadataClientDriver driver = MetadataDrivers.getClientDriver(
            URI.create(baseClientConf.getMetadataServiceUri()));
        driver.initialize(baseClientConf, scheduler, NullStatsLogger.INSTANCE, Optional.empty());

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();

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
     * Test that the replication worker will not shutdown on a simple ZK disconnection.
     */
    @Test
    public void testRWZKConnectionLost() throws Exception {
        try (ZooKeeperClient zk = ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .sessionTimeoutMs(10000)
                .build()) {

            ReplicationWorker rw = new ReplicationWorker(baseConf);
            rw.start();
            for (int i = 0; i < 10; i++) {
                if (rw.isRunning()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertTrue("Replication worker should be running", rw.isRunning());

            stopZKCluster();
            // Wait for disconnection to be picked up
            for (int i = 0; i < 10; i++) {
                if (!zk.getState().isConnected()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertFalse(zk.getState().isConnected());
            startZKCluster();

            assertTrue("Replication worker should still be running", rw.isRunning());
        }
    }

    private void killAllBookies(LedgerHandle lh, BookieSocketAddress excludeBK)
            throws Exception {
        // Killing all bookies except newly replicated bookie
        for (Entry<Long, ? extends List<BookieSocketAddress>> entry :
                 lh.getLedgerMetadata().getAllEnsembles().entrySet()) {
            List<BookieSocketAddress> bookies = entry.getValue();
            for (BookieSocketAddress bookie : bookies) {
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
