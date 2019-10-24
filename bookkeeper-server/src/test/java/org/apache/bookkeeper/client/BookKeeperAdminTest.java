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
package org.apache.bookkeeper.client;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the bookkeeper admin.
 */
public class BookKeeperAdminTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperAdminTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final int numOfBookies = 2;
    private final int lostBookieRecoveryDelayInitValue = 1800;

    public BookKeeperAdminTest() {
        super(numOfBookies, 480);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);
    }

    @Test
    public void testLostBookieRecoveryDelayValue() throws Exception {
        try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
            assertEquals("LostBookieRecoveryDelay",
                lostBookieRecoveryDelayInitValue, bkAdmin.getLostBookieRecoveryDelay());
            int newLostBookieRecoveryDelayValue = 2400;
            bkAdmin.setLostBookieRecoveryDelay(newLostBookieRecoveryDelayValue);
            assertEquals("LostBookieRecoveryDelay",
                newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
            newLostBookieRecoveryDelayValue = 3000;
            bkAdmin.setLostBookieRecoveryDelay(newLostBookieRecoveryDelayValue);
            assertEquals("LostBookieRecoveryDelay",
                newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
            LOG.info("Test Done");
        }
    }

    @Test
    public void testTriggerAuditWithStoreSystemTimeAsLedgerUnderreplicatedMarkTime() throws Exception {
        testTriggerAudit(true);
    }

    @Test
    public void testTriggerAuditWithoutStoreSystemTimeAsLedgerUnderreplicatedMarkTime() throws Exception {
        testTriggerAudit(false);
    }

    public void testTriggerAudit(boolean storeSystemTimeAsLedgerUnderreplicatedMarkTime) throws Exception {
        ServerConfiguration thisServerConf = new ServerConfiguration(baseConf);
        thisServerConf
                .setStoreSystemTimeAsLedgerUnderreplicatedMarkTime(storeSystemTimeAsLedgerUnderreplicatedMarkTime);
        restartBookies(thisServerConf);
        ClientConfiguration thisClientConf = new ClientConfiguration(baseClientConf);
        thisClientConf
                .setStoreSystemTimeAsLedgerUnderreplicatedMarkTime(storeSystemTimeAsLedgerUnderreplicatedMarkTime);
        long testStartSystime = System.currentTimeMillis();
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(thisClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        int lostBookieRecoveryDelayValue = bkAdmin.getLostBookieRecoveryDelay();
        urLedgerMgr.disableLedgerReplication();
        try {
            bkAdmin.triggerAudit();
            fail("Trigger Audit should have failed because LedgerReplication is disabled");
        } catch (UnavailableException une) {
            // expected
        }
        assertEquals("LostBookieRecoveryDelay", lostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        urLedgerMgr.enableLedgerReplication();
        bkAdmin.triggerAudit();
        assertEquals("LostBookieRecoveryDelay", lostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        long ledgerId = 1L;
        LedgerHandle ledgerHandle = bkc.createLedgerAdv(ledgerId, numBookies, numBookies, numBookies, digestType,
                PASSWORD.getBytes(), null);
        ledgerHandle.addEntry(0, "data".getBytes());
        ledgerHandle.close();

        BookieServer bookieToKill = bs.get(1);
        killBookie(1);
        /*
         * since lostBookieRecoveryDelay is set, when a bookie is died, it will
         * not start Audit process immediately. But when triggerAudit is called
         * it will force audit process.
         */
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<UnderreplicatedLedger> underreplicatedLedgerItr = urLedgerMgr.listLedgersToRereplicate(null);
        assertTrue("There are supposed to be underreplicatedledgers", underreplicatedLedgerItr.hasNext());
        UnderreplicatedLedger underreplicatedLedger = underreplicatedLedgerItr.next();
        assertEquals("Underreplicated ledgerId", ledgerId, underreplicatedLedger.getLedgerId());
        assertTrue("Missingreplica of Underreplicated ledgerId should contain " + bookieToKill.getLocalAddress(),
                underreplicatedLedger.getReplicaList().contains(bookieToKill.getLocalAddress().toString()));
        if (storeSystemTimeAsLedgerUnderreplicatedMarkTime) {
            long ctimeOfURL = underreplicatedLedger.getCtime();
            assertTrue("ctime of underreplicated ledger should be greater than test starttime",
                    (ctimeOfURL > testStartSystime) && (ctimeOfURL < System.currentTimeMillis()));
        } else {
            assertEquals("ctime of underreplicated ledger should not be set", UnderreplicatedLedger.UNASSIGNED_CTIME,
                    underreplicatedLedger.getCtime());
        }
        bkAdmin.close();
    }

    @Test
    public void testBookieInit() throws Exception {
        int bookieindex = 0;
        ServerConfiguration confOfExistingBookie = bsConfs.get(bookieindex);
        Assert.assertFalse("initBookie shouldn't have succeeded, since bookie is still running with that configuration",
                BookKeeperAdmin.initBookie(confOfExistingBookie));
        killBookie(bookieindex);
        Assert.assertFalse("initBookie shouldn't have succeeded, since previous bookie is not formatted yet",
                BookKeeperAdmin.initBookie(confOfExistingBookie));

        File[] journalDirs = confOfExistingBookie.getJournalDirs();
        for (File journalDir : journalDirs) {
            FileUtils.deleteDirectory(journalDir);
        }
        Assert.assertFalse("initBookie shouldn't have succeeded, since previous bookie is not formatted yet completely",
                BookKeeperAdmin.initBookie(confOfExistingBookie));

        File[] ledgerDirs = confOfExistingBookie.getLedgerDirs();
        for (File ledgerDir : ledgerDirs) {
            FileUtils.deleteDirectory(ledgerDir);
        }
        Assert.assertFalse("initBookie shouldn't have succeeded, since previous bookie is not formatted yet completely",
                BookKeeperAdmin.initBookie(confOfExistingBookie));

        File[] indexDirs = confOfExistingBookie.getIndexDirs();
        if (indexDirs != null) {
            for (File indexDir : indexDirs) {
                FileUtils.deleteDirectory(indexDir);
            }
        }
        Assert.assertFalse("initBookie shouldn't have succeeded, since cookie in ZK is not deleted yet",
                BookKeeperAdmin.initBookie(confOfExistingBookie));
        String bookieId = Bookie.getBookieAddress(confOfExistingBookie).toString();
        String bookieCookiePath =
            ZKMetadataDriverBase.resolveZkLedgersRootPath(confOfExistingBookie)
                + "/" + BookKeeperConstants.COOKIE_NODE
                + "/" + bookieId;
        zkc.delete(bookieCookiePath, -1);

        Assert.assertTrue("initBookie shouldn't succeeded",
                BookKeeperAdmin.initBookie(confOfExistingBookie));
    }

    @Test
    public void testInitNewCluster() throws Exception {
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        String ledgersRootPath = "/testledgers";
        newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
        Assert.assertTrue("New cluster should be initialized successfully", BookKeeperAdmin.initNewCluster(newConfig));

        Assert.assertTrue("Cluster rootpath should have been created successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) != null));
        String availableBookiesPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE;
        Assert.assertTrue("AvailableBookiesPath should have been created successfully " + availableBookiesPath,
                (zkc.exists(availableBookiesPath, false) != null));
        String readonlyBookiesPath = availableBookiesPath + "/" + READONLY;
        Assert.assertTrue("ReadonlyBookiesPath should have been created successfully " + readonlyBookiesPath,
            (zkc.exists(readonlyBookiesPath, false) != null));
        String instanceIdPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
            + "/" + BookKeeperConstants.INSTANCEID;
        Assert.assertTrue("InstanceId node should have been created successfully" + instanceIdPath,
                (zkc.exists(instanceIdPath, false) != null));

        String ledgersLayout = ledgersRootPath + "/" + BookKeeperConstants.LAYOUT_ZNODE;
        Assert.assertTrue("Layout node should have been created successfully" + ledgersLayout,
                (zkc.exists(ledgersLayout, false) != null));

        /**
         * create znodes simulating existence of Bookies in the cluster
         */
        int numOfBookies = 3;
        Random rand = new Random();
        for (int i = 0; i < numOfBookies; i++) {
            String ipString = InetAddresses.fromInteger(rand.nextInt()).getHostAddress();
            String regPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
                + "/" + AVAILABLE_NODE + "/" + ipString + ":3181";
            zkc.create(regPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        /*
         * now it should be possible to create ledger and delete the same
         */
        BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
        LedgerHandle lh = bk.createLedger(numOfBookies, numOfBookies, numOfBookies, BookKeeper.DigestType.MAC,
                new byte[0]);
        bk.deleteLedger(lh.ledgerId);
        bk.close();
    }

    @Test
    public void testNukeExistingClusterWithForceOption() throws Exception {
        String ledgersRootPath = "/testledgers";
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
        List<String> bookiesRegPaths = new ArrayList<String>();
        initiateNewClusterAndCreateLedgers(newConfig, bookiesRegPaths);

        /*
         * before nuking existing cluster, bookies shouldn't be registered
         * anymore
         */
        for (int i = 0; i < bookiesRegPaths.size(); i++) {
            zkc.delete(bookiesRegPaths.get(i), -1);
        }

        Assert.assertTrue("New cluster should be nuked successfully",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, null, true));
        Assert.assertTrue("Cluster rootpath should have been deleted successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) == null));
    }

    @Test
    public void testNukeExistingClusterWithInstanceId() throws Exception {
        String ledgersRootPath = "/testledgers";
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
        List<String> bookiesRegPaths = new ArrayList<String>();
        initiateNewClusterAndCreateLedgers(newConfig, bookiesRegPaths);

        /*
         * before nuking existing cluster, bookies shouldn't be registered
         * anymore
         */
        for (int i = 0; i < bookiesRegPaths.size(); i++) {
            zkc.delete(bookiesRegPaths.get(i), -1);
        }

        byte[] data = zkc.getData(
            ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + BookKeeperConstants.INSTANCEID,
            false, null);
        String readInstanceId = new String(data, UTF_8);

        Assert.assertTrue("New cluster should be nuked successfully",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false));
        Assert.assertTrue("Cluster rootpath should have been deleted successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) == null));
    }

    @Test
    public void tryNukingExistingClustersWithInvalidParams() throws Exception {
        String ledgersRootPath = "/testledgers";
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
        List<String> bookiesRegPaths = new ArrayList<String>();
        initiateNewClusterAndCreateLedgers(newConfig, bookiesRegPaths);

        /*
         * create ledger with a specific ledgerid
         */
        BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
        long ledgerId = 23456789L;
        LedgerHandle lh = bk.createLedgerAdv(ledgerId, 1, 1, 1, BookKeeper.DigestType.MAC, new byte[0], null);
        lh.close();

        /*
         * read instanceId
         */
        byte[] data = zkc.getData(
            ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + BookKeeperConstants.INSTANCEID,
            false, null);
        String readInstanceId = new String(data, UTF_8);

        /*
         * register a RO bookie
         */
        String ipString = InetAddresses.fromInteger((new Random()).nextInt()).getHostAddress();
        String roBookieRegPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
            + "/" + AVAILABLE_NODE + "/" + READONLY + "/" + ipString + ":3181";
        zkc.create(roBookieRegPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Assert.assertFalse("Cluster should'nt be nuked since instanceid is not provided and force option is not set",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, null, false));
        Assert.assertFalse("Cluster should'nt be nuked since incorrect instanceid is provided",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, "incorrectinstanceid", false));
        Assert.assertFalse("Cluster should'nt be nuked since bookies are still registered",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false));
        /*
         * delete all rw bookies registration
         */
        for (int i = 0; i < bookiesRegPaths.size(); i++) {
            zkc.delete(bookiesRegPaths.get(i), -1);
        }
        Assert.assertFalse("Cluster should'nt be nuked since ro bookie is still registered",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false));

        /*
         * make sure no node is deleted
         */
        Assert.assertTrue("Cluster rootpath should be existing " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) != null));
        String availableBookiesPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE;
        Assert.assertTrue("AvailableBookiesPath should be existing " + availableBookiesPath,
                (zkc.exists(availableBookiesPath, false) != null));
        String instanceIdPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
            + "/" + BookKeeperConstants.INSTANCEID;
        Assert.assertTrue("InstanceId node should be existing" + instanceIdPath,
                (zkc.exists(instanceIdPath, false) != null));
        String ledgersLayout = ledgersRootPath + "/" + BookKeeperConstants.LAYOUT_ZNODE;
        Assert.assertTrue("Layout node should be existing" + ledgersLayout, (zkc.exists(ledgersLayout, false) != null));

        /*
         * ledger should not be deleted.
         */
        lh = bk.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.MAC, new byte[0]);
        lh.close();
        bk.close();

        /*
         * delete ro bookie reg znode
         */
        zkc.delete(roBookieRegPath, -1);

        Assert.assertTrue("Cluster should be nuked since no bookie is registered",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false));
        Assert.assertTrue("Cluster rootpath should have been deleted successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) == null));
    }

    void initiateNewClusterAndCreateLedgers(ServerConfiguration newConfig, List<String> bookiesRegPaths)
            throws Exception {
        Assert.assertTrue("New cluster should be initialized successfully", BookKeeperAdmin.initNewCluster(newConfig));

        /**
         * create znodes simulating existence of Bookies in the cluster
         */
        int numberOfBookies = 3;
        Random rand = new Random();
        for (int i = 0; i < numberOfBookies; i++) {
            String ipString = InetAddresses.fromInteger(rand.nextInt()).getHostAddress();
            bookiesRegPaths.add(ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
                + "/" + AVAILABLE_NODE + "/" + ipString + ":3181");
            zkc.create(bookiesRegPaths.get(i), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        /*
         * now it should be possible to create ledger and delete the same
         */
        BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
        LedgerHandle lh;
        int numOfLedgers = 5;
        for (int i = 0; i < numOfLedgers; i++) {
            lh = bk.createLedger(numberOfBookies, numberOfBookies, numberOfBookies, BookKeeper.DigestType.MAC,
                    new byte[0]);
            lh.close();
        }
        bk.close();
    }

    @Test
    public void testGetListOfEntriesOfClosedLedger() throws Exception {
        testGetListOfEntriesOfLedger(true);
    }

    @Test
    public void testGetListOfEntriesOfNotClosedLedger() throws Exception {
        testGetListOfEntriesOfLedger(false);
    }

    @Test
    public void testGetListOfEntriesOfNonExistingLedger() throws Exception {
        long nonExistingLedgerId = 56789L;
        try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
            for (int i = 0; i < bs.size(); i++) {
                CompletableFuture<AvailabilityOfEntriesOfLedger> futureResult = bkAdmin
                        .asyncGetListOfEntriesOfLedger(bs.get(i).getLocalAddress(), nonExistingLedgerId);
                try {
                    futureResult.get();
                    fail("asyncGetListOfEntriesOfLedger is supposed to be failed with NoSuchLedgerExistsException");
                } catch (ExecutionException ee) {
                    assertTrue(ee.getCause() instanceof BKException);
                    BKException e = (BKException) ee.getCause();
                    assertEquals(e.getCode(), BKException.Code.NoSuchLedgerExistsException);
                }
            }
        }
    }

    public void testGetListOfEntriesOfLedger(boolean isLedgerClosed) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        int numOfEntries = 6;
        BookKeeper bkc = new BookKeeper(conf);
        LedgerHandle lh = bkc.createLedger(numOfBookies, numOfBookies, digestType, "testPasswd".getBytes());
        long lId = lh.getId();
        for (int i = 0; i < numOfEntries; i++) {
            lh.addEntry("000".getBytes());
        }
        if (isLedgerClosed) {
            lh.close();
        }
        try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
            for (int i = 0; i < bs.size(); i++) {
                CompletableFuture<AvailabilityOfEntriesOfLedger> futureResult = bkAdmin
                        .asyncGetListOfEntriesOfLedger(bs.get(i).getLocalAddress(), lId);
                AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = futureResult.get();
                assertEquals("Number of entries", numOfEntries,
                        availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
                for (int j = 0; j < numOfEntries; j++) {
                    assertTrue("Entry should be available: " + j, availabilityOfEntriesOfLedger.isEntryAvailable(j));
                }
                assertFalse("Entry should not be available: " + numOfEntries,
                        availabilityOfEntriesOfLedger.isEntryAvailable(numOfEntries));
            }
        }
        bkc.close();
    }

    @Test
    public void testGetListOfEntriesOfLedgerWithJustOneBookieInWriteQuorum() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        int numOfEntries = 6;
        BookKeeper bkc = new BookKeeper(conf);
        /*
         * in this testsuite there are going to be 2 (numOfBookies) and if
         * writeQuorum is 1 then it will stripe entries to those two bookies.
         */
        LedgerHandle lh = bkc.createLedger(2, 1, digestType, "testPasswd".getBytes());
        long lId = lh.getId();
        for (int i = 0; i < numOfEntries; i++) {
            lh.addEntry("000".getBytes());
        }

        try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
            for (int i = 0; i < bs.size(); i++) {
                CompletableFuture<AvailabilityOfEntriesOfLedger> futureResult = bkAdmin
                        .asyncGetListOfEntriesOfLedger(bs.get(i).getLocalAddress(), lId);
                AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = futureResult.get();
                /*
                 * since num of bookies in the ensemble is 2 and
                 * writeQuorum/ackQuorum is 1, it will stripe to these two
                 * bookies and hence in each bookie there will be only
                 * numOfEntries/2 entries.
                 */
                assertEquals("Number of entries", numOfEntries / 2,
                        availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
            }
        }
        bkc.close();
    }

    @Test
    public void testGetBookies() throws Exception {
        String ledgersRootPath = "/ledgers";
        Assert.assertTrue("Cluster rootpath should have been created successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) != null));
        String bookieCookiePath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf)
                + "/" + BookKeeperConstants.COOKIE_NODE;
        Assert.assertTrue("AvailableBookiesPath should have been created successfully " + bookieCookiePath,
                (zkc.exists(bookieCookiePath, false) != null));

        try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
            Collection<BookieSocketAddress> availableBookies = bkAdmin.getAvailableBookies();
            Assert.assertEquals(availableBookies.size(), bs.size());

            for (int i = 0; i < bs.size(); i++) {
                availableBookies.contains(bs.get(i).getLocalAddress());
            }

            BookieServer killedBookie = bs.get(1);
            killBookieAndWaitForZK(1);

            Collection<BookieSocketAddress> remainingBookies = bkAdmin.getAvailableBookies();
            Assert.assertFalse(remainingBookies.contains(killedBookie.getLocalAddress()));

            Collection<BookieSocketAddress> allBookies = bkAdmin.getAllBookies();
            for (int i = 0; i < bs.size(); i++) {
                remainingBookies.contains(bs.get(i).getLocalAddress());
                allBookies.contains(bs.get(i).getLocalAddress());
            }

            Assert.assertEquals(remainingBookies.size(), allBookies.size() - 1);
            Assert.assertTrue(allBookies.contains(killedBookie.getLocalAddress()));
        }
    }

    @Test
    public void testGetListOfEntriesOfLedgerWithEntriesNotStripedToABookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);
        /*
         * in this testsuite there are going to be 2 (numOfBookies) bookies and
         * we are having ensemble of size 2.
         */
        LedgerHandle lh = bkc.createLedger(2, 1, digestType, "testPasswd".getBytes());
        long lId = lh.getId();
        /*
         * ledger is writeclosed without adding any entry.
         */
        lh.close();
        CountDownLatch callbackCalled = new CountDownLatch(1);
        AtomicBoolean exceptionInCallback = new AtomicBoolean(false);
        AtomicInteger exceptionCode = new AtomicInteger(BKException.Code.OK);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        /*
         * since no entry is added, callback is supposed to fail with
         * NoSuchLedgerExistsException.
         */
        bkAdmin.asyncGetListOfEntriesOfLedger(bs.get(0).getLocalAddress(), lId)
                .whenComplete((availabilityOfEntriesOfLedger, throwable) -> {
                    exceptionInCallback.set(throwable != null);
                    if (throwable != null) {
                        exceptionCode.set(BKException.getExceptionCode(throwable));
                    }
                    callbackCalled.countDown();
                });
        callbackCalled.await();
        assertTrue("Exception occurred", exceptionInCallback.get());
        assertEquals("Exception code", BKException.Code.NoSuchLedgerExistsException, exceptionCode.get());
        bkAdmin.close();
        bkc.close();
    }

    @Test
    public void testAreEntriesOfLedgerStoredInTheBookieForMultipleSegments() throws Exception {
        int lastEntryId = 10;
        long ledgerId = 100L;
        BookieSocketAddress bookie0 = new BookieSocketAddress("bookie0:3181");
        BookieSocketAddress bookie1 = new BookieSocketAddress("bookie1:3181");
        BookieSocketAddress bookie2 = new BookieSocketAddress("bookie2:3181");
        BookieSocketAddress bookie3 = new BookieSocketAddress("bookie3:3181");

        List<BookieSocketAddress> ensembleOfSegment1 = new ArrayList<BookieSocketAddress>();
        ensembleOfSegment1.add(bookie0);
        ensembleOfSegment1.add(bookie1);
        ensembleOfSegment1.add(bookie2);

        List<BookieSocketAddress> ensembleOfSegment2 = new ArrayList<BookieSocketAddress>();
        ensembleOfSegment2.add(bookie3);
        ensembleOfSegment2.add(bookie1);
        ensembleOfSegment2.add(bookie2);

        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create();
        builder.withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withDigestType(digestType.toApiDigestType())
                .withPassword(PASSWORD.getBytes())
                .newEnsembleEntry(0, ensembleOfSegment1)
                .newEnsembleEntry(lastEntryId + 1, ensembleOfSegment2)
                .withLastEntryId(lastEntryId).withLength(65576).withClosedState();
        LedgerMetadata meta = builder.build();

        assertFalse("expected areEntriesOfLedgerStoredInTheBookie to return False for bookie3",
                BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerId, bookie3, meta));
        assertTrue("expected areEntriesOfLedgerStoredInTheBookie to return true for bookie2",
                BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerId, bookie2, meta));
    }
}
