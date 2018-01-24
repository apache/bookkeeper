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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BKException.BKIllegalOpException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.annotations.FlakyTest;
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
    private static final int numOfBookies = 6;
    private final int lostBookieRecoveryDelayInitValue = 1800;

    public BookKeeperAdminTest() {
        super(numOfBookies, 480);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);
    }

    @Test
    public void testLostBookieRecoveryDelayValue() throws Exception {
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        assertEquals("LostBookieRecoveryDelay", lostBookieRecoveryDelayInitValue, bkAdmin.getLostBookieRecoveryDelay());
        int newLostBookieRecoveryDelayValue = 2400;
        bkAdmin.setLostBookieRecoveryDelay(newLostBookieRecoveryDelayValue);
        assertEquals("LostBookieRecoveryDelay", newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        assertEquals("LostBookieRecoveryDelay", newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        newLostBookieRecoveryDelayValue = 3000;
        bkAdmin.setLostBookieRecoveryDelay(newLostBookieRecoveryDelayValue);
        assertEquals("LostBookieRecoveryDelay", newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        bkAdmin.close();
    }

    @Test
    public void testTriggerAudit() throws Exception {
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
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

        killBookie(1);
        /*
         * since lostBookieRecoveryDelay is set, when a bookie is died, it will
         * not start Audit process immediately. But when triggerAudit is called
         * it will force audit process.
         */
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<Long> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        assertTrue("There are supposed to be underreplicatedledgers", ledgersToRereplicate.hasNext());
        assertEquals("Underreplicated ledgerId", ledgerId, ledgersToRereplicate.next().longValue());
        bkAdmin.close();
    }

    @FlakyTest("https://github.com/apache/bookkeeper/issues/502")
    public void testDecommissionBookie() throws Exception {
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());

        int numOfLedgers = 2 * numOfBookies;
        int numOfEntries = 2 * numOfBookies;
        for (int i = 0; i < numOfLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 2, digestType, PASSWORD.getBytes());
            for (int j = 0; j < numOfEntries; j++) {
                lh.addEntry("entry".getBytes());
            }
            lh.close();
        }
        /*
         * create ledgers having empty segments (segment with no entries)
         */
        for (int i = 0; i < numOfLedgers; i++) {
            LedgerHandle emptylh = bkc.createLedger(3, 2, digestType, PASSWORD.getBytes());
            emptylh.close();
        }

        try {
            /*
             * if we try to call decommissionBookie for a bookie which is not
             * shutdown, then it should throw BKIllegalOpException
             */
            bkAdmin.decommissionBookie(bs.get(0).getLocalAddress());
            fail("Expected BKIllegalOpException because that bookie is not shutdown yet");
        } catch (BKIllegalOpException bkioexc) {
            // expected IllegalException
        }

        ServerConfiguration killedBookieConf = killBookie(1);
        /*
         * this decommisionBookie should make sure that there are no
         * underreplicated ledgers because of this bookie
         */
        bkAdmin.decommissionBookie(Bookie.getBookieAddress(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<Long> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                Long ledgerId = ledgersToRereplicate.next();
                LOG.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
        }

        killedBookieConf = killBookie(0);
        bkAdmin.decommissionBookie(Bookie.getBookieAddress(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                Long ledgerId = ledgersToRereplicate.next();
                LOG.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
        }
        bkAdmin.close();
    }

    @Test
    public void testDecommissionForLedgersWithMultipleSegmentsAndNotWriteClosed() throws Exception {
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        int numOfEntries = 2 * numOfBookies;

        LedgerHandle lh1 = bkc.createLedgerAdv(1L, numBookies, 3, 3, digestType, PASSWORD.getBytes(), null);
        LedgerHandle lh2 = bkc.createLedgerAdv(2L, numBookies, 3, 3, digestType, PASSWORD.getBytes(), null);
        LedgerHandle lh3 = bkc.createLedgerAdv(3L, numBookies, 3, 3, digestType, PASSWORD.getBytes(), null);
        LedgerHandle lh4 = bkc.createLedgerAdv(4L, numBookies, 3, 3, digestType, PASSWORD.getBytes(), null);
        for (int j = 0; j < numOfEntries; j++) {
            lh1.addEntry(j, "data".getBytes());
            lh2.addEntry(j, "data".getBytes());
            lh3.addEntry(j, "data".getBytes());
            lh4.addEntry(j, "data".getBytes());
        }

        startNewBookie();

        assertEquals("Number of Available Bookies", numOfBookies + 1, bkAdmin.getAvailableBookies().size());

        ServerConfiguration killedBookieConf = killBookie(0);

        /*
         * since one of the bookie is killed, ensemble change happens when next
         * write is made.So new fragment will be created for those 2 ledgers.
         */
        for (int j = numOfEntries; j < 2 * numOfEntries; j++) {
            lh1.addEntry(j, "data".getBytes());
            lh2.addEntry(j, "data".getBytes());
        }

        /*
         * Here lh1 and lh2 have multiple fragments and are writeclosed. But lh3 and lh4 are
         * not writeclosed and contains only one fragment.
         */
        lh1.close();
        lh2.close();

        /*
         * If the last fragment of the ledger is underreplicated and if the
         * ledger is not closed then it will remain underreplicated for
         * openLedgerRereplicationGracePeriod (by default 30 secs). For more
         * info. Check BOOKKEEPER-237 and BOOKKEEPER-325. But later
         * ReplicationWorker will fence the ledger.
         */
        bkAdmin.decommissionBookie(Bookie.getBookieAddress(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<Long> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                Long ledgerId = ledgersToRereplicate.next();
                LOG.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
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
        String bookieCookiePath = confOfExistingBookie.getZkLedgersRootPath() + "/" + BookKeeperConstants.COOKIE_NODE
                + "/" + bookieId;
        zkc.delete(bookieCookiePath, -1);

        Assert.assertTrue("initBookie shouldn't succeeded",
                BookKeeperAdmin.initBookie(confOfExistingBookie));
    }

    @Test
    public void testInitNewCluster() throws Exception {
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        String ledgersRootPath = "/testledgers";
        newConfig.setZkLedgersRootPath(ledgersRootPath);
        newConfig.setZkServers(zkUtil.getZooKeeperConnectString());
        Assert.assertTrue("New cluster should be initialized successfully", BookKeeperAdmin.initNewCluster(newConfig));

        Assert.assertTrue("Cluster rootpath should have been created successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) != null));
        String availableBookiesPath = newConfig.getZkAvailableBookiesPath();
        Assert.assertTrue("AvailableBookiesPath should have been created successfully " + availableBookiesPath,
                (zkc.exists(availableBookiesPath, false) != null));
        String instanceIdPath = newConfig.getZkLedgersRootPath() + "/" + BookKeeperConstants.INSTANCEID;
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
            String regPath = newConfig.getZkAvailableBookiesPath() + "/" + ipString + ":3181";
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
        newConfig.setZkLedgersRootPath(ledgersRootPath);
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
        newConfig.setZkLedgersRootPath(ledgersRootPath);
        List<String> bookiesRegPaths = new ArrayList<String>();
        initiateNewClusterAndCreateLedgers(newConfig, bookiesRegPaths);

        /*
         * before nuking existing cluster, bookies shouldn't be registered
         * anymore
         */
        for (int i = 0; i < bookiesRegPaths.size(); i++) {
            zkc.delete(bookiesRegPaths.get(i), -1);
        }

        byte[] data = zkc.getData(newConfig.getZkLedgersRootPath() + "/" + BookKeeperConstants.INSTANCEID, false, null);
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
        newConfig.setZkLedgersRootPath(ledgersRootPath);
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
        byte[] data = zkc.getData(newConfig.getZkLedgersRootPath() + "/" + BookKeeperConstants.INSTANCEID, false, null);
        String readInstanceId = new String(data, UTF_8);

        /*
         * register a RO bookie
         */
        String ipString = InetAddresses.fromInteger((new Random()).nextInt()).getHostAddress();
        String roBookieRegPath = newConfig.getZkAvailableBookiesPath() + "/" + BookKeeperConstants.READONLY + "/"
                + ipString + ":3181";
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
        String availableBookiesPath = newConfig.getZkAvailableBookiesPath();
        Assert.assertTrue("AvailableBookiesPath should be existing " + availableBookiesPath,
                (zkc.exists(availableBookiesPath, false) != null));
        String instanceIdPath = newConfig.getZkLedgersRootPath() + "/" + BookKeeperConstants.INSTANCEID;
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
        newConfig.setZkServers(zkUtil.getZooKeeperConnectString());
        Assert.assertTrue("New cluster should be initialized successfully", BookKeeperAdmin.initNewCluster(newConfig));

        /**
         * create znodes simulating existence of Bookies in the cluster
         */
        int numberOfBookies = 3;
        Random rand = new Random();
        for (int i = 0; i < numberOfBookies; i++) {
            String ipString = InetAddresses.fromInteger(rand.nextInt()).getHostAddress();
            bookiesRegPaths.add(newConfig.getZkAvailableBookiesPath() + "/" + ipString + ":3181");
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
}
