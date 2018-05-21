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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
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

        BookieServer bookieToKill = bs.get(1);
        killBookie(1);
        /*
         * since lostBookieRecoveryDelay is set, when a bookie is died, it will
         * not start Audit process immediately. But when triggerAudit is called
         * it will force audit process.
         */
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<Map.Entry<Long, List<String>>> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null,
                true);
        assertTrue("There are supposed to be underreplicatedledgers", ledgersToRereplicate.hasNext());
        Entry<Long, List<String>> urlWithReplicaList = ledgersToRereplicate.next();
        assertEquals("Underreplicated ledgerId", ledgerId, urlWithReplicaList.getKey().longValue());
        assertTrue("Missingreplica of Underreplicated ledgerId should contain " + bookieToKill.getLocalAddress(),
                urlWithReplicaList.getValue().contains(bookieToKill.getLocalAddress().toString()));
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
}
