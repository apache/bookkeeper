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
package org.apache.bookkeeper.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.CookieValidation;
import org.apache.bookkeeper.bookie.LegacyCookieValidation;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the bookkeeper admin.
 */
public class BookKeeperAdminTest extends BookKeeperClusterTestCase {

  private static final Logger LOG = LoggerFactory.getLogger(BookKeeperAdminTest.class);

  private static final String PASSWORD = "testPasswd";

  private static final int numOfBookies = 2;

  private final int lostBookieRecoveryDelayInitValue = 1800;

  private final DigestType digestType = DigestType.CRC32;

  public BookKeeperAdminTest() {
    super(numOfBookies, 480);
    baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
    baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
    setAutoRecoveryEnabled(true);
  }

  @Test
  void lostBookieRecoveryDelayValue() throws Exception {
    try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
      assertEquals(lostBookieRecoveryDelayInitValue, bkAdmin.getLostBookieRecoveryDelay(),
          "LostBookieRecoveryDelay");
      int newLostBookieRecoveryDelayValue = 2400;
      bkAdmin.setLostBookieRecoveryDelay(newLostBookieRecoveryDelayValue);
      assertEquals(newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay(),
          "LostBookieRecoveryDelay");
      newLostBookieRecoveryDelayValue = 3000;
      bkAdmin.setLostBookieRecoveryDelay(newLostBookieRecoveryDelayValue);
      assertEquals(newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay(),
          "LostBookieRecoveryDelay");
      LOG.info("Test Done");
    }
  }

  @Test
  void triggerAuditWithStoreSystemTimeAsLedgerUnderreplicatedMarkTime() throws Exception {
    testTriggerAudit(true);
  }

  @Test
  void triggerAuditWithoutStoreSystemTimeAsLedgerUnderreplicatedMarkTime() throws Exception {
    testTriggerAudit(false);
  }

  public void testTriggerAudit(boolean storeSystemTimeAsLedgerUnderreplicatedMarkTime)
      throws Exception {
    restartBookies(c -> {
      c.setStoreSystemTimeAsLedgerUnderreplicatedMarkTime(
          storeSystemTimeAsLedgerUnderreplicatedMarkTime);
      return c;
    });
    ClientConfiguration thisClientConf = new ClientConfiguration(baseClientConf);
    thisClientConf
        .setStoreSystemTimeAsLedgerUnderreplicatedMarkTime(
            storeSystemTimeAsLedgerUnderreplicatedMarkTime);
    long testStartSystime = System.currentTimeMillis();
    ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(
        thisClientConf, zkc);
    BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
    int lostBookieRecoveryDelayValue = bkAdmin.getLostBookieRecoveryDelay();
    urLedgerMgr.disableLedgerReplication();
    try {
      bkAdmin.triggerAudit();
      fail("Trigger Audit should have failed because LedgerReplication is disabled");
    } catch (UnavailableException une) {
      // expected
    }
    assertEquals(lostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay(),
        "LostBookieRecoveryDelay");
    urLedgerMgr.enableLedgerReplication();
    bkAdmin.triggerAudit();
    assertEquals(lostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay(),
        "LostBookieRecoveryDelay");
    long ledgerId = 1L;
    LedgerHandle ledgerHandle =
        bkc.createLedgerAdv(ledgerId, numBookies, numBookies, numBookies, digestType,
            PASSWORD.getBytes(), null);
    ledgerHandle.addEntry(0, "data".getBytes());
    ledgerHandle.close();

    BookieServer bookieToKill = serverByIndex(1);
    killBookie(1);
    /*
     * since lostBookieRecoveryDelay is set, when a bookie is died, it will
     * not start Audit process immediately. But when triggerAudit is called
     * it will force audit process.
     */
    bkAdmin.triggerAudit();
    Thread.sleep(500);
    Iterator<UnderreplicatedLedger> underreplicatedLedgerItr = urLedgerMgr
        .listLedgersToRereplicate(null);
    assertTrue(underreplicatedLedgerItr.hasNext(),
        "There are supposed to be underreplicatedledgers");
    UnderreplicatedLedger underreplicatedLedger = underreplicatedLedgerItr.next();
    assertEquals(ledgerId, underreplicatedLedger.getLedgerId(), "Underreplicated ledgerId");
    assertTrue(underreplicatedLedger.getReplicaList().contains(bookieToKill.getBookieId().getId()),
        "Missingreplica of Underreplicated ledgerId should contain " + bookieToKill);
    if (storeSystemTimeAsLedgerUnderreplicatedMarkTime) {
      long ctimeOfURL = underreplicatedLedger.getCtime();
      assertTrue((ctimeOfURL > testStartSystime) && (ctimeOfURL < System.currentTimeMillis()),
          "ctime of underreplicated ledger should be greater than test starttime");
    } else {
      assertEquals(UnderreplicatedLedger.UNASSIGNED_CTIME, underreplicatedLedger.getCtime(),
          "ctime of underreplicated ledger should not be set");
    }
    bkAdmin.close();
  }

  @Test
  void bookieInit() throws Exception {
    ServerConfiguration confOfExistingBookie = newServerConfiguration();
    BookieId bookieId = BookieImpl.getBookieId(confOfExistingBookie);
    try (
        MetadataBookieDriver driver =
            BookieResources.createMetadataDriver(confOfExistingBookie, NullStatsLogger.INSTANCE);
        RegistrationManager rm = driver.createRegistrationManager()) {
      CookieValidation cookieValidation = new LegacyCookieValidation(confOfExistingBookie, rm);
      cookieValidation.checkCookies(Main.storageDirectoriesFromConf(confOfExistingBookie));
      rm.registerBookie(bookieId, false /* readOnly */, BookieServiceInfo.EMPTY);
      assertFalse(BookKeeperAdmin.initBookie(confOfExistingBookie),
          "initBookie shouldn't have succeeded, since bookie is still running with that configuration");
    }

    assertFalse(BookKeeperAdmin.initBookie(confOfExistingBookie),
        "initBookie shouldn't have succeeded, since previous bookie is not formatted yet completely");

    File[] ledgerDirs = confOfExistingBookie.getLedgerDirs();
    for (File ledgerDir : ledgerDirs) {
      FileUtils.deleteDirectory(ledgerDir);
    }
    assertFalse(BookKeeperAdmin.initBookie(confOfExistingBookie),
        "initBookie shouldn't have succeeded, since previous bookie is not formatted yet completely");

    File[] indexDirs = confOfExistingBookie.getIndexDirs();
    if (indexDirs != null) {
      for (File indexDir : indexDirs) {
        FileUtils.deleteDirectory(indexDir);
      }
    }
    assertFalse(BookKeeperAdmin.initBookie(confOfExistingBookie),
        "initBookie shouldn't have succeeded, since cookie in ZK is not deleted yet");
    String bookieCookiePath =
        ZKMetadataDriverBase.resolveZkLedgersRootPath(confOfExistingBookie) + "/"
            + BookKeeperConstants.COOKIE_NODE + "/" + bookieId.toString();
    zkc.delete(bookieCookiePath, -1);

    assertTrue(BookKeeperAdmin.initBookie(confOfExistingBookie), "initBookie shouldn't succeeded");
  }

  @Test
  void initNewCluster() throws Exception {
    ServerConfiguration newConfig = new ServerConfiguration(baseConf);
    String ledgersRootPath = "/testledgers";
    newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
    assertTrue(BookKeeperAdmin.initNewCluster(newConfig),
        "New cluster should be initialized successfully");

    assertTrue((zkc.exists(ledgersRootPath, false) != null),
        "Cluster rootpath should have been created successfully " + ledgersRootPath);
    String availableBookiesPath =
        ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE;
    assertTrue((zkc.exists(availableBookiesPath, false) != null),
        "AvailableBookiesPath should have been created successfully " + availableBookiesPath);
    String readonlyBookiesPath = availableBookiesPath + "/" + READONLY;
    assertTrue((zkc.exists(readonlyBookiesPath, false) != null),
        "ReadonlyBookiesPath should have been created successfully " + readonlyBookiesPath);
    String instanceIdPath =
        ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/"
            + BookKeeperConstants.INSTANCEID;
    assertTrue((zkc.exists(instanceIdPath, false) != null),
        "InstanceId node should have been created successfully" + instanceIdPath);

    String ledgersLayout = ledgersRootPath + "/" + BookKeeperConstants.LAYOUT_ZNODE;
    assertTrue((zkc.exists(ledgersLayout, false) != null),
        "Layout node should have been created successfully" + ledgersLayout);

    /**
     * create znodes simulating existence of Bookies in the cluster
     */
    int numOfBookies = 3;
    Random rand = new Random();
    for (int i = 0; i < numOfBookies; i++) {
      String ipString = InetAddresses.fromInteger(rand.nextInt()).getHostAddress();
      String regPath =
          ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE + "/"
              + ipString + ":3181";
      zkc.create(regPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /*
     * now it should be possible to create ledger and delete the same
     */
    BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
    LedgerHandle lh =
        bk.createLedger(numOfBookies, numOfBookies, numOfBookies, BookKeeper.DigestType.MAC,
            new byte[0]);
    bk.deleteLedger(lh.ledgerId);
    bk.close();
  }

  @Test
  void nukeExistingClusterWithForceOption() throws Exception {
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

    assertTrue(BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, null, true),
        "New cluster should be nuked successfully");
    assertTrue((zkc.exists(ledgersRootPath, false) == null),
        "Cluster rootpath should have been deleted successfully " + ledgersRootPath);
  }

  @Test
  void nukeExistingClusterWithInstanceId() throws Exception {
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

    byte[] data =
        zkc.getData(ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/"
                + BookKeeperConstants.INSTANCEID,
            false, null);
    String readInstanceId = new String(data, UTF_8);

    assertTrue(
        BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false),
        "New cluster should be nuked successfully");
    assertTrue((zkc.exists(ledgersRootPath, false) == null),
        "Cluster rootpath should have been deleted successfully " + ledgersRootPath);
  }

  @Test
  void tryNukingExistingClustersWithInvalidParams() throws Exception {
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
    LedgerHandle lh = bk
        .createLedgerAdv(ledgerId, 1, 1, 1, BookKeeper.DigestType.MAC, new byte[0], null);
    lh.close();

    /*
     * read instanceId
     */
    byte[] data =
        zkc.getData(ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/"
                + BookKeeperConstants.INSTANCEID,
            false, null);
    String readInstanceId = new String(data, UTF_8);

    /*
     * register a RO bookie
     */
    String ipString = InetAddresses.fromInteger((new Random()).nextInt()).getHostAddress();
    String roBookieRegPath =
        ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE + "/"
            + READONLY + "/" + ipString + ":3181";
    zkc.create(roBookieRegPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

    assertFalse(BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, null, false),
        "Cluster should'nt be nuked since instanceid is not provided and force option is not set");
    assertFalse(BookKeeperAdmin
            .nukeExistingCluster(newConfig, ledgersRootPath, "incorrectinstanceid", false),
        "Cluster should'nt be nuked since incorrect instanceid is provided");
    assertFalse(
        BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false),
        "Cluster should'nt be nuked since bookies are still registered");
    /*
     * delete all rw bookies registration
     */
    for (int i = 0; i < bookiesRegPaths.size(); i++) {
      zkc.delete(bookiesRegPaths.get(i), -1);
    }
    assertFalse(
        BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false),
        "Cluster should'nt be nuked since ro bookie is still registered");

    /*
     * make sure no node is deleted
     */
    assertTrue((zkc.exists(ledgersRootPath, false) != null),
        "Cluster rootpath should be existing " + ledgersRootPath);
    String availableBookiesPath =
        ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE;
    assertTrue((zkc.exists(availableBookiesPath, false) != null),
        "AvailableBookiesPath should be existing " + availableBookiesPath);
    String instanceIdPath =
        ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/"
            + BookKeeperConstants.INSTANCEID;
    assertTrue((zkc.exists(instanceIdPath, false) != null),
        "InstanceId node should be existing" + instanceIdPath);
    String ledgersLayout = ledgersRootPath + "/" + BookKeeperConstants.LAYOUT_ZNODE;
    assertTrue((zkc.exists(ledgersLayout, false) != null),
        "Layout node should be existing" + ledgersLayout);

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

    assertTrue(
        BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false),
        "Cluster should be nuked since no bookie is registered");
    assertTrue((zkc.exists(ledgersRootPath, false) == null),
        "Cluster rootpath should have been deleted successfully " + ledgersRootPath);
  }

  void initiateNewClusterAndCreateLedgers(ServerConfiguration newConfig,
      List<String> bookiesRegPaths)
      throws Exception {
    assertTrue(BookKeeperAdmin.initNewCluster(newConfig),
        "New cluster should be initialized successfully");

    /**
     * create znodes simulating existence of Bookies in the cluster
     */
    int numberOfBookies = 3;
    Random rand = new Random();
    for (int i = 0; i < numberOfBookies; i++) {
      String ipString = InetAddresses.fromInteger(rand.nextInt()).getHostAddress();
      bookiesRegPaths
          .add(ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE + "/"
              + ipString + ":3181");
      zkc.create(bookiesRegPaths.get(i), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /*
     * now it should be possible to create ledger and delete the same
     */
    BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
    LedgerHandle lh;
    int numOfLedgers = 5;
    for (int i = 0; i < numOfLedgers; i++) {
      lh = bk.createLedger(numberOfBookies, numberOfBookies, numberOfBookies,
          BookKeeper.DigestType.MAC,
          new byte[0]);
      lh.close();
    }
    bk.close();
  }

  @Test
  void getListOfEntriesOfClosedLedger() throws Exception {
    testGetListOfEntriesOfLedger(true);
  }

  @Test
  void getListOfEntriesOfNotClosedLedger() throws Exception {
    testGetListOfEntriesOfLedger(false);
  }

  @Test
  void getListOfEntriesOfNonExistingLedger() throws Exception {
    long nonExistingLedgerId = 56789L;

    try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
      for (int i = 0; i < bookieCount(); i++) {
        CompletableFuture<AvailabilityOfEntriesOfLedger> futureResult =
            bkAdmin.asyncGetListOfEntriesOfLedger(addressByIndex(i), nonExistingLedgerId);
        try {
          futureResult.get();
          fail(
              "asyncGetListOfEntriesOfLedger is supposed to be failed with NoSuchLedgerExistsException");
        } catch (ExecutionException ee) {
          assertTrue(ee.getCause() instanceof BKException);
          BKException e = (BKException) ee.getCause();
          assertEquals(BKException.Code.NoSuchLedgerExistsException, e.getCode());
        }
      }
    }
  }

  public void testGetListOfEntriesOfLedger(boolean isLedgerClosed) throws Exception {
    ClientConfiguration conf = new ClientConfiguration();
    conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
    int numOfEntries = 6;
    BookKeeper bkc = new BookKeeper(conf);
    LedgerHandle lh = bkc
        .createLedger(numOfBookies, numOfBookies, digestType, "testPasswd".getBytes());
    long lId = lh.getId();
    for (int i = 0; i < numOfEntries; i++) {
      lh.addEntry("000".getBytes());
    }
    if (isLedgerClosed) {
      lh.close();
    }
    try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
      for (int i = 0; i < bookieCount(); i++) {
        CompletableFuture<AvailabilityOfEntriesOfLedger> futureResult =
            bkAdmin.asyncGetListOfEntriesOfLedger(addressByIndex(i), lId);
        AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = futureResult.get();
        assertEquals(numOfEntries, availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(),
            "Number of entries");
        for (int j = 0; j < numOfEntries; j++) {
          assertTrue(availabilityOfEntriesOfLedger.isEntryAvailable(j),
              "Entry should be available: " + j);
        }
        assertFalse(availabilityOfEntriesOfLedger.isEntryAvailable(numOfEntries),
            "Entry should not be available: " + numOfEntries);
      }
    }
    bkc.close();
  }

  @Test
  void getEntriesFromEmptyLedger() throws Exception {
    ClientConfiguration conf = new ClientConfiguration();
    conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
    BookKeeper bkc = new BookKeeper(conf);
    LedgerHandle lh = bkc
        .createLedger(numOfBookies, numOfBookies, digestType, "testPasswd".getBytes(UTF_8));
    lh.close();
    long ledgerId = lh.getId();

    try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
      Iterator<LedgerEntry> iter = bkAdmin.readEntries(ledgerId, 0, 0).iterator();
      assertFalse(iter.hasNext());
    }

    bkc.close();
  }

  @Test
  void getListOfEntriesOfLedgerWithJustOneBookieInWriteQuorum() throws Exception {
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
      for (int i = 0; i < bookieCount(); i++) {
        CompletableFuture<AvailabilityOfEntriesOfLedger> futureResult =
            bkAdmin.asyncGetListOfEntriesOfLedger(addressByIndex(i), lId);
        AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = futureResult.get();
        /*
         * since num of bookies in the ensemble is 2 and
         * writeQuorum/ackQuorum is 1, it will stripe to these two
         * bookies and hence in each bookie there will be only
         * numOfEntries/2 entries.
         */
        assertEquals(numOfEntries / 2,
            availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(),
            "Number of entries");
      }
    }
    bkc.close();
  }

  @Test
  void getBookies() throws Exception {
    String ledgersRootPath = "/ledgers";
    assertTrue((zkc.exists(ledgersRootPath, false) != null),
        "Cluster rootpath should have been created successfully " + ledgersRootPath);
    String bookieCookiePath =
        ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf) + "/"
            + BookKeeperConstants.COOKIE_NODE;
    assertTrue((zkc.exists(bookieCookiePath, false) != null),
        "AvailableBookiesPath should have been created successfully " + bookieCookiePath);

    try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
      Collection<BookieId> availableBookies = bkAdmin.getAvailableBookies();
      assertEquals(availableBookies.size(), bookieCount());

      for (int i = 0; i < bookieCount(); i++) {
        availableBookies.contains(addressByIndex(i));
      }

      BookieServer killedBookie = serverByIndex(1);
      killBookieAndWaitForZK(1);

      Collection<BookieId> remainingBookies = bkAdmin.getAvailableBookies();
      assertFalse(remainingBookies.contains(killedBookie));

      Collection<BookieId> allBookies = bkAdmin.getAllBookies();
      for (int i = 0; i < bookieCount(); i++) {
        remainingBookies.contains(addressByIndex(i));
        allBookies.contains(addressByIndex(i));
      }

      assertEquals(remainingBookies.size(), allBookies.size() - 1);
      assertTrue(allBookies.contains(killedBookie.getBookieId()));
    }
  }

  @Test
  void getListOfEntriesOfLedgerWithEntriesNotStripedToABookie() throws Exception {
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
    bkAdmin.asyncGetListOfEntriesOfLedger(addressByIndex(0), lId)
        .whenComplete((availabilityOfEntriesOfLedger, throwable) -> {
          exceptionInCallback.set(throwable != null);
          if (throwable != null) {
            exceptionCode.set(BKException.getExceptionCode(throwable));
          }
          callbackCalled.countDown();
        });
    callbackCalled.await();
    assertTrue(exceptionInCallback.get(), "Exception occurred");
    assertEquals(BKException.Code.NoSuchLedgerExistsException, exceptionCode.get(),
        "Exception code");
    bkAdmin.close();
    bkc.close();
  }

  @Test
  void areEntriesOfLedgerStoredInTheBookieForLastEmptySegment() throws Exception {
    int lastEntryId = 10;
    long ledgerId = 100L;
    BookieId bookie0 = new BookieSocketAddress("bookie0:3181").toBookieId();
    BookieId bookie1 = new BookieSocketAddress("bookie1:3181").toBookieId();
    BookieId bookie2 = new BookieSocketAddress("bookie2:3181").toBookieId();
    BookieId bookie3 = new BookieSocketAddress("bookie3:3181").toBookieId();

    List<BookieId> ensembleOfSegment1 = new ArrayList<BookieId>();
    ensembleOfSegment1.add(bookie0);
    ensembleOfSegment1.add(bookie1);
    ensembleOfSegment1.add(bookie2);

    List<BookieId> ensembleOfSegment2 = new ArrayList<BookieId>();
    ensembleOfSegment2.add(bookie3);
    ensembleOfSegment2.add(bookie1);
    ensembleOfSegment2.add(bookie2);

    LedgerMetadataBuilder builder = LedgerMetadataBuilder.create();
    builder.withId(ledgerId).withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
        .withDigestType(digestType.toApiDigestType()).withPassword(PASSWORD.getBytes())
        .newEnsembleEntry(0, ensembleOfSegment1)
        .newEnsembleEntry(lastEntryId + 1, ensembleOfSegment2)
        .withLastEntryId(lastEntryId).withLength(65576).withClosedState();
    LedgerMetadata meta = builder.build();

    assertFalse(BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerId, bookie3, meta),
        "expected areEntriesOfLedgerStoredInTheBookie to return False for bookie3");
    assertTrue(BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerId, bookie2, meta),
        "expected areEntriesOfLedgerStoredInTheBookie to return true for bookie2");
  }

  @Test
  void bookkeeperAdminFormatResetsLedgerIds() throws Exception {
    ClientConfiguration conf = new ClientConfiguration();
    conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

    /*
     * in this testsuite there are going to be 2 (numOfBookies) ledgers
     * written and when formatting the BookieAdmin i expect that the
     * ledger ids restart from 0
     */
    int numOfLedgers = 2;
    try (BookKeeper bkc = new BookKeeper(conf)) {
      Set<Long> ledgerIds = new HashSet<>();
      for (int n = 0; n < numOfLedgers; n++) {
        try (LedgerHandle lh = bkc
            .createLedger(numOfBookies, numOfBookies, digestType, "L".getBytes())) {
          ledgerIds.add(lh.getId());
          lh.addEntry("000".getBytes());
        }
      }

      try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
        BookKeeperAdmin.format(baseConf, false, true);
      }

      /**
       * ledgers created after format produce the same ids
       */
      for (int n = 0; n < numOfLedgers; n++) {
        try (LedgerHandle lh = bkc
            .createLedger(numOfBookies, numOfBookies, digestType, "L".getBytes())) {
          lh.addEntry("000".getBytes());
          assertTrue(ledgerIds.contains(lh.getId()));
        }
      }
    }
  }

  private void testBookieServiceInfo(boolean readonly, boolean legacy) throws Exception {
    File tmpDir = tmpDirs.createNew("bookie", "test");
    final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
        .setJournalDirName(tmpDir.getPath()).setLedgerDirNames(new String[]{tmpDir.getPath()})
        .setBookiePort(PortManager.nextFreePort()).setMetadataServiceUri(metadataServiceUri);

    LifecycleComponent server = Main.buildBookieServer(new BookieConfiguration(conf));
    // 2. start the server
    CompletableFuture<Void> stackComponentFuture = ComponentStarter.startComponent(server);
    while (server.lifecycleState() != Lifecycle.State.STARTED) {
      Thread.sleep(100);
    }

    ServerConfiguration bkConf = newServerConfiguration().setForceReadOnlyBookie(readonly);
    BookieServer bkServer = startBookie(bkConf).getServer();

    BookieId bookieId = bkServer.getBookieId();
    String host = bkServer.getLocalAddress().getHostName();
    int port = bkServer.getLocalAddress().getPort();

    if (legacy) {
      String regPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(bkConf) + "/" + AVAILABLE_NODE;
      regPath =
          readonly ? regPath + READONLY + "/" + bookieId : regPath + "/" + bookieId.toString();
      // deleting the metadata, so that the bookie registration should
      // continue successfully with legacy BookieServiceInfo
      zkc.setData(regPath, new byte[]{}, -1);
    }

    try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
      BookieServiceInfo bookieServiceInfo = bkAdmin.getBookieServiceInfo(bookieId);

      assertThat(bookieServiceInfo.getEndpoints().size(), is(1));
      BookieServiceInfo.Endpoint endpoint = bookieServiceInfo.getEndpoints().stream()
          .filter(e -> Objects.equals(e.getId(), bookieId.getId())).findFirst().get();
      assertNotNull(endpoint, "Endpoint " + bookieId + " not found.");

      assertThat(endpoint.getHost(), is(host));
      assertThat(endpoint.getPort(), is(port));
      assertThat(endpoint.getProtocol(), is("bookie-rpc"));
    }

    bkServer.shutdown();
    stackComponentFuture.cancel(true);
  }

  @Test
  void bookieServiceInfoWritable() throws Exception {
    testBookieServiceInfo(false, false);
  }

  @Test
  void bookieServiceInfoReadonly() throws Exception {
    testBookieServiceInfo(true, false);
  }

  @Test
  void legacyBookieServiceInfo() throws Exception {
    testBookieServiceInfo(false, true);
  }
}
