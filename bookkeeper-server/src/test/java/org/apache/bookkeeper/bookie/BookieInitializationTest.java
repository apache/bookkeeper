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
package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.BOOKIE_STATUS_FILENAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.BindException;
import java.net.InetAddress;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.bookie.BookieException.DiskPartitionDuplicationException;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing bookie initialization cases.
 */
public class BookieInitializationTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieInitializationTest.class);

    @Rule
    public final TestName runtime = new TestName();
    ZKMetadataBookieDriver driver;

    public BookieInitializationTest() {
        super(0);
    }

    @Override
    public void setUp() throws Exception {
        String ledgersPath = "/ledgers" + runtime.getMethodName();
        super.setUp(ledgersPath);
        zkUtil.createBKEnsemble(ledgersPath);
        driver = new ZKMetadataBookieDriver();
    }

    @Override
    public void tearDown() throws Exception {
        if (driver != null) {
            driver.close();
        }
        super.tearDown();
    }

    /**
     * Verify the bookie server exit code. On ZooKeeper exception, should return
     * exit code ZK_REG_FAIL = 4
     */
    @Test
    public void testExitCodeZK_REG_FAIL() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setMetadataServiceUri(metadataServiceUri);

        RegistrationManager rm = mock(RegistrationManager.class);
        doThrow(new MetadataStoreException("mocked exception"))
            .when(rm)
            .registerBookie(anyString(), anyBoolean());

        // simulating ZooKeeper exception by assigning a closed zk client to bk
        BookieServer bkServer = new BookieServer(conf) {
            protected Bookie newBookie(ServerConfiguration conf)
                    throws IOException, KeeperException, InterruptedException,
                    BookieException {
                Bookie bookie = new Bookie(conf);
                MetadataBookieDriver driver = Whitebox.getInternalState(bookie, "metadataDriver");
                ((ZKMetadataBookieDriver) driver).setRegManager(rm);
                return bookie;
            }
        };

        bkServer.start();
        bkServer.join();
        assertEquals("Failed to return ExitCode.ZK_REG_FAIL",
                ExitCode.ZK_REG_FAIL, bkServer.getExitCode());
    }

    @Test
    public void testBookieRegistrationWithSameZooKeeperClient() throws Exception {
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri)
            .setListeningInterface(null);

        String bookieId = Bookie.getBookieAddress(conf).toString();

        driver.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        try (StateManager manager = new BookieStateManager(conf, driver)) {
            manager.registerBookie(true).get();
            assertTrue(
                "Bookie registration node doesn't exists!",
                driver.getRegistrationManager().isBookieRegistered(bookieId));

            // test register bookie again if the registeration node is created by itself.
            manager.registerBookie(true).get();
            assertTrue(
                "Bookie registration node doesn't exists!",
                driver.getRegistrationManager().isBookieRegistered(bookieId));
        }
    }

    /**
     * Verify the bookie reg. Restarting bookie server will wait for the session
     * timeout when previous reg node exists in zk. On zNode delete event,
     * should continue startup
     */
    @Test
    public void testBookieRegistration() throws Exception {
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri)
            .setListeningInterface(null);

        String bookieId = Bookie.getBookieAddress(conf).toString();
        final String bkRegPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf)
            + "/" + AVAILABLE_NODE + "/" + bookieId;

        driver.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        try (StateManager manager = new BookieStateManager(conf, driver)) {
            manager.registerBookie(true).get();
        }
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        assertNotNull("Bookie registration has been failed", bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        try (MetadataBookieDriver newDriver = new ZKMetadataBookieDriver()) {
            newDriver.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);

            try (ZooKeeperClient newZk = createNewZKClient()) {
                // deleting the znode, so that the bookie registration should
                // continue successfully on NodeDeleted event
                new Thread(() -> {
                    try {
                        Thread.sleep(conf.getZkTimeout() / 3);
                        zkc.delete(bkRegPath, -1);
                    } catch (Exception e) {
                        // Not handling, since the testRegisterBookie will fail
                        LOG.error("Failed to delete the znode :" + bkRegPath, e);
                    }
                }).start();
                try (StateManager newMgr = new BookieStateManager(conf, newDriver)) {
                    newMgr.registerBookie(true).get();
                } catch (IOException e) {
                    Throwable t = e.getCause();
                    if (t instanceof KeeperException) {
                        KeeperException ke = (KeeperException) t;
                        assertTrue("ErrorCode:" + ke.code()
                                + ", Registration node exists",
                            ke.code() != KeeperException.Code.NODEEXISTS);
                    }
                    throw e;
                }

                // verify ephemeral owner of the bkReg znode
                Stat bkRegNode2 = newZk.exists(bkRegPath, false);
                assertNotNull("Bookie registration has been failed", bkRegNode2);
                assertTrue("Bookie is referring to old registration znode:"
                    + bkRegNode1 + ", New ZNode:" + bkRegNode2, bkRegNode1
                    .getEphemeralOwner() != bkRegNode2.getEphemeralOwner());
            }
        }
    }

    @Test(timeout = 20000)
    public void testBookieRegistrationWithFQDNHostNameAsBookieID() throws Exception {
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setMetadataServiceUri(metadataServiceUri)
            .setUseHostNameAsBookieID(true)
            .setListeningInterface(null);

        final String bookieId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + conf.getBookiePort();

        driver.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        try (StateManager manager = new BookieStateManager(conf, driver)) {
            manager.registerBookie(true).get();
            assertTrue("Bookie registration node doesn't exists!",
                driver.getRegistrationManager().isBookieRegistered(bookieId));
        }
    }

    @Test(timeout = 20000)
    public void testBookieRegistrationWithShortHostNameAsBookieID() throws Exception {
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setMetadataServiceUri(metadataServiceUri)
            .setUseHostNameAsBookieID(true)
            .setUseShortHostName(true)
            .setListeningInterface(null);

        final String bookieId = InetAddress.getLocalHost().getCanonicalHostName().split("\\.", 2)[0]
            + ":" + conf.getBookiePort();

        driver.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        try (StateManager manager = new BookieStateManager(conf, driver)) {
            manager.registerBookie(true).get();
            assertTrue("Bookie registration node doesn't exists!",
                driver.getRegistrationManager().isBookieRegistered(bookieId));
        }
    }

    /**
     * Verify the bookie registration, it should throw
     * KeeperException.NodeExistsException if the znode still exists even after
     * the zk session timeout.
     */
    @Test
    public void testRegNodeExistsAfterSessionTimeOut() throws Exception {
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setMetadataServiceUri(metadataServiceUri)
            .setListeningInterface(null);

        String bookieId = InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();
        String bkRegPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + "/" + AVAILABLE_NODE + "/" + bookieId;

        driver.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        try (StateManager manager = new BookieStateManager(conf, driver)) {
            manager.registerBookie(true).get();
            assertTrue("Bookie registration node doesn't exists!",
                driver.getRegistrationManager().isBookieRegistered(bookieId));
        }
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        assertNotNull("Bookie registration has been failed",
            bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        try (MetadataBookieDriver newDriver = new ZKMetadataBookieDriver()) {
            newDriver.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
            try (StateManager newMgr = new BookieStateManager(conf, newDriver)) {
                newMgr.registerBookie(true).get();
                fail("Should throw NodeExistsException as the znode is not getting expired");
            } catch (ExecutionException ee) {
                Throwable e = ee.getCause(); // IOException
                Throwable t1 = e.getCause(); // BookieException.MetadataStoreException
                Throwable t2 = t1.getCause(); // IOException
                Throwable t3 = t2.getCause(); // KeeperException.NodeExistsException

                if (t3 instanceof KeeperException) {
                    KeeperException ke = (KeeperException) t3;
                    assertTrue("ErrorCode:" + ke.code()
                            + ", Registration node doesn't exists",
                        ke.code() == KeeperException.Code.NODEEXISTS);

                    // verify ephemeral owner of the bkReg znode
                    Stat bkRegNode2 = zkc.exists(bkRegPath, false);
                    assertNotNull("Bookie registration has been failed",
                        bkRegNode2);
                    assertTrue(
                        "Bookie wrongly registered. Old registration znode:"
                            + bkRegNode1 + ", New znode:" + bkRegNode2,
                        bkRegNode1.getEphemeralOwner() == bkRegNode2
                            .getEphemeralOwner());
                    return;
                }
                throw ee;
            }
        }
    }

    /**
     * Verify user cannot start if user is in permittedStartupUsers conf list BKException BKUnauthorizedAccessException
     * if cannot start.
     */
    @Test
    public void testUserNotPermittedToStart() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = PortManager.nextFreePort();
        conf.setMetadataServiceUri(null)
            .setBookiePort(port)
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() });
        String userString = "larry, curly,moe,,";
        conf.setPermittedStartupUsers(userString);
        BookieServer bs1 = null;

        boolean sawException = false;
        try {
            bs1 = new BookieServer(conf);
            Assert.fail("Bookkeeper should not have started since current user isn't in permittedStartupUsers");
        } catch (AccessControlException buae) {
            sawException = true;
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }
        assertTrue("Should have thrown exception", sawException);
    }

    /**
     * Verify user cannot start if user is in permittedStartupUsers conf list BKException BKUnauthorizedAccessException
     * if cannot start.
     */
    @Test
    public void testUserPermittedToStart() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = PortManager.nextFreePort();
        conf.setMetadataServiceUri(null)
            .setBookiePort(port)
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() });

        BookieServer bs1 = null;

        // Multiple commas
        String userString = "larry,,,curly ," + System.getProperty("user.name") + " ,moe";
        conf.setPermittedStartupUsers(userString);
        try {
            bs1 = new BookieServer(conf);
            bs1.start();
        } catch (AccessControlException buae) {
            Assert.fail("Bookkeeper should have started since current user is in permittedStartupUsers");
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }

        // Comma at end
        userString = "larry ,curly, moe," + System.getProperty("user.name") + ",";
        conf.setPermittedStartupUsers(userString);
        try {
            bs1 = new BookieServer(conf);
            bs1.start();
        } catch (AccessControlException buae) {
            Assert.fail("Bookkeeper should have started since current user is in permittedStartupUsers");
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }
    }

    /**
     * Verify user can start if user is not in permittedStartupUsers but it is empty BKException
     * BKUnauthorizedAccessException if cannot start.
     */
    @Test
    public void testUserPermittedToStartWithMissingProperty() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        LOG.info("{}", conf);

        int port = PortManager.nextFreePort();
        conf.setMetadataServiceUri(null)
            .setBookiePort(port)
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() });
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            bs1.start();
        } catch (AccessControlException buae) {
            Assert.fail("Bookkeeper should have started since permittedStartupUser is not specified");
        } finally {
            if (bs1 != null && bs1.isRunning()) {
                bs1.shutdown();
            }
        }
    }

    /**
     * Verify duplicate bookie server startup. Should throw
     * java.net.BindException if already BK server is running
     */
    @Test
    public void testDuplicateBookieServerStartup() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = PortManager.nextFreePort();
        conf.setBookiePort(port)
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setMetadataServiceUri(metadataServiceUri);
        BookieServer bs1 = new BookieServer(conf);
        bs1.start();
        BookieServer bs2 = null;
        // starting bk server with same conf
        try {
            bs2 = new BookieServer(conf);
            bs2.start();
            fail("Should throw BindException, as the bk server is already running!");
        } catch (BindException e) {
            // Ok
        } catch (IOException e) {
            assertTrue("BKServer allowed duplicate Startups!",
                    e.getMessage().contains("bind"));
        } finally {
            bs1.shutdown();
            if (bs2 != null) {
                bs2.shutdown();
            }
        }
    }

    /**
     * Verify bookie server starts up on ephemeral ports.
     */
    @Test
    public void testBookieServerStartupOnEphemeralPorts() throws Exception {
        File tmpDir1 = createTempDir("bookie", "test1");
        File tmpDir2 = createTempDir("bookie", "test2");

        ServerConfiguration conf1 = TestBKConfiguration.newServerConfiguration();
        conf1.setBookiePort(0)
            .setJournalDirName(tmpDir1.getPath())
            .setLedgerDirNames(
                new String[] { tmpDir1.getPath() })
            .setMetadataServiceUri(null);
        assertEquals(0, conf1.getBookiePort());
        BookieServer bs1 = new BookieServer(conf1);
        bs1.start();
        assertFalse(0 == conf1.getBookiePort());

        // starting bk server with same conf
        ServerConfiguration conf2 = TestBKConfiguration.newServerConfiguration();
        conf2.setBookiePort(0)
            .setJournalDirName(tmpDir2.getPath())
            .setLedgerDirNames(
                new String[] { tmpDir2.getPath() })
            .setMetadataServiceUri(null);
        BookieServer bs2 = new BookieServer(conf2);
        bs2.start();
        assertFalse(0 == conf2.getBookiePort());

        // these two bookies are listening on different ports eventually
        assertFalse(conf1.getBookiePort() == conf2.getBookiePort());
    }

    /**
     * Verify bookie start behaviour when ZK Server is not running.
     */
    @Test
    public void testStartBookieWithoutZKServer() throws Exception {
        zkUtil.killServer();

        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri()).setZkTimeout(5000);

        try {
            new Bookie(conf);
            fail("Should throw ConnectionLossException as ZKServer is not running!");
        } catch (BookieException.MetadataStoreException e) {
            // expected behaviour
        }
    }

    /**
     * Verify that if I try to start a bookie without zk initialized, it won't
     * prevent me from starting the bookie when zk is initialized.
     */
    @Test
    public void testStartBookieWithoutZKInitialized() throws Exception {
        File tmpDir = createTempDir("bookie", "test");
        final String zkRoot = "/ledgers2";

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri(zkRoot))
            .setZkTimeout(5000);
        try {
            new Bookie(conf);
            fail("Should throw NoNodeException");
        } catch (Exception e) {
            // shouldn't be able to start
        }
        ServerConfiguration adminConf = new ServerConfiguration();
        adminConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri(zkRoot));
        BookKeeperAdmin.format(adminConf, false, false);

        Bookie b = new Bookie(conf);
        b.shutdown();
    }

    /**
     * Check disk full. Expected to fail on start.
     */
    @Test
    public void testWithDiskFullReadOnlyDisabledOrForceGCAllowDisabled() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");
        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setLedgerStorageClass(InterleavedLedgerStorage.class.getName())
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setDiskCheckInterval(1000)
                .setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.999f)
                .setDiskUsageWarnThreshold(0.0f)
                .setMetadataServiceUri(metadataServiceUri)
                .setZkTimeout(5000);

        // if isForceGCAllowWhenNoSpace or readOnlyModeEnabled is not set and Bookie is
        // started when Disk is full, then it will fail to start with NoWritableLedgerDirException

        conf.setMinUsableSizeForEntryLogCreation(Long.MAX_VALUE)
            .setReadOnlyModeEnabled(false);
        try {
            new Bookie(conf);
            fail("NoWritableLedgerDirException expected");
        } catch (NoWritableLedgerDirException e) {
            // expected
        }

        conf.setMinUsableSizeForEntryLogCreation(Long.MIN_VALUE)
            .setReadOnlyModeEnabled(false);
        try {
            new Bookie(conf);
            fail("NoWritableLedgerDirException expected");
        } catch (NoWritableLedgerDirException e) {
            // expected
        }

        conf.setMinUsableSizeForEntryLogCreation(Long.MAX_VALUE)
            .setReadOnlyModeEnabled(true);
        Bookie bookie = null;
        try {
            // bookie is okay to start up when readonly mode is enabled because entry log file creation
            // is deferred.
            bookie = new Bookie(conf);
        } catch (NoWritableLedgerDirException e) {
            fail("NoWritableLedgerDirException unexpected");
        } finally {
            if (null != bookie) {
                bookie.shutdown();
            }
        }
    }

    /**
     * Check disk full. Expected to start as read-only.
     */
    @Test
    public void testWithDiskFullReadOnlyEnabledAndForceGCAllowAllowed() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");
        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setDiskCheckInterval(1000)
                .setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.999f)
                .setDiskUsageWarnThreshold(0.0f)
                .setMetadataServiceUri(metadataServiceUri)
                .setZkTimeout(5000);

        // if isForceGCAllowWhenNoSpace and readOnlyModeEnabled are set, then Bookie should
        // start with readonlymode when Disk is full (assuming there is no need for creation of index file
        // while replaying the journal)
        conf.setReadOnlyModeEnabled(true)
            .setIsForceGCAllowWhenNoSpace(true);
        final Bookie bk = new Bookie(conf);
        bk.start();
        Thread.sleep((conf.getDiskCheckInterval() * 2) + 100);

        assertTrue(bk.isReadOnly());
        bk.shutdown();
    }

    class MockBookieServer extends BookieServer {
        ServerConfiguration conf;

        public MockBookieServer(ServerConfiguration conf) throws IOException, KeeperException, InterruptedException,
                BookieException, UnavailableException, CompatibilityException, SecurityException {
            super(conf);
            this.conf = conf;
        }

        @Override
        protected Bookie newBookie(ServerConfiguration conf)
                throws IOException, KeeperException, InterruptedException, BookieException {
            return new MockBookieWithNoopShutdown(conf, NullStatsLogger.INSTANCE);
        }
    }

    class MockBookieWithNoopShutdown extends Bookie {
        public MockBookieWithNoopShutdown(ServerConfiguration conf, StatsLogger statsLogger)
                throws IOException, KeeperException, InterruptedException, BookieException {
            super(conf, statsLogger);
        }

        // making Bookie Shutdown no-op. Ideally for this testcase we need to
        // kill bookie abruptly to simulate the scenario where bookie is killed
        // without execution of shutdownhook (and corresponding shutdown logic).
        // Since there is no easy way to simulate abrupt kill of Bookie we are
        // injecting noop Bookie Shutdown
        @Override
        synchronized int shutdown(int exitCode) {
            return exitCode;
        }
    }

    @Test
    public void testWithDiskFullAndAbilityToCreateNewIndexFile() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = newServerConfiguration()
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setDiskCheckInterval(1000)
            .setLedgerStorageClass(SortedLedgerStorage.class.getName())
            .setAutoRecoveryDaemonEnabled(false)
            .setZkTimeout(5000);

        BookieServer server = new MockBookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setMetadataServiceUri(metadataServiceUri);
        BookKeeper bkClient = new BookKeeper(clientConf);
        LedgerHandle lh = bkClient.createLedger(1, 1, 1, DigestType.CRC32, "passwd".getBytes());
        long entryId = -1;
        long numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            entryId = lh.addEntry("data".getBytes());
        }
        assertTrue("EntryId of the recently added entry should be 0", entryId == (numOfEntries - 1));
        // We want to simulate the scenario where Bookie is killed abruptly, so
        // SortedLedgerStorage's EntryMemTable and IndexInMemoryPageManager are
        // not flushed and hence when bookie is restarted it will replay the
        // journal. Since there is no easy way to kill the Bookie abruptly, we
        // are injecting no-op shutdown.
        server.shutdown();

        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        conf.setDiskUsageThreshold(0.001f)
                .setDiskUsageWarnThreshold(0.0f).setReadOnlyModeEnabled(true).setIsForceGCAllowWhenNoSpace(true)
                .setMinUsableSizeForIndexFileCreation(Long.MAX_VALUE);
        server = new BookieServer(conf);
        // Now we are trying to start the Bookie, which tries to replay the
        // Journal. While replaying the Journal it tries to create the IndexFile
        // for the ledger (whose entries are not flushed). but since we set
        // minUsableSizeForIndexFileCreation to very high value, it wouldn't. be
        // able to find any index dir when all discs are full
        server.start();
        assertFalse("Bookie should be Shutdown", server.getBookie().isRunning());
        server.shutdown();

        // Here we are setting MinUsableSizeForIndexFileCreation to very low
        // value. So if index dirs are full then it will consider the dirs which
        // have atleast MinUsableSizeForIndexFileCreation usable space for the
        // creation of new Index file.
        conf.setMinUsableSizeForIndexFileCreation(5 * 1024);
        server = new BookieServer(conf);
        server.start();
        Thread.sleep((conf.getDiskCheckInterval() * 2) + 100);
        assertTrue("Bookie should be up and running", server.getBookie().isRunning());
        assertTrue(server.getBookie().isReadOnly());
        server.shutdown();
        bkClient.close();
    }

    /**
     * Check disk error for file. Expected to throw DiskErrorException.
     */
    @Test
    public void testWithDiskError() throws Exception {
        File parent = createTempDir("DiskCheck", "test");
        File child = File.createTempFile("DiskCheck", "test", parent);
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(child.getPath())
                .setLedgerDirNames(new String[] { child.getPath() });
        conf.setMetadataServiceUri(metadataServiceUri)
            .setZkTimeout(5000);
        try {
            // LedgerDirsManager#init() is used in Bookie instantiation.
            // Simulating disk errors by directly calling #init
            LedgerDirsManager ldm = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                    new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
            LedgerDirsMonitor ledgerMonitor = new LedgerDirsMonitor(conf,
                    new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), ldm);
            ledgerMonitor.init();
            fail("should throw exception");
        } catch (Exception e) {
            // expected
        }
    }

    /**
     * if ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION is disabled then Bookie initialization
     * will fail if there are multiple ledger/index/journal dirs are in same partition/filesystem.
     */
    @Test
    public void testAllowDiskPartitionDuplicationDisabled() throws Exception {
        File tmpDir1 = createTempDir("bookie", "test");
        File tmpDir2 = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = PortManager.nextFreePort();
        // multiple ledgerdirs in same diskpartition
        conf.setMetadataServiceUri(metadataServiceUri)
            .setZkTimeout(5000)
            .setBookiePort(port)
            .setJournalDirName(tmpDir1.getPath())
            .setLedgerDirNames(new String[] { tmpDir1.getPath(), tmpDir2.getPath() })
            .setIndexDirName(new String[] { tmpDir1.getPath() })
            .setAllowMultipleDirsUnderSameDiskPartition(false);
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            fail("Bookkeeper should not have started since AllowMultipleDirsUnderSameDiskPartition is not enabled");
        } catch (DiskPartitionDuplicationException dpde) {
            // Expected
        } finally {
            if (bs1 != null) {
                bs1.shutdown();
            }
        }

        tmpDir1 = createTempDir("bookie", "test");
        tmpDir2 = createTempDir("bookie", "test");
        port = PortManager.nextFreePort();
        // multiple indexdirs in same diskpartition
        conf.setMetadataServiceUri(metadataServiceUri)
            .setZkTimeout(5000)
            .setBookiePort(port)
            .setJournalDirName(tmpDir1.getPath())
            .setLedgerDirNames(new String[] { tmpDir1.getPath() })
            .setIndexDirName(new String[] { tmpDir1.getPath(), tmpDir2.getPath() })
            .setAllowMultipleDirsUnderSameDiskPartition(false);
        bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            fail("Bookkeeper should not have started since AllowMultipleDirsUnderSameDiskPartition is not enabled");
        } catch (DiskPartitionDuplicationException dpde) {
            // Expected
        } finally {
            if (bs1 != null) {
                bs1.shutdown();
            }
        }

        tmpDir1 = createTempDir("bookie", "test");
        tmpDir2 = createTempDir("bookie", "test");
        port = PortManager.nextFreePort();
        // multiple journaldirs in same diskpartition
        conf.setMetadataServiceUri(metadataServiceUri)
            .setZkTimeout(5000)
            .setBookiePort(port)
            .setJournalDirsName(new String[] { tmpDir1.getPath(), tmpDir2.getPath() })
            .setLedgerDirNames(new String[] { tmpDir1.getPath() })
            .setIndexDirName(new String[] { tmpDir1.getPath()})
            .setAllowMultipleDirsUnderSameDiskPartition(false);
        bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            fail("Bookkeeper should not have started since AllowMultipleDirsUnderSameDiskPartition is not enabled");
        } catch (DiskPartitionDuplicationException dpde) {
            // Expected
        } finally {
            if (bs1 != null) {
                bs1.shutdown();
            }
        }
    }

    /**
     * if ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION is enabled then Bookie initialization
     * should succeed even if there are multiple ledger/index/journal dirs in the same diskpartition/filesystem.
     */
    @Test
    public void testAllowDiskPartitionDuplicationAllowed() throws Exception {
        File tmpDir1 = createTempDir("bookie", "test");
        File tmpDir2 = createTempDir("bookie", "test");
        File tmpDir3 = createTempDir("bookie", "test");
        File tmpDir4 = createTempDir("bookie", "test");
        File tmpDir5 = createTempDir("bookie", "test");
        File tmpDir6 = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setMetadataServiceUri(metadataServiceUri)
            .setZkTimeout(5000)
            .setBookiePort(port)
            .setJournalDirsName(new String[] { tmpDir1.getPath(), tmpDir2.getPath() })
            .setLedgerDirNames(new String[] { tmpDir3.getPath(), tmpDir4.getPath() })
            .setIndexDirName(new String[] { tmpDir5.getPath(), tmpDir6.getPath() });
        conf.setAllowMultipleDirsUnderSameDiskPartition(true);
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);
        } catch (DiskPartitionDuplicationException dpde) {
            fail("Bookkeeper should have started since AllowMultipleDirsUnderSameDiskPartition is enabled");
        } finally {
            if (bs1 != null) {
                bs1.shutdown();
            }
        }
    }

    private ZooKeeperClient createNewZKClient() throws Exception {
        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        return ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .build();
    }

    /**
     * Check bookie status should be able to persist on disk and retrieve when restart the bookie.
     */
    @Test(timeout = 10000)
    public void testPersistBookieStatus() throws Exception {
        // enable persistent bookie status
        File tmpDir = createTempDir("bookie", "test");
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setReadOnlyModeEnabled(true)
            .setPersistBookieStatusEnabled(true)
            .setMetadataServiceUri(metadataServiceUri);
        BookieServer bookieServer = new BookieServer(conf);
        bookieServer.start();
        Bookie bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        // transition to readonly mode, bookie status should be persisted in ledger disks
        bookie.getStateManager().doTransitionToReadOnlyMode();
        assertTrue(bookie.isReadOnly());

        // restart bookie should start in read only mode
        bookieServer.shutdown();
        bookieServer = new BookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertTrue(bookie.isReadOnly());
        // transition to writable mode
        bookie.getStateManager().doTransitionToWritableMode();
        // restart bookie should start in writable mode
        bookieServer.shutdown();
        bookieServer = new BookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        bookieServer.shutdown();
    }

    /**
     * Check when we start a ReadOnlyBookie, we should ignore bookie status.
     */
    @Test(timeout = 10000)
    public void testReadOnlyBookieShouldIgnoreBookieStatus() throws Exception {
        File tmpDir = createTempDir("bookie", "test");
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setReadOnlyModeEnabled(true)
            .setPersistBookieStatusEnabled(true)
            .setMetadataServiceUri(metadataServiceUri);
        // start new bookie
        BookieServer bookieServer = new BookieServer(conf);
        bookieServer.start();
        Bookie bookie = bookieServer.getBookie();
        // persist bookie status
        bookie.getStateManager().doTransitionToReadOnlyMode();
        bookie.getStateManager().doTransitionToWritableMode();
        assertFalse(bookie.isReadOnly());
        bookieServer.shutdown();
        // start read only bookie
        final ServerConfiguration readOnlyConf = TestBKConfiguration.newServerConfiguration();
        readOnlyConf.loadConf(conf);
        readOnlyConf.setForceReadOnlyBookie(true);
        bookieServer = new BookieServer(readOnlyConf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertTrue(bookie.isReadOnly());
        // transition to writable should fail
        bookie.getStateManager().doTransitionToWritableMode();
        assertTrue(bookie.isReadOnly());
        bookieServer.shutdown();
    }

    /**
     * Check that if there's multiple bookie status copies, as long as not all of them are corrupted,
     * the bookie status should be retrievable.
     */
    @Test(timeout = 10000)
    public void testRetrieveBookieStatusWhenStatusFileIsCorrupted() throws Exception {
        File[] tmpLedgerDirs = new File[3];
        String[] filePath = new String[tmpLedgerDirs.length];
        for (int i = 0; i < tmpLedgerDirs.length; i++) {
            tmpLedgerDirs[i] = createTempDir("bookie", "test" + i);
            filePath[i] = tmpLedgerDirs[i].getPath();
        }
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(filePath[0])
            .setLedgerDirNames(filePath)
            .setReadOnlyModeEnabled(true)
            .setPersistBookieStatusEnabled(true)
            .setMetadataServiceUri(metadataServiceUri);
        // start a new bookie
        BookieServer bookieServer = new BookieServer(conf);
        bookieServer.start();
        // transition in to read only and persist the status on disk
        Bookie bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        bookie.getStateManager().doTransitionToReadOnlyMode();
        assertTrue(bookie.isReadOnly());
        // corrupt status file
        List<File> ledgerDirs = bookie.getLedgerDirsManager().getAllLedgerDirs();
        corruptFile(new File(ledgerDirs.get(0), BOOKIE_STATUS_FILENAME));
        corruptFile(new File(ledgerDirs.get(1), BOOKIE_STATUS_FILENAME));
        // restart the bookie should be in read only mode
        bookieServer.shutdown();
        bookieServer = new BookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertTrue(bookie.isReadOnly());
        bookieServer.shutdown();
    }

    /**
     * Check if the bookie would read the latest status if the status files are not consistent.
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testReadLatestBookieStatus() throws Exception {
        File[] tmpLedgerDirs = new File[3];
        String[] filePath = new String[tmpLedgerDirs.length];
        for (int i = 0; i < tmpLedgerDirs.length; i++) {
            tmpLedgerDirs[i] = createTempDir("bookie", "test" + i);
            filePath[i] = tmpLedgerDirs[i].getPath();
        }
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(filePath[0])
            .setLedgerDirNames(filePath)
            .setReadOnlyModeEnabled(true)
            .setPersistBookieStatusEnabled(true)
            .setMetadataServiceUri(metadataServiceUri);
        // start a new bookie
        BookieServer bookieServer = new BookieServer(conf);
        bookieServer.start();
        // transition in to read only and persist the status on disk
        Bookie bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        bookie.getStateManager().doTransitionToReadOnlyMode();
        assertTrue(bookie.isReadOnly());
        // Manually update a status file, so it becomes the latest
        Thread.sleep(1);
        BookieStatus status = new BookieStatus();
        List<File> dirs = new ArrayList<File>();
        dirs.add(bookie.getLedgerDirsManager().getAllLedgerDirs().get(0));
        status.writeToDirectories(dirs);
        // restart the bookie should start in writable state
        bookieServer.shutdown();
        bookieServer = new BookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        bookieServer.shutdown();
    }

    private void corruptFile(File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
            byte[] bytes = new byte[64];
            new Random().nextBytes(bytes);
            bw.write(new String(bytes));
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

}
