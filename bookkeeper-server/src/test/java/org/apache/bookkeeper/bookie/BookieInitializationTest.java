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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import org.apache.bookkeeper.bookie.BookieException.DiskPartitionDuplicationException;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
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
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing bookie initialization cases
 */
public class BookieInitializationTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieInitializationTest.class);

    @Rule
    public final TestName runtime = new TestName();
    RegistrationManager rm;

    public BookieInitializationTest() {
        super(0);
        String ledgersPath = "/" + "ledgers" + runtime.getMethodName();
        baseClientConf.setZkLedgersRootPath(ledgersPath);
        baseConf.setZkLedgersRootPath(ledgersPath);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LOG.info("setUp");
        zkUtil.createBKEnsemble("/" + runtime.getMethodName());
        rm = new ZKRegistrationManager();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if(rm != null) {
            rm.close();
        }
    }

    private static class MockBookie extends Bookie {
        MockBookie(ServerConfiguration conf) throws IOException,
                KeeperException, InterruptedException, BookieException {
            super(conf);
        }

        void testRegisterBookie(ServerConfiguration conf) throws IOException {
            super.doRegisterBookie();
        }
    }

    /**
     * Verify the bookie server exit code. On ZooKeeper exception, should return
     * exit code ZK_REG_FAIL = 4
     */
    @Test
    public void testExitCodeZK_REG_FAIL() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        // simulating ZooKeeper exception by assigning a closed zk client to bk
        BookieServer bkServer = new BookieServer(conf) {
            protected Bookie newBookie(ServerConfiguration conf)
                    throws IOException, KeeperException, InterruptedException,
                    BookieException {
                MockBookie bookie = new MockBookie(conf);
                rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
                bookie.registrationManager = rm;
                ((ZKRegistrationManager) bookie.registrationManager).setZk(zkc);
                ((ZKRegistrationManager) bookie.registrationManager).getZk().close();
                return bookie;
            }
        };

        bkServer.start();
        bkServer.join();
        Assert.assertEquals("Failed to return ExitCode.ZK_REG_FAIL",
                ExitCode.ZK_REG_FAIL, bkServer.getExitCode());
    }

    @Test
    public void testBookieRegistrationWithSameZooKeeperClient() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        conf.setZkServers(zkUtil.getZooKeeperConnectString());
        rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        b.registrationManager = rm;

        b.testRegisterBookie(conf);
        ZooKeeper zooKeeper = ((ZKRegistrationManager) rm).getZk();
        Assert.assertNotNull("Bookie registration node doesn't exists!",
            zooKeeper.exists(bkRegPath, false));

        // test register bookie again if the registeration node is created by itself.
        b.testRegisterBookie(conf);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
            zooKeeper.exists(bkRegPath, false));
    }

    /**
     * Verify the bookie reg. Restarting bookie server will wait for the session
     * timeout when previous reg node exists in zk. On zNode delete event,
     * should continue startup
     */
    @Test
    public void testBookieRegistration() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();
        MockBookie b = new MockBookie(conf);

        conf.setZkServers(zkUtil.getZooKeeperConnectString());
        rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        b.registrationManager = rm;

        b.testRegisterBookie(conf);

        Stat bkRegNode1 = ((ZKRegistrationManager) rm).getZk().exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        ZooKeeperClient newZk = createNewZKClient();
        RegistrationManager newRm = new ZKRegistrationManager();
        newRm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        b.registrationManager = newRm;

        try {
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
            try {
                b.testRegisterBookie(conf);
            } catch (IOException e) {
                Throwable t = e.getCause();
                if (t instanceof KeeperException) {
                    KeeperException ke = (KeeperException) t;
                    Assert.assertTrue("ErrorCode:" + ke.code()
                            + ", Registration node exists",
                        ke.code() != KeeperException.Code.NODEEXISTS);
                }
                throw e;
            }

            // verify ephemeral owner of the bkReg znode
            Stat bkRegNode2 = newZk.exists(bkRegPath, false);
            Assert.assertNotNull("Bookie registration has been failed", bkRegNode2);
            Assert.assertTrue("Bookie is referring to old registration znode:"
                + bkRegNode1 + ", New ZNode:" + bkRegNode2, bkRegNode1
                .getEphemeralOwner() != bkRegNode2.getEphemeralOwner());
        } finally {
            newZk.close();
        }
    }

    /**
     * Verify the bookie registration, it should throw
     * KeeperException.NodeExistsException if the znode still exists even after
     * the zk session timeout.
     */
    @Test
    public void testRegNodeExistsAfterSessionTimeOut() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration().setZkServers(null)
                .setJournalDirName(tmpDir.getPath()).setLedgerDirNames(
                        new String[] { tmpDir.getPath() });

        String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);

        conf.setZkServers(zkUtil.getZooKeeperConnectString());
        rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        b.registrationManager = rm;

        b.testRegisterBookie(conf);
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        ZooKeeperClient newzk = createNewZKClient();
        RegistrationManager newRm = new ZKRegistrationManager();
        newRm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        b.registrationManager = newRm;
        try {
            b.testRegisterBookie(conf);
            fail("Should throw NodeExistsException as the znode is not getting expired");
        } catch (IOException e) {
            Throwable t1 = e.getCause(); // BookieException.MetadataStoreException
            Throwable t2 = t1.getCause(); // IOException
            Throwable t3 = t2.getCause(); // KeeperException.NodeExistsException

            if (t3 instanceof KeeperException) {
                KeeperException ke = (KeeperException) t3;
                Assert.assertTrue("ErrorCode:" + ke.code()
                        + ", Registration node doesn't exists",
                        ke.code() == KeeperException.Code.NODEEXISTS);

                // verify ephemeral owner of the bkReg znode
                Stat bkRegNode2 = newzk.exists(bkRegPath, false);
                Assert.assertNotNull("Bookie registration has been failed",
                        bkRegNode2);
                Assert.assertTrue(
                        "Bookie wrongly registered. Old registration znode:"
                                + bkRegNode1 + ", New znode:" + bkRegNode2,
                        bkRegNode1.getEphemeralOwner() == bkRegNode2
                                .getEphemeralOwner());
                return;
            }
            throw e;
        } finally {
            newzk.close();
            newRm.close();
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
        conf.setZkServers(null).setBookiePort(port).setJournalDirName(
                tmpDir.getPath()).setLedgerDirNames(
                new String[] { tmpDir.getPath() });
        BookieServer bs1 = new BookieServer(conf);
        conf.setZkServers(zkUtil.getZooKeeperConnectString());
        rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        bs1.getBookie().setRegistrationManager(rm);
        bs1.start();

        // starting bk server with same conf
        try {
            BookieServer bs2 = new BookieServer(conf);
            RegistrationManager newRm = new ZKRegistrationManager();
            newRm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
            bs2.getBookie().registrationManager = newRm;
            bs2.start();
            fail("Should throw BindException, as the bk server is already running!");
        } catch (BindException e) {
            // Ok
        } catch (IOException e) {
            Assert.assertTrue("BKServer allowed duplicate Startups!",
                    e.getMessage().contains("bind"));
        }
    }

    /**
     * Verify bookie server starts up on ephemeral ports.
     */
    @Test
    public void testBookieServerStartupOnEphemeralPorts() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setZkServers(null)
            .setBookiePort(0)
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(
                new String[] { tmpDir.getPath() });
        assertEquals(0, conf.getBookiePort());

        ServerConfiguration conf1 = new ServerConfiguration();
        conf1.addConfiguration(conf);
        BookieServer bs1 = new BookieServer(conf1);
        conf.setZkServers(zkUtil.getZooKeeperConnectString());
        rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        bs1.getBookie().registrationManager = rm;
        bs1.start();
        assertFalse(0 == conf1.getBookiePort());

        // starting bk server with same conf
        ServerConfiguration conf2 = new ServerConfiguration();
        conf2.addConfiguration(conf);
        BookieServer bs2 = new BookieServer(conf2);
        RegistrationManager newRm = new ZKRegistrationManager();
        newRm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
        bs2.getBookie().registrationManager = newRm;
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
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        try {
            new Bookie(conf);
            fail("Should throw ConnectionLossException as ZKServer is not running!");
        } catch (BookieException.MetadataStoreException e) {
            // expected behaviour
        }
    }

    /**
     * Verify that if I try to start a bookie without zk initialized, it won't
     * prevent me from starting the bookie when zk is initialized
     */
    @Test
    public void testStartBookieWithoutZKInitialized() throws Exception {
        File tmpDir = createTempDir("bookie", "test");
        final String ZK_ROOT = "/ledgers2";

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() });
        conf.setZkLedgersRootPath(ZK_ROOT);
        try {
            new Bookie(conf);
            fail("Should throw NoNodeException");
        } catch (Exception e) {
            // shouldn't be able to start
        }
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        clientConf.setZkLedgersRootPath(ZK_ROOT);
        BookKeeperAdmin.format(clientConf, false, false);

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
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setDiskCheckInterval(1000)
                .setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.999f)
                .setDiskUsageWarnThreshold(0.0f);
        
        // if isForceGCAllowWhenNoSpace or readOnlyModeEnabled is not set and Bookie is 
        // started when Disk is full, then it will fail to start with NoWritableLedgerDirException
        
        conf.setIsForceGCAllowWhenNoSpace(false)
            .setReadOnlyModeEnabled(false);
        try {
            new Bookie(conf);
            fail("NoWritableLedgerDirException expected");
        } catch(NoWritableLedgerDirException e) {
            // expected
        }
        
        conf.setIsForceGCAllowWhenNoSpace(true)
            .setReadOnlyModeEnabled(false);
        try {
            new Bookie(conf);
            fail("NoWritableLedgerDirException expected");
        } catch(NoWritableLedgerDirException e) {
            // expected
        }
        
        conf.setIsForceGCAllowWhenNoSpace(false)
            .setReadOnlyModeEnabled(true);
        try {
            new Bookie(conf);
            fail("NoWritableLedgerDirException expected");
        } catch(NoWritableLedgerDirException e) {
            // expected
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
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setDiskCheckInterval(1000)
                .setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.999f)
                .setDiskUsageWarnThreshold(0.0f);
        
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

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString()).setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() }).setDiskCheckInterval(1000)
                .setLedgerStorageClass(SortedLedgerStorage.class.getName()).setAutoRecoveryDaemonEnabled(false);

        BookieServer server = new MockBookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkClient = new BookKeeper(clientConf);
        LedgerHandle lh = bkClient.createLedger(1, 1, 1, DigestType.CRC32, "passwd".getBytes());
        long entryId = -1;
        long numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            entryId = lh.addEntry("data".getBytes());
        }
        Assert.assertTrue("EntryId of the recently added entry should be 0", entryId == (numOfEntries - 1));
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
        Assert.assertTrue("Bookie should be up and running", server.getBookie().isRunning());
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
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(child.getPath())
                .setLedgerDirNames(new String[] { child.getPath() });
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
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setZkTimeout(5000).setBookiePort(port)
        .setJournalDirName(tmpDir1.getPath())
        .setLedgerDirNames(new String[] { tmpDir1.getPath(), tmpDir2.getPath() })
        .setIndexDirName(new String[] { tmpDir1.getPath()});;
        conf.setAllowMultipleDirsUnderSameDiskPartition(false);
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            Assert.fail("Bookkeeper should not have started since AllowMultipleDirsUnderSameDiskPartition is not enabled");
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
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setZkTimeout(5000).setBookiePort(port)
        .setJournalDirName(tmpDir1.getPath())
        .setLedgerDirNames(new String[] { tmpDir1.getPath() })
        .setIndexDirName(new String[] { tmpDir1.getPath(), tmpDir2.getPath() });
        conf.setAllowMultipleDirsUnderSameDiskPartition(false);
        bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            Assert.fail("Bookkeeper should not have started since AllowMultipleDirsUnderSameDiskPartition is not enabled");
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
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setZkTimeout(5000).setBookiePort(port)
        .setJournalDirsName(new String[] { tmpDir1.getPath(), tmpDir2.getPath() })
        .setLedgerDirNames(new String[] { tmpDir1.getPath() })
        .setIndexDirName(new String[] { tmpDir1.getPath()});
        conf.setAllowMultipleDirsUnderSameDiskPartition(false);
        bs1 = null;
        try {
            bs1 = new BookieServer(conf);
            Assert.fail(
                    "Bookkeeper should not have started since AllowMultipleDirsUnderSameDiskPartition is not enabled");
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
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setZkTimeout(5000).setBookiePort(port)
                .setJournalDirsName(new String[] { tmpDir1.getPath(), tmpDir2.getPath() })
                .setLedgerDirNames(new String[] { tmpDir3.getPath(), tmpDir4.getPath() })
                .setIndexDirName(new String[] { tmpDir5.getPath(), tmpDir6.getPath() });
        conf.setAllowMultipleDirsUnderSameDiskPartition(true);
        BookieServer bs1 = null;
        try {
            bs1 = new BookieServer(conf);          
        } catch (DiskPartitionDuplicationException dpde) {
            Assert.fail("Bookkeeper should have started since AllowMultipleDirsUnderSameDiskPartition is enabled");
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
}
