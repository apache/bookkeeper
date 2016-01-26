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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import org.junit.Assert;

import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.channel.ChannelException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing bookie initialization cases
 */
public class BookieInitializationTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieInitializationTest.class);

    ZooKeeper newzk = null;

    public BookieInitializationTest() {
        super(0);
    }

    @Override
    public void tearDown() throws Exception {
        if (null != newzk) {
            newzk.close();
        }
        super.tearDown();
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
    @Test(timeout = 20000)
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
                bookie.zk = zkc;
                zkc.close();
                return bookie;
            };
        };

        bkServer.start();
        bkServer.join();
        Assert.assertEquals("Failed to return ExitCode.ZK_REG_FAIL",
                ExitCode.ZK_REG_FAIL, bkServer.getExitCode());
    }

    @Test(timeout = 20000)
    public void testBookieRegistrationWithSameZooKeeperClient() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                             zkc.exists(bkRegPath, false));

        // test register bookie again if the registeration node is created by itself.
        b.testRegisterBookie(conf);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                zkc.exists(bkRegPath, false));
    }

    /**
     * Verify the bookie reg. Restarting bookie server will wait for the session
     * timeout when previous reg node exists in zk. On zNode delete event,
     * should continue startup
     */
    @Test(timeout = 20000)
    public void testBookieRegistration() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        createNewZKClient();
        b.zk = newzk;

        // deleting the znode, so that the bookie registration should
        // continue successfully on NodeDeleted event
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(conf.getZkTimeout() / 3);
                    zkc.delete(bkRegPath, -1);
                } catch (Exception e) {
                    // Not handling, since the testRegisterBookie will fail
                    LOG.error("Failed to delete the znode :" + bkRegPath, e);
                }
            }
        }.start();
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
        Stat bkRegNode2 = newzk.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration has been failed", bkRegNode2);
        Assert.assertTrue("Bookie is referring to old registration znode:"
                + bkRegNode1 + ", New ZNode:" + bkRegNode2, bkRegNode1
                .getEphemeralOwner() != bkRegNode2.getEphemeralOwner());
    }

    /**
     * Verify the bookie registration, it should throw
     * KeeperException.NodeExistsException if the znode still exists even after
     * the zk session timeout.
     */
    @Test(timeout = 30000)
    public void testRegNodeExistsAfterSessionTimeOut() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration().setZkServers(null)
                .setJournalDirName(tmpDir.getPath()).setLedgerDirNames(
                        new String[] { tmpDir.getPath() });

        String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        createNewZKClient();
        b.zk = newzk;
        try {
            b.testRegisterBookie(conf);
            fail("Should throw NodeExistsException as the znode is not getting expired");
        } catch (IOException e) {
            Throwable t = e.getCause();
            if (t instanceof KeeperException) {
                KeeperException ke = (KeeperException) t;
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
        }
    }

    /**
     * Verify duplicate bookie server startup. Should throw
     * java.net.BindException if already BK server is running
     */
    @Test(timeout = 20000)
    public void testDuplicateBookieServerStartup() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(null).setBookiePort(port).setJournalDirName(
                tmpDir.getPath()).setLedgerDirNames(
                new String[] { tmpDir.getPath() });
        BookieServer bs1 = new BookieServer(conf);
        bs1.start();

        // starting bk server with same conf
        try {
            BookieServer bs2 = new BookieServer(conf);
            bs2.start();
            fail("Should throw BindException, as the bk server is already running!");
        } catch (ChannelException ce) {
            Assert.assertTrue("Should be caused by a bind exception",
                              ce.getCause() instanceof BindException);
            Assert.assertTrue("BKServer allowed duplicate startups!",
                    ce.getCause().getMessage().contains("Address already in use"));
        }
    }

    /**
     * Verify bookie start behaviour when ZK Server is not running.
     */
    @Test(timeout = 20000)
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
        } catch (KeeperException.ConnectionLossException e) {
            // expected behaviour
        } 
    }

    /**
     * Verify that if I try to start a bookie without zk initialized, it won't
     * prevent me from starting the bookie when zk is initialized
     */
    @Test(timeout = 20000)
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
     * Check disk full. Expected to throw NoWritableLedgerDirException
     * during bookie initialisation.
     */
    @Test(timeout = 30000)
    public void testWithDiskFull() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");
        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        conf.setDiskUsageThreshold((1f - ((float) usableSpace / (float) totalSpace)) - 0.05f);
        conf.setDiskUsageWarnThreshold((1f - ((float) usableSpace / (float) totalSpace)) - 0.25f);
        try {
            new Bookie(conf);
        } catch (Exception e) {
            // expected
        }
    }

    /**
     * Check disk error for file. Expected to throw DiskErrorException.
     */
    @Test(timeout = 30000)
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
            LedgerDirsManager ldm = new LedgerDirsManager(conf, conf.getLedgerDirs());
            ldm.init();
        } catch (Exception e) {
            // expected
        }
    }

    private void createNewZKClient() throws Exception {
        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        newzk = ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .build();
    }
}
