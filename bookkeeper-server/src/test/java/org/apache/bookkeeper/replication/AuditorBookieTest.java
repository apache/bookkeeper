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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies the auditor bookie scenarios which will be monitoring the
 * bookie failures
 */
public class AuditorBookieTest extends BookKeeperClusterTestCase {
    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // static Logger LOG = Logger.getRootLogger();
    private final static Logger LOG = LoggerFactory
            .getLogger(AuditorBookieTest.class);
    private String electionPath;
    private HashMap<String, AuditorElector> auditorElectors = new HashMap<String, AuditorElector>();
    private List<ZooKeeper> zkClients = new LinkedList<ZooKeeper>();

    public AuditorBookieTest() {
        super(6);
        electionPath = baseConf.getZkLedgersRootPath()
                + "/underreplication/auditorelection";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        startAuditorElectors();
    }

    @Override
    public void tearDown() throws Exception {
        stopAuditorElectors();
        for (ZooKeeper zk : zkClients) {
            zk.close();
        }
        zkClients.clear();
        super.tearDown();
    }

    /**
     * Test should ensure only one should act as Auditor. Starting/shutdown
     * other than auditor bookie shouldn't initiate re-election and multiple
     * auditors.
     */
    @Test(timeout=60000)
    public void testEnsureOnlySingleAuditor() throws Exception {
        BookieServer auditor = verifyAuditor();

        // shutdown bookie which is not an auditor
        int indexOf = bs.indexOf(auditor);
        int bkIndexDownBookie;
        if (indexOf < bs.size() - 1) {
            bkIndexDownBookie = indexOf + 1;
        } else {
            bkIndexDownBookie = indexOf - 1;
        }
        shutdownBookie(bs.get(bkIndexDownBookie));

        startNewBookie();
        startNewBookie();
        // grace period for the auditor re-election if any
        BookieServer newAuditor = waitForNewAuditor(auditor);
        Assert.assertSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
    }

    /**
     * Test Auditor crashes should trigger re-election and another bookie should
     * take over the auditor ship
     */
    @Test(timeout=60000)
    public void testSuccessiveAuditorCrashes() throws Exception {
        BookieServer auditor = verifyAuditor();
        shutdownBookie(auditor);

        BookieServer newAuditor1 = waitForNewAuditor(auditor);
        bs.remove(auditor);

        shutdownBookie(newAuditor1);
        BookieServer newAuditor2 = waitForNewAuditor(newAuditor1);
        Assert.assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor2);
        bs.remove(newAuditor1);
    }

    /**
     * Test restarting the entire bookie cluster. It shouldn't create multiple
     * bookie auditors
     */
    @Test(timeout=60000)
    public void testBookieClusterRestart() throws Exception {
        BookieServer auditor = verifyAuditor();
        for (AuditorElector auditorElector : auditorElectors.values()) {
            assertTrue("Auditor elector is not running!", auditorElector
                    .isRunning());
        }
        stopBKCluster();
        stopAuditorElectors();

        startBKCluster();
        startAuditorElectors();
        BookieServer newAuditor = waitForNewAuditor(auditor);
        Assert.assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
    }

    /**
     * Test the vote is deleting from the ZooKeeper during shutdown.
     */
    @Test(timeout=60000)
    public void testShutdown() throws Exception {
        BookieServer auditor = verifyAuditor();
        shutdownBookie(auditor);

        // waiting for new auditor
        BookieServer newAuditor = waitForNewAuditor(auditor);
        Assert.assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
        int indexOfDownBookie = bs.indexOf(auditor);
        bs.remove(indexOfDownBookie);
        bsConfs.remove(indexOfDownBookie);
        tmpDirs.remove(indexOfDownBookie);
        List<String> children = zkc.getChildren(electionPath, false);
        for (String child : children) {
            byte[] data = zkc.getData(electionPath + '/' + child, false, null);
            String bookieIP = new String(data);
            String addr = StringUtils.addrToString(auditor.getLocalAddress());
            Assert.assertFalse("AuditorElection cleanup fails", bookieIP
                    .contains(addr));
        }
    }

    /**
     * Test restart of the previous Auditor bookie shouldn't initiate
     * re-election and should create new vote after restarting.
     */
    @Test(timeout=60000)
    public void testRestartAuditorBookieAfterCrashing() throws Exception {
        BookieServer auditor = verifyAuditor();

        shutdownBookie(auditor);
        String addr = StringUtils.addrToString(auditor.getLocalAddress());

        // restarting Bookie with same configurations.
        int indexOfDownBookie = bs.indexOf(auditor);
        ServerConfiguration serverConfiguration = bsConfs
                .get(indexOfDownBookie);
        bs.remove(indexOfDownBookie);
        bsConfs.remove(indexOfDownBookie);
        tmpDirs.remove(indexOfDownBookie);
        auditorElectors.remove(addr);
        startBookie(serverConfiguration);
        // starting corresponding auditor elector

        LOG.debug("Performing Auditor Election:" + addr);
        startAuditorElector(addr);

        // waiting for new auditor to come
        BookieServer newAuditor = waitForNewAuditor(auditor);
        Assert.assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
        Assert.assertFalse("No relection after old auditor rejoins", auditor
                .getLocalAddress().getPort() == newAuditor.getLocalAddress()
                .getPort());
    }

    private void startAuditorElector(String addr) throws Exception {
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(10000);
        ZooKeeper zk = ZkUtils.createConnectedZookeeperClient(
                zkUtil.getZooKeeperConnectString(), w);
        zkClients.add(zk);

        AuditorElector auditorElector = new AuditorElector(addr,
                                                           baseConf, zk);
        auditorElectors.put(addr, auditorElector);
        auditorElector.start();
        LOG.debug("Starting Auditor Elector");
    }

    private void startAuditorElectors() throws Exception {
        for (BookieServer bserver : bs) {
            String addr = StringUtils.addrToString(bserver.getLocalAddress());
            startAuditorElector(addr);
        }
    }

    private void stopAuditorElectors() throws Exception {
        for (AuditorElector auditorElector : auditorElectors.values()) {
            auditorElector.shutdown();
            LOG.debug("Stopping Auditor Elector!");
        }
    }

    private BookieServer verifyAuditor() throws Exception {
        List<BookieServer> auditors = getAuditorBookie();
        Assert.assertEquals("Multiple Bookies acting as Auditor!", 1, auditors
                .size());
        LOG.debug("Bookie running as Auditor:" + auditors.get(0));
        return auditors.get(0);
    }

    private List<BookieServer> getAuditorBookie() throws Exception {
        List<BookieServer> auditors = new LinkedList<BookieServer>();
        byte[] data = zkc.getData(electionPath, false, null);
        Assert.assertNotNull("Auditor election failed", data);
        for (BookieServer bks : bs) {
            if (new String(data).contains(bks.getLocalAddress().getPort() + "")) {
                auditors.add(bks);
            }
        }
        return auditors;
    }

    private void shutdownBookie(BookieServer bkServer) throws Exception {
        String addr = StringUtils.addrToString(bkServer.getLocalAddress());
        LOG.debug("Shutting down bookie:" + addr);

        // shutdown bookie which is an auditor
        bkServer.shutdown();
        // stopping corresponding auditor elector
        auditorElectors.get(addr).shutdown();
    }

    private BookieServer waitForNewAuditor(BookieServer auditor)
            throws Exception {
        BookieServer newAuditor = null;
        int retryCount = 8;
        while (retryCount > 0) {
            List<BookieServer> auditors = getAuditorBookie();
            if (auditors.size() > 0) {
                newAuditor = auditors.get(0);
                if (auditor != newAuditor) {
                    break;
                }
            }
            Thread.sleep(500);
            retryCount--;
        }
        Assert.assertNotNull(
                "New Auditor is not reelected after auditor crashes",
                newAuditor);
        verifyAuditor();
        return newAuditor;
    }
}
