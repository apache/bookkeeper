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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.test.MultiLedgerManagerTestCase;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests publishing of under replicated ledgers by the Auditor bookie node when
 * corresponding bookies identifes as not running
 */
public class AuditorLedgerCheckerTest extends MultiLedgerManagerTestCase {

    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // static Logger LOG = Logger.getRootLogger();
    private final static Logger LOG = LoggerFactory
            .getLogger(AuditorLedgerCheckerTest.class);

    private static final byte[] ledgerPassword = "aaa".getBytes();
    private Random rng; // Random Number Generator

    private DigestType digestType;

    private final String UNDERREPLICATED_PATH = baseClientConf
            .getZkLedgersRootPath()
            + "/underreplication/ledgers";
    private HashMap<String, AuditorElector> auditorElectors = new HashMap<String, AuditorElector>();
    private LedgerUnderreplicationManager urLedgerMgr;
    private Set<Long> urLedgerList;

    private List<Long> ledgerList;

    public AuditorLedgerCheckerTest(String ledgerManagerFactoryClass)
            throws IOException, KeeperException, InterruptedException,
            CompatibilityException {
        super(3);
        LOG.info("Running test case using ledger manager : "
                + ledgerManagerFactoryClass);
        this.digestType = DigestType.CRC32;
        // set ledger manager name
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactoryClass);
        baseClientConf
                .setLedgerManagerFactoryClassName(ledgerManagerFactoryClass);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        startAuditorElectors();
        rng = new Random(System.currentTimeMillis()); // Initialize the Random
        urLedgerList = new HashSet<Long>();
        ledgerList = new ArrayList<Long>(2);
    }

    @Override
    public void tearDown() throws Exception {
        stopAuditorElectors();
        super.tearDown();
    }

    private void startAuditorElectors() throws Exception {
        for (BookieServer bserver : bs) {
            String addr = StringUtils.addrToString(bserver.getLocalAddress());
            AuditorElector auditorElector = new AuditorElector(addr,
                    baseConf, zkc);
            auditorElectors.put(addr, auditorElector);
            auditorElector.start();
            LOG.debug("Starting Auditor Elector");
        }
    }

    private void stopAuditorElectors() throws Exception {
        for (AuditorElector auditorElector : auditorElectors.values()) {
            auditorElector.shutdown();
            LOG.debug("Stopping Auditor Elector!");
        }
    }

    /**
     * Test publishing of under replicated ledgers by the auditor bookie
     */
    @Test(timeout=60000)
    public void testSimpleLedger() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        LOG.debug("Created ledger : " + ledgerId);
        ledgerList.add(ledgerId);
        lh1.close();

        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        int bkShutdownIndex = bs.size() - 1;
        String shutdownBookie = shutdownBookie(bkShutdownIndex);

        // grace period for publishing the bk-ledger
        LOG.debug("Waiting for ledgers to be marked as under replicated");
        underReplicaLatch.await(5, TimeUnit.SECONDS);
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);
        assertEquals("Missed identifying under replicated ledgers", 1,
                urLedgerList.size());

        /*
         * Sample data format present in the under replicated ledger path
         * 
         * {4=replica: "10.18.89.153:5002"}
         */
        assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                urLedgerList.contains(ledgerId));
        String data = urLedgerData.get(ledgerId);
        assertTrue("Bookie " + shutdownBookie
                + "is not listed in the ledger as missing replica :" + data,
                data.contains(shutdownBookie));
    }

    /**
     * Test once published under replicated ledger should exists even after
     * restarting respective bookie
     */
    @Test(timeout=60000)
    public void testRestartBookie() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        ledgerList.add(lh1.getId());
        LedgerHandle lh2 = createAndAddEntriesToLedger();
        ledgerList.add(lh2.getId());
        LOG.debug("Created following ledgers : " + ledgerList);

        // 2 is added to the latch, since after the ledger reformation, again
        // the reformed bookie is stopped. So auditor will modify the zk
        // underreplicated metadata
        int count = ledgerList.size() + 2;
        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(count);

        int bkShutdownIndex = bs.size() - 1;
        ServerConfiguration bookieConf1 = bsConfs.get(bkShutdownIndex);
        String shutdownBookie = shutdownBookie(bkShutdownIndex);

        // restart the failed bookie and simulate previously listed ledgers are
        // rereplicated
        bs.add(startBookie(bookieConf1));

        // grace period for publishing the bk-ledger
        LOG.debug("Waiting for ledgers to be marked as under replicated");
        underReplicaLatch.await(5, TimeUnit.SECONDS);
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);

        assertEquals("Missed identifying under replicated ledgers", 2,
                urLedgerList.size());

        /*
         * Sample data format present in the under replicated ledger path
         * 
         * {4=replica: "10.18.89.153:5002", 5=replica: "10.18.89.153:5003"}
         */
        for (Long ledgerId : ledgerList) {
            assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                    urLedgerList.contains(ledgerId));
            String data = urLedgerData.get(ledgerId);
            assertTrue("Bookie " + shutdownBookie
                    + " is not listed in the ledger as missing " + data, data
                    .contains(shutdownBookie));
        }
    }

    /**
     * Test publishing of under replicated ledgers when multiple bookie failures
     * one after another.
     */
    @Test(timeout=60000)
    public void testMultipleBookieFailures() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        ledgerList.add(lh1.getId());
        LedgerHandle lh2 = createAndAddEntriesToLedger();
        ledgerList.add(lh2.getId());
        LOG.debug("Created following ledgers : " + ledgerList);

        // failing first bookie
        shutdownBookie(bs.size() - 1);
        // simulate re-replication
        doLedgerRereplication(lh1.getId(), lh2.getId());

        // failing another bookie
        CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());
        String shutdownBookie = shutdownBookie(bs.size() - 1);

        // grace period for publishing the bk-ledger
        LOG.debug("Waiting for ledgers to be marked as under replicated");
        underReplicaLatch.await(5, TimeUnit.SECONDS);
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);

        assertEquals("Missed identifying under replicated ledgers", 2,
                urLedgerList.size());

        /*
         * Sample data format present in the under replicated ledger path
         * {4=replica: "10.18.89.153:5002", 5=replica: "10.18.89.153:5003"}
         */
        for (Long ledgerId : ledgerList) {
            assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                    urLedgerList.contains(ledgerId));
            String data = urLedgerData.get(ledgerId);
            assertTrue("Bookie " + shutdownBookie
                    + " is not listed in the ledger as missing " + data, data
                    .contains(shutdownBookie));
        }
    }

    @Test(timeout = 30000)
    public void testToggleLedgerReplication() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        ledgerList.add(lh1.getId());
        LOG.debug("Created following ledgers : " + ledgerList);

        // failing another bookie
        CountDownLatch urReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        // disabling ledger replication
        urLedgerMgr.disableLedgerReplication();
        ArrayList<String> shutdownBookieList = new ArrayList<String>();
        shutdownBookieList.add(shutdownBookie(bs.size() - 1));
        shutdownBookieList.add(shutdownBookie(bs.size() - 1));

        assertFalse("Ledger replication is not disabled!", urReplicaLatch
                .await(5, TimeUnit.SECONDS));

        // enabling ledger replication
        urLedgerMgr.enableLedgerReplication();
        assertTrue("Ledger replication is not enabled!", urReplicaLatch.await(
                5, TimeUnit.SECONDS));
    }

    @Test(timeout = 20000)
    public void testDuplicateEnDisableAutoRecovery() throws Exception {
        urLedgerMgr.disableLedgerReplication();
        try {
            urLedgerMgr.disableLedgerReplication();
            fail("Must throw exception, since AutoRecovery is already disabled");
        } catch (UnavailableException e) {
            assertTrue("AutoRecovery is not disabled previously!",
                    e.getCause() instanceof KeeperException.NodeExistsException);
        }
        urLedgerMgr.enableLedgerReplication();
        try {
            urLedgerMgr.enableLedgerReplication();
            fail("Must throw exception, since AutoRecovery is already enabled");
        } catch (UnavailableException e) {
            assertTrue("AutoRecovery is not enabled previously!",
                    e.getCause() instanceof KeeperException.NoNodeException);
        }
    }

    /**
     * Test Auditor should consider Readonly bookie as available bookie. Should not publish ur ledgers for
     * readonly bookies.
     */
    @Test(timeout = 20000)
    public void testReadOnlyBookieExclusionFromURLedgersCheck() throws Exception {
        LedgerHandle lh = createAndAddEntriesToLedger();
        ledgerList.add(lh.getId());
        LOG.debug("Created following ledgers : " + ledgerList);

        int count = ledgerList.size();
        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(count);

        ServerConfiguration bookieConf = bsConfs.get(2);
        BookieServer bk = bs.get(2);
        bookieConf.setReadOnlyModeEnabled(true);
        bk.getBookie().transitionToReadOnlyMode();

        // grace period for publishing the bk-ledger
        LOG.debug("Waiting for Auditor to finish ledger check.");
        assertFalse("latch should not have completed", underReplicaLatch.await(5, TimeUnit.SECONDS));
    }

    private CountDownLatch registerUrLedgerWatcher(int count)
            throws KeeperException, InterruptedException {
        final CountDownLatch underReplicaLatch = new CountDownLatch(count);
        for (Long ledgerId : ledgerList) {
            Watcher urLedgerWatcher = new ChildWatcher(underReplicaLatch);
            String znode = ZkLedgerUnderreplicationManager.getUrLedgerZnode(UNDERREPLICATED_PATH,
                                                                            ledgerId);
            zkc.exists(znode, urLedgerWatcher);
        }
        return underReplicaLatch;
    }

    private void doLedgerRereplication(Long... ledgerIds)
            throws UnavailableException {
        for (int i = 0; i < ledgerIds.length; i++) {
            long lid = urLedgerMgr.getLedgerToRereplicate();
            assertTrue("Received unexpected ledgerid", Arrays.asList(ledgerIds).contains(lid));
            urLedgerMgr.markLedgerReplicated(lid);
            urLedgerMgr.releaseUnderreplicatedLedger(lid);
        }
    }

    private String shutdownBookie(int bkShutdownIndex) throws Exception {
        BookieServer bkServer = bs.get(bkShutdownIndex);
        String bookieAddr = StringUtils.addrToString(bkServer.getLocalAddress());
        LOG.debug("Shutting down bookie:" + bookieAddr);
        killBookie(bkShutdownIndex);
        auditorElectors.get(bookieAddr).shutdown();
        auditorElectors.remove(bookieAddr);
        return bookieAddr;
    }

    private LedgerHandle createAndAddEntriesToLedger() throws BKException,
            InterruptedException {
        int numEntriesToWrite = 100;
        // Create a ledger
        LedgerHandle lh = bkc.createLedger(digestType, ledgerPassword);
        LOG.info("Ledger ID: " + lh.getId());
        addEntry(numEntriesToWrite, lh);
        return lh;
    }

    private void addEntry(int numEntriesToWrite, LedgerHandle lh)
            throws InterruptedException, BKException {
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(Integer.MAX_VALUE));
            entry.position(0);
            lh.addEntry(entry.array());
        }
    }

    private Map<Long, String> getUrLedgerData(Set<Long> urLedgerList)
            throws KeeperException, InterruptedException {
        Map<Long, String> urLedgerData = new HashMap<Long, String>();
        for (Long ledgerId : urLedgerList) {
            String znode = ZkLedgerUnderreplicationManager.getUrLedgerZnode(UNDERREPLICATED_PATH,
                                                                            ledgerId);
            byte[] data = zkc.getData(znode, false, null);
            urLedgerData.put(ledgerId, new String(data));
        }
        return urLedgerData;
    }

    private class ChildWatcher implements Watcher {
        private final CountDownLatch underReplicaLatch;

        public ChildWatcher(CountDownLatch underReplicaLatch) {
            this.underReplicaLatch = underReplicaLatch;
        }

        @Override
        public void process(WatchedEvent event) {
            LOG.info("Received notification for the ledger path : "
                    + event.getPath());
            for (Long ledgerId : ledgerList) {
                if (event.getPath().contains(ledgerId + "")) {
                    urLedgerList.add(Long.valueOf(ledgerId));
                }
            }
            LOG.debug("Count down and waiting for next notification");
            // count down and waiting for next notification
            underReplicaLatch.countDown();
        }
    }
}
