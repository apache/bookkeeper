/*
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests verifies the complete functionality of the
 * Auditor-rereplication process: Auditor will publish the bookie failures,
 * consequently ReplicationWorker will get the notifications and act on it.
 */
public class BookieAutoRecoveryTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieAutoRecoveryTest.class);
    private static final byte[] PASSWD = "admin".getBytes();
    private static final byte[] data = "TESTDATA".getBytes();
    private static final String openLedgerRereplicationGracePeriod = "3000"; // milliseconds

    private DigestType digestType;
    private MetadataClientDriver metadataClientDriver;
    private LedgerManagerFactory mFactory;
    private LedgerUnderreplicationManager underReplicationManager;
    private LedgerManager ledgerManager;
    private OrderedScheduler scheduler;

    private final String underreplicatedPath = "/ledgers/underreplication/ledgers";

    public BookieAutoRecoveryTest() throws IOException, KeeperException,
            InterruptedException, UnavailableException, CompatibilityException {
        super(3);

        baseConf.setLedgerManagerFactoryClassName(
                "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory");
        baseConf.setOpenLedgerRereplicationGracePeriod(openLedgerRereplicationGracePeriod);
        baseConf.setRwRereplicateBackoffMs(500);
        baseClientConf.setLedgerManagerFactoryClassName(
                "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory");
        this.digestType = DigestType.MAC;
        setAutoRecoveryEnabled(true);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        baseClientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-scheduler")
            .numThreads(1)
            .build();

        metadataClientDriver = MetadataDrivers.getClientDriver(
            URI.create(baseClientConf.getMetadataServiceUri()));
        metadataClientDriver.initialize(
            baseClientConf,
            scheduler,
            NullStatsLogger.INSTANCE,
            Optional.empty());

        // initialize urReplicationManager
        mFactory = metadataClientDriver.getLedgerManagerFactory();
        underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        ledgerManager = mFactory.newLedgerManager();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        if (null != underReplicationManager) {
            underReplicationManager.close();
            underReplicationManager = null;
        }
        if (null != ledgerManager) {
            ledgerManager.close();
            ledgerManager = null;
        }
        if (null != metadataClientDriver) {
            metadataClientDriver.close();
            metadataClientDriver = null;
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

//    @Test
//    public void testOpenLedgers1() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers1() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testOpenLedgers2() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers2() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testOpenLedgers3() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers3() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testOpenLedgers4() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers4() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testOpenLedgers5() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers5() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testOpenLedgers6() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers6() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testOpenLedgers7() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers7() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testOpenLedgers8() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers8() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testOpenLedgers9() throws Exception {
//        testOpenLedgers();
//    }
//
//    @Test
//    public void testClosedLedgers9() throws Exception {
//        testOpenLedgers();
//    }

    /**
     * Test verifies publish urLedger by Auditor and replication worker is
     * picking up the entries and finishing the rereplication of open ledger.
     */
    @Test
    public void testOpenLedgers() throws Exception {
        LOG.info("===> Testing open ledgers");
        List<LedgerHandle> listOfLedgerHandle = createLedgersAndAddEntries(1, 5);
        LedgerHandle lh = listOfLedgerHandle.get(0);
        int ledgerReplicaIndex = 0;
        BookieId replicaToKillAddr = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        final String urLedgerZNode = getUrLedgerZNode(lh);
        ledgerReplicaIndex = getReplicaIndexInLedger(lh, replicaToKillAddr);

        CountDownLatch latch = new CountDownLatch(1);
        LOG.info("===> 1 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        assertNull("UrLedger already exists!",
                watchUrLedgerNode(urLedgerZNode, latch));

        LOG.info("Killing Bookie :" + replicaToKillAddr);
        killBookie(replicaToKillAddr);

        // waiting to publish urLedger znode by Auditor
        LOG.info("===> 2 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        latch.await();
        latch = new CountDownLatch(1);
        LOG.info("===> 3 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        assertNotNull("UrLedger doesn't exists!",
                watchUrLedgerNode(urLedgerZNode, latch));

        // starting the replication service, so that he will be able to act as
        // target bookie
        startNewBookie();
        int newBookieIndex = lastBookieIndex();
        BookieServer newBookieServer = serverByIndex(newBookieIndex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting to finish the replication of failed bookie : "
                    + replicaToKillAddr);
        }
        LOG.info("===> 4 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        latch.await();
        LOG.info("===> 5 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        // grace period to update the urledger metadata in zookeeper
        LOG.info("Waiting to update the urledger metadata in zookeeper");

        verifyLedgerEnsembleMetadataAfterReplication(newBookieServer,
                listOfLedgerHandle.get(0), ledgerReplicaIndex);
        LOG.info("===> Finished test open ledgers");
    }

    /**
     * Test verifies publish urLedger by Auditor and replication worker is
     * picking up the entries and finishing the rereplication of closed ledgers.
     */
    @Test
    public void testClosedLedgers() throws Exception {
        LOG.info("===> Testing close ledgers");
        List<Integer> listOfReplicaIndex = new ArrayList<Integer>();
        List<LedgerHandle> listOfLedgerHandle = createLedgersAndAddEntries(1, 5);
        closeLedgers(listOfLedgerHandle);
        LedgerHandle lhandle = listOfLedgerHandle.get(0);
        int ledgerReplicaIndex = 0;
        BookieId replicaToKillAddr = lhandle.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        String urLedgerZNode = null;
        CountDownLatch latch = new CountDownLatch(listOfLedgerHandle.size());
        for (LedgerHandle lh : listOfLedgerHandle) {
            ledgerReplicaIndex = getReplicaIndexInLedger(lh, replicaToKillAddr);
            listOfReplicaIndex.add(ledgerReplicaIndex);
            urLedgerZNode = getUrLedgerZNode(lh);
            LOG.info("===> Watching on urLedgerPath:" + urLedgerZNode
                    + " to know the status of rereplication process");
            assertNull("UrLedger already exists!",
                    watchUrLedgerNode(urLedgerZNode, latch));
        }

        LOG.info("Killing Bookie :" + replicaToKillAddr);
        LOG.info("===> 2 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        killBookie(replicaToKillAddr);
        LOG.info("===> 3 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");

        // waiting to publish urLedger znode by Auditor
        latch.await();

        // Again watching the urLedger znode to know the replication status
        latch = new CountDownLatch(listOfLedgerHandle.size());
        for (LedgerHandle lh : listOfLedgerHandle) {
            urLedgerZNode = getUrLedgerZNode(lh);
            LOG.info("===> 4 Watching on urLedgerPath:" + urLedgerZNode
                    + " to know the status of rereplication process");
            assertNotNull("UrLedger doesn't exists!",
                    watchUrLedgerNode(urLedgerZNode, latch));
        }

        // starting the replication service, so that he will be able to act as
        // target bookie
        startNewBookie();
        int newBookieIndex = lastBookieIndex();
        BookieServer newBookieServer = serverByIndex(newBookieIndex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting to finish the replication of failed bookie : "
                    + replicaToKillAddr);
        }

        // waiting to finish replication
        LOG.info("===> 5 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        latch.await();
        LOG.info("===> 6 Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        // grace period to update the urledger metadata in zookeeper
        LOG.info("Waiting to update the urledger metadata in zookeeper");

        for (int index = 0; index < listOfLedgerHandle.size(); index++) {
            verifyLedgerEnsembleMetadataAfterReplication(newBookieServer,
                    listOfLedgerHandle.get(index),
                    listOfReplicaIndex.get(index));
        }
        LOG.info("===> Finished test close ledgers");
    }


    private int getReplicaIndexInLedger(LedgerHandle lh, BookieId replicaToKill) {
        SortedMap<Long, ? extends List<BookieId>> ensembles = lh.getLedgerMetadata().getAllEnsembles();
        int ledgerReplicaIndex = -1;
        for (BookieId addr : ensembles.get(0L)) {
            ++ledgerReplicaIndex;
            if (addr.equals(replicaToKill)) {
                break;
            }
        }
        return ledgerReplicaIndex;
    }

    private void verifyLedgerEnsembleMetadataAfterReplication(
            BookieServer newBookieServer, LedgerHandle lh,
            int ledgerReplicaIndex) throws Exception {
        LedgerHandle openLedger = bkc
                .openLedger(lh.getId(), digestType, PASSWD);

        BookieId inetSocketAddress = openLedger.getLedgerMetadata().getAllEnsembles().get(0L)
                .get(ledgerReplicaIndex);
        assertEquals("Rereplication has been failed and ledgerReplicaIndex :"
                + ledgerReplicaIndex, newBookieServer.getBookieId(),
                inetSocketAddress);
        openLedger.close();
    }

    private void closeLedgers(List<LedgerHandle> listOfLedgerHandle)
            throws InterruptedException, BKException {
        for (LedgerHandle lh : listOfLedgerHandle) {
            lh.close();
        }
    }

    private List<LedgerHandle> createLedgersAndAddEntries(int numberOfLedgers,
            int numberOfEntries) throws InterruptedException, BKException {
        List<LedgerHandle> listOfLedgerHandle = new ArrayList<LedgerHandle>(
                numberOfLedgers);
        for (int index = 0; index < numberOfLedgers; index++) {
            LedgerHandle lh = bkc.createLedger(3, 3, digestType, PASSWD);
            listOfLedgerHandle.add(lh);
            for (int i = 0; i < numberOfEntries; i++) {
                lh.addEntry(data);
            }
        }
        return listOfLedgerHandle;
    }

    private String getUrLedgerZNode(LedgerHandle lh) {
        return ZkLedgerUnderreplicationManager.getUrLedgerZnode(
                underreplicatedPath, lh.getId());
    }

    private Stat watchUrLedgerNode(final String znode,
            final CountDownLatch latch) throws KeeperException,
            InterruptedException {
        return zkc.exists(znode, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.NodeDeleted) {
                    LOG.info("Received Ledger replication completion. event : {}, path: {}, latchCount: {}",
                            event.getType(), event.getPath(), latch.getCount());
                    latch.countDown();
                }
                if (event.getType() == EventType.NodeCreated) {
                    LOG.info("Received urLedger publishing event: {}, path: {}, latchCount: {}",
                            event.getType(), event.getPath(), latch.getCount());
                    latch.countDown();
                }
            }
        });
    }
}
