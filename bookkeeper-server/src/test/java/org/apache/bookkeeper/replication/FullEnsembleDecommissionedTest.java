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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests verifies the complete decommission tasks.
 */
public class FullEnsembleDecommissionedTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(FullEnsembleDecommissionedTest.class);
    private static final byte[] PASSWD = "admin".getBytes();
    private static final byte[] data = "TESTDATA".getBytes();
    private static final String openLedgerRereplicationGracePeriod = "3000"; // milliseconds

    private DigestType digestType;
    private MetadataClientDriver metadataClientDriver;
    private LedgerManagerFactory mFactory;
    private LedgerUnderreplicationManager underReplicationManager;
    private LedgerManager ledgerManager;
    private OrderedScheduler scheduler;

    public FullEnsembleDecommissionedTest() throws Exception{
        super(2);

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

    /**
     * The purpose of this test:
     * 1. Client service open a readonly ledger handle, which has been closed.
     * 2. All BKs that relates to the ledger have been decommissioned.
     * 3. Auto recovery component moved the data into other BK instances who is alive.
     * 4. Verify: lhe ledger handle in the client memory keeps updating the ledger ensemble set, and the new read
     *    request works.
     */
    @Test
    public void testOpenedLedgerHandleStillWorkAfterDecommissioning() throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, digestType, PASSWD);
        assertTrue(lh.getLedgerMetadata().getAllEnsembles().get(0L).size() == 2);
        lh.addEntry(data);
        lh.close();
        List<BookieId> originalEnsemble = lh.getLedgerMetadata().getAllEnsembles().get(0L);
        LedgerHandle readonlyLh = bkc.openLedger(lh.getId(), digestType, PASSWD, true);
        assertTrue(originalEnsemble.size() == 2);

        startNewBookie();
        BookieServer newBookieServer3 = serverByIndex(lastBookieIndex());
        killBookie(originalEnsemble.get(0));
        waitAutoRecoveryFinished(lh.getId(), originalEnsemble.get(0), newBookieServer3.getBookieId());

        startNewBookie();
        int newBookieIndex4 = lastBookieIndex();
        BookieServer newBookieServer4 = serverByIndex(newBookieIndex4);
        killBookie(originalEnsemble.get(1));
        waitAutoRecoveryFinished(lh.getId(), originalEnsemble.get(1), newBookieServer4.getBookieId());

        Awaitility.await().untilAsserted(() -> {
            LedgerEntries ledgerEntries = readonlyLh.read(0, 0);
            assertNotNull(ledgerEntries);
            byte[] entryBytes = ledgerEntries.getEntry(0L).getEntryBytes();
            assertEquals(new String(data), new String(entryBytes));
            ledgerEntries.close();
        });
        readonlyLh.close();
    }

    private void waitAutoRecoveryFinished(long lId, BookieId originalBookie,
                                          BookieId newBookie) throws Exception {
        Awaitility.await().untilAsserted(() -> {
            LedgerHandle openLedger = bkc.openLedger(lId, digestType, PASSWD);
            NavigableMap<Long, ? extends List<BookieId>> map = openLedger.getLedgerMetadata().getAllEnsembles();
            try {
                for (Map.Entry<Long, ? extends List<BookieId>> entry : map.entrySet()) {
                    assertFalse(entry.getValue().contains(originalBookie));
                    assertTrue(entry.getValue().contains(newBookie));
                }
            } finally {
                openLedger.close();
            }
        });
    }
}
