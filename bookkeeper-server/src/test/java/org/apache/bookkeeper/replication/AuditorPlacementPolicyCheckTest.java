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

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.replication.AuditorPeriodicCheckTest.TestAuditor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestCounter;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the logic of Auditor's PlacementPolicyCheck.
 */
public class AuditorPlacementPolicyCheckTest extends BookKeeperClusterTestCase {
    private MetadataBookieDriver driver;

    public AuditorPlacementPolicyCheckTest() {
        super(1);
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        StaticDNSResolver.reset();
        driver = MetadataDrivers.getBookieDriver(URI.create(bsConfs.get(0).getMetadataServiceUri()));
        driver.initialize(bsConfs.get(0), () -> {
        }, NullStatsLogger.INSTANCE);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (null != driver) {
            driver.close();
        }
        super.tearDown();
    }

    @Test
    public void testPlacementPolicyCheckWithBookiesFromDifferentRacks() throws Exception {
        int numOfBookies = 5;
        List<BookieSocketAddress> bookieAddresses = new ArrayList<BookieSocketAddress>();
        BookieSocketAddress bookieAddress;
        RegistrationManager regManager = driver.getRegistrationManager();

        // all the numOfBookies (5) are going to be in different racks
        for (int i = 0; i < numOfBookies; i++) {
            bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181);
            StaticDNSResolver.addNodeToRack(bookieAddress.getHostName(), "/rack" + (i));
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress.toString(), false);
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 4;
        int ackQuorumSize = 2;
        int minNumRacksPerWriteQuorumConfValue = 4;
        Collections.shuffle(bookieAddresses);

        // closed ledger
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();

        Collections.shuffle(bookieAddresses);
        ensembleSize = 4;
        // closed ledger with multiple segments
        initMeta = LedgerMetadataBuilder.create()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 4))
                .newEnsembleEntry(20L, bookieAddresses.subList(1, 5))
                .newEnsembleEntry(60L, bookieAddresses.subList(0, 4))
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(2L, initMeta).get();

        Collections.shuffle(bookieAddresses);
        // non-closed ledger
        initMeta = LedgerMetadataBuilder.create()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 4))
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(3L, initMeta).get();

        Collections.shuffle(bookieAddresses);
        // non-closed ledger with multiple segments
        initMeta = LedgerMetadataBuilder.create()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 4))
                .newEnsembleEntry(20L, bookieAddresses.subList(1, 5))
                .newEnsembleEntry(60L, bookieAddresses.subList(0, 4))
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(4L, initMeta).get();

        ServerConfiguration servConf = new ServerConfiguration(bsConfs.get(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        setServerConfigProperties(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            TestCounter placementPolicyCheckEnsembleNotAdheringToPlacementPolicy = (TestCounter) statsLogger.getCounter(
                    ReplicationStats.PLACEMENT_POLICY_CHECK_ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER);
            /*
             * since all of the bookies are in different racks, there shouldn't be any ledger not adhering
             * to placement policy.
             */
            assertEquals("PLACEMENT_POLICY_CHECK_ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER SuccessCount", 0L,
                    placementPolicyCheckEnsembleNotAdheringToPlacementPolicy.get().longValue());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
        }
    }

    @Test
    public void testPlacementPolicyCheckWithLedgersNotAdheringToPlacementPolicy() throws Exception {
        int numOfBookies = 5;
        int numOfLedgersNotAdheringToPlacementPolicy = 0;
        List<BookieSocketAddress> bookieAddresses = new ArrayList<BookieSocketAddress>();
        RegistrationManager regManager = driver.getRegistrationManager();

        for (int i = 0; i < numOfBookies; i++) {
            BookieSocketAddress bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181);
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress.toString(), false);
        }

        // only three racks
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(0).getHostName(), "/rack1");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(1).getHostName(), "/rack2");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(2).getHostName(), "/rack3");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(3).getHostName(), "/rack1");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(4).getHostName(), "/rack2");

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;
        int minNumRacksPerWriteQuorumConfValue = 3;

        /*
         * this closed ledger doesn't adhere to placement policy because there are only
         * 3 racks, and the ensembleSize is 5.
         */
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();
        numOfLedgersNotAdheringToPlacementPolicy++;

        /*
         * this is non-closed ledger, so it shouldn't count as ledger not
         * adhering to placement policy
         */
        initMeta = LedgerMetadataBuilder.create()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(2L, initMeta).get();

        ServerConfiguration servConf = new ServerConfiguration(bsConfs.get(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        setServerConfigProperties(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            TestCounter placementPolicyCheckEnsembleNotAdheringToPlacementPolicy = (TestCounter) statsLogger.getCounter(
                    ReplicationStats.PLACEMENT_POLICY_CHECK_ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER);
            assertEquals("PLACEMENT_POLICY_CHECK_ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER SuccessCount",
                    (long) numOfLedgersNotAdheringToPlacementPolicy,
                    placementPolicyCheckEnsembleNotAdheringToPlacementPolicy.get().longValue());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
        }
    }

    @Test
    public void testPlacementPolicyCheckWithLedgersNotAdheringToPolicyWithMultipleSegments() throws Exception {
        int numOfBookies = 7;
        int numOfLedgersNotAdheringToPlacementPolicy = 0;
        List<BookieSocketAddress> bookieAddresses = new ArrayList<BookieSocketAddress>();
        RegistrationManager regManager = driver.getRegistrationManager();

        for (int i = 0; i < numOfBookies; i++) {
            BookieSocketAddress bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181);
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress.toString(), false);
        }

        // only three racks
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(0).getHostName(), "/rack1");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(1).getHostName(), "/rack2");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(2).getHostName(), "/rack3");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(3).getHostName(), "/rack4");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(4).getHostName(), "/rack1");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(5).getHostName(), "/rack2");
        StaticDNSResolver.addNodeToRack(bookieAddresses.get(6).getHostName(), "/rack3");

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 5;
        int ackQuorumSize = 2;
        int minNumRacksPerWriteQuorumConfValue = 4;

        /*
         * this closed ledger in each writeQuorumSize (5), there would be
         * atleast minNumRacksPerWriteQuorumConfValue (4) racks. So it wont be
         * counted as ledgers not adhering to placement policy.
         */
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 5))
                .newEnsembleEntry(20L, bookieAddresses.subList(1, 6))
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();

        /*
         * for the second segment bookies are from /rack1, /rack2 and /rack3,
         * which is < minNumRacksPerWriteQuorumConfValue (4). So it is not
         * adhering to placement policy.
         *
         * also for the third segment are from /rack1, /rack2 and /rack3, which
         * is < minNumRacksPerWriteQuorumConfValue (4). So it is not adhering to
         * placement policy.
         *
         * Though there are multiple segments are not adhering to placement
         * policy, it should be counted as single ledger.
         */
        initMeta = LedgerMetadataBuilder.create()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 5))
                .newEnsembleEntry(20L,
                        Arrays.asList(bookieAddresses.get(0), bookieAddresses.get(1), bookieAddresses.get(2),
                                bookieAddresses.get(4), bookieAddresses.get(5)))
                .newEnsembleEntry(40L,
                        Arrays.asList(bookieAddresses.get(0), bookieAddresses.get(1), bookieAddresses.get(2),
                                bookieAddresses.get(4), bookieAddresses.get(6)))
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(2L, initMeta).get();
        numOfLedgersNotAdheringToPlacementPolicy++;

        ServerConfiguration servConf = new ServerConfiguration(bsConfs.get(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        setServerConfigProperties(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            TestCounter placementPolicyCheckEnsembleNotAdheringToPlacementPolicy = (TestCounter) statsLogger.getCounter(
                    ReplicationStats.PLACEMENT_POLICY_CHECK_ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER);
            assertEquals("PLACEMENT_POLICY_CHECK_ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER SuccessCount",
                    (long) numOfLedgersNotAdheringToPlacementPolicy,
                    placementPolicyCheckEnsembleNotAdheringToPlacementPolicy.get().longValue());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
        }
    }

    private void setServerConfigProperties(ServerConfiguration servConf) {
        servConf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        servConf.setProperty(ClientConfiguration.ENSEMBLE_PLACEMENT_POLICY,
                RackawareEnsemblePlacementPolicy.class.getName());
        servConf.setAuditorPeriodicCheckInterval(0);
        servConf.setAuditorPeriodicBookieCheckInterval(0);
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(1000);
    }

    private TestStatsLogger startAuditorAndWaitForPlacementPolicyCheck(ServerConfiguration servConf,
            MutableObject<Auditor> auditorRef) throws MetadataException, CompatibilityException, KeeperException,
            InterruptedException, UnavailableException, UnknownHostException {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        TestOpStatsLogger placementPolicyCheckStatsLogger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.PLACEMENT_POLICY_CHECK_TIME);

        final TestAuditor auditor = new TestAuditor(Bookie.getBookieAddress(servConf).toString(), servConf,
                statsLogger);
        auditorRef.setValue(auditor);
        CountDownLatch latch = auditor.getLatch();
        assertEquals("PLACEMENT_POLICY_CHECK_TIME SuccessCount", 0, placementPolicyCheckStatsLogger.getSuccessCount());
        urm.setPlacementPolicyCheckCTime(-1);
        auditor.start();
        /*
         * since placementPolicyCheckCTime is set to -1, placementPolicyCheck should be
         * scheduled to run with no initialdelay
         */
        assertTrue("placementPolicyCheck should have executed", latch.await(20, TimeUnit.SECONDS));
        for (int i = 0; i < 20; i++) {
            Thread.sleep(100);
            if (placementPolicyCheckStatsLogger.getSuccessCount() >= 1) {
                break;
            }
        }
        assertEquals("PLACEMENT_POLICY_CHECK_TIME SuccessCount", 1, placementPolicyCheckStatsLogger.getSuccessCount());
        return statsLogger;
    }
}
