/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RoundRobinDistributionSchedule.writeSetFromValues;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;
import static org.junit.Assert.assertNotEquals;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import junit.framework.TestCase;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementResult;
import org.apache.bookkeeper.client.ZoneawareEnsemblePlacementPolicyImpl.ZoneAwareNodeLocation;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the zoneaware ensemble placement policy.
 */
public class TestZoneawareEnsemblePlacementPolicy extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestZoneawareEnsemblePlacementPolicy.class);

    ZoneawareEnsemblePlacementPolicy zepp;
    final List<BookieId> ensemble = new ArrayList<BookieId>();
    DistributionSchedule.WriteSet writeSet = DistributionSchedule.NULL_WRITE_SET;
    ClientConfiguration conf = new ClientConfiguration();
    BookieSocketAddress addr1;
    BookieSocketAddress addr2, addr3, addr4;
    io.netty.util.HashedWheelTimer timer;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        StaticDNSResolver.reset();
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(),
                NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack("127.0.0.1", NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack("localhost", NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        LOG.info("Set up static DNS Resolver.");
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_ZONE + "/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), NetworkTopology.DEFAULT_ZONE + "/ud2");
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr3.toBookieId());
        ensemble.add(addr4.toBookieId());
        writeSet = writeSetFromValues(0, 1, 2, 3);

        timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS, conf.getTimeoutTimerNumTicks());

        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(conf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
    }

    @Override
    protected void tearDown() throws Exception {
        zepp.uninitalize();
        super.tearDown();
    }

    static BookiesHealthInfo getBookiesHealthInfo() {
        return getBookiesHealthInfo(new HashMap<>(), new HashMap<>());
    }

    static BookiesHealthInfo getBookiesHealthInfo(Map<BookieId, Long> bookieFailureHistory,
            Map<BookieId, Long> bookiePendingRequests) {
        return new BookiesHealthInfo() {
            @Override
            public long getBookieFailureHistory(BookieId bookieSocketAddress) {
                return bookieFailureHistory.getOrDefault(bookieSocketAddress, -1L);
            }

            @Override
            public long getBookiePendingRequests(BookieId bookieSocketAddress) {
                return bookiePendingRequests.getOrDefault(bookieSocketAddress, 0L);
            }
        };
    }

    static void updateMyUpgradeDomain(String zoneAndUD) throws Exception {
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), zoneAndUD);
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostName(), zoneAndUD);
        StaticDNSResolver.addNodeToRack("127.0.0.1", zoneAndUD);
        StaticDNSResolver.addNodeToRack("localhost", zoneAndUD);
    }

    @Test
    public void testNotEnoughRWBookies() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone4/ud1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone5/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone6/ud1");

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(1);
        newConf.setMinNumZonesPerWriteQuorum(1);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        try {
            // only 3 rw bookies are available
            zepp.newEnsemble(6, 3, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because enough writable nodes are not available");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // expected to get BKNotEnoughBookiesException
        }

        roAddrs.add(addr4.toBookieId());
        roAddrs.add(addr5.toBookieId());
        roAddrs.add(addr6.toBookieId());
        zepp.onClusterChanged(rwAddrs, roAddrs);
        try {
            // only 3 rw bookies are available
            zepp.newEnsemble(6, 3, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because enough writable nodes are not available");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // expected to get BKNotEnoughBookiesException
        }

        rwAddrs.clear();
        roAddrs.add(addr1.toBookieId());
        roAddrs.add(addr2.toBookieId());
        roAddrs.add(addr3.toBookieId());
        zepp.onClusterChanged(rwAddrs, roAddrs);
        try {
            // no rw bookie is available
            zepp.newEnsemble(6, 3, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because enough writable nodes are not available");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // expected to get BKNotEnoughBookiesException
        }
    }

    @Test
    public void testEnoughRWBookies() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone4/ud1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone5/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone6/ud1");

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(2);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        /*
         * there are enough bookies so newEnsemble should succeed.
         */
        PlacementResult<List<BookieId>> newEnsemblePlacementResult = zepp.newEnsemble(6, 3, 2, null,
                new HashSet<>());
        Set<BookieId> newEnsembleSet = new HashSet<BookieId>(
                newEnsemblePlacementResult.getResult());
        assertTrue("New ensemble should contain all 6 rw bookies", newEnsembleSet.containsAll(rwAddrs));
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                newEnsemblePlacementResult.getAdheringToPolicy());

        /*
         * there are enough bookies so newEnsemble should succeed.
         */
        newEnsemblePlacementResult = zepp.newEnsemble(3, 3, 2, null, new HashSet<>());
        newEnsembleSet = new HashSet<BookieId>(newEnsemblePlacementResult.getResult());
        assertTrue("New ensemble should contain 3 rw bookies",
                (newEnsembleSet.size() == 3) && (rwAddrs.containsAll(newEnsembleSet)));
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                newEnsemblePlacementResult.getAdheringToPolicy());
    }

    @Test
    public void testWithDefaultBookies() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone4/ud1");

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        Set<BookieId> bookiesInDefaultFaultDomain = new HashSet<BookieId>();
        bookiesInDefaultFaultDomain.add(addr5.toBookieId());
        bookiesInDefaultFaultDomain.add(addr6.toBookieId());
        bookiesInDefaultFaultDomain.add(addr7.toBookieId());

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());
        rwAddrs.add(addr7.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        for (int i = 0; i < 3; i++) {
            /*
             * make sure bookies from DEFAULT_ZONE_AND_UPGRADEDOMAIN are not
             * part of the new ensemble created.
             */
            PlacementResult<List<BookieId>> newEnsemblePlacementResult = zepp.newEnsemble(4, 4, 2, null,
                    new HashSet<>());
            Set<BookieId> newEnsembleSet = new HashSet<BookieId>(
                    newEnsemblePlacementResult.getResult());
            assertTrue("Bookie from default faultDomain shouldn't be part of ensemble",
                    Collections.disjoint(newEnsembleSet, bookiesInDefaultFaultDomain));

            newEnsemblePlacementResult = zepp.newEnsemble(3, 3, 2, null, new HashSet<>());
            newEnsembleSet = new HashSet<BookieId>(newEnsemblePlacementResult.getResult());
            assertTrue("Bookie from default faultDomain shouldn't be part of ensemble",
                    Collections.disjoint(newEnsembleSet, bookiesInDefaultFaultDomain));
            assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                    newEnsemblePlacementResult.getAdheringToPolicy());
        }
    }

    @Test
    public void testMinZonesPerWriteQuorum() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud3");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(3);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        Set<BookieId> bookiesInDefaultFaultDomain = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());
        rwAddrs.add(addr9.toBookieId());
        rwAddrs.add(addr10.toBookieId());
        roAddrs.add(addr7.toBookieId());
        roAddrs.add(addr8.toBookieId());
        bookiesInDefaultFaultDomain.add(addr9.toBookieId());
        bookiesInDefaultFaultDomain.add(addr10.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        PlacementResult<List<BookieId>> newEnsemblePlacementResult;

        newEnsemblePlacementResult = zepp.newEnsemble(4, 4, 2, null, new HashSet<>());
        Set<BookieId> newEnsembleSet = new HashSet<BookieId>(
                newEnsemblePlacementResult.getResult());
        assertTrue("New ensemble should contain all 6 rw bookies in non-default fault domains",
                rwAddrs.containsAll(newEnsembleSet) && (newEnsembleSet.size() == 4));
        assertTrue("Bookie from default faultDomain shouldn't be part of ensemble",
                Collections.disjoint(newEnsembleSet, bookiesInDefaultFaultDomain));
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_SOFT,
                newEnsemblePlacementResult.getAdheringToPolicy());

        try {
            /*
             * If ensembleSize is not multiple of writeQuorumSize, then it is
             * expected to fail with IllegalArgumentException.
             */
            zepp.newEnsemble(4, 3, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail with IllegalArgumentException");
        } catch (IllegalArgumentException illExc) {
            // expected IllegalArgumentException
        }
        zepp.uninitalize();
        newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(3);
        newConf.setEnforceStrictZoneawarePlacement(false);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        zepp.onClusterChanged(rwAddrs, roAddrs);

        /*
         * If enforceStrictZoneawarePlacement is not enabled, then there are no
         * limitations on eligible values of ensembleSize and writeQuorumSize.
         */
        newEnsemblePlacementResult = zepp.newEnsemble(4, 3, 2, null, new HashSet<>());
        newEnsembleSet = new HashSet<BookieId>(newEnsemblePlacementResult.getResult());
        assertTrue("New ensemble should contain 4 different bookies", newEnsembleSet.size() == 4);
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                newEnsemblePlacementResult.getAdheringToPolicy());
    }

    @Test
    public void testMinUDsNotAvailable() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud3");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(2);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        Set<BookieId> bookiesInDefaultFaultDomain = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());
        rwAddrs.add(addr9.toBookieId());
        rwAddrs.add(addr10.toBookieId());

        roAddrs.add(addr7.toBookieId());
        roAddrs.add(addr8.toBookieId());

        bookiesInDefaultFaultDomain.add(addr9.toBookieId());
        bookiesInDefaultFaultDomain.add(addr10.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        PlacementResult<List<BookieId>> newEnsemblePlacementResult;
        try {
            /*
             * since rw bookies are not spread across UDs in zones, newEnsemble
             * of writeQuorum 6 is expected to fail.
             */
            zepp.newEnsemble(6, 6, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because writeQuorum cannt be created with insufficient UDs");
        } catch (BKException.BKNotEnoughBookiesException bkne) {
            // expected NotEnoughBookiesException
        }

        int ensSize = 6;
        int writeQuorum = 3;
        /*
         * though bookies are not spread across UDs in zones, newEnsemble would
         * succeed because writeQuorum is just 3.
         */
        newEnsemblePlacementResult = zepp.newEnsemble(ensSize, writeQuorum, 2, null, new HashSet<>());
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                newEnsemblePlacementResult.getAdheringToPolicy());
        List<BookieId> newEnsemble = newEnsemblePlacementResult.getResult();
        Set<BookieId> newEnsembleSet = new HashSet<BookieId>(newEnsemble);
        assertTrue("New ensemble should contain all 6 rw bookies in non-default fault domains",
                rwAddrs.containsAll(newEnsembleSet) && (newEnsembleSet.size() == 6));
        assertTrue("Bookie from default faultDomain shouldn't be part of ensemble",
                Collections.disjoint(newEnsembleSet, bookiesInDefaultFaultDomain));

        Set<String> zonesOfBookiesInAWriteQuorum = new HashSet<String>();
        for (int i = 0; i < 6; i++) {
            zonesOfBookiesInAWriteQuorum.clear();
            for (int j = 0; j < writeQuorum; j++) {
                zonesOfBookiesInAWriteQuorum
                        .add(zepp.getZoneAwareNodeLocation(newEnsemble.get((i + j) % ensSize)).getZone());
            }
            assertEquals("Since bookies are not spread across multiple UDs in a zone, write quorum should"
                    + " contain bookies from all 3 zones", 3, zonesOfBookiesInAWriteQuorum.size());
        }
    }

    @Test
    public void testUniqueUds() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.0.0.12", 3181);
        BookieSocketAddress addr12 = new BookieSocketAddress("127.0.0.13", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), "/zone2/ud3");
        StaticDNSResolver.addNodeToRack(addr12.getHostName(), "/zone2/ud3");

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(2);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());
        rwAddrs.add(addr7.toBookieId());
        rwAddrs.add(addr8.toBookieId());
        rwAddrs.add(addr9.toBookieId());
        rwAddrs.add(addr10.toBookieId());
        rwAddrs.add(addr11.toBookieId());
        rwAddrs.add(addr12.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        /*
         * Since there are enough bookies in different UDs in 2 zones
         * (MinNumZonesPerWriteQuorum), new ensemble should succeed.
         */
        PlacementResult<List<BookieId>> newEnsemblePlacementResult = zepp.newEnsemble(6, 6, 2, null,
                new HashSet<>());
        List<BookieId> newEnsembleList = newEnsemblePlacementResult.getResult();
        Set<BookieId> newEnsembleSet = new HashSet<BookieId>(newEnsembleList);
        assertTrue("New ensemble should contain 6 rw bookies in non-default fault domains",
                rwAddrs.containsAll(newEnsembleSet) && (newEnsembleSet.size() == 6));
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_SOFT,
                newEnsemblePlacementResult.getAdheringToPolicy());
        Set<String> bookiesNetworkLocations = new HashSet<String>();

        for (BookieId bookieAddr : newEnsembleSet) {
            bookiesNetworkLocations.add(zepp.resolveNetworkLocation(bookieAddr));
        }
        /*
         * Since there are enough bookies in different UDs, bookies from same
         * zone should be from different UDs.
         */
        assertTrue("Bookies should be from different UpgradeDomains if they belong to same zone",
                (bookiesNetworkLocations.size() == 6));
        List<ZoneAwareNodeLocation> bookiesNodeLocationList = new ArrayList<ZoneAwareNodeLocation>();
        for (BookieId bookieAddr : newEnsembleList) {
            bookiesNodeLocationList.add(zepp.getZoneAwareNodeLocation(bookieAddr));
        }
        for (int i = 0; i < 5; i++) {
            /*
             * in newEnsemble order, bookies should be from alternating zones.
             */
            assertNotEquals("Alternate bookies should be from different zones",
                    bookiesNodeLocationList.get(i).getZone(), bookiesNodeLocationList.get(i + 1).getZone());
        }
    }

    @Test
    public void testNewBookieUniformDistributionWithMinZoneAndMinUDs() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.0.0.12", 3181);
        BookieSocketAddress addr12 = new BookieSocketAddress("127.0.0.13", 3181);
        BookieSocketAddress addr13 = new BookieSocketAddress("127.0.0.14", 3181);
        BookieSocketAddress addr14 = new BookieSocketAddress("127.0.0.15", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr12.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr13.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr14.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());
        rwAddrs.add(addr7.toBookieId());
        rwAddrs.add(addr8.toBookieId());
        rwAddrs.add(addr9.toBookieId());
        rwAddrs.add(addr10.toBookieId());
        rwAddrs.add(addr11.toBookieId());
        rwAddrs.add(addr12.toBookieId());
        rwAddrs.add(addr13.toBookieId());
        rwAddrs.add(addr14.toBookieId());

        int minNumZonesPerWriteQuorum = 3;
        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(5);
        newConf.setMinNumZonesPerWriteQuorum(minNumZonesPerWriteQuorum);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        Set<BookieId> excludedBookies = new HashSet<BookieId>();

        PlacementResult<List<BookieId>> newEnsemblePlacementResult = zepp.newEnsemble(6, 6, 4, null,
                excludedBookies);
        List<BookieId> newEnsembleList = newEnsemblePlacementResult.getResult();
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_SOFT,
                newEnsemblePlacementResult.getAdheringToPolicy());
        Set<BookieId> newEnsembleSet = new HashSet<BookieId>(newEnsembleList);
        Set<String> bookiesNetworkLocationsSet = new HashSet<String>();
        List<ZoneAwareNodeLocation> bookiesNodeLocationList = new ArrayList<ZoneAwareNodeLocation>();
        for (BookieId bookieAddr : newEnsembleSet) {
            bookiesNetworkLocationsSet.add(zepp.resolveNetworkLocation(bookieAddr));
        }
        for (BookieId bookieAddr : newEnsembleList) {
            bookiesNodeLocationList.add(zepp.getZoneAwareNodeLocation(bookieAddr));
        }
        /*
         * since there are enough bookies from minNumZonesPerWriteQuorum (3),
         * bookies should be from 3 different zones and 2 different UDs.
         */
        assertTrue("Bookies should be from different UpgradeDomains if they belong to same zone",
                (bookiesNetworkLocationsSet.size() == 6));
        Set<String> zonesOfFirstNodes = new HashSet<String>();
        for (int i = 0; i < minNumZonesPerWriteQuorum; i++) {
            zonesOfFirstNodes.add(bookiesNodeLocationList.get(i).getZone());
        }
        assertEquals("Num of zones", minNumZonesPerWriteQuorum, zonesOfFirstNodes.size());
        for (int i = 0; i < minNumZonesPerWriteQuorum; i++) {
            assertEquals("Zone", bookiesNodeLocationList.get(i).getZone(),
                    bookiesNodeLocationList.get(i + minNumZonesPerWriteQuorum).getZone());
            assertNotEquals("UpgradeDomain", bookiesNodeLocationList.get(i).getUpgradeDomain(),
                    bookiesNodeLocationList.get(i + minNumZonesPerWriteQuorum).getUpgradeDomain());
        }
    }

    @Test
    public void testReplaceBookie() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.0.0.12", 3181);
        BookieSocketAddress addr12 = new BookieSocketAddress("127.0.0.13", 3181);
        BookieSocketAddress addr13 = new BookieSocketAddress("127.0.0.14", 3181);
        BookieSocketAddress addr14 = new BookieSocketAddress("127.0.0.15", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr12.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr13.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr14.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(3);
        newConf.setMinNumZonesPerWriteQuorum(3);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());
        rwAddrs.add(addr7.toBookieId());
        rwAddrs.add(addr8.toBookieId());
        rwAddrs.add(addr9.toBookieId());
        rwAddrs.add(addr10.toBookieId());
        rwAddrs.add(addr11.toBookieId());
        rwAddrs.add(addr12.toBookieId());
        rwAddrs.add(addr13.toBookieId());
        rwAddrs.add(addr14.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        List<BookieId> ensemble = new ArrayList<BookieId>();
        Set<BookieId> excludedBookies = new HashSet<BookieId>();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr5.toBookieId());
        ensemble.add(addr9.toBookieId());
        ensemble.add(addr3.toBookieId());
        ensemble.add(addr7.toBookieId());
        ensemble.add(addr11.toBookieId());
        /*
         * since addr5 (/zone2/ud1) is already part of ensemble of size 6, write
         * quorum of size 6, to replace bookie addr7 (/zone2/ud2), new bookie
         * should be from /zone2/ud2.
         */
        PlacementResult<BookieId> replacePlacementResult = zepp.replaceBookie(6, 6, 2, null, ensemble,
                addr7.toBookieId(),
                excludedBookies);
        BookieId replacedBookie = replacePlacementResult.getResult();
        assertEquals("replaced bookie", addr8.toBookieId(), replacedBookie);
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                replacePlacementResult.getAdheringToPolicy());

        excludedBookies.add(addr8.toBookieId());
        /*
         * here addr8 is excluded, and writeQuorumSize is 3. So to replace
         * bookie addr7, addr6 (belonging to same zone) is the candidate.
         */
        replacePlacementResult = zepp.replaceBookie(6, 3, 2, null, ensemble, addr7.toBookieId(),
                excludedBookies);
        replacedBookie = replacePlacementResult.getResult();
        assertEquals("replaced bookie", addr6.toBookieId(), replacedBookie);

        excludedBookies.add(addr6.toBookieId());
        try {
            /*
             * here addr6 is also excluded, so replaceBookie should fail.
             */
            replacedBookie = zepp.replaceBookie(6, 3, 2, null, ensemble, addr7.toBookieId(), excludedBookies)
                    .getResult();
            fail("Expected BKNotEnoughBookiesException for replaceBookie with added excludedBookies");
        } catch (BKException.BKNotEnoughBookiesException bkne) {
            // expected NotEnoughBookiesException
        }
    }

    @Test
    public void testReplaceBookieMinUDs() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.0.0.12", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(3);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());
        rwAddrs.add(addr7.toBookieId());
        rwAddrs.add(addr8.toBookieId());
        rwAddrs.add(addr9.toBookieId());
        rwAddrs.add(addr10.toBookieId());
        rwAddrs.add(addr11.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        List<BookieId> ensemble = new ArrayList<BookieId>();
        Set<BookieId> excludedBookies = new HashSet<BookieId>();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr3.toBookieId());
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr5.toBookieId());
        ensemble.add(addr6.toBookieId());
        /*
         * though all the remaining non-default bookies are in /zone3/ud2, for
         * replacing addr4 replaceBookie should be able to find some other
         * bookie in /zone3/ud2.
         */
        PlacementResult<BookieId> replaceResponse = zepp.replaceBookie(6, 6, 2, null, ensemble, addr4.toBookieId(),
                excludedBookies);
        BookieId replacedBookie = replaceResponse.getResult();
        assertEquals("replaced bookie", "/zone3/ud2", zepp.resolveNetworkLocation(replacedBookie));
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_SOFT,
                replaceResponse.getAdheringToPolicy());
    }

    @Test
    public void testAreAckedBookiesAdheringToPlacementPolicy() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud3");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3/ud3");

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(2);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr6.toBookieId());
        rwAddrs.add(addr7.toBookieId());
        rwAddrs.add(addr8.toBookieId());
        rwAddrs.add(addr9.toBookieId());

        zepp.onClusterChanged(rwAddrs, roAddrs);
        Set<BookieId> ackedBookies = new HashSet<BookieId>();
        ackedBookies.add(addr1.toBookieId());
        ackedBookies.add(addr4.toBookieId());
        assertFalse("since both the bookies are in the same zone, it should return false",
                zepp.areAckedBookiesAdheringToPlacementPolicy(ackedBookies, 10, 2));
        ackedBookies.clear();
        ackedBookies.add(addr1.toBookieId());
        ackedBookies.add(addr2.toBookieId());
        assertFalse("since ackQuorumSize is 3, it should return false",
                zepp.areAckedBookiesAdheringToPlacementPolicy(ackedBookies, 10, 3));
        assertTrue("since ackQuorumSize is 2 and bookies are from minNumZonesPerWriteQuorum it should return true",
                zepp.areAckedBookiesAdheringToPlacementPolicy(ackedBookies, 10, 2));

        zepp.uninitalize();
        newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(4);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        ackedBookies.clear();
        ackedBookies.add(addr1.toBookieId());
        ackedBookies.add(addr2.toBookieId());
        ackedBookies.add(addr3.toBookieId());
        assertFalse("since minNumZonesPerWriteQuorum is set to 4, it should return false",
                zepp.areAckedBookiesAdheringToPlacementPolicy(ackedBookies, 4, 3));
        assertTrue("since writeQuorumSize is set to 3, it should return true",
                zepp.areAckedBookiesAdheringToPlacementPolicy(ackedBookies, 3, 3));
        ackedBookies.clear();
        ackedBookies.add(addr1.toBookieId());
        ackedBookies.add(addr2.toBookieId());
        ackedBookies.add(addr4.toBookieId());
        assertFalse("since bookies are in just 2 zones but not in 3 zones, it should return false",
                zepp.areAckedBookiesAdheringToPlacementPolicy(ackedBookies, 3, 3));
    }

    @Test
    public void testWeightedPlacement() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());

        int multiple = 10;

        ClientConfiguration newConf = new ClientConfiguration(conf);
        newConf.addConfiguration(conf);
        newConf.setDiskWeightBasedPlacementEnabled(true);
        /*
         * since BookieMaxWeightMultipleForWeightBasedPlacement is set to -1,
         * there is no max cap on weight.
         */
        newConf.setBookieMaxWeightMultipleForWeightBasedPlacement(-1);
        newConf.setMinNumZonesPerWriteQuorum(0);
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        zepp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<BookieId, BookieInfo>();
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(multiple * 100L, multiple * 100L));
        bookieInfoMap.put(addr5.toBookieId(), new BookieInfo(100L, 100L));
        zepp.updateBookieInfo(bookieInfoMap);

        Map<BookieId, Long> selectionCounts = new HashMap<BookieId, Long>();
        int numTries = 50000;
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> newEnsembleResponse;
        List<BookieId> newEnsemble;
        for (BookieId addr : addrs) {
            selectionCounts.put(addr, (long) 0);
        }
        for (int i = 0; i < numTries; i++) {
            // new ensemble response
            newEnsembleResponse = zepp.newEnsemble(1, 1, 1, null, new HashSet<BookieId>());
            newEnsemble = newEnsembleResponse.getResult();
            selectionCounts.put(newEnsemble.get(0), selectionCounts.get(newEnsemble.get(0)) + 1);
        }
        double observedMultiple = ((double) selectionCounts.get(addr4.toBookieId())
                / (double) selectionCounts.get(addr3.toBookieId()));
        /*
         * since there is no cap on maxWeight, observedMultiple should be
         * roughly equal to multiple
         */
        assertTrue("Weights not being honored " + observedMultiple, Math.abs(observedMultiple - multiple) < 1);

        selectionCounts.clear();
        selectionCounts.put(addr3.toBookieId(), (long) 0);
        selectionCounts.put(addr4.toBookieId(), (long) 0);
        newEnsemble = new ArrayList<BookieId>();
        newEnsemble.add(addr2.toBookieId());
        Set<BookieId> excludedBookies = new HashSet<BookieId>();
        excludedBookies.add(addr1.toBookieId());
        EnsemblePlacementPolicy.PlacementResult<BookieId> replacedBookieResponse;
        BookieId replacedBookie;
        for (int i = 0; i < numTries; i++) {
            // replace bookie response
            replacedBookieResponse = zepp.replaceBookie(1, 1, 1, null, newEnsemble, addr2.toBookieId(),
                    excludedBookies);
            replacedBookie = replacedBookieResponse.getResult();
            /*
             * only addr3 and addr4 are eligible for replacedBookie.
             */
            assertTrue("replaced : " + replacedBookie, addr3.toBookieId().equals(replacedBookie)
                    || addr4.toBookieId().equals(replacedBookie));
            selectionCounts.put(replacedBookie, selectionCounts.get(replacedBookie) + 1);
        }
        observedMultiple = ((double) selectionCounts.get(addr4.toBookieId())
                / (double) selectionCounts.get(addr3.toBookieId()));
        /*
         * since there is no cap on maxWeight, observedMultiple should be
         * roughly equal to multiple
         */
        assertTrue("Weights not being honored " + observedMultiple, Math.abs(observedMultiple - multiple) < 1);
    }

    @Test
    public void testPlacementOnStabilizeNetworkTopology() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone4/ud1");

        zepp = new ZoneawareEnsemblePlacementPolicy();
        ClientConfiguration confLocal = new ClientConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setNetworkTopologyStabilizePeriodSeconds(99999);
        zepp.initialize(confLocal, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        zepp.onClusterChanged(addrs, new HashSet<BookieId>());
        // addr4 left
        addrs.remove(addr4.toBookieId());
        Set<BookieId> deadBookies = zepp.onClusterChanged(addrs, new HashSet<BookieId>());
        assertTrue(deadBookies.isEmpty());

        // we will never use addr4 even it is in the stabilized network topology
        for (int i = 0; i < 5; i++) {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse = zepp.newEnsemble(3, 3,
                    2, null, new HashSet<BookieId>());
            List<BookieId> ensemble = ensembleResponse.getResult();
            assertFalse(ensemble.contains(addr4.toBookieId()));
            assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                    ensembleResponse.getAdheringToPolicy());
        }

        // we could still use addr4 for urgent allocation if it is just bookie
        // flapping
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse = zepp.newEnsemble(4, 4, 2,
                null, new HashSet<BookieId>());
        List<BookieId> ensemble = ensembleResponse.getResult();
        assertTrue(ensemble.contains(addr4.toBookieId()));
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                ensembleResponse.getAdheringToPolicy());
    }

    @Test
    public void testCreateNewEnsembleRandomly() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone1/ud1");

        zepp = new ZoneawareEnsemblePlacementPolicy();
        ClientConfiguration confLocal = new ClientConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setEnforceStrictZoneawarePlacement(false);
        confLocal.setMinNumZonesPerWriteQuorum(3);
        confLocal.setDesiredNumZonesPerWriteQuorum(4);
        zepp.initialize(confLocal, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        Set<BookieId> excludeBookies = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        excludeBookies.add(addr5.toBookieId());
        zepp.onClusterChanged(rwAddrs, roAddrs);
        /*
         * if enforceStrictZoneawarePlacement is not enabled, then there is no
         * restrictions on ensSize and writeQSize and also bookie belonging to
         * DEFAULT_ZONE_AND_UPGRADEDOMAIN can be a candidate.
         */
        PlacementResult<List<BookieId>> newEnsemblePlacementResult = zepp.newEnsemble(4, 3, 2, null,
                excludeBookies);
        Set<BookieId> newEnsembleSet = new HashSet<BookieId>(
                newEnsemblePlacementResult.getResult());
        assertEquals("New ensemble should contain 4 rw bookies", 4, newEnsembleSet.size());
        assertFalse("excludeBookie should not be included in the ensemble",
                newEnsembleSet.contains(addr5.toBookieId()));
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                newEnsemblePlacementResult.getAdheringToPolicy());

        rwAddrs.remove(addr4.toBookieId());
        roAddrs.add(addr4.toBookieId());
        zepp.onClusterChanged(rwAddrs, roAddrs);
        try {
            /*
             * since there is no bookie available, newEnsemble should fail.
             */
            zepp.newEnsemble(4, 3, 2, null, excludeBookies);
            fail("Creation of new ensemble randomly should fail because of not sufficient bookies");
        } catch (BKException.BKNotEnoughBookiesException bkne) {
            // expected NotEnoughBookiesException
        }
    }

    @Test
    public void testReplaceBookieRandomly() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        zepp = new ZoneawareEnsemblePlacementPolicy();
        ClientConfiguration confLocal = new ClientConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setEnforceStrictZoneawarePlacement(false);
        confLocal.setMinNumZonesPerWriteQuorum(3);
        confLocal.setDesiredNumZonesPerWriteQuorum(4);
        zepp.initialize(confLocal, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieId> rwAddrs = new HashSet<BookieId>();
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        Set<BookieId> excludeBookies = new HashSet<BookieId>();
        rwAddrs.add(addr1.toBookieId());
        rwAddrs.add(addr2.toBookieId());
        rwAddrs.add(addr3.toBookieId());
        rwAddrs.add(addr4.toBookieId());
        rwAddrs.add(addr5.toBookieId());
        rwAddrs.add(addr7.toBookieId());

        roAddrs.add(addr6.toBookieId());
        excludeBookies.add(addr5.toBookieId());
        zepp.onClusterChanged(rwAddrs, roAddrs);
        List<BookieId> ensembleList = new ArrayList<BookieId>();
        ensembleList.add(addr1.toBookieId());
        ensembleList.add(addr2.toBookieId());
        ensembleList.add(addr3.toBookieId());
        ensembleList.add(addr4.toBookieId());

        PlacementResult<BookieId> replaceResponse = zepp.replaceBookie(4, 3, 2, null, ensembleList, addr3.toBookieId(),
                excludeBookies);
        BookieId replaceBookie = replaceResponse.getResult();
        /*
         * if enforceStrictZoneawarePlacement is not enabled, then there is no
         * restrictions on ensSize and writeQSize and also bookie belonging to
         * DEFAULT_ZONE_AND_UPGRADEDOMAIN can be a candidate.
         */
        assertEquals("ReplaceBookie candidate", addr7.toBookieId(), replaceBookie);
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                replaceResponse.getAdheringToPolicy());

        rwAddrs.remove(addr7.toBookieId());
        excludeBookies.add(addr7.toBookieId());
        zepp.onClusterChanged(rwAddrs, roAddrs);
        try {
            /*
             * since there is no bookie available, replaceBookie should fail.
             */
            zepp.replaceBookie(4, 3, 2, null, ensembleList, addr3.toBookieId(), excludeBookies);
            fail("ReplaceBookie should fail because of unavailable bookies");
        } catch (BKException.BKNotEnoughBookiesException bkne) {
            // expected NotEnoughBookiesException
        }
    }

    @Test
    public void testIsEnsembleAdheringToPlacementPolicy() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        zepp = new ZoneawareEnsemblePlacementPolicy();
        ClientConfiguration confLocal = new ClientConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setEnforceStrictZoneawarePlacement(true);
        confLocal.setMinNumZonesPerWriteQuorum(2);
        confLocal.setDesiredNumZonesPerWriteQuorum(3);
        zepp.initialize(confLocal, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        List<BookieId> emptyEnsmeble = new ArrayList<>();
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                zepp.isEnsembleAdheringToPlacementPolicy(emptyEnsmeble, 3, 2));

        List<BookieId> ensemble = new ArrayList<BookieId>();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr3.toBookieId());
        // all bookies in same rack
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 2));

        ensemble.clear();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr4.toBookieId());
        // bookies spread across minZones
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_SOFT,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 2));

        ensemble.clear();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr7.toBookieId());
        // bookies spread across desirednumofzones
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 2));

        ensemble.clear();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr4.toBookieId());
        // writeQuorum should be greater than minZones
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 2, 2));

        ensemble.clear();
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr3.toBookieId());
        ensemble.add(addr4.toBookieId());
        // bookies from zone1 (addr2 and addr3) are in same UD
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 2));

        ensemble.clear();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr7.toBookieId());
        ensemble.add(addr10.toBookieId());
        // bookie from default faultdomain will cause PlacementPolicyAdherence
        // to fail
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 4, 2));

        ensemble.clear();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr7.toBookieId());
        ensemble.add(addr8.toBookieId());
        ensemble.add(addr9.toBookieId());
        // bookies are spread across desired zones and bookie from same zone are
        // spread across 2 UDs
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 5, 2));

        ensemble.clear();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr7.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr8.toBookieId());
        ensemble.add(addr9.toBookieId());
        /*
         * writeset of addr2, addr8 and addr9 fails, because addr8 and addr9
         * belong to z3u2
         */
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 2));

        ensemble.clear();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr9.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr8.toBookieId());
        ensemble.add(addr7.toBookieId());
        /*
         * writeset of addr9, addr2 and addr8 fails, because addr8 and addr9
         * belong to z3u2
         */
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 2));

        ensemble.clear();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr9.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr7.toBookieId());
        ensemble.add(addr8.toBookieId());
        /*
         * writeset of addr2, addr7 and addr8 just meets soft.
         */
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_SOFT,
                zepp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 2));
    }
}
