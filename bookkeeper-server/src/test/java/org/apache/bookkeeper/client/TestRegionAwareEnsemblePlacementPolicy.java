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

import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy
        .REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy
        .REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_VALIDATION;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_MINIMUM_REGIONS_FOR_DURABILITY;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_REGIONS_TO_WRITE;
import static org.apache.bookkeeper.client.RoundRobinDistributionSchedule.writeSetFromValues;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import junit.framework.TestCase;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a region-aware ensemble placement policy.
 */
public class TestRegionAwareEnsemblePlacementPolicy extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestRegionAwareEnsemblePlacementPolicy.class);

    RegionAwareEnsemblePlacementPolicy repp;
    final ClientConfiguration conf = new ClientConfiguration();
    final ArrayList<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
    DistributionSchedule.WriteSet writeSet = DistributionSchedule.NULL_WRITE_SET;
    BookieSocketAddress addr1, addr2, addr3, addr4;
    HashedWheelTimer timer;

    static void updateMyRack(String rack) throws Exception {
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), rack);
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostName(), rack);
        BookieSocketAddress bookieAddress = new BookieSocketAddress(
            InetAddress.getLocalHost().getHostAddress(), 0);
        StaticDNSResolver.addNodeToRack(bookieAddress.getSocketAddress().getHostName(), rack);
        StaticDNSResolver.addNodeToRack(bookieAddress.getSocketAddress().getAddress().getHostAddress(), rack);
        StaticDNSResolver.addNodeToRack("127.0.0.1", rack);
        StaticDNSResolver.addNodeToRack("localhost", rack);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        StaticDNSResolver.reset();
        updateMyRack(NetworkTopology.DEFAULT_REGION_AND_RACK);
        LOG.info("Set up static DNS Resolver.");
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());

        addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/r1/rack1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/r1/rack2");
        ensemble.add(addr1);
        ensemble.add(addr2);
        ensemble.add(addr3);
        ensemble.add(addr4);

        writeSet = writeSetFromValues(0, 1, 2, 3);

        timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getTimeoutTimerNumTicks());

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
    }

    @Override
    protected void tearDown() throws Exception {
        repp.uninitalize();
        super.tearDown();
    }

    static BookiesHealthInfo getBookiesHealthInfo() {
        return getBookiesHealthInfo(new HashMap<>(), new HashMap<>());
    }

    static BookiesHealthInfo getBookiesHealthInfo(Map<BookieSocketAddress, Long> bookieFailureHistory,
                                                  Map<BookieSocketAddress, Long> bookiePendingRequests) {
        return new BookiesHealthInfo() {
            @Override
            public long getBookieFailureHistory(BookieSocketAddress bookieSocketAddress) {
                return bookieFailureHistory.getOrDefault(bookieSocketAddress, -1L);
            }

            @Override
            public long getBookiePendingRequests(BookieSocketAddress bookieSocketAddress) {
                return bookiePendingRequests.getOrDefault(bookieSocketAddress, 0L);
            }
        };
    }


    @Test
    public void testNotReorderReadIfInDefaultRack() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(), writeSet);
        assertEquals(origWriteSet, reorderSet);
    }

    @Test
    public void testNodeInSameRegion() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack3");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        // make sure we've detected the right region
        assertEquals("r1", repp.myRegion);

        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(), writeSet.copy());
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(0, 3, 1, 2);
        LOG.info("write set : {}", writeSet);
        LOG.info("reorder set : {}", reorderSet);
        LOG.info("expected set : {}", expectedSet);
        LOG.info("reorder equals {}", reorderSet.equals(writeSet));
        assertFalse(reorderSet.equals(writeSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeNotInSameRegions() throws Exception {
        repp.uninitalize();
        updateMyRack("/r2/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(), writeSet);
        LOG.info("reorder set : {}", reorderSet);
        assertEquals(origWriteSet, reorderSet);
    }

    @Test
    public void testNodeDown() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(3, 1, 2, 0);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeReadOnly() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        Set<BookieSocketAddress> ro = new HashSet<BookieSocketAddress>();
        ro.add(addr1);
        repp.onClusterChanged(addrs, ro);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(3, 1, 2, 0);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeSlow() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        repp.registerSlowBookie(addr1, 0L);
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1, 1L);
        repp.onClusterChanged(addrs, new HashSet<>());

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(3, 1, 2, 0);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testTwoNodesSlow() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        repp.registerSlowBookie(addr1, 0L);
        repp.registerSlowBookie(addr2, 0L);
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1, 1L);
        bookiePendingMap.put(addr2, 2L);
        repp.onClusterChanged(addrs, new HashSet<>());

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(3, 2, 0, 1);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testTwoNodesDown() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        addrs.remove(addr2);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(3, 2, 0, 1);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeDownAndNodeSlow() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        repp.registerSlowBookie(addr1, 0L);
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1, 1L);
        addrs.remove(addr2);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(3, 2, 0, 1);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeDownAndReadOnlyAndNodeSlow() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        addrs.remove(addr2);
        Set<BookieSocketAddress> ro = new HashSet<>();
        ro.add(addr2);
        repp.registerSlowBookie(addr3, 0L);
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr3, 1L);
        repp.onClusterChanged(addrs, ro);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(3, 1, 2, 0);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInSameRegion() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Pair<BookieSocketAddress, Boolean> replaceBookieResponse = repp.replaceBookie(1, 1, 1, null,
                new ArrayList<BookieSocketAddress>(), addr2, new HashSet<BookieSocketAddress>());
        BookieSocketAddress replacedBookie = replaceBookieResponse.getLeft();
        assertEquals(addr3, replacedBookie);
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInDifferentRegion() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region2/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region3/r4");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
        excludedAddrs.add(addr1);
        Pair<BookieSocketAddress, Boolean> replaceBookieResponse = repp.replaceBookie(1, 1, 1, null,
                new ArrayList<BookieSocketAddress>(), addr2, excludedAddrs);
        BookieSocketAddress replacedBookie = replaceBookieResponse.getLeft();

        assertFalse(addr1.equals(replacedBookie));
        assertTrue(addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
    }

    @Test
    public void testNewEnsembleBookieWithNotEnoughBookies() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region2/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region3/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region4/r4");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(5, 5, 3, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> list = ensembleResponse.getLeft();
            LOG.info("Ensemble : {}", list);
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
        } catch (BKNotEnoughBookiesException bnebe) {
            // should throw not enou
        }
    }

    @Test
    public void testReplaceBookieWithNotEnoughBookies() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region2/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region3/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region4/r4");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
        excludedAddrs.add(addr1);
        excludedAddrs.add(addr3);
        excludedAddrs.add(addr4);
        try {
            repp.replaceBookie(1, 1, 1, null, new ArrayList<BookieSocketAddress>(), addr2, excludedAddrs);
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
        } catch (BKNotEnoughBookiesException bnebe) {
            // should throw not enou
        }
    }

    @Test
    public void testNewEnsembleWithSingleRegion() throws Exception {
        repp.uninitalize();
        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region1/r2");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(3, 2, 2, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
            assertEquals(0, getNumCoveredRegionsInWriteQuorum(ensemble, 2));
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse2 = repp.newEnsemble(4, 2, 2, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble2 = ensembleResponse2.getLeft();
            assertEquals(0, getNumCoveredRegionsInWriteQuorum(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testNewEnsembleWithMultipleRegions() throws Exception {
        repp.uninitalize();
        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region1/r2");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(3, 2, 2, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
            int numCovered = getNumCoveredRegionsInWriteQuorum(ensemble, 2);
            assertTrue(numCovered >= 1);
            assertTrue(numCovered < 3);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
        try {
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse2 = repp.newEnsemble(4, 2, 2, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble2 = ensembleResponse2.getLeft();
            int numCovered = getNumCoveredRegionsInWriteQuorum(ensemble2, 2);
            assertTrue(numCovered >= 1 && numCovered < 3);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testNewEnsembleWithEnoughRegions() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/default-region/default-rack1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region2/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region3/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/default-region/default-rack2");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/region1/r12");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/region2/r13");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/region3/r14");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse1 = repp.newEnsemble(3, 2, 2, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble1 = ensembleResponse1.getLeft();
            assertEquals(3, getNumCoveredRegionsInWriteQuorum(ensemble1, 2));
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse2 = repp.newEnsemble(4, 2, 2, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble2 = ensembleResponse2.getLeft();
            assertEquals(4, getNumCoveredRegionsInWriteQuorum(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testNewEnsembleWithThreeRegions() throws Exception {
        repp.uninitalize();
        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/region2/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region2/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region3/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/region1/r11");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/region1/r12");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/region2/r13");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/region3/r14");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/region2/r23");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/region1/r24");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        addrs.add(addr9);
        addrs.add(addr10);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(6, 6, 4, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
            assert(ensemble.contains(addr4));
            assert(ensemble.contains(addr8));
            assert(ensemble.size() == 6);
            assertEquals(3, getNumRegionsInEnsemble(ensemble));
            ensembleResponse = repp.newEnsemble(7, 7, 4, null, new HashSet<BookieSocketAddress>());
            ensemble = ensembleResponse.getLeft();
            assert(ensemble.contains(addr4));
            assert(ensemble.contains(addr8));
            assert(ensemble.size() == 7);
            assertEquals(3, getNumRegionsInEnsemble(ensemble));
            ensembleResponse = repp.newEnsemble(8, 8, 5, null, new HashSet<BookieSocketAddress>());
            ensemble = ensembleResponse.getLeft();
            assert(ensemble.contains(addr4));
            assert(ensemble.contains(addr8));
            assert(ensemble.size() == 8);
            assertEquals(3, getNumRegionsInEnsemble(ensemble));
            ensembleResponse = repp.newEnsemble(9, 9, 5, null, new HashSet<BookieSocketAddress>());
            ensemble = ensembleResponse.getLeft();
            assert(ensemble.contains(addr4));
            assert(ensemble.contains(addr8));
            assert(ensemble.size() == 9);
            assertEquals(3, getNumRegionsInEnsemble(ensemble));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testNewEnsembleWithThreeRegionsWithDisable() throws Exception {
        FeatureProvider featureProvider = new SettableFeatureProvider("", 0);
        repp.uninitalize();
        repp = new RegionAwareEnsemblePlacementPolicy();
        conf.setProperty(REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME, "disallowBookies");
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, featureProvider, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/region2/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region2/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region3/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/region1/r11");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/region1/r12");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/region2/r13");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/region3/r14");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/region2/r23");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/region1/r24");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        addrs.add(addr9);
        addrs.add(addr10);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            ((SettableFeature) featureProvider.scope("region1").getFeature("disallowBookies")).set(true);
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(6, 6, 4, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
            assertEquals(2, getNumRegionsInEnsemble(ensemble));
            assert(ensemble.contains(addr1));
            assert(ensemble.contains(addr3));
            assert(ensemble.contains(addr4));
            assert(ensemble.contains(addr7));
            assert(ensemble.contains(addr8));
            assert(ensemble.contains(addr9));
            assert(ensemble.size() == 6);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
        try {
            ((SettableFeature) featureProvider.scope("region2").getFeature("disallowBookies")).set(true);
            repp.newEnsemble(6, 6, 4, null, new HashSet<BookieSocketAddress>());
            fail("Should get not enough bookies exception even there is only one region with insufficient bookies.");
        } catch (BKNotEnoughBookiesException bnebe) {
            // Expected
        }
        try {
            ((SettableFeature) featureProvider.scope("region2").getFeature("disallowBookies")).set(false);
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(6, 6, 4, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
            assert(ensemble.contains(addr1));
            assert(ensemble.contains(addr3));
            assert(ensemble.contains(addr4));
            assert(ensemble.contains(addr7));
            assert(ensemble.contains(addr8));
            assert(ensemble.contains(addr9));
            assert(ensemble.size() == 6);
            assertEquals(2, getNumRegionsInEnsemble(ensemble));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }


    @Test
    public void testNewEnsembleWithFiveRegions() throws Exception {
        repp.uninitalize();
        repp = new RegionAwareEnsemblePlacementPolicy();
        conf.setProperty(REPP_REGIONS_TO_WRITE, "region1;region2;region3;region4;region5");
        conf.setProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, 5);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.1.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.1.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.1.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.1.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.1.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.1.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.1.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.1.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.1.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.1.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.1.0.12", 3181);
        BookieSocketAddress addr12 = new BookieSocketAddress("127.1.0.13", 3181);
        BookieSocketAddress addr13 = new BookieSocketAddress("127.1.0.14", 3181);
        BookieSocketAddress addr14 = new BookieSocketAddress("127.1.0.15", 3181);
        BookieSocketAddress addr15 = new BookieSocketAddress("127.1.0.16", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/region1/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region1/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region2/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/region2/r11");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/region2/r12");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/region3/r13");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/region3/r14");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/region3/r23");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/region4/r24");
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), "/region4/r31");
        StaticDNSResolver.addNodeToRack(addr12.getHostName(), "/region4/r32");
        StaticDNSResolver.addNodeToRack(addr13.getHostName(), "/region5/r33");
        StaticDNSResolver.addNodeToRack(addr14.getHostName(), "/region5/r34");
        StaticDNSResolver.addNodeToRack(addr15.getHostName(), "/region5/r35");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        addrs.add(addr9);
        addrs.add(addr10);
        addrs.add(addr11);
        addrs.add(addr12);
        addrs.add(addr13);
        addrs.add(addr14);
        addrs.add(addr15);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        try {
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(10, 10, 10, null,
                    new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
            assert(ensemble.size() == 10);
            assertEquals(5, getNumRegionsInEnsemble(ensemble));
        } catch (BKNotEnoughBookiesException bnebe) {
            LOG.error("BKNotEnoughBookiesException", bnebe);
            fail("Should not get not enough bookies exception even there is only one rack.");
        }

        try {
            Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
            excludedAddrs.add(addr10);
            Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(10, 10, 10, null,
                    excludedAddrs);
            List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
            assert(ensemble.contains(addr11) && ensemble.contains(addr12));
            assert(ensemble.size() == 10);
            assertEquals(5, getNumRegionsInEnsemble(ensemble));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testEnsembleWithThreeRegionsReplace() throws Exception {
        testEnsembleWithThreeRegionsReplaceInternal(3, false, false);
    }

    @Test
    public void testEnsembleWithThreeRegionsReplaceDisableOneRegion() throws Exception {
        testEnsembleWithThreeRegionsReplaceInternal(2, false, true);
    }

    @Test
    public void testEnsembleWithThreeRegionsReplaceMinDurabilityOne() throws Exception {
        testEnsembleWithThreeRegionsReplaceInternal(1, false, false);
    }

    @Test
    public void testEnsembleWithThreeRegionsReplaceDisableDurability() throws Exception {
        testEnsembleWithThreeRegionsReplaceInternal(1, true, false);
    }

    public void testEnsembleWithThreeRegionsReplaceInternal(int minDurability, boolean disableDurability,
            boolean disableOneRegion) throws Exception {
        repp.uninitalize();
        repp = new RegionAwareEnsemblePlacementPolicy();
        conf.setProperty(REPP_REGIONS_TO_WRITE, "region1;region2;region3");
        conf.setProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, minDurability);
        FeatureProvider featureProvider = new SettableFeatureProvider("", 0);
        if (minDurability <= 1) {
            conf.setProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, false);
        } else {
            conf.setProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, true);
        }
        conf.setProperty(REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME, "disallowBookies");

        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, featureProvider, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.1.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.1.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.1.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.1.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.1.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.1.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.1.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.1.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.1.0.10", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/region1/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region1/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region2/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/region2/r11");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/region2/r12");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/region3/r13");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/region3/r14");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/region3/r23");

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        addrs.add(addr9);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        SettableFeature disableDurabilityFeature =
                (SettableFeature) featureProvider.getFeature(
                        BookKeeperConstants.FEATURE_REPP_DISABLE_DURABILITY_ENFORCEMENT);

        if (disableDurability) {
            disableDurabilityFeature.set(true);
        }

        int ackQuorum = 4;
        if (minDurability > 2) {
            ackQuorum = 5;
        }

        Pair<List<BookieSocketAddress>, Boolean> ensembleResponse;
        List<BookieSocketAddress> ensemble;
        try {
            ensembleResponse = repp.newEnsemble(6, 6, ackQuorum, null, new HashSet<BookieSocketAddress>());
            ensemble = ensembleResponse.getLeft();
            assert(ensemble.size() == 6);
            assertEquals(3, getNumRegionsInEnsemble(ensemble));
        } catch (BKNotEnoughBookiesException bnebe) {
            LOG.error("BKNotEnoughBookiesException", bnebe);
            fail("Should not get not enough bookies exception even there is only one rack.");
            throw bnebe;
        }

        if (disableOneRegion) {
            ((SettableFeature) featureProvider.scope("region2").getFeature("disallowBookies")).set(true);
            Set<BookieSocketAddress> region2Bookies = new HashSet<BookieSocketAddress>();
            region2Bookies.add(addr4);
            region2Bookies.add(addr5);
            region2Bookies.add(addr6);
            Set<BookieSocketAddress> region1And3Bookies = new HashSet<BookieSocketAddress>(addrs);
            region1And3Bookies.removeAll(region2Bookies);

            Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
            for (BookieSocketAddress addr: region2Bookies) {
                if (ensemble.contains(addr)) {
                    Pair<BookieSocketAddress, Boolean> replaceBookieResponse = repp.replaceBookie(6, 6, ackQuorum, null,
                            ensemble, addr, excludedAddrs);
                    BookieSocketAddress replacedBookie = replaceBookieResponse.getLeft();
                    ensemble.remove(addr);
                    ensemble.add(replacedBookie);
                }
            }
            assertEquals(2, getNumRegionsInEnsemble(ensemble));
            assertTrue(ensemble.containsAll(region1And3Bookies));
        } else {
            BookieSocketAddress bookieToReplace;
            BookieSocketAddress replacedBookieExpected;
            if (ensemble.contains(addr4)) {
                bookieToReplace = addr4;
                if (ensemble.contains(addr5)) {
                    replacedBookieExpected = addr6;
                } else {
                    replacedBookieExpected = addr5;
                }
            } else {
                replacedBookieExpected = addr4;
                bookieToReplace = addr5;
            }
            Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();

            try {
                Pair<BookieSocketAddress, Boolean> replaceBookieResponse = repp.replaceBookie(6, 6, ackQuorum, null,
                        ensemble, bookieToReplace, excludedAddrs);
                BookieSocketAddress replacedBookie = replaceBookieResponse.getLeft();
                assert (replacedBookie.equals(replacedBookieExpected));
                assertEquals(3, getNumRegionsInEnsemble(ensemble));
            } catch (BKNotEnoughBookiesException bnebe) {
                fail("Should not get not enough bookies exception even there is only one rack.");
            }

            excludedAddrs.add(replacedBookieExpected);
            try {
                repp.replaceBookie(6, 6, ackQuorum, null, ensemble, bookieToReplace, excludedAddrs);
                if (minDurability > 1 && !disableDurabilityFeature.isAvailable()) {
                    fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
                }
            } catch (BKNotEnoughBookiesException bnebe) {
                if (minDurability <= 1 || disableDurabilityFeature.isAvailable()) {
                    fail("Should not throw BKNotEnoughBookiesException when there is not enough bookies");
                }
            }
        }
    }

    @Test
    public void testEnsembleMinDurabilityOne() throws Exception {
        testEnsembleDurabilityDisabledInternal(1, false);
    }

    @Test
    public void testEnsembleDisableDurability() throws Exception {
        testEnsembleDurabilityDisabledInternal(2, true);
    }

    public void testEnsembleDurabilityDisabledInternal(int minDurability, boolean disableDurability) throws Exception {
        repp.uninitalize();
        repp = new RegionAwareEnsemblePlacementPolicy();
        conf.setProperty(REPP_REGIONS_TO_WRITE, "region1;region2;region3");
        conf.setProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, minDurability);
        FeatureProvider featureProvider = new SettableFeatureProvider("", 0);
        if (minDurability <= 1) {
            conf.setProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, false);
        } else {
            conf.setProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, true);
        }

        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, featureProvider, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.1.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.1.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.1.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.1.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.1.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.1.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.1.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.1.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.1.0.10", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/region1/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region1/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region1/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/region1/r11");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/region1/r12");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/region1/r13");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/region1/r14");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/region1/r23");

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        addrs.add(addr9);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        if (disableDurability) {
            ((SettableFeature) featureProvider.getFeature(
                BookKeeperConstants.FEATURE_REPP_DISABLE_DURABILITY_ENFORCEMENT))
                    .set(true);
        }

        Pair<List<BookieSocketAddress>, Boolean> ensembleResponse;
        List<BookieSocketAddress> ensemble;
        try {
            ensembleResponse = repp.newEnsemble(6, 6, 4, null, new HashSet<BookieSocketAddress>());
            ensemble = ensembleResponse.getLeft();
            assert(ensemble.size() == 6);
        } catch (BKNotEnoughBookiesException bnebe) {
            LOG.error("BKNotEnoughBookiesException", bnebe);
            fail("Should not get not enough bookies exception even there is only one rack.");
            throw bnebe;
        }

        Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();

        try {
            repp.replaceBookie(6, 6, 4, null, ensemble, ensemble.get(2), excludedAddrs);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testNewEnsembleFailWithFiveRegions() throws Exception {
        repp.uninitalize();
        repp = new RegionAwareEnsemblePlacementPolicy();
        conf.setProperty(REPP_REGIONS_TO_WRITE, "region1;region2;region3;region4;region5");
        conf.setProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, 5);
        conf.setProperty(REPP_ENABLE_VALIDATION, false);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/region1/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region2/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region2/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/region3/r11");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/region3/r12");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/region4/r13");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/region4/r14");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/region5/r23");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/region5/r24");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        addrs.add(addr9);
        addrs.add(addr10);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
        excludedAddrs.add(addr10);
        excludedAddrs.add(addr9);
        try {
            Pair<List<BookieSocketAddress>, Boolean> list = repp.newEnsemble(5, 5, 5, null, excludedAddrs);
            LOG.info("Ensemble : {}", list.getLeft());
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
        } catch (BKNotEnoughBookiesException bnebe) {
            // should throw not enou
        }
    }

    private void prepareNetworkTopologyForReorderTests(String myRegion) throws Exception {
        repp.uninitalize();
        updateMyRack("/" + myRegion);

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/region1/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/region1/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/region2/r1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/region2/r2");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/region2/r3");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/region3/r1");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/region3/r2");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/region3/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        addrs.add(addr9);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
    }

    @Test
    public void testBasicReorderReadSequenceWithLocalRegion() throws Exception {
        basicReorderReadSequenceWithLocalRegionTest("region2", false);
    }

    @Test
    public void testBasicReorderReadLACSequenceWithLocalRegion() throws Exception {
        basicReorderReadSequenceWithLocalRegionTest("region2", true);
    }

    private void basicReorderReadSequenceWithLocalRegionTest(String myRegion, boolean isReadLAC) throws Exception {
        prepareNetworkTopologyForReorderTests(myRegion);
        Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(9, 9, 5, null,
                new HashSet<BookieSocketAddress>());
        List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
        assertEquals(9, getNumCoveredRegionsInWriteQuorum(ensemble, 9));

        DistributionSchedule ds = new RoundRobinDistributionSchedule(9, 9, 9);

        LOG.info("My region is {}, ensemble : {}", repp.myRegion, ensemble);

        int ensembleSize = ensemble.size();
        for (int i = 0; i < ensembleSize; i++) {
            DistributionSchedule.WriteSet writeSet = ds.getWriteSet(i);
            DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
            DistributionSchedule.WriteSet readSet;
            if (isReadLAC) {
                readSet = repp.reorderReadLACSequence(
                        ensemble,
                        getBookiesHealthInfo(), writeSet);
            } else {
                readSet = repp.reorderReadSequence(
                        ensemble,
                        getBookiesHealthInfo(), writeSet);
            }

            LOG.info("Reorder {} => {}.", origWriteSet, readSet);

            // first few nodes less than REMOTE_NODE_IN_REORDER_SEQUENCE should be local region
            int k = 0;
            for (; k < RegionAwareEnsemblePlacementPolicy.REMOTE_NODE_IN_REORDER_SEQUENCE; k++) {
                BookieSocketAddress address = ensemble.get(readSet.get(k));
                assertEquals(myRegion, StaticDNSResolver.getRegion(address.getHostName()));
            }
            BookieSocketAddress remoteAddress = ensemble.get(readSet.get(k));
            assertFalse(myRegion.equals(StaticDNSResolver.getRegion(remoteAddress.getHostName())));
            k++;
            BookieSocketAddress localAddress = ensemble.get(readSet.get(k));
            assertEquals(myRegion, StaticDNSResolver.getRegion(localAddress.getHostName()));
            k++;
            for (; k < ensembleSize; k++) {
                BookieSocketAddress address = ensemble.get(readSet.get(k));
                assertFalse(myRegion.equals(StaticDNSResolver.getRegion(address.getHostName())));
            }
        }
    }

    @Test
    public void testBasicReorderReadSequenceWithRemoteRegion() throws Exception {
        basicReorderReadSequenceWithRemoteRegionTest("region4", false);
    }

    @Test
    public void testBasicReorderReadLACSequenceWithRemoteRegion() throws Exception {
        basicReorderReadSequenceWithRemoteRegionTest("region4", true);
    }

    private void basicReorderReadSequenceWithRemoteRegionTest(String myRegion, boolean isReadLAC) throws Exception {
        prepareNetworkTopologyForReorderTests(myRegion);

        Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(9, 9, 5, null,
                new HashSet<BookieSocketAddress>());
        List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
        assertEquals(9, getNumCoveredRegionsInWriteQuorum(ensemble, 9));

        DistributionSchedule ds = new RoundRobinDistributionSchedule(9, 9, 9);

        LOG.info("My region is {}, ensemble : {}", repp.myRegion, ensemble);

        int ensembleSize = ensemble.size();
        for (int i = 0; i < ensembleSize; i++) {
            DistributionSchedule.WriteSet writeSet = ds.getWriteSet(i);
            DistributionSchedule.WriteSet readSet;

            if (isReadLAC) {
                readSet = repp.reorderReadLACSequence(
                        ensemble,
                        getBookiesHealthInfo(),
                        writeSet.copy());
            } else {
                readSet = repp.reorderReadSequence(
                        ensemble,
                        getBookiesHealthInfo(),
                        writeSet.copy());
            }

            assertEquals(writeSet, readSet);
        }
    }

    @Test
    public void testReorderReadSequenceWithUnavailableOrReadOnlyBookies() throws Exception {
        reorderReadSequenceWithUnavailableOrReadOnlyBookiesTest(false);
    }

    @Test
    public void testReorderReadLACSequenceWithUnavailableOrReadOnlyBookies() throws Exception {
        reorderReadSequenceWithUnavailableOrReadOnlyBookiesTest(true);
    }

    static Set<BookieSocketAddress> getBookiesForRegion(List<BookieSocketAddress> ensemble, String region) {
        Set<BookieSocketAddress> regionBookies = new HashSet<BookieSocketAddress>();
        for (BookieSocketAddress address : ensemble) {
            String r = StaticDNSResolver.getRegion(address.getHostName());
            if (r.equals(region)) {
                regionBookies.add(address);
            }
        }
        return regionBookies;
    }

    static void appendBookieIndexByRegion(List<BookieSocketAddress> ensemble,
                                          DistributionSchedule.WriteSet writeSet,
                                          String region,
                                          List<Integer> finalSet) {
        for (int i = 0; i < writeSet.size(); i++) {
            int bi = writeSet.get(i);
            String r = StaticDNSResolver.getRegion(ensemble.get(bi).getHostName());
            if (r.equals(region)) {
                finalSet.add(bi);
            }
        }
    }

    private void reorderReadSequenceWithUnavailableOrReadOnlyBookiesTest(boolean isReadLAC) throws Exception {
        String myRegion = "region4";
        String unavailableRegion = "region1";
        String writeRegion = "region2";
        String readOnlyRegion = "region3";

        prepareNetworkTopologyForReorderTests(myRegion);

        Pair<List<BookieSocketAddress>, Boolean> ensembleResponse = repp.newEnsemble(9, 9, 5, null,
                new HashSet<BookieSocketAddress>());
        List<BookieSocketAddress> ensemble = ensembleResponse.getLeft();
        assertEquals(9, getNumCoveredRegionsInWriteQuorum(ensemble, 9));

        DistributionSchedule ds = new RoundRobinDistributionSchedule(9, 9, 9);

        LOG.info("My region is {}, ensemble : {}", repp.myRegion, ensemble);

        Set<BookieSocketAddress> readOnlyBookies = getBookiesForRegion(ensemble, readOnlyRegion);
        Set<BookieSocketAddress> writeBookies = getBookiesForRegion(ensemble, writeRegion);

        repp.onClusterChanged(writeBookies, readOnlyBookies);

        LOG.info("Writable Bookies {}, ReadOnly Bookies {}.", repp.knownBookies.keySet(), repp.readOnlyBookies);

        int ensembleSize = ensemble.size();
        for (int i = 0; i < ensembleSize; i++) {
            DistributionSchedule.WriteSet writeSet = ds.getWriteSet(i);
            DistributionSchedule.WriteSet readSet;
            if (isReadLAC) {
                readSet = repp.reorderReadLACSequence(
                        ensemble, getBookiesHealthInfo(),
                        writeSet.copy());
            } else {
                readSet = repp.reorderReadSequence(
                        ensemble, getBookiesHealthInfo(),
                        writeSet.copy());
            }

            LOG.info("Reorder {} => {}.", writeSet, readSet);

            List<Integer> expectedReadSet = new ArrayList<Integer>();
            // writable bookies
            appendBookieIndexByRegion(ensemble, writeSet, writeRegion, expectedReadSet);
            // readonly bookies
            appendBookieIndexByRegion(ensemble, writeSet, readOnlyRegion, expectedReadSet);
            // unavailable bookies
            appendBookieIndexByRegion(ensemble, writeSet, unavailableRegion, expectedReadSet);
            assertEquals(expectedReadSet.size(), readSet.size());
            for (int j = 0; j < expectedReadSet.size(); j++) {
                assertEquals(expectedReadSet.get(j).intValue(), readSet.get(j));
            }
        }
    }

    private int getNumRegionsInEnsemble(List<BookieSocketAddress> ensemble) {
        Set<String> regions = new HashSet<String>();
        for (BookieSocketAddress addr: ensemble) {
            regions.add(StaticDNSResolver.getRegion(addr.getHostName()));
        }
        return regions.size();
    }

    private int getNumCoveredRegionsInWriteQuorum(List<BookieSocketAddress> ensemble, int writeQuorumSize)
            throws Exception {
        int ensembleSize = ensemble.size();
        int numCoveredWriteQuorums = 0;
        for (int i = 0; i < ensembleSize; i++) {
            Set<String> regions = new HashSet<String>();
            for (int j = 0; j < writeQuorumSize; j++) {
                int bookieIdx = (i + j) % ensembleSize;
                BookieSocketAddress addr = ensemble.get(bookieIdx);
                regions.add(StaticDNSResolver.getRegion(addr.getHostName()));
            }
            numCoveredWriteQuorums += (regions.size() > 1 ? 1 : 0);
        }
        return numCoveredWriteQuorums;
    }

    @Test
    public void testNodeWithFailures() throws Exception {
        repp.uninitalize();
        updateMyRack("/r2/rack1");

        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);

        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/r2/rack1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/r2/rack2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/r1/rack3");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/r2/rack3");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/r2/rack4");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/r1/rack4");
        ensemble.add(addr5);
        ensemble.add(addr6);
        ensemble.add(addr7);
        ensemble.add(addr8);

        DistributionSchedule.WriteSet writeSet2 = writeSetFromValues(0, 1, 2, 3, 4, 5, 6, 7);

        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        HashMap<BookieSocketAddress, Long> bookieFailures = new HashMap<BookieSocketAddress, Long>();

        bookieFailures.put(addr1, 20L);
        bookieFailures.put(addr2, 22L);
        bookieFailures.put(addr3, 24L);
        bookieFailures.put(addr4, 25L);

        LOG.info("write set : {}", writeSet2);
        DistributionSchedule.WriteSet reoderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(bookieFailures, new HashMap<>()), writeSet2);
        LOG.info("reorder set : {}", reoderSet);
        assertEquals(ensemble.get(reoderSet.get(0)), addr6);
        assertEquals(ensemble.get(reoderSet.get(1)), addr7);
        assertEquals(ensemble.get(reoderSet.get(2)), addr5);
        assertEquals(ensemble.get(reoderSet.get(3)), addr2);
        assertEquals(ensemble.get(reoderSet.get(4)), addr3);
        assertEquals(ensemble.get(reoderSet.get(5)), addr8);
        assertEquals(ensemble.get(reoderSet.get(6)), addr1);
        assertEquals(ensemble.get(reoderSet.get(7)), addr4);
    }

}
