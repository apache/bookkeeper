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
import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.shuffleWithMask;
import static org.apache.bookkeeper.client.RoundRobinDistributionSchedule.writeSetFromValues;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.HashedWheelTimer;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble;
import org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy.BookieNode;
import org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy.EnsembleForReplacementWithNoConstraints;
import org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy.TruePredicate;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the rackaware ensemble placement policy.
 */
public class TestRackawareEnsemblePlacementPolicy extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestRackawareEnsemblePlacementPolicy.class);

    RackawareEnsemblePlacementPolicy repp;
    final List<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
    DistributionSchedule.WriteSet writeSet = DistributionSchedule.NULL_WRITE_SET;
    ClientConfiguration conf = new ClientConfiguration();
    BookieSocketAddress addr1, addr2, addr3, addr4;
    io.netty.util.HashedWheelTimer timer;
    final int minNumRacksPerWriteQuorumConfValue = 2;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        StaticDNSResolver.reset();
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(),
                NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack("127.0.0.1", NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack("localhost", NetworkTopology.DEFAULT_REGION_AND_RACK);
        LOG.info("Set up static DNS Resolver.");
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        conf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION + "/rack1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), NetworkTopology.DEFAULT_REGION + "/rack2");
        ensemble.add(addr1);
        ensemble.add(addr2);
        ensemble.add(addr3);
        ensemble.add(addr4);
        writeSet = writeSetFromValues(0, 1, 2, 3);

        timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getTimeoutTimerNumTicks());

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);
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

    static void updateMyRack(String rack) throws Exception {
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), rack);
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostName(), rack);
        StaticDNSResolver.addNodeToRack("127.0.0.1", rack);
        StaticDNSResolver.addNodeToRack("localhost", rack);
    }

    @Test
    public void testNodeDown() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

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
                ensemble, getBookiesHealthInfo(),
                writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(1, 2, 3, 0);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeReadOnly() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

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
        LOG.info("reorder set : {}", reorderSet);
        assertEquals(reorderSet, origWriteSet);
    }

    @Test
    public void testNodeSlow() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

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
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(1, 2, 3, 0);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testTwoNodesSlow() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

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
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(2, 3, 0, 1);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testTwoNodesDown() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

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
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(2, 3, 0, 1);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeDownAndReadOnly() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        addrs.remove(addr2);
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        roAddrs.add(addr2);
        repp.onClusterChanged(addrs, roAddrs);
        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(2, 3, 1, 0);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeDownAndNodeSlow() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

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
        repp.onClusterChanged(addrs, new HashSet<>());

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(2, 3, 0, 1);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    @Test
    public void testNodeDownAndReadOnlyAndNodeSlow() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        addrs.remove(addr2);
        Set<BookieSocketAddress> ro = new HashSet<BookieSocketAddress>();
        ro.add(addr2);
        repp.registerSlowBookie(addr3, 0L);
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr3, 1L);
        addrs.remove(addr2);
        repp.onClusterChanged(addrs, ro);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(3, 1, 2, 0);
        LOG.info("reorder set : {}", reorderSet);
        assertFalse(reorderSet.equals(origWriteSet));
        assertEquals(expectedSet, reorderSet);
    }

    /*
     * Tests the reordering of the writeSet based on number of pending requests.
     * Expect the third bookie to be placed first since its number of pending requests
     * is READ_REORDER_THRESHOLD_PENDING_REQUESTS=10 less than the originally first bookie.
     */
    @Test
    public void testPendingRequestsReorder() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        ClientConfiguration conf = (ClientConfiguration) this.conf.clone();
        conf.setReorderThresholdPendingRequests(10);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1, 20L);
        bookiePendingMap.put(addr2, 7L);
        bookiePendingMap.put(addr3, 1L); // best bookie -> this one first
        bookiePendingMap.put(addr4, 5L);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(2, 0, 1, 3);
        LOG.info("reorder set : {}", reorderSet);
        assertEquals("expect bookie idx 2 first", expectedSet, reorderSet);
    }

    /*
     * Tests the reordering of the writeSet based on number of pending requests for
     * an ensemble that is larger than the writeSet.
     * Expect the sixth bookie to be placed first since its number of pending requests
     * is READ_REORDER_THRESHOLD_PENDING_REQUESTS=10 less than the originally first bookie.
     */
    @Test
    public void testPendingRequestsReorderLargeEnsemble() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        ClientConfiguration conf = (ClientConfiguration) this.conf.clone();
        conf.setReorderThresholdPendingRequests(10);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1, 1L); // not in write set
        bookiePendingMap.put(addr2, 20L);
        bookiePendingMap.put(addr3, 0L); // not in write set
        bookiePendingMap.put(addr4, 12L);
        bookiePendingMap.put(addr5, 9L); // not in write set
        bookiePendingMap.put(addr6, 2L); // best bookie -> this one first
        bookiePendingMap.put(addr7, 10L);
        List<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
        ensemble.add(addr1);
        ensemble.add(addr2);
        ensemble.add(addr3);
        ensemble.add(addr4);
        ensemble.add(addr5);
        ensemble.add(addr6);
        ensemble.add(addr7);

        DistributionSchedule.WriteSet writeSet = writeSetFromValues(1, 3, 5, 6);
        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        DistributionSchedule.WriteSet expectedSet = writeSetFromValues(5, 1, 3, 6);
        LOG.info("reorder set : {}", reorderSet);
        assertEquals("expect bookie idx 5 first", expectedSet, reorderSet);
    }

    /*
     * Tests the reordering of the writeSet based on number of pending requests.
     * Expect no reordering in this case since the currently first bookie's number of
     * pending requests is less than READ_REORDER_THRESHOLD_PENDING_REQUESTS=10 lower
     * than the best bookie.
     */
    @Test
    public void testPendingRequestsNoReorder1() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        ClientConfiguration conf = (ClientConfiguration) this.conf.clone();
        conf.setReorderThresholdPendingRequests(10);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1, 10L); // -> this one first
        bookiePendingMap.put(addr2, 7L);
        bookiePendingMap.put(addr3, 1L); // best bookie, but below threshold
        bookiePendingMap.put(addr4, 5L);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        LOG.info("reorder set : {}", reorderSet);
        assertEquals("writeSet should be in original order", origWriteSet, reorderSet);
    }

    /*
     * Tests the reordering of the writeSet based on number of pending requests.
     * Expect no reordering in this case since the currently first bookie's number of
     * pending requests is lowest among all bookies already.
     */
    @Test
    public void testPendingRequestsNoReorder2() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        ClientConfiguration conf = (ClientConfiguration) this.conf.clone();
        conf.setReorderThresholdPendingRequests(10);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        Map<BookieSocketAddress, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1, 1L); // -> this one first
        bookiePendingMap.put(addr2, 7L);
        bookiePendingMap.put(addr3, 1L);
        bookiePendingMap.put(addr4, 5L);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        LOG.info("reorder set : {}", reorderSet);
        assertEquals("writeSet should be in original order", origWriteSet, reorderSet);
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInSameRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        EnsemblePlacementPolicy.PlacementResult<BookieSocketAddress> replaceBookieResponse =
            repp.replaceBookie(1, 1, 1, null, new ArrayList<>(), addr2, new HashSet<>());
        BookieSocketAddress replacedBookie = replaceBookieResponse.getResult();
        boolean isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isStrictlyAdheringToPolicy();
        assertEquals(addr3, replacedBookie);
        assertTrue(isEnsembleAdheringToPlacementPolicy);
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInDifferentRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r4");
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
        EnsemblePlacementPolicy.PlacementResult<BookieSocketAddress> replaceBookieResponse =
            repp.replaceBookie(1, 1, 1, null, new ArrayList<>(), addr2, excludedAddrs);
        BookieSocketAddress replacedBookie = replaceBookieResponse.getResult();
        boolean isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isStrictlyAdheringToPolicy();
        assertFalse(addr1.equals(replacedBookie));
        assertTrue(addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
        assertTrue(isEnsembleAdheringToPlacementPolicy);
    }

    @Test
    public void testReplaceBookieWithNotEnoughBookies() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r4");
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
    public void testReplaceBookieWithEnoughBookiesInSameRackAsEnsemble() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        List<BookieSocketAddress> ensembleBookies = new ArrayList<BookieSocketAddress>();
        ensembleBookies.add(addr2);
        ensembleBookies.add(addr4);
        EnsemblePlacementPolicy.PlacementResult<BookieSocketAddress> replaceBookieResponse = repp.replaceBookie(
            1, 1, 1 , null,
            ensembleBookies,
            addr4,
            new HashSet<>());
        BookieSocketAddress replacedBookie = replaceBookieResponse.getResult();
        boolean isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isStrictlyAdheringToPolicy();
        assertEquals(addr1, replacedBookie);
        assertTrue(isEnsembleAdheringToPlacementPolicy);
    }

    @Test
    public void testNewEnsembleWithSingleRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.9", 3181);
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse;
            ensembleResponse = repp.newEnsemble(3, 2, 2, null, new HashSet<>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getResult();
            boolean isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
            assertEquals(0, getNumCoveredWriteQuorums(ensemble, 2, conf.getMinNumRacksPerWriteQuorum()));
            assertFalse(isEnsembleAdheringToPlacementPolicy);
            EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse2;
            ensembleResponse2 = repp.newEnsemble(4, 2, 2, null, new HashSet<>());
            List<BookieSocketAddress> ensemble2 = ensembleResponse2.getResult();
            boolean isEnsembleAdheringToPlacementPolicy2 = ensembleResponse2.isStrictlyAdheringToPolicy();
            assertEquals(0, getNumCoveredWriteQuorums(ensemble2, 2, conf.getMinNumRacksPerWriteQuorum()));
            assertFalse(isEnsembleAdheringToPlacementPolicy2);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testSingleRackWithEnforceMinNumRacks() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);

        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(2);
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        List<BookieSocketAddress> ensemble;
        try {
            ensemble = repp.newEnsemble(3, 2, 2, null, new HashSet<>()).getResult();
            fail("Should get not enough bookies exception since there is only one rack.");
        } catch (BKNotEnoughBookiesException bnebe) {
        }

        try {
            ensemble = repp.newEnsemble(3, 2, 2, new HashSet<>(),
                    EnsembleForReplacementWithNoConstraints.INSTANCE, TruePredicate.INSTANCE).getResult();
            fail("Should get not enough bookies exception since there is only one rack.");
        } catch (BKNotEnoughBookiesException bnebe) {
        }
    }

    @Test
    public void testNewEnsembleWithEnforceMinNumRacks() throws Exception {
        String defaultRackForThisTest = NetworkTopology.DEFAULT_REGION_AND_RACK;
        repp.uninitalize();
        updateMyRack(defaultRackForThisTest);

        int minNumRacksPerWriteQuorum = 4;
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        // set enforceMinNumRacksPerWriteQuorum
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger("");
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, statsLogger);
        repp.withDefaultRack(defaultRackForThisTest);
        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        BookieSocketAddress[] bookieSocketAddresses = new BookieSocketAddress[numOfRacks * numOfBookiesPerRack];

        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieSocketAddresses[index] = new BookieSocketAddress("128.0.0." + index, 3181);
                StaticDNSResolver.addNodeToRack(bookieSocketAddresses[index].getHostName(), "/default-region/r" + i);
            }
        }

        int numOfBookiesInDefaultRack = 5;
        BookieSocketAddress[] bookieSocketAddressesInDefaultRack = new BookieSocketAddress[numOfBookiesInDefaultRack];
        for (int i = 0; i < numOfBookiesInDefaultRack; i++) {
            bookieSocketAddressesInDefaultRack[i] = new BookieSocketAddress("128.0.0." + (100 + i), 3181);
            StaticDNSResolver.addNodeToRack(bookieSocketAddressesInDefaultRack[i].getHostName(),
                    defaultRackForThisTest);
        }

        List<BookieSocketAddress> nonDefaultRackBookiesList = Arrays.asList(bookieSocketAddresses);
        List<BookieSocketAddress> defaultRackBookiesList = Arrays.asList(bookieSocketAddressesInDefaultRack);
        Set<BookieSocketAddress> writableBookies = new HashSet<BookieSocketAddress>(nonDefaultRackBookiesList);
        writableBookies.addAll(defaultRackBookiesList);
        repp.onClusterChanged(writableBookies, new HashSet<BookieSocketAddress>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());

        try {
            // this newEnsemble call will exclude default rack bookies
            repp.newEnsemble(8, 4, 4, null, new HashSet<>());
            fail("Should get not enough bookies exception since there are only 3 non-default racks");
        } catch (BKNotEnoughBookiesException bnebe) {
        }

        try {
            repp.newEnsemble(8, 4, 4, new HashSet<>(defaultRackBookiesList),
                    EnsembleForReplacementWithNoConstraints.INSTANCE, TruePredicate.INSTANCE);
            fail("Should get not enough bookies exception since there are only 3 non-default racks"
                    + " and defaultrack bookies are excluded");
        } catch (BKNotEnoughBookiesException bnebe) {
        }

        /*
         * Though minNumRacksPerWriteQuorum is set to 4, since writeQuorum is 3
         * and there are enough bookies in 3 racks, this newEnsemble calls
         * should succeed.
         */
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse;
        List<BookieSocketAddress> ensemble;
        boolean isEnsembleAdheringToPlacementPolicy;
        int ensembleSize = numOfRacks * numOfBookiesPerRack;
        int writeQuorumSize = numOfRacks;
        int ackQuorumSize = numOfRacks;

        ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
        ensemble = ensembleResponse.getResult();
        isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
        assertEquals("Number of writeQuorum sets covered", ensembleSize,
                getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum()));
        assertTrue(isEnsembleAdheringToPlacementPolicy);

        ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize,
                new HashSet<>(defaultRackBookiesList), EnsembleForReplacementWithNoConstraints.INSTANCE,
                TruePredicate.INSTANCE);
        ensemble = ensembleResponse.getResult();
        isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
        assertEquals("Number of writeQuorum sets covered", ensembleSize,
                getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum()));
        assertTrue(isEnsembleAdheringToPlacementPolicy);
    }

    @Test
    public void testNewEnsembleWithSufficientRacksAndEnforceMinNumRacks() throws Exception {
        repp.uninitalize();

        int minNumRacksPerWriteQuorum = 4;
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        // set enforceMinNumRacksPerWriteQuorum
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int writeQuorumSize = 3;
        int ackQuorumSize = 3;
        int effectiveMinNumRacksPerWriteQuorum = Math.min(minNumRacksPerWriteQuorum, writeQuorumSize);

        int numOfRacks = 2 * effectiveMinNumRacksPerWriteQuorum - 1;
        int numOfBookiesPerRack = 20;
        BookieSocketAddress[] bookieSocketAddresses = new BookieSocketAddress[numOfRacks * numOfBookiesPerRack];

        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieSocketAddresses[index] = new BookieSocketAddress("128.0.0." + index, 3181);
                StaticDNSResolver.addNodeToRack(bookieSocketAddresses[index].getHostName(), "/default-region/r" + i);
            }
        }

        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        repp.onClusterChanged(new HashSet<BookieSocketAddress>(Arrays.asList(bookieSocketAddresses)),
                new HashSet<BookieSocketAddress>());

        /*
         * in this scenario we have enough number of racks (2 *
         * effectiveMinNumRacksPerWriteQuorum - 1) and more number of bookies in
         * each rack. So we should be able to create ensemble for all
         * ensembleSizes (as long as there are enough number of bookies in each
         * rack).
         */
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse;
        List<BookieSocketAddress> ensemble;
        boolean isEnsembleAdheringToPlacementPolicy;
        for (int ensembleSize = effectiveMinNumRacksPerWriteQuorum; ensembleSize < 40; ensembleSize++) {
            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
            ensemble = ensembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
            assertEquals("Number of writeQuorum sets covered", ensembleSize,
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum()));
            assertTrue(isEnsembleAdheringToPlacementPolicy);

            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, new HashSet<>(),
                    EnsembleForReplacementWithNoConstraints.INSTANCE, TruePredicate.INSTANCE);
            ensemble = ensembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
            assertEquals("Number of writeQuorum sets covered", ensembleSize,
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum()));
            assertTrue(isEnsembleAdheringToPlacementPolicy);
        }
    }

    @Test
    public void testReplaceBookieWithEnforceMinNumRacks() throws Exception {
        String defaultRackForThisTest = NetworkTopology.DEFAULT_REGION_AND_RACK;
        repp.uninitalize();
        updateMyRack(defaultRackForThisTest);

        int minNumRacksPerWriteQuorum = 4;
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        // set enforceMinNumRacksPerWriteQuorum
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger("");
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                statsLogger);
        repp.withDefaultRack(defaultRackForThisTest);
        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        Set<BookieSocketAddress> bookieSocketAddresses = new HashSet<BookieSocketAddress>();
        Map<BookieSocketAddress, String> bookieRackMap = new HashMap<BookieSocketAddress, String>();
        BookieSocketAddress bookieAddress;
        String rack;
        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181);
                rack = "/default-region/r" + i;
                StaticDNSResolver.addNodeToRack(bookieAddress.getHostName(), rack);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rack);
            }
        }
        /*
         * bookies in this default rack should not be returned for replacebookie
         * response.
         */
        int numOfBookiesInDefaultRack = 5;
        BookieSocketAddress[] bookieSocketAddressesInDefaultRack = new BookieSocketAddress[numOfBookiesInDefaultRack];
        for (int i = 0; i < numOfBookiesInDefaultRack; i++) {
            bookieSocketAddressesInDefaultRack[i] = new BookieSocketAddress("127.0.0." + (i + 100), 3181);
            StaticDNSResolver.addNodeToRack(bookieSocketAddressesInDefaultRack[i].getHostName(),
                    defaultRackForThisTest);
        }

        Set<BookieSocketAddress> nonDefaultRackBookiesList = bookieSocketAddresses;
        List<BookieSocketAddress> defaultRackBookiesList = Arrays.asList(bookieSocketAddressesInDefaultRack);
        Set<BookieSocketAddress> writableBookies = new HashSet<BookieSocketAddress>(nonDefaultRackBookiesList);
        writableBookies.addAll(defaultRackBookiesList);
        repp.onClusterChanged(writableBookies, new HashSet<BookieSocketAddress>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());

        /*
         * Though minNumRacksPerWriteQuorum is set to 4, since writeQuorum is 3
         * and there are enough bookies in 3 racks, this newEnsemble call should
         * succeed.
         */
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse;
        List<BookieSocketAddress> ensemble;
        int ensembleSize = numOfRacks * numOfBookiesPerRack;
        int writeQuorumSize = numOfRacks;
        int ackQuorumSize = numOfRacks;

        ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
        ensemble = ensembleResponse.getResult();

        BookieSocketAddress bookieInEnsembleToBeReplaced = ensemble.get(7);
        // get rack of some other bookie
        String rackOfOtherBookieInEnsemble = bookieRackMap.get(ensemble.get(8));
        BookieSocketAddress newBookieAddress1 = new BookieSocketAddress("128.0.0.100", 3181);
        /*
         * add the newBookie to the rack of some other bookie in the current
         * ensemble
         */
        StaticDNSResolver.addNodeToRack(newBookieAddress1.getHostName(), rackOfOtherBookieInEnsemble);
        bookieSocketAddresses.add(newBookieAddress1);
        writableBookies.add(newBookieAddress1);
        bookieRackMap.put(newBookieAddress1, rackOfOtherBookieInEnsemble);

        repp.onClusterChanged(writableBookies, new HashSet<BookieSocketAddress>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());
        try {
            repp.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, null,
                    ensemble, bookieInEnsembleToBeReplaced, new HashSet<>());
            fail("Should get not enough bookies exception since there are no more bookies in rack"
                    + "of 'bookieInEnsembleToReplace'"
                    + "and new bookie added belongs to the rack of some other bookie in the ensemble");
        } catch (BKNotEnoughBookiesException bnebe) {
            // this is expected
        }

        String newRack = "/default-region/r100";
        BookieSocketAddress newBookieAddress2 = new BookieSocketAddress("128.0.0.101", 3181);
        /*
         * add the newBookie to a new rack.
         */
        StaticDNSResolver.addNodeToRack(newBookieAddress2.getHostName(), newRack);
        bookieSocketAddresses.add(newBookieAddress2);
        writableBookies.add(newBookieAddress2);
        bookieRackMap.put(newBookieAddress2, newRack);

        repp.onClusterChanged(writableBookies, new HashSet<BookieSocketAddress>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());
        /*
         * this replaceBookie should succeed, because a new bookie is added to a
         * new rack.
         */
        EnsemblePlacementPolicy.PlacementResult<BookieSocketAddress> replaceBookieResponse;
        BookieSocketAddress replacedBookieAddress;
        boolean isEnsembleAdheringToPlacementPolicy;
        replaceBookieResponse = repp.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, null, ensemble,
                bookieInEnsembleToBeReplaced, new HashSet<>());
        replacedBookieAddress = replaceBookieResponse.getResult();
        isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isStrictlyAdheringToPolicy();
        assertEquals("It should be newBookieAddress2", newBookieAddress2, replacedBookieAddress);
        assertTrue(isEnsembleAdheringToPlacementPolicy);

        Set<BookieSocketAddress> bookiesToExclude = new HashSet<>();
        bookiesToExclude.add(newBookieAddress2);
        repp.onClusterChanged(writableBookies, new HashSet<BookieSocketAddress>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());
        try {
            repp.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, null, ensemble,
                    bookieInEnsembleToBeReplaced, bookiesToExclude);
            fail("Should get not enough bookies exception since the only available bookie to replace"
                    + "is added to excludedBookies list");
        } catch (BKNotEnoughBookiesException bnebe) {
            // this is expected
        }

        // get rack of the bookie to be replaced
        String rackOfBookieToBeReplaced = bookieRackMap.get(bookieInEnsembleToBeReplaced);
        BookieSocketAddress newBookieAddress3 = new BookieSocketAddress("128.0.0.102", 3181);
        /*
         * add the newBookie to rack of the bookie to be replaced.
         */
        StaticDNSResolver.addNodeToRack(newBookieAddress3.getHostName(), rackOfBookieToBeReplaced);
        bookieSocketAddresses.add(newBookieAddress3);
        writableBookies.add(newBookieAddress3);
        bookieRackMap.put(newBookieAddress3, rackOfBookieToBeReplaced);

        repp.onClusterChanged(writableBookies, new HashSet<BookieSocketAddress>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());
        /*
         * here we have added new bookie to the rack of the bookie to be
         * replaced, so we should be able to replacebookie though
         * newBookieAddress2 is added to excluded bookies list.
         */
        replaceBookieResponse = repp.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, null,
                ensemble, bookieInEnsembleToBeReplaced, bookiesToExclude);
        replacedBookieAddress = replaceBookieResponse.getResult();
        isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isStrictlyAdheringToPolicy();
        assertEquals("It should be newBookieAddress3", newBookieAddress3, replacedBookieAddress);
        assertTrue(isEnsembleAdheringToPlacementPolicy);
    }

    @Test
    public void testSelectBookieFromNetworkLoc() throws Exception {
        repp.uninitalize();

        int minNumRacksPerWriteQuorum = 4;
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        // set enforceMinNumRacksPerWriteQuorum
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        String[] rackLocationNames = new String[numOfRacks];
        List<BookieSocketAddress> bookieSocketAddresses = new ArrayList<BookieSocketAddress>();
        Map<BookieSocketAddress, String> bookieRackMap = new HashMap<BookieSocketAddress, String>();
        BookieSocketAddress bookieAddress;

        for (int i = 0; i < numOfRacks; i++) {
            rackLocationNames[i] = "/default-region/r" + i;
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181);
                StaticDNSResolver.addNodeToRack(bookieAddress.getHostName(), rackLocationNames[i]);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rackLocationNames[i]);
            }
        }
        String nonExistingRackLocation = "/default-region/r25";

        repp.onClusterChanged(new HashSet<BookieSocketAddress>(bookieSocketAddresses),
                new HashSet<BookieSocketAddress>());

        String rack = bookieRackMap.get(bookieSocketAddresses.get(0));
        BookieNode bookieNode = repp.selectFromNetworkLocation(rack, new HashSet<Node>(), TruePredicate.INSTANCE,
                EnsembleForReplacementWithNoConstraints.INSTANCE, false);
        String recRack = bookieNode.getNetworkLocation();
        assertEquals("Rack of node", rack, recRack);

        try {
            repp.selectFromNetworkLocation(nonExistingRackLocation, new HashSet<Node>(), TruePredicate.INSTANCE,
                    EnsembleForReplacementWithNoConstraints.INSTANCE, false);
            fail("Should get not enough bookies exception since there are no bookies in this rack");
        } catch (BKNotEnoughBookiesException bnebe) {
            // this is expected
        }

        // it should not fail, since fallback is set to true and it should pick
        // some random one
        repp.selectFromNetworkLocation(nonExistingRackLocation, new HashSet<Node>(), TruePredicate.INSTANCE,
                EnsembleForReplacementWithNoConstraints.INSTANCE, true);

        Set<BookieSocketAddress> excludeBookiesOfRackR0 = new HashSet<BookieSocketAddress>();
        for (int i = 0; i < numOfBookiesPerRack; i++) {
            excludeBookiesOfRackR0.add(bookieSocketAddresses.get(i));
        }

        Set<Node> excludeBookieNodesOfRackR0 = repp.convertBookiesToNodes(excludeBookiesOfRackR0);
        try {
            repp.selectFromNetworkLocation(bookieRackMap.get(bookieSocketAddresses.get(0)), excludeBookieNodesOfRackR0,
                    TruePredicate.INSTANCE, EnsembleForReplacementWithNoConstraints.INSTANCE, false);
            fail("Should get not enough bookies exception since all the bookies in r0 are added to the exclusion list");
        } catch (BKNotEnoughBookiesException bnebe) {
            // this is expected
        }

        // not expected to get exception since fallback is set to true
        bookieNode = repp.selectFromNetworkLocation(bookieRackMap.get(bookieSocketAddresses.get(0)),
                excludeBookieNodesOfRackR0, TruePredicate.INSTANCE, EnsembleForReplacementWithNoConstraints.INSTANCE,
                true);
        assertTrue("BookieNode should not be from Rack /r0" + bookieNode.getNetworkLocation(),
                rackLocationNames[1].equals(bookieNode.getNetworkLocation())
                        || rackLocationNames[2].equals(bookieNode.getNetworkLocation()));
    }

    @Test
    public void testSelectBookieFromExcludingRacks() throws Exception {
        repp.uninitalize();

        int minNumRacksPerWriteQuorum = 4;
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        // set enforceMinNumRacksPerWriteQuorum
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        String[] rackLocationNames = new String[numOfRacks];
        List<BookieSocketAddress> bookieSocketAddresses = new ArrayList<BookieSocketAddress>();
        Map<BookieSocketAddress, String> bookieRackMap = new HashMap<BookieSocketAddress, String>();
        BookieSocketAddress bookieAddress;

        for (int i = 0; i < numOfRacks; i++) {
            rackLocationNames[i] = "/default-region/r" + i;
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181);
                StaticDNSResolver.addNodeToRack(bookieAddress.getHostName(), rackLocationNames[i]);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rackLocationNames[i]);
            }
        }

        repp.onClusterChanged(new HashSet<BookieSocketAddress>(bookieSocketAddresses),
                new HashSet<BookieSocketAddress>());

        Set<BookieSocketAddress> excludeBookiesOfRackR0 = new HashSet<BookieSocketAddress>();
        for (int i = 0; i < numOfBookiesPerRack; i++) {
            excludeBookiesOfRackR0.add(bookieSocketAddresses.get(i));
        }

        Set<Node> excludeBookieNodesOfRackR0 = repp.convertBookiesToNodes(excludeBookiesOfRackR0);

        Set<String> excludeRacksRackR1AndR2 = new HashSet<String>();
        excludeRacksRackR1AndR2.add(rackLocationNames[1]);
        excludeRacksRackR1AndR2.add(rackLocationNames[2]);

        try {
            repp.selectFromNetworkLocation(excludeRacksRackR1AndR2, excludeBookieNodesOfRackR0, TruePredicate.INSTANCE,
                    EnsembleForReplacementWithNoConstraints.INSTANCE, false);
            fail("Should get not enough bookies exception racks R1 and R2 are"
                    + "excluded and all the bookies in r0 are added to the exclusion list");
        } catch (BKNotEnoughBookiesException bnebe) {
            // this is expected
        }

        BookieNode bookieNode = repp.selectFromNetworkLocation(excludeRacksRackR1AndR2, new HashSet<Node>(),
                TruePredicate.INSTANCE, EnsembleForReplacementWithNoConstraints.INSTANCE, false);
        assertTrue("BookieNode should be from Rack /r0" + bookieNode.getNetworkLocation(),
                rackLocationNames[0].equals(bookieNode.getNetworkLocation()));

        // not expected to get exception since fallback is set to true
        bookieNode = repp.selectFromNetworkLocation(excludeRacksRackR1AndR2, excludeBookieNodesOfRackR0,
                TruePredicate.INSTANCE, EnsembleForReplacementWithNoConstraints.INSTANCE, true);
        assertTrue("BookieNode should not be from Rack /r0" + bookieNode.getNetworkLocation(),
                rackLocationNames[1].equals(bookieNode.getNetworkLocation())
                        || rackLocationNames[2].equals(bookieNode.getNetworkLocation()));
    }

    @Test
    public void testSelectBookieFromNetworkLocAndExcludingRacks() throws Exception {
        repp.uninitalize();

        int minNumRacksPerWriteQuorum = 4;
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        // set enforceMinNumRacksPerWriteQuorum
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        String[] rackLocationNames = new String[numOfRacks];
        List<BookieSocketAddress> bookieSocketAddresses = new ArrayList<BookieSocketAddress>();
        Map<BookieSocketAddress, String> bookieRackMap = new HashMap<BookieSocketAddress, String>();
        BookieSocketAddress bookieAddress;

        for (int i = 0; i < numOfRacks; i++) {
            rackLocationNames[i] = "/default-region/r" + i;
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181);
                StaticDNSResolver.addNodeToRack(bookieAddress.getHostName(), rackLocationNames[i]);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rackLocationNames[i]);
            }
        }
        String nonExistingRackLocation = "/default-region/r25";

        repp.onClusterChanged(new HashSet<BookieSocketAddress>(bookieSocketAddresses),
                new HashSet<BookieSocketAddress>());

        Set<BookieSocketAddress> excludeBookiesOfRackR0 = new HashSet<BookieSocketAddress>();
        for (int i = 0; i < numOfBookiesPerRack; i++) {
            excludeBookiesOfRackR0.add(bookieSocketAddresses.get(i));
        }

        Set<Node> excludeBookieNodesOfRackR0 = repp.convertBookiesToNodes(excludeBookiesOfRackR0);

        Set<String> excludeRacksRackR1AndR2 = new HashSet<String>();
        excludeRacksRackR1AndR2.add(rackLocationNames[1]);
        excludeRacksRackR1AndR2.add(rackLocationNames[2]);

        try {
            repp.selectFromNetworkLocation(nonExistingRackLocation, excludeRacksRackR1AndR2,
                    excludeBookieNodesOfRackR0,
                    TruePredicate.INSTANCE, EnsembleForReplacementWithNoConstraints.INSTANCE, false);
            fail("Should get not enough bookies exception racks R1 and R2 are excluded and all the bookies in"
                    + "r0 are added to the exclusion list");
        } catch (BKNotEnoughBookiesException bnebe) {
            // this is expected
        }

        BookieNode bookieNode = repp.selectFromNetworkLocation(rackLocationNames[0], excludeRacksRackR1AndR2,
                new HashSet<Node>(), TruePredicate.INSTANCE, EnsembleForReplacementWithNoConstraints.INSTANCE, false);
        assertTrue("BookieNode should be from Rack /r0" + bookieNode.getNetworkLocation(),
                rackLocationNames[0].equals(bookieNode.getNetworkLocation()));

        bookieNode = repp.selectFromNetworkLocation(rackLocationNames[0], new HashSet<String>(),
                excludeBookieNodesOfRackR0, TruePredicate.INSTANCE,
                EnsembleForReplacementWithNoConstraints.INSTANCE, false);
        assertTrue("BookieNode should not be from Rack /r0" + bookieNode.getNetworkLocation(),
                rackLocationNames[1].equals(bookieNode.getNetworkLocation())
                        || rackLocationNames[2].equals(bookieNode.getNetworkLocation()));

        bookieNode = repp.selectFromNetworkLocation(nonExistingRackLocation, excludeRacksRackR1AndR2,
                excludeBookieNodesOfRackR0, TruePredicate.INSTANCE, EnsembleForReplacementWithNoConstraints.INSTANCE,
                true);
        assertTrue("BookieNode should not be from Rack /r0" + bookieNode.getNetworkLocation(),
                rackLocationNames[1].equals(bookieNode.getNetworkLocation())
                        || rackLocationNames[2].equals(bookieNode.getNetworkLocation()));
    }

    @Test
    public void testSelectBookieByExcludingRacksAndBookies() throws Exception {
        repp.uninitalize();

        int minNumRacksPerWriteQuorum = 4;
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        // set enforceMinNumRacksPerWriteQuorum
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        /*
         * Durability is enforced
         *
         * When durability is being enforced; we must not violate the predicate
         * even when selecting a random bookie; as durability guarantee is not
         * best effort; correctness is implied by it
         */
        repp = new RackawareEnsemblePlacementPolicy(true);
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        String[] rackLocationNames = new String[numOfRacks];
        List<BookieSocketAddress> bookieSocketAddresses = new ArrayList<BookieSocketAddress>();
        Map<BookieSocketAddress, String> bookieRackMap = new HashMap<BookieSocketAddress, String>();
        BookieSocketAddress bookieAddress;

        for (int i = 0; i < numOfRacks; i++) {
            rackLocationNames[i] = "/default-region/r" + i;
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181);
                StaticDNSResolver.addNodeToRack(bookieAddress.getHostName(), rackLocationNames[i]);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rackLocationNames[i]);
            }
        }

        repp.onClusterChanged(new HashSet<BookieSocketAddress>(bookieSocketAddresses),
                new HashSet<BookieSocketAddress>());

        Set<BookieSocketAddress> excludeBookiesOfRackR0 = new HashSet<BookieSocketAddress>();
        for (int i = 0; i < numOfBookiesPerRack; i++) {
            excludeBookiesOfRackR0.add(bookieSocketAddresses.get(i));
        }

        Set<Node> excludeBookieNodesOfRackR0 = repp.convertBookiesToNodes(excludeBookiesOfRackR0);

        Set<String> excludeRackR1 = new HashSet<String>();
        excludeRackR1.add(rackLocationNames[1]);

        BookieNode nodeSelected;
        nodeSelected = repp.selectFromNetworkLocation(excludeRackR1, excludeBookieNodesOfRackR0, TruePredicate.INSTANCE,
                EnsembleForReplacementWithNoConstraints.INSTANCE, false);
        assertEquals("BookieNode should be from Rack2", rackLocationNames[2], nodeSelected.getNetworkLocation());

        try {
            /*
             * durability is enforced, so false predicate will reject all
             * bookies.
             */
            repp.selectFromNetworkLocation(excludeRackR1, excludeBookieNodesOfRackR0, (candidate, chosenBookies) -> {
                return false;
            }, EnsembleForReplacementWithNoConstraints.INSTANCE, false);
            fail("Should get not enough bookies exception since we are using false predicate");
        } catch (BKNotEnoughBookiesException bnebe) {
            // this is expected
        }

        try {
            /*
             * Using ensemble which rejects all the nodes.
             */
            repp.selectFromNetworkLocation(excludeRackR1, excludeBookieNodesOfRackR0, TruePredicate.INSTANCE,
                    new Ensemble<BookieNode>() {

                        @Override
                        public boolean addNode(BookieNode node) {
                            return false;
                        }

                        @Override
                        public List<BookieSocketAddress> toList() {
                            return null;
                        }

                        @Override
                        public boolean validate() {
                            return false;
                        }

                    }, false);
            fail("Should get not enough bookies exception since ensemble rejects all the nodes");
        } catch (BKNotEnoughBookiesException bnebe) {
            // this is expected
        }
    }

    @Test
    public void testNewEnsembleWithMultipleRacks() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r2");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            int ensembleSize = 3;
            int writeQuorumSize = 2;
            int acqQuorumSize = 2;
            EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse =
                repp.newEnsemble(ensembleSize, writeQuorumSize,
                                 acqQuorumSize, null, new HashSet<>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getResult();
            boolean isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
            int numCovered = getNumCoveredWriteQuorums(ensemble, writeQuorumSize, conf.getMinNumRacksPerWriteQuorum());
            assertTrue(numCovered >= 1 && numCovered < 3);
            assertFalse(isEnsembleAdheringToPlacementPolicy);
            ensembleSize = 4;
            EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse2 =
                repp.newEnsemble(ensembleSize, writeQuorumSize,
                                 acqQuorumSize, null, new HashSet<>());
            List<BookieSocketAddress> ensemble2 = ensembleResponse2.getResult();
            boolean isEnsembleAdheringToPlacementPolicy2 = ensembleResponse2.isStrictlyAdheringToPolicy();
            numCovered = getNumCoveredWriteQuorums(ensemble2, writeQuorumSize, conf.getMinNumRacksPerWriteQuorum());
            assertTrue(numCovered >= 1 && numCovered < 3);
            assertFalse(isEnsembleAdheringToPlacementPolicy2);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testMinNumRacksPerWriteQuorumOfRacks() throws Exception {
        int numOfRacksToCreate = 6;
        int numOfNodesInEachRack = 5;

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        BookieSocketAddress addr;
        for (int i = 0; i < numOfRacksToCreate; i++) {
            for (int j = 0; j < numOfNodesInEachRack; j++) {
                addr = new BookieSocketAddress("128.0.0." + ((i * numOfNodesInEachRack) + j), 3181);
                // update dns mapping
                StaticDNSResolver.addNodeToRack(addr.getHostName(), "/default-region/r" + i);
                addrs.add(addr);
            }
        }

        try {
            ClientConfiguration newConf = new ClientConfiguration(conf);
            // set MinNumRacksPerWriteQuorum to 4
            int minNumRacksPerWriteQuorum = 4;
            int ensembleSize = 12;
            int writeQuorumSize = 6;
            validateNumOfWriteQuorumsCoveredInEnsembleCreation(addrs, minNumRacksPerWriteQuorum, ensembleSize,
                    writeQuorumSize);

            // set MinNumRacksPerWriteQuorum to 6
            newConf = new ClientConfiguration(conf);
            minNumRacksPerWriteQuorum = 6;
            ensembleSize = 6;
            writeQuorumSize = 6;
            validateNumOfWriteQuorumsCoveredInEnsembleCreation(addrs, minNumRacksPerWriteQuorum, ensembleSize,
                    writeQuorumSize);

            // set MinNumRacksPerWriteQuorum to 6
            newConf = new ClientConfiguration(conf);
            minNumRacksPerWriteQuorum = 6;
            ensembleSize = 10;
            writeQuorumSize = ensembleSize;
            validateNumOfWriteQuorumsCoveredInEnsembleCreation(addrs, minNumRacksPerWriteQuorum, ensembleSize,
                    writeQuorumSize);

            // set MinNumRacksPerWriteQuorum to 5
            newConf = new ClientConfiguration(conf);
            minNumRacksPerWriteQuorum = 5;
            ensembleSize = 24;
            writeQuorumSize = 12;
            validateNumOfWriteQuorumsCoveredInEnsembleCreation(addrs, minNumRacksPerWriteQuorum, ensembleSize,
                    writeQuorumSize);

        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    void validateNumOfWriteQuorumsCoveredInEnsembleCreation(Set<BookieSocketAddress> addrs,
            int minNumRacksPerWriteQuorum, int ensembleSize, int writeQuorumSize) throws Exception {
        ClientConfiguration newConf = new ClientConfiguration(conf);
        newConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse =
            repp.newEnsemble(ensembleSize, writeQuorumSize,
                             writeQuorumSize, null, new HashSet<>());
        List<BookieSocketAddress> ensemble = ensembleResponse.getResult();
        boolean isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
        int numCovered = getNumCoveredWriteQuorums(ensemble, writeQuorumSize, minNumRacksPerWriteQuorum);
        assertEquals("minimum number of racks covered for writequorum ensemble: " + ensemble, ensembleSize, numCovered);
        assertTrue(isEnsembleAdheringToPlacementPolicy);
    }

    @Test
    public void testNewEnsembleWithEnoughRacks() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/default-region/r4");
        int availableNumOfRacks = 4;
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
            int ensembleSize = 3;
            int writeQuorumSize = 3;
            int ackQuorumSize = 2;
            EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse =
                repp.newEnsemble(ensembleSize, writeQuorumSize,
                                   ackQuorumSize, null, new HashSet<>());
            List<BookieSocketAddress> ensemble1 = ensembleResponse.getResult();
            boolean isEnsembleAdheringToPlacementPolicy1 = ensembleResponse.isStrictlyAdheringToPolicy();
            assertEquals(ensembleSize,
                    getNumCoveredWriteQuorums(ensemble1, writeQuorumSize, conf.getMinNumRacksPerWriteQuorum()));
            assertTrue(isEnsembleAdheringToPlacementPolicy1);
            ensembleSize = 4;
            writeQuorumSize = 4;
            EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse2 =
                repp.newEnsemble(ensembleSize, writeQuorumSize, 2, null, new HashSet<>());
            List<BookieSocketAddress> ensemble2 = ensembleResponse2.getResult();
            boolean isEnsembleAdheringToPlacementPolicy2 = ensembleResponse2.isStrictlyAdheringToPolicy();
            assertEquals(ensembleSize,
                    getNumCoveredWriteQuorums(ensemble2, writeQuorumSize, conf.getMinNumRacksPerWriteQuorum()));
            assertTrue(isEnsembleAdheringToPlacementPolicy2);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    /**
     * Test for BOOKKEEPER-633.
     */
    @Test
    public void testRemoveBookieFromCluster() {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
    }

    @Test
    public void testWeightedPlacementAndReplaceBookieWithEnoughBookiesInSameRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);

        int multiple = 10;
        conf.setDiskWeightBasedPlacementEnabled(true);
        conf.setBookieMaxWeightMultipleForWeightBasedPlacement(-1); // no max cap on weight
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        Map<BookieSocketAddress, BookieInfo> bookieInfoMap = new HashMap<BookieSocketAddress, BookieInfo>();
        bookieInfoMap.put(addr1, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4, new BookieInfo(multiple * 100L, multiple * 100L));
        repp.updateBookieInfo(bookieInfoMap);

        Map<BookieSocketAddress, Long> selectionCounts = new HashMap<BookieSocketAddress, Long>();
        selectionCounts.put(addr3, 0L);
        selectionCounts.put(addr4, 0L);
        int numTries = 50000;
        EnsemblePlacementPolicy.PlacementResult<BookieSocketAddress> replaceBookieResponse;
        boolean isEnsembleAdheringToPlacementPolicy;
        BookieSocketAddress replacedBookie;
        for (int i = 0; i < numTries; i++) {
            // replace node under r2
            replaceBookieResponse = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(), addr2, new HashSet<>());
            replacedBookie = replaceBookieResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isStrictlyAdheringToPolicy();
            assertTrue("replaced : " + replacedBookie, addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
            assertTrue(isEnsembleAdheringToPlacementPolicy);
            selectionCounts.put(replacedBookie, selectionCounts.get(replacedBookie) + 1);
        }
        double observedMultiple = ((double) selectionCounts.get(addr4) / (double) selectionCounts.get(addr3));
        assertTrue("Weights not being honored " + observedMultiple, Math.abs(observedMultiple - multiple) < 1);
    }

    @Test
    public void testWeightedPlacementAndReplaceBookieWithoutEnoughBookiesInSameRack() throws Exception {
        BookieSocketAddress addr0 = new BookieSocketAddress("126.0.0.1", 3181);
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.reset();
        StaticDNSResolver.addNodeToRack(addr0.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r0");
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r3");
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r4");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr0);
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);

        int multiple = 10, maxMultiple = 4;
        conf.setDiskWeightBasedPlacementEnabled(true);
        conf.setBookieMaxWeightMultipleForWeightBasedPlacement(maxMultiple);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        Map<BookieSocketAddress, BookieInfo> bookieInfoMap = new HashMap<BookieSocketAddress, BookieInfo>();
        bookieInfoMap.put(addr0, new BookieInfo(50L, 50L));
        bookieInfoMap.put(addr1, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3, new BookieInfo(200L, 200L));
        bookieInfoMap.put(addr4, new BookieInfo(multiple * 50L, multiple * 50L));
        repp.updateBookieInfo(bookieInfoMap);

        Map<BookieSocketAddress, Long> selectionCounts = new HashMap<BookieSocketAddress, Long>();
        selectionCounts.put(addr0, 0L);
        selectionCounts.put(addr1, 0L);
        selectionCounts.put(addr2, 0L);
        selectionCounts.put(addr3, 0L);
        selectionCounts.put(addr4, 0L);
        int numTries = 50000;
        EnsemblePlacementPolicy.PlacementResult<BookieSocketAddress> replaceBookieResponse;
        BookieSocketAddress replacedBookie;
        boolean isEnsembleAdheringToPlacementPolicy;
        for (int i = 0; i < numTries; i++) {
            // addr2 is on /r2 and this is the only one on this rack. So the replacement
            // will come from other racks. However, the weight should be honored in such
            // selections as well
            replaceBookieResponse = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(), addr2, new HashSet<>());
            replacedBookie = replaceBookieResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isStrictlyAdheringToPolicy();
            assertTrue(addr0.equals(replacedBookie) || addr1.equals(replacedBookie) || addr3.equals(replacedBookie)
                    || addr4.equals(replacedBookie));
            assertTrue(isEnsembleAdheringToPlacementPolicy);
            selectionCounts.put(replacedBookie, selectionCounts.get(replacedBookie) + 1);
        }
        /*
         * since addr2 has to be replaced, the remaining bookies weight are - 50, 100, 200, 500 (10*50)
         * So the median calculated by WeightedRandomSelection is (100 + 200) / 2 = 150
         */
        double medianWeight = 150;
        double medianSelectionCounts = (double) (medianWeight / bookieInfoMap.get(addr1).getWeight())
            * selectionCounts.get(addr1);
        double observedMultiple1 = ((double) selectionCounts.get(addr4) / (double) medianSelectionCounts);
        double observedMultiple2 = ((double) selectionCounts.get(addr4) / (double) selectionCounts.get(addr3));
        LOG.info("oM1 " + observedMultiple1 + " oM2 " + observedMultiple2);
        assertTrue("Weights not being honored expected " + maxMultiple + " observed " + observedMultiple1,
                Math.abs(observedMultiple1 - maxMultiple) < 1);
        // expected multiple for addr3
        double expected = (medianWeight * maxMultiple) / bookieInfoMap.get(addr3).getWeight();
        assertTrue("Weights not being honored expected " + expected + " observed " + observedMultiple2,
                Math.abs(observedMultiple2 - expected) < 1);
    }

    @Test
    public void testWeightedPlacementAndNewEnsembleWithEnoughBookiesInSameRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.9", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr5.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr6.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r3");
        StaticDNSResolver.addNodeToRack(addr7.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r3");
        StaticDNSResolver.addNodeToRack(addr8.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r3");
        StaticDNSResolver.addNodeToRack(addr9.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r3");

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

        int maxMultiple = 4;
        conf.setDiskWeightBasedPlacementEnabled(true);
        conf.setBookieMaxWeightMultipleForWeightBasedPlacement(maxMultiple);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        Map<BookieSocketAddress, BookieInfo> bookieInfoMap = new HashMap<BookieSocketAddress, BookieInfo>();
        bookieInfoMap.put(addr1, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr5, new BookieInfo(1000L, 1000L));
        bookieInfoMap.put(addr6, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr7, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr8, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr9, new BookieInfo(1000L, 1000L));

        repp.updateBookieInfo(bookieInfoMap);

        Map<BookieSocketAddress, Long> selectionCounts = new HashMap<BookieSocketAddress, Long>();
        for (BookieSocketAddress b : addrs) {
            selectionCounts.put(b, 0L);
        }
        int numTries = 10000;

        Set<BookieSocketAddress> excludeList = new HashSet<BookieSocketAddress>();
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse;
        List<BookieSocketAddress> ensemble;
        int ensembleSize = 3;
        int writeQuorumSize = 2;
        int acqQuorumSize = 2;
        for (int i = 0; i < numTries; i++) {
            // addr2 is on /r2 and this is the only one on this rack. So the replacement
            // will come from other racks. However, the weight should be honored in such
            // selections as well
            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, acqQuorumSize, null, excludeList);
            ensemble = ensembleResponse.getResult();
            assertTrue(
                    "Rackaware selection not happening "
                            + getNumCoveredWriteQuorums(ensemble, writeQuorumSize, conf.getMinNumRacksPerWriteQuorum()),
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, conf.getMinNumRacksPerWriteQuorum()) >= 2);
            for (BookieSocketAddress b : ensemble) {
                selectionCounts.put(b, selectionCounts.get(b) + 1);
            }
        }

        // the median weight used is 100 since addr2 and addr6 have the same weight, we use their
        // selection counts as the same as median
        double observedMultiple1 = ((double) selectionCounts.get(addr5) / (double) selectionCounts.get(addr2));
        double observedMultiple2 = ((double) selectionCounts.get(addr9) / (double) selectionCounts.get(addr6));
        assertTrue("Weights not being honored expected 2 observed " + observedMultiple1,
                Math.abs(observedMultiple1 - maxMultiple) < 0.5);
        assertTrue("Weights not being honored expected 4 observed " + observedMultiple2,
                Math.abs(observedMultiple2 - maxMultiple) < 0.5);
    }

    @Test
    public void testWeightedPlacementAndNewEnsembleWithoutEnoughBookies() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r2");
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r3");
        StaticDNSResolver.addNodeToRack(addr5.getSocketAddress().getAddress().getHostAddress(),
                NetworkTopology.DEFAULT_REGION + "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);

        int maxMultiple = 4;
        conf.setDiskWeightBasedPlacementEnabled(true);
        conf.setBookieMaxWeightMultipleForWeightBasedPlacement(maxMultiple);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        Map<BookieSocketAddress, BookieInfo> bookieInfoMap = new HashMap<BookieSocketAddress, BookieInfo>();
        bookieInfoMap.put(addr1, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3, new BookieInfo(1000L, 1000L));
        bookieInfoMap.put(addr4, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr5, new BookieInfo(1000L, 1000L));

        repp.updateBookieInfo(bookieInfoMap);
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse;
        List<BookieSocketAddress> ensemble;
        Set<BookieSocketAddress> excludeList = new HashSet<BookieSocketAddress>();
        try {
            excludeList.add(addr1);
            excludeList.add(addr2);
            excludeList.add(addr3);
            excludeList.add(addr4);
            ensembleResponse = repp.newEnsemble(3, 2, 2, null, excludeList);
            ensemble = ensembleResponse.getResult();
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies" + ensemble);
        } catch (BKNotEnoughBookiesException e) {
            // this is expected
        }
        try {
            ensembleResponse = repp.newEnsemble(1, 1, 1, null, excludeList);
            ensemble = ensembleResponse.getResult();
        } catch (BKNotEnoughBookiesException e) {
            fail("Should not throw BKNotEnoughBookiesException when there are enough bookies for the ensemble");
        }
    }

    static int getNumCoveredWriteQuorums(List<BookieSocketAddress> ensemble, int writeQuorumSize,
            int minNumRacksPerWriteQuorumConfValue) throws Exception {
        int ensembleSize = ensemble.size();
        int numCoveredWriteQuorums = 0;
        for (int i = 0; i < ensembleSize; i++) {
            Set<String> racks = new HashSet<String>();
            for (int j = 0; j < writeQuorumSize; j++) {
                int bookieIdx = (i + j) % ensembleSize;
                BookieSocketAddress addr = ensemble.get(bookieIdx);
                racks.add(StaticDNSResolver.getRack(addr.getHostName()));
            }
            int numOfRacksToCoverTo = Math.max(Math.min(writeQuorumSize, minNumRacksPerWriteQuorumConfValue), 2);
            numCoveredWriteQuorums += (racks.size() >= numOfRacksToCoverTo ? 1 : 0);
        }
        return numCoveredWriteQuorums;
    }

    @Test
    public void testNodeWithFailures() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        HashMap<BookieSocketAddress, Long> bookieFailures = new HashMap<BookieSocketAddress, Long>();

        bookieFailures.put(addr1, 20L);
        bookieFailures.put(addr2, 22L);

        // remove failure bookies: addr1 and addr2
        addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        DistributionSchedule.WriteSet reoderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(bookieFailures, new HashMap<>()), writeSet);
        LOG.info("reorder set : {}", reoderSet);
        assertEquals(ensemble.get(reoderSet.get(2)), addr1);
        assertEquals(ensemble.get(reoderSet.get(3)), addr2);
        assertEquals(ensemble.get(reoderSet.get(0)), addr3);
        assertEquals(ensemble.get(reoderSet.get(1)), addr4);
    }

    @Test
    public void testPlacementOnStabilizeNetworkTopology() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp = new RackawareEnsemblePlacementPolicy();
        ClientConfiguration confLocal = new ClientConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setNetworkTopologyStabilizePeriodSeconds(99999);
        repp.initialize(confLocal, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // addr4 left
        addrs.remove(addr4);
        Set<BookieSocketAddress> deadBookies = repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        assertTrue(deadBookies.isEmpty());

        // we will never use addr4 even it is in the stabilized network topology
        for (int i = 0; i < 5; i++) {
            EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse =
                repp.newEnsemble(3, 2, 2, null, new HashSet<BookieSocketAddress>());
            List<BookieSocketAddress> ensemble = ensembleResponse.getResult();
            boolean isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
            assertFalse(ensemble.contains(addr4));
            assertFalse(isEnsembleAdheringToPlacementPolicy);
        }

        // we could still use addr4 for urgent allocation if it is just bookie flapping
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse =
            repp.newEnsemble(4, 2, 2, null, new HashSet<BookieSocketAddress>());
        List<BookieSocketAddress> ensemble = ensembleResponse.getResult();
        boolean isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
        assertFalse(isEnsembleAdheringToPlacementPolicy);
        assertTrue(ensemble.contains(addr4));
    }

    @Test
    public void testShuffleWithMask() {
        int mask = 0xE1 << 16;
        int maskBits = 0xFF << 16;
        boolean shuffleOccurred = false;

        for (int i = 0; i < 100; i++) {
            DistributionSchedule.WriteSet w = writeSetFromValues(
                    1, 2, 3 & mask, 4 & mask, 5 & mask, 6);
            shuffleWithMask(w, mask, maskBits);
            assertEquals(w.get(0), 1);
            assertEquals(w.get(1), 2);
            assertEquals(w.get(5), 6);

            if (w.get(3) == (3 & mask)
                || w.get(4) == (3 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(2) != (3 & mask)) {
                fail("3 not found");
            }

            if (w.get(2) == (4 & mask)
                || w.get(4) == (4 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(3) != (4 & mask)) {
                fail("4 not found");
            }

            if (w.get(2) == (5 & mask)
                || w.get(3) == (5 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(4) != (5 & mask)) {
                fail("5 not found");
            }
        }
        assertTrue(shuffleOccurred);

        // at start of array
        shuffleOccurred = false;
        for (int i = 0; i < 100; i++) {
            DistributionSchedule.WriteSet w = writeSetFromValues(
                    1 & mask, 2 & mask, 3 & mask, 4, 5, 6);
            shuffleWithMask(w, mask, maskBits);
            assertEquals(w.get(3), 4);
            assertEquals(w.get(4), 5);
            assertEquals(w.get(5), 6);

            if (w.get(1) == (1 & mask)
                || w.get(2) == (1 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(0) != (1 & mask)) {
                fail("1 not found");
            }

            if (w.get(0) == (2 & mask)
                || w.get(2) == (2 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(1) != (2 & mask)) {
                fail("2 not found");
            }

            if (w.get(0) == (3 & mask)
                || w.get(1) == (3 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(2) != (3 & mask)) {
                fail("3 not found");
            }
        }
        assertTrue(shuffleOccurred);

        // at end of array
        shuffleOccurred = false;
        for (int i = 0; i < 100; i++) {
            DistributionSchedule.WriteSet w = writeSetFromValues(
                    1, 2, 3, 4 & mask, 5 & mask, 6 & mask);
            shuffleWithMask(w, mask, maskBits);
            assertEquals(w.get(0), 1);
            assertEquals(w.get(1), 2);
            assertEquals(w.get(2), 3);

            if (w.get(4) == (4 & mask)
                || w.get(5) == (4 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(3) != (4 & mask)) {
                fail("4 not found");
            }

            if (w.get(3) == (5 & mask)
                || w.get(5) == (5 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(4) != (5 & mask)) {
                fail("5 not found");
            }

            if (w.get(3) == (6 & mask)
                || w.get(4) == (6 & mask)) {
                shuffleOccurred = true;
            } else if (w.get(5) != (6 & mask)) {
                fail("6 not found");
            }
        }
        assertTrue(shuffleOccurred);
    }

    @Test
    public void testNumBookiesInDefaultRackGauge() throws Exception {
        String defaultRackForThisTest = NetworkTopology.DEFAULT_REGION_AND_RACK;
        repp.uninitalize();
        updateMyRack(defaultRackForThisTest);

        // Update cluster
        BookieSocketAddress newAddr1 = new BookieSocketAddress("127.0.0.100", 3181);
        BookieSocketAddress newAddr2 = new BookieSocketAddress("127.0.0.101", 3181);
        BookieSocketAddress newAddr3 = new BookieSocketAddress("127.0.0.102", 3181);
        BookieSocketAddress newAddr4 = new BookieSocketAddress("127.0.0.103", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(newAddr1.getHostName(), defaultRackForThisTest);
        StaticDNSResolver.addNodeToRack(newAddr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(newAddr3.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(newAddr4.getHostName(), defaultRackForThisTest);

        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger("");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, statsLogger);
        repp.withDefaultRack(defaultRackForThisTest);

        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        Set<BookieSocketAddress> writeableBookies = new HashSet<BookieSocketAddress>();
        writeableBookies.add(newAddr1);
        writeableBookies.add(newAddr2);
        Set<BookieSocketAddress> readOnlyBookies = new HashSet<BookieSocketAddress>();
        readOnlyBookies.add(newAddr3);
        readOnlyBookies.add(newAddr4);
        repp.onClusterChanged(writeableBookies, readOnlyBookies);
        // only writable bookie - newAddr1 in default rack
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 1, numBookiesInDefaultRackGauge.getSample());

        readOnlyBookies.remove(newAddr4);
        writeableBookies.add(newAddr4);
        repp.onClusterChanged(writeableBookies, readOnlyBookies);
        // newAddr4 is also added to writable bookie so 2 writable bookies -
        // newAddr1 and newAddr4
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 2, numBookiesInDefaultRackGauge.getSample());

        // newAddr4 rack is changed and it is not in default anymore
        StaticDNSResolver.changeRack(Arrays.asList(newAddr4), Arrays.asList("/default-region/r4"));
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 1, numBookiesInDefaultRackGauge.getSample());

        writeableBookies.clear();
        // writeableBookies is empty so 0 writable bookies in default rack
        repp.onClusterChanged(writeableBookies, readOnlyBookies);
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 0, numBookiesInDefaultRackGauge.getSample());

        StaticDNSResolver.changeRack(Arrays.asList(newAddr1), Arrays.asList("/default-region/r2"));
        readOnlyBookies.clear();
        writeableBookies.add(newAddr1);
        writeableBookies.add(newAddr2);
        writeableBookies.add(newAddr3);
        writeableBookies.add(newAddr4);
        repp.onClusterChanged(writeableBookies, readOnlyBookies);
        // newAddr1 rack is changed and it is not in default anymore. So no
        // bookies in default rack anymore
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 0, numBookiesInDefaultRackGauge.getSample());
    }

    @Test
    public void testNewEnsembleExcludesDefaultRackBookiesEnforceMinNumRacks() throws Exception {
        String defaultRackForThisTest = NetworkTopology.DEFAULT_REGION_AND_RACK;
        repp.uninitalize();
        updateMyRack(defaultRackForThisTest);
        int minNumRacksPerWriteQuorum = 4;
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        // set enforceMinNumRacksPerWriteQuorum
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);

        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger("");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, statsLogger);
        repp.withDefaultRack(defaultRackForThisTest);
        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        int writeQuorumSize = 3;
        int ackQuorumSize = 3;
        int effectiveMinNumRacksPerWriteQuorum = Math.min(minNumRacksPerWriteQuorum, writeQuorumSize);

        int numOfRacks = 2 * effectiveMinNumRacksPerWriteQuorum - 1;
        int numOfBookiesPerRack = 20;
        BookieSocketAddress[] bookieSocketAddresses = new BookieSocketAddress[numOfRacks * numOfBookiesPerRack];

        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieSocketAddresses[index] = new BookieSocketAddress("128.0.0." + index, 3181);
                StaticDNSResolver.addNodeToRack(bookieSocketAddresses[index].getHostName(), "/default-region/r" + i);
            }
        }

        int numOfBookiesInDefaultRack = 10;
        BookieSocketAddress[] bookieSocketAddressesInDefaultRack = new BookieSocketAddress[numOfBookiesInDefaultRack];
        for (int i = 0; i < numOfBookiesInDefaultRack; i++) {
            bookieSocketAddressesInDefaultRack[i] = new BookieSocketAddress("127.0.0." + (i + 100), 3181);
            StaticDNSResolver.addNodeToRack(bookieSocketAddressesInDefaultRack[i].getHostName(),
                    defaultRackForThisTest);
        }

        Set<BookieSocketAddress> writableBookies = new HashSet<BookieSocketAddress>(
                Arrays.asList(bookieSocketAddresses));
        writableBookies.addAll(Arrays.asList(bookieSocketAddressesInDefaultRack));
        repp.onClusterChanged(writableBookies, new HashSet<BookieSocketAddress>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());

        /*
         * in this scenario we have enough number of racks (2 *
         * effectiveMinNumRacksPerWriteQuorum - 1) and more number of bookies in
         * each rack. So we should be able to create ensemble for all
         * ensembleSizes (as long as there are enough number of bookies in each
         * rack).
         *
         * Since minNumRacksPerWriteQuorum is enforced, it shouldn't select node
         * from default rack.
         */
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> ensembleResponse;
        List<BookieSocketAddress> ensemble;
        boolean isEnsembleAdheringToPlacementPolicy;
        for (int ensembleSize = effectiveMinNumRacksPerWriteQuorum; ensembleSize < 40; ensembleSize++) {
            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
            ensemble = ensembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
            assertEquals("Number of writeQuorum sets covered", ensembleSize,
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum()));
            assertTrue(isEnsembleAdheringToPlacementPolicy);

            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
            ensemble = ensembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = ensembleResponse.isStrictlyAdheringToPolicy();
            assertEquals("Number of writeQuorum sets covered", ensembleSize,
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum()));
            assertTrue(isEnsembleAdheringToPlacementPolicy);
            Collection<BookieSocketAddress> bookiesOfDefaultRackInEnsemble = CollectionUtils
                    .intersection(Arrays.asList(bookieSocketAddressesInDefaultRack), ensemble);
            assertTrue("Ensemble is not supposed to contain bookies from default rack, but ensemble contains - "
                    + bookiesOfDefaultRackInEnsemble, bookiesOfDefaultRackInEnsemble.isEmpty());
        }
    }
}
