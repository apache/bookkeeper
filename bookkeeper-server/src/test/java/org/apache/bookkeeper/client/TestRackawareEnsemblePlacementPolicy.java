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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import junit.framework.TestCase;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble;
import org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy.EnsembleForReplacementWithNoConstraints;
import org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy.TruePredicate;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.AbstractDNSToSwitchMapping;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the rackaware ensemble placement policy.
 */
public class TestRackawareEnsemblePlacementPolicy extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestRackawareEnsemblePlacementPolicy.class);

    RackawareEnsemblePlacementPolicy repp;
    final List<BookieId> ensemble = new ArrayList<BookieId>();
    DistributionSchedule.WriteSet writeSet = DistributionSchedule.NULL_WRITE_SET;
    ClientConfiguration conf = new ClientConfiguration();
    BookieSocketAddress addr1;
    BookieSocketAddress addr2, addr3, addr4;
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
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr3.toBookieId());
        ensemble.add(addr4.toBookieId());
        writeSet = writeSetFromValues(0, 1, 2, 3);

        timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getTimeoutTimerNumTicks());

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
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

    static void updateMyRack(String rack) throws Exception {
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), rack);
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostName(), rack);
        StaticDNSResolver.addNodeToRack("127.0.0.1", rack);
        StaticDNSResolver.addNodeToRack("localhost", rack);
    }

    @Test
    public void testInitalize() throws Exception{
        String dnsResolverName = conf.getString(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
        DNSToSwitchMapping dnsResolver = ReflectionUtils.newInstance(dnsResolverName, DNSToSwitchMapping.class);
        AbstractDNSToSwitchMapping tmp = (AbstractDNSToSwitchMapping) dnsResolver;
        assertNull(tmp.getBookieAddressResolver());

        dnsResolver.setBookieAddressResolver(repp.bookieAddressResolver);
        assertNotNull(tmp.getBookieAddressResolver());
    }

    @Test
    public void testNodeDown() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        addrs.remove(addr1.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        addrs.remove(addr1.toBookieId());
        Set<BookieId> ro = new HashSet<BookieId>();
        ro.add(addr1.toBookieId());
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        repp.registerSlowBookie(addr1.toBookieId(), 0L);
        Map<BookieId, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1.toBookieId(), 1L);
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        repp.registerSlowBookie(addr1.toBookieId(), 0L);
        repp.registerSlowBookie(addr2.toBookieId(), 0L);
        Map<BookieId, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1.toBookieId(), 1L);
        bookiePendingMap.put(addr2.toBookieId(), 2L);
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        addrs.remove(addr1.toBookieId());
        addrs.remove(addr2.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        addrs.remove(addr1.toBookieId());
        addrs.remove(addr2.toBookieId());
        Set<BookieId> roAddrs = new HashSet<BookieId>();
        roAddrs.add(addr2.toBookieId());
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        repp.registerSlowBookie(addr1.toBookieId(), 0L);
        Map<BookieId, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1.toBookieId(), 1L);
        addrs.remove(addr2.toBookieId());
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        addrs.remove(addr1.toBookieId());
        addrs.remove(addr2.toBookieId());
        Set<BookieId> ro = new HashSet<BookieId>();
        ro.add(addr2.toBookieId());
        repp.registerSlowBookie(addr3.toBookieId(), 0L);
        Map<BookieId, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr3.toBookieId(), 1L);
        addrs.remove(addr2.toBookieId());
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1.toBookieId(), 20L);
        bookiePendingMap.put(addr2.toBookieId(), 7L);
        bookiePendingMap.put(addr3.toBookieId(), 1L); // best bookie -> this one first
        bookiePendingMap.put(addr4.toBookieId(), 5L);

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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());
        addrs.add(addr7.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1.toBookieId(), 1L); // not in write set
        bookiePendingMap.put(addr2.toBookieId(), 20L);
        bookiePendingMap.put(addr3.toBookieId(), 0L); // not in write set
        bookiePendingMap.put(addr4.toBookieId(), 12L);
        bookiePendingMap.put(addr5.toBookieId(), 9L); // not in write set
        bookiePendingMap.put(addr6.toBookieId(), 2L); // best bookie -> this one first
        bookiePendingMap.put(addr7.toBookieId(), 10L);
        List<BookieId> ensemble = new ArrayList<BookieId>();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr3.toBookieId());
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr5.toBookieId());
        ensemble.add(addr6.toBookieId());
        ensemble.add(addr7.toBookieId());

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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1.toBookieId(), 10L); // -> this one first
        bookiePendingMap.put(addr2.toBookieId(), 7L);
        bookiePendingMap.put(addr3.toBookieId(), 1L); // best bookie, but below threshold
        bookiePendingMap.put(addr4.toBookieId(), 5L);

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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, Long> bookiePendingMap = new HashMap<>();
        bookiePendingMap.put(addr1.toBookieId(), 1L); // -> this one first
        bookiePendingMap.put(addr2.toBookieId(), 7L);
        bookiePendingMap.put(addr3.toBookieId(), 1L);
        bookiePendingMap.put(addr4.toBookieId(), 5L);

        DistributionSchedule.WriteSet origWriteSet = writeSet.copy();
        DistributionSchedule.WriteSet reorderSet = repp.reorderReadSequence(
            ensemble, getBookiesHealthInfo(new HashMap<>(), bookiePendingMap), writeSet);
        LOG.info("reorder set : {}", reorderSet);
        assertEquals("writeSet should be in original order", origWriteSet, reorderSet);
    }

    @Test
    public void testIsEnsembleAdheringToPlacementPolicy() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/default-region/r3");
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        List<BookieId> emptyEnsemble = new ArrayList<>();
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                repp.isEnsembleAdheringToPlacementPolicy(emptyEnsemble, 3, 3));

        List<BookieId> ensemble = new ArrayList<>();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr2.toBookieId());
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                repp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 3));

        ensemble = new ArrayList<>();
        ensemble.add(addr1.toBookieId());
        ensemble.add(addr2.toBookieId());
        ensemble.add(addr3.toBookieId());
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.MEETS_STRICT,
                repp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 3));

        ensemble = new ArrayList<>();
        ensemble.add(addr4.toBookieId());
        ensemble.add(addr5.toBookieId());
        ensemble.add(addr6.toBookieId());
        assertEquals("PlacementPolicyAdherence", PlacementPolicyAdherence.FAIL,
                repp.isEnsembleAdheringToPlacementPolicy(ensemble, 3, 3));
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        // replace node under r2
        EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse =
            repp.replaceBookie(1, 1, 1, null, new ArrayList<>(), addr2.toBookieId(), new HashSet<>());
        BookieId replacedBookie = replaceBookieResponse.getResult();
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
        assertEquals(addr3.toBookieId(), replacedBookie);
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        // replace node under r2
        Set<BookieId> excludedAddrs = new HashSet<BookieId>();
        excludedAddrs.add(addr1.toBookieId());
        EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse =
            repp.replaceBookie(1, 1, 1, null, new ArrayList<>(), addr2.toBookieId(), excludedAddrs);
        BookieId replacedBookie = replaceBookieResponse.getResult();
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
        assertFalse(addr1.toBookieId().equals(replacedBookie));
        assertTrue(addr3.toBookieId().equals(replacedBookie) || addr4.toBookieId().equals(replacedBookie));
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        // replace node under r2
        Set<BookieId> excludedAddrs = new HashSet<BookieId>();
        excludedAddrs.add(addr1.toBookieId());
        excludedAddrs.add(addr3.toBookieId());
        excludedAddrs.add(addr4.toBookieId());
        try {
            repp.replaceBookie(1, 1, 1, null, new ArrayList<BookieId>(), addr2.toBookieId(), excludedAddrs);
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        // replace node under r2
        List<BookieId> ensembleBookies = new ArrayList<BookieId>();
        ensembleBookies.add(addr2.toBookieId());
        ensembleBookies.add(addr4.toBookieId());
        EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse = repp.replaceBookie(
            1, 1, 1 , null,
            ensembleBookies,
            addr4.toBookieId(),
            new HashSet<>());
        BookieId replacedBookie = replaceBookieResponse.getResult();
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
        assertEquals(addr1.toBookieId(), replacedBookie);
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
    }

    @Test
    public void testNewEnsembleWithSingleRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.9", 3181);
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        try {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse;
            ensembleResponse = repp.newEnsemble(3, 2, 2, null, new HashSet<>());
            List<BookieId> ensemble = ensembleResponse.getResult();
            PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
            assertEquals(0, getNumCoveredWriteQuorums(ensemble, 2, conf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
            assertEquals(PlacementPolicyAdherence.FAIL, isEnsembleAdheringToPlacementPolicy);
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse2;
            ensembleResponse2 = repp.newEnsemble(4, 2, 2, null, new HashSet<>());
            List<BookieId> ensemble2 = ensembleResponse2.getResult();
            PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy2 = ensembleResponse2.getAdheringToPolicy();
            assertEquals(0, getNumCoveredWriteQuorums(ensemble2, 2, conf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
            assertEquals(PlacementPolicyAdherence.FAIL, isEnsembleAdheringToPlacementPolicy);
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
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        List<BookieId> ensemble;
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
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer,
                DISABLE_ALL, statsLogger, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(defaultRackForThisTest);
        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        BookieId[] bookieSocketAddresses = new BookieId[numOfRacks * numOfBookiesPerRack];

        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieSocketAddresses[index] = new BookieSocketAddress("128.0.0." + index, 3181).toBookieId();
                StaticDNSResolver.addNodeToRack("128.0.0." + index, "/default-region/r" + i);
            }
        }

        int numOfBookiesInDefaultRack = 5;
        BookieId[] bookieSocketAddressesInDefaultRack = new BookieId[numOfBookiesInDefaultRack];
        for (int i = 0; i < numOfBookiesInDefaultRack; i++) {
            bookieSocketAddressesInDefaultRack[i] = new BookieSocketAddress("128.0.0." + (100 + i), 3181).toBookieId();
            StaticDNSResolver.addNodeToRack("128.0.0." + (100 + i),
                    defaultRackForThisTest);
        }

        List<BookieId> nonDefaultRackBookiesList = Arrays.asList(bookieSocketAddresses);
        List<BookieId> defaultRackBookiesList = Arrays.asList(bookieSocketAddressesInDefaultRack);
        Set<BookieId> writableBookies = new HashSet<BookieId>(nonDefaultRackBookiesList);
        writableBookies.addAll(defaultRackBookiesList);
        repp.onClusterChanged(writableBookies, new HashSet<BookieId>());
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
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse;
        List<BookieId> ensemble;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        int ensembleSize = numOfRacks * numOfBookiesPerRack;
        int writeQuorumSize = numOfRacks;
        int ackQuorumSize = numOfRacks;

        ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
        ensemble = ensembleResponse.getResult();
        isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
        assertEquals("Number of writeQuorum sets covered", ensembleSize,
                getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);

        ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize,
                new HashSet<>(defaultRackBookiesList), EnsembleForReplacementWithNoConstraints.INSTANCE,
                TruePredicate.INSTANCE);
        ensemble = ensembleResponse.getResult();
        isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
        assertEquals("Number of writeQuorum sets covered", ensembleSize,
                getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
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
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int writeQuorumSize = 3;
        int ackQuorumSize = 3;
        int effectiveMinNumRacksPerWriteQuorum = Math.min(minNumRacksPerWriteQuorum, writeQuorumSize);

        int numOfRacks = 2 * effectiveMinNumRacksPerWriteQuorum - 1;
        int numOfBookiesPerRack = 20;
        BookieId[] bookieSocketAddresses = new BookieId[numOfRacks * numOfBookiesPerRack];

        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieSocketAddresses[index] = new BookieSocketAddress("128.0.0." + index, 3181).toBookieId();
                StaticDNSResolver.addNodeToRack("128.0.0." + index, "/default-region/r" + i);
            }
        }

        Set<BookieId> addrs = new HashSet<BookieId>();
        repp.onClusterChanged(new HashSet<BookieId>(Arrays.asList(bookieSocketAddresses)),
                new HashSet<BookieId>());

        /*
         * in this scenario we have enough number of racks (2 *
         * effectiveMinNumRacksPerWriteQuorum - 1) and more number of bookies in
         * each rack. So we should be able to create ensemble for all
         * ensembleSizes (as long as there are enough number of bookies in each
         * rack).
         */
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse;
        List<BookieId> ensemble;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        for (int ensembleSize = effectiveMinNumRacksPerWriteQuorum; ensembleSize < 40; ensembleSize++) {
            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
            ensemble = ensembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
            assertEquals("Number of writeQuorum sets covered", ensembleSize,
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);

            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, new HashSet<>(),
                    EnsembleForReplacementWithNoConstraints.INSTANCE, TruePredicate.INSTANCE);
            ensemble = ensembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
            assertEquals("Number of writeQuorum sets covered", ensembleSize,
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
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
                statsLogger, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(defaultRackForThisTest);
        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        Set<BookieId> bookieSocketAddresses = new HashSet<BookieId>();
        Map<BookieId, String> bookieRackMap = new HashMap<BookieId, String>();
        BookieId bookieAddress;
        String rack;
        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181).toBookieId();
                rack = "/default-region/r" + i;
                StaticDNSResolver.addNodeToRack("128.0.0." + index, rack);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rack);
            }
        }
        /*
         * bookies in this default rack should not be returned for replacebookie
         * response.
         */
        int numOfBookiesInDefaultRack = 5;
        BookieId[] bookieSocketAddressesInDefaultRack = new BookieId[numOfBookiesInDefaultRack];
        for (int i = 0; i < numOfBookiesInDefaultRack; i++) {
            bookieSocketAddressesInDefaultRack[i] = new BookieSocketAddress("127.0.0." + (i + 100), 3181).toBookieId();
            StaticDNSResolver.addNodeToRack("127.0.0." + (i + 100),
                    defaultRackForThisTest);
        }

        Set<BookieId> nonDefaultRackBookiesList = bookieSocketAddresses;
        List<BookieId> defaultRackBookiesList = Arrays.asList(bookieSocketAddressesInDefaultRack);
        Set<BookieId> writableBookies = new HashSet<BookieId>(nonDefaultRackBookiesList);
        writableBookies.addAll(defaultRackBookiesList);
        repp.onClusterChanged(writableBookies, new HashSet<BookieId>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());

        /*
         * Though minNumRacksPerWriteQuorum is set to 4, since writeQuorum is 3
         * and there are enough bookies in 3 racks, this newEnsemble call should
         * succeed.
         */
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse;
        List<BookieId> ensemble;
        int ensembleSize = numOfRacks * numOfBookiesPerRack;
        int writeQuorumSize = numOfRacks;
        int ackQuorumSize = numOfRacks;

        ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
        ensemble = ensembleResponse.getResult();

        BookieId bookieInEnsembleToBeReplaced = ensemble.get(7);
        // get rack of some other bookie
        String rackOfOtherBookieInEnsemble = bookieRackMap.get(ensemble.get(8));
        BookieSocketAddress newBookieAddress1 = new BookieSocketAddress("128.0.0.100", 3181);
        /*
         * add the newBookie to the rack of some other bookie in the current
         * ensemble
         */
        StaticDNSResolver.addNodeToRack(newBookieAddress1.getHostName(), rackOfOtherBookieInEnsemble);
        bookieSocketAddresses.add(newBookieAddress1.toBookieId());
        writableBookies.add(newBookieAddress1.toBookieId());
        bookieRackMap.put(newBookieAddress1.toBookieId(), rackOfOtherBookieInEnsemble);

        repp.onClusterChanged(writableBookies, new HashSet<BookieId>());
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
        bookieSocketAddresses.add(newBookieAddress2.toBookieId());
        writableBookies.add(newBookieAddress2.toBookieId());
        bookieRackMap.put(newBookieAddress2.toBookieId(), newRack);

        repp.onClusterChanged(writableBookies, new HashSet<BookieId>());
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", numOfBookiesInDefaultRack,
                numBookiesInDefaultRackGauge.getSample());
        /*
         * this replaceBookie should succeed, because a new bookie is added to a
         * new rack.
         */
        EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse;
        BookieId replacedBookieAddress;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        replaceBookieResponse = repp.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, null, ensemble,
                bookieInEnsembleToBeReplaced, new HashSet<>());
        replacedBookieAddress = replaceBookieResponse.getResult();
        isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
        assertEquals("It should be newBookieAddress2", newBookieAddress2.toBookieId(), replacedBookieAddress);
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);

        Set<BookieId> bookiesToExclude = new HashSet<>();
        bookiesToExclude.add(newBookieAddress2.toBookieId());
        repp.onClusterChanged(writableBookies, new HashSet<BookieId>());
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
        bookieSocketAddresses.add(newBookieAddress3.toBookieId());
        writableBookies.add(newBookieAddress3.toBookieId());
        bookieRackMap.put(newBookieAddress3.toBookieId(), rackOfBookieToBeReplaced);

        repp.onClusterChanged(writableBookies, new HashSet<BookieId>());
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
        isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
        assertEquals("It should be newBookieAddress3", newBookieAddress3.toBookieId(), replacedBookieAddress);
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
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
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        String[] rackLocationNames = new String[numOfRacks];
        List<BookieId> bookieSocketAddresses = new ArrayList<BookieId>();
        Map<BookieId, String> bookieRackMap = new HashMap<BookieId, String>();
        BookieId bookieAddress;

        for (int i = 0; i < numOfRacks; i++) {
            rackLocationNames[i] = "/default-region/r" + i;
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181).toBookieId();
                StaticDNSResolver.addNodeToRack("128.0.0." + index, rackLocationNames[i]);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rackLocationNames[i]);
            }
        }
        String nonExistingRackLocation = "/default-region/r25";

        repp.onClusterChanged(new HashSet<BookieId>(bookieSocketAddresses),
                new HashSet<BookieId>());

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

        Set<BookieId> excludeBookiesOfRackR0 = new HashSet<BookieId>();
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
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        String[] rackLocationNames = new String[numOfRacks];
        List<BookieId> bookieSocketAddresses = new ArrayList<BookieId>();
        Map<BookieId, String> bookieRackMap = new HashMap<BookieId, String>();
        BookieId bookieAddress;

        for (int i = 0; i < numOfRacks; i++) {
            rackLocationNames[i] = "/default-region/r" + i;
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181).toBookieId();
                StaticDNSResolver.addNodeToRack("128.0.0." + index, rackLocationNames[i]);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rackLocationNames[i]);
            }
        }

        repp.onClusterChanged(new HashSet<BookieId>(bookieSocketAddresses),
                new HashSet<BookieId>());

        Set<BookieId> excludeBookiesOfRackR0 = new HashSet<BookieId>();
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
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        String[] rackLocationNames = new String[numOfRacks];
        List<BookieId> bookieSocketAddresses = new ArrayList<BookieId>();
        Map<BookieId, String> bookieRackMap = new HashMap<BookieId, String>();
        BookieId bookieAddress;

        for (int i = 0; i < numOfRacks; i++) {
            rackLocationNames[i] = "/default-region/r" + i;
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181).toBookieId();
                StaticDNSResolver.addNodeToRack("128.0.0." + index, rackLocationNames[i]);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rackLocationNames[i]);
            }
        }
        String nonExistingRackLocation = "/default-region/r25";

        repp.onClusterChanged(new HashSet<BookieId>(bookieSocketAddresses),
                new HashSet<BookieId>());

        Set<BookieId> excludeBookiesOfRackR0 = new HashSet<BookieId>();
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
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        int numOfRacks = 3;
        int numOfBookiesPerRack = 5;
        String[] rackLocationNames = new String[numOfRacks];
        List<BookieId> bookieSocketAddresses = new ArrayList<BookieId>();
        Map<BookieId, String> bookieRackMap = new HashMap<BookieId, String>();
        BookieId bookieAddress;

        for (int i = 0; i < numOfRacks; i++) {
            rackLocationNames[i] = "/default-region/r" + i;
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieAddress = new BookieSocketAddress("128.0.0." + index, 3181).toBookieId();
                StaticDNSResolver.addNodeToRack("128.0.0." + index, rackLocationNames[i]);
                bookieSocketAddresses.add(bookieAddress);
                bookieRackMap.put(bookieAddress, rackLocationNames[i]);
            }
        }

        repp.onClusterChanged(new HashSet<BookieId>(bookieSocketAddresses),
                new HashSet<BookieId>());

        Set<BookieId> excludeBookiesOfRackR0 = new HashSet<BookieId>();
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
                        public List<BookieId> toList() {
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        try {
            int ensembleSize = 3;
            int writeQuorumSize = 2;
            int acqQuorumSize = 2;
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                repp.newEnsemble(ensembleSize, writeQuorumSize,
                                 acqQuorumSize, null, new HashSet<>());
            List<BookieId> ensemble = ensembleResponse.getResult();
            PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
            int numCovered = getNumCoveredWriteQuorums(ensemble, writeQuorumSize,
                                                       conf.getMinNumRacksPerWriteQuorum(), repp.bookieAddressResolver);
            assertTrue(numCovered >= 1 && numCovered < 3);
            assertEquals(PlacementPolicyAdherence.FAIL, isEnsembleAdheringToPlacementPolicy);
            ensembleSize = 4;
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse2 =
                repp.newEnsemble(ensembleSize, writeQuorumSize,
                                 acqQuorumSize, null, new HashSet<>());
            List<BookieId> ensemble2 = ensembleResponse2.getResult();
            PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy2 = ensembleResponse2.getAdheringToPolicy();
            numCovered = getNumCoveredWriteQuorums(ensemble2, writeQuorumSize,
                                                   conf.getMinNumRacksPerWriteQuorum(), repp.bookieAddressResolver);
            assertTrue(numCovered >= 1 && numCovered < 3);
            assertEquals(PlacementPolicyAdherence.FAIL, isEnsembleAdheringToPlacementPolicy2);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    //see: https://github.com/apache/bookkeeper/issues/3722
    @Test
    public void testNewEnsembleWithMultipleRacksWithCommonRack() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        clientConf.setMinNumRacksPerWriteQuorum(3);
        repp.uninitalize();
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.10", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/default-region/r3");
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());
        addrs.add(addr7.toBookieId());
        addrs.add(addr8.toBookieId());
        addrs.add(addr9.toBookieId());
        addrs.add(addr10.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        try {
            int ensembleSize = 10;
            int writeQuorumSize = 10;
            int ackQuorumSize = 2;

            for (int i = 0; i < 50; ++i) {
                Set<BookieId> excludeBookies = new HashSet<>();
                EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                        repp.newEnsemble(ensembleSize, writeQuorumSize,
                                ackQuorumSize, null, excludeBookies);
            }
        } catch (Exception e) {
            fail("Can not new ensemble selection succeed");
        }
    }

    @Test
    public void testNewEnsembleWithMultipleRacksWithCommonRackFailed() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setEnforceMinNumRacksPerWriteQuorum(true);
        clientConf.setMinNumRacksPerWriteQuorum(3);
        repp.uninitalize();
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.10", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/default-region/r2");
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());
        addrs.add(addr7.toBookieId());
        addrs.add(addr8.toBookieId());
        addrs.add(addr9.toBookieId());
        addrs.add(addr10.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        try {
            int ensembleSize = 10;
            int writeQuorumSize = 10;
            int ackQuorumSize = 2;

            Set<BookieId> excludeBookies = new HashSet<>();
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                    repp.newEnsemble(ensembleSize, writeQuorumSize,
                            ackQuorumSize, null, excludeBookies);
            fail("Can not new ensemble selection succeed");
        } catch (Exception e) {
            assertTrue(e instanceof BKNotEnoughBookiesException);
        }
    }

    @Test
    public void testNewEnsembleWithPickDifferentRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r3");
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        int ensembleSize = 3;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;

        Set<BookieId> excludeBookies = new HashSet<>();

        for (int i = 0; i < 50; ++i) {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                    repp.newEnsemble(ensembleSize, writeQuorumSize,
                            ackQuorumSize, null, excludeBookies);
            List<BookieId> ensemble = ensembleResponse.getResult();
            if (ensemble.contains(addr1.toBookieId()) && ensemble.contains(addr2.toBookieId())) {
                fail("addr1 and addr2 is same rack.");
            }
        }

        //addr4 shutdown.
        addrs.remove(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        for (int i = 0; i < 50; ++i) {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                    repp.newEnsemble(ensembleSize, writeQuorumSize,
                            ackQuorumSize, null, excludeBookies);
              List<BookieId> ensemble = ensembleResponse.getResult();
            assertTrue(ensemble.contains(addr1.toBookieId()) && ensemble.contains(addr2.toBookieId()));
        }
    }

    @Test
    public void testNewEnsemblePickLocalRackBookiesByHostname() throws Exception {
        testNewEnsemblePickLocalRackBookiesInternal(true);
    }

    @Test
    public void testNewEnsemblePickLocalRackBookiesByIP() throws Exception {
        testNewEnsemblePickLocalRackBookiesInternal(false);
    }

    public void testNewEnsemblePickLocalRackBookiesInternal(boolean useHostnameResolveLocalNodePlacementPolicy)
        throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.7", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/default-region/r1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/default-region/r4");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/default-region/r5");

        String hostname = useHostnameResolveLocalNodePlacementPolicy
            ? InetAddress.getLocalHost().getCanonicalHostName() : InetAddress.getLocalHost().getHostAddress();
        StaticDNSResolver.addNodeToRack(hostname, "/default-region/r1");
        if (useHostnameResolveLocalNodePlacementPolicy) {
            conf.setUseHostnameResolveLocalNodePlacementPolicy(useHostnameResolveLocalNodePlacementPolicy);
        }

        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
            DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());
        addrs.add(addr7.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        int ensembleSize = 3;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;

        Set<BookieId> excludeBookies = new HashSet<>();

        for (int i = 0; i < 50000; ++i) {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                repp.newEnsemble(ensembleSize, writeQuorumSize,
                    ackQuorumSize, null, excludeBookies);
            List<BookieId> ensemble = ensembleResponse.getResult();
            if (!ensemble.contains(addr1.toBookieId())) {
                fail("Failed to select bookie located on the same rack with bookie client");
            }
            if (ensemble.contains(addr2.toBookieId()) && ensemble.contains(addr3.toBookieId())) {
                fail("addr2 and addr3 is same rack.");
            }
        }

        //addr4 shutdown.
        addrs.remove(addr5.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        for (int i = 0; i < 50000; ++i) {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                repp.newEnsemble(ensembleSize, writeQuorumSize,
                    ackQuorumSize, null, excludeBookies);
            List<BookieId> ensemble = ensembleResponse.getResult();
            if (!ensemble.contains(addr1.toBookieId())) {
                fail("Failed to select bookie located on the same rack with bookie client");
            }
        }

    }

    @Test
    public void testMinNumRacksPerWriteQuorumOfRacks() throws Exception {
        int numOfRacksToCreate = 6;
        int numOfNodesInEachRack = 5;

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        BookieId addr;
        for (int i = 0; i < numOfRacksToCreate; i++) {
            for (int j = 0; j < numOfNodesInEachRack; j++) {
                addr = new BookieSocketAddress("128.0.0." + ((i * numOfNodesInEachRack) + j), 3181).toBookieId();
                // update dns mapping
                StaticDNSResolver.addNodeToRack("128.0.0." + ((i * numOfNodesInEachRack) + j), "/default-region/r" + i);
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

    void validateNumOfWriteQuorumsCoveredInEnsembleCreation(Set<BookieId> addrs,
            int minNumRacksPerWriteQuorum, int ensembleSize, int writeQuorumSize) throws Exception {
        ClientConfiguration newConf = new ClientConfiguration(conf);
        newConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorum);
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
            repp.newEnsemble(ensembleSize, writeQuorumSize,
                             writeQuorumSize, null, new HashSet<>());
        List<BookieId> ensemble = ensembleResponse.getResult();
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
        int numCovered = getNumCoveredWriteQuorums(ensemble, writeQuorumSize,
                                                   minNumRacksPerWriteQuorum, repp.bookieAddressResolver);
        assertEquals("minimum number of racks covered for writequorum ensemble: " + ensemble, ensembleSize, numCovered);
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());
        addrs.add(addr7.toBookieId());
        addrs.add(addr8.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        try {
            int ensembleSize = 3;
            int writeQuorumSize = 3;
            int ackQuorumSize = 2;
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                repp.newEnsemble(ensembleSize, writeQuorumSize,
                                   ackQuorumSize, null, new HashSet<>());
            List<BookieId> ensemble1 = ensembleResponse.getResult();
            PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy1 = ensembleResponse.getAdheringToPolicy();
            assertEquals(ensembleSize,
                    getNumCoveredWriteQuorums(ensemble1, writeQuorumSize, conf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy1);
            ensembleSize = 4;
            writeQuorumSize = 4;
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse2 =
                repp.newEnsemble(ensembleSize, writeQuorumSize, 2, null, new HashSet<>());
            List<BookieId> ensemble2 = ensembleResponse2.getResult();
            PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy2 = ensembleResponse2.getAdheringToPolicy();
            assertEquals(ensembleSize,
                    getNumCoveredWriteQuorums(ensemble2, writeQuorumSize, conf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy2);
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        addrs.remove(addr1.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());

        int multiple = 10;
        conf.setDiskWeightBasedPlacementEnabled(true);
        conf.setBookieMaxWeightMultipleForWeightBasedPlacement(-1); // no max cap on weight
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<BookieId, BookieInfo>();
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(multiple * 100L, multiple * 100L));
        repp.updateBookieInfo(bookieInfoMap);

        Map<BookieId, Long> selectionCounts = new HashMap<BookieId, Long>();
        selectionCounts.put(addr3.toBookieId(), 0L);
        selectionCounts.put(addr4.toBookieId(), 0L);
        int numTries = 50000;
        EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        BookieId replacedBookie;
        for (int i = 0; i < numTries; i++) {
            // replace node under r2
            replaceBookieResponse = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(),
                                                       addr2.toBookieId(), new HashSet<>());
            replacedBookie = replaceBookieResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
            assertTrue("replaced : " + replacedBookie, addr3.toBookieId().equals(replacedBookie)
                    || addr4.toBookieId().equals(replacedBookie));
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
            selectionCounts.put(replacedBookie, selectionCounts.get(replacedBookie) + 1);
        }
        double observedMultiple = ((double) selectionCounts.get(addr4.toBookieId())
                / (double) selectionCounts.get(addr3.toBookieId()));
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr0.toBookieId());
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());

        int multiple = 10, maxMultiple = 4;
        conf.setDiskWeightBasedPlacementEnabled(true);
        conf.setBookieMaxWeightMultipleForWeightBasedPlacement(maxMultiple);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<BookieId, BookieInfo>();
        bookieInfoMap.put(addr0.toBookieId(), new BookieInfo(50L, 50L));
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(200L, 200L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(multiple * 50L, multiple * 50L));
        repp.updateBookieInfo(bookieInfoMap);

        Map<BookieId, Long> selectionCounts = new HashMap<BookieId, Long>();
        selectionCounts.put(addr0.toBookieId(), 0L);
        selectionCounts.put(addr1.toBookieId(), 0L);
        selectionCounts.put(addr2.toBookieId(), 0L);
        selectionCounts.put(addr3.toBookieId(), 0L);
        selectionCounts.put(addr4.toBookieId(), 0L);
        int numTries = 50000;
        EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse;
        BookieId replacedBookie;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        for (int i = 0; i < numTries; i++) {
            // addr2 is on /r2 and this is the only one on this rack. So the replacement
            // will come from other racks. However, the weight should be honored in such
            // selections as well
            replaceBookieResponse = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(),
                                                       addr2.toBookieId(), new HashSet<>());
            replacedBookie = replaceBookieResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
            assertTrue(addr0.toBookieId().equals(replacedBookie)
                    || addr1.toBookieId().equals(replacedBookie)
                    || addr3.toBookieId().equals(replacedBookie)
                    || addr4.toBookieId().equals(replacedBookie));
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
            selectionCounts.put(replacedBookie, selectionCounts.get(replacedBookie) + 1);
        }
        /*
         * since addr2 has to be replaced, the remaining bookies weight are - 50, 100, 200, 500 (10*50)
         * So the median calculated by WeightedRandomSelection is (100 + 200) / 2 = 150
         */
        double medianWeight = 150;
        double medianSelectionCounts = (double) (medianWeight / bookieInfoMap.get(addr1.toBookieId()).getWeight())
            * selectionCounts.get(addr1.toBookieId());
        double observedMultiple1 = ((double) selectionCounts.get(addr4.toBookieId())
                / (double) medianSelectionCounts);
        double observedMultiple2 = ((double) selectionCounts.get(addr4.toBookieId())
                / (double) selectionCounts.get(addr3.toBookieId()));
        LOG.info("oM1 " + observedMultiple1 + " oM2 " + observedMultiple2);
        assertTrue("Weights not being honored expected " + maxMultiple + " observed " + observedMultiple1,
                Math.abs(observedMultiple1 - maxMultiple) < 1);
        // expected multiple for addr3
        double expected = (medianWeight * maxMultiple) / bookieInfoMap.get(addr3.toBookieId()).getWeight();
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());
        addrs.add(addr7.toBookieId());
        addrs.add(addr8.toBookieId());
        addrs.add(addr9.toBookieId());

        int maxMultiple = 4;
        conf.setDiskWeightBasedPlacementEnabled(true);
        conf.setBookieMaxWeightMultipleForWeightBasedPlacement(maxMultiple);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<BookieId, BookieInfo>();
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr5.toBookieId(), new BookieInfo(1000L, 1000L));
        bookieInfoMap.put(addr6.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr7.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr8.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr9.toBookieId(), new BookieInfo(1000L, 1000L));

        repp.updateBookieInfo(bookieInfoMap);

        Map<BookieId, Long> selectionCounts = new HashMap<BookieId, Long>();
        for (BookieId b : addrs) {
            selectionCounts.put(b, 0L);
        }
        int numTries = 10000;

        Set<BookieId> excludeList = new HashSet<BookieId>();
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse;
        List<BookieId> ensemble;
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
                            + getNumCoveredWriteQuorums(ensemble, writeQuorumSize,
                                    conf.getMinNumRacksPerWriteQuorum(), repp.bookieAddressResolver),
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize,
                            conf.getMinNumRacksPerWriteQuorum(), repp.bookieAddressResolver) >= 2);
            for (BookieId b : ensemble) {
                selectionCounts.put(b, selectionCounts.get(b) + 1);
            }
        }

        // the median weight used is 100 since addr2 and addr6 have the same weight, we use their
        // selection counts as the same as median
        double observedMultiple1 = ((double) selectionCounts.get(addr5.toBookieId())
                / (double) selectionCounts.get(addr2.toBookieId()));
        double observedMultiple2 = ((double) selectionCounts.get(addr9.toBookieId())
                / (double) selectionCounts.get(addr6.toBookieId()));
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
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());

        int maxMultiple = 4;
        conf.setDiskWeightBasedPlacementEnabled(true);
        conf.setBookieMaxWeightMultipleForWeightBasedPlacement(maxMultiple);
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<BookieId, BookieInfo>();
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(1000L, 1000L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr5.toBookieId(), new BookieInfo(1000L, 1000L));

        repp.updateBookieInfo(bookieInfoMap);
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse;
        List<BookieId> ensemble;
        Set<BookieId> excludeList = new HashSet<BookieId>();
        try {
            excludeList.add(addr1.toBookieId());
            excludeList.add(addr2.toBookieId());
            excludeList.add(addr3.toBookieId());
            excludeList.add(addr4.toBookieId());
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

    static int getNumCoveredWriteQuorums(List<BookieId> ensemble, int writeQuorumSize,
            int minNumRacksPerWriteQuorumConfValue, BookieAddressResolver bookieAddressResolver) throws Exception {
        int ensembleSize = ensemble.size();
        int numCoveredWriteQuorums = 0;
        for (int i = 0; i < ensembleSize; i++) {
            Set<String> racks = new HashSet<String>();
            for (int j = 0; j < writeQuorumSize; j++) {
                int bookieIdx = (i + j) % ensembleSize;
                BookieId addr = ensemble.get(bookieIdx);
                racks.add(StaticDNSResolver.getRack(bookieAddressResolver.resolve(addr).getHostName()));
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        HashMap<BookieId, Long> bookieFailures = new HashMap<BookieId, Long>();

        bookieFailures.put(addr1.toBookieId(), 20L);
        bookieFailures.put(addr2.toBookieId(), 22L);

        // remove failure bookies: addr1 and addr2
        addrs = new HashSet<BookieId>();
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        DistributionSchedule.WriteSet reoderSet = repp.reorderReadSequence(
                ensemble, getBookiesHealthInfo(bookieFailures, new HashMap<>()), writeSet);
        LOG.info("reorder set : {}", reoderSet);
        assertEquals(ensemble.get(reoderSet.get(2)), addr1.toBookieId());
        assertEquals(ensemble.get(reoderSet.get(3)), addr2.toBookieId());
        assertEquals(ensemble.get(reoderSet.get(0)), addr3.toBookieId());
        assertEquals(ensemble.get(reoderSet.get(1)), addr4.toBookieId());
    }

    @Test
    public void testPlacementOnStabilizeNetworkTopology() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp = new RackawareEnsemblePlacementPolicy();
        ClientConfiguration confLocal = new ClientConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setNetworkTopologyStabilizePeriodSeconds(99999);
        repp.initialize(confLocal, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        // addr4 left
        addrs.remove(addr4.toBookieId());
        Set<BookieId> deadBookies = repp.onClusterChanged(addrs, new HashSet<BookieId>());
        assertTrue(deadBookies.isEmpty());

        // we will never use addr4 even it is in the stabilized network topology
        for (int i = 0; i < 5; i++) {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
                repp.newEnsemble(3, 2, 2, null, new HashSet<BookieId>());
            List<BookieId> ensemble = ensembleResponse.getResult();
            PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
            assertFalse(ensemble.contains(addr4.toBookieId()));
            assertEquals(PlacementPolicyAdherence.FAIL, isEnsembleAdheringToPlacementPolicy);
        }

        // we could still use addr4 for urgent allocation if it is just bookie flapping
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse =
            repp.newEnsemble(4, 2, 2, null, new HashSet<BookieId>());
        List<BookieId> ensemble = ensembleResponse.getResult();
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
        assertEquals(PlacementPolicyAdherence.FAIL, isEnsembleAdheringToPlacementPolicy);
        assertTrue(ensemble.contains(addr4.toBookieId()));
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
    public void testUpdateTopologyWithRackChange() throws Exception {
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
        StaticDNSResolver.addNodeToRack(newAddr2.getHostName(), defaultRackForThisTest);
        StaticDNSResolver.addNodeToRack(newAddr3.getHostName(), defaultRackForThisTest);
        StaticDNSResolver.addNodeToRack(newAddr4.getHostName(), defaultRackForThisTest);

        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger("");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping> empty(), timer,
                DISABLE_ALL, statsLogger, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(defaultRackForThisTest);

        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        Set<BookieId> writeableBookies = new HashSet<>();
        Set<BookieId> readOnlyBookies = new HashSet<>();
        writeableBookies.add(newAddr1.toBookieId());
        writeableBookies.add(newAddr2.toBookieId());
        writeableBookies.add(newAddr3.toBookieId());
        writeableBookies.add(newAddr4.toBookieId());
        repp.onClusterChanged(writeableBookies, readOnlyBookies);
        // only writable bookie - newAddr1 in default rack
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 4, numBookiesInDefaultRackGauge.getSample());

        // newAddr4 rack is changed and it is not in default anymore
        StaticDNSResolver
                .changeRack(Collections.singletonList(newAddr3), Collections.singletonList("/default-region/r4"));
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 3, numBookiesInDefaultRackGauge.getSample());

        StaticDNSResolver
                .changeRack(Collections.singletonList(newAddr1), Collections.singletonList(defaultRackForThisTest));
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 3, numBookiesInDefaultRackGauge.getSample());
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping> empty(), timer,
                DISABLE_ALL, statsLogger, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(defaultRackForThisTest);

        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        Set<BookieId> writeableBookies = new HashSet<BookieId>();
        writeableBookies.add(newAddr1.toBookieId());
        writeableBookies.add(newAddr2.toBookieId());
        Set<BookieId> readOnlyBookies = new HashSet<BookieId>();
        readOnlyBookies.add(newAddr3.toBookieId());
        readOnlyBookies.add(newAddr4.toBookieId());
        repp.onClusterChanged(writeableBookies, readOnlyBookies);
        // only writable bookie - newAddr1 in default rack
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 1, numBookiesInDefaultRackGauge.getSample());

        readOnlyBookies.remove(newAddr4.toBookieId());
        writeableBookies.add(newAddr4.toBookieId());
        repp.onClusterChanged(writeableBookies, readOnlyBookies);
        // newAddr4 is also added to writable bookie so 2 writable bookies -
        // newAddr1 and newAddr4
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 2, numBookiesInDefaultRackGauge.getSample());

        // newAddr4 rack is changed and it is not in default anymore
        StaticDNSResolver
            .changeRack(Collections.singletonList(newAddr4), Collections.singletonList("/default-region/r4"));
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 1, numBookiesInDefaultRackGauge.getSample());

        writeableBookies.clear();
        // writeableBookies is empty so 0 writable bookies in default rack
        repp.onClusterChanged(writeableBookies, readOnlyBookies);
        assertEquals("NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK guage value", 0, numBookiesInDefaultRackGauge.getSample());

        StaticDNSResolver
            .changeRack(Collections.singletonList(newAddr1), Collections.singletonList("/default-region/r2"));
        readOnlyBookies.clear();
        writeableBookies.add(newAddr1.toBookieId());
        writeableBookies.add(newAddr2.toBookieId());
        writeableBookies.add(newAddr3.toBookieId());
        writeableBookies.add(newAddr4.toBookieId());
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
        repp.initialize(clientConf, Optional.<DNSToSwitchMapping> empty(), timer,
                DISABLE_ALL, statsLogger, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(defaultRackForThisTest);
        Gauge<? extends Number> numBookiesInDefaultRackGauge = statsLogger
                .getGauge(BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK);

        int writeQuorumSize = 3;
        int ackQuorumSize = 3;
        int effectiveMinNumRacksPerWriteQuorum = Math.min(minNumRacksPerWriteQuorum, writeQuorumSize);

        int numOfRacks = 2 * effectiveMinNumRacksPerWriteQuorum - 1;
        int numOfBookiesPerRack = 20;
        BookieId[] bookieSocketAddresses = new BookieId[numOfRacks * numOfBookiesPerRack];

        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieSocketAddresses[index] = new BookieSocketAddress("128.0.0." + index, 3181).toBookieId();
                StaticDNSResolver.addNodeToRack("128.0.0." + index, "/default-region/r" + i);
            }
        }

        int numOfBookiesInDefaultRack = 10;
        BookieId[] bookieSocketAddressesInDefaultRack = new BookieId[numOfBookiesInDefaultRack];
        for (int i = 0; i < numOfBookiesInDefaultRack; i++) {
            bookieSocketAddressesInDefaultRack[i] = new BookieSocketAddress("127.0.0." + (i + 100), 3181).toBookieId();
            StaticDNSResolver.addNodeToRack("127.0.0." + (i + 100), defaultRackForThisTest);
        }

        Set<BookieId> writableBookies = new HashSet<BookieId>(
                Arrays.asList(bookieSocketAddresses));
        writableBookies.addAll(Arrays.asList(bookieSocketAddressesInDefaultRack));
        repp.onClusterChanged(writableBookies, new HashSet<BookieId>());
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
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> ensembleResponse;
        List<BookieId> ensemble;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        for (int ensembleSize = effectiveMinNumRacksPerWriteQuorum; ensembleSize < 40; ensembleSize++) {
            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
            ensemble = ensembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
            assertEquals("Number of writeQuorum sets covered", ensembleSize,
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);

            ensembleResponse = repp.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, null, new HashSet<>());
            ensemble = ensembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = ensembleResponse.getAdheringToPolicy();
            assertEquals("Number of writeQuorum sets covered", ensembleSize,
                    getNumCoveredWriteQuorums(ensemble, writeQuorumSize, clientConf.getMinNumRacksPerWriteQuorum(),
                                                      repp.bookieAddressResolver));
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, isEnsembleAdheringToPlacementPolicy);
            Collection<BookieId> bookiesOfDefaultRackInEnsemble = CollectionUtils
                    .intersection(Arrays.asList(bookieSocketAddressesInDefaultRack), ensemble);
            assertTrue("Ensemble is not supposed to contain bookies from default rack, but ensemble contains - "
                    + bookiesOfDefaultRackInEnsemble, bookiesOfDefaultRackInEnsemble.isEmpty());
        }
    }

    private void testAreAckedBookiesAdheringToPlacementPolicyHelper(int minNumRacksPerWriteQuorumConfValue,
                                                                    int ensembleSize,
                                                                    int writeQuorumSize,
                                                                    int ackQuorumSize,
                                                                    int numOfBookiesInDefaultRack,
                                                                    int numOfRacks,
                                                                    int numOfBookiesPerRack) throws Exception {
        String defaultRackForThisTest = NetworkTopology.DEFAULT_REGION_AND_RACK;
        repp.uninitalize();
        updateMyRack(defaultRackForThisTest);

        ClientConfiguration conf = new ClientConfiguration(this.conf);
        conf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);

        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger("");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.empty(), timer,
                DISABLE_ALL, statsLogger, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(defaultRackForThisTest);

        List<BookieId> bookieSocketAddressesDefaultRack = new ArrayList<>();
        List<BookieId> bookieSocketAddressesNonDefaultRack = new ArrayList<>();
        Set<BookieId> writableBookies;
        Set<BookieId> bookiesForEntry = new HashSet<>();

        for (int i = 0; i < numOfRacks; i++) {
            for (int j = 0; j < numOfBookiesPerRack; j++) {
                int index = i * numOfBookiesPerRack + j;
                bookieSocketAddressesNonDefaultRack.add(new BookieSocketAddress("128.0.0." + index, 3181).toBookieId());
                StaticDNSResolver.addNodeToRack("128.0.0." + index, "/default-region/r" + i);
            }
        }

        for (int i = 0; i < numOfBookiesInDefaultRack; i++) {
            bookieSocketAddressesDefaultRack.add(new BookieSocketAddress("127.0.0." + (i + 100), 3181).toBookieId());
            StaticDNSResolver.addNodeToRack("127.0.0." + (i + 100), defaultRackForThisTest);
        }

        writableBookies = new HashSet<>(bookieSocketAddressesNonDefaultRack);
        writableBookies.addAll(bookieSocketAddressesDefaultRack);
        repp.onClusterChanged(writableBookies, new HashSet<>());

        // Case 1 : Bookies in the ensemble from the same rack.
        // Manually crafting the ensemble here to create the error case when the check should return false

        List<BookieId> ensemble = new ArrayList<>(bookieSocketAddressesDefaultRack);
        for (int entryId = 0; entryId < 10; entryId++) {
            DistributionSchedule ds = new RoundRobinDistributionSchedule(writeQuorumSize, ackQuorumSize, ensembleSize);
            DistributionSchedule.WriteSet ws = ds.getWriteSet(entryId);

            for (int i = 0; i < ws.size(); i++) {
                bookiesForEntry.add(ensemble.get(ws.get(i)));
            }

            assertFalse(repp.areAckedBookiesAdheringToPlacementPolicy(bookiesForEntry, writeQuorumSize, ackQuorumSize));
        }

        // Case 2 : Bookies in the ensemble from the different racks

        EnsemblePlacementPolicy.PlacementResult<List<BookieId>>
                ensembleResponse = repp.newEnsemble(ensembleSize,
                                                    writeQuorumSize,
                                                    ackQuorumSize,
                                                    null,
                                                    new HashSet<>());
        ensemble = ensembleResponse.getResult();
        for (int entryId = 0; entryId < 10; entryId++) {
            DistributionSchedule ds = new RoundRobinDistributionSchedule(writeQuorumSize, ackQuorumSize, ensembleSize);
            DistributionSchedule.WriteSet ws = ds.getWriteSet(entryId);

            for (int i = 0; i < ws.size(); i++) {
                bookiesForEntry.add(ensemble.get(ws.get(i)));
            }

            assertTrue(repp.areAckedBookiesAdheringToPlacementPolicy(bookiesForEntry, writeQuorumSize, ackQuorumSize));
        }
    }

    /**
     * This tests areAckedBookiesAdheringToPlacementPolicy function in RackawareEnsemblePlacementPolicy.
     */
    @Test
    public void testAreAckedBookiesAdheringToPlacementPolicy() throws Exception {
        testAreAckedBookiesAdheringToPlacementPolicyHelper(2, 7, 3, 2, 7, 3, 3);
        testAreAckedBookiesAdheringToPlacementPolicyHelper(4, 6, 3, 2, 6, 3, 3);
        testAreAckedBookiesAdheringToPlacementPolicyHelper(5, 7, 5, 3, 7, 5, 2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReplaceToAdherePlacementPolicy() throws Exception {
        final BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        final BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        final BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        final BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        final BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        final BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);
        final BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.7", 3181);
        final BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.8", 3181);
        final BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.9", 3181);

        final String rackName1 = NetworkTopology.DEFAULT_REGION + "/r1";
        final String rackName2 = NetworkTopology.DEFAULT_REGION + "/r2";
        final String rackName3 = NetworkTopology.DEFAULT_REGION + "/r3";

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr5.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr6.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr7.getSocketAddress().getAddress().getHostAddress(), rackName3);
        StaticDNSResolver.addNodeToRack(addr8.getSocketAddress().getAddress().getHostAddress(), rackName3);
        StaticDNSResolver.addNodeToRack(addr9.getSocketAddress().getAddress().getHostAddress(), rackName3);

        // Update cluster
        final Set<BookieId> addrs = new HashSet<>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());
        addrs.add(addr7.toBookieId());
        addrs.add(addr8.toBookieId());
        addrs.add(addr9.toBookieId());

        final ClientConfiguration newConf = new ClientConfiguration(conf);
        newConf.setDiskWeightBasedPlacementEnabled(false);
        newConf.setMinNumRacksPerWriteQuorum(2);
        newConf.setEnforceMinNumRacksPerWriteQuorum(true);

        repp.initialize(newConf, Optional.empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<>());
        final Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<>();
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr5.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr6.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr7.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr8.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr9.toBookieId(), new BookieInfo(100L, 100L));

        repp.updateBookieInfo(bookieInfoMap);

        final Set<BookieId> excludeList = new HashSet<>();
        final int ensembleSize = 7;
        final int writeQuorumSize = 2;
        final int ackQuorumSize = 2;

        final BookieRackMatcher rack1 = new BookieRackMatcher(rackName1);
        final BookieRackMatcher rack2 = new BookieRackMatcher(rackName2);
        final BookieRackMatcher rack3 = new BookieRackMatcher(rackName3);
        final BookieRackMatcher rack12 = new BookieRackMatcher(rackName1, rackName2);
        final BookieRackMatcher rack13 = new BookieRackMatcher(rackName1, rackName3);
        final BookieRackMatcher rack23 = new BookieRackMatcher(rackName2, rackName3);
        final BookieRackMatcher rack123 = new BookieRackMatcher(rackName1, rackName2, rackName3);
        final Consumer<Pair<List<BookieId>, Matcher<Iterable<? extends BookieId>>>> test = (pair) -> {
            // RackawareEnsemblePlacementPolicyImpl#isEnsembleAdheringToPlacementPolicy
            // is not scope of this test case. So, use the method in assertion for convenience.
            assertEquals(PlacementPolicyAdherence.FAIL,
                    repp.isEnsembleAdheringToPlacementPolicy(pair.getLeft(), writeQuorumSize, ackQuorumSize));
            final EnsemblePlacementPolicy.PlacementResult<List<BookieId>> result =
                    repp.replaceToAdherePlacementPolicy(ensembleSize, writeQuorumSize, ackQuorumSize,
                            excludeList, pair.getLeft());
            if (LOG.isDebugEnabled()) {
                LOG.debug("input: {}, result: {}", pair.getLeft(), result.getResult());
            }
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, result.getAdheringToPolicy());
            assertThat(result.getResult(), pair.getRight());
        };

        for (int i = 0; i < 1000; i++) {
            test.accept(Pair.of(Arrays.asList(addr1.toBookieId(), addr4.toBookieId(), addr7.toBookieId(),
                            addr2.toBookieId(), addr5.toBookieId(), addr8.toBookieId(), addr9.toBookieId()),
                    // first, same, same, same, same, same, condition[0]
                    contains(is(addr1.toBookieId()), is(addr4.toBookieId()), is(addr7.toBookieId()),
                            is(addr2.toBookieId()), is(addr5.toBookieId()), is(addr8.toBookieId()),
                            is(addr6.toBookieId()))));

            test.accept(Pair.of(Arrays.asList(addr6.toBookieId(), addr4.toBookieId(), addr7.toBookieId(),
                            addr2.toBookieId(), addr5.toBookieId(), addr8.toBookieId(), addr3.toBookieId()),
                    // first, condition[0], same, same, same, same, same
                    contains(is(addr6.toBookieId()), is(addr1.toBookieId()), is(addr7.toBookieId()),
                            is(addr2.toBookieId()), is(addr5.toBookieId()), is(addr8.toBookieId()),
                            is(addr3.toBookieId()))));

            test.accept(Pair.of(Arrays.asList(addr1.toBookieId(), addr2.toBookieId(), addr3.toBookieId(),
                            addr4.toBookieId(), addr5.toBookieId(), addr6.toBookieId(), addr7.toBookieId()),
                    // first, candidate[0], same, same, candidate[0], same, same
                    contains(is(addr1.toBookieId()), is(rack3), is(addr3.toBookieId()),
                            is(addr4.toBookieId()), is(rack13), is(addr6.toBookieId()), is(addr7.toBookieId()))));

            test.accept(Pair.of(Arrays.asList(addr1.toBookieId(), addr2.toBookieId(), addr4.toBookieId(),
                            addr5.toBookieId(), addr7.toBookieId(), addr8.toBookieId(), addr9.toBookieId()),
                    contains(is(addr1.toBookieId()), is(rack23), is(rack123), is(rack123),
                            is(rack123), is(rack123), is(rack23))));
        }
        StaticDNSResolver.reset();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReplaceToAdherePlacementPolicyWithOutOfOrder() throws Exception {
        final BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        final BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        final BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        final BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        final BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        final BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);

        final String rackName1 = NetworkTopology.DEFAULT_REGION + "/r1";
        final String rackName2 = NetworkTopology.DEFAULT_REGION + "/r2";

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr5.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr6.getSocketAddress().getAddress().getHostAddress(), rackName2);

        // Update cluster
        final Set<BookieId> addrs = new HashSet<>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());

        final ClientConfiguration newConf = new ClientConfiguration(conf);
        newConf.setDiskWeightBasedPlacementEnabled(false);
        newConf.setMinNumRacksPerWriteQuorum(2);
        newConf.setEnforceMinNumRacksPerWriteQuorum(true);

        repp.initialize(newConf, Optional.empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<>());
        final Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<>();
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr5.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr6.toBookieId(), new BookieInfo(100L, 100L));

        repp.updateBookieInfo(bookieInfoMap);

        final Set<BookieId> excludeList = new HashSet<>();
        final int ensembleSize = 6;
        final int writeQuorumSize = 2;
        final int ackQuorumSize = 2;

        final Consumer<Pair<List<BookieId>, Matcher<Iterable<? extends BookieId>>>> test = (pair) -> {
            // RackawareEnsemblePlacementPolicyImpl#isEnsembleAdheringToPlacementPolicy
            // is not scope of this test case. So, use the method in assertion for convenience.
            assertEquals(PlacementPolicyAdherence.FAIL,
                    repp.isEnsembleAdheringToPlacementPolicy(pair.getLeft(), writeQuorumSize, ackQuorumSize));
            final EnsemblePlacementPolicy.PlacementResult<List<BookieId>> result =
                    repp.replaceToAdherePlacementPolicy(ensembleSize, writeQuorumSize, ackQuorumSize,
                            excludeList, pair.getLeft());
            if (LOG.isDebugEnabled()) {
                LOG.debug("input: {}, result: {}", pair.getLeft(), result.getResult());
            }
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, result.getAdheringToPolicy());
        };

        for (int i = 0; i < 1000; i++) {
            //All bookies already in the ensemble, the bookie order not adhere the placement policy.
            test.accept(Pair.of(Arrays.asList(addr1.toBookieId(), addr2.toBookieId(), addr3.toBookieId(),
                            addr4.toBookieId(), addr5.toBookieId(), addr6.toBookieId()),
                    //The result is not predict. We know the best min replace place is 2.
                    //1,2,3,4,5,6 => 1,5,3,4,2,6
                    //But maybe the final result is 1,6,3,4,2,5.
                    //When we from index 0 to replace, the first bookie(1) is /rack1, we only pick /rack2 bookie
                    //for the second bookie, so we can choose 4,5,6, the choice is random. If we pick 6 for the second,
                    // the final result is 1,6,3,4,2,5. If we pick 5 for the second, the final result is 1,5,3,4,2,6
                    null));
        }
        StaticDNSResolver.reset();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReplaceToAdherePlacementPolicyWithNoMoreRackBookie() throws Exception {
        final BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        final BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        final BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        final BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        final BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        final BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);

        final String rackName1 = NetworkTopology.DEFAULT_REGION + "/r1";
        final String rackName2 = NetworkTopology.DEFAULT_REGION + "/r2";

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr5.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr6.getSocketAddress().getAddress().getHostAddress(), rackName2);

        // Update cluster
        final Set<BookieId> addrs = new HashSet<>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());

        final ClientConfiguration newConf = new ClientConfiguration(conf);
        newConf.setDiskWeightBasedPlacementEnabled(false);
        newConf.setMinNumRacksPerWriteQuorum(2);
        newConf.setEnforceMinNumRacksPerWriteQuorum(true);

        repp.initialize(newConf, Optional.empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<>());
        final Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<>();
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr5.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr6.toBookieId(), new BookieInfo(100L, 100L));

        repp.updateBookieInfo(bookieInfoMap);

        final Set<BookieId> excludeList = new HashSet<>();
        final int ensembleSize = 3;
        final int writeQuorumSize = 2;
        final int ackQuorumSize = 2;

        final Consumer<Pair<List<BookieId>, Matcher<Iterable<? extends BookieId>>>> test = (pair) -> {
            // RackawareEnsemblePlacementPolicyImpl#isEnsembleAdheringToPlacementPolicy
            // is not scope of this test case. So, use the method in assertion for convenience.
            assertEquals(PlacementPolicyAdherence.FAIL,
                    repp.isEnsembleAdheringToPlacementPolicy(pair.getLeft(), writeQuorumSize, ackQuorumSize));
            final EnsemblePlacementPolicy.PlacementResult<List<BookieId>> result =
                    repp.replaceToAdherePlacementPolicy(ensembleSize, writeQuorumSize, ackQuorumSize,
                            excludeList, pair.getLeft());
            if (LOG.isDebugEnabled()) {
                LOG.debug("input: {}, result: {}", pair.getLeft(), result.getResult());
            }
            assertEquals(PlacementPolicyAdherence.FAIL, result.getAdheringToPolicy());
            assertEquals(0, result.getResult().size());
        };

        for (int i = 0; i < 1000; i++) {
            test.accept(Pair.of(Arrays.asList(addr1.toBookieId(), addr2.toBookieId(), addr4.toBookieId()),
                    null));
        }
        StaticDNSResolver.reset();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReplaceToAdherePlacementPolicyWithUnknowBookie() throws Exception {
        final BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        final BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        final BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        final BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        final BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        final BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);

        final String rackName1 = NetworkTopology.DEFAULT_REGION + "/r1";
        final String rackName2 = NetworkTopology.DEFAULT_REGION + "/r2";

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr5.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr6.getSocketAddress().getAddress().getHostAddress(), rackName2);

        // Update cluster
        final Set<BookieId> addrs = new HashSet<>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());

        final ClientConfiguration newConf = new ClientConfiguration(conf);
        newConf.setDiskWeightBasedPlacementEnabled(false);
        newConf.setMinNumRacksPerWriteQuorum(2);
        newConf.setEnforceMinNumRacksPerWriteQuorum(true);

        repp.initialize(newConf, Optional.empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);

        repp.onClusterChanged(addrs, new HashSet<>());
        final Map<BookieId, BookieInfo> bookieInfoMap = new HashMap<>();
        bookieInfoMap.put(addr1.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr4.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr5.toBookieId(), new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr6.toBookieId(), new BookieInfo(100L, 100L));

        repp.updateBookieInfo(bookieInfoMap);

        final Set<BookieId> excludeList = new HashSet<>();
        final int ensembleSize = 6;
        final int writeQuorumSize = 2;
        final int ackQuorumSize = 2;

        final BookieRackMatcher rack1 = new BookieRackMatcher(rackName1);

        final Consumer<Pair<List<BookieId>, Matcher<Iterable<? extends BookieId>>>> test = (pair) -> {
            // RackawareEnsemblePlacementPolicyImpl#isEnsembleAdheringToPlacementPolicy
            // is not scope of this test case. So, use the method in assertion for convenience.
            assertEquals(PlacementPolicyAdherence.FAIL,
                    repp.isEnsembleAdheringToPlacementPolicy(pair.getLeft(), writeQuorumSize, ackQuorumSize));
            final EnsemblePlacementPolicy.PlacementResult<List<BookieId>> result =
                    repp.replaceToAdherePlacementPolicy(ensembleSize, writeQuorumSize, ackQuorumSize,
                            excludeList, pair.getLeft());
            if (LOG.isDebugEnabled()) {
                LOG.debug("input: {}, result: {}", pair.getLeft(), result.getResult());
            }
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, result.getAdheringToPolicy());
            assertThat(result.getResult(), pair.getRight());
        };

        for (int i = 0; i < 1000; i++) {
            test.accept(Pair.of(Arrays.asList(BookieId.parse("127.0.0.10:3181"), BookieId.parse("127.0.0.11:3181"),
                            addr3.toBookieId(),
                            addr4.toBookieId(), addr5.toBookieId(), addr6.toBookieId()),
                    contains(is(rack1), is(addr5.toBookieId()), is(addr3.toBookieId()),
                            is(addr4.toBookieId()), is(rack1), is(addr6.toBookieId()))));
        }
        StaticDNSResolver.reset();
    }

    private static class BookieRackMatcher extends TypeSafeMatcher<BookieId> {
        final List<String> expectedRacks;

        public BookieRackMatcher(String... expectedRacks) {
            this.expectedRacks = Arrays.asList(expectedRacks);
        }

        @Override
        protected boolean matchesSafely(BookieId bookieId) {
            return expectedRacks.contains(StaticDNSResolver.getRack(bookieId.toString().split(":")[0]));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("expected racks " + expectedRacks);
        }
    }
}
