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

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.shuffleWithMask;
import static org.apache.bookkeeper.client.RoundRobinDistributionSchedule.writeSetFromValues;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.HashedWheelTimer;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the rackaware ensemble placement policy.
 */
public class TestRackawareEnsemblePlacementPolicy extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestRackawareEnsemblePlacementPolicy.class);

    RackawareEnsemblePlacementPolicy repp;
    final ArrayList<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
    DistributionSchedule.WriteSet writeSet = DistributionSchedule.NULL_WRITE_SET;
    ClientConfiguration conf = new ClientConfiguration();
    BookieSocketAddress addr1, addr2, addr3, addr4;
    io.netty.util.HashedWheelTimer timer;

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
        BookieSocketAddress replacedBookie = repp.replaceBookie(1, 1, 1, null, new HashSet<>(), addr2, new HashSet<>());
        assertEquals(addr3, replacedBookie);
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
        BookieSocketAddress replacedBookie = repp.replaceBookie(1, 1, 1, null, new HashSet<>(), addr2, excludedAddrs);

        assertFalse(addr1.equals(replacedBookie));
        assertTrue(addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
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
            repp.replaceBookie(1, 1, 1, null, new HashSet<BookieSocketAddress>(), addr2, excludedAddrs);
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
        } catch (BKNotEnoughBookiesException bnebe) {
            // should throw not enou
        }
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInSameRackAsEnsemble() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
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
        Set<BookieSocketAddress> ensembleBookies = new HashSet<BookieSocketAddress>();
        ensembleBookies.add(addr2);
        ensembleBookies.add(addr4);
        BookieSocketAddress replacedBookie = repp.replaceBookie(
            1, 1, 1 , null,
            ensembleBookies,
            addr4,
            new HashSet<>());
        assertEquals(addr1, replacedBookie);
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
            ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, 2, null, new HashSet<>());
            assertEquals(0, getNumCoveredWriteQuorums(ensemble, 2));
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<>());
            assertEquals(0, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
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
            ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, 2, null, new HashSet<>());
            int numCovered = getNumCoveredWriteQuorums(ensemble, 2);
            assertTrue(numCovered >= 1 && numCovered < 3);
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<>());
            numCovered = getNumCoveredWriteQuorums(ensemble2, 2);
            assertTrue(numCovered >= 1 && numCovered < 3);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
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
            ArrayList<BookieSocketAddress> ensemble1 = repp.newEnsemble(3, 2, 2, null, new HashSet<>());
            assertEquals(3, getNumCoveredWriteQuorums(ensemble1, 2));
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<>());
            assertEquals(4, getNumCoveredWriteQuorums(ensemble2, 2));
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
        BookieSocketAddress replacedBookie;
        for (int i = 0; i < numTries; i++) {
            // replace node under r2
            replacedBookie = repp.replaceBookie(1, 1, 1, null, new HashSet<>(), addr2, new HashSet<>());
            assertTrue("replaced : " + replacedBookie, addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
            selectionCounts.put(replacedBookie, selectionCounts.get(replacedBookie) + 1);
        }
        double observedMultiple = ((double) selectionCounts.get(addr4) / (double) selectionCounts.get(addr3));
        assertTrue("Weights not being honored " + observedMultiple, Math.abs(observedMultiple - multiple) < 1);
    }

    @Test
    public void testWeightedPlacementAndReplaceBookieWithoutEnoughBookiesInSameRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.reset();
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
        bookieInfoMap.put(addr1, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr2, new BookieInfo(100L, 100L));
        bookieInfoMap.put(addr3, new BookieInfo(200L, 200L));
        bookieInfoMap.put(addr4, new BookieInfo(multiple * 100L, multiple * 100L));
        repp.updateBookieInfo(bookieInfoMap);

        Map<BookieSocketAddress, Long> selectionCounts = new HashMap<BookieSocketAddress, Long>();
        selectionCounts.put(addr1, 0L);
        selectionCounts.put(addr2, 0L);
        selectionCounts.put(addr3, 0L);
        selectionCounts.put(addr4, 0L);
        int numTries = 50000;
        BookieSocketAddress replacedBookie;
        for (int i = 0; i < numTries; i++) {
            // addr2 is on /r2 and this is the only one on this rack. So the replacement
            // will come from other racks. However, the weight should be honored in such
            // selections as well
            replacedBookie = repp.replaceBookie(1, 1, 1, null, new HashSet<>(), addr2, new HashSet<>());
            assertTrue(addr1.equals(replacedBookie) || addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
            selectionCounts.put(replacedBookie, selectionCounts.get(replacedBookie) + 1);
        }

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
        ArrayList<BookieSocketAddress> ensemble;
        for (int i = 0; i < numTries; i++) {
            // addr2 is on /r2 and this is the only one on this rack. So the replacement
            // will come from other racks. However, the weight should be honored in such
            // selections as well
            ensemble = repp.newEnsemble(3, 2, 2, null, excludeList);
            assertTrue("Rackaware selection not happening " + getNumCoveredWriteQuorums(ensemble, 2),
                    getNumCoveredWriteQuorums(ensemble, 2) >= 2);
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

        ArrayList<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
        Set<BookieSocketAddress> excludeList = new HashSet<BookieSocketAddress>();
        try {
            excludeList.add(addr1);
            excludeList.add(addr2);
            excludeList.add(addr3);
            excludeList.add(addr4);
            ensemble = repp.newEnsemble(3, 2, 2, null, excludeList);
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies" + ensemble);
        } catch (BKNotEnoughBookiesException e) {
            // this is expected
        }
        try {
            ensemble = repp.newEnsemble(1, 1, 1, null, excludeList);
        } catch (BKNotEnoughBookiesException e) {
            fail("Should not throw BKNotEnoughBookiesException when there are enough bookies for the ensemble");
        }
    }

    private int getNumCoveredWriteQuorums(ArrayList<BookieSocketAddress> ensemble, int writeQuorumSize)
            throws Exception {
        int ensembleSize = ensemble.size();
        int numCoveredWriteQuorums = 0;
        for (int i = 0; i < ensembleSize; i++) {
            Set<String> racks = new HashSet<String>();
            for (int j = 0; j < writeQuorumSize; j++) {
                int bookieIdx = (i + j) % ensembleSize;
                BookieSocketAddress addr = ensemble.get(bookieIdx);
                racks.add(StaticDNSResolver.getRack(addr.getHostName()));
            }
            numCoveredWriteQuorums += (racks.size() > 1 ? 1 : 0);
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
            ArrayList<BookieSocketAddress> ensemble =
                    repp.newEnsemble(3, 3, 3, null, new HashSet<BookieSocketAddress>());
            assertFalse(ensemble.contains(addr4));
        }

        // we could still use addr4 for urgent allocation if it is just bookie flapping
        ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(4, 4, 4, null, new HashSet<BookieSocketAddress>());
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

}
