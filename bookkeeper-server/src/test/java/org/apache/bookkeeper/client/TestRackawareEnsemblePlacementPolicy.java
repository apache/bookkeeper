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
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import junit.framework.TestCase;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRackawareEnsemblePlacementPolicy extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestRackawareEnsemblePlacementPolicy.class);

    RackawareEnsemblePlacementPolicy repp;
    final ArrayList<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
    final List<Integer> writeSet = new ArrayList<Integer>();
    ClientConfiguration conf = new ClientConfiguration();
    BookieSocketAddress addr1, addr2, addr3, addr4;
    HashedWheelTimer timer;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        StaticDNSResolver.reset();
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack("127.0.0.1", NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack("localhost", NetworkTopology.DEFAULT_RACK);
        LOG.info("Set up static DNS Resolver.");
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_REGION + "/rack1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), NetworkTopology.DEFAULT_REGION + "/rack2");
        ensemble.add(addr1);
        ensemble.add(addr2);
        ensemble.add(addr3);
        ensemble.add(addr4);
        for (int i = 0; i < 4; i++) {
            writeSet.add(i);
        }

        timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getTimeoutTimerNumTicks());

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>absent(), timer, DISABLE_ALL, null);
    }

    @Override
    protected void tearDown() throws Exception {
        repp.uninitalize();
        super.tearDown();
    }

    static void updateMyRack(String rack) throws Exception {
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), rack);
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostName(), rack);
        StaticDNSResolver.addNodeToRack("127.0.0.1", rack);
        StaticDNSResolver.addNodeToRack("localhost", rack);
    }

    @Test(timeout = 60000)
    public void testNodeDown() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_RACK);

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>absent(), timer, DISABLE_ALL, null);

        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        List<Integer> reoderSet = repp.reorderReadSequence(ensemble, writeSet, new HashMap<BookieSocketAddress, Long>());
        List<Integer> expectedSet = new ArrayList<Integer>();
        expectedSet.add(1);
        expectedSet.add(2);
        expectedSet.add(3);
        expectedSet.add(0);
        LOG.info("reorder set : {}", reoderSet);
        assertFalse(reoderSet == writeSet);
        assertEquals(expectedSet, reoderSet);
    }

    @Test(timeout = 60000)
    public void testNodeReadOnly() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>absent(), timer, DISABLE_ALL, null);

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

        List<Integer> reoderSet = repp.reorderReadSequence(ensemble, writeSet, new HashMap<BookieSocketAddress, Long>());
        List<Integer> expectedSet = new ArrayList<Integer>();
        expectedSet.add(1);
        expectedSet.add(2);
        expectedSet.add(3);
        expectedSet.add(0);
        LOG.info("reorder set : {}", reoderSet);
        assertFalse(reoderSet == writeSet);
        assertEquals(expectedSet, reoderSet);
    }

    @Test(timeout = 60000)
    public void testTwoNodesDown() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>absent(), timer, DISABLE_ALL, null);

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

        List<Integer> reoderSet = repp.reorderReadSequence(ensemble, writeSet, new HashMap<BookieSocketAddress, Long>());
        List<Integer> expectedSet = new ArrayList<Integer>();
        expectedSet.add(2);
        expectedSet.add(3);
        expectedSet.add(0);
        expectedSet.add(1);
        LOG.info("reorder set : {}", reoderSet);
        assertFalse(reoderSet == writeSet);
        assertEquals(expectedSet, reoderSet);
    }

    @Test(timeout = 60000)
    public void testNodeDownAndReadOnly() throws Exception {
        repp.uninitalize();
        updateMyRack("/r1/rack1");

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>absent(), timer, DISABLE_ALL, null);

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
        List<Integer> reoderSet = repp.reorderReadSequence(ensemble, writeSet, new HashMap<BookieSocketAddress, Long>());
        List<Integer> expectedSet = new ArrayList<Integer>();
        expectedSet.add(2);
        expectedSet.add(3);
        expectedSet.add(1);
        expectedSet.add(0);
        assertFalse(reoderSet == writeSet);
        assertEquals(expectedSet, reoderSet);
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithEnoughBookiesInSameRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_RACK);
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
        BookieSocketAddress replacedBookie = repp.replaceBookie(1, 1, 1, null, new HashSet<BookieSocketAddress>(), addr2, new HashSet<BookieSocketAddress>());
        assertEquals(addr3, replacedBookie);
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithEnoughBookiesInDifferentRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_RACK);
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
        BookieSocketAddress replacedBookie = repp.replaceBookie(1, 1, 1, null, new HashSet<BookieSocketAddress>(), addr2, excludedAddrs);

        assertFalse(addr1.equals(replacedBookie));
        assertTrue(addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithNotEnoughBookies() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_RACK);
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

    @Test(timeout = 60000)
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
            ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, 2, null, new HashSet<BookieSocketAddress>());
            assertEquals(0, getNumCoveredWriteQuorums(ensemble, 2));
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<BookieSocketAddress>());
            assertEquals(0, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test(timeout = 60000)
    public void testNewEnsembleWithMultipleRacks() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_RACK);
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
            ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, 2, null, new HashSet<BookieSocketAddress>());
            int numCovered = getNumCoveredWriteQuorums(ensemble, 2);
            assertTrue(numCovered >= 1 && numCovered < 3);
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<BookieSocketAddress>());
            numCovered = getNumCoveredWriteQuorums(ensemble2, 2);
            assertTrue(numCovered >= 1 && numCovered < 3);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test(timeout = 60000)
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
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/default-region/r2");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/default-region/r3");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/default-region/r4");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), NetworkTopology.DEFAULT_RACK);
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
            ArrayList<BookieSocketAddress> ensemble1 = repp.newEnsemble(3, 2, 2, null, new HashSet<BookieSocketAddress>());
            assertEquals(3, getNumCoveredWriteQuorums(ensemble1, 2));
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<BookieSocketAddress>());
            assertEquals(4, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    /**
     * Test for BOOKKEEPER-633
     */
    @Test(timeout = 60000)
    public void testRemoveBookieFromCluster() {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_RACK);
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

    @Test(timeout = 60000)
    public void testNodeWithFailures() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_RACK);

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>absent(), timer, DISABLE_ALL, null);

        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        HashMap<BookieSocketAddress, Long> bookieFailures = new HashMap<BookieSocketAddress, Long>();

        bookieFailures.put(addr1, 20L);
        bookieFailures.put(addr2, 22L);

        List<Integer> reoderSet = repp.reorderReadSequence(ensemble, writeSet, bookieFailures);
        LOG.info("reorder set : {}", reoderSet);
        assertEquals(ensemble.get(reoderSet.get(2)), addr1);
        assertEquals(ensemble.get(reoderSet.get(3)), addr2);
        assertEquals(ensemble.get(reoderSet.get(0)), addr3);
        assertEquals(ensemble.get(reoderSet.get(1)), addr4);
    }

    @Test(timeout = 60000)
    public void testPlacementOnStabilizeNetworkTopology() throws Exception {
        repp.uninitalize();
        updateMyRack(NetworkTopology.DEFAULT_RACK);

        repp = new RackawareEnsemblePlacementPolicy();
        ClientConfiguration confLocal = new ClientConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setNetworkTopologyStabilizePeriodSeconds(99999);
        repp.initialize(confLocal, Optional.<DNSToSwitchMapping>absent(), timer, DISABLE_ALL, null);

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
        for (int i = 0 ; i < 5; i++) {
            ArrayList<BookieSocketAddress> ensemble =
                    repp.newEnsemble(3, 3, 3, null, new HashSet<BookieSocketAddress>());
            assertFalse(ensemble.contains(addr4));
        }

        // we could still use addr4 for urgent allocation if it is just bookie flapping
        ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(4, 4, 4, null, new HashSet<BookieSocketAddress>());
        assertTrue(ensemble.contains(addr4));
    }
}
