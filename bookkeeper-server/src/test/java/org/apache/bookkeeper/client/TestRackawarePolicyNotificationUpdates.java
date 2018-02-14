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
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.HashedWheelTimer;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

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
public class TestRackawarePolicyNotificationUpdates extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestRackawarePolicyNotificationUpdates.class);

    RackawareEnsemblePlacementPolicy repp;
    HashedWheelTimer timer;
    ClientConfiguration conf = new ClientConfiguration();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());

        StaticDNSResolver.reset();
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(),
                NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack("127.0.0.1", NetworkTopology.DEFAULT_REGION_AND_RACK);
        StaticDNSResolver.addNodeToRack("localhost", NetworkTopology.DEFAULT_REGION_AND_RACK);
        LOG.info("Set up static DNS Resolver.");

        timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS, conf.getTimeoutTimerNumTicks());

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        repp.withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);
    }

    @Override
    protected void tearDown() throws Exception {
        repp.uninitalize();
        super.tearDown();
    }

    @Test
    public void testNotifyRackChange() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);

        // Update cluster
        Set<BookieSocketAddress> addrs = Sets.newHashSet(addr1, addr2, addr3, addr4);
        repp.onClusterChanged(addrs, new HashSet<>());

        // update dns mapping
        StaticDNSResolver.changeRack(Lists.newArrayList(addr1, addr2, addr3, addr4),
                Lists.newArrayList("/default-region/rack-1", "/default-region/rack-2", "/default-region/rack-2",
                        "/default-region/rack-2"));

        ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, 2, Collections.emptyMap(),
                Collections.emptySet());
        int numCovered = TestRackawareEnsemblePlacementPolicy.getNumCoveredWriteQuorums(ensemble, 2);
        assertTrue(numCovered >= 1 && numCovered < 3);
        assertTrue(ensemble.contains(addr1));

        List<BookieSocketAddress> bookieAddressList = new ArrayList<>();
        List<String> rackList = new ArrayList<>();
        bookieAddressList.add(addr2);
        rackList.add("/default-region/rack-3");
        StaticDNSResolver.changeRack(bookieAddressList, rackList);

        ensemble = repp.newEnsemble(3, 2, 1, Collections.emptyMap(), Collections.emptySet());
        assertEquals(3, TestRackawareEnsemblePlacementPolicy.getNumCoveredWriteQuorums(ensemble, 2));
        assertTrue(ensemble.contains(addr1));
        assertTrue(ensemble.contains(addr2));
    }
}
