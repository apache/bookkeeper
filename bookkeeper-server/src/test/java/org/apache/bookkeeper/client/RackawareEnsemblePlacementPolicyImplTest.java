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
 */

package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.TopologyAwareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.junit.Before;
import org.junit.Test;

public class RackawareEnsemblePlacementPolicyImplTest {

    private RackawareEnsemblePlacementPolicyImpl policy;
    private NetworkTopology topology;
    private Set<BookieId> excludeBookies;
    private BookieId bookie1;
    private BookieId bookie2;

    String DEFAULT_RACK = "/default-rack";

    @Before
    public void setUp() {
        TestStatsProvider provider = new TestStatsProvider();
        policy = new RackawareEnsemblePlacementPolicyImpl();
        ClientConfiguration clientConfiguration = mock(ClientConfiguration.class);
        when(clientConfiguration.getEnforceMinNumRacksPerWriteQuorum()).thenReturn(true);
        when(clientConfiguration.getString(REPP_DNS_RESOLVER_CLASS,
                ScriptBasedMapping.class.getName())).thenReturn(StaticDNSResolver.class.getName());
        policy.initialize(clientConfiguration,
                Optional.empty(),
                mock(io.netty.util.HashedWheelTimer.class),
                mock(FeatureProvider.class),
                provider.getStatsLogger(""),
                mock(BookieAddressResolver.class));
        topology = mock(NetworkTopology.class);
        excludeBookies = new HashSet<>();
        bookie1 = BookieId.parse("bookie1:3181");
        bookie2 = BookieId.parse("bookie2:3181");

        // Set the default rack
        when(topology.getLeaves(DEFAULT_RACK))
                .thenReturn(Collections.emptySet());

        // Use reflection to inject the mock topology into the policy
        TestUtils.setField(TopologyAwareEnsemblePlacementPolicy.class, policy, "topology", topology);
    }

    @Test
    public void testWithDefaultRackBookies() {
        // Scenario: bookie nodes exist in the default rack
        BookieNode node1 = new BookieNode(bookie1, DEFAULT_RACK);
        BookieNode node2 = new BookieNode(bookie2, DEFAULT_RACK);

        Set<Node> defaultRackNodes = new HashSet<>();
        defaultRackNodes.add(node1);
        defaultRackNodes.add(node2);

        when(topology.getLeaves(DEFAULT_RACK))
                .thenReturn(defaultRackNodes);

        // Add some bookies to be excluded
        excludeBookies.add(BookieId.parse("other:3181"));

        Set<BookieId> result = policy.addDefaultRackBookiesIfMinNumRacksIsEnforced(excludeBookies);

        // Verify: the result should contain the original exclusion set + all bookies from the default rack
        assertEquals(3, result.size());
        assertTrue(result.contains(bookie1));
        assertTrue(result.contains(bookie2));
        assertTrue(result.containsAll(excludeBookies));
    }

    @Test
    public void testMixedNodesInDefaultRack() {
        // Scenario: default rack contains mixed BookieNode and non-BookieNode nodes
        BookieNode bookieNode = new BookieNode(bookie1, DEFAULT_RACK);
        Node nonBookieNode = mock(Node.class);

        Set<Node> defaultRackNodes = new HashSet<>();
        defaultRackNodes.add(bookieNode);
        defaultRackNodes.add(nonBookieNode);

        when(topology.getLeaves(DEFAULT_RACK))
                .thenReturn(defaultRackNodes);

        Set<BookieId> result = policy.addDefaultRackBookiesIfMinNumRacksIsEnforced(excludeBookies);

        // Verify: the result should include the BookieNode but not the non-BookieNode
        assertEquals(1, result.size());
        assertTrue(result.contains(bookie1));
    }

    @Test
    public void testEmptyDefaultRackBookies() {
        // Scenario: the collection in default rack is empty
        Set<Node> emptySet = Collections.emptySet();
        when(topology.getLeaves(DEFAULT_RACK))
                .thenReturn(emptySet);

        // Add some bookies to be excluded
        excludeBookies.add(BookieId.parse("other:3181"));
        Set<BookieId> result = policy.addDefaultRackBookiesIfMinNumRacksIsEnforced(excludeBookies);

        // Verify: should return the original exclusion set
        assertSame(excludeBookies, result);
    }
}

// Utility class for setting private fields
class TestUtils {
    public static void setField(Class<?> tclass , Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = tclass.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
