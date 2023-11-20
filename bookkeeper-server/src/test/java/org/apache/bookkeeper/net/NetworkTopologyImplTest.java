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
package org.apache.bookkeeper.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;
import org.junit.Test;

/**
 * Tests for {@link NetworkTopologyImpl}.
 */
public class NetworkTopologyImplTest {

  @Test
  public void getLeavesShouldReturnEmptySetForNonExistingScope() {
      NetworkTopologyImpl networkTopology = new NetworkTopologyImpl();
      final Set<Node> leaves = networkTopology.getLeaves("/non-existing-scope");
      assertTrue(leaves.isEmpty());
  }

  @Test
  public void getLeavesShouldReturnNodesInScope() {
      // GIVEN
      // Topology with two racks and 1 bookie in each rack.
      NetworkTopologyImpl networkTopology = new NetworkTopologyImpl();

      String rack0Scope = "/rack-0";
      BookieId bookieIdScopeRack0 = BookieId.parse("bookieIdScopeRack0");
      BookieNode bookieRack0ScopeNode = new BookieNode(bookieIdScopeRack0, rack0Scope);

      String rack1Scope = "/rack-1";
      BookieId bookieIdScopeRack1 = BookieId.parse("bookieIdScopeRack1");
      BookieNode bookieRack1ScopeNode = new BookieNode(bookieIdScopeRack1, rack1Scope);

      networkTopology.add(bookieRack0ScopeNode);
      networkTopology.add(bookieRack1ScopeNode);

      // WHEN
      Set<Node> leavesScopeRack0 = networkTopology.getLeaves(rack0Scope);
      Set<Node> leavesScopeRack1 = networkTopology.getLeaves(rack1Scope);

      // THEN
      assertTrue(leavesScopeRack0.size() == 1);
      assertTrue(leavesScopeRack0.contains(bookieRack0ScopeNode));

      assertTrue(leavesScopeRack1.size() == 1);
      assertTrue(leavesScopeRack1.contains(bookieRack1ScopeNode));
  }

  @Test
  public void testRestartBKWithNewRackDepth() {
      NetworkTopologyImpl networkTopology = new NetworkTopologyImpl();
      String dp1Rack = "/rack-1";
      String dp2Rack = "/dp/rack-1";
      BookieId bkId1 = BookieId.parse("bookieIdScopeRack0");
      BookieId bkId2 = BookieId.parse("bookieIdScopeRack1");

      // Register 2 BKs with depth 1 rack.
      BookieNode dp1BkNode1 = new BookieNode(bkId1, dp1Rack);
      BookieNode dp1BkNode2 = new BookieNode(bkId2, dp1Rack);
      networkTopology.add(dp1BkNode1);
      networkTopology.add(dp1BkNode2);

      // Update one BK with depth 2 rack.
      // Assert it can not be added due to different depth.
      networkTopology.remove(dp1BkNode1);
      BookieNode dp2BkNode1 = new BookieNode(bkId1, dp2Rack);
      try {
          networkTopology.add(dp2BkNode1);
          fail("Expected add node failed caused by different depth of rack");
      } catch (NetworkTopologyImpl.InvalidTopologyException ex) {
          // Expected ex.
      }
      Set<Node> leaves = networkTopology.getLeaves(dp1Rack);
      assertEquals(leaves.size(), 1);
      assertTrue(leaves.contains(dp1BkNode2));

      // Update all Bks with depth 2 rack.
      // Verify update success.
      networkTopology.remove(dp1BkNode2);
      BookieNode dp2BkNode2 = new BookieNode(bkId2, dp2Rack);
      networkTopology.add(dp2BkNode1);
      networkTopology.add(dp2BkNode2);
      leaves = networkTopology.getLeaves(dp2Rack);
      assertEquals(leaves.size(), 2);
      assertTrue(leaves.contains(dp2BkNode1));
      assertTrue(leaves.contains(dp2BkNode2));
  }

  @Test
  public void getLeavesShouldReturnLeavesThatAreNotInExcludedScope() {
      // GIVEN
      // Topology with three racks and 1 bookie in each rack.
      NetworkTopologyImpl networkTopology = new NetworkTopologyImpl();

      String rack0Scope = "/rack-0";
      BookieId bookieIdScopeRack0 = BookieId.parse("bookieIdScopeRack0");
      BookieNode bookieRack0ScopeNode = new BookieNode(bookieIdScopeRack0, rack0Scope);

      String rack1Scope = "/rack-1";
      BookieId bookieIdScopeRack1 = BookieId.parse("bookieIdScopeRack1");
      BookieNode bookieRack1ScopeNode = new BookieNode(bookieIdScopeRack1, rack1Scope);

      String rack2Scope = "/rack-2";
      BookieId bookieIdScopeRack2 = BookieId.parse("bookieIdScopeRack2");
      BookieNode bookieRack2ScopeNode = new BookieNode(bookieIdScopeRack2, rack2Scope);

      networkTopology.add(bookieRack0ScopeNode);
      networkTopology.add(bookieRack1ScopeNode);
      networkTopology.add(bookieRack2ScopeNode);

      // Excluded scopes are beginned with '~' character.
      String scopeExcludingRack1 = "~/rack-1";

      // WHEN
      // ask for leaves not being at rack1 scope.
      Set<Node> leavesExcludingRack2Scope = networkTopology.getLeaves(scopeExcludingRack1);

      // THEN
      assertTrue(leavesExcludingRack2Scope.size() == 2);
      assertTrue(leavesExcludingRack2Scope.contains(bookieRack0ScopeNode));
      assertTrue(leavesExcludingRack2Scope.contains(bookieRack2ScopeNode));
  }

  @Test
  public void testInvalidRackName() {
      NetworkTopologyImpl networkTopology = new NetworkTopologyImpl();
      String rack0Scope = "";
      BookieId bookieIdScopeRack0 = BookieId.parse("bookieIdScopeRack0");
      BookieNode bookieRack0ScopeNode = new BookieNode(bookieIdScopeRack0, rack0Scope);

      String rack1Scope = "/";
      BookieId bookieIdScopeRack1 = BookieId.parse("bookieIdScopeRack1");
      BookieNode bookieRack1ScopeNode = new BookieNode(bookieIdScopeRack1, rack1Scope);

      try {
          networkTopology.add(bookieRack0ScopeNode);
          fail();
      } catch (IllegalArgumentException e) {
          assertEquals("bookieIdScopeRack0, which is located at , is not a decendent of /", e.getMessage());
      }

      try {
          networkTopology.add(bookieRack1ScopeNode);
          fail();
      } catch (IllegalArgumentException e) {
          assertEquals("bookieIdScopeRack1, which is located at , is not a decendent of /", e.getMessage());
      }

  }
}