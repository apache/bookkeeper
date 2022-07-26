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