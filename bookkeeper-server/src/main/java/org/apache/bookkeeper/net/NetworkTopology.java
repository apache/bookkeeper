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

import java.util.Collection;
import java.util.Set;

/**
 * Network Topology Interface.
 */
public interface NetworkTopology {

    String DEFAULT_REGION = "/default-region";
    String DEFAULT_RACK = "/default-rack";
    String DEFAULT_ZONE = "/default-zone";
    String DEFAULT_UPGRADEDOMAIN = "/default-upgradedomain";
    String DEFAULT_ZONE_AND_UPGRADEDOMAIN = DEFAULT_ZONE + DEFAULT_UPGRADEDOMAIN;
    String DEFAULT_REGION_AND_RACK = DEFAULT_REGION + DEFAULT_RACK;

    /**
     * Add a node to the network topology.
     *
     * @param node
     *          add the node to network topology
     */
    void add(Node node);

    /**
     * Remove a node from nework topology.
     *
     * @param node
     *          remove the node from network topology
     */
    void remove(Node node);

    /**
     * Check if the tree contains node <i>node</i>.
     *
     * @param node
     *          node to check
     * @return true if <i>node</i> is already in the network topology, otherwise false.
     */
    boolean contains(Node node);

    /**
     * Retrieve a node from the network topology.
     * @param loc
     * @return
     */
    Node getNode(String loc);

    /**
     * Returns number of racks in the network topology.
     *
     * @return number of racks in the network topology.
     */
    int getNumOfRacks();

    /**
     * Returns the nodes under a location.
     *
     * @param loc
     *      network location
     * @return nodes under a location
     */
    Set<Node> getLeaves(String loc);

    /**
     * Return the number of leaves in <i>scope</i> but not in <i>excludedNodes</i>.
     *
     * <p>If scope starts with ~, return the number of nodes that are not
     * in <i>scope</i> and <i>excludedNodes</i>;
     * @param scope a path string that may start with ~
     * @param excludedNodes a list of nodes
     * @return number of available nodes
     */
    int countNumOfAvailableNodes(String scope, Collection<Node> excludedNodes);
}
