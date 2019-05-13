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

import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble;
import org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.Node;

/**
 * Interface for topology aware ensemble placement policy.
 *
 * <p>All the implementations of this interface are using {@link org.apache.bookkeeper.net.NetworkTopology}
 *    for placing ensembles.
 *
 * @see EnsemblePlacementPolicy
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ITopologyAwareEnsemblePlacementPolicy<T extends Node> extends EnsemblePlacementPolicy {
    /**
     * Predicate used when choosing an ensemble.
     */
    interface Predicate<T extends Node> {
        boolean apply(T candidate, Ensemble<T> chosenBookies);
    }

    /**
     * Ensemble used to hold the result of an ensemble selected for placement.
     */
    interface Ensemble<T extends Node> {

        /**
         * Append the new bookie node to the ensemble only if the ensemble doesnt
         * already contain the same bookie.
         *
         * @param node
         *          new candidate bookie node.
         * @return
         *          true if the node was added
         */
        boolean addNode(T node);

        /**
         * @return list of addresses representing the ensemble
         */
        List<BookieSocketAddress> toList();

        /**
         * Validates if an ensemble is valid.
         *
         * @return true if the ensemble is valid; false otherwise
         */
        boolean validate();

    }

    /**
     * Create an ensemble with parent ensemble.
     *
     * @param ensembleSize
     *          ensemble size
     * @param writeQuorumSize
     *          write quorum size
     * @param ackQuorumSize
     *          ack quorum size
     * @param excludeBookies
     *          exclude bookies
     * @param parentEnsemble
     *          parent ensemble
     * @return list of bookies forming the ensemble
     * @throws BKException.BKNotEnoughBookiesException
     */
    PlacementResult<List<BookieSocketAddress>> newEnsemble(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Set<BookieSocketAddress> excludeBookies,
            Ensemble<T> parentEnsemble,
            Predicate<T> parentPredicate)
            throws BKException.BKNotEnoughBookiesException;

    /**
     * Select a node from a given network location.
     *
     * @param networkLoc
     *          network location
     * @param excludeBookies
     *          exclude bookies set
     * @param predicate
     *          predicate to apply
     * @param ensemble
     *          ensemble
     * @param fallbackToRandom
     *          fallbackToRandom
     * @return the selected bookie.
     * @throws BKException.BKNotEnoughBookiesException
     */
    T selectFromNetworkLocation(String networkLoc,
                                Set<Node> excludeBookies,
                                Predicate<T> predicate,
                                Ensemble<T> ensemble,
                                boolean fallbackToRandom)
            throws BKException.BKNotEnoughBookiesException;

    /**
     * Select a node from cluster excluding excludeBookies and bookie nodes of
     * excludeRacks. If there isn't a BookieNode excluding those racks and
     * nodes, then if fallbackToRandom is set to true then pick a random node
     * from cluster just excluding excludeBookies.
     *
     * @param excludeRacks
     * @param excludeBookies
     * @param predicate
     * @param ensemble
     * @param fallbackToRandom
     * @return
     * @throws BKException.BKNotEnoughBookiesException
     */
    T selectFromNetworkLocation(Set<String> excludeRacks,
                                Set<Node> excludeBookies,
                                Predicate<BookieNode> predicate,
                                Ensemble<BookieNode> ensemble,
                                boolean fallbackToRandom)
            throws BKException.BKNotEnoughBookiesException;

    /**
     * Select a node from networkLoc rack excluding excludeBookies. If there
     * isn't any node in 'networkLoc', then it will try to get a node from
     * cluster excluding excludeRacks and excludeBookies. If fallbackToRandom is
     * set to true then it will get a random bookie from cluster excluding
     * excludeBookies if it couldn't find a bookie
     *
     * @param networkLoc
     * @param excludeRacks
     * @param excludeBookies
     * @param predicate
     * @param ensemble
     * @param fallbackToRandom
     * @return
     * @throws BKNotEnoughBookiesException
     */
    T selectFromNetworkLocation(String networkLoc,
                                Set<String> excludeRacks,
                                Set<Node> excludeBookies,
                                Predicate<BookieNode> predicate,
                                Ensemble<BookieNode> ensemble,
                                boolean fallbackToRandom)
            throws BKNotEnoughBookiesException;

    /**
     * Handle bookies that left.
     *
     * @param leftBookies
     *          bookies that left
     */
    void handleBookiesThatLeft(Set<BookieSocketAddress> leftBookies);

    /**
     * Handle bookies that joined.
     *
     * @param joinedBookies
     *          bookies that joined.
     */
    void handleBookiesThatJoined(Set<BookieSocketAddress> joinedBookies);

    /**
     * Handle rack change for the bookies.
     *
     * @param bookieAddressList
     */
    void onBookieRackChange(List<BookieSocketAddress> bookieAddressList);
}
