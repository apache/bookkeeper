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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.stats.StatsLogger;

import io.netty.util.HashedWheelTimer;

public class RackawareEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicyImpl
        implements ITopologyAwareEnsemblePlacementPolicy<TopologyAwareEnsemblePlacementPolicy.BookieNode> {
    RackawareEnsemblePlacementPolicyImpl slave = null;

    RackawareEnsemblePlacementPolicy() {
        super();
    }

    RackawareEnsemblePlacementPolicy(boolean enforceDurability) {
        super(enforceDurability);
    }

    @Override
    protected RackawareEnsemblePlacementPolicy initialize(DNSToSwitchMapping dnsResolver,
                                                          HashedWheelTimer timer,
                                                          boolean reorderReadsRandom,
                                                          int stabilizePeriodSeconds,
                                                          boolean isWeighted,
                                                          int maxWeightMultiple,
                                                          StatsLogger statsLogger) {
        if (stabilizePeriodSeconds > 0) {
            super.initialize(dnsResolver, timer, reorderReadsRandom, 0, isWeighted, maxWeightMultiple, statsLogger);
            slave = new RackawareEnsemblePlacementPolicyImpl(enforceDurability);
            slave.initialize(dnsResolver, timer, reorderReadsRandom, stabilizePeriodSeconds, isWeighted,
                    maxWeightMultiple, statsLogger);
        } else {
            super.initialize(dnsResolver, timer, reorderReadsRandom, stabilizePeriodSeconds, isWeighted,
                    maxWeightMultiple, statsLogger);
            slave = null;
        }
        return this;
    }

    @Override
    public void uninitalize() {
        super.uninitalize();
        if (null != slave) {
            slave.uninitalize();
        }
    }

    @Override
    public Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies, Set<BookieSocketAddress> readOnlyBookies) {
        Set<BookieSocketAddress> deadBookies = super.onClusterChanged(writableBookies, readOnlyBookies);
        if (null != slave) {
            deadBookies = slave.onClusterChanged(writableBookies, readOnlyBookies);
        }
        return deadBookies;
    }

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(
        int ensembleSize, int writeQuorumSize, int ackQuorumSize, java.util.Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
        try {
            return super.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, excludeBookies);
            }
        }
    }

    @Override
    public BookieSocketAddress replaceBookie(
        int ensembleSize, int writeQuorumSize, int ackQuorumSize, java.util.Map<String, byte[]> customMetadata, Collection<BookieSocketAddress> currentEnsemble, BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
       try {
            return super.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                    currentEnsemble, bookieToReplace, excludeBookies);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize,customMetadata,
                        currentEnsemble, bookieToReplace, excludeBookies);
            }
        }
    }

    @Override
    public List<Integer> reorderReadSequence(ArrayList<BookieSocketAddress> ensemble,
                                             List<Integer> writeSet,
                                             Map<BookieSocketAddress, Long> bookieFailureHistory) {
        return super.reorderReadSequence(ensemble, writeSet, bookieFailureHistory);
    }

    @Override
    public List<Integer> reorderReadLACSequence(ArrayList<BookieSocketAddress> ensemble,
                                                List<Integer> writeSet,
                                                Map<BookieSocketAddress, Long> bookieFailureHistory) {
        return super.reorderReadLACSequence(ensemble, writeSet, bookieFailureHistory);
    }

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize,
                                                    int writeQuorumSize,
                                                    int ackQuorumSize,
                                                    Set<BookieSocketAddress> excludeBookies,
                                                    Ensemble<BookieNode> parentEnsemble,
                                                    Predicate<BookieNode> parentPredicate)
            throws BKException.BKNotEnoughBookiesException {
        try {
            return super.newEnsemble(
                    ensembleSize,
                    writeQuorumSize,
                    ackQuorumSize,
                    excludeBookies,
                    parentEnsemble,
                    parentPredicate);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize,
                        excludeBookies, parentEnsemble, parentPredicate);
            }
        }
    }

    @Override
    public BookieNode selectFromNetworkLocation(
            String networkLoc,
            Set<Node> excludeBookies,
            Predicate<BookieNode> predicate,
            Ensemble<BookieNode> ensemble)
            throws BKException.BKNotEnoughBookiesException {
        try {
            return super.selectFromNetworkLocation(networkLoc, excludeBookies, predicate, ensemble);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.selectFromNetworkLocation(networkLoc, excludeBookies, predicate, ensemble);
            }
        }
    }

    @Override
    public void handleBookiesThatLeft(Set<BookieSocketAddress> leftBookies) {
        super.handleBookiesThatLeft(leftBookies);
        if (null != slave) {
            slave.handleBookiesThatLeft(leftBookies);
        }
    }

    @Override
    public void handleBookiesThatJoined(Set<BookieSocketAddress> joinedBookies) {
        super.handleBookiesThatJoined(joinedBookies);
        if (null != slave) {
            slave.handleBookiesThatJoined(joinedBookies);
        }
    }
}
