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

import io.netty.util.HashedWheelTimer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A placement policy implementation use rack information for placing ensembles.
 *
 * @see EnsemblePlacementPolicy
 */
public class RackawareEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicyImpl
        implements ITopologyAwareEnsemblePlacementPolicy<BookieNode> {
    RackawareEnsemblePlacementPolicyImpl slave = null;

    public RackawareEnsemblePlacementPolicy() {
        super();
    }

    public RackawareEnsemblePlacementPolicy(boolean enforceDurability) {
        super(enforceDurability);
    }

    @Override
    protected RackawareEnsemblePlacementPolicy initialize(DNSToSwitchMapping dnsResolver,
                                                          HashedWheelTimer timer,
                                                          boolean reorderReadsRandom,
                                                          int stabilizePeriodSeconds,
                                                          int reorderThresholdPendingRequests,
                                                          boolean isWeighted,
                                                          int maxWeightMultiple,
                                                          int minNumRacksPerWriteQuorum,
                                                          boolean enforceMinNumRacksPerWriteQuorum,
                                                          boolean ignoreLocalNodeInPlacementPolicy,
            StatsLogger statsLogger, BookieAddressResolver bookieAddressResolver) {
        return initialize(dnsResolver, timer, reorderReadsRandom, stabilizePeriodSeconds,
            reorderThresholdPendingRequests, isWeighted, maxWeightMultiple, minNumRacksPerWriteQuorum,
            enforceMinNumRacksPerWriteQuorum, ignoreLocalNodeInPlacementPolicy, false,
            statsLogger, bookieAddressResolver);
    }

    @Override
    protected RackawareEnsemblePlacementPolicy initialize(DNSToSwitchMapping dnsResolver,
                                                          HashedWheelTimer timer,
                                                          boolean reorderReadsRandom,
                                                          int stabilizePeriodSeconds,
                                                          int reorderThresholdPendingRequests,
                                                          boolean isWeighted,
                                                          int maxWeightMultiple,
                                                          int minNumRacksPerWriteQuorum,
                                                          boolean enforceMinNumRacksPerWriteQuorum,
                                                          boolean ignoreLocalNodeInPlacementPolicy,
                                                          boolean useHostnameResolveLocalNodePlacementPolicy,
            StatsLogger statsLogger, BookieAddressResolver bookieAddressResolver) {
        if (stabilizePeriodSeconds > 0) {
            super.initialize(dnsResolver, timer, reorderReadsRandom, 0, reorderThresholdPendingRequests, isWeighted,
                    maxWeightMultiple, minNumRacksPerWriteQuorum, enforceMinNumRacksPerWriteQuorum,
                    ignoreLocalNodeInPlacementPolicy, useHostnameResolveLocalNodePlacementPolicy,
                statsLogger, bookieAddressResolver);
            slave = new RackawareEnsemblePlacementPolicyImpl(enforceDurability);
            slave.initialize(dnsResolver, timer, reorderReadsRandom, stabilizePeriodSeconds,
                    reorderThresholdPendingRequests, isWeighted, maxWeightMultiple, minNumRacksPerWriteQuorum,
                    enforceMinNumRacksPerWriteQuorum, ignoreLocalNodeInPlacementPolicy,
                    useHostnameResolveLocalNodePlacementPolicy, statsLogger, bookieAddressResolver);
        } else {
            super.initialize(dnsResolver, timer, reorderReadsRandom, stabilizePeriodSeconds,
                    reorderThresholdPendingRequests, isWeighted, maxWeightMultiple, minNumRacksPerWriteQuorum,
                    enforceMinNumRacksPerWriteQuorum, ignoreLocalNodeInPlacementPolicy,
                    useHostnameResolveLocalNodePlacementPolicy, statsLogger, bookieAddressResolver);
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
    public Set<BookieId> onClusterChanged(Set<BookieId> writableBookies,
            Set<BookieId> readOnlyBookies) {
        Set<BookieId> deadBookies = super.onClusterChanged(writableBookies, readOnlyBookies);
        if (null != slave) {
            deadBookies = slave.onClusterChanged(writableBookies, readOnlyBookies);
        }
        return deadBookies;
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies)
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
    public PlacementResult<BookieId> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble,
            BookieId bookieToReplace, Set<BookieId> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
       try {
            return super.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                    currentEnsemble, bookieToReplace, excludeBookies);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                        currentEnsemble, bookieToReplace, excludeBookies);
            }
        }
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(
            List<BookieId> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        return super.reorderReadSequence(ensemble, bookiesHealthInfo,
                                         writeSet);
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(
            List<BookieId> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        return super.reorderReadLACSequence(ensemble, bookiesHealthInfo,
                                            writeSet);
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize,
                                                 int writeQuorumSize,
                                                 int ackQuorumSize,
                                                 Set<BookieId> excludeBookies,
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
            Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom)
            throws BKException.BKNotEnoughBookiesException {
        try {
            return super.selectFromNetworkLocation(networkLoc, excludeBookies, predicate, ensemble,
                    fallbackToRandom);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.selectFromNetworkLocation(networkLoc, excludeBookies, predicate, ensemble,
                        fallbackToRandom);
            }
        }
    }

    @Override
    public BookieNode selectFromNetworkLocation(
            Set<String> excludeRacks,
            Set<Node> excludeBookies,
            Predicate<BookieNode> predicate,
            Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom)
                    throws BKException.BKNotEnoughBookiesException {
        try {
            return super.selectFromNetworkLocation(excludeRacks, excludeBookies, predicate, ensemble, fallbackToRandom);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.selectFromNetworkLocation(excludeRacks, excludeBookies, predicate, ensemble,
                        fallbackToRandom);
            }
        }
    }

    @Override
    public BookieNode selectFromNetworkLocation(
            String networkLoc,
            Set<String> excludeRacks,
            Set<Node> excludeBookies,
            Predicate<BookieNode> predicate,
            Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom)
            throws BKNotEnoughBookiesException {
        try {
            return super.selectFromNetworkLocation(networkLoc, excludeRacks, excludeBookies, predicate, ensemble,
                    fallbackToRandom);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.selectFromNetworkLocation(networkLoc, excludeRacks, excludeBookies, predicate, ensemble,
                        fallbackToRandom);
            }
        }
    }

    @Override
    public PlacementResult<List<BookieId>> replaceToAdherePlacementPolicy(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Set<BookieId> excludeBookies,
            List<BookieId> currentEnsemble) {
        final PlacementResult<List<BookieId>> placementResult =
                super.replaceToAdherePlacementPolicy(ensembleSize, writeQuorumSize, ackQuorumSize,
                        excludeBookies, currentEnsemble);
        if (placementResult.getAdheringToPolicy() != PlacementPolicyAdherence.FAIL) {
            return placementResult;
        } else {
            if (slave == null) {
                return placementResult;
            } else {
                return slave.replaceToAdherePlacementPolicy(ensembleSize, writeQuorumSize, ackQuorumSize,
                        excludeBookies, currentEnsemble);
            }
        }
    }

    @Override
    public void handleBookiesThatLeft(Set<BookieId> leftBookies) {
        super.handleBookiesThatLeft(leftBookies);
        if (null != slave) {
            slave.handleBookiesThatLeft(leftBookies);
        }
    }

    @Override
    public void handleBookiesThatJoined(Set<BookieId> joinedBookies) {
        super.handleBookiesThatJoined(joinedBookies);
        if (null != slave) {
            slave.handleBookiesThatJoined(joinedBookies);
        }
    }
}
