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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A placement policy use region information in the network topology for placing ensembles.
 *
 * @see EnsemblePlacementPolicy
 */
public class RegionAwareEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicy {
    static final Logger LOG = LoggerFactory.getLogger(RegionAwareEnsemblePlacementPolicy.class);

    public static final String REPP_REGIONS_TO_WRITE = "reppRegionsToWrite";
    public static final String REPP_MINIMUM_REGIONS_FOR_DURABILITY = "reppMinimumRegionsForDurability";
    public static final String REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE =
        "reppEnableDurabilityEnforcementInReplace";
    public static final String REPP_DISABLE_DURABILITY_FEATURE_NAME = "reppDisableDurabilityFeatureName";
    public static final String REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME =
        "reppDisallowBookiePlacementInRegionFeatureName";
    public static final String REPP_DISABLE_DURABILITY_ENFORCEMENT_FEATURE = "reppDisableDurabilityEnforcementFeature";
    public static final String REPP_ENABLE_VALIDATION = "reppEnableValidation";
    public static final String REGION_AWARE_ANOMALOUS_ENSEMBLE = "region_aware_anomalous_ensemble";
    static final int MINIMUM_REGIONS_FOR_DURABILITY_DEFAULT = 2;
    static final int REGIONID_DISTANCE_FROM_LEAVES = 2;
    static final String UNKNOWN_REGION = "UnknownRegion";
    static final int REMOTE_NODE_IN_REORDER_SEQUENCE = 2;

    protected final Map<String, TopologyAwareEnsemblePlacementPolicy> perRegionPlacement;
    protected final ConcurrentMap<BookieSocketAddress, String> address2Region;
    protected FeatureProvider featureProvider;
    protected String disallowBookiePlacementInRegionFeatureName;
    protected String myRegion = null;
    protected int minRegionsForDurability = 0;
    protected boolean enableValidation = true;
    protected boolean enforceDurabilityInReplace = false;
    protected Feature disableDurabilityFeature;

    RegionAwareEnsemblePlacementPolicy() {
        super();
        perRegionPlacement = new HashMap<String, TopologyAwareEnsemblePlacementPolicy>();
        address2Region = new ConcurrentHashMap<BookieSocketAddress, String>();
    }

    protected String getRegion(BookieSocketAddress addr) {
        String region = address2Region.get(addr);
        if (null == region) {
            String networkLocation = resolveNetworkLocation(addr);
            if (NetworkTopology.DEFAULT_REGION_AND_RACK.equals(networkLocation)) {
                region = UNKNOWN_REGION;
            } else {
                String[] parts = networkLocation.split(NodeBase.PATH_SEPARATOR_STR);
                if (parts.length <= 1) {
                    region = UNKNOWN_REGION;
                } else {
                    region = parts[1];
                }
            }
            address2Region.putIfAbsent(addr, region);
        }
        return region;
    }

    protected String getLocalRegion(BookieNode node) {
        if (null == node || null == node.getAddr()) {
            return UNKNOWN_REGION;
        }
        return getRegion(node.getAddr());
    }

    @Override
    public void handleBookiesThatLeft(Set<BookieSocketAddress> leftBookies) {
        super.handleBookiesThatLeft(leftBookies);

        for (TopologyAwareEnsemblePlacementPolicy policy: perRegionPlacement.values()) {
            policy.handleBookiesThatLeft(leftBookies);
        }
    }

    @Override
    public void handleBookiesThatJoined(Set<BookieSocketAddress> joinedBookies) {
        Map<String, Set<BookieSocketAddress>> perRegionClusterChange = new HashMap<String, Set<BookieSocketAddress>>();

        // node joined
        for (BookieSocketAddress addr : joinedBookies) {
            BookieNode node = createBookieNode(addr);
            topology.add(node);
            knownBookies.put(addr, node);
            String region = getLocalRegion(node);
            if (null == perRegionPlacement.get(region)) {
                perRegionPlacement.put(region, new RackawareEnsemblePlacementPolicy()
                        .initialize(dnsResolver, timer, this.reorderReadsRandom, this.stabilizePeriodSeconds,
                                this.reorderThresholdPendingRequests, this.isWeighted, this.maxWeightMultiple,
                                this.minNumRacksPerWriteQuorum, this.enforceMinNumRacksPerWriteQuorum,
                                this.ignoreLocalNodeInPlacementPolicy, statsLogger)
                        .withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK));
            }

            Set<BookieSocketAddress> regionSet = perRegionClusterChange.get(region);
            if (null == regionSet) {
                regionSet = new HashSet<BookieSocketAddress>();
                regionSet.add(addr);
                perRegionClusterChange.put(region, regionSet);
            } else {
                regionSet.add(addr);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Cluster changed : bookie {} joined the cluster.", addr);
            }
        }

        for (Map.Entry<String, TopologyAwareEnsemblePlacementPolicy> regionEntry : perRegionPlacement.entrySet()) {
            Set<BookieSocketAddress> regionSet = perRegionClusterChange.get(regionEntry.getKey());
            if (null == regionSet) {
                regionSet = new HashSet<BookieSocketAddress>();
            }
            regionEntry.getValue().handleBookiesThatJoined(regionSet);
        }
    }

    @Override
    public RegionAwareEnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                                         Optional<DNSToSwitchMapping> optionalDnsResolver,
                                                         HashedWheelTimer timer,
                                                         FeatureProvider featureProvider,
                                                         StatsLogger statsLogger) {
        super.initialize(conf, optionalDnsResolver, timer, featureProvider, statsLogger)
                .withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK);
        myRegion = getLocalRegion(localNode);
        enableValidation = conf.getBoolean(REPP_ENABLE_VALIDATION, true);
        // We have to statically provide regions we want the writes to go through and how many regions
        // are required for durability. This decision cannot be driven by the active bookies as the
        // current topology will not be indicative of constraints that must be enforced for durability
        String regionsString = conf.getString(REPP_REGIONS_TO_WRITE, null);
        if (null != regionsString) {
            // Regions are specified as
            // R1;R2;...
            String[] regions = regionsString.split(";");
            for (String region : regions) {
                perRegionPlacement.put(region, new RackawareEnsemblePlacementPolicy(true)
                        .initialize(dnsResolver, timer, this.reorderReadsRandom, this.stabilizePeriodSeconds,
                                this.reorderThresholdPendingRequests, this.isWeighted, this.maxWeightMultiple,
                                this.minNumRacksPerWriteQuorum, this.enforceMinNumRacksPerWriteQuorum,
                                this.ignoreLocalNodeInPlacementPolicy, statsLogger)
                        .withDefaultRack(NetworkTopology.DEFAULT_REGION_AND_RACK));
            }
            minRegionsForDurability = conf.getInt(REPP_MINIMUM_REGIONS_FOR_DURABILITY,
                    MINIMUM_REGIONS_FOR_DURABILITY_DEFAULT);
            if (minRegionsForDurability > 0) {
                enforceDurability = true;
                enforceDurabilityInReplace = conf.getBoolean(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, true);
            }
            if (regions.length < minRegionsForDurability) {
                throw new IllegalArgumentException(
                        "Regions provided are insufficient to meet the durability constraints");
            }
        }
        this.featureProvider = featureProvider;
        this.disallowBookiePlacementInRegionFeatureName =
            conf.getString(REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME);
        this.disableDurabilityFeature = conf.getFeature(REPP_DISABLE_DURABILITY_ENFORCEMENT_FEATURE, null);
        if (null == disableDurabilityFeature) {
            this.disableDurabilityFeature =
                    featureProvider.getFeature(
                        conf.getString(REPP_DISABLE_DURABILITY_FEATURE_NAME,
                                BookKeeperConstants.FEATURE_REPP_DISABLE_DURABILITY_ENFORCEMENT));
        }
        return this;
    }

    protected List<BookieNode> selectRandomFromRegions(Set<String> availableRegions,
                                            int numBookies,
                                            Set<Node> excludeBookies,
                                            Predicate<BookieNode> predicate,
                                            Ensemble<BookieNode> ensemble)
        throws BKException.BKNotEnoughBookiesException {
        List<BookieNode> availableBookies = new ArrayList<BookieNode>();
        for (BookieNode bookieNode: knownBookies.values()) {
            if (availableRegions.contains(getLocalRegion(bookieNode))) {
                availableBookies.add(bookieNode);
            }
        }

        return selectRandomInternal(availableBookies,  numBookies, excludeBookies, predicate, ensemble);
    }


    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludedBookies)
            throws BKException.BKNotEnoughBookiesException {

        int effectiveMinRegionsForDurability = disableDurabilityFeature.isAvailable() ? 1 : minRegionsForDurability;

        // All of these conditions indicate bad configuration
        if (ackQuorumSize < effectiveMinRegionsForDurability) {
            throw new IllegalArgumentException(
                    "Ack Quorum size provided are insufficient to meet the durability constraints");
        } else if (ensembleSize < writeQuorumSize) {
            throw new IllegalArgumentException(
                    "write quorum (" + writeQuorumSize + ") cannot exceed ensemble size (" + ensembleSize + ")");
        } else if (writeQuorumSize < ackQuorumSize) {
            throw new IllegalArgumentException(
                    "ack quorum (" + ackQuorumSize + ") cannot exceed write quorum size (" + writeQuorumSize + ")");
        } else if (effectiveMinRegionsForDurability > 0) {
            // We must survive the failure of numRegions - effectiveMinRegionsForDurability. When these
            // regions have failed we would spread the replicas over the remaining
            // effectiveMinRegionsForDurability regions; we have to make sure that the ack quorum is large
            // enough such that there is a configuration for spreading the replicas across
            // effectiveMinRegionsForDurability - 1 regions
            if (ackQuorumSize <= (writeQuorumSize - (writeQuorumSize / effectiveMinRegionsForDurability))) {
                throw new IllegalArgumentException("ack quorum (" + ackQuorumSize + ") "
                    + "violates the requirement to satisfy durability constraints when running in degraded mode");
            }
        }

        rwLock.readLock().lock();
        try {
            Set<BookieSocketAddress> comprehensiveExclusionBookiesSet = addDefaultRackBookiesIfMinNumRacksIsEnforced(
                    excludedBookies);
            Set<Node> excludeNodes = convertBookiesToNodes(comprehensiveExclusionBookiesSet);
            Set<String> availableRegions = new HashSet<String>();
            for (String region: perRegionPlacement.keySet()) {
                if ((null == disallowBookiePlacementInRegionFeatureName)
                        || !featureProvider.scope(region).getFeature(disallowBookiePlacementInRegionFeatureName)
                            .isAvailable()) {
                    availableRegions.add(region);
                }
            }
            int numRegionsAvailable = availableRegions.size();

            // If we were unable to get region information or all regions are disallowed which is
            // an invalid configuration; default to random selection from the set of nodes
            if (numRegionsAvailable < 1) {
                // We cant disallow all regions; if we did, raise an alert to draw attention
                if (perRegionPlacement.keySet().size() >= 1) {
                    LOG.error("No regions available, invalid configuration");
                }
                List<BookieNode> bns = selectRandom(ensembleSize, excludeNodes, TruePredicate.INSTANCE,
                    EnsembleForReplacementWithNoConstraints.INSTANCE);
                ArrayList<BookieSocketAddress> addrs = new ArrayList<BookieSocketAddress>(ensembleSize);
                for (BookieNode bn : bns) {
                    addrs.add(bn.getAddr());
                }
                return PlacementResult.of(addrs,
                                          isEnsembleAdheringToPlacementPolicy(
                                                  addrs, writeQuorumSize, ackQuorumSize));
            }

            // Single region, fall back to RackAwareEnsemblePlacement
            if (numRegionsAvailable < 2) {
                RRTopologyAwareCoverageEnsemble ensemble = new RRTopologyAwareCoverageEnsemble(ensembleSize,
                        writeQuorumSize, ackQuorumSize, REGIONID_DISTANCE_FROM_LEAVES,
                        effectiveMinRegionsForDurability > 0 ? new HashSet<>(perRegionPlacement.keySet()) : null,
                        effectiveMinRegionsForDurability, minNumRacksPerWriteQuorum);
                TopologyAwareEnsemblePlacementPolicy nextPolicy = perRegionPlacement.get(
                        availableRegions.iterator().next());
                return nextPolicy.newEnsemble(ensembleSize, writeQuorumSize, writeQuorumSize,
                        comprehensiveExclusionBookiesSet, ensemble, ensemble);
            }

            int remainingEnsemble = ensembleSize;
            int remainingWriteQuorum = writeQuorumSize;

            // Equally distribute the nodes across all regions to whatever extent possible
            // with the hierarchy in mind
            // Try and place as many nodes in a region as possible, the ones that cannot be
            // accommodated are placed on other regions
            // Within each region try and follow rack aware placement
            Map<String, Pair<Integer, Integer>> regionsWiseAllocation = new HashMap<>();
            for (String region: availableRegions) {
                regionsWiseAllocation.put(region, Pair.of(0, 0));
            }
            int remainingEnsembleBeforeIteration;
            int numRemainingRegions;
            Set<String> regionsReachedMaxAllocation = new HashSet<String>();
            RRTopologyAwareCoverageEnsemble ensemble;
            do {
                numRemainingRegions = numRegionsAvailable - regionsReachedMaxAllocation.size();
                ensemble = new RRTopologyAwareCoverageEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize,
                        REGIONID_DISTANCE_FROM_LEAVES,
                        // We pass all regions we know off to the coverage ensemble as
                        // regardless of regions that are available; constraints are
                        // always applied based on all possible regions
                        effectiveMinRegionsForDurability > 0 ? new HashSet<>(perRegionPlacement.keySet()) : null,
                        effectiveMinRegionsForDurability, minNumRacksPerWriteQuorum);
                remainingEnsembleBeforeIteration = remainingEnsemble;
                int regionsToAllocate = numRemainingRegions;
                for (Map.Entry<String, Pair<Integer, Integer>> regionEntry: regionsWiseAllocation.entrySet()) {
                    String region = regionEntry.getKey();
                    final Pair<Integer, Integer> currentAllocation = regionEntry.getValue();
                    TopologyAwareEnsemblePlacementPolicy policyWithinRegion = perRegionPlacement.get(region);
                    if (!regionsReachedMaxAllocation.contains(region)) {
                        if (numRemainingRegions <= 0) {
                            LOG.error("Inconsistent State: This should never happen");
                            throw new BKException.BKNotEnoughBookiesException();
                        }
                        // try to place the bookies as balance as possible across all the regions
                        int addToEnsembleSize = Math.min(remainingEnsemble, remainingEnsemble / regionsToAllocate
                                + (remainingEnsemble % regionsToAllocate == 0 ? 0 : 1));
                        boolean success = false;
                        while (addToEnsembleSize > 0) {
                            int addToWriteQuorum = Math.max(1, Math.min(remainingWriteQuorum,
                                        Math.round(1.0f * writeQuorumSize * addToEnsembleSize / ensembleSize)));
                            // Temp ensemble will be merged back into the ensemble only if we are able to successfully
                            // allocate the target number of bookies in this region; if we fail because we dont have
                            // enough bookies; then we retry the process with a smaller target
                            RRTopologyAwareCoverageEnsemble tempEnsemble =
                                new RRTopologyAwareCoverageEnsemble(ensemble);
                            int newEnsembleSize = currentAllocation.getLeft() + addToEnsembleSize;
                            int newWriteQuorumSize = currentAllocation.getRight() + addToWriteQuorum;
                            try {
                                List<BookieSocketAddress> allocated = policyWithinRegion
                                        .newEnsemble(newEnsembleSize, newWriteQuorumSize, newWriteQuorumSize,
                                                comprehensiveExclusionBookiesSet, tempEnsemble, tempEnsemble)
                                        .getResult();
                                ensemble = tempEnsemble;
                                remainingEnsemble -= addToEnsembleSize;
                                remainingWriteQuorum -= addToWriteQuorum;
                                regionsWiseAllocation.put(region, Pair.of(newEnsembleSize, newWriteQuorumSize));
                                success = true;
                                regionsToAllocate--;
                                LOG.info("Region {} allocating bookies with ensemble size {} "
                                        + "and write quorum size {} : {}",
                                        region, newEnsembleSize, newWriteQuorumSize, allocated);
                                break;
                            } catch (BKException.BKNotEnoughBookiesException exc) {
                                LOG.warn("Could not allocate {} bookies in region {}, try allocating {} bookies",
                                        newEnsembleSize, region, (newEnsembleSize - 1));
                                addToEnsembleSize--;
                            }
                        }

                        // we couldn't allocate additional bookies from the region,
                        // it should have reached its max allocation.
                        if (!success) {
                            regionsReachedMaxAllocation.add(region);
                        }
                    }

                    if (regionsReachedMaxAllocation.contains(region)) {
                        if (currentAllocation.getLeft() > 0) {
                            LOG.info("Allocating {} bookies in region {} : ensemble {} exclude {}",
                                    currentAllocation.getLeft(), region, comprehensiveExclusionBookiesSet, ensemble);
                            policyWithinRegion.newEnsemble(
                                    currentAllocation.getLeft(),
                                    currentAllocation.getRight(),
                                    currentAllocation.getRight(),
                                    comprehensiveExclusionBookiesSet,
                                    ensemble,
                                    ensemble);
                            LOG.info("Allocated {} bookies in region {} : {}",
                                    currentAllocation.getLeft(), region, ensemble);
                        }
                    }
                }

                if (regionsReachedMaxAllocation.containsAll(regionsWiseAllocation.keySet())) {
                    break;
                }
            } while ((remainingEnsemble > 0) && (remainingEnsemble < remainingEnsembleBeforeIteration));

            List<BookieSocketAddress> bookieList = ensemble.toList();
            if (ensembleSize != bookieList.size()) {
                LOG.error("Not enough {} bookies are available to form an ensemble : {}.",
                          ensembleSize, bookieList);
                throw new BKException.BKNotEnoughBookiesException();
            }

            if (enableValidation && !ensemble.validate()) {
                LOG.error("Not enough {} bookies are available to form a valid ensemble : {}.",
                    ensembleSize, bookieList);
                throw new BKException.BKNotEnoughBookiesException();
            }
            LOG.info("Bookies allocated successfully {}", ensemble);
            List<BookieSocketAddress> ensembleList = ensemble.toList();
            return PlacementResult.of(ensembleList,
                    isEnsembleAdheringToPlacementPolicy(ensembleList, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public PlacementResult<BookieSocketAddress> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            boolean enforceDurability = enforceDurabilityInReplace && !disableDurabilityFeature.isAvailable();
            int effectiveMinRegionsForDurability = enforceDurability ? minRegionsForDurability : 1;
            Set<BookieSocketAddress> comprehensiveExclusionBookiesSet = addDefaultRackBookiesIfMinNumRacksIsEnforced(
                    excludeBookies);
            Set<Node> excludeNodes = convertBookiesToNodes(comprehensiveExclusionBookiesSet);
            RRTopologyAwareCoverageEnsemble ensemble = new RRTopologyAwareCoverageEnsemble(ensembleSize,
                writeQuorumSize,
                ackQuorumSize,
                REGIONID_DISTANCE_FROM_LEAVES,
                effectiveMinRegionsForDurability > 0 ? new HashSet<String>(perRegionPlacement.keySet()) : null,
                effectiveMinRegionsForDurability, minNumRacksPerWriteQuorum);

            BookieNode bookieNodeToReplace = knownBookies.get(bookieToReplace);
            if (null == bookieNodeToReplace) {
                bookieNodeToReplace = createBookieNode(bookieToReplace);
            }
            excludeNodes.add(bookieNodeToReplace);

            for (BookieSocketAddress bookieAddress: currentEnsemble) {
                if (bookieAddress.equals(bookieToReplace)) {
                    continue;
                }

                BookieNode bn = knownBookies.get(bookieAddress);
                if (null == bn) {
                    bn = createBookieNode(bookieAddress);
                }

                excludeNodes.add(bn);

                if (!ensemble.apply(bn, ensemble)) {
                    LOG.warn("Anomalous ensemble detected");
                    if (null != statsLogger) {
                        statsLogger.getCounter(REGION_AWARE_ANOMALOUS_ENSEMBLE).inc();
                    }
                    enforceDurability = false;
                }

                ensemble.addNode(bn);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Try to choose a new bookie to replace {}, excluding {}.", bookieToReplace,
                    excludeNodes);
            }
            // pick a candidate from same rack to replace
            BookieNode candidate = replaceFromRack(bookieNodeToReplace, excludeNodes,
                ensemble, ensemble, enforceDurability);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Bookie {} is chosen to replace bookie {}.", candidate, bookieNodeToReplace);
            }
            BookieSocketAddress candidateAddr = candidate.getAddr();
            List<BookieSocketAddress> newEnsemble = new ArrayList<BookieSocketAddress>(currentEnsemble);
            if (currentEnsemble.isEmpty()) {
                /*
                 * in testing code there are test cases which would pass empty
                 * currentEnsemble
                 */
                newEnsemble.add(candidateAddr);
            } else {
                newEnsemble.set(currentEnsemble.indexOf(bookieToReplace), candidateAddr);
            }
            return PlacementResult.of(candidateAddr,
                    isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected BookieNode replaceFromRack(BookieNode bookieNodeToReplace,
                                         Set<Node> excludeBookies,
                                         Predicate<BookieNode> predicate,
                                         Ensemble<BookieNode> ensemble,
                                         boolean enforceDurability)
        throws BKException.BKNotEnoughBookiesException {
        Set<String> availableRegions = new HashSet<String>();
        for (String region: perRegionPlacement.keySet()) {
            if ((null == disallowBookiePlacementInRegionFeatureName)
                    || !featureProvider.scope(region).getFeature(disallowBookiePlacementInRegionFeatureName)
                        .isAvailable()) {
                availableRegions.add(region);
            }
        }
        String regionForBookieToReplace = getLocalRegion(bookieNodeToReplace);
        if (availableRegions.contains(regionForBookieToReplace)) {
            TopologyAwareEnsemblePlacementPolicy regionPolicy = perRegionPlacement.get(regionForBookieToReplace);
            if (null != regionPolicy) {
                try {
                    // select one from local rack => it falls back to selecting a node from the region
                    // if the rack does not have an available node, selecting from the same region
                    // should not violate durability constraints so we can simply not have to check
                    // for that.
                    return regionPolicy.selectFromNetworkLocation(
                        bookieNodeToReplace.getNetworkLocation(),
                        excludeBookies,
                        TruePredicate.INSTANCE,
                        EnsembleForReplacementWithNoConstraints.INSTANCE,
                        true);
                } catch (BKException.BKNotEnoughBookiesException e) {
                    LOG.warn("Failed to choose a bookie from {} : "
                            + "excluded {}, fallback to choose bookie randomly from the cluster.",
                        bookieNodeToReplace.getNetworkLocation(), excludeBookies);
                }
            }
        }

        // randomly choose one from all the regions that are available, ignore the provided predicate if we are not
        // enforcing durability.
        return selectRandomFromRegions(availableRegions, 1,
            excludeBookies,
            enforceDurability ? predicate : TruePredicate.INSTANCE,
            enforceDurability ? ensemble : EnsembleForReplacementWithNoConstraints.INSTANCE).get(0);
    }

    @Override
    public final DistributionSchedule.WriteSet reorderReadSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        if (UNKNOWN_REGION.equals(myRegion)) {
            return super.reorderReadSequence(ensemble, bookiesHealthInfo, writeSet);
        } else {
            Map<Integer, String> writeSetWithRegion = new HashMap<>();
            for (int i = 0; i < writeSet.size(); i++) {
                int idx = writeSet.get(i);
                writeSetWithRegion.put(idx, getRegion(ensemble.get(idx)));
            }
            return super.reorderReadSequenceWithRegion(ensemble, writeSet, writeSetWithRegion,
                bookiesHealthInfo, true, myRegion, REMOTE_NODE_IN_REORDER_SEQUENCE);
        }
    }

    @Override
    public final DistributionSchedule.WriteSet reorderReadLACSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        if (UNKNOWN_REGION.equals(myRegion)) {
            return super.reorderReadLACSequence(ensemble, bookiesHealthInfo, writeSet);
        }
        DistributionSchedule.WriteSet finalList = reorderReadSequence(ensemble, bookiesHealthInfo, writeSet);
        finalList.addMissingIndices(ensemble.size());
        return finalList;
    }

    @Override
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieSocketAddress> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        /**
         * TODO: have to implement actual logic for this method for
         * RegionAwareEnsemblePlacementPolicy. For now return true value.
         *
         * - https://github.com/apache/bookkeeper/issues/1898
         */
        return PlacementPolicyAdherence.MEETS_STRICT;
    }
}
