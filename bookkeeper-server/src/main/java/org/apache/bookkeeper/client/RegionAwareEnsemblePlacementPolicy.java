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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Optional;


import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionAwareEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicy {
    static final Logger LOG = LoggerFactory.getLogger(RegionAwareEnsemblePlacementPolicy.class);

    public static final String REPP_REGIONS_TO_WRITE = "reppRegionsToWrite";
    public static final String REPP_MINIMUM_REGIONS_FOR_DURABILITY = "reppMinimumRegionsForDurability";
    public static final String REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE = "reppEnableDurabilityEnforcementInReplace";
    public static final String REPP_DISABLE_DURABILITY_FEATURE_NAME = "reppDisableDurabilityFeatureName";
    public static final String REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME = "reppDisallowBookiePlacementInRegionFeatureName";
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
            if (NetworkTopology.DEFAULT_RACK.equals(networkLocation)) {
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

        for(TopologyAwareEnsemblePlacementPolicy policy: perRegionPlacement.values()) {
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
                        .initialize(dnsResolver, timer, this.reorderReadsRandom, this.stabilizePeriodSeconds, statsLogger));
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
        super.initialize(conf, optionalDnsResolver, timer, featureProvider, statsLogger);
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
            for (String region: regions) {
                perRegionPlacement.put(region, new RackawareEnsemblePlacementPolicy(true)
                        .initialize(dnsResolver, timer, this.reorderReadsRandom, this.stabilizePeriodSeconds, statsLogger));
            }
            minRegionsForDurability = conf.getInt(REPP_MINIMUM_REGIONS_FOR_DURABILITY, MINIMUM_REGIONS_FOR_DURABILITY_DEFAULT);
            if (minRegionsForDurability > 0) {
                enforceDurability = true;
                enforceDurabilityInReplace = conf.getBoolean(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, true);
            }
            if (regions.length < minRegionsForDurability) {
                throw new IllegalArgumentException("Regions provided are insufficient to meet the durability constraints");
            }
        }
        this.featureProvider = featureProvider;
        this.disallowBookiePlacementInRegionFeatureName = conf.getString(REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME);
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
        for(BookieNode bookieNode: knownBookies.values()) {
            if (availableRegions.contains(getLocalRegion(bookieNode))) {
                availableBookies.add(bookieNode);
            }
        }

        return selectRandomInternal(availableBookies,  numBookies, excludeBookies, predicate, ensemble);
    }


    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                                    Set<BookieSocketAddress> excludeBookies) throws BKException.BKNotEnoughBookiesException {

        int effectiveMinRegionsForDurability = disableDurabilityFeature.isAvailable() ? 1 : minRegionsForDurability;

        // All of these conditions indicate bad configuration
        if (ackQuorumSize < effectiveMinRegionsForDurability) {
            throw new IllegalArgumentException("Ack Quorum size provided are insufficient to meet the durability constraints");
        } else if (ensembleSize < writeQuorumSize) {
            throw new IllegalArgumentException("write quorum (" + writeQuorumSize + ") cannot exceed ensemble size (" + ensembleSize + ")");
        } else if (writeQuorumSize < ackQuorumSize) {
            throw new IllegalArgumentException("ack quorum (" + ackQuorumSize + ") cannot exceed write quorum size (" + writeQuorumSize + ")");
        } else if (effectiveMinRegionsForDurability > 0) {
            // We must survive the failure of numRegions - effectiveMinRegionsForDurability. When these
            // regions have failed we would spread the replicas over the remaining
            // effectiveMinRegionsForDurability regions; we have to make sure that the ack quorum is large
            // enough such that there is a configuration for spreading the replicas across
            // effectiveMinRegionsForDurability - 1 regions
            if (ackQuorumSize <= (writeQuorumSize - (writeQuorumSize / effectiveMinRegionsForDurability))) {
                throw new IllegalArgumentException("ack quorum (" + ackQuorumSize + ") " +
                    "violates the requirement to satisfy durability constraints when running in degraded mode");
            }
        }

        rwLock.readLock().lock();
        try {
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            Set<String> availableRegions = new HashSet<String>();
            for (String region: perRegionPlacement.keySet()) {
                if ((null == disallowBookiePlacementInRegionFeatureName) ||
                    !featureProvider.scope(region).getFeature(disallowBookiePlacementInRegionFeatureName).isAvailable()) {
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
                List<BookieNode> bns = selectRandom(ensembleSize, excludeNodes, TruePredicate.instance,
                    EnsembleForReplacementWithNoConstraints.instance);
                ArrayList<BookieSocketAddress> addrs = new ArrayList<BookieSocketAddress>(ensembleSize);
                for (BookieNode bn : bns) {
                    addrs.add(bn.getAddr());
                }
                return addrs;
            }

            // Single region, fall back to RackAwareEnsemblePlacement
            if (numRegionsAvailable < 2) {
                RRTopologyAwareCoverageEnsemble ensemble = new RRTopologyAwareCoverageEnsemble(ensembleSize,
                                                                    writeQuorumSize,
                                                                    ackQuorumSize,
                                                                    REGIONID_DISTANCE_FROM_LEAVES,
                                                                    effectiveMinRegionsForDurability > 0 ? new HashSet<String>(perRegionPlacement.keySet()) : null,
                                                                    effectiveMinRegionsForDurability);
                TopologyAwareEnsemblePlacementPolicy nextPolicy = perRegionPlacement.get(availableRegions.iterator().next());
                return nextPolicy.newEnsemble(ensembleSize, writeQuorumSize, writeQuorumSize, excludeBookies, ensemble, ensemble);
            }

            int remainingEnsemble = ensembleSize;
            int remainingWriteQuorum = writeQuorumSize;

            // Equally distribute the nodes across all regions to whatever extent possible
            // with the hierarchy in mind
            // Try and place as many nodes in a region as possible, the ones that cannot be
            // accommodated are placed on other regions
            // Within each region try and follow rack aware placement
            Map<String, Pair<Integer,Integer>> regionsWiseAllocation = new HashMap<String, Pair<Integer,Integer>>();
            for (String region: availableRegions) {
                regionsWiseAllocation.put(region, Pair.of(0,0));
            }
            int remainingEnsembleBeforeIteration;
            int numRemainingRegions;
            Set<String> regionsReachedMaxAllocation = new HashSet<String>();
            RRTopologyAwareCoverageEnsemble ensemble;
            do {
                numRemainingRegions = numRegionsAvailable - regionsReachedMaxAllocation.size();
                ensemble = new RRTopologyAwareCoverageEnsemble(ensembleSize,
                                    writeQuorumSize,
                                    ackQuorumSize,
                                    REGIONID_DISTANCE_FROM_LEAVES,
                                    // We pass all regions we know off to the coverage ensemble as
                                    // regardless of regions that are available; constraints are
                                    // always applied based on all possible regions
                                    effectiveMinRegionsForDurability > 0 ? new HashSet<String>(perRegionPlacement.keySet()) : null,
                                    effectiveMinRegionsForDurability);
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
                        int addToEnsembleSize = Math.min(remainingEnsemble, remainingEnsemble / regionsToAllocate + (remainingEnsemble % regionsToAllocate == 0 ? 0 : 1));
                        boolean success = false;
                        while (addToEnsembleSize > 0) {
                            int addToWriteQuorum = Math.max(1, Math.min(remainingWriteQuorum, Math.round(1.0f * writeQuorumSize * addToEnsembleSize / ensembleSize)));
                            // Temp ensemble will be merged back into the ensemble only if we are able to successfully allocate
                            // the target number of bookies in this region; if we fail because we dont have enough bookies; then we
                            // retry the process with a smaller target
                            RRTopologyAwareCoverageEnsemble tempEnsemble = new RRTopologyAwareCoverageEnsemble(ensemble);
                            int newEnsembleSize = currentAllocation.getLeft() + addToEnsembleSize;
                            int newWriteQuorumSize = currentAllocation.getRight() + addToWriteQuorum;
                            try {
                                List<BookieSocketAddress> allocated = policyWithinRegion.newEnsemble(newEnsembleSize, newWriteQuorumSize, newWriteQuorumSize, excludeBookies, tempEnsemble, tempEnsemble);
                                ensemble = tempEnsemble;
                                remainingEnsemble -= addToEnsembleSize;
                                remainingWriteQuorum -= addToWriteQuorum;
                                regionsWiseAllocation.put(region, Pair.of(newEnsembleSize, newWriteQuorumSize));
                                success = true;
                                regionsToAllocate--;
                                LOG.info("Region {} allocating bookies with ensemble size {} and write quorum size {} : {}",
                                    new Object[]{region, newEnsembleSize, newWriteQuorumSize, allocated});
                                break;
                            } catch (BKException.BKNotEnoughBookiesException exc) {
                                LOG.warn("Could not allocate {} bookies in region {}, try allocating {} bookies",
                                         new Object[] {newEnsembleSize, region, (newEnsembleSize - 1) });
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
                                new Object[]{currentAllocation.getLeft(), region, excludeBookies, ensemble});
                            policyWithinRegion.newEnsemble(
                                    currentAllocation.getLeft(),
                                    currentAllocation.getRight(),
                                    currentAllocation.getRight(),
                                    excludeBookies,
                                    ensemble,
                                    ensemble);
                            LOG.info("Allocated {} bookies in region {} : {}",
                                new Object[]{currentAllocation.getLeft(), region, ensemble});
                        }
                    }
                }

                if (regionsReachedMaxAllocation.containsAll(regionsWiseAllocation.keySet())) {
                    break;
                }
            } while ((remainingEnsemble > 0) && (remainingEnsemble < remainingEnsembleBeforeIteration));

            ArrayList<BookieSocketAddress> bookieList = ensemble.toList();
            if (ensembleSize != bookieList.size()) {
                LOG.error("Not enough {} bookies are available to form an ensemble : {}.",
                          ensembleSize, bookieList);
                throw new BKException.BKNotEnoughBookiesException();
            }

            if(enableValidation && !ensemble.validate()) {
                LOG.error("Not enough {} bookies are available to form a valid ensemble : {}.",
                    ensembleSize, bookieList);
                throw new BKException.BKNotEnoughBookiesException();
            }
            LOG.info("Bookies allocated successfully {}", ensemble);
            return ensemble.toList();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize, Collection<BookieSocketAddress> currentEnsemble, BookieSocketAddress bookieToReplace,
                                           Set<BookieSocketAddress> excludeBookies) throws BKException.BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            boolean enforceDurability = enforceDurabilityInReplace && !disableDurabilityFeature.isAvailable();
            int effectiveMinRegionsForDurability = enforceDurability ? minRegionsForDurability : 1;
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            RRTopologyAwareCoverageEnsemble ensemble = new RRTopologyAwareCoverageEnsemble(ensembleSize,
                writeQuorumSize,
                ackQuorumSize,
                REGIONID_DISTANCE_FROM_LEAVES,
                effectiveMinRegionsForDurability > 0 ? new HashSet<String>(perRegionPlacement.keySet()) : null,
                effectiveMinRegionsForDurability);

            BookieNode bookieNodeToReplace = knownBookies.get(bookieToReplace);
            if (null == bookieNodeToReplace) {
                bookieNodeToReplace = createBookieNode(bookieToReplace);
            }
            excludeNodes.add(bookieNodeToReplace);

            for(BookieSocketAddress bookieAddress: currentEnsemble) {
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
            return candidate.getAddr();
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
            if ((null == disallowBookiePlacementInRegionFeatureName) ||
                !featureProvider.scope(region).getFeature(disallowBookiePlacementInRegionFeatureName).isAvailable()) {
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
                        TruePredicate.instance,
                        EnsembleForReplacementWithNoConstraints.instance);
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
            enforceDurability ? predicate : TruePredicate.instance,
            enforceDurability ? ensemble : EnsembleForReplacementWithNoConstraints.instance).get(0);
    }

    @Override
    public final List<Integer> reorderReadSequence(ArrayList<BookieSocketAddress> ensemble, List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory) {
        if (UNKNOWN_REGION.equals(myRegion)) {
            return super.reorderReadSequence(ensemble, writeSet, bookieFailureHistory);
        } else {
            int ensembleSize = ensemble.size();
            List<Integer> finalList = new ArrayList<Integer>(writeSet.size());
            List<Integer> localList = new ArrayList<Integer>(writeSet.size());
            List<Long> localFailures = new ArrayList<Long>(writeSet.size());
            List<Integer> remoteList = new ArrayList<Integer>(writeSet.size());
            List<Long> remoteFailures = new ArrayList<Long>(writeSet.size());
            List<Integer> readOnlyList = new ArrayList<Integer>(writeSet.size());
            List<Integer> unAvailableList = new ArrayList<Integer>(writeSet.size());
            for (Integer idx : writeSet) {
                BookieSocketAddress address = ensemble.get(idx);
                String region = getRegion(address);
                Long lastFailedEntryOnBookie = bookieFailureHistory.get(address);
                if (null == knownBookies.get(address)) {
                    // there isn't too much differences between readonly bookies from unavailable bookies. since there
                    // is no write requests to them, so we shouldn't try reading from readonly bookie in prior to writable
                    // bookies.
                    if ((null == readOnlyBookies) || !readOnlyBookies.contains(address)) {
                        unAvailableList.add(idx);
                    } else {
                        readOnlyList.add(idx);
                    }
                } else if (region.equals(myRegion)) {
                    if ((lastFailedEntryOnBookie == null) || (lastFailedEntryOnBookie < 0)) {
                        localList.add(idx);
                    } else {
                         localFailures.add(lastFailedEntryOnBookie * ensembleSize + idx);
                    }
                } else {
                    if ((lastFailedEntryOnBookie == null) || (lastFailedEntryOnBookie < 0)) {
                        remoteList.add(idx);
                    } else {
                        remoteFailures.add(lastFailedEntryOnBookie * ensembleSize + idx);
                    }
                }
            }

            // Given that idx is less than ensemble size the order of the elements in these two lists
            // is determined by the lastFailedEntryOnBookie
            Collections.sort(localFailures);
            Collections.sort(remoteFailures);

            if (reorderReadsRandom) {
                Collections.shuffle(localList);
                Collections.shuffle(remoteList);
                Collections.shuffle(readOnlyList);
                Collections.shuffle(unAvailableList);
            }

            // nodes within a region are ordered as follows
            // (Random?) list of nodes that have no history of failure
            // Nodes with Failure history are ordered in the reverse
            // order of the most recent entry that generated an error
            for(long value: localFailures) {
                localList.add((int)(value % ensembleSize));
            }

            for(long value: remoteFailures) {
                remoteList.add((int)(value % ensembleSize));
            }

            // Insert a node from the remote region at the specified location so we
            // try more than one region within the max allowed latency
            for (int i = 0; i < REMOTE_NODE_IN_REORDER_SEQUENCE; i++) {
                if (localList.size() > 0) {
                    finalList.add(localList.remove(0));
                } else {
                    break;
                }
            }

            if (remoteList.size() > 0) {
                finalList.add(remoteList.remove(0));
            }

            // Add all the local nodes
            finalList.addAll(localList);
            finalList.addAll(remoteList);
            finalList.addAll(readOnlyList);
            finalList.addAll(unAvailableList);
            return finalList;
        }
    }

    @Override
    public final List<Integer> reorderReadLACSequence(ArrayList<BookieSocketAddress> ensemble, List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory) {
        if (UNKNOWN_REGION.equals(myRegion)) {
            return super.reorderReadLACSequence(ensemble, writeSet, bookieFailureHistory);
        }
        List<Integer> finalList = reorderReadSequence(ensemble, writeSet, bookieFailureHistory);

        if (finalList.size() < ensemble.size()) {
            for (int i = 0; i < ensemble.size(); i++) {
                if (!finalList.contains(i)) {
                    finalList.add(i);
                }
            }
        }
        return finalList;
    }
}
