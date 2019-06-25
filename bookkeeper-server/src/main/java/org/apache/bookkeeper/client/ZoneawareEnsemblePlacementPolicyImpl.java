/**
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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIES_JOINED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIES_LEFT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER;
import static org.apache.bookkeeper.client.BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import io.netty.util.HashedWheelTimer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.Configurable;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.NetworkTopologyImpl;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.net.StabilizeNetworkTopology;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple zoneaware ensemble placement policy.
 */
public class ZoneawareEnsemblePlacementPolicyImpl extends TopologyAwareEnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(ZoneawareEnsemblePlacementPolicyImpl.class);

    public static final String UNKNOWN_ZONE = "UnknownZone";
    /*
     * this defaultFaultDomain is used as placeholder network location for
     * bookies for which network location can't be resolved. In
     * ZoneawareEnsemblePlacementPolicyImpl zone is the fault domain and upgrade
     * domain is logical concept to enable parallel patching by bringing down
     * all the bookies in the upgrade domain.
     */
    private String defaultFaultDomain = NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN;
    protected ZoneAwareNodeLocation unresolvedNodeLocation = new ZoneAwareNodeLocation(
            NetworkTopology.DEFAULT_ZONE, NetworkTopology.DEFAULT_UPGRADEDOMAIN);
    private final Random rand;
    protected StatsLogger statsLogger = null;
    // Use a loading cache so slow bookies are expired. Use entryId as values.
    protected Cache<BookieSocketAddress, Long> slowBookies;
    protected BookieNode myNode = null;
    protected String myZone = null;
    protected boolean reorderReadsRandom = false;
    protected int stabilizePeriodSeconds = 0;
    protected int reorderThresholdPendingRequests = 0;
    protected int maxWeightMultiple;
    protected int minNumZonesPerWriteQuorum;
    protected int desiredNumZonesPerWriteQuorum;
    protected boolean enforceStrictZoneawarePlacement;
    protected HashedWheelTimer timer;
    protected final ConcurrentMap<BookieSocketAddress, ZoneAwareNodeLocation> address2NodePlacement;

    @StatsDoc(name = FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER, help = "Counter for number of times"
            + " DNSResolverDecorator failed to resolve Network Location")
    protected Counter failedToResolveNetworkLocationCounter = null;
    @StatsDoc(name = NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN, help = "Gauge for the number of writable"
            + " Bookies in default fault domain")
    protected Gauge<Integer> numWritableBookiesInDefaultFaultDomain;

    /**
     * Zone and UpgradeDomain pair of a node.
     */
    public static class ZoneAwareNodeLocation {
        private final String zone;
        private final String upgradeDomain;
        private final String repString;

        public ZoneAwareNodeLocation(String zone, String upgradeDomain) {
            this.zone = zone;
            this.upgradeDomain = upgradeDomain;
            repString = zone + upgradeDomain;
        }

        public String getZone() {
            return zone;
        }

        public String getUpgradeDomain() {
            return upgradeDomain;
        }

        @Override
        public int hashCode() {
            return repString.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return ((obj instanceof ZoneAwareNodeLocation)
                    && repString.equals(((ZoneAwareNodeLocation) obj).repString));
        }
    }


    ZoneawareEnsemblePlacementPolicyImpl() {
        super();
        address2NodePlacement = new ConcurrentHashMap<BookieSocketAddress, ZoneAwareNodeLocation>();
        rand = new Random(System.currentTimeMillis());
    }

    protected ZoneAwareNodeLocation getZoneAwareNodeLocation(BookieSocketAddress addr) {
        ZoneAwareNodeLocation nodeLocation = address2NodePlacement.get(addr);
        if (null == nodeLocation) {
            String networkLocation = resolveNetworkLocation(addr);
            if (getDefaultFaultDomain().equals(networkLocation)) {
                nodeLocation = unresolvedNodeLocation;
            } else {
                String[] parts = StringUtils.split(NodeBase.normalize(networkLocation), NodeBase.PATH_SEPARATOR);
                if (parts.length != 2) {
                    nodeLocation = unresolvedNodeLocation;
                } else {
                    nodeLocation = new ZoneAwareNodeLocation(NodeBase.PATH_SEPARATOR_STR + parts[0],
                            NodeBase.PATH_SEPARATOR_STR + parts[1]);
                }
            }
            address2NodePlacement.putIfAbsent(addr, nodeLocation);
        }
        return nodeLocation;
    }

    protected ZoneAwareNodeLocation getZoneAwareNodeLocation(BookieNode node) {
        if (null == node || null == node.getAddr()) {
            return unresolvedNodeLocation;
        }
        return getZoneAwareNodeLocation(node.getAddr());
    }

    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
            Optional<DNSToSwitchMapping> optionalDnsResolver, HashedWheelTimer timer, FeatureProvider featureProvider,
            StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        this.timer = timer;
        this.bookiesJoinedCounter = statsLogger.getOpStatsLogger(BOOKIES_JOINED);
        this.bookiesLeftCounter = statsLogger.getOpStatsLogger(BOOKIES_LEFT);
        this.failedToResolveNetworkLocationCounter = statsLogger.getCounter(FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER);
        this.numWritableBookiesInDefaultFaultDomain = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                rwLock.readLock().lock();
                try {
                    return topology.countNumOfAvailableNodes(getDefaultFaultDomain(), Collections.emptySet());
                } finally {
                    rwLock.readLock().unlock();
                }
            }
        };
        this.statsLogger.registerGauge(NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN,
                numWritableBookiesInDefaultFaultDomain);
        this.reorderThresholdPendingRequests = conf.getReorderThresholdPendingRequests();
        this.isWeighted = conf.getDiskWeightBasedPlacementEnabled();
        if (this.isWeighted) {
            this.maxWeightMultiple = conf.getBookieMaxWeightMultipleForWeightBasedPlacement();
            this.weightedSelection = new DynamicWeightedRandomSelectionImpl<BookieNode>(this.maxWeightMultiple);
            LOG.info("Weight based placement with max multiple of {}", this.maxWeightMultiple);
        } else {
            LOG.info("Not weighted");
        }
        this.minNumZonesPerWriteQuorum = conf.getMinNumZonesPerWriteQuorum();
        this.desiredNumZonesPerWriteQuorum = conf.getDesiredNumZonesPerWriteQuorum();
        this.enforceStrictZoneawarePlacement = conf.getEnforceStrictZoneawarePlacement();
        if (minNumZonesPerWriteQuorum > desiredNumZonesPerWriteQuorum) {
            LOG.error(
                    "It is misconfigured, for ZoneawareEnsemblePlacementPolicy, minNumZonesPerWriteQuorum: {} cann't be"
                            + " greater than desiredNumZonesPerWriteQuorum: {}",
                    minNumZonesPerWriteQuorum, desiredNumZonesPerWriteQuorum);
            throw new IllegalArgumentException("minNumZonesPerWriteQuorum: " + minNumZonesPerWriteQuorum
                    + " cann't be greater than desiredNumZonesPerWriteQuorum: " + desiredNumZonesPerWriteQuorum);
        }
        DNSToSwitchMapping actualDNSResolver;
        if (optionalDnsResolver.isPresent()) {
            actualDNSResolver = optionalDnsResolver.get();
        } else {
            String dnsResolverName = conf.getString(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
            actualDNSResolver = ReflectionUtils.newInstance(dnsResolverName, DNSToSwitchMapping.class);
            if (actualDNSResolver instanceof Configurable) {
                ((Configurable) actualDNSResolver).setConf(conf);
            }
        }

        this.dnsResolver = new DNSResolverDecorator(actualDNSResolver, () -> this.getDefaultFaultDomain(),
                failedToResolveNetworkLocationCounter);
        this.stabilizePeriodSeconds = conf.getNetworkTopologyStabilizePeriodSeconds();
        // create the network topology
        if (stabilizePeriodSeconds > 0) {
            this.topology = new StabilizeNetworkTopology(timer, stabilizePeriodSeconds);
        } else {
            this.topology = new NetworkTopologyImpl();
        }
        try {
            myNode = createBookieNode(new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0));
            myZone = getZoneAwareNodeLocation(myNode).getZone();
        } catch (UnknownHostException e) {
            LOG.error("Failed to get local host address : ", e);
            throw new RuntimeException(e);
        }
        LOG.info("Initialized zoneaware ensemble placement policy @ {} @ {} : {}.", myNode,
                myNode.getNetworkLocation(), dnsResolver.getClass().getName());

        slowBookies = CacheBuilder.newBuilder()
                .expireAfterWrite(conf.getBookieFailureHistoryExpirationMSec(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<BookieSocketAddress, Long>() {
                    @Override
                    public Long load(BookieSocketAddress key) throws Exception {
                        return -1L;
                    }
                });
        return this;
    }

    public ZoneawareEnsemblePlacementPolicyImpl withDefaultFaultDomain(String defaultFaultDomain) {
        checkNotNull(defaultFaultDomain, "Default fault domain cannot be null");

        String[] parts = StringUtils.split(NodeBase.normalize(defaultFaultDomain), NodeBase.PATH_SEPARATOR);
        if (parts.length != 2) {
            LOG.error("provided defaultFaultDomain: {} is not valid", defaultFaultDomain);
            throw new IllegalArgumentException("invalid defaultFaultDomain");
        } else {
            unresolvedNodeLocation = new ZoneAwareNodeLocation(NodeBase.PATH_SEPARATOR_STR + parts[0],
                    NodeBase.PATH_SEPARATOR_STR + parts[1]);
        }

        this.defaultFaultDomain = defaultFaultDomain;
        return this;
    }

    public String getDefaultFaultDomain() {
        return defaultFaultDomain;
    }

    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Set<BookieSocketAddress> excludeBookies,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble<BookieNode> parentEnsemble,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate<BookieNode> parentPredicate)
            throws BKNotEnoughBookiesException {
        throw new UnsupportedOperationException(
                "newEnsemble method with parentEnsemble and parentPredicate is not supported for "
                        + "ZoneawareEnsemblePlacementPolicyImpl");
    }

    @Override
    public BookieNode selectFromNetworkLocation(String networkLoc, Set<Node> excludeBookies,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate<BookieNode> predicate,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom) throws BKNotEnoughBookiesException {
        throw new UnsupportedOperationException(
                "selectFromNetworkLocation is not supported for ZoneawareEnsemblePlacementPolicyImpl");
    }

    @Override
    public BookieNode selectFromNetworkLocation(Set<String> excludeRacks, Set<Node> excludeBookies,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate<BookieNode> predicate,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom) throws BKNotEnoughBookiesException {
        throw new UnsupportedOperationException(
                "selectFromNetworkLocation is not supported for ZoneawareEnsemblePlacementPolicyImpl");
    }

    @Override
    public BookieNode selectFromNetworkLocation(String networkLoc, Set<String> excludeRacks, Set<Node> excludeBookies,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate<BookieNode> predicate,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom) throws BKNotEnoughBookiesException {
        throw new UnsupportedOperationException(
                "selectFromNetworkLocation is not supported for ZoneawareEnsemblePlacementPolicyImpl");
    }

    @Override
    public void uninitalize() {
    }

    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        if (enforceStrictZoneawarePlacement) {
            if (ensembleSize % writeQuorumSize != 0) {
                /*
                 * if ensembleSize is not multiple of writeQuorumSize, then the
                 * write quorums which are wrapped will have bookies from just
                 * minNumberOfZones though bookies are available from
                 * desiredNumZones.
                 *
                 * lets say for example - desiredZones = 3, minZones = 2,
                 * ensembleSize = 5, writeQuorumSize = 3, ackQuorumSize = 2
                 *
                 * z1, z2, z3, z1, z2 is a legal ensemble. (lets assume here z1
                 * represents a node belonging to zone z1)
                 *
                 * the writeQuorum for entry 3 will be z1, z2 and z1, since
                 * ackQuorumSize is 2, an entry could be written just to two
                 * bookies that belong to z1. If the zone z1 goes down then the
                 * entry could potentially be unavailable until the zone z1 has
                 * come back.
                 *
                 * Also, it is not ideal to allow combination which fallsback to
                 * minZones, when bookies are available from desiredNumZones.
                 *
                 * So prohibiting this combination of configuration.
                 */
                LOG.error("It is illegal for ensembleSize to be not multiple of"
                        + " writeQuorumSize When StrictZoneawarePlacement is enabled");
                throw new IllegalArgumentException("It is illegal for ensembleSize to be not multiple of"
                        + " writeQuorumSize When StrictZoneawarePlacement is enabled");
            }
            if (writeQuorumSize <= minNumZonesPerWriteQuorum) {
                /*
                 * if we allow writeQuorumSize <= minNumZonesPerWriteQuorum,
                 * then replaceBookie may fail to find a candidate to replace a
                 * node when a zone goes down.
                 *
                 * lets say for example - desiredZones = 3, minZones = 2,
                 * ensembleSize = 6, writeQuorumSize = 2, ackQuorumSize = 2
                 *
                 * z1, z2, z3, z1, z2, z3 is a legal ensemble. (lets assume here
                 * z1 represents a node belonging to zone z1)
                 *
                 * Now if Zone z2 goes down, you need to replace Index 1 and 4.
                 * To replace index 1, you need to find a zone that is not z1
                 * and Z3 which is not possible.
                 *
                 * So prohibiting this combination of configuration.
                 */
                LOG.error("It is illegal for writeQuorumSize to be lesser than or equal"
                        + " to minNumZonesPerWriteQuorum When StrictZoneawarePlacement is enabled");
                throw new IllegalArgumentException("It is illegal for writeQuorumSize to be lesser than or equal"
                        + " to minNumZonesPerWriteQuorum When StrictZoneawarePlacement is enabled");
            }
        }
        int desiredNumZonesPerWriteQuorumForThisEnsemble = Math.min(writeQuorumSize, desiredNumZonesPerWriteQuorum);
        List<BookieSocketAddress> newEnsemble = new ArrayList<BookieSocketAddress>(
                Collections.nCopies(ensembleSize, null));
        rwLock.readLock().lock();
        try {
            if (!enforceStrictZoneawarePlacement) {
                return createNewEnsembleRandomly(newEnsemble, writeQuorumSize, ackQuorumSize, customMetadata,
                        excludeBookies);
            }
            Set<BookieSocketAddress> comprehensiveExclusionBookiesSet = addDefaultFaultDomainBookies(excludeBookies);
            for (int index = 0; index < ensembleSize; index++) {
                BookieSocketAddress selectedBookie = setBookieInTheEnsemble(ensembleSize, writeQuorumSize, newEnsemble,
                        newEnsemble, index, desiredNumZonesPerWriteQuorumForThisEnsemble,
                        comprehensiveExclusionBookiesSet);
                comprehensiveExclusionBookiesSet.add(selectedBookie);
            }
            return PlacementResult.of(newEnsemble,
                    isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public PlacementResult<BookieSocketAddress> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        int bookieToReplaceIndex = currentEnsemble.indexOf(bookieToReplace);
        int desiredNumZonesPerWriteQuorumForThisEnsemble = (writeQuorumSize < desiredNumZonesPerWriteQuorum)
                ? writeQuorumSize : desiredNumZonesPerWriteQuorum;
        List<BookieSocketAddress> newEnsemble = new ArrayList<BookieSocketAddress>(currentEnsemble);
        rwLock.readLock().lock();
        try {
            if (!enforceStrictZoneawarePlacement) {
                return selectBookieRandomly(newEnsemble, bookieToReplace, excludeBookies, writeQuorumSize,
                        ackQuorumSize);
            }
            Set<BookieSocketAddress> comprehensiveExclusionBookiesSet = addDefaultFaultDomainBookies(excludeBookies);
            comprehensiveExclusionBookiesSet.addAll(currentEnsemble);
            BookieSocketAddress candidateAddr = setBookieInTheEnsemble(ensembleSize, writeQuorumSize, currentEnsemble,
                    newEnsemble, bookieToReplaceIndex, desiredNumZonesPerWriteQuorumForThisEnsemble,
                    comprehensiveExclusionBookiesSet);
            return PlacementResult.of(candidateAddr,
                    isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private PlacementResult<List<BookieSocketAddress>> createNewEnsembleRandomly(List<BookieSocketAddress> newEnsemble,
            int writeQuorumSize, int ackQuorumSize, Map<String, byte[]> customMetadata,
            Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        int ensembleSize = newEnsemble.size();
        Set<BookieNode> bookiesToConsider = getBookiesToConsider(excludeBookies);
        if (bookiesToConsider.size() < newEnsemble.size()) {
            LOG.error("Not enough bookies are available to form ensemble of size: {}", newEnsemble.size());
            throw new BKNotEnoughBookiesException();
        }

        for (int i = 0; i < ensembleSize; i++) {
            BookieNode candidateNode = selectCandidateNode(bookiesToConsider);
            newEnsemble.set(i, candidateNode.getAddr());
            bookiesToConsider.remove(candidateNode);
        }
        return PlacementResult.of(newEnsemble,
                isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
    }

    private PlacementResult<BookieSocketAddress> selectBookieRandomly(List<BookieSocketAddress> newEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies, int writeQuorumSize,
            int ackQuorumSize) throws BKNotEnoughBookiesException {
        Set<BookieSocketAddress> bookiesToExcludeIncludingEnsemble = new HashSet<BookieSocketAddress>(excludeBookies);
        bookiesToExcludeIncludingEnsemble.addAll(newEnsemble);
        Set<BookieNode> bookiesToConsider = getBookiesToConsider(bookiesToExcludeIncludingEnsemble);
        int bookieToReplaceIndex = newEnsemble.indexOf(bookieToReplace);

        if (bookiesToConsider.isEmpty()) {
            LOG.error("There is no bookie available to replace a bookie");
            throw new BKNotEnoughBookiesException();
        }
        BookieSocketAddress candidateAddr = (selectCandidateNode(bookiesToConsider)).getAddr();
        newEnsemble.set(bookieToReplaceIndex, candidateAddr);
        return PlacementResult.of(candidateAddr,
                isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
    }

    private Set<BookieNode> getBookiesToConsider(Set<BookieSocketAddress> excludeBookies) {
        Set<Node> leaves = topology.getLeaves(NodeBase.ROOT);
        Set<BookieNode> bookiesToConsider = new HashSet<BookieNode>();
        BookieNode bookieNode;
        for (Node leaf : leaves) {
            if (leaf instanceof BookieNode) {
                bookieNode = ((BookieNode) leaf);
                if (excludeBookies.contains(bookieNode.getAddr())) {
                    continue;
                }
                bookiesToConsider.add(bookieNode);
            }
        }
        return bookiesToConsider;
    }

    /*
     * This method finds the appropriate bookie for newEnsemble by finding
     * bookie to replace at bookieToReplaceIndex in the currentEnsemble.
     *
     * It goes through following filtering process 1) Exclude zones of
     * desiredNumZonesPerWriteQuorumForThisEnsemble neighboring nodes 2) Find
     * bookies to consider by excluding zones (found from previous step) and
     * excluding UDs of the zones to consider. 3) If it can't find eligible
     * bookie, then keep reducing the number of neighboring nodes to
     * minNumZonesPerWriteQuorum and repeat step 2. 4) If it still can't find
     * eligible bookies then find the zones to exclude such that in a writeset
     * there will be bookies from atleast minNumZonesPerWriteQuorum zones and
     * repeat step 2 5) After getting the list of eligible candidates select a
     * node randomly. 6) If step-4 couldn't find eligible candidates then throw
     * BKNotEnoughBookiesException.
     *
     * Example: Ensemble:6 Qw:6 desiredNumZonesPerWriteQuorumForThisEnsemble:3
     * minNumZonesPerWriteQuorum:2 The selection process is as follows:
     *
     * 1) Find bookies by excluding zones of
     * (desiredNumZonesPerWriteQuorumForThisEnsemble -1) neighboring bookies on
     * the left and and the right side of the bookieToReplaceIndex. i.e Zones of
     * 2 bookies(3-1) on both sides of the index in question will be excluded to
     * find bookies. 2) Get the set of zones of the bookies selected above. 3)
     * Get the UpgradeDomains to exclude of the each zone selected above to make
     * sure bookies of writeSets containing bookieToReplaceIndex are from
     * different UD if they belong to same zone. 4) now from the zones selected
     * in step 2, apply the filter of UDs to exclude found in previous step and
     * get the eligible bookies. 5) If no bookie matches this filter, then
     * instead of aiming for unique UDs, fallback to UDs to exclude such that if
     * bookies are from same zone in the writeSets containing
     * bookieToReplaceIndex then they must be atleast from 2 different UDs. 6)
     * now from the zones selected in step 2, apply the filter of UDs to exclude
     * found in previous step and get the eligible bookies. 7) If no bookie
     * matches this filter, repeat from Step1 to Step6 by decreasing neighboring
     * exclude zones from (desiredNumZonesPerWriteQuorumForThisEnsemble - 1),
     * which is 2 to (minNumZonesPerWriteQuorum - 1), which is 1 8) If even
     * after this, bookies are not found matching the criteria fallback to
     * minNumZonesPerWriteQuorum, for this find the zones to exclude such that
     * in writesets containing this bookieToReplaceIndex there will be bookies
     * from atleast minNumZonesPerWriteQuorum zones, which is 2. 9) Get the set
     * of the zones of the bookies by excluding zones selected above. 10) repeat
     * Step3 to Step6. 11) After getting the list of eligible candidates select
     * a node randomly. 12) If even after Step10 there are no eligible
     * candidates then throw BKNotEnoughBookiesException.
     */
    private BookieSocketAddress setBookieInTheEnsemble(int ensembleSize, int writeQuorumSize,
            List<BookieSocketAddress> currentEnsemble, List<BookieSocketAddress> newEnsemble, int bookieToReplaceIndex,
            int desiredNumZonesPerWriteQuorumForThisEnsemble, Set<BookieSocketAddress> excludeBookies)
                    throws BKNotEnoughBookiesException {
        BookieSocketAddress bookieToReplace = currentEnsemble.get(bookieToReplaceIndex);
        Set<String> zonesToExclude = null;
        Set<BookieNode> bookiesToConsiderAfterExcludingZonesAndUDs = null;
        for (int numberOfNeighborsToConsider = (desiredNumZonesPerWriteQuorumForThisEnsemble
                - 1); numberOfNeighborsToConsider >= (minNumZonesPerWriteQuorum - 1); numberOfNeighborsToConsider--) {
            zonesToExclude = getZonesOfNeighboringNodesInEnsemble(currentEnsemble, bookieToReplaceIndex,
                    (numberOfNeighborsToConsider));
            bookiesToConsiderAfterExcludingZonesAndUDs = getBookiesToConsiderAfterExcludingZonesAndUDs(ensembleSize,
                    writeQuorumSize, currentEnsemble, bookieToReplaceIndex, excludeBookies, zonesToExclude);
            if (!bookiesToConsiderAfterExcludingZonesAndUDs.isEmpty()) {
                break;
            }
        }
        if (bookiesToConsiderAfterExcludingZonesAndUDs.isEmpty()) {
            zonesToExclude = getZonesToExcludeToMaintainMinZones(currentEnsemble, bookieToReplaceIndex,
                    writeQuorumSize);
            bookiesToConsiderAfterExcludingZonesAndUDs = getBookiesToConsiderAfterExcludingZonesAndUDs(ensembleSize,
                    writeQuorumSize, currentEnsemble, bookieToReplaceIndex, excludeBookies, zonesToExclude);
        }
        if (bookiesToConsiderAfterExcludingZonesAndUDs.isEmpty()) {
            LOG.error("Not enough bookies are available to replaceBookie : {} in ensemble : {} with excludeBookies {}.",
                    bookieToReplace, currentEnsemble, excludeBookies);
            throw new BKNotEnoughBookiesException();
        }

        BookieSocketAddress candidateAddr = selectCandidateNode(bookiesToConsiderAfterExcludingZonesAndUDs).getAddr();
        newEnsemble.set(bookieToReplaceIndex, candidateAddr);
        return candidateAddr;
    }

    /*
     * this method should be called in readlock scope of 'rwLock'. This method
     * returns a new set, by adding excludedBookies and bookies in
     * defaultfaultdomain.
     */
    protected Set<BookieSocketAddress> addDefaultFaultDomainBookies(Set<BookieSocketAddress> excludeBookies) {
        Set<BookieSocketAddress> comprehensiveExclusionBookiesSet = new HashSet<BookieSocketAddress>(excludeBookies);
        Set<Node> defaultFaultDomainLeaves = topology.getLeaves(getDefaultFaultDomain());
        for (Node node : defaultFaultDomainLeaves) {
            if (node instanceof BookieNode) {
                comprehensiveExclusionBookiesSet.add(((BookieNode) node).getAddr());
            } else {
                LOG.error("found non-BookieNode: {} as leaf of defaultFaultDomain: {}", node, getDefaultFaultDomain());
            }
        }
        return comprehensiveExclusionBookiesSet;
    }

    /*
     * Select bookie randomly from the bookiesToConsiderAfterExcludingUDs set.
     * If diskWeightBasedPlacement is enabled then it will select node randomly
     * based on node weight.
     */
    private BookieNode selectCandidateNode(Set<BookieNode> bookiesToConsiderAfterExcludingUDs) {
        BookieNode candidate = null;
        if (!this.isWeighted) {
            int randSelIndex = rand.nextInt(bookiesToConsiderAfterExcludingUDs.size());
            int ind = 0;
            for (BookieNode bookieNode : bookiesToConsiderAfterExcludingUDs) {
                if (ind == randSelIndex) {
                    candidate = bookieNode;
                    break;
                }
                ind++;
            }
        } else {
            candidate = weightedSelection.getNextRandom(bookiesToConsiderAfterExcludingUDs);
        }
        return candidate;
    }

    private String getExcludedZonesString(Set<String> excludeZones) {
        if (excludeZones.isEmpty()) {
            return "";
        }
        StringBuilder excludedZonesString = new StringBuilder("~");
        boolean firstZone = true;
        for (String excludeZone : excludeZones) {
            if (!firstZone) {
                excludedZonesString.append(NetworkTopologyImpl.NODE_SEPARATOR);
            }
            excludedZonesString.append(excludeZone);
            firstZone = false;
        }
        return excludedZonesString.toString();
    }

    private Set<BookieNode> getBookiesToConsider(String excludedZonesString, Set<BookieSocketAddress> excludeBookies) {
        Set<BookieNode> bookiesToConsider = new HashSet<BookieNode>();
        Set<Node> leaves = topology.getLeaves(excludedZonesString);
        for (Node leaf : leaves) {
            BookieNode bookieNode = ((BookieNode) leaf);
            if (excludeBookies.contains(bookieNode.getAddr())) {
                continue;
            }
            bookiesToConsider.add(bookieNode);
        }
        return bookiesToConsider;
    }

    /*
     * For the position of 'bookieToReplaceIndex' in currentEnsemble, get the
     * set of bookies eligible by excluding the 'excludeZones' and
     * 'excludeBookies'. After excluding excludeZones and excludeBookies, it
     * would first try to exclude upgrade domains of neighboring nodes
     * (writeset) so the bookie would be from completely new upgrade domain
     * of a zone, if a writeset contains bookie from the zone. If Bookie is
     * not found matching this criteria, then it will fallback to maintain min
     * upgrade domains (two) from a zone, such that if multiple bookies in a
     * write quorum are from the same zone then they will be spread across two
     * upgrade domains.
     */
    private Set<BookieNode> getBookiesToConsiderAfterExcludingZonesAndUDs(int ensembleSize, int writeQuorumSize,
            List<BookieSocketAddress> currentEnsemble, int bookieToReplaceIndex,
            Set<BookieSocketAddress> excludeBookies, Set<String> excludeZones) {
        Set<BookieNode> bookiesToConsiderAfterExcludingZonesAndUDs = new HashSet<BookieNode>();
        HashMap<String, Set<String>> excludingUDsOfZonesToConsider = new HashMap<String, Set<String>>();
        Set<BookieNode> bookiesToConsiderAfterExcludingZones = getBookiesToConsider(
                getExcludedZonesString(excludeZones), excludeBookies);

        if (!bookiesToConsiderAfterExcludingZones.isEmpty()) {
            Set<String> zonesToConsider = getZonesOfBookies(bookiesToConsiderAfterExcludingZones);
            for (String zoneToConsider : zonesToConsider) {
                Set<String> upgradeDomainsOfAZoneInNeighboringNodes = getUpgradeDomainsOfAZoneInNeighboringNodes(
                        currentEnsemble, bookieToReplaceIndex, writeQuorumSize, zoneToConsider);
                excludingUDsOfZonesToConsider.put(zoneToConsider, upgradeDomainsOfAZoneInNeighboringNodes);
            }

            updateBookiesToConsiderAfterExcludingZonesAndUDs(bookiesToConsiderAfterExcludingZonesAndUDs,
                    bookiesToConsiderAfterExcludingZones, excludingUDsOfZonesToConsider);

            /*
             * If no eligible bookie is found, then instead of aiming for unique
             * UDs, fallback to UDs to exclude such that if bookies are from
             * same zone in the writeSets containing bookieToReplaceIndex then
             * they must be atleast from 2 different UDs
             */
            if (bookiesToConsiderAfterExcludingZonesAndUDs.isEmpty()) {
                excludingUDsOfZonesToConsider.clear();
                for (String zoneToConsider : zonesToConsider) {
                    Set<String> udsToExcludeToMaintainMinUDsInWriteQuorums =
                            getUDsToExcludeToMaintainMinUDsInWriteQuorums(currentEnsemble, bookieToReplaceIndex,
                                    writeQuorumSize, zoneToConsider);
                    excludingUDsOfZonesToConsider.put(zoneToConsider, udsToExcludeToMaintainMinUDsInWriteQuorums);
                }

                updateBookiesToConsiderAfterExcludingZonesAndUDs(bookiesToConsiderAfterExcludingZonesAndUDs,
                        bookiesToConsiderAfterExcludingZones, excludingUDsOfZonesToConsider);
            }
        }
        return bookiesToConsiderAfterExcludingZonesAndUDs;
    }

    /*
     * Filter bookies which belong to excludingUDs of zones to consider from
     * 'bookiesToConsider' set and add them to
     * 'bookiesToConsiderAfterExcludingUDs' set.
     */
    private void updateBookiesToConsiderAfterExcludingZonesAndUDs(Set<BookieNode> bookiesToConsiderAfterExcludingUDs,
            Set<BookieNode> bookiesToConsider, HashMap<String, Set<String>> excludingUDsOfZonesToConsider) {
        for (BookieNode bookieToConsider : bookiesToConsider) {
            ZoneAwareNodeLocation nodeLocation = getZoneAwareNodeLocation(bookieToConsider);
            if (excludingUDsOfZonesToConsider.get(nodeLocation.getZone()).contains(nodeLocation.getUpgradeDomain())) {
                continue;
            }
            bookiesToConsiderAfterExcludingUDs.add(bookieToConsider);
        }
    }

    /*
     * Gets the set of zones of neighboring nodes.
     */
    private Set<String> getZonesOfNeighboringNodesInEnsemble(List<BookieSocketAddress> currentEnsemble, int indexOfNode,
            int numOfNeighboringNodes) {
        Set<String> zonesOfNeighboringNodes = new HashSet<String>();
        int ensembleSize = currentEnsemble.size();
        for (int i = (-1 * numOfNeighboringNodes); i <= numOfNeighboringNodes; i++) {
            if (i == 0) {
                continue;
            }
            int index = (indexOfNode + i + ensembleSize) % ensembleSize;
            BookieSocketAddress addrofNode = currentEnsemble.get(index);
            if (addrofNode == null) {
                continue;
            }
            String zoneOfNode = getZoneAwareNodeLocation(addrofNode).getZone();
            zonesOfNeighboringNodes.add(zoneOfNode);
        }
        return zonesOfNeighboringNodes;
    }

    /*
     * This method returns set of zones to exclude for the position of
     * 'indexOfNode', so that writequorums, containing this index, would have
     * atleast minNumZonesPerWriteQuorum.
     */
    private Set<String> getZonesToExcludeToMaintainMinZones(List<BookieSocketAddress> currentEnsemble, int indexOfNode,
            int writeQuorumSize) {
        int ensSize = currentEnsemble.size();
        Set<String> zonesToExclude = new HashSet<String>();
        Set<String> zonesInWriteQuorum = new HashSet<String>();
        for (int i = -(writeQuorumSize - 1); i <= 0; i++) {
            zonesInWriteQuorum.clear();
            for (int j = 0; j < writeQuorumSize; j++) {
                int indexInEnsemble = (i + j + indexOfNode + ensSize) % ensSize;
                if (indexInEnsemble == indexOfNode) {
                    continue;
                }
                BookieSocketAddress bookieAddr = currentEnsemble.get(indexInEnsemble);
                if (bookieAddr == null) {
                    continue;
                }
                ZoneAwareNodeLocation nodeLocation = getZoneAwareNodeLocation(bookieAddr);
                zonesInWriteQuorum.add(nodeLocation.getZone());
            }
            if (zonesInWriteQuorum.size() <= (minNumZonesPerWriteQuorum - 1)) {
                zonesToExclude.addAll(zonesInWriteQuorum);
            }
        }
        return zonesToExclude;
    }

    private Set<String> getZonesOfBookies(Collection<BookieNode> bookieNodes) {
        Set<String> zonesOfBookies = new HashSet<String>();
        for (BookieNode bookieNode : bookieNodes) {
            ZoneAwareNodeLocation nodeLocation = getZoneAwareNodeLocation(bookieNode);
            zonesOfBookies.add(nodeLocation.getZone());
        }
        return zonesOfBookies;
    }

    /*
     * Gets the set of upgradedomains of neighboring nodes (writeQuorumSize)
     * which belong to this 'zone'.
     */
    private Set<String> getUpgradeDomainsOfAZoneInNeighboringNodes(List<BookieSocketAddress> currentEnsemble,
            int indexOfNode, int writeQuorumSize, String zone) {
        int ensSize = currentEnsemble.size();
        Set<String> upgradeDomainsOfAZoneInNeighboringNodes = new HashSet<String>();
        for (int i = -(writeQuorumSize - 1); i <= (writeQuorumSize - 1); i++) {
            if (i == 0) {
                continue;
            }
            int indexInEnsemble = (indexOfNode + i + ensSize) % ensSize;
            BookieSocketAddress bookieAddr = currentEnsemble.get(indexInEnsemble);
            if (bookieAddr == null) {
                continue;
            }
            ZoneAwareNodeLocation nodeLocation = getZoneAwareNodeLocation(bookieAddr);
            if (nodeLocation.getZone().equals(zone)) {
                upgradeDomainsOfAZoneInNeighboringNodes.add(nodeLocation.getUpgradeDomain());
            }
        }
        return upgradeDomainsOfAZoneInNeighboringNodes;
    }

    /*
     * This method returns set of UpgradeDomains to exclude if a bookie from
     * the 'zone' has to be selected for the position of 'indexOfNode', then if
     * there are multiple bookies from the 'zone' in a write quorum then they
     * will be atleast from minimum of two upgrade domains.
     */
    private Set<String> getUDsToExcludeToMaintainMinUDsInWriteQuorums(List<BookieSocketAddress> currentEnsemble,
            int indexOfNode, int writeQuorumSize, String zone) {
        int ensSize = currentEnsemble.size();
        Set<String> upgradeDomainsToExclude = new HashSet<String>();
        Set<String> upgradeDomainsOfThisZoneInWriteQuorum = new HashSet<String>();
        for (int i = -(writeQuorumSize - 1); i <= 0; i++) {
            upgradeDomainsOfThisZoneInWriteQuorum.clear();
            for (int j = 0; j < writeQuorumSize; j++) {
                int indexInEnsemble = (i + j + indexOfNode + ensSize) % ensSize;
                if (indexInEnsemble == indexOfNode) {
                    continue;
                }
                BookieSocketAddress bookieAddr = currentEnsemble.get(indexInEnsemble);
                if (bookieAddr == null) {
                    continue;
                }
                ZoneAwareNodeLocation nodeLocation = getZoneAwareNodeLocation(bookieAddr);
                if (nodeLocation.getZone().equals(zone)) {
                    upgradeDomainsOfThisZoneInWriteQuorum.add(nodeLocation.getUpgradeDomain());
                }
            }
            if (upgradeDomainsOfThisZoneInWriteQuorum.size() == 1) {
                upgradeDomainsToExclude.addAll(upgradeDomainsOfThisZoneInWriteQuorum);
            }
        }
        return upgradeDomainsToExclude;
    }

    @Override
    public void registerSlowBookie(BookieSocketAddress bookieSocketAddress, long entryId) {
        // TODO Auto-generated method stub
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        return writeSet;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        DistributionSchedule.WriteSet retList = reorderReadSequence(ensemble, bookiesHealthInfo, writeSet);
        retList.addMissingIndices(ensemble.size());
        return retList;
    }

    /*
     * In ZoneAwareEnsemblePlacementPolicy if bookies in the writeset are from
     * 'desiredNumOfZones' then it is considered as MEETS_STRICT if they are
     * from 'minNumOfZones' then it is considered as MEETS_SOFT otherwise
     * considered as FAIL. Also in a writeset if there are multiple bookies from
     * the same zone then they are expected to be from different upgrade
     * domains.
     */
    @Override
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieSocketAddress> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        PlacementPolicyAdherence placementPolicyAdherence = PlacementPolicyAdherence.MEETS_STRICT;
        rwLock.readLock().lock();
        try {
            HashMap<String, Set<String>> bookiesLocationInWriteSet = new HashMap<String, Set<String>>();
            HashMap<String, Integer> numOfBookiesInZones = new HashMap<String, Integer>();
            BookieSocketAddress bookieNode;
            if (ensembleList.size() % writeQuorumSize != 0) {
                placementPolicyAdherence = PlacementPolicyAdherence.FAIL;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "For ensemble: {}, ensembleSize: {} is not a multiple of writeQuorumSize: {}",
                            ensembleList, ensembleList.size(), writeQuorumSize);
                }
                return placementPolicyAdherence;
            }
            if (writeQuorumSize <= minNumZonesPerWriteQuorum) {
                placementPolicyAdherence = PlacementPolicyAdherence.FAIL;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "For ensemble: {}, writeQuorumSize: {} is less than or equal to"
                            + " minNumZonesPerWriteQuorum: {}",
                            ensembleList, writeQuorumSize, minNumZonesPerWriteQuorum);
                }
                return placementPolicyAdherence;
            }
            int desiredNumZonesPerWriteQuorumForThisEnsemble = Math.min(writeQuorumSize, desiredNumZonesPerWriteQuorum);
            for (int i = 0; i < ensembleList.size(); i++) {
                bookiesLocationInWriteSet.clear();
                numOfBookiesInZones.clear();
                for (int j = 0; j < writeQuorumSize; j++) {
                    int indexOfNode = (i + j) % ensembleList.size();
                    bookieNode = ensembleList.get(indexOfNode);
                    ZoneAwareNodeLocation nodeLocation = getZoneAwareNodeLocation(bookieNode);
                    if (nodeLocation.equals(unresolvedNodeLocation)) {
                        placementPolicyAdherence = PlacementPolicyAdherence.FAIL;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("ensemble: {}, contains bookie: {} for which network location is unresolvable",
                                    ensembleList, bookieNode);
                        }
                        return placementPolicyAdherence;
                    }
                    String zone = nodeLocation.getZone();
                    String upgradeDomain = nodeLocation.getUpgradeDomain();
                    Set<String> udsOfThisZoneInThisWriteSet = bookiesLocationInWriteSet.get(zone);
                    if (udsOfThisZoneInThisWriteSet == null) {
                        udsOfThisZoneInThisWriteSet = new HashSet<String>();
                        udsOfThisZoneInThisWriteSet.add(upgradeDomain);
                        bookiesLocationInWriteSet.put(zone, udsOfThisZoneInThisWriteSet);
                        numOfBookiesInZones.put(zone, 1);
                    } else {
                        udsOfThisZoneInThisWriteSet.add(upgradeDomain);
                        Integer numOfNodesInAZone = numOfBookiesInZones.get(zone);
                        numOfBookiesInZones.put(zone, (numOfNodesInAZone + 1));
                    }
                }
                if (numOfBookiesInZones.entrySet().size() < minNumZonesPerWriteQuorum) {
                    placementPolicyAdherence = PlacementPolicyAdherence.FAIL;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("in ensemble: {}, writeset starting at: {} doesn't contain bookies from"
                                + " minNumZonesPerWriteQuorum: {}", ensembleList, i, minNumZonesPerWriteQuorum);
                    }
                    return placementPolicyAdherence;
                } else if (numOfBookiesInZones.entrySet().size() >= desiredNumZonesPerWriteQuorumForThisEnsemble) {
                    if (!validateMinUDsAreMaintained(numOfBookiesInZones, bookiesLocationInWriteSet)) {
                        placementPolicyAdherence = PlacementPolicyAdherence.FAIL;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("in ensemble: {}, writeset starting at: {} doesn't maintain min of 2 UDs"
                                    + " when there are multiple bookies from the same zone.", ensembleList, i);
                        }
                        return placementPolicyAdherence;
                    }
                } else {
                    if (!validateMinUDsAreMaintained(numOfBookiesInZones, bookiesLocationInWriteSet)) {
                        placementPolicyAdherence = PlacementPolicyAdherence.FAIL;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("in ensemble: {}, writeset starting at: {} doesn't maintain min of 2 UDs"
                                    + " when there are multiple bookies from the same zone.", ensembleList, i);
                        }
                        return placementPolicyAdherence;
                    }
                    if (placementPolicyAdherence == PlacementPolicyAdherence.MEETS_STRICT) {
                        placementPolicyAdherence = PlacementPolicyAdherence.MEETS_SOFT;
                    }
                }
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return placementPolicyAdherence;
    }

    private boolean validateMinUDsAreMaintained(HashMap<String, Integer> numOfNodesInZones,
            HashMap<String, Set<String>> nodesLocationInWriteSet) {
        for (Entry<String, Integer> numOfNodesInZone : numOfNodesInZones.entrySet()) {
            String zone = numOfNodesInZone.getKey();
            Integer numOfNodesInThisZone = numOfNodesInZone.getValue();
            if (numOfNodesInThisZone > 1) {
                Set<String> udsOfThisZone = nodesLocationInWriteSet.get(zone);
                if (udsOfThisZone.size() < 2) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean areAckedBookiesAdheringToPlacementPolicy(Set<BookieSocketAddress> ackedBookies, int writeQuorumSize,
            int ackQuorumSize) {
        HashSet<String> zonesOfAckedBookies = new HashSet<>();
        int minNumZonesPerWriteQuorumForThisEnsemble = Math.min(writeQuorumSize, minNumZonesPerWriteQuorum);
        boolean areAckedBookiesAdheringToPlacementPolicy = false;
        ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
        readLock.lock();
        try {
            for (BookieSocketAddress ackedBookie : ackedBookies) {
                zonesOfAckedBookies.add(getZoneAwareNodeLocation(ackedBookie).getZone());
            }
            areAckedBookiesAdheringToPlacementPolicy = ((zonesOfAckedBookies
                    .size() >= minNumZonesPerWriteQuorumForThisEnsemble) && (ackedBookies.size() >= ackQuorumSize));
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "areAckedBookiesAdheringToPlacementPolicy returning {}, because number of ackedBookies = {},"
                                + " number of Zones of ackedbookies = {},"
                                + " number of minNumZonesPerWriteQuorumForThisEnsemble = {}",
                        areAckedBookiesAdheringToPlacementPolicy, ackedBookies.size(), zonesOfAckedBookies.size(),
                        minNumZonesPerWriteQuorumForThisEnsemble);
            }
        } finally {
            readLock.unlock();
        }
        return areAckedBookiesAdheringToPlacementPolicy;
    }
}
