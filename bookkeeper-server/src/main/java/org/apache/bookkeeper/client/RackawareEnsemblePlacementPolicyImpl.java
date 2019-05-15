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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIES_JOINED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIES_LEFT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK;
import static org.apache.bookkeeper.client.BookKeeperClientStats.READ_REQUESTS_REORDERED;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.UNKNOWN_REGION;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import io.netty.util.HashedWheelTimer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
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
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple rackware ensemble placement policy.
 *
 * <p>Make most of the class and methods as protected, so it could be extended to implement other algorithms.
 */
@StatsDoc(
    name = CLIENT_SCOPE,
    help = "BookKeeper client stats"
)
public class RackawareEnsemblePlacementPolicyImpl extends TopologyAwareEnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(RackawareEnsemblePlacementPolicyImpl.class);
    int maxWeightMultiple;

    protected int minNumRacksPerWriteQuorum;
    protected boolean enforceMinNumRacksPerWriteQuorum;
    protected boolean ignoreLocalNodeInPlacementPolicy;

    public static final String REPP_DNS_RESOLVER_CLASS = "reppDnsResolverClass";
    public static final String REPP_RANDOM_READ_REORDERING = "ensembleRandomReadReordering";

    static final int RACKNAME_DISTANCE_FROM_LEAVES = 1;

    // masks for reordering
    static final int LOCAL_MASK       = 0x01 << 24;
    static final int LOCAL_FAIL_MASK  = 0x02 << 24;
    static final int REMOTE_MASK      = 0x04 << 24;
    static final int REMOTE_FAIL_MASK = 0x08 << 24;
    static final int READ_ONLY_MASK   = 0x10 << 24;
    static final int SLOW_MASK        = 0x20 << 24;
    static final int UNAVAIL_MASK     = 0x40 << 24;
    static final int MASK_BITS        = 0xFFF << 20;

    protected HashedWheelTimer timer;
    // Use a loading cache so slow bookies are expired. Use entryId as values.
    protected Cache<BookieSocketAddress, Long> slowBookies;
    protected BookieNode localNode;
    protected boolean reorderReadsRandom = false;
    protected boolean enforceDurability = false;
    protected int stabilizePeriodSeconds = 0;
    protected int reorderThresholdPendingRequests = 0;
    // looks like these only assigned in the same thread as constructor, immediately after constructor;
    // no need to make volatile
    protected StatsLogger statsLogger = null;

    @StatsDoc(
            name = READ_REQUESTS_REORDERED,
            help = "The distribution of number of bookies reordered on each read request"
    )
    protected OpStatsLogger readReorderedCounter = null;
    @StatsDoc(
            name = FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER,
            help = "Counter for number of times DNSResolverDecorator failed to resolve Network Location"
    )
    protected Counter failedToResolveNetworkLocationCounter = null;
    @StatsDoc(
            name = NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK,
            help = "Gauge for the number of writable Bookies in default rack"
    )
    protected Gauge<Integer> numWritableBookiesInDefaultRack;

    private String defaultRack = NetworkTopology.DEFAULT_RACK;

    RackawareEnsemblePlacementPolicyImpl() {
        this(false);
    }

    RackawareEnsemblePlacementPolicyImpl(boolean enforceDurability) {
        this.enforceDurability = enforceDurability;
        topology = new NetworkTopologyImpl();
    }

    /**
     * Initialize the policy.
     *
     * @param dnsResolver the object used to resolve addresses to their network address
     * @return initialized ensemble placement policy
     */
    protected RackawareEnsemblePlacementPolicyImpl initialize(DNSToSwitchMapping dnsResolver,
                                                              HashedWheelTimer timer,
                                                              boolean reorderReadsRandom,
                                                              int stabilizePeriodSeconds,
                                                              int reorderThresholdPendingRequests,
                                                              boolean isWeighted,
                                                              int maxWeightMultiple,
                                                              int minNumRacksPerWriteQuorum,
                                                              boolean enforceMinNumRacksPerWriteQuorum,
                                                              boolean ignoreLocalNodeInPlacementPolicy,
                                                              StatsLogger statsLogger) {
        checkNotNull(statsLogger, "statsLogger should not be null, use NullStatsLogger instead.");
        this.statsLogger = statsLogger;
        this.bookiesJoinedCounter = statsLogger.getOpStatsLogger(BOOKIES_JOINED);
        this.bookiesLeftCounter = statsLogger.getOpStatsLogger(BOOKIES_LEFT);
        this.readReorderedCounter = statsLogger.getOpStatsLogger(READ_REQUESTS_REORDERED);
        this.failedToResolveNetworkLocationCounter = statsLogger.getCounter(FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER);
        this.numWritableBookiesInDefaultRack = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                rwLock.readLock().lock();
                try {
                    return topology.countNumOfAvailableNodes(getDefaultRack(), Collections.emptySet());
                } finally {
                    rwLock.readLock().unlock();
                }
            }
        };
        this.statsLogger.registerGauge(NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK, numWritableBookiesInDefaultRack);
        this.reorderReadsRandom = reorderReadsRandom;
        this.stabilizePeriodSeconds = stabilizePeriodSeconds;
        this.reorderThresholdPendingRequests = reorderThresholdPendingRequests;
        this.dnsResolver = new DNSResolverDecorator(dnsResolver, () -> this.getDefaultRack(),
                failedToResolveNetworkLocationCounter);
        this.timer = timer;
        this.minNumRacksPerWriteQuorum = minNumRacksPerWriteQuorum;
        this.enforceMinNumRacksPerWriteQuorum = enforceMinNumRacksPerWriteQuorum;
        this.ignoreLocalNodeInPlacementPolicy = ignoreLocalNodeInPlacementPolicy;

        // create the network topology
        if (stabilizePeriodSeconds > 0) {
            this.topology = new StabilizeNetworkTopology(timer, stabilizePeriodSeconds);
        } else {
            this.topology = new NetworkTopologyImpl();
        }

        BookieNode bn = null;
        if (!ignoreLocalNodeInPlacementPolicy) {
            try {
                bn = createBookieNode(new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0));
            } catch (UnknownHostException e) {
                LOG.error("Failed to get local host address : ", e);
            }
        } else {
            LOG.info("Ignoring LocalNode in Placementpolicy");
        }
        localNode = bn;
        LOG.info("Initialize rackaware ensemble placement policy @ {} @ {} : {}.",
                localNode, null == localNode ? "Unknown" : localNode.getNetworkLocation(),
                dnsResolver.getClass().getName());

        this.isWeighted = isWeighted;
        if (this.isWeighted) {
            this.maxWeightMultiple = maxWeightMultiple;
            this.weightedSelection = new WeightedRandomSelectionImpl<BookieNode>(this.maxWeightMultiple);
            LOG.info("Weight based placement with max multiple of " + this.maxWeightMultiple);
        } else {
            LOG.info("Not weighted");
        }
        return this;
    }

    /*
     * sets default rack for the policy.
     * i.e. region-aware policy may want to have /region/rack while regular
     * rack-aware policy needs /rack only since we cannot mix both styles
     */
    public RackawareEnsemblePlacementPolicyImpl withDefaultRack(String rack) {
        checkNotNull(rack, "Default rack cannot be null");

        this.defaultRack = rack;
        return this;
    }

    public String getDefaultRack() {
        return defaultRack;
    }

    @Override
    public RackawareEnsemblePlacementPolicyImpl initialize(ClientConfiguration conf,
                                                           Optional<DNSToSwitchMapping> optionalDnsResolver,
                                                           HashedWheelTimer timer,
                                                           FeatureProvider featureProvider,
                                                           StatsLogger statsLogger) {
        DNSToSwitchMapping dnsResolver;
        if (optionalDnsResolver.isPresent()) {
            dnsResolver = optionalDnsResolver.get();
        } else {
            String dnsResolverName = conf.getString(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
            try {
                dnsResolver = ReflectionUtils.newInstance(dnsResolverName, DNSToSwitchMapping.class);
                if (dnsResolver instanceof Configurable) {
                    ((Configurable) dnsResolver).setConf(conf);
                }

                if (dnsResolver instanceof RackChangeNotifier) {
                    ((RackChangeNotifier) dnsResolver).registerRackChangeListener(this);
                }
            } catch (RuntimeException re) {
                if (!conf.getEnforceMinNumRacksPerWriteQuorum()) {
                    LOG.error("Failed to initialize DNS Resolver {}, used default subnet resolver : {}",
                            dnsResolverName, re, re.getMessage());
                    dnsResolver = new DefaultResolver(() -> this.getDefaultRack());
                } else {
                    /*
                     * if minNumRacksPerWriteQuorum is enforced, then it
                     * shouldn't continue in the case of failure to create
                     * dnsResolver.
                     */
                    throw re;
                }
            }
        }
        slowBookies = CacheBuilder.newBuilder()
            .expireAfterWrite(conf.getBookieFailureHistoryExpirationMSec(), TimeUnit.MILLISECONDS)
            .build(new CacheLoader<BookieSocketAddress, Long>() {
                @Override
                public Long load(BookieSocketAddress key) throws Exception {
                    return -1L;
                }
            });
        return initialize(
                dnsResolver,
                timer,
                conf.getBoolean(REPP_RANDOM_READ_REORDERING, false),
                conf.getNetworkTopologyStabilizePeriodSeconds(),
                conf.getReorderThresholdPendingRequests(),
                conf.getDiskWeightBasedPlacementEnabled(),
                conf.getBookieMaxWeightMultipleForWeightBasedPlacement(),
                conf.getMinNumRacksPerWriteQuorum(),
                conf.getEnforceMinNumRacksPerWriteQuorum(),
                conf.getIgnoreLocalNodeInPlacementPolicy(),
                statsLogger);
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    /*
     * this method should be called in readlock scope of 'rwLock'
     */
    protected Set<BookieSocketAddress> addDefaultRackBookiesIfMinNumRacksIsEnforced(
            Set<BookieSocketAddress> excludeBookies) {
        Set<BookieSocketAddress> comprehensiveExclusionBookiesSet;
        if (enforceMinNumRacksPerWriteQuorum) {
            Set<BookieSocketAddress> bookiesInDefaultRack = null;
            Set<Node> defaultRackLeaves = topology.getLeaves(getDefaultRack());
            for (Node node : defaultRackLeaves) {
                if (node instanceof BookieNode) {
                    if (bookiesInDefaultRack == null) {
                        bookiesInDefaultRack = new HashSet<BookieSocketAddress>(excludeBookies);
                    }
                    bookiesInDefaultRack.add(((BookieNode) node).getAddr());
                } else {
                    LOG.error("found non-BookieNode: {} as leaf of defaultrack: {}", node, getDefaultRack());
                }
            }
            if ((bookiesInDefaultRack == null) || bookiesInDefaultRack.isEmpty()) {
                comprehensiveExclusionBookiesSet = excludeBookies;
            } else {
                comprehensiveExclusionBookiesSet = new HashSet<BookieSocketAddress>(excludeBookies);
                comprehensiveExclusionBookiesSet.addAll(bookiesInDefaultRack);
                LOG.info("enforceMinNumRacksPerWriteQuorum is enabled, so Excluding bookies of defaultRack: {}",
                        bookiesInDefaultRack);
            }
        } else {
            comprehensiveExclusionBookiesSet = excludeBookies;
        }
        return comprehensiveExclusionBookiesSet;
    }

    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            Set<BookieSocketAddress> comprehensiveExclusionBookiesSet = addDefaultRackBookiesIfMinNumRacksIsEnforced(
                    excludeBookies);
            PlacementResult<List<BookieSocketAddress>> newEnsembleResult = newEnsembleInternal(ensembleSize,
                    writeQuorumSize, ackQuorumSize, comprehensiveExclusionBookiesSet, null, null);
            return newEnsembleResult;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize,
                                                                  int writeQuorumSize,
                                                                  int ackQuorumSize,
                                                                  Set<BookieSocketAddress> excludeBookies,
                                                                  Ensemble<BookieNode> parentEnsemble,
                                                                  Predicate<BookieNode> parentPredicate)
            throws BKNotEnoughBookiesException {
        return newEnsembleInternal(
                ensembleSize,
                writeQuorumSize,
                ackQuorumSize,
                excludeBookies,
                parentEnsemble,
                parentPredicate);
    }

    protected PlacementResult<List<BookieSocketAddress>> newEnsembleInternal(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Set<BookieSocketAddress> excludeBookies,
            Ensemble<BookieNode> parentEnsemble,
            Predicate<BookieNode> parentPredicate) throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            int minNumRacksPerWriteQuorumForThisEnsemble = Math.min(writeQuorumSize, minNumRacksPerWriteQuorum);
            RRTopologyAwareCoverageEnsemble ensemble =
                    new RRTopologyAwareCoverageEnsemble(
                            ensembleSize,
                            writeQuorumSize,
                            ackQuorumSize,
                            RACKNAME_DISTANCE_FROM_LEAVES,
                            parentEnsemble,
                            parentPredicate,
                            minNumRacksPerWriteQuorumForThisEnsemble);
            BookieNode prevNode = null;
            int numRacks = topology.getNumOfRacks();
            // only one rack, use the random algorithm.
            if (numRacks < 2) {
                if (enforceMinNumRacksPerWriteQuorum && (minNumRacksPerWriteQuorumForThisEnsemble > 1)) {
                    LOG.error("Only one rack available and minNumRacksPerWriteQuorum is enforced, so giving up");
                    throw new BKNotEnoughBookiesException();
                }
                List<BookieNode> bns = selectRandom(ensembleSize, excludeNodes, TruePredicate.INSTANCE,
                        ensemble);
                ArrayList<BookieSocketAddress> addrs = new ArrayList<BookieSocketAddress>(ensembleSize);
                for (BookieNode bn : bns) {
                    addrs.add(bn.getAddr());
                }
                return PlacementResult.of(addrs, PlacementPolicyAdherence.FAIL);
            }

            for (int i = 0; i < ensembleSize; i++) {
                String curRack;
                if (null == prevNode) {
                    if ((null == localNode) || defaultRack.equals(localNode.getNetworkLocation())) {
                        curRack = NodeBase.ROOT;
                    } else {
                        curRack = localNode.getNetworkLocation();
                    }
                } else {
                    curRack = "~" + prevNode.getNetworkLocation();
                }
                boolean firstBookieInTheEnsemble = (null == prevNode);
                prevNode = selectFromNetworkLocation(curRack, excludeNodes, ensemble, ensemble,
                        !enforceMinNumRacksPerWriteQuorum || firstBookieInTheEnsemble);
            }
            List<BookieSocketAddress> bookieList = ensemble.toList();
            if (ensembleSize != bookieList.size()) {
                LOG.error("Not enough {} bookies are available to form an ensemble : {}.",
                          ensembleSize, bookieList);
                throw new BKNotEnoughBookiesException();
            }
            return PlacementResult.of(bookieList,
                                      isEnsembleAdheringToPlacementPolicy(
                                              bookieList, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public PlacementResult<BookieSocketAddress> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            excludeBookies = addDefaultRackBookiesIfMinNumRacksIsEnforced(excludeBookies);
            excludeBookies.addAll(currentEnsemble);
            BookieNode bn = knownBookies.get(bookieToReplace);
            if (null == bn) {
                bn = createBookieNode(bookieToReplace);
            }

            Set<Node> ensembleNodes = convertBookiesToNodes(currentEnsemble);
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);

            excludeNodes.addAll(ensembleNodes);
            excludeNodes.add(bn);
            ensembleNodes.remove(bn);

            Set<String> networkLocationsToBeExcluded = getNetworkLocations(ensembleNodes);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Try to choose a new bookie to replace {} from ensemble {}, excluding {}.",
                    bookieToReplace, ensembleNodes, excludeNodes);
            }
            // pick a candidate from same rack to replace
            BookieNode candidate = selectFromNetworkLocation(
                    bn.getNetworkLocation(),
                    networkLocationsToBeExcluded,
                    excludeNodes,
                    TruePredicate.INSTANCE,
                    EnsembleForReplacementWithNoConstraints.INSTANCE,
                    !enforceMinNumRacksPerWriteQuorum);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Bookie {} is chosen to replace bookie {}.", candidate, bn);
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

    @Override
    public BookieNode selectFromNetworkLocation(
            String networkLoc,
            Set<Node> excludeBookies,
            Predicate<BookieNode> predicate,
            Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom)
            throws BKNotEnoughBookiesException {
        // select one from local rack
        try {
            return selectRandomFromRack(networkLoc, excludeBookies, predicate, ensemble);
        } catch (BKNotEnoughBookiesException e) {
            if (!fallbackToRandom) {
                LOG.error(
                        "Failed to choose a bookie from {} : "
                                + "excluded {}, enforceMinNumRacksPerWriteQuorum is enabled so giving up.",
                        networkLoc, excludeBookies);
                throw e;
            }
            LOG.warn("Failed to choose a bookie from {} : "
                     + "excluded {}, fallback to choose bookie randomly from the cluster.",
                     networkLoc, excludeBookies);
            // randomly choose one from whole cluster, ignore the provided predicate.
            return selectRandom(1, excludeBookies, predicate, ensemble).get(0);
        }
    }

    @Override
    public BookieNode selectFromNetworkLocation(String networkLoc,
                                                   Set<String> excludeRacks,
                                                   Set<Node> excludeBookies,
                                                   Predicate<BookieNode> predicate,
                                                   Ensemble<BookieNode> ensemble,
                                                   boolean fallbackToRandom)
            throws BKNotEnoughBookiesException {
        // first attempt to select one from local rack
        try {
            return selectRandomFromRack(networkLoc, excludeBookies, predicate, ensemble);
        } catch (BKNotEnoughBookiesException e) {
            /*
             * there is no enough bookie from local rack, select bookies from
             * the whole cluster and exclude the racks specified at
             * <tt>excludeRacks</tt>.
             */
            return selectFromNetworkLocation(excludeRacks, excludeBookies, predicate, ensemble, fallbackToRandom);
        }
    }


    /**
     * It randomly selects a {@link BookieNode} that is not on the <i>excludeRacks</i> set, excluding the nodes in
     * <i>excludeBookies</i> set. If it fails to find one, it selects a random {@link BookieNode} from the whole
     * cluster.
     */
    @Override
    public BookieNode selectFromNetworkLocation(Set<String> excludeRacks,
                                                   Set<Node> excludeBookies,
                                                   Predicate<BookieNode> predicate,
                                                   Ensemble<BookieNode> ensemble,
                                                   boolean fallbackToRandom)
            throws BKNotEnoughBookiesException {

        List<BookieNode> knownNodes = new ArrayList<>(knownBookies.values());
        Set<Node> fullExclusionBookiesList = new HashSet<Node>(excludeBookies);
        for (BookieNode knownNode : knownNodes) {
            if (excludeRacks.contains(knownNode.getNetworkLocation())) {
                fullExclusionBookiesList.add(knownNode);
            }
        }

        try {
            return selectRandomInternal(knownNodes, 1, fullExclusionBookiesList, predicate, ensemble).get(0);
        } catch (BKNotEnoughBookiesException e) {
            if (!fallbackToRandom) {
                LOG.error(
                        "Failed to choose a bookie excluding Racks: {} "
                                + "Nodes: {}, enforceMinNumRacksPerWriteQuorum is enabled so giving up.",
                        excludeRacks, excludeBookies);
                throw e;
            }

            LOG.warn("Failed to choose a bookie: excluded {}, fallback to choose bookie randomly from the cluster.",
                    excludeBookies);
            // randomly choose one from whole cluster
            return selectRandom(1, excludeBookies, predicate, ensemble).get(0);
        }
    }

    private WeightedRandomSelection<BookieNode> prepareForWeightedSelection(List<Node> leaves) {
        // create a map of bookieNode->freeDiskSpace for this rack. The assumption is that
        // the number of nodes in a rack is of the order of 40, so it shouldn't be too bad
        // to build it every time during a ledger creation
        Map<BookieNode, WeightedObject> rackMap = new HashMap<BookieNode, WeightedObject>();
        for (Node n : leaves) {
            if (!(n instanceof BookieNode)) {
                continue;
            }
            BookieNode bookie = (BookieNode) n;
            if (this.bookieInfoMap.containsKey(bookie)) {
                rackMap.put(bookie, this.bookieInfoMap.get(bookie));
            } else {
                rackMap.put(bookie, new BookieInfo());
            }
        }
        if (rackMap.size() == 0) {
            return null;
        }

        WeightedRandomSelection<BookieNode> wRSelection = new WeightedRandomSelectionImpl<BookieNode>(
                maxWeightMultiple);
        wRSelection.updateMap(rackMap);
        return wRSelection;
    }

    /**
     * Choose random node under a given network path.
     *
     * @param netPath
     *          network path
     * @param excludeBookies
     *          exclude bookies
     * @param predicate
     *          predicate to check whether the target is a good target.
     * @param ensemble
     *          ensemble structure
     * @return chosen bookie.
     */
    protected BookieNode selectRandomFromRack(String netPath, Set<Node> excludeBookies, Predicate<BookieNode> predicate,
            Ensemble<BookieNode> ensemble) throws BKNotEnoughBookiesException {
        WeightedRandomSelection<BookieNode> wRSelection = null;
        List<Node> leaves = new ArrayList<Node>(topology.getLeaves(netPath));
        if (!this.isWeighted) {
            Collections.shuffle(leaves);
        } else {
            if (CollectionUtils.subtract(leaves, excludeBookies).size() < 1) {
                throw new BKNotEnoughBookiesException();
            }
            wRSelection = prepareForWeightedSelection(leaves);
            if (wRSelection == null) {
                throw new BKNotEnoughBookiesException();
            }
        }

        Iterator<Node> it = leaves.iterator();
        Set<Node> bookiesSeenSoFar = new HashSet<Node>();
        while (true) {
            Node n;
            if (isWeighted) {
                if (bookiesSeenSoFar.size() == leaves.size()) {
                    // Don't loop infinitely.
                    break;
                }
                n = wRSelection.getNextRandom();
                bookiesSeenSoFar.add(n);
            } else {
                if (it.hasNext()) {
                    n = it.next();
                } else {
                    break;
                }
            }
            if (excludeBookies.contains(n)) {
                continue;
            }
            if (!(n instanceof BookieNode) || !predicate.apply((BookieNode) n, ensemble)) {
                continue;
            }
            BookieNode bn = (BookieNode) n;
            // got a good candidate
            if (ensemble.addNode(bn)) {
                // add the candidate to exclude set
                excludeBookies.add(bn);
            }
            return bn;
        }
        throw new BKNotEnoughBookiesException();
    }

    /**
     * Choose a random node from whole cluster.
     *
     * @param numBookies
     *          number bookies to choose
     * @param excludeBookies
     *          bookies set to exclude.
     * @param ensemble
     *          ensemble to hold the bookie chosen.
     * @return the bookie node chosen.
     * @throws BKNotEnoughBookiesException
     */
    protected List<BookieNode> selectRandom(int numBookies,
                                            Set<Node> excludeBookies,
                                            Predicate<BookieNode> predicate,
                                            Ensemble<BookieNode> ensemble)
            throws BKNotEnoughBookiesException {
        return selectRandomInternal(null,  numBookies, excludeBookies, predicate, ensemble);
    }

    protected List<BookieNode> selectRandomInternal(List<BookieNode> bookiesToSelectFrom,
                                                    int numBookies,
                                                    Set<Node> excludeBookies,
                                                    Predicate<BookieNode> predicate,
                                                    Ensemble<BookieNode> ensemble)
        throws BKNotEnoughBookiesException {
        WeightedRandomSelection<BookieNode> wRSelection = null;
        if (bookiesToSelectFrom == null) {
            // If the list is null, we need to select from the entire knownBookies set
            wRSelection = this.weightedSelection;
            bookiesToSelectFrom = new ArrayList<BookieNode>(knownBookies.values());
        }
        if (isWeighted) {
            if (CollectionUtils.subtract(bookiesToSelectFrom, excludeBookies).size() < numBookies) {
                throw new BKNotEnoughBookiesException();
            }
            if (wRSelection == null) {
                Map<BookieNode, WeightedObject> rackMap = new HashMap<BookieNode, WeightedObject>();
                for (BookieNode n : bookiesToSelectFrom) {
                    if (excludeBookies.contains(n)) {
                        continue;
                    }
                    if (this.bookieInfoMap.containsKey(n)) {
                        rackMap.put(n, this.bookieInfoMap.get(n));
                    } else {
                        rackMap.put(n, new BookieInfo());
                    }
                }
                wRSelection = new WeightedRandomSelectionImpl<BookieNode>(this.maxWeightMultiple);
                wRSelection.updateMap(rackMap);
            }
        } else {
            Collections.shuffle(bookiesToSelectFrom);
        }

        BookieNode bookie;
        List<BookieNode> newBookies = new ArrayList<BookieNode>(numBookies);
        Iterator<BookieNode> it = bookiesToSelectFrom.iterator();
        Set<BookieNode> bookiesSeenSoFar = new HashSet<BookieNode>();
        while (numBookies > 0) {
            if (isWeighted) {
                if (bookiesSeenSoFar.size() == bookiesToSelectFrom.size()) {
                    // If we have gone through the whole available list of bookies,
                    // and yet haven't been able to satisfy the ensemble request, bail out.
                    // We don't want to loop infinitely.
                    break;
                }
                bookie = wRSelection.getNextRandom();
                bookiesSeenSoFar.add(bookie);
            } else {
                if (it.hasNext()) {
                    bookie = it.next();
                } else {
                    break;
                }
            }
            if (excludeBookies.contains(bookie)) {
                continue;
            }

            // When durability is being enforced; we must not violate the
            // predicate even when selecting a random bookie; as durability
            // guarantee is not best effort; correctness is implied by it
            if (enforceDurability && !predicate.apply(bookie, ensemble)) {
                continue;
            }

            if (ensemble.addNode(bookie)) {
                excludeBookies.add(bookie);
                newBookies.add(bookie);
                --numBookies;
            }
        }
        if (numBookies == 0) {
            return newBookies;
        }
        LOG.warn("Failed to find {} bookies : excludeBookies {}, allBookies {}.",
            numBookies, excludeBookies, bookiesToSelectFrom);

        throw new BKNotEnoughBookiesException();
    }

    @Override
    public void registerSlowBookie(BookieSocketAddress bookieSocketAddress, long entryId) {
        if (reorderThresholdPendingRequests <= 0) {
            // only put bookies on slowBookies list if reorderThresholdPendingRequests is *not* set (0);
            // otherwise, rely on reordering of reads based on reorderThresholdPendingRequests
            slowBookies.put(bookieSocketAddress, entryId);
        }
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        Map<Integer, String> writeSetWithRegion = new HashMap<>();
        for (int i = 0; i < writeSet.size(); i++) {
            writeSetWithRegion.put(writeSet.get(i), "");
        }
        return reorderReadSequenceWithRegion(
            ensemble, writeSet, writeSetWithRegion, bookiesHealthInfo, false, "", writeSet.size());
    }

    /**
     * This function orders the read sequence with a given region. For region-unaware policies (e.g.
     * RackAware), we pass in false for regionAware and an empty myRegion. When this happens, any
     * remote list will stay empty. The ordering is as follows (the R* at the beginning of each list item
     * is only present for region aware policies).
     *      1. available (local) bookies
     *      2. R* a remote bookie (based on remoteNodeInReorderSequence
     *      3. R* remaining (local) bookies
     *      4. R* remaining remote bookies
     *      5. read only bookies
     *      6. slow bookies
     *      7. unavailable bookies
     *
     * @param ensemble
     *          ensemble of bookies
     * @param writeSet
     *          write set
     * @param writeSetWithRegion
     *          write set with region information
     * @param bookiesHealthInfo
     *          heuristics about health of boookies
     * @param regionAware
     *          whether or not a region-aware policy is used
     * @param myRegion
     *          current region of policy
     * @param remoteNodeInReorderSequence
     *          number of local bookies to try before trying a remote bookie
     * @return ordering of bookies to send read to
     */
    DistributionSchedule.WriteSet reorderReadSequenceWithRegion(
        List<BookieSocketAddress> ensemble,
        DistributionSchedule.WriteSet writeSet,
        Map<Integer, String> writeSetWithRegion,
        BookiesHealthInfo bookiesHealthInfo,
        boolean regionAware,
        String myRegion,
        int remoteNodeInReorderSequence) {
        boolean useRegionAware = regionAware && (!myRegion.equals(UNKNOWN_REGION));
        int ensembleSize = ensemble.size();

        // For rack aware, If all the bookies in the write set are available, simply return the original write set,
        // to avoid creating more lists
        boolean isAnyBookieUnavailable = false;

        if (useRegionAware || reorderReadsRandom) {
            isAnyBookieUnavailable = true;
        } else {
            for (int i = 0; i < ensemble.size(); i++) {
                BookieSocketAddress bookieAddr = ensemble.get(i);
                if ((!knownBookies.containsKey(bookieAddr) && !readOnlyBookies.contains(bookieAddr))
                    || slowBookies.getIfPresent(bookieAddr) != null) {
                    // Found at least one bookie not available in the ensemble, or in slowBookies
                    isAnyBookieUnavailable = true;
                    break;
                }
            }
        }

        boolean reordered = false;
        if (reorderThresholdPendingRequests > 0) {
            // if there are no slow or unavailable bookies, capture each bookie's number of
            // pending request to reorder requests based on a threshold of pending requests

            // number of pending requests per bookie (same index as writeSet)
            long[] pendingReqs = new long[writeSet.size()];
            int bestBookieIdx = -1;

            for (int i = 0; i < writeSet.size(); i++) {
                pendingReqs[i] = bookiesHealthInfo.getBookiePendingRequests(ensemble.get(writeSet.get(i)));
                if (bestBookieIdx < 0 || pendingReqs[i] < pendingReqs[bestBookieIdx]) {
                    bestBookieIdx = i;
                }
            }

            // reorder the writeSet if the currently first bookie in our writeSet has at
            // least
            // reorderThresholdPendingRequests more outstanding request than the best bookie
            if (bestBookieIdx > 0 && pendingReqs[0] >= pendingReqs[bestBookieIdx] + reorderThresholdPendingRequests) {
                // We're not reordering the entire write set, but only move the best bookie
                // to the first place. Chances are good that this bookie will be fast enough
                // to not trigger the speculativeReadTimeout. But even if it hits that timeout,
                // things may have changed by then so much that whichever bookie we put second
                // may actually not be the second-best choice any more.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("read set reordered from {} ({} pending) to {} ({} pending)",
                            ensemble.get(writeSet.get(0)), pendingReqs[0], ensemble.get(writeSet.get(bestBookieIdx)),
                            pendingReqs[bestBookieIdx]);
                }
                writeSet.moveAndShift(bestBookieIdx, 0);
                reordered = true;
            }
        }

        if (!isAnyBookieUnavailable) {
            if (reordered) {
                readReorderedCounter.registerSuccessfulValue(1);
            }
            return writeSet;
        }

        for (int i = 0; i < writeSet.size(); i++) {
            int idx = writeSet.get(i);
            BookieSocketAddress address = ensemble.get(idx);
            String region = writeSetWithRegion.get(idx);
            Long lastFailedEntryOnBookie = bookiesHealthInfo.getBookieFailureHistory(address);
            if (null == knownBookies.get(address)) {
                // there isn't too much differences between readonly bookies
                // from unavailable bookies. since there
                // is no write requests to them, so we shouldn't try reading
                // from readonly bookie prior to writable bookies.
                if ((null == readOnlyBookies)
                    || !readOnlyBookies.contains(address)) {
                    writeSet.set(i, idx | UNAVAIL_MASK);
                } else {
                    if (slowBookies.getIfPresent(address) != null) {
                        long numPendingReqs = bookiesHealthInfo.getBookiePendingRequests(address);
                        // use slow bookies with less pending requests first
                        long slowIdx = numPendingReqs * ensembleSize + idx;
                        writeSet.set(i, (int) (slowIdx & ~MASK_BITS) | SLOW_MASK);
                    } else {
                        writeSet.set(i, idx | READ_ONLY_MASK);
                    }
                }
            } else if (lastFailedEntryOnBookie < 0) {
                if (slowBookies.getIfPresent(address) != null) {
                    long numPendingReqs = bookiesHealthInfo.getBookiePendingRequests(address);
                    long slowIdx = numPendingReqs * ensembleSize + idx;
                    writeSet.set(i, (int) (slowIdx & ~MASK_BITS) | SLOW_MASK);
                } else {
                    if (useRegionAware && !myRegion.equals(region)) {
                        writeSet.set(i, idx | REMOTE_MASK);
                    } else {
                        writeSet.set(i, idx | LOCAL_MASK);
                    }
                }
            } else {
                // use bookies with earlier failed entryIds first
                long failIdx = lastFailedEntryOnBookie * ensembleSize + idx;
                if (useRegionAware && !myRegion.equals(region)) {
                    writeSet.set(i, (int) (failIdx & ~MASK_BITS) | REMOTE_FAIL_MASK);
                } else {
                    writeSet.set(i, (int) (failIdx & ~MASK_BITS) | LOCAL_FAIL_MASK);
                }
            }
        }

        // Add a mask to ensure the sort is stable, sort,
        // and then remove mask. This maintains stability as
        // long as there are fewer than 16 bookies in the write set.
        for (int i = 0; i < writeSet.size(); i++) {
            writeSet.set(i, writeSet.get(i) | ((i & 0xF) << 20));
        }
        writeSet.sort();
        for (int i = 0; i < writeSet.size(); i++) {
            writeSet.set(i, writeSet.get(i) & ~((0xF) << 20));
        }

        if (reorderReadsRandom) {
            shuffleWithMask(writeSet, LOCAL_MASK, MASK_BITS);
            shuffleWithMask(writeSet, REMOTE_MASK, MASK_BITS);
            shuffleWithMask(writeSet, READ_ONLY_MASK, MASK_BITS);
            shuffleWithMask(writeSet, UNAVAIL_MASK, MASK_BITS);
        }

        // nodes within a region are ordered as follows
        // (Random?) list of nodes that have no history of failure
        // Nodes with Failure history are ordered in the reverse
        // order of the most recent entry that generated an error
        // The sort will have put them in correct order,
        // so remove the bits that sort by age.
        for (int i = 0; i < writeSet.size(); i++) {
            int mask = writeSet.get(i) & MASK_BITS;
            int idx = (writeSet.get(i) & ~MASK_BITS) % ensembleSize;
            if (mask == LOCAL_FAIL_MASK) {
                writeSet.set(i, LOCAL_MASK | idx);
            } else if (mask == REMOTE_FAIL_MASK) {
                writeSet.set(i, REMOTE_MASK | idx);
            } else if (mask == SLOW_MASK) {
                writeSet.set(i, SLOW_MASK | idx);
            }
        }

        // Insert a node from the remote region at the specified location so
        // we try more than one region within the max allowed latency
        int firstRemote = -1;
        for (int i = 0; i < writeSet.size(); i++) {
            if ((writeSet.get(i) & MASK_BITS) == REMOTE_MASK) {
                firstRemote = i;
                break;
            }
        }
        if (firstRemote != -1) {
            int i = 0;
            for (; i < remoteNodeInReorderSequence
                && i < writeSet.size(); i++) {
                if ((writeSet.get(i) & MASK_BITS) != LOCAL_MASK) {
                    break;
                }
            }
            writeSet.moveAndShift(firstRemote, i);
        }


        // remove all masks
        for (int i = 0; i < writeSet.size(); i++) {
            writeSet.set(i, writeSet.get(i) & ~MASK_BITS);
        }
        readReorderedCounter.registerSuccessfulValue(1);
        return writeSet;
    }

    // this method should be called in readlock scope of 'rwlock'
    @Override
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieSocketAddress> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        int ensembleSize = ensembleList.size();
        int minNumRacksPerWriteQuorumForThisEnsemble = Math.min(writeQuorumSize, minNumRacksPerWriteQuorum);
        HashSet<String> racksInQuorum = new HashSet<String>();
        BookieSocketAddress bookie;
        for (int i = 0; i < ensembleList.size(); i++) {
            racksInQuorum.clear();
            for (int j = 0; j < writeQuorumSize; j++) {
                bookie = ensembleList.get((i + j) % ensembleSize);
                try {
                    racksInQuorum.add(knownBookies.get(bookie).getNetworkLocation());
                } catch (Exception e) {
                    /*
                     * any issue/exception in analyzing whether ensemble is
                     * strictly adhering to placement policy should be
                     * swallowed.
                     */
                    LOG.warn("Received exception while trying to get network location of bookie: {}", bookie, e);
                }
            }
            if ((racksInQuorum.size() < minNumRacksPerWriteQuorumForThisEnsemble)
                    || (enforceMinNumRacksPerWriteQuorum && racksInQuorum.contains(getDefaultRack()))) {
                return PlacementPolicyAdherence.FAIL;
            }
        }
        return PlacementPolicyAdherence.MEETS_STRICT;
    }

    @Override
    public boolean areAckedBookiesAdheringToPlacementPolicy(Set<BookieSocketAddress> ackedBookies,
                                                            int writeQuorumSize,
                                                            int ackQuorumSize) {
        HashSet<String> rackCounter = new HashSet<>();
        int minWriteQuorumNumRacksPerWriteQuorum = Math.min(writeQuorumSize, minNumRacksPerWriteQuorum);

        ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
        readLock.lock();
        try {
            for (BookieSocketAddress bookie : ackedBookies) {
                rackCounter.add(knownBookies.get(bookie).getNetworkLocation());
            }

            // Check to make sure that ensemble is writing to `minNumberOfRacks`'s number of racks at least.
            if (LOG.isDebugEnabled()) {
                LOG.debug("areAckedBookiesAdheringToPlacementPolicy returning {} because number of racks = {} and "
                          + "minNumRacksPerWriteQuorum = {}",
                          rackCounter.size() >= minNumRacksPerWriteQuorum,
                          rackCounter.size(),
                          minNumRacksPerWriteQuorum);
            }
        } finally {
            readLock.unlock();
        }
        return rackCounter.size() >= minWriteQuorumNumRacksPerWriteQuorum;
    }
}
