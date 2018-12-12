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
import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.READ_REQUESTS_REORDERED;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.UNKNOWN_REGION;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.Configurable;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetUtils;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.NetworkTopologyImpl;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.net.StabilizeNetworkTopology;
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
    boolean isWeighted;
    int maxWeightMultiple;
    private Map<BookieNode, WeightedObject> bookieInfoMap = new HashMap<BookieNode, WeightedObject>();
    private WeightedRandomSelection<BookieNode> weightedSelection;

    protected int minNumRacksPerWriteQuorum;
    protected boolean enforceMinNumRacksPerWriteQuorum;

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

    static class DefaultResolver implements DNSToSwitchMapping {

        final Supplier<String> defaultRackSupplier;

        // for backwards compat
        public DefaultResolver() {
            this(() -> NetworkTopology.DEFAULT_REGION_AND_RACK);
        }

        public DefaultResolver(Supplier<String> defaultRackSupplier) {
            checkNotNull(defaultRackSupplier, "defaultRackSupplier should not be null");
            this.defaultRackSupplier = defaultRackSupplier;
        }

        @Override
        public List<String> resolve(List<String> names) {
            List<String> rNames = new ArrayList<String>(names.size());
            for (@SuppressWarnings("unused") String name : names) {
                final String defaultRack = defaultRackSupplier.get();
                checkNotNull(defaultRack, "defaultRack cannot be null");
                rNames.add(defaultRack);
            }
            return rNames;
        }

        @Override
        public void reloadCachedMappings() {
            // nop
        }

    }

    /**
     * Decorator for any existing dsn resolver.
     * Backfills returned data with appropriate default rack info.
     */
    static class DNSResolverDecorator implements DNSToSwitchMapping {

        final Supplier<String> defaultRackSupplier;
        final DNSToSwitchMapping resolver;

        DNSResolverDecorator(DNSToSwitchMapping resolver, Supplier<String> defaultRackSupplier) {
            checkNotNull(resolver, "Resolver cannot be null");
            checkNotNull(defaultRackSupplier, "defaultRackSupplier should not be null");
            this.defaultRackSupplier = defaultRackSupplier;
            this.resolver = resolver;
        }

        public List<String> resolve(List<String> names) {
            if (names == null) {
                return Collections.emptyList();
            }
            final String defaultRack = defaultRackSupplier.get();
            checkNotNull(defaultRack, "Default rack cannot be null");

            List<String> rNames = resolver.resolve(names);
            if (rNames != null && rNames.size() == names.size()) {
                for (int i = 0; i < rNames.size(); ++i) {
                    if (rNames.get(i) == null) {
                        LOG.warn("Failed to resolve network location for {}, using default rack for it : {}.",
                                names.get(i), defaultRack);
                        rNames.set(i, defaultRack);
                    }
                }
                return rNames;
            }

            LOG.warn("Failed to resolve network location for {}, using default rack for them : {}.", names,
                    defaultRack);
            rNames = new ArrayList<>(names.size());

            for (int i = 0; i < names.size(); ++i) {
                rNames.add(defaultRack);
            }
            return rNames;
        }

        @Override
        public boolean useHostName() {
            return resolver.useHostName();
        }

        @Override
        public void reloadCachedMappings() {
            resolver.reloadCachedMappings();
        }
    }

    // for now, we just maintain the writable bookies' topology
    protected NetworkTopology topology;
    protected DNSToSwitchMapping dnsResolver;
    protected HashedWheelTimer timer;
    protected final Map<BookieSocketAddress, BookieNode> knownBookies;
    // Use a loading cache so slow bookies are expired. Use entryId as values.
    protected Cache<BookieSocketAddress, Long> slowBookies;
    protected BookieNode localNode;
    protected final ReentrantReadWriteLock rwLock;
    // Initialize to empty set
    protected ImmutableSet<BookieSocketAddress> readOnlyBookies = ImmutableSet.of();
    protected boolean reorderReadsRandom = false;
    protected boolean enforceDurability = false;
    protected int stabilizePeriodSeconds = 0;
    protected int reorderThresholdPendingRequests = 0;
    // looks like these only assigned in the same thread as constructor, immediately after constructor;
    // no need to make volatile
    protected StatsLogger statsLogger = null;
    @StatsDoc(
        name = BOOKIES_JOINED,
        help = "The distribution of number of bookies joined the cluster on each network topology change"
    )
    protected OpStatsLogger bookiesJoinedCounter = null;
    @StatsDoc(
        name = BOOKIES_LEFT,
        help = "The distribution of number of bookies left the cluster on each network topology change"
    )
    protected OpStatsLogger bookiesLeftCounter = null;
    @StatsDoc(
        name = READ_REQUESTS_REORDERED,
        help = "The distribution of number of bookies reordered on each read request"
    )
    protected OpStatsLogger readReorderedCounter = null;

    private String defaultRack = NetworkTopology.DEFAULT_RACK;

    RackawareEnsemblePlacementPolicyImpl() {
        this(false);
    }

    RackawareEnsemblePlacementPolicyImpl(boolean enforceDurability) {
        this.enforceDurability = enforceDurability;
        topology = new NetworkTopologyImpl();
        knownBookies = new HashMap<BookieSocketAddress, BookieNode>();

        rwLock = new ReentrantReadWriteLock();
    }

    protected BookieNode createBookieNode(BookieSocketAddress addr) {
        return new BookieNode(addr, resolveNetworkLocation(addr));
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
                                                              StatsLogger statsLogger) {
        checkNotNull(statsLogger, "statsLogger should not be null, use NullStatsLogger instead.");
        this.statsLogger = statsLogger;
        this.bookiesJoinedCounter = statsLogger.getOpStatsLogger(BOOKIES_JOINED);
        this.bookiesLeftCounter = statsLogger.getOpStatsLogger(BOOKIES_LEFT);
        this.readReorderedCounter = statsLogger.getOpStatsLogger(READ_REQUESTS_REORDERED);
        this.reorderReadsRandom = reorderReadsRandom;
        this.stabilizePeriodSeconds = stabilizePeriodSeconds;
        this.reorderThresholdPendingRequests = reorderThresholdPendingRequests;
        this.dnsResolver = new DNSResolverDecorator(dnsResolver, () -> this.getDefaultRack());
        this.timer = timer;
        this.minNumRacksPerWriteQuorum = minNumRacksPerWriteQuorum;
        this.enforceMinNumRacksPerWriteQuorum = enforceMinNumRacksPerWriteQuorum;

        // create the network topology
        if (stabilizePeriodSeconds > 0) {
            this.topology = new StabilizeNetworkTopology(timer, stabilizePeriodSeconds);
        } else {
            this.topology = new NetworkTopologyImpl();
        }

        BookieNode bn;
        try {
            bn = createBookieNode(new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0));
        } catch (UnknownHostException e) {
            LOG.error("Failed to get local host address : ", e);
            bn = null;
        }
        localNode = bn;
        LOG.info("Initialize rackaware ensemble placement policy @ {} @ {} : {}.",
                localNode, null == localNode ? "Unknown" : localNode.getNetworkLocation(),
                dnsResolver.getClass().getName());

        this.isWeighted = isWeighted;
        if (this.isWeighted) {
            this.maxWeightMultiple = maxWeightMultiple;
            this.weightedSelection = new WeightedRandomSelection<BookieNode>(this.maxWeightMultiple);
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
                LOG.info("Failed to initialize DNS Resolver {}, used default subnet resolver : {}",
                    dnsResolverName, re, re.getMessage());
                dnsResolver = new DefaultResolver(() -> this.getDefaultRack());
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
                statsLogger);
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    protected String resolveNetworkLocation(BookieSocketAddress addr) {
        return NetUtils.resolveNetworkLocation(dnsResolver, addr.getSocketAddress());
    }

    public void onBookieRackChange(List<BookieSocketAddress> bookieAddressList) {
        rwLock.writeLock().lock();
        try {
            for (BookieSocketAddress bookieAddress : bookieAddressList) {
                BookieNode node = knownBookies.get(bookieAddress);
                if (node != null) {
                    // refresh the rack info if its a known bookie
                    topology.remove(node);
                    topology.add(createBookieNode(bookieAddress));
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
            Set<BookieSocketAddress> readOnlyBookies) {
        rwLock.writeLock().lock();
        try {
            ImmutableSet<BookieSocketAddress> joinedBookies, leftBookies, deadBookies;
            Set<BookieSocketAddress> oldBookieSet = knownBookies.keySet();
            // left bookies : bookies in known bookies, but not in new writable bookie cluster.
            leftBookies = Sets.difference(oldBookieSet, writableBookies).immutableCopy();
            // joined bookies : bookies in new writable bookie cluster, but not in known bookies
            joinedBookies = Sets.difference(writableBookies, oldBookieSet).immutableCopy();
            // dead bookies.
            deadBookies = Sets.difference(leftBookies, readOnlyBookies).immutableCopy();
            LOG.debug("Cluster changed : left bookies are {}, joined bookies are {}, while dead bookies are {}.",
                    leftBookies, joinedBookies, deadBookies);
            handleBookiesThatLeft(leftBookies);
            handleBookiesThatJoined(joinedBookies);
            if (this.isWeighted && (leftBookies.size() > 0 || joinedBookies.size() > 0)) {
                this.weightedSelection.updateMap(this.bookieInfoMap);
            }
            if (!readOnlyBookies.isEmpty()) {
                this.readOnlyBookies = ImmutableSet.copyOf(readOnlyBookies);
            }

            return deadBookies;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void handleBookiesThatLeft(Set<BookieSocketAddress> leftBookies) {
        for (BookieSocketAddress addr : leftBookies) {
            try {
                BookieNode node = knownBookies.remove(addr);
                if (null != node) {
                    topology.remove(node);
                    if (this.isWeighted) {
                        this.bookieInfoMap.remove(node);
                    }

                    bookiesLeftCounter.registerSuccessfulValue(1L);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cluster changed : bookie {} left from cluster.", addr);
                    }
                }
            } catch (Throwable t) {
                LOG.error("Unexpected exception while handling leaving bookie {}", addr, t);
                if (bookiesLeftCounter != null) {
                    bookiesLeftCounter.registerFailedValue(1L);
                }
                // no need to re-throw; we want to process the rest of the bookies
                // exception anyways will be caught/logged/suppressed in the ZK's event handler
            }
        }
    }

    @Override
    public void handleBookiesThatJoined(Set<BookieSocketAddress> joinedBookies) {
        // node joined
        for (BookieSocketAddress addr : joinedBookies) {
            try {
                BookieNode node = createBookieNode(addr);
                topology.add(node);
                knownBookies.put(addr, node);
                if (this.isWeighted) {
                    this.bookieInfoMap.putIfAbsent(node, new BookieInfo());
                }

                bookiesJoinedCounter.registerSuccessfulValue(1L);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cluster changed : bookie {} joined the cluster.", addr);
                }
            } catch (Throwable t) {
                // topology.add() throws unchecked exception
                LOG.error("Unexpected exception while handling joining bookie {}", addr, t);

                bookiesJoinedCounter.registerFailedValue(1L);
                // no need to re-throw; we want to process the rest of the bookies
                // exception anyways will be caught/logged/suppressed in the ZK's event handler
            }
        }
    }

    protected Set<Node> convertBookiesToNodes(Set<BookieSocketAddress> excludeBookies) {
        Set<Node> nodes = new HashSet<Node>();
        for (BookieSocketAddress addr : excludeBookies) {
            BookieNode bn = knownBookies.get(addr);
            if (null == bn) {
                bn = createBookieNode(addr);
            }
            nodes.add(bn);
        }
        return nodes;
    }

    private static Set<String> getNetworkLocations(Set<Node> bookieNodes) {
        Set<String> networkLocs = new HashSet<>();
        for (Node bookieNode : bookieNodes) {
            networkLocs.add(bookieNode.getNetworkLocation());
        }
        return networkLocs;
    }

    @Override
    public List<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        return newEnsembleInternal(ensembleSize, writeQuorumSize, excludeBookies, null, null);
    }

    protected List<BookieSocketAddress> newEnsembleInternal(int ensembleSize,
                                                            int writeQuorumSize,
                                                            Set<BookieSocketAddress> excludeBookies,
                                                            Ensemble<BookieNode> parentEnsemble,
                                                            Predicate<BookieNode> parentPredicate)
            throws BKNotEnoughBookiesException {
        return newEnsembleInternal(
                ensembleSize,
                writeQuorumSize,
                writeQuorumSize,
                excludeBookies,
                parentEnsemble,
                parentPredicate);
    }

    @Override
    public List<BookieSocketAddress> newEnsemble(int ensembleSize,
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

    protected List<BookieSocketAddress> newEnsembleInternal(
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
                return addrs;
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
            return bookieList;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, Set<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
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
            return candidate.getAddr();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void updateBookieInfo(Map<BookieSocketAddress, BookieInfo> bookieInfoMap) {
        if (!isWeighted) {
            LOG.info("bookieFreeDiskInfo callback called even without weighted placement policy being used.");
            return;
        }
         List<BookieNode> allBookies = new ArrayList<BookieNode>(knownBookies.values());

         // create a new map to reflect the new mapping
        Map<BookieNode, WeightedObject> map = new HashMap<BookieNode, WeightedObject>();
        for (BookieNode bookie : allBookies) {
            if (bookieInfoMap.containsKey(bookie.getAddr())) {
                map.put(bookie, bookieInfoMap.get(bookie.getAddr()));
            } else {
                map.put(bookie, new BookieInfo());
            }
        }
        rwLock.writeLock().lock();
        try {
            this.bookieInfoMap = map;
            this.weightedSelection.updateMap(this.bookieInfoMap);
        } finally {
            rwLock.writeLock().unlock();
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

        WeightedRandomSelection<BookieNode> wRSelection = new WeightedRandomSelection<BookieNode>(maxWeightMultiple);
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
                wRSelection = new WeightedRandomSelection<BookieNode>(this.maxWeightMultiple);
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

    /**
     * Shuffle all the entries of an array that matches a mask.
     * It assumes all entries with the same mask are contiguous in the array.
     */
    static void shuffleWithMask(DistributionSchedule.WriteSet writeSet,
                                int mask, int bits) {
        int first = -1;
        int last = -1;
        for (int i = 0; i < writeSet.size(); i++) {
            if ((writeSet.get(i) & bits) == mask) {
                if (first == -1) {
                    first = i;
                }
                last = i;
            }
        }
        if (first != -1) {
            for (int i = last + 1; i > first; i--) {
                int swapWith = ThreadLocalRandom.current().nextInt(i);
                writeSet.set(swapWith, writeSet.set(i, writeSet.get(swapWith)));
            }
        }
    }
}
