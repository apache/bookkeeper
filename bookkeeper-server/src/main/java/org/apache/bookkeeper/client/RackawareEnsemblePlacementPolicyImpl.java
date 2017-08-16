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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import org.apache.bookkeeper.bookie.BookKeeperServerStats;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.Configurable;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.*;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.netty.util.HashedWheelTimer;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Simple rackware ensemble placement policy.
 *
 * Make most of the class and methods as protected, so it could be extended to implement other algorithms.
 */
class RackawareEnsemblePlacementPolicyImpl extends TopologyAwareEnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(RackawareEnsemblePlacementPolicyImpl.class);
    boolean isWeighted;
    int maxWeightMultiple;
    private Map<BookieNode, WeightedObject> bookieInfoMap = new HashMap<BookieNode, WeightedObject>();
    private WeightedRandomSelection<BookieNode> weightedSelection;

    public static final String REPP_DNS_RESOLVER_CLASS = "reppDnsResolverClass";
    public static final String REPP_RANDOM_READ_REORDERING = "ensembleRandomReadReordering";

    static final int RACKNAME_DISTANCE_FROM_LEAVES = 1;

    static class DefaultResolver implements DNSToSwitchMapping {

        final Supplier<String> defaultRackSupplier;

        // for backwards compat
        public DefaultResolver() {
            this(() -> NetworkTopology.DEFAULT_REGION_AND_RACK);
        }

        public DefaultResolver(Supplier<String> defaultRackSupplier) {
            Preconditions.checkNotNull(defaultRackSupplier, "defaultRackSupplier should not be null");
            this.defaultRackSupplier = defaultRackSupplier;
        }

        @Override
        public List<String> resolve(List<String> names) {
            List<String> rNames = new ArrayList<String>(names.size());
            for (@SuppressWarnings("unused") String name : names) {
                final String defaultRack = defaultRackSupplier.get();
                Preconditions.checkNotNull(defaultRack, "defaultRack cannot be null");
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
            Preconditions.checkNotNull(resolver, "Resolver cannot be null");
            Preconditions.checkNotNull(defaultRackSupplier, "defaultRackSupplier should not be null");
            this.defaultRackSupplier = defaultRackSupplier;
            this.resolver= resolver;
        }

        public List<String> resolve(List<String> names) {
            if (names == null) {
                return Collections.emptyList();
            }
            final String defaultRack = defaultRackSupplier.get();
            Preconditions.checkNotNull(defaultRack, "Default rack cannot be null");

            List<String> rNames = resolver.resolve(names);
            if (rNames != null && rNames.size() == names.size()) {
                for (int i = 0; i < rNames.size(); ++i) {
                    if (rNames.get(i) == null) {
                        LOG.warn("Failed to resolve network location for {}, using default rack for it : {}.",
                                rNames.get(i), defaultRack);
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
    protected BookieNode localNode;
    protected final ReentrantReadWriteLock rwLock;
    protected ImmutableSet<BookieSocketAddress> readOnlyBookies = null;
    protected boolean reorderReadsRandom = false;
    protected boolean enforceDurability = false;
    protected int stabilizePeriodSeconds = 0;
    // looks like these only assigned in the same thread as constructor, immediately after constructor; 
    // no need to make volatile
    protected StatsLogger statsLogger = null;
    protected OpStatsLogger bookiesJoinedCounter = null;
    protected OpStatsLogger bookiesLeftCounter = null;

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
                                                              boolean isWeighted,
                                                              int maxWeightMultiple,
                                                              StatsLogger statsLogger) {
        Preconditions.checkNotNull(statsLogger, "statsLogger should not be null, use NullStatsLogger instead.");
        this.statsLogger = statsLogger;
        this.bookiesJoinedCounter = statsLogger.getOpStatsLogger(BookKeeperServerStats.BOOKIES_JOINED);
        this.bookiesLeftCounter = statsLogger.getOpStatsLogger(BookKeeperServerStats.BOOKIES_LEFT);
        this.reorderReadsRandom = reorderReadsRandom;
        this.stabilizePeriodSeconds = stabilizePeriodSeconds;
        this.dnsResolver = new DNSResolverDecorator(dnsResolver, () -> this.getDefaultRack());
        this.timer = timer;

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
            new Object[] { localNode, null == localNode ? "Unknown" : localNode.getNetworkLocation(),
                dnsResolver.getClass().getName() });

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
        Preconditions.checkNotNull(rack, "Default rack cannot be null");

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
            } catch (RuntimeException re) {
                LOG.info("Failed to initialize DNS Resolver {}, used default subnet resolver.", dnsResolverName, re);
                dnsResolver = new DefaultResolver(() -> this.getDefaultRack());
            }
        }
        return initialize(
                dnsResolver,
                timer,
                conf.getBoolean(REPP_RANDOM_READ_REORDERING, false),
                conf.getNetworkTopologyStabilizePeriodSeconds(),
                conf.getDiskWeightBasedPlacementEnabled(),
                conf.getBookieMaxWeightMultipleForWeightBasedPlacement(),
                statsLogger);
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    protected String resolveNetworkLocation(BookieSocketAddress addr) {
        return NetUtils.resolveNetworkLocation(dnsResolver, addr.getSocketAddress());
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
                if(null != node) {
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
                if (bookiesLeftCounter != null ) {
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

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize, java.util.Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        return newEnsembleInternal(ensembleSize, writeQuorumSize, excludeBookies, null, null);
    }

    protected ArrayList<BookieSocketAddress> newEnsembleInternal(int ensembleSize,
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
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize,
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

    protected ArrayList<BookieSocketAddress> newEnsembleInternal(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Set<BookieSocketAddress> excludeBookies,
            Ensemble<BookieNode> parentEnsemble,
            Predicate<BookieNode> parentPredicate) throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            RRTopologyAwareCoverageEnsemble ensemble =
                    new RRTopologyAwareCoverageEnsemble(
                            ensembleSize,
                            writeQuorumSize,
                            ackQuorumSize,
                            RACKNAME_DISTANCE_FROM_LEAVES,
                            parentEnsemble,
                            parentPredicate);
            BookieNode prevNode = null;
            int numRacks = topology.getNumOfRacks();
            // only one rack, use the random algorithm.
            if (numRacks < 2) {
                List<BookieNode> bns = selectRandom(ensembleSize, excludeNodes, TruePredicate.instance,
                        ensemble);
                ArrayList<BookieSocketAddress> addrs = new ArrayList<BookieSocketAddress>(ensembleSize);
                for (BookieNode bn : bns) {
                    addrs.add(bn.getAddr());
                }
                return addrs;
            }
            // pick nodes by racks, to ensure there is at least two racks per write quorum.
            for (int i = 0; i < ensembleSize; i++) {
                String curRack;
                if (null == prevNode) {
                    if ((null == localNode) ||
                            defaultRack.equals(localNode.getNetworkLocation())) {
                        curRack = NodeBase.ROOT;
                    } else {
                        curRack = localNode.getNetworkLocation();
                    }
                } else {
                    curRack = "~" + prevNode.getNetworkLocation();
                }
                prevNode = selectFromNetworkLocation(curRack, excludeNodes, ensemble, ensemble);
            }
            ArrayList<BookieSocketAddress> bookieList = ensemble.toList();
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
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize, java.util.Map<String, byte[]> customMetadata, Collection<BookieSocketAddress> currentEnsemble, BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            excludeBookies.addAll(currentEnsemble);
            BookieNode bn = knownBookies.get(bookieToReplace);
            if (null == bn) {
                bn = createBookieNode(bookieToReplace);
            }

            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            // add the bookie to replace in exclude set
            excludeNodes.add(bn);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Try to choose a new bookie to replace {}, excluding {}.", bookieToReplace,
                        excludeNodes);
            }
            // pick a candidate from same rack to replace
            BookieNode candidate = selectFromNetworkLocation(
                    bn.getNetworkLocation(),
                    excludeNodes,
                    TruePredicate.instance,
                    EnsembleForReplacementWithNoConstraints.instance);
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
            Ensemble<BookieNode> ensemble)
            throws BKNotEnoughBookiesException {
        // select one from local rack
        try {
            return selectRandomFromRack(networkLoc, excludeBookies, predicate, ensemble);
        } catch (BKNotEnoughBookiesException e) {
            LOG.warn("Failed to choose a bookie from {} : "
                     + "excluded {}, fallback to choose bookie randomly from the cluster.",
                     networkLoc, excludeBookies);
            // randomly choose one from whole cluster, ignore the provided predicate.
            return selectRandom(1, excludeBookies, predicate, ensemble).get(0);
        }
    }

    protected String getRemoteRack(BookieNode node) {
        return "~" + node.getNetworkLocation();
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

        WeightedRandomSelection<BookieNode> wRSelection = new WeightedRandomSelection<BookieNode>(this.maxWeightMultiple);
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
    public List<Integer> reorderReadSequence(ArrayList<BookieSocketAddress> ensemble, List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory) {
        int ensembleSize = ensemble.size();
        List<Integer> finalList = new ArrayList<Integer>(writeSet.size());
        List<Long> observedFailuresList = new ArrayList<Long>(writeSet.size());
        List<Integer> readOnlyList = new ArrayList<Integer>(writeSet.size());
        List<Integer> unAvailableList = new ArrayList<Integer>(writeSet.size());
        for (Integer idx : writeSet) {
            BookieSocketAddress address = ensemble.get(idx);
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
            } else {
                if ((lastFailedEntryOnBookie == null) || (lastFailedEntryOnBookie < 0)) {
                    finalList.add(idx);
                } else {
                    observedFailuresList.add(lastFailedEntryOnBookie * ensembleSize + idx);
                }
            }
        }

        if (reorderReadsRandom) {
            Collections.shuffle(finalList);
            Collections.shuffle(readOnlyList);
            Collections.shuffle(unAvailableList);
        }

        Collections.sort(observedFailuresList);

        for(long value: observedFailuresList) {
            finalList.add((int)(value % ensembleSize));
        }

        finalList.addAll(readOnlyList);
        finalList.addAll(unAvailableList);
        return finalList;
    }
}
