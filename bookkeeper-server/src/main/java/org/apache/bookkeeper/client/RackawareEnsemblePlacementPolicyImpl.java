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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
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
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Simple rackware ensemble placement policy.
 *
 * Make most of the class and methods as protected, so it could be extended to implement other algorithms.
 */
class RackawareEnsemblePlacementPolicyImpl extends TopologyAwareEnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(RackawareEnsemblePlacementPolicyImpl.class);

    public static final String REPP_DNS_RESOLVER_CLASS = "reppDnsResolverClass";
    public static final String REPP_RANDOM_READ_REORDERING = "ensembleRandomReadReordering";

    static final int RACKNAME_DISTANCE_FROM_LEAVES = 1;

    static class DefaultResolver implements DNSToSwitchMapping {

        @Override
        public List<String> resolve(List<String> names) {
            List<String> rNames = new ArrayList<String>(names.size());
            for (@SuppressWarnings("unused") String name : names) {
                rNames.add(NetworkTopology.DEFAULT_RACK);
            }
            return rNames;
        }

        @Override
        public void reloadCachedMappings() {
            // nop
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
    protected StatsLogger statsLogger = null;

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
                                                              StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        this.reorderReadsRandom = reorderReadsRandom;
        this.stabilizePeriodSeconds = stabilizePeriodSeconds;
        this.dnsResolver = dnsResolver;
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
        return this;
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
                dnsResolver = new DefaultResolver();
            }
        }
        return initialize(
                dnsResolver,
                timer,
                conf.getBoolean(REPP_RANDOM_READ_REORDERING, false),
                conf.getNetworkTopologyStabilizePeriodSeconds(),
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
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Cluster changed : left bookies are {}, joined bookies are {}, while dead bookies are {}.",
                        new Object[] { leftBookies, joinedBookies, deadBookies });
            }
            handleBookiesThatLeft(leftBookies);
            handleBookiesThatJoined(joinedBookies);

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
            BookieNode node = knownBookies.remove(addr);
            if(null != node) {
                topology.remove(node);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cluster changed : bookie {} left from cluster.", addr);
                }
            }
        }
    }

    @Override
    public void handleBookiesThatJoined(Set<BookieSocketAddress> joinedBookies) {
        // node joined
        for (BookieSocketAddress addr : joinedBookies) {
            BookieNode node = createBookieNode(addr);
            topology.add(node);
            knownBookies.put(addr, node);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cluster changed : bookie {} joined the cluster.", addr);
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
                            localNode.getNetworkLocation().equals(NetworkTopology.DEFAULT_RACK)) {
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
        List<Node> leaves = new ArrayList<Node>(topology.getLeaves(netPath));
        Collections.shuffle(leaves);
        for (Node n : leaves) {
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
        return selectRandomInternal(new ArrayList<BookieNode>(knownBookies.values()),  numBookies, excludeBookies, predicate, ensemble);
    }

    protected List<BookieNode> selectRandomInternal(List<BookieNode> bookiesToSelectFrom,
                                                    int numBookies,
                                                    Set<Node> excludeBookies,
                                                    Predicate<BookieNode> predicate,
                                                    Ensemble<BookieNode> ensemble)
        throws BKNotEnoughBookiesException {
        Collections.shuffle(bookiesToSelectFrom);
        List<BookieNode> newBookies = new ArrayList<BookieNode>(numBookies);
        for (BookieNode bookie : bookiesToSelectFrom) {
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

            if (numBookies == 0) {
                return newBookies;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to find {} bookies : excludeBookies {}, allBookies {}.", new Object[] {
                numBookies, excludeBookies, bookiesToSelectFrom });
        }
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
