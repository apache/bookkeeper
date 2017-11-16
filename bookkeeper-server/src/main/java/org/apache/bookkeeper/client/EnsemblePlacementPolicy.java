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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * {@link EnsemblePlacementPolicy} encapsulates the algorithm that bookkeeper client uses to select a number of bookies
 * from the cluster as an ensemble for storing entries.
 *
 * <p>The algorithm is typically implemented based on the data input as well as the network topology properties.
 *
 * <h2>How does it work?</h2>
 *
 * This interface basically covers three parts:
 * <p><ul>
 * <li>Initialization and uninitialization</li>
 * <li>How to choose bookies to place data</li>
 * <li>How to choose bookies to do speculative reads</li>
 * </ul></p>
 *
 * <h3>Initialization and uninitialization</h3>
 *
 * <p>The ensemble placement policy is constructed by jvm reflection during constructing bookkeeper client.
 * After the {@code EnsemblePlacementPolicy} is constructed, bookkeeper client will call
 * {@link #initialize(ClientConfiguration, Optional, HashedWheelTimer, FeatureProvider, StatsLogger)} to initialize
 * the placement policy.
 *
 * <p>The {@link #initialize(ClientConfiguration, Optional, HashedWheelTimer, FeatureProvider, StatsLogger)}
 * method takes a few resources from bookkeeper for instantiating itself. These resources include:
 *
 * <p><ul>
 * <li>`ClientConfiguration` : The client configuration that used for constructing the bookkeeper client.
 *                             The implementation of the placement policy could obtain its settings from this
 *                             configuration.
 * <li>`DNSToSwitchMapping`: The DNS resolver for the ensemble policy to build the network topology of the bookies
 *                           cluster. It is optional.
 * <li>`HashedWheelTimer`: A hashed wheel timer that could be used for timing related work.
 *                         For example, a stabilize network topology could use it to delay network topology changes to
 *                         reduce impacts of flapping bookie registrations due to zk session expires.
 * <li>`FeatureProvider`: A {@link FeatureProvider} that the policy could use for enabling or disabling its offered
 *                        features. For example, a {@link RegionAwareEnsemblePlacementPolicy} could offer features
 *                        to disable placing data to a specific region at runtime.
 * <li>`StatsLogger`: A {@link StatsLogger} for exposing stats.
 * </ul></p>
 *
 * <p>The ensemble placement policy is a single instance per bookkeeper client. The instance will
 * be {@link #uninitalize()} when closing the bookkeeper client. The implementation of a placement policy should be
 * responsible for releasing all the resources that allocated during
 * {@link #initialize(ClientConfiguration, Optional, HashedWheelTimer, FeatureProvider, StatsLogger)}.
 *
 * <h3>How to choose bookies to place data</h3>
 *
 * <p>The bookkeeper client discovers list of bookies from zookeeper via {@code BookieWatcher} - whenever there are
 * bookie changes, the ensemble placement policy will be notified with new list of bookies via
 * {@link #onClusterChanged(Set, Set)}. The implementation of the ensemble placement policy will react on those
 * changes to build new network topology. Subsequent operations like {@link #newEnsemble(int, int, int, Map, Set)} or
 * {@link #replaceBookie(int, int, int, Map, Collection, BookieSocketAddress, Set)} hence can operate on the new
 * network topology.
 *
 * <p>Both {@link RackawareEnsemblePlacementPolicy} and {@link RegionAwareEnsemblePlacementPolicy} are
 * {@link TopologyAwareEnsemblePlacementPolicy}s. They build a {@link org.apache.bookkeeper.net.NetworkTopology} on
 * bookie changes, use it for ensemble placement and ensure {@code rack/region} coverage for write quorums.
 *
 * <h4>Network Topology</h4>
 *
 * <p>The network topology is presenting a cluster of bookies in a tree hierarchical structure. For example,
 * a bookie cluster may be consists of many data centers (aka regions) filled with racks of machines.
 * In this tree structure, leaves represent bookies and inner nodes represent switches/routes that manage
 * traffic in/out of regions or racks.
 *
 * <p>For example, there are 3 bookies in region `A`. They are `bk1`, `bk2` and `bk3`. And their network locations are
 * {@code /region-a/rack-1/bk1}, {@code /region-a/rack-1/bk2} and {@code /region-a/rack-2/bk3}. So the network topology
 * will look like below:
 *
 * <pre>
 *      root
 *        |
 *     region-a
 *       /  \
 * rack-1  rack-2
 *   /  \       \
 * bk1  bk2     bk3
 * </pre>
 *
 * <p>Another example, there are 4 bookies spanning in two regions `A` and `B`. They are `bk1`, `bk2`, `bk3` and `bk4`.
 * And their network locations are {@code /region-a/rack-1/bk1}, {@code /region-a/rack-1/bk2},
 * {@code /region-b/rack-2/bk3} and {@code /region-b/rack-2/bk4}. The network topology will look like below:
 *
 * <pre>
 *         root
 *         /  \
 * region-a  region-b
 *     |         |
 *   rack-1    rack-2
 *     / \       / \
 *   bk1  bk2  bk3  bk4
 * </pre>
 *
 * <p> The network location of each bookie is resolved by a {@link DNSToSwitchMapping}. The {@link DNSToSwitchMapping}
 * resolves a list of DNS-names or IP-addresses into a list of network locations. The network location that is returned
 * must be a network path of the form `/region/rack`, where `/` is the root, and `region` is the region id representing
 * the data center where `rack` is located. The network topology of the bookie cluster would determine the number of
 *
 * <h4>RackAware and RegionAware</h4>
 *
 * <p>{@link RackawareEnsemblePlacementPolicy} basically just chooses bookies from different racks in the built
 * network topology. It guarantees that a write quorum will cover at least two racks. It expects the network locations
 * resolved by {@link DNSToSwitchMapping} have at least 2 levels. For example, network location paths like
 * {@code /dc1/rack0} and {@code /dc1/row1/rack0} are okay, but {@link /rack0} is not acceptable.
 *
 * <p>{@link RegionAwareEnsemblePlacementPolicy} is a hierarchical placement policy, which it chooses
 * equal-sized bookies from regions, and within each region it uses {@link RackawareEnsemblePlacementPolicy} to choose
 * bookies from racks. For example, if there is 3 regions - {@code region-a}, {@code region-b} and {@code region-c},
 * an application want to allocate a {@code 15-bookies} ensemble. First, it would figure out there are 3 regions and
 * it should allocate 5 bookies from each region. Second, for each region, it would use
 * {@link RackawareEnsemblePlacementPolicy} to choose <i>5</i> bookies.
 *
 * <p>Since {@link RegionAwareEnsemblePlacementPolicy} is based on {@link RackawareEnsemblePlacementPolicy}, it expects
 * the network locations resolved by {@link DNSToSwitchMapping} have at least <b>3</b> levels.
 *
 * <h3>How to choose bookies to do speculative reads?</h3>
 *
 * <p>{@link #reorderReadSequence(ArrayList, List, Map)} and {@link #reorderReadLACSequence(ArrayList, List, Map)} are
 * two methods exposed by the placement policy, to help client determine a better read sequence according to the
 * network topology and the bookie failure history.
 *
 * <p>For example, in {@link RackawareEnsemblePlacementPolicy}, the reads will be attempted in following sequence:
 *
 * <p><ul>
 * <li>bookies are writable and didn't experience failures before
 * <li>bookies are writable and experienced failures before
 * <li>bookies are readonly
 * <li>bookies already disappeared from network topology
 * </ul></p>
 *
 * <p>In {@link RegionAwareEnsemblePlacementPolicy}, the reads will be tried in similar following sequence
 * as `RackAware` placement policy. There is a slight different on trying writable bookies: after trying every 2
 * bookies from local region, it would try a bookie from remote region. Hence it would achieve low latency even
 * there is network issues within local region.
 *
 * <h2>How to configure the placement policy?</h2>
 *
 * <p>Currently there are 3 implementations available by default. They are:
 * <ul>
 *     <li>{@link DefaultEnsemblePlacementPolicy}</li>
 *     <li>{@link RackawareEnsemblePlacementPolicy}</li>
 *     <li>{@link RegionAwareEnsemblePlacementPolicy}</li>
 * </ul>
 *
 * <p>You can configure the ensemble policy by specifying the placement policy class in
 * {@link ClientConfiguration#setEnsemblePlacementPolicy(Class)}.
 *
 * <p>{@link DefaultEnsemblePlacementPolicy} randomly pickups bookies from the cluster, while both
 * {@link RackawareEnsemblePlacementPolicy} and {@link RegionAwareEnsemblePlacementPolicy} choose bookies based on
 * network locations. So you might also consider configuring a proper {@link DNSToSwitchMapping} in
 * {@link BookKeeper.Builder} to resolve the correct network locations for your cluster.
 *
 * @see TopologyAwareEnsemblePlacementPolicy
 * @see DefaultEnsemblePlacementPolicy
 * @see RackawareEnsemblePlacementPolicy
 * @see RegionAwareEnsemblePlacementPolicy
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface EnsemblePlacementPolicy {

    /**
     * Initialize the policy.
     *
     * @param conf client configuration
     * @param optionalDnsResolver dns resolver
     * @param hashedWheelTimer timer
     * @param featureProvider feature provider
     * @param statsLogger stats logger
     *
     * @since 4.5
     */
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                              Optional<DNSToSwitchMapping> optionalDnsResolver,
                                              HashedWheelTimer hashedWheelTimer,
                                              FeatureProvider featureProvider,
                                              StatsLogger statsLogger);

    /**
     * Uninitialize the policy
     */
    public void uninitalize();

    /**
     * A consistent view of the cluster (what bookies are available as writable, what bookies are available as
     * readonly) is updated when any changes happen in the cluster.
     *
     * <p>The implementation should take actions when the cluster view is changed. So subsequent
     * {@link #newEnsemble(int, int, int, Map, Set)} and
     * {@link #replaceBookie(int, int, int, Map, Collection, BookieSocketAddress, Set)} can choose proper bookies.
     *
     * @param writableBookies
     *          All the bookies in the cluster available for write/read.
     * @param readOnlyBookies
     *          All the bookies in the cluster available for readonly.
     * @return the dead bookies during this cluster change.
     */
    public Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
                                                     Set<BookieSocketAddress> readOnlyBookies);

    /**
     * Choose <i>numBookies</i> bookies for ensemble. If the count is more than the number of available
     * nodes, {@link BKNotEnoughBookiesException} is thrown.
     *
     * <p>The implementation should respect to the replace settings. The size of the returned bookie list
     * should be equal to the provide {@code ensembleSize}.
     *
     * <p>{@code customMetadata} is the same user defined data that user provides
     * when {@link BookKeeper#createLedger(int, int, int, BookKeeper.DigestType, byte[], Map)}.
     *
     * @param ensembleSize
     *          Ensemble Size
     * @param writeQuorumSize
     *          Write Quorum Size
     * @param ackQuorumSize
     *          the value of ackQuorumSize (added since 4.5)
     * @param customMetadata the value of customMetadata. it is the same user defined metadata that user
     *                       provides in {@link BookKeeper#createLedger(int, int, int, BookKeeper.DigestType, byte[])}
     * @param excludeBookies Bookies that should not be considered as targets.
     * @throws BKNotEnoughBookiesException if not enough bookies available.
     * @return the java.util.ArrayList<org.apache.bookkeeper.net.BookieSocketAddress>
     */
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize,
                                                      int writeQuorumSize,
                                                      int ackQuorumSize,
                                                      Map<String, byte[]> customMetadata,
                                                      Set<BookieSocketAddress> excludeBookies)
        throws BKNotEnoughBookiesException;

    /**
     * Choose a new bookie to replace <i>bookieToReplace</i>. If no bookie available in the cluster,
     * {@link BKNotEnoughBookiesException} is thrown.
     *
     * @param ensembleSize
     *          the value of ensembleSize
     * @param writeQuorumSize
     *          the value of writeQuorumSize
     * @param ackQuorumSize the value of ackQuorumSize (added since 4.5)
     * @param customMetadata the value of customMetadata. it is the same user defined metadata that user
     *                       provides in {@link BookKeeper#createLedger(int, int, int, BookKeeper.DigestType, byte[])}
     * @param currentEnsemble the value of currentEnsemble
     * @param bookieToReplace bookie to replace
     * @param excludeBookies bookies that should not be considered as candidate.
     * @throws BKNotEnoughBookiesException
     * @return the org.apache.bookkeeper.net.BookieSocketAddress
     */
    public BookieSocketAddress replaceBookie(int ensembleSize,
                                             int writeQuorumSize,
                                             int ackQuorumSize,
                                             Map<String, byte[]> customMetadata,
                                             Collection<BookieSocketAddress> currentEnsemble,
                                             BookieSocketAddress bookieToReplace,
                                             Set<BookieSocketAddress> excludeBookies)
        throws BKNotEnoughBookiesException;

    /**
     * Reorder the read sequence of a given write quorum <i>writeSet</i>.
     *
     * @param ensemble
     *          Ensemble to read entries.
     * @param bookieFailureHistory
     *          Observed failures on the bookies
     * @param writeSet
     *          Write quorum to read entries. This will be modified, rather than
     *          allocating a new WriteSet.
     * @return The read sequence. This will be the same object as the passed in
     *         writeSet.
     * @since 4.5
     */
    public DistributionSchedule.WriteSet reorderReadSequence(
            ArrayList<BookieSocketAddress> ensemble,
            Map<BookieSocketAddress, Long> bookieFailureHistory,
            DistributionSchedule.WriteSet writeSet);


    /**
     * Reorder the read last add confirmed sequence of a given write quorum <i>writeSet</i>.
     *
     * @param ensemble
     *          Ensemble to read entries.
     * @param bookieFailureHistory
     *          Observed failures on the bookies
     * @param writeSet
     *          Write quorum to read entries. This will be modified, rather than
     *          allocating a new WriteSet.
     * @return The read sequence. This will be the same object as the passed in
     *         writeSet.
     * @since 4.5
     */
    public DistributionSchedule.WriteSet reorderReadLACSequence(
            ArrayList<BookieSocketAddress> ensemble,
            Map<BookieSocketAddress, Long> bookieFailureHistory,
            DistributionSchedule.WriteSet writeSet);

    /**
     * Send the bookie info details.
     * 
     * @param bookieInfoMap
     *          A map that has the bookie to BookieInfo
     * @since 4.5
     */
    default public void updateBookieInfo(Map<BookieSocketAddress, BookieInfo> bookieInfoMap) {
    }
}
