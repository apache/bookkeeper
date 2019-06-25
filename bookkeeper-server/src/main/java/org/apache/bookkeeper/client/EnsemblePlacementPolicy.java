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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.DistributionSchedule.WriteSet;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.common.util.MathUtils;
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
 * <p>This interface basically covers three parts:</p>
 * <ul>
 * <li>Initialization and uninitialization</li>
 * <li>How to choose bookies to place data</li>
 * <li>How to choose bookies to do speculative reads</li>
 * </ul>
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
 * <ul>
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
 * </ul>
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
 * {@link #replaceBookie(int, int, int, java.util.Map, java.util.Set,
 * org.apache.bookkeeper.net.BookieSocketAddress, java.util.Set)}
 * hence can operate on the new
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
 * <p>The network location of each bookie is resolved by a {@link DNSToSwitchMapping}. The {@link DNSToSwitchMapping}
 * resolves a list of DNS-names or IP-addresses into a list of network locations. The network location that is returned
 * must be a network path of the form `/region/rack`, where `/` is the root, and `region` is the region id representing
 * the data center where `rack` is located. The network topology of the bookie cluster would determine the number of
 *
 * <h4>RackAware and RegionAware</h4>
 *
 * <p>{@link RackawareEnsemblePlacementPolicy} basically just chooses bookies from different racks in the built
 * network topology. It guarantees that a write quorum will cover at least two racks. It expects the network locations
 * resolved by {@link DNSToSwitchMapping} have at least 2 levels. For example, network location paths like
 * {@code /dc1/rack0} and {@code /dc1/row1/rack0} are okay, but {@code /rack0} is not acceptable.
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
 * <p>{@link #reorderReadSequence(List, BookiesHealthInfo, WriteSet)} and
 * {@link #reorderReadLACSequence(List, BookiesHealthInfo, WriteSet)} are
 * two methods exposed by the placement policy, to help client determine a better read sequence according to the
 * network topology and the bookie failure history.
 *
 * <p>For example, in {@link RackawareEnsemblePlacementPolicy}, the reads will be attempted in following sequence:
 *
 * <ul>
 * <li>bookies are writable and didn't experience failures before
 * <li>bookies are writable and experienced failures before
 * <li>bookies are readonly
 * <li>bookies already disappeared from network topology
 * </ul>
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
    EnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                       Optional<DNSToSwitchMapping> optionalDnsResolver,
                                       HashedWheelTimer hashedWheelTimer,
                                       FeatureProvider featureProvider,
                                       StatsLogger statsLogger);

    /**
     * Uninitialize the policy.
     */
    void uninitalize();

    /**
     * A consistent view of the cluster (what bookies are available as writable, what bookies are available as
     * readonly) is updated when any changes happen in the cluster.
     *
     * <p>The implementation should take actions when the cluster view is changed. So subsequent
     * {@link #newEnsemble(int, int, int, Map, Set)} and
     * {@link #replaceBookie(int, int, int, java.util.Map, java.util.Set,
     * org.apache.bookkeeper.net.BookieSocketAddress, java.util.Set) }
     * can choose proper bookies.
     *
     * @param writableBookies
     *          All the bookies in the cluster available for write/read.
     * @param readOnlyBookies
     *          All the bookies in the cluster available for readonly.
     * @return the dead bookies during this cluster change.
     */
    Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
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
     * <p>If 'enforceMinNumRacksPerWriteQuorum' config is enabled then the bookies belonging to default
     * faultzone (rack) will be excluded while selecting bookies.
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
     * @return a placement result containing list of bookie addresses for the ensemble.
     */
    PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize,
                                                           int writeQuorumSize,
                                                           int ackQuorumSize,
                                                           Map<String, byte[]> customMetadata,
                                                           Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException;

    /**
     * Choose a new bookie to replace <i>bookieToReplace</i>. If no bookie available in the cluster,
     * {@link BKNotEnoughBookiesException} is thrown.
     *
     * <p>If 'enforceMinNumRacksPerWriteQuorum' config is enabled then the bookies belonging to default
     * faultzone (rack) will be excluded while selecting bookies.
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
     * @return a placement result containing the new bookie address.
     */
    PlacementResult<BookieSocketAddress> replaceBookie(int ensembleSize,
                                                       int writeQuorumSize,
                                                       int ackQuorumSize,
                                                       Map<String, byte[]> customMetadata,
                                                       List<BookieSocketAddress> currentEnsemble,
                                                       BookieSocketAddress bookieToReplace,
                                                       Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException;

    /**
     * Register a bookie as slow so that it is tried after available and read-only bookies.
     *
     * @param bookieSocketAddress
     *          Address of bookie host
     * @param entryId
     *          Entry ID that caused a speculative timeout on the bookie.
     */
    void registerSlowBookie(BookieSocketAddress bookieSocketAddress, long entryId);

    /**
     * Reorder the read sequence of a given write quorum <i>writeSet</i>.
     *
     * @param ensemble
     *          Ensemble to read entries.
     * @param bookiesHealthInfo
     *          Health info for bookies
     * @param writeSet
     *          Write quorum to read entries. This will be modified, rather than
     *          allocating a new WriteSet.
     * @return The read sequence. This will be the same object as the passed in
     *         writeSet.
     * @since 4.5
     */
    DistributionSchedule.WriteSet reorderReadSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet);


    /**
     * Reorder the read last add confirmed sequence of a given write quorum <i>writeSet</i>.
     *
     * @param ensemble
     *          Ensemble to read entries.
     * @param bookiesHealthInfo
     *          Health info for bookies
     * @param writeSet
     *          Write quorum to read entries. This will be modified, rather than
     *          allocating a new WriteSet.
     * @return The read sequence. This will be the same object as the passed in
     *         writeSet.
     * @since 4.5
     */
    DistributionSchedule.WriteSet reorderReadLACSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet);

    /**
     * Send the bookie info details.
     *
     * @param bookieInfoMap
     *          A map that has the bookie to BookieInfo
     * @since 4.5
     */
    default void updateBookieInfo(Map<BookieSocketAddress, BookieInfo> bookieInfoMap) {
    }

    /**
     * Select one bookie to the "sticky" bookie where all reads for a particular
     * ledger will be directed to.
     *
     * <p>The default implementation will pick a bookie randomly from the ensemble.
     * Other placement policies will be able to do better decisions based on
     * additional informations (eg: rack or region awareness).
     *
     * @param metadata
     *            the {@link LedgerMetadata} object
     * @param currentStickyBookieIndex
     *            if we are changing the sticky bookie after a read failure, the
     *            current sticky bookie is passed in so that we will avoid
     *            choosing it again
     * @return the index, within the ensemble of the bookie chosen as the sticky
     *         bookie
     *
     * @since 4.9
     */
    default int getStickyReadBookieIndex(LedgerMetadata metadata, Optional<Integer> currentStickyBookieIndex) {
        if (!currentStickyBookieIndex.isPresent()) {
            // Pick one bookie randomly from the current ensemble as the initial
            // "sticky bookie"
            return ThreadLocalRandom.current().nextInt(metadata.getEnsembleSize());
        } else {
            // When choosing a new sticky bookie index (eg: after the current
            // one has read failures), by default we pick the next one in the
            // ensemble, to avoid picking up the same one again.
            return MathUtils.signSafeMod(currentStickyBookieIndex.get() + 1, metadata.getEnsembleSize());
        }
    }

    /**
     * returns AdherenceLevel if the Ensemble is strictly/softly/fails adhering
     * to placement policy, like in the case of
     * RackawareEnsemblePlacementPolicy, bookies in the writeset are from
     * 'minNumRacksPerWriteQuorum' number of racks. And in the case of
     * RegionawareEnsemblePlacementPolicy, check for
     * minimumRegionsForDurability, reppRegionsToWrite, rack distribution within
     * a region and other parameters of RegionAwareEnsemblePlacementPolicy. In
     * ZoneAwareEnsemblePlacementPolicy if bookies in the writeset are from
     * 'desiredNumOfZones' then it is considered as MEETS_STRICT if they are
     * from 'minNumOfZones' then it is considered as MEETS_SOFT otherwise
     * considered as FAIL.
     *
     * @param ensembleList
     *            list of BookieSocketAddress of bookies in the ensemble
     * @param writeQuorumSize
     *            writeQuorumSize of the ensemble
     * @param ackQuorumSize
     *            ackQuorumSize of the ensemble
     * @return
     */
    default PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieSocketAddress> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        return PlacementPolicyAdherence.FAIL;
    }

    /**
     * Returns true if the bookies that have acknowledged a write adhere to the minimum fault domains as defined in the
     * placement policy in use. Ex: In the case of RackawareEnsemblePlacementPolicy, bookies belong to at least
     * 'minNumRacksPerWriteQuorum' number of racks.
     *
     * @param ackedBookies
     *            list of BookieSocketAddress of bookies that have acknowledged a write.
     * @param writeQuorumSize
     *            writeQuorumSize of the ensemble
     * @param ackQuorumSize
     *            ackQuorumSize of the ensemble
     * @return
     */
    default boolean areAckedBookiesAdheringToPlacementPolicy(Set<BookieSocketAddress> ackedBookies,
                                                             int writeQuorumSize,
                                                             int ackQuorumSize) {
        return true;
    }

    /**
     * enum for PlacementPolicyAdherence. Currently we are supporting tri-value
     * enum for PlacementPolicyAdherence. If placement policy is met strictly
     * then it is MEETS_STRICT, if it doesn't adhere to placement policy then it
     * is FAIL. But there are certain placement policies, like
     * ZoneAwareEnsemblePlacementPolicy which has definition of soft adherence
     * level to support zone down scenarios.
     */
    enum PlacementPolicyAdherence {
        FAIL(1), MEETS_SOFT(3), MEETS_STRICT(5);
        private int numVal;

        private PlacementPolicyAdherence(int numVal) {
            this.numVal = numVal;
        }

        public int getNumVal() {
            return numVal;
        }
    }

    /**
     * Result of a placement calculation against a placement policy.
     */
    final class PlacementResult<T> {
        private final T result;
        private final PlacementPolicyAdherence policyAdherence;

        public static <T> PlacementResult<T> of(T result, PlacementPolicyAdherence policyAdherence) {
            return new PlacementResult<>(result, policyAdherence);
        }

        private PlacementResult(T result, PlacementPolicyAdherence policyAdherence) {
            this.result = result;
            this.policyAdherence = policyAdherence;
        }

        public T getResult() {
            return result;
        }

        public PlacementPolicyAdherence isAdheringToPolicy() {
            return policyAdherence;
        }
    }
}
