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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.util.HashedWheelTimer;

/**
 * Encapsulation of the algorithm that selects a number of bookies from the cluster as an ensemble for storing
 * data, based on the data input as well as the node properties.
 */
public interface EnsemblePlacementPolicy {

    /**
     * Initialize the policy.
     *
     * @param conf client configuration
     * @param optionalDnsResolver dns resolver
     * @param hashedWheelTimer timer
     * @param featureProvider feature provider
     * @param statsLogger stats logger
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
     * @param ensembleSize
     *          Ensemble Size
     * @param writeQuorumSize
     *          Write Quorum Size
     * @param excludeBookies
     *          Bookies that should not be considered as targets.
     * @return list of bookies chosen as targets.
     * @throws BKNotEnoughBookiesException if not enough bookies available.
     */
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                                      Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException;

    /**
     * Choose a new bookie to replace <i>bookieToReplace</i>. If no bookie available in the cluster,
     * {@link BKNotEnoughBookiesException} is thrown.
     *
     * @param bookieToReplace
     *          bookie to replace
     * @param excludeBookies
     *          bookies that should not be considered as candidate.
     * @return the bookie chosen as target.
     * @throws BKNotEnoughBookiesException
     */
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                             Collection<BookieSocketAddress> currentEnsemble, BookieSocketAddress bookieToReplace,
                                             Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException;

    /**
     * Reorder the read sequence of a given write quorum <i>writeSet</i>.
     *
     * @param ensemble
     *          Ensemble to read entries.
     * @param writeSet
     *          Write quorum to read entries.
     * @param bookieFailureHistory
     *          Observed failures on the bookies
     * @return read sequence of bookies
     */
    public List<Integer> reorderReadSequence(ArrayList<BookieSocketAddress> ensemble,
                                             List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory);


    /**
     * Reorder the read last add confirmed sequence of a given write quorum <i>writeSet</i>.
     *
     * @param ensemble
     *          Ensemble to read entries.
     * @param writeSet
     *          Write quorum to read entries.
     * @param bookieFailureHistory
     *          Observed failures on the bookies
     * @return read sequence of bookies
     */
    public List<Integer> reorderReadLACSequence(ArrayList<BookieSocketAddress> ensemble,
                                                List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory);
}
