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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.commons.configuration.Configuration;

/**
 * Encapsulation of the algorithm that selects a number of bookies from the cluster as an ensemble for storing
 * data, based on the data input as well as the node properties.
 */
public interface EnsemblePlacementPolicy {

    /**
     * Initialize the policy.
     *
     * @param conf
     *          client configuration.
     * @return initialized ensemble placement policy
     */
    public EnsemblePlacementPolicy initialize(Configuration conf);

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
    public Set<InetSocketAddress> onClusterChanged(Set<InetSocketAddress> writableBookies,
            Set<InetSocketAddress> readOnlyBookies);

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
    public ArrayList<InetSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize,
            Set<InetSocketAddress> excludeBookies) throws BKNotEnoughBookiesException;

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
    public InetSocketAddress replaceBookie(InetSocketAddress bookieToReplace,
            Set<InetSocketAddress> excludeBookies) throws BKNotEnoughBookiesException;
}
