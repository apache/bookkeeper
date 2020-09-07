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
package org.apache.bookkeeper.bookie;

import com.google.common.collect.Lists;
import io.netty.util.HashedWheelTimer;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.BookiesHealthInfo;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Special ensemble placement policy that always return local bookie. Only works with ledgers with ensemble=1.
 *
 * @see EnsemblePlacementPolicy
 */
public class LocalBookieEnsemblePlacementPolicy implements EnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(LocalBookieEnsemblePlacementPolicy.class);

    private BookieId bookieAddress;

    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                              Optional<DNSToSwitchMapping> optionalDnsResolver,
                                              HashedWheelTimer hashedWheelTimer,
                                              FeatureProvider featureProvider,
                                              StatsLogger statsLogger, BookieAddressResolver bookieAddressResolver) {
        // Configuration will have already the bookie configuration inserted
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.addConfiguration(conf);

        try {
            bookieAddress = Bookie.getBookieId(serverConf);
        } catch (UnknownHostException e) {
            LOG.warn("Unable to get bookie address", e);
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    @Override
    public Set<BookieId> onClusterChanged(Set<BookieId> writableBookies,
            Set<BookieId> readOnlyBookies) {
        return Collections.emptySet();
    }

    @Override
    public PlacementResult<BookieId> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            java.util.Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble,
            BookieId bookieToReplace, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        throw new BKNotEnoughBookiesException();
    }

    @Override
    public void registerSlowBookie(BookieId bookieSocketAddress, long entryId) {
        return;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(
            List<BookieId> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        return null;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(
            List<BookieId> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        return null;
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, java.util.Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        if (ensembleSize > 1) {
            throw new IllegalArgumentException("Local ensemble policy can only return 1 bookie");
        }

        return PlacementResult.of(Lists.newArrayList(bookieAddress), PlacementPolicyAdherence.MEETS_STRICT);
    }

    @Override
    public void updateBookieInfo(Map<BookieId, BookieInfo> bookieToFreeSpaceMap) {
        return;
    }

    @Override
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieId> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        return PlacementPolicyAdherence.MEETS_STRICT;
    }
}
