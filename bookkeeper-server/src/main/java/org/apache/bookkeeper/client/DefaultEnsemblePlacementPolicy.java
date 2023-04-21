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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Ensemble Placement Policy, which picks bookies randomly.
 *
 * @see EnsemblePlacementPolicy
 */
public class DefaultEnsemblePlacementPolicy implements EnsemblePlacementPolicy {
    static final Logger LOG = LoggerFactory.getLogger(DefaultEnsemblePlacementPolicy.class);
    static final Set<BookieId> EMPTY_SET = new HashSet<BookieId>();

    private boolean isWeighted;
    private int maxWeightMultiple;
    private Set<BookieId> knownBookies = new HashSet<BookieId>();
    private Map<BookieId, WeightedObject> bookieInfoMap;
    private WeightedRandomSelection<BookieId> weightedSelection;
    private final ReentrantReadWriteLock rwLock;

    DefaultEnsemblePlacementPolicy() {
        bookieInfoMap = new HashMap<BookieId, WeightedObject>();
        rwLock = new ReentrantReadWriteLock();
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize, int quorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        ArrayList<BookieId> newBookies = new ArrayList<BookieId>(ensembleSize);
        if (ensembleSize <= 0) {
            return PlacementResult.of(newBookies, PlacementPolicyAdherence.FAIL);
        }
        List<BookieId> allBookies;
        rwLock.readLock().lock();
        try {
            allBookies = new ArrayList<BookieId>(knownBookies);
        } finally {
            rwLock.readLock().unlock();
        }

        if (isWeighted) {
            // hold the readlock while selecting bookies. We don't want the list of bookies
            // changing while we are creating the ensemble
            rwLock.readLock().lock();
            try {
                if (CollectionUtils.subtract(allBookies, excludeBookies).size() < ensembleSize) {
                    throw new BKNotEnoughBookiesException();
                }
                while (ensembleSize > 0) {
                    BookieId b = weightedSelection.getNextRandom();
                    if (newBookies.contains(b) || excludeBookies.contains(b)) {
                        continue;
                    }
                    newBookies.add(b);
                    --ensembleSize;
                    if (ensembleSize == 0) {
                        return PlacementResult.of(newBookies,
                                isEnsembleAdheringToPlacementPolicy(newBookies, quorumSize, ackQuorumSize));
                    }
                }
            } finally {
                rwLock.readLock().unlock();
            }
        } else {
            Collections.shuffle(allBookies);
            for (BookieId bookie : allBookies) {
                if (excludeBookies.contains(bookie)) {
                    continue;
                }
                newBookies.add(bookie);
                --ensembleSize;
                if (ensembleSize == 0) {
                    return PlacementResult.of(newBookies,
                            isEnsembleAdheringToPlacementPolicy(newBookies, quorumSize, ackQuorumSize));
                }
            }
        }
        throw new BKNotEnoughBookiesException();
    }

    @Override
    public PlacementResult<BookieId> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble,
            BookieId bookieToReplace, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        excludeBookies.addAll(currentEnsemble);
        List<BookieId> addresses = newEnsemble(1, 1, 1, customMetadata, excludeBookies).getResult();

        BookieId candidateAddr = addresses.get(0);
        List<BookieId> newEnsemble = new ArrayList<BookieId>(currentEnsemble);
        newEnsemble.set(currentEnsemble.indexOf(bookieToReplace), candidateAddr);
        return PlacementResult.of(candidateAddr,
                isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
    }

    @Override
    public Set<BookieId> onClusterChanged(Set<BookieId> writableBookies,
            Set<BookieId> readOnlyBookies) {
        rwLock.writeLock().lock();
        try {
            HashSet<BookieId> deadBookies;
            deadBookies = new HashSet<BookieId>(knownBookies);
            deadBookies.removeAll(writableBookies);
            // readonly bookies should not be treated as dead bookies
            deadBookies.removeAll(readOnlyBookies);
            if (this.isWeighted) {
                for (BookieId b : deadBookies) {
                    this.bookieInfoMap.remove(b);
                }
                @SuppressWarnings("unchecked")
                Collection<BookieId> newBookies = CollectionUtils.subtract(writableBookies, knownBookies);
                for (BookieId b : newBookies) {
                    this.bookieInfoMap.put(b, new BookieInfo());
                }
                if (deadBookies.size() > 0 || newBookies.size() > 0) {
                    this.weightedSelection.updateMap(this.bookieInfoMap);
                }
            }
            knownBookies = writableBookies;
            return deadBookies;
        } finally {
            rwLock.writeLock().unlock();
        }
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
        return writeSet;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(
            List<BookieId> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        writeSet.addMissingIndices(ensemble.size());
        return writeSet;
    }

    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                       Optional<DNSToSwitchMapping> optionalDnsResolver,
                                       HashedWheelTimer hashedWheelTimer,
                                       FeatureProvider featureProvider,
                                       StatsLogger statsLogger,
                                       BookieAddressResolver bookieAddressResolver) {
        this.isWeighted = conf.getDiskWeightBasedPlacementEnabled();
        if (this.isWeighted) {
            this.maxWeightMultiple = conf.getBookieMaxWeightMultipleForWeightBasedPlacement();
            this.weightedSelection = new WeightedRandomSelectionImpl<BookieId>(this.maxWeightMultiple);
        }
        return this;
    }

    @Override
    public void updateBookieInfo(Map<BookieId, BookieInfo> bookieInfoMap) {
        rwLock.writeLock().lock();
        try {
            for (Map.Entry<BookieId, BookieInfo> e : bookieInfoMap.entrySet()) {
                this.bookieInfoMap.put(e.getKey(), e.getValue());
            }
            this.weightedSelection.updateMap(this.bookieInfoMap);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    @Override
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieId> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        return PlacementPolicyAdherence.MEETS_STRICT;
    }
}
