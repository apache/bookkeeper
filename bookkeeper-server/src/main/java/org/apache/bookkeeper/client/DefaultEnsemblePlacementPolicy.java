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
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
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
    static final Set<BookieSocketAddress> EMPTY_SET = new HashSet<BookieSocketAddress>();

    private boolean isWeighted;
    private int maxWeightMultiple;
    private Set<BookieSocketAddress> knownBookies = new HashSet<BookieSocketAddress>();
    private Map<BookieSocketAddress, WeightedObject> bookieInfoMap;
    private WeightedRandomSelection<BookieSocketAddress> weightedSelection;
    private final ReentrantReadWriteLock rwLock;

    DefaultEnsemblePlacementPolicy() {
        bookieInfoMap = new HashMap<BookieSocketAddress, WeightedObject>();
        rwLock = new ReentrantReadWriteLock();
    }

    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize, int quorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        ArrayList<BookieSocketAddress> newBookies = new ArrayList<BookieSocketAddress>(ensembleSize);
        if (ensembleSize <= 0) {
            return PlacementResult.of(newBookies, PlacementPolicyAdherence.FAIL);
        }
        List<BookieSocketAddress> allBookies;
        rwLock.readLock().lock();
        try {
            allBookies = new ArrayList<BookieSocketAddress>(knownBookies);
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
                    BookieSocketAddress b = weightedSelection.getNextRandom();
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
            for (BookieSocketAddress bookie : allBookies) {
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
    public PlacementResult<BookieSocketAddress> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        excludeBookies.addAll(currentEnsemble);
        List<BookieSocketAddress> addresses = newEnsemble(1, 1, 1, customMetadata, excludeBookies).getResult();

        BookieSocketAddress candidateAddr = addresses.get(0);
        List<BookieSocketAddress> newEnsemble = new ArrayList<BookieSocketAddress>(currentEnsemble);
        newEnsemble.set(currentEnsemble.indexOf(bookieToReplace), candidateAddr);
        return PlacementResult.of(candidateAddr,
                isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
    }

    @Override
    public Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
            Set<BookieSocketAddress> readOnlyBookies) {
        rwLock.writeLock().lock();
        try {
            HashSet<BookieSocketAddress> deadBookies;
            deadBookies = new HashSet<BookieSocketAddress>(knownBookies);
            deadBookies.removeAll(writableBookies);
            // readonly bookies should not be treated as dead bookies
            deadBookies.removeAll(readOnlyBookies);
            if (this.isWeighted) {
                for (BookieSocketAddress b : deadBookies) {
                    this.bookieInfoMap.remove(b);
                }
                @SuppressWarnings("unchecked")
                Collection<BookieSocketAddress> newBookies = CollectionUtils.subtract(writableBookies, knownBookies);
                for (BookieSocketAddress b : newBookies) {
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
    public void registerSlowBookie(BookieSocketAddress bookieSocketAddress, long entryId) {
        return;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        return writeSet;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        writeSet.addMissingIndices(ensemble.size());
        return writeSet;
    }

    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                              Optional<DNSToSwitchMapping> optionalDnsResolver,
                                              HashedWheelTimer timer,
                                              FeatureProvider featureProvider,
                                              StatsLogger statsLogger) {
        this.isWeighted = conf.getDiskWeightBasedPlacementEnabled();
        if (this.isWeighted) {
            this.maxWeightMultiple = conf.getBookieMaxWeightMultipleForWeightBasedPlacement();
            this.weightedSelection = new WeightedRandomSelectionImpl<BookieSocketAddress>(this.maxWeightMultiple);
        }
        return this;
    }

    @Override
    public void updateBookieInfo(Map<BookieSocketAddress, BookieInfo> bookieInfoMap) {
        rwLock.writeLock().lock();
        try {
            for (Map.Entry<BookieSocketAddress, BookieInfo> e : bookieInfoMap.entrySet()) {
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
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieSocketAddress> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        return PlacementPolicyAdherence.MEETS_STRICT;
    }
}
