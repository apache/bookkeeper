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
import java.util.Collections;
import java.util.HashSet;
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
 * Default Ensemble Placement Policy, which picks bookies randomly
 */
public class DefaultEnsemblePlacementPolicy implements EnsemblePlacementPolicy {

    static final Set<BookieSocketAddress> EMPTY_SET = new HashSet<BookieSocketAddress>();

    private Set<BookieSocketAddress> knownBookies = new HashSet<BookieSocketAddress>();

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int quorumSize, int ackQuorumSize,
            Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        ArrayList<BookieSocketAddress> newBookies = new ArrayList<BookieSocketAddress>(ensembleSize);
        if (ensembleSize <= 0) {
            return newBookies;
        }
        List<BookieSocketAddress> allBookies;
        synchronized (this) {
            allBookies = new ArrayList<BookieSocketAddress>(knownBookies);
        }
        Collections.shuffle(allBookies);
        for (BookieSocketAddress bookie : allBookies) {
            if (excludeBookies.contains(bookie)) {
                continue;
            }
            newBookies.add(bookie);
            --ensembleSize;
            if (ensembleSize == 0) {
                return newBookies;
            }
        }
        throw new BKNotEnoughBookiesException();
    }

    @Override
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize, Collection<BookieSocketAddress> currentEnsemble,
                                           BookieSocketAddress bookieToReplace,
                                           Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        excludeBookies.addAll(currentEnsemble);
        ArrayList<BookieSocketAddress> addresses = newEnsemble(1, 1, 1, excludeBookies);
        return addresses.get(0);
    }

    @Override
    public synchronized Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
            Set<BookieSocketAddress> readOnlyBookies) {
        HashSet<BookieSocketAddress> deadBookies;
        deadBookies = new HashSet<BookieSocketAddress>(knownBookies);
        deadBookies.removeAll(writableBookies);
        // readonly bookies should not be treated as dead bookies
        deadBookies.removeAll(readOnlyBookies);
        knownBookies = writableBookies;
        return deadBookies;
    }

    @Override
    public List<Integer> reorderReadSequence(ArrayList<BookieSocketAddress> ensemble, List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory) {
        return writeSet;
    }

    @Override
    public List<Integer> reorderReadLACSequence(ArrayList<BookieSocketAddress> ensemble, List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory) {
        List<Integer> retList = new ArrayList<Integer>(writeSet);
        if (retList.size() < ensemble.size()) {
            for (int i = 0; i < ensemble.size(); i++) {
                if (!retList.contains(i)) {
                    retList.add(i);
                }
            }
        }
        return retList;
    }

    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
                                              Optional<DNSToSwitchMapping> optionalDnsResolver,
                                              HashedWheelTimer timer,
                                              FeatureProvider featureProvider,
                                              StatsLogger statsLogger) {
        return this;
    }

    @Override
    public void uninitalize() {
        // do nothing
    }
}
