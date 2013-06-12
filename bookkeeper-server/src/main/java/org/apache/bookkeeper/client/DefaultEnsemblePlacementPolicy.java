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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.commons.configuration.Configuration;

/**
 * Default Ensemble Placement Policy, which picks bookies randomly
 */
public class DefaultEnsemblePlacementPolicy implements EnsemblePlacementPolicy {

    static final Set<InetSocketAddress> EMPTY_SET = new HashSet<InetSocketAddress>();

    private Set<InetSocketAddress> knownBookies = new HashSet<InetSocketAddress>();

    @Override
    public ArrayList<InetSocketAddress> newEnsemble(int ensembleSize, int quorumSize,
            Set<InetSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        ArrayList<InetSocketAddress> newBookies = new ArrayList<InetSocketAddress>(ensembleSize);
        if (ensembleSize <= 0) {
            return newBookies;
        }
        List<InetSocketAddress> allBookies;
        synchronized (this) {
            allBookies = new ArrayList<InetSocketAddress>(knownBookies);
        }
        Collections.shuffle(allBookies);
        for (InetSocketAddress bookie : allBookies) {
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
    public InetSocketAddress replaceBookie(InetSocketAddress bookieToReplace,
            Set<InetSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        ArrayList<InetSocketAddress> addresses = newEnsemble(1, 1, excludeBookies);
        return addresses.get(0);
    }

    @Override
    public synchronized Set<InetSocketAddress> onClusterChanged(Set<InetSocketAddress> writableBookies,
            Set<InetSocketAddress> readOnlyBookies) {
        HashSet<InetSocketAddress> deadBookies;
        deadBookies = new HashSet<InetSocketAddress>(knownBookies);
        deadBookies.removeAll(writableBookies);
        // readonly bookies should not be treated as dead bookies
        deadBookies.removeAll(readOnlyBookies);
        knownBookies = writableBookies;
        return deadBookies;
    }

    @Override
    public EnsemblePlacementPolicy initialize(Configuration conf) {
        return this;
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

}
