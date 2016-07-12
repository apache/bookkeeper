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

import com.google.common.base.Optional;

import java.net.UnknownHostException;
import java.util.*;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.jboss.netty.util.HashedWheelTimer;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Special ensemble placement policy that always return local bookie. Only works with ledgers with ensemble=1.
 */
public class LocalBookieEnsemblePlacementPolicy implements EnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(LocalBookieEnsemblePlacementPolicy.class);

    private BookieSocketAddress bookieAddress;

    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf, Optional<DNSToSwitchMapping> optionalDnsResolver, HashedWheelTimer hashedWheelTimer, FeatureProvider featureProvider, StatsLogger statsLogger) {
        // Configuration will have already the bookie configuration inserted
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.addConfiguration(conf);

        try {
            bookieAddress = Bookie.getBookieAddress(serverConf);
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
    public Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
            Set<BookieSocketAddress> readOnlyBookies) {
        return Collections.emptySet();
    }

    @Override
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize, Collection<BookieSocketAddress> currentEnsemble, BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        throw new BKNotEnoughBookiesException();
    }

    @Override
    public List<Integer> reorderReadSequence(ArrayList<BookieSocketAddress> ensemble, List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory) {
        return null;
    }

    @Override
    public List<Integer> reorderReadLACSequence(ArrayList<BookieSocketAddress> ensemble, List<Integer> writeSet, Map<BookieSocketAddress, Long> bookieFailureHistory) {
        return null;
    }

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Set<BookieSocketAddress> excludeBookies) throws BKNotEnoughBookiesException {
        if (ensembleSize > 1) {
            throw new IllegalArgumentException("Local ensemble policy can only return 1 bookie");
        }

        return Lists.newArrayList(bookieAddress);
    }

}
