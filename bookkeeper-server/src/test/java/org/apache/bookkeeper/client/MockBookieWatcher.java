/**
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.net.BookieSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock implementation of BookieWatcher.
 */
public class MockBookieWatcher implements BookieWatcher {
    static final Logger LOG = LoggerFactory.getLogger(MockBookieWatcher.class);

    final Set<BookieSocketAddress> bookies =
        Collections.newSetFromMap(new ConcurrentHashMap<BookieSocketAddress, Boolean>());

    void addBookies(BookieSocketAddress... bookies) {
        for (BookieSocketAddress b : bookies) {
            this.bookies.add(b);
        }
    }

    void removeBookies(BookieSocketAddress... bookies) {
        for (BookieSocketAddress b : bookies) {
            this.bookies.remove(b);
        }
    }

    @Override
    public Set<BookieSocketAddress> getBookies() throws BKException {
        return Collections.unmodifiableSet(bookies);
    }

    @Override
    public Set<BookieSocketAddress> getReadOnlyBookies() throws BKException {
        return Collections.emptySet();
    }

    @Override
    public List<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize,
                                                 int ackQuorumSize, Map<String, byte[]> customMetadata)
            throws BKNotEnoughBookiesException {
        List<BookieSocketAddress> ensemble = bookies.stream().limit(ensembleSize).collect(Collectors.toList());
        if (ensemble.size() < ensembleSize) {
            LOG.error("Cannot create new ensemble of size {}, there are only {} bookies",
                      ensembleSize, ensemble.size());
            throw new BKNotEnoughBookiesException();
        }
        return ensemble;
    }

    @Override
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                             Map<String, byte[]> customMetadata,
                                             List<BookieSocketAddress> existingBookies, int bookieIdx,
                                             Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        LOG.info("Finding bookie to replace idx {} in {}, excluding {}", bookieIdx, existingBookies, excludeBookies);
        Optional<BookieSocketAddress> bookie = bookies.stream()
            .filter(b -> !(existingBookies.contains(b) || excludeBookies.contains(b)))
            .findAny();
        if (bookie.isPresent()) {
            LOG.info("Found {}", bookie.get());
            return bookie.get();
        } else {
            throw new BKNotEnoughBookiesException();
        }
    }

    @Override
    public void quarantineBookie(BookieSocketAddress bookie) {
        // noop until someone wants it
    }
}
