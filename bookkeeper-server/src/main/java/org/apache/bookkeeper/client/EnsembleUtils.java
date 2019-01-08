/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EnsembleUtils {
    private static final Logger LOG = LoggerFactory.getLogger(EnsembleUtils.class);

    static List<BookieSocketAddress> replaceBookiesInEnsemble(BookieWatcher bookieWatcher,
                                                              LedgerMetadata metadata,
                                                              List<BookieSocketAddress> oldEnsemble,
                                                              Map<Integer, BookieSocketAddress> failedBookies,
                                                              String logContext)
            throws BKException.BKNotEnoughBookiesException {
        List<BookieSocketAddress> newEnsemble = new ArrayList<>(oldEnsemble);

        int ensembleSize = metadata.getEnsembleSize();
        int writeQ = metadata.getWriteQuorumSize();
        int ackQ = metadata.getAckQuorumSize();
        Map<String, byte[]> customMetadata = metadata.getCustomMetadata();

        Set<BookieSocketAddress> exclude = new HashSet<>(failedBookies.values());

        int replaced = 0;
        for (Map.Entry<Integer, BookieSocketAddress> entry : failedBookies.entrySet()) {
            int idx = entry.getKey();
            BookieSocketAddress addr = entry.getValue();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} replacing bookie: {} index: {}", logContext, addr, idx);
            }

            if (!newEnsemble.get(idx).equals(addr)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} Not changing failed bookie {} at index {}, already changed to {}",
                              logContext, addr, idx, newEnsemble.get(idx));
                }
                continue;
            }
            try {
                BookieSocketAddress newBookie = bookieWatcher.replaceBookie(
                        ensembleSize, writeQ, ackQ, customMetadata, newEnsemble, idx, exclude);
                newEnsemble.set(idx, newBookie);

                replaced++;
            } catch (BKException.BKNotEnoughBookiesException e) {
                // if there is no bookie replaced, we throw not enough bookie exception
                if (replaced <= 0) {
                    throw e;
                } else {
                    break;
                }
            }
        }
        return newEnsemble;
    }

    static Set<Integer> diffEnsemble(List<BookieSocketAddress> e1,
                                     List<BookieSocketAddress> e2) {
        checkArgument(e1.size() == e2.size(), "Ensembles must be of same size");
        Set<Integer> diff = new HashSet<>();
        for (int i = 0; i < e1.size(); i++) {
            if (!e1.get(i).equals(e2.get(i))) {
                diff.add(i);
            }
        }
        return diff;
    }
}
