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

import com.google.common.collect.ImmutableMap;
import org.apache.bookkeeper.net.BookieSocketAddress;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

/**
 * A specific {@link DistributionSchedule} that places entries in round-robin
 * fashion. For ensemble size 3, and quorum size 2, Entry 0 goes to bookie 0 and
 * 1, entry 1 goes to bookie 1 and 2, and entry 2 goes to bookie 2 and 0, and so
 * on.
 *
 */
class RoundRobinDistributionSchedule implements DistributionSchedule {
    private int writeQuorumSize;
    private int ackQuorumSize;
    private int ensembleSize;


    public RoundRobinDistributionSchedule(int writeQuorumSize, int ackQuorumSize, int ensembleSize) {
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.ensembleSize = ensembleSize;
    }

    @Override
    public List<Integer> getWriteSet(long entryId) {
        List<Integer> set = new ArrayList<Integer>();
        for (int i = 0; i < this.writeQuorumSize; i++) {
            set.add((int)((entryId + i) % ensembleSize));
        }
        return set;
    }

    @Override
    public AckSet getAckSet() {
        final HashSet<Integer> ackSet = new HashSet<Integer>();
        final HashMap<Integer, BookieSocketAddress> failureMap =
                new HashMap<Integer, BookieSocketAddress>();
        return new AckSet() {
            public boolean completeBookieAndCheck(int bookieIndexHeardFrom) {
                failureMap.remove(bookieIndexHeardFrom);
                ackSet.add(bookieIndexHeardFrom);
                return ackSet.size() >= ackQuorumSize;
            }

            @Override
            public boolean failBookieAndCheck(int bookieIndexHeardFrom, BookieSocketAddress address) {
                ackSet.remove(bookieIndexHeardFrom);
                failureMap.put(bookieIndexHeardFrom, address);
                return failureMap.size() > (writeQuorumSize - ackQuorumSize);
            }

            @Override
            public Map<Integer, BookieSocketAddress> getFailedBookies() {
                return ImmutableMap.copyOf(failureMap);
            }

            public boolean removeBookieAndCheck(int bookie) {
                ackSet.remove(bookie);
                failureMap.remove(bookie);
                return ackSet.size() >= ackQuorumSize;
            }
        };
    }

    private class RRQuorumCoverageSet implements QuorumCoverageSet {
        private final int[] covered = new int[ensembleSize];

        private RRQuorumCoverageSet() {
            for (int i = 0; i < covered.length; i++) {
                covered[i] = BKException.Code.UNINITIALIZED;
            }
        }

        @Override
        public synchronized void addBookie(int bookieIndexHeardFrom, int rc) {
            covered[bookieIndexHeardFrom] = rc;
        }

        @Override
        public synchronized boolean checkCovered() {
            // now check if there are any write quorums, with |ackQuorum| nodes available
            for (int i = 0; i < ensembleSize; i++) {
                int nodesNotCovered = 0;
                int nodesOkay = 0;
                int nodesUninitialized = 0;
                for (int j = 0; j < writeQuorumSize; j++) {
                    int nodeIndex = (i + j) % ensembleSize;
                    if (covered[nodeIndex] == BKException.Code.OK) {
                        nodesOkay++;
                    } else if (covered[nodeIndex] != BKException.Code.NoSuchEntryException &&
                            covered[nodeIndex] != BKException.Code.NoSuchLedgerExistsException) {
                        nodesNotCovered++;
                    } else if (covered[nodeIndex] == BKException.Code.UNINITIALIZED) {
                        nodesUninitialized++;
                    }
                }
                // if we haven't seen any OK responses and there are still nodes not heard from,
                // let's wait until
                if (nodesNotCovered >= ackQuorumSize ||
                        (nodesOkay == 0 && nodesUninitialized > 0)) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public QuorumCoverageSet getCoverageSet() {
        return new RRQuorumCoverageSet();
    }

    @Override
    public boolean hasEntry(long entryId, int bookieIndex) {
        return getWriteSet(entryId).contains(bookieIndex);
    }
}
