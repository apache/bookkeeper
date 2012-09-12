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

import org.apache.bookkeeper.util.MathUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

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
        return new AckSet() {
            public boolean addBookieAndCheck(int bookieIndexHeardFrom) {
                ackSet.add(bookieIndexHeardFrom);
                return ackSet.size() >= ackQuorumSize;
            }

            public void removeBookie(int bookie) {
                ackSet.remove(bookie);
            }
        };
    }

    private class RRQuorumCoverageSet implements QuorumCoverageSet {
        // covered[i] is true if the quorum starting at bookie index i has been
        // covered by a recovery reply
        private boolean[] covered = null;
        private int numQuorumsUncovered;

        private RRQuorumCoverageSet() {
            covered = new boolean[ensembleSize];
            numQuorumsUncovered = ensembleSize;
        }

        public synchronized boolean addBookieAndCheckCovered(int bookieIndexHeardFrom) {
            if (numQuorumsUncovered == 0) {
                return true;
            }

            for (int i = 0; i < ackQuorumSize; i++) {
                int quorumStartIndex = MathUtils.signSafeMod(bookieIndexHeardFrom - i, ensembleSize);
                if (!covered[quorumStartIndex]) {
                    covered[quorumStartIndex] = true;
                    numQuorumsUncovered--;

                    if (numQuorumsUncovered == 0) {
                        return true;
                    }
                }
            }
            return false;
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
