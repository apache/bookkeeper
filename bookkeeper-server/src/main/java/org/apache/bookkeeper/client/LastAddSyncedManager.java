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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles LastAddSynced on the client side
 */
class LastAddSyncedManager {

    private final Map<Integer, Long> lastAddSyncedMap = new HashMap<>();
    private final int writeQuorumSize;
    private final int ackQuorumSize;

    public LastAddSyncedManager(int writeQuorumSize, int ackQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
    }

    /**
     * Save the lastAddSynced value for a given bookie
     *
     */
    public void updateBookie(int bookieIndexHeardFrom, long lastAddSynced) {
        lastAddSyncedMap.put(bookieIndexHeardFrom, lastAddSynced);
    }

    /**
     * Estimates the LastAddConfirmed entry
     *
     * @return the estimated value, considering the lastAddSynced piggybacked value from each bookie
     */
    public long calculateCurrentLastAddSynced() {
        /*
                Sort the ensemble by its `LastAddSynced` in ascending order
                LastAddConfirmed = max(ensemble[0..(write_quorum_size - ack_quorum_size)])
         */
        List<Long> sorted = new ArrayList<>(lastAddSyncedMap.values());
        sorted.sort(Comparator.naturalOrder());
        int maxIndex = writeQuorumSize - ackQuorumSize;
        if (sorted.size() < maxIndex || sorted.size() < ackQuorumSize) {
            return -1;
        }
        long max = -1;
        for (int i = 0; i <= maxIndex; i++) {
            long value = sorted.get(i);
            if (max < value) {
                max = value;
            }
        }
        return max;
    }
}
