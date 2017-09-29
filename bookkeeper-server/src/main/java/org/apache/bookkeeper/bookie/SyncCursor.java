/**
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
package org.apache.bookkeeper.bookie;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * Handle the LastAddSynced entryId. As entries do not arrive to the Journal in a known order we have to handle
 * increments of this pointer.
 *
 */
public class SyncCursor {

    private long minAddSynced = -1;
    private SortedMap<Long, Long> ranges = new TreeMap<>();

    public synchronized long getCurrentMinAddSynced() {
        return minAddSynced;
    }

    @VisibleForTesting
    synchronized int getNumRanges() {
        return ranges.size();
    }

    public synchronized void update(long entryId) {
        System.out.println("update: " + entryId + ", minAddSynced:" + minAddSynced + " ranges:" + ranges);
        Preconditions.checkArgument(entryId >= 0, "invalid entryId {}", entryId);

        if (entryId < minAddSynced) {
            // cannot go backward
            return;
        }
        if (minAddSynced < 0 && entryId == 0) {
            // first entry, bookie could in theory receive only a subset of entries and not the whole range from 0..X,
            // but as we are not supporting ensemble changes actually each bookie will receive every entry
            minAddSynced = entryId;
            if (!ranges.isEmpty()) {
                long first = ranges.firstKey();
                if (first == 1) {
                    minAddSynced = ranges.get(first);
                    ranges.remove(first);
                }
            }
            return;
        }

        if (entryId == minAddSynced + 1) {
            minAddSynced = entryId;
            if (!ranges.isEmpty()) {
                long first = ranges.firstKey();
                if (minAddSynced + 1 == first) {
                    Long endOfRange = ranges.remove(first);
                    minAddSynced = endOfRange;
                }
            }
            return;
        }
        SortedMap<Long, Long> headMap = ranges.headMap(entryId);

        if (headMap.isEmpty()) {
            SortedMap<Long, Long> tailMap = ranges.tailMap(entryId);
            if (tailMap.isEmpty()) {
                // allocate new range
                ranges.put(entryId, entryId);
            } else {
                long min = tailMap.firstKey();
                if (min - 1 == entryId) {
                    // move start of range
                    long existingEndOfRange = ranges.remove(min);
                    ranges.put(entryId, existingEndOfRange);
                } else {
                    // allocate new range
                    ranges.put(entryId, entryId);
                }
            }
        } else {
            long expectedNewRangeStart = headMap.lastKey();
            long endOfRange = headMap.get(expectedNewRangeStart);
            if (entryId == endOfRange + 1) {
                // add to existing range
                ranges.put(expectedNewRangeStart, entryId);
            } else {
                // allocate new range
                ranges.put(entryId, entryId);
            }
        }
    }

}
