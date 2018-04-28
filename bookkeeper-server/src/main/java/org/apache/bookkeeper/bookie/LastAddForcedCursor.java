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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.proto.BookieProtocol;

/**
 * Handle the LastAddForced entryId. As entries do not arrive to the Journal in a known order we have to handle
 * increments of this pointer. This is how it works: we keep a value for the current {@link #lastAddForced} entryId and
 * a list of continuous ranges of ids which are known to have been persisted but are 'after' {@link #lastAddForced}.
 * <p>
 * Examples:
 * <ul>
 * <li>0, 1, 2, 3 we have maxAddForced = 3 (all entries from 0 to 3 are stored)</li>
 * <li>0, 1, 3, 5 we have maxAddForced = 1 (all entries from 0 to 1 are stored, 2 and 4 are not stored)</li>
 * <li>1, 2, 3 we have maxAddForced = -1 (not available, as 0 is missing, so there is an gap)</li>
 * <li>&lt;empty sequence&gt; we have maxAddForced = -1 (not available)</li>
 * </ul>
 * </p>
 * <p>
 * When an gap is filled, like when we have a sequence of 0, 1, 2, 4, 5, 6, 8 and we receive a 3, we can advance
 * {@link #lastAddForced} until we find a continuos sequence of ids, so we will get to 6.
 * </p>
 * <p>
 * This cursor is kept in memory and in order to handle the case of Bookie restart we are reconstructing this
 * {@link #lastAddForced} value from {@link LedgerStorage} just be asking for the maximum written entryId (with a scan)
 * </p>
 *
 * @since 4.8
 */
class LastAddForcedCursor {

    private long lastAddForced = -1;
    private final SortedMap<Long, Long> ranges = new TreeMap<>();

    /**
     * Returns the maximum entryId which is known to have been persisted durably on local Bookie, taking into account
     * possible gaps in the sequence of entries arrived to the Bookie (think about {@link WriteAdvHandle}).
     *
     * @return an entryId if known or {@link BookieProtocol#INVALID_ENTRY_ID} if there is no availble information
     */
    public synchronized long getLastAddForced() {
        return lastAddForced;
    }

    public synchronized void update(long entryId) {
        checkArgument(entryId >= 0, "invalid entryId {}", entryId);
        if (entryId <= lastAddForced) {
            // cannot go backward
            return;
        }
        if (lastAddForced < 0 && entryId == 0) {
            // first entry, bookie could in theory receive only a subset of entries and not the whole range from 0..X,
            // but as we are not supporting ensemble changes actually each bookie will receive every entry
            lastAddForced = entryId;
            if (!ranges.isEmpty()) {
                long first = ranges.firstKey();
                if (first == 1) {
                    lastAddForced = ranges.get(first);
                    ranges.remove(first);
                }
            }
            return;
        }

        if (entryId == lastAddForced + 1) {
            // this is the most common case in case of monotonic sequence of ids
            lastAddForced = entryId;
            if (!ranges.isEmpty()) {
                // check if we are filling a gap
                long first = ranges.firstKey();
                if (lastAddForced + 1 == first) {
                    Long endOfRange = ranges.remove(first);
                    lastAddForced = endOfRange;
                }
            }
            return;
        }

        // we are out of sequence, let's add the entryId to an existing range or allocate a new one
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

    @VisibleForTesting
    synchronized int getNumRanges() {
        return ranges.size();
    }

    @VisibleForTesting
    synchronized SortedMap<Long, Long> getRanges() {
        return new TreeMap<>(ranges);
    }

}
