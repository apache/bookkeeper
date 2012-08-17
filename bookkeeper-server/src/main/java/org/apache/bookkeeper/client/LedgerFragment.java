/**
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

import java.util.List;
import java.net.InetSocketAddress;

/**
 * Represents the entries of a segment of a ledger which are stored on a single
 * bookie in the segments bookie ensemble.
 * 
 * Used for checking and recovery
 */
public class LedgerFragment {
    final int bookieIndex;
    final List<InetSocketAddress> ensemble;
    final long firstEntryId;
    final long lastEntryId;
    final long ledgerId;
    final DistributionSchedule schedule;

    LedgerFragment(long ledgerId, long firstEntryId, long lastEntryId,
            int bookieIndex, List<InetSocketAddress> ensemble,
            DistributionSchedule schedule) {
        this.ledgerId = ledgerId;
        this.firstEntryId = firstEntryId;
        this.lastEntryId = lastEntryId;
        this.bookieIndex = bookieIndex;
        this.ensemble = ensemble;
        this.schedule = schedule;
    }

    long getLedgerId() {
        return ledgerId;
    }

    /**
     * Gets the failedBookie address
     */
    public InetSocketAddress getAddress() {
        return ensemble.get(bookieIndex);
    }

    /**
     * Gets the first stored entry id of the fragment in failed bookie.
     * 
     * @return entryId
     */
    public long getFirstStoredEntryId() {
        long firstEntry = firstEntryId;

        for (int i = 0; i < ensemble.size() && firstEntry <= lastEntryId; i++) {
            if (schedule.hasEntry(firstEntry, bookieIndex)) {
                return firstEntry;
            } else {
                firstEntry++;
            }
        }
        return LedgerHandle.INVALID_ENTRY_ID;
    }

    /**
     * Gets the last stored entry id of the fragment in failed bookie.
     * 
     * @return entryId
     */
    public long getLastStoredEntryId() {
        long lastEntry = lastEntryId;
        for (int i = 0; i < ensemble.size() && lastEntry >= firstEntryId; i++) {
            if (schedule.hasEntry(lastEntry, bookieIndex)) {
                return lastEntry;
            } else {
                lastEntry--;
            }
        }
        return LedgerHandle.INVALID_ENTRY_ID;
    }

    /**
     * Gets the ensemble of fragment
     * 
     * @return
     */
    public List<InetSocketAddress> getEnsemble() {
        return this.ensemble;
    }

    @Override
    public String toString() {
        return String.format("Fragment(LedgerID: %d, FirstEntryID: %d[%d], "
                + "LastEntryID: %d[%d], Host: %s)", ledgerId, firstEntryId,
                getFirstStoredEntryId(), lastEntryId, getLastStoredEntryId(),
                getAddress());
    }
}