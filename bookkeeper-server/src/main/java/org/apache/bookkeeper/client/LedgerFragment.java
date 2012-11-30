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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

/**
 * Represents the entries of a segment of a ledger which are stored on a single
 * bookie in the segments bookie ensemble.
 * 
 * Used for checking and recovery
 */
public class LedgerFragment {
    private final int bookieIndex;
    private final List<InetSocketAddress> ensemble;
    private final long firstEntryId;
    private final long lastKnownEntryId;
    private final long ledgerId;
    private final DistributionSchedule schedule;
    private final boolean isLedgerClosed;

    LedgerFragment(LedgerHandle lh, long firstEntryId,
            long lastKnownEntryId, int bookieIndex) {
        this.ledgerId = lh.getId();
        this.firstEntryId = firstEntryId;
        this.lastKnownEntryId = lastKnownEntryId;
        this.bookieIndex = bookieIndex;
        this.ensemble = lh.getLedgerMetadata().getEnsemble(firstEntryId);
        this.schedule = lh.getDistributionSchedule();
        SortedMap<Long, ArrayList<InetSocketAddress>> ensembles = lh
                .getLedgerMetadata().getEnsembles();
        this.isLedgerClosed = lh.getLedgerMetadata().isClosed()
                || !ensemble.equals(ensembles.get(ensembles.lastKey()));
    }

    /**
     * Returns true, if and only if the ledger fragment will never be modified
     * by any of the clients in future, otherwise false. i.e,
     * <ol>
     * <li>If ledger is in closed state, then no other clients can modify this
     * fragment.</li>
     * <li>If ledger is not in closed state and the current fragment is not a
     * last fragment, then no one will modify this fragment.</li>
     * </ol>
     */
    public boolean isClosed() {
        return isLedgerClosed;
    }

    long getLedgerId() {
        return ledgerId;
    }

    long getFirstEntryId() {
        return firstEntryId;
    }

    long getLastKnownEntryId() {
        return lastKnownEntryId;
    }

    /**
     * Gets the failedBookie address
     */
    public InetSocketAddress getAddress() {
        return ensemble.get(bookieIndex);
    }
    
    /**
     * Gets the failedBookie index
     */
    public int getBookiesIndex() {
        return bookieIndex;
    }

    /**
     * Gets the first stored entry id of the fragment in failed bookie.
     * 
     * @return entryId
     */
    public long getFirstStoredEntryId() {
        long firstEntry = firstEntryId;

        for (int i = 0; i < ensemble.size() && firstEntry <= lastKnownEntryId; i++) {
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
        long lastEntry = lastKnownEntryId;
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
     * @return the ensemble for the segment which this fragment is a part of
     */
    public List<InetSocketAddress> getEnsemble() {
        return this.ensemble;
    }

    @Override
    public String toString() {
        return String.format("Fragment(LedgerID: %d, FirstEntryID: %d[%d], "
                + "LastKnownEntryID: %d[%d], Host: %s, Closed: %s)", ledgerId, firstEntryId,
                getFirstStoredEntryId(), lastKnownEntryId, getLastStoredEntryId(),
                getAddress(), isLedgerClosed);
    }
}