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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Represents the entries of a segment of a ledger which are stored on subset of
 * bookies in the segments bookie ensemble.
 *
 * <p>Used for checking and recovery
 */
public class LedgerFragment {
    private final Set<Integer> bookieIndexes;
    private final List<BookieSocketAddress> ensemble;
    private final long firstEntryId;
    private final long lastKnownEntryId;
    private final long ledgerId;
    private final DistributionSchedule schedule;
    private final boolean isLedgerClosed;

    LedgerFragment(LedgerHandle lh,
                   long firstEntryId,
                   long lastKnownEntryId,
                   Set<Integer> bookieIndexes) {
        this.ledgerId = lh.getId();
        this.firstEntryId = firstEntryId;
        this.lastKnownEntryId = lastKnownEntryId;
        this.bookieIndexes = bookieIndexes;
        this.ensemble = lh.getLedgerMetadata().getEnsembleAt(firstEntryId);
        this.schedule = lh.getDistributionSchedule();
        SortedMap<Long, ? extends List<BookieSocketAddress>> ensembles = lh
                .getLedgerMetadata().getAllEnsembles();
        this.isLedgerClosed = lh.getLedgerMetadata().isClosed()
                || !ensemble.equals(ensembles.get(ensembles.lastKey()));
    }

    LedgerFragment(LedgerFragment lf, Set<Integer> subset) {
        this.ledgerId = lf.ledgerId;
        this.firstEntryId = lf.firstEntryId;
        this.lastKnownEntryId = lf.lastKnownEntryId;
        this.bookieIndexes = subset;
        this.ensemble = lf.ensemble;
        this.schedule = lf.schedule;
        this.isLedgerClosed = lf.isLedgerClosed;
    }

    /**
     * Return a ledger fragment contains subset of bookies.
     *
     * @param subset
     *          subset of bookies.
     * @return ledger fragment contains subset of bookies
     */
    public LedgerFragment subset(Set<Integer> subset) {
        return new LedgerFragment(this, subset);
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

    public long getFirstEntryId() {
        return firstEntryId;
    }

    public long getLastKnownEntryId() {
        return lastKnownEntryId;
    }

    /**
     * Gets the failedBookie address.
     */
    public BookieSocketAddress getAddress(int bookieIndex) {
        return ensemble.get(bookieIndex);
    }

    public Set<BookieSocketAddress> getAddresses() {
        Set<BookieSocketAddress> addresses = new HashSet<BookieSocketAddress>();
        for (int bookieIndex : bookieIndexes) {
            addresses.add(ensemble.get(bookieIndex));
        }
        return addresses;
    }

    /**
     * Gets the failedBookie index.
     */
    public Set<Integer> getBookiesIndexes() {
        return bookieIndexes;
    }

    /**
     * Gets the first stored entry id of the fragment in failed bookies.
     *
     * @return entryId
     */
    public long getFirstStoredEntryId() {
        long firstEntry = LedgerHandle.INVALID_ENTRY_ID;
        for (int bookieIndex : bookieIndexes) {
            Long firstStoredEntryForBookie = getFirstStoredEntryId(bookieIndex);
            if (firstStoredEntryForBookie != LedgerHandle.INVALID_ENTRY_ID) {
                if (firstEntry == LedgerHandle.INVALID_ENTRY_ID) {
                    firstEntry = firstStoredEntryForBookie;
                } else {
                    firstEntry = Math.min(firstEntry, firstStoredEntryForBookie);
                }
            }
        }
        return firstEntry;
    }

    /**
     * Get the first stored entry id of the fragment in the given failed bookies.
     *
     * @param bookieIndex
     *          the bookie index in the ensemble.
     * @return first stored entry id on the bookie.
     */
    public Long getFirstStoredEntryId(int bookieIndex) {
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
        long lastEntry = LedgerHandle.INVALID_ENTRY_ID;
        for (int bookieIndex : bookieIndexes) {
            Long lastStoredEntryIdForBookie = getLastStoredEntryId(bookieIndex);
            if (lastStoredEntryIdForBookie != LedgerHandle.INVALID_ENTRY_ID) {
                if (lastEntry == LedgerHandle.INVALID_ENTRY_ID) {
                    lastEntry = lastStoredEntryIdForBookie;
                } else {
                    lastEntry = Math.max(lastEntry, lastStoredEntryIdForBookie);
                }
            }
        }
        return lastEntry;
    }

    /**
     * Get the last stored entry id of the fragment in the given failed bookie.
     *
     * @param bookieIndex
     *          the bookie index in the ensemble.
     * @return first stored entry id on the bookie.
     */
    public Long getLastStoredEntryId(int bookieIndex) {
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

    public boolean isStoredEntryId(long entryId, int bookieIndex) {
        return schedule.hasEntry(entryId, bookieIndex);
    }

    /**
     * Gets the ensemble of fragment.
     *
     * @return the ensemble for the segment which this fragment is a part of
     */
    public List<BookieSocketAddress> getEnsemble() {
        return this.ensemble;
    }

    @Override
    public String toString() {
        return String.format("Fragment(LedgerID: %d, FirstEntryID: %d[%d], "
                + "LastKnownEntryID: %d[%d], Host: %s, Closed: %s)", ledgerId, firstEntryId,
                getFirstStoredEntryId(), lastKnownEntryId, getLastStoredEntryId(),
                getAddresses(), isLedgerClosed);
    }
}
