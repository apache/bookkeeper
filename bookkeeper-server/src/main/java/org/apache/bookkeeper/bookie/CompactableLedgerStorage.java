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

package org.apache.bookkeeper.bookie;

import java.io.IOException;

/**
 * Interface that identifies LedgerStorage implementations using EntryLogger and running periodic entries compaction.
 */
public interface CompactableLedgerStorage extends LedgerStorage {

    /**
     * @return the EntryLogger used by the ledger storage
     */
    EntryLogger getEntryLogger();

    /**
     * Get an iterator over a range of ledger ids stored in the bookie.
     *
     * @param firstLedgerId first ledger id in the sequence (included)
     * @param lastLedgerId last ledger id in the sequence (not included)
     * @return
     */
    Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId)
            throws IOException;

    /**
     * Update the location of several entries.
     *
     * @param locations the list of locations to update
     * @throws IOException
     */
    void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException;

    /**
     * Flush the entries locations index for the compacted entries.
     *
     * @throws IOException
     */
    void flushEntriesLocationsIndex() throws IOException;
}
