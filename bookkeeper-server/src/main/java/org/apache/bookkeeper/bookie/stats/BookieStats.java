/*
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
 */

package org.apache.bookkeeper.bookie.stats;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_ADD_ENTRY_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_FORCE_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_GET_LIST_OF_ENTRIES_OF_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_READ_ENTRY_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_RECOVERY_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.GET_LIST_OF_ENTRIES_OF_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_BYTES;

import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for bookie related stats.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "Bookie related stats"
)
@Getter
public class BookieStats {

    // Expose Stats
    final StatsLogger statsLogger;
    @StatsDoc(name = WRITE_BYTES, help = "total bytes written to a bookie")
    private final Counter writeBytes;
    @StatsDoc(name = READ_BYTES, help = "total bytes read from a bookie")
    private final Counter readBytes;
    @StatsDoc(name = BOOKIE_FORCE_LEDGER, help = "total force operations occurred on a bookie")
    private final Counter forceLedgerOps;
    // Bookie Operation Latency Stats
    @StatsDoc(
        name = BOOKIE_ADD_ENTRY,
        help = "operations stats of AddEntry on a bookie",
        parent = ADD_ENTRY
    )
    private final OpStatsLogger addEntryStats;
    @StatsDoc(name = BOOKIE_RECOVERY_ADD_ENTRY, help = "operation stats of RecoveryAddEntry on a bookie")
    private final OpStatsLogger recoveryAddEntryStats;
    @StatsDoc(
        name = BOOKIE_READ_ENTRY,
        help = "operation stats of ReadEntry on a bookie",
        parent = READ_ENTRY
    )
    private final OpStatsLogger readEntryStats;
    @StatsDoc(
            name = BOOKIE_GET_LIST_OF_ENTRIES_OF_LEDGER,
            help = "operation stats of GetListOfEntriesOfLedger on a bookie",
            parent = GET_LIST_OF_ENTRIES_OF_LEDGER
    )
    private final OpStatsLogger getListOfEntriesOfLedgerStats;
    // Bookie Operation Bytes Stats
    @StatsDoc(name = BOOKIE_ADD_ENTRY_BYTES, help = "bytes stats of AddEntry on a bookie")
    private final OpStatsLogger addBytesStats;
    @StatsDoc(name = BOOKIE_READ_ENTRY_BYTES, help = "bytes stats of ReadEntry on a bookie")
    private final OpStatsLogger readBytesStats;

    public BookieStats(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        writeBytes = statsLogger.getCounter(WRITE_BYTES);
        readBytes = statsLogger.getCounter(READ_BYTES);
        forceLedgerOps = statsLogger.getCounter(BOOKIE_FORCE_LEDGER);
        addEntryStats = statsLogger.getOpStatsLogger(BOOKIE_ADD_ENTRY);
        recoveryAddEntryStats = statsLogger.getOpStatsLogger(BOOKIE_RECOVERY_ADD_ENTRY);
        readEntryStats = statsLogger.getOpStatsLogger(BOOKIE_READ_ENTRY);
        getListOfEntriesOfLedgerStats = statsLogger.getOpStatsLogger(BOOKIE_GET_LIST_OF_ENTRIES_OF_LEDGER);
        addBytesStats = statsLogger.getOpStatsLogger(BOOKIE_ADD_ENTRY_BYTES);
        readBytesStats = statsLogger.getOpStatsLogger(BOOKIE_READ_ENTRY_BYTES);
    }


}
