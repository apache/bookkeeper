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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_FLUSH_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_GET_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_PUT_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_SNAPSHOT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_THROTTLING;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_THROTTLING_LATENCY;

import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for memtable related stats.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "EntryMemTable related stats"
)
@Getter
public class EntryMemTableStats {

    @StatsDoc(
        name = SKIP_LIST_SNAPSHOT,
        help = "operation stats of taking memtable snapshots"
    )
    private final OpStatsLogger snapshotStats;
    @StatsDoc(
        name = SKIP_LIST_PUT_ENTRY,
        help = "operation stats of putting entries to memtable",
        parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger putEntryStats;
    @StatsDoc(
        name = SKIP_LIST_GET_ENTRY,
        help = "operation stats of getting entries from memtable",
        parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger getEntryStats;
    @StatsDoc(
        name = SKIP_LIST_FLUSH_BYTES,
        help = "The number of bytes flushed from memtable to entry log files"
    )
    private final Counter flushBytesCounter;
    @StatsDoc(
        name = SKIP_LIST_THROTTLING,
        help = "The number of requests throttled due to memtables are full"
    )
    private final Counter throttlingCounter;
    @StatsDoc(
        name = SKIP_LIST_THROTTLING_LATENCY,
        help = "The distribution of request throttled duration"
    )
    private final OpStatsLogger throttlingStats;

    public EntryMemTableStats(StatsLogger statsLogger) {
        this.snapshotStats = statsLogger.getOpStatsLogger(SKIP_LIST_SNAPSHOT);
        this.putEntryStats = statsLogger.getOpStatsLogger(SKIP_LIST_PUT_ENTRY);
        this.getEntryStats = statsLogger.getOpStatsLogger(SKIP_LIST_GET_ENTRY);
        this.flushBytesCounter = statsLogger.getCounter(SKIP_LIST_FLUSH_BYTES);
        this.throttlingCounter = statsLogger.getCounter(SKIP_LIST_THROTTLING);
        this.throttlingStats = statsLogger.getOpStatsLogger(SKIP_LIST_THROTTLING_LATENCY);
    }

}
