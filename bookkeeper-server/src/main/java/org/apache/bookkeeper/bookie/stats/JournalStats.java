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
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FORCE_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_CB_QUEUE_SIZE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_CREATION_LATENCY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_FLUSH_LATENCY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_FORCE_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_FORCE_WRITE_BATCH_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_FORCE_WRITE_BATCH_ENTRIES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_FORCE_WRITE_ENQUEUE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_FORCE_WRITE_GROUPING_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_FORCE_WRITE_QUEUE_SIZE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_NUM_FLUSH_EMPTY_QUEUE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_NUM_FLUSH_MAX_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_PROCESS_TIME_LATENCY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_QUEUE_LATENCY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_QUEUE_SIZE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_SYNC;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_WRITE_BYTES;

import lombok.Getter;
import org.apache.bookkeeper.bookie.BookKeeperServerStats;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for journal related stats.
 */
@StatsDoc(
    name = JOURNAL_SCOPE,
    category = CATEGORY_SERVER,
    help = "Journal related stats"
)
@Getter
public class JournalStats {

    @StatsDoc(
        name = JOURNAL_ADD_ENTRY,
        help = "operation stats of recording addEntry requests in the journal",
        parent = ADD_ENTRY
    )
    private final OpStatsLogger journalAddEntryStats;
    @StatsDoc(
        name = JOURNAL_FORCE_LEDGER,
        help = "operation stats of recording forceLedger requests in the journal",
        parent = FORCE_LEDGER
    )
    private final OpStatsLogger journalForceLedgerStats;
    @StatsDoc(
        name = JOURNAL_SYNC,
        help = "operation stats of syncing data to journal disks",
        parent = JOURNAL_ADD_ENTRY,
        happensAfter = JOURNAL_FORCE_WRITE_ENQUEUE
    )
    private final OpStatsLogger journalSyncStats;
    @StatsDoc(
        name = JOURNAL_FORCE_WRITE_ENQUEUE,
        help = "operation stats of enqueueing force write requests to force write queue",
        parent = JOURNAL_ADD_ENTRY,
        happensAfter = JOURNAL_PROCESS_TIME_LATENCY
    )
    private final OpStatsLogger fwEnqueueTimeStats;
    @StatsDoc(
        name = JOURNAL_CREATION_LATENCY,
        help = "operation stats of creating journal files",
        parent = JOURNAL_PROCESS_TIME_LATENCY
    )
    private final OpStatsLogger journalCreationStats;
    @StatsDoc(
        name = JOURNAL_FLUSH_LATENCY,
        help = "operation stats of flushing data from memory to filesystem (but not yet fsyncing to disks)",
        parent = JOURNAL_PROCESS_TIME_LATENCY,
        happensAfter = JOURNAL_CREATION_LATENCY
    )
    private final OpStatsLogger journalFlushStats;
    @StatsDoc(
        name = JOURNAL_PROCESS_TIME_LATENCY,
        help = "operation stats of processing requests in a journal (from dequeue an item to finish processing it)",
        parent = JOURNAL_ADD_ENTRY,
        happensAfter = JOURNAL_QUEUE_LATENCY
    )
    private final OpStatsLogger journalProcessTimeStats;
    @StatsDoc(
        name = JOURNAL_QUEUE_LATENCY,
        help = "operation stats of enqueuing requests to a journal",
        parent = JOURNAL_ADD_ENTRY
    )
    private final OpStatsLogger journalQueueStats;
    @StatsDoc(
        name = JOURNAL_FORCE_WRITE_GROUPING_COUNT,
        help = "The distribution of number of force write requests grouped in a force write"
    )
    private final OpStatsLogger forceWriteGroupingCountStats;
    @StatsDoc(
        name = JOURNAL_FORCE_WRITE_BATCH_ENTRIES,
        help = "The distribution of number of entries grouped together into a force write request"
    )
    private final OpStatsLogger forceWriteBatchEntriesStats;
    @StatsDoc(
        name = JOURNAL_FORCE_WRITE_BATCH_BYTES,
        help = "The distribution of number of bytes grouped together into a force write request"
    )
    private final OpStatsLogger forceWriteBatchBytesStats;
    @StatsDoc(
        name = JOURNAL_QUEUE_SIZE,
        help = "The journal queue size"
    )
    private final Counter journalQueueSize;
    @StatsDoc(
        name = JOURNAL_FORCE_WRITE_QUEUE_SIZE,
        help = "The force write queue size"
    )
    private final Counter forceWriteQueueSize;
    @StatsDoc(
        name = JOURNAL_CB_QUEUE_SIZE,
        help = "The journal callback queue size"
    )
    private final Counter journalCbQueueSize;
    @StatsDoc(
        name = JOURNAL_NUM_FLUSH_MAX_WAIT,
        help = "The number of journal flushes triggered by MAX_WAIT time"
    )
    private final Counter flushMaxWaitCounter;
    @StatsDoc(
        name = JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES,
        help = "The number of journal flushes triggered by MAX_OUTSTANDING_BYTES"
    )
    private final Counter flushMaxOutstandingBytesCounter;
    @StatsDoc(
        name = JOURNAL_NUM_FLUSH_EMPTY_QUEUE,
        help = "The number of journal flushes triggered when journal queue becomes empty"
    )
    private final Counter flushEmptyQueueCounter;
    @StatsDoc(
        name = JOURNAL_WRITE_BYTES,
        help = "The number of bytes appended to the journal"
    )
    private final Counter journalWriteBytes;

    public JournalStats(StatsLogger statsLogger) {
        journalAddEntryStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_ADD_ENTRY);
        journalForceLedgerStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FORCE_LEDGER);
        journalSyncStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_SYNC);
        fwEnqueueTimeStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FORCE_WRITE_ENQUEUE);
        journalCreationStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_CREATION_LATENCY);
        journalFlushStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FLUSH_LATENCY);
        journalQueueStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_QUEUE_LATENCY);
        journalProcessTimeStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_PROCESS_TIME_LATENCY);
        forceWriteGroupingCountStats =
                statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FORCE_WRITE_GROUPING_COUNT);
        forceWriteBatchEntriesStats =
                statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FORCE_WRITE_BATCH_ENTRIES);
        forceWriteBatchBytesStats = statsLogger.getOpStatsLogger(BookKeeperServerStats.JOURNAL_FORCE_WRITE_BATCH_BYTES);
        journalQueueSize = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_QUEUE_SIZE);
        forceWriteQueueSize = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_FORCE_WRITE_QUEUE_SIZE);
        journalCbQueueSize = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_CB_QUEUE_SIZE);
        flushMaxWaitCounter = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_NUM_FLUSH_MAX_WAIT);
        flushMaxOutstandingBytesCounter =
                statsLogger.getCounter(BookKeeperServerStats.JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES);
        flushEmptyQueueCounter = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_NUM_FLUSH_EMPTY_QUEUE);
        journalWriteBytes = statsLogger.getCounter(BookKeeperServerStats.JOURNAL_WRITE_BYTES);
    }

}
