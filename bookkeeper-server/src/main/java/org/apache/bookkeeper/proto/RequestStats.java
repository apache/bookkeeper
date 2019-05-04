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

package org.apache.bookkeeper.proto;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_BLOCKED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_BLOCKED_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_IN_PROGRESS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CHANNEL_WRITE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FORCE_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FORCE_LEDGER_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.GET_BOOKIE_INFO;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.GET_BOOKIE_INFO_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.GET_LIST_OF_ENTRIES_OF_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.GET_LIST_OF_ENTRIES_OF_LEDGER_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_BLOCKED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_BLOCKED_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_FENCE_READ;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_FENCE_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_FENCE_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_IN_PROGRESS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_LONG_POLL_PRE_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_LONG_POLL_READ;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_LONG_POLL_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_LONG_POLL_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_SCHEDULING_DELAY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_LAC;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_LAC_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_LAST_ENTRY_NOENTRY_ERROR;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SERVER_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_LAC;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_LAC_REQUEST;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for request related stats.
 */
@StatsDoc(
    name = SERVER_SCOPE,
    category = CATEGORY_SERVER,
    help = "Bookie request stats"
)
@Getter
public class RequestStats {

    final AtomicInteger addsInProgress = new AtomicInteger(0);
    final AtomicInteger maxAddsInProgress = new AtomicInteger(0);
    final AtomicInteger addsBlocked = new AtomicInteger(0);
    final AtomicInteger readsInProgress = new AtomicInteger(0);
    final AtomicInteger readsBlocked = new AtomicInteger(0);
    final AtomicInteger maxReadsInProgress = new AtomicInteger(0);

    @StatsDoc(
        name = ADD_ENTRY_REQUEST,
        help = "request stats of AddEntry on a bookie"
    )
    private final OpStatsLogger addRequestStats;
    @StatsDoc(
        name = ADD_ENTRY,
        help = "operation stats of AddEntry on a bookie",
        parent = ADD_ENTRY_REQUEST
    )
    private final OpStatsLogger addEntryStats;
    @StatsDoc(
        name = READ_ENTRY_REQUEST,
        help = "request stats of ReadEntry on a bookie"
    )
    final OpStatsLogger readRequestStats;
    @StatsDoc(
        name = READ_ENTRY,
        help = "operation stats of ReadEntry on a bookie",
        parent = READ_ENTRY_REQUEST
    )
    final OpStatsLogger readEntryStats;
    @StatsDoc(
        name = FORCE_LEDGER,
        help = "operation stats of ForceLedger on a bookie",
        parent = FORCE_LEDGER_REQUEST
    )
    final OpStatsLogger forceLedgerStats;
    @StatsDoc(
        name = FORCE_LEDGER_REQUEST,
        help = "request stats of ForceLedger on a bookie"
    )
    final OpStatsLogger forceLedgerRequestStats;
    @StatsDoc(
        name = READ_ENTRY_FENCE_REQUEST,
        help = "request stats of FenceRead on a bookie"
    )
    final OpStatsLogger fenceReadRequestStats;
    @StatsDoc(
        name = READ_ENTRY_FENCE_READ,
        help = "operation stats of FenceRead on a bookie",
        parent = READ_ENTRY_FENCE_REQUEST,
        happensAfter = READ_ENTRY_FENCE_WAIT
    )
    final OpStatsLogger fenceReadEntryStats;
    @StatsDoc(
        name = READ_ENTRY_FENCE_WAIT,
        help = "operation stats of FenceReadWait on a bookie",
        parent = READ_ENTRY_FENCE_REQUEST
    )
    final OpStatsLogger fenceReadWaitStats;
    @StatsDoc(
        name = READ_ENTRY_SCHEDULING_DELAY,
        help = "operation stats of ReadEntry scheduling delays on a bookie"
    )
    final OpStatsLogger readEntrySchedulingDelayStats;
    @StatsDoc(
        name = READ_ENTRY_LONG_POLL_PRE_WAIT,
        help = "operation stats of LongPoll Reads pre wait time on a bookie",
        parent = READ_ENTRY_LONG_POLL_REQUEST
    )
    final OpStatsLogger longPollPreWaitStats;
    @StatsDoc(
        name = READ_ENTRY_LONG_POLL_WAIT,
        help = "operation stats of LongPoll Reads wait time on a bookie",
        happensAfter = READ_ENTRY_LONG_POLL_PRE_WAIT,
        parent = READ_ENTRY_LONG_POLL_REQUEST
    )
    final OpStatsLogger longPollWaitStats;
    @StatsDoc(
        name = READ_ENTRY_LONG_POLL_READ,
        help = "operation stats of LongPoll Reads on a bookie",
        happensAfter = READ_ENTRY_LONG_POLL_WAIT,
        parent = READ_ENTRY_LONG_POLL_REQUEST
    )
    final OpStatsLogger longPollReadStats;
    @StatsDoc(
        name = READ_ENTRY_LONG_POLL_REQUEST,
        help = "request stats of LongPoll Reads on a bookie"
    )
    final OpStatsLogger longPollReadRequestStats;
    @StatsDoc(
        name = READ_LAST_ENTRY_NOENTRY_ERROR,
        help = "total NOENTRY errors of reading last entry on a bookie"
    )
    final Counter readLastEntryNoEntryErrorCounter;
    @StatsDoc(
        name = WRITE_LAC_REQUEST,
        help = "request stats of WriteLac on a bookie"
    )
    final OpStatsLogger writeLacRequestStats;
    @StatsDoc(
        name = WRITE_LAC,
        help = "operation stats of WriteLac on a bookie",
        parent = WRITE_LAC_REQUEST
    )
    final OpStatsLogger writeLacStats;
    @StatsDoc(
        name = READ_LAC_REQUEST,
        help = "request stats of ReadLac on a bookie"
    )
    final OpStatsLogger readLacRequestStats;
    @StatsDoc(
        name = READ_LAC,
        help = "operation stats of ReadLac on a bookie",
        parent = READ_LAC_REQUEST
    )
    final OpStatsLogger readLacStats;
    @StatsDoc(
        name = GET_BOOKIE_INFO_REQUEST,
        help = "request stats of GetBookieInfo on a bookie"
    )
    final OpStatsLogger getBookieInfoRequestStats;
    @StatsDoc(
        name = GET_BOOKIE_INFO,
        help = "operation stats of GetBookieInfo on a bookie"
    )
    final OpStatsLogger getBookieInfoStats;
    @StatsDoc(
        name = CHANNEL_WRITE,
        help = "channel write stats on a bookie"
    )
    final OpStatsLogger channelWriteStats;
    @StatsDoc(
        name = ADD_ENTRY_BLOCKED,
        help = "operation stats of AddEntry blocked on a bookie"
    )
    final OpStatsLogger addEntryBlockedStats;
    @StatsDoc(
        name = READ_ENTRY_BLOCKED,
        help = "operation stats of ReadEntry blocked on a bookie"
    )
    final OpStatsLogger readEntryBlockedStats;
    @StatsDoc(
            name = GET_LIST_OF_ENTRIES_OF_LEDGER_REQUEST,
            help = "request stats of GetListOfEntriesOfLedger on a bookie"
    )
    final OpStatsLogger getListOfEntriesOfLedgerRequestStats;
    @StatsDoc(
            name = "GET_LIST_OF_ENTRIES_OF_LEDGER",
            help = "operation stats of GetListOfEntriesOfLedger",
            parent = GET_LIST_OF_ENTRIES_OF_LEDGER_REQUEST
    )
    final OpStatsLogger getListOfEntriesOfLedgerStats;

    public RequestStats(StatsLogger statsLogger) {
        this.addEntryStats = statsLogger.getOpStatsLogger(ADD_ENTRY);
        this.addRequestStats = statsLogger.getOpStatsLogger(ADD_ENTRY_REQUEST);
        this.readEntryStats = statsLogger.getOpStatsLogger(READ_ENTRY);
        this.forceLedgerStats = statsLogger.getOpStatsLogger(FORCE_LEDGER);
        this.forceLedgerRequestStats = statsLogger.getOpStatsLogger(FORCE_LEDGER_REQUEST);
        this.readRequestStats = statsLogger.getOpStatsLogger(READ_ENTRY_REQUEST);
        this.fenceReadEntryStats = statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_READ);
        this.fenceReadRequestStats = statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_REQUEST);
        this.fenceReadWaitStats = statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_WAIT);
        this.readEntrySchedulingDelayStats = statsLogger.getOpStatsLogger(READ_ENTRY_SCHEDULING_DELAY);
        this.longPollPreWaitStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_PRE_WAIT);
        this.longPollWaitStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_WAIT);
        this.longPollReadStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_READ);
        this.longPollReadRequestStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_REQUEST);
        this.readLastEntryNoEntryErrorCounter = statsLogger.getCounter(READ_LAST_ENTRY_NOENTRY_ERROR);
        this.writeLacStats = statsLogger.getOpStatsLogger(WRITE_LAC);
        this.writeLacRequestStats = statsLogger.getOpStatsLogger(WRITE_LAC_REQUEST);
        this.readLacStats = statsLogger.getOpStatsLogger(READ_LAC);
        this.readLacRequestStats = statsLogger.getOpStatsLogger(READ_LAC_REQUEST);
        this.getBookieInfoStats = statsLogger.getOpStatsLogger(GET_BOOKIE_INFO);
        this.getBookieInfoRequestStats = statsLogger.getOpStatsLogger(GET_BOOKIE_INFO_REQUEST);
        this.channelWriteStats = statsLogger.getOpStatsLogger(CHANNEL_WRITE);

        this.addEntryBlockedStats = statsLogger.getOpStatsLogger(ADD_ENTRY_BLOCKED_WAIT);
        this.readEntryBlockedStats = statsLogger.getOpStatsLogger(READ_ENTRY_BLOCKED_WAIT);

        this.getListOfEntriesOfLedgerStats = statsLogger.getOpStatsLogger(GET_LIST_OF_ENTRIES_OF_LEDGER);
        this.getListOfEntriesOfLedgerRequestStats =
                statsLogger.getOpStatsLogger(GET_LIST_OF_ENTRIES_OF_LEDGER_REQUEST);

        statsLogger.registerGauge(ADD_ENTRY_IN_PROGRESS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return addsInProgress;
            }
        });

        statsLogger.registerGauge(ADD_ENTRY_BLOCKED, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return addsBlocked;
            }
        });

        statsLogger.registerGauge(READ_ENTRY_IN_PROGRESS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return readsInProgress;
            }
        });

        statsLogger.registerGauge(READ_ENTRY_BLOCKED, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return readsBlocked;
            }
        });
    }

    //
    // Add requests
    //

    void blockAddRequest() {
        addsBlocked.incrementAndGet();
    }

    void unblockAddRequest(long delayNanos) {
        addEntryBlockedStats.registerSuccessfulEvent(delayNanos, TimeUnit.NANOSECONDS);
        addsBlocked.decrementAndGet();
    }

    void trackAddRequest() {
        final int curr = addsInProgress.incrementAndGet();
        maxAddsInProgress.accumulateAndGet(curr, Integer::max);
    }

    void untrackAddRequest() {
        addsInProgress.decrementAndGet();
    }

    int maxAddsInProgressCount() {
        return maxAddsInProgress.get();
    }

    //
    // Read requests
    //

    void blockReadRequest() {
        readsBlocked.incrementAndGet();
    }

    void unblockReadRequest(long delayNanos) {
        readEntryBlockedStats.registerSuccessfulEvent(delayNanos, TimeUnit.NANOSECONDS);
        readsBlocked.decrementAndGet();
    }

    void trackReadRequest() {
        final int curr = readsInProgress.incrementAndGet();
        maxReadsInProgress.accumulateAndGet(curr, Integer::max);
    }

    void untrackReadRequest() {
        readsInProgress.decrementAndGet();
    }

    int maxReadsInProgressCount() {
        return maxReadsInProgress.get();
    }

}
