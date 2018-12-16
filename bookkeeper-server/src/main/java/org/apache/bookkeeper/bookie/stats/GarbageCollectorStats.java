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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ACTIVE_ENTRY_LOG_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ACTIVE_ENTRY_LOG_SPACE_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ACTIVE_LEDGER_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.DELETED_LEDGER_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.MAJOR_COMPACTION_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.MINOR_COMPACTION_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.RECLAIMED_COMPACTION_SPACE_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.RECLAIMED_DELETION_SPACE_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.THREAD_RUNTIME;

import java.util.function.Supplier;
import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for gc stats.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "Garbage Collector related stats"
)
@Getter
public class GarbageCollectorStats {

    final StatsLogger statsLogger;
    @StatsDoc(
        name = MINOR_COMPACTION_COUNT,
        help = "Number of minor compactions"
    )
    private final Counter minorCompactionCounter;
    @StatsDoc(
        name = MAJOR_COMPACTION_COUNT,
        help = "Number of major compactions"
    )
    private final Counter majorCompactionCounter;
    @StatsDoc(
        name = RECLAIMED_DELETION_SPACE_BYTES,
        help = "Number of disk space bytes reclaimed via deleting entry log files"
    )
    private final Counter reclaimedSpaceViaDeletes;
    @StatsDoc(
        name = RECLAIMED_COMPACTION_SPACE_BYTES,
        help = "Number of disk space bytes reclaimed via compacting entry log files"
    )
    private final Counter reclaimedSpaceViaCompaction;
    @StatsDoc(
        name = DELETED_LEDGER_COUNT,
        help = "Number of ledgers deleted by garbage collection"
    )
    private final Counter deletedLedgerCounter;
    @StatsDoc(
        name = THREAD_RUNTIME,
        help = "Operation stats of garbage collections"
    )
    private final OpStatsLogger gcThreadRuntime;
    @StatsDoc(
        name = ACTIVE_ENTRY_LOG_COUNT,
        help = "Current number of active entry log files"
    )
    private final Gauge<Integer> activeEntryLogCountGauge;
    @StatsDoc(
        name = ACTIVE_ENTRY_LOG_SPACE_BYTES,
        help = "Current number of active entry log space bytes"
    )
    private final Gauge<Long> activeEntryLogSpaceBytesGauge;
    @StatsDoc(
        name = ACTIVE_LEDGER_COUNT,
        help = "Current number of active ledgers"
    )
    private final Gauge<Integer> activeLedgerCountGauge;

    public GarbageCollectorStats(StatsLogger statsLogger,
                                 Supplier<Integer> activeEntryLogCountSupplier,
                                 Supplier<Long> activeEntryLogSpaceBytesSupplier,
                                 Supplier<Integer> activeLedgerCountSupplier) {
        this.statsLogger = statsLogger;

        this.minorCompactionCounter = statsLogger.getCounter(MINOR_COMPACTION_COUNT);
        this.majorCompactionCounter = statsLogger.getCounter(MAJOR_COMPACTION_COUNT);
        this.reclaimedSpaceViaCompaction = statsLogger.getCounter(RECLAIMED_COMPACTION_SPACE_BYTES);
        this.reclaimedSpaceViaDeletes = statsLogger.getCounter(RECLAIMED_DELETION_SPACE_BYTES);
        this.gcThreadRuntime = statsLogger.getOpStatsLogger(THREAD_RUNTIME);
        this.deletedLedgerCounter = statsLogger.getCounter(DELETED_LEDGER_COUNT);

        this.activeEntryLogCountGauge = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return activeEntryLogCountSupplier.get();
            }
        };
        statsLogger.registerGauge(ACTIVE_ENTRY_LOG_COUNT, activeEntryLogCountGauge);
        this.activeEntryLogSpaceBytesGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return activeEntryLogSpaceBytesSupplier.get();
            }
        };
        statsLogger.registerGauge(ACTIVE_ENTRY_LOG_SPACE_BYTES, activeEntryLogSpaceBytesGauge);
        this.activeLedgerCountGauge = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return activeLedgerCountSupplier.get();
            }
        };
        statsLogger.registerGauge(ACTIVE_LEDGER_COUNT, activeLedgerCountGauge);
    }

}
