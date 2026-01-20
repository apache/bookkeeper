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

package org.apache.bookkeeper.bookie.storage.ldb;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;

import java.util.function.Supplier;
import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for db ledger storage stats with one instance per
 * ledger directory.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "DbLedgerStorage related stats"
)
@Getter
class DbLedgerStorageStats {

    private static final String ADD_ENTRY = "add-entry";
    private static final String READ_ENTRY = "read-entry";
    private static final String READ_ENTRY_LOCATIONS_INDEX_TIME = "read-locations-index-time";
    private static final String READ_ENTRYLOG_TIME = "read-entrylog-time";
    private static final String WRITE_CACHE_HITS = "write-cache-hits";
    private static final String WRITE_CACHE_MISSES = "write-cache-misses";
    private static final String READ_CACHE_HITS = "read-cache-hits";
    private static final String READ_CACHE_MISSES = "read-cache-misses";
    private static final String READAHEAD_BATCH_COUNT = "readahead-batch-count";
    private static final String READAHEAD_BATCH_SIZE = "readahead-batch-size";
    private static final String READAHEAD_TIME = "readahead-time";
    private static final String FLUSH = "flush";
    private static final String FLUSH_ENTRYLOG = "flush-entrylog";
    private static final String FLUSH_LOCATIONS_INDEX = "flush-locations-index";
    private static final String FLUSH_LEDGER_INDEX = "flush-ledger-index";
    private static final String FLUSH_SIZE = "flush-size";

    @Deprecated
    private static final String THROTTLED_WRITE_REQUESTS = "throttled-write-requests";
    // throttled-write-requests is deprecated, use new metric: throttled-write
    private static final String THROTTLED_WRITE = "throttled-write";
    private static final String REJECTED_WRITE_REQUESTS = "rejected-write-requests";
    private static final String WRITE_CACHE_SIZE = "write-cache-size";
    private static final String WRITE_CACHE_COUNT = "write-cache-count";
    private static final String READ_CACHE_SIZE = "read-cache-size";
    private static final String READ_CACHE_COUNT = "read-cache-count";

    @StatsDoc(
        name = ADD_ENTRY,
        help = "operation stats of adding entries to db ledger storage",
        parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger addEntryStats;
    @StatsDoc(
        name = READ_ENTRY,
        help = "operation stats of reading entries from db ledger storage",
        parent = BOOKIE_READ_ENTRY
    )
    private final OpStatsLogger readEntryStats;
    @StatsDoc(
            name = READ_ENTRY_LOCATIONS_INDEX_TIME,
            help = "time spent reading entries from the locations index of the db ledger storage engine",
            parent = READ_ENTRY
    )
    private final OpStatsLogger readFromLocationIndexTime;
    @StatsDoc(
            name = READ_ENTRYLOG_TIME,
            help = "time spent reading entries from the entry log files of the db ledger storage engine",
            parent = READ_ENTRY
    )
    private final OpStatsLogger readFromEntryLogTime;
    @StatsDoc(
            name = WRITE_CACHE_HITS,
            help = "number of write cache hits (on reads)",
            parent = READ_ENTRY
    )
    private final Counter writeCacheHitCounter;
    @StatsDoc(
            name = WRITE_CACHE_MISSES,
            help = "number of write cache misses (on reads)",
            parent = READ_ENTRY
    )
    private final Counter writeCacheMissCounter;
    @StatsDoc(
        name = READ_CACHE_HITS,
        help = "number of read cache hits",
        parent = READ_ENTRY
    )
    private final Counter readCacheHitCounter;
    @StatsDoc(
        name = READ_CACHE_MISSES,
        help = "number of read cache misses",
        parent = READ_ENTRY
    )
    private final Counter readCacheMissCounter;
    @StatsDoc(
        name = READAHEAD_BATCH_COUNT,
        help = "the distribution of num of entries to read in one readahead batch"
    )
    private final OpStatsLogger readAheadBatchCountStats;
    @StatsDoc(
        name = READAHEAD_BATCH_SIZE,
        help = "the distribution of num of bytes to read in one readahead batch"
    )
    private final OpStatsLogger readAheadBatchSizeStats;
    @StatsDoc(
            name = READAHEAD_TIME,
            help = "Time spent on readahead operations"
    )
    private final OpStatsLogger readAheadTime;
    @StatsDoc(
        name = FLUSH,
        help = "operation stats of flushing write cache to entry log files"
    )
    private final OpStatsLogger flushStats;
    @StatsDoc(
            name = FLUSH_ENTRYLOG,
            help = "operation stats of flushing to the current entry log file"
    )
    private final OpStatsLogger flushEntryLogStats;
    @StatsDoc(
            name = FLUSH_LOCATIONS_INDEX,
            help = "operation stats of flushing to the locations index"
    )
    private final OpStatsLogger flushLocationIndexStats;
    @StatsDoc(
            name = FLUSH_LOCATIONS_INDEX,
            help = "operation stats of flushing to the ledger index"
    )
    private final OpStatsLogger flushLedgerIndexStats;
    @StatsDoc(
        name = FLUSH_SIZE,
        help = "the distribution of number of bytes flushed from write cache to entry log files"
    )
    private final OpStatsLogger flushSizeStats;
    @StatsDoc(
        name = THROTTLED_WRITE_REQUESTS,
        help = "The number of requests throttled due to write cache is full"
    )
    private final Counter throttledWriteRequests;
    @StatsDoc(
            name = THROTTLED_WRITE,
            help = "The stats of throttled write due to write cache is full"
    )
    private final OpStatsLogger throttledWriteStats;
    @StatsDoc(
        name = REJECTED_WRITE_REQUESTS,
        help = "The number of requests rejected due to write cache is full"
    )
    private final Counter rejectedWriteRequests;

    @StatsDoc(
        name = WRITE_CACHE_SIZE,
        help = "Current number of bytes in write cache"
    )
    private final Gauge<Long> writeCacheSizeGauge;
    @StatsDoc(
        name = WRITE_CACHE_COUNT,
        help = "Current number of entries in write cache"
    )
    private final Gauge<Long> writeCacheCountGauge;
    @StatsDoc(
        name = READ_CACHE_SIZE,
        help = "Current number of bytes in read cache"
    )
    private final Gauge<Long> readCacheSizeGauge;
    @StatsDoc(
        name = READ_CACHE_COUNT,
        help = "Current number of entries in read cache"
    )
    private final Gauge<Long> readCacheCountGauge;

    DbLedgerStorageStats(StatsLogger stats,
                         Supplier<Long> writeCacheSizeSupplier,
                         Supplier<Long> writeCacheCountSupplier,
                         Supplier<Long> readCacheSizeSupplier,
                         Supplier<Long> readCacheCountSupplier) {
        addEntryStats = stats.getThreadScopedOpStatsLogger(ADD_ENTRY);
        readEntryStats = stats.getThreadScopedOpStatsLogger(READ_ENTRY);
        readFromLocationIndexTime = stats.getThreadScopedOpStatsLogger(READ_ENTRY_LOCATIONS_INDEX_TIME);
        readFromEntryLogTime = stats.getThreadScopedOpStatsLogger(READ_ENTRYLOG_TIME);
        readCacheHitCounter = stats.getCounter(READ_CACHE_HITS);
        readCacheMissCounter = stats.getCounter(READ_CACHE_MISSES);
        writeCacheHitCounter = stats.getCounter(WRITE_CACHE_HITS);
        writeCacheMissCounter = stats.getCounter(WRITE_CACHE_MISSES);
        readAheadBatchCountStats = stats.getOpStatsLogger(READAHEAD_BATCH_COUNT);
        readAheadBatchSizeStats = stats.getOpStatsLogger(READAHEAD_BATCH_SIZE);
        readAheadTime = stats.getOpStatsLogger(READAHEAD_TIME);
        flushStats = stats.getOpStatsLogger(FLUSH);
        flushEntryLogStats = stats.getOpStatsLogger(FLUSH_ENTRYLOG);
        flushLocationIndexStats = stats.getOpStatsLogger(FLUSH_LOCATIONS_INDEX);
        flushLedgerIndexStats = stats.getOpStatsLogger(FLUSH_LEDGER_INDEX);
        flushSizeStats = stats.getOpStatsLogger(FLUSH_SIZE);

        throttledWriteRequests = stats.getThreadScopedCounter(THROTTLED_WRITE_REQUESTS);
        throttledWriteStats = stats.getOpStatsLogger(THROTTLED_WRITE);
        rejectedWriteRequests = stats.getThreadScopedCounter(REJECTED_WRITE_REQUESTS);

        writeCacheSizeGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return writeCacheSizeSupplier.get();
            }
        };
        stats.registerGauge(WRITE_CACHE_SIZE, writeCacheSizeGauge);
        writeCacheCountGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return writeCacheCountSupplier.get();
            }
        };
        stats.registerGauge(WRITE_CACHE_COUNT, writeCacheCountGauge);
        readCacheSizeGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return readCacheSizeSupplier.get();
            }
        };
        stats.registerGauge(READ_CACHE_SIZE, readCacheSizeGauge);
        readCacheCountGauge = new Gauge<Long>() {

            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return readCacheCountSupplier.get();
            }
        };
        stats.registerGauge(READ_CACHE_COUNT, readCacheCountGauge);
    }

}
