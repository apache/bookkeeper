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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LEDGER_CACHE_NUM_EVICTED_LEDGERS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.PENDING_GET_FILE_INFO;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_FILE_INFO_CACHE_SIZE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_FILE_INFO_CACHE_SIZE;

import java.util.function.Supplier;
import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for {@link org.apache.bookkeeper.bookie.IndexPersistenceMgr} stats.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "Index Persistence Manager related stats"
)
@Getter
public class IndexPersistenceMgrStats {

    // Stats
    @StatsDoc(
        name = LEDGER_CACHE_NUM_EVICTED_LEDGERS,
        help = "Number of ledgers evicted from ledger caches"
    )
    private final Counter evictedLedgersCounter;
    @StatsDoc(
        name = PENDING_GET_FILE_INFO,
        help = "Number of pending get-file-info requests"
    )
    private final Counter pendingGetFileInfoCounter;
    @StatsDoc(
        name = WRITE_FILE_INFO_CACHE_SIZE,
        help = "Current write file info cache size"
    )
    private final Gauge<Number> writeFileInfoCacheSizeGauge;
    @StatsDoc(
        name = READ_FILE_INFO_CACHE_SIZE,
        help = "Current read file info cache size"
    )
    private final Gauge<Number> readFileInfoCacheSizeGauge;

    public IndexPersistenceMgrStats(StatsLogger statsLogger,
                                    Supplier<Number> writeFileInfoCacheSizeSupplier,
                                    Supplier<Number> readFileInfoCacheSizeSupplier) {
        evictedLedgersCounter = statsLogger.getCounter(LEDGER_CACHE_NUM_EVICTED_LEDGERS);
        pendingGetFileInfoCounter = statsLogger.getCounter(PENDING_GET_FILE_INFO);
        writeFileInfoCacheSizeGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return writeFileInfoCacheSizeSupplier.get();
            }
        };
        statsLogger.registerGauge(WRITE_FILE_INFO_CACHE_SIZE, writeFileInfoCacheSizeGauge);
        readFileInfoCacheSizeGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return readFileInfoCacheSizeSupplier.get();
            }
        };
        statsLogger.registerGauge(READ_FILE_INFO_CACHE_SIZE, readFileInfoCacheSizeGauge);
    }


}
