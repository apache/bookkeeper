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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;

import java.util.function.Supplier;
import lombok.Getter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for ledger metadata index stats.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "Ledger metadata index stats"
)
@Getter
class LedgerMetadataIndexStats {

    private static final String LEDGERS_COUNT = "ledgers-count";

    @StatsDoc(
        name = LEDGERS_COUNT,
        help = "Current number of ledgers"
    )
    private final Gauge<Long> ledgersCountGauge;

    LedgerMetadataIndexStats(StatsLogger statsLogger,
                             Supplier<Long> ledgersCountSupplier) {
        ledgersCountGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return ledgersCountSupplier.get();
            }
        };
        statsLogger.registerGauge(LEDGERS_COUNT, ledgersCountGauge);
    }

}
