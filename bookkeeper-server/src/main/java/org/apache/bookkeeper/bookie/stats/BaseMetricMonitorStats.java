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

import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BASE_METRIC_MONITOR_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CPU_USED_RATE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_IO_UTIL;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LEDGER_IO_UTIL;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_BYTES_PER_SECOND;


@StatsDoc(
    name = BASE_METRIC_MONITOR_SCOPE,
    category = CATEGORY_SERVER,
    help = "Base metric monitor stats"
)
@Getter
public class BaseMetricMonitorStats {

    @StatsDoc(
        name = JOURNAL_IO_UTIL,
        help = "Journal io util"
    )
    private final Counter journalIoUtil;

    @StatsDoc(
        name = LEDGER_IO_UTIL,
        help = "ledger io util"
    )
    private final Counter ledgerIoUtil;

    @StatsDoc(
        name = CPU_USED_RATE,
        help = "cpu used rate"
    )
    private final Counter cpuUsedRate;

    @StatsDoc(
        name = WRITE_BYTES_PER_SECOND,
        help = "write byte per second"
    )
    private final Counter writeBytePerSecond;

    public BaseMetricMonitorStats(StatsLogger statsLogger) {
        journalIoUtil = statsLogger.getCounter(JOURNAL_IO_UTIL);
        ledgerIoUtil = statsLogger.getCounter(LEDGER_IO_UTIL);
        cpuUsedRate = statsLogger.getCounter(CPU_USED_RATE);
        writeBytePerSecond = statsLogger.getCounter(WRITE_BYTES_PER_SECOND);
    }
}
