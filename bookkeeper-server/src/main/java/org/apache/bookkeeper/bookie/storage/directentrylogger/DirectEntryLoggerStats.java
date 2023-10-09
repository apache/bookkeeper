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
package org.apache.bookkeeper.bookie.storage.directentrylogger;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

@StatsDoc(
        name = BOOKIE_SCOPE,
        category = CATEGORY_SERVER,
        help = "Direct entry logger stats"
)
class DirectEntryLoggerStats {
    private static final String ADD_ENTRY = "entrylog-add-entry";
    private static final String READ_ENTRY = "entrylog-read-entry";
    private static final String FLUSH = "entrylog-flush";
    private static final String WRITER_FLUSH = "entrylog-writer-flush";
    private static final String READ_BLOCK = "entrylog-read-block";
    private static final String READER_OPEN = "entrylog-open-reader";
    private static final String READER_CLOSE = "entrylog-close-reader";
    private static final String CACHED_READER_SERVED_CLOSED = "entrylog-cached-reader-closed";

    @StatsDoc(
              name = ADD_ENTRY,
              help = "Operation stats of adding entries to the entry log",
              parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger addEntryStats;

    @StatsDoc(
              name = READ_ENTRY,
              help = "Operation stats of reading entries from the entry log",
              parent = BOOKIE_READ_ENTRY
    )
    private static ThreadLocal<OpStatsLogger> readEntryStats;

    @StatsDoc(
              name = FLUSH,
              help = "Stats for persisting outstanding entrylog writes to disk"
    )
    private final OpStatsLogger flushStats;

    @StatsDoc(
              name = WRITER_FLUSH,
              help = "Stats for persisting outstanding entrylog writes for a single writer"
    )
    private final OpStatsLogger writerFlushStats;

    @StatsDoc(
              name = READ_BLOCK,
              help = "Stats for reading blocks from disk"
    )
    private static ThreadLocal<OpStatsLogger> readBlockStats;

    @StatsDoc(
            name = READER_OPEN,
            help = "Stats for reader open operations"
    )
    private static ThreadLocal<Counter> openReaderStats;

    @StatsDoc(
            name = READER_CLOSE,
            help = "Stats for reader close operations"
    )
    private static ThreadLocal<Counter> closeReaderStats;

    @StatsDoc(
            name = CACHED_READER_SERVED_CLOSED,
            help = "Stats for cached readers being served closed"
    )
    private static ThreadLocal<Counter> cachedReadersServedClosed;

    DirectEntryLoggerStats(StatsLogger stats) {
        addEntryStats = stats.getOpStatsLogger(ADD_ENTRY);

        flushStats = stats.getOpStatsLogger(FLUSH);
        writerFlushStats = stats.getOpStatsLogger(WRITER_FLUSH);
        setStats(stats);
    }

    private static synchronized void setStats(StatsLogger stats) {
        readEntryStats = new ThreadLocal<OpStatsLogger>() {
            @Override
            public OpStatsLogger initialValue() {
                return stats.scopeLabel("thread", String.valueOf(Thread.currentThread().getId()))
                    .getOpStatsLogger(READ_ENTRY);
            }
        };
        readBlockStats = new ThreadLocal<OpStatsLogger>() {
            @Override
            public OpStatsLogger initialValue() {
                return stats.scopeLabel("thread", String.valueOf(Thread.currentThread().getId()))
                    .getOpStatsLogger(READ_BLOCK);
            }
        };

        DirectEntryLoggerStats.openReaderStats = new ThreadLocal<Counter>() {
            @Override
            public Counter initialValue() {
                return stats.scopeLabel("thread", String.valueOf(Thread.currentThread().getId()))
                    .getCounter(READER_OPEN);
            }
        };

        DirectEntryLoggerStats.closeReaderStats = new ThreadLocal<Counter>() {
            @Override
            public Counter initialValue() {
                return stats.scopeLabel("thread", String.valueOf(Thread.currentThread().getId()))
                    .getCounter(READER_CLOSE);
            }
        };

        DirectEntryLoggerStats.cachedReadersServedClosed = new ThreadLocal<Counter>() {
            @Override
            public Counter initialValue() {
                return stats.scopeLabel("thread", String.valueOf(Thread.currentThread().getId()))
                    .getCounter(CACHED_READER_SERVED_CLOSED);
            }
        };
    }

    OpStatsLogger getAddEntryStats() {
        return addEntryStats;
    }

    OpStatsLogger getFlushStats() {
        return flushStats;
    }

    OpStatsLogger getWriterFlushStats() {
        return writerFlushStats;
    }

    OpStatsLogger getReadEntryStats() {
        return readEntryStats.get();
    }

    OpStatsLogger getReadBlockStats() {
        return readBlockStats.get();
    }

    Counter getOpenReaderCounter() {
        return openReaderStats.get();
    }

    Counter getCloseReaderCounter() {
        return closeReaderStats.get();
    }

    Counter getCachedReadersServedClosedCounter() {
        return cachedReadersServedClosed.get();
    }
}
