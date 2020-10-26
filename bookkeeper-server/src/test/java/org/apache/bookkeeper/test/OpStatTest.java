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

package org.apache.bookkeeper.test;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_CB_QUEUE_SIZE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_FORCE_WRITE_QUEUE_SIZE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_QUEUE_SIZE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SERVER_SCOPE;
import static org.junit.Assert.assertTrue;

import java.util.function.BiConsumer;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.util.MathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic tests to verify that stats are being updated as expected.
 */
public class OpStatTest extends BookKeeperClusterTestCase {
    private LedgerHandle lh;

    public OpStatTest() {
        super(1);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        lh = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32, "".getBytes());
        resetBookieOpLoggers();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        lh.close();
        lh = null;
        super.tearDown();
    }

    private void validateOpStat(TestStatsProvider stats, String path, BiConsumer<Long, Double> f) {
        assertTrue(stats != null);
        TestStatsProvider.TestOpStatsLogger logger = stats.getOpStatsLogger(path);
        assertTrue(logger != null);
        f.accept(logger.getSuccessCount(), logger.getSuccessAverage());
    }

    private void validateOpStat(TestStatsProvider stats, String[] paths, BiConsumer<Long, Double> f) {
        for (String path : paths) {
            validateOpStat(stats, path, f);
        }
    }

    private void validateNonMonotonicCounterGauge(TestStatsProvider stats, String path, BiConsumer<Long, Long> f) {
        assertTrue(stats != null);
        TestStatsProvider.TestCounter counter = stats.getCounter(path);
        assertTrue(counter != null);
        f.accept(counter.get(), counter.getMax());
    }

    private void validateNonMonotonicCounterGauges(TestStatsProvider stats, String[] paths, BiConsumer<Long, Long> f) {
        for (String path : paths) {
            validateNonMonotonicCounterGauge(stats, path, f);
        }
    }

    @Test
    public void testTopLevelBookieWriteCounters() throws Exception {
        long startNanos = MathUtils.nowInNano();
        lh.addEntry("test".getBytes());
        long elapsed = MathUtils.elapsedNanos(startNanos);
        TestStatsProvider stats = getStatsProvider(0);
        validateOpStat(stats, new String[]{
                SERVER_SCOPE + ".ADD_ENTRY",
                SERVER_SCOPE + ".ADD_ENTRY_REQUEST",
                SERVER_SCOPE + ".BookieWriteThreadPool.task_queued",
                SERVER_SCOPE + ".BookieWriteThreadPool.task_execution",
                SERVER_SCOPE + ".CHANNEL_WRITE"
        }, (count, average) -> {
            assertTrue(count == 1);
            assertTrue(average > 0);
            assertTrue(average <= elapsed);
        });
        validateOpStat(stats, new String[]{
                SERVER_SCOPE + ".CHANNEL_WRITE"
        }, (count, average) -> {
            assertTrue(count > 0);
            assertTrue(average > 0);
            assertTrue(average <= elapsed);
        });
        validateNonMonotonicCounterGauges(stats, new String[]{
                BOOKIE_SCOPE + "." + JOURNAL_SCOPE + "." + JOURNAL_CB_QUEUE_SIZE,
                BOOKIE_SCOPE + "." + JOURNAL_SCOPE + "." + JOURNAL_FORCE_WRITE_QUEUE_SIZE,
                BOOKIE_SCOPE + "." + JOURNAL_SCOPE + "." + JOURNAL_QUEUE_SIZE
        }, (value, max) -> {
            assertTrue(max > 0);
        });
    }

    @Test
    public void testTopLevelBookieReadCounters() throws Exception {
        long startNanos = MathUtils.nowInNano();
        lh.addEntry("test".getBytes());
        lh.readEntries(0, 0);
        long elapsed = MathUtils.elapsedNanos(startNanos);
        TestStatsProvider stats = getStatsProvider(0);
        validateOpStat(stats, new String[]{
                SERVER_SCOPE + ".READ_ENTRY",
                SERVER_SCOPE + ".READ_ENTRY_REQUEST",
                SERVER_SCOPE + ".BookieReadThreadPool.task_queued",
                SERVER_SCOPE + ".BookieReadThreadPool.task_execution",
        }, (count, average) -> {
            assertTrue(count == 1);
            assertTrue(average > 0);
            assertTrue(average <= elapsed);
        });
        validateOpStat(stats, new String[]{
                SERVER_SCOPE + ".CHANNEL_WRITE"
        }, (count, average) -> {
            assertTrue(count > 0);
            assertTrue(average > 0);
            assertTrue(average <= elapsed);
        });
    }
}
