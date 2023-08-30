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

import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.logIdFromLocation;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.makeEntry;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newDirsManager;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newLegacyEntryLogger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.io.File;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.EntryLogIds;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.util.LedgerDirUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Test;

/**
 * TestEntryLogIds.
 */
public class TestEntryLogIds {
    private static final Slogger slog = Slogger.CONSOLE;

    private final TmpDirs tmpDirs = new TmpDirs();

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
    }

    @Test
    public void testNoStomping() throws Exception {
        File ledgerDir = tmpDirs.createNew("entryLogIds", "ledgers");

        int highestSoFar = -1;
        try (EntryLogger legacy = newLegacyEntryLogger(1024, ledgerDir)) {
            ByteBuf e1 = makeEntry(1L, 1L, 2048);
            long loc1 = legacy.addEntry(1L, e1);
            int logId1 = logIdFromLocation(loc1);

            ByteBuf e2 = makeEntry(1L, 2L, 2048);
            long loc2 = legacy.addEntry(1L, e2);
            int logId2 = logIdFromLocation(loc2);
            assertThat(logId2, greaterThan(logId1));
            highestSoFar = logId2;
        }

        EntryLogIds ids = new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        int logId3 = ids.nextId();
        assertThat(logId3, greaterThan(highestSoFar));
        touchLog(ledgerDir, logId3);
        highestSoFar = logId3;

        int logId4 = ids.nextId();
        assertThat(logId4, greaterThan(highestSoFar));
        touchLog(ledgerDir, logId4);
        highestSoFar = logId4;

        try (EntryLogger legacy = newLegacyEntryLogger(1024, ledgerDir)) {
            ByteBuf e1 = makeEntry(1L, 1L, 2048);
            long loc5 = legacy.addEntry(1L, e1);
            int logId5 = logIdFromLocation(loc5);
            assertThat(logId5, greaterThan(highestSoFar));

            ByteBuf e2 = makeEntry(1L, 2L, 2048);
            long loc6 = legacy.addEntry(1L, e2);
            int logId6 = logIdFromLocation(loc6);
            assertThat(logId6, greaterThan(logId5));
        }
    }

    @Test
    public void testNoStompingDirectStartsFirst() throws Exception {
        File ledgerDir = tmpDirs.createNew("entryLogIds", "ledgers");

        int highestSoFar = -1;
        EntryLogIds ids = new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        int logId1 = ids.nextId();
        assertThat(logId1, greaterThan(highestSoFar));
        touchLog(ledgerDir, logId1);
        highestSoFar = logId1;

        try (EntryLogger legacy = newLegacyEntryLogger(1024, ledgerDir)) {
            ByteBuf e1 = makeEntry(1L, 1L, 2048);
            long loc2 = legacy.addEntry(1L, e1);
            int logId2 = logIdFromLocation(loc2);
            assertThat(logId2, greaterThan(highestSoFar));
            highestSoFar = logId2;

            ByteBuf e2 = makeEntry(1L, 2L, 2048);
            long loc3 = legacy.addEntry(1L, e2);
            int logId3 = logIdFromLocation(loc3);
            assertThat(logId3, greaterThan(logId2));
            highestSoFar = logId3;
        }

        // reinitialize to pick up legacy
        ids = new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        int logId4 = ids.nextId();
        assertThat(logId4, greaterThan(highestSoFar));
        touchLog(ledgerDir, logId4);
        highestSoFar = logId4;
    }

    @Test
    public void testIdGenerator() throws Exception {
        File base = tmpDirs.createNew("entryLogIds", "ledgers");
        File ledgerDir1 = new File(base, "l1");
        File ledgerDir2 = new File(base, "l2");
        File ledgerDir3 = new File(base, "l3");
        File ledgerDir4 = new File(base, "l4");
        ledgerDir1.mkdir();
        ledgerDir2.mkdir();
        ledgerDir3.mkdir();
        ledgerDir4.mkdir();

        //case 1: use root ledgerDirsManager
        LedgerDirsManager ledgerDirsManager = newDirsManager(ledgerDir1, ledgerDir2);
        EntryLogIds ids1 = new EntryLogIdsImpl(ledgerDirsManager, slog);
        for (int i = 0; i < 10; i++) {
            int logId = ids1.nextId();
            File log1 = new File(ledgerDir1 + "/current", logId + ".log");
            log1.createNewFile();
            assertEquals(logId, i);
        }

        EntryLogIds ids2 = new EntryLogIdsImpl(ledgerDirsManager, slog);
        for (int i = 0; i < 10; i++) {
            int logId = ids2.nextId();
            assertEquals(logId, 10 + i);
        }

        // case 2: new LedgerDirsManager for per directory
        LedgerDirsManager ledgerDirsManager3 = newDirsManager(ledgerDir3);
        LedgerDirsManager ledgerDirsManager4 = newDirsManager(ledgerDir4);
        EntryLogIds ids3 = new EntryLogIdsImpl(ledgerDirsManager3, slog);
        for (int i = 0; i < 10; i++) {
            int logId = ids3.nextId();
            File log1 = new File(ledgerDir3 + "/current", logId + ".log");
            log1.createNewFile();
            assertEquals(logId, i);
        }

        EntryLogIds ids4 = new EntryLogIdsImpl(ledgerDirsManager4, slog);
        for (int i = 0; i < 10; i++) {
            int logId = ids4.nextId();
            assertEquals(logId, i);
        }
    }

    @Test
    public void testMultiDirectory() throws Exception {
        File base = tmpDirs.createNew("entryLogIds", "ledgers");
        File ledgerDir1 = new File(base, "l1");
        File ledgerDir2 = new File(base, "l2");
        File ledgerDir3 = new File(base, "l3");

        int highestSoFar = -1;
        try (EntryLogger legacy = newLegacyEntryLogger(1024, ledgerDir1, ledgerDir2, ledgerDir3)) {
            ByteBuf e1 = makeEntry(1L, 1L, 2048);
            long loc1 = legacy.addEntry(1L, e1);
            int logId1 = logIdFromLocation(loc1);
            assertThat(logId1, greaterThan(highestSoFar));
            highestSoFar = logId1;

            ByteBuf e2 = makeEntry(1L, 2L, 2048);
            long loc2 = legacy.addEntry(1L, e2);
            int logId2 = logIdFromLocation(loc2);
            assertThat(logId2, greaterThan(highestSoFar));
            highestSoFar = logId2;

            ByteBuf e3 = makeEntry(1L, 3L, 2048);
            long loc3 = legacy.addEntry(1L, e3);
            int logId3 = logIdFromLocation(loc3);
            assertThat(logId3, greaterThan(highestSoFar));
            highestSoFar = logId3;
        }

        EntryLogIds ids = new EntryLogIdsImpl(newDirsManager(ledgerDir1, ledgerDir2, ledgerDir3), slog);
        int logId4 = ids.nextId();
        assertThat(logId4, greaterThan(highestSoFar));
        touchLog(ledgerDir2, logId4);
        highestSoFar = logId4;

        try (EntryLogger legacy = newLegacyEntryLogger(1024, ledgerDir1, ledgerDir2, ledgerDir3)) {
            ByteBuf e1 = makeEntry(1L, 1L, 2048);
            long loc5 = legacy.addEntry(1L, e1);
            int logId5 = logIdFromLocation(loc5);
            assertThat(logId5, greaterThan(highestSoFar));
            highestSoFar = logId5;
        }
    }

    @Test
    public void testWrapAround() throws Exception {
        File ledgerDir = tmpDirs.createNew("entryLogIds", "ledgers");
        new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        touchLog(ledgerDir, Integer.MAX_VALUE - 1);

        EntryLogIds ids = new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        int logId = ids.nextId();
        assertThat(logId, equalTo(0));
    }

    @Test
    public void testCompactingLogsNotConsidered() throws Exception {
        // if there is a process restart, all "compacting" logs will be deleted
        // so their IDs are safe to reuse. Even in the case of two processes acting
        // the directory concurrently, the transactional rename will prevent data
        // loss.
        File ledgerDir = tmpDirs.createNew("entryLogIds", "ledgers");
        new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        touchLog(ledgerDir, 123);
        touchCompacting(ledgerDir, 129);

        EntryLogIds ids = new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        int logId = ids.nextId();
        assertThat(logId, equalTo(124));
    }

    @Test
    public void testCompactedLogsConsidered() throws Exception {
        File ledgerDir = tmpDirs.createNew("entryLogIds", "ledgers");
        new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        touchLog(ledgerDir, 123);
        touchCompacted(ledgerDir, 129, 123);

        EntryLogIds ids = new EntryLogIdsImpl(newDirsManager(ledgerDir), slog);
        int logId = ids.nextId();
        assertThat(logId, equalTo(130));
    }


    @Test
    public void testGapSelection() throws Exception {
        assertEquals(LedgerDirUtil.findLargestGap(Lists.newArrayList()), Pair.of(0, Integer.MAX_VALUE));
        assertEquals(LedgerDirUtil.findLargestGap(Lists.newArrayList(0)),
            Pair.of(1, Integer.MAX_VALUE));
        assertEquals(LedgerDirUtil.findLargestGap(Lists.newArrayList(1, 2, 3, 4, 5, 6)),
            Pair.of(7, Integer.MAX_VALUE));
        assertEquals(LedgerDirUtil.findLargestGap(Lists.newArrayList(Integer.MAX_VALUE)),
            Pair.of(0, Integer.MAX_VALUE));
        assertEquals(LedgerDirUtil.findLargestGap(Lists.newArrayList(Integer.MAX_VALUE / 2)),
            Pair.of(0, Integer.MAX_VALUE / 2));
        assertEquals(LedgerDirUtil.findLargestGap(Lists.newArrayList(Integer.MAX_VALUE / 2 - 1)),
            Pair.of(Integer.MAX_VALUE / 2, Integer.MAX_VALUE));
    }

    private static void touchLog(File ledgerDir, int logId) throws Exception {
        assertThat(DirectEntryLogger.logFile(new File(ledgerDir, "current"), logId).createNewFile(),
                   equalTo(true));
    }

    private static void touchCompacting(File ledgerDir, int logId) throws Exception {
        assertThat(DirectCompactionEntryLog.compactingFile(new File(ledgerDir, "current"), logId).createNewFile(),
                   equalTo(true));
    }

    private static void touchCompacted(File ledgerDir, int newLogId, int compactedLogId) throws Exception {
        assertThat(DirectCompactionEntryLog.compactedFile(new File(ledgerDir, "current"), newLogId, compactedLogId)
                   .createNewFile(), equalTo(true));
    }
}
