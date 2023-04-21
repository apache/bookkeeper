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
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.logIdFromLocation;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.makeEntry;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newDirectEntryLogger;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newDirsManager;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newLegacyEntryLogger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MockLedgerManager;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.powermock.reflect.Whitebox;

/**
 * Unit test for {@link GarbageCollectorThread}.
 */
@SuppressWarnings("deprecation")
public class GarbageCollectorThreadTest {
    private static final Slogger slog = Slogger.CONSOLE;

    private final TmpDirs tmpDirs = new TmpDirs();

    @InjectMocks
    @Spy
    private GarbageCollectorThread mockGCThread;

    @Mock
    private LedgerManager ledgerManager;
    @Mock
    private StatsLogger statsLogger;
    @Mock
    private ScheduledExecutorService gcExecutor;

    private ServerConfiguration conf = spy(new ServerConfiguration().setAllowLoopback(true));
    private CompactableLedgerStorage ledgerStorage = mock(CompactableLedgerStorage.class);

    @Before
    public void setUp() throws Exception {
        conf.setAllowLoopback(true);
        openMocks(this);
    }

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
    }

    @Test
    public void testCompactEntryLogWithException() throws Exception {
        AbstractLogCompactor mockCompactor = mock(AbstractLogCompactor.class);
        when(mockCompactor.compact(any(EntryLogMetadata.class)))
                .thenThrow(new RuntimeException("Unexpected compaction error"));
        Whitebox.setInternalState(mockGCThread, "compactor", mockCompactor);

        // Although compaction of an entry log fails due to an unexpected error,
        // the `compacting` flag should return to false
        AtomicBoolean compacting = Whitebox.getInternalState(mockGCThread, "compacting");
        assertFalse(compacting.get());
        mockGCThread.compactEntryLog(new EntryLogMetadata(9999));
        assertFalse(compacting.get());
    }

    @Test
    public void testCalculateUsageBucket() {
        // Valid range for usage is [0.0 to 1.0]
        final int numBuckets = 10;
        int[] usageBuckets = new int[numBuckets];
        String[] bucketNames = new String[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            usageBuckets[i] = 0;
            bucketNames[i] = String.format("%d%%", (i + 1) * 10);
        }

        int items = 10000;

        for (int item = 0; item <= items; item++) {
            double usage = ((double) item / (double) items);
            int index = mockGCThread.calculateUsageIndex(numBuckets, usage);
            assertFalse("Boundary condition exceeded", index < 0 || index >= numBuckets);
            slog.kv("usage", usage)
                .kv("index", index)
                .info("Mapped usage to index");
            usageBuckets[index]++;
        }

        Slogger sl = slog.ctx();
        for (int i = 0; i < numBuckets; i++) {
            sl = sl.kv(bucketNames[i], usageBuckets[i]);
        }
        sl.info("Compaction: entry log usage buckets");

        int sum = 0;
        for (int i = 0; i < numBuckets; i++) {
            sum += usageBuckets[i];
        }
        Assert.assertEquals("Incorrect number of items", items + 1, sum);
    }

    @Test
    public void testExtractMetaFromEntryLogsLegacy() throws Exception {
        File ledgerDir = tmpDirs.createNew("testExtractMeta", "ledgers");
        testExtractMetaFromEntryLogs(
                newLegacyEntryLogger(20000, ledgerDir), ledgerDir);
    }

    @Test
    public void testExtractMetaFromEntryLogsDirect() throws Exception {
        File ledgerDir = tmpDirs.createNew("testExtractMeta", "ledgers");
        testExtractMetaFromEntryLogs(
                newDirectEntryLogger(23000, // direct header is 4kb rather than 1kb
                                                       ledgerDir), ledgerDir);
    }

    private void testExtractMetaFromEntryLogs(EntryLogger entryLogger, File ledgerDir)
            throws Exception {

        MockLedgerStorage storage = new MockLedgerStorage();
        MockLedgerManager lm = new MockLedgerManager();

        GarbageCollectorThread gcThread = new GarbageCollectorThread(
                TestBKConfiguration.newServerConfiguration(), lm,
                newDirsManager(ledgerDir),
                storage, entryLogger,
                NullStatsLogger.INSTANCE);

        // Add entries.
        // Ledger 1 is on first entry log
        // Ledger 2 spans first, second and third entry log
        // Ledger 3 is on the third entry log (which is still active when extract meta)
        long loc1 = entryLogger.addEntry(1L, makeEntry(1L, 1L, 5000));
        long loc2 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 5000));
        assertThat(logIdFromLocation(loc2), equalTo(logIdFromLocation(loc1)));
        long loc3 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 15000));
        assertThat(logIdFromLocation(loc3), greaterThan(logIdFromLocation(loc2)));
        long loc4 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 15000));
        assertThat(logIdFromLocation(loc4), greaterThan(logIdFromLocation(loc3)));
        long loc5 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 1000));
        assertThat(logIdFromLocation(loc5), equalTo(logIdFromLocation(loc4)));

        long logId1 = logIdFromLocation(loc2);
        long logId2 = logIdFromLocation(loc3);
        long logId3 = logIdFromLocation(loc5);
        entryLogger.flush();

        storage.setMasterKey(1L, new byte[0]);
        storage.setMasterKey(2L, new byte[0]);
        storage.setMasterKey(3L, new byte[0]);

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2));
        assertTrue(entryLogger.logExists(logId3));

        // all ledgers exist, nothing should disappear
        final EntryLogMetadataMap entryLogMetaMap = gcThread.getEntryLogMetaMap();
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2));
        assertTrue(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId2));
        assertTrue(entryLogger.logExists(logId3));

        // log 2 is 100% ledger 2, so it should disappear if ledger 2 is deleted
        entryLogMetaMap.clear();
        storage.deleteLedger(2L);
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogger.logExists(logId3));

        // delete all ledgers, all logs except the current should be deleted
        entryLogMetaMap.clear();
        storage.deleteLedger(1L);
        storage.deleteLedger(3L);
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), empty());
        assertTrue(entryLogMetaMap.isEmpty());
        assertTrue(entryLogger.logExists(logId3));

        // add enough entries to roll log, log 3 can not be GC'd
        long loc6 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 25000));
        assertThat(logIdFromLocation(loc6), greaterThan(logIdFromLocation(loc5)));
        entryLogger.flush();
        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId3));

        entryLogMetaMap.clear();
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), empty());
        assertTrue(entryLogMetaMap.isEmpty());
        assertFalse(entryLogger.logExists(logId3));
    }

    @Test
    public void testCompactionWithFileSizeCheck() throws Exception {
        File ledgerDir = tmpDirs.createNew("testFileSize", "ledgers");
        EntryLogger entryLogger = newLegacyEntryLogger(20000, ledgerDir);

        MockLedgerStorage storage = new MockLedgerStorage();
        MockLedgerManager lm = new MockLedgerManager();

        GarbageCollectorThread gcThread = new GarbageCollectorThread(
            TestBKConfiguration.newServerConfiguration().setUseTargetEntryLogSizeForGc(true), lm,
            newDirsManager(ledgerDir),
            storage, entryLogger, NullStatsLogger.INSTANCE);

        // Add entries.
        // Ledger 1 is on first entry log
        // Ledger 2 spans first, second and third entry log
        // Ledger 3 is on the third entry log (which is still active when extract meta)
        long loc1 = entryLogger.addEntry(1L, makeEntry(1L, 1L, 5000));
        long loc2 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 5000));
        assertThat(logIdFromLocation(loc2), equalTo(logIdFromLocation(loc1)));
        long loc3 = entryLogger.addEntry(2L, makeEntry(2L, 2L, 15000));
        assertThat(logIdFromLocation(loc3), greaterThan(logIdFromLocation(loc2)));
        long loc4 = entryLogger.addEntry(2L, makeEntry(2L, 3L, 15000));
        assertThat(logIdFromLocation(loc4), greaterThan(logIdFromLocation(loc3)));
        long loc5 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 1000));
        assertThat(logIdFromLocation(loc5), equalTo(logIdFromLocation(loc4)));
        long loc6 = entryLogger.addEntry(3L, makeEntry(3L, 2L, 5000));

        long logId1 = logIdFromLocation(loc2);
        long logId2 = logIdFromLocation(loc3);
        long logId3 = logIdFromLocation(loc5);
        long logId4 = logIdFromLocation(loc6);
        entryLogger.flush();

        storage.setMasterKey(1L, new byte[0]);
        storage.setMasterKey(2L, new byte[0]);
        storage.setMasterKey(3L, new byte[0]);

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2, logId3));
        assertTrue(entryLogger.logExists(logId1));
        assertTrue(entryLogger.logExists(logId2));
        assertTrue(entryLogger.logExists(logId3));
        assertTrue(entryLogger.logExists(logId4));

        // all ledgers exist, nothing should disappear
        final EntryLogMetadataMap entryLogMetaMap = gcThread.getEntryLogMetaMap();
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2, logId3));
        assertTrue(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId2));
        assertTrue(entryLogger.logExists(logId3));

        storage.deleteLedger(1);
        // only logId 1 will be compacted.
        gcThread.runWithFlags(true, true, false);

        // logId1 and logId2 should be compacted
        assertFalse(entryLogger.logExists(logId1));
        assertTrue(entryLogger.logExists(logId2));
        assertTrue(entryLogger.logExists(logId3));
        assertFalse(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId2));

        assertEquals(1, storage.getUpdatedLocations().size());

        EntryLocation location2 = storage.getUpdatedLocations().get(0);
        assertEquals(2, location2.getLedger());
        assertEquals(1, location2.getEntry());
        assertEquals(logIdFromLocation(location2.getLocation()), logId4);
    }

    @Test
    public void testCompactionWithoutFileSizeCheck() throws Exception {
        File ledgerDir = tmpDirs.createNew("testFileSize", "ledgers");
        EntryLogger entryLogger = newLegacyEntryLogger(20000, ledgerDir);

        MockLedgerStorage storage = new MockLedgerStorage();
        MockLedgerManager lm = new MockLedgerManager();

        GarbageCollectorThread gcThread = new GarbageCollectorThread(
            TestBKConfiguration.newServerConfiguration(), lm,
            newDirsManager(ledgerDir),
            storage, entryLogger, NullStatsLogger.INSTANCE);

        // Add entries.
        // Ledger 1 is on first entry log
        // Ledger 2 spans first, second and third entry log
        // Ledger 3 is on the third entry log (which is still active when extract meta)
        long loc1 = entryLogger.addEntry(1L, makeEntry(1L, 1L, 5000));
        long loc2 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 5000));
        assertThat(logIdFromLocation(loc2), equalTo(logIdFromLocation(loc1)));
        long loc3 = entryLogger.addEntry(2L, makeEntry(2L, 2L, 15000));
        assertThat(logIdFromLocation(loc3), greaterThan(logIdFromLocation(loc2)));
        long loc4 = entryLogger.addEntry(2L, makeEntry(2L, 3L, 15000));
        assertThat(logIdFromLocation(loc4), greaterThan(logIdFromLocation(loc3)));
        long loc5 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 1000));
        assertThat(logIdFromLocation(loc5), equalTo(logIdFromLocation(loc4)));

        long logId1 = logIdFromLocation(loc2);
        long logId2 = logIdFromLocation(loc3);
        long logId3 = logIdFromLocation(loc5);
        entryLogger.flush();

        storage.setMasterKey(1L, new byte[0]);
        storage.setMasterKey(2L, new byte[0]);
        storage.setMasterKey(3L, new byte[0]);

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2));
        assertTrue(entryLogger.logExists(logId1));
        assertTrue(entryLogger.logExists(logId2));
        assertTrue(entryLogger.logExists(logId3));

        // all ledgers exist, nothing should disappear
        final EntryLogMetadataMap entryLogMetaMap = gcThread.getEntryLogMetaMap();
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2));
        assertTrue(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId2));
        assertTrue(entryLogger.logExists(logId3));

        gcThread.runWithFlags(true, true, false);

        assertTrue(entryLogger.logExists(logId1));
        assertTrue(entryLogger.logExists(logId2));
        assertTrue(entryLogger.logExists(logId3));
        assertTrue(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId2));

        assertEquals(0, storage.getUpdatedLocations().size());
    }

}
