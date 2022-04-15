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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ACTIVE_ENTRY_LOG_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ACTIVE_ENTRY_LOG_SPACE_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.MAJOR_COMPACTION_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.MINOR_COMPACTION_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.RECLAIMED_COMPACTION_SPACE_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.RECLAIMED_DELETION_SPACE_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.THREAD_RUNTIME;
import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTED_SUFFIX;
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.BookieException.EntryLogMetadataMapException;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.storage.ldb.PersistentEntryLogMetadataMap;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.HardLink;
import org.apache.bookkeeper.util.TestUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the entry log compaction functionality.
 */
public abstract class CompactionTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionTest.class);

    private static final int ENTRY_SIZE = 1024;
    private static final int NUM_BOOKIES = 1;

    private final boolean isThrottleByBytes;
    private final DigestType digestType;
    private final byte[] passwdBytes;
    private final int numEntries;
    private final int gcWaitTime;
    private final double minorCompactionThreshold;
    private final double majorCompactionThreshold;
    private final long minorCompactionInterval;
    private final long majorCompactionInterval;
    private final String msg;
    private final boolean useMetadataCache;

    public CompactionTest(boolean isByBytes, boolean useMetadataCache) {
        super(NUM_BOOKIES);

        this.isThrottleByBytes = isByBytes;
        this.useMetadataCache = useMetadataCache;
        this.digestType = DigestType.CRC32;
        this.passwdBytes = "".getBytes();
        numEntries = 100;
        gcWaitTime = 1000;
        minorCompactionThreshold = 0.1f;
        majorCompactionThreshold = 0.5f;
        minorCompactionInterval = 2 * gcWaitTime / 1000;
        majorCompactionInterval = 4 * gcWaitTime / 1000;

        // a dummy message
        StringBuilder msgSB = new StringBuilder();
        for (int i = 0; i < ENTRY_SIZE; i++) {
            msgSB.append("a");
        }
        msg = msgSB.toString();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseConf.setEntryLogSizeLimit(numEntries * ENTRY_SIZE);
        // Disable skip list for compaction
        baseConf.setGcWaitTime(gcWaitTime);
        baseConf.setFlushInterval(100);
        baseConf.setMinorCompactionThreshold(minorCompactionThreshold);
        baseConf.setMajorCompactionThreshold(majorCompactionThreshold);
        baseConf.setMinorCompactionInterval(minorCompactionInterval);
        baseConf.setMajorCompactionInterval(majorCompactionInterval);
        baseConf.setEntryLogFilePreAllocationEnabled(false);
        baseConf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        baseConf.setIsThrottleByBytes(this.isThrottleByBytes);
        baseConf.setIsForceGCAllowWhenNoSpace(false);
        baseConf.setGcEntryLogMetadataCacheEnabled(useMetadataCache);
        super.setUp();
    }

    private GarbageCollectorThread getGCThread() throws Exception {
        assertEquals(1, bookieCount());
        BookieServer server = serverByIndex(0);
        return ((InterleavedLedgerStorage) server.getBookie().getLedgerStorage()).gcThread;
    }

    LedgerHandle[] prepareData(int numEntryLogs, boolean changeNum)
        throws Exception {
        // since an entry log file can hold at most 100 entries
        // first ledger write 2 entries, which is less than low water mark
        int num1 = 2;
        // third ledger write more than high water mark entries
        int num3 = (int) (numEntries * 0.7f);
        // second ledger write remaining entries, which is higher than low water mark
        // and less than high water mark
        int num2 = numEntries - num3 - num1;

        LedgerHandle[] lhs = new LedgerHandle[3];
        for (int i = 0; i < 3; ++i) {
            lhs[i] = bkc.createLedger(NUM_BOOKIES, NUM_BOOKIES, digestType, passwdBytes);
        }

        for (int n = 0; n < numEntryLogs; n++) {
            for (int k = 0; k < num1; k++) {
                lhs[0].addEntry(msg.getBytes());
            }
            for (int k = 0; k < num2; k++) {
                lhs[1].addEntry(msg.getBytes());
            }
            for (int k = 0; k < num3; k++) {
                lhs[2].addEntry(msg.getBytes());
            }
            if (changeNum) {
                --num2;
                ++num3;
            }
        }

        return lhs;
    }

    private void verifyLedger(long lid, long startEntryId, long endEntryId) throws Exception {
        LedgerHandle lh = bkc.openLedger(lid, digestType, passwdBytes);
        Enumeration<LedgerEntry> entries = lh.readEntries(startEntryId, endEntryId);
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals(msg, new String(entry.getEntry()));
        }
    }

    @Test
    public void testDisableCompaction() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        // disable compaction
        // restart bookies
        restartBookies(c -> {
                c.setMinorCompactionThreshold(0.0f);
                c.setMajorCompactionThreshold(0.0f);
                return c;
            });

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;

        // remove ledger2 and ledger3
        // so entry log 1 and 2 would have ledger1 entries left
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");

        assertFalse(getGCThread().enableMajorCompaction);
        assertFalse(getGCThread().enableMinorCompaction);
        getGCThread().triggerGC().get();
        if (useMetadataCache) {
            assertTrue(getGCThread().getEntryLogMetaMap() instanceof PersistentEntryLogMetadataMap);
        }

        // after garbage collection, compaction should not be executed
        assertEquals(lastMinorCompactionTime, getGCThread().lastMinorCompactionTime);
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);

        // entry logs ([0,1].log) should not be compacted.
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertTrue("Not Found entry log file ([0,1].log that should have been compacted in ledgerDirectory: "
                            + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1));
        }
    }

    @Test
    public void testForceGarbageCollectionWhenDisableCompactionConfigurationSettings() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        restartBookies(c -> {
            c.setForceAllowCompaction(true);
            c.setMajorCompactionThreshold(0.5f);
            c.setMinorCompactionThreshold(0.2f);
            c.setMajorCompactionInterval(0);
            c.setMinorCompactionInterval(0);
            return c;
        });

        assertFalse(getGCThread().enableMajorCompaction);
        assertFalse(getGCThread().enableMinorCompaction);
        assertTrue(getGCThread().isForceMajorCompactionAllow);
        assertTrue(getGCThread().isForceMinorCompactionAllow);

        assertEquals(0.5f, getGCThread().majorCompactionThreshold, 0f);
        assertEquals(0.2f, getGCThread().minorCompactionThreshold, 0f);
    }

    @Test
    public void testForceGarbageCollection() throws Exception {
        testForceGarbageCollection(true);
        testForceGarbageCollection(false);
    }

    public void testForceGarbageCollection(boolean isForceCompactionAllowWhenDisableCompaction) throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        conf.setGcWaitTime(60000);
        if (isForceCompactionAllowWhenDisableCompaction) {
            conf.setMinorCompactionInterval(0);
            conf.setMajorCompactionInterval(0);
            conf.setForceAllowCompaction(true);
            conf.setMajorCompactionThreshold(0.5f);
            conf.setMinorCompactionThreshold(0.2f);
        } else {
            conf.setMinorCompactionInterval(120000);
            conf.setMajorCompactionInterval(240000);
        }
        LedgerDirsManager dirManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        CheckpointSource cp = new CheckpointSource() {
            @Override
            public Checkpoint newCheckpoint() {
                // Do nothing.
                return null;
            }

            @Override
            public void checkpointComplete(Checkpoint checkPoint, boolean compact)
                throws IOException {
                // Do nothing.
            }
        };
        for (File journalDir : conf.getJournalDirs()) {
            BookieImpl.checkDirectoryStructure(journalDir);
        }
        for (File dir : dirManager.getAllLedgerDirs()) {
            BookieImpl.checkDirectoryStructure(dir);
        }
        runFunctionWithLedgerManagerFactory(conf, lmf -> {
            try (LedgerManager lm = lmf.newLedgerManager()) {
                InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
                storage.initialize(
                    conf,
                    lm,
                    dirManager,
                    dirManager,
                    NullStatsLogger.INSTANCE,
                    UnpooledByteBufAllocator.DEFAULT);
                storage.setCheckpointSource(cp);
                storage.setCheckpointer(Checkpointer.NULL);

                storage.start();
                long startTime = System.currentTimeMillis();
                storage.gcThread.enableForceGC();
                storage.gcThread.triggerGC().get(); //major
                storage.gcThread.triggerGC().get(); //minor
                // Minor and Major compaction times should be larger than when we started
                // this test.
                assertTrue("Minor or major compaction did not trigger even on forcing.",
                    storage.gcThread.lastMajorCompactionTime > startTime
                        && storage.gcThread.lastMinorCompactionTime > startTime);
                storage.shutdown();
            } catch (Exception e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
            return null;
        });
    }

    @Test
    public void testMinorCompaction() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // restart bookies
        restartBookies(c -> {
                // disable major compaction
                c.setMajorCompactionThreshold(0.0f);
                c.setGcWaitTime(60000);
                c.setMinorCompactionInterval(120000);
                c.setMajorCompactionInterval(240000);
                return c;
            });


        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();
        if (useMetadataCache) {
            assertTrue(getGCThread().getEntryLogMetaMap() instanceof PersistentEntryLogMetadataMap);
        }
        assertTrue(
                "ACTIVE_ENTRY_LOG_COUNT should have been updated",
                getStatsProvider(0)
                        .getGauge("bookie.gc." + ACTIVE_ENTRY_LOG_COUNT)
                        .getSample().intValue() > 0);
        assertTrue(
                "ACTIVE_ENTRY_LOG_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getGauge("bookie.gc." + ACTIVE_ENTRY_LOG_SPACE_BYTES)
                        .getSample().intValue() > 0);

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();

        // after garbage collection, major compaction should not be executed
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);

        // entry logs ([0,1,2].log) should be compacted.
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even though entry log files are removed, we still can access entries for ledger1
        // since those entries have been compacted to a new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());

        assertTrue(
                "RECLAIMED_COMPACTION_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getCounter("bookie.gc." + RECLAIMED_COMPACTION_SPACE_BYTES)
                        .get().intValue() > 0);
        assertTrue(
                "RECLAIMED_DELETION_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getCounter("bookie.gc." + RECLAIMED_DELETION_SPACE_BYTES)
                        .get().intValue() > 0);
    }

    @Test
    public void testMinorCompactionWithMaxTimeMillisOk() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(6, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable major compaction
        // restart bookies
        restartBookies(c-> {
            c.setMajorCompactionThreshold(0.0f);
            c.setGcWaitTime(60000);
            c.setMinorCompactionInterval(120000);
            c.setMajorCompactionInterval(240000);

            // Setup limit on compaction duration.
            // The limit is enough to compact.
            c.setMinorCompactionMaxTimeMillis(5000);
            c.setMajorCompactionMaxTimeMillis(5000);

            return c;
        });

        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();
        assertTrue(
                "ACTIVE_ENTRY_LOG_COUNT should have been updated",
                getStatsProvider(0)
                        .getGauge("bookie.gc." + ACTIVE_ENTRY_LOG_COUNT)
                        .getSample().intValue() > 0);
        assertTrue(
                "ACTIVE_ENTRY_LOG_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getGauge("bookie.gc." + ACTIVE_ENTRY_LOG_SPACE_BYTES)
                        .getSample().intValue() > 0);

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();

        // after garbage collection, major compaction should not be executed
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);

        // entry logs ([0,1,2].log) should be compacted.
        for (File ledgerDirectory : tmpDirs.getDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                            + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even though entry log files are removed, we still can access entries for ledger1
        // since those entries have been compacted to a new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());

        assertTrue(
                "RECLAIMED_COMPACTION_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getCounter("bookie.gc." + RECLAIMED_COMPACTION_SPACE_BYTES)
                        .get().intValue() > 0);
        assertTrue(
                "RECLAIMED_DELETION_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getCounter("bookie.gc." + RECLAIMED_DELETION_SPACE_BYTES)
                        .get().intValue() > 0);
    }


    @Test
    public void testMinorCompactionWithMaxTimeMillisTooShort() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(6, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable major compaction
        // restart bookies
        restartBookies(c-> {
            c.setMajorCompactionThreshold(0.0f);
            c.setGcWaitTime(60000);
            c.setMinorCompactionInterval(120000);
            c.setMajorCompactionInterval(240000);

            // Setup limit on compaction duration.
            // The limit is not enough to finish the compaction
            c.setMinorCompactionMaxTimeMillis(1);
            c.setMajorCompactionMaxTimeMillis(1);

            return c;
        });

        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();
        assertTrue(
                "ACTIVE_ENTRY_LOG_COUNT should have been updated",
                getStatsProvider(0)
                        .getGauge("bookie.gc." + ACTIVE_ENTRY_LOG_COUNT)
                        .getSample().intValue() > 0);
        assertTrue(
                "ACTIVE_ENTRY_LOG_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getGauge("bookie.gc." + ACTIVE_ENTRY_LOG_SPACE_BYTES)
                        .getSample().intValue() > 0);

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();

        // after garbage collection, major compaction should not be executed
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);

        // entry logs ([0,1,2].log) should be compacted.
        for (File ledgerDirectory : tmpDirs.getDirs()) {
            // Compaction of at least one of the files should not finish up
            assertTrue("Not found entry log file ([0,1,2].log that should not have been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());
    }

    @Test
    public void testForceMinorCompaction() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // restart bookies
        restartBookies(c-> {
            c.setMajorCompactionThreshold(0.0f);
            c.setGcWaitTime(60000);
            c.setMinorCompactionInterval(-1);
            c.setMajorCompactionInterval(-1);
            c.setForceAllowCompaction(true);
            return c;
        });

        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();
        assertTrue(
                "ACTIVE_ENTRY_LOG_COUNT should have been updated",
                getStatsProvider(0)
                        .getGauge("bookie.gc." + ACTIVE_ENTRY_LOG_COUNT)
                        .getSample().intValue() > 0);
        assertTrue(
                "ACTIVE_ENTRY_LOG_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getGauge("bookie.gc." + ACTIVE_ENTRY_LOG_SPACE_BYTES)
                        .getSample().intValue() > 0);

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertFalse(getGCThread().enableMinorCompaction);

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();

        // after garbage collection, major compaction should not be executed
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);

        // entry logs ([0,1,2].log) should be compacted.
        for (File ledgerDirectory : tmpDirs.getDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even though entry log files are removed, we still can access entries for ledger1
        // since those entries have been compacted to a new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());

        assertTrue(
                "RECLAIMED_COMPACTION_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getCounter("bookie.gc." + RECLAIMED_COMPACTION_SPACE_BYTES)
                        .get().intValue() > 0);
        assertTrue(
                "RECLAIMED_DELETION_SPACE_BYTES should have been updated",
                getStatsProvider(0)
                        .getCounter("bookie.gc." + RECLAIMED_DELETION_SPACE_BYTES)
                        .get().intValue() > 0);
    }

    @Test
    public void testMinorCompactionWithEntryLogPerLedgerEnabled() throws Exception {
        // restart bookies
        restartBookies(c-> {
            c.setMajorCompactionThreshold(0.0f);
            c.setGcWaitTime(60000);
            c.setMinorCompactionInterval(120000);
            c.setMajorCompactionInterval(240000);
            c.setForceAllowCompaction(true);
            c.setEntryLogPerLedgerEnabled(true);
            return c;
        });

        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);

        // remove ledgers 1 and 2
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        // Need to wait until entry log 3 gets flushed before initiating GC to satisfy assertions.
        while (!getGCThread().entryLogger.isFlushedEntryLog(3L)) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().triggerGC(true, false, false).get();

        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);

        // At this point, we have the following state of ledgers end entry logs:
        // L0 (not deleted) -> E0 (un-flushed): Entry log should exist.
        // L1 (deleted) -> E1 (un-flushed): Entry log should exist as un-flushed entry logs are not considered for GC.
        // L2 (deleted) -> E2 (flushed): Entry log should have been garbage collected.
        //                 E3 (flushed): Entry log should have been garbage collected.
        //                 E4 (un-flushed): Entry log should exist as un-flushed entry logs are not considered for GC.
        assertTrue("Not found entry log files [0, 1, 4].log that should not have been compacted in: "
                + tmpDirs.getDirs().get(0), TestUtils.hasAllLogFiles(tmpDirs.getDirs().get(0), 0, 1, 4));
        assertTrue("Found entry log files [2, 3].log that should have been compacted in ledgerDirectory: "
                + tmpDirs.getDirs().get(0), TestUtils.hasNoneLogFiles(tmpDirs.getDirs().get(0), 2, 3));

        // Now, let's mark E1 as flushed, as its ledger L1 has been deleted already. In this case, the GC algorithm
        // should consider it for deletion.
        getGCThread().entryLogger.recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(1L);
        getGCThread().triggerGC(true, false, false).get();
        assertTrue("Found entry log file 1.log that should have been compacted in ledgerDirectory: "
                + tmpDirs.getDirs().get(0), TestUtils.hasNoneLogFiles(tmpDirs.getDirs().get(0), 1));

        // Once removed the ledger L0, then deleting E0 is fine (only if it has been flushed).
        bkc.deleteLedger(lhs[0].getId());
        getGCThread().triggerGC(true, false, false).get();
        assertTrue("Found entry log file 0.log that should not have been compacted in ledgerDirectory: "
                + tmpDirs.getDirs().get(0), TestUtils.hasAllLogFiles(tmpDirs.getDirs().get(0), 0));
        getGCThread().entryLogger.recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(0L);
        getGCThread().triggerGC(true, false, false).get();
        assertTrue("Found entry log file 0.log that should have been compacted in ledgerDirectory: "
                + tmpDirs.getDirs().get(0), TestUtils.hasNoneLogFiles(tmpDirs.getDirs().get(0), 0));
    }

    @Test
    public void testMinorCompactionWithNoWritableLedgerDirs() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // restart bookies
        restartBookies(c -> {
                // disable major compaction
                c.setMajorCompactionThreshold(0.0f);
                c.setGcWaitTime(60000);
                c.setMinorCompactionInterval(120000);
                c.setMajorCompactionInterval(240000);
                return c;
            });

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);

        for (int i = 0; i < bookieCount(); i++) {
            BookieImpl bookie = ((BookieImpl) serverByIndex(i).getBookie());
            LedgerDirsManager ledgerDirsManager = bookie.getLedgerDirsManager();
            List<File> ledgerDirs = ledgerDirsManager.getAllLedgerDirs();
            // if all the discs are full then Major and Minor compaction would be disabled since
            // 'isForceGCAllowWhenNoSpace' is not enabled. Check LedgerDirsListener of interleavedLedgerStorage.
            for (File ledgerDir : ledgerDirs) {
                ledgerDirsManager.addToFilledDirs(ledgerDir);
            }
        }

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().triggerGC().get();
        if (useMetadataCache) {
            assertTrue(getGCThread().getEntryLogMetaMap() instanceof PersistentEntryLogMetadataMap);
        }
        // after garbage collection, major compaction should not be executed
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);
        assertEquals(lastMinorCompactionTime, getGCThread().lastMinorCompactionTime);

        // entry logs ([0,1,2].log) should still remain, because both major and Minor compaction are disabled.
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertTrue(
                    "All the entry log files ([0,1,2].log are not available, which is not expected" + ledgerDirectory,
                    TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1, 2));
        }
    }

    @Test
    public void testMinorCompactionWithNoWritableLedgerDirsButIsForceGCAllowWhenNoSpaceIsSet() throws Exception {
        stopAllBookies();
        ServerConfiguration conf = newServerConfiguration();
        // disable major compaction
        conf.setMajorCompactionThreshold(0.0f);
        // here we are setting isForceGCAllowWhenNoSpace to true, so Major and Minor compaction wont be disabled in case
        // when discs are full
        conf.setIsForceGCAllowWhenNoSpace(true);
        conf.setGcWaitTime(600000);
        conf.setMinorCompactionInterval(120000);
        conf.setMajorCompactionInterval(240000);
        // We need at least 2 ledger dirs because compaction will flush ledger cache, and will
        // trigger relocateIndexFileAndFlushHeader. If we only have one ledger dir, compaction will always fail
        // when there's no writeable ledger dir.
        File ledgerDir1 = tmpDirs.createNew("ledger", "test1");
        File ledgerDir2 = tmpDirs.createNew("ledger", "test2");
        File journalDir = tmpDirs.createNew("journal", "test");
        String[] ledgerDirNames = new String[]{
            ledgerDir1.getPath(),
            ledgerDir2.getPath()
        };
        conf.setLedgerDirNames(ledgerDirNames);
        conf.setJournalDirName(journalDir.getPath());
        BookieServer server = startAndAddBookie(conf).getServer();
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);
        if (useMetadataCache) {
            assertTrue(getGCThread().getEntryLogMetaMap() instanceof PersistentEntryLogMetadataMap);
        }
        for (int i = 0; i < bookieCount(); i++) {
            BookieImpl bookie = ((BookieImpl) serverByIndex(i).getBookie());
            bookie.getLedgerStorage().flush();
            bookie.dirsMonitor.shutdown();
            LedgerDirsManager ledgerDirsManager = bookie.getLedgerDirsManager();
            List<File> ledgerDirs = ledgerDirsManager.getAllLedgerDirs();
            // Major and Minor compaction are not disabled even though discs are full. Check LedgerDirsListener of
            // interleavedLedgerStorage.
            for (File ledgerDir : ledgerDirs) {
                ledgerDirsManager.addToFilledDirs(ledgerDir);
            }
        }

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().triggerGC(true, false, false).get();

        // after garbage collection, major compaction should not be executed
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);

        // though all discs are added to filled dirs list, compaction would succeed, because in EntryLogger for
        // allocating newlog
        // we get getWritableLedgerDirsForNewLog() of ledgerDirsManager instead of getWritableLedgerDirs()
        // entry logs ([0,1,2].log) should be compacted.
        for (File ledgerDirectory : ((BookieImpl) server.getBookie()).getLedgerDirsManager().getAllLedgerDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory.getParentFile(), true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());

        // for the sake of validity of test lets make sure that there is no writableLedgerDir in the bookies
        for (int i = 0; i < bookieCount(); i++) {
            BookieImpl bookie = (BookieImpl) serverByIndex(i).getBookie();
            LedgerDirsManager ledgerDirsManager = bookie.getLedgerDirsManager();
            try {
                List<File> ledgerDirs = ledgerDirsManager.getWritableLedgerDirs();
                // it is expected not to have any writableLedgerDirs since we added all of them to FilledDirs
                fail("It is expected not to have any writableLedgerDirs");
            } catch (NoWritableLedgerDirException nwe) {

            }
        }
    }

    @Test
    public void testMajorCompaction() throws Exception {

        // prepare data
        LedgerHandle[] lhs = prepareData(3, true);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // restart bookies
        restartBookies(c -> {
                // disable minor compaction
                c.setMinorCompactionThreshold(0.0f);
                c.setGcWaitTime(60000);
                c.setMinorCompactionInterval(120000);
                c.setMajorCompactionInterval(240000);
                return c;
            });

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertTrue(getGCThread().enableMajorCompaction);
        assertFalse(getGCThread().enableMinorCompaction);

        // remove ledger1 and ledger3
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();
        if (useMetadataCache) {
            assertTrue(getGCThread().getEntryLogMetaMap() instanceof PersistentEntryLogMetadataMap);
        }
        // after garbage collection, minor compaction should not be executed
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);
        assertTrue(getGCThread().lastMajorCompactionTime > lastMajorCompactionTime);

        // entry logs ([0,1,2].log) should be compacted
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                      + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger2
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[1].getId(), 0, lhs[1].getLastAddConfirmed());
    }

    @Test
    public void testForceMajorCompaction() throws Exception {

        // prepare data
        LedgerHandle[] lhs = prepareData(3, true);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // restart bookies
        restartBookies(c-> {
            // disable minor compaction
            c.setMinorCompactionThreshold(0.0f);
            c.setGcWaitTime(60000);
            c.setMinorCompactionInterval(-1);
            c.setMajorCompactionInterval(-1);
            c.setForceAllowCompaction(true);
            return c;
        });

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertFalse(getGCThread().enableMinorCompaction);
        assertTrue(getGCThread().isForceMajorCompactionAllow);
        assertFalse(getGCThread().isForceMinorCompactionAllow);

        // remove ledger1 and ledger3
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();
        if (useMetadataCache) {
            assertTrue(getGCThread().getEntryLogMetaMap() instanceof PersistentEntryLogMetadataMap);
        }
        // after garbage collection, minor compaction should not be executed
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);
        assertTrue(getGCThread().lastMajorCompactionTime > lastMajorCompactionTime);

        // entry logs ([0,1,2].log) should be compacted
        for (File ledgerDirectory : tmpDirs.getDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger2
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[1].getId(), 0, lhs[1].getLastAddConfirmed());
    }

    @Test
    public void testCompactionPersistence() throws Exception {
        /*
         * for this test scenario we are assuming that there will be only one
         * bookie in the cluster
         */
        assertEquals("Numbers of Bookies in this cluster", 1, numBookies);
        /*
         * this test is for validating EntryLogCompactor, so make sure
         * TransactionalCompaction is not enabled.
         */
        assertFalse("Bookies must be using EntryLogCompactor", baseConf.getUseTransactionalCompaction());
        // prepare data
        LedgerHandle[] lhs = prepareData(3, true);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // restart bookies
        restartBookies(c -> {
                // disable minor compaction
                c.setMinorCompactionThreshold(0.0f);
                c.setGcWaitTime(60000);
                c.setMinorCompactionInterval(120000);
                c.setMajorCompactionInterval(240000);
                return c;
            });

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertTrue(getGCThread().enableMajorCompaction);
        assertFalse(getGCThread().enableMinorCompaction);

        // remove ledger1 and ledger3
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();

        // after garbage collection, minor compaction should not be executed
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);
        assertTrue(getGCThread().lastMajorCompactionTime > lastMajorCompactionTime);

        // entry logs ([0,1,2].log) should be compacted
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for
        // ledger2
        // since those entries has been compacted to new entry log
        long ledgerId = lhs[1].getId();
        long lastAddConfirmed = lhs[1].getLastAddConfirmed();
        verifyLedger(ledgerId, 0, lastAddConfirmed);

        /*
         * there is only one bookie in the cluster so we should be able to read
         * entries from this bookie.
         */
        ServerConfiguration bookieServerConfig = ((BookieImpl) serverByIndex(0).getBookie()).conf;
        ServerConfiguration newBookieConf = new ServerConfiguration(bookieServerConfig);
        /*
         * by reusing bookieServerConfig and setting metadataServiceUri to null
         * we can create/start new Bookie instance using the same data
         * (journal/ledger/index) of the existing BookeieServer for our testing
         * purpose.
         */
        newBookieConf.setMetadataServiceUri(null);
        String entryLogCachePath = tmpDirs.createNew("entry", "bk2").getAbsolutePath();
        newBookieConf.setGcEntryLogMetadataCachePath(entryLogCachePath);
        Bookie newbookie = new TestBookieImpl(newBookieConf);

        DigestManager digestManager = DigestManager.instantiate(ledgerId, passwdBytes,
                BookKeeper.DigestType.toProtoDigestType(digestType), UnpooledByteBufAllocator.DEFAULT,
                baseClientConf.getUseV2WireProtocol());

        for (long entryId = 0; entryId <= lastAddConfirmed; entryId++) {
            ByteBuf readEntryBufWithChecksum = newbookie.readEntry(ledgerId, entryId);
            ByteBuf readEntryBuf = digestManager.verifyDigestAndReturnData(entryId, readEntryBufWithChecksum);
            byte[] readEntryBytes = new byte[readEntryBuf.readableBytes()];
            readEntryBuf.readBytes(readEntryBytes);
            assertEquals(msg, new String(readEntryBytes));
        }
    }

    @Test
    public void testCompactionWhenLedgerDirsAreFull() throws Exception {
        /*
         * for this test scenario we are assuming that there will be only one
         * bookie in the cluster
         */
        assertEquals("Numbers of Bookies in this cluster", 1, bookieCount());
        ServerConfiguration serverConfig = confByIndex(0);
        File ledgerDir = serverConfig.getLedgerDirs()[0];
        assertEquals("Number of Ledgerdirs for this bookie", 1, serverConfig.getLedgerDirs().length);
        assertTrue("indexdirs should be configured to null", null == serverConfig.getIndexDirs());
        /*
         * this test is for validating EntryLogCompactor, so make sure
         * TransactionalCompaction is not enabled.
         */
        assertFalse("Bookies must be using EntryLogCompactor", baseConf.getUseTransactionalCompaction());
        // prepare data
        LedgerHandle[] lhs = prepareData(3, true);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        serverByIndex(0).getBookie().getLedgerStorage().flush();
        assertTrue(
                "entry log file ([0,1,2].log should be available in ledgerDirectory: "
                        + serverConfig.getLedgerDirs()[0],
                TestUtils.hasLogFiles(serverConfig.getLedgerDirs()[0], false, 0, 1, 2));

        long usableSpace = ledgerDir.getUsableSpace();
        long totalSpace = ledgerDir.getTotalSpace();

        /*
         * because of the value set for diskUsageThreshold, when bookie is
         * restarted it wouldn't find any writableledgerdir. But we have set
         * very low values for minUsableSizeForEntryLogCreation and
         * minUsableSizeForIndexFileCreation, so it should be able to create
         * EntryLog file and Index file for doing compaction.
         */

        // restart bookies
        restartBookies(c -> {
                c.setForceReadOnlyBookie(true);
                c.setIsForceGCAllowWhenNoSpace(true);
                // disable minor compaction
                c.setMinorCompactionThreshold(0.0f);
                c.setGcWaitTime(60000);
                c.setMinorCompactionInterval(120000);
                c.setMajorCompactionInterval(240000);
                c.setMinUsableSizeForEntryLogCreation(1);
                c.setMinUsableSizeForIndexFileCreation(1);
                c.setDiskUsageThreshold((1.0f - ((float) usableSpace / (float) totalSpace)) * 0.9f);
                c.setDiskUsageWarnThreshold(0.0f);
                return c;
            });

        assertFalse("There shouldn't be any writable ledgerDir",
                    ((BookieImpl) serverByIndex(0).getBookie()).getLedgerDirsManager().hasWritableLedgerDirs());

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertTrue(getGCThread().enableMajorCompaction);
        assertFalse(getGCThread().enableMinorCompaction);

        // remove ledger1 and ledger3
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();
        if (useMetadataCache) {
            assertTrue(getGCThread().getEntryLogMetaMap() instanceof PersistentEntryLogMetadataMap);
        }
        // after garbage collection, minor compaction should not be executed
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);
        assertTrue(getGCThread().lastMajorCompactionTime > lastMajorCompactionTime);

        /*
         * GarbageCollection should have succeeded, so no previous entrylog
         * should be available.
         */

        // entry logs ([0,1,2].log) should be compacted
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for
        // ledger2
        // since those entries has been compacted to new entry log
        long ledgerId = lhs[1].getId();
        long lastAddConfirmed = lhs[1].getLastAddConfirmed();
        verifyLedger(ledgerId, 0, lastAddConfirmed);
    }

    @Test
    public void testMajorCompactionAboveThreshold() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertTrue(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);

        // remove ledger1 and ledger2
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[1].getId());
        LOG.info("Finished deleting the ledgers contains less entries.");
        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();

        // after garbage collection, minor compaction should not be executed
        assertTrue(getGCThread().lastMinorCompactionTime > lastMinorCompactionTime);
        assertTrue(getGCThread().lastMajorCompactionTime > lastMajorCompactionTime);

        // entry logs ([0,1,2].log) should not be compacted
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertTrue("Not Found entry log file ([1,2].log that should have been compacted in ledgerDirectory: "
                     + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1, 2));
        }
    }

    @Test
    public void testCompactionSmallEntryLogs() throws Exception {

        // create a ledger to write a few entries
        LedgerHandle alh = bkc.createLedger(NUM_BOOKIES, NUM_BOOKIES, digestType, "".getBytes());
        for (int i = 0; i < 3; i++) {
           alh.addEntry(msg.getBytes());
        }
        alh.close();

        // restart bookie to roll entry log files
        restartBookies();

        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        // restart bookies again to roll entry log files.
        restartBookies();

        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();
        if (useMetadataCache) {
            assertTrue(getGCThread().getEntryLogMetaMap() instanceof PersistentEntryLogMetadataMap);
        }
        // entry logs (0.log) should not be compacted
        // entry logs ([1,2,3].log) should be compacted.
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertTrue("Not Found entry log file ([0].log that should have been compacted in ledgerDirectory: "
                     + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0));
            assertFalse("Found entry log file ([1,2,3].log that should have not been compacted in ledgerDirectory: "
                      + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 1, 2, 3));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());
    }

    /**
     * Test that compaction doesnt add to index without having persisted
     * entrylog first. This is needed because compaction doesn't go through the journal.
     * {@see https://issues.apache.org/jira/browse/BOOKKEEPER-530}
     * {@see https://issues.apache.org/jira/browse/BOOKKEEPER-664}
     */
    @Test
    public void testCompactionSafety() throws Exception {
        tearDown(); // I dont want the test infrastructure
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        final Set<Long> ledgers = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        LedgerManager manager = getLedgerManager(ledgers);

        File tmpDir = tmpDirs.createNew("bkTest", ".dir");
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});

        conf.setEntryLogSizeLimit(EntryLogger.LOGFILE_HEADER_SIZE + 3 * (4 + ENTRY_SIZE));
        conf.setGcWaitTime(100);
        conf.setMinorCompactionThreshold(0.7f);
        conf.setMajorCompactionThreshold(0.0f);
        conf.setMinorCompactionInterval(1);
        conf.setMajorCompactionInterval(10);
        conf.setPageLimit(1);

        CheckpointSource checkpointSource = new CheckpointSource() {
                AtomicInteger idGen = new AtomicInteger(0);
                class MyCheckpoint implements CheckpointSource.Checkpoint {
                    int id = idGen.incrementAndGet();
                    @Override
                    public int compareTo(CheckpointSource.Checkpoint o) {
                        if (o == CheckpointSource.Checkpoint.MAX) {
                            return -1;
                        } else if (o == CheckpointSource.Checkpoint.MIN) {
                            return 1;
                        }
                        return id - ((MyCheckpoint) o).id;
                    }
                }

                @Override
                public CheckpointSource.Checkpoint newCheckpoint() {
                    return new MyCheckpoint();
                }

                public void checkpointComplete(CheckpointSource.Checkpoint checkpoint, boolean compact)
                        throws IOException {
                }
            };
        final byte[] key = "foobar".getBytes();
        File log0 = new File(curDir, "0.log");
        LedgerDirsManager dirs = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        assertFalse("Log shouldnt exist", log0.exists());
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
        storage.initialize(
            conf,
            manager,
            dirs,
            dirs,
            NullStatsLogger.INSTANCE,
            UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(Checkpointer.NULL);

        ledgers.add(1L);
        ledgers.add(2L);
        ledgers.add(3L);
        storage.setMasterKey(1, key);
        storage.setMasterKey(2, key);
        storage.setMasterKey(3, key);
        storage.addEntry(genEntry(1, 1, ENTRY_SIZE));
        storage.addEntry(genEntry(2, 1, ENTRY_SIZE));
        storage.addEntry(genEntry(2, 2, ENTRY_SIZE));
        storage.addEntry(genEntry(3, 2, ENTRY_SIZE));
        storage.flush();
        storage.shutdown();

        assertTrue("Log should exist", log0.exists());
        ledgers.remove(2L);
        ledgers.remove(3L);

        storage = new InterleavedLedgerStorage();
        storage.initialize(
            conf,
            manager,
            dirs, dirs,
            NullStatsLogger.INSTANCE,
            UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(Checkpointer.NULL);

        storage.start();
        for (int i = 0; i < 10; i++) {
            if (!log0.exists()) {
                break;
            }
            Thread.sleep(1000);
            storage.entryLogger.flush(); // simulate sync thread
        }
        assertFalse("Log shouldnt exist", log0.exists());

        ledgers.add(4L);
        storage.setMasterKey(4, key);
        storage.addEntry(genEntry(4, 1, ENTRY_SIZE)); // force ledger 1 page to flush
        storage.shutdown();

        storage = new InterleavedLedgerStorage();
        storage.initialize(
            conf,
            manager,
            dirs,
            dirs,
            NullStatsLogger.INSTANCE,
            UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(Checkpointer.NULL);

        storage.getEntry(1, 1); // entry should exist
    }

    private LedgerManager getLedgerManager(final Set<Long> ledgers) {
        LedgerManager manager = new LedgerManager() {
                @Override
                public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long lid,
                                                                                         LedgerMetadata metadata) {
                    unsupported();
                    return null;
                }
                @Override
                public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
                    unsupported();
                    return null;
                }
                @Override
                public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
                    unsupported();
                    return null;
                }
                @Override
                public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId,
                                                                                        LedgerMetadata metadata,
                                                                                        Version currentVersion) {
                    unsupported();
                    return null;
                }
                @Override
                public void asyncProcessLedgers(Processor<Long> processor,
                                                AsyncCallback.VoidCallback finalCb,
                        Object context, int successRc, int failureRc) {
                    unsupported();
                }
                @Override
                public void registerLedgerMetadataListener(long ledgerId,
                        LedgerMetadataListener listener) {
                    unsupported();
                }
                @Override
                public void unregisterLedgerMetadataListener(long ledgerId,
                        LedgerMetadataListener listener) {
                    unsupported();
                }
                @Override
                public void close() throws IOException {}

                void unsupported() {
                    LOG.error("Unsupported operation called", new Exception());
                    throw new RuntimeException("Unsupported op");
                }

                @Override
                public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
                    final AtomicBoolean hasnext = new AtomicBoolean(true);
                    return new LedgerManager.LedgerRangeIterator() {
                        @Override
                        public boolean hasNext() throws IOException {
                            return hasnext.get();
                        }
                        @Override
                        public LedgerManager.LedgerRange next() throws IOException {
                            hasnext.set(false);
                            return new LedgerManager.LedgerRange(ledgers);
                        }
                    };
                 }
            };
        return manager;
    }

    /**
     * Test that compaction should execute silently when there is no entry logs
     * to compact. {@see https://issues.apache.org/jira/browse/BOOKKEEPER-700}
     */
    @Test
    public void testWhenNoLogsToCompact() throws Exception {
        tearDown(); // I dont want the test infrastructure
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        File tmpDir = tmpDirs.createNew("bkTest", ".dir");
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });

        LedgerDirsManager dirs = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        final Set<Long> ledgers = Collections
                .newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        LedgerManager manager = getLedgerManager(ledgers);
        CheckpointSource checkpointSource = new CheckpointSource() {

            @Override
            public Checkpoint newCheckpoint() {
                return null;
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint,
                    boolean compact) throws IOException {
            }
        };
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
        storage.initialize(
            conf,
            manager,
            dirs,
            dirs,
            NullStatsLogger.INSTANCE,
            UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(Checkpointer.NULL);

        double threshold = 0.1;
        long limit = 0;

        // shouldn't throw exception
        storage.gcThread.doCompactEntryLogs(threshold, limit);
    }

    /**
     * Test extractMetaFromEntryLogs optimized method to avoid excess memory usage.
     */
    public void testExtractMetaFromEntryLogs() throws Exception {
        // restart bookies
        restartBookies(c -> {
                // Always run this test with Throttle enabled.
                c.setIsThrottleByBytes(true);
                return c;
            });
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        File tmpDir = tmpDirs.createNew("bkTest", ".dir");
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });

        LedgerDirsManager dirs = new LedgerDirsManager(conf, conf.getLedgerDirs(),
            new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        final Set<Long> ledgers = Collections
            .newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

        LedgerManager manager = getLedgerManager(ledgers);

        CheckpointSource checkpointSource = new CheckpointSource() {

            @Override
            public Checkpoint newCheckpoint() {
                return null;
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint,
                                           boolean compact) throws IOException {
            }
        };
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
        storage.initialize(conf, manager, dirs, dirs,
                           NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(Checkpointer.NULL);


        for (long ledger = 0; ledger <= 10; ledger++) {
            ledgers.add(ledger);
            for (int entry = 1; entry <= 50; entry++) {
                try {
                    storage.addEntry(genEntry(ledger, entry, ENTRY_SIZE));
                } catch (IOException e) {
                    //ignore exception on failure to add entry.
                }
            }
        }

        storage.flush();
        storage.shutdown();

        storage = new InterleavedLedgerStorage();
        storage.initialize(conf, manager, dirs, dirs, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(Checkpointer.NULL);

        long startingEntriesCount = storage.gcThread.entryLogger.getLeastUnflushedLogId()
            - storage.gcThread.scannedLogId;
        LOG.info("The old Log Entry count is: " + startingEntriesCount);

        Map<Long, EntryLogMetadata> entryLogMetaData = new HashMap<>();
        long finalEntriesCount = storage.gcThread.entryLogger.getLeastUnflushedLogId()
            - storage.gcThread.scannedLogId;
        LOG.info("The latest Log Entry count is: " + finalEntriesCount);

        assertTrue("The GC did not clean up entries...", startingEntriesCount != finalEntriesCount);
        assertTrue("Entries Count is zero", finalEntriesCount == 0);
    }

    private ByteBuf genEntry(long ledger, long entry, int size) {
        ByteBuf bb = Unpooled.buffer(size);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        while (bb.isWritable()) {
            bb.writeByte((byte) 0xFF);
        }
        return bb;
    }

    /**
     * Suspend garbage collection when suspendMajor/suspendMinor is set.
     */
    @Test
    public void testSuspendGarbageCollection() throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        conf.setGcWaitTime(500);
        conf.setMinorCompactionInterval(1);
        conf.setMajorCompactionInterval(2);
        conf.setMajorCompactionMaxTimeMillis(5000);
        runFunctionWithLedgerManagerFactory(conf, lmf -> {
            try (LedgerManager lm = lmf.newLedgerManager()) {
                testSuspendGarbageCollection(conf, lm);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
            return null;
        });
    }

    private void testSuspendGarbageCollection(ServerConfiguration conf,
                                              LedgerManager lm) throws Exception {
        LedgerDirsManager dirManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        CheckpointSource cp = new CheckpointSource() {
            @Override
            public Checkpoint newCheckpoint() {
                // Do nothing.
                return null;
            }

            @Override
            public void checkpointComplete(Checkpoint checkPoint, boolean compact)
                throws IOException {
                // Do nothing.
            }
        };
        for (File journalDir : conf.getJournalDirs()) {
            BookieImpl.checkDirectoryStructure(journalDir);
        }
        for (File dir : dirManager.getAllLedgerDirs()) {
            BookieImpl.checkDirectoryStructure(dir);
        }
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
        TestStatsProvider stats = new TestStatsProvider();
        storage.initialize(
            conf,
            lm,
            dirManager,
            dirManager,
            stats.getStatsLogger("storage"),
            UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(cp);
        storage.setCheckpointer(Checkpointer.NULL);

        storage.start();

        int majorCompactions = stats.getCounter("storage.gc." + MAJOR_COMPACTION_COUNT).get().intValue();
        int minorCompactions = stats.getCounter("storage.gc." + MINOR_COMPACTION_COUNT).get().intValue();
        Thread.sleep(3 * (conf.getMajorCompactionInterval() * 1000
                + conf.getGcWaitTime()
                + conf.getMajorCompactionMaxTimeMillis()));
        assertTrue(
                "Major compaction should have happened",
                stats.getCounter("storage.gc." + MAJOR_COMPACTION_COUNT).get() > majorCompactions);

        // test suspend Major GC.
        storage.gcThread.suspendMajorGC();

        Thread.sleep(1000);
        long startTime = System.currentTimeMillis();
        majorCompactions = stats.getCounter("storage.gc." + MAJOR_COMPACTION_COUNT).get().intValue();
        Thread.sleep(conf.getMajorCompactionInterval() * 1000
                   + conf.getGcWaitTime());
        assertTrue("major compaction triggered while suspended",
                storage.gcThread.lastMajorCompactionTime < startTime);
        assertTrue("major compaction triggered while suspended",
                stats.getCounter("storage.gc." + MAJOR_COMPACTION_COUNT).get() == majorCompactions);

        // test suspend Major GC.
        Thread.sleep(conf.getMinorCompactionInterval() * 1000
                + conf.getGcWaitTime());
        assertTrue(
                "Minor compaction should have happened",
                stats.getCounter("storage.gc." + MINOR_COMPACTION_COUNT).get() > minorCompactions);

        // test suspend Minor GC.
        storage.gcThread.suspendMinorGC();

        Thread.sleep(1000);
        startTime = System.currentTimeMillis();
        minorCompactions = stats.getCounter("storage.gc." + MINOR_COMPACTION_COUNT).get().intValue();
        Thread.sleep(conf.getMajorCompactionInterval() * 1000
                   + conf.getGcWaitTime());
        assertTrue("minor compaction triggered while suspended",
                storage.gcThread.lastMinorCompactionTime < startTime);
        assertTrue("minor compaction triggered while suspended",
                stats.getCounter("storage.gc." + MINOR_COMPACTION_COUNT).get() == minorCompactions);

        // test resume
        storage.gcThread.resumeMinorGC();
        storage.gcThread.resumeMajorGC();

        Thread.sleep((conf.getMajorCompactionInterval() + conf.getMinorCompactionInterval()) * 1000
                + (conf.getGcWaitTime() * 2));
        assertTrue(
                "Major compaction should have happened",
                stats.getCounter("storage.gc." + MAJOR_COMPACTION_COUNT).get() > majorCompactions);
        assertTrue(
                "Minor compaction should have happened",
                stats.getCounter("storage.gc." + MINOR_COMPACTION_COUNT).get() > minorCompactions);
        assertTrue(
                "gcThreadRunttime should be non-zero",
                stats.getOpStatsLogger("storage.gc." + THREAD_RUNTIME).getSuccessCount() > 0);

    }

    @Test
    public void testRecoverIndexWhenIndexIsPartiallyFlush() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // restart bookies
        restartBookies(c -> {
                // disable compaction
                c.setMinorCompactionThreshold(0.0f);
                c.setMajorCompactionThreshold(0.0f);
                c.setGcWaitTime(600000);
                return c;
            });



        BookieImpl bookie = ((BookieImpl) serverByIndex(0).getBookie());
        InterleavedLedgerStorage storage = (InterleavedLedgerStorage) bookie.ledgerStorage;

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");

        MockTransactionalEntryLogCompactor partialCompactionWorker = new MockTransactionalEntryLogCompactor(
                ((InterleavedLedgerStorage) bookie.getLedgerStorage()).gcThread);

        for (long logId = 0; logId < 3; logId++) {
            EntryLogMetadata meta = storage.entryLogger.getEntryLogMetadata(logId);
            partialCompactionWorker.compactWithIndexFlushFailure(meta);
        }

        // entry logs ([0,1,2].log) should not be compacted because of partial flush throw IOException
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertTrue("Entry log file ([0,1,2].log should not be compacted in ledgerDirectory: "
                + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // entries should be available
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());

        // But we should see .compacted file with index flush failure
        assertEquals(findCompactedEntryLogFiles().size(), 3);

        // Now try to recover those flush failed index files
        partialCompactionWorker.cleanUpAndRecover();

        // There should be no .compacted files after recovery
        assertEquals(findCompactedEntryLogFiles().size(), 0);

        // compaction worker should recover partial flushed index and delete [0,1,2].log
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertFalse("Entry log file ([0,1,2].log should have been compacted in ledgerDirectory: "
                + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());
    }

    @Test
    public void testCompactionFailureShouldNotResultInDuplicatedData() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(5, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // restart bookies
        restartBookies(c -> {
                // disable compaction
                c.setMinorCompactionThreshold(0.0f);
                c.setMajorCompactionThreshold(0.0f);
                c.setUseTransactionalCompaction(true);
                return c;
            });

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
            + baseConf.getGcWaitTime());
        BookieImpl bookie = (BookieImpl) serverByIndex(0).getBookie();
        InterleavedLedgerStorage storage = (InterleavedLedgerStorage) bookie.ledgerStorage;

        List<File> ledgerDirs = bookie.getLedgerDirsManager().getAllLedgerDirs();
        List<Long> usageBeforeCompaction = new ArrayList<>();
        ledgerDirs.forEach(file -> usageBeforeCompaction.add(getDirectorySpaceUsage(file)));

        MockTransactionalEntryLogCompactor partialCompactionWorker = new MockTransactionalEntryLogCompactor(
            ((InterleavedLedgerStorage) bookie.ledgerStorage).gcThread);

        for (long logId = 0; logId < 5; logId++) {
            EntryLogMetadata meta = storage.entryLogger.getEntryLogMetadata(logId);
            partialCompactionWorker.compactWithLogFlushFailure(meta);
        }

        // entry logs ([0-4].log) should not be compacted because of failure in flush compaction log
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertTrue("Entry log file ([0,1,2].log should not be compacted in ledgerDirectory: "
                + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2, 3, 4));
        }
        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());

        List<Long> freeSpaceAfterCompactionFailed = new ArrayList<>();
        ledgerDirs.forEach(file -> freeSpaceAfterCompactionFailed.add(getDirectorySpaceUsage(file)));

        // No extra data is generated after compaction fail
        for (int i = 0; i < usageBeforeCompaction.size(); i++) {
            assertEquals(usageBeforeCompaction.get(i), freeSpaceAfterCompactionFailed.get(i));
        }


        // restart bookies
        restartBookies(c -> {
                // now enable normal compaction
                c.setMajorCompactionThreshold(0.5f);
                c.setMajorCompactionMaxTimeMillis(5000);
                return c;
            });

        getGCThread().enableForceGC();
        getGCThread().triggerGC().get();

        Thread.sleep(confByIndex(0).getMajorCompactionInterval() * 1000
                + confByIndex(0).getGcWaitTime()
                + confByIndex(0).getMajorCompactionMaxTimeMillis());
        // compaction worker should compact [0-4].log
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertFalse("Entry log file ([0,1,2].log should have been compacted in ledgerDirectory: "
                + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2, 3, 4));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());
    }

    private long getDirectorySpaceUsage(File dir) {
        long size = 0;
        for (File file : dir.listFiles()) {
            size += file.length();
        }
        return size;
    }

    private Set<File> findCompactedEntryLogFiles() throws Exception {
        Set<File> compactedLogFiles = new HashSet<>();
        for (File ledgerDirectory : bookieLedgerDirs()) {
            File[] files = BookieImpl.getCurrentDirectory(ledgerDirectory).listFiles(
                file -> file.getName().endsWith(COMPACTED_SUFFIX));
            if (files != null) {
                Collections.addAll(compactedLogFiles, files);
            }
        }
        return compactedLogFiles;
    }

    private static class MockTransactionalEntryLogCompactor extends TransactionalEntryLogCompactor {

        public MockTransactionalEntryLogCompactor(GarbageCollectorThread gcThread) {
            super(gcThread.conf,
                  gcThread.entryLogger,
                  gcThread.ledgerStorage,
                    (long entry) -> {
                        try {
                            gcThread.removeEntryLog(entry);
                        } catch (EntryLogMetadataMapException e) {
                            LOG.warn("Failed to remove entry-log metadata {}", entry, e);
                        }
                    });
        }

        synchronized void compactWithIndexFlushFailure(EntryLogMetadata metadata) {
            LOG.info("Compacting entry log {}.", metadata.getEntryLogId());
            CompactionPhase scanEntryLog = new ScanEntryLogPhase(metadata);
            if (!scanEntryLog.run()) {
                LOG.info("Compaction for {} end in ScanEntryLogPhase.", metadata.getEntryLogId());
                return;
            }
            File compactionLogFile = entryLogger.getCurCompactionLogFile();
            CompactionPhase flushCompactionLog = new FlushCompactionLogPhase(metadata.getEntryLogId());
            if (!flushCompactionLog.run()) {
                LOG.info("Compaction for {} end in FlushCompactionLogPhase.", metadata.getEntryLogId());
                return;
            }
            File compactedLogFile = getCompactedLogFile(compactionLogFile, metadata.getEntryLogId());
            CompactionPhase partialFlushIndexPhase = new PartialFlushIndexPhase(compactedLogFile);
            if (!partialFlushIndexPhase.run()) {
                LOG.info("Compaction for {} end in PartialFlushIndexPhase.", metadata.getEntryLogId());
                return;
            }
            logRemovalListener.removeEntryLog(metadata.getEntryLogId());
            LOG.info("Compacted entry log : {}.", metadata.getEntryLogId());
        }

        synchronized void compactWithLogFlushFailure(EntryLogMetadata metadata) {
            LOG.info("Compacting entry log {}", metadata.getEntryLogId());
            CompactionPhase scanEntryLog = new ScanEntryLogPhase(metadata);
            if (!scanEntryLog.run()) {
                LOG.info("Compaction for {} end in ScanEntryLogPhase.", metadata.getEntryLogId());
                return;
            }
            File compactionLogFile = entryLogger.getCurCompactionLogFile();
            CompactionPhase logFlushFailurePhase = new LogFlushFailurePhase(metadata.getEntryLogId());
            if (!logFlushFailurePhase.run()) {
                LOG.info("Compaction for {} end in FlushCompactionLogPhase.", metadata.getEntryLogId());
                return;
            }
            File compactedLogFile = getCompactedLogFile(compactionLogFile, metadata.getEntryLogId());
            CompactionPhase updateIndex = new UpdateIndexPhase(compactedLogFile);
            if (!updateIndex.run()) {
                LOG.info("Compaction for entry log {} end in UpdateIndexPhase.", metadata.getEntryLogId());
                return;
            }
            logRemovalListener.removeEntryLog(metadata.getEntryLogId());
            LOG.info("Compacted entry log : {}.", metadata.getEntryLogId());
        }

        private class PartialFlushIndexPhase extends UpdateIndexPhase {

            public PartialFlushIndexPhase(File compactedLogFile) {
                super(compactedLogFile);
            }

            @Override
            void start() throws IOException {
                if (compactedLogFile != null && compactedLogFile.exists()) {
                    File dir = compactedLogFile.getParentFile();
                    String compactedFilename = compactedLogFile.getName();
                    // create a hard link "x.log" for file "x.log.y.compacted"
                    this.newEntryLogFile = new File(dir, compactedFilename.substring(0,
                                compactedFilename.indexOf(".log") + 4));
                    File hardlinkFile = new File(dir, newEntryLogFile.getName());
                    if (!hardlinkFile.exists()) {
                        HardLink.createHardLink(compactedLogFile, hardlinkFile);
                    }
                    assertTrue(offsets.size() > 1);
                    // only flush index for one entry location
                    EntryLocation el = offsets.get(0);
                    ledgerStorage.updateEntriesLocations(offsets);
                    ledgerStorage.flushEntriesLocationsIndex();
                    throw new IOException("Flush ledger index encounter exception");
                }
            }
        }

        private class LogFlushFailurePhase extends FlushCompactionLogPhase {

            LogFlushFailurePhase(long compactingLogId) {
                super(compactingLogId);
            }

            @Override
            void start() throws IOException {
                // flush the current compaction log
                entryLogger.flushCompactionLog();
                throw new IOException("Encounter IOException when trying to flush compaction log");
            }
        }
    }

}