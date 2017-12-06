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

import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTED_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.HardLink;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.TestUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the entry log compaction functionality.
 */
public abstract class CompactionTest extends BookKeeperClusterTestCase {

    private final static Logger LOG = LoggerFactory.getLogger(CompactionTest.class);

    static int ENTRY_SIZE = 1024;
    static int NUM_BOOKIES = 1;

    private final boolean isThrottleByBytes;
    private final DigestType digestType;
    private final int numEntries;
    private final int gcWaitTime;
    private final double minorCompactionThreshold;
    private final double majorCompactionThreshold;
    private final long minorCompactionInterval;
    private final long majorCompactionInterval;
    private final String msg;

    public CompactionTest(boolean isByBytes) {
        super(NUM_BOOKIES);

        this.isThrottleByBytes = isByBytes;
        this.digestType = DigestType.CRC32;

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

        super.setUp();
    }

    private GarbageCollectorThread getGCThread() {
        assertEquals(1, bs.size());
        BookieServer server = bs.get(0);
        return ((InterleavedLedgerStorage) server.getBookie().ledgerStorage).gcThread;
    }

    LedgerHandle[] prepareData(int numEntryLogs, boolean changeNum)
        throws Exception {
        // since an entry log file can hold at most 100 entries
        // first ledger write 2 entries, which is less than low water mark
        int num1 = 2;
        // third ledger write more than high water mark entries
        int num3 = (int)(numEntries * 0.7f);
        // second ledger write remaining entries, which is higher than low water mark
        // and less than high water mark
        int num2 = numEntries - num3 - num1;

        LedgerHandle[] lhs = new LedgerHandle[3];
        for (int i=0; i<3; ++i) {
            lhs[i] = bkc.createLedger(NUM_BOOKIES, NUM_BOOKIES, digestType, "".getBytes());
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
        LedgerHandle lh = bkc.openLedger(lid, digestType, "".getBytes());
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
        baseConf.setMinorCompactionThreshold(0.0f);
        baseConf.setMajorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies(baseConf);

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

        // after garbage collection, compaction should not be executed
        assertEquals(lastMinorCompactionTime, getGCThread().lastMinorCompactionTime);
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);

        // entry logs ([0,1].log) should not be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([0,1].log that should have been compacted in ledgerDirectory: "
                            + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1));
        }
    }

    @Test
    public void testForceGarbageCollection() throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        conf.setGcWaitTime(60000);
        conf.setMinorCompactionInterval(120000);
        conf.setMajorCompactionInterval(240000);
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
            Bookie.checkDirectoryStructure(journalDir);
        }
        for (File dir : dirManager.getAllLedgerDirs()) {
            Bookie.checkDirectoryStructure(dir);
        }
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
        storage.initialize(
            conf,
            LedgerManagerFactory.newLedgerManagerFactory(conf, zkc).newLedgerManager(),
            dirManager,
            dirManager,
            cp,
            Checkpointer.NULL,
            NullStatsLogger.INSTANCE);
        storage.start();
        long startTime = MathUtils.now();
        storage.gcThread.enableForceGC();
        storage.gcThread.triggerGC().get(); //major
        storage.gcThread.triggerGC().get(); //minor
        // Minor and Major compaction times should be larger than when we started
        // this test.
        assertTrue("Minor or major compaction did not trigger even on forcing.",
                storage.gcThread.lastMajorCompactionTime > startTime &&
                storage.gcThread.lastMinorCompactionTime > startTime);
        storage.shutdown();
    }

    @Test
    public void testMinorCompaction() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable major compaction
        baseConf.setMajorCompactionThreshold(0.0f);
        baseConf.setGcWaitTime(60000);
        baseConf.setMinorCompactionInterval(120000);
        baseConf.setMajorCompactionInterval(240000);

        // restart bookies
        restartBookies(baseConf);

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
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                            + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());
    }

    @Test
    public void testMinorCompactionWithNoWritableLedgerDirs() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable major compaction
        baseConf.setMajorCompactionThreshold(0.0f);
        baseConf.setGcWaitTime(60000);
        baseConf.setMinorCompactionInterval(120000);
        baseConf.setMajorCompactionInterval(240000);

        // restart bookies
        restartBookies(baseConf);

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);

        for (BookieServer bookieServer : bs) {
            Bookie bookie = bookieServer.getBookie();
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

        // after garbage collection, major compaction should not be executed
        assertEquals(lastMajorCompactionTime, getGCThread().lastMajorCompactionTime);
        assertEquals(lastMinorCompactionTime, getGCThread().lastMinorCompactionTime);

        // entry logs ([0,1,2].log) should still remain, because both major and Minor compaction are disabled.
        for (File ledgerDirectory : tmpDirs) {
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
        File ledgerDir1 = createTempDir("ledger", "test1");
        File ledgerDir2 = createTempDir("ledger","test2");
        File journalDir = createTempDir("journal","test");
        String[] ledgerDirNames = new String[]{
            ledgerDir1.getPath(),
            ledgerDir2.getPath()
        };
        conf.setLedgerDirNames(ledgerDirNames);
        conf.setJournalDirName(journalDir.getPath());
        BookieServer server = startBookie(conf);
        bs.add(server);
        bsConfs.add(conf);
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        long lastMinorCompactionTime = getGCThread().lastMinorCompactionTime;
        long lastMajorCompactionTime = getGCThread().lastMajorCompactionTime;
        assertFalse(getGCThread().enableMajorCompaction);
        assertTrue(getGCThread().enableMinorCompaction);

        for (BookieServer bookieServer : bs) {
            Bookie bookie = bookieServer.getBookie();
            bookie.ledgerStorage.flush();
            bookie.ledgerMonitor.shutdown();
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
        for (File ledgerDirectory : server.getBookie().getLedgerDirsManager().getAllLedgerDirs()) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory.getParentFile(), true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());

        // for the sake of validity of test lets make sure that there is no writableLedgerDir in the bookies
        for (BookieServer bookieServer : bs) {
            Bookie bookie = bookieServer.getBookie();
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

        // disable minor compaction
        baseConf.setMinorCompactionThreshold(0.0f);
        baseConf.setGcWaitTime(60000);
        baseConf.setMinorCompactionInterval(120000);
        baseConf.setMajorCompactionInterval(240000);

        // restart bookies
        restartBookies(baseConf);

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
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                      + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger2
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[1].getId(), 0, lhs[1].getLastAddConfirmed());
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
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([1,2].log that should have been compacted in ledgerDirectory: "
                     + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1, 2));
        }
    }

    @Test
    public void testCompactionSmallEntryLogs() throws Exception {

        // create a ledger to write a few entries
        LedgerHandle alh = bkc.createLedger(NUM_BOOKIES, NUM_BOOKIES, digestType, "".getBytes());
        for (int i=0; i<3; i++) {
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

        // entry logs (0.log) should not be compacted
        // entry logs ([1,2,3].log) should be compacted.
        for (File ledgerDirectory : tmpDirs) {
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

        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});

        conf.setEntryLogSizeLimit(EntryLogger.LOGFILE_HEADER_SIZE + 3 * (4+ENTRY_SIZE));
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
                        return id - ((MyCheckpoint)o).id;
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
        final byte[] KEY = "foobar".getBytes();
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
            checkpointSource,
            Checkpointer.NULL,
            NullStatsLogger.INSTANCE);
        ledgers.add(1L);
        ledgers.add(2L);
        ledgers.add(3L);
        storage.setMasterKey(1, KEY);
        storage.setMasterKey(2, KEY);
        storage.setMasterKey(3, KEY);
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
            checkpointSource,
            Checkpointer.NULL,
            NullStatsLogger.INSTANCE);
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
        storage.setMasterKey(4, KEY);
        storage.addEntry(genEntry(4, 1, ENTRY_SIZE)); // force ledger 1 page to flush

        storage = new InterleavedLedgerStorage();
        storage.initialize(
            conf,
            manager,
            dirs,
            dirs,
            checkpointSource,
            Checkpointer.NULL,
            NullStatsLogger.INSTANCE);
        storage.getEntry(1, 1); // entry should exist
    }

    private LedgerManager getLedgerManager(final Set<Long> ledgers) {
        LedgerManager manager = new LedgerManager() {
                @Override
                public void createLedgerMetadata(long lid, LedgerMetadata metadata, GenericCallback<Void> cb) {
                    unsupported();
                }
                @Override
                public void removeLedgerMetadata(long ledgerId, Version version,
                                                 GenericCallback<Void> vb) {
                    unsupported();
                }
                @Override
                public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> readCb) {
                    unsupported();
                }
                @Override
                public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                        GenericCallback<Void> cb) {
                    unsupported();
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
                public LedgerRangeIterator getLedgerRanges() {
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
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);
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
            checkpointSource,
            Checkpointer.NULL,
            NullStatsLogger.INSTANCE);

        double threshold = 0.1;
        // shouldn't throw exception
        storage.gcThread.doCompactEntryLogs(threshold);
    }

    private ByteBuf genEntry(long ledger, long entry, int size) {
        ByteBuf bb = Unpooled.buffer(size);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        while (bb.isWritable()) {
            bb.writeByte((byte)0xFF);
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
            Bookie.checkDirectoryStructure(journalDir);
        }
        for (File dir : dirManager.getAllLedgerDirs()) {
            Bookie.checkDirectoryStructure(dir);
        }
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
        storage.initialize(
            conf,
            LedgerManagerFactory.newLedgerManagerFactory(conf, zkc).newLedgerManager(),
            dirManager,
            dirManager,
            cp,
            Checkpointer.NULL,
            NullStatsLogger.INSTANCE);
        storage.start();

        // test suspend Major GC.
        Thread.sleep(conf.getMajorCompactionInterval() * 1000
                   + conf.getGcWaitTime());
        storage.gcThread.suspendMajorGC();
        Thread.sleep(1000);
        long startTime = MathUtils.now();
        Thread.sleep(conf.getMajorCompactionInterval() * 1000
                   + conf.getGcWaitTime());
        assertTrue("major compaction triggered while set suspend",
                storage.gcThread.lastMajorCompactionTime < startTime);

        // test suspend Minor GC.
        storage.gcThread.suspendMinorGC();
        Thread.sleep(1000);
        startTime = MathUtils.now();
        Thread.sleep(conf.getMajorCompactionInterval() * 1000
                   + conf.getGcWaitTime());
        assertTrue("minor compaction triggered while set suspend",
                storage.gcThread.lastMinorCompactionTime < startTime);
        storage.gcThread.resumeMinorGC();
        storage.gcThread.resumeMajorGC();
    }

    @Test
    public void testRecoverIndexWhenIndexIsPartiallyFlush() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable compaction
        baseConf.setMinorCompactionThreshold(0.0f);
        baseConf.setMajorCompactionThreshold(0.0f);
        baseConf.setGcWaitTime(600000);

        // restart bookies
        restartBookies(baseConf);

        Bookie bookie = bs.get(0).getBookie();
        InterleavedLedgerStorage storage = (InterleavedLedgerStorage) bookie.ledgerStorage;

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");

        MockTransactionalEntryLogCompactor partialCompactionWorker = new MockTransactionalEntryLogCompactor(
            ((InterleavedLedgerStorage) bookie.ledgerStorage).gcThread);

        for (long logId = 0; logId < 3; logId++) {
            EntryLogMetadata meta = storage.entryLogger.getEntryLogMetadata(logId);
            partialCompactionWorker.compactWithIndexFlushFailure(meta);
        }

        // entry logs ([0,1,2].log) should not be compacted because of partial flush throw IOException
        for (File ledgerDirectory : tmpDirs) {
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
        for (File ledgerDirectory : tmpDirs) {
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

        // disable compaction
        baseConf.setMinorCompactionThreshold(0.0f);
        baseConf.setMajorCompactionThreshold(0.0f);
        baseConf.setUseTransactionalCompaction(true);

        // restart bookies
        restartBookies(baseConf);

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
            + baseConf.getGcWaitTime());
        Bookie bookie = bs.get(0).getBookie();
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
        for (File ledgerDirectory : tmpDirs) {
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

        // now enable normal compaction
        baseConf.setMajorCompactionThreshold(0.5f);

        // restart bookies
        restartBookies(baseConf);

        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
            + baseConf.getGcWaitTime());
        // compaction worker should compact [0-4].log
        for (File ledgerDirectory : tmpDirs) {
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

    private Set<File> findCompactedEntryLogFiles() {
        Set<File> compactedLogFiles = new HashSet<>();
        for (File ledgerDirectory : tmpDirs) {
            File[] files = Bookie.getCurrentDirectory(ledgerDirectory).listFiles(
                file -> file.getName().endsWith(COMPACTED_SUFFIX));
            if (files != null) {
                for (File file : files) {
                    compactedLogFiles.add(file);
                }
            }
        }
        return compactedLogFiles;
    }

    private static class MockTransactionalEntryLogCompactor extends TransactionalEntryLogCompactor {

        public MockTransactionalEntryLogCompactor(GarbageCollectorThread gcThread) {
            super(gcThread);
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
            gcThread.removeEntryLog(metadata.getEntryLogId());
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
            gcThread.removeEntryLog(metadata.getEntryLogId());
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
                    this.newEntryLogFile = new File(dir, compactedFilename.substring(0, compactedFilename.indexOf(".log") + 4));
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
