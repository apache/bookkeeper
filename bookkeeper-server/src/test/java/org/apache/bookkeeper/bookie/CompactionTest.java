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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactionScannerFactory;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.client.BKException;
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
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.TestUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
/**
 * This class tests the entry log compaction functionality.
 */
@RunWith(Parameterized.class)
public class CompactionTest extends BookKeeperClusterTestCase {
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{true}, {false}});
    }

    private boolean isThrottleByBytes;

    private final static Logger LOG = LoggerFactory.getLogger(CompactionTest.class);
    DigestType digestType;

    static int ENTRY_SIZE = 1024;
    static int NUM_BOOKIES = 1;

    int numEntries;
    int gcWaitTime;
    double minorCompactionThreshold;
    double majorCompactionThreshold;
    long minorCompactionInterval;
    long majorCompactionInterval;

    String msg;

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

    @Test(timeout=60000)
    public void testDisableCompaction() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        // disable compaction
        baseConf.setMinorCompactionThreshold(0.0f);
        baseConf.setMajorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies(baseConf);

        // remove ledger2 and ledger3
        // so entry log 1 and 2 would have ledger1 entries left
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs ([0,1].log) should not be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([0,1].log that should have been compacted in ledgerDirectory: "
                            + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1));
        }
    }

    @Test(timeout=60000)
    public void testForceGarbageCollection() throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        conf.setGcWaitTime(60000);
        conf.setMinorCompactionInterval(120000);
        conf.setMajorCompactionInterval(240000);
        LedgerDirsManager dirManager = new LedgerDirsManager(conf, conf.getLedgerDirs());
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
        storage.initialize(conf,
                LedgerManagerFactory.newLedgerManagerFactory(conf, zkc).newLedgerManager(),
                dirManager, dirManager, cp, NullStatsLogger.INSTANCE);
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

    @Test(timeout=60000)
    public void testMinorCompaction() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable major compaction
        baseConf.setMajorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies(baseConf);

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        Thread.sleep(baseConf.getMinorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs ([0,1,2].log) should be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                            + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());
    }

    @Test(timeout = 60000)
    public void testMinorCompactionWithNoWritableLedgerDirs() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable major compaction
        baseConf.setMajorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies(baseConf);

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
        Thread.sleep(baseConf.getMinorCompactionInterval() * 1000 + baseConf.getGcWaitTime());

        // entry logs ([0,1,2].log) should still remain, because both major and Minor compaction are disabled.
        for (File ledgerDirectory : tmpDirs) {
            assertTrue(
                    "All the entry log files ([0,1,2].log are not available, which is not expected" + ledgerDirectory,
                    TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1, 2));
        }
    }

    @Test(timeout = 60000)
    public void testMinorCompactionWithNoWritableLedgerDirsButIsForceGCAllowWhenNoSpaceIsSet() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable major compaction
        baseConf.setMajorCompactionThreshold(0.0f);

        // here we are setting isForceGCAllowWhenNoSpace to true, so Major and Minor compaction wont be disabled in case
        // when discs are full
        baseConf.setIsForceGCAllowWhenNoSpace(true);

        // restart bookies
        restartBookies(baseConf);

        for (BookieServer bookieServer : bs) {
            Bookie bookie = bookieServer.getBookie();
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
        Thread.sleep(baseConf.getMinorCompactionInterval() * 1000 + baseConf.getGcWaitTime() + 500);

        // though all discs are added to filled dirs list, compaction would succeed, because in EntryLogger for
        // allocating newlog
        // we get getWritableLedgerDirsForNewLog() of ledgerDirsManager instead of getWritableLedgerDirs()
        // entry logs ([0,1,2].log) should be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
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
    
    @Test(timeout=60000)
    public void testMajorCompaction() throws Exception {

        // prepare data
        LedgerHandle[] lhs = prepareData(3, true);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable minor compaction
        baseConf.setMinorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies(baseConf);

        // remove ledger1 and ledger3
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");

        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs ([0,1,2].log) should be compacted
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                      + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger2
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[1].getId(), 0, lhs[1].getLastAddConfirmed());
    }

    @Test(timeout=60000)
    public void testMajorCompactionAboveThreshold() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // remove ledger1 and ledger2
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[1].getId());
        LOG.info("Finished deleting the ledgers contains less entries.");
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs ([0,1,2].log) should not be compacted
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([1,2].log that should have been compacted in ledgerDirectory: "
                     + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1, 2));
        }
    }

    @Test(timeout=60000)
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
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

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
    @Test(timeout=60000)
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
        LedgerDirsManager dirs = new LedgerDirsManager(conf, conf.getLedgerDirs());
        assertFalse("Log shouldnt exist", log0.exists());
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
        storage.initialize(conf, manager, dirs, dirs, checkpointSource, NullStatsLogger.INSTANCE);
        ledgers.add(1l);
        ledgers.add(2l);
        ledgers.add(3l);
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
        ledgers.remove(2l);
        ledgers.remove(3l);

        storage = new InterleavedLedgerStorage();
        storage.initialize(conf, manager, dirs, dirs, checkpointSource, NullStatsLogger.INSTANCE);
        storage.start();
        for (int i = 0; i < 10; i++) {
            if (!log0.exists()) {
                break;
            }
            Thread.sleep(1000);
            storage.entryLogger.flush(); // simulate sync thread
        }
        assertFalse("Log shouldnt exist", log0.exists());

        ledgers.add(4l);
        storage.setMasterKey(4, KEY);
        storage.addEntry(genEntry(4, 1, ENTRY_SIZE)); // force ledger 1 page to flush

        storage = new InterleavedLedgerStorage();
        storage.initialize(conf, manager, dirs, dirs, checkpointSource, NullStatsLogger.INSTANCE);
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
    @Test(timeout = 60000)
    public void testWhenNoLogsToCompact() throws Exception {
        tearDown(); // I dont want the test infrastructure
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });

        LedgerDirsManager dirs = new LedgerDirsManager(conf, conf.getLedgerDirs());
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
        storage.initialize(conf, manager, dirs, dirs, checkpointSource, NullStatsLogger.INSTANCE);

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
    @Test(timeout=60000)
    public void testSuspendGarbageCollection() throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        conf.setGcWaitTime(500);
        conf.setMinorCompactionInterval(1);
        conf.setMajorCompactionInterval(2);
        LedgerDirsManager dirManager = new LedgerDirsManager(conf, conf.getLedgerDirs());
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
        storage.initialize(conf,
                LedgerManagerFactory.newLedgerManagerFactory(conf, zkc).newLedgerManager(),
                dirManager, dirManager, cp, NullStatsLogger.INSTANCE);
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

    @Test(timeout = 60000)
    public void testCompactionWithEntryLogRollover() throws Exception {
        // Disable bookie gc during this test
        baseConf.setGcWaitTime(60000);
        baseConf.setMinorCompactionInterval(0);
        baseConf.setMajorCompactionInterval(0);
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

        InterleavedLedgerStorage ledgerStorage = (InterleavedLedgerStorage) bs.get(0).getBookie().ledgerStorage;
        GarbageCollectorThread garbageCollectorThread = ledgerStorage.gcThread;
        CompactionScannerFactory compactionScannerFactory = garbageCollectorThread.scannerFactory;
        long entryLogId = 0;
        EntryLogger entryLogger = ledgerStorage.entryLogger;

        LOG.info("Before compaction -- Least unflushed log id: {}", entryLogger.getLeastUnflushedLogId());

        // Compact entryLog 0
        EntryLogScanner scanner = compactionScannerFactory.newScanner(entryLogger.getEntryLogMetadata(entryLogId));

        entryLogger.scanEntryLog(entryLogId, scanner);

        long entryLogIdAfterCompaction = entryLogger.getLeastUnflushedLogId();
        LOG.info("After compaction -- Least unflushed log id: {}", entryLogIdAfterCompaction);

        // Add more entries to trigger entrylog roll over
        LedgerHandle[] lhs2 = prepareData(3, false);

        for (LedgerHandle lh : lhs2) {
            lh.close();
        }

        // Wait for entry logger to move forward
        while (entryLogger.getLeastUnflushedLogId() <= entryLogIdAfterCompaction) {
            Thread.sleep(100);
        }

        long entryLogIdBeforeFlushing = entryLogger.getLeastUnflushedLogId();
        LOG.info("Added more data -- Least unflushed log id: {}", entryLogIdBeforeFlushing);

        Assert.assertTrue(entryLogIdAfterCompaction < entryLogIdBeforeFlushing);

        // Wait for entries to be flushed on entry logs and update index
        // This operation should succeed even if the entry log rolls over after the last entry was compacted
        compactionScannerFactory.flush();
    }
}
