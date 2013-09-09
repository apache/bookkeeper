package org.apache.bookkeeper.bookie;

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
import java.io.File;
import java.io.IOException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.TestUtils;

import org.apache.zookeeper.AsyncCallback;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.Version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the entry log compaction functionality.
 */
public class CompactionTest extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(CompactionTest.class);
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

    public CompactionTest() {
        super(NUM_BOOKIES);

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
        baseConf.setGcWaitTime(gcWaitTime);
        baseConf.setMinorCompactionThreshold(minorCompactionThreshold);
        baseConf.setMajorCompactionThreshold(majorCompactionThreshold);
        baseConf.setMinorCompactionInterval(minorCompactionInterval);
        baseConf.setMajorCompactionInterval(majorCompactionInterval);

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
        ServerConfiguration conf = new ServerConfiguration();
        final Set<Long> ledgers = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        LedgerManager manager = new LedgerManager() {
                @Override
                public void createLedger(LedgerMetadata metadata, GenericCallback<Long> cb) {
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

        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
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

        final byte[] KEY = "foobar".getBytes();
        File log0 = new File(curDir, "0.log");
        LedgerDirsManager dirs = new LedgerDirsManager(conf);
        assertFalse("Log shouldnt exist", log0.exists());
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage(conf, manager, dirs);
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

        storage = new InterleavedLedgerStorage(conf, manager, dirs);
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

        storage = new InterleavedLedgerStorage(conf, manager, dirs);
        storage.getEntry(1, 1); // entry should exist
    }

    private ByteBuffer genEntry(long ledger, long entry, int size) {
        byte[] data = new byte[size];
        ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
        bb.putLong(ledger);
        bb.putLong(entry);
        while (bb.hasRemaining()) {
            bb.put((byte)0xFF);
        }
        bb.flip();
        return bb;
    }
}
