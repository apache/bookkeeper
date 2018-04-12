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

import static org.apache.bookkeeper.bookie.BookieException.Code.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.FileInfoBackingCache.CachedFileInfo;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LedgerCache related test cases.
 */
public class LedgerCacheTest {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerCacheTest.class);

    SnapshotMap<Long, Boolean> activeLedgers;
    LedgerCache ledgerCache;
    Thread flushThread;
    ServerConfiguration conf;
    File txnDir, ledgerDir;

    private final List<File> tempDirs = new ArrayList<File>();

    private Bookie bookie;

    @Before
    public void setUp() throws Exception {
        txnDir = IOUtils.createTempDir("ledgercache", "txn");
        ledgerDir = IOUtils.createTempDir("ledgercache", "ledger");
        // create current dir
        new File(ledgerDir, BookKeeperConstants.CURRENT_DIR).mkdir();

        conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(null);
        conf.setJournalDirName(txnDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });
        bookie = new Bookie(conf);

        activeLedgers = new SnapshotMap<Long, Boolean>();
        ledgerCache = ((InterleavedLedgerStorage) bookie.ledgerStorage).ledgerCache;
    }

    @After
    public void tearDown() throws Exception {
        if (flushThread != null) {
            flushThread.interrupt();
            flushThread.join();
        }
        bookie.ledgerStorage.shutdown();
        FileUtils.deleteDirectory(txnDir);
        FileUtils.deleteDirectory(ledgerDir);
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    private void newLedgerCache() throws IOException {
        if (ledgerCache != null) {
            ledgerCache.close();
        }
        ledgerCache = ((InterleavedLedgerStorage) bookie.ledgerStorage).ledgerCache = new LedgerCacheImpl(
                conf, activeLedgers, bookie.getIndexDirsManager());
        flushThread = new Thread() {
                public void run() {
                    while (true) {
                        try {
                            sleep(conf.getFlushInterval());
                            ledgerCache.flushLedger(true);
                        } catch (InterruptedException ie) {
                            // killed by teardown
                            Thread.currentThread().interrupt();
                            return;
                        } catch (Exception e) {
                            LOG.error("Exception in flush thread", e);
                        }
                    }
                }
            };
        flushThread.start();
    }

    @Test
    public void testAddEntryException() throws IOException {
        // set page limitation
        conf.setPageLimit(10);
        // create a ledger cache
        newLedgerCache();
        /*
         * Populate ledger cache.
         */
        try {
            byte[] masterKey = "blah".getBytes();
            for (int i = 0; i < 100; i++) {
                ledgerCache.setMasterKey((long) i, masterKey);
                ledgerCache.putEntryOffset(i, 0, i * 8);
            }
        } catch (IOException e) {
            LOG.error("Got IOException.", e);
            fail("Failed to add entry.");
        }
    }

    @Test
    public void testLedgerEviction() throws Exception {
        int numEntries = 10;
        // limit open files & pages
        conf.setOpenFileLimit(1).setPageLimit(2)
            .setPageSize(8 * numEntries);
        // create ledger cache
        newLedgerCache();
        try {
            int numLedgers = 3;
            byte[] masterKey = "blah".getBytes();
            for (int i = 1; i <= numLedgers; i++) {
                ledgerCache.setMasterKey((long) i, masterKey);
                for (int j = 0; j < numEntries; j++) {
                    ledgerCache.putEntryOffset(i, j, i * numEntries + j);
                }
            }
        } catch (Exception e) {
            LOG.error("Got Exception.", e);
            fail("Failed to add entry.");
        }
    }

    @Test
    public void testDeleteLedger() throws Exception {
        int numEntries = 10;
        // limit open files & pages
        conf.setOpenFileLimit(999).setPageLimit(2)
            .setPageSize(8 * numEntries);
        // create ledger cache
        newLedgerCache();
        try {
            int numLedgers = 2;
            byte[] masterKey = "blah".getBytes();
            for (int i = 1; i <= numLedgers; i++) {
                ledgerCache.setMasterKey((long) i, masterKey);
                for (int j = 0; j < numEntries; j++) {
                    ledgerCache.putEntryOffset(i, j, i * numEntries + j);
                }
            }
            // ledger cache is exhausted
            // delete ledgers
            for (int i = 1; i <= numLedgers; i++) {
                ledgerCache.deleteLedger((long) i);
            }
            // create num ledgers to add entries
            for (int i = numLedgers + 1; i <= 2 * numLedgers; i++) {
                ledgerCache.setMasterKey((long) i, masterKey);
                for (int j = 0; j < numEntries; j++) {
                    ledgerCache.putEntryOffset(i, j, i * numEntries + j);
                }
            }
        } catch (Exception e) {
            LOG.error("Got Exception.", e);
            fail("Failed to add entry.");
        }
    }

    @Test
    public void testPageEviction() throws Exception {
        int numLedgers = 10;
        byte[] masterKey = "blah".getBytes();
        // limit page count
        conf.setOpenFileLimit(999999).setPageLimit(3);
        // create ledger cache
        newLedgerCache();
        try {
            // create serveral ledgers
            for (int i = 1; i <= numLedgers; i++) {
                ledgerCache.setMasterKey((long) i, masterKey);
                ledgerCache.putEntryOffset(i, 0, i * 8);
                ledgerCache.putEntryOffset(i, 1, i * 8);
            }

            // flush all first to clean previous dirty ledgers
            ledgerCache.flushLedger(true);
            // flush all
            ledgerCache.flushLedger(true);

            // delete serveral ledgers
            for (int i = 1; i <= numLedgers / 2; i++) {
                ledgerCache.deleteLedger(i);
            }

            // bookie restarts
            newLedgerCache();

            // simulate replaying journals to add entries again
            for (int i = 1; i <= numLedgers; i++) {
                try {
                    ledgerCache.putEntryOffset(i, 1, i * 8);
                } catch (NoLedgerException nsle) {
                    if (i <= numLedgers / 2) {
                        // it is ok
                    } else {
                        LOG.error("Error put entry offset : ", nsle);
                        fail("Should not reach here.");
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Got Exception.", e);
            fail("Failed to add entry.");
        }
    }

    /**
     * Test Ledger Cache flush failure.
     */
    @Test
    public void testLedgerCacheFlushFailureOnDiskFull() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File ledgerDir2 = createTempDir("bkTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath() });

        Bookie bookie = new Bookie(conf);
        InterleavedLedgerStorage ledgerStorage = ((InterleavedLedgerStorage) bookie.ledgerStorage);
        LedgerCacheImpl ledgerCache = (LedgerCacheImpl) ledgerStorage.ledgerCache;
        // Create ledger index file
        ledgerStorage.setMasterKey(1, "key".getBytes());

        CachedFileInfo fileInfo = ledgerCache.getIndexPersistenceManager().getFileInfo(Long.valueOf(1), null);

        // Add entries
        ledgerStorage.addEntry(generateEntry(1, 1));
        ledgerStorage.addEntry(generateEntry(1, 2));
        ledgerStorage.flush();

        ledgerStorage.addEntry(generateEntry(1, 3));
        // add the dir to failed dirs
        bookie.getIndexDirsManager().addToFilledDirs(
                fileInfo.getLf().getParentFile().getParentFile().getParentFile());
        File before = fileInfo.getLf();
        // flush after disk is added as failed.
        ledgerStorage.flush();
        File after = fileInfo.getLf();

        assertFalse("After flush index file should be changed", before.equals(after));
        // Verify written entries
        Assert.assertEquals(generateEntry(1, 1), ledgerStorage.getEntry(1, 1));
        Assert.assertEquals(generateEntry(1, 2), ledgerStorage.getEntry(1, 2));
        Assert.assertEquals(generateEntry(1, 3), ledgerStorage.getEntry(1, 3));
    }

    /**
     * Test that if we are writing to more ledgers than there
     * are pages, then we will not flush the index before the
     * entries in the entrylogger have been persisted to disk.
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-447}
     */
    @Test
    public void testIndexPageEvictionWriteOrder() throws Exception {
        final int numLedgers = 10;
        File journalDir = createTempDir("bookie", "journal");
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(ledgerDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(null);
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setFlushInterval(1000)
            .setPageLimit(1)
            .setLedgerStorageClass(InterleavedLedgerStorage.class.getName());

        Bookie b = new Bookie(conf);
        b.start();
        for (int i = 1; i <= numLedgers; i++) {
            ByteBuf packet = generateEntry(i, 1);
            b.addEntry(packet, false, new Bookie.NopWriteCallback(), null, "passwd".getBytes());
        }

        conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(null);
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() });

        b = new Bookie(conf);
        for (int i = 1; i <= numLedgers; i++) {
            try {
                b.readEntry(i, 1);
            } catch (Bookie.NoLedgerException nle) {
                // this is fine, means the ledger was never written to the index cache
                assertEquals("No ledger should only happen for the last ledger",
                             i, numLedgers);
            } catch (Bookie.NoEntryException nee) {
                // this is fine, means the ledger was written to the index cache, but not
                // the entry log
            } catch (IOException ioe) {
                LOG.info("Shouldn't have received IOException", ioe);
                fail("Shouldn't throw IOException, should say that entry is not found");
            }
        }
    }


    /**
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-524}
     * Checks that getLedgerEntryPage does not throw an NPE in the
     * case getFromTable returns a null ledger entry page reference.
     * This NPE might kill the sync thread leaving a bookie with no
     * sync thread running.
     *
     * @throws IOException
     */
    @Test
    public void testSyncThreadNPE() throws IOException {
        newLedgerCache();
        try {
            ((LedgerCacheImpl) ledgerCache).getIndexPageManager().getLedgerEntryPageFromCache(0L, 0L, true);
        } catch (Exception e) {
            LOG.error("Exception when trying to get a ledger entry page", e);
            fail("Shouldn't have thrown an exception");
        }
    }

    /**
     * Race where a flush would fail because a garbage collection occurred at
     * the wrong time.
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-604}
     */
    @Test
    public void testFlushDeleteRace() throws Exception {
        newLedgerCache();
        final AtomicInteger rc = new AtomicInteger(0);
        final LinkedBlockingQueue<Long> ledgerQ = new LinkedBlockingQueue<Long>(1);
        final byte[] masterKey = "masterKey".getBytes();
        Thread newLedgerThread = new Thread() {
                public void run() {
                    try {
                        for (int i = 0; i < 1000 && rc.get() == 0; i++) {
                            ledgerCache.setMasterKey(i, masterKey);
                            ledgerQ.put((long) i);
                        }
                    } catch (Exception e) {
                        rc.set(-1);
                        LOG.error("Exception in new ledger thread", e);
                    }
                }
            };
        newLedgerThread.start();

        Thread flushThread = new Thread() {
                public void run() {
                    try {
                        while (true) {
                            Long id = ledgerQ.peek();
                            if (id == null) {
                                continue;
                            }
                            LOG.info("Put entry for {}", id);
                            try {
                                ledgerCache.putEntryOffset((long) id, 1, 0);
                            } catch (Bookie.NoLedgerException nle) {
                                //ignore
                            }
                            ledgerCache.flushLedger(true);
                        }
                    } catch (Exception e) {
                        rc.set(-1);
                        LOG.error("Exception in flush thread", e);
                    }
                }
            };
        flushThread.start();

        Thread deleteThread = new Thread() {
                public void run() {
                    try {
                        while (true) {
                            long id = ledgerQ.take();
                            LOG.info("Deleting {}", id);
                            ledgerCache.deleteLedger(id);
                        }
                    } catch (Exception e) {
                        rc.set(-1);
                        LOG.error("Exception in delete thread", e);
                    }
                }
            };
        deleteThread.start();

        newLedgerThread.join();
        assertEquals("Should have been no errors", rc.get(), 0);

        deleteThread.interrupt();
        flushThread.interrupt();
    }

    // Mock SortedLedgerStorage to simulate flush failure (Dependency Fault Injection)
    static class FlushTestSortedLedgerStorage extends SortedLedgerStorage {
        final AtomicBoolean injectMemTableSizeLimitReached;
        final AtomicBoolean injectFlushException;

        public FlushTestSortedLedgerStorage() {
            super();
            injectMemTableSizeLimitReached = new AtomicBoolean();
            injectFlushException = new AtomicBoolean();
        }

        public void setInjectMemTableSizeLimitReached(boolean setValue) {
            injectMemTableSizeLimitReached.set(setValue);
        }

        public void setInjectFlushException(boolean setValue) {
            injectFlushException.set(setValue);
        }

        @Override
        public void initialize(ServerConfiguration conf,
                               LedgerManager ledgerManager,
                               LedgerDirsManager ledgerDirsManager,
                               LedgerDirsManager indexDirsManager,
                               StateManager stateManager,
                               CheckpointSource checkpointSource,
                               Checkpointer checkpointer,
                               StatsLogger statsLogger) throws IOException {
            super.initialize(
                conf,
                ledgerManager,
                ledgerDirsManager,
                indexDirsManager,
                stateManager,
                checkpointSource,
                checkpointer,
                statsLogger);
            this.memTable = new EntryMemTable(conf, checkpointSource, statsLogger) {
                @Override
                boolean isSizeLimitReached() {
                    return (injectMemTableSizeLimitReached.get() || super.isSizeLimitReached());
                }
            };
        }

        @Override
        public void process(long ledgerId, long entryId, ByteBuf buffer) throws IOException {
            if (injectFlushException.get()) {
                throw new IOException("Injected Exception");
            }
            super.process(ledgerId, entryId, buffer);
        }
        // simplified memTable full callback.
        @Override
        public void onSizeLimitReached(final CheckpointSource.Checkpoint cp) throws IOException {
            LOG.info("Reached size {}", cp);
            // use synchronous way
            try {
                LOG.info("Started flushing mem table.");
                memTable.flush(FlushTestSortedLedgerStorage.this);
            } catch (IOException e) {
                getStateManager().doTransitionToReadOnlyMode();
                LOG.error("Exception thrown while flushing skip list cache.", e);
         }
         }

    }

    @Test
    public void testEntryMemTableFlushFailure() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setLedgerStorageClass(FlushTestSortedLedgerStorage.class.getName());

        Bookie bookie = new Bookie(conf);
        FlushTestSortedLedgerStorage flushTestSortedLedgerStorage = (FlushTestSortedLedgerStorage) bookie.ledgerStorage;
        EntryMemTable memTable = flushTestSortedLedgerStorage.memTable;

        // this bookie.addEntry call is required. FileInfo for Ledger 1 would be created with this call.
        // without the fileinfo, 'flushTestSortedLedgerStorage.addEntry' calls will fail
        // because of BOOKKEEPER-965 change.
        bookie.addEntry(generateEntry(1, 1), false, new Bookie.NopWriteCallback(), null, "passwd".getBytes());

        flushTestSortedLedgerStorage.addEntry(generateEntry(1, 2));
        assertFalse("Bookie is expected to be in ReadWrite mode", bookie.isReadOnly());
        assertTrue("EntryMemTable SnapShot is expected to be empty", memTable.snapshot.isEmpty());

        // set flags, so that FlushTestSortedLedgerStorage simulates FlushFailure scenario
        flushTestSortedLedgerStorage.setInjectMemTableSizeLimitReached(true);
        flushTestSortedLedgerStorage.setInjectFlushException(true);
        flushTestSortedLedgerStorage.addEntry(generateEntry(1, 2));
        Thread.sleep(1000);

        // since we simulated sizeLimitReached, snapshot shouldn't be empty
        assertFalse("EntryMemTable SnapShot is not expected to be empty", memTable.snapshot.isEmpty());

        // set the flags to false, so flush will succeed this time
        flushTestSortedLedgerStorage.setInjectMemTableSizeLimitReached(false);
        flushTestSortedLedgerStorage.setInjectFlushException(false);

        flushTestSortedLedgerStorage.addEntry(generateEntry(1, 3));
        Thread.sleep(1000);
        // since we expect memtable flush to succeed, memtable snapshot should be empty
        assertTrue("EntryMemTable SnapShot is expected to be empty, because of successful flush",
                memTable.snapshot.isEmpty());
    }

    @Test
    public void testSortedLedgerFlushFailure() throws Exception {
        // most of the code is same to the testEntryMemTableFlushFailure
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime)
            .setLedgerDirNames(new String[] { tmpDir.toString() })
            .setJournalDirName(tmpDir.toString())
            .setLedgerStorageClass(FlushTestSortedLedgerStorage.class.getName());

        Bookie bookie = new Bookie(conf);
        bookie.start();
        FlushTestSortedLedgerStorage flushTestSortedLedgerStorage = (FlushTestSortedLedgerStorage) bookie.ledgerStorage;
        EntryMemTable memTable = flushTestSortedLedgerStorage.memTable;

        bookie.addEntry(generateEntry(1, 1), false, new Bookie.NopWriteCallback(), null, "passwd".getBytes());
        flushTestSortedLedgerStorage.addEntry(generateEntry(1, 2));
        assertFalse("Bookie is expected to be in ReadWrite mode", bookie.isReadOnly());
        assertTrue("EntryMemTable SnapShot is expected to be empty", memTable.snapshot.isEmpty());

        // set flags, so that FlushTestSortedLedgerStorage simulates FlushFailure scenario
        flushTestSortedLedgerStorage.setInjectMemTableSizeLimitReached(true);
        flushTestSortedLedgerStorage.setInjectFlushException(true);
        flushTestSortedLedgerStorage.addEntry(generateEntry(1, 2));

        // since we simulated sizeLimitReached, snapshot shouldn't be empty
        assertFalse("EntryMemTable SnapShot is not expected to be empty", memTable.snapshot.isEmpty());
        // after flush failure, the bookie is set to readOnly
        assertTrue("Bookie is expected to be in Read mode", bookie.isReadOnly());
        // write fail
        bookie.addEntry(generateEntry(1, 3), false, new BookkeeperInternalCallbacks.WriteCallback(){
            public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx){
                LOG.info("fail write to bk");
                assertTrue(rc != OK);
            };

        }, null, "passwd".getBytes());
        bookie.shutdown();

    }

    private ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }
}
