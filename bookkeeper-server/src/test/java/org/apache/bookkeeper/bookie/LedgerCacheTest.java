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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.SnapshotMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

/**
 * LedgerCache related test cases
 */
public class LedgerCacheTest extends TestCase {
    static Logger LOG = LoggerFactory.getLogger(LedgerCacheTest.class);

    SnapshotMap<Long, Boolean> activeLedgers;
    LedgerManagerFactory ledgerManagerFactory;
    LedgerCache ledgerCache;
    Thread flushThread;
    ServerConfiguration conf;
    File txnDir, ledgerDir;

    private Bookie bookie;

    @Override
    @Before
    public void setUp() throws Exception {
        txnDir = File.createTempFile("ledgercache", "txn");
        txnDir.delete();
        txnDir.mkdir();
        ledgerDir = File.createTempFile("ledgercache", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();
        // create current dir
        new File(ledgerDir, BookKeeperConstants.CURRENT_DIR).mkdir();

        conf = new ServerConfiguration();
        conf.setZkServers(null);
        conf.setJournalDirName(txnDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });
        bookie = new Bookie(conf);

        ledgerManagerFactory =
            LedgerManagerFactory.newLedgerManagerFactory(conf, null);
        activeLedgers = new SnapshotMap<Long, Boolean>();
        ledgerCache = ((InterleavedLedgerStorage) bookie.ledgerStorage).ledgerCache;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (flushThread != null) {
            flushThread.interrupt();
            flushThread.join();
        }
        bookie.ledgerStorage.shutdown();
        ledgerManagerFactory.uninitialize();
        FileUtils.deleteDirectory(txnDir);
        FileUtils.deleteDirectory(ledgerDir);
    }

    private void newLedgerCache() throws IOException {
        if (ledgerCache != null) {
            ledgerCache.close();
        }
        ledgerCache = ((InterleavedLedgerStorage) bookie.ledgerStorage).ledgerCache = new LedgerCacheImpl(
                conf, activeLedgers, bookie.getLedgerDirsManager());
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

    @Test(timeout=30000)
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
            for( int i = 0; i < 100; i++) {
                ledgerCache.setMasterKey((long)i, masterKey);
                ledgerCache.putEntryOffset(i, 0, i*8);
            }
        } catch (IOException e) {
            LOG.error("Got IOException.", e);
            fail("Failed to add entry.");
        }
    }

    @Test(timeout=30000)
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
            for (int i=1; i<=numLedgers; i++) {
                ledgerCache.setMasterKey((long)i, masterKey);
                for (int j=0; j<numEntries; j++) {
                    ledgerCache.putEntryOffset(i, j, i * numEntries + j);
                }
            }
        } catch (Exception e) {
            LOG.error("Got Exception.", e);
            fail("Failed to add entry.");
        }
    }

    @Test(timeout=30000)
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
            for (int i=1; i<=numLedgers; i++) {
                ledgerCache.setMasterKey((long)i, masterKey);
                for (int j=0; j<numEntries; j++) {
                    ledgerCache.putEntryOffset(i, j, i*numEntries + j);
                }
            }
            // ledger cache is exhausted
            // delete ledgers
            for (int i=1; i<=numLedgers; i++) {
                ledgerCache.deleteLedger((long)i);
            }
            // create num ledgers to add entries
            for (int i=numLedgers+1; i<=2*numLedgers; i++) {
                ledgerCache.setMasterKey((long)i, masterKey);
                for (int j=0; j<numEntries; j++) {
                    ledgerCache.putEntryOffset(i, j, i*numEntries + j);
                }
            }
        } catch (Exception e) {
            LOG.error("Got Exception.", e);
            fail("Failed to add entry.");
        }
    }

    /**
     * Test simulating race condition between when a ledger is deleting and an
     * eviction from LedgerCache simultaneously
     */
    @Test(timeout = 60000)
    public void testPageEvictionWhileDeleteLedgerInProgress() throws Exception {
        File ledgerDir1 = File.createTempFile("bkTest", ".dir");
        ledgerDir1.delete();
        File ledgerDir2 = File.createTempFile("bkTest", ".dir");
        ledgerDir2.delete();
        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(),
                ledgerDir2.getAbsolutePath() });
        conf.setOpenFileLimit(1);
        Bookie bookie = new Bookie(conf);
        InterleavedLedgerStorage ledgerStorage = ((InterleavedLedgerStorage) bookie.ledgerStorage);
        final LedgerCacheImpl ledgerCache = (LedgerCacheImpl) ledgerStorage.ledgerCache;
        // Create ledger index
        final int ledgerCount = 2;
        putLedger(ledgerCache, ledgerCount);
        for (int id = 0; id < ledgerCount; id++) {
            LOG.info("Deleting ledger id {}", id);
            ledgerCache.deleteLedger(id);
        }
        // Adding ledgers back to the openLedgers data structure. This is done
        // to simulate the case - during ledger eviction it would be deleted
        // from fileInfoCache and exists only in openLedgers list, as these two
        // are not atomic operations
        ledgerCache.openLedgers.add(Long.valueOf(0));
        ledgerCache.openLedgers.add(Long.valueOf(1));
        for (int id = 0; id < ledgerCount; id++) {
            try {
                LOG.info("Reading ledger id {}", id);
                ledgerCache.getFileInfo(Long.valueOf(id), "key".getBytes());
            } catch (Exception e) {
                LOG.info("Exception occured while getting the ledger info!", e);
                Assert.fail("Exception occured while getting the ledger id "
                        + id);
            }
        }
    }

    private void putLedger(LedgerCacheImpl ledgerCache, int count)
            throws IOException {
        for (int id = 0; id < count; id++) {
            ledgerCache.getFileInfo(Long.valueOf(id), "key".getBytes());
        }
    }

    @Test(timeout=30000)
    public void testPageEviction() throws Exception {
        int numLedgers = 10;
        byte[] masterKey = "blah".getBytes();
        // limit page count
        conf.setOpenFileLimit(999999).setPageLimit(3);
        // create ledger cache
        newLedgerCache();
        try {
            // create serveral ledgers
            for (int i=1; i<=numLedgers; i++) {
                ledgerCache.setMasterKey((long)i, masterKey);
                ledgerCache.putEntryOffset(i, 0, i*8);
                ledgerCache.putEntryOffset(i, 1, i*8);
            }

            // flush all first to clean previous dirty ledgers
            ledgerCache.flushLedger(true);
            // flush all 
            ledgerCache.flushLedger(true);

            // delete serveral ledgers
            for (int i=1; i<=numLedgers/2; i++) {
                ledgerCache.deleteLedger(i);
            }

            // bookie restarts
            newLedgerCache();

            // simulate replaying journals to add entries again
            for (int i=1; i<=numLedgers; i++) {
                try {
                    ledgerCache.putEntryOffset(i, 1, i*8);
                } catch (NoLedgerException nsle) {
                    if (i<=numLedgers/2) {
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
     * Test Ledger Cache flush failure
     */
    @Test(timeout=30000)
    public void testLedgerCacheFlushFailureOnDiskFull() throws Exception {
        File ledgerDir1 = File.createTempFile("bkTest", ".dir");
        ledgerDir1.delete();
        File ledgerDir2 = File.createTempFile("bkTest", ".dir");
        ledgerDir2.delete();
        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath() });

        Bookie bookie = new Bookie(conf);
        InterleavedLedgerStorage ledgerStorage = ((InterleavedLedgerStorage) bookie.ledgerStorage);
        LedgerCacheImpl ledgerCache = (LedgerCacheImpl) ledgerStorage.ledgerCache;
        // Create ledger index file
        ledgerStorage.setMasterKey(1, "key".getBytes());

        FileInfo fileInfo = ledgerCache.getFileInfo(Long.valueOf(1), null);

        // Simulate the flush failure
        FileInfo newFileInfo = new FileInfo(fileInfo.getLf(), fileInfo.getMasterKey());
        ledgerCache.fileInfoCache.put(Long.valueOf(1), newFileInfo);
        // Add entries
        ledgerStorage.addEntry(generateEntry(1, 1));
        ledgerStorage.addEntry(generateEntry(1, 2));
        ledgerStorage.flush();

        ledgerStorage.addEntry(generateEntry(1, 3));
        // add the dir to failed dirs
        bookie.getLedgerDirsManager().addToFilledDirs(
                newFileInfo.getLf().getParentFile().getParentFile().getParentFile());
        File before = newFileInfo.getLf();
        // flush after disk is added as failed.
        ledgerStorage.flush();
        File after = newFileInfo.getLf();

        assertEquals("Reference counting for the file info should be zero.", 0, newFileInfo.getUseCount());

        assertFalse("After flush index file should be changed", before.equals(after));
        // Verify written entries
        Assert.assertArrayEquals(generateEntry(1, 1).array(), ledgerStorage.getEntry(1, 1).array());
        Assert.assertArrayEquals(generateEntry(1, 2).array(), ledgerStorage.getEntry(1, 2).array());
        Assert.assertArrayEquals(generateEntry(1, 3).array(), ledgerStorage.getEntry(1, 3).array());
    }

    /**
     * Test that if we are writing to more ledgers than there
     * are pages, then we will not flush the index before the
     * entries in the entrylogger have been persisted to disk.
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-447}
     */
    @Test(timeout=30000)
    public void testIndexPageEvictionWriteOrder() throws Exception {
        final int numLedgers = 10;
        File journalDir = File.createTempFile("bookie", "journal");
        journalDir.delete();
        journalDir.mkdir();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));

        File ledgerDir = File.createTempFile("bookie", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(ledgerDir));

        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setFlushInterval(1000)
            .setPageLimit(1);

        Bookie b = new Bookie(conf);
        b.start();
        for (int i = 1; i <= numLedgers; i++) {
            ByteBuffer packet = generateEntry(i, 1);
            b.addEntry(packet, new Bookie.NopWriteCallback(), null, "passwd".getBytes());
        }

        conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
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
    @Test(timeout=30000)
    public void testSyncThreadNPE() throws IOException {
        newLedgerCache();
        try {
            ((LedgerCacheImpl) ledgerCache).getLedgerEntryPage(0L, 0L, true);
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
    @Test(timeout=60000)
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
                            ledgerQ.put((long)i);
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
                                ledgerCache.putEntryOffset((long)id, 1, 0);
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

    private ByteBuffer generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuffer bb = ByteBuffer.wrap(new byte[8 + 8 + data.length]);
        bb.putLong(ledger);
        bb.putLong(entry);
        bb.put(data);
        bb.flip();
        return bb;
    }
}
