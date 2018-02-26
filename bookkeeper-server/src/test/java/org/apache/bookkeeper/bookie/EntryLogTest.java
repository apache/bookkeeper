/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogManager;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogManagerForEntryLogPerLedger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for EntryLog.
 */
public class EntryLogTest {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogTest.class);

    final List<File> tempDirs = new ArrayList<File>();
    Random rand = new Random();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    @After
    public void tearDown() throws Exception {
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    @Test
    public void testCorruptEntryLog() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.toString());

        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = new Bookie(conf);
        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage) bookie.ledgerStorage).entryLogger;
        logger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3L, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2L, generateEntry(2, 1).nioBuffer());
        logger.flush();
        // now lets truncate the file to corrupt the last entry, which simulates a partial write
        File f = new File(curDir, "0.log");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.setLength(raf.length() - 10);
        raf.close();
        // now see which ledgers are in the log
        logger = new EntryLogger(conf, bookie.getLedgerDirsManager());

        EntryLogMetadata meta = logger.getEntryLogMetadata(0L);
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertTrue(meta.getLedgersMap().containsKey(1L));
        assertFalse(meta.getLedgersMap().containsKey(2L));
        assertTrue(meta.getLedgersMap().containsKey(3L));
    }

    private ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }

    private ByteBuf generateEntry(long ledger, long entry, int length) {
        ByteBuf bb = Unpooled.buffer(length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        byte[] randbyteArray = new byte[length - 8 - 8];
        rand.nextBytes(randbyteArray);
        bb.writeBytes(randbyteArray);
        return bb;
    }

    @Test
    public void testMissingLogId() throws Exception {
        File tmpDir = createTempDir("entryLogTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.toString());
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = new Bookie(conf);
        // create some entries
        int numLogs = 3;
        int numEntries = 10;
        long[][] positions = new long[2 * numLogs][];
        for (int i = 0; i < numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger logger = new EntryLogger(conf,
                    bookie.getLedgerDirsManager());
            for (int j = 0; j < numEntries; j++) {
                positions[i][j] = logger.addEntry((long) i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
        }
        // delete last log id
        File lastLogId = new File(curDir, "lastId");
        lastLogId.delete();

        // write another entries
        for (int i = numLogs; i < 2 * numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger logger = new EntryLogger(conf,
                    bookie.getLedgerDirsManager());
            for (int j = 0; j < numEntries; j++) {
                positions[i][j] = logger.addEntry((long) i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
        }

        EntryLogger newLogger = new EntryLogger(conf,
                bookie.getLedgerDirsManager());
        for (int i = 0; i < (2 * numLogs + 1); i++) {
            File logFile = new File(curDir, Long.toHexString(i) + ".log");
            assertTrue(logFile.exists());
        }
        for (int i = 0; i < 2 * numLogs; i++) {
            for (int j = 0; j < numEntries; j++) {
                String expectedValue = "ledger-" + i + "-" + j;
                ByteBuf value = newLogger.readEntry(i, j, positions[i][j]);
                long ledgerId = value.readLong();
                long entryId = value.readLong();
                byte[] data = new byte[value.readableBytes()];
                value.readBytes(data);
                value.release();
                assertEquals(i, ledgerId);
                assertEquals(j, entryId);
                assertEquals(expectedValue, new String(data));
            }
        }
    }

    @Test
    /**
     * Test that EntryLogger Should fail with FNFE, if entry logger directories does not exist.
     */
    public void testEntryLoggerShouldThrowFNFEIfDirectoriesDoesNotExist()
            throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        EntryLogger entryLogger = null;
        try {
            entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, conf.getLedgerDirs(),
                    new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())));
            fail("Expecting FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertEquals("Entry log directory does not exist", e
                    .getLocalizedMessage());
        } finally {
            if (entryLogger != null) {
                entryLogger.shutdown();
            }
        }
    }

    /**
     * Test to verify the DiskFull during addEntry.
     */
    @Test
    public void testAddEntryFailureOnDiskFull() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File ledgerDir2 = createTempDir("bkTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        conf.setJournalDirName(ledgerDir1.toString());
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(),
                ledgerDir2.getAbsolutePath() });
        Bookie bookie = new Bookie(conf);
        EntryLogger entryLogger = new EntryLogger(conf,
                bookie.getLedgerDirsManager());
        InterleavedLedgerStorage ledgerStorage = ((InterleavedLedgerStorage) bookie.ledgerStorage);
        ledgerStorage.entryLogger = entryLogger;
        // Create ledgers
        ledgerStorage.setMasterKey(1, "key".getBytes());
        ledgerStorage.setMasterKey(2, "key".getBytes());
        ledgerStorage.setMasterKey(3, "key".getBytes());
        // Add entries
        ledgerStorage.addEntry(generateEntry(1, 1));
        ledgerStorage.addEntry(generateEntry(2, 1));
        // Add entry with disk full failure simulation
        bookie.getLedgerDirsManager().addToFilledDirs(entryLogger.entryLogManager
                .getCurrentLogForLedger(EntryLogger.INVALID_LID).getLogFile().getParentFile());
        ledgerStorage.addEntry(generateEntry(3, 1));
        // Verify written entries
        Assert.assertTrue(0 == generateEntry(1, 1).compareTo(ledgerStorage.getEntry(1, 1)));
        Assert.assertTrue(0 == generateEntry(2, 1).compareTo(ledgerStorage.getEntry(2, 1)));
        Assert.assertTrue(0 == generateEntry(3, 1).compareTo(ledgerStorage.getEntry(3, 1)));
    }

    /**
     * Explicitely try to recover using the ledgers map index at the end of the entry log.
     */
    @Test
    public void testRecoverFromLedgersMap() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.toString());

        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = new Bookie(conf);

        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage) bookie.ledgerStorage).entryLogger;
        logger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3L, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2L, generateEntry(2, 1).nioBuffer());
        logger.addEntry(1L, generateEntry(1, 2).nioBuffer());
        logger.createNewLog(EntryLogger.INVALID_LID);
        logger.flushRotatedLogs();

        EntryLogMetadata meta = logger.extractEntryLogMetadataFromIndex(0L);
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertEquals(60, meta.getLedgersMap().get(1L));
        assertEquals(30, meta.getLedgersMap().get(2L));
        assertEquals(30, meta.getLedgersMap().get(3L));
        assertFalse(meta.getLedgersMap().containsKey(4L));
        assertEquals(120, meta.getTotalSize());
        assertEquals(120, meta.getRemainingSize());
    }

    /**
     * Explicitely try to recover using the ledgers map index at the end of the entry log.
     */
    @Test
    public void testRecoverFromLedgersMapOnV0EntryLog() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setJournalDirName(tmpDir.toString());
        Bookie bookie = new Bookie(conf);

        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage) bookie.ledgerStorage).entryLogger;
        logger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3L, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2L, generateEntry(2, 1).nioBuffer());
        logger.addEntry(1L, generateEntry(1, 2).nioBuffer());
        logger.createNewLog(EntryLogger.INVALID_LID);

        // Rewrite the entry log header to be on V0 format
        File f = new File(curDir, "0.log");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(EntryLogger.HEADER_VERSION_POSITION);
        // Write zeros to indicate V0 + no ledgers map info
        raf.write(new byte[4 + 8]);
        raf.close();

        // now see which ledgers are in the log
        logger = new EntryLogger(conf, bookie.getLedgerDirsManager());

        try {
            logger.extractEntryLogMetadataFromIndex(0L);
            fail("Should not be possible to recover from ledgers map index");
        } catch (IOException e) {
            // Ok
        }

        // Public method should succeed by falling back to scanning the file
        EntryLogMetadata meta = logger.getEntryLogMetadata(0L);
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertEquals(60, meta.getLedgersMap().get(1L));
        assertEquals(30, meta.getLedgersMap().get(2L));
        assertEquals(30, meta.getLedgersMap().get(3L));
        assertFalse(meta.getLedgersMap().containsKey(4L));
        assertEquals(120, meta.getTotalSize());
        assertEquals(120, meta.getRemainingSize());
    }

    /**
     * Test pre-allocate for entry log in EntryLoggerAllocator.
     * @throws Exception
     */
    @Test
    public void testPreAllocateLog() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        // enable pre-allocation case
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        conf.setEntryLogFilePreAllocationEnabled(true);
        Bookie bookie = new Bookie(conf);
        // create a logger whose initialization phase allocating a new entry log
        EntryLogger logger = ((InterleavedLedgerStorage) bookie.ledgerStorage).entryLogger;
        logger.createNewLog(EntryLogger.INVALID_LID);
        assertNotNull(logger.getEntryLoggerAllocator().getPreallocationFuture());

        logger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        // the Future<BufferedLogChannel> is not null all the time
        assertNotNull(logger.getEntryLoggerAllocator().getPreallocationFuture());

        // disable pre-allocation case
        ServerConfiguration conf2 = TestBKConfiguration.newServerConfiguration();
        conf2.setLedgerDirNames(new String[] {tmpDir.toString()});
        conf2.setEntryLogFilePreAllocationEnabled(false);
        Bookie bookie2 = new Bookie(conf2);
        // create a logger
        EntryLogger logger2 = ((InterleavedLedgerStorage) bookie2.ledgerStorage).entryLogger;
        logger2.createNewLog(EntryLogger.INVALID_LID);
        assertNull(logger2.getEntryLoggerAllocator().getPreallocationFuture());

        logger2.addEntry(2L, generateEntry(1, 1).nioBuffer());

        // the Future<BufferedLogChannel> is null all the time
        assertNull(logger2.getEntryLoggerAllocator().getPreallocationFuture());

    }

    /**
     * Test the getEntryLogsSet() method.
     */
    @Test
    public void testGetEntryLogsSet() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        Bookie bookie = new Bookie(conf);

        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage) bookie.ledgerStorage).entryLogger;

        assertEquals(Sets.newHashSet(), logger.getEntryLogsSet());

        logger.createNewLog(EntryLogger.INVALID_LID);
        logger.flushRotatedLogs();

        assertEquals(Sets.newHashSet(0L, 1L), logger.getEntryLogsSet());

        logger.createNewLog(EntryLogger.INVALID_LID);
        logger.flushRotatedLogs();

        assertEquals(Sets.newHashSet(0L, 1L, 2L), logger.getEntryLogsSet());
    }


    /**
     * Test to verify the leastUnflushedLogId logic in EntryLogsStatus.
     */
    @Test(timeout = 60000)
    public void testEntryLoggersRecentEntryLogsStatus() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogger.RecentEntryLogsStatus recentlyCreatedLogsStatus = entryLogger.recentlyCreatedEntryLogsStatus;

        recentlyCreatedLogsStatus.createdEntryLog(0L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 0L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(0L);
        // since we marked entrylog - 0 as rotated, LeastUnflushedLogId would be previous rotatedlog+1
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 1L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.createdEntryLog(1L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 1L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.createdEntryLog(2L);
        recentlyCreatedLogsStatus.createdEntryLog(3L);
        recentlyCreatedLogsStatus.createdEntryLog(4L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 1L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(1L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 2L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(3L);
        // here though we rotated entrylog-3, entrylog-2 is not yet rotated so
        // LeastUnflushedLogId should be still 2
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 2L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(2L);
        // entrylog-3 is already rotated, so leastUnflushedLogId should be 4
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 4L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(4L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 5L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.createdEntryLog(5L);
        recentlyCreatedLogsStatus.createdEntryLog(7L);
        recentlyCreatedLogsStatus.createdEntryLog(9L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 5L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(5L);
        // since we marked entrylog-5 as rotated, LeastUnflushedLogId would be previous rotatedlog+1
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 6L, entryLogger.getLeastUnflushedLogId());
        recentlyCreatedLogsStatus.flushRotatedEntryLog(7L);
        Assert.assertEquals("entryLogger's leastUnflushedLogId ", 8L, entryLogger.getLeastUnflushedLogId());
    }

    /*
     * tests logic of EntryLogManager in EntryLogger with EntryLogPerLedger enabled
     */
    @Test(timeout = 60000)
    public void testEntryLogManager() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager entryLogManager = entryLogger.entryLogManager;

        Assert.assertEquals("Number of current active EntryLogs ", 0, entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of Rotated Logs ", 0, entryLogManager.getCopyOfRotatedLogChannels().size());

        try {
            entryLogManager.acquireLock(0L);
            fail("EntryLogManager acquireLock for ledger 0 is supposed to fail,"
                    + "since lock must not have been created yet");
        } catch (NullPointerException npe) {
            // expected, since lock must not have been created
        }

        int numOfLedgers = 5;
        int numOfThreadsPerLedger = 10;
        validateLockAcquireAndRelease(numOfLedgers, numOfThreadsPerLedger, entryLogManager, true);
        validateLockAcquireAndRelease(numOfLedgers, numOfThreadsPerLedger, entryLogManager, false);

        for (long i = 0; i < numOfLedgers; i++) {
            entryLogManager.setCurrentLogForLedger(i, createDummyBufferedLogChannel(entryLogger, i, conf));
        }

        for (long i = 0; i < numOfLedgers; i++) {
            Assert.assertEquals("LogChannel for ledger: " + i, entryLogManager.getCurrentLogIfPresent(i),
                    entryLogManager.getCurrentLogForLedger(i));
        }

        Assert.assertEquals("Number of current active EntryLogs ", numOfLedgers,
                entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of Rotated Logs ", 0, entryLogManager.getCopyOfRotatedLogChannels().size());

        for (long i = 0; i < numOfLedgers; i++) {
            entryLogManager.setCurrentLogForLedger(i,
                    createDummyBufferedLogChannel(entryLogger, numOfLedgers + i, conf));
        }

        /*
         * since new entryLogs are set for all the ledgers, previous entrylogs would be added to rotatedLogChannels
         */
        Assert.assertEquals("Number of current active EntryLogs ", numOfLedgers,
                entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of Rotated Logs ", numOfLedgers,
                entryLogManager.getCopyOfRotatedLogChannels().size());

        for (long i = 0; i < numOfLedgers; i++) {
            entryLogManager.setCurrentLogForLedger(i,
                    createDummyBufferedLogChannel(entryLogger, 2 * numOfLedgers + i, conf));
        }

        /*
         * again since new entryLogs are set for all the ledgers, previous entrylogs would be added to
         * rotatedLogChannels
         */
        Assert.assertEquals("Number of current active EntryLogs ", numOfLedgers,
                entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of Rotated Logs ", 2 * numOfLedgers,
                entryLogManager.getCopyOfRotatedLogChannels().size());

        for (BufferedLogChannel logChannel : entryLogManager.getCopyOfRotatedLogChannels()) {
            entryLogManager.removeFromRotatedLogChannels(logChannel);
        }
        Assert.assertEquals("Number of Rotated Logs ", 0, entryLogManager.getCopyOfRotatedLogChannels().size());

        // entrylogid is sequential
        for (long i = 0; i < numOfLedgers; i++) {
            assertEquals("EntryLogid for Ledger " + i, 2 * numOfLedgers + i,
                    entryLogManager.getCurrentLogForLedger(i).getLogId());
        }

        for (long i = 2 * numOfLedgers; i < (3 * numOfLedgers); i++) {
            assertTrue("EntryLog with logId: " + i + " should be present",
                    entryLogManager.getCurrentLogIfPresent(i) != null);
        }
    }

    private EntryLogger.BufferedLogChannel createDummyBufferedLogChannel(EntryLogger entryLogger, long logid,
            ServerConfiguration servConf) throws IOException {
        File tmpFile = File.createTempFile("entrylog", logid + "");
        tmpFile.deleteOnExit();
        FileChannel fc = FileChannel.open(tmpFile.toPath());
        EntryLogger.BufferedLogChannel logChannel = entryLogger.new BufferedLogChannel(fc, 10, 10, logid, tmpFile,
                servConf.getFlushIntervalInBytes());
        return logChannel;
    }

    /*
     * validates the concurrency aspect of entryLogManager.acquireLockByCreatingIfRequired/acquireLock/releaseLock
     *
     * Executor of fixedThreadPool of size 'numOfLedgers * numOfThreadsPerLedger' is created and the same number
     * of tasks are submitted to the Executor. In each task, lock of that ledger is acquired and then released.
     */
    private void validateLockAcquireAndRelease(int numOfLedgers, int numOfThreadsPerLedger,
            EntryLogManager entryLogManager, boolean acquireLockByCreatingOnly) throws InterruptedException {
        ExecutorService tpe = Executors.newFixedThreadPool(numOfLedgers * numOfThreadsPerLedger);
        CountDownLatch latchToStart = new CountDownLatch(1);
        CountDownLatch latchToWait = new CountDownLatch(1);
        AtomicInteger numberOfThreadsAcquiredLock = new AtomicInteger(0);
        AtomicBoolean irptExceptionHappened = new AtomicBoolean(false);
        Random rand = new Random();

        for (int i = 0; i < numOfLedgers * numOfThreadsPerLedger; i++) {
            long ledgerId = i % numOfLedgers;
            tpe.submit(() -> {
                try {
                    latchToStart.await();
                    if (acquireLockByCreatingOnly) {
                        entryLogManager.acquireLockByCreatingIfRequired(ledgerId);
                    } else {
                        if (rand.nextBoolean()) {
                            entryLogManager.acquireLock(ledgerId);
                        } else {
                            entryLogManager.acquireLockByCreatingIfRequired(ledgerId);
                        }
                    }
                    numberOfThreadsAcquiredLock.incrementAndGet();
                    latchToWait.await();
                    entryLogManager.releaseLock(ledgerId);
                } catch (InterruptedException e) {
                    irptExceptionHappened.set(true);
                }
            });
        }

        assertEquals("Number Of Threads acquired Lock", 0, numberOfThreadsAcquiredLock.get());
        latchToStart.countDown();
        Thread.sleep(1000);
        /*
         * since there are only "numOfLedgers" ledgers, only "numOfLedgers" threads should have been able to acquire
         * lock. After acquiring the lock there must be waiting on 'latchToWait' latch
         */
        assertEquals("Number Of Threads acquired Lock", numOfLedgers, numberOfThreadsAcquiredLock.get());
        latchToWait.countDown();
        Thread.sleep(2000);
        assertEquals("Number Of Threads acquired Lock", numOfLedgers * numOfThreadsPerLedger,
                numberOfThreadsAcquiredLock.get());
    }

    /*
     * test EntryLogManager.EntryLogManagerForEntryLogPerLedger removes the
     * ledger from its cache map if entry is not added to that ledger or its
     * corresponding state is not accessed for more than evictionPeriod
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testEntryLogManagerExpiryRemoval() throws Exception {
        int evictionPeriod = 1;

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        conf.setEntrylogMapAccessExpiryTimeInSeconds(evictionPeriod);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager =
                (EntryLogManagerForEntryLogPerLedger) entryLogger.entryLogManager;

        long ledgerId = 0L;

        entryLogManager.acquireLockByCreatingIfRequired(ledgerId);
        entryLogManager.releaseLock(ledgerId);
        BufferedLogChannel logChannel = createDummyBufferedLogChannel(entryLogger, 0, conf);
        entryLogManager.setCurrentLogForLedger(ledgerId, logChannel);

        BufferedLogChannel currentLogForLedger = entryLogManager.getCurrentLogForLedger(ledgerId);
        assertEquals("LogChannel for ledger " + ledgerId + " should match", logChannel, currentLogForLedger);

        Thread.sleep(evictionPeriod * 1000 + 100);
        entryLogManager.doEntryLogMapCleanup();

        /*
         * since for more than evictionPeriod, that ledger is not accessed and cache is cleaned up, mapping for that
         * ledger should not be available anymore
         */
        currentLogForLedger = entryLogManager.getCurrentLogForLedger(ledgerId);
        assertEquals("LogChannel for ledger " + ledgerId + " should be null", null, currentLogForLedger);
        Assert.assertEquals("Number of current active EntryLogs ", 0, entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of rotated EntryLogs ", 1, entryLogManager.getCopyOfRotatedLogChannels().size());
        Assert.assertTrue("CopyOfRotatedLogChannels should contain the created LogChannel",
                entryLogManager.getCopyOfRotatedLogChannels().contains(logChannel));

        try {
            entryLogManager.acquireLock(ledgerId);
            fail("EntryLogManager acquireLock for ledger is supposed to fail, since mapentry must have been evicted");
        } catch (NullPointerException npe) {
            // expected, since lock must not have been created
        }
    }

    /**
     * test EntryLogManager.EntryLogManagerForEntryLogPerLedger doesn't removes
     * the ledger from its cache map if ledger's corresponding state is accessed
     * within the evictionPeriod.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testExpiryRemovalByAccessingOnAnotherThread() throws Exception {
        int evictionPeriod = 1;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        conf.setEntrylogMapAccessExpiryTimeInSeconds(evictionPeriod);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager =
                (EntryLogManagerForEntryLogPerLedger) entryLogger.entryLogManager;

        long ledgerId = 0L;

        entryLogManager.acquireLockByCreatingIfRequired(ledgerId);
        entryLogManager.releaseLock(ledgerId);
        BufferedLogChannel newLogChannel = createDummyBufferedLogChannel(entryLogger, 1, conf);
        entryLogManager.setCurrentLogForLedger(ledgerId, newLogChannel);

        Thread t = new Thread() {
            public void run() {
                try {
                    Thread.sleep((evictionPeriod * 1000) / 2);
                    entryLogManager.getCurrentLogForLedger(ledgerId);
                } catch (InterruptedException e) {
                }
            }
        };

        t.start();
        Thread.sleep(evictionPeriod * 1000 + 100);
        entryLogManager.doEntryLogMapCleanup();

        /*
         * in this scenario, that ledger is accessed by other thread during
         * eviction period time, so it should not be evicted.
         */
        BufferedLogChannel currentLogForLedger = entryLogManager.getCurrentLogForLedger(ledgerId);
        assertEquals("LogChannel for ledger " + ledgerId, newLogChannel, currentLogForLedger);
        Assert.assertEquals("Number of current active EntryLogs ", 1, entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of rotated EntryLogs ", 0, entryLogManager.getCopyOfRotatedLogChannels().size());
    }

    /**
     * test EntryLogManager.EntryLogManagerForEntryLogPerLedger removes the
     * ledger from its cache map if entry is not added to that ledger or its
     * corresponding state is not accessed for more than evictionPeriod. In this
     * testcase we try to call unrelated methods or access state of other
     * ledgers within the eviction period.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testExpiryRemovalByAccessingNonCacheRelatedMethods() throws Exception {
        int evictionPeriod = 1;

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        conf.setEntrylogMapAccessExpiryTimeInSeconds(evictionPeriod);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager =
                (EntryLogManagerForEntryLogPerLedger) entryLogger.entryLogManager;

        long ledgerId = 0L;

        entryLogManager.acquireLockByCreatingIfRequired(ledgerId);
        entryLogManager.releaseLock(ledgerId);
        BufferedLogChannel newLogChannel = createDummyBufferedLogChannel(entryLogger, 1, conf);
        entryLogManager.setCurrentLogForLedger(ledgerId, newLogChannel);

        AtomicBoolean exceptionOccured = new AtomicBoolean(false);
        Thread t = new Thread() {
            public void run() {
                try {
                    Thread.sleep(500);
                    /*
                     * any of the following operations should not access entry
                     * of 'ledgerId' in the cache
                     */
                    entryLogManager.getCopyOfCurrentLogs();
                    entryLogManager.getCopyOfRotatedLogChannels();
                    entryLogManager.getCurrentLogIfPresent(newLogChannel.getLogId());
                    entryLogManager.removeFromRotatedLogChannels(createDummyBufferedLogChannel(entryLogger, 500, conf));
                    entryLogManager.getDirForNextEntryLog(ledgerDirsManager.getWritableLedgerDirs());
                    long newLedgerId = 100;
                    entryLogManager.acquireLockByCreatingIfRequired(newLedgerId);
                    entryLogManager.releaseLock(newLedgerId);
                    BufferedLogChannel logChannelForNewLedger =
                            createDummyBufferedLogChannel(entryLogger, newLedgerId, conf);
                    entryLogManager.setCurrentLogForLedger(newLedgerId, logChannelForNewLedger);
                    entryLogManager.getCurrentLogIfPresent(newLedgerId);
                } catch (Exception e) {
                    LOG.error("Got Exception in thread", e);
                    exceptionOccured.set(true);
                }
            }
        };

        t.start();
        Thread.sleep(evictionPeriod * 1000 + 100);
        entryLogManager.doEntryLogMapCleanup();
        Assert.assertFalse("Exception occured in thread, which is not expected", exceptionOccured.get());

        /*
         * since for more than evictionPeriod, that ledger is not accessed and cache is cleaned up, mapping for that
         * ledger should not be available anymore
         */
        BufferedLogChannel currentLogForLedger = entryLogManager.getCurrentLogForLedger(ledgerId);
        assertEquals("LogChannel for ledger " + ledgerId + " should be null", null, currentLogForLedger);
        // expected number of current active entryLogs is 1 since we created entrylog for 'newLedgerId'
        Assert.assertEquals("Number of current active EntryLogs ", 1, entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of rotated EntryLogs ", 1, entryLogManager.getCopyOfRotatedLogChannels().size());
        Assert.assertTrue("CopyOfRotatedLogChannels should contain the created LogChannel",
                entryLogManager.getCopyOfRotatedLogChannels().contains(newLogChannel));

        try {
            entryLogManager.acquireLock(ledgerId);
            fail("EntryLogManager acquireLock for ledger is supposed to fail, since mapentry must have been evicted");
        } catch (NullPointerException npe) {
            // expected, since lock must not have been created
        }
    }

    /*
     * testing EntryLogger functionality (addEntry/createNewLog/flush) and EntryLogManager with entryLogPerLedger
     * enabled
     */
    @Test(timeout = 60000)
    public void testEntryLoggerWithEntryLogPerLedgerEnabled() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setFlushIntervalInBytes(10000000);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager entryLogManager = entryLogger.entryLogManager;
        Assert.assertEquals("EntryLogManager class type", EntryLogger.EntryLogManagerForEntryLogPerLedger.class,
                entryLogManager.getClass());

        int numOfActiveLedgers = 20;
        int numEntries = 5;

        for (int j = 0; j < numEntries; j++) {
            for (long i = 0; i < numOfActiveLedgers; i++) {
                entryLogger.addEntry(i, generateEntry(i, j));
            }
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            BufferedLogChannel logChannel =  entryLogManager.getCurrentLogForLedger(i);
            Assert.assertTrue("unpersistedBytes should be greater than LOGFILE_HEADER_SIZE",
                    logChannel.getUnpersistedBytes() > EntryLogger.LOGFILE_HEADER_SIZE);
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            entryLogger.createNewLog(i);
        }

        /*
         * since we created new entrylog for all the activeLedgers, entrylogs of all the ledgers
         * should be rotated and hence the size of copyOfRotatedLogChannels should be numOfActiveLedgers
         */
        Set<BufferedLogChannel> rotatedLogs = entryLogManager.getCopyOfRotatedLogChannels();
        Assert.assertEquals("Number of rotated entrylogs", numOfActiveLedgers, rotatedLogs.size());

        /*
         * Since newlog is created for all slots, so they are moved to rotated logs and hence unpersistedBytes of all
         * the slots should be just EntryLogger.LOGFILE_HEADER_SIZE
         *
         */
        for (long i = 0; i < numOfActiveLedgers; i++) {
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogForLedger(i);
            Assert.assertEquals("unpersistedBytes should be LOGFILE_HEADER_SIZE", EntryLogger.LOGFILE_HEADER_SIZE,
                    logChannel.getUnpersistedBytes());
        }

        for (int j = numEntries; j < 2 * numEntries; j++) {
            for (long i = 0; i < numOfActiveLedgers; i++) {
                entryLogger.addEntry(i, generateEntry(i, j));
            }
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            BufferedLogChannel logChannel =  entryLogManager.getCurrentLogForLedger(i);
            Assert.assertTrue("unpersistedBytes should be greater than LOGFILE_HEADER_SIZE",
                    logChannel.getUnpersistedBytes() > EntryLogger.LOGFILE_HEADER_SIZE);
        }

        Assert.assertEquals("LeastUnflushedloggerID", 0, entryLogger.getLeastUnflushedLogId());

        /*
         * here flush is called so all the rotatedLogChannels should be file closed and there shouldn't be any
         * rotatedlogchannel and also leastUnflushedLogId should be advanced to numOfActiveLedgers
         */
        entryLogger.flush();
        Assert.assertEquals("Number of rotated entrylogs", 0, entryLogManager.getCopyOfRotatedLogChannels().size());
        Assert.assertEquals("LeastUnflushedloggerID", numOfActiveLedgers, entryLogger.getLeastUnflushedLogId());

        /*
         * after flush (flushCurrentLogs) unpersistedBytes should be 0.
         */
        for (long i = 0; i < numOfActiveLedgers; i++) {
            BufferedLogChannel logChannel =  entryLogManager.getCurrentLogForLedger(i);
            Assert.assertEquals("unpersistedBytes should be 0", 0L, logChannel.getUnpersistedBytes());
        }
    }

    /*
     * with entryLogPerLedger enabled, create multiple entrylogs, add entries of ledgers and read them before and after
     * flush
     */
    @Test(timeout = 60000)
    public void testReadAddCallsOfMultipleEntryLogs() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        // pre allocation enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);

        int numOfActiveLedgers = 10;
        int numEntries = 10;
        long[][] positions = new long[numOfActiveLedgers][];
        for (int i = 0; i < numOfActiveLedgers; i++) {
            positions[i] = new long[numEntries];
        }

        /*
         * addentries to the ledgers
         */
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                positions[i][j] = entryLogger.addEntry((long) i, generateEntry(i, j));
                long entryLogId = (positions[i][j] >> 32L);
                /**
                 *
                 * Though EntryLogFilePreAllocation is enabled, Since things are not done concurrently here,
                 * entryLogIds will be sequential.
                 */
                Assert.assertEquals("EntryLogId for ledger: " + i, i, entryLogId);
            }
        }

        /*
         * read the entries which are written
         */
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                String expectedValue = "ledger-" + i + "-" + j;
                ByteBuf buf = entryLogger.readEntry(i, j, positions[i][j]);
                long ledgerId = buf.readLong();
                long entryId = buf.readLong();
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                assertEquals("LedgerId ", i, ledgerId);
                assertEquals("EntryId ", j, entryId);
                assertEquals("Entry Data ", expectedValue, new String(data));
            }
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            entryLogger.createNewLog(i);
        }

        entryLogger.flushRotatedLogs();

        // reading after flush of rotatedlogs
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                String expectedValue = "ledger-" + i + "-" + j;
                ByteBuf buf = entryLogger.readEntry(i, j, positions[i][j]);
                long ledgerId = buf.readLong();
                long entryId = buf.readLong();
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                assertEquals("LedgerId ", i, ledgerId);
                assertEquals("EntryId ", j, entryId);
                assertEquals("Entry Data ", expectedValue, new String(data));
            }
        }
    }


    class WriteTask implements Callable<Boolean> {
        long ledgerId;
        int entryId;
        AtomicLongArray positions;
        int indexInPositions;
        EntryLogger entryLogger;

        WriteTask(long ledgerId, int entryId, EntryLogger entryLogger, AtomicLongArray positions,
                int indexInPositions) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.entryLogger = entryLogger;
            this.positions = positions;
            this.indexInPositions = indexInPositions;
        }

        @Override
        public Boolean call() {
            try {
                positions.set(indexInPositions, entryLogger.addEntry(ledgerId, generateEntry(ledgerId, entryId)));
            } catch (IOException e) {
                LOG.error("Got Exception for AddEntry call. LedgerId: " + ledgerId + " entryId: " + entryId, e);
                return false;
            }
            return true;
        }
    }

    class ReadTask implements Callable<Boolean> {
        long ledgerId;
        int entryId;
        long position;
        EntryLogger entryLogger;

        ReadTask(long ledgerId, int entryId, long position, EntryLogger entryLogger) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.position = position;
            this.entryLogger = entryLogger;
        }

        @Override
        public Boolean call() {
            try {
                String expectedValue = "ledger-" + ledgerId + "-" + entryId;
                ByteBuf buf = entryLogger.readEntry(ledgerId, entryId, position);
                long actualLedgerId = buf.readLong();
                long actualEntryId = buf.readLong();
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                if (ledgerId != actualLedgerId) {
                    LOG.error("For ledgerId: {} entryId: {} readRequest, actual ledgerId: {}", ledgerId, entryId,
                            actualLedgerId);
                    return false;
                }
                if (entryId != actualEntryId) {
                    LOG.error("For ledgerId: {} entryId: {} readRequest, actual entryId: {}", ledgerId, entryId,
                            actualEntryId);
                    return false;
                }
                if (!expectedValue.equals(new String(data))) {
                    LOG.error("For ledgerId: {} entryId: {} readRequest, actual Data: {}", ledgerId, entryId,
                            new String(data));
                    return false;
                }
            } catch (IOException e) {
                LOG.error("Got Exception for ReadEntry call. LedgerId: " + ledgerId + " entryId: " + entryId, e);
                return false;
            }
            return true;
        }
    }

    class FlushCurrentLogsTask implements Callable<Boolean> {
        EntryLogger entryLogger;

        FlushCurrentLogsTask(EntryLogger entryLogger) {
            this.entryLogger = entryLogger;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                entryLogger.flushCurrentLogs();
                return true;
            } catch (IOException e) {
                LOG.error("Got Exception while trying to flushCurrentLogs");
                return false;
            }
        }
    }

    /*
     * test concurrent write and flush operations and then concurrent read operations with entrylogperledger enabled
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testConcurrentWriteFlushAndReadCallsOfMultipleEntryLogs() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setFlushIntervalInBytes(1000 * 25);
        conf.setLedgerDirNames(createAndGetLedgerDirs(3));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        int numOfActiveLedgers = 15;
        int numEntries = 2000;
        final AtomicLongArray positions = new AtomicLongArray(numOfActiveLedgers * numEntries);
        ExecutorService executor = Executors.newFixedThreadPool(40);

        List<Callable<Boolean>> writeAndFlushTasks = new ArrayList<Callable<Boolean>>();
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                writeAndFlushTasks.add(new WriteTask(i, j, entryLogger, positions, i * numEntries + j));
            }
        }

        // add flushCurrentLogs tasks also
        for (int i = 0; i < writeAndFlushTasks.size() / 100; i++) {
            writeAndFlushTasks.add(i * 100, new FlushCurrentLogsTask(entryLogger));
        }

        // invoke all those write and flushcurrentlogs tasks all at once concurrently and set timeout
        // 6 seconds for them to complete
        List<Future<Boolean>> writeAndFlushTasksFutures = executor.invokeAll(writeAndFlushTasks, 6, TimeUnit.SECONDS);
        for (int i = 0; i < writeAndFlushTasks.size(); i++) {
            Future<Boolean> future = writeAndFlushTasksFutures.get(i);
            Callable<Boolean> task = writeAndFlushTasks.get(i);
            if (task instanceof WriteTask) {
                WriteTask writeTask = (WriteTask) task;
                long ledgerId = writeTask.ledgerId;
                int entryId = writeTask.entryId;
                Assert.assertTrue("WriteTask should have been completed successfully ledgerId: " + ledgerId
                        + " entryId: " + entryId, future.isDone() && (!future.isCancelled()));
                Assert.assertTrue(
                        "Position for ledgerId: " + ledgerId + " entryId: " + entryId + " should have been set",
                        future.get());
            } else {
                Assert.assertTrue("FlushTask should have been completed successfully. Index " + i,
                        future.isDone() && (!future.isCancelled()));
                Assert.assertTrue("FlushTask should have been succeded without exception. Index " + i, future.get());
            }
        }

        List<ReadTask> readTasks = new ArrayList<ReadTask>();
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                readTasks.add(new ReadTask(i, j, positions.get(i * numEntries + j), entryLogger));
            }
        }

        // invoke all those readtasks all at once concurrently and set timeout
        // 6 seconds for them to complete
        List<Future<Boolean>> readTasksFutures = executor.invokeAll(readTasks, 6, TimeUnit.SECONDS);
        for (int i = 0; i < numOfActiveLedgers * numEntries; i++) {
            Future<Boolean> future = readTasksFutures.get(i);
            long ledgerId = readTasks.get(i).ledgerId;
            int entryId = readTasks.get(i).entryId;
            Assert.assertTrue(
                    "ReadTask should have been completed successfully ledgerId: " + ledgerId + " entryId: " + entryId,
                    future.isDone() && (!future.isCancelled()));
            Assert.assertTrue("ReadEntry of ledgerId: " + ledgerId + " entryId: " + entryId
                    + " should have been completed successfully", future.get());
        }
    }

    /*
     * test concurrent read operations for entries which are flushed to entrylogs with entryLogPerLedger enabled
     */
    @Test(timeout = 60000)
    public void testConcurrentReadCallsAfterEntryLogsAreRotated() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setFlushIntervalInBytes(1000 * 25);
        conf.setLedgerDirNames(createAndGetLedgerDirs(3));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        int numOfActiveLedgers = 15;
        int numEntries = 2000;
        final AtomicLongArray positions = new AtomicLongArray(numOfActiveLedgers * numEntries);

        for (int i = 0; i < numOfActiveLedgers; i++) {
            for (int j = 0; j < numEntries; j++) {
                positions.set(i * numEntries + j, entryLogger.addEntry((long) i, generateEntry(i, j)));
                long entryLogId = (positions.get(i * numEntries + j) >> 32L);
                /**
                 *
                 * Though EntryLogFilePreAllocation is enabled, Since things are not done concurrently here, entryLogIds
                 * will be sequential.
                 */
                Assert.assertEquals("EntryLogId for ledger: " + i, i, entryLogId);
            }
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            entryLogger.createNewLog(i);
        }
        entryLogger.flushRotatedLogs();

        // reading after flush of rotatedlogs
        ArrayList<ReadTask> readTasks = new ArrayList<ReadTask>();
        for (int i = 0; i < numOfActiveLedgers; i++) {
            for (int j = 0; j < numEntries; j++) {
                readTasks.add(new ReadTask(i, j, positions.get(i * numEntries + j), entryLogger));
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(40);
        List<Future<Boolean>> readTasksFutures = executor.invokeAll(readTasks, 6, TimeUnit.SECONDS);
        for (int i = 0; i < numOfActiveLedgers * numEntries; i++) {
            Future<Boolean> future = readTasksFutures.get(i);
            long ledgerId = readTasks.get(i).ledgerId;
            int entryId = readTasks.get(i).entryId;
            Assert.assertTrue(
                    "ReadTask should have been completed successfully ledgerId: " + ledgerId + " entryId: " + entryId,
                    future.isDone() && (!future.isCancelled()));
            Assert.assertTrue("ReadEntry of ledgerId: " + ledgerId + " entryId: " + entryId
                    + " should have been completed successfully", future.get());
        }
    }

    String[] createAndGetLedgerDirs(int numOfLedgerDirs) throws IOException {
        File ledgerDir;
        File curDir;
        String[] ledgerDirsPath = new String[numOfLedgerDirs];
        for (int i = 0; i < numOfLedgerDirs; i++) {
            ledgerDir = createTempDir("bkTest", ".dir");
            curDir = Bookie.getCurrentDirectory(ledgerDir);
            Bookie.checkDirectoryStructure(curDir);
            ledgerDirsPath[i] = ledgerDir.getAbsolutePath();
        }
        return ledgerDirsPath;
    }

    /**
     * testcase to validate when ledgerdirs become full and eventually all
     * ledgerdirs become full. Later a ledgerdir becomes writable.
     */
    @Test(timeout = 60000)
    public void testEntryLoggerAddEntryWhenLedgerDirsAreFull() throws Exception {
        int numberOfLedgerDirs = 3;
        List<File> ledgerDirs = new ArrayList<File>();
        String[] ledgerDirsPath = new String[numberOfLedgerDirs];
        List<File> curDirs = new ArrayList<File>();

        File ledgerDir;
        File curDir;
        for (int i = 0; i < numberOfLedgerDirs; i++) {
            ledgerDir = createTempDir("bkTest", ".dir").getAbsoluteFile();
            curDir = Bookie.getCurrentDirectory(ledgerDir);
            Bookie.checkDirectoryStructure(curDir);
            ledgerDirs.add(ledgerDir);
            ledgerDirsPath[i] = ledgerDir.getPath();
            curDirs.add(curDir);
        }

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        // pre-allocation is disabled
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(ledgerDirsPath);

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager entryLogManager = entryLogger.entryLogManager;
        Assert.assertEquals("EntryLogManager class type", EntryLogger.EntryLogManagerForEntryLogPerLedger.class,
                entryLogManager.getClass());

        entryLogger.addEntry(0L, generateEntry(0, 1));
        entryLogger.addEntry(1L, generateEntry(1, 1));
        entryLogger.addEntry(2L, generateEntry(2, 1));

        File ledgerDirForLedger0 = entryLogManager.getCurrentLogForLedger(0L).getLogFile().getParentFile();
        File ledgerDirForLedger1 = entryLogManager.getCurrentLogForLedger(1L).getLogFile().getParentFile();
        File ledgerDirForLedger2 = entryLogManager.getCurrentLogForLedger(2L).getLogFile().getParentFile();

        Set<File> ledgerDirsSet = new HashSet<File>();
        ledgerDirsSet.add(ledgerDirForLedger0);
        ledgerDirsSet.add(ledgerDirForLedger1);
        ledgerDirsSet.add(ledgerDirForLedger2);

        /*
         * since there are 3 ledgerdirs, entrylogs for all the 3 ledgers should be in different ledgerdirs.
         */
        Assert.assertEquals("Current active LedgerDirs size", 3, ledgerDirs.size());
        Assert.assertEquals("Number of rotated logchannels", 0, entryLogManager.getCopyOfRotatedLogChannels().size());

        /*
         * ledgerDirForLedger0 is added to filledDirs, for ledger0 new entrylog should not be created in
         * ledgerDirForLedger0
         */
        ledgerDirsManager.addToFilledDirs(ledgerDirForLedger0);
        addEntryAndValidateFolders(entryLogger, entryLogManager, 2, ledgerDirForLedger0, false, ledgerDirForLedger1,
                ledgerDirForLedger2);
        Assert.assertEquals("Number of rotated logchannels", 1, entryLogManager.getCopyOfRotatedLogChannels().size());

        /*
         * ledgerDirForLedger1 is also added to filledDirs, so for all the ledgers new entryLogs should be in
         * ledgerDirForLedger2
         */
        ledgerDirsManager.addToFilledDirs(ledgerDirForLedger1);
        addEntryAndValidateFolders(entryLogger, entryLogManager, 3, ledgerDirForLedger2, true, ledgerDirForLedger2,
                ledgerDirForLedger2);
        Assert.assertTrue("Number of rotated logchannels", (2 <= entryLogManager.getCopyOfRotatedLogChannels().size())
                && (entryLogManager.getCopyOfRotatedLogChannels().size() <= 3));
        int numOfRotatedLogChannels = entryLogManager.getCopyOfRotatedLogChannels().size();

        /*
         * since ledgerDirForLedger2 is added to filleddirs, all the dirs are full. If all the dirs are full then it
         * will continue to use current entrylogs for new entries instead of creating new one. So for all the ledgers
         * ledgerdirs should be same as before - ledgerDirForLedger2
         */
        ledgerDirsManager.addToFilledDirs(ledgerDirForLedger2);
        addEntryAndValidateFolders(entryLogger, entryLogManager, 4, ledgerDirForLedger2, true, ledgerDirForLedger2,
                ledgerDirForLedger2);
        Assert.assertEquals("Number of rotated logchannels", numOfRotatedLogChannels,
                entryLogManager.getCopyOfRotatedLogChannels().size());

        /*
         *  ledgerDirForLedger1 is added back to writableDirs, so new entrylog for all the ledgers should be created in
         *  ledgerDirForLedger1
         */
        ledgerDirsManager.addToWritableDirs(ledgerDirForLedger1, true);
        addEntryAndValidateFolders(entryLogger, entryLogManager, 4, ledgerDirForLedger1, true, ledgerDirForLedger1,
                ledgerDirForLedger1);
        Assert.assertEquals("Number of rotated logchannels", numOfRotatedLogChannels + 3,
                entryLogManager.getCopyOfRotatedLogChannels().size());
    }

    /*
     * in this method we add an entry and validate the ledgerdir of the
     * currentLogForLedger against the provided expected ledgerDirs.
     */
    void addEntryAndValidateFolders(EntryLogger entryLogger, EntryLogManager entryLogManager, int entryId,
            File expectedDirForLedger0, boolean equalsForLedger0, File expectedDirForLedger1,
            File expectedDirForLedger2) throws IOException {
        entryLogger.addEntry(0L, generateEntry(0, entryId));
        entryLogger.addEntry(1L, generateEntry(1, entryId));
        entryLogger.addEntry(2L, generateEntry(2, entryId));

        if (equalsForLedger0) {
            Assert.assertEquals("LedgerDir for ledger 0 after adding entry " + entryId, expectedDirForLedger0,
                    entryLogManager.getCurrentLogForLedger(0L).getLogFile().getParentFile());
        } else {
            Assert.assertNotEquals("LedgerDir for ledger 0 after adding entry " + entryId, expectedDirForLedger0,
                    entryLogManager.getCurrentLogForLedger(0L).getLogFile().getParentFile());
        }
        Assert.assertEquals("LedgerDir for ledger 1 after adding entry " + entryId, expectedDirForLedger1,
                entryLogManager.getCurrentLogForLedger(1L).getLogFile().getParentFile());
        Assert.assertEquals("LedgerDir for ledger 2 after adding entry " + entryId, expectedDirForLedger2,
                entryLogManager.getCurrentLogForLedger(2L).getLogFile().getParentFile());
    }

    /*
     * entries added using entrylogger with entryLogPerLedger enabled and the same entries are read using entrylogger
     * with entryLogPerLedger disabled
     */
    @Test(timeout = 60000)
    public void testSwappingEntryLogManagerFromEntryLogPerLedgerToSingle() throws Exception {
        testSwappingEntryLogManager(true, false);
    }

    /*
     * entries added using entrylogger with entryLogPerLedger disabled and the same entries are read using entrylogger
     * with entryLogPerLedger enabled
     */
    @Test(timeout = 60000)
    public void testSwappingEntryLogManagerFromSingleToEntryLogPerLedger() throws Exception {
        testSwappingEntryLogManager(false, true);
    }

    public void testSwappingEntryLogManager(boolean initialEntryLogPerLedgerEnabled,
            boolean laterEntryLogPerLedgerEnabled) throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(initialEntryLogPerLedgerEnabled);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        // pre allocation enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager entryLogManager = entryLogger.entryLogManager;
        Assert.assertEquals("EntryLogManager class type",
                initialEntryLogPerLedgerEnabled ? EntryLogger.EntryLogManagerForEntryLogPerLedger.class
                        : EntryLogger.EntryLogManagerForSingleEntryLog.class,
                entryLogManager.getClass());

        int numOfActiveLedgers = 10;
        int numEntries = 10;
        long[][] positions = new long[numOfActiveLedgers][];
        for (int i = 0; i < numOfActiveLedgers; i++) {
            positions[i] = new long[numEntries];
        }

        /*
         * addentries to the ledgers
         */
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                positions[i][j] = entryLogger.addEntry((long) i, generateEntry(i, j));
                long entryLogId = (positions[i][j] >> 32L);
                if (initialEntryLogPerLedgerEnabled) {
                    Assert.assertEquals("EntryLogId for ledger: " + i, i, entryLogId);
                } else {
                    Assert.assertEquals("EntryLogId for ledger: " + i, 0, entryLogId);
                }
            }
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            entryLogger.createNewLog(i);
        }

        /**
         * since new entrylog is created for all the ledgers, the previous
         * entrylogs must be rotated and with the following flushRotatedLogs
         * call they should be forcewritten and file should be closed.
         */
        entryLogger.flushRotatedLogs();

        /*
         * new entrylogger and entryLogManager are created with
         * 'laterEntryLogPerLedgerEnabled' conf
         */
        conf.setEntryLogPerLedgerEnabled(laterEntryLogPerLedgerEnabled);
        LedgerDirsManager newLedgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger newEntryLogger = new EntryLogger(conf, newLedgerDirsManager);
        EntryLogManager newEntryLogManager = newEntryLogger.entryLogManager;
        Assert.assertEquals("EntryLogManager class type",
                laterEntryLogPerLedgerEnabled ? EntryLogger.EntryLogManagerForEntryLogPerLedger.class
                        : EntryLogger.EntryLogManagerForSingleEntryLog.class,
                newEntryLogManager.getClass());

        /*
         * read the entries (which are written with previous entrylogger) with
         * new entrylogger
         */
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                String expectedValue = "ledger-" + i + "-" + j;
                ByteBuf buf = newEntryLogger.readEntry(i, j, positions[i][j]);
                long ledgerId = buf.readLong();
                long entryId = buf.readLong();
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                assertEquals("LedgerId ", i, ledgerId);
                assertEquals("EntryId ", j, entryId);
                assertEquals("Entry Data ", expectedValue, new String(data));
            }
        }
    }

    /*
     * test for validating if the EntryLog/BufferedChannel flushes/forcewrite if the bytes written to it are more than
     * flushIntervalInBytes
     */
    @Test(timeout = 60000)
    public void testFlushIntervalInBytes() throws Exception {
        long flushIntervalInBytes = 5000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setFlushIntervalInBytes(flushIntervalInBytes);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager entryLogManager = entryLogger.entryLogManager;

        /*
         * when entryLogger is created Header of length EntryLogger.LOGFILE_HEADER_SIZE is created
         */
        long ledgerId = 0L;
        int firstEntrySize = 1000;
        long entry0Position = entryLogger.addEntry(0L, generateEntry(ledgerId, 0L, firstEntrySize));
        // entrylogger writes length of the entry (4 bytes) before writing entry
        long expectedUnpersistedBytes = EntryLogger.LOGFILE_HEADER_SIZE + firstEntrySize + 4;
        Assert.assertEquals("Unpersisted Bytes of entrylog", expectedUnpersistedBytes,
                entryLogManager.getCurrentLogForLedger(ledgerId).getUnpersistedBytes());

        /*
         * 'flushIntervalInBytes' number of bytes are flushed so BufferedChannel should be forcewritten
         */
        int secondEntrySize = (int) (flushIntervalInBytes - expectedUnpersistedBytes);
        long entry1Position = entryLogger.addEntry(0L, generateEntry(ledgerId, 1L, secondEntrySize));
        Assert.assertEquals("Unpersisted Bytes of entrylog", 0,
                entryLogManager.getCurrentLogForLedger(ledgerId).getUnpersistedBytes());

        /*
         * since entrylog/Bufferedchannel is persisted (forcewritten), we should be able to read the entrylog using
         * newEntryLogger
         */
        conf.setEntryLogPerLedgerEnabled(false);
        EntryLogger newEntryLogger = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManager newEntryLogManager = newEntryLogger.entryLogManager;
        Assert.assertEquals("EntryLogManager class type", EntryLogger.EntryLogManagerForSingleEntryLog.class,
                newEntryLogManager.getClass());

        ByteBuf buf = newEntryLogger.readEntry(ledgerId, 0L, entry0Position);
        long readLedgerId = buf.readLong();
        long readEntryId = buf.readLong();
        Assert.assertEquals("LedgerId", ledgerId, readLedgerId);
        Assert.assertEquals("EntryId", 0L, readEntryId);

        buf = newEntryLogger.readEntry(ledgerId, 1L, entry1Position);
        readLedgerId = buf.readLong();
        readEntryId = buf.readLong();
        Assert.assertEquals("LedgerId", ledgerId, readLedgerId);
        Assert.assertEquals("EntryId", 1L, readEntryId);
    }
}
