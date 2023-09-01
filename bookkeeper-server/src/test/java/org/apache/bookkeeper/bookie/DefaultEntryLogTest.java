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

import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.assertEntryEquals;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.makeEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.common.testing.annotations.FlakyTest;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for EntryLog.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DefaultEntryLogTest {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultEntryLogTest.class);

    final List<File> tempDirs = new ArrayList<File>();
    final Random rand = new Random();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    private File rootDir;
    private File curDir;
    private ServerConfiguration conf;
    private LedgerDirsManager dirsMgr;
    private DefaultEntryLogger entryLogger;

    @Before
    public void setUp() throws Exception {
        this.rootDir = createTempDir("bkTest", ".dir");
        this.curDir = BookieImpl.getCurrentDirectory(rootDir);
        BookieImpl.checkDirectoryStructure(curDir);
        this.conf = TestBKConfiguration.newServerConfiguration();
        this.dirsMgr = new LedgerDirsManager(
            conf,
            new File[] { rootDir },
            new DiskChecker(
                conf.getDiskUsageThreshold(),
                conf.getDiskUsageWarnThreshold()));
        this.entryLogger = new DefaultEntryLogger(conf, dirsMgr);
    }

    @After
    public void tearDown() throws Exception {
        if (null != this.entryLogger) {
            entryLogger.close();
        }

        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    @Test
    public void testDeferCreateNewLog() throws Exception {
        entryLogger.close();

        // mark `curDir` as filled
        this.conf.setMinUsableSizeForEntryLogCreation(1);
        this.dirsMgr = new LedgerDirsManager(
            conf,
            new File[] { rootDir },
            new DiskChecker(
                conf.getDiskUsageThreshold(),
                conf.getDiskUsageWarnThreshold()));
        this.dirsMgr.addToFilledDirs(curDir);

        entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        EntryLogManagerForSingleEntryLog entryLogManager =
                (EntryLogManagerForSingleEntryLog) entryLogger.getEntryLogManager();
        assertEquals(DefaultEntryLogger.UNINITIALIZED_LOG_ID, entryLogManager.getCurrentLogId());

        // add the first entry will trigger file creation
        entryLogger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        assertEquals(0L, entryLogManager.getCurrentLogId());
    }

    @Test
    public void testDeferCreateNewLogWithoutEnoughDiskSpaces() throws Exception {
        entryLogger.close();

        // mark `curDir` as filled
        this.conf.setMinUsableSizeForEntryLogCreation(Long.MAX_VALUE);
        this.dirsMgr = new LedgerDirsManager(
            conf,
            new File[] { rootDir },
            new DiskChecker(
                conf.getDiskUsageThreshold(),
                conf.getDiskUsageWarnThreshold()));
        this.dirsMgr.addToFilledDirs(curDir);

        entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        EntryLogManagerForSingleEntryLog entryLogManager =
                (EntryLogManagerForSingleEntryLog) entryLogger.getEntryLogManager();
        assertEquals(DefaultEntryLogger.UNINITIALIZED_LOG_ID, entryLogManager.getCurrentLogId());

        // add the first entry will trigger file creation
        try {
            entryLogger.addEntry(1L, generateEntry(1, 1).nioBuffer());
            fail("Should fail to append entry if there is no enough reserved space left");
        } catch (NoWritableLedgerDirException e) {
            assertEquals(DefaultEntryLogger.UNINITIALIZED_LOG_ID, entryLogManager.getCurrentLogId());
        }
    }

    @Test
    public void testCorruptEntryLog() throws Exception {
        // create some entries
        entryLogger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        entryLogger.addEntry(3L, generateEntry(3, 1).nioBuffer());
        entryLogger.addEntry(2L, generateEntry(2, 1).nioBuffer());
        entryLogger.flush();
        entryLogger.close();
        // now lets truncate the file to corrupt the last entry, which simulates a partial write
        File f = new File(curDir, "0.log");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.setLength(raf.length() - 10);
        raf.close();
        // now see which ledgers are in the log
        entryLogger = new DefaultEntryLogger(conf, dirsMgr);

        EntryLogMetadata meta = entryLogger.getEntryLogMetadata(0L);
        String metaString = meta.toString();
        assertEquals(metaString,
                "{totalSize = 60, remainingSize = 60, ledgersMap = ConcurrentLongLongHashMap{1 => 30, 3 => 30}}");
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertTrue(meta.getLedgersMap().containsKey(1L));
        assertFalse(meta.getLedgersMap().containsKey(2L));
        assertTrue(meta.getLedgersMap().containsKey(3L));
    }

    private static ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = generateDataString(ledger, entry).getBytes();
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

    private static String generateDataString(long ledger, long entry) {
        return ("ledger-" + ledger + "-" + entry);
    }

    @Test
    public void testMissingLogId() throws Exception {
        // create some entries
        int numLogs = 3;
        int numEntries = 10;
        long[][] positions = new long[2 * numLogs][];
        for (int i = 0; i < numLogs; i++) {
            positions[i] = new long[numEntries];

            DefaultEntryLogger logger = new DefaultEntryLogger(conf, dirsMgr);
            for (int j = 0; j < numEntries; j++) {
                positions[i][j] = logger.addEntry((long) i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
            logger.close();
        }
        // delete last log id
        File lastLogId = new File(curDir, "lastId");
        lastLogId.delete();

        // write another entries
        for (int i = numLogs; i < 2 * numLogs; i++) {
            positions[i] = new long[numEntries];

            DefaultEntryLogger logger = new DefaultEntryLogger(conf, dirsMgr);
            for (int j = 0; j < numEntries; j++) {
                positions[i][j] = logger.addEntry((long) i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
            logger.close();
        }

        DefaultEntryLogger newLogger = new DefaultEntryLogger(conf, dirsMgr);
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

    /**
     * Test that EntryLogger Should fail with FNFE, if entry logger directories does not exist.
     */
    @Ignore // no longer valid as LedgerDirsManager creates the directory as needed
    public void testEntryLoggerShouldThrowFNFEIfDirectoriesDoesNotExist()
            throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        DefaultEntryLogger entryLogger = null;
        try {
            entryLogger = new DefaultEntryLogger(conf, new LedgerDirsManager(conf, new File[] { tmpDir },
                    new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())));
            fail("Expecting FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertEquals("Entry log directory '" + tmpDir +  "/current' does not exist", e
                    .getLocalizedMessage());
        } finally {
            if (entryLogger != null) {
                entryLogger.close();
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
        BookieImpl bookie = new TestBookieImpl(conf);
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf,
                bookie.getLedgerDirsManager());
        InterleavedLedgerStorage ledgerStorage =
                ((InterleavedLedgerStorage) bookie.ledgerStorage.getUnderlyingLedgerStorage());
        ledgerStorage.entryLogger = entryLogger;
        // Create ledgers
        ledgerStorage.setMasterKey(1, "key".getBytes());
        ledgerStorage.setMasterKey(2, "key".getBytes());
        ledgerStorage.setMasterKey(3, "key".getBytes());
        // Add entries
        ledgerStorage.addEntry(generateEntry(1, 1));
        ledgerStorage.addEntry(generateEntry(2, 1));
        // Add entry with disk full failure simulation
        bookie.getLedgerDirsManager().addToFilledDirs(((EntryLogManagerBase) entryLogger.getEntryLogManager())
                .getCurrentLogForLedger(DefaultEntryLogger.UNASSIGNED_LEDGERID).getLogFile().getParentFile());
        ledgerStorage.addEntry(generateEntry(3, 1));
        // Verify written entries
        Assert.assertTrue(0 == generateEntry(1, 1).compareTo(ledgerStorage.getEntry(1, 1)));
        Assert.assertTrue(0 == generateEntry(2, 1).compareTo(ledgerStorage.getEntry(2, 1)));
        Assert.assertTrue(0 == generateEntry(3, 1).compareTo(ledgerStorage.getEntry(3, 1)));
    }

    /**
     * Explicitly try to recover using the ledgers map index at the end of the entry log.
     */
    @Test
    public void testRecoverFromLedgersMap() throws Exception {
        // create some entries
        entryLogger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        entryLogger.addEntry(3L, generateEntry(3, 1).nioBuffer());
        entryLogger.addEntry(2L, generateEntry(2, 1).nioBuffer());
        entryLogger.addEntry(1L, generateEntry(1, 2).nioBuffer());

        EntryLogManagerBase entryLogManager = (EntryLogManagerBase) entryLogger.getEntryLogManager();
        entryLogManager.createNewLog(DefaultEntryLogger.UNASSIGNED_LEDGERID);
        entryLogManager.flushRotatedLogs();

        EntryLogMetadata meta = entryLogger.extractEntryLogMetadataFromIndex(0L);
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertEquals(60, meta.getLedgersMap().get(1L));
        assertEquals(30, meta.getLedgersMap().get(2L));
        assertEquals(30, meta.getLedgersMap().get(3L));
        assertFalse(meta.getLedgersMap().containsKey(4L));
        assertEquals(120, meta.getTotalSize());
        assertEquals(120, meta.getRemainingSize());
    }

    /**
     * Explicitly try to recover using the ledgers map index at the end of the entry log.
     */
    @Test
    public void testRecoverFromLedgersMapOnV0EntryLog() throws Exception {
        // create some entries
        entryLogger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        entryLogger.addEntry(3L, generateEntry(3, 1).nioBuffer());
        entryLogger.addEntry(2L, generateEntry(2, 1).nioBuffer());
        entryLogger.addEntry(1L, generateEntry(1, 2).nioBuffer());
        ((EntryLogManagerBase) entryLogger.getEntryLogManager()).createNewLog(DefaultEntryLogger.UNASSIGNED_LEDGERID);
        entryLogger.close();

        // Rewrite the entry log header to be on V0 format
        File f = new File(curDir, "0.log");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(DefaultEntryLogger.HEADER_VERSION_POSITION);
        // Write zeros to indicate V0 + no ledgers map info
        raf.write(new byte[4 + 8]);
        raf.close();

        // now see which ledgers are in the log
        entryLogger = new DefaultEntryLogger(conf, dirsMgr);

        try {
            entryLogger.extractEntryLogMetadataFromIndex(0L);
            fail("Should not be possible to recover from ledgers map index");
        } catch (IOException e) {
            // Ok
        }

        // Public method should succeed by falling back to scanning the file
        EntryLogMetadata meta = entryLogger.getEntryLogMetadata(0L);
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
        entryLogger.close();

        // enable pre-allocation case
        conf.setEntryLogFilePreAllocationEnabled(true);

        entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        // create a logger whose initialization phase allocating a new entry log
        ((EntryLogManagerBase) entryLogger.getEntryLogManager()).createNewLog(DefaultEntryLogger.UNASSIGNED_LEDGERID);
        assertNotNull(entryLogger.getEntryLoggerAllocator().getPreallocationFuture());

        entryLogger.addEntry(1L, generateEntry(1, 1).nioBuffer());
        // the Future<BufferedLogChannel> is not null all the time
        assertNotNull(entryLogger.getEntryLoggerAllocator().getPreallocationFuture());
        entryLogger.close();

        // disable pre-allocation case
        conf.setEntryLogFilePreAllocationEnabled(false);
        // create a logger
        entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        assertNull(entryLogger.getEntryLoggerAllocator().getPreallocationFuture());

        entryLogger.addEntry(2L, generateEntry(1, 1).nioBuffer());

        // the Future<BufferedLogChannel> is null all the time
        assertNull(entryLogger.getEntryLoggerAllocator().getPreallocationFuture());
    }

    /**
     * Test the getEntryLogsSet() method.
     */
    @Test
    public void testGetEntryLogsSet() throws Exception {
        // create some entries
        EntryLogManagerBase entryLogManagerBase = ((EntryLogManagerBase) entryLogger.getEntryLogManager());
        assertEquals(Sets.newHashSet(), entryLogger.getEntryLogsSet());

        entryLogManagerBase.createNewLog(DefaultEntryLogger.UNASSIGNED_LEDGERID);
        entryLogManagerBase.flushRotatedLogs();

        Thread.sleep(2000);
        assertEquals(Sets.newHashSet(0L, 1L), entryLogger.getEntryLogsSet());

        entryLogManagerBase.createNewLog(DefaultEntryLogger.UNASSIGNED_LEDGERID);
        entryLogManagerBase.flushRotatedLogs();

        assertEquals(Sets.newHashSet(0L, 1L, 2L), entryLogger.getEntryLogsSet());
    }

    /**
     * In this testcase, entryLogger flush and entryLogger addEntry (which would
     * call createNewLog) are called concurrently. Since entryLogger flush
     * method flushes both currentlog and rotatedlogs, it is expected all the
     * currentLog and rotatedLogs are supposed to be flush and forcewritten.
     *
     * @throws Exception
     */
    @Test
    public void testFlushOrder() throws Exception {
        entryLogger.close();

        int logSizeLimit = 256 * 1024;
        conf.setEntryLogPerLedgerEnabled(false);
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setFlushIntervalInBytes(0);
        conf.setEntryLogSizeLimit(logSizeLimit);

        entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        EntryLogManagerBase entryLogManager = (EntryLogManagerBase) entryLogger.getEntryLogManager();
        AtomicBoolean exceptionHappened = new AtomicBoolean(false);

        CyclicBarrier barrier = new CyclicBarrier(2);
        List<BufferedLogChannel> rotatedLogChannels;
        BufferedLogChannel currentActiveChannel;

        exceptionHappened.set(false);

        /*
         * higher the number of rotated logs, it would be easier to reproduce
         * the issue regarding flush order
         */
        addEntriesAndRotateLogs(entryLogger, 30);

        rotatedLogChannels = new LinkedList<BufferedLogChannel>(entryLogManager.getRotatedLogChannels());
        currentActiveChannel = entryLogManager.getCurrentLogForLedger(DefaultEntryLogger.UNASSIGNED_LEDGERID);
        long currentActiveChannelUnpersistedBytes = currentActiveChannel.getUnpersistedBytes();

        Thread flushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    entryLogger.flush();
                } catch (InterruptedException | BrokenBarrierException | IOException e) {
                    LOG.error("Exception happened for entryLogger.flush", e);
                    exceptionHappened.set(true);
                }
            }
        });

        Thread createdNewLogThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    /*
                     * here we are adding entry of size logSizeLimit with
                     * rolllog=true, so it would create a new entrylog.
                     */
                    entryLogger.addEntry(123, generateEntry(123, 456, logSizeLimit));
                } catch (InterruptedException | BrokenBarrierException | IOException e) {
                    LOG.error("Exception happened for entryLogManager.createNewLog", e);
                    exceptionHappened.set(true);
                }
            }
        });

        /*
         * concurrently entryLogger flush and entryLogger addEntry (which would
         * call createNewLog) would be called from different threads.
         */
        flushThread.start();
        createdNewLogThread.start();
        flushThread.join();
        createdNewLogThread.join();

        Assert.assertFalse("Exception happened in one of the operation", exceptionHappened.get());

        if (conf.getFlushIntervalInBytes() > 0) {
            /*
             * if flush of the previous current channel is called then the
             * unpersistedBytes should be less than what it was before, actually
             * it would be close to zero (but when new log is created with
             * addEntry call, ledgers map will be appended at the end of entry
             * log)
             */
            Assert.assertTrue(
                    "previous currentChannel unpersistedBytes should be less than "
                            + currentActiveChannelUnpersistedBytes
                            + ", but it is actually " + currentActiveChannel.getUnpersistedBytes(),
                    currentActiveChannel.getUnpersistedBytes() < currentActiveChannelUnpersistedBytes);
        }
        for (BufferedLogChannel rotatedLogChannel : rotatedLogChannels) {
            Assert.assertEquals("previous rotated entrylog should be flushandforcewritten", 0,
                    rotatedLogChannel.getUnpersistedBytes());
        }
    }

    void addEntriesAndRotateLogs(DefaultEntryLogger entryLogger, int numOfRotations)
            throws IOException {
        EntryLogManagerBase entryLogManager = (EntryLogManagerBase) entryLogger.getEntryLogManager();
        entryLogManager.setCurrentLogForLedgerAndAddToRotate(DefaultEntryLogger.UNASSIGNED_LEDGERID, null);
        for (int i = 0; i < numOfRotations; i++) {
            addEntries(entryLogger, 10);
            entryLogManager.setCurrentLogForLedgerAndAddToRotate(DefaultEntryLogger.UNASSIGNED_LEDGERID, null);
        }
        addEntries(entryLogger, 10);
    }

    void addEntries(DefaultEntryLogger entryLogger, int noOfEntries) throws IOException {
        for (int j = 0; j < noOfEntries; j++) {
            int ledgerId = Math.abs(rand.nextInt());
            int entryId = Math.abs(rand.nextInt());
            entryLogger.addEntry(ledgerId, generateEntry(ledgerId, entryId).nioBuffer());
        }
    }

    static class LedgerStorageWriteTask implements Callable<Boolean> {
        long ledgerId;
        int entryId;
        LedgerStorage ledgerStorage;

        LedgerStorageWriteTask(long ledgerId, int entryId, LedgerStorage ledgerStorage) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.ledgerStorage = ledgerStorage;
        }

        @Override
        public Boolean call() throws IOException, BookieException {
            try {
                ledgerStorage.addEntry(generateEntry(ledgerId, entryId));
            } catch (IOException e) {
                LOG.error("Got Exception for AddEntry call. LedgerId: " + ledgerId + " entryId: " + entryId, e);
                throw new IOException("Got Exception for AddEntry call. LedgerId: " + ledgerId + " entryId: " + entryId,
                        e);
            }
            return true;
        }
    }

    static class LedgerStorageFlushTask implements Callable<Boolean> {
        LedgerStorage ledgerStorage;

        LedgerStorageFlushTask(LedgerStorage ledgerStorage) {
            this.ledgerStorage = ledgerStorage;
        }

        @Override
        public Boolean call() throws IOException {
            try {
                ledgerStorage.flush();
            } catch (IOException e) {
                LOG.error("Got Exception for flush call", e);
                throw new IOException("Got Exception for Flush call", e);
            }
            return true;
        }
    }

    static class LedgerStorageReadTask implements Callable<Boolean> {
        long ledgerId;
        int entryId;
        LedgerStorage ledgerStorage;

        LedgerStorageReadTask(long ledgerId, int entryId, LedgerStorage ledgerStorage) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.ledgerStorage = ledgerStorage;
        }

        @Override
        public Boolean call() throws IOException, BookieException {
            try {
                ByteBuf expectedByteBuf = generateEntry(ledgerId, entryId);
                ByteBuf actualByteBuf = ledgerStorage.getEntry(ledgerId, entryId);
                if (!expectedByteBuf.equals(actualByteBuf)) {
                    LOG.error("Expected Entry: {} Actual Entry: {}", expectedByteBuf.toString(Charset.defaultCharset()),
                            actualByteBuf.toString(Charset.defaultCharset()));
                    throw new IOException("Expected Entry: " + expectedByteBuf.toString(Charset.defaultCharset())
                            + " Actual Entry: " + actualByteBuf.toString(Charset.defaultCharset()));
                }
            } catch (IOException e) {
                LOG.error("Got Exception for GetEntry call. LedgerId: " + ledgerId + " entryId: " + entryId, e);
                throw new IOException("Got Exception for GetEntry call. LedgerId: " + ledgerId + " entryId: " + entryId,
                        e);
            }
            return true;
        }
    }

    /**
     * test concurrent write operations and then concurrent read operations
     * using InterleavedLedgerStorage.
     */
    @FlakyTest(value = "https://github.com/apache/bookkeeper/issues/1516")
    public void testConcurrentWriteAndReadCallsOfInterleavedLedgerStorage() throws Exception {
        testConcurrentWriteAndReadCalls(InterleavedLedgerStorage.class.getName(), false);
    }

    /**
     * test concurrent write operations and then concurrent read operations
     * using InterleavedLedgerStorage with EntryLogPerLedger enabled.
     */
    @FlakyTest(value = "https://github.com/apache/bookkeeper/issues/1516")
    public void testConcurrentWriteAndReadCallsOfInterleavedLedgerStorageWithELPLEnabled() throws Exception {
        testConcurrentWriteAndReadCalls(InterleavedLedgerStorage.class.getName(), true);
    }

    /**
     * test concurrent write operations and then concurrent read operations
     * using SortedLedgerStorage.
     */
    @FlakyTest(value = "https://github.com/apache/bookkeeper/issues/1516")
    public void testConcurrentWriteAndReadCallsOfSortedLedgerStorage() throws Exception {
        testConcurrentWriteAndReadCalls(SortedLedgerStorage.class.getName(), false);
    }

    /**
     * test concurrent write operations and then concurrent read operations
     * using SortedLedgerStorage with EntryLogPerLedger enabled.
     */
    @FlakyTest(value = "https://github.com/apache/bookkeeper/issues/1516")
    public void testConcurrentWriteAndReadCallsOfSortedLedgerStorageWithELPLEnabled() throws Exception {
        testConcurrentWriteAndReadCalls(SortedLedgerStorage.class.getName(), true);
    }

    public void testConcurrentWriteAndReadCalls(String ledgerStorageClass, boolean entryLogPerLedgerEnabled)
            throws Exception {
        File ledgerDir = createTempDir("bkTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(ledgerDir.toString());
        conf.setLedgerDirNames(new String[] { ledgerDir.getAbsolutePath()});
        conf.setLedgerStorageClass(ledgerStorageClass);
        conf.setEntryLogPerLedgerEnabled(entryLogPerLedgerEnabled);
        BookieImpl bookie = new TestBookieImpl(conf);
        CompactableLedgerStorage ledgerStorage = (CompactableLedgerStorage) bookie.ledgerStorage;
        Random rand = new Random(0);

        if (ledgerStorageClass.equals(SortedLedgerStorage.class.getName())) {
            Assert.assertEquals("LedgerStorage Class", SortedLedgerStorage.class, ledgerStorage.getClass());
            if (entryLogPerLedgerEnabled) {
                Assert.assertEquals("MemTable Class", EntryMemTableWithParallelFlusher.class,
                        ((SortedLedgerStorage) ledgerStorage).memTable.getClass());
            } else {
                Assert.assertEquals("MemTable Class", EntryMemTable.class,
                        ((SortedLedgerStorage) ledgerStorage).memTable.getClass());
            }
        }

        int numOfLedgers = 70;
        int numEntries = 1500;
        // Create ledgers
        for (int i = 0; i < numOfLedgers; i++) {
            ledgerStorage.setMasterKey(i, "key".getBytes());
        }

        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Callable<Boolean>> writeAndFlushTasks = new ArrayList<Callable<Boolean>>();
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfLedgers; i++) {
                writeAndFlushTasks.add(new LedgerStorageWriteTask(i, j, ledgerStorage));
            }
        }

        /*
         * add some flush tasks to the list of writetasks list.
         */
        for (int i = 0; i < (numOfLedgers * numEntries) / 500; i++) {
            writeAndFlushTasks.add(rand.nextInt(writeAndFlushTasks.size()), new LedgerStorageFlushTask(ledgerStorage));
        }

        // invoke all those write/flush tasks all at once concurrently
        executor.invokeAll(writeAndFlushTasks).forEach((future) -> {
            try {
                future.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Write/Flush task failed because of InterruptedException", ie);
                Assert.fail("Write/Flush task interrupted");
            } catch (Exception ex) {
                LOG.error("Write/Flush task failed because of  exception", ex);
                Assert.fail("Write/Flush task failed " + ex.getMessage());
            }
        });

        List<Callable<Boolean>> readAndFlushTasks = new ArrayList<Callable<Boolean>>();
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfLedgers; i++) {
                readAndFlushTasks.add(new LedgerStorageReadTask(i, j, ledgerStorage));
            }
        }

        /*
         * add some flush tasks to the list of readtasks list.
         */
        for (int i = 0; i < (numOfLedgers * numEntries) / 500; i++) {
            readAndFlushTasks.add(rand.nextInt(readAndFlushTasks.size()), new LedgerStorageFlushTask(ledgerStorage));
        }

        // invoke all those read/flush tasks all at once concurrently
        executor.invokeAll(readAndFlushTasks).forEach((future) -> {
            try {
                future.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Read/Flush task failed because of InterruptedException", ie);
                Assert.fail("Read/Flush task interrupted");
            } catch (Exception ex) {
                LOG.error("Read/Flush task failed because of  exception", ex);
                Assert.fail("Read/Flush task failed " + ex.getMessage());
            }
        });

        executor.shutdownNow();
    }

    /**
     * Test to verify the leastUnflushedLogId logic in EntryLogsStatus.
     */
    @Test
    public void testEntryLoggersRecentEntryLogsStatus() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedLogsStatus = entryLogger.recentlyCreatedEntryLogsStatus;

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

    String[] createAndGetLedgerDirs(int numOfLedgerDirs) throws IOException {
        File ledgerDir;
        File curDir;
        String[] ledgerDirsPath = new String[numOfLedgerDirs];
        for (int i = 0; i < numOfLedgerDirs; i++) {
            ledgerDir = createTempDir("bkTest", ".dir");
            curDir = BookieImpl.getCurrentDirectory(ledgerDir);
            BookieImpl.checkDirectoryStructure(curDir);
            ledgerDirsPath[i] = ledgerDir.getAbsolutePath();
        }
        return ledgerDirsPath;
    }

    /*
     * test for validating if the EntryLog/BufferedChannel flushes/forcewrite if the bytes written to it are more than
     * flushIntervalInBytes
     */
    @Test
    public void testFlushIntervalInBytes() throws Exception {
        long flushIntervalInBytes = 5000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setFlushIntervalInBytes(flushIntervalInBytes);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManagerBase = ((EntryLogManagerBase) entryLogger.getEntryLogManager());

        /*
         * when entryLogger is created Header of length EntryLogger.LOGFILE_HEADER_SIZE is created
         */
        long ledgerId = 0L;
        int firstEntrySize = 1000;
        long entry0Position = entryLogger.addEntry(0L, generateEntry(ledgerId, 0L, firstEntrySize));
        // entrylogger writes length of the entry (4 bytes) before writing entry
        long expectedUnpersistedBytes = DefaultEntryLogger.LOGFILE_HEADER_SIZE + firstEntrySize + 4;
        Assert.assertEquals("Unpersisted Bytes of entrylog", expectedUnpersistedBytes,
                entryLogManagerBase.getCurrentLogForLedger(ledgerId).getUnpersistedBytes());

        /*
         * 'flushIntervalInBytes' number of bytes are flushed so BufferedChannel should be forcewritten
         */
        int secondEntrySize = (int) (flushIntervalInBytes - expectedUnpersistedBytes);
        long entry1Position = entryLogger.addEntry(0L, generateEntry(ledgerId, 1L, secondEntrySize));
        Assert.assertEquals("Unpersisted Bytes of entrylog", 0,
                entryLogManagerBase.getCurrentLogForLedger(ledgerId).getUnpersistedBytes());

        /*
         * since entrylog/Bufferedchannel is persisted (forcewritten), we should be able to read the entrylog using
         * newEntryLogger
         */
        conf.setEntryLogPerLedgerEnabled(false);
        DefaultEntryLogger newEntryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManager newEntryLogManager = newEntryLogger.getEntryLogManager();
        Assert.assertEquals("EntryLogManager class type", EntryLogManagerForSingleEntryLog.class,
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

    @Test
    public void testReadEntryWithoutLedgerID() throws Exception {
        List<Long> locations = new ArrayList<>();
        // `+ 1` is not a typo: create one more log file than the max number of o cached readers
        for (int i = 0; i < 10; i++) {
            ByteBuf e = makeEntry(1L, i, 100);
            long loc = entryLogger.addEntry(1L, e.slice());
            locations.add(loc);
        }
        entryLogger.flush();
        for (Long loc : locations) {
            int i = locations.indexOf(loc);
            ByteBuf data = entryLogger.readEntry(loc);
            assertEntryEquals(data, makeEntry(1L, i, 100));
            long readLedgerId = data.readLong();
            long readEntryId = data.readLong();
            Assert.assertEquals("LedgerId", 1L, readLedgerId);
            Assert.assertEquals("EntryId", i, readEntryId);
            ReferenceCountUtil.release(data);
        }
    }


    /*
     * tests basic logic of EntryLogManager interface for
     * EntryLogManagerForEntryLogPerLedger.
     */
    @Test
    public void testEntryLogManagerInterfaceForEntryLogPerLedger() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger) entryLogger
                .getEntryLogManager();

        Assert.assertEquals("Number of current active EntryLogs ", 0, entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of Rotated Logs ", 0, entryLogManager.getRotatedLogChannels().size());

        int numOfLedgers = 5;
        int numOfThreadsPerLedger = 10;
        validateLockAcquireAndRelease(numOfLedgers, numOfThreadsPerLedger, entryLogManager);

        for (long i = 0; i < numOfLedgers; i++) {
            entryLogManager.setCurrentLogForLedgerAndAddToRotate(i,
                    createDummyBufferedLogChannel(entryLogger, i, conf));
        }

        for (long i = 0; i < numOfLedgers; i++) {
            Assert.assertEquals("LogChannel for ledger: " + i, entryLogManager.getCurrentLogIfPresent(i),
                    entryLogManager.getCurrentLogForLedger(i));
        }

        Assert.assertEquals("Number of current active EntryLogs ", numOfLedgers,
                entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of Rotated Logs ", 0, entryLogManager.getRotatedLogChannels().size());

        for (long i = 0; i < numOfLedgers; i++) {
            entryLogManager.setCurrentLogForLedgerAndAddToRotate(i,
                    createDummyBufferedLogChannel(entryLogger, numOfLedgers + i, conf));
        }

        /*
         * since new entryLogs are set for all the ledgers, previous entrylogs would be added to rotatedLogChannels
         */
        Assert.assertEquals("Number of current active EntryLogs ", numOfLedgers,
                entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of Rotated Logs ", numOfLedgers,
                entryLogManager.getRotatedLogChannels().size());

        for (long i = 0; i < numOfLedgers; i++) {
            entryLogManager.setCurrentLogForLedgerAndAddToRotate(i,
                    createDummyBufferedLogChannel(entryLogger, 2 * numOfLedgers + i, conf));
        }

        /*
         * again since new entryLogs are set for all the ledgers, previous entrylogs would be added to
         * rotatedLogChannels
         */
        Assert.assertEquals("Number of current active EntryLogs ", numOfLedgers,
                entryLogManager.getCopyOfCurrentLogs().size());
        Assert.assertEquals("Number of Rotated Logs ", 2 * numOfLedgers,
                entryLogManager.getRotatedLogChannels().size());

        for (BufferedLogChannel logChannel : entryLogManager.getRotatedLogChannels()) {
            entryLogManager.getRotatedLogChannels().remove(logChannel);
        }
        Assert.assertEquals("Number of Rotated Logs ", 0, entryLogManager.getRotatedLogChannels().size());

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

    private DefaultEntryLogger.BufferedLogChannel createDummyBufferedLogChannel(DefaultEntryLogger entryLogger,
                                                                                long logid,
                                                                                ServerConfiguration servConf)
        throws IOException {
        File tmpFile = File.createTempFile("entrylog", logid + "");
        tmpFile.deleteOnExit();
        FileChannel fc = new RandomAccessFile(tmpFile, "rw").getChannel();
        DefaultEntryLogger.BufferedLogChannel logChannel =
            new BufferedLogChannel(UnpooledByteBufAllocator.DEFAULT, fc, 10, 10,
                logid, tmpFile, servConf.getFlushIntervalInBytes());
        return logChannel;
    }

    /*
     * validates the concurrency aspect of entryLogManager's lock
     *
     * Executor of fixedThreadPool of size 'numOfLedgers * numOfThreadsPerLedger' is created and the same number
     * of tasks are submitted to the Executor. In each task, lock of that ledger is acquired and then released.
     */
    private void validateLockAcquireAndRelease(int numOfLedgers, int numOfThreadsPerLedger,
            EntryLogManagerForEntryLogPerLedger entryLogManager) throws InterruptedException {
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
                    Lock lock = entryLogManager.getLock(ledgerId);
                    lock.lock();
                    numberOfThreadsAcquiredLock.incrementAndGet();
                    latchToWait.await();
                    lock.unlock();
                } catch (InterruptedException | IOException e) {
                    irptExceptionHappened.set(true);
                }
            });
        }

        assertEquals("Number Of Threads acquired Lock", 0, numberOfThreadsAcquiredLock.get());
        latchToStart.countDown();
        Thread.sleep(1000);
        /*
         * since there are only "numOfLedgers" ledgers, only < "numOfLedgers"
         * threads should have been able to acquire lock, because multiple
         * ledgers can end up getting same lock because their hashcode might
         * fall in the same bucket.
         *
         *
         * After acquiring the lock there must be waiting on 'latchToWait' latch
         */
        int currentNumberOfThreadsAcquiredLock = numberOfThreadsAcquiredLock.get();
        assertTrue("Number Of Threads acquired Lock " + currentNumberOfThreadsAcquiredLock,
                (currentNumberOfThreadsAcquiredLock > 0) && (currentNumberOfThreadsAcquiredLock <= numOfLedgers));
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
    @Test
    public void testEntryLogManagerExpiryRemoval() throws Exception {
        int evictionPeriod = 1;

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        conf.setEntrylogMapAccessExpiryTimeInSeconds(evictionPeriod);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager =
                (EntryLogManagerForEntryLogPerLedger) entryLogger.getEntryLogManager();

        long ledgerId = 0L;

        BufferedLogChannel logChannel = createDummyBufferedLogChannel(entryLogger, 0, conf);
        entryLogManager.setCurrentLogForLedgerAndAddToRotate(ledgerId, logChannel);

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
        Assert.assertEquals("Number of rotated EntryLogs ", 1, entryLogManager.getRotatedLogChannels().size());
        Assert.assertTrue("CopyOfRotatedLogChannels should contain the created LogChannel",
                entryLogManager.getRotatedLogChannels().contains(logChannel));

        Assert.assertTrue("since mapentry must have been evicted, it should be null",
                (entryLogManager.getCacheAsMap().get(ledgerId) == null)
                        || (entryLogManager.getCacheAsMap().get(ledgerId).getEntryLogWithDirInfo() == null));
    }

    /*
     * tests if the maximum size of cache (maximumNumberOfActiveEntryLogs) is
     * honored in EntryLogManagerForEntryLogPerLedger's cache eviction policy.
     */
    @Test
    public void testCacheMaximumSizeEvictionPolicy() throws Exception {
        entryLogger.close();
        final int cacheMaximumSize = 20;

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(1));
        conf.setMaximumNumberOfActiveEntryLogs(cacheMaximumSize);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager =
                (EntryLogManagerForEntryLogPerLedger) entryLogger.getEntryLogManager();

        for (int i = 0; i < cacheMaximumSize + 10; i++) {
            entryLogManager.createNewLog(i);
            int cacheSize = entryLogManager.getCacheAsMap().size();
            Assert.assertTrue("Cache maximum size is expected to be less than " + cacheMaximumSize
                    + " but current cacheSize is " + cacheSize, cacheSize <= cacheMaximumSize);
        }
    }

    @Test
    public void testLongLedgerIdsWithEntryLogPerLedger() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(1));
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger) entryLogger
                .getEntryLogManager();

        int numOfLedgers = 5;
        int numOfEntries = 4;
        long[][] pos = new long[numOfLedgers][numOfEntries];
        for (int i = 0; i < numOfLedgers; i++) {
            long ledgerId = Long.MAX_VALUE - i;
            entryLogManager.createNewLog(ledgerId);
            for (int entryId = 0; entryId < numOfEntries; entryId++) {
                pos[i][entryId] = entryLogger.addEntry(ledgerId, generateEntry(ledgerId, entryId).nioBuffer());
            }
        }
        /*
         * do checkpoint to make sure entrylog files are persisted
         */
        entryLogger.checkpoint();

        for (int i = 0; i < numOfLedgers; i++) {
            long ledgerId = Long.MAX_VALUE - i;
            for (int entryId = 0; entryId < numOfEntries; entryId++) {
                String expectedValue = generateDataString(ledgerId, entryId);
                ByteBuf buf = entryLogger.readEntry(ledgerId, entryId, pos[i][entryId]);
                long readLedgerId = buf.readLong();
                long readEntryId = buf.readLong();
                byte[] readData = new byte[buf.readableBytes()];
                buf.readBytes(readData);
                assertEquals("LedgerId ", ledgerId, readLedgerId);
                assertEquals("EntryId ", entryId, readEntryId);
                assertEquals("Entry Data ", expectedValue, new String(readData));
            }
        }
    }

    /*
     * when entrylog for ledger is removed from ledgerIdEntryLogMap, then
     * ledgermap should be appended to that entrylog, before moving that
     * entrylog to rotatedlogchannels.
     */
    @Test
    public void testAppendLedgersMapOnCacheRemoval() throws Exception {
        final int cacheMaximumSize = 5;

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(true);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(1));
        conf.setMaximumNumberOfActiveEntryLogs(cacheMaximumSize);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger) entryLogger
                .getEntryLogManager();

        long ledgerId = 0L;
        entryLogManager.createNewLog(ledgerId);
        int entrySize = 200;
        int numOfEntries = 4;
        for (int i = 0; i < numOfEntries; i++) {
            entryLogger.addEntry(ledgerId, generateEntry(ledgerId, i, entrySize));
        }

        BufferedLogChannel logChannelForledger = entryLogManager.getCurrentLogForLedger(ledgerId);
        long logIdOfLedger = logChannelForledger.getLogId();
        /*
         * do checkpoint to make sure entrylog files are persisted
         */
        entryLogger.checkpoint();

        try {
            entryLogger.extractEntryLogMetadataFromIndex(logIdOfLedger);
        } catch (IOException ie) {
            // expected because appendLedgersMap wouldn't have been called
        }

        /*
         * create entrylogs for more ledgers, so that ledgerIdEntryLogMap would
         * reach its limit and remove the oldest entrylog.
         */
        for (int i = 1; i <= cacheMaximumSize; i++) {
            entryLogManager.createNewLog(i);
        }
        /*
         * do checkpoint to make sure entrylog files are persisted
         */
        entryLogger.checkpoint();

        EntryLogMetadata entryLogMetadata = entryLogger.extractEntryLogMetadataFromIndex(logIdOfLedger);
        ConcurrentLongLongHashMap ledgersMap = entryLogMetadata.getLedgersMap();
        Assert.assertEquals("There should be only one entry in entryLogMetadata", 1, ledgersMap.size());
        Assert.assertTrue("Usage should be 1", Double.compare(1.0, entryLogMetadata.getUsage()) == 0);
        Assert.assertEquals("Total size of entries", (entrySize + 4) * numOfEntries, ledgersMap.get(ledgerId));
    }

    /**
     * test EntryLogManager.EntryLogManagerForEntryLogPerLedger doesn't removes
     * the ledger from its cache map if ledger's corresponding state is accessed
     * within the evictionPeriod.
     *
     * @throws Exception
     */
    @Test
    public void testExpiryRemovalByAccessingOnAnotherThread() throws Exception {
        int evictionPeriod = 1;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        conf.setEntrylogMapAccessExpiryTimeInSeconds(evictionPeriod);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager =
                (EntryLogManagerForEntryLogPerLedger) entryLogger.getEntryLogManager();

        long ledgerId = 0L;

        BufferedLogChannel newLogChannel = createDummyBufferedLogChannel(entryLogger, 1, conf);
        entryLogManager.setCurrentLogForLedgerAndAddToRotate(ledgerId, newLogChannel);

        Thread t = new Thread() {
            public void run() {
                try {
                    Thread.sleep((evictionPeriod * 1000) / 2);
                    entryLogManager.getCurrentLogForLedger(ledgerId);
                } catch (InterruptedException | IOException e) {
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
        Assert.assertEquals("Number of rotated EntryLogs ", 0, entryLogManager.getRotatedLogChannels().size());
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
    @Test
    public void testExpiryRemovalByAccessingNonCacheRelatedMethods() throws Exception {
        int evictionPeriod = 1;

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        conf.setEntrylogMapAccessExpiryTimeInSeconds(evictionPeriod);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager =
                (EntryLogManagerForEntryLogPerLedger) entryLogger.getEntryLogManager();

        long ledgerId = 0L;

        BufferedLogChannel newLogChannel = createDummyBufferedLogChannel(entryLogger, 1, conf);
        entryLogManager.setCurrentLogForLedgerAndAddToRotate(ledgerId, newLogChannel);

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
                    entryLogManager.getRotatedLogChannels();
                    entryLogManager.getCurrentLogIfPresent(newLogChannel.getLogId());
                    entryLogManager.getDirForNextEntryLog(ledgerDirsManager.getWritableLedgerDirs());
                    long newLedgerId = 100;
                    BufferedLogChannel logChannelForNewLedger =
                            createDummyBufferedLogChannel(entryLogger, newLedgerId, conf);
                    entryLogManager.setCurrentLogForLedgerAndAddToRotate(newLedgerId, logChannelForNewLedger);
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
        Assert.assertEquals("Number of rotated EntryLogs ", 1, entryLogManager.getRotatedLogChannels().size());
        Assert.assertTrue("CopyOfRotatedLogChannels should contain the created LogChannel",
                entryLogManager.getRotatedLogChannels().contains(newLogChannel));

        Assert.assertTrue("since mapentry must have been evicted, it should be null",
                (entryLogManager.getCacheAsMap().get(ledgerId) == null)
                        || (entryLogManager.getCacheAsMap().get(ledgerId).getEntryLogWithDirInfo() == null));
    }

    /*
     * testing EntryLogger functionality (addEntry/createNewLog/flush) and EntryLogManager with entryLogPerLedger
     * enabled
     */
    @Test
    public void testEntryLogManagerForEntryLogPerLedger() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setFlushIntervalInBytes(10000000);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManager = (EntryLogManagerBase) entryLogger.getEntryLogManager();
        Assert.assertEquals("EntryLogManager class type", EntryLogManagerForEntryLogPerLedger.class,
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
                    logChannel.getUnpersistedBytes() > DefaultEntryLogger.LOGFILE_HEADER_SIZE);
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            entryLogManager.createNewLog(i);
        }

        /*
         * since we created new entrylog for all the activeLedgers, entrylogs of all the ledgers
         * should be rotated and hence the size of copyOfRotatedLogChannels should be numOfActiveLedgers
         */
        List<BufferedLogChannel> rotatedLogs = entryLogManager.getRotatedLogChannels();
        Assert.assertEquals("Number of rotated entrylogs", numOfActiveLedgers, rotatedLogs.size());

        /*
         * Since newlog is created for all slots, so they are moved to rotated logs and hence unpersistedBytes of all
         * the slots should be just EntryLogger.LOGFILE_HEADER_SIZE
         *
         */
        for (long i = 0; i < numOfActiveLedgers; i++) {
            BufferedLogChannel logChannel = entryLogManager.getCurrentLogForLedger(i);
            Assert.assertEquals("unpersistedBytes should be LOGFILE_HEADER_SIZE",
                DefaultEntryLogger.LOGFILE_HEADER_SIZE, logChannel.getUnpersistedBytes());
        }

        for (int j = numEntries; j < 2 * numEntries; j++) {
            for (long i = 0; i < numOfActiveLedgers; i++) {
                entryLogger.addEntry(i, generateEntry(i, j));
            }
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            BufferedLogChannel logChannel =  entryLogManager.getCurrentLogForLedger(i);
            Assert.assertTrue("unpersistedBytes should be greater than LOGFILE_HEADER_SIZE",
                    logChannel.getUnpersistedBytes() > DefaultEntryLogger.LOGFILE_HEADER_SIZE);
        }

        Assert.assertEquals("LeastUnflushedloggerID", 0, entryLogger.getLeastUnflushedLogId());

        /*
         * here flush is called so all the rotatedLogChannels should be file closed and there shouldn't be any
         * rotatedlogchannel and also leastUnflushedLogId should be advanced to numOfActiveLedgers
         */
        entryLogger.flush();
        Assert.assertEquals("Number of rotated entrylogs", 0, entryLogManager.getRotatedLogChannels().size());
        Assert.assertEquals("LeastUnflushedloggerID", numOfActiveLedgers, entryLogger.getLeastUnflushedLogId());

        /*
         * after flush (flushCurrentLogs) unpersistedBytes should be 0.
         */
        for (long i = 0; i < numOfActiveLedgers; i++) {
            BufferedLogChannel logChannel =  entryLogManager.getCurrentLogForLedger(i);
            Assert.assertEquals("unpersistedBytes should be 0", 0L, logChannel.getUnpersistedBytes());
        }
    }

    @Test
    public void testSingleEntryLogCreateNewLog() throws Exception {
        Assert.assertTrue(entryLogger.getEntryLogManager() instanceof EntryLogManagerForSingleEntryLog);
        EntryLogManagerForSingleEntryLog singleEntryLog =
                (EntryLogManagerForSingleEntryLog) entryLogger.getEntryLogManager();
        EntryLogManagerForSingleEntryLog mockSingleEntryLog = spy(singleEntryLog);
        BufferedLogChannel activeLogChannel = mockSingleEntryLog.getCurrentLogForLedgerForAddEntry(1, 1024, true);
        Assert.assertTrue(activeLogChannel != null);

        verify(mockSingleEntryLog, times(1)).createNewLog(anyLong(), anyString());
        // `readEntryLogHardLimit` and `reachEntryLogLimit` should not call if new create log
        verify(mockSingleEntryLog, times(0)).reachEntryLogLimit(any(), anyLong());
        verify(mockSingleEntryLog, times(0)).readEntryLogHardLimit(any(), anyLong());
    }

    /*
     * with entryLogPerLedger enabled, create multiple entrylogs, add entries of ledgers and read them before and after
     * flush
     */
    @Test
    public void testReadAddCallsOfMultipleEntryLogs() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setLedgerDirNames(createAndGetLedgerDirs(2));
        // pre allocation enabled
        conf.setEntryLogFilePreAllocationEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManagerBase = ((EntryLogManagerBase) entryLogger.getEntryLogManager());

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
            entryLogManagerBase.createNewLog(i);
        }

        entryLogManagerBase.flushRotatedLogs();

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

    class ReadTask implements Callable<Boolean> {
        long ledgerId;
        int entryId;
        long position;
        DefaultEntryLogger entryLogger;

        ReadTask(long ledgerId, int entryId, long position, DefaultEntryLogger entryLogger) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.position = position;
            this.entryLogger = entryLogger;
        }

        @Override
        public Boolean call() throws IOException {
            try {
                ByteBuf expectedByteBuf = generateEntry(ledgerId, entryId);
                ByteBuf actualByteBuf = entryLogger.readEntry(ledgerId, entryId, position);
                if (!expectedByteBuf.equals(actualByteBuf)) {
                    LOG.error("Expected Entry: {} Actual Entry: {}", expectedByteBuf.toString(Charset.defaultCharset()),
                            actualByteBuf.toString(Charset.defaultCharset()));
                    throw new IOException("Expected Entry: " + expectedByteBuf.toString(Charset.defaultCharset())
                            + " Actual Entry: " + actualByteBuf.toString(Charset.defaultCharset()));
                }
            } catch (IOException e) {
                LOG.error("Got Exception for GetEntry call. LedgerId: " + ledgerId + " entryId: " + entryId, e);
                throw new IOException("Got Exception for GetEntry call. LedgerId: " + ledgerId + " entryId: " + entryId,
                        e);
            }
            return true;
        }
    }

    /*
     * test concurrent read operations of entries from flushed rotatedlogs with entryLogPerLedgerEnabled
     */
    @Test
    public void testConcurrentReadCallsAfterEntryLogsAreRotated() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setEntryLogPerLedgerEnabled(true);
        conf.setFlushIntervalInBytes(1000 * 25);
        conf.setLedgerDirNames(createAndGetLedgerDirs(3));
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        int numOfActiveLedgers = 15;
        int numEntries = 2000;
        final AtomicLongArray positions = new AtomicLongArray(numOfActiveLedgers * numEntries);
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger)
                entryLogger.getEntryLogManager();

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
            entryLogManager.createNewLog(i);
        }
        entryLogManager.flushRotatedLogs();

        // reading after flush of rotatedlogs
        ArrayList<ReadTask> readTasks = new ArrayList<ReadTask>();
        for (int i = 0; i < numOfActiveLedgers; i++) {
            for (int j = 0; j < numEntries; j++) {
                readTasks.add(new ReadTask(i, j, positions.get(i * numEntries + j), entryLogger));
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(40);
        executor.invokeAll(readTasks).forEach((future) -> {
            try {
                future.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Read/Flush task failed because of InterruptedException", ie);
                Assert.fail("Read/Flush task interrupted");
            } catch (Exception ex) {
                LOG.error("Read/Flush task failed because of  exception", ex);
                Assert.fail("Read/Flush task failed " + ex.getMessage());
            }
        });
    }

    /**
     * testcase to validate when ledgerdirs become full and eventually all
     * ledgerdirs become full. Later a ledgerdir becomes writable.
     */
    @Test
    public void testEntryLoggerAddEntryWhenLedgerDirsAreFull() throws Exception {
        int numberOfLedgerDirs = 3;
        List<File> ledgerDirs = new ArrayList<File>();
        String[] ledgerDirsPath = new String[numberOfLedgerDirs];
        List<File> curDirs = new ArrayList<File>();

        File ledgerDir;
        File curDir;
        for (int i = 0; i < numberOfLedgerDirs; i++) {
            ledgerDir = createTempDir("bkTest", ".dir").getAbsoluteFile();
            curDir = BookieImpl.getCurrentDirectory(ledgerDir);
            BookieImpl.checkDirectoryStructure(curDir);
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

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger)
                entryLogger.getEntryLogManager();
        Assert.assertEquals("EntryLogManager class type", EntryLogManagerForEntryLogPerLedger.class,
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
        Assert.assertEquals("Number of rotated logchannels", 0, entryLogManager.getRotatedLogChannels().size());

        /*
         * ledgerDirForLedger0 is added to filledDirs, for ledger0 new entrylog should not be created in
         * ledgerDirForLedger0
         */
        ledgerDirsManager.addToFilledDirs(ledgerDirForLedger0);
        addEntryAndValidateFolders(entryLogger, entryLogManager, 2, ledgerDirForLedger0, false, ledgerDirForLedger1,
                ledgerDirForLedger2);
        Assert.assertEquals("Number of rotated logchannels", 1, entryLogManager.getRotatedLogChannels().size());

        /*
         * ledgerDirForLedger1 is also added to filledDirs, so for all the ledgers new entryLogs should be in
         * ledgerDirForLedger2
         */
        ledgerDirsManager.addToFilledDirs(ledgerDirForLedger1);
        addEntryAndValidateFolders(entryLogger, entryLogManager, 3, ledgerDirForLedger2, true, ledgerDirForLedger2,
                ledgerDirForLedger2);
        Assert.assertTrue("Number of rotated logchannels", (2 <= entryLogManager.getRotatedLogChannels().size())
                && (entryLogManager.getRotatedLogChannels().size() <= 3));
        int numOfRotatedLogChannels = entryLogManager.getRotatedLogChannels().size();

        /*
         * since ledgerDirForLedger2 is added to filleddirs, all the dirs are full. If all the dirs are full then it
         * will continue to use current entrylogs for new entries instead of creating new one. So for all the ledgers
         * ledgerdirs should be same as before - ledgerDirForLedger2
         */
        ledgerDirsManager.addToFilledDirs(ledgerDirForLedger2);
        addEntryAndValidateFolders(entryLogger, entryLogManager, 4, ledgerDirForLedger2, true, ledgerDirForLedger2,
                ledgerDirForLedger2);
        Assert.assertEquals("Number of rotated logchannels", numOfRotatedLogChannels,
                entryLogManager.getRotatedLogChannels().size());

        /*
         *  ledgerDirForLedger1 is added back to writableDirs, so new entrylog for all the ledgers should be created in
         *  ledgerDirForLedger1
         */
        ledgerDirsManager.addToWritableDirs(ledgerDirForLedger1, true);
        addEntryAndValidateFolders(entryLogger, entryLogManager, 4, ledgerDirForLedger1, true, ledgerDirForLedger1,
                ledgerDirForLedger1);
        Assert.assertEquals("Number of rotated logchannels", numOfRotatedLogChannels + 3,
                entryLogManager.getRotatedLogChannels().size());
    }

    /*
     * in this method we add an entry and validate the ledgerdir of the
     * currentLogForLedger against the provided expected ledgerDirs.
     */
    void addEntryAndValidateFolders(DefaultEntryLogger entryLogger, EntryLogManagerBase entryLogManager, int entryId,
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
    @Test
    public void testSwappingEntryLogManagerFromEntryLogPerLedgerToSingle() throws Exception {
        testSwappingEntryLogManager(true, false);
    }

    /*
     * entries added using entrylogger with entryLogPerLedger disabled and the same entries are read using entrylogger
     * with entryLogPerLedger enabled
     */
    @Test
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

        DefaultEntryLogger defaultEntryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManager = (EntryLogManagerBase) defaultEntryLogger.getEntryLogManager();
        Assert.assertEquals(
                "EntryLogManager class type", initialEntryLogPerLedgerEnabled
                        ? EntryLogManagerForEntryLogPerLedger.class : EntryLogManagerForSingleEntryLog.class,
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
                positions[i][j] = defaultEntryLogger.addEntry((long) i, generateEntry(i, j));
                long entryLogId = (positions[i][j] >> 32L);
                if (initialEntryLogPerLedgerEnabled) {
                    Assert.assertEquals("EntryLogId for ledger: " + i, i, entryLogId);
                } else {
                    Assert.assertEquals("EntryLogId for ledger: " + i, 0, entryLogId);
                }
            }
        }

        for (long i = 0; i < numOfActiveLedgers; i++) {
            entryLogManager.createNewLog(i);
        }

        /**
         * since new entrylog is created for all the ledgers, the previous
         * entrylogs must be rotated and with the following flushRotatedLogs
         * call they should be forcewritten and file should be closed.
         */
        entryLogManager.flushRotatedLogs();

        /*
         * new entrylogger and entryLogManager are created with
         * 'laterEntryLogPerLedgerEnabled' conf
         */
        conf.setEntryLogPerLedgerEnabled(laterEntryLogPerLedgerEnabled);
        LedgerDirsManager newLedgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        DefaultEntryLogger newEntryLogger = new DefaultEntryLogger(conf, newLedgerDirsManager);
        EntryLogManager newEntryLogManager = newEntryLogger.getEntryLogManager();
        Assert.assertEquals("EntryLogManager class type",
                laterEntryLogPerLedgerEnabled ? EntryLogManagerForEntryLogPerLedger.class
                        : EntryLogManagerForSingleEntryLog.class,
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

}
