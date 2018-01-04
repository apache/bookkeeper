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

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
        logger.addEntry(1, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2, generateEntry(2, 1).nioBuffer());
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
                positions[i][j] = logger.addEntry(i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
            logger.shutdown();
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
                positions[i][j] = logger.addEntry(i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
            logger.shutdown();
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
        bookie.getLedgerDirsManager().addToFilledDirs(entryLogger.currentDir);
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
        logger.addEntry(1, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2, generateEntry(2, 1).nioBuffer());
        logger.addEntry(1, generateEntry(1, 2).nioBuffer());
        logger.rollLog();
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
        logger.addEntry(1, generateEntry(1, 1).nioBuffer());
        logger.addEntry(3, generateEntry(3, 1).nioBuffer());
        logger.addEntry(2, generateEntry(2, 1).nioBuffer());
        logger.addEntry(1, generateEntry(1, 2).nioBuffer());
        logger.rollLog();

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
     * Test Cache for logid2Channel and concurrentMap for logid2FileChannel work correctly.
     * Note that, when an entryLogger is initialized, the entry log id will increase one.
     * when the preallocation is enabled, a new entrylogger will cost 2 logId.
     */
    @Test
    public void testCacheInEntryLog() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        int expireTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        //since last access, expire after 1s
        conf.setReadChannelCacheExpireTimeMs(expireTime);
        conf.setEntryLogFilePreAllocationEnabled(false);
        // below one will cost logId 0
        Bookie bookie = new Bookie(conf);
        // create some entries
        int numLogs = 4;
        int numEntries = 10;
        long[][] positions = new long[numLogs][];
        for (int i = 0; i < numLogs; i++) {
            positions[i] = new long[numEntries];
            EntryLogger logger = new EntryLogger(conf,
                    bookie.getLedgerDirsManager());
            for (int j = 0; j < numEntries; j++) {
                positions[i][j] = logger.addEntry(i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
            LOG.info("log id is {}, LeastUnflushedLogId is {} ", logger.getCurrentLogId(),
                    logger.getLeastUnflushedLogId());
            logger.shutdown();
        }

        for (int i = 1; i < numLogs + 1; i++) {
            File logFile = new File(curDir, Long.toHexString(i) + ".log");
            assertTrue(logFile.exists());
        }

        FakeTicker t = new FakeTicker();
        FakeEntryLogger logger = new FakeEntryLogger(conf, bookie.getLedgerDirsManager(), t);
        // create some read for the entry log
        ThreadLocal<Cache<Long, BufferedReadChannel>>  cacheThreadLocal = logger.logid2Channel;
        ConcurrentMap<Long, EntryLogger.ReferenceCountedFileChannel> logid2FileChannel = logger.getLogid2FileChannel();
        for (int j = 0; j < numEntries; j++) {
            logger.readEntry(0, j, positions[0][j]);
        }
        LOG.info("cache size is {}, content is {}", cacheThreadLocal.get().size(),
                cacheThreadLocal.get().asMap().toString());
        // the cache has readChannel for 1.log
        assertNotNull(cacheThreadLocal.get().getIfPresent(1L));
        for (int j = 0; j < numEntries; j++) {
            logger.readEntry(1, j, positions[1][j]);
        }
        LOG.info("cache size is {}, content is {}", cacheThreadLocal.get().size(),
                cacheThreadLocal.get().asMap().toString());
        // the cache has readChannel for 2.log
        assertNotNull(cacheThreadLocal.get().getIfPresent(2L));

        // advance expire time
        t.advance(expireTime, TimeUnit.SECONDS);

        LOG.info("cache size is {}, content is {}", cacheThreadLocal.get().size(),
                cacheThreadLocal.get().asMap().toString());
        // read to new entry log, the old values in logid2Channel should has been invalidated
        for (int j = 0; j < numEntries; j++) {
            logger.readEntry(2, j, positions[2][j]);
        }
        for (int j = 0; j < numEntries; j++) {
            logger.readEntry(3, j, positions[3][j]);
        }
        LOG.info("cache size is {}, content is {}", cacheThreadLocal.get().size(),
                cacheThreadLocal.get().asMap().toString());
        // the cache has readChannel for 3.log
        assertNotNull(cacheThreadLocal.get().getIfPresent(3L));
        // the cache has readChannel for 4.log
        assertNotNull(cacheThreadLocal.get().getIfPresent(4L));
        // the cache hasn't readChannel for 1.log
        assertNull(cacheThreadLocal.get().getIfPresent(1L));
        // the cache hasn't readChannel for 2.log
        assertNull(cacheThreadLocal.get().getIfPresent(2L));
        // the corresponding file channel should be closed
        LOG.info("map content is {}", logid2FileChannel.toString());
        assertEquals(0, logid2FileChannel.get(1L).refCnt());
        assertEquals(0, logid2FileChannel.get(2L).refCnt());
        assertEquals(1, logid2FileChannel.get(3L).refCnt());
        assertEquals(1, logid2FileChannel.get(4L).refCnt());
        try {
            logid2FileChannel.get(1L).getFileChannel().write(ByteBuffer.allocate(5));
            fail("FileChannel has been closed, should not come here");
        } catch (ClosedChannelException exception){

        }
        try {
            logid2FileChannel.get(2L).getFileChannel().write(ByteBuffer.allocate(5));
            fail("FileChannel has been closed, should not come here");
        } catch (ClosedChannelException exception){

        }
        logger.shutdown();
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
        assertNotNull(logger.getEntryLoggerAllocator().getPreallocationFuture());

        logger.addEntry(1, generateEntry(1, 1).nioBuffer());
        // the Future<BufferedLogChannel> is not null all the time
        assertNotNull(logger.getEntryLoggerAllocator().getPreallocationFuture());

        // disable pre-allocation case
        ServerConfiguration conf2 = TestBKConfiguration.newServerConfiguration();
        conf2.setLedgerDirNames(new String[] {tmpDir.toString()});
        conf2.setEntryLogFilePreAllocationEnabled(false);
        Bookie bookie2 = new Bookie(conf2);
        // create a logger
        EntryLogger logger2 = ((InterleavedLedgerStorage) bookie2.ledgerStorage).entryLogger;
        assertNull(logger2.getEntryLoggerAllocator().getPreallocationFuture());

        logger2.addEntry(2, generateEntry(1, 1).nioBuffer());

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

        assertEquals(Sets.newHashSet(0L, 1L), logger.getEntryLogsSet());

        logger.rollLog();
        logger.flushRotatedLogs();

        assertEquals(Sets.newHashSet(0L, 1L, 2L), logger.getEntryLogsSet());

        logger.rollLog();
        logger.flushRotatedLogs();

        assertEquals(Sets.newHashSet(0L, 1L, 2L, 3L), logger.getEntryLogsSet());
    }

    /**
     * Fake Ticker to advance ticker as expected.
     */
    class FakeTicker extends Ticker {

        private final AtomicLong nanos = new AtomicLong();

        /** Advances the ticker value by {@code time} in {@code timeUnit}. */
        public FakeTicker advance(long time, TimeUnit timeUnit) {
            nanos.addAndGet(timeUnit.toNanos(time));
            return this;
        }

        @Override
        public long read() {
            long value = nanos.getAndAdd(0);
            return value;
        }
    }

    /**
     * Fake EntryLogger using customized cache to hide superclass'.
     */
    class FakeEntryLogger extends EntryLogger {
        private Ticker ticker;
        FakeEntryLogger(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
                        Ticker ticker) throws IOException{
            super(conf, ledgerDirsManager);
            this.ticker = ticker;
        }

        private final ThreadLocal<Cache<Long, BufferedReadChannel>> logid2Channel =
                new ThreadLocal<Cache<Long, BufferedReadChannel>>() {
                    @Override
                    public Cache<Long, BufferedReadChannel> initialValue() {
                        return CacheBuilder.newBuilder().concurrencyLevel(1)
                                .expireAfterAccess(getReadChannelCacheExpireTimeMs(), TimeUnit.MILLISECONDS)
                                .removalListener(removal -> getLogid2FileChannel().get(removal.getKey()).release())
                                .ticker(ticker).build(getReadChannelLoader());
                    }
                };

        public void putInReadChannels(long logId, BufferedReadChannel bc) {
            Cache<Long, BufferedReadChannel> threadCahe = logid2Channel.get();
            threadCahe.put(logId, bc);
        }

    }
}
