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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.bookkeeper.bookie.FileInfoBackingCache.CachedFileInfo;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for IndexPersistenceMgr.
 */
public class IndexPersistenceMgrTest {

    ServerConfiguration conf;
    File journalDir, ledgerDir1, ledgerDir2;
    LedgerDirsManager ledgerDirsManager;
    LedgerDirsMonitor ledgerMonitor;

    @Before
    public void setUp() throws Exception {
        journalDir = File.createTempFile("IndexPersistenceMgr", "Journal");
        journalDir.delete();
        journalDir.mkdir();
        ledgerDir1 = File.createTempFile("IndexPersistenceMgr", "Ledger1");
        ledgerDir1.delete();
        ledgerDir1.mkdir();
        ledgerDir2 = File.createTempFile("IndexPersistenceMgr", "Ledger2");
        ledgerDir2.delete();
        ledgerDir2.mkdir();
        // Create current directories
        BookieImpl.getCurrentDirectory(journalDir).mkdir();
        BookieImpl.getCurrentDirectory(ledgerDir1).mkdir();
        BookieImpl.getCurrentDirectory(ledgerDir2).mkdir();

        conf = new ServerConfiguration();
        conf.setMetadataServiceUri(null);
        conf.setJournalDirName(journalDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir1.getPath(), ledgerDir2.getPath() });

        ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        ledgerMonitor = new LedgerDirsMonitor(conf,
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()),
                Collections.singletonList(ledgerDirsManager));
        ledgerMonitor.init();
    }

    @After
    public void tearDown() throws Exception {
        ledgerMonitor.shutdown();
        FileUtils.deleteDirectory(journalDir);
        FileUtils.deleteDirectory(ledgerDir1);
        FileUtils.deleteDirectory(ledgerDir2);
    }

    private IndexPersistenceMgr createIndexPersistenceManager(int openFileLimit) throws Exception {
        ServerConfiguration newConf = new ServerConfiguration();
        newConf.addConfiguration(conf);
        newConf.setOpenFileLimit(openFileLimit);

        return new IndexPersistenceMgr(
                newConf.getPageSize(), newConf.getPageSize() / LedgerEntryPage.getIndexEntrySize(),
                newConf, new SnapshotMap<Long, Boolean>(), ledgerDirsManager, NullStatsLogger.INSTANCE);
    }

    private static void getNumFileInfos(IndexPersistenceMgr indexPersistenceMgr,
                                        int numFiles, byte[] masterKey) throws Exception {
        for (int i = 0; i < numFiles; i++) {
            indexPersistenceMgr.getFileInfo((long) i, masterKey);
        }
    }

    @Test
    public void testEvictFileInfoWhenUnderlyingFileExists() throws Exception {
        evictFileInfoTest(true);
    }

    @Test
    public void testEvictFileInfoWhenUnderlyingFileDoesntExist() throws Exception {
        evictFileInfoTest(false);
    }

    private void evictFileInfoTest(boolean createFile) throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = createIndexPersistenceManager(2);
        try {
            long lid = 99999L;
            byte[] masterKey = "evict-file-info".getBytes(UTF_8);
            // get file info and make sure the file created
            FileInfo fi = indexPersistenceMgr.getFileInfo(lid, masterKey);
            if (createFile) {
                fi.checkOpen(true);
            }
            fi.setFenced();

            // fill up the cache to evict file infos
            getNumFileInfos(indexPersistenceMgr, 10, masterKey);

            // get the file info again, state should have been flushed
            fi = indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertTrue("Fence bit should be persisted", fi.isFenced());
        } finally {
            indexPersistenceMgr.close();
        }
    }

    final long lid = 1L;
    final byte[] masterKey = "write".getBytes();

    @Test
    public void testGetFileInfoReadBeforeWrite() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = createIndexPersistenceManager(1);
            // get the file info for read
            try {
                indexPersistenceMgr.getFileInfo(lid, null);
                fail("Should fail get file info for reading if the file doesn't exist");
            } catch (Bookie.NoLedgerException nle) {
                // expected
            }
            assertEquals(0, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(0, indexPersistenceMgr.readFileInfoCache.size());

            CachedFileInfo writeFileInfo = indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertEquals(2, writeFileInfo.getRefCount());
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(0, indexPersistenceMgr.readFileInfoCache.size());
            writeFileInfo.release();
            assertEquals(1, writeFileInfo.getRefCount());
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test
    public void testGetFileInfoWriteBeforeRead() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = createIndexPersistenceManager(1);

            CachedFileInfo writeFileInfo = indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertEquals(2, writeFileInfo.getRefCount());
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(0, indexPersistenceMgr.readFileInfoCache.size());
            writeFileInfo.release();

            CachedFileInfo readFileInfo = indexPersistenceMgr.getFileInfo(lid, null);
            assertEquals(3, readFileInfo.getRefCount());
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(1, indexPersistenceMgr.readFileInfoCache.size());
            readFileInfo.release();
            assertEquals(2, writeFileInfo.getRefCount());
            assertEquals(2, readFileInfo.getRefCount());
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test
    public void testReadFileInfoCacheEviction() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = createIndexPersistenceManager(1);
            for (int i = 0; i < 3; i++) {
                CachedFileInfo fileInfo = indexPersistenceMgr.getFileInfo(lid + i, masterKey);
                // We need to make sure index file is created, otherwise the test case can be flaky
                fileInfo.checkOpen(true);
                fileInfo.release();

                // load into read cache also
                indexPersistenceMgr.getFileInfo(lid + i, null).release();
            }

            indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(2, indexPersistenceMgr.readFileInfoCache.size());

            // trigger file info eviction on read file info cache
            for (int i = 1; i <= 2; i++) {
                indexPersistenceMgr.getFileInfo(lid + i, null);
            }
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(2, indexPersistenceMgr.readFileInfoCache.size());

            CachedFileInfo fileInfo = indexPersistenceMgr.writeFileInfoCache.asMap().get(lid);
            assertNotNull(fileInfo);
            assertEquals(2, fileInfo.getRefCount());
            fileInfo = indexPersistenceMgr.writeFileInfoCache.asMap().get(lid + 1);
            assertNull(fileInfo);
            fileInfo = indexPersistenceMgr.writeFileInfoCache.asMap().get(lid + 2);
            assertNull(fileInfo);
            fileInfo = indexPersistenceMgr.readFileInfoCache.asMap().get(lid);
            assertNull(fileInfo);
            fileInfo = indexPersistenceMgr.readFileInfoCache.asMap().get(lid + 1);
            assertNotNull(fileInfo);
            assertEquals(2, fileInfo.getRefCount());
            fileInfo = indexPersistenceMgr.readFileInfoCache.asMap().get(lid + 2);
            assertNotNull(fileInfo);
            assertEquals(2, fileInfo.getRefCount());
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test
    public void testEvictionShouldNotAffectLongPollRead() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        Watcher<LastAddConfirmedUpdateNotification> watcher = notification -> notification.recycle();
        try {
            indexPersistenceMgr = createIndexPersistenceManager(1);
            indexPersistenceMgr.getFileInfo(lid, masterKey);
            indexPersistenceMgr.getFileInfo(lid, null);
            indexPersistenceMgr.updateLastAddConfirmed(lid, 1);
            // watch should succeed because ledger is not evicted or closed
            assertTrue(
                indexPersistenceMgr.waitForLastAddConfirmedUpdate(lid, 1, watcher));
            // now evict ledger 1 from write cache
            indexPersistenceMgr.getFileInfo(lid + 1, masterKey);
            // even if ledger 1 is evicted from write cache, watcher should still succeed
            assertTrue(
                indexPersistenceMgr.waitForLastAddConfirmedUpdate(lid, 1, watcher));
            // now evict ledger 1 from read cache
            indexPersistenceMgr.getFileInfo(lid + 2, masterKey);
            indexPersistenceMgr.getFileInfo(lid + 2, null);
            // even if ledger 1 is evicted from both cache, watcher should still succeed because it
            // will create a new FileInfo when cache miss
            assertTrue(
                indexPersistenceMgr.waitForLastAddConfirmedUpdate(lid, 1, watcher));
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test
    public void testEvictBeforeReleaseRace() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        Watcher<LastAddConfirmedUpdateNotification> watcher = notification -> notification.recycle();
        try {
            indexPersistenceMgr = createIndexPersistenceManager(1);

            indexPersistenceMgr.getFileInfo(1L, masterKey);
            indexPersistenceMgr.getFileInfo(2L, masterKey);
            indexPersistenceMgr.getFileInfo(3L, masterKey);
            indexPersistenceMgr.getFileInfo(4L, masterKey);

            CachedFileInfo fi = indexPersistenceMgr.getFileInfo(1L, masterKey);

            // trigger eviction
            indexPersistenceMgr.getFileInfo(2L, masterKey);
            indexPersistenceMgr.getFileInfo(3L, null);
            indexPersistenceMgr.getFileInfo(4L, null);

            Thread.sleep(1000);

            fi.setFenced();
            fi.release();

            assertTrue(indexPersistenceMgr.isFenced(1));
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    /*
     * In this testcase index files (FileInfos) are precreated with different
     * FileInfo header versions (FileInfo.V0 and FileInfo.V1) and it is
     * validated that the current implementation of IndexPersistenceMgr (and
     * corresponding FileInfo) is able to function as per the specifications of
     * FileInfo header version. If it is FileInfo.V0 then explicitLac is not
     * persisted and if it is FileInfo.V1 then explicitLac is persisted.
     */
    @Test
    public void testFileInfosOfVariousHeaderVersions() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = createIndexPersistenceManager(1);
            long ledgerIdWithVersionZero = 25L;
            validateFileInfo(indexPersistenceMgr, ledgerIdWithVersionZero, FileInfo.V0);

            long ledgerIdWithVersionOne = 135L;
            validateFileInfo(indexPersistenceMgr, ledgerIdWithVersionOne, FileInfo.V1);
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test
    public void testIndexFileRelocation() throws Exception {
        final long ledgerId = Integer.MAX_VALUE;
        final String ledgerName = IndexPersistenceMgr.getLedgerName(ledgerId);

        IndexPersistenceMgr indexPersistenceMgr = createIndexPersistenceManager(1);
        preCreateFileInfoForLedgerInDir1(ledgerId, FileInfo.V1);

        ledgerDirsManager.addToFilledDirs(BookieImpl.getCurrentDirectory(ledgerDir1));
        indexPersistenceMgr.flushLedgerHeader(ledgerId);

        File expectedIndexFile = new File(BookieImpl.getCurrentDirectory(ledgerDir2), ledgerName);
        CachedFileInfo fileInfo = indexPersistenceMgr.getFileInfo(ledgerId, null);
        assertTrue(fileInfo.isSameFile(expectedIndexFile));
        assertFalse(fileInfo.isDeleted());

        indexPersistenceMgr.close();

        // Test startup after clean shutdown.
        //
        // Index file should stay in original location.
        IndexPersistenceMgr indexPersistenceMgr2 = createIndexPersistenceManager(1);
        CachedFileInfo fileInfo2 = indexPersistenceMgr2.getFileInfo(ledgerId, null);
        assertTrue(fileInfo2.isSameFile(expectedIndexFile));
        indexPersistenceMgr2.close();
    }

    @Test
    public void testIndexFileRelocationCrashBeforeOriginalFileDeleted() throws Exception {
        final long ledgerId = Integer.MAX_VALUE;
        final String ledgerName = IndexPersistenceMgr.getLedgerName(ledgerId);
        final String reason = "crash before original file deleted";

        try {
            IndexPersistenceMgr indexPersistenceMgr = createIndexPersistenceManager(1);
            preCreateFileInfoForLedgerInDir1(ledgerId, FileInfo.V1);

            CachedFileInfo fileInfo = spy(indexPersistenceMgr.getFileInfo(ledgerId, null));
            doAnswer(invocation -> {
                throw new RuntimeException(reason);
            }).when(fileInfo).delete();
            indexPersistenceMgr.readFileInfoCache.put(ledgerId, fileInfo);

            ledgerDirsManager.addToFilledDirs(BookieImpl.getCurrentDirectory(ledgerDir1));
            indexPersistenceMgr.flushLedgerHeader(ledgerId);
            fail("should fail due to " + reason);
        } catch (RuntimeException ex) {
            assertEquals(reason, ex.getMessage());
        }

        // Test startup after:
        //   1. relocation file created.
        //   2. crashed with possible corrupted relocation file.
        //
        // Index file should stay in original location in this case.
        IndexPersistenceMgr indexPersistenceMgr2 = createIndexPersistenceManager(1);
        File expectedIndexFile = new File(BookieImpl.getCurrentDirectory(ledgerDir1), ledgerName);
        CachedFileInfo fileInfo2 = indexPersistenceMgr2.getFileInfo(ledgerId, null);
        assertTrue(fileInfo2.isSameFile(expectedIndexFile));
        indexPersistenceMgr2.close();
    }

    @Test
    public void testIndexFileRelocationCrashAfterOriginalFileDeleted() throws Exception {
        final long ledgerId = Integer.MAX_VALUE;
        final String ledgerName = IndexPersistenceMgr.getLedgerName(ledgerId);
        final String reason = "crash after original file deleted";

        try {
            IndexPersistenceMgr indexPersistenceMgr = createIndexPersistenceManager(1);
            preCreateFileInfoForLedgerInDir1(ledgerId, FileInfo.V1);

            CachedFileInfo fileInfo = spy(indexPersistenceMgr.getFileInfo(ledgerId, null));
            doAnswer(invocation -> {
                invocation.callRealMethod();
                throw new RuntimeException(reason);
            }).when(fileInfo).delete();
            indexPersistenceMgr.readFileInfoCache.put(ledgerId, fileInfo);

            ledgerDirsManager.addToFilledDirs(BookieImpl.getCurrentDirectory(ledgerDir1));
            indexPersistenceMgr.flushLedgerHeader(ledgerId);
            fail("should fail due to " + reason);
        } catch (RuntimeException ex) {
            assertEquals(reason, ex.getMessage());
        }

        // Test startup after:
        //   1. relocation file created, filled and synced.
        //   2. original index file deleted.
        //   3. crashed.
        //
        // Index file should stay in new location in this case.
        IndexPersistenceMgr indexPersistenceMgr2 = createIndexPersistenceManager(1);
        File expectedIndexFile = new File(BookieImpl.getCurrentDirectory(ledgerDir2), ledgerName);
        CachedFileInfo fileInfo2 = indexPersistenceMgr2.getFileInfo(ledgerId, null);
        assertTrue(fileInfo2.isSameFile(expectedIndexFile));
        indexPersistenceMgr2.close();
    }

    void validateFileInfo(IndexPersistenceMgr indexPersistenceMgr, long ledgerId, int headerVersion)
            throws IOException, GeneralSecurityException {
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        boolean getUseV2WireProtocol = true;

        preCreateFileInfoForLedgerInDir1(ledgerId, headerVersion);
        DigestManager digestManager = DigestManager.instantiate(ledgerId, masterKey,
                BookKeeper.DigestType.toProtoDigestType(digestType), UnpooledByteBufAllocator.DEFAULT,
                getUseV2WireProtocol);

        CachedFileInfo fileInfo = indexPersistenceMgr.getFileInfo(ledgerId, masterKey);
        fileInfo.readHeader();
        assertEquals("ExplicitLac should be null", null, fileInfo.getExplicitLac());
        assertEquals("Header Version should match with precreated fileinfos headerversion", headerVersion,
                fileInfo.headerVersion);
        assertTrue("Masterkey should match with precreated fileinfos masterkey",
                Arrays.equals(masterKey, fileInfo.masterKey));
        long explicitLac = 22;
        ByteBuf explicitLacByteBuf = digestManager.computeDigestAndPackageForSendingLac(explicitLac).getBuffer(0);
        explicitLacByteBuf.markReaderIndex();
        indexPersistenceMgr.setExplicitLac(ledgerId, explicitLacByteBuf);
        explicitLacByteBuf.resetReaderIndex();
        assertEquals("explicitLac ByteBuf contents should match", 0,
                ByteBufUtil.compare(explicitLacByteBuf, indexPersistenceMgr.getExplicitLac(ledgerId)));
        /*
         * release fileInfo until it is marked dead and closed, so that
         * contents of it are persisted.
         */
        while (fileInfo.refCount.get() != FileInfoBackingCache.DEAD_REF) {
            fileInfo.release();
        }
        /*
         * reopen the fileinfo and readHeader, so that whatever was persisted
         * would be read.
         */
        fileInfo = indexPersistenceMgr.getFileInfo(ledgerId, masterKey);
        fileInfo.readHeader();
        assertEquals("Header Version should match with precreated fileinfos headerversion even after reopening",
                headerVersion, fileInfo.headerVersion);
        assertTrue("Masterkey should match with precreated fileinfos masterkey",
                Arrays.equals(masterKey, fileInfo.masterKey));
        if (headerVersion == FileInfo.V0) {
            assertEquals("Since it is V0 Header, explicitLac will not be persisted and should be null after reopening",
                    null, indexPersistenceMgr.getExplicitLac(ledgerId));
        } else {
            explicitLacByteBuf.resetReaderIndex();
            assertEquals("Since it is V1 Header, explicitLac will be persisted and should not be null after reopening",
                    0, ByteBufUtil.compare(explicitLacByteBuf, indexPersistenceMgr.getExplicitLac(ledgerId)));
        }
    }

    void preCreateFileInfoForLedgerInDir1(long ledgerId, int headerVersion) throws IOException {
        File ledgerCurDir = BookieImpl.getCurrentDirectory(ledgerDir1);
        String ledgerName = IndexPersistenceMgr.getLedgerName(ledgerId);
        File indexFile = new File(ledgerCurDir, ledgerName);
        indexFile.getParentFile().mkdirs();
        indexFile.createNewFile();
        /*
         * precreate index file (FileInfo) for the ledger with specified
         * headerversion. Even in FileInfo.V1 case, it is valid for
         * explicitLacBufLength to be 0. If it is 0, then explicitLac is
         * considered null (not set).
         */
        try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw")) {
            FileChannel fcForIndexFile = raf.getChannel();
            ByteBuffer bb = ByteBuffer.allocate((int) FileInfo.START_OF_DATA);
            bb.putInt(FileInfo.SIGNATURE);
            bb.putInt(headerVersion);
            bb.putInt(masterKey.length);
            bb.put(masterKey);
            // statebits
            bb.putInt(0);
            if (headerVersion == FileInfo.V1) {
                // explicitLacBufLength
                bb.putInt(0);
            }
            bb.rewind();
            fcForIndexFile.position(0);
            fcForIndexFile.write(bb);
            fcForIndexFile.close();
        }
    }
}
