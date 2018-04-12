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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import org.apache.bookkeeper.bookie.FileInfoBackingCache.CachedFileInfo;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases for IndexPersistenceMgr.
 */
public class IndexPersistenceMgrTest {

    private static final Logger logger = LoggerFactory.getLogger(IndexPersistenceMgr.class);

    ServerConfiguration conf;
    File journalDir, ledgerDir;
    LedgerDirsManager ledgerDirsManager;
    LedgerDirsMonitor ledgerMonitor;

    @Before
    public void setUp() throws Exception {
        journalDir = File.createTempFile("IndexPersistenceMgr", "Journal");
        journalDir.delete();
        journalDir.mkdir();
        ledgerDir = File.createTempFile("IndexPersistenceMgr", "Ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();
        // Create current directories
        Bookie.getCurrentDirectory(journalDir).mkdir();
        Bookie.getCurrentDirectory(ledgerDir).mkdir();

        conf = new ServerConfiguration();
        conf.setMetadataServiceUri(null);
        conf.setJournalDirName(journalDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });

        ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        ledgerMonitor = new LedgerDirsMonitor(conf,
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), ledgerDirsManager);
        ledgerMonitor.init();
    }

    @After
    public void tearDown() throws Exception {
        ledgerMonitor.shutdown();
        FileUtils.deleteDirectory(journalDir);
        FileUtils.deleteDirectory(ledgerDir);
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
                // exepcted
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
}
