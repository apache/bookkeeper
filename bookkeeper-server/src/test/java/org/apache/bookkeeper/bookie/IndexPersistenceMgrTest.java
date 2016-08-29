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

import java.io.File;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test cases for IndexPersistenceMgr
 */
public class IndexPersistenceMgrTest {

    static final Logger logger = LoggerFactory.getLogger(IndexPersistenceMgr.class);

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
        conf.setZkServers(null);
        conf.setJournalDirName(journalDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });

        ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs());
        ledgerMonitor = new LedgerDirsMonitor(conf, 
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), ledgerDirsManager);
        ledgerMonitor.init();
    }

    @After
    public void tearDown() throws Exception {
        //TODO: it is being shut down but never started. why?
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

    @Test(timeout = 60000)
    public void testEvictFileInfoWhenUnderlyingFileExists() throws Exception {
        evictFileInfoTest(true);
    }

    @Test(timeout = 60000)
    public void testEvictFileInfoWhenUnderlyingFileDoesntExist() throws Exception {
        evictFileInfoTest(false);
    }

    private void evictFileInfoTest(boolean createFile) throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = createIndexPersistenceManager(5);
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
}
