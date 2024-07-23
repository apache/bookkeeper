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
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage.READ_AHEAD_CACHE_BATCH_BYTES_SIZE;
import static org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage.READ_AHEAD_CACHE_BATCH_SIZE;
import static org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.List;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.DefaultEntryLogger;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link DbLedgerStorage}.
 */
public class DbLedgerStorageReadCacheTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbLedgerStorageReadCacheTest.class);

    @Test
    public void chargeReadAheadCacheRegressionTest() {
        TestDB testDB = new TestDB();
        try {
            long readAheadCacheMaxSizeMb = 16L;
            int readAheadCacheBatchSize = 1024;
            long readAheadCacheBatchBytesSize = -1;
            setup(testDB, readAheadCacheMaxSizeMb, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
            SingleDirectoryDbLedgerStorage sdb = testDB.getStorage().getLedgerStorageList().get(0);
            /**
             * case1: currentReadAheadCount < readAheadCacheBatchSize
             *        currentReadAheadBytes < maxReadAheadBytesSize
             * result: true
             */
            int currentReadAheadCount = 1;
            long currentReadAheadBytes = 1;
            assertTrue(sdb.chargeReadAheadCache(currentReadAheadCount, currentReadAheadBytes));

            /**
             * case2: currentReadAheadCount > readAheadCacheBatchSize
             *        currentReadAheadBytes < maxReadAheadBytesSize
             * result: false
             */
            currentReadAheadCount = readAheadCacheBatchSize + 1;
            currentReadAheadBytes = 1;
            assertFalse(sdb.chargeReadAheadCache(currentReadAheadCount, currentReadAheadBytes));

            /**
             * case3: currentReadAheadCount < readAheadCacheBatchSize
             *        currentReadAheadBytes > maxReadAheadBytesSize
             * result: false
             */
            currentReadAheadCount = 1;
            currentReadAheadBytes = readAheadCacheMaxSizeMb / 2 * 1024 * 1024 + 1;
            assertFalse(sdb.chargeReadAheadCache(currentReadAheadCount, currentReadAheadBytes));
        } catch (Throwable e) {
            LOGGER.error("readAheadCacheBatchSizeUnitTest run error", e);
        } finally {
            teardown(testDB.getStorage(), testDB.getTmpDir());
        }
    }

    @Test
    public void chargeReadAheadCacheUnitTest() {
        TestDB testDB = new TestDB();
        try {
            long readAheadCacheMaxSizeMb = 16L;
            int readAheadCacheBatchSize = 1024;
            long readAheadCacheBatchBytesSize = 2 * 1024 * 1024;
            setup(testDB, readAheadCacheMaxSizeMb, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
            SingleDirectoryDbLedgerStorage sdb = testDB.getStorage().getLedgerStorageList().get(0);
            /**
             * case1: currentReadAheadCount < readAheadCacheBatchSize
             *        currentReadAheadBytes < readAheadCacheBatchBytesSize
             *        currentReadAheadBytes < readCacheMaxSize
             * result: true
             */
            int currentReadAheadCount = 1;
            long currentReadAheadBytes = 1;
            assertTrue(sdb.chargeReadAheadCache(currentReadAheadCount, currentReadAheadBytes));

            /**
             * case2: currentReadAheadCount > readAheadCacheBatchSize
             *        currentReadAheadBytes < readAheadCacheBatchBytesSize
             *        currentReadAheadBytes < readCacheMaxSize
             * result: false
             */
            currentReadAheadCount = readAheadCacheBatchSize + 1;
            currentReadAheadBytes = 1;
            assertFalse(sdb.chargeReadAheadCache(currentReadAheadCount, currentReadAheadBytes));

            /**
             * case3: currentReadAheadCount < readAheadCacheBatchSize
             *        currentReadAheadBytes > readAheadCacheBatchBytesSize
             *        currentReadAheadBytes < readCacheMaxSize
             * result: false
             */
            currentReadAheadCount = 1;
            currentReadAheadBytes = readAheadCacheBatchBytesSize + 1;
            assertFalse(sdb.chargeReadAheadCache(currentReadAheadCount, currentReadAheadBytes));
        } catch (Throwable e) {
            LOGGER.error("readAheadCacheBatchSizeUnitTest run error", e);
        } finally {
            teardown(testDB.getStorage(), testDB.getTmpDir());
        }
    }

    @Test
    public void compareDiffReadAheadPerfTest() {
        /**
         * case1(read ahead cache by limit bytes size):
         * config: readAheadCacheMaxSizeMb = 2 * 8;
         *         readAheadCacheBatchSize = 1024;
         *         readAheadCacheBatchBytesSize = 2 * 1024 * 1024;
         * case content:
         * LedgerId:0, read 1024 pieces of entry,each piece of entry is 10KB
         * LedgerId:1, read 1024 pieces of entry,each piece of entry is 10KB
         * LedgerId:2, read 1024 pieces of entry,each piece of entry is 10KB
         * LedgerId:3, read 1024 pieces of entry,each piece of entry is 10KB
         */
        CacheResult cacheBatchBytesSizeResult = readAheadCacheBatchBytesSize();

        /**
         * case2(read ahead cache by limit count):
         * config: readAheadCacheMaxSizeMb = 2 * 8;
         *         readAheadCacheBatchSize = 1024;
         * case content:
         * LedgerId:0, read 1024 pieces of entry,each piece of entry is 10KB
         * LedgerId:1, read 1024 pieces of entry,each piece of entry is 10KB
         * LedgerId:2, read 1024 pieces of entry,each piece of entry is 10KB
         * LedgerId:3, read 1024 pieces of entry,each piece of entry is 10KB
         */
        CacheResult cacheBatchSizeResult = readAheadCacheBatchSize();

        /**
         * result: case1(read ahead cache by limit bytes size) get less cachemiss,
         * it is suitable for large messages, reduce the pollution of readAhead large messages to readCache
         */
        assertEquals(8, cacheBatchBytesSizeResult.getCacheMissCount());
        assertEquals(132, cacheBatchSizeResult.getCacheMissCount());
        assertTrue(cacheBatchBytesSizeResult.getCacheMissCount() < cacheBatchSizeResult.getCacheMissCount());
        assertEquals(
                cacheBatchBytesSizeResult.getCacheMissCount() + cacheBatchBytesSizeResult.getCacheHitCount(),
                cacheBatchSizeResult.getCacheMissCount() + cacheBatchSizeResult.getCacheHitCount());
    }

    public void setup(TestDB testDB, long readAheadCacheMaxSizeMb,
                      int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize) throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setLedgerDirNames(new String[]{tmpDir.toString()});
        if (readAheadCacheMaxSizeMb > 0) {
            conf.setProperty(READ_AHEAD_CACHE_MAX_SIZE_MB, readAheadCacheMaxSizeMb);
        }
        if (readAheadCacheBatchSize > 0) {
            conf.setProperty(READ_AHEAD_CACHE_BATCH_SIZE, readAheadCacheBatchSize);
        }
        if (readAheadCacheBatchBytesSize > 0) {
            conf.setProperty(READ_AHEAD_CACHE_BATCH_BYTES_SIZE, readAheadCacheBatchBytesSize);
        }
        TestStatsProvider.TestStatsLogger statsLogger = new TestStatsProvider().getStatsLogger("test");
        BookieImpl bookie = new TestBookieImpl(new TestBookieImpl.ResourceBuilder(conf).build(statsLogger),
                statsLogger);

        DbLedgerStorage storage = (DbLedgerStorage) bookie.getLedgerStorage();

        storage.getLedgerStorageList().forEach(singleDirectoryDbLedgerStorage -> {
            assertTrue(singleDirectoryDbLedgerStorage.getEntryLogger() instanceof DefaultEntryLogger);
        });
        testDB.setStorage(storage);
        testDB.setTmpDir(tmpDir);
    }

    public void teardown(DbLedgerStorage storage, File tmpDir) {
        if (storage != null) {
            try {
                storage.shutdown();
            } catch (InterruptedException e) {
                LOGGER.error("storage.shutdown has error", e);
            }
        }
        if (tmpDir != null) {
            tmpDir.delete();
        }
    }

    private void addEntries(DbLedgerStorage storage, long minLedgerId, long maxLedgerId,
                            long minEntryId, long maxEntryId) throws Exception {
        // Add entries
        for (long lid = minLedgerId; lid < maxLedgerId; lid++) {
            long lac = 0;
            for (long eid = minEntryId; eid < maxEntryId; eid++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(lid); // ledger id
                entry.writeLong(eid); // entry id
                entry.writeLong(lac); // lac
                entry.writeBytes((get4KbMsg()).getBytes());
                assertEquals(eid, storage.addEntry(entry));
                lac++;
            }
        }
    }

    private String get4KbMsg() {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 1024; i++) {
            buffer.append("1234");
        }
        assertEquals(4 * 1024, buffer.toString().length());
        return buffer.toString();
    }

    private CacheResult readAheadCacheBatchBytesSize() {
        Long cacheMissCount;
        TestDB testDB = new TestDB();
        try {
            long readAheadCacheMaxSizeMb = 2 * 8L;
            int readAheadCacheBatchSize = 1024;
            long readAheadCacheBatchBytesSize = 2 * 1024 * 1024;
            long minEntryId = 0;
            long maxEntryId = 1024;

            setup(testDB, readAheadCacheMaxSizeMb, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
            addEntries(testDB.getStorage(), 0, 4, minEntryId, maxEntryId);

            testDB.getStorage().flush();
            assertEquals(false, testDB.getStorage().isFlushRequired());
            // Read from db
            for (long eid = minEntryId; eid < maxEntryId / 2; eid++) {
                testDB.getStorage().getEntry(0, eid);
                testDB.getStorage().getEntry(1, eid);
                testDB.getStorage().getEntry(2, eid);
                testDB.getStorage().getEntry(3, eid);
            }
            List<SingleDirectoryDbLedgerStorage> ledgerStorageList = testDB.getStorage().getLedgerStorageList();
            DbLedgerStorageStats ledgerStats = ledgerStorageList.get(0).getDbLedgerStorageStats();
            cacheMissCount = ledgerStats.getReadCacheMissCounter().get();
            Long cacheHitCount = ledgerStats.getReadCacheHitCounter().get();
            LOGGER.info("simple1.cacheMissCount={},cacheHitCount={}", cacheMissCount, cacheHitCount);
            return new CacheResult(cacheMissCount, cacheHitCount);
        } catch (Throwable e) {
            LOGGER.error("test case run error", e);
            return new CacheResult(0, 0);
        } finally {
            teardown(testDB.getStorage(), testDB.getTmpDir());
        }
    }

    public CacheResult readAheadCacheBatchSize() {
        Long cacheMissCount;
        TestDB testDB = new TestDB();
        try {
            long readAheadCacheMaxSizeMb = 2 * 8L;
            int readAheadCacheBatchSize = 1024;
            long readAheadCacheBatchBytesSize = -1;
            long minEntryId = 0;
            long maxEntryId = 1024;

            setup(testDB, readAheadCacheMaxSizeMb, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
            addEntries(testDB.getStorage(), 0, 4, minEntryId, maxEntryId);

            testDB.getStorage().flush();
            assertEquals(false, testDB.getStorage().isFlushRequired());
            // Read from db
            for (long eid = minEntryId; eid < maxEntryId / 2; eid++) {
                testDB.getStorage().getEntry(0, eid);
                testDB.getStorage().getEntry(1, eid);
                testDB.getStorage().getEntry(2, eid);
                testDB.getStorage().getEntry(3, eid);
            }
            List<SingleDirectoryDbLedgerStorage> ledgerStorageList = testDB.getStorage().getLedgerStorageList();
            DbLedgerStorageStats ledgerStats = ledgerStorageList.get(0).getDbLedgerStorageStats();
            cacheMissCount = ledgerStats.getReadCacheMissCounter().get();
            Long cacheHitCount = ledgerStats.getReadCacheHitCounter().get();
            LOGGER.info("simple2.cacheMissCount={},cacheHitCount={}", cacheMissCount, cacheHitCount);
            return new CacheResult(cacheMissCount, cacheHitCount);
        } catch (Throwable e) {
            LOGGER.error("test case run error", e);
            return new CacheResult(0, 0);
        } finally {
            teardown(testDB.getStorage(), testDB.getTmpDir());
        }
    }

    private class TestDB {
        private DbLedgerStorage storage;
        private File tmpDir;

        public DbLedgerStorage getStorage() {
            return storage;
        }

        public void setStorage(DbLedgerStorage storage) {
            this.storage = storage;
        }

        public File getTmpDir() {
            return tmpDir;
        }

        public void setTmpDir(File tmpDir) {
            this.tmpDir = tmpDir;
        }
    }

    private class CacheResult {
        private long cacheMissCount;
        private long cacheHitCount;

        private CacheResult(long cacheMissCount, long cacheHitCount) {
            this.cacheMissCount = cacheMissCount;
            this.cacheHitCount = cacheHitCount;
        }

        public long getCacheMissCount() {
            return cacheMissCount;
        }

        public void setCacheMissCount(long cacheMissCount) {
            this.cacheMissCount = cacheMissCount;
        }

        public long getCacheHitCount() {
            return cacheHitCount;
        }

        public void setCacheHitCount(long cacheHitCount) {
            this.cacheHitCount = cacheHitCount;
        }
    }
}