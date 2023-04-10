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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException.OperationRejectedException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link DbLedgerStorage}.
 */
public class DbLedgerStorageWriteCacheTest {

    private DbLedgerStorage storage;
    private File tmpDir;

    private static class MockedDbLedgerStorage extends DbLedgerStorage {

        @Override
        protected SingleDirectoryDbLedgerStorage newSingleDirectoryDbLedgerStorage(ServerConfiguration conf,
            LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
            EntryLogger entryLogger, StatsLogger statsLogger,
            long writeCacheSize, long readCacheSize, int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize)
                throws IOException {
            return new MockedSingleDirectoryDbLedgerStorage(conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                entryLogger, statsLogger, allocator, writeCacheSize,
                readCacheSize, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
        }

        private static class MockedSingleDirectoryDbLedgerStorage extends SingleDirectoryDbLedgerStorage {
            public MockedSingleDirectoryDbLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                    LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager, EntryLogger entryLogger,
                    StatsLogger statsLogger,
                    ByteBufAllocator allocator, long writeCacheSize,
                    long readCacheSize, int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize)
                    throws IOException {
                super(conf, ledgerManager, ledgerDirsManager, indexDirsManager, entryLogger,
                      statsLogger, allocator, writeCacheSize, readCacheSize, readAheadCacheBatchSize,
                      readAheadCacheBatchBytesSize);
            }

          @Override
          public void flush() throws IOException {
              flushMutex.lock();
              try {
                  // Swap the write caches and block indefinitely to simulate a slow disk
                  WriteCache tmp = writeCacheBeingFlushed;
                  writeCacheBeingFlushed = writeCache;
                  writeCache = tmp;

                  // since the cache is switched, we can allow flush to be triggered
                  hasFlushBeenTriggered.set(false);

                  // Block the flushing thread
                  try {
                      Thread.sleep(1000);
                  } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      return;
                  }
              } finally {
                  flushMutex.unlock();
              }
          }
        }
    }

    @Before
    public void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(MockedDbLedgerStorage.class.getName());
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 1);
        conf.setProperty(DbLedgerStorage.MAX_THROTTLE_TIME_MILLIS, 1000);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        Bookie bookie = new TestBookieImpl(conf);

        storage = (DbLedgerStorage) bookie.getLedgerStorage();
    }

    @After
    public void teardown() throws Exception {
        storage.shutdown();
        tmpDir.delete();
    }

    @Test
    public void writeCacheFull() throws Exception {
        storage.setMasterKey(4, "key".getBytes());
        assertEquals(false, storage.isFenced(4));
        assertEquals(true, storage.ledgerExists(4));

        assertEquals("key", new String(storage.readMasterKey(4)));

        // Add enough entries to fill the 1st write cache
        for (int i = 0; i < 5; i++) {
            ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
            entry.writeLong(4); // ledger id
            entry.writeLong(i); // entry id
            entry.writeZero(100 * 1024);
            storage.addEntry(entry);
        }

        for (int i = 0; i < 5; i++) {
            ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
            entry.writeLong(4); // ledger id
            entry.writeLong(5 + i); // entry id
            entry.writeZero(100 * 1024);
            storage.addEntry(entry);
        }

        // Next add should fail for cache full
        ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
        entry.writeLong(4); // ledger id
        entry.writeLong(22); // entry id
        entry.writeZero(100 * 1024);

        try {
            storage.addEntry(entry);
            fail("Should have thrown exception");
        } catch (OperationRejectedException e) {
            // Expected
        }
    }
}
