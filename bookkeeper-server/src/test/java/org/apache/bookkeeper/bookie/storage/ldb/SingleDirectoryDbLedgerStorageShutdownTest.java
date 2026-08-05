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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.EntryLogWriteException;
import org.apache.bookkeeper.bookie.GarbageCollectorThread;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests shutdown cleanup for {@link SingleDirectoryDbLedgerStorage}.
 */
public class SingleDirectoryDbLedgerStorageShutdownTest {

    private static final long MB = 1024 * 1024;

    private File tmpDir;
    private EntryLogger entryLogger;
    private FailingFlushSingleDirectoryDbLedgerStorage storage;

    @Before
    public void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(1000);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(), diskChecker);
        LedgerDirsManager indexDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(), diskChecker);
        entryLogger = mock(EntryLogger.class);

        storage = new FailingFlushSingleDirectoryDbLedgerStorage(conf, mock(LedgerManager.class),
                ledgerDirsManager, indexDirsManager, entryLogger, NullStatsLogger.INSTANCE,
                ByteBufAllocator.DEFAULT, MB, MB, 1, 1024);
    }

    @After
    public void teardown() throws Exception {
        if (storage != null) {
            storage.shutdown();
        }
        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void shutdownContinuesCleanupAfterFlushFailure() throws Exception {
        storage.shutdown();
        verify(entryLogger).close();
        assertFalse(isGcThreadRunning());
        assertTrue(getCleanupExecutor().isShutdown());
        storage = null;
    }

    private boolean isGcThreadRunning() throws Exception {
        Field gcThreadField = SingleDirectoryDbLedgerStorage.class.getDeclaredField("gcThread");
        gcThreadField.setAccessible(true);
        GarbageCollectorThread gcThread = (GarbageCollectorThread) gcThreadField.get(storage);

        Field runningField = GarbageCollectorThread.class.getDeclaredField("running");
        runningField.setAccessible(true);
        return runningField.getBoolean(gcThread);
    }

    private ExecutorService getCleanupExecutor() throws Exception {
        Field cleanupExecutorField = SingleDirectoryDbLedgerStorage.class.getDeclaredField("cleanupExecutor");
        cleanupExecutorField.setAccessible(true);
        return (ExecutorService) cleanupExecutorField.get(storage);
    }

    private static class FailingFlushSingleDirectoryDbLedgerStorage extends SingleDirectoryDbLedgerStorage {

        FailingFlushSingleDirectoryDbLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager, EntryLogger entryLogger,
                StatsLogger statsLogger, ByteBufAllocator allocator, long writeCacheSize, long readCacheSize,
                int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize)
                throws IOException {
            super(conf, ledgerManager, ledgerDirsManager, indexDirsManager, entryLogger, statsLogger, allocator,
                    writeCacheSize, readCacheSize, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
        }

        @Override
        public void flush() throws IOException {
            throw new EntryLogWriteException("entry log flush failed", new IOException("injected"));
        }
    }
}
