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

import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.bookie.EntryLogWriteException;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;

public class FailOnFlushDbLedgerStorage extends DbLedgerStorage {
    private static final AtomicBoolean failNextFlushWithEntryLogWriteException = new AtomicBoolean(false);
    private static final AtomicBoolean entryLogFlushFailed = new AtomicBoolean(false);

    public static void injectFailureOnNextFlush() {
        failNextFlushWithEntryLogWriteException.set(true);
    }

    public static void resetFailure() {
        failNextFlushWithEntryLogWriteException.set(false);
        entryLogFlushFailed.set(false);
    }

    @Override
    protected SingleDirectoryDbLedgerStorage newSingleDirectoryDbLedgerStorage(ServerConfiguration conf,
            LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
            EntryLogger entryLogger, StatsLogger statsLogger, long writeCacheSize, long readCacheSize,
            int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize)
            throws IOException {
        return new FailOnFlushSingleDirectoryDbLedgerStorage(conf, ledgerManager, ledgerDirsManager,
                indexDirsManager, entryLogger, statsLogger, allocator, writeCacheSize, readCacheSize,
                readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
    }

    private static class FailOnFlushSingleDirectoryDbLedgerStorage extends SingleDirectoryDbLedgerStorage {
        FailOnFlushSingleDirectoryDbLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager, EntryLogger entryLogger,
                StatsLogger statsLogger, ByteBufAllocator allocator, long writeCacheSize, long readCacheSize,
                int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize)
                throws IOException {
            super(conf, ledgerManager, ledgerDirsManager, indexDirsManager, entryLogger, statsLogger, allocator,
                    writeCacheSize, readCacheSize, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
        }

        @Override
        public void flush() throws IOException {
            if (entryLogFlushFailed.get() || failNextFlushWithEntryLogWriteException.compareAndSet(true, false)) {
                entryLogFlushFailed.set(true);
                throw new EntryLogWriteException("entry log flush failed", new IOException("injected"));
            }
            super.flush();
        }
    }
}
