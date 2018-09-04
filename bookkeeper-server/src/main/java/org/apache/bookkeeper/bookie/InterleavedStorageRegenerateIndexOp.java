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


import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scan all entries in the entry log and rebuild the index file for one ledger.
 */
public class InterleavedStorageRegenerateIndexOp {
    private static final Logger LOG = LoggerFactory.getLogger(InterleavedStorageRegenerateIndexOp.class);

    private final ServerConfiguration conf;
    private final long ledgerId;

    public InterleavedStorageRegenerateIndexOp(ServerConfiguration conf, long ledgerId) {
        this.conf = conf;
        this.ledgerId = ledgerId;
    }

    public void initiate(boolean dryRun) throws IOException {
        LOG.info("Starting index rebuilding");

        DiskChecker diskChecker = Bookie.createDiskChecker(conf);
        LedgerDirsManager ledgerDirsManager = Bookie.createLedgerDirsManager(
                conf, diskChecker, NullStatsLogger.INSTANCE);
        LedgerDirsManager indexDirsManager = Bookie.createIndexDirsManager(
                conf, diskChecker,  NullStatsLogger.INSTANCE, ledgerDirsManager);
        EntryLogger entryLogger = new EntryLogger(conf, ledgerDirsManager);
        final LedgerCache ledgerCache;
        if (dryRun) {
            ledgerCache = new DryRunLedgerCache();
        } else {
            ledgerCache = new LedgerCacheImpl(conf, new SnapshotMap<Long, Boolean>(),
                                              indexDirsManager, NullStatsLogger.INSTANCE);
        }

        Set<Long> entryLogs = entryLogger.getEntryLogsSet();

        int totalEntryLogs = entryLogs.size();
        int completedEntryLogs = 0;
        long startTime = System.nanoTime();

        LOG.info("Scanning {} entry logs", totalEntryLogs);

        AtomicLong firstEntry = new AtomicLong(Long.MAX_VALUE);
        AtomicLong lastEntry = new AtomicLong(0);
        AtomicLong numEntries = new AtomicLong(0);
        for (long entryLogId : entryLogs) {
            LOG.info("Scanning {}", entryLogId);
            entryLogger.scanEntryLog(entryLogId, new EntryLogScanner() {
                @Override
                public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                    long entryId = entry.getLong(8);

                    if (entryId < firstEntry.get()) {
                        firstEntry.set(entryId);
                    }
                    if (entryId > lastEntry.get()) {
                        lastEntry.set(entryId);
                    }
                    numEntries.incrementAndGet();

                    // Actual location indexed is pointing past the entry size
                    long location = (entryLogId << 32L) | (offset + 4);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Rebuilding {}:{} at location {} / {}", ledgerId, entryId, location >> 32,
                                location & (Integer.MAX_VALUE - 1));
                    }

                    if (!ledgerCache.ledgerExists(ledgerId)) {
                        // set dummy master key and set fenced.
                        // master key is only used on write operations
                        ledgerCache.setMasterKey(ledgerId, new byte[0]);
                        ledgerCache.setFenced(ledgerId);
                    }
                    ledgerCache.putEntryOffset(ledgerId, entryId, location);
                }

                @Override
                public boolean accept(long ledgerId) {
                    return ledgerId == InterleavedStorageRegenerateIndexOp.this.ledgerId;
                }
            });

            ledgerCache.flushLedger(true);

            ++completedEntryLogs;
            LOG.info("Completed scanning of log {}.log -- {} / {}", Long.toHexString(entryLogId), completedEntryLogs,
                    totalEntryLogs);
        }

        String duration = DurationFormatUtils.formatDurationHMS(
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
        LOG.info("Rebuilding index for ledger {} is done. {} entries found, first {}, last {}. Total time: {}",
                 ledgerId, numEntries.get(), firstEntry.get(), lastEntry.get(), duration);
    }


    static class DryRunLedgerCache implements LedgerCache {
        @Override
        public void close() {
        }
        @Override
        public boolean setFenced(long ledgerId) throws IOException {
            return false;
        }
        @Override
        public boolean isFenced(long ledgerId) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        }
        @Override
        public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean ledgerExists(long ledgerId) throws IOException {
            return false;
        }
        @Override
        public void putEntryOffset(long ledger, long entry, long offset) throws IOException {
        }
        @Override
        public long getEntryOffset(long ledger, long entry) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void flushLedger(boolean doAll) throws IOException {
        }
        @Override
        public long getLastEntry(long ledgerId) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long getLastAddConfirmed(long ledgerId) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override
        public long updateLastAddConfirmed(long ledgerId, long lac) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                                     long previousLAC,
                                                     Watcher<LastAddConfirmedUpdateNotification> watcher)
                throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void deleteLedger(long ledgerId) throws IOException {
        }
        @Override
        public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        }
        @Override
        public ByteBuf getExplicitLac(long ledgerId) {
            throw new UnsupportedOperationException();
        }
    }
}
