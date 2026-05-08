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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator.OfLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 * Scan all entries in the entry log and rebuild the index file for one ledger.
 */
@CustomLog
public class InterleavedStorageRegenerateIndexOp {

    private final ServerConfiguration conf;
    private final Set<Long> ledgerIds;
    private final byte[] masterKey;

    public InterleavedStorageRegenerateIndexOp(ServerConfiguration conf, Set<Long> ledgerIds, byte[] password)
            throws NoSuchAlgorithmException {
        this.conf = conf;
        this.ledgerIds = ledgerIds;
        this.masterKey = DigestManager.generateMasterKey(password);
    }

    static class RecoveryStats {
        long firstEntry = Long.MAX_VALUE;
        long lastEntry = Long.MIN_VALUE;
        long numEntries = 0;

        void registerEntry(long entryId) {
            numEntries++;
            if (entryId < firstEntry) {
                firstEntry = entryId;
            }
            if (entryId > lastEntry) {
                lastEntry = entryId;
            }
        }

        long getNumEntries() {
            return numEntries;
        }

        long getFirstEntry() {
            return firstEntry;
        }

        long getLastEntry() {
            return lastEntry;
        }
    }

    public void initiate(boolean dryRun) throws IOException {
        log.info("Starting index rebuilding");

        DiskChecker diskChecker = BookieResources.createDiskChecker(conf);
        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                conf, diskChecker, NullStatsLogger.INSTANCE);
        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                conf, diskChecker,  NullStatsLogger.INSTANCE, ledgerDirsManager);
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, ledgerDirsManager);
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

        log.info().attr("totalEntryLogs", totalEntryLogs).log("Scanning entry logs");

        Map<Long, RecoveryStats> stats = new HashMap<>();
        for (long entryLogId : entryLogs) {
            log.info().attr("entryLogId", entryLogId).log("Scanning");
            entryLogger.scanEntryLog(entryLogId, new EntryLogScanner() {
                @Override
                public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                    long entryId = entry.getLong(8);

                    stats.computeIfAbsent(ledgerId, (ignore) -> new RecoveryStats()).registerEntry(entryId);

                    // Actual location indexed is pointing past the entry size
                    long location = (entryLogId << 32L) | (offset + 4);
                    log.debug()
                            .attr("ledgerId", ledgerId)
                            .attr("entryId", entryId)
                            .attr("32", location >> 32)
                            .attr("value", location & (Integer.MAX_VALUE - 1))
                            .log("Rebuilding entry");

                    if (!ledgerCache.ledgerExists(ledgerId)) {
                        ledgerCache.setMasterKey(ledgerId, masterKey);
                        ledgerCache.setFenced(ledgerId);
                    }
                    ledgerCache.putEntryOffset(ledgerId, entryId, location);
                }

                @Override
                public boolean accept(long ledgerId) {
                    return ledgerIds.contains(ledgerId);
                }
            });

            ledgerCache.flushLedger(true);

            ++completedEntryLogs;
            log.info()
                    .attr("value", Long.toHexString(entryLogId))
                    .attr("completedEntryLogs", completedEntryLogs)
                    .attr("totalEntryLogs", totalEntryLogs)
                    .log("Completed scanning of log .log -- /");
        }

        log.info("Rebuilding indices done");
        for (long ledgerId : ledgerIds) {
            RecoveryStats ledgerStats = stats.get(ledgerId);
            if (ledgerStats == null || ledgerStats.getNumEntries() == 0) {
                log.info().attr("ledgerId", ledgerId).log("- No entries found");
            } else {
                log.info()
                        .attr("ledgerId", ledgerId)
                        .attr("numEntries", ledgerStats.getNumEntries())
                        .attr("firstEntry", ledgerStats.getFirstEntry())
                        .attr("lastEntry", ledgerStats.getLastEntry())
                        .log("Found entries");
            }
        }
        log.info()
                .attr("value", DurationFormatUtils.formatDurationHMS(
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)))
                .log("Total time");
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
        public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
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
        @Override
        public PageEntriesIterable listEntries(long ledgerId) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override
        public LedgerIndexMetadata readLedgerIndexMetadata(long ledgerId) throws IOException {
            throw new UnsupportedOperationException();
        }
        @Override
        public OfLong getEntriesIterator(long ledgerId) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
