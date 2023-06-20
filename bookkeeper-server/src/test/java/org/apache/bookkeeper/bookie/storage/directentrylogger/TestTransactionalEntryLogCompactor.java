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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTED_SUFFIX;
import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTING_SUFFIX;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.assertEntryEquals;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.logIdFromLocation;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.makeEntry;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newDirectEntryLogger;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newDirsManager;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newLegacyEntryLogger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.MockLedgerStorage;
import org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor;
import org.apache.bookkeeper.bookie.storage.CompactionEntryLog;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.junit.After;
import org.junit.Test;

/**
 * TestTransactionalEntryLogCompactor.
 */
public class TestTransactionalEntryLogCompactor {
    private static final Slogger slog = Slogger.CONSOLE;

    private final TmpDirs tmpDirs = new TmpDirs();
    private static final long deadLedger = 1L;
    private static final long liveLedger = 2L;

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
    }

    @Test
    public void testHappyCase() throws Exception {
        File ledgerDir = tmpDirs.createNew("compactHappyCase", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long logId = writeLogData(ledgerDir);
        MockLedgerStorage ledgerStorage = new MockLedgerStorage();
        try (EntryLogger entryLogger = newDirectEntryLogger(2 << 20, ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> {});
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            assertThat(meta.containsLedger(deadLedger), equalTo(true));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + 1000 + (Integer.BYTES * 2)));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(true));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(1));
            EntryLocation loc = ledgerStorage.getUpdatedLocations().get(0);

            long compactedLogId = logIdFromLocation(loc.getLocation());
            assertThat(compactedLogId, not(equalTo(logId)));
            assertThat(loc.getLedger(), equalTo(liveLedger));
            assertThat(loc.getEntry(), equalTo(2L));

            meta = entryLogger.getEntryLogMetadata(compactedLogId);
            assertThat(meta.containsLedger(deadLedger), equalTo(false));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + Integer.BYTES));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            ByteBuf bb = entryLogger.readEntry(loc.getLedger(), loc.getEntry(), loc.getLocation());
            assertEntryEquals(bb, makeEntry(liveLedger, 2L, 1000, (byte) 0xfa));
            assertThat(entryLogger.incompleteCompactionLogs(), empty());
        }
    }

    @Test
    public void testHappyCase1000() throws Exception {
        File ledgerDir = tmpDirs.createNew("compactHappyCase1000", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long logId = writeLogData1000(ledgerDir);
        MockLedgerStorage ledgerStorage = new MockLedgerStorage();
        try (EntryLogger entryLogger = newDirectEntryLogger(2 << 20, ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> {});
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            assertThat(meta.containsLedger(deadLedger), equalTo(true));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo((1000L + Integer.BYTES) * 1000 * 2));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(true));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(1000));
            long compactedLogId = -1;
            for (int i = 0; i < 1000; i++) {
                EntryLocation loc = ledgerStorage.getUpdatedLocations().get(i);
                compactedLogId = logIdFromLocation(loc.getLocation());
                assertThat(compactedLogId, not(equalTo(logId)));
                assertThat(loc.getLedger(), equalTo(liveLedger));
                assertThat(loc.getEntry(), equalTo(Long.valueOf(i)));

                ByteBuf bb = entryLogger.readEntry(loc.getLedger(), loc.getEntry(), loc.getLocation());
                assertEntryEquals(bb, makeEntry(liveLedger, i, 1000, (byte) (0xfa + i)));
            }

            meta = entryLogger.getEntryLogMetadata(compactedLogId);
            assertThat(meta.containsLedger(deadLedger), equalTo(false));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo((1000L + Integer.BYTES) * 1000));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            assertThat(entryLogger.incompleteCompactionLogs(), empty());
        }
    }

    @Test
    public void testScanFail() throws Exception {
        File ledgerDir = tmpDirs.createNew("compactScanFail", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long logId = writeLogData(ledgerDir);
        MockLedgerStorage ledgerStorage = new MockLedgerStorage();
        try (EntryLogger entryLogger = newDirectEntryLoggerFailAdd(ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> {});
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            assertThat(meta.containsLedger(deadLedger), equalTo(true));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + 1000 + (Integer.BYTES * 2)));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(false));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(0));
            assertThat(entryLogger.incompleteCompactionLogs(), empty());

            assertThat(compactingFiles(curDir), empty());
            assertThat(compactedFiles(curDir), empty());
        }
    }

    @Test
    public void testScanFailNoAbortAndContinue() throws Exception {
        File ledgerDir = tmpDirs.createNew("compactScanFail", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long logId = writeLogData(ledgerDir);
        MockLedgerStorage ledgerStorage = new MockLedgerStorage();
        try (EntryLogger entryLogger = newDirectEntryLoggerFailAddNoAbort(ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> {});
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            assertThat(meta.containsLedger(deadLedger), equalTo(true));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + 1000 + (Integer.BYTES * 2)));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(false));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(0));
            assertThat(compactingFiles(curDir).size(), equalTo(1));
            assertThat(compactedFiles(curDir), empty());
        }

        try (EntryLogger entryLogger = newDirectEntryLogger(2 << 20, ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> {});
            compactor.cleanUpAndRecover();
            assertThat(compactingFiles(curDir), empty());
            assertThat(compactedFiles(curDir), empty());

            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(true));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(1));

            EntryLocation loc = ledgerStorage.getUpdatedLocations().get(0);

            long compactedLogId = logIdFromLocation(loc.getLocation());
            assertThat(compactedLogId, not(equalTo(logId)));
            assertThat(loc.getLedger(), equalTo(liveLedger));
            assertThat(loc.getEntry(), equalTo(2L));
        }
    }

    @Test
    public void testFlushFail() throws Exception {
        File ledgerDir = tmpDirs.createNew("compactScanFail", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long logId = writeLogData(ledgerDir);
        MockLedgerStorage ledgerStorage = new MockLedgerStorage();
        try (EntryLogger entryLogger = newDirectEntryLoggerFailFlush(ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> {});
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            assertThat(meta.containsLedger(deadLedger), equalTo(true));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + 1000 + (Integer.BYTES * 2)));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(false));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(0));
            assertThat(entryLogger.incompleteCompactionLogs(), empty());

            assertThat(compactingFiles(curDir), empty());
            assertThat(compactedFiles(curDir), empty());
        }
    }

    @Test
    public void testMarkCompactFailNoAbort() throws Exception {
        File ledgerDir = tmpDirs.createNew("compactScanFail", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long logId = writeLogData(ledgerDir);
        MockLedgerStorage ledgerStorage = new MockLedgerStorage();
        try (EntryLogger entryLogger = newDirectEntryLoggerFailMarkCompactedNoAbort(ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> {});
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            assertThat(meta.containsLedger(deadLedger), equalTo(true));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + 1000 + (Integer.BYTES * 2)));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(false));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(0));
            assertThat(compactingFiles(curDir), empty());
            assertThat(compactedFiles(curDir), hasSize(1));
        }

        try (EntryLogger entryLogger = newDirectEntryLogger(2 << 20, ledgerDir)) {
            assertThat(entryLogger.logExists(logId), equalTo(true));
            CompletableFuture<Long> removedId = new CompletableFuture<>();
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> removedId.complete(removedLogId));
            compactor.cleanUpAndRecover();
            assertThat(compactingFiles(curDir), empty());
            assertThat(compactedFiles(curDir), empty());

            assertThat(removedId.isDone(), equalTo(true));
            assertThat(removedId.get(), equalTo(logId));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(1));

            EntryLocation loc = ledgerStorage.getUpdatedLocations().get(0);

            long compactedLogId = logIdFromLocation(loc.getLocation());
            assertThat(compactedLogId, not(equalTo(logId)));
            assertThat(loc.getLedger(), equalTo(liveLedger));
            assertThat(loc.getEntry(), equalTo(2L));

            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(compactedLogId);
            assertThat(meta.containsLedger(deadLedger), equalTo(false));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + Integer.BYTES));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            ByteBuf bb = entryLogger.readEntry(loc.getLedger(), loc.getEntry(), loc.getLocation());
            assertEntryEquals(bb, makeEntry(liveLedger, 2L, 1000, (byte) 0xfa));
            assertThat(entryLogger.incompleteCompactionLogs(), empty());
        }
    }

    @Test
    public void testIndexFail() throws Exception {
        File ledgerDir = tmpDirs.createNew("compactScanFail", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long logId = writeLogData(ledgerDir);
        MockLedgerStorage ledgerStorageFailFlush = new MockLedgerStorage() {
                @Override
                public void flushEntriesLocationsIndex() throws IOException {
                    throw new IOException("fail on flush");
                }
            };
        try (EntryLogger entryLogger = newDirectEntryLogger(2 << 20, ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorageFailFlush,
                    (removedLogId) -> {});
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            assertThat(meta.containsLedger(deadLedger), equalTo(true));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + 1000 + (Integer.BYTES * 2)));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(false));

            assertThat(ledgerStorageFailFlush.getUpdatedLocations(), hasSize(1));
            assertThat(compactingFiles(curDir), empty());
            assertThat(compactedFiles(curDir), hasSize(1));
        }

        MockLedgerStorage ledgerStorage = new MockLedgerStorage();
        CompletableFuture<Long> removedId = new CompletableFuture<>();
        try (EntryLogger entryLogger = newDirectEntryLogger(2 << 20, ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> removedId.complete(removedLogId));
            assertThat(entryLogger.logExists(logId), equalTo(true));
            compactor.cleanUpAndRecover();
            assertThat(compactingFiles(curDir), empty());
            assertThat(compactedFiles(curDir), empty());

            assertThat(removedId.isDone(), equalTo(true));
            assertThat(removedId.get(), equalTo(logId));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(1));

            EntryLocation loc = ledgerStorage.getUpdatedLocations().get(0);

            long compactedLogId = logIdFromLocation(loc.getLocation());
            assertThat(compactedLogId, not(equalTo(logId)));
            assertThat(loc.getLedger(), equalTo(liveLedger));
            assertThat(loc.getEntry(), equalTo(2L));

            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(compactedLogId);
            assertThat(meta.containsLedger(deadLedger), equalTo(false));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo(1000L + Integer.BYTES));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));

            ByteBuf bb = entryLogger.readEntry(loc.getLedger(), loc.getEntry(), loc.getLocation());
            assertEntryEquals(bb, makeEntry(liveLedger, 2L, 1000, (byte) 0xfa));
            assertThat(entryLogger.incompleteCompactionLogs(), empty());
        }
    }

    @Test
    public void testMetadataWritten() throws Exception {
        File ledgerDir = tmpDirs.createNew("compactHappyCase", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long logId = writeLogData1000(ledgerDir);
        MockLedgerStorage ledgerStorage = new MockLedgerStorage();
        try (EntryLogger entryLogger = newDirectEntryLogger(2 << 20, ledgerDir)) {
            TransactionalEntryLogCompactor compactor = new TransactionalEntryLogCompactor(
                    new ServerConfiguration(),
                    entryLogger,
                    ledgerStorage,
                    (removedLogId) -> {});
            EntryLogMetadata meta = entryLogger.getEntryLogMetadata(logId);
            meta.removeLedgerIf((ledgerId) -> ledgerId == deadLedger);
            assertThat(compactor.compact(meta), equalTo(true));

            assertThat(ledgerStorage.getUpdatedLocations(), hasSize(1000));
            long compactedLogId = logIdFromLocation(
                    ledgerStorage.getUpdatedLocations().get(0).getLocation());

            meta = ((DirectEntryLogger) entryLogger).readEntryLogIndex(compactedLogId);
            assertThat(meta.containsLedger(deadLedger), equalTo(false));
            assertThat(meta.containsLedger(liveLedger), equalTo(true));
            assertThat(meta.getTotalSize(), equalTo((1000L + Integer.BYTES) * 1000));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));
        }
    }

    Set<File> compactingFiles(File dir) throws Exception {
        return Arrays.stream(dir.listFiles((f) -> f.getName().endsWith(COMPACTING_SUFFIX)))
            .collect(Collectors.toSet());
    }

    Set<File> compactedFiles(File dir) throws Exception {
        return Arrays.stream(dir.listFiles((f) -> f.getName().endsWith(COMPACTED_SUFFIX)))
            .collect(Collectors.toSet());
    }

    int writeLogData(File ledgerDir) throws Exception {
        try (EntryLogger entryLogger = newLegacyEntryLogger(2 << 20, ledgerDir)) {
            long loc1 = entryLogger.addEntry(deadLedger, makeEntry(deadLedger, 1L, 1000, (byte) 0xde));
            long loc2 = entryLogger.addEntry(liveLedger, makeEntry(liveLedger, 2L, 1000, (byte) 0xfa));
            assertThat(logIdFromLocation(loc1), equalTo(logIdFromLocation(loc2)));
            return logIdFromLocation(loc2);
        }
    }

    int writeLogData1000(File ledgerDir) throws Exception {
        try (EntryLogger entryLogger = newDirectEntryLogger(2 << 20, ledgerDir)) {
            long loc1, loc2 = -1;
            for (int i = 0; i < 1000; i++) {
                loc1 = entryLogger.addEntry(deadLedger, makeEntry(deadLedger, i, 1000, (byte) (0xde + i)));
                if (loc2 != -1) {
                    assertThat(logIdFromLocation(loc1), equalTo(logIdFromLocation(loc2)));
                }
                loc2 = entryLogger.addEntry(liveLedger, makeEntry(liveLedger, i, 1000, (byte) (0xfa + i)));
                assertThat(logIdFromLocation(loc1), equalTo(logIdFromLocation(loc2)));
            }
            return logIdFromLocation(loc2);
        }
    }

    private static DirectEntryLogger newDirectEntryLoggerFailAdd(File ledgerDir) throws Exception {
        return newDirectEntryLoggerCompactionOverride(
                ledgerDir,
                (cel) -> new CompactionEntryLogProxy(cel) {
                        @Override
                        public long addEntry(long ledgerId, ByteBuf entry) throws IOException {
                            throw new IOException("Don't allow adds");
                        }
                    });
    }

    private static DirectEntryLogger newDirectEntryLoggerFailAddNoAbort(File ledgerDir) throws Exception {
        return newDirectEntryLoggerCompactionOverride(
                ledgerDir,
                (cel) -> new CompactionEntryLogProxy(cel) {
                        @Override
                        public long addEntry(long ledgerId, ByteBuf entry) throws IOException {
                            throw new IOException("Don't allow adds");
                        }

                        @Override
                        public void abort() {}
                    });
    }

    private static DirectEntryLogger newDirectEntryLoggerFailFlush(File ledgerDir) throws Exception {
        return newDirectEntryLoggerCompactionOverride(
                ledgerDir,
                (cel) -> new CompactionEntryLogProxy(cel) {
                        @Override
                        public void flush() throws IOException {
                            throw new IOException("No flushing");
                        }
                    });
    }

    private static DirectEntryLogger newDirectEntryLoggerFailMarkCompactedNoAbort(File ledgerDir) throws Exception {
        return newDirectEntryLoggerCompactionOverride(
                ledgerDir,
                (cel) -> new CompactionEntryLogProxy(cel) {
                        @Override
                        public void markCompacted() throws IOException {
                            super.markCompacted();
                            throw new IOException("No compact");
                        }

                        @Override
                        public void abort() {}
                    });
    }

    private static DirectEntryLogger newDirectEntryLoggerCompactionOverride(
            File ledgerDir,
            Function<CompactionEntryLog, CompactionEntryLog> override) throws Exception {
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        return new DirectEntryLogger(
                curDir, new EntryLogIdsImpl(newDirsManager(ledgerDir), slog),
                new NativeIOImpl(),
                ByteBufAllocator.DEFAULT,
                MoreExecutors.newDirectExecutorService(),
                MoreExecutors.newDirectExecutorService(),
                2 << 20, // max file size
                10 * 1024 * 1024, // max sane entry size
                1024 * 1024, // total write buffer size
                1024 * 1024, // total read buffer size
                4 * 1024, // read buffer size
                1, // numReadThreads
                300, // max fd cache time in seconds
                slog, NullStatsLogger.INSTANCE) {
            @Override
            public CompactionEntryLog newCompactionLog(long logToCompact) throws IOException {
                return override.apply(super.newCompactionLog(logToCompact));
            }
        };
    }

    private static class CompactionEntryLogProxy implements CompactionEntryLog {
        protected final CompactionEntryLog delegate;

        CompactionEntryLogProxy(CompactionEntryLog delegate) {
            this.delegate = delegate;
        }

        @Override
        public long addEntry(long ledgerId, ByteBuf entry) throws IOException {
            return delegate.addEntry(ledgerId, entry);
        }

        @Override
        public void scan(EntryLogScanner scanner) throws IOException {
            delegate.scan(scanner);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void abort() {
            delegate.abort();
        }

        @Override
        public void markCompacted() throws IOException {
            delegate.markCompacted();
        }

        @Override
        public void makeAvailable() throws IOException {
            delegate.makeAvailable();
        }

        @Override
        public void finalizeAndCleanup() {
            delegate.finalizeAndCleanup();
        }

        @Override
        public long getDstLogId() {
            return delegate.getDstLogId();
        }

        @Override
        public long getSrcLogId() {
            return delegate.getSrcLogId();
        }
    }
}
