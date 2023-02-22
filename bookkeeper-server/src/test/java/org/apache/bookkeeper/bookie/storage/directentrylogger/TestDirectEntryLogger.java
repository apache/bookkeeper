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

import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.assertEntryEquals;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.logIdFromLocation;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.makeEntry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.MockEntryLogIds;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * TestDirectEntryLogger.
 */
@Slf4j
public class TestDirectEntryLogger {
    private final Slogger slog = Slogger.CONSOLE;

    private static final long ledgerId1 = 1234;

    private final TmpDirs tmpDirs = new TmpDirs();

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
    }

    @Test
    public void testLogRolling() throws Exception {
        File ledgerDir = tmpDirs.createNew("logRolling", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 4000);
        ByteBuf e2 = makeEntry(ledgerId1, 2L, 4000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 4000);

        try (EntryLogger elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     9000, // max file size (header + size of one entry)
                     10 * 1024 * 1024, // max sane entry size
                     1024 * 1024, // total write buffer size
                     1024 * 1024, // total read buffer size
                     64 * 1024, // read buffer size
                     1, // numReadThreads
                     300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE)) {
            long loc1 = elog.addEntry(ledgerId1, e1.slice());
            int logId1 = logIdFromLocation(loc1);
            assertThat(logId1, equalTo(1));

            long loc2 = elog.addEntry(ledgerId1, e2.slice());
            int logId2 = logIdFromLocation(loc2);
            assertThat(logId2, equalTo(2));

            long loc3 = elog.addEntry(ledgerId1, e3.slice());
            int logId3 = logIdFromLocation(loc3);
            assertThat(logId3, equalTo(3));
        }
    }

    @Test
    public void testReadLog() throws Exception {
        File ledgerDir = tmpDirs.createNew("logRolling", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 100);
        ByteBuf e2 = makeEntry(ledgerId1, 2L, 100);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 100);

        try (EntryLogger elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     200000, // max file size (header + size of one entry)
                     10 * 1024 * 1024, // max sane entry size
                     1024 * 1024, // total write buffer size
                     1024 * 1024, // total read buffer size
                     64 * 1024, // read buffer size
                     1, // numReadThreads
                     300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE)) {
            long loc1 = elog.addEntry(ledgerId1, e1.slice());
            long loc2 = elog.addEntry(ledgerId1, e2.slice());
            elog.flush();

            ByteBuf e1read = elog.readEntry(ledgerId1, 1L, loc1);
            ByteBuf e2read = elog.readEntry(ledgerId1, 2L, loc2);
            assertEntryEquals(e1read, e1);
            assertEntryEquals(e2read, e2);
            ReferenceCountUtil.release(e1read);
            ReferenceCountUtil.release(e2read);

            long loc3 = elog.addEntry(ledgerId1, e3.slice());
            elog.flush();

            ByteBuf e3read = elog.readEntry(ledgerId1, 3L, loc3);
            assertEntryEquals(e3read, e3);
            ReferenceCountUtil.release(e3read);
        }
    }

    @Test
    public void testLogReaderCleanup() throws Exception {
        File ledgerDir = tmpDirs.createNew("logRolling", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        final int entrySize = Buffer.ALIGNMENT;
        final int maxFileSize = Header.EMPTY_HEADER.length + entrySize;
        final int maxCachedReaders = 16;

        AtomicInteger outstandingReaders = new AtomicInteger(0);
        EntryLogger elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     maxFileSize,
                     10 * 1024 * 1024, // max sane entry size
                     1024 * 1024, // total write buffer size
                     maxCachedReaders * maxFileSize, // total read buffer size
                     maxFileSize, // read buffer size
                     1, // numReadThreads
                     300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE) {
                @Override
                LogReader newDirectReader(int logId) throws IOException {
                    outstandingReaders.incrementAndGet();
                    return new DirectReader(logId, logFilename(curDir, logId), ByteBufAllocator.DEFAULT,
                                            new NativeIOImpl(), Buffer.ALIGNMENT, 10 * 1024 * 1024,
                                            NullStatsLogger.INSTANCE.getOpStatsLogger("")) {
                        @Override
                        public void close() throws IOException {
                            super.close();
                            outstandingReaders.decrementAndGet();
                        }
                    };
                }
            };
        try {
            List<Long> locations = new ArrayList<>();
            // `+ 1` is not a typo: create one more log file than the max number of o cached readers
            for (int i = 0; i < maxCachedReaders + 1; i++) {
                ByteBuf e = makeEntry(ledgerId1, i, entrySize);
                long loc = elog.addEntry(ledgerId1, e.slice());
                locations.add(loc);
            }
            elog.flush();
            for (Long loc : locations) {
                ReferenceCountUtil.release(elog.readEntry(loc));
            }
            assertThat(outstandingReaders.get(), equalTo(maxCachedReaders));
        } finally {
            elog.close();
        }
        assertThat(outstandingReaders.get(), equalTo(0));
    }

    @Test
    public void testReadMetadataAndScan() throws Exception {
        File ledgerDir = tmpDirs.createNew("directCanReadAndScanMeta", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long ledgerId1 = 1L;
        long ledgerId2 = 2L;

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 1000);
        ByteBuf e2 = makeEntry(ledgerId2, 2L, 2000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 3000);

        long loc1, loc2, loc3;
        try (DirectEntryLogger elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     2 << 16, // max file size
                     10 * 1024 * 1024, // max sane entry size
                     1024 * 1024, // total write buffer size
                     1024 * 1024, // total read buffer size
                     64 * 1024, // read buffer size
                     1, // numReadThreads
                     300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE)) {
            loc1 = elog.addEntry(ledgerId1, e1);
            loc2 = elog.addEntry(ledgerId2, e2);
            loc3 = elog.addEntry(ledgerId1, e3);
        }

        try (DirectEntryLogger elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     2 << 16, // max file size
                     10 * 1024 * 1024, // max sane entry size
                     1024 * 1024, // total write buffer size
                     1024 * 1024, // total read buffer size
                     64 * 1024, // read buffer size
                     1, // numReadThreads
                     300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE)) {
            int logId = logIdFromLocation(loc1);
            assertThat(logId, equalTo(logIdFromLocation(loc2)));
            assertThat(logId, equalTo(logIdFromLocation(loc3)));

            EntryLogMetadata metaRead = elog.readEntryLogIndex(logId);
            assertThat(metaRead.getEntryLogId(), equalTo((long) logId));
            assertThat(metaRead.getTotalSize(), equalTo(1000L + 2000 + 3000 + (Integer.BYTES * 3)));
            assertThat(metaRead.getRemainingSize(), equalTo(metaRead.getTotalSize()));
            assertThat(metaRead.getLedgersMap().get(ledgerId1), equalTo(1000L + 3000L + (Integer.BYTES * 2)));
            assertThat(metaRead.getLedgersMap().get(ledgerId2), equalTo(2000L + Integer.BYTES));

            EntryLogMetadata metaScan = elog.scanEntryLogMetadata(logId, null);
            assertThat(metaScan.getEntryLogId(), equalTo((long) logId));
            assertThat(metaScan.getTotalSize(), equalTo(1000L + 2000 + 3000 + (Integer.BYTES * 3)));
            assertThat(metaScan.getRemainingSize(), equalTo(metaScan.getTotalSize()));
            assertThat(metaScan.getLedgersMap().get(ledgerId1), equalTo(1000L + 3000L + (Integer.BYTES * 2)));
            assertThat(metaScan.getLedgersMap().get(ledgerId2), equalTo(2000L + Integer.BYTES));
        }
    }

    @Test
    public void testMetadataFallback() throws Exception {
        File ledgerDir = tmpDirs.createNew("directMetaFallback", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long ledgerId1 = 1L;
        long ledgerId2 = 2L;

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 1000);
        ByteBuf e2 = makeEntry(ledgerId2, 2L, 2000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 3000);

        int maxFileSize = 1000 + 2000 + 3000 + (Integer.BYTES * 3) + 4096;
        long loc1, loc2, loc3;
        try (DirectEntryLogger writer = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     2 << 16, // max file size
                     10 * 1024 * 1024, // max sane entry size
                     1024 * 1024, // total write buffer size
                     1024 * 1024, // total read buffer size
                     64 * 1024, // read buffer size
                     1, // numReadThreads
                     300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE)) {
            loc1 = writer.addEntry(ledgerId1, e1);
            loc2 = writer.addEntry(ledgerId2, e2);
            loc3 = writer.addEntry(ledgerId1, e3);
            writer.flush();

            try (DirectEntryLogger reader = new DirectEntryLogger(
                         curDir, new MockEntryLogIds(),
                         new NativeIOImpl(),
                         ByteBufAllocator.DEFAULT,
                         MoreExecutors.newDirectExecutorService(),
                         MoreExecutors.newDirectExecutorService(),
                         2 << 16, // max file size
                         10 * 1024 * 1024, // max sane entry size
                         1024 * 1024, // total write buffer size
                         1024 * 1024, // total read buffer size
                         64 * 1024, // read buffer size
                         1, // numReadThreads
                         300, // max fd cache time in seconds
                         slog, NullStatsLogger.INSTANCE)) {
                int logId = logIdFromLocation(loc1);
                try {
                    reader.readEntryLogIndex(logId);
                    Assert.fail("Shouldn't be there");
                } catch (IOException ioe) {
                    // expected
                }

                EntryLogMetadata metaRead = reader.getEntryLogMetadata(logId); // should fail read, fallback to scan
                assertThat(metaRead.getEntryLogId(), equalTo((long) logId));
                assertThat(metaRead.getTotalSize(), equalTo(1000L + 2000 + 3000 + (Integer.BYTES * 3)));
                assertThat(metaRead.getRemainingSize(), equalTo(metaRead.getTotalSize()));
                assertThat(metaRead.getLedgersMap().get(ledgerId1), equalTo(1000L + 3000L + (Integer.BYTES * 2)));
                assertThat(metaRead.getLedgersMap().get(ledgerId2), equalTo(2000L + Integer.BYTES));
            }
        }
    }

    @Test
    public void testMetadataManyBatch() throws Exception {
        File ledgerDir = tmpDirs.createNew("directMetaManyBatches", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long lastLoc = -1;
        int ledgerCount = 11000;
        try (DirectEntryLogger writer = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     2 << 24, // max file size
                     10 * 1024 * 1024, // max sane entry size
                     32 * 1024 * 1024, // total write buffer size
                     32 * 1024 * 1024, // total read buffer size
                     16 * 1024 * 1024, // read buffer size
                     1, // numReadThreads
                     300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE)) {
            for (int i = 0; i < ledgerCount; i++) {
                long loc = writer.addEntry(i, makeEntry(i, 1L, 1000));
                if (lastLoc >= 0) {
                    assertThat(logIdFromLocation(loc), equalTo(logIdFromLocation(lastLoc)));
                }
                lastLoc = loc;
            }
            writer.flush();
        }

        try (DirectEntryLogger reader = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     2 << 20, // max file size
                     10 * 1024 * 1024, // max sane entry size
                     32 * 1024 * 1024, // total write buffer size
                     32 * 1024 * 1024, // total read buffer size
                     16 * 1024 * 1024, // read buffer size
                     1, // numReadThreads
                     300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE)) {
            int logId = logIdFromLocation(lastLoc);
            EntryLogMetadata metaRead = reader.readEntryLogIndex(logId);

            assertThat(metaRead.getEntryLogId(), equalTo((long) logId));
            assertThat(metaRead.getTotalSize(), equalTo((1000L + Integer.BYTES) * ledgerCount));
            assertThat(metaRead.getRemainingSize(), equalTo(metaRead.getTotalSize()));
            for (int i = 0; i < ledgerCount; i++) {
                assertThat(metaRead.getLedgersMap().containsKey(i), equalTo(true));
            }
        }
    }

    @Test
    public void testGetFlushedLogs() throws Exception {
        File ledgerDir = tmpDirs.createNew("testFlushedLogs", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ExecutorService executor = Executors.newFixedThreadPool(5);
        CompletableFuture<Void> blockClose = new CompletableFuture<>();
        NativeIOImpl nativeIO = new NativeIOImpl() {
                @Override
                public int close(int fd) {
                    try {
                        blockClose.join();
                        return super.close(fd);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

            };
        DirectEntryLogger entryLogger = new DirectEntryLogger(
                curDir, new MockEntryLogIds(),
                nativeIO,
                ByteBufAllocator.DEFAULT,
                executor,
                executor,
                23000, // max file size
                10 * 1024 * 1024, // max sane entry size
                1024 * 1024, // total write buffer size
                1024 * 1024, // total read buffer size
                32 * 1024, // read buffer size
                1, // numReadThreads
                300, // max fd cache time in seconds
                slog, NullStatsLogger.INSTANCE);
        try { // not using try-with-resources because close needs to be unblocked in failure
            // Add entries.
            // Ledger 1 is on first entry log
            // Ledger 2 spans first, second and third entry log
            // Ledger 3 is on the third entry log (which is still active when extract meta)
            long loc1 = entryLogger.addEntry(1L, makeEntry(1L, 1L, 5000));
            long loc2 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 5000));
            assertThat(logIdFromLocation(loc2), equalTo(logIdFromLocation(loc1)));
            long loc3 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 15000));
            assertThat(logIdFromLocation(loc3), greaterThan(logIdFromLocation(loc2)));
            long loc4 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 15000));
            assertThat(logIdFromLocation(loc4), greaterThan(logIdFromLocation(loc3)));
            long loc5 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 1000));
            assertThat(logIdFromLocation(loc5), equalTo(logIdFromLocation(loc4)));

            long logId1 = logIdFromLocation(loc2);
            long logId2 = logIdFromLocation(loc3);
            long logId3 = logIdFromLocation(loc5);

            // all three should exist
            assertThat(entryLogger.logExists(logId1), equalTo(true));
            assertThat(entryLogger.logExists(logId2), equalTo(true));
            assertThat(entryLogger.logExists(logId3), equalTo(true));

            assertThat(entryLogger.getFlushedLogIds(), empty());

            blockClose.complete(null);
            entryLogger.flush();

            assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2));

            long loc6 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 25000));
            assertThat(logIdFromLocation(loc6), greaterThan(logIdFromLocation(loc5)));
            entryLogger.flush();

            assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2, logId3));
        } finally {
            blockClose.complete(null);
            entryLogger.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testBufferSizeNotPageAligned() throws Exception {
        File ledgerDir = tmpDirs.createNew("logRolling", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 4000);
        ByteBuf e2 = makeEntry(ledgerId1, 2L, 4000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 4000);

        try (EntryLogger elog = new DirectEntryLogger(
                curDir, new MockEntryLogIds(),
                new NativeIOImpl(),
                ByteBufAllocator.DEFAULT,
                MoreExecutors.newDirectExecutorService(),
                MoreExecutors.newDirectExecutorService(),
                9000, // max file size (header + size of one entry)
                10 * 1024 * 1024, // max sane entry size
                128 * 1024 + 500, // total write buffer size
                128 * 1024 + 300, // total read buffer size
                64 * 1024, // read buffer size
                1, // numReadThreads
                300, // max fd cache time in seconds
                slog, NullStatsLogger.INSTANCE)) {
            long loc1 = elog.addEntry(ledgerId1, e1.slice());
            int logId1 = logIdFromLocation(loc1);
            assertThat(logId1, equalTo(1));

            long loc2 = elog.addEntry(ledgerId1, e2.slice());
            int logId2 = logIdFromLocation(loc2);
            assertThat(logId2, equalTo(2));

            long loc3 = elog.addEntry(ledgerId1, e3.slice());
            int logId3 = logIdFromLocation(loc3);
            assertThat(logId3, equalTo(3));
        }
    }
}

