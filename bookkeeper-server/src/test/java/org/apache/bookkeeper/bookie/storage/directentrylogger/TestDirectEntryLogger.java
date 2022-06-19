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
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.DefaultEntryLogger;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.MockEntryLogIds;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.util.DiskChecker;
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

    // step1: default is DirectEntryLogger, write entries, read entries
    // step2: change DirectEntryLogger to DefaultEntryLogger, write entries, and read all entries both written
    // by DirectEntryLogger and DefaultEntryLogger
    // step3: change DefaultEntryLogger to DirectEntryLogger, write entries, and read all entries written by
    // DirectEntryLogger, DefaultEntryLogger and DirectEntryLogger.
    // DirectEntryLogger -> DefaultEntryLogge -> DirectEntryLogger.
    @Test
    public void testCompatFromDirectToDefaultToDirectLogger() throws Exception {
        File ledgerDir = tmpDirs.createNew("entryCompatTest", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();
        MockEntryLogIds entryLogIds = new MockEntryLogIds();

        ByteBuf e1 = buildEntry(ledgerId1, 1, 1024, "entry-1".getBytes(StandardCharsets.UTF_8));
        ByteBuf e2 = buildEntry(ledgerId1, 2, 1024, "entry-2".getBytes(StandardCharsets.UTF_8));
        ByteBuf e3 = buildEntry(ledgerId1, 3, 1024, "entry-3".getBytes(StandardCharsets.UTF_8));
        ByteBuf e4 = buildEntry(ledgerId1, 4, 1024, "entry-4".getBytes(StandardCharsets.UTF_8));
        ByteBuf e5 = buildEntry(ledgerId1, 5, 1024, "entry-5".getBytes(StandardCharsets.UTF_8));
        ByteBuf e6 = buildEntry(ledgerId1, 6, 1024, "entry-6".getBytes(StandardCharsets.UTF_8));
        ByteBuf e7 = buildEntry(ledgerId1, 7, 1024, "entry-7".getBytes(StandardCharsets.UTF_8));

        long loc1, loc2, loc3, loc4, loc5, loc6, loc7;

        // write entry into DirectEntryLogger
        try (EntryLogger elog = new DirectEntryLogger(
            curDir, entryLogIds,
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
            loc1 = elog.addEntry(ledgerId1, e1.slice());
            loc2 = elog.addEntry(ledgerId1, e2.slice());
            loc3 = elog.addEntry(ledgerId1, e3.slice());
            elog.flush();

            ByteBuf entry1 = elog.readEntry(ledgerId1, 1, loc1);
            ByteBuf entry2 = elog.readEntry(ledgerId1, 2, loc2);
            ByteBuf entry3 = elog.readEntry(ledgerId1, 3, loc3);

            assertEntryEquals(entry1, e1);
            assertEntryEquals(entry2, e2);
            assertEntryEquals(entry3, e3);

            entry1.release();
            entry2.release();
            entry3.release();
        }

        // read entry from DefaultEntryLogger
        ServerConfiguration conf = new ServerConfiguration();
        LedgerDirsManager dirsMgr = new LedgerDirsManager(
            conf,
            new File[] { ledgerDir },
            new DiskChecker(
                conf.getDiskUsageThreshold(),
                conf.getDiskUsageWarnThreshold()));
        EntryLogger entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        loc4 = entryLogger.addEntry(ledgerId1, e4.slice());
        loc5 = entryLogger.addEntry(ledgerId1, e5.slice());
        entryLogger.flush();

        ByteBuf entry1 = entryLogger.readEntry(ledgerId1, 1, loc1);
        ByteBuf entry2 = entryLogger.readEntry(ledgerId1, 2, loc2);
        ByteBuf entry3 = entryLogger.readEntry(ledgerId1, 3, loc3);
        ByteBuf entry4 = entryLogger.readEntry(ledgerId1, 4, loc4);
        ByteBuf entry5 = entryLogger.readEntry(ledgerId1, 5, loc5);

        assertEntryEquals(entry1, e1);
        assertEntryEquals(entry2, e2);
        assertEntryEquals(entry3, e3);
        assertEntryEquals(entry4, e4);
        assertEntryEquals(entry5, e5);

        entry1.release();
        entry2.release();
        entry3.release();
        entry4.release();
        entry5.release();

        // use DirectEntryLogger to read entries written by both DirectEntryLogger and DefaultEntryLogger
        entryLogIds.nextId();
        try (EntryLogger elog = new DirectEntryLogger(
            curDir, entryLogIds,
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
            loc6 = elog.addEntry(ledgerId1, e6.slice());
            loc7 = elog.addEntry(ledgerId1, e7.slice());
            elog.flush();

            log.info("[hangc] {} - {} - {} - {} - {} - {} - {}", loc1, loc2, loc3, loc4, loc5, loc6, loc7);

            entry1 = elog.readEntry(ledgerId1, 1, loc1);
            entry2 = elog.readEntry(ledgerId1, 2, loc2);
            entry3 = elog.readEntry(ledgerId1, 3, loc3);
            entry4 = elog.readEntry(ledgerId1, 4, loc4);
            entry5 = elog.readEntry(ledgerId1, 5, loc5);
            ByteBuf entry6 = elog.readEntry(ledgerId1, 6, loc6);
            ByteBuf entry7 = elog.readEntry(ledgerId1, 7, loc7);

            assertEntryEquals(entry1, e1);
            assertEntryEquals(entry2, e2);
            assertEntryEquals(entry3, e3);
            assertEntryEquals(entry4, e4);
            assertEntryEquals(entry5, e5);
            assertEntryEquals(entry6, e6);
            assertEntryEquals(entry7, e7);

            entry1.release();
            entry2.release();
            entry3.release();
            entry4.release();
            entry5.release();
            entry6.release();
            entry7.release();
        }

        ledgerDir.deleteOnExit();

    }

    // step1: default is DefaultEntryLogger, write entries and read entries.
    // step2: change DefaultEntryLogger to DirectEntryLogger, write entries, and read all entries both writer
    // by DefaultEntryLogger and DirectEntryLogger
    // step3: change DirectEntryLogger to DefaultEntryLogger, write entries, and read all entries both written
    // by DirectEntryLogger and DefaultEntryLogger
    // step4: change DefaultEntryLogger to DirectEntryLogger, write entries, and read all entries written by
    // DirectEntryLogger, DefaultEntryLogger and DirectEntryLogger.
    // DefaultEntryLogger -> DirectEntryLogger -> DefaultEntryLogger -> DirectEntryLogger.
    @Test
    public void testCompatFromDefaultToDirectToDefaultToDirectLogger() throws Exception {
        File ledgerDir = tmpDirs.createNew("entryCompatTest", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();
        MockEntryLogIds entryLogIds = new MockEntryLogIds();

        ByteBuf e1 = buildEntry(ledgerId1, 1, 1024, "entry-1".getBytes(StandardCharsets.UTF_8));
        ByteBuf e2 = buildEntry(ledgerId1, 2, 1024, "entry-2".getBytes(StandardCharsets.UTF_8));
        ByteBuf e3 = buildEntry(ledgerId1, 3, 1024, "entry-3".getBytes(StandardCharsets.UTF_8));
        ByteBuf e4 = buildEntry(ledgerId1, 4, 1024, "entry-4".getBytes(StandardCharsets.UTF_8));
        ByteBuf e5 = buildEntry(ledgerId1, 5, 1024, "entry-5".getBytes(StandardCharsets.UTF_8));
        ByteBuf e6 = buildEntry(ledgerId1, 6, 1024, "entry-6".getBytes(StandardCharsets.UTF_8));
        ByteBuf e7 = buildEntry(ledgerId1, 7, 1024, "entry-7".getBytes(StandardCharsets.UTF_8));
        ByteBuf e8 = buildEntry(ledgerId1, 8, 1024, "entry-8".getBytes(StandardCharsets.UTF_8));
        ByteBuf e9 = buildEntry(ledgerId1, 9, 1024, "entry-9".getBytes(StandardCharsets.UTF_8));

        long loc1, loc2, loc3, loc4, loc5, loc6, loc7, loc8, loc9;

        // write e1 and e2 using DefaultEntryLogger
        ServerConfiguration conf = new ServerConfiguration();
        LedgerDirsManager dirsMgr = new LedgerDirsManager(
            conf,
            new File[] { ledgerDir },
            new DiskChecker(
                conf.getDiskUsageThreshold(),
                conf.getDiskUsageWarnThreshold()));
        EntryLogger entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        loc1 = entryLogger.addEntry(ledgerId1, e1.slice());
        loc2 = entryLogger.addEntry(ledgerId1, e2.slice());
        entryLogger.flush();

        ByteBuf entry1 = entryLogger.readEntry(ledgerId1, 1, loc1);
        ByteBuf entry2 = entryLogger.readEntry(ledgerId1, 2, loc2);

        assertEntryEquals(entry1, e1);
        assertEntryEquals(entry2, e2);

        entry1.release();
        entry2.release();

        // write e3, e4 and e5 using DirectEntryLogger and read all entries.
        entryLogIds.nextId();
        try (EntryLogger elog = new DirectEntryLogger(
            curDir, entryLogIds,
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
            loc3 = elog.addEntry(ledgerId1, e3.slice());
            loc4 = elog.addEntry(ledgerId1, e4.slice());
            loc5 = elog.addEntry(ledgerId1, e5.slice());
            elog.flush();

            entry1 = elog.readEntry(ledgerId1, 1, loc1);
            entry2 = elog.readEntry(ledgerId1, 2, loc2);
            ByteBuf entry3 = elog.readEntry(ledgerId1, 3, loc3);
            ByteBuf entry4 = elog.readEntry(ledgerId1, 4, loc4);
            ByteBuf entry5 = elog.readEntry(ledgerId1, 5, loc5);

            assertEntryEquals(entry1, e1);
            assertEntryEquals(entry2, e2);
            assertEntryEquals(entry3, e3);
            assertEntryEquals(entry4, e4);
            assertEntryEquals(entry5, e5);

            entry1.release();
            entry2.release();
            entry3.release();
            entry4.release();
            entry5.release();
        }

        // write e6 and e7 using DefaultEntryLogger and read all entries
        entryLogger = new DefaultEntryLogger(conf, dirsMgr);
        loc6 = entryLogger.addEntry(ledgerId1, e6.slice());
        loc7 = entryLogger.addEntry(ledgerId1, e7.slice());
        entryLogger.flush();

        entry1 = entryLogger.readEntry(ledgerId1, 1, loc1);
        entry2 = entryLogger.readEntry(ledgerId1, 2, loc2);
        ByteBuf entry3 = entryLogger.readEntry(ledgerId1, 3, loc3);
        ByteBuf entry4 = entryLogger.readEntry(ledgerId1, 4, loc4);
        ByteBuf entry5 = entryLogger.readEntry(ledgerId1, 5, loc5);
        ByteBuf entry6 = entryLogger.readEntry(ledgerId1, 6, loc6);
        ByteBuf entry7 = entryLogger.readEntry(ledgerId1, 7, loc7);

        assertEntryEquals(entry1, e1);
        assertEntryEquals(entry2, e2);
        assertEntryEquals(entry3, e3);
        assertEntryEquals(entry4, e4);
        assertEntryEquals(entry5, e5);
        assertEntryEquals(entry6, e6);
        assertEntryEquals(entry7, e7);

        entry1.release();
        entry2.release();
        entry3.release();
        entry4.release();
        entry5.release();
        entry6.release();
        entry7.release();

        // use DirectEntryLogger to read entries written by both DirectEntryLogger and DefaultEntryLogger
        entryLogIds.nextId();
        try (EntryLogger elog = new DirectEntryLogger(
            curDir, entryLogIds,
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
            loc8 = elog.addEntry(ledgerId1, e8.slice());
            loc9 = elog.addEntry(ledgerId1, e9.slice());
            elog.flush();

            log.info("[hangc] {} - {} - {} - {} - {} - {} - {}", loc1, loc2, loc3, loc4, loc5, loc6, loc7);

            entry1 = elog.readEntry(ledgerId1, 1, loc1);
            entry2 = elog.readEntry(ledgerId1, 2, loc2);
            entry3 = elog.readEntry(ledgerId1, 3, loc3);
            entry4 = elog.readEntry(ledgerId1, 4, loc4);
            entry5 = elog.readEntry(ledgerId1, 5, loc5);
            entry6 = elog.readEntry(ledgerId1, 6, loc6);
            entry7 = elog.readEntry(ledgerId1, 7, loc7);
            ByteBuf entry8 = elog.readEntry(ledgerId1, 8, loc8);
            ByteBuf entry9 = elog.readEntry(ledgerId1, 9, loc9);

            assertEntryEquals(entry1, e1);
            assertEntryEquals(entry2, e2);
            assertEntryEquals(entry3, e3);
            assertEntryEquals(entry4, e4);
            assertEntryEquals(entry5, e5);
            assertEntryEquals(entry6, e6);
            assertEntryEquals(entry7, e7);
            assertEntryEquals(entry8, e8);
            assertEntryEquals(entry9, e9);

            entry1.release();
            entry2.release();
            entry3.release();
            entry4.release();
            entry5.release();
            entry6.release();
            entry7.release();
            entry8.release();
            entry9.release();
        }

        ledgerDir.deleteOnExit();
    }

    private ByteBuf buildEntry(long ledgerId, long entryId, int size, byte[] bytes) {
        ByteBuf entry = Unpooled.buffer(size);
        entry.writeLong(ledgerId); // ledger id
        entry.writeLong(entryId); // entry id
        entry.writeBytes(bytes);
        return entry;
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
            e1read.release();
            e2read.release();

            long loc3 = elog.addEntry(ledgerId1, e3.slice());
            elog.flush();

            ByteBuf e3read = elog.readEntry(ledgerId1, 3L, loc3);
            assertEntryEquals(e3read, e3);
            e3read.release();
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
                elog.readEntry(loc).release();
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

