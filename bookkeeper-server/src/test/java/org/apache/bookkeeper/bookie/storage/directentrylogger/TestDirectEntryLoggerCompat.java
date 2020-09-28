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
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.newLegacyEntryLogger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.storage.EntryLoggerIface;
import org.apache.bookkeeper.bookie.storage.MockEntryLogIds;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.junit.After;
import org.junit.Test;

/**
 * TestDirectEntryLoggerCompat.
 */
public class TestDirectEntryLoggerCompat {
    private final Slogger slog = Slogger.CONSOLE;

    private static final long ledgerId1 = 1234;
    private static final long ledgerId2 = 4567;
    private static final long ledgerId3 = 7890;

    private final TmpDirs tmpDirs = new TmpDirs();

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
    }

    @Test
    public void testLegacyCanReadDirect() throws Exception {
        File ledgerDir = tmpDirs.createNew("legacyCanRead", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 1000);
        ByteBuf e2 = makeEntry(ledgerId1, 2L, 1000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 1000);

        long loc1, loc2, loc3;
        try (EntryLoggerIface elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     10 * 1024 * 1024, // 10MiB, max file size
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
        }

        try (EntryLogger legacy = newLegacyEntryLogger(2000000, ledgerDir)) {
            assertEntryEquals(legacy.readEntry(ledgerId1, 1L, loc1), e1);
            assertEntryEquals(legacy.readEntry(ledgerId1, 2L, loc2), e2);
            assertEntryEquals(legacy.readEntry(ledgerId1, 3L, loc3), e3);
        }
    }

    @Test
    public void testDirectCanReadLegacy() throws Exception {
        File ledgerDir = tmpDirs.createNew("legacyCanRead", "ledgers");

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 1000);
        ByteBuf e2 = makeEntry(ledgerId1, 2L, 1000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 1000);

        long loc1, loc2, loc3;
        try (EntryLogger legacy = newLegacyEntryLogger(2000000, ledgerDir)) {
            loc1 = legacy.addEntry(ledgerId1, e1.slice());
            loc2 = legacy.addEntry(ledgerId1, e2.slice());
            loc3 = legacy.addEntry(ledgerId1, e3.slice());
            legacy.flush();
        }

        try (EntryLoggerIface elog = new DirectEntryLogger(
                     new File(ledgerDir, "current"), new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     10 * 1024 * 1024, // 10MiB, max file size
                     10 * 1024 * 1024, // max sane entry size
                     1024 * 1024, // total write buffer size
                     1024 * 1024, // total read buffer size
                     64 * 1024, // read buffer size
                     1, // numReadThreads
                 300, // max fd cache time in seconds
                     slog, NullStatsLogger.INSTANCE)) {
            assertEntryEquals(elog.readEntry(ledgerId1, 1L, loc1), e1);
            assertEntryEquals(elog.readEntry(ledgerId1, 2L, loc2), e2);
            assertEntryEquals(elog.readEntry(ledgerId1, 3L, loc3), e3);
        }
    }

    @Test
    public void testLegacyCanReadDirectAfterMultipleRolls() throws Exception {
        File ledgerDir = tmpDirs.createNew("legacyCanRead", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 4000);
        ByteBuf e2 = makeEntry(ledgerId1, 2L, 4000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 4000);

        long loc1, loc2, loc3;
        try (EntryLoggerIface elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     6000, // max file size
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
        }

        try (EntryLogger legacy = newLegacyEntryLogger(2000000, ledgerDir)) {
            assertEntryEquals(legacy.readEntry(ledgerId1, 1L, loc1), e1);
            assertEntryEquals(legacy.readEntry(ledgerId1, 2L, loc2), e2);
            assertEntryEquals(legacy.readEntry(ledgerId1, 3L, loc3), e3);
        }
    }

    @Test
    public void testLegacyCanReadMetadataOfDirectWithIndexWritten() throws Exception {
        File ledgerDir = tmpDirs.createNew("legacyCanReadMeta", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 1000);
        ByteBuf e2 = makeEntry(ledgerId2, 2L, 2000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 3000);
        ByteBuf e4 = makeEntry(ledgerId1, 4L, 4000);

        int maxFileSize = 1000 + 2000 + 3000 + (Integer.BYTES * 3) + 4096;
        long loc1, loc2, loc3, loc4;
        try (EntryLoggerIface elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     maxFileSize, // max file size
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
             loc4 = elog.addEntry(ledgerId1, e4);
        }

        try (EntryLogger legacy = newLegacyEntryLogger(
                     maxFileSize, // size of first 3 entries + header
                     ledgerDir)) {
            int logId = logIdFromLocation(loc1);
            assertThat(logId, equalTo(logIdFromLocation(loc2)));
            assertThat(logId, equalTo(logIdFromLocation(loc3)));
            assertThat(logId, not(equalTo(logIdFromLocation(loc4))));

            EntryLogMetadata meta = legacy.getEntryLogMetadata(logId);

            assertThat(meta.getEntryLogId(), equalTo((long) logId));
            assertThat(meta.getTotalSize(), equalTo(1000L + 2000 + 3000 + (Integer.BYTES * 3)));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));
            assertThat(meta.getLedgersMap().get(ledgerId1), equalTo(1000L + 3000L + (Integer.BYTES * 2)));
            assertThat(meta.getLedgersMap().get(ledgerId2), equalTo(2000L + Integer.BYTES));
        }
    }

    @Test
    public void testLegacyCanReadMetadataOfDirectWithNoIndexWritten() throws Exception {
        File ledgerDir = tmpDirs.createNew("legacyCanReadMeta", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 1000);
        ByteBuf e2 = makeEntry(ledgerId2, 2L, 2000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 3000);
        ByteBuf e4 = makeEntry(ledgerId1, 4L, 4000);

        int maxFileSize = 1000 + 2000 + 3000 + (Integer.BYTES * 3) + 4096;
        long loc1, loc2, loc3;
        try (EntryLoggerIface elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     maxFileSize * 10, // max file size
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

        try (EntryLogger legacy = newLegacyEntryLogger(
                     maxFileSize, // size of first 3 entries + header
                     ledgerDir)) {
            int logId = logIdFromLocation(loc1);
            assertThat(logId, equalTo(logIdFromLocation(loc2)));
            assertThat(logId, equalTo(logIdFromLocation(loc3)));
            EntryLogMetadata meta = legacy.getEntryLogMetadata(logId);

            assertThat(meta.getEntryLogId(), equalTo((long) logId));
            assertThat(meta.getTotalSize(), equalTo(1000L + 2000 + 3000 + (Integer.BYTES * 3)));
            assertThat(meta.getRemainingSize(), equalTo(meta.getTotalSize()));
            assertThat(meta.getLedgersMap().get(ledgerId1), equalTo(1000L + 3000L + (Integer.BYTES * 2)));
            assertThat(meta.getLedgersMap().get(ledgerId2), equalTo(2000L + Integer.BYTES));
        }
    }

    @Test
    public void testDirectCanReadMetadataAndScanFromLegacy() throws Exception {
        File ledgerDir = tmpDirs.createNew("directCanReadLegacyMeta", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        ByteBuf e1 = makeEntry(ledgerId1, 1L, 1000);
        ByteBuf e2 = makeEntry(ledgerId2, 2L, 2000);
        ByteBuf e3 = makeEntry(ledgerId1, 3L, 3000);
        ByteBuf e4 = makeEntry(ledgerId1, 4L, 4000);

        int maxFileSize = 1000 + 2000 + 3000 + (Integer.BYTES * 3) + 4096;
        long loc1, loc2, loc3, loc4;
        try (EntryLogger legacy = newLegacyEntryLogger(
                     maxFileSize, // size of first 3 entries + header
                     ledgerDir)) {
            loc1 = legacy.addEntry(ledgerId1, e1);
            loc2 = legacy.addEntry(ledgerId2, e2);
            loc3 = legacy.addEntry(ledgerId1, e3);
            loc4 = legacy.addEntry(ledgerId1, e4); // should force a roll
        }

        try (DirectEntryLogger elog = new DirectEntryLogger(
                     curDir, new MockEntryLogIds(),
                     new NativeIOImpl(),
                     ByteBufAllocator.DEFAULT,
                     MoreExecutors.newDirectExecutorService(),
                     MoreExecutors.newDirectExecutorService(),
                     maxFileSize * 10, // max file size
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
            assertThat(logId, not(equalTo(logIdFromLocation(loc4))));

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

}

