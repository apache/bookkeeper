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
package org.apache.bookkeeper.bookie.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.Arrays;
import org.apache.bookkeeper.bookie.DefaultEntryLogger;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.directentrylogger.DirectEntryLogger;
import org.apache.bookkeeper.bookie.storage.directentrylogger.EntryLogIdsImpl;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;

/**
 * EntryLogTestUtils.
 */
public class EntryLogTestUtils {
    private static final Slogger slog = Slogger.CONSOLE;

    public static LedgerDirsManager newDirsManager(File... ledgerDir) throws Exception {
        return new LedgerDirsManager(
                new ServerConfiguration(), ledgerDir, new DiskChecker(0.999f, 0.999f));
    }

    public static EntryLogger newLegacyEntryLogger(int logSizeLimit, File... ledgerDir) throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setEntryLogSizeLimit(logSizeLimit);
        return new DefaultEntryLogger(conf, newDirsManager(ledgerDir), null,
                               NullStatsLogger.INSTANCE, ByteBufAllocator.DEFAULT);
    }

    public static DirectEntryLogger newDirectEntryLogger(int logSizeLimit, File ledgerDir) throws Exception {
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        return new DirectEntryLogger(
                curDir, new EntryLogIdsImpl(newDirsManager(ledgerDir), slog),
                new NativeIOImpl(),
                ByteBufAllocator.DEFAULT,
                MoreExecutors.newDirectExecutorService(),
                MoreExecutors.newDirectExecutorService(),
                logSizeLimit, // max file size
                10 * 1024 * 1024, // max sane entry size
                1024 * 1024, // total write buffer size
                1024 * 1024, // total read buffer size
                64 * 1024, // read buffer size
                1, // numReadThreads
                300, // max fd cache time in seconds
                slog, NullStatsLogger.INSTANCE);
    }

    public static int logIdFromLocation(long location) {
        return (int) (location >> 32);
    }

    public static ByteBuf makeEntry(long ledgerId, long entryId, int size) {
        return makeEntry(ledgerId, entryId, size, (byte) 0xdd);
    }

    public static ByteBuf makeEntry(long ledgerId, long entryId, int size, byte pattern) {
        ByteBuf buf = Unpooled.buffer(size);
        buf.writeLong(ledgerId).writeLong(entryId);
        byte[] data = new byte[buf.writableBytes()];
        Arrays.fill(data, pattern);
        buf.writeBytes(data);
        return buf;
    }

    public static void assertEntryEquals(ByteBuf e1, ByteBuf e2) throws Exception {
        assertThat(e1.readableBytes(), equalTo(e2.readableBytes()));
        assertThat(e1, equalTo(e2));
    }

}

