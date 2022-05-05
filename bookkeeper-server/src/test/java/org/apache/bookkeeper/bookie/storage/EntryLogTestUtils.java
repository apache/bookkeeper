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
package org.apache.bookkeeper.bookie.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.Arrays;
import org.apache.bookkeeper.bookie.DefaultEntryLogger;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
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

    public static int logIdFromLocation(long location) {
        return (int) (location >> 32);
    }

    public static ByteBuf makeEntry(long ledgerId, long entryId, int size) {
        ByteBuf buf = Unpooled.buffer(size);
        buf.writeLong(ledgerId).writeLong(entryId);
        byte[] random = new byte[buf.writableBytes()];
        Arrays.fill(random, (byte) 0xdd);
        buf.writeBytes(random);
        return buf;
    }
}

