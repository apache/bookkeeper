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

import static org.apache.bookkeeper.bookie.storage.directentrylogger.DirectEntryLogger.logFilename;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.junit.After;
import org.junit.Test;

public class TestMetadata {
    private static final Slogger slog = Slogger.CONSOLE;
    private final OpStatsLogger opLogger = NullStatsLogger.INSTANCE.getOpStatsLogger("null");

    private final TmpDirs tmpDirs = new TmpDirs();
    private final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
        writeExecutor.shutdownNow();
    }

    @Test
    public void testReadMetaFromHeader() throws Exception {
        File ledgerDir = tmpDirs.createNew("writeMetadataBeforeFsync", "logs");
        int logId = 5678;
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(logId, logFilename(ledgerDir, logId),
                     1 << 24, writeExecutor,
                     buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            long offset = 4096L;
            writer.position(offset);
            EntryLogMetadata entryLogMetadata = new EntryLogMetadata(logId);
            entryLogMetadata.addLedgerSize(1, 10);
            entryLogMetadata.addLedgerSize(2, 11);
            LogMetadata.write(writer, entryLogMetadata, ByteBufAllocator.DEFAULT);
            try (LogReader reader = new DirectReader(logId, logFilename(ledgerDir, logId),
                    ByteBufAllocator.DEFAULT,
                    new NativeIOImpl(), Buffer.ALIGNMENT,
                    1 << 20, opLogger)) {
                ByteBuf header = reader.readBufferAt(0, Header.LOGFILE_LEGACY_HEADER_SIZE);
                assertThat(Header.HEADER_V1, equalTo(Header.extractVersion(header)));
                assertThat(offset, equalTo(Header.extractLedgerMapOffset(header)));
                assertThat(2, equalTo(Header.extractLedgerCount(header)));
            }
        }
    }

}
