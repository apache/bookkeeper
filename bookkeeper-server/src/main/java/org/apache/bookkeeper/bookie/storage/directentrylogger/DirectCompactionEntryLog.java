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
import static org.apache.bookkeeper.common.util.ExceptionMessageHelper.exMsg;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.storage.CompactionEntryLog;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * DirectCompactionEntryLog.
 */
public abstract class DirectCompactionEntryLog implements CompactionEntryLog {
    protected final int srcLogId;
    protected final int dstLogId;
    protected final Slogger slog;

    protected final File compactingFile;
    protected final File compactedFile;
    protected final File completeFile;

    static CompactionEntryLog newLog(int srcLogId,
                                     int dstLogId,
                                     File ledgerDir,
                                     long maxFileSize,
                                     ExecutorService writeExecutor,
                                     BufferPool writeBuffers,
                                     NativeIO nativeIO,
                                     ByteBufAllocator allocator,
                                     Slogger slog) throws IOException {
        return new WritingDirectCompactionEntryLog(
                srcLogId, dstLogId, ledgerDir, maxFileSize,
                writeExecutor, writeBuffers, nativeIO, allocator, slog);
    }

    static CompactionEntryLog recoverLog(int srcLogId,
                                         int dstLogId,
                                         File ledgerDir,
                                         int readBufferSize,
                                         int maxSaneEntrySize,
                                         NativeIO nativeIO,
                                         ByteBufAllocator allocator,
                                         OpStatsLogger readBlockStats,
                                         Slogger slog) {
        return new RecoveredDirectCompactionEntryLog(srcLogId, dstLogId, ledgerDir, readBufferSize,
                                                     maxSaneEntrySize, nativeIO, allocator, readBlockStats, slog);
    }

    private DirectCompactionEntryLog(int srcLogId,
                                     int dstLogId,
                                     File ledgerDir,
                                     Slogger slog) {
        compactingFile = compactingFile(ledgerDir, dstLogId);
        compactedFile = compactedFile(ledgerDir, dstLogId, srcLogId);
        completeFile = DirectEntryLogger.logFile(ledgerDir, dstLogId);

        this.srcLogId = srcLogId;
        this.dstLogId = dstLogId;

        this.slog = slog.kv("dstLogId", dstLogId).kv("srcLogId", srcLogId).ctx(DirectCompactionEntryLog.class);
    }

    @Override
    public void abort() {
        try {
            Files.deleteIfExists(compactingFile.toPath());
        } catch (IOException ioe) {
            slog.kv("compactingFile", compactingFile).warn(Events.COMPACTION_ABORT_EXCEPTION, ioe);
        }

        try {
            Files.deleteIfExists(compactedFile.toPath());
        } catch (IOException ioe) {
            slog.kv("compactedFile", compactedFile).warn(Events.COMPACTION_ABORT_EXCEPTION, ioe);
        }
    }


    @Override
    public void makeAvailable() throws IOException {
        idempotentLink(compactedFile, completeFile);
        slog.kv("compactedFile", compactedFile).kv("completeFile", completeFile)
            .info(Events.COMPACTION_MAKE_AVAILABLE);
    }

    private static void idempotentLink(File src, File dst) throws IOException {
        if (!src.exists()) {
            throw new IOException(exMsg("src doesn't exist, aborting link")
                                  .kv("src", src).kv("dst", dst).toString());
        }
        if (!dst.exists()) {
            Files.createLink(dst.toPath(), src.toPath());
        } else if (!Files.isSameFile(src.toPath(), dst.toPath())) {
            throw new IOException(exMsg("dst exists, but doesn't match src")
                                  .kv("src", src)
                                  .kv("dst", dst).toString());
        } // else src and dst point to the same inode so we have nothing to do
    }

    @Override
    public void finalizeAndCleanup() {
        try {
            Files.deleteIfExists(compactingFile.toPath());
        } catch (IOException ioe) {
            slog.kv("compactingFile", compactingFile).warn(Events.COMPACTION_DELETE_FAILURE, ioe);
        }

        try {
            Files.deleteIfExists(compactedFile.toPath());
        } catch (IOException ioe) {
            slog.kv("compactedFile", compactedFile).warn(Events.COMPACTION_DELETE_FAILURE, ioe);
        }
        slog.info(Events.COMPACTION_COMPLETE);
    }

    @Override
    public long getDstLogId() {
        return dstLogId;
    }

    @Override
    public long getSrcLogId() {
        return srcLogId;
    }

    private static class RecoveredDirectCompactionEntryLog extends DirectCompactionEntryLog {
        private final ByteBufAllocator allocator;
        private final NativeIO nativeIO;
        private final int readBufferSize;
        private final int maxSaneEntrySize;
        private final OpStatsLogger readBlockStats;

        RecoveredDirectCompactionEntryLog(int srcLogId,
                                          int dstLogId,
                                          File ledgerDir,
                                          int readBufferSize,
                                          int maxSaneEntrySize,
                                          NativeIO nativeIO,
                                          ByteBufAllocator allocator,
                                          OpStatsLogger readBlockStats,
                                          Slogger slog) {
            super(srcLogId, dstLogId, ledgerDir, slog);
            this.allocator = allocator;
            this.nativeIO = nativeIO;
            this.readBufferSize = readBufferSize;
            this.maxSaneEntrySize = maxSaneEntrySize;
            this.readBlockStats = readBlockStats;

            this.slog.info(Events.COMPACTION_LOG_RECOVERED);
        }

        private IllegalStateException illegalOpException() {
            return new IllegalStateException(exMsg("Invalid operation for recovered log")
                                             .kv("srcLogId", srcLogId)
                                             .kv("dstLogId", dstLogId)
                                             .kv("compactingFile", compactingFile)
                                             .kv("compactedFile", compactedFile)
                                             .kv("completeFile", completeFile).toString());
        }

        @Override
        public long addEntry(long ledgerId, ByteBuf entry) throws IOException {
            throw illegalOpException();
        }

        @Override
        public void flush() throws IOException {
            throw illegalOpException();
        }

        @Override
        public void markCompacted() throws IOException {
            throw illegalOpException();
        }

        @Override
        public void scan(EntryLogScanner scanner) throws IOException {
            try (LogReader reader = new DirectReader(dstLogId, compactedFile.toString(), allocator, nativeIO,
                                                     readBufferSize, maxSaneEntrySize, readBlockStats)) {
                LogReaderScan.scan(allocator, reader, scanner);
            }
        }
    }

    private static class WritingDirectCompactionEntryLog extends DirectCompactionEntryLog {
        private final WriterWithMetadata writer;

        WritingDirectCompactionEntryLog(int srcLogId,
                                        int dstLogId,
                                        File ledgerDir,
                                        long maxFileSize,
                                        ExecutorService writeExecutor,
                                        BufferPool writeBuffers,
                                        NativeIO nativeIO,
                                        ByteBufAllocator allocator,
                                        Slogger slog) throws IOException {
            super(srcLogId, dstLogId, ledgerDir, slog);

            this.writer = new WriterWithMetadata(
                    new DirectWriter(dstLogId, compactingFile.toString(), maxFileSize,
                                     writeExecutor, writeBuffers, nativeIO, slog),
                    new EntryLogMetadata(dstLogId),
                    allocator);

            this.slog.info(Events.COMPACTION_LOG_CREATED);
        }

        @Override
        public long addEntry(long ledgerId, ByteBuf entry) throws IOException {
            return writer.addEntry(ledgerId, entry);
        }

        @Override
        public void flush() throws IOException {
            writer.flush();
        }

        @Override
        public void markCompacted() throws IOException {
            writer.finalizeAndClose();

            idempotentLink(compactingFile, compactedFile);
            if (!compactingFile.delete()) {
                slog.kv("compactingFile", compactingFile)
                    .kv("compactedFile", compactedFile)
                    .info(Events.COMPACTION_DELETE_FAILURE);
            } else {
                slog.kv("compactingFile", compactingFile)
                    .kv("compactedFile", compactedFile)
                    .info(Events.COMPACTION_MARK_COMPACTED);
            }
        }

        @Override
        public void scan(EntryLogScanner scanner) throws IOException {
            throw new IllegalStateException(exMsg("Scan only valid for recovered log")
                                            .kv("srcLogId", srcLogId)
                                            .kv("dstLogId", dstLogId)
                                            .kv("compactingFile", compactingFile)
                                            .kv("compactedFile", compactedFile)
                                            .kv("completeFile", completeFile).toString());
        }
    }

    public static File compactingFile(File directory, int logId) {
        return new File(directory, String.format("%x%s", logId, COMPACTING_SUFFIX));
    }

    public static File compactedFile(File directory, int newLogId, int compactedLogId) {
        return new File(directory, String.format("%x.log.%x%s", newLogId,
                                                 compactedLogId, COMPACTED_SUFFIX));
    }
}
