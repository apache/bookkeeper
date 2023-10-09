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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTING_SUFFIX;
import static org.apache.bookkeeper.common.util.ExceptionMessageHelper.exMsg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.apache.bookkeeper.bookie.AbstractLogCompactor;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.storage.CompactionEntryLog;
import org.apache.bookkeeper.bookie.storage.EntryLogIds;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.LedgerDirUtil;

/**
 * DirectEntryLogger.
 */
public class DirectEntryLogger implements EntryLogger {
    private final Slogger slog;
    private final File ledgerDir;
    private final EntryLogIds ids;
    private final ExecutorService writeExecutor;
    private final ExecutorService flushExecutor;
    private final long maxFileSize;
    private final DirectEntryLoggerStats stats;
    private final ByteBufAllocator allocator;
    private final BufferPool writeBuffers;
    private final int readBufferSize;
    private final int maxSaneEntrySize;
    private final Set<Integer> unflushedLogs;

    private WriterWithMetadata curWriter;

    private List<Future<?>> pendingFlushes;
    private final NativeIO nativeIO;
    private final List<Cache<?, ?>> allCaches = new CopyOnWriteArrayList<>();
    private final ThreadLocal<Cache<Integer, LogReader>> caches;

    private static final int NUMBER_OF_WRITE_BUFFERS = 8;

    public DirectEntryLogger(File ledgerDir,
                             EntryLogIds ids,
                             NativeIO nativeIO,
                             ByteBufAllocator allocator,
                             ExecutorService writeExecutor,
                             ExecutorService flushExecutor,
                             long maxFileSize,
                             int maxSaneEntrySize,
                             long totalWriteBufferSize,
                             long totalReadBufferSize,
                             int readBufferSize,
                             int numReadThreads,
                             int maxFdCacheTimeSeconds,
                             Slogger slogParent,
                             StatsLogger stats) throws IOException {
        this.ledgerDir = ledgerDir;
        this.flushExecutor = flushExecutor;
        this.writeExecutor = writeExecutor;
        this.pendingFlushes = new ArrayList<>();
        this.nativeIO = nativeIO;
        this.unflushedLogs = ConcurrentHashMap.newKeySet();

        this.maxFileSize = maxFileSize;
        this.maxSaneEntrySize = maxSaneEntrySize;
        this.readBufferSize = Buffer.nextAlignment(readBufferSize);
        this.ids = ids;
        this.slog = slogParent.kv("directory", ledgerDir).ctx(DirectEntryLogger.class);

        this.stats = new DirectEntryLoggerStats(stats);

        this.allocator = allocator;

        int singleWriteBufferSize = Buffer.nextAlignment((int) (totalWriteBufferSize / NUMBER_OF_WRITE_BUFFERS));
        this.writeBuffers = new BufferPool(nativeIO, allocator, singleWriteBufferSize, NUMBER_OF_WRITE_BUFFERS);

        // The total read buffer memory needs to get split across all the read threads, since the caches
        // are thread-specific and we want to ensure we don't pass the total memory limit.
        long perThreadBufferSize = totalReadBufferSize / numReadThreads;

        // if the amount of total read buffer size is too low, and/or the number of read threads is too high
        // then the perThreadBufferSize can be lower than the readBufferSize causing immediate eviction of readers
        // from the cache
        if (perThreadBufferSize < readBufferSize) {
            slog.kv("reason", "perThreadBufferSize lower than readBufferSize (causes immediate reader cache eviction)")
                .kv("totalReadBufferSize", totalReadBufferSize)
                .kv("totalNumReadThreads", numReadThreads)
                .kv("readBufferSize", readBufferSize)
                .kv("perThreadBufferSize", perThreadBufferSize)
                .error(Events.ENTRYLOGGER_MISCONFIGURED);
        }

        long maxCachedReadersPerThread = perThreadBufferSize / readBufferSize;
        long maxCachedReaders = maxCachedReadersPerThread * numReadThreads;

        this.slog
            .kv("maxFileSize", maxFileSize)
            .kv("maxSaneEntrySize", maxSaneEntrySize)
            .kv("totalWriteBufferSize", totalWriteBufferSize)
            .kv("singleWriteBufferSize", singleWriteBufferSize)
            .kv("totalReadBufferSize", totalReadBufferSize)
            .kv("readBufferSize", readBufferSize)
            .kv("perThreadBufferSize", perThreadBufferSize)
            .kv("maxCachedReadersPerThread", maxCachedReadersPerThread)
            .kv("maxCachedReaders", maxCachedReaders)
            .info(Events.ENTRYLOGGER_CREATED);

        this.caches = ThreadLocal.withInitial(() -> {
            RemovalListener<Integer, LogReader> rl = (notification) -> {
                try {
                    notification.getValue().close();
                    this.stats.getCloseReaderCounter().inc();
                } catch (IOException ioe) {
                    slog.kv("logID", notification.getKey()).error(Events.READER_CLOSE_ERROR);
                }
            };
            Cache<Integer, LogReader> cache = CacheBuilder.newBuilder()
                    .maximumWeight(perThreadBufferSize)
                    .weigher((key, value) -> readBufferSize)
                    .removalListener(rl)
                    .expireAfterAccess(maxFdCacheTimeSeconds, TimeUnit.SECONDS)
                    .concurrencyLevel(1) // important to avoid too aggressive eviction
                    .build();
            allCaches.add(cache);
            return cache;
        });
    }

    @Override
    public long addEntry(long ledgerId, ByteBuf buf) throws IOException {
        long start = System.nanoTime();

        long offset;
        synchronized (this) {
            if (curWriter != null
                && curWriter.shouldRoll(buf, maxFileSize)) {
                // roll the log. asynchronously flush and close current log
                flushAndCloseCurrent();
                curWriter = null;
            }
            if (curWriter == null) {
                int newId = ids.nextId();
                curWriter = new WriterWithMetadata(newDirectWriter(newId),
                                                   new EntryLogMetadata(newId),
                                                   allocator);
                slog.kv("newLogId", newId).info(Events.LOG_ROLL);
            }

            offset = curWriter.addEntry(ledgerId, buf);
        }
        stats.getAddEntryStats().registerSuccessfulEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        return offset;
    }

    @Override
    public ByteBuf readEntry(long entryLocation)
            throws IOException, NoEntryException {
        return internalReadEntry(-1L, -1L, entryLocation, false);
    }

    @Override
    public ByteBuf readEntry(long ledgerId, long entryId, long entryLocation)
            throws IOException, NoEntryException {
        return internalReadEntry(ledgerId, entryId, entryLocation, true);
    }

    private LogReader getReader(int logId) throws IOException {
        Cache<Integer, LogReader> cache = caches.get();
        try {
            LogReader reader = cache.get(logId, () -> {
                this.stats.getOpenReaderCounter().inc();
                return newDirectReader(logId);
            });

            // it is possible though unlikely, that the cache has already cleaned up this cache entry
            // during the get operation. This is more likely to happen when there is great demand
            // for many separate readers in a low memory environment.
            if (reader.isClosed()) {
                this.stats.getCachedReadersServedClosedCounter().inc();
                throw new IOException(exMsg("Cached reader already closed").kv("logId", logId).toString());
            }

            return reader;
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
                throw (IOException) ee.getCause();
            } else {
                throw new IOException(exMsg("Error loading reader in cache").kv("logId", logId).toString(), ee);
            }
        }
    }

    private ByteBuf internalReadEntry(long ledgerId, long entryId, long location, boolean validateEntry)
            throws IOException, NoEntryException {
        int logId = (int) (location >> 32);
        int pos = (int) (location & 0xFFFFFFFF);

        long start = System.nanoTime();
        LogReader reader = getReader(logId);

        try {
            ByteBuf buf = reader.readEntryAt(pos);
            if (validateEntry) {
                long thisLedgerId = buf.getLong(0);
                long thisEntryId = buf.getLong(8);
                if (thisLedgerId != ledgerId
                    || thisEntryId != entryId) {
                    throw new IOException(
                            exMsg("Bad location").kv("location", location)
                            .kv("expectedLedger", ledgerId).kv("expectedEntry", entryId)
                            .kv("foundLedger", thisLedgerId).kv("foundEntry", thisEntryId)
                            .toString());
                }
            }
            stats.getReadEntryStats().registerSuccessfulEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            return buf;
        } catch (EOFException eof) {
            stats.getReadEntryStats().registerFailedEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            throw new NoEntryException(
                    exMsg("Entry location doesn't exist").kv("location", location).toString(),
                    ledgerId, entryId);
        }
    }

    @Override
    public void flush() throws IOException {
        long start = System.nanoTime();
        Future<?> currentFuture = flushCurrent();

        List<Future<?>> outstandingFlushes;
        synchronized (this) {
            outstandingFlushes = this.pendingFlushes;
            this.pendingFlushes = new ArrayList<>();
        }
        outstandingFlushes.add(currentFuture);

        for (Future<?> f: outstandingFlushes) {
            try {
                f.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interruped while flushing", ie);
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof IOException) {
                    throw (IOException) ee.getCause();
                } else {
                    throw new IOException("Exception flushing writer", ee);
                }
            }
        }
        stats.getFlushStats().registerSuccessfulEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    }

    private Future<?> flushCurrent() throws IOException {
        WriterWithMetadata flushWriter;
        synchronized (this) {
            flushWriter = this.curWriter;
        }
        if (flushWriter != null) {
            return flushExecutor.submit(() -> {
                    long start = System.nanoTime();
                    try {
                        flushWriter.flush();
                        stats.getWriterFlushStats().registerSuccessfulEvent(
                                System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    } catch (Throwable t) {
                        stats.getWriterFlushStats().registerFailedEvent(
                                System.nanoTime() - start, TimeUnit.NANOSECONDS);
                        throw t;
                    }
                    return null;
                });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private void flushAndCloseCurrent() throws IOException {
        WriterWithMetadata flushWriter;

        CompletableFuture<Void> flushPromise = new CompletableFuture<>();
        synchronized (this) {
            flushWriter = this.curWriter;
            this.curWriter = null;

            pendingFlushes.add(flushPromise);
        }
        if (flushWriter != null) {
            flushExecutor.execute(() -> {
                long start = System.nanoTime();
                try {
                    flushWriter.finalizeAndClose();
                    stats.getWriterFlushStats()
                        .registerSuccessfulEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    unflushedLogs.remove(flushWriter.logId());
                    flushPromise.complete(null);
                } catch (Throwable t) {
                    stats.getWriterFlushStats()
                        .registerFailedEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    flushPromise.completeExceptionally(t);
                }
            });
        } else {
            flushPromise.complete(null);
        }
    }

    @Override
    public void close() throws IOException {
        flushAndCloseCurrent(); // appends metadata to current log
        flush(); // wait for all outstanding flushes

        for (Cache<?, ?> c : allCaches) {
            c.invalidateAll();
        }

        writeBuffers.close();
    }

    @Override
    public Collection<Long> getFlushedLogIds() {
        return LedgerDirUtil.logIdsInDirectory(ledgerDir).stream()
            .filter(logId -> !unflushedLogs.contains(logId))
            .map(i -> Long.valueOf(i))
            .collect(Collectors.toList());
    }

    @Override
    public boolean removeEntryLog(long entryLogId) {
        checkArgument(entryLogId < Integer.MAX_VALUE, "Entry log id must be an int [%d]", entryLogId);
        File file = logFile(ledgerDir, (int) entryLogId);
        boolean result = file.delete();
        slog.kv("file", file).kv("logId", entryLogId).kv("result", result).info(Events.LOG_DELETED);
        return result;
    }

    @Override
    public void scanEntryLog(long entryLogId, EntryLogScanner scanner) throws IOException {
        checkArgument(entryLogId < Integer.MAX_VALUE, "Entry log id must be an int [%d]", entryLogId);
        try (LogReader reader = newDirectReader((int) entryLogId)) {
            LogReaderScan.scan(allocator, reader, scanner);
        }
    }

    @Override
    public boolean logExists(long logId) {
        checkArgument(logId < Integer.MAX_VALUE, "Entry log id must be an int [%d]", logId);
        return logFile(ledgerDir, (int) logId).exists();
    }

    @Override
    public EntryLogMetadata getEntryLogMetadata(long entryLogId, AbstractLogCompactor.Throttler throttler)
            throws IOException {
        try {
            return readEntryLogIndex(entryLogId);
        } catch (IOException e) {
            slog.kv("entryLogId", entryLogId).kv("reason", e.getMessage())
                .info(Events.READ_METADATA_FALLBACK);
            return scanEntryLogMetadata(entryLogId, throttler);
        }
    }

    @VisibleForTesting
    EntryLogMetadata readEntryLogIndex(long logId) throws IOException {
        checkArgument(logId < Integer.MAX_VALUE, "Entry log id must be an int [%d]", logId);

        try (LogReader reader = newDirectReader((int) logId)) {
            return LogMetadata.read(reader);
        }
    }

    @VisibleForTesting
    EntryLogMetadata scanEntryLogMetadata(long logId, AbstractLogCompactor.Throttler throttler) throws IOException {
        final EntryLogMetadata meta = new EntryLogMetadata(logId);

        // Read through the entry log file and extract the entry log meta
        scanEntryLog(logId, new EntryLogScanner() {
            @Override
            public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                // add new entry size of a ledger to entry log meta
                if (throttler != null) {
                    throttler.acquire(entry.readableBytes());
                }
                meta.addLedgerSize(ledgerId, entry.readableBytes() + Integer.BYTES);
            }

            @Override
            public boolean accept(long ledgerId) {
                return ledgerId >= 0;
            }
        });
        return meta;
    }

    @VisibleForTesting
    LogReader newDirectReader(int logId) throws IOException {
        return new DirectReader(logId, logFilename(ledgerDir, logId),
                                allocator, nativeIO, readBufferSize,
                                maxSaneEntrySize, stats.getReadBlockStats());
    }

    private LogWriter newDirectWriter(int newId) throws IOException {
        unflushedLogs.add(newId);
        LogWriter writer = new DirectWriter(newId, logFilename(ledgerDir, newId), maxFileSize,
                                            writeExecutor, writeBuffers, nativeIO, slog);
        ByteBuf buf = allocator.buffer(Buffer.ALIGNMENT);
        try {
            Header.writeEmptyHeader(buf);
            writer.writeAt(0, buf);
            writer.position(buf.capacity());
        } finally {
            ReferenceCountUtil.release(buf);
        }
        return writer;
    }

    public static File logFile(File directory, int logId) {
        return new File(directory, Long.toHexString(logId) + LOG_FILE_SUFFIX);
    }

    public static String logFilename(File directory, int logId) {
        return logFile(directory, logId).toString();
    }

    @Override
    public CompactionEntryLog newCompactionLog(long srcLogId) throws IOException {
        int dstLogId = ids.nextId();
        return DirectCompactionEntryLog.newLog((int) srcLogId, dstLogId, ledgerDir,
                                               maxFileSize, writeExecutor, writeBuffers,
                                               nativeIO, allocator, slog);
    }

    @Override
    public Collection<CompactionEntryLog> incompleteCompactionLogs() {
        List<CompactionEntryLog> logs = new ArrayList<>();

        if (ledgerDir.exists() && ledgerDir.isDirectory()) {
            File[] files = ledgerDir.listFiles();
            if (files != null && files.length > 0) {
                for (File f : files) {
                    if (f.getName().endsWith(COMPACTING_SUFFIX)) {
                        try {
                            Files.deleteIfExists(f.toPath());
                        } catch (IOException ioe) {
                            slog.kv("file", f).warn(Events.COMPACTION_DELETE_FAILURE);
                        }
                    }

                    Matcher m = LedgerDirUtil.COMPACTED_FILE_PATTERN.matcher(f.getName());
                    if (m.matches()) {
                        int dstLogId = Integer.parseUnsignedInt(m.group(1), 16);
                        int srcLogId = Integer.parseUnsignedInt(m.group(2), 16);

                        logs.add(DirectCompactionEntryLog.recoverLog(srcLogId, dstLogId, ledgerDir,
                            readBufferSize, maxSaneEntrySize,
                            nativeIO, allocator,
                            stats.getReadBlockStats(),
                            slog));
                    }
                }
            }
        }
        return logs;
    }
}
