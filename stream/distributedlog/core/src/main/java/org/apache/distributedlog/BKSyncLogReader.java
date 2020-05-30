/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.apache.distributedlog.exceptions.IdleReaderException;
import org.apache.distributedlog.util.Utils;




/**
 * Synchronous Log Reader based on {@link AsyncLogReader}.
 */
class BKSyncLogReader implements LogReader, AsyncNotification {

    private final BKDistributedLogManager bkdlm;
    private final BKLogReadHandler readHandler;
    private final AtomicReference<IOException> readerException =
            new AtomicReference<IOException>(null);
    private final int maxReadAheadWaitTime;
    private CompletableFuture<Void> closeFuture;
    private final Optional<Long> startTransactionId;
    private boolean positioned = false;
    private Entry.Reader currentEntry = null;

    // readahead reader
    ReadAheadEntryReader readAheadReader = null;

    // idle reader settings
    private final boolean shouldCheckIdleReader;
    private final int idleErrorThresholdMillis;

    // Stats
    private final Counter idleReaderError;

    BKSyncLogReader(DistributedLogConfiguration conf,
                    BKDistributedLogManager bkdlm,
                    DLSN startDLSN,
                    Optional<Long> startTransactionId,
                    StatsLogger statsLogger) throws IOException {
        this.bkdlm = bkdlm;
        this.readHandler = bkdlm.createReadHandler(
                Optional.<String>empty(),
                this,
                true);
        this.maxReadAheadWaitTime = conf.getReadAheadWaitTime();
        this.idleErrorThresholdMillis = conf.getReaderIdleErrorThresholdMillis();
        this.shouldCheckIdleReader = idleErrorThresholdMillis > 0 && idleErrorThresholdMillis < Integer.MAX_VALUE;
        this.startTransactionId = startTransactionId;

        // start readahead
        startReadAhead(startDLSN);
        if (!startTransactionId.isPresent()) {
            positioned = true;
        }

        // Stats
        StatsLogger syncReaderStatsLogger = statsLogger.scope("sync_reader");
        idleReaderError = syncReaderStatsLogger.getCounter("idle_reader_error");
    }

    private void startReadAhead(DLSN startDLSN) throws IOException {
        readAheadReader = new ReadAheadEntryReader(
                    bkdlm.getStreamName(),
                    startDLSN,
                    bkdlm.getConf(),
                    readHandler,
                    bkdlm.getReaderEntryStore(),
                    bkdlm.getScheduler(),
                    Ticker.systemTicker(),
                    bkdlm.alertStatsLogger);
        readHandler.registerListener(readAheadReader);
        readHandler.asyncStartFetchLogSegments()
                .thenApply(logSegments -> {
                    readAheadReader.addStateChangeNotification(BKSyncLogReader.this);
                    readAheadReader.start(logSegments.getValue());
                    return null;
                });
    }

    synchronized void releaseCurrentEntry() {
        if (null != currentEntry) {
            currentEntry.release();
            currentEntry = null;
        }
    }

    synchronized void checkClosedOrException() throws IOException {
        if (null != closeFuture) {
            throw new IOException("Reader is closed");
        }
        if (null != readerException.get()) {
            throw readerException.get();
        }
    }

    @VisibleForTesting
    ReadAheadEntryReader getReadAheadReader() {
        return readAheadReader;
    }

    @VisibleForTesting
    BKLogReadHandler getReadHandler() {
        return readHandler;
    }

    private Entry.Reader readNextEntry(boolean nonBlocking) throws IOException {
        Entry.Reader entry = null;
        if (nonBlocking) {
            return readAheadReader.getNextReadAheadEntry(0L, TimeUnit.MILLISECONDS);
        } else {
            while (!readAheadReader.isReadAheadCaughtUp()
                    && null == readerException.get()
                    && null == entry) {
                entry = readAheadReader.getNextReadAheadEntry(maxReadAheadWaitTime, TimeUnit.MILLISECONDS);
            }
            if (null != entry) {
                return entry;
            }
            // reader is caught up
            if (readAheadReader.isReadAheadCaughtUp()
                    && null == readerException.get()) {
                entry = readAheadReader.getNextReadAheadEntry(maxReadAheadWaitTime, TimeUnit.MILLISECONDS);
            }
            return entry;
        }
    }

    private void markReaderAsIdle() throws IdleReaderException {
        idleReaderError.inc();
        IdleReaderException ire = new IdleReaderException("Sync reader on stream "
                + readHandler.getFullyQualifiedName()
                + " is idle for more than " + idleErrorThresholdMillis + " ms");
        readerException.compareAndSet(null, ire);
        throw ire;
    }

    @Override
    public synchronized LogRecordWithDLSN readNext(boolean nonBlocking)
            throws IOException {
        checkClosedOrException();

        LogRecordWithDLSN record = doReadNext(nonBlocking);
        // no record is returned, check if the reader becomes idle
        if (null == record && shouldCheckIdleReader) {
            if (readAheadReader.getNumCachedEntries() <= 0
                    && readAheadReader.isReaderIdle(idleErrorThresholdMillis, TimeUnit.MILLISECONDS)) {
                markReaderAsIdle();
            }
        }
        return record;
    }

    private LogRecordWithDLSN doReadNext(boolean nonBlocking) throws IOException {
        LogRecordWithDLSN record = null;

        do {
            // fetch one record until we don't find any entry available in the readahead cache
            while (null == record) {
                if (null == currentEntry) {
                    currentEntry = readNextEntry(nonBlocking);
                    if (null == currentEntry) {
                        return null;
                    }
                }
                record = currentEntry.nextRecord();
                if (null == record) {
                    currentEntry = null;
                }
            }

            // check if we reached the end of stream
            if (record.isEndOfStream()) {
                EndOfStreamException eos = new EndOfStreamException("End of Stream Reached for "
                        + readHandler.getFullyQualifiedName());
                readerException.compareAndSet(null, eos);
                throw eos;
            }
            // skip control records
            if (record.isControl()) {
                record = null;
                continue;
            }
            if (!positioned) {
                if (record.getTransactionId() < startTransactionId.get()) {
                    record = null;
                    continue;
                } else {
                    positioned = true;
                    break;
                }
            } else {
                break;
            }
        } while (true);
        return record;
    }

    @Override
    public synchronized List<LogRecordWithDLSN> readBulk(boolean nonBlocking, int numLogRecords)
            throws IOException {
        LinkedList<LogRecordWithDLSN> retList =
                new LinkedList<LogRecordWithDLSN>();

        int numRead = 0;
        LogRecordWithDLSN record = readNext(nonBlocking);
        while ((null != record)) {
            retList.add(record);
            numRead++;
            if (numRead >= numLogRecords) {
                break;
            }
            record = readNext(nonBlocking);
        }
        return retList;
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        CompletableFuture<Void> closePromise;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closeFuture = closePromise = new CompletableFuture<Void>();
            releaseCurrentEntry();
        }
        readHandler.unregisterListener(readAheadReader);
        readAheadReader.removeStateChangeNotification(this);
        FutureUtils.proxyTo(
            Utils.closeSequence(bkdlm.getScheduler(), true,
                    readAheadReader,
                    readHandler
            ),
            closePromise);
        return closePromise;
    }

    @Override
    public void close() throws IOException {
        Utils.ioResult(asyncClose());
    }

    //
    // Notification From ReadHandler
    //

    @Override
    public void notifyOnError(Throwable cause) {
        if (cause instanceof IOException) {
            readerException.compareAndSet(null, (IOException) cause);
        } else {
            readerException.compareAndSet(null, new IOException(cause));
        }
    }

    @Override
    public void notifyOnOperationComplete() {
        // no-op
    }
}
