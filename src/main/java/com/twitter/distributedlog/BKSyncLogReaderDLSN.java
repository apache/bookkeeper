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
package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.IdleReaderException;
import com.twitter.distributedlog.injector.AsyncFailureInjector;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Synchronous Log Reader based on {@link AsyncLogReader}
 */
class BKSyncLogReaderDLSN implements LogReader, AsyncNotification {

    private final BKLogReadHandler readHandler;
    private final AtomicReference<IOException> readerException =
            new AtomicReference<IOException>(null);
    private final int maxReadAheadWaitTime;
    private Promise<Void> closeFuture;
    private final Optional<Long> startTransactionId;
    private boolean positioned = false;
    private Entry.Reader currentEntry = null;

    // idle reader settings
    private final boolean shouldCheckIdleReader;
    private final int idleErrorThresholdMillis;

    // Stats
    private final Counter idleReaderError;

    BKSyncLogReaderDLSN(DistributedLogConfiguration conf,
                        BKDistributedLogManager bkdlm,
                        DLSN startDLSN,
                        Optional<Long> startTransactionId,
                        StatsLogger statsLogger) {
        this.readHandler = bkdlm.createReadHandler(
                Optional.<String>absent(),
                bkdlm.getLockStateExecutor(true),
                this,
                conf.getDeserializeRecordSetOnReads(),
                true);
        this.maxReadAheadWaitTime = conf.getReadAheadWaitTime();
        this.idleErrorThresholdMillis = conf.getReaderIdleErrorThresholdMillis();
        this.shouldCheckIdleReader = idleErrorThresholdMillis > 0 && idleErrorThresholdMillis < Integer.MAX_VALUE;
        this.startTransactionId = startTransactionId;
        readHandler.startReadAhead(
                new LedgerReadPosition(startDLSN),
                AsyncFailureInjector.NULL);
        if (!startTransactionId.isPresent()) {
            positioned = true;
        }

        // Stats
        StatsLogger syncReaderStatsLogger = statsLogger.scope("sync_reader");
        idleReaderError = syncReaderStatsLogger.getCounter("idle_reader_error");
    }

    @VisibleForTesting
    BKLogReadHandler getReadHandler() {
        return readHandler;
    }

    // reader is still catching up, waiting for next record

    private Entry.Reader readNextEntry(boolean nonBlocking) throws IOException {
        Entry.Reader entry = null;
        if (nonBlocking) {
            return readHandler.getNextReadAheadEntry();
        } else {
            while (!readHandler.isReadAheadCaughtUp()
                    && null == readerException.get()
                    && null == entry) {
                entry = readHandler.getNextReadAheadEntry(maxReadAheadWaitTime,
                        TimeUnit.MILLISECONDS);
            }
            if (null != entry) {
                return entry;
            }
            // reader is caught up
            if (readHandler.isReadAheadCaughtUp()
                    && null == entry
                    && null == readerException.get()) {
                entry = readHandler.getNextReadAheadEntry(maxReadAheadWaitTime,
                        TimeUnit.MILLISECONDS);
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
        if (null != readerException.get()) {
            throw readerException.get();
        }

        LogRecordWithDLSN record = doReadNext(nonBlocking);

        // no record is returned, check if the reader becomes idle
        if (null == record && shouldCheckIdleReader) {
            ReadAheadCache cache = readHandler.getReadAheadCache();
            if (cache.getNumCachedEntries() <= 0 &&
                    cache.isReadAheadIdle(idleErrorThresholdMillis, TimeUnit.MILLISECONDS)) {
                markReaderAsIdle();
            }
        }

        return record;
    }

    private synchronized LogRecordWithDLSN doReadNext(boolean nonBlocking)
            throws IOException {
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
    public Future<Void> asyncClose() {
        Promise<Void> closePromise;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closeFuture = closePromise = new Promise<Void>();
        }
        readHandler.asyncClose().proxyTo(closePromise);
        return closePromise;
    }

    @Override
    public void close() throws IOException {
        FutureUtils.result(asyncClose());
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
