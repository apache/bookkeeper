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
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.StreamNotReadyException;
import org.apache.distributedlog.exceptions.WriteCancelledException;
import org.apache.distributedlog.exceptions.WriteException;
import org.apache.distributedlog.feature.CoreFeatureKeys;
import org.apache.distributedlog.util.FailpointUtils;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BookKeeper based {@link AsyncLogWriter} implementation.
 *
 * <h3>Metrics</h3>
 * All the metrics are exposed under `log_writer`.
 * <ul>
 * <li> `log_writer/write`: opstats. latency characteristics about the time that write operations spent.
 * <li> `log_writer/bulk_write`: opstats. latency characteristics about the time that bulk_write
 * operations spent.
 * are pending in the queue for long time due to log segment rolling.
 * <li> `log_writer/get_writer`: opstats. the time spent on getting the writer. it could spike when there
 * is log segment rolling happened during getting the writer. it is a good stat to look into when the latency
 * is caused by queuing time.
 * <li> `log_writer/pending_request_dispatch`: counter. the number of queued operations that are dispatched
 * after log segment is rolled. it is an metric on measuring how many operations has been queued because of
 * log segment rolling.
 * </ul>
 * See {@link BKLogSegmentWriter} for segment writer stats.
 */
class BKAsyncLogWriter extends BKAbstractLogWriter implements AsyncLogWriter {

    static final Logger LOG = LoggerFactory.getLogger(BKAsyncLogWriter.class);

    static Function<List<LogSegmentMetadata>, Boolean> truncationResultConverter =
        segments -> true;

    // Records pending for roll log segment.
    class PendingLogRecord implements FutureEventListener<DLSN> {

        final LogRecord record;
        final CompletableFuture<DLSN> promise;
        final boolean flush;

        PendingLogRecord(LogRecord record, boolean flush) {
            this.record = record;
            this.promise = new CompletableFuture<DLSN>();
            this.flush = flush;
        }

        @Override
        public void onSuccess(DLSN value) {
            promise.complete(value);
        }

        @Override
        public void onFailure(Throwable cause) {
            promise.completeExceptionally(cause);
            encounteredError = true;
        }
    }

    /**
     * Last pending record in current log segment. After it is satisified, it would
     * roll log segment.
     * This implementation is based on the assumption that all future satisified in same
     * order future pool.
     */
    class LastPendingLogRecord extends PendingLogRecord {

        LastPendingLogRecord(LogRecord record, boolean flush) {
            super(record, flush);
        }

        @Override
        public void onSuccess(DLSN value) {
            super.onSuccess(value);
            // roll log segment and issue all pending requests.
            rollLogSegmentAndIssuePendingRequests(record.getTransactionId());
        }

        @Override
        public void onFailure(Throwable cause) {
            super.onFailure(cause);
            // error out pending requests.
            errorOutPendingRequestsAndWriter(cause);
        }
    }

    private final boolean streamFailFast;
    private final boolean disableRollOnSegmentError;
    private LinkedList<PendingLogRecord> pendingRequests = null;
    private volatile boolean encounteredError = false;
    private CompletableFuture<BKLogSegmentWriter> rollingFuture = null;
    private long lastTxId = DistributedLogConstants.INVALID_TXID;

    private final StatsLogger statsLogger;
    private final OpStatsLogger writeOpStatsLogger;
    private final OpStatsLogger markEndOfStreamOpStatsLogger;
    private final OpStatsLogger bulkWriteOpStatsLogger;
    private final OpStatsLogger getWriterOpStatsLogger;
    private final Counter pendingRequestDispatch;

    private final Feature disableLogSegmentRollingFeature;

    BKAsyncLogWriter(DistributedLogConfiguration conf,
                     DynamicDistributedLogConfiguration dynConf,
                     BKDistributedLogManager bkdlm,
                     BKLogWriteHandler writeHandler, /** log writer owns the handler **/
                     FeatureProvider featureProvider,
                     StatsLogger dlmStatsLogger) {
        super(conf, dynConf, bkdlm);
        this.writeHandler = writeHandler;
        this.streamFailFast = conf.getFailFastOnStreamNotReady();
        this.disableRollOnSegmentError = conf.getDisableRollingOnLogSegmentError();

        // features
        disableLogSegmentRollingFeature = featureProvider
                .getFeature(CoreFeatureKeys.DISABLE_LOGSEGMENT_ROLLING.name().toLowerCase());
        // stats
        this.statsLogger = dlmStatsLogger.scope("log_writer");
        this.writeOpStatsLogger = statsLogger.getOpStatsLogger("write");
        this.markEndOfStreamOpStatsLogger = statsLogger.getOpStatsLogger("mark_end_of_stream");
        this.bulkWriteOpStatsLogger = statsLogger.getOpStatsLogger("bulk_write");
        this.getWriterOpStatsLogger = statsLogger.getOpStatsLogger("get_writer");
        this.pendingRequestDispatch = statsLogger.getCounter("pending_request_dispatch");
    }

    @VisibleForTesting
    synchronized void setLastTxId(long txId) {
        lastTxId = Math.max(lastTxId, txId);
    }

    @Override
    public synchronized long getLastTxId() {
        return lastTxId;
    }

    /**
     * Write a log record as control record.
     * The method will be used by Monitor Service to enforce a new inprogress segment.
     *
     * @param record
     *          log record
     * @return future of the write
     */
    public CompletableFuture<DLSN> writeControlRecord(final LogRecord record) {
        record.setControl();
        return write(record);
    }

    private BKLogSegmentWriter getCachedLogSegmentWriter() throws WriteException {
        if (encounteredError) {
            throw new WriteException(bkDistributedLogManager.getStreamName(),
                    "writer has been closed due to error.");
        }
        BKLogSegmentWriter segmentWriter = getCachedLogWriter();
        if (null != segmentWriter
                && segmentWriter.isLogSegmentInError()
                && !disableRollOnSegmentError) {
            return null;
        } else {
            return segmentWriter;
        }
    }

    private CompletableFuture<BKLogSegmentWriter> getLogSegmentWriter(long firstTxid,
                                                           boolean bestEffort,
                                                           boolean rollLog,
                                                           boolean allowMaxTxID) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        return FutureUtils.stats(
                doGetLogSegmentWriter(firstTxid, bestEffort, rollLog, allowMaxTxID),
                getWriterOpStatsLogger,
                stopwatch);
    }

    private CompletableFuture<BKLogSegmentWriter> doGetLogSegmentWriter(final long firstTxid,
                                                             final boolean bestEffort,
                                                             final boolean rollLog,
                                                             final boolean allowMaxTxID) {
        if (encounteredError) {
            return FutureUtils.exception(new WriteException(bkDistributedLogManager.getStreamName(),
                    "writer has been closed due to error."));
        }
        CompletableFuture<BKLogSegmentWriter> writerFuture = asyncGetLedgerWriter(!disableRollOnSegmentError);
        if (null == writerFuture) {
            return rollLogSegmentIfNecessary(null, firstTxid, bestEffort, allowMaxTxID);
        } else if (rollLog) {
            return writerFuture.thenCompose(
                writer -> rollLogSegmentIfNecessary(writer, firstTxid, bestEffort, allowMaxTxID));
        } else {
            return writerFuture;
        }
    }

    /**
     * We write end of stream marker by writing a record with MAX_TXID, so we need to allow using
     * max txid when rolling for this case only.
     */
    private CompletableFuture<BKLogSegmentWriter> getLogSegmentWriterForEndOfStream() {
        return getLogSegmentWriter(DistributedLogConstants.MAX_TXID,
                                     false /* bestEffort */,
                                     false /* roll log */,
                                     true /* allow max txid */);
    }

    private CompletableFuture<BKLogSegmentWriter> getLogSegmentWriter(long firstTxid,
                                                           boolean bestEffort,
                                                           boolean rollLog) {
        return getLogSegmentWriter(firstTxid, bestEffort, rollLog, false /* allow max txid */);
    }

    CompletableFuture<DLSN> queueRequest(LogRecord record, boolean flush) {
        PendingLogRecord pendingLogRecord = new PendingLogRecord(record, flush);
        pendingRequests.add(pendingLogRecord);
        return pendingLogRecord.promise;
    }

    boolean shouldRollLog(BKLogSegmentWriter w) {
        try {
            return null == w
                    || (!disableLogSegmentRollingFeature.isAvailable()
                    && shouldStartNewSegment(w));
        } catch (IOException ioe) {
            return false;
        }
    }

    void startQueueingRequests() {
        assert(null == pendingRequests && null == rollingFuture);
        pendingRequests = new LinkedList<PendingLogRecord>();
        rollingFuture = new CompletableFuture<BKLogSegmentWriter>();
    }

    // for ordering guarantee, we shouldn't send requests to next log segments until
    // previous log segment is done.
    private synchronized CompletableFuture<DLSN> asyncWrite(final LogRecord record,
                                                 boolean flush) {
        // The passed in writer may be stale since we acquire the writer outside of sync
        // lock. If we recently rolled and the new writer is cached, use that instead.
        CompletableFuture<DLSN> result = null;
        BKLogSegmentWriter w;
        try {
            w = getCachedLogSegmentWriter();
        } catch (WriteException we) {
            return FutureUtils.exception(we);
        }
        if (null != rollingFuture) {
            if (streamFailFast) {
                result = FutureUtils.exception(new StreamNotReadyException("Rolling log segment"));
            } else {
                result = queueRequest(record, flush);
            }
        } else if (shouldRollLog(w)) {
            // insert a last record, so when it called back, we will trigger a log segment rolling
            startQueueingRequests();
            if (null != w) {
                LastPendingLogRecord lastLogRecordInCurrentSegment = new LastPendingLogRecord(record, flush);
                w.asyncWrite(record, true).whenComplete(lastLogRecordInCurrentSegment);
                result = lastLogRecordInCurrentSegment.promise;
            } else { // no log segment yet. roll the log segment and issue pending requests.
                result = queueRequest(record, flush);
                rollLogSegmentAndIssuePendingRequests(record.getTransactionId());
            }
        } else {
            result = w.asyncWrite(record, flush);
        }
        // use map here rather than onSuccess because we want lastTxId to be updated before
        // satisfying the future
        return result.thenApply(dlsn -> {
            setLastTxId(record.getTransactionId());
            return dlsn;
        });
    }

    private List<CompletableFuture<DLSN>> asyncWriteBulk(List<LogRecord> records) {
        final ArrayList<CompletableFuture<DLSN>> results = new ArrayList<CompletableFuture<DLSN>>(records.size());
        Iterator<LogRecord> iterator = records.iterator();
        while (iterator.hasNext()) {
            LogRecord record = iterator.next();
            CompletableFuture<DLSN> future = asyncWrite(record, !iterator.hasNext());
            results.add(future);

            // Abort early if an individual write has already failed.
            if (future.isDone() && future.isCompletedExceptionally()) {
                break;
            }
        }
        if (records.size() > results.size()) {
            appendCancelledFutures(results, records.size() - results.size());
        }
        return results;
    }

    private void appendCancelledFutures(List<CompletableFuture<DLSN>> futures, int numToAdd) {
        final WriteCancelledException cre =
            new WriteCancelledException(getStreamName());
        for (int i = 0; i < numToAdd; i++) {
            CompletableFuture<DLSN> cancelledFuture = FutureUtils.exception(cre);
            futures.add(cancelledFuture);
        }
    }

    private void rollLogSegmentAndIssuePendingRequests(final long firstTxId) {
        getLogSegmentWriter(firstTxId, true, true)
                .whenComplete(new FutureEventListener<BKLogSegmentWriter>() {
            @Override
            public void onSuccess(BKLogSegmentWriter writer) {
                try {
                    synchronized (BKAsyncLogWriter.this) {
                        for (PendingLogRecord pendingLogRecord : pendingRequests) {
                            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_LogWriterIssuePending);
                            writer.asyncWrite(pendingLogRecord.record, pendingLogRecord.flush)
                                    .whenComplete(pendingLogRecord);
                        }
                        // if there are no records in the pending queue, let's write a control record
                        // so that when a new log segment is rolled, a control record will be added and
                        // the corresponding bookies would be able to create its ledger.
                        if (pendingRequests.isEmpty()) {
                            LogRecord controlRecord = new LogRecord(firstTxId,
                                    DistributedLogConstants.CONTROL_RECORD_CONTENT);
                            controlRecord.setControl();
                            PendingLogRecord controlReq = new PendingLogRecord(controlRecord, false);
                            writer.asyncWrite(controlReq.record, controlReq.flush)
                                    .whenComplete(controlReq);
                        }
                        if (null != rollingFuture) {
                            FutureUtils.complete(rollingFuture, writer);
                        }
                        rollingFuture = null;
                        pendingRequestDispatch.add(pendingRequests.size());
                        pendingRequests = null;
                    }
                } catch (IOException ioe) {
                    errorOutPendingRequestsAndWriter(ioe);
                }
            }
            @Override
            public void onFailure(Throwable cause) {
                errorOutPendingRequestsAndWriter(cause);
            }
        });
    }

    @VisibleForTesting
    void errorOutPendingRequests(Throwable cause, boolean errorOutWriter) {
        final List<PendingLogRecord> pendingRequestsSnapshot;
        synchronized (this) {
            pendingRequestsSnapshot = pendingRequests;
            encounteredError = errorOutWriter;
            pendingRequests = null;
            if (null != rollingFuture) {
                FutureUtils.completeExceptionally(rollingFuture, cause);
            }
            rollingFuture = null;
        }

        pendingRequestDispatch.add(pendingRequestsSnapshot.size());

        // After erroring out the writer above, no more requests
        // will be enqueued to pendingRequests
        for (PendingLogRecord pendingLogRecord : pendingRequestsSnapshot) {
            pendingLogRecord.promise.completeExceptionally(cause);
        }
    }

    void errorOutPendingRequestsAndWriter(Throwable cause) {
        errorOutPendingRequests(cause, true /* error out writer */);
    }

    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     */
    @Override
    public CompletableFuture<DLSN> write(final LogRecord record) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return FutureUtils.stats(
                asyncWrite(record, true),
                writeOpStatsLogger,
                stopwatch);
    }

    /**
     * Write many log records to the stream. The return type here is unfortunate but its a direct result
     * of having to combine FuturePool and the asyncWriteBulk method which returns a future as well. The
     * problem is the List that asyncWriteBulk returns can't be materialized until getLogSegmentWriter
     * completes, so it has to be wrapped in a future itself.
     *
     * @param records list of records
     */
    @Override
    public CompletableFuture<List<CompletableFuture<DLSN>>> writeBulk(final List<LogRecord> records) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return FutureUtils.stats(
                FutureUtils.value(asyncWriteBulk(records)),
                bulkWriteOpStatsLogger,
                stopwatch);
    }

    @Override
    public CompletableFuture<Boolean> truncate(final DLSN dlsn) {
        if (DLSN.InvalidDLSN == dlsn) {
            return FutureUtils.value(false);
        }
        BKLogWriteHandler writeHandler;
        try {
            writeHandler = getWriteHandler();
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        return writeHandler.setLogSegmentsOlderThanDLSNTruncated(dlsn).thenApply(truncationResultConverter);
    }

    CompletableFuture<Long> flushAndCommit() {
        CompletableFuture<BKLogSegmentWriter> writerFuture;
        synchronized (this) {
            if (null != this.rollingFuture) {
                writerFuture = this.rollingFuture;
            } else {
                writerFuture = getCachedLogWriterFuture();
            }
        }
        if (null == writerFuture) {
            return FutureUtils.value(getLastTxId());
        }
        return writerFuture.thenCompose(writer -> writer.flushAndCommit());
    }

    @Override
    public CompletableFuture<Long> markEndOfStream() {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        CompletableFuture<BKLogSegmentWriter> logSegmentWriterFuture;
        synchronized (this) {
            logSegmentWriterFuture = this.rollingFuture;
        }
        if (null == logSegmentWriterFuture) {
            logSegmentWriterFuture = getLogSegmentWriterForEndOfStream();
        }

        return FutureUtils.stats(
                logSegmentWriterFuture.thenCompose(w -> w.markEndOfStream()),
                markEndOfStreamOpStatsLogger,
                stopwatch);
    }

    @Override
    protected CompletableFuture<Void> asyncCloseAndComplete() {
        CompletableFuture<BKLogSegmentWriter> logSegmentWriterFuture;
        synchronized (this) {
            logSegmentWriterFuture = this.rollingFuture;
        }

        if (null == logSegmentWriterFuture) {
            return super.asyncCloseAndComplete();
        } else {
            return logSegmentWriterFuture.thenCompose(segmentWriter1 -> super.asyncCloseAndComplete());
        }
    }

    @Override
    void closeAndComplete() throws IOException {
        Utils.ioResult(asyncCloseAndComplete());
    }

    /**
     * *TEMP HACK*
     * Get the name of the stream this writer writes data to.
     */
    @Override
    public String getStreamName() {
        return bkDistributedLogManager.getStreamName();
    }

    @Override
    public CompletableFuture<Void> asyncAbort() {
        CompletableFuture<Void> result = super.asyncAbort();
        synchronized (this) {
            if (pendingRequests != null) {
                for (PendingLogRecord pendingLogRecord : pendingRequests) {
                    pendingLogRecord.promise
                            .completeExceptionally(new WriteException(bkDistributedLogManager.getStreamName(),
                            "abort wring: writer has been closed due to error."));
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("AsyncLogWriter:%s", getStreamName());
    }
}
