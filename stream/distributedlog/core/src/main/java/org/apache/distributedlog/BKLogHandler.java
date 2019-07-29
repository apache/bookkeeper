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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.callback.LogSegmentNamesListener;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.exceptions.LogSegmentNotFoundException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.io.AsyncAbortable;
import org.apache.distributedlog.io.AsyncCloseable;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.logsegment.LogSegmentFilter;
import org.apache.distributedlog.logsegment.LogSegmentMetadataCache;
import org.apache.distributedlog.logsegment.LogSegmentMetadataStore;
import org.apache.distributedlog.logsegment.PerStreamLogSegmentCache;
import org.apache.distributedlog.metadata.LogMetadata;
import org.apache.distributedlog.metadata.LogStreamMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base class about log handler on managing log segments.
 *
 * <h3>Metrics</h3>
 * The log handler is a base class on managing log segments. so all the metrics
 * here are related to log segments retrieval and exposed under `logsegments`.
 * These metrics are all OpStats, in the format of <code>`scope`/logsegments/`op`</code>.
 *
 * <p>Those operations are:
 * <ul>
 * <li>get_inprogress_segment: time between the inprogress log segment created and
 * the handler read it.
 * <li>get_completed_segment: time between a log segment is turned to completed and
 * the handler read it.
 * <li>negative_get_inprogress_segment: record the negative values for `get_inprogress_segment`.
 * <li>negative_get_completed_segment: record the negative values for `get_completed_segment`.
 * <li>recover_last_entry: recovering last entry from a log segment
 * <li>recover_scanned_entries: the number of entries that are scanned during recovering.
 * </ul>
 * @see BKLogWriteHandler
 * @see BKLogReadHandler
 */
abstract class BKLogHandler implements AsyncCloseable, AsyncAbortable {
    static final Logger LOG = LoggerFactory.getLogger(BKLogHandler.class);

    protected final LogMetadata logMetadata;
    protected final DistributedLogConfiguration conf;
    protected final LogStreamMetadataStore streamMetadataStore;
    protected final LogSegmentMetadataStore metadataStore;
    protected final LogSegmentMetadataCache metadataCache;
    protected final LogSegmentEntryStore entryStore;
    protected final int firstNumEntriesPerReadLastRecordScan;
    protected final int maxNumEntriesPerReadLastRecordScan;
    protected volatile long lastLedgerRollingTimeMillis = -1;
    protected final OrderedScheduler scheduler;
    protected final StatsLogger statsLogger;
    protected final AlertStatsLogger alertStatsLogger;
    protected volatile boolean reportGetSegmentStats = false;
    private final String lockClientId;
    protected static final AtomicReferenceFieldUpdater<BKLogHandler, IOException> METADATA_EXCEPTION_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(BKLogHandler.class, IOException.class, "metadataException");
    private volatile IOException metadataException = null;

    // Maintain the list of log segments per stream
    protected final PerStreamLogSegmentCache logSegmentCache;

    // trace
    protected final long metadataLatencyWarnThresholdMillis;

    // Stats
    private final OpStatsLogger getInprogressSegmentStat;
    private final OpStatsLogger getCompletedSegmentStat;
    private final OpStatsLogger negativeGetInprogressSegmentStat;
    private final OpStatsLogger negativeGetCompletedSegmentStat;
    private final OpStatsLogger recoverLastEntryStats;
    private final OpStatsLogger recoverScannedEntriesStats;

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogHandler(LogMetadata metadata,
                 DistributedLogConfiguration conf,
                 LogStreamMetadataStore streamMetadataStore,
                 LogSegmentMetadataCache metadataCache,
                 LogSegmentEntryStore entryStore,
                 OrderedScheduler scheduler,
                 StatsLogger statsLogger,
                 AlertStatsLogger alertStatsLogger,
                 String lockClientId) {
        this.logMetadata = metadata;
        this.conf = conf;
        this.scheduler = scheduler;
        this.statsLogger = statsLogger;
        this.alertStatsLogger = alertStatsLogger;
        this.logSegmentCache = new PerStreamLogSegmentCache(
                metadata.getLogName(),
                conf.isLogSegmentSequenceNumberValidationEnabled());
        firstNumEntriesPerReadLastRecordScan = conf.getFirstNumEntriesPerReadLastRecordScan();
        maxNumEntriesPerReadLastRecordScan = conf.getMaxNumEntriesPerReadLastRecordScan();
        this.streamMetadataStore = streamMetadataStore;
        this.metadataStore = streamMetadataStore.getLogSegmentMetadataStore();
        this.metadataCache = metadataCache;
        this.entryStore = entryStore;
        this.lockClientId = lockClientId;

        // Traces
        this.metadataLatencyWarnThresholdMillis = conf.getMetadataLatencyWarnThresholdMillis();

        // Stats
        StatsLogger segmentsLogger = statsLogger.scope("logsegments");
        getInprogressSegmentStat = segmentsLogger.getOpStatsLogger("get_inprogress_segment");
        getCompletedSegmentStat = segmentsLogger.getOpStatsLogger("get_completed_segment");
        negativeGetInprogressSegmentStat = segmentsLogger.getOpStatsLogger("negative_get_inprogress_segment");
        negativeGetCompletedSegmentStat = segmentsLogger.getOpStatsLogger("negative_get_completed_segment");
        recoverLastEntryStats = segmentsLogger.getOpStatsLogger("recover_last_entry");
        recoverScannedEntriesStats = segmentsLogger.getOpStatsLogger("recover_scanned_entries");
    }

    BKLogHandler checkMetadataException() throws IOException {
        IOException ioe = METADATA_EXCEPTION_UPDATER.get(this);
        if (null != ioe) {
            throw ioe;
        }
        return this;
    }

    public void reportGetSegmentStats(boolean enabled) {
        this.reportGetSegmentStats = enabled;
    }

    public String getLockClientId() {
        return lockClientId;
    }

    public CompletableFuture<LogRecordWithDLSN> asyncGetFirstLogRecord() {
        final CompletableFuture<LogRecordWithDLSN> promise = new CompletableFuture<LogRecordWithDLSN>();
        streamMetadataStore.logExists(
            logMetadata.getUri(),
            logMetadata.getLogName()
        ).whenComplete(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null
                ).whenComplete(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {

                    @Override
                    public void onSuccess(Versioned<List<LogSegmentMetadata>> ledgerList) {
                        if (ledgerList.getValue().isEmpty()) {
                            promise.completeExceptionally(new LogEmptyException("Log "
                                    + getFullyQualifiedName() + " has no records"));
                            return;
                        }
                        CompletableFuture<LogRecordWithDLSN> firstRecord = null;
                        for (LogSegmentMetadata ledger : ledgerList.getValue()) {
                            if (!ledger.isTruncated() && (ledger.getRecordCount() > 0 || ledger.isInProgress())) {
                                firstRecord = asyncReadFirstUserRecord(ledger, DLSN.InitialDLSN);
                                break;
                            }
                        }
                        if (null != firstRecord) {
                            FutureUtils.proxyTo(firstRecord, promise);
                        } else {
                            promise.completeExceptionally(new LogEmptyException("Log "
                                    + getFullyQualifiedName() + " has no records"));
                        }
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.completeExceptionally(cause);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                promise.completeExceptionally(cause);
            }
        });
        return promise;
    }

    public CompletableFuture<LogRecordWithDLSN> getLastLogRecordAsync(final boolean recover,
                                                                      final boolean includeEndOfStream) {
        final CompletableFuture<LogRecordWithDLSN> promise = new CompletableFuture<LogRecordWithDLSN>();
        streamMetadataStore.logExists(
            logMetadata.getUri(),
            logMetadata.getLogName()
        ).whenComplete(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                readLogSegmentsFromStore(
                        LogSegmentMetadata.DESC_COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null
                ).whenComplete(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {

                    @Override
                    public void onSuccess(Versioned<List<LogSegmentMetadata>> ledgerList) {
                        if (ledgerList.getValue().isEmpty()) {
                            promise.completeExceptionally(
                                    new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
                            return;
                        }
                        asyncGetLastLogRecord(
                                ledgerList.getValue().iterator(),
                                promise,
                                recover,
                                false,
                                includeEndOfStream);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.completeExceptionally(cause);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                promise.completeExceptionally(cause);
            }
        });
        return promise;
    }

    private void asyncGetLastLogRecord(final Iterator<LogSegmentMetadata> ledgerIter,
                                       final CompletableFuture<LogRecordWithDLSN> promise,
                                       final boolean fence,
                                       final boolean includeControlRecord,
                                       final boolean includeEndOfStream) {
        if (ledgerIter.hasNext()) {
            LogSegmentMetadata metadata = ledgerIter.next();
            asyncReadLastRecord(
                metadata, fence, includeControlRecord, includeEndOfStream
            ).whenComplete(
                new FutureEventListener<LogRecordWithDLSN>() {
                    @Override
                    public void onSuccess(LogRecordWithDLSN record) {
                        if (null == record) {
                            asyncGetLastLogRecord(ledgerIter, promise, fence, includeControlRecord, includeEndOfStream);
                        } else {
                            promise.complete(record);
                        }
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                                                         promise.completeExceptionally(cause);
                                                                                              }
                }
            );
        } else {
            promise.completeExceptionally(new LogEmptyException("Log " + getFullyQualifiedName() + " has no records"));
        }
    }

    private CompletableFuture<LogRecordWithDLSN> asyncReadFirstUserRecord(LogSegmentMetadata ledger, DLSN beginDLSN) {
        return ReadUtils.asyncReadFirstUserRecord(
                getFullyQualifiedName(),
                ledger,
                firstNumEntriesPerReadLastRecordScan,
                maxNumEntriesPerReadLastRecordScan,
                new AtomicInteger(0),
                scheduler,
                entryStore,
                beginDLSN
        );
    }

    /**
     * This is a helper method to compactly return the record count between two records, the first denoted by
     * beginDLSN and the second denoted by endPosition. Its up to the caller to ensure that endPosition refers to
     * position in the same ledger as beginDLSN.
     */
    private CompletableFuture<Long> asyncGetLogRecordCount(LogSegmentMetadata ledger,
                                                           final DLSN beginDLSN,
                                                           final long endPosition) {
        return asyncReadFirstUserRecord(
            ledger, beginDLSN
        ).thenApply(beginRecord -> {
            long recordCount = 0;
            if (null != beginRecord) {
                recordCount = endPosition + 1 - beginRecord.getLastPositionWithinLogSegment();
            }
            return recordCount;
        });
    }

    /**
     * Ledger metadata tells us how many records are in each completed segment, but for the first and last segments
     * we may have to crack open the entry and count. For the first entry, we need to do so because beginDLSN may be
     * an interior entry. For the last entry, if it is inprogress, we need to recover it and find the last user
     * entry.
     */
    private CompletableFuture<Long> asyncGetLogRecordCount(final LogSegmentMetadata ledger, final DLSN beginDLSN) {
        if (ledger.isInProgress() && ledger.isDLSNinThisSegment(beginDLSN)) {
            return asyncReadLastUserRecord(ledger).thenCompose(
                (Function<LogRecordWithDLSN, CompletableFuture<Long>>) endRecord -> {
                    if (null != endRecord) {
                        return asyncGetLogRecordCount(
                            ledger, beginDLSN, endRecord.getLastPositionWithinLogSegment() /* end position */);
                    } else {
                        return FutureUtils.value((long) 0);
                    }
                });
        } else if (ledger.isInProgress()) {
            return asyncReadLastUserRecord(ledger).thenApply(endRecord -> {
                if (null != endRecord) {
                    return (long) endRecord.getLastPositionWithinLogSegment();
                } else {
                    return (long) 0;
                }
            });
        } else if (ledger.isDLSNinThisSegment(beginDLSN)) {
            return asyncGetLogRecordCount(ledger, beginDLSN, ledger.getRecordCount() /* end position */);
        } else {
            return FutureUtils.value((long) ledger.getRecordCount());
        }
    }

    /**
     * Get a count of records between beginDLSN and the end of the stream.
     *
     * @param beginDLSN dlsn marking the start of the range
     * @return the count of records present in the range
     */
    public CompletableFuture<Long> asyncGetLogRecordCount(final DLSN beginDLSN) {
        return streamMetadataStore.logExists(logMetadata.getUri(), logMetadata.getLogName())
                .thenCompose(new Function<Void, CompletableFuture<Long>>() {
            public CompletableFuture<Long> apply(Void done) {

                return readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null
                ).thenCompose(new Function<Versioned<List<LogSegmentMetadata>>, CompletableFuture<Long>>() {
                    public CompletableFuture<Long> apply(Versioned<List<LogSegmentMetadata>> ledgerList) {

                        List<CompletableFuture<Long>> futureCounts =
                          Lists.newArrayListWithExpectedSize(ledgerList.getValue().size());
                        for (LogSegmentMetadata ledger : ledgerList.getValue()) {
                            if (ledger.getLogSegmentSequenceNumber() >= beginDLSN.getLogSegmentSequenceNo()) {
                                futureCounts.add(asyncGetLogRecordCount(ledger, beginDLSN));
                            }
                        }
                        return FutureUtils.collect(futureCounts).thenApply(counts -> sum(counts));
                    }
                });
            }
        });
    }

    private Long sum(List<Long> values) {
        long sum = 0;
        for (Long value : values) {
            sum += value;
        }
        return sum;
    }

    @Override
    public CompletableFuture<Void> asyncAbort() {
        return asyncClose();
    }

    public CompletableFuture<LogRecordWithDLSN> asyncReadLastUserRecord(final LogSegmentMetadata l) {
        return asyncReadLastRecord(l, false, false, false);
    }

    public CompletableFuture<LogRecordWithDLSN> asyncReadLastRecord(final LogSegmentMetadata l,
                                                         final boolean fence,
                                                         final boolean includeControl,
                                                         final boolean includeEndOfStream) {
        final AtomicInteger numRecordsScanned = new AtomicInteger(0);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return ReadUtils.asyncReadLastRecord(
                getFullyQualifiedName(),
                l,
                fence,
                includeControl,
                includeEndOfStream,
                firstNumEntriesPerReadLastRecordScan,
                maxNumEntriesPerReadLastRecordScan,
                numRecordsScanned,
                scheduler,
                entryStore
        ).whenComplete(new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onSuccess(LogRecordWithDLSN value) {
                recoverLastEntryStats.registerSuccessfulEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
                recoverScannedEntriesStats.registerSuccessfulValue(numRecordsScanned.get());
            }

            @Override
            public void onFailure(Throwable cause) {
                recoverLastEntryStats.registerFailedEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS),
                    TimeUnit.MICROSECONDS);
            }
        });
    }

    protected void setLastLedgerRollingTimeMillis(long rollingTimeMillis) {
        if (lastLedgerRollingTimeMillis < rollingTimeMillis) {
            lastLedgerRollingTimeMillis = rollingTimeMillis;
        }
    }

    public String getFullyQualifiedName() {
        return logMetadata.getFullyQualifiedName();
    }

    // Log Segments Related Functions
    //
    // ***Note***
    // Get log segment list should go through #getCachedLogSegments as we need to assign start sequence id
    // for inprogress log segment so the reader could generate the right sequence id.
    //
    // ***PerStreamCache vs LogSegmentMetadataCache **
    // The per stream cache maintains the list of segments per stream, while the metadata cache
    // maintains log segments. The metadata cache is just to reduce the access to zookeeper, it is
    // okay that some of the log segments are not in the cache; however the per stream cache can not
    // have any gaps between log segment sequence numbers which it has to be accurate.

    /**
     * Get the cached log segments.
     *
     * @param comparator the comparator to sort the returned log segments.
     * @return list of sorted log segments
     * @throws UnexpectedException if unexpected condition detected.
     */
    protected List<LogSegmentMetadata> getCachedLogSegments(Comparator<LogSegmentMetadata> comparator)
        throws UnexpectedException {
        try {
            return logSegmentCache.getLogSegments(comparator);
        } catch (UnexpectedException ue) {
            // the log segments cache went wrong
            LOG.error("Unexpected exception on getting log segments from the cache for stream {}",
                    getFullyQualifiedName(), ue);
            METADATA_EXCEPTION_UPDATER.compareAndSet(this, null, ue);
            throw ue;
        }
    }

    /**
     * Add the segment <i>metadata</i> for <i>name</i> in the cache.
     *
     * @param name
     *          segment znode name.
     * @param metadata
     *          segment metadata.
     */
    protected void addLogSegmentToCache(String name, LogSegmentMetadata metadata) {
        metadataCache.put(metadata.getZkPath(), metadata);
        logSegmentCache.add(name, metadata);
        // update the last ledger rolling time
        if (!metadata.isInProgress() && (lastLedgerRollingTimeMillis < metadata.getCompletionTime())) {
            lastLedgerRollingTimeMillis = metadata.getCompletionTime();
        }

        if (reportGetSegmentStats) {
            // update stats
            long ts = System.currentTimeMillis();
            if (metadata.isInProgress()) {
                // as we used timestamp as start tx id we could take it as start time
                // NOTE: it is a hack here.
                long elapsedMillis = ts - metadata.getFirstTxId();
                long elapsedMicroSec = TimeUnit.MILLISECONDS.toMicros(elapsedMillis);
                if (elapsedMicroSec > 0) {
                    if (elapsedMillis > metadataLatencyWarnThresholdMillis) {
                        LOG.warn("{} received inprogress log segment in {} millis: {}",
                            getFullyQualifiedName(), elapsedMillis, metadata);
                    }
                    getInprogressSegmentStat.registerSuccessfulEvent(elapsedMicroSec, TimeUnit.MICROSECONDS);
                } else {
                    negativeGetInprogressSegmentStat.registerSuccessfulEvent(-elapsedMicroSec, TimeUnit.MICROSECONDS);
                }
            } else {
                long elapsedMillis = ts - metadata.getCompletionTime();
                long elapsedMicroSec = TimeUnit.MILLISECONDS.toMicros(elapsedMillis);
                if (elapsedMicroSec > 0) {
                    if (elapsedMillis > metadataLatencyWarnThresholdMillis) {
                        LOG.warn("{} received completed log segment in {} millis : {}",
                            getFullyQualifiedName(), elapsedMillis, metadata);
                    }
                    getCompletedSegmentStat.registerSuccessfulEvent(elapsedMicroSec, TimeUnit.MICROSECONDS);
                } else {
                    negativeGetCompletedSegmentStat.registerSuccessfulEvent(-elapsedMicroSec, TimeUnit.MICROSECONDS);
                }
            }
        }
    }

    /**
     * Read log segment <i>name</i> from the cache.
     *
     * @param name name of the log segment
     * @return log segment metadata
     */
    protected LogSegmentMetadata readLogSegmentFromCache(String name) {
        return logSegmentCache.get(name);
    }

    /**
     * Remove the log segment <i>name</i> from the cache.
     *
     * @param name name of the log segment.
     * @return log segment metadata
     */
    protected LogSegmentMetadata removeLogSegmentFromCache(String name) {
        metadataCache.invalidate(name);
        return logSegmentCache.remove(name);
    }

    /**
     * Update the log segment cache with updated mapping.
     *
     * @param logSegmentsRemoved log segments removed
     * @param logSegmentsAdded log segments added
     */
    protected void updateLogSegmentCache(Set<String> logSegmentsRemoved,
                                         Map<String, LogSegmentMetadata> logSegmentsAdded) {
        for (String segmentName : logSegmentsRemoved) {
            metadataCache.invalidate(segmentName);
        }
        for (Map.Entry<String, LogSegmentMetadata> entry : logSegmentsAdded.entrySet()) {
            metadataCache.put(entry.getKey(), entry.getValue());
        }
        logSegmentCache.update(logSegmentsRemoved, logSegmentsAdded);
    }

    /**
     * Read the log segments from the store and register a listener.
     * @param comparator
     * @param segmentFilter
     * @param logSegmentNamesListener
     * @return future represents the result of log segments
     */
    public CompletableFuture<Versioned<List<LogSegmentMetadata>>> readLogSegmentsFromStore(
            final Comparator<LogSegmentMetadata> comparator,
            final LogSegmentFilter segmentFilter,
            final LogSegmentNamesListener logSegmentNamesListener) {
        final CompletableFuture<Versioned<List<LogSegmentMetadata>>> readResult =
                new CompletableFuture<Versioned<List<LogSegmentMetadata>>>();
        metadataStore.getLogSegmentNames(logMetadata.getLogSegmentsPath(), logSegmentNamesListener)
                .whenComplete(new FutureEventListener<Versioned<List<String>>>() {
                    @Override
                    public void onFailure(Throwable cause) {
                        readResult.completeExceptionally(cause);
                    }

                    @Override
                    public void onSuccess(Versioned<List<String>> logSegmentNames) {
                        readLogSegmentsFromStore(logSegmentNames, comparator, segmentFilter, readResult);
                    }
                });
        return readResult;
    }

    protected void readLogSegmentsFromStore(final Versioned<List<String>> logSegmentNames,
                                            final Comparator<LogSegmentMetadata> comparator,
                                            final LogSegmentFilter segmentFilter,
                                            final CompletableFuture<Versioned<List<LogSegmentMetadata>>> readResult) {
        Set<String> segmentsReceived = new HashSet<String>();
        segmentsReceived.addAll(segmentFilter.filter(logSegmentNames.getValue()));
        Set<String> segmentsAdded;
        final Set<String> removedSegments = Collections.synchronizedSet(new HashSet<String>());
        final Map<String, LogSegmentMetadata> addedSegments =
                Collections.synchronizedMap(new HashMap<String, LogSegmentMetadata>());
        Pair<Set<String>, Set<String>> segmentChanges = logSegmentCache.diff(segmentsReceived);
        segmentsAdded = segmentChanges.getLeft();
        removedSegments.addAll(segmentChanges.getRight());

        if (segmentsAdded.isEmpty()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("No segments added for {}.", getFullyQualifiedName());
            }

            // update the cache before #getCachedLogSegments to return
            updateLogSegmentCache(removedSegments, addedSegments);

            List<LogSegmentMetadata> segmentList;
            try {
                segmentList = getCachedLogSegments(comparator);
            } catch (UnexpectedException e) {
                readResult.completeExceptionally(e);
                return;
            }

            readResult.complete(new Versioned<List<LogSegmentMetadata>>(segmentList, logSegmentNames.getVersion()));
            return;
        }

        final AtomicInteger numChildren = new AtomicInteger(segmentsAdded.size());
        final AtomicInteger numFailures = new AtomicInteger(0);
        for (final String segment: segmentsAdded) {
            String logSegmentPath = logMetadata.getLogSegmentPath(segment);
            LogSegmentMetadata cachedSegment = metadataCache.get(logSegmentPath);
            if (null != cachedSegment) {
                addedSegments.put(segment, cachedSegment);
                completeReadLogSegmentsFromStore(
                        removedSegments,
                        addedSegments,
                        comparator,
                        readResult,
                        logSegmentNames.getVersion(),
                        numChildren,
                        numFailures);
                continue;
            }
            metadataStore.getLogSegment(logSegmentPath)
                    .whenComplete(new FutureEventListener<LogSegmentMetadata>() {

                        @Override
                        public void onSuccess(LogSegmentMetadata result) {
                            addedSegments.put(segment, result);
                            complete();
                        }

                        @Override
                        public void onFailure(Throwable cause) {
                            // LogSegmentNotFoundException exception is possible in two cases
                            // 1. A log segment was deleted by truncation between the call to getChildren and read
                            // attempt on the znode corresponding to the segment
                            // 2. In progress segment has been completed => inprogress ZNode does not exist
                            if (cause instanceof LogSegmentNotFoundException) {
                                removedSegments.add(segment);
                                complete();
                            } else {
                                // fail fast
                                if (1 == numFailures.incrementAndGet()) {
                                    readResult.completeExceptionally(cause);
                                    return;
                                }
                            }
                        }

                        private void complete() {
                            completeReadLogSegmentsFromStore(
                                    removedSegments,
                                    addedSegments,
                                    comparator,
                                    readResult,
                                    logSegmentNames.getVersion(),
                                    numChildren,
                                    numFailures);
                        }
                    });
        }
    }

    private void completeReadLogSegmentsFromStore(final Set<String> removedSegments,
                                                  final Map<String, LogSegmentMetadata> addedSegments,
                                                  final Comparator<LogSegmentMetadata> comparator,
                                                  final CompletableFuture<Versioned<List<LogSegmentMetadata>>>
                                                          readResult,
                                                  final Version logSegmentNamesVersion,
                                                  final AtomicInteger numChildren,
                                                  final AtomicInteger numFailures) {
        if (0 != numChildren.decrementAndGet()) {
            return;
        }
        if (numFailures.get() > 0) {
            return;
        }
        // update the cache only when fetch completed and before #getCachedLogSegments
        updateLogSegmentCache(removedSegments, addedSegments);
        List<LogSegmentMetadata> segmentList;
        try {
            segmentList = getCachedLogSegments(comparator);
        } catch (UnexpectedException e) {
            readResult.completeExceptionally(e);
            return;
        }
        readResult.complete(new Versioned<List<LogSegmentMetadata>>(segmentList, logSegmentNamesVersion));
    }

}
