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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.logsegment.LogSegmentRandomAccessEntryReader;
import org.apache.distributedlog.selector.FirstDLSNNotLessThanSelector;
import org.apache.distributedlog.selector.FirstTxIdNotLessThanSelector;
import org.apache.distributedlog.selector.LastRecordSelector;
import org.apache.distributedlog.selector.LogRecordSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility function for readers.
 */
public class ReadUtils {

    static final Logger LOG = LoggerFactory.getLogger(ReadUtils.class);

    private static final int MIN_SEARCH_BATCH_SIZE = 2;

    //
    // Read First & Last Record Functions
    //

    /**
     * Read last record from a log segment.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param l
     *          log segment metadata.
     * @param fence
     *          whether to fence the log segment.
     * @param includeControl
     *          whether to include control record.
     * @param includeEndOfStream
     *          whether to include end of stream.
     * @param scanStartBatchSize
     *          first num entries used for read last record scan
     * @param scanMaxBatchSize
     *          max num entries used for read last record scan
     * @param numRecordsScanned
     *          num of records scanned to get last record
     * @param executorService
     *          executor service used for processing entries
     * @param entryStore
     *          log segment entry store
     * @return a future with last record.
     */
    public static CompletableFuture<LogRecordWithDLSN> asyncReadLastRecord(
            final String streamName,
            final LogSegmentMetadata l,
            final boolean fence,
            final boolean includeControl,
            final boolean includeEndOfStream,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final AtomicInteger numRecordsScanned,
            final ExecutorService executorService,
            final LogSegmentEntryStore entryStore) {
        final LogRecordSelector selector = new LastRecordSelector();
        return asyncReadRecord(streamName, l, fence, includeControl, includeEndOfStream, scanStartBatchSize,
                               scanMaxBatchSize, numRecordsScanned, executorService, entryStore,
                               selector, true /* backward */, 0L);
    }

    /**
     * Read first record from a log segment with a DLSN larger than that given.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param l
     *          log segment metadata.
     * @param scanStartBatchSize
     *          first num entries used for read last record scan
     * @param scanMaxBatchSize
     *          max num entries used for read last record scan
     * @param numRecordsScanned
     *          num of records scanned to get last record
     * @param executorService
     *          executor service used for processing entries
     * @param entryStore
     *          log segment entry store
     * @param dlsn
     *          threshold dlsn
     * @return a future with last record.
     */
    public static CompletableFuture<LogRecordWithDLSN> asyncReadFirstUserRecord(
            final String streamName,
            final LogSegmentMetadata l,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final AtomicInteger numRecordsScanned,
            final ExecutorService executorService,
            final LogSegmentEntryStore entryStore,
            final DLSN dlsn) {
        long startEntryId = 0L;
        if (l.getLogSegmentSequenceNumber() == dlsn.getLogSegmentSequenceNo()) {
            startEntryId = dlsn.getEntryId();
        }
        final LogRecordSelector selector = new FirstDLSNNotLessThanSelector(dlsn);
        return asyncReadRecord(streamName, l, false, false, false, scanStartBatchSize,
                               scanMaxBatchSize, numRecordsScanned, executorService, entryStore,
                               selector, false /* backward */, startEntryId);
    }

    //
    // Private methods for scanning log segments
    //

    private static class ScanContext {
        // variables to about current scan state
        final AtomicInteger numEntriesToScan;
        final AtomicLong curStartEntryId;
        final AtomicLong curEndEntryId;

        // scan settings
        final long startEntryId;
        final long endEntryId;
        final int scanStartBatchSize;
        final int scanMaxBatchSize;
        final boolean includeControl;
        final boolean includeEndOfStream;
        final boolean backward;

        // number of records scanned
        final AtomicInteger numRecordsScanned;

        ScanContext(long startEntryId, long endEntryId,
                    int scanStartBatchSize,
                    int scanMaxBatchSize,
                    boolean includeControl,
                    boolean includeEndOfStream,
                    boolean backward,
                    AtomicInteger numRecordsScanned) {
            this.startEntryId = startEntryId;
            this.endEntryId = endEntryId;
            this.scanStartBatchSize = scanStartBatchSize;
            this.scanMaxBatchSize = scanMaxBatchSize;
            this.includeControl = includeControl;
            this.includeEndOfStream = includeEndOfStream;
            this.backward = backward;
            // Scan state
            this.numEntriesToScan = new AtomicInteger(scanStartBatchSize);
            if (backward) {
                this.curStartEntryId = new AtomicLong(
                        Math.max(startEntryId, (endEntryId - scanStartBatchSize + 1)));
                this.curEndEntryId = new AtomicLong(endEntryId);
            } else {
                this.curStartEntryId = new AtomicLong(startEntryId);
                this.curEndEntryId = new AtomicLong(
                        Math.min(endEntryId, (startEntryId + scanStartBatchSize - 1)));
            }
            this.numRecordsScanned = numRecordsScanned;
        }

        boolean moveToNextRange() {
            if (backward) {
                return moveBackward();
            } else {
                return moveForward();
            }
        }

        boolean moveBackward() {
            long nextEndEntryId = curStartEntryId.get() - 1;
            if (nextEndEntryId < startEntryId) {
                // no entries to read again
                return false;
            }
            curEndEntryId.set(nextEndEntryId);
            // update num entries to scan
            numEntriesToScan.set(
                    Math.min(numEntriesToScan.get() * 2, scanMaxBatchSize));
            // update start entry id
            curStartEntryId.set(Math.max(startEntryId, nextEndEntryId - numEntriesToScan.get() + 1));
            return true;
        }

        boolean moveForward() {
            long nextStartEntryId = curEndEntryId.get() + 1;
            if (nextStartEntryId > endEntryId) {
                // no entries to read again
                return false;
            }
            curStartEntryId.set(nextStartEntryId);
            // update num entries to scan
            numEntriesToScan.set(
                    Math.min(numEntriesToScan.get() * 2, scanMaxBatchSize));
            // update start entry id
            curEndEntryId.set(Math.min(endEntryId, nextStartEntryId + numEntriesToScan.get() - 1));
            return true;
        }
    }

    private static class SingleEntryScanContext extends ScanContext {
        SingleEntryScanContext(long entryId) {
            super(entryId, entryId, 1, 1, true, true, false, new AtomicInteger(0));
        }
    }

    /**
     * Read record from a given range of log segment entries.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param reader
     *          log segment random access reader
     * @param executorService
     *          executor service used for processing entries
     * @param context
     *          scan context
     * @return a future with the log record.
     */
    private static CompletableFuture<LogRecordWithDLSN> asyncReadRecordFromEntries(
            final String streamName,
            final LogSegmentRandomAccessEntryReader reader,
            final LogSegmentMetadata metadata,
            final ExecutorService executorService,
            final ScanContext context,
            final LogRecordSelector selector) {
        final CompletableFuture<LogRecordWithDLSN> promise = new CompletableFuture<LogRecordWithDLSN>();
        final long startEntryId = context.curStartEntryId.get();
        final long endEntryId = context.curEndEntryId.get();
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} reading entries [{} - {}] from {}.",
                streamName, startEntryId, endEntryId, metadata);
        }
        FutureEventListener<List<Entry.Reader>> readEntriesListener =
            new FutureEventListener<List<Entry.Reader>>() {
                @Override
                public void onSuccess(final List<Entry.Reader> entries) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} finished reading entries [{} - {}] from {}",
                            streamName, startEntryId, endEntryId, metadata);
                    }
                    for (Entry.Reader entry : entries) {
                        try {
                            visitEntryRecords(entry, context, selector);
                        } catch (IOException ioe) {
                            // exception is only thrown due to bad ledger entry, so it might be corrupted
                            // we shouldn't do anything beyond this point. throw the exception to application
                            promise.completeExceptionally(ioe);
                            return;
                        }
                    }

                    LogRecordWithDLSN record = selector.result();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} got record from entries [{} - {}] of {} : {}",
                            streamName, startEntryId, endEntryId, metadata, record);
                    }
                    promise.complete(record);
                }

                @Override
                public void onFailure(final Throwable cause) {
                    promise.completeExceptionally(cause);
                }
            };
        reader.readEntries(startEntryId, endEntryId)
                .whenCompleteAsync(readEntriesListener, executorService);
        return promise;
    }

    /**
     * Process each record using LogRecordSelector.
     *
     * @param entry
     *          ledger entry
     * @param context
     *          scan context
     * @return log record with dlsn inside the ledger entry
     * @throws IOException
     */
    private static void visitEntryRecords(
            Entry.Reader entry,
            ScanContext context,
            LogRecordSelector selector) throws IOException {
        LogRecordWithDLSN nextRecord = entry.nextRecord();
        while (nextRecord != null) {
            LogRecordWithDLSN record = nextRecord;
            nextRecord = entry.nextRecord();
            context.numRecordsScanned.incrementAndGet();
            if (!context.includeControl && record.isControl()) {
                continue;
            }
            if (!context.includeEndOfStream && record.isEndOfStream()) {
                continue;
            }
            selector.process(record);
        }
    }

    /**
     * Scan entries for the given record.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param reader
     *          log segment random access reader
     * @param executorService
     *          executor service used for processing entries
     * @param promise
     *          promise to return desired record.
     * @param context
     *          scan context
     */
    private static void asyncReadRecordFromEntries(
            final String streamName,
            final LogSegmentRandomAccessEntryReader reader,
            final LogSegmentMetadata metadata,
            final ExecutorService executorService,
            final CompletableFuture<LogRecordWithDLSN> promise,
            final ScanContext context,
            final LogRecordSelector selector) {
        FutureEventListener<LogRecordWithDLSN> readEntriesListener =
            new FutureEventListener<LogRecordWithDLSN>() {
                @Override
                public void onSuccess(LogRecordWithDLSN value) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} read record from [{} - {}] of {} : {}",
                            streamName, context.curStartEntryId.get(), context.curEndEntryId.get(),
                            metadata, value);
                    }
                    if (null != value) {
                        promise.complete(value);
                        return;
                    }
                    if (!context.moveToNextRange()) {
                        // no entries to read again
                        promise.complete(null);
                        return;
                    }
                    // scan next range
                    asyncReadRecordFromEntries(streamName,
                            reader,
                            metadata,
                            executorService,
                            promise,
                            context,
                            selector);
                }

                @Override
                public void onFailure(Throwable cause) {
                    promise.completeExceptionally(cause);
                }
            };
        asyncReadRecordFromEntries(streamName, reader, metadata, executorService, context, selector)
                .whenCompleteAsync(readEntriesListener, executorService);
    }

    private static void asyncReadRecordFromLogSegment(
            final String streamName,
            final LogSegmentRandomAccessEntryReader reader,
            final LogSegmentMetadata metadata,
            final ExecutorService executorService,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final boolean includeControl,
            final boolean includeEndOfStream,
            final CompletableFuture<LogRecordWithDLSN> promise,
            final AtomicInteger numRecordsScanned,
            final LogRecordSelector selector,
            final boolean backward,
            final long startEntryId) {
        final long lastAddConfirmed = reader.getLastAddConfirmed();
        if (lastAddConfirmed < 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Log segment {} is empty for {}.", new Object[] { metadata, streamName });
            }
            promise.complete(null);
            return;
        }
        final ScanContext context = new ScanContext(
                startEntryId, lastAddConfirmed,
                scanStartBatchSize, scanMaxBatchSize,
                includeControl, includeEndOfStream, backward, numRecordsScanned);
        asyncReadRecordFromEntries(streamName, reader, metadata, executorService,
                                   promise, context, selector);
    }

    private static CompletableFuture<LogRecordWithDLSN> asyncReadRecord(
            final String streamName,
            final LogSegmentMetadata l,
            final boolean fence,
            final boolean includeControl,
            final boolean includeEndOfStream,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final AtomicInteger numRecordsScanned,
            final ExecutorService executorService,
            final LogSegmentEntryStore entryStore,
            final LogRecordSelector selector,
            final boolean backward,
            final long startEntryId) {

        final CompletableFuture<LogRecordWithDLSN> promise = new CompletableFuture<LogRecordWithDLSN>();

        FutureEventListener<LogSegmentRandomAccessEntryReader> openReaderListener =
            new FutureEventListener<LogSegmentRandomAccessEntryReader>() {
                @Override
                public void onSuccess(final LogSegmentRandomAccessEntryReader reader) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} Opened log segment {} for reading record", streamName, l);
                    }
                    promise.whenComplete((value, cause) -> reader.asyncClose());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} {} scanning {}.", (backward ? "backward" : "forward"), streamName, l);
                    }
                    asyncReadRecordFromLogSegment(
                            streamName, reader, l, executorService,
                            scanStartBatchSize, scanMaxBatchSize,
                            includeControl, includeEndOfStream,
                            promise, numRecordsScanned, selector, backward, startEntryId);
                }

                @Override
                public void onFailure(final Throwable cause) {
                    promise.completeExceptionally(cause);
                }
            };
        entryStore.openRandomAccessReader(l, fence)
                .whenCompleteAsync(openReaderListener, executorService);
        return promise;
    }

    //
    // Search Functions
    //

    /**
     * Get the log record whose transaction id is not less than provided <code>transactionId</code>.
     *
     * <p>
     * It uses a binary-search like algorithm to find the log record whose transaction id is not less than
     * provided <code>transactionId</code> within a log <code>segment</code>. You could think of a log segment
     * in terms of a sequence of records whose transaction ids are non-decreasing.
     *
     * - The sequence of records within a log segment is divided into N pieces.
     * - Find the piece of records that contains a record whose transaction id is not less than provided
     *   <code>transactionId</code>.
     *
     * N could be chosen based on trading off concurrency and latency.
     * </p>
     *
     * @param logName
     *          name of the log
     * @param segment
     *          metadata of the log segment
     * @param transactionId
     *          transaction id
     * @param executorService
     *          executor service used for processing entries
     * @param entryStore
     *          log segment entry store
     * @param nWays
     *          how many number of entries to search in parallel
     * @return found log record. none if all transaction ids are less than provided <code>transactionId</code>.
     */
    public static CompletableFuture<Optional<LogRecordWithDLSN>> getLogRecordNotLessThanTxId(
            final String logName,
            final LogSegmentMetadata segment,
            final long transactionId,
            final ExecutorService executorService,
            final LogSegmentEntryStore entryStore,
            final int nWays) {
        if (!segment.isInProgress()) {
            if (segment.getLastTxId() < transactionId) {
                // all log records whose transaction id is less than provided transactionId
                // then return none
                Optional<LogRecordWithDLSN> noneRecord = Optional.empty();
                return FutureUtils.value(noneRecord);
            }
        }

        final CompletableFuture<Optional<LogRecordWithDLSN>> promise =
                new CompletableFuture<Optional<LogRecordWithDLSN>>();
        final FutureEventListener<LogSegmentRandomAccessEntryReader> openReaderListener =
            new FutureEventListener<LogSegmentRandomAccessEntryReader>() {
                @Override
                public void onSuccess(final LogSegmentRandomAccessEntryReader reader) {
                    promise.whenComplete((value, cause) -> reader.asyncClose());
                    long lastEntryId = reader.getLastAddConfirmed();
                    if (lastEntryId < 0) {
                        // it means that the log segment is created but not written yet or an empty log segment.
                        //it is equivalent to 'all log records whose transaction id is less than provided transactionId'
                        Optional<LogRecordWithDLSN> nonRecord = Optional.empty();
                        promise.complete(nonRecord);
                        return;
                    }
                    // all log records whose transaction id is not less than provided transactionId
                    if (segment.getFirstTxId() >= transactionId) {
                        final FirstTxIdNotLessThanSelector selector =
                                new FirstTxIdNotLessThanSelector(transactionId);
                        asyncReadRecordFromEntries(
                                logName,
                                reader,
                                segment,
                                executorService,
                                new SingleEntryScanContext(0L),
                                selector
                        ).whenComplete(new FutureEventListener<LogRecordWithDLSN>() {
                            @Override
                            public void onSuccess(LogRecordWithDLSN value) {
                                promise.complete(Optional.of(selector.result()));
                            }

                            @Override
                            public void onFailure(Throwable cause) {
                                promise.completeExceptionally(cause);
                            }
                        });

                        return;
                    }
                    getLogRecordNotLessThanTxIdFromEntries(
                            logName,
                            segment,
                            transactionId,
                            executorService,
                            reader,
                            Lists.newArrayList(0L, lastEntryId),
                            nWays,
                            Optional.<LogRecordWithDLSN>empty(),
                            promise);
                }

                @Override
                public void onFailure(final Throwable cause) {
                    promise.completeExceptionally(cause);
                }
            };

        entryStore.openRandomAccessReader(segment, false)
                .whenCompleteAsync(openReaderListener, executorService);
        return promise;
    }

    /**
     * Find the log record whose transaction id is not less than provided <code>transactionId</code> from
     * entries between <code>startEntryId</code> and <code>endEntryId</code>.
     *
     * @param logName
     *          name of the log
     * @param segment
     *          log segment
     * @param transactionId
     *          provided transaction id to search
     * @param executorService
     *          executor service
     * @param reader
     *          log segment random access reader
     * @param entriesToSearch
     *          list of entries to search
     * @param nWays
     *          how many entries to search in parallel
     * @param prevFoundRecord
     *          the log record found in previous search
     * @param promise
     *          promise to satisfy the result
     */
    private static void getLogRecordNotLessThanTxIdFromEntries(
            final String logName,
            final LogSegmentMetadata segment,
            final long transactionId,
            final ExecutorService executorService,
            final LogSegmentRandomAccessEntryReader reader,
            final List<Long> entriesToSearch,
            final int nWays,
            final Optional<LogRecordWithDLSN> prevFoundRecord,
            final CompletableFuture<Optional<LogRecordWithDLSN>> promise) {
        final List<CompletableFuture<LogRecordWithDLSN>> searchResults =
                Lists.newArrayListWithExpectedSize(entriesToSearch.size());
        for (Long entryId : entriesToSearch) {
            LogRecordSelector selector = new FirstTxIdNotLessThanSelector(transactionId);
            CompletableFuture<LogRecordWithDLSN> searchResult = asyncReadRecordFromEntries(
                    logName,
                    reader,
                    segment,
                    executorService,
                    new SingleEntryScanContext(entryId),
                    selector);
            searchResults.add(searchResult);
        }
        FutureEventListener<List<LogRecordWithDLSN>> processSearchResultsListener =
                new FutureEventListener<List<LogRecordWithDLSN>>() {
                    @Override
                    public void onSuccess(List<LogRecordWithDLSN> resultList) {
                        processSearchResults(
                                logName,
                                segment,
                                transactionId,
                                executorService,
                                reader,
                                resultList,
                                nWays,
                                prevFoundRecord,
                                promise);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.completeExceptionally(cause);
                    }
                };
        FutureUtils.collect(searchResults).whenCompleteAsync(
                processSearchResultsListener, executorService);
    }

    /**
     * Process the search results.
     */
    static void processSearchResults(
            final String logName,
            final LogSegmentMetadata segment,
            final long transactionId,
            final ExecutorService executorService,
            final LogSegmentRandomAccessEntryReader reader,
            final List<LogRecordWithDLSN> searchResults,
            final int nWays,
            final Optional<LogRecordWithDLSN> prevFoundRecord,
            final CompletableFuture<Optional<LogRecordWithDLSN>> promise) {
        int found = -1;
        for (int i = 0; i < searchResults.size(); i++) {
            LogRecordWithDLSN record = searchResults.get(i);
            if (record.getTransactionId() >= transactionId) {
                found = i;
                break;
            }
        }
        if (found == -1) { // all log records' transaction id is less than provided transaction id
            promise.complete(prevFoundRecord);
            return;
        }
        // we found a log record
        LogRecordWithDLSN foundRecord = searchResults.get(found);

        // we found it
        //   - it is not the first record
        //   - it is the first record in first search entry
        //   - its entry is adjacent to previous search entry
        if (foundRecord.getDlsn().getSlotId() != 0L
                || found == 0
                || foundRecord.getDlsn().getEntryId() == (searchResults.get(found - 1).getDlsn().getEntryId() + 1)) {
            promise.complete(Optional.of(foundRecord));
            return;
        }

        // otherwise, we need to search
        List<Long> nextSearchBatch = getEntriesToSearch(
                transactionId,
                searchResults.get(found - 1),
                searchResults.get(found),
                nWays);
        if (nextSearchBatch.isEmpty()) {
            promise.complete(prevFoundRecord);
            return;
        }
        getLogRecordNotLessThanTxIdFromEntries(
                logName,
                segment,
                transactionId,
                executorService,
                reader,
                nextSearchBatch,
                nWays,
                Optional.of(foundRecord),
                promise);
    }

    /**
     * Get the entries to search provided <code>transactionId</code> between
     * <code>firstRecord</code> and <code>lastRecord</code>. <code>firstRecord</code>
     * and <code>lastRecord</code> are already searched, which the transaction id
     * of <code>firstRecord</code> is less than <code>transactionId</code> and the
     * transaction id of <code>lastRecord</code> is not less than <code>transactionId</code>.
     *
     * @param transactionId
     *          transaction id to search
     * @param firstRecord
     *          log record that already searched whose transaction id is leass than <code>transactionId</code>.
     * @param lastRecord
     *          log record that already searched whose transaction id is not less than <code>transactionId</code>.
     * @param nWays
     *          N-ways to search
     * @return the list of entries to search
     */
    static List<Long> getEntriesToSearch(
            long transactionId,
            LogRecordWithDLSN firstRecord,
            LogRecordWithDLSN lastRecord,
            int nWays) {
        long txnDiff = lastRecord.getTransactionId() - firstRecord.getTransactionId();
        if (txnDiff > 0) {
            if (lastRecord.getTransactionId() == transactionId) {
                List<Long> entries = getEntriesToSearch(
                        firstRecord.getDlsn().getEntryId() + 1,
                        lastRecord.getDlsn().getEntryId() - 2,
                        Math.max(MIN_SEARCH_BATCH_SIZE, nWays - 1));
                entries.add(lastRecord.getDlsn().getEntryId() - 1);
                return entries;
            } else {
                // TODO: improve it by estimating transaction ids.
                return getEntriesToSearch(
                        firstRecord.getDlsn().getEntryId() + 1,
                        lastRecord.getDlsn().getEntryId() - 1,
                        nWays);
            }
        } else {
            // unexpected condition
            return Lists.newArrayList();
        }
    }

    static List<Long> getEntriesToSearch(
            long startEntryId,
            long endEntryId,
            int nWays) {
        if (startEntryId > endEntryId) {
            return Lists.newArrayList();
        }
        long numEntries = endEntryId - startEntryId + 1;
        long step = Math.max(1L, numEntries / nWays);
        List<Long> entryList = Lists.newArrayListWithExpectedSize(nWays);
        for (long i = startEntryId, j = nWays - 1; i <= endEntryId && j > 0; i += step, j--) {
            entryList.add(i);
        }
        if (entryList.get(entryList.size() - 1) < endEntryId) {
            entryList.add(endEntryId);
        }
        return entryList;
    }
}
