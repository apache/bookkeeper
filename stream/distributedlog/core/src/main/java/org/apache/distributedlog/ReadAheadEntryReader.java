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
import com.google.common.base.Ticker;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.callback.LogSegmentListener;
import org.apache.distributedlog.exceptions.AlreadyTruncatedTransactionException;
import org.apache.distributedlog.exceptions.DLIllegalStateException;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.EndOfLogSegmentException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.io.AsyncCloseable;
import org.apache.distributedlog.logsegment.LogSegmentEntryReader;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.logsegment.LogSegmentFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * New ReadAhead Reader that uses {@link org.apache.distributedlog.logsegment.LogSegmentEntryReader}.
 *
 * <p>NOTE: all the state changes happen in the same thread. All *unsafe* methods should be submitted to the order
 * scheduler using stream name as the key.</p>
 */
class ReadAheadEntryReader implements
        AsyncCloseable,
        LogSegmentListener,
        LogSegmentEntryReader.StateChangeListener,
        FutureEventListener<List<Entry.Reader>> {

    private static final Logger logger = LoggerFactory.getLogger(ReadAheadEntryReader.class);

    private enum State {
        IDLE,
        READING,
        PAUSED,
        CLOSED,
        ERROR
    }
    private State state = State.IDLE;

    //
    // Static Functions
    //

    private static Function<LogSegmentEntryReader, Void> START_READER_FUNC = reader -> {
        reader.start();
        return null;
    };

    //
    // Internal Classes
    //

    class SegmentReader implements FutureEventListener<LogSegmentEntryReader> {

        private LogSegmentMetadata metadata;
        private final long startEntryId;
        private CompletableFuture<LogSegmentEntryReader> openFuture = null;
        private LogSegmentEntryReader reader = null;
        private boolean isStarted = false;
        private boolean isClosed = false;

        SegmentReader(LogSegmentMetadata metadata,
                      long startEntryId) {
            this.metadata = metadata;
            this.startEntryId = startEntryId;
        }

        synchronized LogSegmentEntryReader getEntryReader() {
            return reader;
        }

        synchronized boolean isBeyondLastAddConfirmed() {
            return null != reader && reader.isBeyondLastAddConfirmed();
        }

        synchronized LogSegmentMetadata getSegment() {
            return metadata;
        }

        synchronized boolean isReaderOpen() {
            return null != openFuture;
        }

        synchronized void openReader() {
            if (null != openFuture) {
                return;
            }
            openFuture = entryStore.openReader(metadata, startEntryId).whenComplete(this);
        }

        synchronized boolean isReaderStarted() {
            return isStarted;
        }

        synchronized void startRead() {
            if (isStarted) {
                return;
            }
            isStarted = true;
            if (null != reader) {
                reader.start();
            } else {
                openFuture.thenApply(START_READER_FUNC);
            }
        }

        synchronized CompletableFuture<List<Entry.Reader>> readNext() {
            if (null != reader) {
                checkCatchingUpStatus(reader);
                return reader.readNext(numReadAheadEntries);
            } else {
                return openFuture.thenCompose(readFunc);
            }
        }

        synchronized void updateLogSegmentMetadata(final LogSegmentMetadata segment) {
            if (null != reader) {
                reader.onLogSegmentMetadataUpdated(segment);
                this.metadata = segment;
            } else {
                openFuture.thenAccept(reader1 -> {
                    reader1.onLogSegmentMetadataUpdated(segment);
                    synchronized (SegmentReader.this) {
                        SegmentReader.this.metadata = segment;
                    }
                });
            }
        }

        @Override
        public synchronized  void onSuccess(LogSegmentEntryReader reader) {
            this.reader = reader;
            if (reader.getSegment().isInProgress()) {
                reader.registerListener(ReadAheadEntryReader.this);
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            // no-op, the failure will be propagated on first read.
        }

        synchronized boolean isClosed() {
            return isClosed;
        }

        synchronized CompletableFuture<Void> close() {
            if (null == openFuture) {
                return FutureUtils.Void();
            }
            return FutureUtils.ensure(
                openFuture.thenCompose(reader1 -> reader1.asyncClose()),
                () -> {
                    synchronized (SegmentReader.this) {
                        isClosed = true;
                    }
                });
        }
    }

    private class ReadEntriesFunc implements Function<LogSegmentEntryReader, CompletableFuture<List<Entry.Reader>>> {

        private final int numEntries;

        ReadEntriesFunc(int numEntries) {
            this.numEntries = numEntries;
        }

        @Override
        public CompletableFuture<List<Entry.Reader>> apply(LogSegmentEntryReader reader) {
            checkCatchingUpStatus(reader);
            return reader.readNext(numEntries);
        }
    }

    private abstract class CloseableRunnable implements SafeRunnable {

        @Override
        public void run() {
            synchronized (ReadAheadEntryReader.this) {
                if (null != closePromise) {
                    return;
                }
            }
            try {
                safeRun();
            } catch (Throwable cause) {
                logger.error("Caught unexpected exception : ", cause);
            }
        }

    }

    //
    // Functions
    //
    private final Function<LogSegmentEntryReader, CompletableFuture<List<Entry.Reader>>> readFunc;
    private final Runnable removeClosedSegmentReadersFunc = () -> removeClosedSegmentReaders();

    //
    // Resources
    //
    private final DistributedLogConfiguration conf;
    private final BKLogReadHandler readHandler;
    private final LogSegmentEntryStore entryStore;
    private final OrderedScheduler scheduler;

    //
    // Parameters
    //
    private final String streamName;
    private final DLSN fromDLSN;
    private final int maxCachedEntries;
    private final int numReadAheadEntries;
    private final int idleWarnThresholdMillis;

    //
    // Cache
    //
    private final LinkedBlockingQueue<Entry.Reader> entryQueue;

    //
    // State of the reader
    //

    private final AtomicBoolean started = new AtomicBoolean(false);
    private boolean isInitialized = false;
    private boolean readAheadPaused = false;
    private CompletableFuture<Void> closePromise = null;
    // segment readers
    private long currentSegmentSequenceNumber;
    private SegmentReader currentSegmentReader;
    private SegmentReader nextSegmentReader;
    private DLSN lastDLSN;
    private final EntryPosition nextEntryPosition;
    private volatile boolean isCatchingUp = true;
    private final LinkedList<SegmentReader> segmentReaders;
    private final LinkedList<SegmentReader> segmentReadersToClose;
    // last exception that this reader encounters
    private final AtomicReference<IOException> lastException = new AtomicReference<IOException>(null);
    // last entry added time
    private final Stopwatch lastEntryAddedTime;
    // state change notification
    private final CopyOnWriteArraySet<AsyncNotification> stateChangeNotifications =
            new CopyOnWriteArraySet<AsyncNotification>();
    // idle reader check task
    private final ScheduledFuture<?> idleReaderCheckTask;

    //
    // Stats
    //
    private final AlertStatsLogger alertStatsLogger;

    public ReadAheadEntryReader(String streamName,
                                DLSN fromDLSN,
                                DistributedLogConfiguration conf,
                                BKLogReadHandler readHandler,
                                LogSegmentEntryStore entryStore,
                                OrderedScheduler scheduler,
                                Ticker ticker,
                                AlertStatsLogger alertStatsLogger) {
        this.streamName = streamName;
        this.fromDLSN = lastDLSN = fromDLSN;
        this.nextEntryPosition = new EntryPosition(
                fromDLSN.getLogSegmentSequenceNo(),
                fromDLSN.getEntryId());
        this.conf = conf;
        this.maxCachedEntries = conf.getReadAheadMaxRecords();
        this.numReadAheadEntries = conf.getReadAheadBatchSize();
        this.idleWarnThresholdMillis = conf.getReaderIdleWarnThresholdMillis();
        this.readHandler = readHandler;
        this.entryStore = entryStore;
        this.scheduler = scheduler;
        this.readFunc = new ReadEntriesFunc(numReadAheadEntries);
        this.alertStatsLogger = alertStatsLogger;

        // create the segment reader list
        this.segmentReaders = new LinkedList<SegmentReader>();
        this.segmentReadersToClose = new LinkedList<SegmentReader>();
        // create the readahead entry queue
        this.entryQueue = new LinkedBlockingQueue<Entry.Reader>();

        // start the idle reader detection
        lastEntryAddedTime = Stopwatch.createStarted(ticker);
        // start the idle reader check task
        idleReaderCheckTask = scheduleIdleReaderTaskIfNecessary();
    }

    private ScheduledFuture<?> scheduleIdleReaderTaskIfNecessary() {
        if (idleWarnThresholdMillis < Integer.MAX_VALUE && idleWarnThresholdMillis > 0) {
            return scheduler.scheduleAtFixedRateOrdered(streamName, () -> {
                if (!isReaderIdle(idleWarnThresholdMillis, TimeUnit.MILLISECONDS)) {
                    return;
                }
                // the readahead has been idle
                unsafeCheckIfReadAheadIsIdle();
            }, idleWarnThresholdMillis, idleWarnThresholdMillis, TimeUnit.MILLISECONDS);
        }
        return null;
    }

    private void unsafeCheckIfReadAheadIsIdle() {
        boolean forceReadLogSegments =
                (null == currentSegmentReader) || currentSegmentReader.isBeyondLastAddConfirmed();
        if (forceReadLogSegments) {
            readHandler.readLogSegmentsFromStore(
                    LogSegmentMetadata.COMPARATOR,
                    LogSegmentFilter.DEFAULT_FILTER,
                    null
            ).whenComplete(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {
                @Override
                public void onFailure(Throwable cause) {
                    // do nothing here since it would be retried on next idle reader check task
                }

                @Override
                public void onSuccess(Versioned<List<LogSegmentMetadata>> segments) {
                    onSegmentsUpdated(segments.getValue());
                }
            });
        }
    }

    private void cancelIdleReaderTask() {
        if (null != idleReaderCheckTask) {
            idleReaderCheckTask.cancel(true);
        }
    }

    @VisibleForTesting
    EntryPosition getNextEntryPosition() {
        return nextEntryPosition;
    }

    @VisibleForTesting
    SegmentReader getCurrentSegmentReader() {
        return currentSegmentReader;
    }

    @VisibleForTesting
    long getCurrentSegmentSequenceNumber() {
        return currentSegmentSequenceNumber;
    }

    @VisibleForTesting
    SegmentReader getNextSegmentReader() {
        return nextSegmentReader;
    }

    @VisibleForTesting
    LinkedList<SegmentReader> getSegmentReaders() {
        return segmentReaders;
    }

    @VisibleForTesting
    boolean isInitialized() {
        return isInitialized;
    }

    private void orderedSubmit(SafeRunnable runnable) {
        synchronized (this) {
            if (null != closePromise) {
                return;
            }
        }
        try {
            scheduler.executeOrdered(streamName, runnable);
        } catch (RejectedExecutionException ree) {
            logger.debug("Failed to submit and execute an operation for readhead entry reader of {}",
                    streamName, ree);
        }
    }

    public void start(final List<LogSegmentMetadata> segmentList) {
        logger.info("Starting the readahead entry reader for {} : segments = {}",
                readHandler.getFullyQualifiedName(), segmentList);
        started.set(true);
        processLogSegments(segmentList);
    }

    private void removeClosedSegmentReaders() {
        orderedSubmit(new CloseableRunnable() {
            @Override
            public void safeRun() {
                unsafeRemoveClosedSegmentReaders();
            }
        });
    }

    private void unsafeRemoveClosedSegmentReaders() {
        SegmentReader reader = segmentReadersToClose.peekFirst();
        while (null != reader) {
            if (reader.isClosed()) {
                segmentReadersToClose.pollFirst();
                reader = segmentReadersToClose.peekFirst();
            } else {
                break;
            }
        }
    }

    synchronized boolean isClosed() {
        return null != closePromise;
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        final CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closePromise = closeFuture = new CompletableFuture<Void>();
        }

        // cancel the idle reader task
        cancelIdleReaderTask();

        // use runnable here instead of CloseableRunnable,
        // because we need this to be executed
        try {
            scheduler.executeOrdered(streamName, () -> unsafeAsyncClose(closeFuture));
        } catch (RejectedExecutionException ree) {
            logger.warn("Scheduler has been shutdown before closing the readahead entry reader for stream {}",
                    streamName, ree);
            unsafeAsyncClose(closeFuture);
        }

        return closeFuture;
    }

    private void unsafeAsyncClose(CompletableFuture<Void> closePromise) {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}][state:{}] Closing read ahead", streamName, state);
        }
        state = State.CLOSED;

        List<CompletableFuture<Void>> closeFutures = Lists.newArrayListWithExpectedSize(
                segmentReaders.size() + segmentReadersToClose.size() + 1);
        if (null != currentSegmentReader) {
            segmentReadersToClose.add(currentSegmentReader);
        }
        if (null != nextSegmentReader) {
            segmentReadersToClose.add(nextSegmentReader);
        }
        segmentReadersToClose.addAll(segmentReaders);
        segmentReaders.clear();
        for (SegmentReader reader : segmentReadersToClose) {
            closeFutures.add(reader.close());
        }
        FutureUtils.proxyTo(
            FutureUtils.collect(closeFutures).thenApply((value) -> null),
            closePromise);
    }

    //
    // Reader State Changes
    //

    ReadAheadEntryReader addStateChangeNotification(AsyncNotification notification) {
        this.stateChangeNotifications.add(notification);
        return this;
    }

    ReadAheadEntryReader removeStateChangeNotification(AsyncNotification notification) {
        this.stateChangeNotifications.remove(notification);
        return this;
    }

    private void notifyStateChangeOnSuccess() {
        for (AsyncNotification notification : stateChangeNotifications) {
            notification.notifyOnOperationComplete();
        }
    }

    private void notifyStateChangeOnFailure(Throwable cause) {
        for (AsyncNotification notification : stateChangeNotifications) {
            notification.notifyOnError(cause);
        }
    }

    void setLastException(IOException cause) {
        if (!lastException.compareAndSet(null, cause)) {
            logger.debug("last exception has already been set to ", lastException.get());
        }
        // the exception is set and notify the state change
        notifyStateChangeOnFailure(cause);

        orderedSubmit(new CloseableRunnable() {
                @Override
                public void safeRun() {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][state:{}] Read ahead errored", streamName, state);
                    }
                    state = State.ERROR;
                }
            });
    }

    void checkLastException() throws IOException {
        if (null != lastException.get()) {
            throw lastException.get();
        }
    }

    void checkCatchingUpStatus(LogSegmentEntryReader reader) {
        if (reader.getSegment().isInProgress()
                && isCatchingUp
                && reader.hasCaughtUpOnInprogress()) {
            logger.info("ReadAhead for {} is caught up at entry {} @ log segment {}.",
                readHandler.getFullyQualifiedName(),
                reader.getLastAddConfirmed(), reader.getSegment());
            isCatchingUp = false;
        }
    }

    void markCaughtup() {
        if (isCatchingUp) {
            isCatchingUp = false;
            logger.info("ReadAhead for {} is caught up", readHandler.getFullyQualifiedName());
        }
    }

    public boolean isReadAheadCaughtUp() {
        return !isCatchingUp;
    }

    @Override
    public void onCaughtupOnInprogress() {
        markCaughtup();
    }

    //
    // ReadAhead State Machine
    //

    @Override
    public void onSuccess(List<Entry.Reader> entries) {
        if (isClosed()) {
            // if readahead is closing, don't put the entries to the queue
            for (Entry.Reader entry : entries) {
                entry.release();
            }
            return;
        }

        lastEntryAddedTime.reset().start();
        entryQueue.addAll(entries);
        if (!entries.isEmpty()) {
            Entry.Reader lastEntry = entries.get(entries.size() - 1);
            nextEntryPosition.advance(lastEntry.getLSSN(), lastEntry.getEntryId() + 1);
        }
        // notify on data available
        notifyStateChangeOnSuccess();

        completeRead();
        scheduleRead();
    }

    @Override
    public void onFailure(Throwable cause) {
        completeRead();

        if (cause instanceof EndOfLogSegmentException) {
            // we reach end of the log segment
            moveToNextLogSegment();
            return;
        }
        if (cause instanceof IOException) {
            setLastException((IOException) cause);
        } else {
            setLastException(new UnexpectedException("Unexpected non I/O exception", cause));
        }
    }

    //
    // Cache Related Methods
    //

    public Entry.Reader getNextReadAheadEntry(long waitTime, TimeUnit waitTimeUnit) throws IOException {
        if (null != lastException.get()) {
            throw lastException.get();
        }
        Entry.Reader entry;
        try {
            entry = entryQueue.poll(waitTime, waitTimeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on waiting next readahead entry : ", e);
        }
        try {
            return entry;
        } finally {
            // resume readahead if the cache becomes empty
            if (null != entry && !isCacheFull()) {
                scheduleRead();
            }
        }
    }

    /**
     * Return number cached entries.
     *
     * @return number cached entries.
     */
    public int getNumCachedEntries() {
        return entryQueue.size();
    }

    /**
     * Return if the cache is full.
     *
     * @return true if the cache is full, otherwise false.
     */
    public boolean isCacheFull() {
        return getNumCachedEntries() >= maxCachedEntries;
    }

    @VisibleForTesting
    public boolean isCacheEmpty() {
        return entryQueue.isEmpty();
    }

    /**
     * Check whether the readahead becomes stall.
     *
     * @param idleReaderErrorThreshold idle reader error threshold
     * @param timeUnit time unit of the idle reader error threshold
     * @return true if the readahead becomes stall, otherwise false.
     */
    public boolean isReaderIdle(int idleReaderErrorThreshold, TimeUnit timeUnit) {
        return (lastEntryAddedTime.elapsed(timeUnit) > idleReaderErrorThreshold);
    }

    //
    // LogSegment Management
    //

    void processLogSegments(final List<LogSegmentMetadata> segments) {
        orderedSubmit(new CloseableRunnable() {
            @Override
            public void safeRun() {
                unsafeProcessLogSegments(segments);
            }
        });
    }

    private void unsafeProcessLogSegments(List<LogSegmentMetadata> segments) {
        if (isInitialized) {
            unsafeReinitializeLogSegments(segments);
        } else {
            unsafeInitializeLogSegments(segments);
        }
    }

    /**
     * Update the log segment metadata.
     *
     * @param reader the reader to update the metadata
     * @param newMetadata the new metadata received
     * @return true if successfully, false on encountering errors
     */
    private boolean updateLogSegmentMetadata(SegmentReader reader,
                                             LogSegmentMetadata newMetadata) {
        if (reader.getSegment().getLogSegmentSequenceNumber() != newMetadata.getLogSegmentSequenceNumber()) {
            logger.error("Inconsistent state found in entry reader for {} : "
                + "current segment = {}, new segment = {}",
                streamName, reader.getSegment(), newMetadata);
            setLastException(new DLIllegalStateException("Inconsistent state found in entry reader for "
                    + streamName + " : current segment = " + reader.getSegment() + ", new segment = " + newMetadata));
            return false;
        }
        if (!reader.getSegment().isInProgress() && newMetadata.isInProgress()) {
            setLastException(new DLIllegalStateException("An inprogress log segment " + newMetadata
                    + " received after a closed log segment " + reader.getSegment() + " on reading segment "
                    + newMetadata.getLogSegmentSequenceNumber() + " @ stream " + streamName));
            return false;
        }
        if (reader.getSegment().isInProgress() && !newMetadata.isInProgress()) {
            reader.updateLogSegmentMetadata(newMetadata);
        }
        return true;
    }

    /**
     * Reinitialize the log segments.
     */
    private void unsafeReinitializeLogSegments(List<LogSegmentMetadata> segments) {
        logger.info("Reinitialize log segments with {}", segments);
        int segmentIdx = 0;
        for (; segmentIdx < segments.size(); segmentIdx++) {
            LogSegmentMetadata segment = segments.get(segmentIdx);
            if (segment.getLogSegmentSequenceNumber() < currentSegmentSequenceNumber) {
                continue;
            }
            break;
        }
        if (segmentIdx >= segments.size()) {
            return;
        }
        LogSegmentMetadata segment = segments.get(segmentIdx);
        if (null != currentSegmentReader) {
            if (!updateLogSegmentMetadata(currentSegmentReader, segment)) {
                return;
            }
        } else {
            if (currentSegmentSequenceNumber != segment.getLogSegmentSequenceNumber()) {
                logger.error("Inconsistent state found in entry reader for {} : "
                    + "current segment sn = {}, new segment sn = {}",
                    streamName, currentSegmentSequenceNumber, segment.getLogSegmentSequenceNumber());
                setLastException(new DLIllegalStateException("Inconsistent state found in entry reader for "
                        + streamName + " : current segment sn = " + currentSegmentSequenceNumber
                        + ", new segment sn = " + segment.getLogSegmentSequenceNumber()));
                return;
            }
        }
        segmentIdx++;
        if (segmentIdx >= segments.size()) {
            return;
        }
        // check next segment
        segment = segments.get(segmentIdx);
        if (null != nextSegmentReader) {
            if (!updateLogSegmentMetadata(nextSegmentReader, segment)) {
                return;
            }
            segmentIdx++;
        }
        // check the segment readers in the queue
        for (int readerIdx = 0;
             readerIdx < segmentReaders.size() && segmentIdx < segments.size();
             readerIdx++, segmentIdx++) {
            SegmentReader reader = segmentReaders.get(readerIdx);
            segment = segments.get(segmentIdx);
            if (!updateLogSegmentMetadata(reader, segment)) {
                return;
            }
        }
        // add the remaining segments to the reader queue
        for (; segmentIdx < segments.size(); segmentIdx++) {
            segment = segments.get(segmentIdx);
            SegmentReader reader = new SegmentReader(segment, 0L);
            reader.openReader();
            segmentReaders.add(reader);
        }
        if (null == currentSegmentReader) {
            unsafeMoveToNextLogSegment();
        }
        // resume readahead if necessary
        scheduleRead();
    }

    /**
     * Initialize the reader with the log <i>segments</i>.
     *
     * @param segments list of log segments
     */
    private void unsafeInitializeLogSegments(List<LogSegmentMetadata> segments) {
        if (segments.isEmpty()) {
            // not initialize the background reader, until the first log segment is notified
            return;
        }
        boolean skipTruncatedLogSegments = true;
        DLSN dlsnToStart = fromDLSN;
        // positioning the reader
        for (int i = 0; i < segments.size(); i++) {
            LogSegmentMetadata segment = segments.get(i);
            // skip any log segments that have smaller log segment sequence numbers
            if (segment.getLogSegmentSequenceNumber() < fromDLSN.getLogSegmentSequenceNo()) {
                continue;
            }
            // if the log segment is truncated, skip it.
            if (skipTruncatedLogSegments
                    && !conf.getIgnoreTruncationStatus()
                    && segment.isTruncated()) {
                continue;
            }
            // if the log segment is partially truncated, move the start dlsn to the min active dlsn
            if (skipTruncatedLogSegments
                    && !conf.getIgnoreTruncationStatus()
                    && segment.isPartiallyTruncated()) {
                if (segment.getMinActiveDLSN().compareTo(fromDLSN) > 0) {
                    dlsnToStart = segment.getMinActiveDLSN();
                }
            }
            skipTruncatedLogSegments = false;
            if (!isAllowedToPosition(segment, dlsnToStart)) {
                logger.error("segment {} is not allowed to position at {}", segment, dlsnToStart);
                return;
            }

            SegmentReader reader = new SegmentReader(segment,
                    segment.getLogSegmentSequenceNumber() == dlsnToStart.getLogSegmentSequenceNo()
                            ? dlsnToStart.getEntryId() : 0L);
            segmentReaders.add(reader);
        }
        if (segmentReaders.isEmpty()) {
            // not initialize the background reader, until the first log segment is available to read
            return;
        }
        currentSegmentReader = segmentReaders.pollFirst();
        currentSegmentReader.openReader();
        currentSegmentReader.startRead();
        currentSegmentSequenceNumber = currentSegmentReader.getSegment().getLogSegmentSequenceNumber();

        scheduleRead();

        if (!segmentReaders.isEmpty()) {
            for (SegmentReader reader : segmentReaders) {
                reader.openReader();
            }
            unsafePrefetchNextSegment(true);
        }
        // mark the reader initialized
        isInitialized = true;
    }

    private void unsafePrefetchNextSegment(boolean onlyInprogressLogSegment) {
        SegmentReader nextReader = segmentReaders.peekFirst();
        // open the next log segment if it is inprogress
        if (null != nextReader) {
            if (onlyInprogressLogSegment && !nextReader.getSegment().isInProgress()) {
                return;
            }
            nextReader.startRead();
            nextSegmentReader = nextReader;
            segmentReaders.pollFirst();
        }
    }

    /**
     * Check if we are allowed to position the reader at <i>fromDLSN</i>.
     *
     * @return true if it is allowed, otherwise false.
     */
    private boolean isAllowedToPosition(LogSegmentMetadata segment, DLSN fromDLSN) {
        if (segment.isTruncated()
                && segment.getLastDLSN().compareTo(fromDLSN) >= 0
                && !conf.getIgnoreTruncationStatus()) {
            setLastException(new AlreadyTruncatedTransactionException(streamName
                    + " : trying to position read ahead at " + fromDLSN
                    + " on a segment " + segment + " that is already marked as truncated"));
            return false;
        }
        if (segment.isPartiallyTruncated()
                && segment.getMinActiveDLSN().compareTo(fromDLSN) > 0) {
            if (conf.getAlertWhenPositioningOnTruncated()) {
                alertStatsLogger.raise("Trying to position reader on {} when {} is marked partially truncated",
                    fromDLSN, segment);
            }
            if (!conf.getIgnoreTruncationStatus()) {
                logger.error("{}: Trying to position reader on {} when {} is marked partially truncated",
                    streamName, fromDLSN, segment);

                setLastException(new AlreadyTruncatedTransactionException(streamName
                        + " : trying to position read ahead at " + fromDLSN
                        + " on a segment " + segment + " that is already marked as truncated"));
                return false;
            }
        }
        return true;
    }

    void moveToNextLogSegment() {
        orderedSubmit(new CloseableRunnable() {
            @Override
            public void safeRun() {
                unsafeMoveToNextLogSegment();
            }
        });
    }

    private void unsafeMoveToNextLogSegment() {
        if (null != currentSegmentReader) {
            segmentReadersToClose.add(currentSegmentReader);
            FutureUtils.ensure(
                currentSegmentReader.close(),
                removeClosedSegmentReadersFunc);
            logger.debug("close current segment reader {}", currentSegmentReader.getSegment());
            currentSegmentReader = null;
        }
        boolean hasSegmentToRead = false;
        if (null != nextSegmentReader) {
            currentSegmentReader = nextSegmentReader;
            logger.debug("move to read segment {}", currentSegmentReader.getSegment());
            currentSegmentSequenceNumber = currentSegmentReader.getSegment().getLogSegmentSequenceNumber();
            nextSegmentReader = null;
            // start reading
            unsafePrefetchNextSegment(true);
            hasSegmentToRead = true;
        } else {
            unsafePrefetchNextSegment(false);
            if (null != nextSegmentReader) {
                currentSegmentReader = nextSegmentReader;
                logger.debug("move to read segment {}", currentSegmentReader.getSegment());
                currentSegmentSequenceNumber = currentSegmentReader.getSegment().getLogSegmentSequenceNumber();
                nextSegmentReader = null;
                unsafePrefetchNextSegment(true);
                hasSegmentToRead = true;
            }
        }
        if (!hasSegmentToRead) { // no more segment to read, wait until new log segment arrive
            if (isCatchingUp) {
                logger.info("ReadAhead for {} is caught up and no log segments to read now",
                        readHandler.getFullyQualifiedName());
                isCatchingUp = false;
            }
        }

        scheduleRead();
    }

    void completeRead() {
        orderedSubmit(new CloseableRunnable() {
                @Override
                public void safeRun() {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][state:{}] Read completed", streamName, state);
                    }
                    if (state == State.READING) {
                        state = State.IDLE;
                    }
                }
            });
    }

    void scheduleRead() {
        orderedSubmit(new CloseableRunnable() {
                @Override
                public void safeRun() {

                    boolean cacheFull = isCacheFull();
                    SegmentReader reader = currentSegmentReader;
                    boolean hasMoreSegments = reader != null;
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][state:{}] scheduling read, cacheFull {}, hasMoreSegments {}",
                                     streamName, state, cacheFull, hasMoreSegments);
                    }
                    switch (state) {
                    case IDLE:
                        if (cacheFull || !hasMoreSegments) {
                            state = State.PAUSED;
                        } else {
                            reader.readNext().whenComplete(ReadAheadEntryReader.this);
                            state = State.READING;
                        }
                        break;
                    case PAUSED:
                        if (!cacheFull && hasMoreSegments) {
                            reader.readNext().whenComplete(ReadAheadEntryReader.this);
                            state = State.READING;
                        }
                        break;
                    case READING:
                    case ERROR:
                    case CLOSED:
                        // do nothing
                        break;
                    }
                }
            });
    }

    @Override
    public void onSegmentsUpdated(List<LogSegmentMetadata> segments) {
        if (!started.get()) {
            return;
        }
        logger.info("segments is updated with {}", segments);
        processLogSegments(segments);
    }

    @Override
    public void onLogStreamDeleted() {
        setLastException(new LogNotFoundException("Log stream "
                + streamName + " is deleted"));
    }

}
