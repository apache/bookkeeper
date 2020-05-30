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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.exceptions.DLIllegalStateException;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.apache.distributedlog.exceptions.IdleReaderException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.ReadCancelledException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BookKeeper based {@link AsyncLogReader} implementation.
 *
 * <h3>Metrics</h3>
 * All the metrics are exposed under `async_reader`.
 * <ul>
 * <li> `async_reader`/future_set: opstats. time spent on satisfying futures of read requests.
 * if it is high, it means that the caller takes time on processing the result of read requests.
 * The side effect is blocking consequent reads.
 * <li> `async_reader`/schedule: opstats. time spent on scheduling next reads.
 * <li> `async_reader`/background_read: opstats. time spent on background reads.
 * <li> `async_reader`/read_next_exec: opstats. time spent on executing {@link #readNext()}.
 * <li> `async_reader`/time_between_read_next: opstats. time spent on between two consequent {@link #readNext()}.
 * if it is high, it means that the caller is slowing down on calling {@link #readNext()}.
 * <li> `async_reader`/delay_until_promise_satisfied: opstats. total latency for the read requests.
 * <li> `async_reader`/idle_reader_error: counter. the number idle reader errors.
 * </ul>
 */
class BKAsyncLogReader implements AsyncLogReader, SafeRunnable, AsyncNotification {
    static final Logger LOG = LoggerFactory.getLogger(BKAsyncLogReader.class);

    private static final Function<List<LogRecordWithDLSN>, LogRecordWithDLSN> READ_NEXT_MAP_FUNCTION =
        records -> records.get(0);

    private final String streamName;
    protected final BKDistributedLogManager bkDistributedLogManager;
    protected final BKLogReadHandler readHandler;
    private static final AtomicReferenceFieldUpdater<BKAsyncLogReader, Throwable> lastExceptionUpdater =
        AtomicReferenceFieldUpdater.newUpdater(BKAsyncLogReader.class, Throwable.class, "lastException");
    private volatile Throwable lastException = null;
    private final OrderedScheduler scheduler;
    private final ConcurrentLinkedQueue<PendingReadRequest> pendingRequests =
            new ConcurrentLinkedQueue<PendingReadRequest>();
    private final Object scheduleLock = new Object();
    private static final AtomicLongFieldUpdater<BKAsyncLogReader> scheduleCountUpdater =
        AtomicLongFieldUpdater.newUpdater(BKAsyncLogReader.class, "scheduleCount");
    private volatile long scheduleCount = 0L;
    private final Stopwatch scheduleDelayStopwatch;
    private final Stopwatch readNextDelayStopwatch;
    private DLSN startDLSN;
    private ReadAheadEntryReader readAheadReader = null;
    private int lastPosition = 0;
    private final boolean positionGapDetectionEnabled;
    private final int idleErrorThresholdMillis;
    final ScheduledFuture<?> idleReaderTimeoutTask;
    private ScheduledFuture<?> backgroundScheduleTask = null;
    // last process time
    private final Stopwatch lastProcessTime;

    protected CompletableFuture<Void> closeFuture = null;

    private boolean lockStream = false;

    private final boolean returnEndOfStreamRecord;

    private final SafeRunnable BACKGROUND_READ_SCHEDULER = () -> {
        synchronized (scheduleLock) {
            backgroundScheduleTask = null;
        }
        scheduleBackgroundRead();
    };

    // State
    private Entry.Reader currentEntry = null;
    private LogRecordWithDLSN nextRecord = null;

    // Failure Injector
    private boolean disableProcessingReadRequests = false;

    // Stats
    private final OpStatsLogger readNextExecTime;
    private final OpStatsLogger delayUntilPromiseSatisfied;
    private final OpStatsLogger timeBetweenReadNexts;
    private final OpStatsLogger futureSetLatency;
    private final OpStatsLogger scheduleLatency;
    private final OpStatsLogger backgroundReaderRunTime;
    private final Counter idleReaderCheckCount;
    private final Counter idleReaderCheckIdleReadRequestCount;
    private final Counter idleReaderCheckIdleReadAheadCount;
    private final Counter idleReaderError;

    private class PendingReadRequest {
        private final Stopwatch enqueueTime;
        private final int numEntries;
        private final List<LogRecordWithDLSN> records;
        private final CompletableFuture<List<LogRecordWithDLSN>> promise;
        private final long deadlineTime;
        private final TimeUnit deadlineTimeUnit;

        PendingReadRequest(int numEntries,
                           long deadlineTime,
                           TimeUnit deadlineTimeUnit) {
            this.numEntries = numEntries;
            this.enqueueTime = Stopwatch.createStarted();
            // optimize the space usage for single read.
            if (numEntries == 1) {
                this.records = new ArrayList<LogRecordWithDLSN>(1);
            } else {
                this.records = new ArrayList<LogRecordWithDLSN>();
            }
            this.promise = new CompletableFuture<List<LogRecordWithDLSN>>();
            this.deadlineTime = deadlineTime;
            this.deadlineTimeUnit = deadlineTimeUnit;
        }

        CompletableFuture<List<LogRecordWithDLSN>> getPromise() {
            return promise;
        }

        long elapsedSinceEnqueue(TimeUnit timeUnit) {
            return enqueueTime.elapsed(timeUnit);
        }

        void completeExceptionally(Throwable throwable) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            if (promise.completeExceptionally(throwable)) {
                futureSetLatency.registerFailedEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
                delayUntilPromiseSatisfied.registerFailedEvent(
                    enqueueTime.elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
            }
        }

        boolean hasReadRecords() {
            return records.size() > 0;
        }

        boolean hasReadEnoughRecords() {
            return records.size() >= numEntries;
        }

        long getRemainingWaitTime() {
            if (deadlineTime <= 0L) {
                return 0L;
            }
            return deadlineTime - elapsedSinceEnqueue(deadlineTimeUnit);
        }

        void addRecord(LogRecordWithDLSN record) {
            records.add(record);
        }

        void complete() {
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} : Satisfied promise with {} records",
                        readHandler.getFullyQualifiedName(), records.size());
            }
            delayUntilPromiseSatisfied.registerSuccessfulEvent(
                enqueueTime.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
            Stopwatch stopwatch = Stopwatch.createStarted();
            promise.complete(records);
            futureSetLatency.registerSuccessfulEvent(
                stopwatch.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
        }
    }

    BKAsyncLogReader(BKDistributedLogManager bkdlm,
                     OrderedScheduler scheduler,
                     DLSN startDLSN,
                     Optional<String> subscriberId,
                     boolean returnEndOfStreamRecord,
                     StatsLogger statsLogger) {
        this.streamName = bkdlm.getStreamName();
        this.bkDistributedLogManager = bkdlm;
        this.scheduler = scheduler;
        this.readHandler = bkDistributedLogManager.createReadHandler(subscriberId,
                this, true);
        LOG.debug("Starting async reader at {}", startDLSN);
        this.startDLSN = startDLSN;
        this.scheduleDelayStopwatch = Stopwatch.createUnstarted();
        this.readNextDelayStopwatch = Stopwatch.createStarted();
        this.positionGapDetectionEnabled = bkdlm.getConf().getPositionGapDetectionEnabled();
        this.idleErrorThresholdMillis = bkdlm.getConf().getReaderIdleErrorThresholdMillis();
        this.returnEndOfStreamRecord = returnEndOfStreamRecord;

        // Stats
        StatsLogger asyncReaderStatsLogger = statsLogger.scope("async_reader");
        futureSetLatency = asyncReaderStatsLogger.getOpStatsLogger("future_set");
        scheduleLatency = asyncReaderStatsLogger.getOpStatsLogger("schedule");
        backgroundReaderRunTime = asyncReaderStatsLogger.getOpStatsLogger("background_read");
        readNextExecTime = asyncReaderStatsLogger.getOpStatsLogger("read_next_exec");
        timeBetweenReadNexts = asyncReaderStatsLogger.getOpStatsLogger("time_between_read_next");
        delayUntilPromiseSatisfied = asyncReaderStatsLogger.getOpStatsLogger("delay_until_promise_satisfied");
        idleReaderError = asyncReaderStatsLogger.getCounter("idle_reader_error");
        idleReaderCheckCount = asyncReaderStatsLogger.getCounter("idle_reader_check_total");
        idleReaderCheckIdleReadRequestCount = asyncReaderStatsLogger.getCounter("idle_reader_check_idle_read_requests");
        idleReaderCheckIdleReadAheadCount = asyncReaderStatsLogger.getCounter("idle_reader_check_idle_readahead");

        // Lock the stream if requested. The lock will be released when the reader is closed.
        this.lockStream = false;
        this.idleReaderTimeoutTask = scheduleIdleReaderTaskIfNecessary();
        this.lastProcessTime = Stopwatch.createStarted();
    }

    synchronized void releaseCurrentEntry() {
        if (null != currentEntry) {
            currentEntry.release();
            currentEntry = null;
        }
    }

    private ScheduledFuture<?> scheduleIdleReaderTaskIfNecessary() {
        if (idleErrorThresholdMillis < Integer.MAX_VALUE) {
            // Dont run the task more than once every seconds (for sanity)
            long period = Math.max(idleErrorThresholdMillis / 10, 1000);
            // Except when idle reader threshold is less than a second (tests?)
            period = Math.min(period, idleErrorThresholdMillis / 5);

            return scheduler.scheduleAtFixedRateOrdered(streamName, new SafeRunnable() {
                @Override
                public void safeRun() {
                    PendingReadRequest nextRequest = pendingRequests.peek();

                    idleReaderCheckCount.inc();
                    if (null == nextRequest) {
                        return;
                    }

                    idleReaderCheckIdleReadRequestCount.inc();
                    if (nextRequest.elapsedSinceEnqueue(TimeUnit.MILLISECONDS) < idleErrorThresholdMillis) {
                        return;
                    }

                    ReadAheadEntryReader readAheadReader = getReadAheadReader();

                    // read request has been idle
                    //   - cache has records but read request are idle,
                    //     that means notification was missed between readahead and reader.
                    //   - cache is empty and readahead is idle (no records added for a long time)
                    idleReaderCheckIdleReadAheadCount.inc();
                    try {
                        if (null == readAheadReader || (!hasMoreRecords()
                                && readAheadReader.isReaderIdle(idleErrorThresholdMillis, TimeUnit.MILLISECONDS))) {
                            markReaderAsIdle();
                            return;
                        } else if (lastProcessTime.elapsed(TimeUnit.MILLISECONDS) > idleErrorThresholdMillis) {
                            markReaderAsIdle();
                        }
                    } catch (IOException e) {
                        setLastException(e);
                        return;
                    }
                }
            }, period, period, TimeUnit.MILLISECONDS);
        }
        return null;
    }

    synchronized ReadAheadEntryReader getReadAheadReader() {
        return readAheadReader;
    }

    void cancelIdleReaderTask() {
        // Do this after we have checked that the reader was not previously closed
        try {
            if (null != idleReaderTimeoutTask) {
                idleReaderTimeoutTask.cancel(true);
            }
        } catch (Exception exc) {
            LOG.info("{}: Failed to cancel the background idle reader timeout task",
                    readHandler.getFullyQualifiedName());
        }
    }

    private void markReaderAsIdle() {
        idleReaderError.inc();
        IdleReaderException ire = new IdleReaderException("Reader on stream "
                + readHandler.getFullyQualifiedName()
                + " is idle for " + idleErrorThresholdMillis + " ms");
        setLastException(ire);
        // cancel all pending reads directly rather than notifying on error
        // because idle reader could happen on idle read requests that usually means something wrong
        // in scheduling reads
        cancelAllPendingReads(ire);
    }

    protected synchronized void setStartDLSN(DLSN fromDLSN) throws UnexpectedException {
        if (null != readAheadReader) {
            throw new UnexpectedException("Could't reset from dlsn after reader already starts reading.");
        }
        startDLSN = fromDLSN;
    }

    @VisibleForTesting
    public synchronized DLSN getStartDLSN() {
        return startDLSN;
    }

    public CompletableFuture<Void> lockStream() {
        this.lockStream = true;
        return readHandler.lockStream();
    }

    private boolean checkClosedOrInError(String operation) {
        if (null == lastExceptionUpdater.get(this)) {
            try {
                if (null != readHandler && null != getReadAheadReader()) {
                    getReadAheadReader().checkLastException();
                }

                bkDistributedLogManager.checkClosedOrInError(operation);
            } catch (IOException exc) {
                setLastException(exc);
            }
        }

        if (lockStream) {
            try {
                readHandler.checkReadLock();
            } catch (IOException ex) {
                setLastException(ex);
            }
        }

        Throwable cause = lastExceptionUpdater.get(this);
        if (null != cause) {
            LOG.trace("Cancelling pending reads");
            cancelAllPendingReads(cause);
            return true;
        }

        return false;
    }

    private void setLastException(IOException exc) {
        lastExceptionUpdater.compareAndSet(this, null, exc);
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    /**
     * @return A promise that when satisfied will contain the Log Record with its DLSN.
     */
    @Override
    public synchronized CompletableFuture<LogRecordWithDLSN> readNext() {
        return readInternal(1, 0, TimeUnit.MILLISECONDS).thenApply(READ_NEXT_MAP_FUNCTION);
    }

    @Override
    public synchronized CompletableFuture<List<LogRecordWithDLSN>> readBulk(int numEntries) {
        return readInternal(numEntries, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized CompletableFuture<List<LogRecordWithDLSN>> readBulk(int numEntries,
                                                                 long waitTime,
                                                                 TimeUnit timeUnit) {
        return readInternal(numEntries, waitTime, timeUnit);
    }

    /**
     * Read up to <i>numEntries</i> entries. The future will be satisfied when any number of entries are
     * ready (1 to <i>numEntries</i>).
     *
     * @param numEntries
     *          num entries to read
     * @return A promise that satisfied with a non-empty list of log records with their DLSN.
     */
    private synchronized CompletableFuture<List<LogRecordWithDLSN>> readInternal(int numEntries,
                                                                      long deadlineTime,
                                                                      TimeUnit deadlineTimeUnit) {
        timeBetweenReadNexts.registerSuccessfulEvent(
            readNextDelayStopwatch.elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
        readNextDelayStopwatch.reset().start();
        final PendingReadRequest readRequest = new PendingReadRequest(numEntries, deadlineTime, deadlineTimeUnit);

        if (null == readAheadReader) {
            final ReadAheadEntryReader readAheadEntryReader = this.readAheadReader = new ReadAheadEntryReader(
                    getStreamName(),
                    getStartDLSN(),
                    bkDistributedLogManager.getConf(),
                    readHandler,
                    bkDistributedLogManager.getReaderEntryStore(),
                    bkDistributedLogManager.getScheduler(),
                    Ticker.systemTicker(),
                    bkDistributedLogManager.alertStatsLogger);
            readHandler.checkLogStreamExists().whenComplete(new FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    try {
                        readHandler.registerListener(readAheadEntryReader);
                        readHandler.asyncStartFetchLogSegments()
                                .thenAccept(logSegments -> {
                                    readAheadEntryReader.addStateChangeNotification(BKAsyncLogReader.this);
                                    readAheadEntryReader.start(logSegments.getValue());
                                });
                    } catch (Exception exc) {
                        notifyOnError(exc);
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    notifyOnError(cause);
                }
            });
        }

        if (checkClosedOrInError("readNext")) {
            readRequest.completeExceptionally(lastExceptionUpdater.get(this));
        } else {
            boolean queueEmpty = pendingRequests.isEmpty();
            pendingRequests.add(readRequest);

            if (queueEmpty) {
                scheduleBackgroundRead();
            }
        }

        readNextExecTime.registerSuccessfulEvent(
            readNextDelayStopwatch.elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
        readNextDelayStopwatch.reset().start();

        return readRequest.getPromise();
    }

    public synchronized void scheduleBackgroundRead() {
        // if the reader is already closed, we don't need to schedule background read again.
        if (null != closeFuture) {
            return;
        }

        long prevCount = scheduleCountUpdater.getAndIncrement(this);
        if (0 == prevCount) {
            scheduleDelayStopwatch.reset().start();
            scheduler.executeOrdered(streamName, this);
        }
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        // Cancel the idle reader timeout task, interrupting if necessary
        ReadCancelledException exception;
        CompletableFuture<Void> closePromise;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closePromise = closeFuture = new CompletableFuture<Void>();
            exception = new ReadCancelledException(readHandler.getFullyQualifiedName(), "Reader was closed");
            setLastException(exception);
            releaseCurrentEntry();
        }

        // Do this after we have checked that the reader was not previously closed
        cancelIdleReaderTask();

        synchronized (scheduleLock) {
            if (null != backgroundScheduleTask) {
                backgroundScheduleTask.cancel(true);
            }
        }

        cancelAllPendingReads(exception);

        ReadAheadEntryReader readAheadReader = getReadAheadReader();
        if (null != readAheadReader) {
            readHandler.unregisterListener(readAheadReader);
            readAheadReader.removeStateChangeNotification(this);
        }
        FutureUtils.proxyTo(
            Utils.closeSequence(bkDistributedLogManager.getScheduler(), true,
                    readAheadReader,
                    readHandler
            ),
            closePromise);
        return closePromise;
    }

    private void cancelAllPendingReads(Throwable throwExc) {
        for (PendingReadRequest promise : pendingRequests) {
            promise.completeExceptionally(throwExc);
        }
        pendingRequests.clear();
    }

    synchronized boolean hasMoreRecords() throws IOException {
        if (null == readAheadReader) {
            return false;
        }
        if (readAheadReader.getNumCachedEntries() > 0 || null != nextRecord) {
            return true;
        } else if (null != currentEntry) {
            nextRecord = currentEntry.nextRecord();
            return null != nextRecord;
        }
        return false;
    }

    private synchronized LogRecordWithDLSN readNextRecord() throws IOException {
        if (null == readAheadReader) {
            return null;
        }
        if (null == currentEntry) {
            currentEntry = readAheadReader.getNextReadAheadEntry(0L, TimeUnit.MILLISECONDS);
            // no entry after reading from read ahead then return null
            if (null == currentEntry) {
                return null;
            }
        }

        LogRecordWithDLSN recordToReturn;
        if (null == nextRecord) {
            nextRecord = currentEntry.nextRecord();
            // no more records in current entry
            if (null == nextRecord) {
                currentEntry = null;
                return readNextRecord();
            }
        }

        // found a record to return and prefetch the next one
        recordToReturn = nextRecord;
        nextRecord = currentEntry.nextRecord();
        return recordToReturn;
    }

    @Override
    public void safeRun() {
        synchronized (scheduleLock) {
            if (scheduleDelayStopwatch.isRunning()) {
                scheduleLatency.registerSuccessfulEvent(
                    scheduleDelayStopwatch.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
            }

            Stopwatch runTime = Stopwatch.createStarted();
            int iterations = 0;
            long scheduleCountLocal = scheduleCountUpdater.get(this);
            LOG.debug("{}: Scheduled Background Reader", readHandler.getFullyQualifiedName());
            while (true) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{}: Executing Iteration: {}", readHandler.getFullyQualifiedName(), iterations++);
                }

                PendingReadRequest nextRequest = null;
                synchronized (this) {
                    nextRequest = pendingRequests.peek();

                    // Queue is empty, nothing to read, return
                    if (null == nextRequest) {
                        LOG.trace("{}: Queue Empty waiting for Input", readHandler.getFullyQualifiedName());
                        scheduleCountUpdater.set(this, 0);
                        backgroundReaderRunTime.registerSuccessfulEvent(
                            runTime.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
                        return;
                    }

                    if (disableProcessingReadRequests) {
                        LOG.info("Reader of {} is forced to stop processing read requests",
                            readHandler.getFullyQualifiedName());
                        return;
                    }
                }
                lastProcessTime.reset().start();

                // If the oldest pending promise is interrupted then we must mark
                // the reader in error and abort all pending reads since we dont
                // know the last consumed read
                if (null == lastExceptionUpdater.get(this)) {
                    if (nextRequest.getPromise().isCancelled()) {
                        setLastException(new DLInterruptedException("Interrupted on reading "
                            + readHandler.getFullyQualifiedName()));
                    }
                }

                if (checkClosedOrInError("readNext")) {
                    Throwable lastException = lastExceptionUpdater.get(this);
                    if (lastException != null && !(lastException.getCause() instanceof LogNotFoundException)) {
                        LOG.warn("{}: Exception", readHandler.getFullyQualifiedName(), lastException);
                    }
                    backgroundReaderRunTime.registerFailedEvent(
                        runTime.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
                    return;
                }

                try {
                    // Fail 10% of the requests when asked to simulate errors
                    if (bkDistributedLogManager.getFailureInjector().shouldInjectErrors()) {
                        throw new IOException("Reader Simulated Exception");
                    }
                    LogRecordWithDLSN record;
                    while (!nextRequest.hasReadEnoughRecords()) {
                        // read single record
                        do {
                            record = readNextRecord();
                        } while (null != record && (record.isControl()
                                || (record.getDlsn().compareTo(getStartDLSN()) < 0)));
                        if (null == record) {
                            break;
                        } else {
                            if (record.isEndOfStream() && !returnEndOfStreamRecord) {
                                setLastException(new EndOfStreamException("End of Stream Reached for "
                                        + readHandler.getFullyQualifiedName()));
                                break;
                            }

                            // gap detection
                            if (recordPositionsContainsGap(record, lastPosition)) {
                                bkDistributedLogManager.raiseAlert("Gap detected between records at record = {}",
                                        record);
                                if (positionGapDetectionEnabled) {
                                    throw new DLIllegalStateException("Gap detected between records at record = "
                                            + record);
                                }
                            }
                            lastPosition = record.getLastPositionWithinLogSegment();
                            nextRequest.addRecord(record);
                        }
                    }
                } catch (IOException exc) {
                    setLastException(exc);
                    if (!(exc instanceof LogNotFoundException)) {
                        LOG.warn("{} : read with skip Exception",
                                readHandler.getFullyQualifiedName(), lastExceptionUpdater.get(this));
                    }
                    continue;
                }

                if (nextRequest.hasReadRecords()) {
                    long remainingWaitTime = nextRequest.getRemainingWaitTime();
                    if (remainingWaitTime > 0 && !nextRequest.hasReadEnoughRecords()) {
                        backgroundReaderRunTime.registerSuccessfulEvent(
                            runTime.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
                        scheduleDelayStopwatch.reset().start();
                        scheduleCountUpdater.set(this, 0);
                        // the request could still wait for more records
                        backgroundScheduleTask = scheduler.scheduleOrdered(
                                streamName,
                                BACKGROUND_READ_SCHEDULER,
                                remainingWaitTime,
                                nextRequest.deadlineTimeUnit);
                        return;
                    }

                    PendingReadRequest request = pendingRequests.poll();
                    if (null != request && nextRequest == request) {
                        request.complete();
                        if (null != backgroundScheduleTask) {
                            backgroundScheduleTask.cancel(true);
                            backgroundScheduleTask = null;
                        }
                    } else {
                        DLIllegalStateException ise = new DLIllegalStateException("Unexpected condition at dlsn = "
                                + nextRequest.records.get(0).getDlsn());
                        nextRequest.completeExceptionally(ise);
                        if (null != request) {
                            request.completeExceptionally(ise);
                        }
                        // We should never get here as we should have exited the loop if
                        // pendingRequests were empty
                        bkDistributedLogManager.raiseAlert("Unexpected condition at dlsn = {}",
                                nextRequest.records.get(0).getDlsn());
                        setLastException(ise);
                    }
                } else {
                    if (0 == scheduleCountLocal) {
                        LOG.trace("Schedule count dropping to zero", lastExceptionUpdater.get(this));
                        backgroundReaderRunTime.registerSuccessfulEvent(
                            runTime.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
                        return;
                    }
                    scheduleCountLocal = scheduleCountUpdater.decrementAndGet(this);
                }
            }
        }
    }

    private boolean recordPositionsContainsGap(LogRecordWithDLSN record, long lastPosition) {
        final boolean firstLogRecord = (1 == record.getPositionWithinLogSegment());
        final boolean endOfStreamRecord = record.isEndOfStream();
        final boolean emptyLogSegment = (0 == lastPosition);
        final boolean positionIncreasedByOne = (record.getPositionWithinLogSegment() == (lastPosition + 1));

        return !firstLogRecord && !endOfStreamRecord && !emptyLogSegment
                && !positionIncreasedByOne;
    }

    /**
     * Triggered when the background activity encounters an exception.
     */
    @Override
    public void notifyOnError(Throwable cause) {
        if (cause instanceof IOException) {
            setLastException((IOException) cause);
        } else {
            setLastException(new IOException(cause));
        }
        scheduleBackgroundRead();
    }

    /**
     * Triggered when the background activity completes an operation.
     */
    @Override
    public void notifyOnOperationComplete() {
        scheduleBackgroundRead();
    }

    @VisibleForTesting
    void simulateErrors() {
        bkDistributedLogManager.getFailureInjector().injectErrors(true);
    }

    @VisibleForTesting
    synchronized void disableReadAheadLogSegmentsNotification() {
        readHandler.disableReadAheadLogSegmentsNotification();
    }

    @VisibleForTesting
    synchronized void disableProcessingReadRequests() {
        disableProcessingReadRequests = true;
    }
}

