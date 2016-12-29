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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.callback.LogSegmentNamesListener;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.LockingException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.exceptions.LogSegmentNotFoundException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.logsegment.LogSegmentEntryStore;
import com.twitter.distributedlog.metadata.LogMetadataForReader;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.distributedlog.logsegment.LogSegmentFilter;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataCache;
import com.twitter.distributedlog.metadata.LogStreamMetadataStore;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import com.twitter.util.Try;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import javax.annotation.Nullable;

/**
 * Log Handler for Readers.
 * <h3>Metrics</h3>
 *
 * <h4>ReadAhead Worker</h4>
 * Most of readahead stats are exposed under scope `readahead_worker`. Only readahead exceptions are exposed
 * in parent scope via <code>readAheadExceptionsLogger</code>.
 * <ul>
 * <li> `readahead_worker`/wait: counter. number of waits that readahead worker is waiting. If this keeps increasing,
 * it usually means readahead keep getting full because of reader slows down reading.
 * <li> `readahead_worker`/repositions: counter. number of repositions that readhead worker encounters. reposition
 * means that a readahead worker finds that it isn't advancing to a new log segment and force re-positioning.
 * <li> `readahead_worker`/entry_piggy_back_hits: counter. it increases when the last add confirmed being advanced
 * because of the piggy-back lac.
 * <li> `readahead_worker`/entry_piggy_back_misses: counter. it increases when the last add confirmed isn't advanced
 * by a read entry because it doesn't piggy back a newer lac.
 * <li> `readahead_worker`/read_entries: opstats. stats on number of entries read per readahead read batch.
 * <li> `readahead_worker`/read_lac_counter: counter. stats on the number of readLastConfirmed operations
 * <li> `readahead_worker`/read_lac_and_entry_counter: counter. stats on the number of readLastConfirmedAndEntry
 * operations.
 * <li> `readahead_worker`/cache_full: counter. it increases each time readahead worker finds cache become full.
 * If it keeps increasing, that means reader slows down reading.
 * <li> `readahead_worker`/resume: opstats. stats on readahead worker resuming reading from wait state.
 * <li> `readahead_worker`/read_lac_lag: opstats. stats on the number of entries diff between the lac reader knew
 * last time and the lac that it received. if `lag` between two subsequent lacs is high, that might means delay
 * might be high. because reader is only allowed to read entries after lac is advanced.
 * <li> `readahead_worker`/long_poll_interruption: opstats. stats on the number of interruptions happened to long
 * poll. the interruptions are usually because of receiving zookeeper notifications.
 * <li> `readahead_worker`/notification_execution: opstats. stats on executions over the notifications received from
 * zookeeper.
 * <li> `readahead_worker`/metadata_reinitialization: opstats. stats on metadata reinitialization after receiving
 * notifcation from log segments updates.
 * <li> `readahead_worker`/idle_reader_warn: counter. it increases each time the readahead worker detects itself
 * becoming idle.
 * </ul>
 * <h4>Read Lock</h4>
 * All read lock related stats are exposed under scope `read_lock`.
 * for detail stats.
 */
class BKLogReadHandler extends BKLogHandler implements LogSegmentNamesListener {
    static final Logger LOG = LoggerFactory.getLogger(BKLogReadHandler.class);

    protected final LogMetadataForReader logMetadataForReader;

    protected final DynamicDistributedLogConfiguration dynConf;

    private final Optional<String> subscriberId;
    private DistributedLock readLock;
    private Future<Void> lockAcquireFuture;

    // notify the state change about the read handler
    protected final AsyncNotification readerStateNotification;

    // log segments listener
    protected boolean logSegmentsNotificationDisabled = false;
    protected final CopyOnWriteArraySet<LogSegmentListener> listeners =
            new CopyOnWriteArraySet<LogSegmentListener>();
    protected Versioned<List<LogSegmentMetadata>> lastNotifiedLogSegments =
            new Versioned<List<LogSegmentMetadata>>(null, Version.NEW);

    // stats
    private final StatsLogger perLogStatsLogger;

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogReadHandler(LogMetadataForReader logMetadata,
                     Optional<String> subscriberId,
                     DistributedLogConfiguration conf,
                     DynamicDistributedLogConfiguration dynConf,
                     LogStreamMetadataStore streamMetadataStore,
                     LogSegmentMetadataCache metadataCache,
                     LogSegmentEntryStore entryStore,
                     OrderedScheduler scheduler,
                     AlertStatsLogger alertStatsLogger,
                     StatsLogger statsLogger,
                     StatsLogger perLogStatsLogger,
                     String clientId,
                     AsyncNotification readerStateNotification,
                     boolean isHandleForReading) {
        super(logMetadata,
                conf,
                streamMetadataStore,
                metadataCache,
                entryStore,
                scheduler,
                statsLogger,
                alertStatsLogger,
                clientId);
        this.logMetadataForReader = logMetadata;
        this.dynConf = dynConf;
        this.perLogStatsLogger =
                isHandleForReading ? perLogStatsLogger : NullStatsLogger.INSTANCE;
        this.readerStateNotification = readerStateNotification;
        this.subscriberId = subscriberId;
    }

    @VisibleForTesting
    String getReadLockPath() {
        return logMetadataForReader.getReadLockPath(subscriberId);
    }

    <T> void satisfyPromiseAsync(final Promise<T> promise, final Try<T> result) {
        scheduler.submit(new SafeRunnable() {
            @Override
            public void safeRun() {
                promise.update(result);
            }
        });
    }

    Future<Void> checkLogStreamExists() {
        return streamMetadataStore.logExists(logMetadata.getUri(), logMetadata.getLogName());
    }

    /**
     * Elective stream lock--readers are not required to acquire the lock before using the stream.
     */
    synchronized Future<Void> lockStream() {
        if (null == lockAcquireFuture) {
            lockAcquireFuture = streamMetadataStore.createReadLock(logMetadataForReader, subscriberId)
                    .flatMap(new ExceptionalFunction<DistributedLock, Future<Void>>() {
                        @Override
                        public Future<Void> applyE(DistributedLock lock) throws Throwable {
                            BKLogReadHandler.this.readLock = lock;
                            LOG.info("acquiring readlock {} at {}", getLockClientId(), getReadLockPath());
                            return acquireLockOnExecutorThread(lock);
                        }
                    });
        }
        return lockAcquireFuture;
    }

    /**
     * Begin asynchronous lock acquire, but ensure that the returned future is satisfied on an
     * executor service thread.
     */
    Future<Void> acquireLockOnExecutorThread(DistributedLock lock) throws LockingException {
        final Future<? extends DistributedLock> acquireFuture = lock.asyncAcquire();

        // The future we return must be satisfied on an executor service thread. If we simply
        // return the future returned by asyncAcquire, user callbacks may end up running in
        // the lock state executor thread, which will cause deadlocks and introduce latency
        // etc.
        final Promise<Void> threadAcquirePromise = new Promise<Void>();
        threadAcquirePromise.setInterruptHandler(new Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable t) {
                FutureUtils.cancel(acquireFuture);
                return null;
            }
        });
        acquireFuture.addEventListener(new FutureEventListener<DistributedLock>() {
            @Override
            public void onSuccess(DistributedLock lock) {
                LOG.info("acquired readlock {} at {}", getLockClientId(), getReadLockPath());
                satisfyPromiseAsync(threadAcquirePromise, new Return<Void>(null));
            }

            @Override
            public void onFailure(Throwable cause) {
                LOG.info("failed to acquire readlock {} at {}",
                        new Object[]{ getLockClientId(), getReadLockPath(), cause });
                satisfyPromiseAsync(threadAcquirePromise, new Throw<Void>(cause));
            }
        });
        return threadAcquirePromise;
    }

    /**
     * Check ownership of elective stream lock.
     */
    void checkReadLock() throws DLIllegalStateException, LockingException {
        synchronized (this) {
            if ((null == lockAcquireFuture) ||
                (!lockAcquireFuture.isDefined())) {
                throw new DLIllegalStateException("Attempt to check for lock before it has been acquired successfully");
            }
        }

        readLock.checkOwnership();
    }

    public Future<Void> asyncClose() {
        DistributedLock lockToClose;
        synchronized (this) {
            if (null != lockAcquireFuture && !lockAcquireFuture.isDefined()) {
                FutureUtils.cancel(lockAcquireFuture);
            }
            lockToClose = readLock;
        }
        return Utils.closeSequence(scheduler, lockToClose)
                .flatMap(new AbstractFunction1<Void, Future<Void>>() {
            @Override
            public Future<Void> apply(Void result) {
                // unregister the log segment listener
                metadataStore.unregisterLogSegmentListener(logMetadata.getLogSegmentsPath(), BKLogReadHandler.this);
                return Future.Void();
            }
        });
    }

    @Override
    public Future<Void> asyncAbort() {
        return asyncClose();
    }

    /**
     * Start fetch the log segments and register the {@link LogSegmentNamesListener}.
     * The future is satisfied only on a successful fetch or encountered a fatal failure.
     *
     * @return future represents the fetch result
     */
    Future<Versioned<List<LogSegmentMetadata>>> asyncStartFetchLogSegments() {
        Promise<Versioned<List<LogSegmentMetadata>>> promise =
                new Promise<Versioned<List<LogSegmentMetadata>>>();
        asyncStartFetchLogSegments(promise);
        return promise;
    }

    void asyncStartFetchLogSegments(final Promise<Versioned<List<LogSegmentMetadata>>> promise) {
        readLogSegmentsFromStore(
                LogSegmentMetadata.COMPARATOR,
                LogSegmentFilter.DEFAULT_FILTER,
                this).addEventListener(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {
            @Override
            public void onFailure(Throwable cause) {
                if (cause instanceof LogNotFoundException ||
                        cause instanceof LogSegmentNotFoundException ||
                        cause instanceof UnexpectedException) {
                    // indicate some inconsistent behavior, abort
                    metadataException.compareAndSet(null, (IOException) cause);
                    // notify the reader that read handler is in error state
                    notifyReaderOnError(cause);
                    FutureUtils.setException(promise, cause);
                    return;
                }
                scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        asyncStartFetchLogSegments(promise);
                    }
                }, conf.getZKRetryBackoffMaxMillis(), TimeUnit.MILLISECONDS);
            }

            @Override
            public void onSuccess(Versioned<List<LogSegmentMetadata>> segments) {
                // no-op
                FutureUtils.setValue(promise, segments);
            }
        });
    }

    @VisibleForTesting
    void disableReadAheadLogSegmentsNotification() {
        logSegmentsNotificationDisabled = true;
    }

    @Override
    public void onSegmentsUpdated(final Versioned<List<String>> segments) {
        synchronized (this) {
            if (lastNotifiedLogSegments.getVersion() != Version.NEW &&
                    lastNotifiedLogSegments.getVersion().compare(segments.getVersion()) != Version.Occurred.BEFORE) {
                // the log segments has been read, and it is possibly a retry from last segments update
                return;
            }
        }

        Promise<Versioned<List<LogSegmentMetadata>>> readLogSegmentsPromise =
                new Promise<Versioned<List<LogSegmentMetadata>>>();
        readLogSegmentsPromise.addEventListener(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {
            @Override
            public void onFailure(Throwable cause) {
                if (cause instanceof LogNotFoundException ||
                        cause instanceof LogSegmentNotFoundException ||
                        cause instanceof UnexpectedException) {
                    // indicate some inconsistent behavior, abort
                    metadataException.compareAndSet(null, (IOException) cause);
                    // notify the reader that read handler is in error state
                    notifyReaderOnError(cause);
                    return;
                }
                scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        onSegmentsUpdated(segments);
                    }
                }, conf.getZKRetryBackoffMaxMillis(), TimeUnit.MILLISECONDS);
            }

            @Override
            public void onSuccess(Versioned<List<LogSegmentMetadata>> logSegments) {
                List<LogSegmentMetadata> segmentsToNotify = null;
                synchronized (BKLogReadHandler.this) {
                    Versioned<List<LogSegmentMetadata>> lastLogSegments = lastNotifiedLogSegments;
                    if (lastLogSegments.getVersion() == Version.NEW ||
                            lastLogSegments.getVersion().compare(logSegments.getVersion()) == Version.Occurred.BEFORE) {
                        lastNotifiedLogSegments = logSegments;
                        segmentsToNotify = logSegments.getValue();
                    }
                }
                if (null != segmentsToNotify) {
                    notifyUpdatedLogSegments(segmentsToNotify);
                }
            }
        });
        // log segments list is updated, read their metadata
        readLogSegmentsFromStore(
                segments,
                LogSegmentMetadata.COMPARATOR,
                LogSegmentFilter.DEFAULT_FILTER,
                readLogSegmentsPromise);
    }

    @Override
    public void onLogStreamDeleted() {
        notifyLogStreamDeleted();
    }

    //
    // Listener for log segments
    //

    protected void registerListener(@Nullable LogSegmentListener listener) {
        if (null != listener) {
            listeners.add(listener);
        }
    }

    protected void unregisterListener(@Nullable LogSegmentListener listener) {
        if (null != listener) {
            listeners.remove(listener);
        }
    }

    protected void notifyUpdatedLogSegments(List<LogSegmentMetadata> segments) {
        if (logSegmentsNotificationDisabled) {
            return;
        }

        for (LogSegmentListener listener : listeners) {
            List<LogSegmentMetadata> listToReturn =
                    new ArrayList<LogSegmentMetadata>(segments);
            Collections.sort(listToReturn, LogSegmentMetadata.COMPARATOR);
            listener.onSegmentsUpdated(listToReturn);
        }
    }

    protected void notifyLogStreamDeleted() {
        if (logSegmentsNotificationDisabled) {
            return;
        }

        for (LogSegmentListener listener : listeners) {
            listener.onLogStreamDeleted();
        }
    }

    // notify the errors
    protected void notifyReaderOnError(Throwable cause) {
        if (null != readerStateNotification) {
            readerStateNotification.notifyOnError(cause);
        }
    }
}
