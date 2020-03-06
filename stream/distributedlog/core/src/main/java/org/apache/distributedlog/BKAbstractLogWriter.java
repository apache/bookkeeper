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
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.bk.LedgerMetadata;
import org.apache.distributedlog.common.util.PermitManager;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.AlreadyClosedException;
import org.apache.distributedlog.exceptions.LockingException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.io.Abortable;
import org.apache.distributedlog.io.Abortables;
import org.apache.distributedlog.io.AsyncAbortable;
import org.apache.distributedlog.io.AsyncCloseable;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract class BKAbstractLogWriter implements Closeable, AsyncCloseable, Abortable, AsyncAbortable {
    static final Logger LOG = LoggerFactory.getLogger(BKAbstractLogWriter.class);

    protected final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;
    protected final BKDistributedLogManager bkDistributedLogManager;

    // States
    private CompletableFuture<Void> closePromise = null;
    private volatile boolean forceRolling = false;
    private boolean forceRecovery = false;

    // Truncation Related
    private CompletableFuture<List<LogSegmentMetadata>> lastTruncationAttempt = null;
    @VisibleForTesting
    private Long minTimestampToKeepOverride = null;

    // Log Segment Writers
    protected BKLogSegmentWriter segmentWriter = null;
    protected CompletableFuture<BKLogSegmentWriter> segmentWriterFuture = null;
    protected BKLogSegmentWriter allocatedSegmentWriter = null;
    protected BKLogWriteHandler writeHandler = null;

    BKAbstractLogWriter(DistributedLogConfiguration conf,
                        DynamicDistributedLogConfiguration dynConf,
                        BKDistributedLogManager bkdlm) {
        this.conf = conf;
        this.dynConf = dynConf;
        this.bkDistributedLogManager = bkdlm;
        LOG.debug("Initial retention period for {} : {}", bkdlm.getStreamName(),
                TimeUnit.MILLISECONDS.convert(dynConf.getRetentionPeriodHours(), TimeUnit.HOURS));
    }

    @VisibleForTesting
    CompletableFuture<List<LogSegmentMetadata>> getLastTruncationAttempt() {
        return lastTruncationAttempt;
    }

    // manage write handler

    protected synchronized BKLogWriteHandler getCachedWriteHandler() {
        return writeHandler;
    }

    protected BKLogWriteHandler getWriteHandler() throws IOException {
        BKLogWriteHandler writeHandler = createAndCacheWriteHandler();
        writeHandler.checkMetadataException();
        return writeHandler;
    }

    protected BKLogWriteHandler createAndCacheWriteHandler()
            throws IOException {
        return createAndCacheWriteHandler(null);
    }

    protected BKLogWriteHandler createAndCacheWriteHandler(LedgerMetadata ledgerMetadata)
            throws IOException {
        synchronized (this) {
            if (writeHandler != null) {
                return writeHandler;
            }
        }
        // This code path will be executed when the handler is not set or has been closed
        // due to forceRecovery during testing
        BKLogWriteHandler newHandler =
                Utils.ioResult(bkDistributedLogManager.asyncCreateWriteHandler(false, ledgerMetadata));
        boolean success = false;
        try {
            synchronized (this) {
                if (writeHandler == null) {
                    writeHandler = newHandler;
                    success = true;
                }
                return writeHandler;
            }
        } finally {
            if (!success) {
                newHandler.asyncAbort();
            }
        }
    }

    // manage log segment writers

    protected synchronized BKLogSegmentWriter getCachedLogWriter() {
        return segmentWriter;
    }

    protected synchronized CompletableFuture<BKLogSegmentWriter> getCachedLogWriterFuture() {
        return segmentWriterFuture;
    }

    protected synchronized void cacheLogWriter(BKLogSegmentWriter logWriter) {
        this.segmentWriter = logWriter;
        this.segmentWriterFuture = FutureUtils.value(logWriter);
    }

    protected synchronized BKLogSegmentWriter removeCachedLogWriter() {
        try {
            return segmentWriter;
        } finally {
            segmentWriter = null;
            segmentWriterFuture = null;
        }
    }

    protected synchronized BKLogSegmentWriter getAllocatedLogWriter() {
        return allocatedSegmentWriter;
    }

    protected synchronized void cacheAllocatedLogWriter(BKLogSegmentWriter logWriter) {
        this.allocatedSegmentWriter = logWriter;
    }

    protected synchronized BKLogSegmentWriter removeAllocatedLogWriter() {
        try {
            return allocatedSegmentWriter;
        } finally {
            allocatedSegmentWriter = null;
        }
    }

    private CompletableFuture<Void> asyncCloseAndComplete(boolean shouldThrow) {
        BKLogSegmentWriter segmentWriter = getCachedLogWriter();
        BKLogWriteHandler writeHandler = getCachedWriteHandler();
        if (null != segmentWriter && null != writeHandler) {
            cancelTruncation();
            CompletableFuture<Void> completePromise = new CompletableFuture<Void>();
            asyncCloseAndComplete(segmentWriter, writeHandler, completePromise, shouldThrow);
            return completePromise;
        } else {
            return closeNoThrow();
        }
    }

    private void asyncCloseAndComplete(final BKLogSegmentWriter segmentWriter,
                                       final BKLogWriteHandler writeHandler,
                                       final CompletableFuture<Void> completePromise,
                                       final boolean shouldThrow) {
        writeHandler.completeAndCloseLogSegment(segmentWriter)
                .whenComplete(new FutureEventListener<LogSegmentMetadata>() {
                    @Override
                    public void onSuccess(LogSegmentMetadata segment) {
                        removeCachedLogWriter();
                        complete(null);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        LOG.error("Completing Log segments encountered exception", cause);
                        complete(cause);
                    }

                    private void complete(final Throwable cause) {
                        FutureUtils.ensure(closeNoThrow(), () -> {
                            if (null != cause && shouldThrow) {
                                FutureUtils.completeExceptionally(completePromise, cause);
                            } else {
                                FutureUtils.complete(completePromise, null);
                            }
                        });
                    }
                });
    }

    @VisibleForTesting
    void closeAndComplete() throws IOException {
        Utils.ioResult(asyncCloseAndComplete(true));
    }

    protected CompletableFuture<Void> asyncCloseAndComplete() {
        return asyncCloseAndComplete(true);
    }

    @Override
    public void close() throws IOException {
        Utils.ioResult(asyncClose());
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return asyncCloseAndComplete(false);
    }

    /**
     * Close the writer and release all the underlying resources.
     */
    protected CompletableFuture<Void> closeNoThrow() {
        CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new CompletableFuture<Void>();
        }
        cancelTruncation();
        FutureUtils.proxyTo(
            Utils.closeSequence(bkDistributedLogManager.getScheduler(),
                    true, /** ignore close errors **/
                    getCachedLogWriter(),
                    getAllocatedLogWriter(),
                    getCachedWriteHandler()
            ),
            closeFuture);
        return closeFuture;
    }

    @Override
    public void abort() throws IOException {
        Utils.ioResult(asyncAbort());
    }

    @Override
    public CompletableFuture<Void> asyncAbort() {
        CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new CompletableFuture<Void>();
        }
        cancelTruncation();
        FutureUtils.proxyTo(
            Abortables.abortSequence(bkDistributedLogManager.getScheduler(),
                    getCachedLogWriter(),
                    getAllocatedLogWriter(),
                    getCachedWriteHandler()),
            closeFuture);
        return closeFuture;
    }

    // used by sync writer
    protected BKLogSegmentWriter getLedgerWriter(final long startTxId,
                                                 final boolean allowMaxTxID)
            throws IOException {
        CompletableFuture<BKLogSegmentWriter> logSegmentWriterFuture = asyncGetLedgerWriter(true);
        BKLogSegmentWriter logSegmentWriter = null;
        if (null != logSegmentWriterFuture) {
            logSegmentWriter = Utils.ioResult(logSegmentWriterFuture);
        }
        if (null == logSegmentWriter || (shouldStartNewSegment(logSegmentWriter) || forceRolling)) {
            logSegmentWriter = Utils.ioResult(rollLogSegmentIfNecessary(
                    logSegmentWriter, startTxId, true /* bestEffort */, allowMaxTxID));
        }
        return logSegmentWriter;
    }

    // used by async writer
    protected synchronized CompletableFuture<BKLogSegmentWriter> asyncGetLedgerWriter(boolean resetOnError) {
        final BKLogSegmentWriter ledgerWriter = getCachedLogWriter();
        CompletableFuture<BKLogSegmentWriter> ledgerWriterFuture = getCachedLogWriterFuture();
        if (null == ledgerWriterFuture || null == ledgerWriter) {
            return null;
        }

        // Handle the case where the last call to write actually caused an error in the log
        if ((ledgerWriter.isLogSegmentInError() || forceRecovery) && resetOnError) {
            // Close the ledger writer so that we will recover and start a new log segment
            CompletableFuture<Void> closeFuture;
            if (ledgerWriter.isLogSegmentInError()) {
                closeFuture = ledgerWriter.asyncAbort();
            } else {
                closeFuture = ledgerWriter.asyncClose();
            }
            return closeFuture.thenCompose(
                    new Function<Void, CompletionStage<BKLogSegmentWriter>>() {
                @Override
                public CompletableFuture<BKLogSegmentWriter> apply(Void result) {
                    removeCachedLogWriter();

                    if (ledgerWriter.isLogSegmentInError()) {
                        return FutureUtils.value(null);
                    }

                    BKLogWriteHandler writeHandler;
                    try {
                        writeHandler = getWriteHandler();
                    } catch (IOException e) {
                        return FutureUtils.exception(e);
                    }
                    if (null != writeHandler && forceRecovery) {
                        return writeHandler.completeAndCloseLogSegment(ledgerWriter)
                                .thenApply(new Function<LogSegmentMetadata, BKLogSegmentWriter>() {
                            @Override
                            public BKLogSegmentWriter apply(LogSegmentMetadata completedLogSegment) {
                                return null;
                            }
                        });
                    } else {
                        return FutureUtils.value(null);
                    }
                }
            });
        } else {
            return ledgerWriterFuture;
        }
    }

    boolean shouldStartNewSegment(BKLogSegmentWriter ledgerWriter) throws IOException {
        BKLogWriteHandler writeHandler = getWriteHandler();
        return null == ledgerWriter || writeHandler.shouldStartNewSegment(ledgerWriter) || forceRolling;
    }

    private void truncateLogSegmentsIfNecessary(BKLogWriteHandler writeHandler) {
        boolean truncationEnabled = false;

        long minTimestampToKeep = 0;

        long retentionPeriodInMillis = TimeUnit.MILLISECONDS.convert(dynConf.getRetentionPeriodHours(), TimeUnit.HOURS);
        if (retentionPeriodInMillis > 0) {
            minTimestampToKeep = Utils.nowInMillis() - retentionPeriodInMillis;
            truncationEnabled = true;
        }

        if (null != minTimestampToKeepOverride) {
            minTimestampToKeep = minTimestampToKeepOverride;
            truncationEnabled = true;
        }

        // skip scheduling if there is task that's already running
        //
        synchronized (this) {
            if (truncationEnabled && ((lastTruncationAttempt == null) || lastTruncationAttempt.isDone())) {
                lastTruncationAttempt = writeHandler.purgeLogSegmentsOlderThanTimestamp(minTimestampToKeep);
            }
        }
    }

    private CompletableFuture<BKLogSegmentWriter> asyncStartNewLogSegment(final BKLogWriteHandler writeHandler,
                                                               final long startTxId,
                                                               final boolean allowMaxTxID) {
        return writeHandler.recoverIncompleteLogSegments()
            .thenCompose(
                lastTxId -> writeHandler.asyncStartLogSegment(startTxId, false, allowMaxTxID)
                    .thenApply(newSegmentWriter -> {
                        cacheLogWriter(newSegmentWriter);
                        return newSegmentWriter;
                    }));
    }

    private CompletableFuture<BKLogSegmentWriter> closeOldLogSegmentAndStartNewOneWithPermit(
            final BKLogSegmentWriter oldSegmentWriter,
            final BKLogWriteHandler writeHandler,
            final long startTxId,
            final boolean bestEffort,
            final boolean allowMaxTxID) {
        final PermitManager.Permit switchPermit =
                bkDistributedLogManager.getLogSegmentRollingPermitManager().acquirePermit();
        if (switchPermit.isAllowed()) {
            return FutureUtils.ensure(
                FutureUtils.rescue(
                     closeOldLogSegmentAndStartNewOne(
                            oldSegmentWriter,
                            writeHandler,
                            startTxId,
                            bestEffort,
                            allowMaxTxID
                    ),
                    // rescue function
                    cause -> {
                        if (cause instanceof LockingException) {
                            LOG.warn("We lost lock during completeAndClose log segment for {}."
                                            + "Disable ledger rolling until it is recovered : ",
                                    writeHandler.getFullyQualifiedName(), cause);
                            bkDistributedLogManager.getLogSegmentRollingPermitManager()
                                    .disallowObtainPermits(switchPermit);
                            return FutureUtils.value(oldSegmentWriter);
                        } else if (cause instanceof ZKException) {
                            ZKException zke = (ZKException) cause;
                            if (ZKException.isRetryableZKException(zke)) {
                                LOG.warn("Encountered zookeeper connection issues during completeAndClose "
                                                + "log segment for {}. "
                                                + "Disable ledger rolling until it is recovered : {}",
                                        writeHandler.getFullyQualifiedName(),
                                        zke.getKeeperExceptionCode());
                                bkDistributedLogManager.getLogSegmentRollingPermitManager()
                                        .disallowObtainPermits(switchPermit);
                                return FutureUtils.value(oldSegmentWriter);
                            }
                        }
                        return FutureUtils.exception(cause);
                    }
                ),
                // ensure function
                () -> bkDistributedLogManager.getLogSegmentRollingPermitManager()
                                .releasePermit(switchPermit)
            );
        } else {
            bkDistributedLogManager.getLogSegmentRollingPermitManager().releasePermit(switchPermit);
            return FutureUtils.value(oldSegmentWriter);
        }
    }

    private CompletableFuture<BKLogSegmentWriter> closeOldLogSegmentAndStartNewOne(
            final BKLogSegmentWriter oldSegmentWriter,
            final BKLogWriteHandler writeHandler,
            final long startTxId,
            final boolean bestEffort,
            final boolean allowMaxTxID) {
        // we switch only when we could allocate a new log segment.
        BKLogSegmentWriter newSegmentWriter = getAllocatedLogWriter();
        if (null == newSegmentWriter) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Allocating a new log segment from {} for {}.", startTxId,
                        writeHandler.getFullyQualifiedName());
            }
            return writeHandler.asyncStartLogSegment(startTxId, bestEffort, allowMaxTxID)
                    .thenCompose(new Function<BKLogSegmentWriter, CompletableFuture<BKLogSegmentWriter>>() {
                        @Override
                        public CompletableFuture<BKLogSegmentWriter> apply(BKLogSegmentWriter newSegmentWriter) {
                            if (null == newSegmentWriter) {
                                if (bestEffort) {
                                    return FutureUtils.value(oldSegmentWriter);
                                } else {
                                    return FutureUtils.exception(
                                            new UnexpectedException("StartLogSegment returns "
                                                    + "null for bestEffort rolling"));
                                }
                            }
                            cacheAllocatedLogWriter(newSegmentWriter);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Allocated a new log segment from {} for {}.", startTxId,
                                        writeHandler.getFullyQualifiedName());
                            }
                            return completeOldSegmentAndCacheNewLogSegmentWriter(oldSegmentWriter, newSegmentWriter);
                        }
                    });
        } else {
            return completeOldSegmentAndCacheNewLogSegmentWriter(oldSegmentWriter, newSegmentWriter);
        }
    }

    private CompletableFuture<BKLogSegmentWriter> completeOldSegmentAndCacheNewLogSegmentWriter(
            BKLogSegmentWriter oldSegmentWriter,
            final BKLogSegmentWriter newSegmentWriter) {
        final CompletableFuture<BKLogSegmentWriter> completePromise = new CompletableFuture<BKLogSegmentWriter>();
        // complete the old log segment
        writeHandler.completeAndCloseLogSegment(oldSegmentWriter)
                .whenComplete(new FutureEventListener<LogSegmentMetadata>() {

                    @Override
                    public void onSuccess(LogSegmentMetadata value) {
                        cacheLogWriter(newSegmentWriter);
                        removeAllocatedLogWriter();
                        FutureUtils.complete(completePromise, newSegmentWriter);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        FutureUtils.completeExceptionally(completePromise, cause);
                    }
                });
        return completePromise;
    }

    protected synchronized CompletableFuture<BKLogSegmentWriter> rollLogSegmentIfNecessary(
            final BKLogSegmentWriter segmentWriter,
            long startTxId,
            boolean bestEffort,
            boolean allowMaxTxID) {
        final BKLogWriteHandler writeHandler;
        try {
            writeHandler = getWriteHandler();
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        CompletableFuture<BKLogSegmentWriter> rollPromise;
        if (null != segmentWriter && (writeHandler.shouldStartNewSegment(segmentWriter) || forceRolling)) {
            rollPromise = closeOldLogSegmentAndStartNewOneWithPermit(
                    segmentWriter, writeHandler, startTxId, bestEffort, allowMaxTxID);
        } else if (null == segmentWriter) {
            rollPromise = asyncStartNewLogSegment(writeHandler, startTxId, allowMaxTxID);
        } else {
            rollPromise = FutureUtils.value(segmentWriter);
        }
        return rollPromise.thenApply(new Function<BKLogSegmentWriter, BKLogSegmentWriter>() {
            @Override
            public BKLogSegmentWriter apply(BKLogSegmentWriter newSegmentWriter) {
                if (segmentWriter == newSegmentWriter) {
                    return newSegmentWriter;
                }
                truncateLogSegmentsIfNecessary(writeHandler);
                return newSegmentWriter;
            }
        });
    }

    protected synchronized void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (null != closePromise) {
            LOG.error("Executing " + operation + " on already closed Log Writer");
            throw new AlreadyClosedException("Executing " + operation + " on already closed Log Writer");
        }
    }

    @VisibleForTesting
    public void setForceRolling(boolean forceRolling) {
        this.forceRolling = forceRolling;
    }

    @VisibleForTesting
    public synchronized void overRideMinTimeStampToKeep(Long minTimestampToKeepOverride) {
        this.minTimestampToKeepOverride = minTimestampToKeepOverride;
    }

    protected synchronized void cancelTruncation() {
        if (null != lastTruncationAttempt) {
            lastTruncationAttempt.cancel(true);
            lastTruncationAttempt = null;
        }
    }

    @VisibleForTesting
    public synchronized void setForceRecovery(boolean forceRecovery) {
        this.forceRecovery = forceRecovery;
    }

}
