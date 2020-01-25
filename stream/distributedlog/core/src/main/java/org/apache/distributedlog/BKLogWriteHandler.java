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

import static org.apache.distributedlog.impl.ZKLogSegmentFilters.WRITE_HANDLE_FILTER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.common.util.PermitLimiter;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.DLIllegalStateException;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.apache.distributedlog.exceptions.LockingException;
import org.apache.distributedlog.exceptions.LogSegmentNotFoundException;
import org.apache.distributedlog.exceptions.TransactionIdOutOfOrderException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.function.GetLastTxIdFunction;
import org.apache.distributedlog.lock.DistributedLock;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.logsegment.LogSegmentEntryWriter;
import org.apache.distributedlog.logsegment.LogSegmentFilter;
import org.apache.distributedlog.logsegment.LogSegmentMetadataCache;
import org.apache.distributedlog.logsegment.RollingPolicy;
import org.apache.distributedlog.logsegment.SizeBasedRollingPolicy;
import org.apache.distributedlog.logsegment.TimeBasedRollingPolicy;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import org.apache.distributedlog.metadata.LogStreamMetadataStore;
import org.apache.distributedlog.metadata.MetadataUpdater;
import org.apache.distributedlog.util.Allocator;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.util.FailpointUtils;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log Handler for Writers.
 *
 * <h3>Metrics</h3>
 * All the metrics about log write handler are exposed under scope `segments`.
 * <ul>
 * <li> `segments`/open : opstats. latency characteristics on starting a new log segment.
 * <li> `segments`/close : opstats. latency characteristics on completing an inprogress log segment.
 * <li> `segments`/recover : opstats. latency characteristics on recovering a log segment.
 * <li> `segments`/delete : opstats. latency characteristics on deleting a log segment.
 * </ul>
 */
class BKLogWriteHandler extends BKLogHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogWriteHandler.class);

    private static Transaction.OpListener<LogSegmentEntryWriter> NULL_OP_LISTENER =
            new Transaction.OpListener<LogSegmentEntryWriter>() {
        @Override
        public void onCommit(LogSegmentEntryWriter r) {
            // no-op
        }

        @Override
        public void onAbort(Throwable t) {
            // no-op
        }
    };

    protected final LogMetadataForWriter logMetadataForWriter;
    protected final Allocator<LogSegmentEntryWriter, Object> logSegmentAllocator;
    protected final DistributedLock lock;
    protected final MaxTxId maxTxId;
    protected final MaxLogSegmentSequenceNo maxLogSegmentSequenceNo;
    protected final boolean validateLogSegmentSequenceNumber;
    protected final int regionId;
    protected final RollingPolicy rollingPolicy;
    protected CompletableFuture<? extends DistributedLock> lockFuture = null;
    protected final PermitLimiter writeLimiter;
    protected final FeatureProvider featureProvider;
    protected final DynamicDistributedLogConfiguration dynConf;
    protected final MetadataUpdater metadataUpdater;
    // tracking the inprogress log segments
    protected final LinkedList<Long> inprogressLSSNs;

    // Fetch LogSegments State: write can continue without full list of log segments while truncation needs
    private final CompletableFuture<Versioned<List<LogSegmentMetadata>>> fetchForWrite;
    private CompletableFuture<Versioned<List<LogSegmentMetadata>>> fetchForTruncation;

    // Recover Functions
    private final RecoverLogSegmentFunction recoverLogSegmentFunction =
            new RecoverLogSegmentFunction();
    private final Function<List<LogSegmentMetadata>, CompletableFuture<Long>> recoverLogSegmentsFunction =
            new Function<List<LogSegmentMetadata>, CompletableFuture<Long>>() {
                @Override
                public CompletableFuture<Long> apply(List<LogSegmentMetadata> segmentList) {
                    LOG.info("Initiating Recovery For {} : {}", getFullyQualifiedName(), segmentList);
                    // if lastLedgerRollingTimeMillis is not updated, we set it to now.
                    synchronized (BKLogWriteHandler.this) {
                        if (lastLedgerRollingTimeMillis < 0) {
                            lastLedgerRollingTimeMillis = Utils.nowInMillis();
                        }
                    }

                    if (validateLogSegmentSequenceNumber) {
                        synchronized (inprogressLSSNs) {
                            for (LogSegmentMetadata segment : segmentList) {
                                if (segment.isInProgress()) {
                                    inprogressLSSNs.addLast(segment.getLogSegmentSequenceNumber());
                                }
                            }
                        }
                    }

                    return FutureUtils.processList(
                        segmentList,
                        recoverLogSegmentFunction,
                        scheduler
                    ).thenApply(removeEmptySegments)
                     .thenApply(GetLastTxIdFunction.INSTANCE);
                }
            };
    private final Function<List<LogSegmentMetadata>, List<LogSegmentMetadata>> removeEmptySegments =
        new Function<List<LogSegmentMetadata>, List<LogSegmentMetadata>>() {
            @Override
            public List<LogSegmentMetadata> apply(List<LogSegmentMetadata> segmentList) {
                Iterator<LogSegmentMetadata> iter = segmentList.iterator();
                while (iter.hasNext()) {
                    LogSegmentMetadata segment = iter.next();
                    if (segment == null) {
                        iter.remove();
                    }
                }
                return segmentList;
            }
        };

    // Stats
    private final StatsLogger perLogStatsLogger;
    private final OpStatsLogger closeOpStats;
    private final OpStatsLogger openOpStats;
    private final OpStatsLogger recoverOpStats;
    private final OpStatsLogger deleteOpStats;

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogWriteHandler(LogMetadataForWriter logMetadata,
                      DistributedLogConfiguration conf,
                      LogStreamMetadataStore streamMetadataStore,
                      LogSegmentMetadataCache metadataCache,
                      LogSegmentEntryStore entryStore,
                      OrderedScheduler scheduler,
                      Allocator<LogSegmentEntryWriter, Object> segmentAllocator,
                      StatsLogger statsLogger,
                      StatsLogger perLogStatsLogger,
                      AlertStatsLogger alertStatsLogger,
                      String clientId,
                      int regionId,
                      PermitLimiter writeLimiter,
                      FeatureProvider featureProvider,
                      DynamicDistributedLogConfiguration dynConf,
                      DistributedLock lock /** owned by handler **/) {
        super(logMetadata,
                conf,
                streamMetadataStore,
                metadataCache,
                entryStore,
                scheduler,
                statsLogger,
                alertStatsLogger,
                clientId);
        this.logMetadataForWriter = logMetadata;
        this.logSegmentAllocator = segmentAllocator;
        this.perLogStatsLogger = perLogStatsLogger;
        this.writeLimiter = writeLimiter;
        this.featureProvider = featureProvider;
        this.dynConf = dynConf;
        this.lock = lock;
        this.metadataUpdater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(conf, metadataStore);

        if (conf.getEncodeRegionIDInLogSegmentMetadata()) {
            this.regionId = regionId;
        } else {
            this.regionId = DistributedLogConstants.LOCAL_REGION_ID;
        }
        this.validateLogSegmentSequenceNumber = conf.isLogSegmentSequenceNumberValidationEnabled();

        // Construct the max sequence no
        maxLogSegmentSequenceNo = new MaxLogSegmentSequenceNo(logMetadata.getMaxLSSNData());
        inprogressLSSNs = new LinkedList<Long>();
        // Construct the max txn id.
        maxTxId = new MaxTxId(logMetadata.getMaxTxIdData());

        // Schedule fetching log segment list in background before we access it.
        // We don't need to watch the log segment list changes for writer, as it manages log segment list.
        fetchForWrite = readLogSegmentsFromStore(
                LogSegmentMetadata.COMPARATOR,
                WRITE_HANDLE_FILTER,
                null);

        // Initialize other parameters.
        setLastLedgerRollingTimeMillis(Utils.nowInMillis());

        // Rolling Policy
        if (conf.getLogSegmentRollingIntervalMinutes() > 0) {
            rollingPolicy = new TimeBasedRollingPolicy(conf.getLogSegmentRollingIntervalMinutes() * 60 * 1000L);
        } else {
            rollingPolicy = new SizeBasedRollingPolicy(conf.getMaxLogSegmentBytes());
        }

        // Stats
        StatsLogger segmentsStatsLogger = statsLogger.scope("segments");
        openOpStats = segmentsStatsLogger.getOpStatsLogger("open");
        closeOpStats = segmentsStatsLogger.getOpStatsLogger("close");
        recoverOpStats = segmentsStatsLogger.getOpStatsLogger("recover");
        deleteOpStats = segmentsStatsLogger.getOpStatsLogger("delete");
    }

    private CompletableFuture<List<LogSegmentMetadata>> getCachedLogSegmentsAfterFirstFetch(
            final Comparator<LogSegmentMetadata> comparator) {
        final CompletableFuture<List<LogSegmentMetadata>> promise = new CompletableFuture<List<LogSegmentMetadata>>();
        fetchForWrite.whenComplete(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {
            @Override
            public void onFailure(Throwable cause) {
                promise.completeExceptionally(cause);
            }

            @Override
            public void onSuccess(Versioned<List<LogSegmentMetadata>> result) {
                try {
                    promise.complete(getCachedLogSegments(comparator));
                } catch (UnexpectedException e) {
                    promise.completeExceptionally(e);
                }
            }
        });
        return promise;
    }

    private CompletableFuture<List<LogSegmentMetadata>> getCachedLogSegmentsAfterFirstFullFetch(
            final Comparator<LogSegmentMetadata> comparator) {
        CompletableFuture<Versioned<List<LogSegmentMetadata>>> result;
        synchronized (this) {
            if (null == fetchForTruncation) {
                fetchForTruncation = readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null);
            }
            result = fetchForTruncation;
        }

        final CompletableFuture<List<LogSegmentMetadata>> promise = new CompletableFuture<List<LogSegmentMetadata>>();
        result.whenComplete(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {
            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(promise, cause);
            }

            @Override
            public void onSuccess(Versioned<List<LogSegmentMetadata>> result) {
                try {
                    FutureUtils.complete(promise, getCachedLogSegments(comparator));
                } catch (UnexpectedException e) {
                    FutureUtils.completeExceptionally(promise, e);
                }
            }
        });
        return promise;
    }

    // Transactional operations for MaxLogSegmentSequenceNo
    void storeMaxSequenceNumber(final Transaction<Object> txn,
                                final MaxLogSegmentSequenceNo maxSeqNo,
                                final long seqNo,
                                final boolean isInprogress) {
        metadataStore.storeMaxLogSegmentSequenceNumber(txn, logMetadata, maxSeqNo.getVersionedData(seqNo),
                new Transaction.OpListener<Version>() {
            @Override
            public void onCommit(Version version) {
                if (validateLogSegmentSequenceNumber) {
                    synchronized (inprogressLSSNs) {
                        if (isInprogress) {
                            inprogressLSSNs.add(seqNo);
                        } else {
                            inprogressLSSNs.removeFirst();
                        }
                    }
                }
                maxSeqNo.update(version, seqNo);
            }

            @Override
            public void onAbort(Throwable t) {
                // no-op
            }
        });
    }

    // Transactional operations for MaxTxId
    void storeMaxTxId(final Transaction<Object> txn,
                      final MaxTxId maxTxId,
                      final long txId) {
        metadataStore.storeMaxTxnId(txn, logMetadataForWriter, maxTxId.getVersionedData(txId),
                new Transaction.OpListener<Version>() {
                    @Override
                    public void onCommit(Version version) {
                                                        maxTxId.update(version, txId);
                                                                                      }

                    @Override
                    public void onAbort(Throwable t) {
                        // no-op
                    }
                });
    }

    // Transactional operations for logsegment
    void writeLogSegment(final Transaction<Object> txn,
                         final LogSegmentMetadata metadata) {
        metadataStore.createLogSegment(txn, metadata, new Transaction.OpListener<Void>() {
            @Override
            public void onCommit(Void r) {
                addLogSegmentToCache(metadata.getSegmentName(), metadata);
            }

            @Override
            public void onAbort(Throwable t) {
                // no-op
            }
        });
    }

    void deleteLogSegment(final Transaction<Object> txn,
                          final LogSegmentMetadata metadata) {
        metadataStore.deleteLogSegment(txn, metadata, new Transaction.OpListener<Void>() {
            @Override
            public void onCommit(Void r) {
                removeLogSegmentFromCache(metadata.getSegmentName());
            }

            @Override
            public void onAbort(Throwable t) {
                // no-op
            }
        });
    }

    /**
     * Delete the whole log and all log segments under the log.
     */
    void deleteLog() throws IOException {
        lock.checkOwnershipAndReacquire();
        Utils.ioResult(purgeLogSegmentsOlderThanTxnId(-1));
        Utils.closeQuietly(lock);
    }

    /**
     * The caller could call this before any actions, which to hold the lock for
     * the write handler of its whole lifecycle. The lock will only be released
     * when closing the write handler.
     * This method is useful to prevent releasing underlying zookeeper lock during
     * recovering/completing log segments. Releasing underlying zookeeper lock means
     * 1) increase latency when re-lock on starting new log segment. 2) increase the
     * possibility of a stream being re-acquired by other instances.
     *
     * @return future represents the lock result
     */
    CompletableFuture<? extends DistributedLock> lockHandler() {
        if (null != lockFuture) {
            return lockFuture;
        }
        lockFuture = lock.asyncAcquire();
        return lockFuture;
    }

    CompletableFuture<Void> unlockHandler() {
        if (null != lockFuture) {
            return lock.asyncClose();
        } else {
            return FutureUtils.Void();
        }
    }

    /**
     * Start a new log segment in a BookKeeper ledger.
     * First ensure that we have the write lock for this journal.
     * Then create a ledger and stream based on that ledger.
     * The ledger id is written to the inprogress znode, so that in the
     * case of a crash, a recovery process can find the ledger we were writing
     * to when we crashed.
     *
     * @param txId First transaction id to be written to the stream
     * @return
     * @throws IOException
     */
    public BKLogSegmentWriter startLogSegment(long txId) throws IOException {
        return startLogSegment(txId, false, false);
    }

    /**
     * Start a new log segment in a BookKeeper ledger.
     * First ensure that we have the write lock for this journal.
     * Then create a ledger and stream based on that ledger.
     * The ledger id is written to the inprogress znode, so that in the
     * case of a crash, a recovery process can find the ledger we were writing
     * to when we crashed.
     *
     * @param txId First transaction id to be written to the stream
     * @param bestEffort
     * @param allowMaxTxID
     *          allow using max tx id to start log segment
     * @return
     * @throws IOException
     */
    public BKLogSegmentWriter startLogSegment(long txId, boolean bestEffort, boolean allowMaxTxID)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            BKLogSegmentWriter writer = doStartLogSegment(txId, bestEffort, allowMaxTxID);
            success = true;
            return writer;
        } finally {
            if (success) {
                openOpStats.registerSuccessfulEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
            } else {
                openOpStats.registerFailedEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS), TimeUnit.MICROSECONDS);
            }
        }
    }

    protected long assignLogSegmentSequenceNumber() throws IOException {
        // For any active stream we will always make sure that there is at least one
        // active ledger (except when the stream first starts out). Therefore when we
        // see no ledger metadata for a stream, we assume that this is the first ledger
        // in the stream
        long logSegmentSeqNo = DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO;
        boolean logSegmentsFound = false;

        if (LogSegmentMetadata.supportsLogSegmentSequenceNo(conf.getDLLedgerMetadataLayoutVersion())) {
            List<LogSegmentMetadata> ledgerListDesc = getCachedLogSegments(LogSegmentMetadata.DESC_COMPARATOR);
            Long nextLogSegmentSeqNo = DLUtils.nextLogSegmentSequenceNumber(ledgerListDesc);

            if (null == nextLogSegmentSeqNo) {
                logSegmentsFound = false;
                // we don't find last assigned log segment sequence number
                // then we start the log segment with configured FirstLogSegmentSequenceNumber.
                logSegmentSeqNo = conf.getFirstLogSegmentSequenceNumber();
            } else {
                logSegmentsFound = true;
                // latest log segment is assigned with a sequence number, start with next sequence number
                logSegmentSeqNo = nextLogSegmentSeqNo;
            }
        }

        // We only skip log segment sequence number validation only when no log segments found &
        // the maximum log segment sequence number is "UNASSIGNED".
        if (!logSegmentsFound
                && (DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO
                == maxLogSegmentSequenceNo.getSequenceNumber())) {
            // no ledger seqno stored in /ledgers before
            LOG.info("No max ledger sequence number found while creating log segment {} for {}.",
                logSegmentSeqNo, getFullyQualifiedName());
        } else if (maxLogSegmentSequenceNo.getSequenceNumber() + 1 != logSegmentSeqNo // case 1
                   && maxLogSegmentSequenceNo.getSequenceNumber() != logSegmentSeqNo) { // case 2
            // case 1 is the common case, where the new log segment is 1 more than the previous
            // case 2 can occur when the writer crashes with an empty in progress ledger. This is then deleted
            //        on recovery, so the next new segment will have a matching sequence number
            LOG.warn("Unexpected max log segment sequence number {} for {} : list of cached segments = {}",
                maxLogSegmentSequenceNo.getSequenceNumber(), getFullyQualifiedName(),
                getCachedLogSegments(LogSegmentMetadata.DESC_COMPARATOR));
            // there is max log segment number recorded there and it isn't match. throw exception.
            throw new DLIllegalStateException("Unexpected max log segment sequence number "
                + maxLogSegmentSequenceNo.getSequenceNumber() + " for " + getFullyQualifiedName()
                + ", expected " + (logSegmentSeqNo - 1));
        }

        return logSegmentSeqNo;
    }

    protected BKLogSegmentWriter doStartLogSegment(long txId,
                                                   boolean bestEffort, boolean allowMaxTxID) throws IOException {
        return Utils.ioResult(asyncStartLogSegment(txId, bestEffort, allowMaxTxID));
    }

    protected CompletableFuture<BKLogSegmentWriter> asyncStartLogSegment(final long txId,
                                                              final boolean bestEffort,
                                                              final boolean allowMaxTxID) {
        final CompletableFuture<BKLogSegmentWriter> promise = new CompletableFuture<BKLogSegmentWriter>();
        try {
            lock.checkOwnershipAndReacquire();
        } catch (LockingException e) {
            FutureUtils.completeExceptionally(promise, e);
            return promise;
        }
        fetchForWrite.whenComplete(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {
            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(promise, cause);
            }

            @Override
            public void onSuccess(Versioned<List<LogSegmentMetadata>> list) {
                doStartLogSegment(txId, bestEffort, allowMaxTxID, promise);
            }
        });
        return promise;
    }

    protected void doStartLogSegment(final long txId,
                                     final boolean bestEffort,
                                     final boolean allowMaxTxID,
                                     final CompletableFuture<BKLogSegmentWriter> promise) {
        // validate the tx id
        if ((txId < 0)
                || (!allowMaxTxID && (txId == DistributedLogConstants.MAX_TXID))) {
            FutureUtils.completeExceptionally(promise, new IOException("Invalid Transaction Id " + txId));
            return;
        }

        long highestTxIdWritten = maxTxId.get();
        if (txId < highestTxIdWritten) {
            if (highestTxIdWritten == DistributedLogConstants.MAX_TXID) {
                LOG.error("We've already marked the stream as ended and attempting to start a new log segment");
                FutureUtils.completeExceptionally(promise,
                        new EndOfStreamException("Writing to a stream after it has been marked as completed"));
                return;
            } else {
                LOG.error("We've already seen TxId {} the max TXId is {}", txId, highestTxIdWritten);
                FutureUtils.completeExceptionally(promise,
                        new TransactionIdOutOfOrderException(txId, highestTxIdWritten));
                return;
            }
        }

        try {
            logSegmentAllocator.allocate();
        } catch (IOException e) {
            // failed to issue an allocation request
            failStartLogSegment(promise, bestEffort, e);
            return;
        }

        // start the transaction from zookeeper
        final Transaction<Object> txn = streamMetadataStore.newTransaction();

        // failpoint injected before creating ledger
        try {
            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentBeforeLedgerCreate);
        } catch (IOException ioe) {
            failStartLogSegment(promise, bestEffort, ioe);
            return;
        }

        logSegmentAllocator.tryObtain(txn, NULL_OP_LISTENER)
                .whenComplete(new FutureEventListener<LogSegmentEntryWriter>() {

            @Override
            public void onSuccess(LogSegmentEntryWriter entryWriter) {
                // try-obtain succeed
                createInprogressLogSegment(
                        txn,
                        txId,
                        entryWriter,
                        bestEffort,
                        promise);
            }

            @Override
            public void onFailure(Throwable cause) {
                failStartLogSegment(promise, bestEffort, cause);
            }
        });
    }

    private void failStartLogSegment(CompletableFuture<BKLogSegmentWriter> promise,
                                     boolean bestEffort,
                                     Throwable cause) {
        if (bestEffort) {
            FutureUtils.complete(promise, null);
        } else {
            FutureUtils.completeExceptionally(promise, cause);
        }
    }

    // once the ledger handle is obtained from allocator, this function should guarantee
    // either the transaction is executed or aborted. Otherwise, the ledger handle will
    // just leak from the allocation pool - hence cause "No Ledger Allocator"
    private void createInprogressLogSegment(Transaction<Object> txn,
                                            final long txId,
                                            final LogSegmentEntryWriter entryWriter,
                                            boolean bestEffort,
                                            final CompletableFuture<BKLogSegmentWriter> promise) {
        final long logSegmentSeqNo;
        try {
            FailpointUtils.checkFailPoint(
                    FailpointUtils.FailPointName.FP_StartLogSegmentOnAssignLogSegmentSequenceNumber);
            logSegmentSeqNo = assignLogSegmentSequenceNumber();
        } catch (IOException e) {
            // abort the current prepared transaction
            txn.abort(e);
            failStartLogSegment(promise, bestEffort, e);
            return;
        }

        final String inprogressZnodePath = inprogressZNode(
                entryWriter.getLogSegmentId(), txId, logSegmentSeqNo);
        final LogSegmentMetadata l =
            new LogSegmentMetadata.LogSegmentMetadataBuilder(inprogressZnodePath,
                conf.getDLLedgerMetadataLayoutVersion(), entryWriter.getLogSegmentId(), txId)
                    .setLogSegmentSequenceNo(logSegmentSeqNo)
                    .setRegionId(regionId)
                    .setEnvelopeEntries(
                            LogSegmentMetadata.supportsEnvelopedEntries(conf.getDLLedgerMetadataLayoutVersion()))
                    .build();

        // Create an inprogress segment
        writeLogSegment(txn, l);

        // Try storing max sequence number.
        LOG.debug("Try storing max sequence number in startLogSegment {} : {}", inprogressZnodePath, logSegmentSeqNo);
        storeMaxSequenceNumber(txn, maxLogSegmentSequenceNo, logSegmentSeqNo, true);

        txn.execute().whenCompleteAsync(new FutureEventListener<Void>() {

            @Override
            public void onSuccess(Void value) {
                try {
                    FutureUtils.complete(promise, new BKLogSegmentWriter(
                            getFullyQualifiedName(),
                            l.getSegmentName(),
                            conf,
                            conf.getDLLedgerMetadataLayoutVersion(),
                            entryWriter,
                            lock,
                            txId,
                            logSegmentSeqNo,
                            scheduler,
                            statsLogger,
                            perLogStatsLogger,
                            alertStatsLogger,
                            writeLimiter,
                            featureProvider,
                            dynConf));
                } catch (IOException ioe) {
                    failStartLogSegment(promise, false, ioe);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                failStartLogSegment(promise, false, cause);
            }
        }, scheduler);
    }

    boolean shouldStartNewSegment(BKLogSegmentWriter writer) {
        return rollingPolicy.shouldRollover(writer, lastLedgerRollingTimeMillis);
    }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     * <p/>
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
    CompletableFuture<LogSegmentMetadata> completeAndCloseLogSegment(final BKLogSegmentWriter writer) {
        final CompletableFuture<LogSegmentMetadata> promise = new CompletableFuture<LogSegmentMetadata>();
        completeAndCloseLogSegment(writer, promise);
        return promise;
    }

    private void completeAndCloseLogSegment(final BKLogSegmentWriter writer,
                                            final CompletableFuture<LogSegmentMetadata> promise) {
        writer.asyncClose().whenComplete(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                // in theory closeToFinalize should throw exception if a stream is in error.
                // just in case, add another checking here to make sure
                // we don't close log segment is a stream is in error.
                if (writer.shouldFailCompleteLogSegment()) {
                    FutureUtils.completeExceptionally(promise,
                            new IOException("LogSegmentWriter for " + writer.getFullyQualifiedLogSegment()
                                    + " is already in error."));
                    return;
                }
                doCompleteAndCloseLogSegment(
                        inprogressZNodeName(writer.getLogSegmentId(), writer.getStartTxId(),
                                writer.getLogSegmentSequenceNumber()),
                        writer.getLogSegmentSequenceNumber(),
                        writer.getLogSegmentId(),
                        writer.getStartTxId(),
                        writer.getLastTxId(),
                        writer.getPositionWithinLogSegment(),
                        writer.getLastDLSN().getEntryId(),
                        writer.getLastDLSN().getSlotId(),
                        promise);
            }

            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(promise, cause);
            }
        });
    }

    @VisibleForTesting
    LogSegmentMetadata completeAndCloseLogSegment(long logSegmentSeqNo,
                                                  long logSegmentId,
                                                  long firstTxId,
                                                  long lastTxId,
                                                  int recordCount)
        throws IOException {
        return completeAndCloseLogSegment(inprogressZNodeName(logSegmentId, firstTxId, logSegmentSeqNo),
                logSegmentSeqNo, logSegmentId, firstTxId, lastTxId, recordCount, -1, -1);
    }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     * <p/>
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
    LogSegmentMetadata completeAndCloseLogSegment(String inprogressZnodeName, long logSegmentSeqNo,
                                                  long logSegmentId, long firstTxId, long lastTxId,
                                                  int recordCount, long lastEntryId, long lastSlotId)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            LogSegmentMetadata completedLogSegment =
                    doCompleteAndCloseLogSegment(inprogressZnodeName, logSegmentSeqNo,
                            logSegmentId, firstTxId, lastTxId, recordCount,
                            lastEntryId, lastSlotId);
            success = true;
            return completedLogSegment;
        } finally {
            if (success) {
                closeOpStats.registerSuccessfulEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS),
                    TimeUnit.MICROSECONDS);
            } else {
                closeOpStats.registerFailedEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS),
                    TimeUnit.MICROSECONDS);
            }
        }
    }

    protected long computeStartSequenceId(LogSegmentMetadata segment) throws IOException {
        if (!segment.isInProgress()) {
            return segment.getStartSequenceId();
        }

        long startSequenceId = DistributedLogConstants.UNASSIGNED_SEQUENCE_ID;

        // we only record sequence id when both write version and logsegment's version support sequence id
        if (LogSegmentMetadata.supportsSequenceId(conf.getDLLedgerMetadataLayoutVersion())
                && segment.supportsSequenceId()) {
            List<LogSegmentMetadata> logSegmentDescList =
                    getCachedLogSegments(LogSegmentMetadata.DESC_COMPARATOR);
            startSequenceId = DLUtils.computeStartSequenceId(logSegmentDescList, segment);
        }

        return startSequenceId;
    }

    /**
     * Close log segment.
     *
     * @param inprogressZnodeName
     * @param logSegmentSeqNo
     * @param logSegmentId
     * @param firstTxId
     * @param lastTxId
     * @param recordCount
     * @param lastEntryId
     * @param lastSlotId
     * @throws IOException
     */
    protected LogSegmentMetadata doCompleteAndCloseLogSegment(
            String inprogressZnodeName,
            long logSegmentSeqNo,
            long logSegmentId,
            long firstTxId,
            long lastTxId,
            int recordCount,
            long lastEntryId,
            long lastSlotId) throws IOException {
        CompletableFuture<LogSegmentMetadata> promise = new CompletableFuture<LogSegmentMetadata>();
        doCompleteAndCloseLogSegment(
                inprogressZnodeName,
                logSegmentSeqNo,
                logSegmentId,
                firstTxId,
                lastTxId,
                recordCount,
                lastEntryId,
                lastSlotId,
                promise);
        return Utils.ioResult(promise);
    }

    protected void doCompleteAndCloseLogSegment(final String inprogressZnodeName,
                                                final long logSegmentSeqNo,
                                                final long logSegmentId,
                                                final long firstTxId,
                                                final long lastTxId,
                                                final int recordCount,
                                                final long lastEntryId,
                                                final long lastSlotId,
                                                final CompletableFuture<LogSegmentMetadata> promise) {
        fetchForWrite.whenComplete(new FutureEventListener<Versioned<List<LogSegmentMetadata>>>() {
            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(promise, cause);
            }

            @Override
            public void onSuccess(Versioned<List<LogSegmentMetadata>> segments) {
                doCompleteAndCloseLogSegmentAfterLogSegmentListFetched(
                        inprogressZnodeName,
                        logSegmentSeqNo,
                        logSegmentId,
                        firstTxId,
                        lastTxId,
                        recordCount,
                        lastEntryId,
                        lastSlotId,
                        promise);
            }
        });
    }

    private void doCompleteAndCloseLogSegmentAfterLogSegmentListFetched(
            final String inprogressZnodeName,
            long logSegmentSeqNo,
            long logSegmentId,
            long firstTxId,
            long lastTxId,
            int recordCount,
            long lastEntryId,
            long lastSlotId,
            final CompletableFuture<LogSegmentMetadata> promise) {
        try {
            lock.checkOwnershipAndReacquire();
        } catch (IOException ioe) {
            FutureUtils.completeExceptionally(promise, ioe);
            return;
        }

        LOG.debug("Completing and Closing Log Segment {} {}", firstTxId, lastTxId);
        LogSegmentMetadata inprogressLogSegment = readLogSegmentFromCache(inprogressZnodeName);

        // validate log segment
        if (inprogressLogSegment.getLogSegmentId() != logSegmentId) {
            FutureUtils.completeExceptionally(promise, new IOException(
                "Active ledger has different ID to inprogress. "
                    + inprogressLogSegment.getLogSegmentId() + " found, "
                    + logSegmentId + " expected"));
            return;
        }
        // validate the transaction id
        if (inprogressLogSegment.getFirstTxId() != firstTxId) {
            FutureUtils.completeExceptionally(promise, new IOException("Transaction id not as expected, "
                + inprogressLogSegment.getFirstTxId() + " found, " + firstTxId + " expected"));
            return;
        }
        // validate the log sequence number
        if (validateLogSegmentSequenceNumber) {
            synchronized (inprogressLSSNs) {
                if (inprogressLSSNs.isEmpty()) {
                    FutureUtils.completeExceptionally(promise, new UnexpectedException(
                            "Didn't find matched inprogress log segments when completing inprogress "
                                    + inprogressLogSegment));
                    return;
                }
                long leastInprogressLSSN = inprogressLSSNs.getFirst();
                // the log segment sequence number in metadata
                // {@link inprogressLogSegment.getLogSegmentSequenceNumber()}
                // should be same as the sequence number we are completing (logSegmentSeqNo)
                // and
                // it should also be same as the least inprogress log segment sequence number
                // tracked in {@link inprogressLSSNs}
                if ((inprogressLogSegment.getLogSegmentSequenceNumber() != logSegmentSeqNo)
                        || (leastInprogressLSSN != logSegmentSeqNo)) {
                    FutureUtils.completeExceptionally(promise, new UnexpectedException(
                            "Didn't find matched inprogress log segments when completing inprogress "
                                    + inprogressLogSegment));
                    return;
                }
            }
        }

        // store max sequence number.
        long maxSeqNo = Math.max(logSegmentSeqNo, maxLogSegmentSequenceNo.getSequenceNumber());
        if (maxLogSegmentSequenceNo.getSequenceNumber() == logSegmentSeqNo
                || (maxLogSegmentSequenceNo.getSequenceNumber() == logSegmentSeqNo + 1)) {
            // ignore the case that a new inprogress log segment is pre-allocated
            // before completing current inprogress one
            LOG.info("Try storing max sequence number {} in completing {}.",
                    new Object[] { logSegmentSeqNo, inprogressLogSegment.getZkPath() });
        } else {
            LOG.warn("Unexpected max ledger sequence number {} found while completing log segment {} for {}",
                maxLogSegmentSequenceNo.getSequenceNumber(), logSegmentSeqNo, getFullyQualifiedName());
            if (validateLogSegmentSequenceNumber) {
                FutureUtils.completeExceptionally(promise,
                        new DLIllegalStateException("Unexpected max log segment sequence number "
                        + maxLogSegmentSequenceNo.getSequenceNumber() + " for " + getFullyQualifiedName()
                        + ", expected " + (logSegmentSeqNo - 1)));
                return;
            }
        }

        // Prepare the completion
        final String pathForCompletedLedger = completedLedgerZNode(firstTxId, lastTxId, logSegmentSeqNo);
        long startSequenceId;
        try {
            startSequenceId = computeStartSequenceId(inprogressLogSegment);
        } catch (IOException ioe) {
            FutureUtils.completeExceptionally(promise, ioe);
            return;
        }
        // write completed ledger znode
        final LogSegmentMetadata completedLogSegment =
                inprogressLogSegment.completeLogSegment(
                        pathForCompletedLedger,
                        lastTxId,
                        recordCount,
                        lastEntryId,
                        lastSlotId,
                        startSequenceId);
        setLastLedgerRollingTimeMillis(completedLogSegment.getCompletionTime());

        // prepare the transaction
        Transaction<Object> txn = streamMetadataStore.newTransaction();

        // create completed log segment
        writeLogSegment(txn, completedLogSegment);
        // delete inprogress log segment
        deleteLogSegment(txn, inprogressLogSegment);
        // store max sequence number
        storeMaxSequenceNumber(txn, maxLogSegmentSequenceNo, maxSeqNo, false);
        // update max txn id.
        LOG.debug("Trying storing LastTxId in Finalize Path {} LastTxId {}", pathForCompletedLedger, lastTxId);
        storeMaxTxId(txn, maxTxId, lastTxId);

        txn.execute().whenCompleteAsync(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                LOG.info("Completed {} to {} for {} : {}",
                    inprogressZnodeName, completedLogSegment.getSegmentName(),
                    getFullyQualifiedName(), completedLogSegment);
                FutureUtils.complete(promise, completedLogSegment);
            }

            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(promise, cause);
            }
        }, scheduler);
    }

    public CompletableFuture<Long> recoverIncompleteLogSegments() {
        try {
            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_RecoverIncompleteLogSegments);
        } catch (IOException ioe) {
            return FutureUtils.exception(ioe);
        }
        return getCachedLogSegmentsAfterFirstFetch(LogSegmentMetadata.COMPARATOR)
                .thenCompose(recoverLogSegmentsFunction);
    }

    class RecoverLogSegmentFunction implements Function<LogSegmentMetadata, CompletableFuture<LogSegmentMetadata>> {

        @Override
        public CompletableFuture<LogSegmentMetadata> apply(final LogSegmentMetadata l) {
            if (!l.isInProgress()) {
                return FutureUtils.value(l);
            }

            LOG.info("Recovering last record in log segment {} for {}.", l, getFullyQualifiedName());
            return asyncReadLastRecord(l, true, true, true).thenCompose(
                lastRecord -> completeLogSegment(l, lastRecord));
        }

        private CompletableFuture<LogSegmentMetadata> completeLogSegment(LogSegmentMetadata l,
                                                              LogRecordWithDLSN lastRecord) {
            LOG.info("Recovered last record in log segment {} for {}.", l, getFullyQualifiedName());

            long endTxId = DistributedLogConstants.EMPTY_LOGSEGMENT_TX_ID;
            int recordCount = 0;
            long lastEntryId = -1;
            long lastSlotId = -1;

            if (null != lastRecord) {
                endTxId = lastRecord.getTransactionId();
                recordCount = lastRecord.getLastPositionWithinLogSegment();
                lastEntryId = lastRecord.getDlsn().getEntryId();
                lastSlotId = lastRecord.getDlsn().getSlotId();
            }

            if (endTxId == DistributedLogConstants.INVALID_TXID) {
                LOG.error("Unrecoverable corruption has occurred in segment "
                    + l.toString() + " at path " + l.getZkPath()
                    + ". Unable to continue recovery.");
                return FutureUtils.exception(new IOException("Unrecoverable corruption,"
                    + " please check logs."));
            } else if (endTxId == DistributedLogConstants.EMPTY_LOGSEGMENT_TX_ID) {
                LOG.info("Inprogress segment {} is empty, deleting", l);

                return deleteLogSegment(l).thenApply(
                        (result) -> {
                            synchronized (inprogressLSSNs) {
                                inprogressLSSNs.remove((Long) l.getLogSegmentSequenceNumber());
                            }
                            return null;
                        });
            } else {
                CompletableFuture<LogSegmentMetadata> promise = new CompletableFuture<LogSegmentMetadata>();
                doCompleteAndCloseLogSegment(
                        l.getZNodeName(),
                        l.getLogSegmentSequenceNumber(),
                        l.getLogSegmentId(),
                        l.getFirstTxId(),
                        endTxId,
                        recordCount,
                        lastEntryId,
                        lastSlotId,
                        promise);
                return promise;
            }
        }

    }

    CompletableFuture<List<LogSegmentMetadata>> setLogSegmentsOlderThanDLSNTruncated(final DLSN dlsn) {
        if (DLSN.InvalidDLSN == dlsn) {
            List<LogSegmentMetadata> emptyList = new ArrayList<LogSegmentMetadata>(0);
            return FutureUtils.value(emptyList);
        }
        return getCachedLogSegmentsAfterFirstFullFetch(LogSegmentMetadata.COMPARATOR).thenCompose(
            logSegments -> setLogSegmentsOlderThanDLSNTruncated(logSegments, dlsn));
    }

    private CompletableFuture<List<LogSegmentMetadata>> setLogSegmentsOlderThanDLSNTruncated(
            List<LogSegmentMetadata> logSegments, final DLSN dlsn) {
        LOG.debug("Setting truncation status on logs older than {} from {} for {}",
            dlsn, logSegments, getFullyQualifiedName());
        List<LogSegmentMetadata> truncateList = new ArrayList<LogSegmentMetadata>(logSegments.size());
        LogSegmentMetadata partialTruncate = null;
        LOG.info("{}: Truncating log segments older than {}", getFullyQualifiedName(), dlsn);
        for (int i = 0; i < logSegments.size(); i++) {
            LogSegmentMetadata l = logSegments.get(i);
            if (!l.isInProgress()) {
                if (l.getLastDLSN().compareTo(dlsn) < 0) {
                    LOG.debug("{}: Truncating log segment {} ", getFullyQualifiedName(), l);
                    truncateList.add(l);
                } else if (l.getFirstDLSN().compareTo(dlsn) < 0) {
                    // Can be satisfied by at most one segment
                    if (null != partialTruncate) {
                        String logMsg = String.format("Potential metadata inconsistency for stream %s at segment %s",
                                getFullyQualifiedName(), l);
                        LOG.error(logMsg);
                        return FutureUtils.exception(new DLIllegalStateException(logMsg));
                    }
                    LOG.info("{}: Partially truncating log segment {} older than {}.",
                        getFullyQualifiedName(), l, dlsn);
                    partialTruncate = l;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        return setLogSegmentTruncationStatus(truncateList, partialTruncate, dlsn);
    }

    private int getNumCandidateLogSegmentsToPurge(List<LogSegmentMetadata> logSegments) {
        if (logSegments.isEmpty()) {
            return 0;
        } else {
            // we have to keep at least one completed log segment for sequence id
            int numCandidateLogSegments = 0;
            for (LogSegmentMetadata segment : logSegments) {
                if (segment.isInProgress()) {
                    break;
                } else {
                    ++numCandidateLogSegments;
                }
            }

            return numCandidateLogSegments - 1;
        }
    }

    CompletableFuture<List<LogSegmentMetadata>> purgeLogSegmentsOlderThanTimestamp(final long minTimestampToKeep) {
        if (minTimestampToKeep >= Utils.nowInMillis()) {
            return FutureUtils.exception(new IllegalArgumentException(
                    "Invalid timestamp " + minTimestampToKeep + " to purge logs for " + getFullyQualifiedName()));
        }
        return getCachedLogSegmentsAfterFirstFullFetch(LogSegmentMetadata.COMPARATOR).thenCompose(
                new Function<List<LogSegmentMetadata>, CompletableFuture<List<LogSegmentMetadata>>>() {
            @Override
            public CompletableFuture<List<LogSegmentMetadata>> apply(List<LogSegmentMetadata> logSegments) {
                List<LogSegmentMetadata> purgeList = new ArrayList<LogSegmentMetadata>(logSegments.size());

                int numCandidates = getNumCandidateLogSegmentsToPurge(logSegments);

                for (int iterator = 0; iterator < numCandidates; iterator++) {
                    LogSegmentMetadata l = logSegments.get(iterator);
                    // When application explicitly truncates segments; timestamp based purge is
                    // only used to cleanup log segments that have been marked for truncation
                    if ((l.isTruncated() || !conf.getExplicitTruncationByApplication())
                            && !l.isInProgress() && (l.getCompletionTime() < minTimestampToKeep)) {
                        purgeList.add(l);
                    } else {
                        // stop truncating log segments if we find either an inprogress or a partially
                        // truncated log segment
                        break;
                    }
                }
                LOG.info("Deleting log segments older than {} for {} : {}",
                    minTimestampToKeep, getFullyQualifiedName(), purgeList);
                return deleteLogSegments(purgeList);
            }
        });
    }

    CompletableFuture<List<LogSegmentMetadata>> purgeLogSegmentsOlderThanTxnId(final long minTxIdToKeep) {
        return getCachedLogSegmentsAfterFirstFullFetch(LogSegmentMetadata.COMPARATOR).thenCompose(
            logSegments -> {
                int numLogSegmentsToProcess;

                if (minTxIdToKeep < 0) {
                    // we are deleting the log, we can remove whole log segments
                    numLogSegmentsToProcess = logSegments.size();
                } else {
                    numLogSegmentsToProcess = getNumCandidateLogSegmentsToPurge(logSegments);
                }
                List<LogSegmentMetadata> purgeList = Lists.newArrayListWithExpectedSize(numLogSegmentsToProcess);
                for (int iterator = 0; iterator < numLogSegmentsToProcess; iterator++) {
                    LogSegmentMetadata l = logSegments.get(iterator);
                    if ((minTxIdToKeep < 0)
                            || ((l.isTruncated() || !conf.getExplicitTruncationByApplication())
                            && !l.isInProgress() && (l.getLastTxId() < minTxIdToKeep))) {
                        purgeList.add(l);
                    } else {
                        // stop truncating log segments if we find either an inprogress or a partially
                        // truncated log segment
                        break;
                    }
                }
                return deleteLogSegments(purgeList);
            });
    }

    private CompletableFuture<List<LogSegmentMetadata>> setLogSegmentTruncationStatus(
            final List<LogSegmentMetadata> truncateList,
            LogSegmentMetadata partialTruncate,
            DLSN minActiveDLSN) {
        final List<LogSegmentMetadata> listToTruncate = Lists.newArrayListWithCapacity(truncateList.size() + 1);
        final List<LogSegmentMetadata> listAfterTruncated = Lists.newArrayListWithCapacity(truncateList.size() + 1);
        Transaction<Object> updateTxn = metadataUpdater.transaction();
        for (LogSegmentMetadata l : truncateList) {
            if (!l.isTruncated()) {
                LogSegmentMetadata newSegment = metadataUpdater.setLogSegmentTruncated(updateTxn, l);
                listToTruncate.add(l);
                listAfterTruncated.add(newSegment);
            }
        }

        if (null != partialTruncate && (partialTruncate.isNonTruncated()
                || (partialTruncate.isPartiallyTruncated()
                && (partialTruncate.getMinActiveDLSN().compareTo(minActiveDLSN) < 0)))) {
            LogSegmentMetadata newSegment = metadataUpdater.setLogSegmentPartiallyTruncated(
                    updateTxn, partialTruncate, minActiveDLSN);
            listToTruncate.add(partialTruncate);
            listAfterTruncated.add(newSegment);
        }

        return updateTxn.execute().thenApply(value -> {
            for (int i = 0; i < listToTruncate.size(); i++) {
                removeLogSegmentFromCache(listToTruncate.get(i).getSegmentName());
                LogSegmentMetadata newSegment = listAfterTruncated.get(i);
                addLogSegmentToCache(newSegment.getSegmentName(), newSegment);
            }
            return listAfterTruncated;
        });
    }

    private CompletableFuture<List<LogSegmentMetadata>> deleteLogSegments(
            final List<LogSegmentMetadata> logs) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Purging logs for {} : {}", getFullyQualifiedName(), logs);
        }
        return FutureUtils.processList(logs,
            segment -> deleteLogSegment(segment), scheduler);
    }

    private CompletableFuture<LogSegmentMetadata> deleteLogSegment(
            final LogSegmentMetadata ledgerMetadata) {
        LOG.info("Deleting ledger {} for {}", ledgerMetadata, getFullyQualifiedName());
        final CompletableFuture<LogSegmentMetadata> promise = new CompletableFuture<LogSegmentMetadata>();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        promise.whenComplete(new FutureEventListener<LogSegmentMetadata>() {
            @Override
            public void onSuccess(LogSegmentMetadata segment) {
                deleteOpStats.registerSuccessfulEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS),
                    TimeUnit.MICROSECONDS);
            }

            @Override
            public void onFailure(Throwable cause) {
                deleteOpStats.registerFailedEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS),
                    TimeUnit.MICROSECONDS);
            }
        });
        entryStore.deleteLogSegment(ledgerMetadata)
                .whenComplete(new FutureEventListener<LogSegmentMetadata>() {
            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.completeExceptionally(promise, cause);
            }

            @Override
            public void onSuccess(LogSegmentMetadata segment) {
                deleteLogSegmentMetadata(segment, promise);
            }
        });
        return promise;
    }

    private void deleteLogSegmentMetadata(final LogSegmentMetadata segmentMetadata,
                                          final CompletableFuture<LogSegmentMetadata> promise) {
        Transaction<Object> deleteTxn = metadataStore.transaction();
        metadataStore.deleteLogSegment(deleteTxn, segmentMetadata, new Transaction.OpListener<Void>() {
            @Override
            public void onCommit(Void r) {
                // purge log segment
                removeLogSegmentFromCache(segmentMetadata.getZNodeName());
                promise.complete(segmentMetadata);
            }

            @Override
            public void onAbort(Throwable t) {
                if (t instanceof LogSegmentNotFoundException) {
                    // purge log segment
                    removeLogSegmentFromCache(segmentMetadata.getZNodeName());
                    promise.complete(segmentMetadata);
                    return;
                } else {
                    LOG.error("Couldn't purge {} for {}: with error {}",
                        segmentMetadata, getFullyQualifiedName(), t);
                    promise.completeExceptionally(t);
                }
            }
        });
        deleteTxn.execute();
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return Utils.closeSequence(scheduler,
                lock,
                logSegmentAllocator);
    }

    @Override
    public CompletableFuture<Void> asyncAbort() {
        return asyncClose();
    }

    String completedLedgerZNodeName(long firstTxId, long lastTxId, long logSegmentSeqNo) {
        if (DistributedLogConstants.LOGSEGMENT_NAME_VERSION == conf.getLogSegmentNameVersion()) {
            return String.format("%s_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, logSegmentSeqNo);
        } else {
            return String.format("%s_%018d_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX,
                    firstTxId, lastTxId);
        }
    }

    /**
     * Get the znode path for a finalize ledger.
     */
    String completedLedgerZNode(long firstTxId, long lastTxId, long logSegmentSeqNo) {
        return String.format("%s/%s", logMetadata.getLogSegmentsPath(),
                completedLedgerZNodeName(firstTxId, lastTxId, logSegmentSeqNo));
    }

    /**
     * Get the name of the inprogress znode.
     *
     * @return name of the inprogress znode.
     */
    String inprogressZNodeName(long logSegmentId, long firstTxId, long logSegmentSeqNo) {
        if (DistributedLogConstants.LOGSEGMENT_NAME_VERSION == conf.getLogSegmentNameVersion()) {
            // Lots of the problems are introduced due to different inprogress names with same ledger sequence number.
            return String.format("%s_%018d", DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX, logSegmentSeqNo);
        } else {
            return DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX + "_" + Long.toString(firstTxId, 16);
        }
    }

    /**
     * Get the znode path for the inprogressZNode.
     */
    String inprogressZNode(long logSegmentId, long firstTxId, long logSegmentSeqNo) {
        return logMetadata.getLogSegmentsPath() + "/" + inprogressZNodeName(logSegmentId, firstTxId, logSegmentSeqNo);
    }

    String inprogressZNode(String inprogressZNodeName) {
        return logMetadata.getLogSegmentsPath() + "/" + inprogressZNodeName;
    }
}
