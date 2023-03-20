/*
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
package org.apache.distributedlog.impl.logsegment;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.Entry;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.exceptions.BKTransmitException;
import org.apache.distributedlog.exceptions.DLIllegalStateException;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.EndOfLogSegmentException;
import org.apache.distributedlog.exceptions.ReadCancelledException;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.logsegment.LogSegmentEntryReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BookKeeper ledger based log segment entry reader.
 */
public class BKLogSegmentEntryReader implements Runnable, LogSegmentEntryReader, AsyncCallback.OpenCallback {

    private static final Logger logger = LoggerFactory.getLogger(BKLogSegmentEntryReader.class);

    private class CacheEntry implements Runnable, AsyncCallback.ReadCallback,
            AsyncCallback.ReadLastConfirmedAndEntryCallback {

        protected final long entryId;
        private boolean done;
        private LedgerEntry entry;
        private int rc;

        private CacheEntry(long entryId) {
            this.entryId = entryId;
            this.entry = null;
            this.rc = BKException.Code.UnexpectedConditionException;
            this.done = false;
        }

        long getEntryId() {
            return entryId;
        }

        synchronized boolean isDone() {
            return done;
        }

        synchronized void release() {
            if (null != this.entry) {
                this.entry.getEntryBuffer().release();
                this.entry = null;
            }
        }

        void release(LedgerEntry entry) {
            if (null != entry) {
                entry.getEntryBuffer().release();
            }
        }

        void complete(LedgerEntry entry) {
            // the reader is already closed
            if (isClosed()) {
                release(entry);
            }
            synchronized (this) {
                if (done) {
                    return;
                }
                this.rc = BKException.Code.OK;
                this.entry = entry;
            }
            setDone(true);
        }

        void completeExceptionally(int rc) {
            synchronized (this) {
                if (done) {
                    return;
                }
                this.rc = rc;
            }
            setDone(false);
        }

        void setDone(boolean success) {
            synchronized (this) {
                this.done = true;
            }
            onReadEntryDone(success);
        }

        synchronized boolean isSuccess() {
            return BKException.Code.OK == rc;
        }

        synchronized LedgerEntry getEntry() {
            // retain reference for the caller
            this.entry.getEntryBuffer().retain();
            return this.entry;
        }

        synchronized int getRc() {
            return rc;
        }

        @Override
        public void readComplete(int rc,
                                 LedgerHandle lh,
                                 Enumeration<LedgerEntry> entries,
                                 Object ctx) {
            if (failureInjector.shouldInjectCorruption(entryId, entryId)) {
                rc = BKException.Code.DigestMatchException;
            }
            processReadEntries(rc, lh, entries, ctx);
        }

        void processReadEntries(int rc,
                                LedgerHandle lh,
                                Enumeration<LedgerEntry> entries,
                                Object ctx) {
            if (isDone()) {
                return;
            }
            if (!checkReturnCodeAndHandleFailure(rc, false)) {
                return;
            }
            LedgerEntry entry = null;
            while (entries.hasMoreElements()) {
                // more entries are returned
                if (null != entry) {
                    completeExceptionally(BKException.Code.UnexpectedConditionException);
                    return;
                }
                entry = entries.nextElement();
            }
            if (null == entry || entry.getEntryId() != entryId) {
                completeExceptionally(BKException.Code.UnexpectedConditionException);
                return;
            }
            complete(entry);
        }

        @Override
        public void readLastConfirmedAndEntryComplete(int rc,
                                                      long entryId,
                                                      LedgerEntry entry,
                                                      Object ctx) {
            if (failureInjector.shouldInjectCorruption(this.entryId, this.entryId)) {
                rc = BKException.Code.DigestMatchException;
            }
            processReadEntry(rc, entryId, entry, ctx);
        }

        void processReadEntry(int rc,
                              long entryId,
                              LedgerEntry entry,
                              Object ctx) {
            if (isDone()) {
                return;
            }
            if (!checkReturnCodeAndHandleFailure(rc, true)) {
                return;
            }
            if (null != entry && this.entryId == entryId) {
                complete(entry);
                return;
            }
            // the long poll is timeout or interrupted; we will retry it again.
            issueRead(this);
        }

        /**
         * Check return code and retry if needed.
         *
         * @param rc the return code
         * @param isLongPoll is it a long poll request
         * @return is the request successful or not
         */
        boolean checkReturnCodeAndHandleFailure(int rc, boolean isLongPoll) {
            if (BKException.Code.OK == rc) {
                numReadErrorsUpdater.set(BKLogSegmentEntryReader.this, 0);
                return true;
            }
            if (BKException.Code.BookieHandleNotAvailableException == rc
                    || (isLongPoll && BKException.Code.NoSuchLedgerExistsException == rc)) {
                int numErrors = Math.max(1, numReadErrorsUpdater.incrementAndGet(BKLogSegmentEntryReader.this));
                int nextReadBackoffTime = Math.min(numErrors * readAheadWaitTime, maxReadBackoffTime);
                scheduler.scheduleOrdered(
                        getSegment().getLogSegmentId(),
                        this,
                        nextReadBackoffTime,
                        TimeUnit.MILLISECONDS);
            } else {
                completeExceptionally(rc);
            }
            return false;
        }

        @Override
        public void run() {
            issueRead(this);
        }
    }

    private class PendingReadRequest {
        private final int numEntries;
        private final List<Entry.Reader> entries;
        private final CompletableFuture<List<Entry.Reader>> promise;

        PendingReadRequest(int numEntries) {
            this.numEntries = numEntries;
            if (numEntries == 1) {
                this.entries = new ArrayList<Entry.Reader>(1);
            } else {
                this.entries = new ArrayList<Entry.Reader>();
            }
            this.promise = new CompletableFuture<List<Entry.Reader>>();
        }

        CompletableFuture<List<Entry.Reader>> getPromise() {
            return promise;
        }

        void completeExceptionally(Throwable throwable) {
            FutureUtils.completeExceptionally(promise, throwable);
        }

        void addEntry(Entry.Reader entry) {
            entries.add(entry);
        }

        void complete() {
            FutureUtils.complete(promise, entries);
            onEntriesConsumed(entries.size());
        }

        boolean hasReadEntries() {
            return entries.size() > 0;
        }

        boolean hasReadEnoughEntries() {
            return entries.size() >= numEntries;
        }
    }

    private final BookKeeper bk;
    private final DistributedLogConfiguration conf;
    private final OrderedScheduler scheduler;
    private final long lssn;
    private final long startSequenceId;
    private final boolean envelopeEntries;
    private final boolean deserializeRecordSet;
    private final int numPrefetchEntries;
    private final int maxPrefetchEntries;
    // state
    private CompletableFuture<Void> closePromise = null;
    private LogSegmentMetadata metadata;
    private LedgerHandle lh;
    private final List<LedgerHandle> openLedgerHandles;
    private CacheEntry outstandingLongPoll;
    private long nextEntryId;
    private static final AtomicReferenceFieldUpdater<BKLogSegmentEntryReader, Throwable> lastExceptionUpdater =
        AtomicReferenceFieldUpdater.newUpdater(BKLogSegmentEntryReader.class, Throwable.class, "lastException");
    private volatile Throwable lastException = null;
    private static final AtomicLongFieldUpdater<BKLogSegmentEntryReader> scheduleCountUpdater =
        AtomicLongFieldUpdater.newUpdater(BKLogSegmentEntryReader.class, "scheduleCount");
    private volatile long scheduleCount = 0L;
    private volatile boolean hasCaughtupOnInprogress = false;
    private final CopyOnWriteArraySet<StateChangeListener> stateChangeListeners =
            new CopyOnWriteArraySet<StateChangeListener>();
    // read retries
    private int readAheadWaitTime;
    private final int maxReadBackoffTime;
    private static final AtomicIntegerFieldUpdater<BKLogSegmentEntryReader> numReadErrorsUpdater =
        AtomicIntegerFieldUpdater.newUpdater(BKLogSegmentEntryReader.class, "numReadErrors");
    private volatile int numReadErrors = 0;
    private final boolean skipBrokenEntries;
    // readahead cache
    int cachedEntries = 0;
    int numOutstandingEntries = 0;
    final LinkedBlockingQueue<CacheEntry> readAheadEntries;
    // request queue
    final LinkedList<PendingReadRequest> readQueue;

    // failure injector
    private final AsyncFailureInjector failureInjector;
    // Stats
    private final Counter skippedBrokenEntriesCounter;

    BKLogSegmentEntryReader(LogSegmentMetadata metadata,
                            LedgerHandle lh,
                            long startEntryId,
                            BookKeeper bk,
                            OrderedScheduler scheduler,
                            DistributedLogConfiguration conf,
                            StatsLogger statsLogger,
                            AsyncFailureInjector failureInjector) {
        this.metadata = metadata;
        this.lssn = metadata.getLogSegmentSequenceNumber();
        this.startSequenceId = metadata.getStartSequenceId();
        this.envelopeEntries = metadata.getEnvelopeEntries();
        this.deserializeRecordSet = conf.getDeserializeRecordSetOnReads();
        this.lh = lh;
        this.nextEntryId = Math.max(startEntryId, 0);
        this.bk = bk;
        this.conf = conf;
        this.numPrefetchEntries = conf.getNumPrefetchEntriesPerLogSegment();
        this.maxPrefetchEntries = conf.getMaxPrefetchEntriesPerLogSegment();
        this.scheduler = scheduler;
        this.openLedgerHandles = Lists.newArrayList();
        this.openLedgerHandles.add(lh);
        this.outstandingLongPoll = null;
        // create the readahead queue
        this.readAheadEntries = new LinkedBlockingQueue<CacheEntry>();
        // create the read request queue
        this.readQueue = new LinkedList<PendingReadRequest>();
        // read backoff settings
        this.readAheadWaitTime = conf.getReadAheadWaitTime();
        this.maxReadBackoffTime = 4 * conf.getReadAheadWaitTime();
        // other read settings
        this.skipBrokenEntries = conf.getReadAheadSkipBrokenEntries();

        // Failure Injection
        this.failureInjector = failureInjector;
        // Stats
        this.skippedBrokenEntriesCounter = statsLogger.getCounter("skipped_broken_entries");
    }

    @VisibleForTesting
    public synchronized CacheEntry getOutstandingLongPoll() {
        return outstandingLongPoll;
    }

    @VisibleForTesting
    LinkedBlockingQueue<CacheEntry> getReadAheadEntries() {
        return this.readAheadEntries;
    }

    synchronized LedgerHandle getLh() {
        return lh;
    }

    @Override
    public synchronized LogSegmentMetadata getSegment() {
        return metadata;
    }

    @VisibleForTesting
    synchronized long getNextEntryId() {
        return nextEntryId;
    }

    @Override
    public void start() {
        prefetchIfNecessary();
    }

    @Override
    public boolean hasCaughtUpOnInprogress() {
        return hasCaughtupOnInprogress;
    }

    @Override
    public LogSegmentEntryReader registerListener(StateChangeListener listener) {
        stateChangeListeners.add(listener);
        return this;
    }

    @Override
    public LogSegmentEntryReader unregisterListener(StateChangeListener listener) {
        stateChangeListeners.remove(listener);
        return this;
    }

    private void notifyCaughtupOnInprogress() {
        for (StateChangeListener listener : stateChangeListeners) {
            listener.onCaughtupOnInprogress();
        }
    }

    //
    // Process on Log Segment Metadata Updates
    //

    @Override
    public synchronized void onLogSegmentMetadataUpdated(LogSegmentMetadata segment) {
        if (metadata == segment
                || LogSegmentMetadata.COMPARATOR.compare(metadata, segment) == 0
                || !(metadata.isInProgress() && !segment.isInProgress())) {
            return;
        }
        // segment is closed from inprogress, then re-open the log segment
        bk.asyncOpenLedger(
                segment.getLogSegmentId(),
                BookKeeper.DigestType.CRC32,
                conf.getBKDigestPW().getBytes(UTF_8),
                this,
                segment);
    }

    @Override
    public void openComplete(int rc, LedgerHandle lh, Object ctx) {
        LogSegmentMetadata segment = (LogSegmentMetadata) ctx;
        if (BKException.Code.OK != rc) {
            // fail current reader or retry opening the reader
            failOrRetryOpenLedger(rc, segment);
            return;
        }
        // switch to new ledger handle if the log segment is moved to completed.
        CacheEntry longPollRead = null;
        synchronized (this) {
            if (isClosed()) {
                lh.asyncClose(new AsyncCallback.CloseCallback() {
                    @Override
                    public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                        logger.debug("Close the open ledger {} since the log segment reader is already closed",
                                lh.getId());
                    }
                }, null);
                return;
            }
            this.metadata = segment;
            this.lh = lh;
            this.openLedgerHandles.add(lh);
            longPollRead = outstandingLongPoll;
        }
        if (null != longPollRead) {
            // reissue the long poll read when the log segment state is changed
            issueRead(longPollRead);
        }
        // notify readers
        notifyReaders();
    }

    private void failOrRetryOpenLedger(int rc, final LogSegmentMetadata segment) {
        if (isClosed()) {
            return;
        }
        if (isBeyondLastAddConfirmed()) {
            // if the reader is already caught up, let's fail the reader immediately
            // as we need to pull the latest metadata of this log segment.
            completeExceptionally(new BKTransmitException("Failed to open ledger for reading log segment "
                            + getSegment(), rc),
                    true);
            return;
        }
        // the reader is still catching up, retry opening the log segment later
        scheduler.scheduleOrdered(
            segment.getLogSegmentId(),
            () -> onLogSegmentMetadataUpdated(segment),
            conf.getZKRetryBackoffStartMillis(),
            TimeUnit.MILLISECONDS);
    }

    //
    // Change the state of this reader
    //

    private boolean checkClosedOrInError() {
        Throwable cause = lastExceptionUpdater.get(this);
        if (null != cause) {
            cancelAllPendingReads(cause);
            return true;
        }
        return false;
    }

    /**
     * Set the reader into error state with return code <i>rc</i>.
     *
     * @param throwable exception indicating the error
     * @param isBackground is the reader set exception by background reads or foreground reads
     */
    private void completeExceptionally(Throwable throwable, boolean isBackground) {
        lastExceptionUpdater.compareAndSet(this, null, throwable);
        if (isBackground) {
            notifyReaders();
        }
    }

    /**
     * Notify the readers with the state change.
     */
    private void notifyReaders() {
        processReadRequests();
    }

    private void cancelAllPendingReads(Throwable throwExc) {
        List<PendingReadRequest> requestsToCancel;
        synchronized (readQueue) {
            requestsToCancel = Lists.newArrayListWithExpectedSize(readQueue.size());
            requestsToCancel.addAll(readQueue);
            readQueue.clear();
        }
        for (PendingReadRequest request : requestsToCancel) {
            request.completeExceptionally(throwExc);
        }
    }

    private void releaseAllCachedEntries() {
        synchronized (this) {
            CacheEntry entry = readAheadEntries.poll();
            while (null != entry) {
                entry.release();
                entry = readAheadEntries.poll();
            }
        }
    }

    //
    // Background Read Operations
    //

    private void onReadEntryDone(boolean success) {
        // we successfully read an entry
        synchronized (this) {
            --numOutstandingEntries;
        }
        // notify reader that there is entry ready
        notifyReaders();
        // stop prefetch if we already encountered exceptions
        if (success) {
            prefetchIfNecessary();
        }
    }

    private void onEntriesConsumed(int numEntries) {
        synchronized (this) {
            cachedEntries -= numEntries;
        }
        prefetchIfNecessary();
    }

    private void prefetchIfNecessary() {
        List<CacheEntry> entriesToFetch;
        synchronized (this) {
            if (cachedEntries >= maxPrefetchEntries) {
                return;
            }
            // we don't have enough entries, do prefetch
            int numEntriesToFetch = numPrefetchEntries - numOutstandingEntries;
            if (numEntriesToFetch <= 0) {
                return;
            }
            entriesToFetch = new ArrayList<CacheEntry>(numEntriesToFetch);
            for (int i = 0; i < numEntriesToFetch; i++) {
                if (cachedEntries >= maxPrefetchEntries) {
                    break;
                }
                if ((isLedgerClosed() && nextEntryId > getLastAddConfirmed())
                        || (!isLedgerClosed() && nextEntryId > getLastAddConfirmed() + 1)) {
                    break;
                }
                CacheEntry entry = new CacheEntry(nextEntryId);
                entriesToFetch.add(entry);
                readAheadEntries.add(entry);
                ++numOutstandingEntries;
                ++cachedEntries;
                ++nextEntryId;
            }
        }
        for (CacheEntry entry : entriesToFetch) {
            issueRead(entry);
        }
    }


    private void issueRead(CacheEntry cacheEntry) {
        if (isClosed()) {
            return;
        }
        if (isLedgerClosed()) {
            if (isNotBeyondLastAddConfirmed(cacheEntry.getEntryId())) {
                issueSimpleRead(cacheEntry);
                return;
            } else {
                // Reach the end of stream
                notifyReaders();
            }
        } else { // the ledger is still in progress
            if (isNotBeyondLastAddConfirmed(cacheEntry.getEntryId())) {
                issueSimpleRead(cacheEntry);
            } else {
                issueLongPollRead(cacheEntry);
            }
        }
    }

    private void issueSimpleRead(CacheEntry cacheEntry) {
        getLh().asyncReadEntries(cacheEntry.entryId, cacheEntry.entryId, cacheEntry, null);
    }

    private void issueLongPollRead(CacheEntry cacheEntry) {
        // register the read as outstanding reads
        synchronized (this) {
            this.outstandingLongPoll = cacheEntry;
        }

        if (!hasCaughtupOnInprogress) {
            hasCaughtupOnInprogress = true;
            notifyCaughtupOnInprogress();
        }
        getLh().asyncReadLastConfirmedAndEntry(
                cacheEntry.entryId,
                conf.getReadLACLongPollTimeout(),
                false,
                cacheEntry,
                null);
    }

    //
    // Foreground Read Operations
    //

    Entry.Reader processReadEntry(LedgerEntry entry) throws IOException {
        return Entry.newBuilder()
                .setLogSegmentInfo(lssn, startSequenceId)
                .setEntryId(entry.getEntryId())
                .setEnvelopeEntry(envelopeEntries)
                .deserializeRecordSet(deserializeRecordSet)
                .setEntry(entry.getEntryBuffer())
                .buildReader();
    }

    @Override
    public CompletableFuture<List<Entry.Reader>> readNext(int numEntries) {
        final PendingReadRequest readRequest = new PendingReadRequest(numEntries);

        if (checkClosedOrInError()) {
            readRequest.completeExceptionally(lastExceptionUpdater.get(this));
        } else {
            boolean wasQueueEmpty;
            synchronized (readQueue) {
                wasQueueEmpty = readQueue.isEmpty();
                readQueue.add(readRequest);
            }
            if (wasQueueEmpty) {
                processReadRequests();
            }
        }
        return readRequest.getPromise();
    }

    private void processReadRequests() {
        if (isClosed()) {
            // the reader is already closed.
            return;
        }

        long prevCount = scheduleCountUpdater.getAndIncrement(this);
        if (0 == prevCount) {
            scheduler.executeOrdered(getSegment().getLogSegmentId(), this);
        }
    }

    /**
     * The core function to propagate fetched entries to read requests.
     */
    @Override
    public void run() {
        long scheduleCountLocal = scheduleCountUpdater.get(this);
        while (true) {
            PendingReadRequest nextRequest = null;
            synchronized (readQueue) {
                nextRequest = readQueue.peek();
            }

            // if read queue is empty, nothing to read, return
            if (null == nextRequest) {
                scheduleCountUpdater.set(this, 0L);
                return;
            }

            // if the oldest pending promise is interrupted then we must
            // mark the reader in error and abort all pending reads since
            // we don't know the last consumed read
            if (null == lastExceptionUpdater.get(this)) {
                if (nextRequest.getPromise().isCancelled()) {
                    completeExceptionally(new DLInterruptedException("Interrupted on reading log segment "
                            + getSegment() + " : " + nextRequest.getPromise().isCancelled()), false);
                }
            }

            // if the reader is in error state, stop read
            if (checkClosedOrInError()) {
                return;
            }

            // read entries from readahead cache to satisfy next read request
            readEntriesFromReadAheadCache(nextRequest);

            // check if we can satisfy the read request
            if (nextRequest.hasReadEntries()) {
                PendingReadRequest request;
                synchronized (readQueue) {
                    request = readQueue.poll();
                }
                if (null != request && nextRequest == request) {
                    request.complete();
                } else {
                    DLIllegalStateException ise = new DLIllegalStateException("Unexpected condition at reading from "
                            + getSegment());
                    nextRequest.completeExceptionally(ise);
                    if (null != request) {
                        request.completeExceptionally(ise);
                    }
                    completeExceptionally(ise, false);
                }
            } else {
                if (0 == scheduleCountLocal) {
                    return;
                }
                scheduleCountLocal = scheduleCountUpdater.decrementAndGet(this);
            }
        }
    }

    private void readEntriesFromReadAheadCache(PendingReadRequest nextRequest) {
        while (!nextRequest.hasReadEnoughEntries()) {
            CacheEntry entry;
            boolean hitEndOfLogSegment;
            synchronized (this) {
                entry = readAheadEntries.peek();
                hitEndOfLogSegment = (null == entry) && isEndOfLogSegment();
            }
            // reach end of log segment
            if (hitEndOfLogSegment) {
                completeExceptionally(new EndOfLogSegmentException(getSegment().getZNodeName()), false);
                return;
            }
            if (null == entry) {
                return;
            }
            // entry is not complete yet.
            if (!entry.isDone()) {
                // we already reached end of the log segment
                if (isEndOfLogSegment(entry.getEntryId())) {
                    completeExceptionally(new EndOfLogSegmentException(getSegment().getZNodeName()), false);
                }
                return;
            }
            if (entry.isSuccess()) {
                CacheEntry removedEntry = readAheadEntries.poll();
                try {
                    if (entry != removedEntry) {
                        DLIllegalStateException ise =
                                new DLIllegalStateException("Unexpected condition at reading from "
                            + getSegment());
                        completeExceptionally(ise, false);
                        return;
                    }
                    try {
                        // the reference is retained on `entry.getEntry()`.
                        // Entry.Reader is responsible for releasing it.
                        nextRequest.addEntry(processReadEntry(entry.getEntry()));
                    } catch (IOException e) {
                        completeExceptionally(e, false);
                        return;
                    }
                } finally {
                    ReferenceCountUtil.release(removedEntry);
                }
            } else if (skipBrokenEntries && BKException.Code.DigestMatchException == entry.getRc()) {
                // skip this entry and move forward
                skippedBrokenEntriesCounter.inc();
                CacheEntry removedEntry = readAheadEntries.poll();
                removedEntry.release();
                continue;
            } else {
                completeExceptionally(new BKTransmitException("Encountered issue on reading entry " + entry.getEntryId()
                        + " @ log segment " + getSegment(), entry.getRc()), false);
                return;
            }
        }
    }

    //
    // State Management
    //

    private synchronized boolean isEndOfLogSegment() {
        return isEndOfLogSegment(nextEntryId);
    }

    private boolean isEndOfLogSegment(long entryId) {
        return isLedgerClosed() && entryId > getLastAddConfirmed();
    }

    @Override
    public synchronized boolean isBeyondLastAddConfirmed() {
        return isBeyondLastAddConfirmed(nextEntryId);
    }

    private boolean isBeyondLastAddConfirmed(long entryId) {
        return entryId > getLastAddConfirmed();
    }

    private boolean isNotBeyondLastAddConfirmed(long entryId) {
        return entryId <= getLastAddConfirmed();
    }

    private boolean isLedgerClosed() {
        return getLh().isClosed();
    }

    @Override
    public long getLastAddConfirmed() {
        return getLh().getLastAddConfirmed();
    }

    synchronized boolean isClosed() {
        return null != closePromise;
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        final CompletableFuture<Void> closeFuture;
        ReadCancelledException exception;
        LedgerHandle[] lhsToClose;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new CompletableFuture<Void>();
            lhsToClose = openLedgerHandles.toArray(new LedgerHandle[openLedgerHandles.size()]);
            // set the exception to cancel pending and subsequent reads
            exception = new ReadCancelledException(getSegment().getZNodeName(), "Reader was closed");
            completeExceptionally(exception, false);
        }

        // release the cached entries
        releaseAllCachedEntries();

        // cancel all pending reads
        cancelAllPendingReads(exception);

        // close all the open ledger
        FutureUtils.proxyTo(
            BKUtils.closeLedgers(lhsToClose),
            closeFuture
        );
        return closeFuture;
    }
}
