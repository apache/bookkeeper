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
package com.twitter.distributedlog.impl.logsegment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.Entry;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.exceptions.BKTransmitException;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfLogSegmentException;
import com.twitter.distributedlog.exceptions.ReadCancelledException;
import com.twitter.distributedlog.logsegment.LogSegmentEntryReader;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;

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

        void setValue(LedgerEntry entry) {
            synchronized (this) {
                if (done) {
                    return;
                }
                this.rc = BKException.Code.OK;
                this.entry = entry;
            }
            setDone(true);
        }

        void setException(int rc) {
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
                    setException(BKException.Code.UnexpectedConditionException);
                    return;
                }
                entry = entries.nextElement();
            }
            if (null == entry || entry.getEntryId() != entryId) {
                setException(BKException.Code.UnexpectedConditionException);
                return;
            }
            setValue(entry);
        }

        @Override
        public void readLastConfirmedAndEntryComplete(int rc,
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
                setValue(entry);
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
                numReadErrors.set(0);
                return true;
            }
            if (BKException.Code.BookieHandleNotAvailableException == rc ||
                    (isLongPoll && BKException.Code.NoSuchLedgerExistsException == rc)) {
                int numErrors = Math.max(1, numReadErrors.incrementAndGet());
                int nextReadBackoffTime = Math.min(numErrors * readAheadWaitTime, maxReadBackoffTime);
                scheduler.schedule(
                        this,
                        nextReadBackoffTime,
                        TimeUnit.MILLISECONDS);
            } else {
                setException(rc);
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
        private final Promise<List<Entry.Reader>> promise;

        PendingReadRequest(int numEntries) {
            this.numEntries = numEntries;
            if (numEntries == 1) {
                this.entries = new ArrayList<Entry.Reader>(1);
            } else {
                this.entries = new ArrayList<Entry.Reader>();
            }
            this.promise = new Promise<List<Entry.Reader>>();
        }

        Promise<List<Entry.Reader>> getPromise() {
            return promise;
        }

        void setException(Throwable throwable) {
            FutureUtils.setException(promise, throwable);
        }

        void addEntry(Entry.Reader entry) {
            entries.add(entry);
        }

        void complete() {
            FutureUtils.setValue(promise, entries);
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
    private final long startEntryId;
    private final long lssn;
    private final long startSequenceId;
    private final boolean envelopeEntries;
    private final int numPrefetchEntries;
    private final int maxPrefetchEntries;
    // state
    private Promise<Void> closePromise = null;
    private LogSegmentMetadata metadata;
    private LedgerHandle lh;
    private final List<LedgerHandle> openLedgerHandles;
    private CacheEntry outstandingLongPoll;
    private long nextEntryId;
    private final AtomicReference<Throwable> lastException = new AtomicReference<Throwable>(null);
    private final AtomicLong scheduleCount = new AtomicLong(0);
    // read retries
    private int readAheadWaitTime;
    private final int maxReadBackoffTime;
    private final AtomicInteger numReadErrors = new AtomicInteger(0);
    // readahead cache
    int cachedEntries = 0;
    int numOutstandingEntries = 0;
    final LinkedBlockingQueue<CacheEntry> readAheadEntries;
    // request queue
    final ConcurrentLinkedQueue<PendingReadRequest> readQueue;

    BKLogSegmentEntryReader(LogSegmentMetadata metadata,
                            LedgerHandle lh,
                            long startEntryId,
                            BookKeeper bk,
                            OrderedScheduler scheduler,
                            DistributedLogConfiguration conf) {
        this.metadata = metadata;
        this.lssn = metadata.getLogSegmentSequenceNumber();
        this.startSequenceId = metadata.getStartSequenceId();
        this.envelopeEntries = metadata.getEnvelopeEntries();
        this.lh = lh;
        this.startEntryId = this.nextEntryId = Math.max(startEntryId, 0);
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
        this.readQueue = new ConcurrentLinkedQueue<PendingReadRequest>();
        // read backoff settings
        this.readAheadWaitTime = conf.getReadAheadWaitTime();
        this.maxReadBackoffTime = 4 * conf.getReadAheadWaitTime();
    }

    synchronized LedgerHandle getLh() {
        return lh;
    }

    synchronized LogSegmentMetadata getSegment() {
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

    //
    // Process on Log Segment Metadata Updates
    //

    @Override
    public synchronized void onLogSegmentMetadataUpdated(LogSegmentMetadata segment) {
        if (metadata == segment ||
                LogSegmentMetadata.COMPARATOR.compare(metadata, segment) == 0 ||
                !(metadata.isInProgress() && !segment.isInProgress())) {
            return;
        }
        // segment is closed from inprogress, then re-open the log segment
        bk.asyncOpenLedger(
                segment.getLedgerId(),
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
            setException(new BKTransmitException("Failed to open ledger for reading log segment " + getSegment(), rc),
                    true);
            return;
        }
        // the reader is still catching up, retry opening the log segment later
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                onLogSegmentMetadataUpdated(segment);
            }
        }, conf.getZKRetryBackoffStartMillis(), TimeUnit.MILLISECONDS);
    }

    //
    // Change the state of this reader
    //

    private boolean checkClosedOrInError() {
        if (null != lastException.get()) {
            cancelAllPendingReads(lastException.get());
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
    private void setException(Throwable throwable, boolean isBackground) {
        lastException.compareAndSet(null, throwable);
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
        for (PendingReadRequest request : readQueue) {
            request.setException(throwExc);
        }
        readQueue.clear();
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
                if ((isLedgerClosed() && nextEntryId > getLastAddConfirmed()) ||
                        (!isLedgerClosed() && nextEntryId > getLastAddConfirmed() + 1)) {
                    break;
                }
                entriesToFetch.add(new CacheEntry(nextEntryId));
                ++numOutstandingEntries;
                ++cachedEntries;
                ++nextEntryId;
            }
        }
        for (CacheEntry entry : entriesToFetch) {
            readAheadEntries.add(entry);
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
                .deserializeRecordSet(false)
                .setInputStream(entry.getEntryInputStream())
                .buildReader();
    }

    @Override
    public Future<List<Entry.Reader>> readNext(int numEntries) {
        final PendingReadRequest readRequest = new PendingReadRequest(numEntries);

        if (checkClosedOrInError()) {
            readRequest.setException(lastException.get());
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

        long prevCount = scheduleCount.getAndIncrement();
        if (0 == prevCount) {
            scheduler.submit(this);
        }
    }

    /**
     * The core function to propagate fetched entries to read requests
     */
    @Override
    public void run() {
        long scheduleCountLocal = scheduleCount.get();
        while (true) {
            PendingReadRequest nextRequest = null;
            synchronized (readQueue) {
                nextRequest = readQueue.peek();
            }

            // if read queue is empty, nothing to read, return
            if (null == nextRequest) {
                scheduleCount.set(0L);
                return;
            }

            // if the oldest pending promise is interrupted then we must
            // mark the reader in error and abort all pending reads since
            // we don't know the last consumed read
            if (null == lastException.get()) {
                if (nextRequest.getPromise().isInterrupted().isDefined()) {
                    setException(new DLInterruptedException("Interrupted on reading log segment "
                            + getSegment() + " : " + nextRequest.getPromise().isInterrupted().get()), false);
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
                    nextRequest.setException(ise);
                    if (null != request) {
                        request.setException(ise);
                    }
                    setException(ise, false);
                }
            } else {
                if (0 == scheduleCountLocal) {
                    return;
                }
                scheduleCountLocal = scheduleCount.decrementAndGet();
            }
        }
    }

    private void readEntriesFromReadAheadCache(PendingReadRequest nextRequest) {
        while (!nextRequest.hasReadEnoughEntries()) {
            CacheEntry entry = readAheadEntries.peek();
            // no entry available in the read ahead cache
            if (null == entry) {
                if (isEndOfLogSegment()) {
                    setException(new EndOfLogSegmentException(getSegment().getZNodeName()), false);
                }
                return;
            }
            // entry is not complete yet.
            if (!entry.isDone()) {
                // we already reached end of the log segment
                if (isEndOfLogSegment(entry.getEntryId())) {
                    setException(new EndOfLogSegmentException(getSegment().getZNodeName()), false);
                }
                return;
            }
            if (entry.isSuccess()) {
                CacheEntry removedEntry = readAheadEntries.poll();
                if (entry != removedEntry) {
                    DLIllegalStateException ise = new DLIllegalStateException("Unexpected condition at reading from "
                            + getSegment());
                    setException(ise, false);
                    return;
                }
                try {
                    nextRequest.addEntry(processReadEntry(entry.getEntry()));
                } catch (IOException e) {
                    setException(e, false);
                    return;
                }
            } else {
                setException(new BKTransmitException("Encountered issue on reading entry " + entry.getEntryId()
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

    private synchronized boolean isBeyondLastAddConfirmed() {
        return isBeyondLastAddConfirmed(nextEntryId);
    }

    private boolean isBeyondLastAddConfirmed(long entryId) {
        return entryId > getLastAddConfirmed();
    }

    private synchronized boolean isNotBeyondLastAddConfirmed() {
        return isNotBeyondLastAddConfirmed(nextEntryId);
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
    public Future<Void> asyncClose() {
        final Promise<Void> closeFuture;
        ReadCancelledException exception;
        LedgerHandle[] lhsToClose;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new Promise<Void>();
            lhsToClose = openLedgerHandles.toArray(new LedgerHandle[openLedgerHandles.size()]);
            // set the exception to cancel pending and subsequent reads
            exception = new ReadCancelledException(getSegment().getZNodeName(), "Reader was closed");
            setException(exception, false);
        }

        // cancel all pending reads
        cancelAllPendingReads(exception);

        // close all the open ledger
        BKUtils.closeLedgers(lhsToClose).proxyTo(closeFuture);
        return closeFuture;
    }
}
