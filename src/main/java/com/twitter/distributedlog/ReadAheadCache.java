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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.twitter.distributedlog.callback.ReadAheadCallback;
import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import com.twitter.distributedlog.exceptions.LogReadException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadAheadCache {
    static final Logger LOG = LoggerFactory.getLogger(ReadAheadCache.class);

    private final String streamName;
    private final LinkedBlockingQueue<Entry.Reader> readAheadEntries;
    private final int maxCachedEntries;
    private final AtomicReference<IOException> lastException = new AtomicReference<IOException>();
    private final boolean deserializeRecordSet;
    // callbacks
    private final AsyncNotification notification;
    private ReadAheadCallback readAheadCallback = null;

    // variables for idle reader detection
    private final Stopwatch lastEntryProcessTime;

    private final AlertStatsLogger alertStatsLogger;

    public ReadAheadCache(String streamName,
                          AlertStatsLogger alertStatsLogger,
                          AsyncNotification notification,
                          int maxCachedRecords,
                          boolean deserializeRecordSet,
                          Ticker ticker) {
        this.streamName = streamName;
        this.maxCachedEntries = maxCachedRecords;
        this.notification = notification;
        this.deserializeRecordSet = deserializeRecordSet;

        // create the readahead queue
        readAheadEntries = new LinkedBlockingQueue<Entry.Reader>();

        // start the idle reader detection
        lastEntryProcessTime = Stopwatch.createStarted(ticker);

        // Stats
        this.alertStatsLogger = alertStatsLogger;
    }

    /**
     * Trigger read ahead callback
     */
    private synchronized void invokeReadAheadCallback() {
        if (null != readAheadCallback) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Cache has space, schedule the read ahead");
            }
            readAheadCallback.resumeReadAhead();
            readAheadCallback = null;
        }
    }

    /**
     * Register a readhead callback.
     *
     * @param readAheadCallback
     *          read ahead callback
     */
    public synchronized void setReadAheadCallback(ReadAheadCallback readAheadCallback) {
        this.readAheadCallback = readAheadCallback;
        if (!isCacheFull()) {
            invokeReadAheadCallback();
        }
    }

    private void setLastException(IOException exc) {
        lastException.set(exc);
    }

    /**
     * Poll next entry from the readahead queue.
     *
     * @return next entry from readahead queue. null if no entries available in the queue.
     * @throws IOException
     */
    public Entry.Reader getNextReadAheadEntry() throws IOException {
        if (null != lastException.get()) {
            throw lastException.get();
        }

        Entry.Reader entry = readAheadEntries.poll();

        if (null != entry) {
            if (!isCacheFull()) {
                invokeReadAheadCallback();
            }
        }

        return entry;
    }

    /**
     * Check whether the readahead becomes stall.
     *
     * @param idleReaderErrorThreshold
     *          idle reader error threshold
     * @param timeUnit
     *          time unit of the idle reader error threshold
     * @return true if the readahead becomes stall, otherwise false.
     */
    public boolean isReadAheadIdle(int idleReaderErrorThreshold, TimeUnit timeUnit) {
        return (lastEntryProcessTime.elapsed(timeUnit) > idleReaderErrorThreshold);
    }

    /**
     * Set an ledger entry to readahead cache
     *
     * @param key
     *          read position of the entry
     * @param entry
     *          the ledger entry
     * @param reason
     *          the reason to add the entry to readahead (for logging)
     * @param envelopeEntries
     *          whether this entry an enveloped entries or not
     * @param startSequenceId
     *          the start sequence id
     */
    public void set(LedgerReadPosition key,
                    LedgerEntry entry,
                    String reason,
                    boolean envelopeEntries,
                    long startSequenceId) {
        processNewLedgerEntry(key, entry, reason, envelopeEntries, startSequenceId);
        lastEntryProcessTime.reset().start();
        AsyncNotification n = notification;
        if (null != n) {
            n.notifyOnOperationComplete();
        }
    }

    public boolean isCacheFull() {
        return getNumCachedEntries() >= maxCachedEntries;
    }

    /**
     * Return number cached records.
     *
     * @return number cached records.
     */
    public int getNumCachedEntries() {
        return readAheadEntries.size();
    }

    /**
     * Process the new ledger entry and propagate the records into readahead queue.
     *
     * @param readPosition
     *          position of the ledger entry
     * @param ledgerEntry
     *          ledger entry
     * @param reason
     *          reason to add this ledger entry
     * @param envelopeEntries
     *          whether this entry is enveloped
     * @param startSequenceId
     *          the start sequence id of this log segment
     */
    private void processNewLedgerEntry(final LedgerReadPosition readPosition,
                                       final LedgerEntry ledgerEntry,
                                       final String reason,
                                       boolean envelopeEntries,
                                       long startSequenceId) {
        try {
            Entry.Reader reader = Entry.newBuilder()
                    .setLogSegmentInfo(readPosition.getLogSegmentSequenceNumber(), startSequenceId)
                    .setEntryId(ledgerEntry.getEntryId())
                    .setEnvelopeEntry(envelopeEntries)
                    .deserializeRecordSet(deserializeRecordSet)
                    .setInputStream(ledgerEntry.getEntryInputStream())
                    .buildReader();
            readAheadEntries.add(reader);
        } catch (InvalidEnvelopedEntryException ieee) {
            alertStatsLogger.raise("Found invalid enveloped entry on stream {} : ", streamName, ieee);
            setLastException(ieee);
        } catch (IOException exc) {
            setLastException(exc);
        }
    }

    public void clear() {
        readAheadEntries.clear();
    }

    @Override
    public String toString() {
        return String.format("%s: Num Cached Entries: %d",
            streamName, getNumCachedEntries());
    }
}
