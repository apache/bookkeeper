/**
 *
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

package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * EntryMemTableWithParallelFlusher.
 */
@CustomLog
class EntryMemTableWithParallelFlusher extends EntryMemTable {

    final OrderedExecutor flushExecutor;

    public EntryMemTableWithParallelFlusher(final ServerConfiguration conf, final CheckpointSource source,
            final StatsLogger statsLogger) {
        super(conf, source, statsLogger);
        this.flushExecutor = OrderedExecutor.newBuilder().numThreads(conf.getNumOfMemtableFlushThreads())
                .name("MemtableFlushThreads").build();
    }

    /**
     * Functionally this overridden flushSnapshot does the same as
     * EntryMemTable's flushSnapshot, but it uses flushExecutor
     * (OrderedExecutor) to process an entry through flusher.
     *
     * <p>SubMaps of the snapshot corresponding to the entries of the ledgers are
     * created and submitted to the flushExecutor with ledgerId as the
     * orderingKey to flush process the entries of a ledger.
     */
    @Override
    long flushSnapshot(final SkipListFlusher flusher, Checkpoint checkpoint) throws IOException {
        AtomicLong flushedSize = new AtomicLong();
        if (this.snapshot.compareTo(checkpoint) < 0) {
            synchronized (this) {
                EntrySkipList keyValues = this.snapshot;

                Phaser pendingNumOfLedgerFlushes = new Phaser(1);
                AtomicReference<Throwable> exceptionWhileFlushingParallelly = new AtomicReference<Throwable>();

                if (keyValues.compareTo(checkpoint) < 0) {

                    Map.Entry<EntryKey, EntryKeyValue> thisLedgerFirstMapEntry = keyValues.firstEntry();
                    EntryKeyValue thisLedgerFirstEntry;
                    long thisLedgerId;

                    while (thisLedgerFirstMapEntry != null) {
                        thisLedgerFirstEntry = thisLedgerFirstMapEntry.getValue();
                        thisLedgerId = thisLedgerFirstEntry.getLedgerId();
                        EntryKey thisLedgerCeilingKeyMarker = new EntryKey(thisLedgerId, Long.MAX_VALUE - 1);
                        /*
                         * Gets a view of the portion of this map that
                         * corresponds to entries of this ledger.
                         */
                        ConcurrentNavigableMap<EntryKey, EntryKeyValue> thisLedgerEntries = keyValues
                                .subMap(thisLedgerFirstEntry, thisLedgerCeilingKeyMarker);
                        pendingNumOfLedgerFlushes.register();
                        try {
                            flushExecutor.executeOrdered(thisLedgerId, () -> {
                            try {
                                long ledger;
                                boolean ledgerDeleted = false;
                                for (EntryKey key : thisLedgerEntries.keySet()) {
                                    EntryKeyValue kv = (EntryKeyValue) key;
                                    flushedSize.addAndGet(kv.getLength());
                                    ledger = kv.getLedgerId();
                                    if (!ledgerDeleted) {
                                        try {
                                            flusher.process(ledger, kv.getEntryId(), kv.getValueAsByteBuffer());
                                        } catch (NoLedgerException exception) {
                                            ledgerDeleted = true;
                                        }
                                    }
                                }
                            } catch (Throwable exc) {
                                log.error().exception(exc).log("Got Exception while trying to flush process entries");
                                recordFlushException(exceptionWhileFlushingParallelly, exc);
                            } finally {
                                pendingNumOfLedgerFlushes.arriveAndDeregister();
                            }
                            });
                        } catch (RejectedExecutionException ree) {
                            pendingNumOfLedgerFlushes.arriveAndDeregister();
                            recordFlushException(exceptionWhileFlushingParallelly, ree);
                        }
                        thisLedgerFirstMapEntry = keyValues.ceilingEntry(thisLedgerCeilingKeyMarker);
                    }

                    try {
                        pendingNumOfLedgerFlushes.arriveAndAwaitAdvance();
                    } catch (IllegalStateException ise) {
                        log.error().exception(ise).log("Got IllegalStateException while awaiting on Phaser");
                        throw new IOException("Got IllegalStateException while awaiting on Phaser", ise);
                    }
                    Throwable flushFailure = exceptionWhileFlushingParallelly.get();
                    if (flushFailure != null) {
                        log.error().exception(flushFailure)
                        .log("Exception while awaiting flushExecutor to complete the entry flushes");
                        throw preserveEntryLogWriteFailure(flushFailure);
                    }
                    memTableStats.getFlushBytesCounter().addCount(flushedSize.get());
                    clearSnapshot(keyValues);
                }
            }
        }
        skipListSemaphore.release(flushedSize.intValue());
        return flushedSize.longValue();
    }

    @Override
    public void close() throws Exception {
        flushExecutor.shutdown();
    }

    private static IOException preserveEntryLogWriteFailure(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof EntryLogWriteException) {
                return (EntryLogWriteException) cause;
            }
        }
        return new IOException("Failed to complete the flushSnapshotByParallelizing", failure);
    }

    private static void recordFlushException(AtomicReference<Throwable> failure, Throwable candidate) {
        failure.updateAndGet(previous -> {
            if (previous == null || (!containsEntryLogWriteException(previous)
                    && containsEntryLogWriteException(candidate))) {
                return candidate;
            }
            return previous;
        });
    }

    private static boolean containsEntryLogWriteException(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof EntryLogWriteException) {
                return true;
            }
        }
        return false;
    }
}
