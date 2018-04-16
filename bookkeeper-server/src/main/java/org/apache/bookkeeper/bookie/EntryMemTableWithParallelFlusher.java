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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.SafeRunnable;

/**
 * EntryMemTableWithParallelFlusher.
 */
@Slf4j
public class EntryMemTableWithParallelFlusher extends EntryMemTable {

    final OrderedExecutor flushExecutor;
    private static final int MEMTABLE_FLUSH_TIMEOUT_IN_SECONDS = 60;

    public EntryMemTableWithParallelFlusher(final ServerConfiguration conf, final CheckpointSource source,
            final StatsLogger statsLogger, OrderedExecutor flushExecutor) {
        super(conf, source, statsLogger);
        this.flushExecutor = flushExecutor;
    }

    /**
     * Functionally this overridden flushSnapshot does the same as
     * EntryMemTable's flushSnapshot, but it uses flushExecutor
     * (OrderedExecutor) to process an entry through flusher.
     *
     * <p>entryKeyValues of a ledger are grouped in a list and runnable is created
     * to flush process all those entries and that runnable is submitted to the
     * flushExecutor with ledgerId as the orderingKey.
     */
    @Override
    long flushSnapshot(final SkipListFlusher flusher, Checkpoint checkpoint) throws IOException {
        AtomicLong flushedSize = new AtomicLong();
        if (this.snapshot.compareTo(checkpoint) < 0) {
            synchronized (this) {
                EntrySkipList keyValues = this.snapshot;
                if (keyValues.compareTo(checkpoint) < 0) {
                    NavigableSet<EntryKey> keyValuesSet = keyValues.keySet();
                    Map<Long, List<EntryKeyValue>> entryKeyValuesMap = new HashMap<Long, List<EntryKeyValue>>();

                    for (EntryKey key : keyValuesSet) {
                        EntryKeyValue kv = (EntryKeyValue) key;
                        Long ledger = kv.getLedgerId();
                        if (!entryKeyValuesMap.containsKey(ledger)) {
                            entryKeyValuesMap.put(ledger, new LinkedList<EntryKeyValue>());
                        }
                        entryKeyValuesMap.get(ledger).add(kv);
                    }

                    CountDownLatch latch = new CountDownLatch(entryKeyValuesMap.size());
                    AtomicBoolean isFlushThreadInterrupted = new AtomicBoolean(false);
                    AtomicReference<Exception> exceptionWhileFlushingParallelly =  new AtomicReference<Exception>();
                    Thread mainFlushThread = Thread.currentThread();

                    for (Map.Entry<Long, List<EntryKeyValue>> entryKeyValuesOfALedgerMapEntry : entryKeyValuesMap
                            .entrySet()) {
                        Long ledgerId = entryKeyValuesOfALedgerMapEntry.getKey();
                        List<EntryKeyValue> entryKeyValuesOfALedger = entryKeyValuesOfALedgerMapEntry.getValue();
                        flushExecutor.executeOrdered(ledgerId.longValue(), new SafeRunnable() {
                            @Override
                            public void safeRun() {
                                for (EntryKeyValue entryKeyValue : entryKeyValuesOfALedger) {
                                    try {
                                        flusher.process(ledgerId, entryKeyValue.getEntryId(),
                                                entryKeyValue.getValueAsByteBuffer());
                                        flushedSize.addAndGet(entryKeyValue.getLength());
                                    } catch (NoLedgerException exception) {
                                        log.debug("Got NoLedgerException while flushing entry: {}. "
                                                + "The ledger must be deleted "
                                                + "after this entry is added to the Memtable",
                                                entryKeyValue);
                                        break;
                                    } catch (Exception exc) {
                                        log.error(
                                                "Got Exception while trying to flush process entry: " + entryKeyValue,
                                                exc);
                                        if (isFlushThreadInterrupted.compareAndSet(false, true)) {
                                            exceptionWhileFlushingParallelly.set(exc);
                                            mainFlushThread.interrupt();
                                        }
                                        // return without countdowning the latch since we got unexpected Exception
                                        return;
                                    }
                                }
                                latch.countDown();
                            }
                        });
                    }

                    try {
                        while (!latch.await(MEMTABLE_FLUSH_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
                            log.error("Entrymemtable parallel flush has not completed in {} secs, so waiting again",
                                    MEMTABLE_FLUSH_TIMEOUT_IN_SECONDS);
                        }
                        flushBytesCounter.add(flushedSize.get());
                        clearSnapshot(keyValues);
                    } catch (InterruptedException ie) {
                        log.error("Got Interrupted exception while waiting for the flushexecutor "
                                + "to complete the entry flushes");
                        throw new IOException("Failed to complete the flushSnapshotByParallelizing",
                                exceptionWhileFlushingParallelly.get());
                    }
                }
            }
        }
        return flushedSize.longValue();
    }
}
