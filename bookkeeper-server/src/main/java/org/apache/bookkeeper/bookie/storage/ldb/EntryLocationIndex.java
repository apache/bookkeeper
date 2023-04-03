/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains an index of the entry locations in the EntryLogger.
 *
 * <p>For each ledger multiple entries are stored in the same "record", represented
 * by the {@link LedgerIndexPage} class.
 */
public class EntryLocationIndex implements Closeable {

    private final KeyValueStorage locationsDb;
    private final ConcurrentLongHashSet deletedLedgers = ConcurrentLongHashSet.newBuilder().build();
    private final EntryLocationIndexStats stats;
    private boolean isCompacting;

    public EntryLocationIndex(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
            StatsLogger stats) throws IOException {
        locationsDb = storageFactory.newKeyValueStorage(basePath, "locations", DbConfigType.EntryLocation, conf);

        this.stats = new EntryLocationIndexStats(
            stats,
            () -> {
                try {
                    return locationsDb.count();
                } catch (IOException e) {
                    return -1L;
                }
            });
    }

    @Override
    public void close() throws IOException {
        locationsDb.close();
    }

    public long getLocation(long ledgerId, long entryId) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        LongWrapper value = LongWrapper.get();

        long startTimeNanos = MathUtils.nowInNano();
        boolean operationSuccess = false;
        try {
            if (locationsDb.get(key.array, value.array) < 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Entry not found {}@{} in db index", ledgerId, entryId);
                }
                return 0;
            }
            operationSuccess = true;
            return value.getValue();
        } finally {
            key.recycle();
            value.recycle();
            if (operationSuccess) {
                stats.getLookupEntryLocationStats()
                        .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                stats.getLookupEntryLocationStats()
                        .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            }
        }
    }

    public long getLastEntryInLedger(long ledgerId) throws IOException {
        if (deletedLedgers.contains(ledgerId)) {
            // Ledger already deleted
            if (log.isDebugEnabled()) {
                log.debug("Ledger {} already deleted in db", ledgerId);
            }
            /**
             * when Ledger already deleted,
             * throw Bookie.NoEntryException same like  the method
             * {@link EntryLocationIndex.getLastEntryInLedgerInternal} solving ledgerId is not found.
             * */
            throw new Bookie.NoEntryException(ledgerId, -1);
        }
        return getLastEntryInLedgerInternal(ledgerId);
    }

    private long getLastEntryInLedgerInternal(long ledgerId) throws IOException {
        LongPairWrapper maxEntryId = LongPairWrapper.get(ledgerId, Long.MAX_VALUE);

        // Search the last entry in storage
        Entry<byte[], byte[]> entry = locationsDb.getFloor(maxEntryId.array);
        maxEntryId.recycle();

        if (entry == null) {
            throw new Bookie.NoEntryException(ledgerId, -1);
        } else {
            long foundLedgerId = ArrayUtil.getLong(entry.getKey(), 0);
            long lastEntryId = ArrayUtil.getLong(entry.getKey(), 8);

            if (foundLedgerId == ledgerId) {
                if (log.isDebugEnabled()) {
                    log.debug("Found last page in storage db for ledger {} - last entry: {}", ledgerId, lastEntryId);
                }
                return lastEntryId;
            } else {
                throw new Bookie.NoEntryException(ledgerId, -1);
            }
        }
    }

    public void addLocation(long ledgerId, long entryId, long location) throws IOException {
        Batch batch = locationsDb.newBatch();
        addLocation(batch, ledgerId, entryId, location);
        batch.flush();
        batch.close();
    }

    public Batch newBatch() {
        return locationsDb.newBatch();
    }

    public void addLocation(Batch batch, long ledgerId, long entryId, long location) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        LongWrapper value = LongWrapper.get(location);

        if (log.isDebugEnabled()) {
            log.debug("Add location - ledger: {} -- entry: {} -- location: {}", ledgerId, entryId, location);
        }

        try {
            batch.put(key.array, value.array);
        } finally {
            key.recycle();
            value.recycle();
        }
    }

    public void updateLocations(Iterable<EntryLocation> newLocations) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Update locations -- {}", Iterables.size(newLocations));
        }

        Batch batch = newBatch();
        // Update all the ledger index pages with the new locations
        for (EntryLocation e : newLocations) {
            if (log.isDebugEnabled()) {
                log.debug("Update location - ledger: {} -- entry: {}", e.ledger, e.entry);
            }

            addLocation(batch, e.ledger, e.entry, e.location);
        }

        batch.flush();
        batch.close();
    }

    public void delete(long ledgerId) throws IOException {
        // We need to find all the LedgerIndexPage records belonging to one specific
        // ledgers
        deletedLedgers.add(ledgerId);
    }

    public String getEntryLocationDBPath() {
        return locationsDb.getDBPath();
    }

    public void compact() throws IOException {
        try {
            isCompacting = true;
            locationsDb.compact();
        } finally {
            isCompacting = false;
        }
    }

    public boolean isCompacting() {
        return isCompacting;
    }

    public void removeOffsetFromDeletedLedgers() throws IOException {
        Set<Long> ledgersToDelete = deletedLedgers.items();

        if (ledgersToDelete.isEmpty()) {
            return;
        }

        LongPairWrapper firstKeyWrapper = LongPairWrapper.get(-1, -1);
        LongPairWrapper lastKeyWrapper = LongPairWrapper.get(-1, -1);

        log.info("Deleting indexes for ledgers: {}", ledgersToDelete);
        long startTime = System.nanoTime();

        try (Batch batch = locationsDb.newBatch()) {
            for (long ledgerId : ledgersToDelete) {
                if (log.isDebugEnabled()) {
                    log.debug("Deleting indexes from ledger {}", ledgerId);
                }

                firstKeyWrapper.set(ledgerId, 0);
                lastKeyWrapper.set(ledgerId, Long.MAX_VALUE);

                batch.deleteRange(firstKeyWrapper.array, lastKeyWrapper.array);
            }

            batch.flush();
            for (long ledgerId : ledgersToDelete) {
                deletedLedgers.remove(ledgerId);
            }
        } finally {
            firstKeyWrapper.recycle();
            lastKeyWrapper.recycle();
        }

        log.info("Deleted indexes from {} ledgers in {} seconds", ledgersToDelete.size(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) / 1000.0);
    }

    private static final Logger log = LoggerFactory.getLogger(EntryLocationIndex.class);
}
