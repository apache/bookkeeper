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
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashSet;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;

/**
 * Maintains an index of the entry locations in the EntryLogger.
 *
 * <p>For each ledger multiple entries are stored in the same "record", represented
 * by the {@link LedgerIndexPage} class.
 */
@CustomLog
public class EntryLocationIndex implements Closeable {

    static final String LAST_ENTRY_CACHE_MAX_SIZE = "dbStorage_lastEntryCacheMaxSize";
    private static final long DEFAULT_LAST_ENTRY_CACHE_MAX_SIZE = 100_000;
    private static final float LAST_ENTRY_CACHE_FILL_FACTOR = 0.66f;

    private final KeyValueStorage locationsDb;
    private final ConcurrentLongHashSet deletedLedgers = ConcurrentLongHashSet.newBuilder().build();
    private final ConcurrentLongLongHashMap lastEntryCache;
    private final long lastEntryCacheMaxSize;
    private final EntryLocationIndexStats stats;
    private boolean isCompacting;

    public EntryLocationIndex(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
            StatsLogger stats) throws IOException {
        locationsDb = storageFactory.newKeyValueStorage(basePath, "locations", DbConfigType.EntryLocation, conf);

        this.lastEntryCacheMaxSize = conf.getLong(LAST_ENTRY_CACHE_MAX_SIZE, DEFAULT_LAST_ENTRY_CACHE_MAX_SIZE);
        // Pre-size to avoid a resize right before we hit the cap and clear().
        // ConcurrentLongLongHashMap resizes at size = expectedItems, so set expectedItems
        // = max / fillFactor to keep the table stable for the lifetime of one fill cycle.
        long expectedItems = (long) (lastEntryCacheMaxSize / LAST_ENTRY_CACHE_FILL_FACTOR);
        this.lastEntryCache = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems((int) Math.min(expectedItems, Integer.MAX_VALUE))
                .build();

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
                log.debug()
                        .attr("ledgerId", ledgerId)
                        .attr("entryId", entryId)
                        .log("Entry not found in db index");
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
            log.debug().attr("ledgerId", ledgerId).log("Ledger already deleted in db");
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
        // Check in-memory cache first to avoid expensive getFloor() calls.
        // ConcurrentLongLongHashMap.get() returns -1 if not found.
        long cachedLastEntry = lastEntryCache.get(ledgerId);
        if (cachedLastEntry >= 0) {
            log.debug()
                .attr("ledgerId", ledgerId)
                .attr("cacheLastEntry", cachedLastEntry)
                .log("Found last entry");
            stats.getLastEntryCacheHits().inc();
            return cachedLastEntry;
        }

        stats.getLastEntryCacheMisses().inc();
        LongPairWrapper maxEntryId = LongPairWrapper.get(ledgerId, Long.MAX_VALUE);

        long startTimeNanos = MathUtils.nowInNano();
        // Search the last entry in storage
        Entry<byte[], byte[]> entry = locationsDb.getFloor(maxEntryId.array);
        if (entry != null) {
            stats.getGetLastEntryInLedgerStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        } else {
            stats.getGetLastEntryInLedgerStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        }
        maxEntryId.recycle();

        if (entry == null) {
            throw new Bookie.NoEntryException(ledgerId, -1);
        } else {
            long foundLedgerId = ArrayUtil.getLong(entry.getKey(), 0);
            long lastEntryId = ArrayUtil.getLong(entry.getKey(), 8);

            if (foundLedgerId == ledgerId) {
                log.debug()
                        .attr("ledgerId", ledgerId)
                        .attr("lastEntryId", lastEntryId)
                        .log("Found last page in storage db for ledger");
                // Populate cache only if no newer entry was inserted concurrently by an
                // addLocation call between our cache miss above and this point. The
                // CAS loop mirrors the pattern in WriteCache.put() (lines 168-178).
                cacheLastEntryIfNewer(ledgerId, lastEntryId);
                return lastEntryId;
            } else {
                throw new Bookie.NoEntryException(ledgerId, -1);
            }
        }
    }

    /**
     * Atomically set the cached last entry for ledgerId to newEntryId, but only if newEntryId
     * is greater than whatever is currently cached. Bounded by lastEntryCacheMaxSize: when the
     * cache is full and ledgerId is not yet present, the whole cache is cleared (working-set
     * eviction) before insertion.
     */
    private void cacheLastEntryIfNewer(long ledgerId, long newEntryId) {
        while (true) {
            long currentLastEntry = lastEntryCache.get(ledgerId);
            if (currentLastEntry >= newEntryId) {
                // A newer or equal entry is already cached.
                return;
            }
            if (currentLastEntry < 0) {
                // Not in cache; enforce the size cap before inserting.
                if (lastEntryCache.size() >= lastEntryCacheMaxSize) {
                    lastEntryCache.clear();
                }
                // putIfAbsent so we don't race with a concurrent insert that beat us here.
                long prior = lastEntryCache.putIfAbsent(ledgerId, newEntryId);
                if (prior < 0 || prior >= newEntryId) {
                    return;
                }
                // Lost the race and the winning value is still older — retry to overwrite.
                continue;
            }
            if (lastEntryCache.compareAndSet(ledgerId, currentLastEntry, newEntryId)) {
                return;
            }
        }
    }

    public void addLocation(long ledgerId, long entryId, long location) throws IOException {
        try (Batch batch = locationsDb.newBatch()) {
            addLocation(batch, ledgerId, entryId, location);
            batch.flush();
        }
    }

    public Batch newBatch() {
        return locationsDb.newBatch();
    }

    public void addLocation(Batch batch, long ledgerId, long entryId, long location) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        LongWrapper value = LongWrapper.get(location);

        log.debug()
                .attr("ledgerId", ledgerId)
                .attr("entryId", entryId)
                .attr("location", location)
                .log("Add location");

        try {
            batch.put(key.array, value.array);
        } finally {
            key.recycle();
            value.recycle();
        }

        // Bump the cached last-entry id if this write extends it. CAS-protected so a
        // concurrent read populating the cache from RocksDB can't regress us, and two
        // concurrent writers can't let the lower entryId win.
        cacheLastEntryIfNewer(ledgerId, entryId);
    }

    public void updateLocations(Iterable<EntryLocation> newLocations) throws IOException {
        log.debug(e -> e.attr("count", Iterables.size(newLocations)).log("Update locations"));

        try (Batch batch = newBatch()) {
            // Update all the ledger index pages with the new locations
            for (EntryLocation e : newLocations) {
                log.debug()
                        .attr("ledgerId", e.ledger)
                        .attr("entryId", e.entry)
                        .log("Update location");

                addLocation(batch, e.ledger, e.entry, e.location);
            }
            batch.flush();
        }
    }

    public void delete(long ledgerId) throws IOException {
        // We need to find all the LedgerIndexPage records belonging to one specific
        // ledgers.
        // Ordering: deletedLedgers.add() must precede the cache.remove(). Any reader that
        // observes a removed cache entry will subsequently consult deletedLedgers via
        // getLastEntryInLedger() and throw NoEntryException rather than re-caching from
        // RocksDB (the actual row deletion happens later in removeOffsetFromDeletedLedgers).
        deletedLedgers.add(ledgerId);
        lastEntryCache.remove(ledgerId);
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

        log.info().attr("ledgers", ledgersToDelete).log("Deleting indexes for ledgers");
        long startTime = System.nanoTime();

        try (Batch batch = locationsDb.newBatch()) {
            for (long ledgerId : ledgersToDelete) {
                log.debug().attr("ledgerId", ledgerId).log("Deleting indexes from ledger");

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

        log.info().attr("count", ledgersToDelete.size())
                .attr("durationSeconds", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) / 1000.0)
                .log("Deleted indexes from ledgers");
    }

}
