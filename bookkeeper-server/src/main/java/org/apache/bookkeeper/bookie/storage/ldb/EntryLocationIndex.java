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
import java.nio.file.FileSystems;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
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
    private final ConcurrentLongHashSet deletedLedgers = new ConcurrentLongHashSet();

    private final EntryLocationIndexStats stats;

    public EntryLocationIndex(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
            StatsLogger stats) throws IOException {
        String locationsDbPath = FileSystems.getDefault().getPath(basePath, "locations").toFile().toString();
        locationsDb = storageFactory.newKeyValueStorage(locationsDbPath, DbConfigType.Huge, conf);

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

        try {
            if (locationsDb.get(key.array, value.array) < 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Entry not found {}@{} in db index", ledgerId, entryId);
                }
                return 0;
            }

            return value.getValue();
        } finally {
            key.recycle();
            value.recycle();
        }
    }

    public long getLastEntryInLedger(long ledgerId) throws IOException {
        if (deletedLedgers.contains(ledgerId)) {
            // Ledger already deleted
            return -1;
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

    private static final int DELETE_ENTRIES_BATCH_SIZE = 100000;

    public void removeOffsetFromDeletedLedgers() throws IOException {
        LongPairWrapper firstKeyWrapper = LongPairWrapper.get(-1, -1);
        LongPairWrapper lastKeyWrapper = LongPairWrapper.get(-1, -1);
        LongPairWrapper keyToDelete = LongPairWrapper.get(-1, -1);

        Set<Long> ledgersToDelete = deletedLedgers.items();

        if (ledgersToDelete.isEmpty()) {
            return;
        }

        log.info("Deleting indexes for ledgers: {}", ledgersToDelete);
        long startTime = System.nanoTime();
        long deletedEntries = 0;
        long deletedEntriesInBatch = 0;

        Batch batch = locationsDb.newBatch();

        try {
            for (long ledgerId : ledgersToDelete) {
                if (log.isDebugEnabled()) {
                    log.debug("Deleting indexes from ledger {}", ledgerId);
                }

                firstKeyWrapper.set(ledgerId, 0);
                lastKeyWrapper.set(ledgerId, Long.MAX_VALUE);

                Entry<byte[], byte[]> firstKeyRes = locationsDb.getCeil(firstKeyWrapper.array);
                if (firstKeyRes == null || ArrayUtil.getLong(firstKeyRes.getKey(), 0) != ledgerId) {
                    // No entries found for ledger
                    if (log.isDebugEnabled()) {
                        log.debug("No entries found for ledger {}", ledgerId);
                    }
                    continue;
                }

                long firstEntryId = ArrayUtil.getLong(firstKeyRes.getKey(), 8);
                long lastEntryId;
                try {
                    lastEntryId = getLastEntryInLedgerInternal(ledgerId);
                } catch (Bookie.NoEntryException nee) {
                    if (log.isDebugEnabled()) {
                        log.debug("No last entry id found for ledger {}", ledgerId);
                    }
                    continue;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Deleting index for ledger {} entries ({} -> {})",
                            ledgerId, firstEntryId, lastEntryId);
                }

                // Iterate over all the keys and remove each of them
                for (long entryId = firstEntryId; entryId <= lastEntryId; entryId++) {
                    keyToDelete.set(ledgerId, entryId);
                    if (log.isDebugEnabled()) {
                        log.debug("Deleting index for ({}, {})", keyToDelete.getFirst(), keyToDelete.getSecond());
                    }
                    batch.remove(keyToDelete.array);
                    ++deletedEntriesInBatch;
                    ++deletedEntries;
                }

                if (deletedEntriesInBatch > DELETE_ENTRIES_BATCH_SIZE) {
                    batch.flush();
                    batch.clear();
                    deletedEntriesInBatch = 0;
                }
            }
        } finally {
            try {
                batch.flush();
                batch.clear();
            } finally {

                firstKeyWrapper.recycle();
                lastKeyWrapper.recycle();
                keyToDelete.recycle();
                batch.close();
            }
        }

        log.info("Deleted indexes for {} entries from {} ledgers in {} seconds", deletedEntries, ledgersToDelete.size(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) / 1000.0);

        // Removed from pending set
        for (long ledgerId : ledgersToDelete) {
            deletedLedgers.remove(ledgerId);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(EntryLocationIndex.class);
}
