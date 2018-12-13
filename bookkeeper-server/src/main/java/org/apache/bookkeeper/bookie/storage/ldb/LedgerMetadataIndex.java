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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains an index for the ledgers metadata.
 *
 * <p>The key is the ledgerId and the value is the {@link LedgerData} content.
 */
public class LedgerMetadataIndex implements Closeable {
    // Contains all ledgers stored in the bookie
    private final ConcurrentLongHashMap<LedgerData> ledgers;
    private final AtomicInteger ledgersCount;

    private final KeyValueStorage ledgersDb;
    private final LedgerMetadataIndexStats stats;

    // Holds ledger modifications applied in memory map, and pending to be flushed on db
    private final ConcurrentLinkedQueue<Entry<Long, LedgerData>> pendingLedgersUpdates;

    // Holds ledger ids that were delete from memory map, and pending to be flushed on db
    private final ConcurrentLinkedQueue<Long> pendingDeletedLedgers;

    public LedgerMetadataIndex(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
            StatsLogger stats) throws IOException {
        String ledgersPath = FileSystems.getDefault().getPath(basePath, "ledgers").toFile().toString();
        ledgersDb = storageFactory.newKeyValueStorage(ledgersPath, DbConfigType.Small, conf);

        ledgers = new ConcurrentLongHashMap<>();
        ledgersCount = new AtomicInteger();

        // Read all ledgers from db
        CloseableIterator<Entry<byte[], byte[]>> iterator = ledgersDb.iterator();
        try {
            while (iterator.hasNext()) {
                Entry<byte[], byte[]> entry = iterator.next();
                long ledgerId = ArrayUtil.getLong(entry.getKey(), 0);
                LedgerData ledgerData = LedgerData.parseFrom(entry.getValue());
                ledgers.put(ledgerId, ledgerData);
                ledgersCount.incrementAndGet();
            }
        } finally {
            iterator.close();
        }

        this.pendingLedgersUpdates = new ConcurrentLinkedQueue<Entry<Long, LedgerData>>();
        this.pendingDeletedLedgers = new ConcurrentLinkedQueue<Long>();

        this.stats = new LedgerMetadataIndexStats(
            stats,
            () -> (long) ledgersCount.get());
    }

    @Override
    public void close() throws IOException {
        ledgersDb.close();
    }

    public LedgerData get(long ledgerId) throws IOException {
        LedgerData ledgerData = ledgers.get(ledgerId);
        if (ledgerData == null) {
            if (log.isDebugEnabled()) {
                log.debug("Ledger not found {}", ledgerId);
            }
            throw new Bookie.NoLedgerException(ledgerId);
        }

        return ledgerData;
    }

    public void set(long ledgerId, LedgerData ledgerData) throws IOException {
        ledgerData = LedgerData.newBuilder(ledgerData).setExists(true).build();

        if (ledgers.put(ledgerId, ledgerData) == null) {
            if (log.isDebugEnabled()) {
                log.debug("Added new ledger {}", ledgerId);
            }
            ledgersCount.incrementAndGet();
        }

        pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, ledgerData));
        pendingDeletedLedgers.remove(ledgerId);
    }

    public void delete(long ledgerId) throws IOException {
        if (ledgers.remove(ledgerId) != null) {
            if (log.isDebugEnabled()) {
                log.debug("Removed ledger {}", ledgerId);
            }
            ledgersCount.decrementAndGet();
        }

        pendingDeletedLedgers.add(ledgerId);
        pendingLedgersUpdates.removeIf(e -> e.getKey() == ledgerId);
    }

    public Iterable<Long> getActiveLedgersInRange(final long firstLedgerId, final long lastLedgerId)
            throws IOException {
        return Iterables.filter(ledgers.keys(), new Predicate<Long>() {
            @Override
            public boolean apply(Long ledgerId) {
                return ledgerId >= firstLedgerId && ledgerId < lastLedgerId;
            }
        });
    }

    public boolean setFenced(long ledgerId) throws IOException {
        LedgerData ledgerData = get(ledgerId);
        if (ledgerData.getFenced()) {
            return false;
        }

        LedgerData newLedgerData = LedgerData.newBuilder(ledgerData).setFenced(true).build();

        if (ledgers.put(ledgerId, newLedgerData) == null) {
            // Ledger had been deleted
            if (log.isDebugEnabled()) {
                log.debug("Re-inserted fenced ledger {}", ledgerId);
            }
            ledgersCount.incrementAndGet();
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Set fenced ledger {}", ledgerId);
            }
        }

        pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, newLedgerData));
        pendingDeletedLedgers.remove(ledgerId);
        return true;
    }

    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        LedgerData ledgerData = ledgers.get(ledgerId);
        if (ledgerData == null) {
            // New ledger inserted
            ledgerData = LedgerData.newBuilder().setExists(true).setFenced(false)
                    .setMasterKey(ByteString.copyFrom(masterKey)).build();
            if (log.isDebugEnabled()) {
                log.debug("Inserting new ledger {}", ledgerId);
            }
        } else {
            byte[] storedMasterKey = ledgerData.getMasterKey().toByteArray();
            if (ArrayUtil.isArrayAllZeros(storedMasterKey)) {
                // update master key of the ledger
                ledgerData = LedgerData.newBuilder(ledgerData).setMasterKey(ByteString.copyFrom(masterKey)).build();
                if (log.isDebugEnabled()) {
                    log.debug("Replace old master key {} with new master key {}", storedMasterKey, masterKey);
                }
            } else if (!Arrays.equals(storedMasterKey, masterKey) && !ArrayUtil.isArrayAllZeros(masterKey)) {
                log.warn("Ledger {} masterKey in db can only be set once.", ledgerId);
                throw new IOException(BookieException.create(BookieException.Code.IllegalOpException));
            }
        }

        if (ledgers.put(ledgerId, ledgerData) == null) {
            ledgersCount.incrementAndGet();
        }

        pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, ledgerData));
        pendingDeletedLedgers.remove(ledgerId);
    }

    /**
     * Flushes all pending changes.
     */
    public void flush() throws IOException {
        LongWrapper key = LongWrapper.get();

        int updatedLedgers = 0;
        while (!pendingLedgersUpdates.isEmpty()) {
            Entry<Long, LedgerData> entry = pendingLedgersUpdates.poll();
            key.set(entry.getKey());
            byte[] value = entry.getValue().toByteArray();
            ledgersDb.put(key.array, value);
            ++updatedLedgers;
        }

        if (log.isDebugEnabled()) {
            log.debug("Persisting updates to {} ledgers", updatedLedgers);
        }

        ledgersDb.sync();
        key.recycle();
    }

    public void removeDeletedLedgers() throws IOException {
        LongWrapper key = LongWrapper.get();

        int deletedLedgers = 0;
        while (!pendingDeletedLedgers.isEmpty()) {
            long ledgerId = pendingDeletedLedgers.poll();
            key.set(ledgerId);
            ledgersDb.delete(key.array);
            deletedLedgers++;
        }

        if (log.isDebugEnabled()) {
            log.debug("Persisting deletes of ledgers {}", deletedLedgers);
        }

        ledgersDb.sync();
        key.recycle();
    }

    private static final Logger log = LoggerFactory.getLogger(LedgerMetadataIndex.class);
}
