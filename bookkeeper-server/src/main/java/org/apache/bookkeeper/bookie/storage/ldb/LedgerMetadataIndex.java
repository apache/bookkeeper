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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
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
    // Non-ledger data should have negative ID
    private static final long STORAGE_FLAGS = -0xeefd;

    // Contains all ledgers stored in the bookie
    private final ConcurrentLongHashMap<LedgerData> ledgers;
    private final AtomicInteger ledgersCount;

    private final KeyValueStorage ledgersDb;
    private final LedgerMetadataIndexStats stats;

    // Holds ledger modifications applied in memory map, and pending to be flushed on db
    private final ConcurrentLinkedQueue<Entry<Long, LedgerData>> pendingLedgersUpdates;

    // Holds ledger ids that were delete from memory map, and pending to be flushed on db
    private final Set<Long> pendingDeletedLedgers;
    private final ReentrantLock[] locks = new ReentrantLock[16];

    public LedgerMetadataIndex(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
            StatsLogger stats) throws IOException {
        ledgersDb = storageFactory.newKeyValueStorage(basePath, "ledgers", DbConfigType.LedgerMetadata, conf);

        ledgers = ConcurrentLongHashMap.<LedgerData>newBuilder().build();
        ledgersCount = new AtomicInteger();

        // Read all ledgers from db
        CloseableIterator<Entry<byte[], byte[]>> iterator = ledgersDb.iterator();
        try {
            while (iterator.hasNext()) {
                Entry<byte[], byte[]> entry = iterator.next();
                long ledgerId = ArrayUtil.getLong(entry.getKey(), 0);
                if (ledgerId >= 0) {
                    LedgerData ledgerData = LedgerData.parseFrom(entry.getValue());
                    ledgers.put(ledgerId, ledgerData);
                    ledgersCount.incrementAndGet();
                }
            }
        } finally {
            iterator.close();
        }

        this.pendingLedgersUpdates = new ConcurrentLinkedQueue<Entry<Long, LedgerData>>();
        this.pendingDeletedLedgers = Sets.newConcurrentHashSet();

        this.stats = new LedgerMetadataIndexStats(
            stats,
            () -> (long) ledgersCount.get());

        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
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

        ReentrantLock lock = lockForLedger(ledgerId);
        lock.lock();
        try {
            if (ledgers.put(ledgerId, ledgerData) == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Added new ledger {}", ledgerId);
                }
                ledgersCount.incrementAndGet();
            }

            pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, ledgerData));
            pendingDeletedLedgers.remove(ledgerId);
        } finally {
            lock.unlock();
        }
    }

    public void delete(long ledgerId) throws IOException {
        ReentrantLock lock = lockForLedger(ledgerId);
        lock.lock();
        try {
            if (ledgers.remove(ledgerId) != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Removed ledger {}", ledgerId);
                }
                ledgersCount.decrementAndGet();
            }

            pendingDeletedLedgers.add(ledgerId);
            pendingLedgersUpdates.removeIf(e -> e.getKey() == ledgerId);
        } finally {
            lock.unlock();
        }
    }

    public Iterable<Long> getActiveLedgersInRange(final long firstLedgerId, final long lastLedgerId)
            throws IOException {
        if (firstLedgerId <= 0 && lastLedgerId == Long.MAX_VALUE) {
            return ledgers.keys();
        }
        return Iterables.filter(ledgers.keys(), new Predicate<Long>() {
            @Override
            public boolean apply(Long ledgerId) {
                return ledgerId >= firstLedgerId && ledgerId < lastLedgerId;
            }
        });
    }

    public boolean setFenced(long ledgerId) throws IOException {
        ReentrantLock lock = lockForLedger(ledgerId);
        lock.lock();
        try {
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
            } else if (log.isDebugEnabled()) {
                log.debug("Set fenced ledger {}", ledgerId);
            }

            pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, newLedgerData));
            pendingDeletedLedgers.remove(ledgerId);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean setLimbo(long ledgerId) throws IOException {
        ReentrantLock lock = lockForLedger(ledgerId);
        lock.lock();
        try {
            LedgerData ledgerData = get(ledgerId);
            if (ledgerData.getLimbo()) {
                return false;
            }

            LedgerData newLedgerData = LedgerData.newBuilder(ledgerData).setLimbo(true).build();

            if (ledgers.put(ledgerId, newLedgerData) == null) {
                // Ledger had been deleted
                if (log.isDebugEnabled()) {
                    log.debug("Re-inserted limbo ledger {}", ledgerId);
                }
                ledgersCount.incrementAndGet();
            } else if (log.isDebugEnabled()) {
                log.debug("Set limbo ledger {}", ledgerId);
            }

            pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, newLedgerData));
            pendingDeletedLedgers.remove(ledgerId);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean clearLimbo(long ledgerId) throws IOException {
        ReentrantLock lock = lockForLedger(ledgerId);
        lock.lock();
        try {
            LedgerData ledgerData = get(ledgerId);
            if (ledgerData == null) {
                throw new Bookie.NoLedgerException(ledgerId);
            }
            final boolean oldValue = ledgerData.getLimbo();
            LedgerData newLedgerData = LedgerData.newBuilder(ledgerData).setLimbo(false).build();

            if (ledgers.put(ledgerId, newLedgerData) == null) {
                // Ledger had been deleted
                if (log.isDebugEnabled()) {
                    log.debug("Re-inserted limbo ledger {}", ledgerId);
                }
                ledgersCount.incrementAndGet();
            } else if (log.isDebugEnabled()) {
                log.debug("Set limbo ledger {}", ledgerId);
            }

            pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, newLedgerData));
            pendingDeletedLedgers.remove(ledgerId);
            return oldValue;
        } finally {
            lock.unlock();
        }
    }


    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        ReentrantLock lock = lockForLedger(ledgerId);
        lock.lock();
        try {
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
        } finally {
            lock.unlock();
        }
    }

    /**
     * Flushes all pending changes.
     */
    public void flush() throws IOException {
        LongWrapper key = LongWrapper.get();

        try {
            int updatedLedgers = 0;
            Entry<Long, LedgerData> entry;
            while ((entry = pendingLedgersUpdates.poll()) != null) {
                key.set(entry.getKey());
                byte[] value = entry.getValue().toByteArray();
                ledgersDb.put(key.array, value);
                ++updatedLedgers;
            }

            if (log.isDebugEnabled()) {
                log.debug("Persisting updates to {} ledgers", updatedLedgers);
            }

            ledgersDb.sync();
        } finally {
            key.recycle();
        }
    }

    public void removeDeletedLedgers() throws IOException {
        LongWrapper key = LongWrapper.get();

        try {
            int deletedLedgers = 0;
            for (Long ledgerId : pendingDeletedLedgers) {
                key.set(ledgerId);
                ledgersDb.delete(key.array);
                ++deletedLedgers;
            }

            if (log.isDebugEnabled()) {
                log.debug("Persisting deletes of ledgers {}", deletedLedgers);
            }

            ledgersDb.sync();
        } finally {
            key.recycle();
        }
    }

    private ReentrantLock lockForLedger(long ledgerId) {
        return locks[Math.abs((int) ledgerId) % locks.length];
    }

    int getStorageStateFlags() throws IOException {
        LongWrapper keyWrapper = LongWrapper.get();
        LongWrapper currentWrapper = LongWrapper.get();

        try {
            keyWrapper.set(STORAGE_FLAGS);
            synchronized (ledgersDb) {
                int current = 0;
                if (ledgersDb.get(keyWrapper.array, currentWrapper.array) >= 0) {
                    current = (int) currentWrapper.getValue();
                }
                return current;
            }
        } finally {
            keyWrapper.recycle();
            currentWrapper.recycle();
        }
    }

    boolean setStorageStateFlags(int expected, int newFlags) throws IOException {
        LongWrapper keyWrapper = LongWrapper.get();
        LongWrapper currentWrapper = LongWrapper.get();
        LongWrapper newFlagsWrapper = LongWrapper.get();

        try {
            keyWrapper.set(STORAGE_FLAGS);
            newFlagsWrapper.set(newFlags);
            synchronized (ledgersDb) {
                int current = 0;
                if (ledgersDb.get(keyWrapper.array, currentWrapper.array) >= 0) {
                    current = (int) currentWrapper.getValue();
                }
                if (current == expected) {
                    ledgersDb.put(keyWrapper.array, newFlagsWrapper.array);
                    ledgersDb.sync();
                    return true;
                }
            }
        } finally {
            keyWrapper.recycle();
            currentWrapper.recycle();
            newFlagsWrapper.recycle();
        }
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(LedgerMetadataIndex.class);

    void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        LedgerData ledgerData = ledgers.get(ledgerId);
        if (ledgerData != null) {
            LedgerData newLedgerData = LedgerData.newBuilder(ledgerData)
                    .setExplicitLac(ByteString.copyFrom(lac.nioBuffer())).build();

            if (ledgers.put(ledgerId, newLedgerData) == null) {
                // Ledger had been deleted
                return;
            } else if (log.isDebugEnabled()) {
                log.debug("Set explicitLac on ledger {}", ledgerId);
            }
            pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, newLedgerData));
        } else {
            // unknown ledger here
        }
    }

}
