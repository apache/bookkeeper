/*
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
 */

package org.apache.bookkeeper.bookie;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A mock for running tests that require ledger storage.
 */
public class MockLedgerStorage implements CompactableLedgerStorage {

    private static class LedgerInfo {
        boolean limbo = false;
        boolean fenced = false;
        long lac = -1;
        final byte[] masterKey;

        LedgerInfo(byte[] masterKey) {
            this.masterKey = Arrays.copyOf(masterKey, masterKey.length);
        }

        ConcurrentHashMap<Long, ByteBuf> entries = new ConcurrentHashMap<>();
    }

    private final ConcurrentHashMap<Long, LedgerInfo> ledgers = new ConcurrentHashMap<>();
    private final EnumSet<StorageState> storageStateFlags = EnumSet.noneOf(StorageState.class);
    private final List<EntryLocation> entryLocations = new ArrayList<>();

    @Override
    public void initialize(ServerConfiguration conf,
                           LedgerManager ledgerManager,
                           LedgerDirsManager ledgerDirsManager,
                           LedgerDirsManager indexDirsManager,
                           StatsLogger statsLogger,
                           ByteBufAllocator allocator)
            throws IOException {}

    @Override
    public void setStateManager(StateManager stateManager) {}
    @Override
    public void setCheckpointSource(CheckpointSource checkpointSource) {}
    @Override
    public void setCheckpointer(Checkpointer checkpointer) {}

    @Override
    public void start() {}
    @Override
    public void shutdown() throws InterruptedException {}

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return ledgers.containsKey(ledgerId);
    }

    @Override
    public boolean entryExists(long ledgerId, long entryId) throws IOException {
        LedgerInfo info = ledgers.get(ledgerId);
        if (info == null) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
        return info != null && info.entries.containsKey(entryId);
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        AtomicBoolean ret = new AtomicBoolean(false);
        LedgerInfo previous = ledgers.computeIfPresent(ledgerId, (ledgerId1, current) -> {
                if (!current.fenced) {
                    current.fenced = true;
                    ret.set(true);
                } else {
                    ret.set(false);
                }
                return current;
            });
        if (previous == null) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
        return ret.get();
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        LedgerInfo info = ledgers.get(ledgerId);
        if (info == null) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
        return info != null && info.fenced;
    }

    @Override
    public void setLimboState(long ledgerId) throws IOException {
        LedgerInfo previous = ledgers.computeIfPresent(ledgerId, (ledgerId1, current) -> {
                current.limbo = true;
                return current;
            });
        if (previous == null) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
    }

    @Override
    public boolean hasLimboState(long ledgerId) throws IOException {
        LedgerInfo info = ledgers.get(ledgerId);
        if (info == null) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
        return info.limbo;
    }
    @Override
    public void clearLimboState(long ledgerId) throws IOException {
        LedgerInfo previous = ledgers.computeIfPresent(ledgerId, (ledgerId1, current) -> {
                current.limbo = false;
                return current;
            });
        if (previous == null) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        LedgerInfo previous = ledgers.compute(ledgerId, (ledgerId1, current) -> {
                if (current != null) {
                    return current;
                }
                return new LedgerInfo(masterKey);
            });
        if (previous != null && !Arrays.equals(masterKey, previous.masterKey)) {
            throw new IOException(BookieException.create(BookieException.Code.IllegalOpException));
        }
    }
    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        LedgerInfo info = ledgers.get(ledgerId);
        if (info == null) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
        return Arrays.copyOf(info.masterKey, info.masterKey.length);
    }

    public long extractLedgerId(ByteBuf entry) {
        return entry.getLong(entry.readerIndex());
    }

    public long extractEntryId(ByteBuf entry) {
        return entry.getLong(entry.readerIndex() + 8);
    }

    public long extractLac(ByteBuf entry) {
        return entry.getLong(entry.readerIndex() + 16);

    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException, BookieException {
        ByteBuf copy = entry.retain().duplicate();
        long ledgerId = extractLedgerId(copy);
        long entryId = extractEntryId(copy);
        long lac = extractLac(copy);

        LedgerInfo previous = ledgers.computeIfPresent(ledgerId, (ledgerId1, current) -> {
                if (lac > current.lac) {
                    current.lac = lac;
                }
                current.entries.put(entryId, copy);
                return current;
            });
        if (previous == null) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
        return entryId;
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(
            long ledgerId,
            long previousLAC,
            Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(
            long ledgerId,
            Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public void flush() throws IOException {
        // this is a noop, as we dont hit disk anyhow
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        ledgers.remove(ledgerId);
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public LedgerStorage getUnderlyingLedgerStorage() {
        return CompactableLedgerStorage.super.getUnderlyingLedgerStorage();
    }

    @Override
    public void forceGC() {
        CompactableLedgerStorage.super.forceGC();
    }

    @Override
    public void forceGC(boolean forceMajor, boolean forceMinor) {
        CompactableLedgerStorage.super.forceGC(forceMajor, forceMinor);
    }

    public void suspendMinorGC() {
        CompactableLedgerStorage.super.suspendMinorGC();
    }

    public void suspendMajorGC() {
        CompactableLedgerStorage.super.suspendMajorGC();
    }

    public void resumeMinorGC() {
        CompactableLedgerStorage.super.resumeMinorGC();
    }

    public void resumeMajorGC() {
        CompactableLedgerStorage.super.suspendMajorGC();
    }

    public boolean isMajorGcSuspended() {
        return CompactableLedgerStorage.super.isMajorGcSuspended();
    }

    public boolean isMinorGcSuspended() {
        return CompactableLedgerStorage.super.isMinorGcSuspended();
    }

    @Override
    public List<DetectedInconsistency> localConsistencyCheck(Optional<RateLimiter> rateLimiter) throws IOException {
        return CompactableLedgerStorage.super.localConsistencyCheck(rateLimiter);
    }

    @Override
    public boolean isInForceGC() {
        return CompactableLedgerStorage.super.isInForceGC();
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        return CompactableLedgerStorage.super.getGarbageCollectionStatus();
    }

    @Override
    public PrimitiveIterator.OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId)
            throws IOException {
        throw new UnsupportedOperationException("Not supported in mock, implement if you need it");
    }

    public List<EntryLocation> getUpdatedLocations() {
        return entryLocations;
    }

    @Override
    public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        synchronized (entryLocations) {
            for (EntryLocation l : locations) {
                entryLocations.add(l);
            }
        }
    }

    @Override
    public EnumSet<StorageState> getStorageStateFlags() throws IOException {
        return storageStateFlags;
    }

    @Override
    public void setStorageStateFlag(StorageState flag) throws IOException {
        storageStateFlags.add(flag);
    }

    @Override
    public void clearStorageStateFlag(StorageState flag) throws IOException {
        storageStateFlags.remove(flag);
    }

    @Override
    public void flushEntriesLocationsIndex() throws IOException { }
}
