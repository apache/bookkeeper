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
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PrimitiveIterator.OfLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Testing SortedLedgerStorage.
 */
@RunWith(Parameterized.class)
public class SortedLedgerStorageTest {

    TestStatsProvider statsProvider = new TestStatsProvider();
    ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
    LedgerDirsManager ledgerDirsManager;
    SortedLedgerStorage sortedLedgerStorage = new SortedLedgerStorage();

    final long numWrites = 2000;
    final long moreNumOfWrites = 3000;
    final long entriesPerWrite = 2;
    final long numOfLedgers = 5;

    @Parameterized.Parameters
    public static Iterable<Boolean> elplSetting() {
        return Arrays.asList(true, false);
    }

    public SortedLedgerStorageTest(boolean elplSetting) {
        conf.setEntryLogSizeLimit(2048);
        conf.setEntryLogPerLedgerEnabled(elplSetting);
    }

    CheckpointSource checkpointSource = new CheckpointSource() {
        @Override
        public Checkpoint newCheckpoint() {
            return Checkpoint.MAX;
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        }
    };

    Checkpointer checkpointer = new Checkpointer() {
        @Override
        public void startCheckpoint(Checkpoint checkpoint) {
            // No-op
        }

        @Override
        public void start() {
            // no-op
        }
    };

    @Before
    public void setUp() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        sortedLedgerStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                                       statsProvider.getStatsLogger(BOOKIE_SCOPE), UnpooledByteBufAllocator.DEFAULT);
        sortedLedgerStorage.setCheckpointSource(checkpointSource);
        sortedLedgerStorage.setCheckpointer(checkpointer);
    }

    @Test
    public void testGetListOfEntriesOfLedger() throws Exception {
        long nonExistingLedgerId = 123456L;
        OfLong entriesItr = sortedLedgerStorage.getListOfEntriesOfLedger(nonExistingLedgerId);
        assertFalse("There shouldn't be any entries for this ledger", entriesItr.hasNext());
        // Insert some ledger & entries in the interleaved storage
        for (long entryId = 0; entryId < numWrites; entryId++) {
            for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
                if (entryId == 0) {
                    sortedLedgerStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
                    sortedLedgerStorage.setFenced(ledgerId);
                }
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * entriesPerWrite);
                entry.writeBytes(("entry-" + entryId).getBytes());

                sortedLedgerStorage.addEntry(entry);
            }
        }

        for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
            OfLong entriesOfLedger = sortedLedgerStorage.getListOfEntriesOfLedger(ledgerId);
            ArrayList<Long> arrayList = new ArrayList<Long>();
            Consumer<Long> addMethod = arrayList::add;
            entriesOfLedger.forEachRemaining(addMethod);
            assertEquals("Number of entries", numWrites, arrayList.size());
            assertTrue("Entries of Ledger", IntStream.range(0, arrayList.size()).allMatch(i -> {
                return arrayList.get(i) == (i * entriesPerWrite);
            }));
        }

        nonExistingLedgerId = 456789L;
        entriesItr = sortedLedgerStorage.getListOfEntriesOfLedger(nonExistingLedgerId);
        assertFalse("There shouldn't be any entry", entriesItr.hasNext());
    }

    @Test
    public void testGetListOfEntriesOfLedgerAfterFlush() throws IOException {
        // Insert some ledger & entries in the interleaved storage
        for (long entryId = 0; entryId < numWrites; entryId++) {
            for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
                if (entryId == 0) {
                    sortedLedgerStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
                    sortedLedgerStorage.setFenced(ledgerId);
                }
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * entriesPerWrite);
                entry.writeBytes(("entry-" + entryId).getBytes());

                sortedLedgerStorage.addEntry(entry);
            }
        }

        sortedLedgerStorage.flush();

        // Insert some more ledger & entries in the interleaved storage
        for (long entryId = numWrites; entryId < moreNumOfWrites; entryId++) {
            for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId * entriesPerWrite);
                entry.writeBytes(("entry-" + entryId).getBytes());

                sortedLedgerStorage.addEntry(entry);
            }
        }

        for (long ledgerId = 0; ledgerId < numOfLedgers; ledgerId++) {
            OfLong entriesOfLedger = sortedLedgerStorage.getListOfEntriesOfLedger(ledgerId);
            ArrayList<Long> arrayList = new ArrayList<Long>();
            Consumer<Long> addMethod = arrayList::add;
            entriesOfLedger.forEachRemaining(addMethod);
            assertEquals("Number of entries", moreNumOfWrites, arrayList.size());
            assertTrue("Entries of Ledger", IntStream.range(0, arrayList.size()).allMatch(i -> {
                return arrayList.get(i) == (i * entriesPerWrite);
            }));
        }
    }

    /**
     * A newer memtable LAC must remain visible after an evicted FileInfo is flushed.
     */
    @Test
    public void testGetLastAddConfirmedAfterFileInfoEviction() throws Exception {
        File tmpDir = File.createTempFile("bkLac", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(tmpDir));

        ServerConfiguration limited = TestBKConfiguration.newServerConfiguration();
        limited.setEntryLogPerLedgerEnabled(conf.isEntryLogPerLedgerEnabled());
        limited.setLedgerDirNames(new String[] { tmpDir.toString() });
        limited.setOpenFileLimit(2);
        limited.setNumAddWorkerThreads(1);
        limited.setNumReadWorkerThreads(1);
        limited.setSkipListSizeLimit(64 * 1024 * 1024);

        LedgerDirsManager dirs = new LedgerDirsManager(limited, limited.getLedgerDirs(),
                new DiskChecker(limited.getDiskUsageThreshold(), limited.getDiskUsageWarnThreshold()));
        InterleavedLedgerStorage underlying = new InterleavedLedgerStorage();
        SortedLedgerStorage storage = new SortedLedgerStorage(underlying);
        storage.initialize(limited, null, dirs, dirs,
                statsProvider.getStatsLogger(BOOKIE_SCOPE), UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(CheckpointSource.DEFAULT);
        storage.setCheckpointer(checkpointer);
        try {
            long ledgerId = 0L;
            storage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            for (long entryId = 0; entryId < 5; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeLong(entryId - 1);
                entry.writeByte(1);
                storage.addEntry(entry);
            }
            storage.flush();

            for (long entryId = 5; entryId < 10; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeLong(entryId - 1);
                entry.writeByte(1);
                storage.addEntry(entry);
            }
            LedgerCacheImpl ledgerCache = (LedgerCacheImpl) underlying.ledgerCache;
            IndexPersistenceMgr indexPersistenceManager = ledgerCache.getIndexPersistenceManager();
            indexPersistenceManager.readFileInfoCache.invalidate(ledgerId);
            indexPersistenceManager.writeFileInfoCache.invalidate(ledgerId);
            indexPersistenceManager.readFileInfoCache.cleanUp();
            indexPersistenceManager.writeFileInfoCache.cleanUp();

            assertEquals("LAC of an evicted ledger must include newer memtable entries",
                    8L, storage.getLastAddConfirmed(ledgerId));
            storage.flush();
            assertTrue("memtable must be empty after a real flush", storage.memTable.isEmpty());
            assertEquals("LAC must not move backwards after the memtable is flushed",
                    8L, storage.getLastAddConfirmed(ledgerId));
            assertThrows(Bookie.NoLedgerException.class, () -> storage.getLastAddConfirmed(9_999L));
        } finally {
            storage.shutdown();
        }
    }

    @Test
    public void testGetLastAddConfirmedRetriesAfterIndexMiss() throws Exception {
        InterleavedLedgerStorage underlying = mock(InterleavedLedgerStorage.class);
        LedgerCache ledgerCache = mock(LedgerCache.class);
        when(underlying.getLedgerCache()).thenReturn(ledgerCache);
        when(ledgerCache.getLastAddConfirmed(0L)).thenReturn(null);
        when(underlying.getLastAddConfirmed(0L))
                .thenThrow(new Bookie.NoEntryException(0L, -1L))
                .thenReturn(1L);
        SortedLedgerStorage storage = new SortedLedgerStorage(underlying);
        storage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                statsProvider.getStatsLogger(BOOKIE_SCOPE), UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(checkpointer);
        try {
            assertEquals(1L, storage.getLastAddConfirmed(0L));
            verify(underlying, times(2)).getLastAddConfirmed(0L);
        } finally {
            storage.shutdown();
        }
    }

    @Test
    public void testGetLastAddConfirmedUsesCachedValueBeforeMemTable() throws Exception {
        InterleavedLedgerStorage underlying = mock(InterleavedLedgerStorage.class);
        LedgerCache ledgerCache = mock(LedgerCache.class);
        when(underlying.getLedgerCache()).thenReturn(ledgerCache);
        when(ledgerCache.getLastAddConfirmed(0L)).thenReturn(8L);
        SortedLedgerStorage storage = new SortedLedgerStorage(underlying);
        storage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                statsProvider.getStatsLogger(BOOKIE_SCOPE), UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(checkpointer);
        EntryMemTable memTable = spy(storage.memTable);
        storage.memTable = memTable;
        try {
            assertEquals(8L, storage.getLastAddConfirmed(0L));
            verify(memTable, times(0)).getLastEntry(0L);
            verify(underlying, times(0)).getLastAddConfirmed(0L);
        } finally {
            storage.shutdown();
        }
    }

    @Test
    public void testGetLastAddConfirmedFallsBackToMemTableWhenCacheMisses() throws Exception {
        InterleavedLedgerStorage underlying = mock(InterleavedLedgerStorage.class);
        LedgerCache ledgerCache = mock(LedgerCache.class);
        when(underlying.getLedgerCache()).thenReturn(ledgerCache);
        when(ledgerCache.getLastAddConfirmed(0L)).thenThrow(new Bookie.NoLedgerException(0L));
        when(ledgerCache.updateLastAddConfirmed(0L, 8L)).thenReturn(8L);
        SortedLedgerStorage storage = new SortedLedgerStorage(underlying);
        storage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                statsProvider.getStatsLogger(BOOKIE_SCOPE), UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(checkpointer);
        try {
            ByteBuffer entry = ByteBuffer.allocate(3 * Long.BYTES);
            entry.putLong(0L).putLong(9L).putLong(8L).flip();
            storage.memTable.addEntry(0L, 9L, entry, storage);

            assertEquals(8L, storage.getLastAddConfirmed(0L));
            verify(ledgerCache).updateLastAddConfirmed(0L, 8L);
            verify(underlying, times(0)).getLastAddConfirmed(0L);
        } finally {
            storage.shutdown();
        }
    }
}
