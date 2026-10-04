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
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
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
     * An unflushed ledger's LAC lives in a FileInfo cache entry. Once that entry is
     * evicted, the index file still does not exist, so the LAC has to come from the memtable.
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
        SortedLedgerStorage storage = new SortedLedgerStorage();
        storage.initialize(limited, null, dirs, dirs,
                statsProvider.getStatsLogger(BOOKIE_SCOPE), UnpooledByteBufAllocator.DEFAULT);
        storage.setCheckpointSource(checkpointSource);
        storage.setCheckpointer(checkpointer);
        try {
            long lac = 3L;
            int ledgers = 12;
            for (long ledgerId = 0; ledgerId < ledgers; ledgerId++) {
                storage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(0L);
                entry.writeLong(lac);
                entry.writeByte(1);
                storage.addEntry(entry);
            }
            assertEquals("LAC of an unflushed ledger after file-info eviction",
                    lac, storage.getLastAddConfirmed(0L));
            storage.flush();
            assertEquals("LAC still readable after the memtable is flushed",
                    lac, storage.getLastAddConfirmed(0L));
            try {
                storage.getLastAddConfirmed(9_999L);
                assertTrue("missing ledger should not report a LAC", false);
            } catch (Bookie.NoLedgerException | Bookie.NoEntryException expected) {
                // index has no file and the memtable has no entry
            }
        } finally {
            storage.shutdown();
        }
    }
}
