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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test the EntryMemTable class.
 */
@Slf4j
@RunWith(Parameterized.class)
public class EntryMemTableTest implements CacheCallback, SkipListFlusher, CheckpointSource {

    private Class entryMemTableClass;
    private EntryMemTable memTable;
    private final Random random = new Random();
    private TestCheckPoint curCheckpoint = new TestCheckPoint(0, 0);

    @Parameters
    public static Collection<Object[]> memTableClass() {
        return Arrays.asList(new Object[][] { { EntryMemTable.class }, { EntryMemTableWithParallelFlusher.class } });
    }

    public EntryMemTableTest(Class entryMemTableClass) {
        this.entryMemTableClass = entryMemTableClass;
    }

    @Override
    public Checkpoint newCheckpoint() {
        return curCheckpoint;
    }

    @Override
    public void checkpointComplete(Checkpoint checkpoint, boolean compact)
            throws IOException {
    }

    @Before
    public void setUp() throws Exception {
        if (entryMemTableClass.equals(EntryMemTableWithParallelFlusher.class)) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            this.memTable = new EntryMemTableWithParallelFlusher(conf, this, NullStatsLogger.INSTANCE);
        } else {
            this.memTable = new EntryMemTable(TestBKConfiguration.newServerConfiguration(), this,
                    NullStatsLogger.INSTANCE);
        }
    }

    @After
    public void cleanup() throws Exception{
        this.memTable.close();
    }

    @Test
    public void testLogMark() throws IOException {
        LogMark mark = new LogMark();
        assertTrue(mark.compare(new LogMark()) == 0);
        assertTrue(mark.compare(LogMark.MAX_VALUE) < 0);
        mark.setLogMark(3, 11);
        byte[] data = new byte[16];
        ByteBuffer buf = ByteBuffer.wrap(data);
        mark.writeLogMark(buf);
        buf.flip();
        LogMark mark1 = new LogMark(9, 13);
        assertTrue(mark1.compare(mark) > 0);
        mark1.readLogMark(buf);
        assertTrue(mark1.compare(mark) == 0);
    }

    /**
     * Basic put/get.
     * @throws IOException
     * */
    @Test
    public void testBasicOps() throws IOException {
        long ledgerId = 1;
        long entryId = 1;
        byte[] data = new byte[10];
        random.nextBytes(data);
        ByteBuffer buf = ByteBuffer.wrap(data);
        memTable.addEntry(ledgerId, entryId, buf, this);
        buf.rewind();
        EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
        assertTrue(kv.getLedgerId() == ledgerId);
        assertTrue(kv.getEntryId() == entryId);
        assertTrue(kv.getValueAsByteBuffer().nioBuffer().equals(buf));
        memTable.flush(this);
    }

    @Override
    public void onSizeLimitReached(Checkpoint cp) throws IOException {
        // No-op
    }

    public void process(long ledgerId, long entryId, ByteBuf entry)
            throws IOException {
        // No-op
    }

    /**
     * Test read/write across snapshot.
     * @throws IOException
     */
    @Test
    public void testScanAcrossSnapshot() throws IOException {
        byte[] data = new byte[10];
        List<EntryKeyValue> keyValues = new ArrayList<EntryKeyValue>();
        for (long entryId = 1; entryId < 100; entryId++) {
            for (long ledgerId = 1; ledgerId < 3; ledgerId++) {
                random.nextBytes(data);
                memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this);
                keyValues.add(memTable.getEntry(ledgerId, entryId));
                if (random.nextInt(16) == 0) {
                    memTable.snapshot();
                }
            }
        }

        for (EntryKeyValue kv : keyValues) {
            assertTrue(memTable.getEntry(kv.getLedgerId(), kv.getEntryId()).equals(kv));
        }
        memTable.flush(this, Checkpoint.MAX);
    }

    private class KVFLusher implements SkipListFlusher {
        final Set<EntryKeyValue> keyValues;

        KVFLusher(final Set<EntryKeyValue> keyValues) {
            this.keyValues = keyValues;
        }

        @Override
        public void process(long ledgerId, long entryId, ByteBuf entry) throws IOException {
            assertTrue(ledgerId + ":" + entryId + " is duplicate in store!",
                    keyValues.add(new EntryKeyValue(ledgerId, entryId, entry.array())));
        }
    }

    private class NoLedgerFLusher implements SkipListFlusher {
        @Override
        public void process(long ledgerId, long entryId, ByteBuf entry) throws IOException {
            throw new NoLedgerException(ledgerId);
        }
    }

    /**
     * Test flush w/ logMark parameter.
     * @throws IOException
     */
    @Test
    public void testFlushLogMark() throws IOException {
        Set<EntryKeyValue> flushedKVs = Collections.newSetFromMap(new ConcurrentHashMap<EntryKeyValue, Boolean>());
        KVFLusher flusher = new KVFLusher(flushedKVs);

        curCheckpoint.setCheckPoint(2, 2);

        byte[] data = new byte[10];
        long ledgerId = 100;
        for (long entryId = 1; entryId < 100; entryId++) {
            random.nextBytes(data);
            memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this);
        }

        assertNull(memTable.snapshot(new TestCheckPoint(1, 1)));
        assertNotNull(memTable.snapshot(new TestCheckPoint(3, 3)));

        assertTrue(0 < memTable.flush(flusher));
        assertTrue(0 == memTable.flush(flusher));

        curCheckpoint.setCheckPoint(4, 4);

        random.nextBytes(data);
        memTable.addEntry(ledgerId, 101, ByteBuffer.wrap(data), this);
        assertTrue(0 == memTable.flush(flusher));

        assertTrue(0 == memTable.flush(flusher, new TestCheckPoint(3, 3)));
        assertTrue(0 < memTable.flush(flusher, new TestCheckPoint(4, 5)));
    }

    /**
     * Test snapshot/flush interaction.
     * @throws IOException
     */
    @Test
    public void testFlushSnapshot() throws IOException {
        HashSet<EntryKeyValue> keyValues = new HashSet<EntryKeyValue>();
        Set<EntryKeyValue> flushedKVs = Collections.newSetFromMap(new ConcurrentHashMap<EntryKeyValue, Boolean>());
        KVFLusher flusher = new KVFLusher(flushedKVs);

        byte[] data = new byte[10];
        for (long entryId = 1; entryId < 100; entryId++) {
            for (long ledgerId = 1; ledgerId < 100; ledgerId++) {
                random.nextBytes(data);
                assertTrue(ledgerId + ":" + entryId + " is duplicate in mem-table!",
                        memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0);
                assertTrue(ledgerId + ":" + entryId + " is duplicate in hash-set!",
                        keyValues.add(memTable.getEntry(ledgerId, entryId)));
                if (random.nextInt(16) == 0) {
                    if (null != memTable.snapshot()) {
                        if (random.nextInt(2) == 0) {
                            memTable.flush(flusher);
                        }
                    }
                }
            }
        }

        memTable.flush(flusher, Checkpoint.MAX);
        for (EntryKeyValue kv : keyValues) {
            assertTrue("kv " + kv.toString() + " was not flushed!", flushedKVs.contains(kv));
        }
    }

    /**
     * Test NoLedger exception/flush interaction.
     * @throws IOException
     */
    @Test
    public void testNoLedgerException() throws IOException {
        NoLedgerFLusher flusher = new NoLedgerFLusher();

        byte[] data = new byte[10];
        for (long entryId = 1; entryId < 100; entryId++) {
            for (long ledgerId = 1; ledgerId < 100; ledgerId++) {
                random.nextBytes(data);
                if (random.nextInt(16) == 0) {
                    if (null != memTable.snapshot()) {
                        memTable.flush(flusher);
                    }
                }
            }
        }

        memTable.flush(flusher, Checkpoint.MAX);
    }

    private static class TestCheckPoint implements Checkpoint {

        LogMark mark;

        public TestCheckPoint(long fid, long fpos) {
            mark = new LogMark(fid, fpos);
        }

        private void setCheckPoint(long fid, long fpos) {
            mark.setLogMark(fid, fpos);
        }

        @Override
        public int compareTo(Checkpoint o) {
            if (Checkpoint.MAX == o) {
                return -1;
            }
            return mark.compare(((TestCheckPoint) o).mark);
        }

    }

    @Test
    public void testGetListOfEntriesOfLedger() throws IOException {
        Set<EntryKeyValue> flushedKVs = Collections.newSetFromMap(new ConcurrentHashMap<EntryKeyValue, Boolean>());
        KVFLusher flusher = new KVFLusher(flushedKVs);
        int numofEntries = 100;
        int numOfLedgers = 5;
        byte[] data = new byte[10];
        for (long entryId = 1; entryId <= numofEntries; entryId++) {
            for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
                random.nextBytes(data);
                assertTrue(ledgerId + ":" + entryId + " is duplicate in mem-table!",
                        memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0);
            }
        }
        for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
            OfLong entriesItr = memTable.getListOfEntriesOfLedger((random.nextInt((int) ledgerId) + 1));
            ArrayList<Long> listOfEntries = new ArrayList<Long>();
            Consumer<Long> addMethod = listOfEntries::add;
            entriesItr.forEachRemaining(addMethod);
            assertEquals("Number of Entries", numofEntries, listOfEntries.size());
            for (int i = 0; i < numofEntries; i++) {
                assertEquals("listOfEntries should be sorted", Long.valueOf(i + 1), listOfEntries.get(i));
            }
        }
        assertTrue("Snapshot is expected to be empty since snapshot is not done", memTable.snapshot.isEmpty());
        assertTrue("Take snapshot and returned checkpoint should not be empty", memTable.snapshot() != null);
        assertFalse("After taking snapshot, snapshot should not be empty ", memTable.snapshot.isEmpty());
        for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
            OfLong entriesItr = memTable.getListOfEntriesOfLedger((random.nextInt((int) ledgerId) + 1));
            ArrayList<Long> listOfEntries = new ArrayList<Long>();
            Consumer<Long> addMethod = listOfEntries::add;
            entriesItr.forEachRemaining(addMethod);
            assertEquals("Number of Entries should be the same even after taking snapshot", numofEntries,
                    listOfEntries.size());
            for (int i = 0; i < numofEntries; i++) {
                assertEquals("listOfEntries should be sorted", Long.valueOf(i + 1), listOfEntries.get(i));
            }
        }

        memTable.flush(flusher);
        for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
            OfLong entriesItr = memTable.getListOfEntriesOfLedger((random.nextInt((int) ledgerId) + 1));
            assertFalse("After flushing there shouldn't be entries in memtable", entriesItr.hasNext());
        }
    }

    @Test
    public void testGetListOfEntriesOfLedgerFromBothKVMapAndSnapshot() throws IOException {
        int numofEntries = 100;
        int newNumOfEntries = 200;
        int numOfLedgers = 5;
        byte[] data = new byte[10];
        for (long entryId = 1; entryId <= numofEntries; entryId++) {
            for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
                random.nextBytes(data);
                assertTrue(ledgerId + ":" + entryId + " is duplicate in mem-table!",
                        memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0);
            }
        }

        assertTrue("Snapshot is expected to be empty since snapshot is not done", memTable.snapshot.isEmpty());
        assertTrue("Take snapshot and returned checkpoint should not be empty", memTable.snapshot() != null);
        assertFalse("After taking snapshot, snapshot should not be empty ", memTable.snapshot.isEmpty());

        for (long entryId = numofEntries + 1; entryId <= newNumOfEntries; entryId++) {
            for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
                random.nextBytes(data);
                assertTrue(ledgerId + ":" + entryId + " is duplicate in mem-table!",
                        memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0);
            }
        }

        for (long ledgerId = 1; ledgerId <= numOfLedgers; ledgerId++) {
            OfLong entriesItr = memTable.getListOfEntriesOfLedger((random.nextInt((int) ledgerId) + 1));
            ArrayList<Long> listOfEntries = new ArrayList<Long>();
            Consumer<Long> addMethod = listOfEntries::add;
            entriesItr.forEachRemaining(addMethod);
            assertEquals("Number of Entries should be the same", newNumOfEntries, listOfEntries.size());
            for (int i = 0; i < newNumOfEntries; i++) {
                assertEquals("listOfEntries should be sorted", Long.valueOf(i + 1), listOfEntries.get(i));
            }
        }
    }

    @Test
    public void testGetListOfEntriesOfLedgerWhileAddingConcurrently() throws IOException, InterruptedException {
        final int numofEntries = 100;
        final int newNumOfEntries = 200;
        final int concurrentAddOfEntries = 300;
        long ledgerId = 5;
        byte[] data = new byte[10];
        for (long entryId = 1; entryId <= numofEntries; entryId++) {
            random.nextBytes(data);
            assertTrue(ledgerId + ":" + entryId + " is duplicate in mem-table!",
                    memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0);
        }

        assertTrue("Snapshot is expected to be empty since snapshot is not done", memTable.snapshot.isEmpty());
        assertTrue("Take snapshot and returned checkpoint should not be empty", memTable.snapshot() != null);
        assertFalse("After taking snapshot, snapshot should not be empty ", memTable.snapshot.isEmpty());

        for (long entryId = numofEntries + 1; entryId <= newNumOfEntries; entryId++) {
            random.nextBytes(data);
            assertTrue(ledgerId + ":" + entryId + " is duplicate in mem-table!",
                    memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0);
        }

        AtomicBoolean successfullyAdded = new AtomicBoolean(true);

        Thread threadToAdd = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (long entryId = newNumOfEntries + 1; entryId <= concurrentAddOfEntries; entryId++) {
                        random.nextBytes(data);
                        boolean thisEntryAddedSuccessfully = (memTable.addEntry(ledgerId, entryId,
                                ByteBuffer.wrap(data), EntryMemTableTest.this) != 0);
                        successfullyAdded.set(successfullyAdded.get() && thisEntryAddedSuccessfully);
                        Thread.sleep(10);
                    }
                } catch (IOException e) {
                    log.error("Got Unexpected exception while adding entries");
                    successfullyAdded.set(false);
                } catch (InterruptedException e) {
                    log.error("Got InterruptedException while waiting");
                    successfullyAdded.set(false);
                }
            }
        });
        threadToAdd.start();

        Thread.sleep(200);
        OfLong entriesItr = memTable.getListOfEntriesOfLedger(ledgerId);
        ArrayList<Long> listOfEntries = new ArrayList<Long>();
        while (entriesItr.hasNext()) {
            listOfEntries.add(entriesItr.next());
            Thread.sleep(5);
        }
        threadToAdd.join(5000);
        assertTrue("Entries should be added successfully in the spawned thread", successfullyAdded.get());

        for (int i = 0; i < newNumOfEntries; i++) {
            assertEquals("listOfEntries should be sorted", Long.valueOf(i + 1), listOfEntries.get(i));
        }
    }
}

