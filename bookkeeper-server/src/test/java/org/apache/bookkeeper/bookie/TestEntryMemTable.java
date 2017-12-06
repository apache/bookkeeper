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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Random;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;
import org.junit.Before;

public class TestEntryMemTable implements CacheCallback, SkipListFlusher, CheckpointSource {

    private EntryMemTable memTable;
    private final Random random = new Random();
    private TestCheckPoint curCheckpoint = new TestCheckPoint(0, 0);

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
        this.memTable = new EntryMemTable(TestBKConfiguration.newServerConfiguration(),
                this, NullStatsLogger.INSTANCE);
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
     * Basic put/get
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

    public void process(long ledgerId, long entryId, ByteBuffer entry)
            throws IOException {
        // No-op
    }

    /**
     * Test read/write across snapshot
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
        final HashSet<EntryKeyValue> keyValues;

        KVFLusher(final HashSet<EntryKeyValue> keyValues) {
            this.keyValues = keyValues;
        }

        @Override
        public void process(long ledgerId, long entryId, ByteBuffer entry) throws IOException {
            assertTrue(ledgerId + ":" + entryId + " is duplicate in store!",
                    keyValues.add(new EntryKeyValue(ledgerId, entryId, entry.array())));
        }
    }

    private class NoLedgerFLusher implements SkipListFlusher {
        @Override
        public void process(long ledgerId, long entryId, ByteBuffer entry) throws IOException {
            throw new NoLedgerException(ledgerId);
        }
    }

    /**
     * Test flush w/ logMark parameter
     * @throws IOException
     */
    @Test
    public void testFlushLogMark() throws IOException {
        HashSet<EntryKeyValue> flushedKVs = new HashSet<EntryKeyValue>();
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
     * Test snapshot/flush interaction
     * @throws IOException
     */
    @Test
    public void testFlushSnapshot() throws IOException {
        HashSet<EntryKeyValue> keyValues = new HashSet<EntryKeyValue>();
        HashSet<EntryKeyValue> flushedKVs = new HashSet<EntryKeyValue>();
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
     * Test NoLedger exception/flush interaction
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
            return mark.compare(((TestCheckPoint)o).mark);
        }

    }
}

