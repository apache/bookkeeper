package org.apache.bookkeeper.proto;

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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.netty.buffer.UnpooledByteBufAllocator;

import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.SlowBufferedChannel;
import org.apache.bookkeeper.bookie.SlowInterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.SlowSortedLedgerStorage;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests for backpressure handling on the server side.
 */
// PowerMock usage is problematic here due to https://github.com/powermock/powermock/issues/822
public class BookieBackpressureTest extends BookKeeperClusterTestCase
        implements AddCallback, ReadCallback, ReadLastConfirmedCallback {

    private static final Logger LOG = LoggerFactory.getLogger(BookieBackpressureTest.class);

    byte[] ledgerPassword = "aaa".getBytes();

    final byte[] data = new byte[8 * 1024];

    // test related constants
    static final int NUM_ENTRIES_TO_WRITE = 200;
    static final int ENTRIES_IN_MEMTABLE = 2;
    static final int MAX_PENDING = 2 * ENTRIES_IN_MEMTABLE + 1;
    static final int NUM_OF_LEDGERS = 2 * MAX_PENDING;

    DigestType digestType;

    long getDelay;
    long addDelay;
    long flushDelay;

    public BookieBackpressureTest() {
        super(1);
        this.digestType = DigestType.CRC32;

        baseClientConf.setAddEntryTimeout(100);
        baseClientConf.setAddEntryQuorumTimeout(100);
        baseClientConf.setReadEntryTimeout(100);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        getDelay = 0;
        addDelay = 0;
        flushDelay = 0;
    }

    class SyncObj {
        long lastConfirmed;
        volatile int counter;
        boolean value;
        AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        Enumeration<LedgerEntry> ls = null;

        public SyncObj() {
            counter = 0;
            lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
            value = false;
        }

        void setReturnCode(int rc) {
            this.rc.compareAndSet(BKException.Code.OK, rc);
        }

        void setLedgerEntries(Enumeration<LedgerEntry> ls) {
            this.ls = ls;
        }
    }

    private void mockJournal(Bookie bookie, long getDelay, long addDelay, long flushDelay) throws Exception {
        if (getDelay <= 0 && addDelay <= 0 && flushDelay <= 0) {
            return;
        }

        List<Journal> journals = getJournals(bookie);
        for (int i = 0; i < journals.size(); i++) {
            Journal mock = spy(journals.get(i));
            when(mock.getBufferedChannelBuilder()).thenReturn((FileChannel fc, int capacity) ->  {
                SlowBufferedChannel sbc = new SlowBufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity);
                sbc.setAddDelay(addDelay);
                sbc.setGetDelay(getDelay);
                sbc.setFlushDelay(flushDelay);
                return sbc;
            });

            journals.set(i, mock);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Journal> getJournals(Bookie bookie) throws NoSuchFieldException, IllegalAccessException {
        Field f = bookie.getClass().getDeclaredField("journals");
        f.setAccessible(true);

        return (List<Journal>) f.get(bookie);
    }

    @Test
    public void testWriteNoBackpressureSlowJournal() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgressLimit(0);
        addDelay = 1;

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteNoBackpressureSlowJournalFlush() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgressLimit(0);
        // to increase frequency of flushes
        bsConfs.get(0).setJournalAdaptiveGroupWrites(false);
        flushDelay = 1;

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteWithBackpressureSlowJournal() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgressLimit(MAX_PENDING);
        flushDelay = 1;

        doWritesWithBackpressure(0);
    }


    @Test
    public void testWriteWithBackpressureSlowJournalFlush() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgressLimit(MAX_PENDING);
        // to increase frequency of flushes
        bsConfs.get(0).setJournalAdaptiveGroupWrites(false);
        flushDelay = 1;

        doWritesWithBackpressure(0);
    }

    @Test
    public void testWriteNoBackpressureSlowInterleavedStorage() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgressLimit(0);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_ADD_DELAY, "1");

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteWithBackpressureSlowInterleavedStorage() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgressLimit(MAX_PENDING);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_ADD_DELAY, "1");

        doWritesWithBackpressure(0);
    }

    @Test
    public void testWriteNoBackpressureSlowInterleavedStorageFlush() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgressLimit(0);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_FLUSH_DELAY, "10");

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteWithBackpressureSlowInterleavedStorageFlush() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgressLimit(MAX_PENDING);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_FLUSH_DELAY, "10");

        doWritesWithBackpressure(0);
    }

    @Test
    public void testWriteNoBackpressureSortedStorage() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgressLimit(0);
        bsConfs.get(0).setLedgerStorageClass(SlowSortedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        // one for memtable being flushed, one for the part accepting the data
        assertTrue("for the test, memtable should not keep more entries than allowed",
                ENTRIES_IN_MEMTABLE * 2 <= MAX_PENDING);
        bsConfs.get(0).setSkipListSizeLimit(data.length * ENTRIES_IN_MEMTABLE - 1);
        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_ADD_DELAY, "1");
        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_FLUSH_DELAY, "10");

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteWithBackpressureSortedStorage() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgressLimit(MAX_PENDING);
        bsConfs.get(0).setLedgerStorageClass(SlowSortedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        // one for memtable being flushed, one for the part accepting the data
        assertTrue("for the test, memtable should not keep more entries than allowed",
                ENTRIES_IN_MEMTABLE * 2 <= MAX_PENDING);
        bsConfs.get(0).setSkipListSizeLimit(data.length * ENTRIES_IN_MEMTABLE - 1);
        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_ADD_DELAY, "1");
        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_FLUSH_DELAY, "10");

        doWritesWithBackpressure(0);
    }

    @Test
    public void testReadsNoBackpressure() throws Exception {
        //disable backpressure for reads
        bsConfs.get(0).setMaxReadsInProgressLimit(0);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_GET_DELAY, "1");

        final BookieRequestProcessor brp = generateDataAndDoReads(0);

        Assert.assertThat("reads in progress should exceed MAX_PENDING",
                brp.maxReadsInProgressCount(), Matchers.greaterThan(MAX_PENDING));
    }

   @Test
    public void testReadsWithBackpressure() throws Exception {
        //enable backpressure for reads
        bsConfs.get(0).setMaxReadsInProgressLimit(MAX_PENDING);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_GET_DELAY, "1");

        final BookieRequestProcessor brp = generateDataAndDoReads(0);

        Assert.assertThat("reads in progress should NOT exceed MAX_PENDING ",
                brp.maxReadsInProgressCount(), Matchers.lessThanOrEqualTo(MAX_PENDING));
    }

    private BookieRequestProcessor generateDataAndDoReads(final int bkId) throws Exception {
        BookieServer bks = bs.get(bkId);
        bks.shutdown();
        bks = new BookieServer(bsConfs.get(bkId));
        mockJournal(bks.getBookie(), getDelay, addDelay, flushDelay);
        bks.start();
        bs.set(bkId, bks);

        LOG.info("creating ledgers");
        // Create ledgers
        final int numEntriesForReads = 10;
        LedgerHandle[] lhs = new LedgerHandle[NUM_OF_LEDGERS];
        for (int i = 0; i < NUM_OF_LEDGERS; i++) {
            lhs[i] = bkc.createLedger(1, 1, digestType, ledgerPassword);
            LOG.info("created ledger ID: {}", lhs[i].getId());
        }

        LOG.info("generating data for reads");
        final CountDownLatch writesCompleteLatch = new CountDownLatch(numEntriesForReads * NUM_OF_LEDGERS);
        for (int i = 0; i < numEntriesForReads; i++) {
            for (int ledger = 0; ledger < NUM_OF_LEDGERS; ledger++) {
                lhs[ledger].asyncAddEntry(data, (rc2, lh, entryId, ctx) -> writesCompleteLatch.countDown(), null);
            }
        }
        writesCompleteLatch.await();

        LOG.info("issue bunch of async reads");
        final CountDownLatch readsCompleteLatch = new CountDownLatch(numEntriesForReads * NUM_OF_LEDGERS);
        for (int i = 0; i < numEntriesForReads; i++) {
            for (int ledger = 0; ledger < NUM_OF_LEDGERS; ledger++) {
                lhs[ledger].asyncReadEntries(i, i, (rc, lh, seq, ctx) -> readsCompleteLatch.countDown(), null);
            }
        }
        readsCompleteLatch.await();
        LOG.info("reads finished");

        return bks.getBookieRequestProcessor();
    }

    // here we expect that backpressure is disabled and number of writes in progress
    // will exceed the limit
    private void doWritesNoBackpressure(final int bkId) throws Exception {
        BookieServer bks = bs.get(bkId);
        bks.shutdown();
        bks = new BookieServer(bsConfs.get(bkId));
        mockJournal(bks.getBookie(), getDelay, addDelay, flushDelay);
        bks.start();
        bs.set(bkId, bks);

        LOG.info("Creating ledgers");
        LedgerHandle[] lhs = new LedgerHandle[NUM_OF_LEDGERS];
        for (int i = 0; i < NUM_OF_LEDGERS; i++) {
            lhs[i] = bkc.createLedger(1, 1, digestType, ledgerPassword);
            LOG.info("created ledger ID: {}", lhs[i].getId());
        }

        final CountDownLatch completeLatch = new CountDownLatch(NUM_ENTRIES_TO_WRITE * NUM_OF_LEDGERS);

        LOG.info("submitting writes");
        for (int i = 0; i < NUM_ENTRIES_TO_WRITE; i++) {
            for (int ledger = 0; ledger < NUM_OF_LEDGERS; ledger++) {
                lhs[ledger].asyncAddEntry(data, (rc2, lh, entryId, ctx) -> completeLatch.countDown(), null);
            }
        }

        boolean exceededLimit = false;
        BookieRequestProcessor brp = bks.getBookieRequestProcessor();
        while (!completeLatch.await(1, TimeUnit.MILLISECONDS)) {
            int val = brp.maxAddsInProgressCount();
            if (val > MAX_PENDING) {
                exceededLimit = true;
                break;
            }
            LOG.info("Waiting until all writes succeeded or maxAddsInProgressCount {} > MAX_PENDING {}",
                    val, MAX_PENDING);
        }

        assertTrue("expected to exceed number of pending writes", exceededLimit);

        for (int i = 0; i < NUM_OF_LEDGERS; i++) {
            lhs[i].close();
        }
    }

    // here we expect that backpressure is enabled and number of writes in progress
    // will never exceed the limit
    private void doWritesWithBackpressure(final int bkId) throws Exception {
        BookieServer bks = bs.get(bkId);
        bks.shutdown();
        bks = new BookieServer(bsConfs.get(bkId));
        mockJournal(bks.getBookie(), getDelay, addDelay, flushDelay);
        bks.start();
        bs.set(bkId, bks);

        LOG.info("Creating ledgers");
        LedgerHandle[] lhs = new LedgerHandle[NUM_OF_LEDGERS];
        for (int i = 0; i < NUM_OF_LEDGERS; i++) {
            lhs[i] = bkc.createLedger(1, 1, digestType, ledgerPassword);
            LOG.info("created ledger ID: {}", lhs[i].getId());
        }

        final CountDownLatch completeLatch = new CountDownLatch(NUM_ENTRIES_TO_WRITE * NUM_OF_LEDGERS);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

        LOG.info("submitting writes");
        for (int i = 0; i < NUM_ENTRIES_TO_WRITE; i++) {
            for (int ledger = 0; ledger < NUM_OF_LEDGERS; ledger++) {
                lhs[ledger].asyncAddEntry(data, (rc2, lh, entryId, ctx) -> {
                    rc.compareAndSet(BKException.Code.OK, rc2);
                    completeLatch.countDown();
                }, null);
            }
        }

        LOG.info("test submitted all writes");
        BookieRequestProcessor brp = bks.getBookieRequestProcessor();
        while (!completeLatch.await(1, TimeUnit.MILLISECONDS)) {
            int val = brp.maxAddsInProgressCount();
            assertTrue("writes in progress should not exceed limit, got " + val, val <= MAX_PENDING);
            LOG.info("Waiting for all writes to succeed, left {} of {}",
                    completeLatch.getCount(), NUM_ENTRIES_TO_WRITE * NUM_OF_LEDGERS);
        }

        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }

        for (int i = 0; i < NUM_OF_LEDGERS; i++) {
            lhs[i].close();
        }
    }


    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        SyncObj sync = (SyncObj) ctx;
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.counter++;
            sync.notify();
        }
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
        SyncObj sync = (SyncObj) ctx;
        sync.setLedgerEntries(seq);
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.value = true;
            sync.notify();
        }
    }

    @Override
    public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
        SyncObj sync = (SyncObj) ctx;
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.lastConfirmed = lastConfirmed;
            sync.notify();
        }
    }

}
