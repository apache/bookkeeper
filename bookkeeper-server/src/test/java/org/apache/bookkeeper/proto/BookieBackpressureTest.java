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

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
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
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests for backpressure handling on the server side.
 */
public class BookieBackpressureTest extends BookKeeperClusterTestCase
        implements AddCallback, ReadCallback, ReadLastConfirmedCallback {

    private static final Logger LOG = LoggerFactory.getLogger(BookieBackpressureTest.class);

    byte[] ledgerPassword = "aaa".getBytes();

    final byte[] data = new byte[8 * 1024];

    // test related variables
    static final int NUM_ENTRIES_TO_WRITE = 500;
    static final int MAX_PENDING = 10;
    static final int NUM_OF_LEDGERS = 25;

    DigestType digestType;

    public BookieBackpressureTest() {
        super(1);
        this.digestType = DigestType.CRC32;

        baseClientConf.setAddEntryTimeout(100);
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

    @Test
    public void testWriteNoBackpressureSlowJournal() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgress(0);
        bsConfs.get(0).setProperty(Bookie.PROP_SLOW_JOURNAL_ADD_DELAY, "1");

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteNoBackpressureSlowJournalFlush() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgress(0);
        // to increase frequency of flushes
        bsConfs.get(0).setJournalAdaptiveGroupWrites(false);
        bsConfs.get(0).setProperty(Bookie.PROP_SLOW_JOURNAL_FLUSH_DELAY, "1");

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteWithBackpressureSlowJournal() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgress(MAX_PENDING);
        bsConfs.get(0).setProperty(Bookie.PROP_SLOW_JOURNAL_FLUSH_DELAY, "1");

        doWritesWithBackpressure(0);
    }


    @Test
    public void testWriteWithBackpressureSlowJournalFlush() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgress(MAX_PENDING);
        // to increase frequency of flushes
        bsConfs.get(0).setJournalAdaptiveGroupWrites(false);
        bsConfs.get(0).setProperty(Bookie.PROP_SLOW_JOURNAL_FLUSH_DELAY, "1");

        doWritesWithBackpressure(0);
    }

    @Test
    public void testWriteNoBackpressureSlowInterleavedStorage() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgress(0);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_ADD_DELAY, "1");

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteWithBackpressureSlowInterleavedStorage() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgress(MAX_PENDING);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_ADD_DELAY, "1");

        doWritesWithBackpressure(0);
    }

    @Test
    public void testWriteNoBackpressureSlowInterleavedStorageFlush() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgress(0);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_FLUSH_DELAY, "10");

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteWithBackpressureSlowInterleavedStorageFlush() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgress(MAX_PENDING);
        bsConfs.get(0).setLedgerStorageClass(SlowInterleavedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_FLUSH_DELAY, "10");

        doWritesWithBackpressure(0);
    }

    @Test
    public void testWriteNoBackpressureSortedStorage() throws Exception {
        //disable backpressure for writes
        bsConfs.get(0).setMaxAddsInProgress(0);
        bsConfs.get(0).setLedgerStorageClass(SlowSortedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        final int entriesInMemtable = 3;
        // one for memtable being flushed, one for the part accepting the data
        assertTrue("for the test, memtable should not keep more entries than allowed",
                entriesInMemtable * 2 <= MAX_PENDING);
        bsConfs.get(0).setSkipListSizeLimit(data.length * entriesInMemtable);
        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_ADD_DELAY, "1");
        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_FLUSH_DELAY, "10");

        doWritesNoBackpressure(0);
    }

    @Test
    public void testWriteWithBackpressureSortedStorage() throws Exception {
        //enable backpressure with MAX_PENDING writes in progress
        bsConfs.get(0).setMaxAddsInProgress(MAX_PENDING);
        bsConfs.get(0).setLedgerStorageClass(SlowSortedLedgerStorage.class.getName());
        bsConfs.get(0).setWriteBufferBytes(data.length);

        final int entriesInMemtable = 3;
        // one for memtable being flushed, one for the part accepting the data
        assertTrue("for the test, memtable should not keep more entries than allowed",
                entriesInMemtable * 2 <= MAX_PENDING);
        bsConfs.get(0).setSkipListSizeLimit(data.length * entriesInMemtable);
        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_ADD_DELAY, "1");
        bsConfs.get(0).setProperty(SlowInterleavedLedgerStorage.PROP_SLOW_STORAGE_FLUSH_DELAY, "10");

        doWritesWithBackpressure(0);
    }

    @Test
    public void testReadsNoBackpressure() throws Exception {
        //disable backpressure for reads
        bsConfs.get(0).setMaxReadsInProgress(0);
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
        bsConfs.get(0).setMaxReadsInProgress(MAX_PENDING);
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
        bks.start();
        bs.set(bkId, bks);

        // Create ledgers
        final int numEntriesForReads = 10;
        LedgerHandle[] lhs = new LedgerHandle[NUM_OF_LEDGERS];
        for (int i = 0; i < NUM_OF_LEDGERS; i++) {
            lhs[i] = bkc.createLedger(1, 1, digestType, ledgerPassword);
            LOG.info("created ledger ID: {}", lhs[i].getId());
        }

        // generate data for reads
        final CountDownLatch writesCompleteLatch = new CountDownLatch(numEntriesForReads * NUM_OF_LEDGERS);
        for (int i = 0; i < numEntriesForReads; i++) {
            for (int ledger = 0; ledger < NUM_OF_LEDGERS; ledger++) {
                lhs[ledger].asyncAddEntry(data, (rc2, lh, entryId, ctx) -> writesCompleteLatch.countDown(), null);
            }
        }
        writesCompleteLatch.await();

        // issue bunch of async reads
        // generate data for reads
        final CountDownLatch readsCompleteLatch = new CountDownLatch(numEntriesForReads * NUM_OF_LEDGERS);
        for (int i = 0; i < numEntriesForReads; i++) {
            for (int ledger = 0; ledger < NUM_OF_LEDGERS; ledger++) {
                lhs[ledger].asyncReadEntries(i, i, (rc, lh, seq, ctx) -> readsCompleteLatch.countDown(), null);
            }
        }
        readsCompleteLatch.await();

        return bks.getBookieRequestProcessor();
    }

    // here we expect that backpressure is disabled and number of writes in progress
    // will exceed the limit
    private void doWritesNoBackpressure(final int bkId) throws Exception {
        BookieServer bks = bs.get(bkId);
        bks.shutdown();
        bks = new BookieServer(bsConfs.get(bkId));
        bks.start();
        bs.set(bkId, bks);

        // Create ledgers
        LedgerHandle[] lhs = new LedgerHandle[NUM_OF_LEDGERS];
        for (int i = 0; i < NUM_OF_LEDGERS; i++) {
            lhs[i] = bkc.createLedger(1, 1, digestType, ledgerPassword);
            LOG.info("created ledger ID: {}", lhs[i].getId());
        }

        final CountDownLatch completeLatch = new CountDownLatch(NUM_ENTRIES_TO_WRITE * NUM_OF_LEDGERS);

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
        bks.start();
        bs.set(bkId, bks);

        LOG.info("test restarted bookie; starting writes");

        // Create ledgers
        LedgerHandle[] lhs = new LedgerHandle[NUM_OF_LEDGERS];
        for (int i = 0; i < NUM_OF_LEDGERS; i++) {
            lhs[i] = bkc.createLedger(1, 1, digestType, ledgerPassword);
            LOG.info("created ledger ID: {}", lhs[i].getId());
        }

        final CountDownLatch completeLatch = new CountDownLatch(NUM_ENTRIES_TO_WRITE * NUM_OF_LEDGERS);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

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
