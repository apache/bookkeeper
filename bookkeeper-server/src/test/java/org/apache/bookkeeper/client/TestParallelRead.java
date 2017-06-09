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
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;

/**
 * Unit tests for parallel reading
 */
public class TestParallelRead extends BookKeeperClusterTestCase {

    static Logger LOG = LoggerFactory.getLogger(TestParallelRead.class);

    final DigestType digestType;
    final byte[] passwd = "parallel-read".getBytes();

    public TestParallelRead() {
        super(6);
        this.digestType = DigestType.CRC32;
    }

    long getLedgerToRead(int ensemble, int writeQuorum, int ackQuorum, int numEntries)
            throws Exception {
        LedgerHandle lh = bkc.createLedger(ensemble, writeQuorum, ackQuorum, digestType, passwd);
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("" + i).getBytes());
        }
        lh.close();
        return lh.getId();
    }

    static class LatchCallback implements ReadCallback {

        final CountDownLatch l = new CountDownLatch(1);
        int rc = -0x1314;
        Enumeration<LedgerEntry> entries;

        Enumeration<LedgerEntry> getEntries() {
            return entries;
        }

        int getRc() {
            return rc;
        }

        @Override
        public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
            this.rc = rc;
            this.entries = seq;
            l.countDown();
        }

        void expectSuccess() throws Exception {
            l.await();
            assertTrue(BKException.Code.OK == rc);
        }

        void expectFail() throws Exception {
            l.await();
            assertFalse(BKException.Code.OK == rc);
        }

    }

    @Test(timeout = 60000)
    public void testNormalParallelRead() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 2, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        // read single entry
        for (int i = 0; i < numEntries; i++) {
            LatchCallback latch = new LatchCallback();
            PendingReadOp readOp =
                    new PendingReadOp(lh, lh.bk.scheduler, i, i, latch, null);
            readOp.parallelRead(true).initiate();
            latch.expectSuccess();
            Enumeration<LedgerEntry> entries = latch.getEntries();
            assertNotNull(entries);
            assertTrue(entries.hasMoreElements());
            LedgerEntry entry = entries.nextElement();
            assertNotNull(entry);
            assertEquals(i, Integer.parseInt(new String(entry.getEntry())));
            assertFalse(entries.hasMoreElements());
        }

        // read multiple entries
        LatchCallback latch = new LatchCallback();
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 0, numEntries - 1, latch, null);
        readOp.parallelRead(true).initiate();
        latch.expectSuccess();
        Enumeration<LedgerEntry> entries = latch.getEntries();
        assertNotNull(entries);

        int numReads = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertNotNull(entry);
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntry())));
            ++numReads;
        }
        assertEquals(numEntries, numReads);

        lh.close();
    }

    @Test(timeout = 60000)
    public void testParallelReadMissingEntries() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 2, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        // read single entry
        LatchCallback latch = new LatchCallback();
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 11, 11, latch, null);
        readOp.parallelRead(true).initiate();
        latch.expectFail();
        assertEquals(BKException.Code.NoSuchEntryException, latch.getRc());

        // read multiple entries
        latch = new LatchCallback();
        readOp = new PendingReadOp(lh, lh.bk.scheduler, 8, 11, latch, null);
        readOp.parallelRead(true).initiate();
        latch.expectFail();
        assertEquals(BKException.Code.NoSuchEntryException, latch.getRc());

        lh.close();
    }

    @Test(timeout = 60000)
    public void testFailParallelReadMissingEntryImmediately() throws Exception {
        int numEntries = 1;

        long id = getLedgerToRead(5, 5, 3, numEntries);

        ClientConfiguration newConf = new ClientConfiguration()
            .setReadEntryTimeout(30000);
        newConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        ArrayList<BookieSocketAddress> ensemble =
                lh.getLedgerMetadata().getEnsemble(10);
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        // sleep two bookie
        sleepBookie(ensemble.get(0), latch1);
        sleepBookie(ensemble.get(1), latch2);

        LatchCallback latchCallback = new LatchCallback();
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 10, 10, latchCallback, null);
        readOp.parallelRead(true).initiate();
        // would fail immediately if found missing entries don't cover ack quorum
        latchCallback.expectFail();
        assertEquals(BKException.Code.NoSuchEntryException, latchCallback.getRc());
        latch1.countDown();
        latch2.countDown();

        lh.close();
        newBk.close();
    }

    @Test(timeout = 60000)
    public void testParallelReadWithFailedBookies() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 3, 3, numEntries);

        ClientConfiguration newConf = new ClientConfiguration()
            .setReadEntryTimeout(30000);
        newConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        ArrayList<BookieSocketAddress> ensemble =
                lh.getLedgerMetadata().getEnsemble(5);
        // kill two bookies
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));

        // read multiple entries
        LatchCallback latch = new LatchCallback();
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 0, numEntries - 1, latch, null);
        readOp.parallelRead(true).initiate();
        latch.expectSuccess();
        Enumeration<LedgerEntry> entries = latch.getEntries();
        assertNotNull(entries);

        int numReads = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertNotNull(entry);
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntry())));
            ++numReads;
        }
        assertEquals(numEntries, numReads);

        lh.close();
        newBk.close();
    }

    @Test(timeout = 60000)
    public void testParallelReadFailureWithFailedBookies() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 3, 3, numEntries);

        ClientConfiguration newConf = new ClientConfiguration()
            .setReadEntryTimeout(30000);
        newConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        ArrayList<BookieSocketAddress> ensemble =
                lh.getLedgerMetadata().getEnsemble(5);
        // kill two bookies
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));
        killBookie(ensemble.get(2));

        // read multiple entries
        LatchCallback latch = new LatchCallback();
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 0, numEntries - 1, latch, null);
        readOp.parallelRead(true).initiate();
        latch.expectFail();
        assertEquals(BKException.Code.BookieHandleNotAvailableException, latch.getRc());

        lh.close();
        newBk.close();
    }

}
