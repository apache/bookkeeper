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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Test
    public void testNormalParallelRead() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 2, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        // read single entry
        for (int i = 0; i < numEntries; i++) {
            PendingReadOp readOp =
                    new PendingReadOp(lh, lh.bk.scheduler, i, i);
            readOp.parallelRead(true).submit();
            Iterable<LedgerEntry> iterable = readOp.future().get();
            assertNotNull(iterable);
            Iterator<LedgerEntry> entries = iterable.iterator();
            assertTrue(entries.hasNext());
            LedgerEntry entry = entries.next();
            assertNotNull(entry);
            assertEquals(i, Integer.parseInt(new String(entry.getEntry())));
            entry.close();
            assertFalse(entries.hasNext());
        }

        // read multiple entries
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 0, numEntries - 1);
        readOp.parallelRead(true).submit();
        Iterable<LedgerEntry> iterable = readOp.future().get();
        assertNotNull(iterable);
        Iterator<LedgerEntry> iterator = iterable.iterator();

        int numReads = 0;
        while (iterator.hasNext()) {
            LedgerEntry entry = iterator.next();
            assertNotNull(entry);
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntry())));
            entry.close();
            ++numReads;
        }
        assertEquals(numEntries, numReads);

        lh.close();
    }

    private static <T> void expectFail(CompletableFuture<T> future, int expectedRc) {
        try {
            result(future);
            fail("Expect to fail");
        } catch (Exception e) {
            assertTrue(e instanceof BKException);
            BKException bke = (BKException) e;
            assertEquals(expectedRc, bke.getCode());
        }
    }

    @Test
    public void testParallelReadMissingEntries() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 2, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        // read single entry
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 11, 11);
        readOp.parallelRead(true).submit();
        expectFail(readOp.future(), Code.NoSuchEntryException);

        // read multiple entries
        readOp = new PendingReadOp(lh, lh.bk.scheduler, 8, 11);
        readOp.parallelRead(true).submit();
        expectFail(readOp.future(), Code.NoSuchEntryException);

        lh.close();
    }

    @Test
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

        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 10, 10);
        readOp.parallelRead(true).submit();
        // would fail immediately if found missing entries don't cover ack quorum
        expectFail(readOp.future(), Code.NoSuchEntryException);
        latch1.countDown();
        latch2.countDown();

        lh.close();
        newBk.close();
    }

    @Test
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
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 0, numEntries - 1);
        readOp.parallelRead(true).submit();
        Iterable<LedgerEntry> iterable = readOp.future().get();
        assertNotNull(iterable);
        Iterator<LedgerEntry> entries = iterable.iterator();

        int numReads = 0;
        while (entries.hasNext()) {
            LedgerEntry entry = entries.next();
            assertNotNull(entry);
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntry())));
            ++numReads;
        }
        assertEquals(numEntries, numReads);

        lh.close();
        newBk.close();
    }

    @Test
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
        PendingReadOp readOp =
                new PendingReadOp(lh, lh.bk.scheduler, 0, numEntries - 1);
        readOp.parallelRead(true).submit();
        expectFail(readOp.future(), Code.BookieHandleNotAvailableException);

        lh.close();
        newBk.close();
    }

}
