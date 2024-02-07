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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for batch reading.
 */
public class TestBatchedRead extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestBatchedRead.class);

    final DigestType digestType;
    final byte[] passwd = "sequence-read".getBytes();

    public TestBatchedRead() {
        super(6);
        baseClientConf.setUseV2WireProtocol(true);
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

    BatchedReadOp createReadOp(LedgerHandle lh, long startEntry, int count) {
        return new BatchedReadOp(lh, bkc.getClientCtx(), startEntry, count, 1024 * count, false);
    }

    BatchedReadOp createRecoveryReadOp(LedgerHandle lh, long startEntry, int count) {
        return new BatchedReadOp(lh, bkc.getClientCtx(), startEntry, count, 1024 * count, true);
    }

    @Test
    public void testNormalRead() throws Exception {
        int numEntries = 10;
        long id = getLedgerToRead(5, 5, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        //read single entry
        for (int i = 0; i < numEntries; i++) {
            BatchedReadOp readOp = createReadOp(lh, i, 1);
            readOp.submit();
            Iterator<LedgerEntry> entries = readOp.future().get().iterator();
            assertTrue(entries.hasNext());
            LedgerEntry entry = entries.next();
            assertNotNull(entry);
            assertEquals(i, Integer.parseInt(new String(entry.getEntryBytes())));
            entry.close();
            assertFalse(entries.hasNext());
        }

        // read multiple entries
        BatchedReadOp readOp = createReadOp(lh, 0, numEntries);
        readOp.submit();
        Iterator<LedgerEntry> iterator = readOp.future().get().iterator();

        int numReads = 0;
        while (iterator.hasNext()) {
            LedgerEntry entry = iterator.next();
            assertNotNull(entry);
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntryBytes())));
            entry.close();
            ++numReads;
        }
        assertEquals(numEntries, numReads);
        lh.close();
    }

    @Test
    public void testReadWhenEnsembleNotEqualWQ() throws Exception {
        int numEntries = 10;
        long id = getLedgerToRead(5, 2, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        //read single entry
        for (int i = 0; i < numEntries; i++) {
            BatchedReadOp readOp = createReadOp(lh, i, 1);
            readOp.submit();
            Iterator<LedgerEntry> entries = readOp.future().get().iterator();
            assertTrue(entries.hasNext());
            LedgerEntry entry = entries.next();
            assertNotNull(entry);
            assertEquals(i, Integer.parseInt(new String(entry.getEntryBytes())));
            entry.close();
            assertFalse(entries.hasNext());
        }

        // read multiple entries, because the ensemble is not equals with write quorum, the return entries
        // will less than max count.
        for (int i = 0; i < numEntries; i++) {
            BatchedReadOp readOp = createReadOp(lh, i, numEntries);
            readOp.submit();
            Iterator<LedgerEntry> entries = readOp.future().get().iterator();
            assertTrue(entries.hasNext());
            LedgerEntry entry = entries.next();
            assertNotNull(entry);
            assertEquals(i, Integer.parseInt(new String(entry.getEntryBytes())));
            entry.close();
            assertFalse(entries.hasNext());
        }
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
    public void testReadMissingEntries() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 5, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        // read single entry
        BatchedReadOp readOp = createReadOp(lh, 10, 1);
        readOp.submit();
        expectFail(readOp.future(), Code.NoSuchEntryException);

        // read multiple entries
        readOp = createReadOp(lh, 8, 3);
        readOp.submit();

        int index = 8;
        int numReads = 0;
        Iterator<LedgerEntry> iterator = readOp.future().get().iterator();
        while (iterator.hasNext()) {
            LedgerEntry entry = iterator.next();
            assertNotNull(entry);
            assertEquals(index, Integer.parseInt(new String(entry.getEntryBytes())));
            entry.close();
            ++index;
            ++numReads;
        }
        assertEquals(2, numReads);
        lh.close();
    }

    @Test
    public void testFailRecoveryReadMissingEntryImmediately() throws Exception {
        int numEntries = 1;

        long id = getLedgerToRead(5, 5, 3, numEntries);

        ClientConfiguration newConf = new ClientConfiguration()
            .setReadEntryTimeout(30000);
        newConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        List<BookieId> ensemble = lh.getLedgerMetadata().getEnsembleAt(10);
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        // sleep two bookie
        sleepBookie(ensemble.get(0), latch1);
        sleepBookie(ensemble.get(1), latch2);

        BatchedReadOp readOp = createRecoveryReadOp(lh, 10, 1);
        readOp.submit();
        // would fail immediately if found missing entries don't cover ack quorum
        expectFail(readOp.future(), Code.NoSuchEntryException);
        latch1.countDown();
        latch2.countDown();

        lh.close();
        newBk.close();
    }

    @Test
    public void testReadWithFailedBookies() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 3, 3, numEntries);

        ClientConfiguration newConf = new ClientConfiguration()
            .setReadEntryTimeout(30000);
        newConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        List<BookieId> ensemble = lh.getLedgerMetadata().getEnsembleAt(5);
        // kill two bookies
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));

        // read multiple entries, because the ensemble is not equals with write quorum, the return entries
        // will less than max count.
        int numReads = 0;
        for (int i = 0; i < numEntries;) {
            BatchedReadOp readOp = createReadOp(lh, i, numEntries);
            readOp.submit();
            Iterator<LedgerEntry> entries = readOp.future().get().iterator();
            if (!entries.hasNext()) {
                i++;
                continue;
            }
            while (entries.hasNext()) {
                LedgerEntry entry = entries.next();
                assertNotNull(entry);
                assertEquals(i, Integer.parseInt(new String(entry.getEntryBytes())));
                entry.close();
                i++;
                numReads++;
            }
        }
        assertEquals(10, numReads);
        lh.close();
        newBk.close();
    }

    @Test
    public void testReadFailureWithFailedBookies() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 3, 3, numEntries);

        ClientConfiguration newConf = new ClientConfiguration()
            .setReadEntryTimeout(30000);
        newConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        List<BookieId> ensemble = lh.getLedgerMetadata().getEnsembleAt(5);
        // kill two bookies
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));
        killBookie(ensemble.get(2));

        // read multiple entries
        BatchedReadOp readOp = createReadOp(lh, 0,  numEntries);
        readOp.submit();
        expectFail(readOp.future(), Code.BookieHandleNotAvailableException);

        lh.close();
        newBk.close();
    }
}
