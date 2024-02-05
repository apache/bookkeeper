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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for parallel reading.
 */
public class TestParallelRead extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestParallelRead.class);

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

    PendingReadOp createReadOp(LedgerHandle lh, long from, long to) {
        return new PendingReadOp(lh, bkc.getClientCtx(), from, to, false);
    }

    PendingReadOp createRecoveryReadOp(LedgerHandle lh, long from, long to) {
        return new PendingReadOp(lh, bkc.getClientCtx(), from, to, true);
    }

    @Test
    public void testNormalParallelRead() throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 2, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        // read single entry
        for (int i = 0; i < numEntries; i++) {
            PendingReadOp readOp = createReadOp(lh, i, i);
            readOp.parallelRead(true).submit();
            Iterator<LedgerEntry> entries = readOp.future().get().iterator();
            assertTrue(entries.hasNext());
            LedgerEntry entry = entries.next();
            assertNotNull(entry);
            assertEquals(i, Integer.parseInt(new String(entry.getEntryBytes())));
            entry.close();
            assertFalse(entries.hasNext());
        }

        // read multiple entries
        PendingReadOp readOp = createReadOp(lh, 0, numEntries - 1);
        readOp.parallelRead(true).submit();
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
        PendingReadOp readOp = createReadOp(lh, 11, 11);
        readOp.parallelRead(true).submit();
        expectFail(readOp.future(), Code.NoSuchEntryException);

        // read multiple entries
        readOp = createReadOp(lh, 8, 11);
        readOp.parallelRead(true).submit();
        expectFail(readOp.future(), Code.NoSuchEntryException);

        lh.close();
    }

    @Test
    public void testFailParallelRecoveryReadMissingEntryImmediately() throws Exception {
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

        PendingReadOp readOp = createRecoveryReadOp(lh, 10, 10);
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
        newConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        List<BookieId> ensemble = lh.getLedgerMetadata().getEnsembleAt(5);
        // kill two bookies
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));

        // read multiple entries
        PendingReadOp readOp = createReadOp(lh, 0, numEntries - 1);
        readOp.parallelRead(true).submit();
        Iterator<LedgerEntry> entries = readOp.future().get().iterator();

        int numReads = 0;
        while (entries.hasNext()) {
            LedgerEntry entry = entries.next();
            assertNotNull(entry);
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntryBytes())));
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
        newConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        List<BookieId> ensemble = lh.getLedgerMetadata().getEnsembleAt(5);
        // kill two bookies
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));
        killBookie(ensemble.get(2));

        // read multiple entries
        PendingReadOp readOp = createReadOp(lh, 0, numEntries - 1);
        readOp.parallelRead(true).submit();
        expectFail(readOp.future(), Code.BookieHandleNotAvailableException);

        lh.close();
        newBk.close();
    }

    @Test
    public void testLedgerEntryRequestComplete() throws Exception {
        LedgerHandle lh = mock(LedgerHandle.class);
        LedgerMetadata ledgerMetadata = mock(LedgerMetadata.class);
        ClientContext clientContext = mock(ClientContext.class);
        ClientInternalConf clientInternalConf = mock(ClientInternalConf.class);
        doReturn(clientInternalConf).when(clientContext).getConf();
        BookKeeperClientStats bookKeeperClientStats = mock(BookKeeperClientStats.class);
        doReturn(bookKeeperClientStats).when(clientContext).getClientStats();
        OpStatsLogger opStatsLogger = mock(OpStatsLogger.class);
        doReturn(opStatsLogger).when(bookKeeperClientStats).getReadOpLogger();
        doReturn(ledgerMetadata).when(lh).getLedgerMetadata();
        doReturn(2).when(ledgerMetadata).getWriteQuorumSize();
        doReturn(1).when(ledgerMetadata).getAckQuorumSize();
        doReturn(new TreeMap<>()).when(ledgerMetadata).getAllEnsembles();
        DistributionSchedule.WriteSet writeSet = mock(DistributionSchedule.WriteSet.class);
        doReturn(writeSet).when(lh).getWriteSetForReadOperation(anyLong());
        PendingReadOp pendingReadOp = new PendingReadOp(lh, clientContext, 1, 2, false);
        pendingReadOp.parallelRead(true);
        pendingReadOp.initiate();
        PendingReadOp.SingleLedgerEntryRequest first = pendingReadOp.seq.get(0);
        PendingReadOp.SingleLedgerEntryRequest second = pendingReadOp.seq.get(1);

        pendingReadOp.submitCallback(-105);

        // pendingReadOp.submitCallback(-105) will close all ledgerEntryImpl
        assertEquals(-1, first.entryImpl.getEntryId());
        assertEquals(-1, first.entryImpl.getLedgerId());
        assertEquals(-1, first.entryImpl.getLength());
        assertNull(first.entryImpl.getEntryBuffer());
        assertTrue(first.complete.get());

        assertEquals(-1, second.entryImpl.getEntryId());
        assertEquals(-1, second.entryImpl.getLedgerId());
        assertEquals(-1, second.entryImpl.getLength());
        assertNull(second.entryImpl.getEntryBuffer());
        assertTrue(second.complete.get());

        // Mock ledgerEntryImpl reuse
        ByteBuf byteBuf = Unpooled.buffer(10);
        pendingReadOp.readEntryComplete(BKException.Code.OK, 1, 1, Unpooled.buffer(10),
                new ReadOpBase.ReadContext(1, BookieId.parse("test"), first));

        // byteBuf has been release
        assertEquals(byteBuf.refCnt(), 1);
        // entryBuffer is not replaced
        assertNull(first.entryImpl.getEntryBuffer());
        // ledgerEntryRequest has been complete
        assertTrue(first.complete.get());

        pendingReadOp = new PendingReadOp(lh, clientContext, 1, 2, false);
        pendingReadOp.parallelRead(true);
        pendingReadOp.initiate();

        // read entry failed twice, will not close twice
        pendingReadOp.readEntryComplete(BKException.Code.TooManyRequestsException, 1, 1, Unpooled.buffer(10),
                new ReadOpBase.ReadContext(1, BookieId.parse("test"), first));

        pendingReadOp.readEntryComplete(BKException.Code.TooManyRequestsException, 1, 1, Unpooled.buffer(10),
                new ReadOpBase.ReadContext(1, BookieId.parse("test"), first));

        // will not complete twice when completed
        byteBuf = Unpooled.buffer(10);
        pendingReadOp.readEntryComplete(Code.OK, 1, 1, Unpooled.buffer(10),
                new ReadOpBase.ReadContext(1, BookieId.parse("test"), first));
        assertEquals(1, byteBuf.refCnt());

    }

}
