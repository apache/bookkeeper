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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.SingleDirectoryDbLedgerStorage;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.PerChannelBookieClient;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.ByteBufList;
import org.awaitility.reflect.WhiteboxImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger fencing.
 *
 */
@Slf4j
public class TestFencing extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestFencing.class);

    private final DigestType digestType;

    public TestFencing() {
        super(10);
        this.digestType = DigestType.CRC32;
    }

    /**
     * Basic fencing test. Create ledger, write to it,
     * open ledger, write again (should fail).
     */
    @Test
    public void testBasicFencing() throws Exception {
        /*
         * Create ledger.
         */
        LedgerHandle writelh = null;
        writelh = bkc.createLedger(digestType, "password".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < 10; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        /*
         * Try to open ledger.
         */
        LedgerHandle readlh = bkc.openLedger(writelh.getId(), digestType, "password".getBytes());
        // should have triggered recovery and fencing

        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
            log.info("expected a fenced error", e);
        }

        /*
         * Check if has recovered properly.
         */
        assertTrue("Has not recovered correctly: " + readlh.getLastAddConfirmed()
                   + " original " + writelh.getLastAddConfirmed(),
                   readlh.getLastAddConfirmed() == writelh.getLastAddConfirmed());
    }

    @Test
    public void testWriteAfterDeleted() throws Exception {
        LedgerHandle writeLedger;
        writeLedger = bkc.createLedger(digestType, "password".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < 10; i++) {
            long entryId = writeLedger.addEntry(tmp.getBytes());
            LOG.info("entryId: {}", entryId);
        }

        // Fence and delete.
        BookKeeperTestClient bkc2 = new BookKeeperTestClient(baseClientConf, new TestStatsProvider());
        LedgerHandle readLedger = bkc2.openLedger(writeLedger.getId(), digestType, "password".getBytes());
        bkc2.deleteLedger(readLedger.ledgerId);

        // Waiting for GC.
        for (ServerTester server : servers) {
            triggerGC(server.getServer().getBookie());
        }

        try {
            long entryId = writeLedger.addEntry(tmp.getBytes());
            LOG.info("Not expected: entryId: {}", entryId);
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            log.info("expected a fenced error", e);
            // correct behaviour
        }

        /*
         * Check it has been recovered properly.
         */
        assertTrue("Has not recovered correctly: " + readLedger.getLastAddConfirmed()
                   + " original " + writeLedger.getLastAddConfirmed(),
                   readLedger.getLastAddConfirmed() == writeLedger.getLastAddConfirmed());

        // cleanup.
        bkc2.close();
    }

    private void triggerGC(Bookie bookie) {
        LedgerStorage ledgerStorage = bookie.getLedgerStorage();
        if (ledgerStorage instanceof InterleavedLedgerStorage
                || ledgerStorage instanceof SingleDirectoryDbLedgerStorage) {
            Runnable gcThread = WhiteboxImpl.getInternalState(ledgerStorage, "gcThread");
            gcThread.run();
        } else if (ledgerStorage instanceof SortedLedgerStorage) {
            Object actLedgerStorage = WhiteboxImpl.getInternalState(ledgerStorage, "interleavedLedgerStorage");
            Runnable gcThread = WhiteboxImpl.getInternalState(actLedgerStorage, "gcThread");
            gcThread.run();
        }
    }

    @Test(timeout = 3000 * 1000)
    public void testConcurrentFenceAndDeleteLedger() throws Exception {
        LedgerHandle writeLedger;
        writeLedger = bkc.createLedger(digestType, "password".getBytes());

        String tmp = "BookKeeper is cool!";
        long lac = 0;
        for (int i = 0; i < 10; i++) {
            long entryId = writeLedger.addEntry(tmp.getBytes());
            LOG.info("entryId: {}", entryId);
            lac = entryId;
        }

        // Fence and delete.
        final BookieId bookieId = writeLedger.getLedgerMetadata().getEnsembleAt(0).get(0);
        ClientConfiguration clientConfiguration2 = newClientConfiguration();
        clientConfiguration2.setUseV2WireProtocol(true);
        ClientConfiguration clientConfiguration3 = newClientConfiguration();
        BookKeeperTestClient bkcV2 = new BookKeeperTestClient(clientConfiguration2, new TestStatsProvider());
        LedgerHandle  writeLedgerV2 = bkcV2.createLedger(digestType, "password".getBytes());
        BookKeeperTestClient bkcV3 = new BookKeeperTestClient(clientConfiguration3, new TestStatsProvider());
        LedgerHandle  writeLedgerV3 = bkcV3.createLedger(digestType, "password".getBytes());
        ReadOnlyLedgerHandle readLedgerV2 =
                (ReadOnlyLedgerHandle) bkcV2.openLedger(writeLedger.getId(), digestType, "password".getBytes());
        ReadOnlyLedgerHandle readLedgerV3 =
                (ReadOnlyLedgerHandle) bkcV3.openLedger(writeLedger.getId(), digestType, "password".getBytes());
        BookieClientImpl bookieClientV2 = (BookieClientImpl) readLedgerV2.clientCtx.getBookieClient();
        BookieClientImpl bookieClientV3 = (BookieClientImpl) readLedgerV3.clientCtx.getBookieClient();
        // Trigger opening connection.
        CompletableFuture<Integer> obtainV2 = new CompletableFuture<>();
        bookieClientV2.lookupClient(bookieId).obtain(
            new BookkeeperInternalCallbacks.GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(int rc, PerChannelBookieClient result) {
                    obtainV2.complete(rc);
                }
            }, writeLedger.getId());
        assertEquals(obtainV2.get().intValue(), BKException.Code.OK);
        CompletableFuture<Integer> obtainV3 = new CompletableFuture<>();
        bookieClientV3.lookupClient(bookieId).obtain(
            new BookkeeperInternalCallbacks.GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(int rc, PerChannelBookieClient result) {
                    obtainV3.complete(rc);
                }
            }, writeLedger.getId());
        assertEquals(obtainV3.get().intValue(), BKException.Code.OK);
        bkcV3.deleteLedger(readLedgerV3.ledgerId);

        // Waiting for GC.
        for (ServerTester server : servers) {
            triggerGC(server.getServer().getBookie());
        }

        // Verify: read requests with V2 protocol will receive a NoSuchLedgerException.
        final byte readEntryFlagFencing = 1;
        CompletableFuture<Integer> readResV2 = new CompletableFuture<>();
        bookieClientV2.readEntry(bookieId,
                writeLedger.getId(), 0, (rc, ledgerId, entryId1, buffer, ctx) -> {
                    readResV2.complete(rc);
                }, null, readEntryFlagFencing, readLedgerV2.ledgerKey);
        assertEquals(BKException.Code.NoSuchLedgerExistsException, readResV2.get().intValue());
        // Verify: read requests with V3 protocol will receive a NoSuchLedgerException.
        CompletableFuture<Integer> readResV3 = new CompletableFuture<>();
        bookieClientV3.readEntry(bookieId,
                writeLedger.getId(), 0, (rc, ledgerId, entryId1, buffer, ctx) -> {
                    readResV3.complete(rc);
                }, null, readEntryFlagFencing, readLedgerV3.ledgerKey);
        assertEquals(BKException.Code.NoSuchLedgerExistsException, readResV3.get().intValue());
        // Verify: add requests with V2 protocol will receive a NoSuchLedgerException.
        log.info("Try to add the next entry: {}:{}", writeLedger.getId(), lac + 1);
        final ByteBuf dataV2 = UnpooledByteBufAllocator.DEFAULT.heapBuffer();
        // Combine add request, and rewrite ledgerId of the request.
        dataV2.writeByte(1);
        final ByteBuf toSendV2 = (ByteBuf) writeLedgerV2.macManager.computeDigestAndPackageForSending(
                lac + 1, lac, 1, dataV2, writeLedger.ledgerKey, BookieProtocol.FLAG_NONE);
        toSendV2.setLong(28, writeLedger.getId());
        CompletableFuture<Integer> addResV2 = new CompletableFuture<>();
        bookieClientV2.addEntry(bookieId, writeLedger.getId(), writeLedger.ledgerKey, lac + 1, toSendV2,
                (rc, ledgerId, entryId1, addr, ctx) -> {
                    addResV2.complete(rc);
                }, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        assertEquals(BKException.Code.LedgerFencedException, addResV2.get().intValue());
        // Verify: read requests with V3 protocol will receive a NoSuchLedgerException.
        final ByteBuf dataV3 = UnpooledByteBufAllocator.DEFAULT.heapBuffer();
        dataV3.writeByte(1);
        // Combine add request, and rewrite ledgerId of the request.
        final ByteBufList toSendV3 = (ByteBufList) writeLedgerV3.macManager.computeDigestAndPackageForSending(
                lac + 1, lac, 1, dataV3, writeLedger.ledgerKey, BookieProtocol.FLAG_NONE);
        toSendV3.getBuffer(0).setLong(0, writeLedger.getId());
        CompletableFuture<Integer> addResV3 = new CompletableFuture<>();
        bookieClientV3.addEntry(bookieId, writeLedger.getId(), writeLedger.ledgerKey, lac + 1, toSendV3,
                (rc, ledgerId, entryId1, addr, ctx) -> {
                    addResV3.complete(rc);
                }, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        assertEquals(BKException.Code.LedgerFencedException, addResV3.get().intValue());

        // cleanup.
        writeLedgerV2.close();
        writeLedgerV3.close();
        bkcV2.close();
        bkcV3.close();
    }

    private static int threadCount = 0;

    class LedgerOpenThread extends Thread {
        private final long ledgerId;
        private long lastConfirmedEntry = 0;


        private final int tid;
        private final DigestType digestType;
        private final CyclicBarrier barrier;

        LedgerOpenThread (int tid, DigestType digestType, long ledgerId, CyclicBarrier barrier)
                throws Exception {
            super("TestFencing-LedgerOpenThread-" + threadCount++);
            this.tid = tid;
            this.ledgerId = ledgerId;
            this.digestType = digestType;
            this.barrier = barrier;
        }

        @Override
        public void run() {
            LOG.info("Thread {} started.", tid);
            LedgerHandle lh = null;
            BookKeeper bk = null;
            try {
                barrier.await();
                while (true) {
                    try {
                        bk = new BookKeeper(new ClientConfiguration(baseClientConf), bkc.getZkHandle());

                        lh = bk.openLedger(ledgerId,
                                           digestType, "".getBytes());
                        lastConfirmedEntry = lh.getLastAddConfirmed();
                        lh.close();
                        break;
                    } catch (BKException.BKMetadataVersionException zke) {
                        LOG.info("Contention with someone else recovering");
                    } catch (BKException.BKLedgerRecoveryException bkre) {
                        LOG.info("Contention with someone else recovering");
                    } finally {
                        if (lh != null) {
                            lh.close();
                        }
                        if (bk != null) {
                            bk.close();
                            bk = null;
                        }
                    }
                }
            } catch (Exception e) {
                // just exit, test should spot bad last add confirmed
                LOG.error("Exception occurred ", e);
            }
            LOG.info("Thread {} exiting, lastConfirmedEntry = {}", tid, lastConfirmedEntry);
        }

        long getLastConfirmedEntry() {
            return lastConfirmedEntry;
        }
    }

    /**
     * Try to open a ledger many times in parallel.
     * All opens should result in a ledger with an equals number of
     * entries.
     */
    @Test
    public void testManyOpenParallel() throws Exception {
        /*
         * Create ledger.
         */
        final LedgerHandle writelh = bkc.createLedger(digestType, "".getBytes());

        final int numRecovery = 10;

        final String tmp = "BookKeeper is cool!";
        final CountDownLatch latch = new CountDownLatch(numRecovery);
        Thread writethread = new Thread() {
                public void run() {
                    try {
                        while (true) {
                            writelh.addEntry(tmp.getBytes());
                            latch.countDown();
                        }
                    } catch (Exception e) {
                        LOG.info("Exception adding entry", e);
                    }
                }
            };
        writethread.start();


        CyclicBarrier barrier = new CyclicBarrier(numRecovery + 1);
        LedgerOpenThread[] threads = new LedgerOpenThread[numRecovery];
        for (int i = 0; i < numRecovery; i++) {
            threads[i] = new LedgerOpenThread(i, digestType, writelh.getId(), barrier);
            threads[i].start();
        }
        latch.await();
        barrier.await(); // should trigger threads to go

        writethread.join();
        long lastConfirmed = writelh.getLastAddConfirmed();

        for (int i = 0; i < numRecovery; i++) {
            threads[i].join();
            assertTrue("Added confirmed is incorrect",
                       lastConfirmed <= threads[i].getLastConfirmedEntry());
        }
    }

    /**
     * Test that opening a ledger in norecovery mode
     * doesn't fence off a ledger.
     */
    @Test
    public void testNoRecoveryOpen() throws Exception {
        /*
         * Create ledger.
         */
        LedgerHandle writelh = null;
        writelh = bkc.createLedger(digestType, "".getBytes());

        String tmp = "BookKeeper is cool!";
        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        /*
         * Try to open ledger.
         */
        LedgerHandle readlh = bkc.openLedgerNoRecovery(writelh.getId(),
                                                        digestType, "".getBytes());
        long numReadable = readlh.getLastAddConfirmed();
        LOG.error("numRead " + numReadable);
        readlh.readEntries(1, numReadable);

        // should not have triggered recovery and fencing
        writelh.addEntry(tmp.getBytes());
        try {
            readlh.readEntries(numReadable + 1, numReadable + 1);
            fail("Shouldn't have been able to read this far");
        } catch (BKException.BKReadException e) {
            // all is good
        }

        writelh.addEntry(tmp.getBytes());
        long numReadable2 = readlh.getLastAddConfirmed();
        assertEquals("Number of readable entries hasn't changed", numReadable2, numReadable);
        readlh.close();

        writelh.addEntry(tmp.getBytes());
        writelh.close();
    }

    /**
     * create a ledger and write entries.
     * kill a bookie in the ensemble. Recover.
     * Fence the ledger. Kill another bookie. Recover.
     */
    @Test
    public void testFencingInteractionWithBookieRecovery() throws Exception {
        System.setProperty("digestType", digestType.toString());
        System.setProperty("passwd", "testPasswd");

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());

        LedgerHandle writelh = bkc.createLedger(digestType, "testPasswd".getBytes());

        String tmp = "Foobar";

        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        BookieId bookieToKill = writelh.getLedgerMetadata().getEnsembleAt(numEntries).get(0);
        killBookie(bookieToKill);

        // write entries to change ensemble
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        admin.recoverBookieData(bookieToKill);

        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        LedgerHandle readlh = bkc.openLedger(writelh.getId(),
                                             digestType, "testPasswd".getBytes());
        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
        }

        readlh.close();
        writelh.close();
    }

    /**
     * create a ledger and write entries.
     * Fence the ledger. Kill a bookie. Recover.
     * Ensure that recover doesn't reallow adding
     */
    @Test
    public void testFencingInteractionWithBookieRecovery2() throws Exception {
        System.setProperty("digestType", digestType.toString());
        System.setProperty("passwd", "testPasswd");

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());

        LedgerHandle writelh = bkc.createLedger(digestType, "testPasswd".getBytes());

        String tmp = "Foobar";

        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        LedgerHandle readlh = bkc.openLedger(writelh.getId(),
                                             digestType, "testPasswd".getBytes());
        // should be fenced by now
        BookieId bookieToKill = writelh.getLedgerMetadata().getEnsembleAt(numEntries).get(0);
        killBookie(bookieToKill);
        admin.recoverBookieData(bookieToKill);

        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
        }

        readlh.close();
        writelh.close();
    }

    /**
     * create a ledger and write entries.
     * sleep a bookie
     * Ensure that fencing proceeds even with the bookie sleeping
     */
    @Test
    public void testFencingWithHungBookie() throws Exception {
        LedgerHandle writelh = bkc.createLedger(digestType, "testPasswd".getBytes());

        String tmp = "Foobar";

        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        CountDownLatch sleepLatch = new CountDownLatch(1);
        sleepBookie(writelh.getLedgerMetadata().getAllEnsembles().get(0L).get(1), sleepLatch);

        LedgerHandle readlh = bkc.openLedger(writelh.getId(),
                                             digestType, "testPasswd".getBytes());

        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
        }

        sleepLatch.countDown();
        readlh.close();
        writelh.close();
    }

    /**
     * Test that fencing doesn't work with a bad password.
     */
    @Test
    public void testFencingBadPassword() throws Exception {
        /*
         * Create ledger.
         */
        LedgerHandle writelh = null;
        writelh = bkc.createLedger(digestType, "password1".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < 10; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        /*
         * Try to open ledger.
         */
        try {
            bkc.openLedger(writelh.getId(), digestType, "badPassword".getBytes());
            fail("Should not have been able to open with a bad password");
        } catch (BKException.BKUnauthorizedAccessException uue) {
            // correct behaviour
        }
        // should have triggered recovery and fencing

        writelh.addEntry(tmp.getBytes());
    }

    @Test
    public void testFencingAndRestartBookies() throws Exception {
        LedgerHandle writelh = null;
        writelh = bkc.createLedger(digestType, "password".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < 10; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        /*
         * Try to open ledger.
         */
        LedgerHandle readlh = bkc.openLedger(writelh.getId(), digestType,
                                             "password".getBytes());

        restartBookies();

        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
        }

        readlh.close();
    }
}
