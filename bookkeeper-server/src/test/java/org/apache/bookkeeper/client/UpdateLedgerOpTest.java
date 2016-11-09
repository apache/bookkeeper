/**
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieShell.UpdateLedgerNotifier;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.MathUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateLedgerOpTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateLedgerOpTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final int printprogress = 5;

    public UpdateLedgerOpTest() {
        super(3);
        baseConf.setAllowLoopback(true);
        baseConf.setGcWaitTime(100000);
    }

    UpdateLedgerNotifier progressable = new UpdateLedgerNotifier() {
        long lastReport = System.nanoTime();

        @Override
        public void progress(long updated, long issued) {
            if (TimeUnit.MILLISECONDS.toSeconds(MathUtils.elapsedMSec(lastReport)) >= printprogress) {
                LOG.info("Number of ledgers issued={}, updated={}", issued, updated);
                lastReport = MathUtils.nowInNano();
            }
        }
    };

    /**
     * Tests verifies update bookie id when there are many ledgers.
     */
    @Test(timeout = 120000)
    public void testManyLedgers() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk);

        LOG.info("Create ledger and add entries to it");
        List<LedgerHandle> ledgers = new ArrayList<LedgerHandle>();
        LedgerHandle lh1 = createLedgerWithEntries(bk, 0);
        ledgers.add(lh1);
        for (int i = 0; i < 99; i++) {
            ledgers.add(createLedgerWithEntries(bk, 0));
        }

        ArrayList<BookieSocketAddress> ensemble = lh1.getLedgerMetadata().getEnsemble(0);

        BookieSocketAddress curBookieAddr = ensemble.get(0);
        baseConf.setUseHostNameAsBookieID(true);
        BookieSocketAddress curBookieId = Bookie.getBookieAddress(baseConf);
        BookieSocketAddress toBookieAddr = new BookieSocketAddress(curBookieId.getHostName() + ":"
                + curBookieAddr.getPort());
        UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, bkadmin);
        updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 5, Integer.MIN_VALUE, progressable);

        for (LedgerHandle lh : ledgers) {
            // ledger#close() would hit BadVersion exception as rename
            // increments cversion. But LedgerMetadata#isConflictWith()
            // gracefully handles this conflicts.
            lh.close();
            LedgerHandle openLedger = bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
            ensemble = openLedger.getLedgerMetadata().getEnsemble(0);
            Assert.assertTrue("Failed to update the ledger metadata to use bookie host name",
                    ensemble.contains(toBookieAddr));
            Assert.assertFalse("Failed to update the ledger metadata to use bookie host name",
                    ensemble.contains(curBookieAddr));
        }
    }

    /**
     * Tests verifies with limit value lesser than the total number of ledgers.
     */
    @Test(timeout = 120000)
    public void testLimitLessThanTotalLedgers() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk);

        LOG.info("Create ledger and add entries to it");
        List<LedgerHandle> ledgers = new ArrayList<LedgerHandle>();
        LedgerHandle lh1 = createLedgerWithEntries(bk, 0);
        ledgers.add(lh1);
        for (int i = 1; i < 10; i++) {
            ledgers.add(createLedgerWithEntries(bk, 0));
        }

        ArrayList<BookieSocketAddress> ensemble = lh1.getLedgerMetadata().getEnsemble(0);

        BookieSocketAddress curBookieAddr = ensemble.get(0);
        baseConf.setUseHostNameAsBookieID(true);
        BookieSocketAddress toBookieId = Bookie.getBookieAddress(baseConf);
        BookieSocketAddress toBookieAddr = new BookieSocketAddress(toBookieId.getHostName() + ":"
                + curBookieAddr.getPort());
        UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, bkadmin);
        updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 7, 4, progressable);
        int updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
        Assert.assertEquals("Failed to update the ledger metadata to use bookie host name", 4, updatedLedgersCount);

        // next execution
        updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 2, 10, progressable);
        updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
        Assert.assertEquals("Failed to update the ledger metadata to use bookie host name", 10, updatedLedgersCount);

        // no ledgers
        updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 3, 20, progressable);
        updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
        Assert.assertEquals("Failed to update the ledger metadata to use bookie host name", 10, updatedLedgersCount);

        // no ledgers
        updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 3, Integer.MIN_VALUE, progressable);
        updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
        Assert.assertEquals("Failed to update the ledger metadata to use bookie host name", 10, updatedLedgersCount);
    }

    /**
     * Tests verifies the ensemble reformation after updating the bookie id in
     * the existing ensemble
     */
    @Test(timeout = 120000)
    public void testChangeEnsembleAfterRenaming() throws Exception {

        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk);

        LOG.info("Create ledger and add entries to it");
        LedgerHandle lh = createLedgerWithEntries(bk, 100);

        BookieServer bookieServer = bs.get(0);
        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsemble(0);
        BookieSocketAddress curBookieAddr = null;
        for (BookieSocketAddress bookieSocketAddress : ensemble) {
            if (bookieServer.getLocalAddress().equals(bookieSocketAddress)) {
                curBookieAddr = bookieSocketAddress;
            }
        }
        Assert.assertNotNull("Couldn't find the bookie in ledger metadata!", curBookieAddr);
        baseConf.setUseHostNameAsBookieID(true);
        BookieSocketAddress toBookieId = Bookie.getBookieAddress(baseConf);
        BookieSocketAddress toBookieAddr = new BookieSocketAddress(toBookieId.getHostName() + ":"
                + curBookieAddr.getPort());
        UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, bkadmin);
        updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 5, 100, progressable);

        bookieServer.shutdown();

        ServerConfiguration serverConf1 = newServerConfiguration();
        bsConfs.add(serverConf1);
        bs.add(startBookie(serverConf1));

        // ledger#asyncAddEntry() would hit BadVersion exception as rename incr
        // cversion. But LedgerMetadata#isConflictWith() gracefully handles
        // this conflicts.
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        lh.asyncAddEntry("foobar".getBytes(), new AddCallback() {
            @Override
            public void addComplete(int rccb, LedgerHandle lh, long entryId, Object ctx) {
                rc.compareAndSet(BKException.Code.OK, rccb);
                latch.countDown();
            }
        }, null);
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new Exception("Entries took too long to add");
        }
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }
        lh.close();
        LedgerHandle openLedger = bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
        final LedgerMetadata ledgerMetadata = openLedger.getLedgerMetadata();
        Assert.assertEquals("Failed to reform ensemble!", 2, ledgerMetadata.getEnsembles().size());
        ensemble = ledgerMetadata.getEnsemble(0);
        Assert.assertTrue("Failed to update the ledger metadata to use bookie host name",
                ensemble.contains(toBookieAddr));
    }

    /**
     * Tests verifies simultaneous flow between adding entries and rename of
     * bookie id
     */
    @Test(timeout = 120000)
    public void testRenameWhenAddEntryInProgress() throws Exception {
        final BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk);

        LOG.info("Create ledger and add entries to it");
        final int numOfEntries = 5000;
        final CountDownLatch latch = new CountDownLatch(numOfEntries);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        final LedgerHandle lh = createLedgerWithEntries(bk, 1);
        latch.countDown();
        Thread th = new Thread() {
            public void run() {
                final AddCallback cb = new AddCallback() {
                    public void addComplete(int rccb, LedgerHandle lh, long entryId, Object ctx) {
                        rc.compareAndSet(BKException.Code.OK, rccb);
                        if (entryId % 100 == 0) {
                            LOG.info("Added entries till entryId:{}", entryId);
                        }
                        latch.countDown();
                    }
                };
                for (int i = 1; i < numOfEntries; i++) {
                    lh.asyncAddEntry(("foobar" + i).getBytes(), cb, null);
                }

            };
        };
        th.start();
        ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsemble(0);
        BookieSocketAddress curBookieAddr = ensemble.get(0);
        BookieSocketAddress toBookieAddr = new BookieSocketAddress("localhost:" + curBookieAddr.getPort());
        UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, bkadmin);
        updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 5, 100, progressable);

        if (!latch.await(120, TimeUnit.SECONDS)) {
            throw new Exception("Entries took too long to add");
        }
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }
        lh.close();
        LedgerHandle openLedger = bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
        ensemble = openLedger.getLedgerMetadata().getEnsemble(0);
        Assert.assertTrue("Failed to update the ledger metadata to use bookie host name",
                ensemble.contains(toBookieAddr));
    }

    private int getUpdatedLedgersCount(BookKeeper bk, List<LedgerHandle> ledgers, BookieSocketAddress toBookieAddr)
            throws InterruptedException, BKException {
        ArrayList<BookieSocketAddress> ensemble;
        int updatedLedgersCount = 0;
        for (LedgerHandle lh : ledgers) {
            // ledger#close() would hit BadVersion exception as rename
            // increments cversion. But LedgerMetadata#isConflictWith()
            // gracefully handles this conflicts.
            lh.close();
            LedgerHandle openLedger = bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
            ensemble = openLedger.getLedgerMetadata().getEnsemble(0);
            if (ensemble.contains(toBookieAddr)) {
                updatedLedgersCount++;
            }
        }
        return updatedLedgersCount;
    }

    private LedgerHandle createLedgerWithEntries(BookKeeper bk, int numOfEntries) throws Exception {
        LedgerHandle lh = bk.createLedger(3, 3, digestType, PASSWORD.getBytes());
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        final CountDownLatch latch = new CountDownLatch(numOfEntries);

        final AddCallback cb = new AddCallback() {
            public void addComplete(int rccb, LedgerHandle lh, long entryId, Object ctx) {
                rc.compareAndSet(BKException.Code.OK, rccb);
                latch.countDown();
            }
        };
        for (int i = 0; i < numOfEntries; i++) {
            lh.asyncAddEntry(("foobar" + i).getBytes(), cb, null);
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new Exception("Entries took too long to add");
        }
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }
        return lh;
    }
}
