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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieShell.UpdateLedgerNotifier;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.MathUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test update operations on a ledger.
 */
public class UpdateLedgerOpTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateLedgerOpTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final int printprogress = 5;

    public UpdateLedgerOpTest() {
        super(3);
        useUUIDasBookieId = false;
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
     * Tests verifies update bookie id to FQDN hostname when there are many ledgers.
     */
    @Test
    public void testManyLedgersWithFQDNHostname() throws Exception {
        testManyLedgers(false);
    }

    /**
     * Tests verifies update bookie id to short hostname when there are many ledgers.
     */
    @Test(timeout = 120000)
    public void testManyLedgersWithShortHostname() throws Exception {
        testManyLedgers(true);
    }

    public void testManyLedgers(boolean useShortHostName) throws Exception {
        try (BookKeeper bk = new BookKeeper(baseClientConf, zkc);
            BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk)) {

            LOG.info("Create ledger and add entries to it");
            List<LedgerHandle> ledgers = new ArrayList<LedgerHandle>();
            LedgerHandle lh1 = createLedgerWithEntries(bk, 0);
            ledgers.add(lh1);
            for (int i = 0; i < 99; i++) {
                ledgers.add(createLedgerWithEntries(bk, 0));
            }

            List<BookieId> ensemble = lh1.getLedgerMetadata().getEnsembleAt(0);

            BookieSocketAddress curBookieAddr = bk.getBookieAddressResolver().resolve(ensemble.get(0));
            baseConf.setUseHostNameAsBookieID(true);
            baseConf.setUseShortHostName(useShortHostName);
            BookieSocketAddress curBookieId = BookieImpl.getBookieAddress(baseConf);
            BookieId toBookieAddr = new BookieSocketAddress(curBookieId.getHostName() + ":"
                    + curBookieAddr.getPort()).toBookieId();
            UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, bkadmin);
            updateLedgerOp.updateBookieIdInLedgers(curBookieAddr.toBookieId(), toBookieAddr,
                                                   5, 25, Integer.MIN_VALUE, progressable);

            for (LedgerHandle lh : ledgers) {
                lh.close();
                LedgerHandle openLedger = bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
                ensemble = openLedger.getLedgerMetadata().getEnsembleAt(0);
                assertTrue("Failed to update the ledger metadata to use bookie host name",
                        ensemble.contains(toBookieAddr));
                assertFalse("Failed to update the ledger metadata to use bookie host name",
                        ensemble.contains(curBookieAddr.toBookieId()));
            }
        }
    }

    /**
     * Tests verifies with limit value lesser than the total number of ledgers.
     */
    @Test
    public void testLimitLessThanTotalLedgers() throws Exception {
        try (BookKeeper bk = new BookKeeper(baseClientConf, zkc);
            BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk)) {

            LOG.info("Create ledger and add entries to it");
            List<LedgerHandle> ledgers = new ArrayList<LedgerHandle>();
            LedgerHandle lh1 = createLedgerWithEntries(bk, 0);
            ledgers.add(lh1);
            for (int i = 1; i < 10; i++) {
                ledgers.add(createLedgerWithEntries(bk, 0));
            }

            List<BookieId> ensemble = lh1.getLedgerMetadata().getEnsembleAt(0);

            BookieId curBookieAddr = ensemble.get(0);
            baseConf.setUseHostNameAsBookieID(true);

            BookieSocketAddress toBookieId = BookieImpl.getBookieAddress(baseConf);
            BookieId toBookieAddr = new BookieSocketAddress(toBookieId.getHostName() + ":"
                    + bk.getBookieAddressResolver().resolve(curBookieAddr).getPort()).toBookieId();
            UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, bkadmin);
            updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 7, 35, 4, progressable);
            int updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
            assertEquals("Failed to update the ledger metadata to use bookie host name", 4, updatedLedgersCount);

            // next execution
            updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 2, 10, 10, progressable);
            updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
            assertEquals("Failed to update the ledger metadata to use bookie host name", 10, updatedLedgersCount);

            // no ledgers
            updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 3, 15, 20, progressable);
            updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
            assertEquals("Failed to update the ledger metadata to use bookie host name", 10, updatedLedgersCount);

            // no ledgers
            updateLedgerOp.updateBookieIdInLedgers(curBookieAddr, toBookieAddr, 3, 15, Integer.MIN_VALUE, progressable);
            updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
            assertEquals("Failed to update the ledger metadata to use bookie host name", 10, updatedLedgersCount);
        }
    }

    /**
     * Tests verifies the ensemble reformation after updating the bookie id to
     * FQDN hostname in the existing ensemble.
     */
    @Test
    public void testChangeEnsembleAfterRenamingToFQDNHostname() throws Exception {
        testChangeEnsembleAfterRenaming(false);
    }

    /**
     * Tests verifies the ensemble reformation after updating the bookie id to
     * short hostname in the existing ensemble.
     */
    @Test(timeout = 120000)
    public void testChangeEnsembleAfterRenamingToShortHostname() throws Exception {
        testChangeEnsembleAfterRenaming(true);
    }

    public void testChangeEnsembleAfterRenaming(boolean useShortHostName) throws Exception {

        try (BookKeeper bk = new BookKeeper(baseClientConf, zkc);
            BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk)) {

            LOG.info("Create ledger and add entries to it");
            LedgerHandle lh = createLedgerWithEntries(bk, 100);

            BookieServer bookieServer = serverByIndex(0);
            List<BookieId> ensemble = lh.getLedgerMetadata().getEnsembleAt(0);
            BookieSocketAddress curBookieAddr = null;
            for (BookieId bookieSocketAddress : ensemble) {
                BookieSocketAddress resolved = bk.getBookieAddressResolver().resolve(bookieSocketAddress);
                if (bookieServer.getLocalAddress().equals(resolved)) {
                    curBookieAddr = resolved;
                }
            }
            assertNotNull("Couldn't find the bookie in ledger metadata!", curBookieAddr);
            baseConf.setUseHostNameAsBookieID(true);
            baseConf.setUseShortHostName(useShortHostName);
            BookieSocketAddress toBookieId = BookieImpl.getBookieAddress(baseConf);
            BookieId toBookieAddr = new BookieSocketAddress(toBookieId.getHostName() + ":"
                    + curBookieAddr.getPort()).toBookieId();
            UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, bkadmin);
            updateLedgerOp.updateBookieIdInLedgers(curBookieAddr.toBookieId(), toBookieAddr, 5, 25, 100, progressable);

            bookieServer.shutdown();

            ServerConfiguration serverConf1 = newServerConfiguration();
            startAndAddBookie(serverConf1);

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
            assertEquals("Failed to reform ensemble!", 2, ledgerMetadata.getAllEnsembles().size());
            ensemble = ledgerMetadata.getEnsembleAt(0);
            assertTrue("Failed to update the ledger metadata to use bookie host name",
                    ensemble.contains(toBookieAddr));
        }
    }

    /**
     * Tests verifies simultaneous flow between adding entries and rename of
     * bookie id.
     */
    @Test
    public void testRenameWhenAddEntryInProgress() throws Exception {
        try (final BookKeeper bk = new BookKeeper(baseClientConf, zkc);
            BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk)) {

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

                }
            };
            th.start();
            List<BookieId> ensemble = lh.getLedgerMetadata().getEnsembleAt(0);
            BookieSocketAddress curBookieAddr = bk.getBookieAddressResolver().resolve(ensemble.get(0));
            BookieId toBookieAddr = BookieId.parse("localhost:" + curBookieAddr.getPort());
            UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, bkadmin);
            updateLedgerOp.updateBookieIdInLedgers(curBookieAddr.toBookieId(), toBookieAddr, 5, 25, 100, progressable);

            if (!latch.await(120, TimeUnit.SECONDS)) {
                throw new Exception("Entries took too long to add");
            }
            if (rc.get() != BKException.Code.OK) {
                throw BKException.create(rc.get());
            }
            lh.close();
            LedgerHandle openLedger = bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
            ensemble = openLedger.getLedgerMetadata().getEnsembleAt(0);
            assertTrue("Failed to update the ledger metadata to use bookie host name",
                    ensemble.contains(toBookieAddr));
        }
    }

    private int getUpdatedLedgersCount(BookKeeper bk, List<LedgerHandle> ledgers, BookieId toBookieAddr)
            throws InterruptedException, BKException {
        List<BookieId> ensemble;
        int updatedLedgersCount = 0;
        for (LedgerHandle lh : ledgers) {
            lh.close();
            LedgerHandle openLedger = bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
            ensemble = openLedger.getLedgerMetadata().getEnsembleAt(0);
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
