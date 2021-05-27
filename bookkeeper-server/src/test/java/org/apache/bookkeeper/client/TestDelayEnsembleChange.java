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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NEW_ENSEMBLE_TIME;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.REPLACE_BOOKIE_TIME;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WATCHER_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.LEDGER_ENSEMBLE_BOOKIE_DISTRIBUTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a delayed ensemble change.
 */
public class TestDelayEnsembleChange extends BookKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestDelayEnsembleChange.class);

    final DigestType digestType;
    final byte[] testPasswd = "".getBytes();

    public TestDelayEnsembleChange() {
        super(5);
        this.digestType = DigestType.CRC32;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        baseClientConf.setDelayEnsembleChange(true);
        super.setUp();
    }

    private static class VerificationCallback implements ReadEntryCallback {
        final CountDownLatch latch;
        final AtomicLong numSuccess;
        final AtomicLong numMissing;
        final AtomicLong numFailure;

        VerificationCallback(int numRequests) {
            latch = new CountDownLatch(numRequests);
            numSuccess = new AtomicLong(0L);
            numMissing = new AtomicLong(0L);
            numFailure = new AtomicLong(0L);
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
            if (rc == BKException.Code.OK) {
                numSuccess.incrementAndGet();
            } else if (rc == BKException.Code.NoSuchEntryException
                    || rc == BKException.Code.NoSuchLedgerExistsException) {
                logger.error("Missed entry({}, {}) from host {}.", ledgerId, entryId, ctx);
                numMissing.incrementAndGet();
            } else {
                logger.error("Failed to get entry({}, {}) from host {} : {}",
                        ledgerId, entryId, ctx, rc);
                numFailure.incrementAndGet();
            }
            latch.countDown();
        }
    }

    private void verifyEntries(LedgerHandle lh, long startEntry, long untilEntry,
                               long expectedSuccess, long expectedMissing) throws Exception {
        LedgerMetadata md = lh.getLedgerMetadata();

        for (long eid = startEntry; eid < untilEntry; eid++) {
            List<BookieId> addresses = md.getEnsembleAt(eid);
            VerificationCallback callback = new VerificationCallback(addresses.size());
            for (BookieId addr : addresses) {
                bkc.getBookieClient().readEntry(addr, lh.getId(), eid,
                                                callback, addr, 0, null);
            }
            callback.latch.await();
            assertEquals(expectedSuccess, callback.numSuccess.get());
            assertEquals(expectedMissing, callback.numMissing.get());
            assertEquals(0, callback.numFailure.get());
        }
    }

    private void verifyEntriesRange(LedgerHandle lh, long startEntry, long untilEntry,
                                    long expectedSuccess, long expectedMissing) throws Exception {
        LedgerMetadata md = lh.getLedgerMetadata();

        for (long eid = startEntry; eid < untilEntry; eid++) {
            List<BookieId> addresses = md.getEnsembleAt(eid);
            VerificationCallback callback = new VerificationCallback(addresses.size());
            for (BookieId addr : addresses) {
                bkc.getBookieClient().readEntry(addr, lh.getId(), eid,
                                                callback, addr, 0, null);
            }
            callback.latch.await();
            assertTrue(expectedSuccess >= callback.numSuccess.get());
            assertTrue(expectedMissing <= callback.numMissing.get());
            assertEquals(0, callback.numFailure.get());
        }
    }

    @Test
    public void testNotChangeEnsembleIfNotBrokenAckQuorum() throws Exception {
        LedgerHandle lh = bkc.createLedger(5, 5, 3, digestType, testPasswd);

        byte[] data = "foobar".getBytes();

        int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }

        // kill two bookies, but we still have 3 bookies for the ack quorum.
        ServerConfiguration conf0 = killBookie(lh.getCurrentEnsemble().get(0));
        ServerConfiguration conf1 = killBookie(lh.getCurrentEnsemble().get(1));

        for (int i = numEntries; i < 2 * numEntries; i++) {
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("There should be no ensemble change if delaying ensemble change is enabled.",
                     1, lh.getLedgerMetadata().getAllEnsembles().size());

        startAndAddBookie(conf0);
        startAndAddBookie(conf1);

        for (int i = 2 * numEntries; i < 3 * numEntries; i++) {
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("There should be no ensemble change if delaying ensemble change is enabled.",
                     1, lh.getLedgerMetadata().getAllEnsembles().size());

        // check entries
        verifyEntries(lh, 0, numEntries, 5, 0);
        verifyEntries(lh, numEntries, 2 * numEntries, 3, 2);
        verifyEntries(lh, 2 * numEntries, 3 * numEntries, 5, 0);
    }

    @Test
    public void testChangeEnsembleIfBrokenAckQuorum() throws Exception {
        startNewBookie();
        startNewBookie();
        startNewBookie();

        bkc.getTestStatsProvider().clear();
        LedgerHandle lh = bkc.createLedger(5, 5, 3, digestType, testPasswd);

        byte[] data = "foobar".getBytes();

        int numEntries = 5;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }

        for (BookieId addr : lh.getLedgerMetadata().getAllEnsembles().get(0L)) {
            StringBuilder nameBuilder = new StringBuilder(CLIENT_SCOPE);
            nameBuilder.append('.').
                    append("bookie_").
                    append(TestUtils.buildStatsCounterPathFromBookieID(addr)).
                    append('.').
                    append(LEDGER_ENSEMBLE_BOOKIE_DISTRIBUTION);
            assertTrue(
                    LEDGER_ENSEMBLE_BOOKIE_DISTRIBUTION + " should be > 0 for " + addr,
                    bkc.getTestStatsProvider().getCounter(nameBuilder.toString())
                            .get() > 0);
        }
        assertTrue(
                "Stats should have captured a new ensemble",
                bkc.getTestStatsProvider().getOpStatsLogger(
                        CLIENT_SCOPE + "." + WATCHER_SCOPE + "." + NEW_ENSEMBLE_TIME)
                        .getSuccessCount() > 0);
        assertTrue(
                "Stats should not have captured an ensemble change",
                bkc.getTestStatsProvider().getOpStatsLogger(
                        CLIENT_SCOPE + "." + WATCHER_SCOPE + "." + REPLACE_BOOKIE_TIME)
                        .getSuccessCount() == 0);

        logger.info("Kill bookie 0 and write {} entries.", numEntries);

        // kill two bookies, but we still have 3 bookies for the ack quorum.
        ServerConfiguration conf0 = killBookie(lh.getCurrentEnsemble().get(0));

        for (int i = numEntries; i < 2 * numEntries; i++) {
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("There should be no ensemble change if delaying ensemble change is enabled.",
                     1, lh.getLedgerMetadata().getAllEnsembles().size());
        assertTrue(
                "Stats should not have captured an ensemble change",
                bkc.getTestStatsProvider().getOpStatsLogger(
                        CLIENT_SCOPE + "." + WATCHER_SCOPE + "." + REPLACE_BOOKIE_TIME)
                        .getSuccessCount() == 0);

        logger.info("Kill bookie 1 and write another {} entries.", numEntries);

        ServerConfiguration conf1 = killBookie(lh.getCurrentEnsemble().get(1));

        for (int i = 2 * numEntries; i < 3 * numEntries; i++) {
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("There should be no ensemble change if delaying ensemble change is enabled.",
                     1, lh.getLedgerMetadata().getAllEnsembles().size());

        logger.info("Kill bookie 2 and write another {} entries.", numEntries);

        ServerConfiguration conf2 = killBookie(lh.getCurrentEnsemble().get(2));

        for (int i = 3 * numEntries; i < 4 * numEntries; i++) {
            lh.addEntry(data);
        }

        // ensemble change should kick in
        assertEquals("There should be ensemble change if ack quorum couldn't be formed.",
                     2, lh.getLedgerMetadata().getAllEnsembles().size());
        assertTrue(
                "Stats should have captured an ensemble change",
                bkc.getTestStatsProvider().getOpStatsLogger(
                        CLIENT_SCOPE + "." + WATCHER_SCOPE + "." + REPLACE_BOOKIE_TIME)
                        .getSuccessCount() > 0);

        List<BookieId> firstFragment = lh.getLedgerMetadata().getEnsembleAt(0);
        List<BookieId> secondFragment = lh.getLedgerMetadata().getEnsembleAt(3 * numEntries);
        assertFalse(firstFragment.get(0).equals(secondFragment.get(0)));
        assertFalse(firstFragment.get(1).equals(secondFragment.get(1)));
        assertFalse(firstFragment.get(2).equals(secondFragment.get(2)));
        assertEquals(firstFragment.get(3), secondFragment.get(3));
        assertEquals(firstFragment.get(4), secondFragment.get(4));

        startAndAddBookie(conf0);
        startAndAddBookie(conf1);
        startAndAddBookie(conf2);

        for (int i = 4 * numEntries; i < 5 * numEntries; i++) {
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("There should be no ensemble change if delaying ensemble change is enabled.",
                     2, lh.getLedgerMetadata().getAllEnsembles().size());

        // check entries
        verifyEntries(lh, 0, numEntries, 5, 0);
        verifyEntries(lh, numEntries, 2 * numEntries, 4, 1);
        verifyEntries(lh, 2 * numEntries, 3 * numEntries, 3, 2);
        verifyEntries(lh, 3 * numEntries, 4 * numEntries, 5, 0);
        verifyEntries(lh, 4 * numEntries, 5 * numEntries, 5, 0);
    }

    @Test
    public void testEnsembleChangeWithNotEnoughBookies() throws Exception {
        startNewBookie();

        LedgerHandle lh = bkc.createLedger(5, 5, 3, digestType, testPasswd);

        byte[] data = "foobar".getBytes();

        int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }

        logger.info("Killed 3 bookies and add {} more entries : {}", numEntries, lh.getLedgerMetadata());

        // kill three bookies, but we only have 2 new bookies for ensemble change.
        ServerConfiguration conf0 = killBookie(lh.getCurrentEnsemble().get(0));
        ServerConfiguration conf1 = killBookie(lh.getCurrentEnsemble().get(1));
        ServerConfiguration conf2 = killBookie(lh.getCurrentEnsemble().get(2));

        for (int i = numEntries; i < 2 * numEntries; i++) {
            lh.addEntry(data);
        }

        logger.info("Ledger metadata after killed bookies : {}", lh.getLedgerMetadata());

        // ensure there is ensemble changed
        assertEquals("There should be ensemble change if ack quorum is broken.",
                     2, lh.getLedgerMetadata().getAllEnsembles().size());

        startAndAddBookie(conf0);
        startAndAddBookie(conf1);
        startAndAddBookie(conf2);

        for (int i = 2 * numEntries; i < 3 * numEntries; i++) {
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("There should be no ensemble change after adding failed bookies back.",
                     2, lh.getLedgerMetadata().getAllEnsembles().size());

        // check entries
        verifyEntries(lh, 0, numEntries, 5, 0);
        verifyEntries(lh, numEntries, 2 * numEntries, 3, 2);
        verifyEntries(lh, 2 * numEntries, 3 * numEntries, 5, 0);
    }

    @Test
    public void testEnsembleChangeWithMoreBookieFailures() throws Exception {
        for (int i = 0; i < 5; i++) {
            startNewBookie();
        }

        LedgerHandle lh = bkc.createLedger(5, 5, 3, digestType, testPasswd);

        byte[] data = "foobar".getBytes();

        int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            logger.info("Add entry {}", i);
            lh.addEntry(data);
        }

        logger.info("Killed 5 bookies and add {} more entries : {}", numEntries, lh.getLedgerMetadata());

        // kill 5 bookies to introduce more bookie failure
        List<ServerConfiguration> confs = new ArrayList<ServerConfiguration>(5);
        for (int i = 0; i < 5; i++) {
            confs.add(killBookie(lh.getCurrentEnsemble().get(i)));
        }

        for (int i = numEntries; i < 2 * numEntries; i++) {
            logger.info("Add entry {}", i);
            lh.addEntry(data);
        }

        logger.info("Ledger metadata after killed bookies : {}", lh.getLedgerMetadata());

        // ensure there is no ensemble changed
        assertEquals("There should be ensemble change if breaking ack quorum.",
                     2, lh.getLedgerMetadata().getAllEnsembles().size());

        for (ServerConfiguration conf : confs) {
            startAndAddBookie(conf);
        }

        for (int i = 2 * numEntries; i < 3 * numEntries; i++) {
            logger.info("Add entry {}", i);
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("There should not be ensemble changed if delaying ensemble change is enabled.",
                     2, lh.getLedgerMetadata().getAllEnsembles().size());

        // check entries
        verifyEntries(lh, 0, numEntries, 5, 0);
        verifyEntriesRange(lh, numEntries, 2 * numEntries, 5, 0);
        verifyEntries(lh, 2 * numEntries, 3 * numEntries, 5, 0);
    }

    @Test
    public void testChangeEnsembleIfBookieReadOnly() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, 2, digestType, testPasswd);

        byte[] data = "foobar".getBytes();

        int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }

        // kill two bookies, but we still have 3 bookies for the ack quorum.
        setBookieToReadOnly(lh.getCurrentEnsemble().get(0));

        for (int i = numEntries; i < 2 * numEntries; i++) {
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("The ensemble should change when a bookie is readonly even if we delay ensemble change.",
            2, lh.getLedgerMetadata().getAllEnsembles().size());

    }

    @Test
    public void testChangeEnsembleSecondBookieReadOnly() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, 2, digestType, testPasswd);

        byte[] data = "foobar".getBytes();

        int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }

        BookieId failedBookie = lh.getCurrentEnsemble().get(0);
        BookieId readOnlyBookie = lh.getCurrentEnsemble().get(1);
        ServerConfiguration conf0 = killBookie(failedBookie);

        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }

        assertEquals("There should be ensemble change if delaying ensemble change is enabled.",
            1, lh.getLedgerMetadata().getAllEnsembles().size());

        // kill two bookies, but we still have 3 bookies for the ack quorum.
        setBookieToReadOnly(readOnlyBookie);

        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }

        // ensure there is no ensemble changed
        assertEquals("The ensemble should change when a bookie is readonly even if we delay ensemble change.",
            2, lh.getLedgerMetadata().getAllEnsembles().size());
        assertEquals(3, lh.getCurrentEnsemble().size());
        assertFalse(lh.getCurrentEnsemble().contains(failedBookie));
        assertFalse(lh.getCurrentEnsemble().contains(readOnlyBookie));
    }

}
