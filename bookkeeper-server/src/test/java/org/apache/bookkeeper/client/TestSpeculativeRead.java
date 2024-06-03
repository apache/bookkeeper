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

import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.SPECULATIVE_READ_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.LocalBookieEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger fencing.
 *
 */
public class TestSpeculativeRead extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestSpeculativeRead.class);

    private final DigestType digestType;
    byte[] passwd = "specPW".getBytes();

    public TestSpeculativeRead() {
        super(10);
        this.digestType = DigestType.CRC32;
    }

    long getLedgerToRead(int ensemble, int quorum) throws Exception {
        byte[] data = "Data for test".getBytes();
        LedgerHandle l = bkc.createLedger(ensemble, quorum, digestType, passwd);
        for (int i = 0; i < 10; i++) {
            l.addEntry(data);
        }
        l.close();

        return l.getId();
    }

    @SuppressWarnings("deprecation")
    BookKeeperTestClient createClient(int specTimeout) throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setSpeculativeReadTimeout(specTimeout)
            .setReadTimeout(30000)
            .setReorderReadSequenceEnabled(true)
            .setEnsemblePlacementPolicySlowBookies(true)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        return new BookKeeperTestClient(conf, new TestStatsProvider());
    }

    class LatchCallback implements ReadCallback {
        CountDownLatch l = new CountDownLatch(1);
        boolean success = false;
        long startMillis = System.currentTimeMillis();
        long endMillis = Long.MAX_VALUE;

        public void readComplete(int rc,
                                 LedgerHandle lh,
                                 Enumeration<LedgerEntry> seq,
                                 Object ctx) {
            endMillis = System.currentTimeMillis();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got response {} {}", rc, getDuration());
            }
            success = rc == BKException.Code.OK;
            l.countDown();
        }

        long getDuration() {
            return endMillis - startMillis;
        }

        void expectSuccess(int milliseconds) throws Exception {
            assertTrue(l.await(milliseconds, TimeUnit.MILLISECONDS));
            assertTrue(success);
        }

        void expectFail(int milliseconds) throws Exception {
            assertTrue(l.await(milliseconds, TimeUnit.MILLISECONDS));
            assertFalse(success);
        }

        void expectTimeout(int milliseconds) throws Exception {
            assertFalse(l.await(milliseconds, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Test basic speculative functionality.
     * - Create 2 clients with read timeout disabled, one with spec
     *   read enabled, the other not.
     * - create ledger
     * - sleep second bookie in ensemble
     * - read first entry, both should find on first bookie.
     * - read second bookie, spec client should find on bookie three,
     *   non spec client should hang.
     */
    @Test
    public void testSpeculativeRead() throws Exception {
        long id = getLedgerToRead(3, 2);
        BookKeeperTestClient bknospec = createClient(0); // disabled
        BookKeeperTestClient bkspec = createClient(2000);

        LedgerHandle lnospec = bknospec.openLedger(id, digestType, passwd);
        LedgerHandle lspec = bkspec.openLedger(id, digestType, passwd);

        // sleep second bookie
        CountDownLatch sleepLatch = new CountDownLatch(1);
        BookieId second = lnospec.getLedgerMetadata().getAllEnsembles().get(0L).get(1);
        sleepBookie(second, sleepLatch);

        try {
            // read first entry, both go to first bookie, should be fine
            LatchCallback nospeccb = new LatchCallback();
            LatchCallback speccb = new LatchCallback();
            lnospec.asyncReadEntries(0, 0, nospeccb, null);
            lspec.asyncReadEntries(0, 0, speccb, null);
            nospeccb.expectSuccess(2000);
            speccb.expectSuccess(2000);

            // read second entry, both look for second book, spec read client
            // tries third bookie, nonspec client hangs as read timeout is very long.
            nospeccb = new LatchCallback();
            speccb = new LatchCallback();
            lnospec.asyncReadEntries(1, 1, nospeccb, null);
            lspec.asyncReadEntries(1, 1, speccb, null);
            speccb.expectSuccess(4000);
            nospeccb.expectTimeout(4000);
            // Check that the second bookie is registered as slow at entryId 1
            RackawareEnsemblePlacementPolicy rep = (RackawareEnsemblePlacementPolicy) bkspec.getPlacementPolicy();
            assertTrue(rep.slowBookies.asMap().size() == 1);

            assertTrue(
                    "Stats should not reflect speculative reads if disabled",
                    bknospec.getTestStatsProvider()
                            .getCounter(CLIENT_SCOPE + "." + SPECULATIVE_READ_COUNT).get() == 0);
            assertTrue(
                    "Stats should reflect speculative reads",
                    bkspec.getTestStatsProvider()
                            .getCounter(CLIENT_SCOPE + "." + SPECULATIVE_READ_COUNT).get() > 0);

        } finally {
            sleepLatch.countDown();
            lspec.close();
            lnospec.close();
            bkspec.close();
            bknospec.close();
        }
    }

    /**
     * Test that if more than one replica is down, we can still read, as long as the quorum
     * size is larger than the number of down replicas.
     */
    @Test
    public void testSpeculativeReadMultipleReplicasDown() throws Exception {
        long id = getLedgerToRead(5, 5);
        int timeout = 5000;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        // sleep bookie 1, 2 & 4
        CountDownLatch sleepLatch = new CountDownLatch(1);
        sleepBookie(l.getLedgerMetadata().getAllEnsembles().get(0L).get(1), sleepLatch);
        sleepBookie(l.getLedgerMetadata().getAllEnsembles().get(0L).get(2), sleepLatch);
        sleepBookie(l.getLedgerMetadata().getAllEnsembles().get(0L).get(4), sleepLatch);

        try {
            // read first entry, should complete faster than timeout
            // as bookie 0 has the entry
            LatchCallback latch0 = new LatchCallback();
            l.asyncReadEntries(0, 0, latch0, null);
            latch0.expectSuccess(timeout / 2);

            // second should have to hit two timeouts (bookie 1 & 2)
            // bookie 3 has the entry
            LatchCallback latch1 = new LatchCallback();
            l.asyncReadEntries(1, 1, latch1, null);
            latch1.expectTimeout(timeout);
            latch1.expectSuccess(timeout * 2);
            LOG.info("Timeout {} latch1 duration {}", timeout, latch1.getDuration());
            assertTrue("should have taken longer than two timeouts, but less than 3",
                       latch1.getDuration() >= timeout * 2
                       && latch1.getDuration() < timeout * 3);

            // bookies 1 & 2 should be registered as slow bookies because of speculative reads
            Set<BookieId> expectedSlowBookies = new HashSet<>();
            expectedSlowBookies.add(l.getLedgerMetadata().getAllEnsembles().get(0L).get(1));
            expectedSlowBookies.add(l.getLedgerMetadata().getAllEnsembles().get(0L).get(2));
            assertEquals(((RackawareEnsemblePlacementPolicy) bkspec.getPlacementPolicy()).slowBookies.asMap().keySet(),
                expectedSlowBookies);

            // third should not hit timeouts since bookies 1 & 2 are registered as slow
            // bookie 3 has the entry
            LatchCallback latch2 = new LatchCallback();
            l.asyncReadEntries(2, 2, latch2, null);
            latch2.expectSuccess(timeout);

            // fourth should have no timeout
            // bookie 3 has the entry
            LatchCallback latch3 = new LatchCallback();
            l.asyncReadEntries(3, 3, latch3, null);
            latch3.expectSuccess(timeout / 2);

            // fifth should hit one timeout, (bookie 4)
            // bookie 0 has the entry
            LatchCallback latch4 = new LatchCallback();
            l.asyncReadEntries(4, 4, latch4, null);
            latch4.expectTimeout(timeout / 2);
            latch4.expectSuccess(timeout);
            LOG.info("Timeout {} latch4 duration {}", timeout, latch4.getDuration());
            assertTrue("should have taken longer than one timeout, but less than 2",
                       latch4.getDuration() >= timeout
                       && latch4.getDuration() < timeout * 2);

        } finally {
            sleepLatch.countDown();
            l.close();
            bkspec.close();
        }
    }

    /**
     * Test that if after a speculative read is kicked off, the original read completes
     * nothing bad happens.
     */
    @Test
    public void testSpeculativeReadFirstReadCompleteIsOk() throws Exception {
        long id = getLedgerToRead(2, 2);
        int timeout = 1000;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        // sleep bookies
        CountDownLatch sleepLatch0 = new CountDownLatch(1);
        CountDownLatch sleepLatch1 = new CountDownLatch(1);
        sleepBookie(l.getLedgerMetadata().getAllEnsembles().get(0L).get(0), sleepLatch0);
        sleepBookie(l.getLedgerMetadata().getAllEnsembles().get(0L).get(1), sleepLatch1);

        try {
            // read goes to first bookie, spec read timeout occurs,
            // goes to second
            LatchCallback latch0 = new LatchCallback();
            l.asyncReadEntries(0, 0, latch0, null);
            latch0.expectTimeout(timeout);

            // wake up first bookie
            sleepLatch0.countDown();
            latch0.expectSuccess(timeout / 2);

            sleepLatch1.countDown();

            // check we can read next entry without issue
            LatchCallback latch1 = new LatchCallback();
            l.asyncReadEntries(1, 1, latch1, null);
            latch1.expectSuccess(timeout / 2);

        } finally {
            sleepLatch0.countDown();
            sleepLatch1.countDown();
            l.close();
            bkspec.close();
        }
    }

    /**
     * Unit test to check if the scheduled speculative task gets cancelled
     * on successful read.
     */
    @Test
    public void testSpeculativeReadScheduledTaskCancel() throws Exception {
        long id = getLedgerToRead(3, 2);
        int timeout = 1000;
        BookKeeper bkspec = createClient(timeout);
        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);
        PendingReadOp op = null;
        try {
            op = new PendingReadOp(l, bkspec.getClientCtx(), 0, 5, false);
            op.initiate();
            op.future().get();
        } finally {
            assertNull("Speculative Read tasks must be null", op.getSpeculativeTask());
        }
    }

    /**
     * Unit test for the speculative read scheduling method.
     */
    @Test
    public void testSpeculativeReadScheduling() throws Exception {
        long id = getLedgerToRead(3, 2);
        int timeout = 1000;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        List<BookieId> ensemble = l.getLedgerMetadata().getAllEnsembles().get(0L);
        BitSet allHosts = new BitSet(ensemble.size());
        for (int i = 0; i < ensemble.size(); i++) {
            allHosts.set(i, true);
        }
        BitSet noHost = new BitSet(ensemble.size());
        BitSet secondHostOnly = new BitSet(ensemble.size());
        secondHostOnly.set(1, true);
        PendingReadOp.LedgerEntryRequest req0 = null, req2 = null, req4 = null;
        try {
            PendingReadOp op = new PendingReadOp(l, bkspec.getClientCtx(), 0, 5, false);
            // if we've already heard from all hosts,
            // we only send the initial read
            req0 = op.new SequenceReadRequest(ensemble, l.getId(), 0);
            assertTrue("Should have sent to first",
                       req0.maybeSendSpeculativeRead(allHosts).equals(ensemble.get(0)));
            assertNull("Should not have sent another",
                       req0.maybeSendSpeculativeRead(allHosts));

            // if we have heard from some hosts, but not one we have sent to
            // send again
            req2 = op.new SequenceReadRequest(ensemble, l.getId(), 2);
            assertTrue("Should have sent to third",
                       req2.maybeSendSpeculativeRead(noHost).equals(ensemble.get(2)));
            assertTrue("Should have sent to first",
                       req2.maybeSendSpeculativeRead(secondHostOnly).equals(ensemble.get(0)));

            // if we have heard from some hosts, which includes one we sent to
            // do not read again
            req4 = op.new SequenceReadRequest(ensemble, l.getId(), 4);
            assertTrue("Should have sent to second",
                       req4.maybeSendSpeculativeRead(noHost).equals(ensemble.get(1)));
            assertNull("Should not have sent another",
                       req4.maybeSendSpeculativeRead(secondHostOnly));
        } finally {
            for (PendingReadOp.LedgerEntryRequest req
                     : new PendingReadOp.LedgerEntryRequest[] { req0, req2, req4 }) {
                if (req != null) {
                    int i = 0;
                    while (!req.isComplete()) {
                        if (i++ > 10) {
                            break; // wait for up to 10 seconds
                        }
                        Thread.sleep(1000);
                    }
                    assertTrue("Request should be done", req.isComplete());
                }
            }

            l.close();
            bkspec.close();
        }
    }

    @Test
    public void testSequenceReadLocalEnsemble() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setSpeculativeReadTimeout(1000)
                .setEnsemblePlacementPolicy(LocalBookieEnsemblePlacementPolicy.class)
                .setReorderReadSequenceEnabled(true)
                .setEnsemblePlacementPolicySlowBookies(true)
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try (BookKeeper bkc = new BookKeeperTestClient(conf, new TestStatsProvider())) {
            LedgerHandle l = bkc.createLedger(1, 1, digestType, passwd);
            List<BookieId> ensemble = l.getLedgerMetadata().getAllEnsembles().get(0L);
            PendingReadOp op = new PendingReadOp(l, bkc.getClientCtx(), 0, 5, false);
            PendingReadOp.LedgerEntryRequest req0 = op.new SequenceReadRequest(ensemble, l.getId(), 0);
            assertNotNull(req0.writeSet);
        }
    }
}
