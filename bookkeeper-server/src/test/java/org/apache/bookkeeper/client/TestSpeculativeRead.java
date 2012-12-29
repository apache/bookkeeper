package org.apache.bookkeeper.client;

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

import org.junit.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BaseTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger fencing;
 *
 */
public class TestSpeculativeRead extends BaseTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestSpeculativeRead.class);

    DigestType digestType;
    byte[] passwd = "specPW".getBytes();

    public TestSpeculativeRead(DigestType digestType) {
        super(10);
        this.digestType = digestType;
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

    BookKeeper createClient(int specTimeout) throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setSpeculativeReadTimeout(specTimeout)
            .setReadTimeout(30000);
        conf.setZkServers(zkUtil.getZooKeeperConnectString());
        return new BookKeeper(conf);
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
            LOG.debug("Got response {} {}", rc, getDuration());
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
    @Test(timeout=60000)
    public void testSpeculativeRead() throws Exception {
        long id = getLedgerToRead(3,2);
        BookKeeper bknospec = createClient(0); // disabled
        BookKeeper bkspec = createClient(2000);

        LedgerHandle lnospec = bknospec.openLedger(id, digestType, passwd);
        LedgerHandle lspec = bkspec.openLedger(id, digestType, passwd);

        // sleep second bookie
        CountDownLatch sleepLatch = new CountDownLatch(1);
        InetSocketAddress second = lnospec.getLedgerMetadata().getEnsembles().get(0L).get(1);
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
    @Test(timeout=60000)
    public void testSpeculativeReadMultipleReplicasDown() throws Exception {
        long id = getLedgerToRead(5,5);
        int timeout = 5000;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        // sleep bookie 1, 2 & 4
        CountDownLatch sleepLatch = new CountDownLatch(1);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(1), sleepLatch);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(2), sleepLatch);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(4), sleepLatch);

        try {
            // read first entry, should complete faster than timeout
            // as bookie 0 has the entry
            LatchCallback latch0 = new LatchCallback();
            l.asyncReadEntries(0, 0, latch0, null);
            latch0.expectSuccess(timeout/2);

            // second should have to hit two timeouts (bookie 1 & 2)
            // bookie 3 has the entry
            LatchCallback latch1 = new LatchCallback();
            l.asyncReadEntries(1, 1, latch1, null);
            latch1.expectTimeout(timeout);
            latch1.expectSuccess(timeout*2);
            LOG.info("Timeout {} latch1 duration {}", timeout, latch1.getDuration());
            assertTrue("should have taken longer than two timeouts, but less than 3",
                       latch1.getDuration() >= timeout*2
                       && latch1.getDuration() < timeout*3);

            // third should have to hit one timeouts (bookie 2)
            // bookie 3 has the entry
            LatchCallback latch2 = new LatchCallback();
            l.asyncReadEntries(2, 2, latch2, null);
            latch2.expectTimeout(timeout/2);
            latch2.expectSuccess(timeout);
            LOG.info("Timeout {} latch2 duration {}", timeout, latch2.getDuration());
            assertTrue("should have taken longer than one timeout, but less than 2",
                       latch2.getDuration() >= timeout
                       && latch2.getDuration() < timeout*2);

            // fourth should have no timeout
            // bookie 3 has the entry
            LatchCallback latch3 = new LatchCallback();
            l.asyncReadEntries(3, 3, latch3, null);
            latch3.expectSuccess(timeout/2);

            // fifth should hit one timeout, (bookie 4)
            // bookie 0 has the entry
            LatchCallback latch4 = new LatchCallback();
            l.asyncReadEntries(4, 4, latch4, null);
            latch4.expectTimeout(timeout/2);
            latch4.expectSuccess(timeout);
            LOG.info("Timeout {} latch4 duration {}", timeout, latch4.getDuration());
            assertTrue("should have taken longer than one timeout, but less than 2",
                       latch4.getDuration() >= timeout
                       && latch4.getDuration() < timeout*2);

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
    @Test(timeout=60000)
    public void testSpeculativeReadFirstReadCompleteIsOk() throws Exception {
        long id = getLedgerToRead(2,2);
        int timeout = 1000;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        // sleep bookies
        CountDownLatch sleepLatch0 = new CountDownLatch(1);
        CountDownLatch sleepLatch1 = new CountDownLatch(1);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(0), sleepLatch0);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(1), sleepLatch1);

        try {
            // read goes to first bookie, spec read timeout occurs,
            // goes to second
            LatchCallback latch0 = new LatchCallback();
            l.asyncReadEntries(0, 0, latch0, null);
            latch0.expectTimeout(timeout);

            // wake up first bookie
            sleepLatch0.countDown();
            latch0.expectSuccess(timeout/2);

            sleepLatch1.countDown();

            // check we can read next entry without issue
            LatchCallback latch1 = new LatchCallback();
            l.asyncReadEntries(1, 1, latch1, null);
            latch1.expectSuccess(timeout/2);

        } finally {
            sleepLatch0.countDown();
            sleepLatch1.countDown();
            l.close();
            bkspec.close();
        }
    }

    /**
     * Unit test for the speculative read scheduling method
     */
    @Test(timeout=60000)
    public void testSpeculativeReadScheduling() throws Exception {
        long id = getLedgerToRead(3,2);
        int timeout = 1000;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        ArrayList<InetSocketAddress> ensemble = l.getLedgerMetadata().getEnsembles().get(0L);
        Set<InetSocketAddress> allHosts = new HashSet(ensemble);
        Set<InetSocketAddress> noHost = new HashSet();
        Set<InetSocketAddress> secondHostOnly = new HashSet();
        secondHostOnly.add(ensemble.get(1));
        PendingReadOp.LedgerEntryRequest req0 = null, req2 = null, req4 = null;
        try {
            LatchCallback latch0 = new LatchCallback();
            PendingReadOp op = new PendingReadOp(l, bkspec.scheduler,
                                                 0, 5, latch0, null);

            // if we've already heard from all hosts,
            // we only send the initial read
            req0 = op.new LedgerEntryRequest(ensemble, l.getId(), 0);
            assertTrue("Should have sent to first",
                       req0.maybeSendSpeculativeRead(allHosts).equals(ensemble.get(0)));
            assertNull("Should not have sent another",
                       req0.maybeSendSpeculativeRead(allHosts));

            // if we have heard from some hosts, but not one we have sent to
            // send again
            req2 = op.new LedgerEntryRequest(ensemble, l.getId(), 2);
            assertTrue("Should have sent to third",
                       req2.maybeSendSpeculativeRead(noHost).equals(ensemble.get(2)));
            assertTrue("Should have sent to first",
                       req2.maybeSendSpeculativeRead(secondHostOnly).equals(ensemble.get(0)));

            // if we have heard from some hosts, which includes one we sent to
            // do not read again
            req4 = op.new LedgerEntryRequest(ensemble, l.getId(), 4);
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
                    assertTrue("Request should be done", req0.isComplete());
                }
            }

            l.close();
            bkspec.close();
        }
    }
}