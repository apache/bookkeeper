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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a slow bookie.
 */
@SuppressWarnings("deprecation")
public class SlowBookieTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(SlowBookieTest.class);

    final byte[] entry = "Test Entry".getBytes();

    public SlowBookieTest() {
        super(4);
        baseConf.setNumAddWorkerThreads(0);
        baseConf.setNumReadWorkerThreads(0);
    }

    @Test
    public void testSlowBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(360)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        BookKeeper bkc = new BookKeeper(conf);

        LedgerHandle lh = bkc.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, new byte[] {});

        byte[] entry = "Test Entry".getBytes();
        for (int i = 0; i < 10; i++) {
            lh.addEntry(entry);
        }
        final CountDownLatch b0latch = new CountDownLatch(1);
        final CountDownLatch b1latch = new CountDownLatch(1);
        final CountDownLatch addEntrylatch = new CountDownLatch(1);
        List<BookieId> curEns = lh.getCurrentEnsemble();
        try {
            sleepBookie(curEns.get(0), b0latch);
            for (int i = 0; i < 10; i++) {
                lh.addEntry(entry);
            }
            sleepBookie(curEns.get(2), b1latch); // should cover all quorums

            final AtomicInteger i = new AtomicInteger(0xdeadbeef);
            AsyncCallback.AddCallback cb = new AsyncCallback.AddCallback() {
                    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                        i.set(rc);
                        addEntrylatch.countDown();
                    }
                };
            lh.asyncAddEntry(entry, cb, null);

            Thread.sleep(3000); // sleep 3 seconds to allow time to complete
            assertEquals("Successfully added entry!", 0xdeadbeef, i.get());
            b0latch.countDown();
            b1latch.countDown();
            addEntrylatch.await(4000, TimeUnit.MILLISECONDS);
            assertEquals("Failed to add entry!", BKException.Code.OK, i.get());
        } finally {
            b0latch.countDown();
            b1latch.countDown();
            addEntrylatch.countDown();
        }
    }

    @Test
    public void testBookieFailureWithSlowBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(5)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        BookKeeper bkc = new BookKeeper(conf);

        byte[] pwd = new byte[] {};
        final LedgerHandle lh = bkc.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, pwd);
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicBoolean failTest = new AtomicBoolean(false);
        Thread t = new Thread() {
                public void run() {
                    try {
                        while (!finished.get()) {
                            lh.addEntry(entry);
                        }
                    } catch (Exception e) {
                        LOG.error("Exception in add entry thread", e);
                        failTest.set(true);
                    }
                }
            };
        t.start();
        final CountDownLatch b0latch = new CountDownLatch(1);

        startNewBookie();
        sleepBookie(getBookie(0), b0latch);

        Thread.sleep(10000);
        b0latch.countDown();

        finished.set(true);
        t.join();

        assertFalse(failTest.get());

        lh.close();

        LedgerHandle lh2 = bkc.openLedger(lh.getId(), BookKeeper.DigestType.CRC32, pwd);
        LedgerChecker lc = new LedgerChecker(bkc);
        final CountDownLatch checklatch = new CountDownLatch(1);
        final AtomicInteger numFragments = new AtomicInteger(-1);
        lc.checkLedger(lh2, new GenericCallback<Set<LedgerFragment>>() {
                public void operationComplete(int rc, Set<LedgerFragment> badFragments) {
                    LOG.debug("Checked ledgers returned {} {}", rc, badFragments);
                    if (rc == BKException.Code.OK) {
                        numFragments.set(badFragments.size());
                    }
                    checklatch.countDown();
                }
            });
        checklatch.await();
        assertEquals("There should be no missing fragments", 0, numFragments.get());
    }

    @Test
    public void testSlowBookieAndBackpressureOn() throws Exception {
        final ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(5)
                .setAddEntryTimeout(1)
                .setAddEntryQuorumTimeout(1)
                .setNumChannelsPerBookie(1)
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setClientWriteBufferLowWaterMark(1)
                .setClientWriteBufferHighWaterMark(entry.length - 1)
                .setWaitTimeoutOnBackpressureMillis(5000);

        final boolean expectWriteError = false;
        final boolean expectFailedTest = false;

        try (LedgerHandle lh = doBackPressureTest(entry, conf, expectWriteError, expectFailedTest, 2000)) {
            assertTrue(lh.readLastConfirmed() < 5);
        }
    }

    @Test
    public void testSlowBookieAndFastFailOn() throws Exception {
        final ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(5)
                .setAddEntryTimeout(1)
                .setAddEntryQuorumTimeout(1)
                .setNumChannelsPerBookie(1)
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setClientWriteBufferLowWaterMark(1)
                .setClientWriteBufferHighWaterMark(2)
                .setWaitTimeoutOnBackpressureMillis(0);

        final boolean expectWriteError = true;
        final boolean expectFailedTest = false;

        try (LedgerHandle lh = doBackPressureTest(entry, conf, expectWriteError, expectFailedTest, 1000)) {
            assertTrue(lh.readLastConfirmed() < 5);
        }
    }

    @Test
    public void testSlowBookieAndNoBackpressure() throws Exception {
        final ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(5)
                .setAddEntryTimeout(1)
                .setAddEntryQuorumTimeout(1)
                .setNumChannelsPerBookie(1)
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setClientWriteBufferLowWaterMark(1)
                .setClientWriteBufferHighWaterMark(entry.length - 1)
                .setWaitTimeoutOnBackpressureMillis(-1);

        final boolean expectWriteError = false;
        final boolean expectFailedTest = false;

        try (LedgerHandle lh = doBackPressureTest(entry, conf, expectWriteError, expectFailedTest, 4000)) {
            assertTrue(lh.readLastConfirmed() > 90);
        }
    }

    private LedgerHandle doBackPressureTest(byte[] entry, ClientConfiguration conf,
                                            boolean expectWriteError, boolean expectFailedTest,
                                            long sleepInMillis) throws Exception {
        BookKeeper bkc = new BookKeeper(conf);

        byte[] pwd = new byte[] {};
        final LedgerHandle lh = bkc.createLedger(4, 3, 1, BookKeeper.DigestType.CRC32, pwd);
        lh.addEntry(entry);

        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicBoolean failTest = new AtomicBoolean(false);
        final AtomicBoolean writeError = new AtomicBoolean(false);
        Thread t = new Thread(() -> {
            try {
                int count = 0;
                while (!finished.get()) {
                    lh.asyncAddEntry(entry, (rc, lh1, entryId, ctx) -> {
                        if (rc != BKException.Code.OK) {
                            finished.set(true);
                            writeError.set(true);
                        }
                    }, null);
                    if (++count > 100) {
                        finished.set(true);
                    }
                }
            } catch (Exception e) {
                LOG.error("Exception in add entry thread", e);
                failTest.set(true);
            }
        });
        final CountDownLatch b0latch = new CountDownLatch(1);
        final CountDownLatch b0latch2 = new CountDownLatch(1);


        sleepBookie(getBookie(0), b0latch);
        sleepBookie(getBookie(1), b0latch2);

        setTargetChannelState(bkc, getBookie(0), 0, false);
        setTargetChannelState(bkc, getBookie(1), 0, false);

        t.start();

        Thread.sleep(sleepInMillis);

        finished.set(true);

        b0latch.countDown();
        b0latch2.countDown();
        setTargetChannelState(bkc, getBookie(0), 0, true);
        setTargetChannelState(bkc, getBookie(1), 0, true);

        t.join();

        assertEquals("write error", expectWriteError, writeError.get());
        assertEquals("test failure", expectFailedTest, failTest.get());

        lh.close();

        LedgerHandle lh2 = bkc.openLedger(lh.getId(), BookKeeper.DigestType.CRC32, pwd);
        LedgerChecker lc = new LedgerChecker(bkc);
        final CountDownLatch checkLatch = new CountDownLatch(1);
        final AtomicInteger numFragments = new AtomicInteger(-1);
        lc.checkLedger(lh2, (rc, fragments) -> {
            LOG.debug("Checked ledgers returned {} {}", rc, fragments);
            if (rc == BKException.Code.OK) {
                numFragments.set(fragments.size());
                LOG.error("Checked ledgers returned {} {}", rc, fragments);
            }
            checkLatch.countDown();
        });
        checkLatch.await();
        assertEquals("There should be no missing fragments", 0, numFragments.get());

        return lh2;
    }

    private void setTargetChannelState(BookKeeper bkc, BookieId address,
                                       long key, boolean writable) throws Exception {
        ((BookieClientImpl) bkc.getBookieClient()).lookupClient(address).obtain((rc, pcbc) -> {
            pcbc.setWritable(writable);
        }, key);
    }

    @Test
    public void testWriteSetWriteableCheck() throws Exception {
        final ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] pwd = new byte[]{};
        try (LedgerHandle lh = bkc.createLedger(4, 2, 2, BookKeeper.DigestType.CRC32, pwd)) {
            lh.addEntry(entry); // [b0, b1]
            long entryId = lh.addEntry(entry); // [b1, b2]

            long nextEntryId = entryId + 1;
            RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(2, 2, 4);
            DistributionSchedule.WriteSet writeSet = schedule.getWriteSet(nextEntryId);

            // b2 or b3 is no more writeable
            int slowBookieIndex = writeSet.get(ThreadLocalRandom.current().nextInt(writeSet.size()));
            List<BookieId> curEns = lh.getCurrentEnsemble();

            // Trigger connection to the bookie service first
            bkc.getBookieInfo().get(curEns.get(slowBookieIndex));
            // then mock channel is not writable
            setTargetChannelState(bkc, curEns.get(slowBookieIndex), 0, false);

            boolean isWriteable = lh.waitForWritable(writeSet, 0, 1000);
            assertFalse("We should check b2,b3 both are not writeable", isWriteable);
        }
    }

    @Test
    public void testManyBookieFailureWithSlowBookies() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(5)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        BookKeeper bkc = new BookKeeper(conf);

        byte[] pwd = new byte[] {};
        final LedgerHandle lh = bkc.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, pwd);
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicBoolean failTest = new AtomicBoolean(false);
        Thread t = new Thread() {
                public void run() {
                    try {
                        while (!finished.get()) {
                            lh.addEntry(entry);
                            Thread.sleep(1);
                        }
                    } catch (Exception e) {
                        LOG.error("Exception in add entry thread", e);
                        failTest.set(true);
                    }
                }
            };
        t.start();
        final CountDownLatch b0latch = new CountDownLatch(1);
        final CountDownLatch b1latch = new CountDownLatch(1);

        startNewBookie();
        startNewBookie();

        sleepBookie(getBookie(0), b0latch);
        sleepBookie(getBookie(1), b1latch);

        Thread.sleep(10000);
        b0latch.countDown();
        b1latch.countDown();
        finished.set(true);
        t.join();

        assertFalse(failTest.get());

        lh.close();

        LedgerHandle lh2 = bkc.openLedger(lh.getId(), BookKeeper.DigestType.CRC32, pwd);
        LedgerChecker lc = new LedgerChecker(bkc);
        final CountDownLatch checklatch = new CountDownLatch(1);
        final AtomicInteger numFragments = new AtomicInteger(-1);
        lc.checkLedger(lh2, new GenericCallback<Set<LedgerFragment>>() {
                public void operationComplete(int rc, Set<LedgerFragment> fragments) {
                    LOG.debug("Checked ledgers returned {} {}", rc, fragments);
                    if (rc == BKException.Code.OK) {
                        numFragments.set(fragments.size());
                    }
                    checklatch.countDown();
                }
            });
        checklatch.await();
        assertEquals("There should be no missing fragments", 0, numFragments.get());
    }

    @Test
    public void testWaitForWritable() throws Exception {
        final ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] pwd = new byte[]{};
        try (LedgerHandle lh = bkc.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, pwd)) {
            long entryId = lh.addEntry(this.entry);

            RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(1, 1, 1);
            DistributionSchedule.WriteSet writeSet = schedule.getWriteSet(entryId);

            int slowBookieIndex = writeSet.get(ThreadLocalRandom.current().nextInt(writeSet.size()));
            List<BookieId> curEns = lh.getCurrentEnsemble();

            // disable channel writable
            setTargetChannelState(bkc, curEns.get(slowBookieIndex), 0, false);

            AtomicBoolean isWriteable = new AtomicBoolean(false);
            final long timeout = 10000;

            // waitForWritable async
           new Thread(() -> {
                isWriteable.set(lh.waitForWritable(writeSet, 0, timeout));
            }).start();
            TimeUnit.MILLISECONDS.sleep(5000);
            assertFalse(isWriteable.get());

            // enable channel writable
            setTargetChannelState(bkc, curEns.get(slowBookieIndex), 0, true);
            Awaitility.await().untilAsserted(() -> assertTrue(isWriteable.get()));
        }
    }

}
