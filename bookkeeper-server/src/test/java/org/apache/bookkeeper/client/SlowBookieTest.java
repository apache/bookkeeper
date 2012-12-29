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

import java.util.Set;
import java.util.List;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.InetSocketAddress;
import org.junit.Test;
import static org.junit.Assert.*;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.conf.ClientConfiguration;

public class SlowBookieTest extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(SlowBookieTest.class);

    public SlowBookieTest() {
        super(4);
    }

    @Test(timeout=60000)
    public void testSlowBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setReadTimeout(360);

        BookKeeper bkc = new BookKeeper(conf);

        LedgerHandle lh = bkc.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, new byte[] {});

        byte[] entry = "Test Entry".getBytes();
        for (int i = 0; i < 10; i++) {
            lh.addEntry(entry);
        }
        final CountDownLatch b0latch = new CountDownLatch(1);
        final CountDownLatch b1latch = new CountDownLatch(1);
        List<InetSocketAddress> curEns = lh.getLedgerMetadata().currentEnsemble;
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
                    }
                };
            lh.asyncAddEntry(entry, cb, null);

            Thread.sleep(1000); // sleep a second to allow time to complete
            assertEquals(i.get(), 0xdeadbeef);
            b0latch.countDown();
            b1latch.countDown();
            Thread.sleep(2000);
            assertEquals(i.get(), BKException.Code.OK);
        } finally {
            b0latch.countDown();
            b1latch.countDown();
        }
    }

    @Test(timeout=60000)
    public void testBookieFailureWithSlowBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setReadTimeout(5);

        BookKeeper bkc = new BookKeeper(conf);

        byte[] pwd = new byte[] {};
        final LedgerHandle lh = bkc.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, pwd);
        long lid = lh.getId();
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicBoolean failTest = new AtomicBoolean(false);
        final byte[] entry = "Test Entry".getBytes();
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

    @Test(timeout=60000)
    public void testManyBookieFailureWithSlowBookies() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setReadTimeout(5);

        BookKeeper bkc = new BookKeeper(conf);

        byte[] pwd = new byte[] {};
        final LedgerHandle lh = bkc.createLedger(4, 3, 1, BookKeeper.DigestType.CRC32, pwd);
        long lid = lh.getId();
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicBoolean failTest = new AtomicBoolean(false);
        final byte[] entry = "Test Entry".getBytes();
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
}
