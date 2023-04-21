/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test try read last confirmed.
 */
public class TestTryReadLastConfirmed extends BookKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestTryReadLastConfirmed.class);

    final DigestType digestType;

    public TestTryReadLastConfirmed() {
        super(6);
        this.digestType = DigestType.CRC32;
    }

    @Test
    public void testTryReadLACWhenAllBookiesUp() throws Exception {
        final int numEntries = 3;

        final LedgerHandle lh = bkc.createLedger(3, 3, 1, digestType, "".getBytes());
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
        // add entries
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
        }
        final AtomicBoolean success = new AtomicBoolean(false);
        final AtomicInteger numCallbacks = new AtomicInteger(0);
        final CountDownLatch latch1 = new CountDownLatch(1);
        readLh.asyncTryReadLastConfirmed(new AsyncCallback.ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                numCallbacks.incrementAndGet();
                if (BKException.Code.OK == rc) {
                    success.set(true);
                } else {
                    success.set(false);
                }
                latch1.countDown();
            }
        }, null);
        latch1.await();
        TimeUnit.SECONDS.sleep(2);
        assertTrue(success.get());
        assertTrue(numCallbacks.get() == 1);
        assertEquals(numEntries - 2, readLh.getLastAddConfirmed());
        // try read last confirmed again
        success.set(false);
        numCallbacks.set(0);
        final CountDownLatch latch2 = new CountDownLatch(1);
        readLh.asyncTryReadLastConfirmed(new AsyncCallback.ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                numCallbacks.incrementAndGet();
                if (BKException.Code.OK == rc && lastConfirmed == (numEntries - 2)) {
                    success.set(true);
                } else {
                    success.set(false);
                }
                latch2.countDown();
            }
        }, null);
        latch2.await();
        TimeUnit.SECONDS.sleep(2);
        assertTrue(success.get());
        assertTrue(numCallbacks.get() == 1);
        assertEquals(numEntries - 2, readLh.getLastAddConfirmed());

        lh.close();
        readLh.close();
    }

    @Test
    public void testTryReadLaCWhenSomeBookiesDown() throws Exception {
        final int numEntries = 3;
        final int ensembleSize = 3;
        final LedgerHandle lh = bkc.createLedger(ensembleSize, 1, 1, digestType, "".getBytes());
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
        // add entries
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
        }
        for (int i = 0; i < numEntries; i++) {
            ServerConfiguration[] confs = new ServerConfiguration[ensembleSize - 1];
            for (int j = 0; j < ensembleSize - 1; j++) {
                int idx = (i + 1 + j) % ensembleSize;
                confs[j] = killBookie(lh.getCurrentEnsemble().get(idx));
            }

            final AtomicBoolean success = new AtomicBoolean(false);
            final AtomicInteger numCallbacks = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(1);
            final int entryId = i;
            readLh.asyncTryReadLastConfirmed(new AsyncCallback.ReadLastConfirmedCallback() {
                @Override
                public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                    numCallbacks.incrementAndGet();
                    if (BKException.Code.OK == rc) {
                        success.set(lastConfirmed == (entryId - 1));
                    } else {
                        success.set(false);
                    }
                    latch.countDown();
                }
            }, null);
            latch.await();
            assertTrue(success.get());
            assertTrue(numCallbacks.get() == 1);

            // start the bookies
            for (ServerConfiguration conf : confs) {
                startAndAddBookie(conf);
            }
        }
        lh.close();
        readLh.close();
    }

    @Test
    public void testTryReadLACWhenAllBookiesDown() throws Exception {
        final int numEntries = 2;
        final int ensembleSize = 3;
        final LedgerHandle lh = bkc.createLedger(ensembleSize, 1, 1, digestType, "".getBytes());
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
        // add entries
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
        }
        for (int i = 0; i < ensembleSize; i++) {
            killBookie(lh.getCurrentEnsemble().get(i));
        }
        final AtomicBoolean success = new AtomicBoolean(false);
        final AtomicInteger numCallbacks = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        readLh.asyncTryReadLastConfirmed(new AsyncCallback.ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                logger.info("ReadLastConfirmedComplete : rc = {}, lac = {}.", rc, lastConfirmed);
                numCallbacks.incrementAndGet();
                if (BKException.Code.OK == rc) {
                    success.set(lastConfirmed == LedgerHandle.INVALID_ENTRY_ID);
                } else {
                    success.set(false);
                }
                latch.countDown();
            }
        }, null);
        latch.await();
        TimeUnit.SECONDS.sleep(2);
        assertFalse(success.get());
        assertTrue(numCallbacks.get() == 1);

        lh.close();
        readLh.close();
    }
}
