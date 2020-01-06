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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Test read last confirmed long by polling.
 */
@RunWith(Parameterized.class)
public class TestReadLastConfirmedLongPoll extends BookKeeperClusterTestCase {
    private static final Logger log = LoggerFactory.getLogger(TestReadLastConfirmedLongPoll.class);
    final DigestType digestType;

    public TestReadLastConfirmedLongPoll(Class<? extends LedgerStorage> storageClass) {
        super(6);
        this.digestType = DigestType.CRC32;
        baseConf.setLedgerStorageClass(storageClass.getName());
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { InterleavedLedgerStorage.class },
            { SortedLedgerStorage.class },
            { DbLedgerStorage.class },
        });
    }

    @Test
    public void testReadLACLongPollWhenAllBookiesUp() throws Exception {
        final int numEntries = 3;

        final LedgerHandle lh = bkc.createLedger(3, 3, 1, digestType, "".getBytes());
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
        // add entries
        for (int i = 0; i < (numEntries - 1); i++) {
            lh.addEntry(("data" + i).getBytes());
        }
        final AtomicBoolean success = new AtomicBoolean(false);
        final AtomicInteger numCallbacks = new AtomicInteger(0);
        final CountDownLatch firstReadComplete = new CountDownLatch(1);
        readLh.asyncTryReadLastConfirmed(new AsyncCallback.ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                numCallbacks.incrementAndGet();
                if (BKException.Code.OK == rc) {
                    success.set(true);
                } else {
                    success.set(false);
                }
                firstReadComplete.countDown();
            }
        }, null);
        firstReadComplete.await();
        assertTrue(success.get());
        assertTrue(numCallbacks.get() == 1);
        assertEquals(numEntries - 3, readLh.getLastAddConfirmed());
        // try read last confirmed again
        success.set(false);
        numCallbacks.set(0);
        long entryId = readLh.getLastAddConfirmed() + 1;
        final CountDownLatch secondReadComplete = new CountDownLatch(1);
        readLh.asyncReadLastConfirmedAndEntry(entryId++, 1000, true,
                new AsyncCallback.ReadLastConfirmedAndEntryCallback() {
            @Override
            public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
                numCallbacks.incrementAndGet();
                if (BKException.Code.OK == rc && lastConfirmed == (numEntries - 2)) {
                    success.set(true);
                } else {
                    success.set(false);
                }
                secondReadComplete.countDown();
            }
        }, null);
        lh.addEntry(("data" + (numEntries - 1)).getBytes());
        secondReadComplete.await();
        assertTrue(success.get());
        assertTrue(numCallbacks.get() == 1);
        assertEquals(numEntries - 2, readLh.getLastAddConfirmed());

        success.set(false);
        numCallbacks.set(0);
        final CountDownLatch thirdReadComplete = new CountDownLatch(1);
        readLh.asyncReadLastConfirmedAndEntry(entryId++, 1000, false,
                new AsyncCallback.ReadLastConfirmedAndEntryCallback() {
            @Override
            public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
                numCallbacks.incrementAndGet();
                if (BKException.Code.OK == rc && lastConfirmed == (numEntries - 1)) {
                    success.set(true);
                } else {
                    success.set(false);
                }
                thirdReadComplete.countDown();
            }
        }, null);
        lh.addEntry(("data" + numEntries).getBytes());
        thirdReadComplete.await();
        assertTrue(success.get());
        assertTrue(numCallbacks.get() == 1);
        assertEquals(numEntries - 1, readLh.getLastAddConfirmed());
        lh.close();
        readLh.close();
    }

    @Test
    public void testReadLACLongPollWhenSomeBookiesDown() throws Exception {
        final int numEntries = 3;
        final LedgerHandle lh = bkc.createLedger(3, 1, 1, digestType, "".getBytes());
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
        // add entries
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
        }
        for (int i = 0; i < numEntries; i++) {
            ServerConfiguration[] confs = new ServerConfiguration[numEntries - 1];
            for (int j = 0; j < numEntries - 1; j++) {
                int idx = (i + 1 + j) % numEntries;
                confs[j] = killBookie(LedgerMetadataUtils.getLastEnsembleValue(lh.getLedgerMetadata()).get(idx));
            }

            final AtomicBoolean entryAsExpected = new AtomicBoolean(false);
            final AtomicBoolean success = new AtomicBoolean(false);
            final AtomicInteger numCallbacks = new AtomicInteger(0);
            final CountDownLatch readComplete = new CountDownLatch(1);
            final int entryId = i;
            readLh.asyncTryReadLastConfirmed(new AsyncCallback.ReadLastConfirmedCallback() {
                @Override
                public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                    numCallbacks.incrementAndGet();
                    if (BKException.Code.OK == rc) {
                        success.set(true);
                        entryAsExpected.set(lastConfirmed == (entryId - 1));
                    } else {
                        System.out.println("Return value" + rc);
                        success.set(false);
                        entryAsExpected.set(false);
                    }
                    readComplete.countDown();
                }
            }, null);
            readComplete.await();
            assertTrue(success.get());
            assertTrue(entryAsExpected.get());
            assertTrue(numCallbacks.get() == 1);

            lh.close();
            readLh.close();

            // start the bookies
            for (ServerConfiguration conf : confs) {
                startAndAddBookie(conf);
            }
        }
    }
}
