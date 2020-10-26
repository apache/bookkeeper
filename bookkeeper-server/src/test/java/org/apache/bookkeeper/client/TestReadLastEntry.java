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
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
  * Test read next entry and the latest last add confirmed.
  */
public class TestReadLastEntry extends BookKeeperClusterTestCase {

    final DigestType digestType;

    public TestReadLastEntry() {
        super(1);
        this.digestType = DigestType.CRC32;
    }

    @Test
    public void testTryReadLastEntryAsyncOnEmptyLedger() throws Exception {
        final LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "".getBytes());
        lh.close();

        LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());

        final CountDownLatch latch1 = new CountDownLatch(1);
        final AtomicInteger rcStore = new AtomicInteger();
        readLh.asyncReadLastEntry(new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                rcStore.set(rc);
                latch1.countDown();
            }
        }, null);

        latch1.await();

        assertEquals(BKException.Code.NoSuchEntryException, rcStore.get());

        lh.close();
        readLh.close();
    }

    @Test
    public void testTryReadLastEntryOnEmptyLedger() throws Exception {
        final LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "".getBytes());
        lh.close();

        LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());
        try {
            LedgerEntry lastEntry = readLh.readLastEntry();
            fail("should fail with NoSuchEntryException");
        } catch (BKException e) {
            assertEquals(e.getCode(), Code.NoSuchEntryException);
        }

        lh.close();
        readLh.close();
    }

    @Test
    public void testTryReadLastEntryAsync() throws Exception {
        final LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "".getBytes());
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'x');
        for (int j = 0; j < 100; j++) {
            data[1023] = Integer.valueOf(j).byteValue();
            lh.addEntry(data);
        }
        lh.close();

        LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());
        final CountDownLatch latch1 = new CountDownLatch(1);
        final AtomicInteger rcStore = new AtomicInteger();
        final AtomicInteger lastByteStore = new AtomicInteger();

        readLh.asyncReadLastEntry(new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                rcStore.set(rc);
                LedgerEntry entry = seq.nextElement();
                lastByteStore.set(Integer.valueOf(entry.getEntry()[1023]));
                latch1.countDown();
            }
        }, null);

        latch1.await();

        assertEquals(BKException.Code.OK, rcStore.get());
        assertEquals(lastByteStore.byteValue(), data[1023]);

        lh.close();
        readLh.close();
    }

    @Test
    public void testTryReadLastEntrySync() throws Exception {
        final LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "".getBytes());
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'x');
        for (int j = 0; j < 100; j++) {
            data[1023] = Integer.valueOf(j).byteValue();
            lh.addEntry(data);
        }
        lh.close();

        LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());
        LedgerEntry lastEntry = readLh.readLastEntry();

        assertEquals(lastEntry.getEntry()[1023], Integer.valueOf(99).byteValue());

        lh.close();
        readLh.close();
    }
}
