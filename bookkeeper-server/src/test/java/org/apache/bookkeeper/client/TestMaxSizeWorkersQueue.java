/**
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

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Test the maximum size of a worker queue.
 */
public class TestMaxSizeWorkersQueue extends BookKeeperClusterTestCase {
    DigestType digestType = DigestType.CRC32;

    public TestMaxSizeWorkersQueue() {
        super(1);

        baseConf.setNumReadWorkerThreads(1);
        baseConf.setNumAddWorkerThreads(1);

        // Configure very small queue sizes
        baseConf.setMaxPendingReadRequestPerThread(1);
        baseConf.setMaxPendingAddRequestPerThread(1);
    }

    @Test(timeout = 60000)
    public void testReadRejected() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, new byte[0]);
        byte[] content = new byte[100];

        final int n = 1000;
        // Write few entries
        for (int i = 0; i < n; i++) {
            lh.addEntry(content);
        }

        // Read asynchronously:
        // - 1st read must always succeed
        // - Subsequent reads may fail, depending on timing
        // - At least few, we expect to fail with TooManyRequestException
        final CountDownLatch counter = new CountDownLatch(2);

        final AtomicInteger rcFirstReadOperation = new AtomicInteger();

        lh.asyncReadEntries(0, 0, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                rcFirstReadOperation.set(rc);
                counter.countDown();
            }
        }, lh);

        final AtomicInteger rcSecondReadOperation = new AtomicInteger();

        lh.asyncReadEntries(0, n - 1, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                rcSecondReadOperation.set(rc);
                counter.countDown();
            }
        }, lh);

        counter.await();

        assertEquals(BKException.Code.OK, rcFirstReadOperation.get());
        assertEquals(BKException.Code.TooManyRequestsException, rcSecondReadOperation.get());
    }

    @Test(timeout = 60000)
    public void testAddRejected() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, new byte[0]);
        byte[] content = new byte[100];

        final int n = 1000;

        // Write asynchronously, and expect at least few writes to have failed with NotEnoughBookies,
        // because when we get the TooManyRequestException, the client will try to form a new ensemble and that
        // operation will fail since we only have 1 bookie available
        final CountDownLatch counter = new CountDownLatch(n);
        final AtomicBoolean receivedTooManyRequestsException = new AtomicBoolean();

        // Write few entries
        for (int i = 0; i < n; i++) {
            lh.asyncAddEntry(content, new AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (rc == BKException.Code.NotEnoughBookiesException) {
                        receivedTooManyRequestsException.set(true);
                    }

                    counter.countDown();
                }
            }, null);
        }

        counter.await();

        assertTrue(receivedTooManyRequestsException.get());
    }
}
