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
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Test the maximum size of a worker queue.
 */
@Slf4j
public class TestMaxSizeWorkersQueue extends BookKeeperClusterTestCase {
    DigestType digestType = DigestType.CRC32;

    public TestMaxSizeWorkersQueue() {
        super(0);

        baseConf.setNumReadWorkerThreads(1);
        baseConf.setNumAddWorkerThreads(1);

        // Configure very small queue sizes
        baseConf.setMaxPendingReadRequestPerThread(1);
        baseConf.setMaxPendingAddRequestPerThread(1);

        baseClientConf.setReadEntryTimeout(Integer.MAX_VALUE);
    }

    @Test
    public void testReadRejected() throws Exception {
        final CountDownLatch readLatch = new CountDownLatch(1);
        final CountDownLatch readReceivedLatch = new CountDownLatch(1);

        ServerConfiguration conf = newServerConfiguration();
        bsConfs.add(conf);
        bs.add(startBookie(conf, () -> {
            try {
                return new Bookie(conf) {
                    @Override
                    public ByteBuf readEntry(long ledgerId, long entryId) throws IOException, NoLedgerException {
                        readReceivedLatch.countDown();
                        try {
                            readLatch.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.warn("Interrupted on reading entry {}-{}",
                                ledgerId, entryId, e);
                        }
                        return super.readEntry(ledgerId, entryId);
                    }
                };
            } catch (IOException | BookieException e) {
                log.warn("Fail to construct bookie", e);
                throw new UncheckedExecutionException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupt on constructing bookie", e);
                throw new UncheckedExecutionException(e);
            }
        }));

        @Cleanup LedgerHandle lh = bkc.createLedger(1, 1, digestType, new byte[0]);
        byte[] content = new byte[100];

        final int n = 3;
        // Write few entries
        for (int i = 0; i < n; i++) {
            lh.addEntry(content);
        }

        // Read asynchronously:
        // - 1st read is running (but blocking by testing logic) at the read thread
        // - 2nd read is pending at the worker queue
        // - 3rd read will be rejected with too many requests exception

        final AtomicInteger rcFirstReadOperation = new AtomicInteger();
        final CountDownLatch firstReadLatch = new CountDownLatch(1);

        lh.asyncReadEntries(0, 0, (rc, lh1, seq, ctx) -> {
            rcFirstReadOperation.set(rc);
            firstReadLatch.countDown();
        }, lh);
        readReceivedLatch.await();

        final AtomicInteger rcSecondReadOperation = new AtomicInteger();
        final CountDownLatch secondReadLatch = new CountDownLatch(1);

        lh.asyncReadEntries(1, 1, (rc, lh12, seq, ctx) -> {
            rcSecondReadOperation.set(rc);
            secondReadLatch.countDown();
        }, lh);

        final AtomicInteger rcThirdReadOperation = new AtomicInteger();
        final CountDownLatch thirdReadLatch = new CountDownLatch(1);

        lh.asyncReadEntries(2, 2, (rc, lh13, seq, ctx) -> {
            rcThirdReadOperation.set(rc);
            thirdReadLatch.countDown();
        }, lh);

        // third read should fail with too many requests exception
        thirdReadLatch.await();
        assertEquals(BKException.Code.TooManyRequestsException, rcThirdReadOperation.get());

        // let first read go through
        readLatch.countDown();
        firstReadLatch.await();
        assertEquals(BKException.Code.OK, rcFirstReadOperation.get());
        secondReadLatch.await();
        assertEquals(BKException.Code.OK, rcSecondReadOperation.get());
    }

    @Test
    public void testAddRejected() throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        bsConfs.add(conf);
        bs.add(startBookie(conf));

        @Cleanup LedgerHandle lh = bkc.createLedger(1, 1, digestType, new byte[0]);
        byte[] content = new byte[100];

        final int n = 1000;

        // Write asynchronously, and expect at least few writes to have failed with NotEnoughBookies,
        // because when we get the TooManyRequestException, the client will try to form a new ensemble and that
        // operation will fail since we only have 1 bookie available
        final CountDownLatch counter = new CountDownLatch(n);
        final AtomicBoolean receivedException = new AtomicBoolean(false);
        final AtomicBoolean receivedTooManyRequestsException = new AtomicBoolean();

        // Write few entries
        for (int i = 0; i < n; i++) {
            lh.asyncAddEntry(content, new AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (rc != Code.OK) {
                        receivedException.set(true);
                    }
                    if (rc == BKException.Code.NotEnoughBookiesException) {
                        receivedTooManyRequestsException.set(true);
                    }

                    counter.countDown();
                }
            }, null);
        }
        counter.await();

        assertTrue(receivedException.get());
        assertTrue(receivedTooManyRequestsException.get());
    }

    @Test
    public void testRecoveryNotRejected() throws Exception {
        ServerConfiguration conf = newServerConfiguration();
        bsConfs.add(conf);
        bs.add(startBookie(conf));

        @Cleanup LedgerHandle lh = bkc.createLedger(1, 1, digestType, new byte[0]);
        byte[] content = new byte[100];

        final int numEntriesToRead = 1000;
        // Write few entries
        for (int i = 0; i < numEntriesToRead; i++) {
            lh.addEntry(content);
        }

        final int numLedgersToRecover = 10;
        List<Long> ledgersToRecover = Lists.newArrayList();
        for (int i = 0; i < numLedgersToRecover; i++) {
            try (LedgerHandle lhr = bkc.createLedger(1, 1, digestType, new byte[0])) {
                lhr.addEntry(content);
                // Leave the ledger in open state
                ledgersToRecover.add(lhr.getId());
            }
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        final CyclicBarrier barrier = new CyclicBarrier(1 + numLedgersToRecover);

        List<Future<?>> futures = Lists.newArrayList();
        futures.add(executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                barrier.await();
                try {
                    lh.readEntries(0, numEntriesToRead - 1);
                    fail("Should have thrown exception");
                } catch (Exception e) {
                    // Expected
                }
                return null;
            }
        }));

        for (long ledgerId : ledgersToRecover) {
            futures.add(executor.submit((Callable<Void>) () -> {
                barrier.await();

                // Recovery should always succeed
                try (LedgerHandle ignored = bkc.openLedger(ledgerId, digestType, new byte[0])) {}
                return null;
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }
    }
}
