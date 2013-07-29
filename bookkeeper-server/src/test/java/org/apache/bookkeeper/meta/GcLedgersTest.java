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

package org.apache.bookkeeper.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.GarbageCollector;
import org.apache.bookkeeper.bookie.ScanAndCompareGarbageCollector;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.versioning.Version;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test garbage collection ledgers in ledger manager
 */
public class GcLedgersTest extends LedgerManagerTestCase {
    static final Logger LOG = LoggerFactory.getLogger(GcLedgersTest.class);

    public GcLedgersTest(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls);
    }

    /**
     * Create ledgers
     */
    private void createLedgers(int numLedgers, final Set<Long> createdLedgers) {
        final AtomicInteger expected = new AtomicInteger(numLedgers);
        for (int i=0; i<numLedgers; i++) {
            getLedgerManager().createLedger(new LedgerMetadata(1, 1, 1, DigestType.MAC, "".getBytes()),
                new GenericCallback<Long>() {
                @Override
                public void operationComplete(int rc, Long ledgerId) {
                    if (rc == BKException.Code.OK) {
                        activeLedgers.put(ledgerId, true);
                        createdLedgers.add(ledgerId);
                    }
                    synchronized (expected) {
                        int num = expected.decrementAndGet();
                        if (num == 0) {
                            expected.notify();
                        }
                    }
                }
            });
        }
        synchronized (expected) {
            try {
                while (expected.get() > 0) {
                    expected.wait(100);
                }
            } catch (InterruptedException ie) {
            }
        }
    }

    private void removeLedger(long ledgerId) throws Exception {
        final AtomicInteger rc = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        getLedgerManager().removeLedgerMetadata(ledgerId, Version.ANY,
                new GenericCallback<Void>() {
                    @Override
                    public void operationComplete(int rc2, Void result) {
                        rc.set(rc2);
                        latch.countDown();
                    }
                   });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals("Remove should have succeeded", 0, rc.get());
    }

    @Test(timeout=60000)
    public void testGarbageCollectLedgers() throws Exception {
        int numLedgers = 100;
        int numRemovedLedgers = 10;

        final Set<Long> createdLedgers = new HashSet<Long>();
        final Set<Long> removedLedgers = new HashSet<Long>();

        // create 100 ledgers
        createLedgers(numLedgers, createdLedgers);

        Random r = new Random(System.currentTimeMillis());
        final List<Long> tmpList = new ArrayList<Long>();
        tmpList.addAll(createdLedgers);
        Collections.shuffle(tmpList, r);
        // random remove several ledgers
        for (int i=0; i<numRemovedLedgers; i++) {
            long ledgerId = tmpList.get(i);
            synchronized (removedLedgers) {
                getLedgerManager().removeLedgerMetadata(ledgerId, Version.ANY,
                    new GenericCallback<Void>() {
                        @Override
                        public void operationComplete(int rc, Void result) {
                            synchronized (removedLedgers) {
                                removedLedgers.notify();
                            }
                        }
                   });
                removedLedgers.wait();
            }
            removedLedgers.add(ledgerId);
            createdLedgers.remove(ledgerId);
        }
        final CountDownLatch inGcProgress = new CountDownLatch(1);
        final CountDownLatch createLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(2);
        final GarbageCollector garbageCollector =
                new ScanAndCompareGarbageCollector(getLedgerManager(), activeLedgers);
        Thread gcThread = new Thread() {
            @Override
            public void run() {
                garbageCollector.gc(new GarbageCollector.GarbageCleaner() {
                    boolean paused = false;
                    @Override
                    public void clean(long ledgerId) {
                        if (!paused) {
                            inGcProgress.countDown();
                            try {
                                createLatch.await();
                            } catch (InterruptedException ie) {
                            }
                            paused = true;
                        }
                        LOG.info("Garbage Collected ledger {}", ledgerId);
                    }
                });
                LOG.info("Gc Thread quits.");
                endLatch.countDown();
            }
        };

        Thread createThread = new Thread() {
            @Override
            public void run() {
                try {
                    inGcProgress.await();
                    // create 10 more ledgers
                    createLedgers(10, createdLedgers);
                    LOG.info("Finished creating 10 more ledgers.");
                    createLatch.countDown();
                } catch (Exception e) {
                }
                LOG.info("Create Thread quits.");
                endLatch.countDown();
            }
        };

        createThread.start();
        gcThread.start();

        endLatch.await();

        // test ledgers
        for (Long ledger : removedLedgers) {
            assertFalse(activeLedgers.containsKey(ledger));
        }
        for (Long ledger : createdLedgers) {
            assertTrue(activeLedgers.containsKey(ledger));
        }
    }

    @Test(timeout=60000)
    public void testGcLedgersOutsideRange() throws Exception {
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final Queue<Long> cleaned = new LinkedList<Long>();
        int numLedgers = 100;

        createLedgers(numLedgers, createdLedgers);

        final GarbageCollector garbageCollector =
                new ScanAndCompareGarbageCollector(getLedgerManager(), activeLedgers);
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
                @Override
                public void clean(long ledgerId) {
                    LOG.info("Cleaned {}", ledgerId);
                    cleaned.add(ledgerId);
                }
            };

        garbageCollector.gc(cleaner);
        assertNull("Should have cleaned nothing", cleaned.poll());

        long last = createdLedgers.last();
        removeLedger(last);
        garbageCollector.gc(cleaner);
        assertNotNull("Should have cleaned something", cleaned.peek());
        assertEquals("Should have cleaned last ledger" + last, (long)last, (long)cleaned.poll());

        long first = createdLedgers.first();
        removeLedger(first);
        garbageCollector.gc(cleaner);
        assertNotNull("Should have cleaned something", cleaned.peek());
        assertEquals("Should have cleaned first ledger" + first, (long)first, (long)cleaned.poll());
    }

    @Test(timeout=60000)
    public void testGcLedgersNotLast() throws Exception {
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final List<Long> cleaned = new ArrayList<Long>();

        // Create enough ledgers to span over 4 ranges in the hierarchical ledger manager implementation
        final int numLedgers = 30001;

        createLedgers(numLedgers, createdLedgers);

        final GarbageCollector garbageCollector =
                new ScanAndCompareGarbageCollector(getLedgerManager(), activeLedgers);
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
                @Override
                public void clean(long ledgerId) {
                    LOG.info("Cleaned {}", ledgerId);
                    cleaned.add(ledgerId);
                }
            };

        SortedSet<Long> scannedLedgers = new TreeSet<Long>();
        LedgerRangeIterator iterator = getLedgerManager().getLedgerRanges();
        while (iterator.hasNext()) {
            LedgerRange ledgerRange = iterator.next();
            scannedLedgers.addAll(ledgerRange.getLedgers());
        }

        assertEquals(createdLedgers, scannedLedgers);

        garbageCollector.gc(cleaner);
        assertTrue("Should have cleaned nothing", cleaned.isEmpty());

        long first = createdLedgers.first();
        removeLedger(first);
        garbageCollector.gc(cleaner);
        assertEquals("Should have cleaned something", 1, cleaned.size());
        assertEquals("Should have cleaned first ledger" + first, (long)first, (long)cleaned.get(0));
    }
}
