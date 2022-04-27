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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PrimitiveIterator.OfLong;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.CompactableLedgerStorage;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.GarbageCollector;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.ScanAndCompareGarbageCollector;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test garbage collection ledgers in ledger manager.
 */
public class GcLedgersTest extends LedgerManagerTestCase {
    static final Logger LOG = LoggerFactory.getLogger(GcLedgersTest.class);

    public GcLedgersTest(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls);
    }

    private void createLedgers(int numLedgers, final Set<Long> createdLedgers) throws IOException {
        BookieId selfBookie = BookieImpl.getBookieId(baseConf);
        createLedgers(numLedgers, createdLedgers, selfBookie);
    }

    /**
     * Create ledgers.
     */
    private void createLedgers(int numLedgers, final Set<Long> createdLedgers, BookieId selfBookie)
            throws IOException {
        final AtomicInteger expected = new AtomicInteger(numLedgers);
        List<BookieId> ensemble = Lists.newArrayList(selfBookie);

        for (int i = 0; i < numLedgers; i++) {
            getLedgerIdGenerator().generateLedgerId(new GenericCallback<Long>() {
                @Override
                public void operationComplete(int rc, final Long ledgerId) {
                    if (BKException.Code.OK != rc) {
                        synchronized (expected) {
                            int num = expected.decrementAndGet();
                            if (num == 0) {
                                expected.notify();
                            }
                        }
                        return;
                    }

                    LedgerMetadata md = LedgerMetadataBuilder.create()
                        .withId(ledgerId)
                        .withDigestType(DigestType.CRC32C)
                        .withPassword(new byte[0])
                        .withEnsembleSize(1).withWriteQuorumSize(1).withAckQuorumSize(1)
                        .newEnsembleEntry(0L, ensemble).build();

                    getLedgerManager().createLedgerMetadata(ledgerId, md)
                        .whenComplete((result, exception) -> {
                                if (exception == null) {
                                    activeLedgers.put(ledgerId, true);
                                    createdLedgers.add(ledgerId);
                                }
                                synchronized (expected) {
                                    int num = expected.decrementAndGet();
                                    if (num == 0) {
                                        expected.notify();
                                    }
                                }
                            });
                }
            });
        }
        synchronized (expected) {
            try {
                while (expected.get() > 0) {
                    expected.wait(100);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void removeLedger(long ledgerId) throws Exception {
        getLedgerManager().removeLedgerMetadata(ledgerId, Version.ANY).get(10, TimeUnit.SECONDS);
    }

    @Test
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
        for (int i = 0; i < numRemovedLedgers; i++) {
            long ledgerId = tmpList.get(i);
            getLedgerManager().removeLedgerMetadata(ledgerId, Version.ANY).get();
            removedLedgers.add(ledgerId);
            createdLedgers.remove(ledgerId);
        }
        final CountDownLatch inGcProgress = new CountDownLatch(1);
        final CountDownLatch createLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(2);
        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        TestStatsProvider stats = new TestStatsProvider();
        final ScanAndCompareGarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(getLedgerManager(),
                mockLedgerStorage, baseConf, stats.getStatsLogger("gc"));
        Thread gcThread = new Thread() {
            @Override
            public void run() {
                garbageCollector.gc(new GarbageCollector.GarbageCleaner() {
                    boolean paused = false;

                    @Override
                    public void clean(long ledgerId) {
                        try {
                            mockLedgerStorage.deleteLedger(ledgerId);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return;
                        }

                        if (!paused) {
                            inGcProgress.countDown();
                            try {
                                createLatch.await();
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
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
        assertTrue(
                "Wrong ACTIVE_LEDGER_COUNT",
                garbageCollector.getNumActiveLedgers() == createdLedgers.size());
    }

    @Test
    public void testGcLedgersOutsideRange() throws Exception {
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final Queue<Long> cleaned = new LinkedList<Long>();
        int numLedgers = 100;

        createLedgers(numLedgers, createdLedgers);

        MockLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        TestStatsProvider stats = new TestStatsProvider();
        final ScanAndCompareGarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(getLedgerManager(),
                mockLedgerStorage, baseConf, stats.getStatsLogger("gc"));
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
                @Override
                public void clean(long ledgerId) {
                    LOG.info("Cleaned {}", ledgerId);
                    cleaned.add(ledgerId);
                    try {
                        mockLedgerStorage.deleteLedger(ledgerId);
                    } catch (IOException e) {
                        e.printStackTrace();
                        fail("Exception from deleteLedger");
                    }
                }
            };

        garbageCollector.gc(cleaner);
        assertNull("Should have cleaned nothing", cleaned.poll());
        assertTrue(
                "Wrong ACTIVE_LEDGER_COUNT",
                garbageCollector.getNumActiveLedgers() == numLedgers);

        long last = createdLedgers.last();
        removeLedger(last);
        garbageCollector.gc(cleaner);
        assertNotNull("Should have cleaned something", cleaned.peek());
        assertEquals("Should have cleaned last ledger" + last, (long) last, (long) cleaned.poll());

        long first = createdLedgers.first();
        removeLedger(first);
        garbageCollector.gc(cleaner);
        assertNotNull("Should have cleaned something", cleaned.peek());
        assertEquals("Should have cleaned first ledger" + first, (long) first, (long) cleaned.poll());

        garbageCollector.gc(cleaner);
        assertTrue(
                "Wrong ACTIVE_LEDGER_COUNT",
                garbageCollector.getNumActiveLedgers() == (numLedgers - 2));

    }

    @Test
    public void testGcLedgersNotLast() throws Exception {
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final List<Long> cleaned = new ArrayList<Long>();

        // Create enough ledgers to span over 4 ranges in the hierarchical ledger manager implementation
        final int numLedgers = 30001;

        createLedgers(numLedgers, createdLedgers);

        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(getLedgerManager(),
                new MockLedgerStorage(), baseConf, NullStatsLogger.INSTANCE);
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
                @Override
                public void clean(long ledgerId) {
                    LOG.info("Cleaned {}", ledgerId);
                    cleaned.add(ledgerId);
                }
            };

        SortedSet<Long> scannedLedgers = new TreeSet<Long>();
        LedgerRangeIterator iterator = getLedgerManager().getLedgerRanges(0);
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
        assertEquals("Should have cleaned first ledger" + first, (long) first, (long) cleaned.get(0));
    }

    /*
     * in this scenario no ledger is created, so ledgeriterator's hasNext call would return false and next would be
     * null. GarbageCollector.gc is expected to behave normally
     */
    @Test
    public void testGcLedgersWithNoLedgers() throws Exception {
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final List<Long> cleaned = new ArrayList<Long>();

        // no ledger created

        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(getLedgerManager(),
                new MockLedgerStorage(), baseConf, NullStatsLogger.INSTANCE);
        AtomicBoolean cleanerCalled = new AtomicBoolean(false);

        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                LOG.info("Cleaned {}", ledgerId);
                cleanerCalled.set(true);
            }
        };

        validateLedgerRangeIterator(createdLedgers);

        garbageCollector.gc(cleaner);
        assertFalse("Should have cleaned nothing, since no ledger is created", cleanerCalled.get());
    }

    // in this scenario all the created ledgers are in one single ledger range.
    @Test
    public void testGcLedgersWithLedgersInSameLedgerRange() throws Exception {
        baseConf.setVerifyMetadataOnGc(true);
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final SortedSet<Long> cleaned = Collections.synchronizedSortedSet(new TreeSet<Long>());

        // Create few ledgers which span over just one ledger range in the hierarchical ledger manager implementation
        final int numLedgers = 5;

        createLedgers(numLedgers, createdLedgers);

        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(getLedgerManager(),
                new MockLedgerStorage(), baseConf, NullStatsLogger.INSTANCE);
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                LOG.info("Cleaned {}", ledgerId);
                cleaned.add(ledgerId);
            }
        };

        validateLedgerRangeIterator(createdLedgers);

        garbageCollector.gc(cleaner);
        assertTrue("Should have cleaned nothing", cleaned.isEmpty());

        for (long ledgerId : createdLedgers) {
            removeLedger(ledgerId);
        }

        garbageCollector.gc(cleaner);
        assertEquals("Should have cleaned all the created ledgers", createdLedgers, cleaned);
    }

    /*
     * in this test scenario no created ledger is deleted, but ledgeriterator is screwed up and returns hasNext to be
     * false and next to be null. So even in this case it is expected not to clean any ledger's data.
     *
     * This testcase is needed for validating fix of bug - W-4292747.
     *
     * ScanAndCompareGarbageCollector/GC should clean data of ledger only if both the LedgerManager.getLedgerRanges says
     * that ledger is not existing and also ledgerManager.readLedgerMetadata fails with error
     * NoSuchLedgerExistsOnMetadataServerException.
     *
     */
    @Test
    public void testGcLedgersIfLedgerManagerIteratorFails() throws Exception {
        baseConf.setVerifyMetadataOnGc(true);
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final SortedSet<Long> cleaned = Collections.synchronizedSortedSet(new TreeSet<Long>());

        // Create few ledgers
        final int numLedgers = 5;

        createLedgers(numLedgers, createdLedgers);

        LedgerManager mockLedgerManager = new CleanupLedgerManager(getLedgerManager()) {
            @Override
            public LedgerRangeIterator getLedgerRanges(long zkOpTimeout) {
                return new LedgerRangeIterator() {
                    @Override
                    public LedgerRange next() throws IOException {
                        return null;
                    }

                    @Override
                    public boolean hasNext() throws IOException {
                        return false;
                    }
                };
            }
        };

        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(mockLedgerManager,
                new MockLedgerStorage(), baseConf, NullStatsLogger.INSTANCE);
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                LOG.info("Cleaned {}", ledgerId);
                cleaned.add(ledgerId);
            }
        };

        validateLedgerRangeIterator(createdLedgers);

        garbageCollector.gc(cleaner);
        assertTrue("Should have cleaned nothing", cleaned.isEmpty());
    }

    /*
     * In this test scenario no ledger is deleted, but LedgerManager.readLedgerMetadata says there is NoSuchLedger. So
     * even in that case, GarbageCollector.gc shouldn't delete ledgers data.
     *
     * Consider the possible scenario - when the LedgerIterator is created that ledger is not deleted, so as per
     * LedgerIterator that is live ledger. But right after the LedgerIterator creation that ledger is deleted, so
     * readLedgerMetadata call would return NoSuchLedger. In this testscenario we are validating that as per Iterator if
     * that ledger is alive though currently that ledger is deleted, we should not clean data of that ledger.
     *
     * ScanAndCompareGarbageCollector/GC should clean data of ledger only if both the LedgerManager.getLedgerRanges says
     * that ledger is not existing and also ledgerManager.readLedgerMetadata fails with error
     * NoSuchLedgerExistsOnMetadataServerException.
     *
     */
    @Test
    public void testGcLedgersIfReadLedgerMetadataSaysNoSuchLedger() throws Exception {
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final SortedSet<Long> cleaned = Collections.synchronizedSortedSet(new TreeSet<Long>());

        // Create few ledgers
        final int numLedgers = 5;

        createLedgers(numLedgers, createdLedgers);

        CompletableFuture<Versioned<LedgerMetadata>> errorFuture = new CompletableFuture<>();
        errorFuture.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
        LedgerManager mockLedgerManager = new CleanupLedgerManager(getLedgerManager()) {
            @Override
            public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
                return errorFuture;
            }
        };

        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(mockLedgerManager,
                new MockLedgerStorage(), baseConf, NullStatsLogger.INSTANCE);
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                LOG.info("Cleaned {}", ledgerId);
                cleaned.add(ledgerId);
            }
        };

        validateLedgerRangeIterator(createdLedgers);

        garbageCollector.gc(cleaner);
        assertTrue("Should have cleaned nothing", cleaned.isEmpty());
    }

    /*
     * In this test scenario all the created ledgers are deleted, but LedgerManager.readLedgerMetadata fails with
     * ZKException. So even in this case, GarbageCollector.gc shouldn't delete ledgers data.
     *
     * ScanAndCompareGarbageCollector/GC should clean data of ledger only if both the LedgerManager.getLedgerRanges says
     * that ledger is not existing and also ledgerManager.readLedgerMetadata fails with error
     * NoSuchLedgerExistsOnMetadataServerException, but is shouldn't delete if the readLedgerMetadata fails with any
     * other error.
     */
    @Test
    public void testGcLedgersIfReadLedgerMetadataFailsForDeletedLedgers() throws Exception {
        baseConf.setVerifyMetadataOnGc(true);
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final SortedSet<Long> cleaned = Collections.synchronizedSortedSet(new TreeSet<Long>());

        // Create few ledgers
        final int numLedgers = 5;

        createLedgers(numLedgers, createdLedgers);

        CompletableFuture<Versioned<LedgerMetadata>> errorFuture = new CompletableFuture<>();
        errorFuture.completeExceptionally(new BKException.ZKException());
        LedgerManager mockLedgerManager = new CleanupLedgerManager(getLedgerManager()) {
            @Override
            public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
                return errorFuture;
            }
        };

        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(mockLedgerManager,
                new MockLedgerStorage(), baseConf, NullStatsLogger.INSTANCE);
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                LOG.info("Cleaned {}", ledgerId);
                cleaned.add(ledgerId);
            }
        };

        validateLedgerRangeIterator(createdLedgers);

        for (long ledgerId : createdLedgers) {
            removeLedger(ledgerId);
        }

        garbageCollector.gc(cleaner);
        assertTrue("Should have cleaned nothing", cleaned.isEmpty());
    }

    public void validateLedgerRangeIterator(SortedSet<Long> createdLedgers) throws IOException {
        SortedSet<Long> scannedLedgers = new TreeSet<Long>();
        LedgerRangeIterator iterator = getLedgerManager().getLedgerRanges(0);
        while (iterator.hasNext()) {
            LedgerRange ledgerRange = iterator.next();
            scannedLedgers.addAll(ledgerRange.getLedgers());
        }

        assertEquals(createdLedgers, scannedLedgers);
    }

    class MockLedgerStorage implements CompactableLedgerStorage {

        @Override
        public void initialize(
            ServerConfiguration conf,
            LedgerManager ledgerManager,
            LedgerDirsManager ledgerDirsManager,
            LedgerDirsManager indexDirsManager,
            StatsLogger statsLogger,
            ByteBufAllocator allocator) throws IOException {
        }

        @Override
        public void setStateManager(StateManager stateManager) {}
        @Override
        public void setCheckpointSource(CheckpointSource checkpointSource) {}
        @Override
        public void setCheckpointer(Checkpointer checkpointer) {}

        @Override
        public void start() {
        }

        @Override
        public void shutdown() throws InterruptedException {
        }

        @Override
        public long getLastAddConfirmed(long ledgerId) throws IOException {
            return 0;
        }

        @Override
        public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        }

        @Override
        public ByteBuf getExplicitLac(long ledgerId) {
            return null;
        }

        @Override
        public boolean ledgerExists(long ledgerId) throws IOException {
            return false;
        }

        @Override
        public boolean entryExists(long ledgerId, long entryId) throws IOException {
            return false;
        }

        @Override
        public boolean setFenced(long ledgerId) throws IOException {
            return false;
        }

        @Override
        public boolean isFenced(long ledgerId) throws IOException {
            return false;
        }

        @Override
        public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        }

        @Override
        public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
            return null;
        }

        @Override
        public long addEntry(ByteBuf entry) throws IOException {
            return 0;
        }

        @Override
        public ByteBuf getEntry(long ledgerId, long entryId) throws IOException {
            return null;
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void checkpoint(Checkpoint checkpoint) throws IOException {
        }

        @Override
        public void deleteLedger(long ledgerId) throws IOException {
            activeLedgers.remove(ledgerId);
        }

        @Override
        public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) {
            NavigableMap<Long, Boolean> bkActiveLedgersSnapshot = activeLedgers.snapshot();
            Map<Long, Boolean> subBkActiveLedgers = bkActiveLedgersSnapshot
                    .subMap(firstLedgerId, true, lastLedgerId, false);

            return subBkActiveLedgers.keySet();
        }

        @Override
        public EntryLogger getEntryLogger() {
            return null;
        }

        @Override
        public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        }

        @Override
        public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        }

        @Override
        public void flushEntriesLocationsIndex() throws IOException {
        }

        @Override
        public boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                                     long previousLAC,
                                                     Watcher<LastAddConfirmedUpdateNotification> watcher)
                throws IOException {
            return false;
        }

        @Override
        public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                        Watcher<LastAddConfirmedUpdateNotification> watcher)
                throws IOException {
        }

        @Override
        public OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
            return null;
        }

        @Override
        public void setLimboState(long ledgerId) throws IOException {
            throw new UnsupportedOperationException(
                    "Limbo state only supported for DbLedgerStorage");
        }

        @Override
        public boolean hasLimboState(long ledgerId) throws IOException {
            throw new UnsupportedOperationException(
                    "Limbo state only supported for DbLedgerStorage");
        }

        @Override
        public void clearLimboState(long ledgerId) throws IOException {
            throw new UnsupportedOperationException(
                    "Limbo state only supported for DbLedgerStorage");
        }

        @Override
        public EnumSet<StorageState> getStorageStateFlags() throws IOException {
            return EnumSet.noneOf(StorageState.class);
        }

        @Override
        public void setStorageStateFlag(StorageState flag) throws IOException {
        }

        @Override
        public void clearStorageStateFlag(StorageState flag) throws IOException {
        }
    }

    /**
     * Verifies that gc should cleaned up overreplicatd ledgers which is not
     * owned by the bookie anymore.
     *
     * @throws Exception
     */
    @Test
    public void testGcLedgersForOverreplicated() throws Exception {
        baseConf.setVerifyMetadataOnGc(true);
        final SortedSet<Long> createdLedgers = Collections.synchronizedSortedSet(new TreeSet<Long>());
        final SortedSet<Long> cleaned = Collections.synchronizedSortedSet(new TreeSet<Long>());

        // Create few ledgers
        final int numLedgers = 5;

        BookieId bookieAddress = new BookieSocketAddress("192.0.0.1", 1234).toBookieId();
        createLedgers(numLedgers, createdLedgers, bookieAddress);

        LedgerManager mockLedgerManager = new CleanupLedgerManager(getLedgerManager()) {
            @Override
            public LedgerRangeIterator getLedgerRanges(long zkOpTimeout) {
                return new LedgerRangeIterator() {
                    @Override
                    public LedgerRange next() throws IOException {
                        return null;
                    }

                    @Override
                    public boolean hasNext() throws IOException {
                        return false;
                    }
                };
            }
        };

        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(mockLedgerManager,
                new MockLedgerStorage(), baseConf, NullStatsLogger.INSTANCE);
        GarbageCollector.GarbageCleaner cleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                LOG.info("Cleaned {}", ledgerId);
                cleaned.add(ledgerId);
            }
        };

        validateLedgerRangeIterator(createdLedgers);

        garbageCollector.gc(cleaner);
        assertEquals("Should have cleaned all ledgers", cleaned.size(), numLedgers);
    }
}
