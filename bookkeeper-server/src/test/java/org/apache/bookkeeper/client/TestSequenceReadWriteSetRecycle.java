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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.Test;

/**
 * Regression test for issue #4680.
 *
 * <p>{@code SequenceReadRequest.complete()} delegates to
 * {@code SingleLedgerEntryRequest.complete()}, which recycles {@code writeSet},
 * and then the subclass accesses {@code writeSet.get(i)} to register slow
 * bookies. Because {@code WriteSetImpl} is pooled via Netty's {@code Recycler},
 * this is a use-after-recycle: the same instance can be handed to another
 * read and resized/repopulated, causing either {@code IndexOutOfBoundsException}
 * or, worse, silently registering the wrong bookie as slow.
 *
 * <p>We inject a tracking {@link DistributionSchedule.WriteSet} via a custom
 * placement policy and assert that no method on the {@code WriteSet} is
 * invoked after {@code recycle()} during a sequence read that retries.
 */
public class TestSequenceReadWriteSetRecycle extends BookKeeperClusterTestCase {

    private static final DigestType DIGEST = DigestType.CRC32;
    private static final int SPEC_TIMEOUT_MS = 500;

    public TestSequenceReadWriteSetRecycle() {
        super(4); // matches the issue scenario: ensemble = 4
    }

    @Test
    public void completeMustNotAccessWriteSetAfterRecycle() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setEnsemblePlacementPolicy(TrackingPlacementPolicy.class)
                .setReorderReadSequenceEnabled(true)
                .setSpeculativeReadTimeout(SPEC_TIMEOUT_MS)
                .setReadTimeout(60);

        try (BookKeeperTestClient bk =
                     new BookKeeperTestClient(conf, new TestStatsProvider())) {
            // ensemble = 4, writeQuorum = 2, ackQuorum = 2: the scenario from #4680.
            LedgerHandle writer = bk.createLedger(4, 2, 2, DIGEST, passwd());
            for (int i = 0; i < 5; i++) {
                writer.addEntry(("entry-" + i).getBytes());
            }
            long ledgerId = writer.getId();
            writer.close();

            try (LedgerHandle reader = bk.openLedger(ledgerId, DIGEST, passwd())) {
                List<BookieId> ensemble = reader.getLedgerMetadata().getAllEnsembles().get(0L);

                // For entry 1 the round-robin write set is [1, 2]. Sleeping ensemble[1]
                // forces SequenceReadRequest to speculatively retry to ensemble[2],
                // which exercises the slow-bookie registration loop in complete().
                BookieId firstReplica = ensemble.get(1);
                CountDownLatch sleepLatch = new CountDownLatch(1);
                sleepBookie(firstReplica, sleepLatch);
                try {
                    assertTrue("read must succeed via speculative retry",
                            reader.readEntries(1, 1).hasMoreElements());
                    sleepLatch.countDown();
                    Thread.sleep(1000);
                } finally {
                    sleepLatch.countDown();
                }

                assertNoViolation(bk);
            }
        }
    }

    /**
     * After {@code entry.complete()} succeeds (via a speculative retry) and
     * recycles {@code writeSet}, a late error response from the originally
     * slow bookie still flows through
     * {@code SequenceReadRequest.logErrorAndReattemptRead}, which calls
     * {@code writeSet.indexOf(bookieIndex)}, another use-after-recycle.
     *
     * <p>Reproduce by giving the slow bookie a read timeout shorter than the
     * sleep window so the in-flight request fails after the speculative read
     * has already completed the entry.
     */
    @Test
    public void logErrorAndReattemptReadMustNotAccessWriteSetAfterRecycle() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setEnsemblePlacementPolicy(TrackingPlacementPolicy.class)
                .setReorderReadSequenceEnabled(true)
                .setSpeculativeReadTimeout(SPEC_TIMEOUT_MS)
                .setReadEntryTimeout(1); // 1s, so the slow bookie's request times out as an error

        try (BookKeeperTestClient bk =
                     new BookKeeperTestClient(conf, new TestStatsProvider())) {
            LedgerHandle writer = bk.createLedger(4, 2, 2, DIGEST, passwd());
            for (int i = 0; i < 5; i++) {
                writer.addEntry(("entry-" + i).getBytes());
            }
            long ledgerId = writer.getId();
            writer.close();

            try (LedgerHandle reader = bk.openLedger(ledgerId, DIGEST, passwd())) {
                List<BookieId> ensemble = reader.getLedgerMetadata().getAllEnsembles().get(0L);

                // Sleep ensemble[1] for longer than readEntryTimeout so its request
                // returns a timeout error after the speculative retry has completed.
                BookieId slowBookie = ensemble.get(1);
                CountDownLatch sleepLatch = new CountDownLatch(1);
                sleepBookie(slowBookie, sleepLatch);
                try {
                    // entry 1: writeSet=[1,2]. Bookie 1 sleeps; spec retries to bookie 2;
                    // bookie 2 responds OK, then complete() recycles writeSet.
                    assertTrue("read must succeed via speculative retry",
                            reader.readEntries(1, 1).hasMoreElements());

                    // Wait long enough for the slow bookie's read to time out and the
                    // late error response to flow through logErrorAndReattemptRead.
                    Thread.sleep(2500);

                    assertNoViolation(bk);
                } finally {
                    sleepLatch.countDown();
                }
            }
        }
    }

    @Test
    public void readLacCompleteMustNotAccessOrderedEnsembleAfterRecycle() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setEnsemblePlacementPolicy(TrackingPlacementPolicy.class)
                .setReorderReadSequenceEnabled(true)
                .setFirstSpeculativeReadLACTimeout(SPEC_TIMEOUT_MS)
                .setReadTimeout(60);

        try (BookKeeperTestClient bk =
                     new BookKeeperTestClient(conf, new TestStatsProvider())) {
            LedgerHandle writer = bk.createLedger(4, 2, 2, DIGEST, passwd());
            writer.addEntry("entry-0".getBytes());
            long ledgerId = writer.getId();

            try (LedgerHandle reader = bk.openLedgerNoRecovery(ledgerId, DIGEST, passwd())) {
                writer.addEntry("entry-1".getBytes());
                writer.addEntry("entry-2".getBytes());

                List<BookieId> ensemble = reader.getLedgerMetadata().getAllEnsembles().get(0L);
                BookieId firstReplica = ensemble.get(1);
                CountDownLatch sleepLatch = new CountDownLatch(1);
                sleepBookie(firstReplica, sleepLatch);
                try {
                    ReadLacResult result = new ReadLacResult();
                    reader.asyncReadLastConfirmedAndEntry(1, 60000, false, result, null);
                    result.await();
                    assertTrue("readLAC must succeed via speculative retry",
                            result.rc == BKException.Code.OK && result.entry != null);
                    sleepLatch.countDown();
                    Thread.sleep(1000);
                } finally {
                    sleepLatch.countDown();
                }

                assertNoViolation(bk);
            } finally {
                writer.close();
            }
        }
    }

    @Test
    public void readLacLateErrorMustNotAccessOrderedEnsembleAfterRecycle() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setEnsemblePlacementPolicy(TrackingPlacementPolicy.class)
                .setReorderReadSequenceEnabled(true)
                .setFirstSpeculativeReadLACTimeout(SPEC_TIMEOUT_MS)
                .setReadEntryTimeout(1);

        try (BookKeeperTestClient bk =
                     new BookKeeperTestClient(conf, new TestStatsProvider())) {
            LedgerHandle writer = bk.createLedger(4, 2, 2, DIGEST, passwd());
            writer.addEntry("entry-0".getBytes());
            long ledgerId = writer.getId();

            try (LedgerHandle reader = bk.openLedgerNoRecovery(ledgerId, DIGEST, passwd())) {
                writer.addEntry("entry-1".getBytes());
                writer.addEntry("entry-2".getBytes());

                List<BookieId> ensemble = reader.getLedgerMetadata().getAllEnsembles().get(0L);
                BookieId slowBookie = ensemble.get(1);
                CountDownLatch sleepLatch = new CountDownLatch(1);
                sleepBookie(slowBookie, sleepLatch);
                try {
                    ReadLacResult result = new ReadLacResult();
                    reader.asyncReadLastConfirmedAndEntry(1, 60000, false, result, null);
                    result.await();
                    assertTrue("readLAC must succeed via speculative retry",
                            result.rc == BKException.Code.OK && result.entry != null);

                    Thread.sleep(2500);

                    assertNoViolation(bk);
                } finally {
                    sleepLatch.countDown();
                }
            } finally {
                writer.close();
            }
        }
    }

    private static void assertNoViolation(BookKeeperTestClient bk) {
        TrackingPlacementPolicy policy = (TrackingPlacementPolicy) bk.getPlacementPolicy();
        String violation = policy.violation.get();
        assertNull("WriteSet method '" + violation + "' was invoked after recycle()", violation);
    }

    private static byte[] passwd() {
        return "pwd".getBytes(StandardCharsets.UTF_8);
    }

    static final class ReadLacResult implements AsyncCallback.ReadLastConfirmedAndEntryCallback {
        private final CountDownLatch doneLatch = new CountDownLatch(1);
        private int rc = BKException.Code.UnexpectedConditionException;
        private LedgerEntry entry;

        @Override
        public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
            this.rc = rc;
            this.entry = entry;
            doneLatch.countDown();
        }

        void await() throws InterruptedException {
            assertTrue("readLAC callback did not complete", doneLatch.await(10, TimeUnit.SECONDS));
        }
    }

    /**
     * Placement policy whose {@code reorderReadSequence} wraps the result with a
     * {@link RecycleTrackingWriteSet}. This is the only injection point for a
     * custom {@code WriteSet} on the read path that does not require changes to
     * production code.
     */
    public static class TrackingPlacementPolicy extends RackawareEnsemblePlacementPolicy {
        private final AtomicReference<String> violation = new AtomicReference<>();

        @Override
        public DistributionSchedule.WriteSet reorderReadSequence(
                List<BookieId> ensemble,
                BookiesHealthInfo bookiesHealthInfo,
                DistributionSchedule.WriteSet writeSet) {
            DistributionSchedule.WriteSet reordered =
                    super.reorderReadSequence(ensemble, bookiesHealthInfo, writeSet);
            return new RecycleTrackingWriteSet(reordered, violation);
        }

        @Override
        public DistributionSchedule.WriteSet reorderReadLACSequence(
                List<BookieId> ensemble,
                BookiesHealthInfo bookiesHealthInfo,
                DistributionSchedule.WriteSet writeSet) {
            DistributionSchedule.WriteSet reordered =
                    super.reorderReadLACSequence(ensemble, bookiesHealthInfo, writeSet);
            return new RecycleTrackingWriteSet(reordered, violation);
        }
    }

    /**
     * Wrapper that records the first method invoked after {@code recycle()}.
     * The wrapper deliberately does <em>not</em> propagate {@code recycle()}
     * to the delegate: keeping the delegate out of the recycler pool means
     * any post-recycle calls still observe consistent data, so a caller bug
     * shows up as a recorded violation rather than a flaky exception.
     */
    static final class RecycleTrackingWriteSet implements DistributionSchedule.WriteSet {
        private final DistributionSchedule.WriteSet delegate;
        private final AtomicReference<String> violation;
        private volatile boolean recycled;

        RecycleTrackingWriteSet(DistributionSchedule.WriteSet delegate, AtomicReference<String> violation) {
            this.delegate = delegate;
            this.violation = violation;
        }

        private void check(String op) {
            if (recycled) {
                violation.compareAndSet(null, op);
            }
        }

        @Override
        public int size() {
            check("size");
            return delegate.size();
        }

        @Override
        public boolean contains(int i) {
            check("contains");
            return delegate.contains(i);
        }

        @Override
        public int get(int i) {
            check("get(" + i + ")");
            return delegate.get(i);
        }

        @Override
        public int set(int i, int idx) {
            check("set");
            return delegate.set(i, idx);
        }

        @Override
        public int indexOf(int idx) {
            check("indexOf");
            return delegate.indexOf(idx);
        }

        @Override
        public void sort() {
            check("sort");
            delegate.sort();
        }

        @Override
        public void addMissingIndices(int max) {
            check("addMissingIndices");
            delegate.addMissingIndices(max);
        }

        @Override
        public void moveAndShift(int from, int to) {
            check("moveAndShift");
            delegate.moveAndShift(from, to);
        }

        @Override
        public DistributionSchedule.WriteSet copy() {
            check("copy");
            return delegate.copy();
        }

        @Override public void recycle() {
            recycled = true;
            // intentionally not delegating recycle(); see class javadoc.
        }
    }
}
