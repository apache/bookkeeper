/*
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests BKAdmin that it should be able to replicate the failed bookie fragments
 * to target bookie.
 */
public class TestLedgerFragmentReplication extends BookKeeperClusterTestCase {

    private static final byte[] TEST_PSSWD = "testpasswd".getBytes();
    private static final DigestType TEST_DIGEST_TYPE = BookKeeper.DigestType.CRC32;
    private static final BiConsumer<Long, Long> NOOP_BICONSUMER = (l, e) -> { };
    private static final Logger LOG = LoggerFactory
            .getLogger(TestLedgerFragmentReplication.class);

    public TestLedgerFragmentReplication() {
        super(3);
    }

    private static class CheckerCallback implements
            GenericCallback<Set<LedgerFragment>> {
        private Set<LedgerFragment> result = null;
        private CountDownLatch latch = new CountDownLatch(1);

        Set<LedgerFragment> waitAndGetResult() throws InterruptedException {
            latch.await();
            return result;
        }

        @Override
        public void operationComplete(int rc, Set<LedgerFragment> result) {
            this.result = result;
            latch.countDown();
        }
    }

    /**
     * Tests that replicate method should replicate the failed bookie fragments
     * to target bookie passed.
     */
    @Test
    public void testReplicateLFShouldCopyFailedBookieFragmentsToTargetBookie()
            throws Exception {
        byte[] data = "TestLedgerFragmentReplication".getBytes();
        LedgerHandle lh = bkc.createLedger(3, 3, TEST_DIGEST_TYPE,
                TEST_PSSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles()
                .get(0L).get(0);

        LOG.info("Killing Bookie : {}", replicaToKill);
        killBookie(replicaToKill);

        BookieId newBkAddr = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        Set<LedgerFragment> result = getFragmentsToReplicate(lh);

        BookKeeperAdmin admin = new BookKeeperAdmin(baseClientConf);
        lh.close();
        // 0-9 entries should be copy to new bookie

        for (LedgerFragment lf : result) {
            admin.replicateLedgerFragment(lh, lf, NOOP_BICONSUMER);
        }

        // Killing all bookies except newly replicated bookie
        SortedMap<Long, ? extends List<BookieId>> allBookiesBeforeReplication = lh
                .getLedgerMetadata().getAllEnsembles();
        for (Entry<Long, ? extends List<BookieId>> entry : allBookiesBeforeReplication.entrySet()) {
            List<BookieId> bookies = entry.getValue();
            for (BookieId bookie : bookies) {
                if (newBkAddr.equals(bookie)) {
                    continue;
                }
                killBookie(bookie);
            }
        }

        // Should be able to read the entries from 0-9
        verifyRecoveredLedgers(lh, 0, 9);
    }

    /**
     * Tests that fragment re-replication fails on last unclosed ledger
     * fragments.
     */
    @Test
    public void testReplicateLFFailsOnlyOnLastUnClosedFragments()
            throws Exception {
        byte[] data = "TestLedgerFragmentReplication".getBytes();
        LedgerHandle lh = bkc.createLedger(3, 3, TEST_DIGEST_TYPE,
                TEST_PSSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles()
                .get(0L).get(0);

        startNewBookie();
        LOG.info("Killing Bookie : {}", replicaToKill);
        killBookie(replicaToKill);

        // Lets reform ensemble
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        BookieId replicaToKill2 = lh.getLedgerMetadata()
                .getAllEnsembles().get(0L).get(1);

        BookieId newBkAddr = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr);

        LOG.info("Killing Bookie : {}", replicaToKill2);
        killBookie(replicaToKill2);

        Set<LedgerFragment> result = getFragmentsToReplicate(lh);

        BookKeeperAdmin admin = new BookKeeperAdmin(baseClientConf);
        // 0-9 entries should be copy to new bookie

        int unclosedCount = 0;
        for (LedgerFragment lf : result) {
            if (lf.isClosed()) {
                admin.replicateLedgerFragment(lh, lf, NOOP_BICONSUMER);
            } else {
                unclosedCount++;
                try {
                    admin.replicateLedgerFragment(lh, lf, NOOP_BICONSUMER);
                    fail("Shouldn't be able to rereplicate unclosed ledger");
                } catch (BKException bke) {
                    // correct behaviour
                }
            }
        }
        assertEquals("Should be only one unclosed fragment", 1, unclosedCount);
    }

    /**
     * Tests that ReplicateLedgerFragment should return false if replication
     * fails.
     */
    @Test
    public void testReplicateLFShouldReturnFalseIfTheReplicationFails()
            throws Exception {
        byte[] data = "TestLedgerFragmentReplication".getBytes();
        LedgerHandle lh = bkc.createLedger(2, 1, TEST_DIGEST_TYPE,
                TEST_PSSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        // Kill the first Bookie
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles()
                .get(0L).get(0);
        killBookie(replicaToKill);
        LOG.info("Killed Bookie =" + replicaToKill);

        // Write some more entries
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        // Kill the second Bookie
        replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        killBookie(replicaToKill);
        LOG.info("Killed Bookie =" + replicaToKill);

        Set<LedgerFragment> fragments = getFragmentsToReplicate(lh);
        BookKeeperAdmin admin = new BookKeeperAdmin(baseClientConf);
        for (LedgerFragment lf : fragments) {
            try {
                admin.replicateLedgerFragment(lh, lf, NOOP_BICONSUMER);
            } catch (BKException.BKLedgerRecoveryException e) {
                // expected
            }
        }
    }

    /**
     * Tests that splitIntoSubFragment should be able to split the original
     * passed fragment into sub fragments at correct boundaries.
     */
    @Test
    public void testSplitIntoSubFragmentsWithDifferentFragmentBoundaries()
            throws Exception {
        List<BookieId> ensemble = Lists.newArrayList(new BookieSocketAddress("192.0.2.1", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.2", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.3", 1234).toBookieId());
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(124L).withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
            .withPassword(TEST_PSSWD).withDigestType(TEST_DIGEST_TYPE.toApiDigestType())
            .withClosedState().withLastEntryId(-1).withLength(0)
            .newEnsembleEntry(0L, ensemble)
            .build();

        LedgerHandle lh = new LedgerHandle(bkc.getClientCtx(), 0,
                                           new Versioned<>(metadata, new LongVersion(0L)),
                                           TEST_DIGEST_TYPE,
                                           TEST_PSSWD, WriteFlag.NONE);
        testSplitIntoSubFragments(10, 21, -1, 1, lh);
        testSplitIntoSubFragments(10, 21, 20, 1, lh);
        testSplitIntoSubFragments(0, 0, 10, 1, lh);
        testSplitIntoSubFragments(0, 1, 1, 2, lh);
        testSplitIntoSubFragments(20, 24, 2, 3, lh);
        testSplitIntoSubFragments(21, 32, 3, 4, lh);
        testSplitIntoSubFragments(22, 103, 11, 8, lh);
        testSplitIntoSubFragments(49, 51, 1, 3, lh);
        testSplitIntoSubFragments(11, 101, 3, 31, lh);
        testSplitIntoSubFragments(0, -1, 1, 1, lh);
        testSplitIntoSubFragments(0, -1, 10, 1, lh);
    }

    /**
     * Assert the sub-fragment boundaries.
     */
    void testSplitIntoSubFragments(final long oriFragmentFirstEntry,
            final long oriFragmentLastEntry, long entriesPerSubFragment,
            long expectedSubFragments, LedgerHandle lh) {
        LedgerFragment fr = new LedgerFragment(lh, oriFragmentFirstEntry,
                oriFragmentLastEntry, Sets.newHashSet(0));
        Set<LedgerFragment> subFragments = LedgerFragmentReplicator
                .splitIntoSubFragments(lh, fr, entriesPerSubFragment);
        assertEquals(expectedSubFragments, subFragments.size());
        int fullSubFragment = 0;
        int partialSubFragment = 0;
        for (LedgerFragment ledgerFragment : subFragments) {
            if ((ledgerFragment.getLastKnownEntryId()
                    - ledgerFragment.getFirstEntryId() + 1) == entriesPerSubFragment) {
                fullSubFragment++;
            } else {
                long totalEntriesToReplicate = oriFragmentLastEntry
                        - oriFragmentFirstEntry + 1;
                if (entriesPerSubFragment <= 0
                        || totalEntriesToReplicate / entriesPerSubFragment == 0) {
                    assertEquals(
                            "FirstEntryId should be same as original fragment's firstEntryId",
                            fr.getFirstEntryId(), ledgerFragment
                                    .getFirstEntryId());
                    assertEquals(
                            "LastEntryId should be same as original fragment's lastEntryId",
                            fr.getLastKnownEntryId(), ledgerFragment
                                    .getLastKnownEntryId());
                } else {
                    long partialSplitEntries = totalEntriesToReplicate
                            % entriesPerSubFragment;
                    assertEquals(
                            "Partial fragment with wrong entry boundaries",
                            ledgerFragment.getLastKnownEntryId()
                                    - ledgerFragment.getFirstEntryId() + 1,
                            partialSplitEntries);
                }
                partialSubFragment++;
            }
        }
        assertEquals("Unexpected number of sub fargments", fullSubFragment
                + partialSubFragment, expectedSubFragments);
        assertTrue("There should be only one or zero partial sub Fragment",
                partialSubFragment == 0 || partialSubFragment == 1);
    }

    private Set<LedgerFragment> getFragmentsToReplicate(LedgerHandle lh)
            throws InterruptedException {
        LedgerChecker checker = new LedgerChecker(bkc);
        CheckerCallback cb = new CheckerCallback();
        checker.checkLedger(lh, cb);
        Set<LedgerFragment> fragments = cb.waitAndGetResult();
        return fragments;
    }

    private void verifyRecoveredLedgers(LedgerHandle lh, long startEntryId,
            long endEntryId) throws BKException, InterruptedException {
        LedgerHandle lhs = bkc.openLedgerNoRecovery(lh.getId(),
                TEST_DIGEST_TYPE, TEST_PSSWD);
        Enumeration<LedgerEntry> entries = lhs.readEntries(startEntryId,
                endEntryId);
        assertTrue("Should have the elements", entries.hasMoreElements());
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("TestLedgerFragmentReplication", new String(entry
                    .getEntry()));
        }
    }

    @Test
    public void testSplitLedgerFragmentState() throws Exception {
        int lastEntryId = 10;
        int rereplicationEntryBatchSize = 10;

        List<BookieId> ensemble = new ArrayList<BookieId>();
        ensemble.add(BookieId.parse("bookie0:3181"));
        ensemble.add(BookieId.parse("bookie1:3181"));
        ensemble.add(BookieId.parse("bookie2:3181"));
        ensemble.add(BookieId.parse("bookie3:3181"));
        ensemble.add(BookieId.parse("bookie4:3181"));
        ensemble.add(BookieId.parse("bookie5:3181"));
        ensemble.add(BookieId.parse("bookie6:3181"));

        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create();
        builder.withId(124L).withEnsembleSize(7).withWriteQuorumSize(3).withAckQuorumSize(2)
                .withDigestType(TEST_DIGEST_TYPE.toApiDigestType()).withPassword(TEST_PSSWD)
                .newEnsembleEntry(0, ensemble).withLastEntryId(lastEntryId).withLength(512).withClosedState();
        LedgerMetadata met = builder.build();

        LedgerHandle lh = new LedgerHandle(bkc.getClientCtx(), 100L, new Versioned<>(met, new LongVersion(0L)),
                TEST_DIGEST_TYPE, TEST_PSSWD, EnumSet.noneOf(WriteFlag.class));

        /*
         * create LedgerFragment from the ledger ensemble for the bookies with
         * indexes 1 and 5.
         */
        Set<Integer> bookieIndexes = new HashSet<>();
        bookieIndexes.add(1);
        bookieIndexes.add(5);
        LedgerFragment lfrag = new LedgerFragment(lh, 0, 10, bookieIndexes);

        /*
         * Since this ledger contains 11 entries (lastEntryId is 10), when it is
         * split into subFragments of size 10 it will be split into 2. In the
         * first subfragment, firstEntryID (and firstStoredEntryId) will be 0.
         * lastKnownEntryID will be 9 but lastStoredEntryId will be 8. Because
         * entry 9 will not be stored in both of the nodes and entry 8 is the
         * last entry that is stored in either one of the node.
         *
         * In the second sub-fragment firstEntryID, firstStoredEntryId,
         * lastKnownEntryID and lastStoredEntryId should be 10.
         */
        Set<LedgerFragment> partionedFragments = LedgerFragmentReplicator.splitIntoSubFragments(lh, lfrag,
                rereplicationEntryBatchSize);
        assertEquals("Number of sub-fragments", 2, partionedFragments.size());
        for (LedgerFragment partionedFragment : partionedFragments) {
            if (partionedFragment.getFirstEntryId() == 0) {
                validateEntryIds(partionedFragment, 0, 0, 9, 8);
            } else {
                validateEntryIds(partionedFragment, 10, 10, 10, 10);
            }
        }
    }

    private void validateEntryIds(LedgerFragment partionedFragment, long expectedFirstEntryId,
            long expectedFirstStoredEntryId, long expectedLastKnownEntryID, long expectedLastStoredEntryId) {
        assertEquals("FirstEntryId", expectedFirstEntryId, partionedFragment.getFirstEntryId());
        assertEquals("FirstStoredEntryId", expectedFirstStoredEntryId, partionedFragment.getFirstStoredEntryId());
        assertEquals("LastKnownEntryID", expectedLastKnownEntryID, partionedFragment.getLastKnownEntryId());
        assertEquals("LastStoredEntryId", expectedLastStoredEntryId, partionedFragment.getLastStoredEntryId());
    }
}
