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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Test the ledger manager iterator.
 */
public class LedgerManagerIteratorTest extends LedgerManagerTestCase {
    public LedgerManagerIteratorTest(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls);
    }

    final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

    Runnable safeWrapper(Runnable r) {
        return () -> {
            try {
                r.run();
            } catch (Throwable e) {
                exceptions.add(e);
            }
        };
    }

    @After
    public void throwAsyncErrors() throws Throwable {
        while (exceptions.peek() != null) {
            throw exceptions.remove();
        }
    }

    class RCCheckCB implements GenericCallback<Void> {
        private final String opType;
        private final CountDownLatch latch;
        private final Optional<Integer> rcExpected;
        private final long ledgerId;

        public RCCheckCB(String opType, CountDownLatch latch, Optional<Integer> rcExpected, long ledgerId) {
            this.opType = opType;
            this.latch = latch;
            this.rcExpected = rcExpected;
            this.ledgerId = ledgerId;
        }

        @Override
        public void operationComplete(int rc, Void result) {
            safeWrapper(() -> {
                try {
                    rcExpected.map((Integer expected) -> {
                        assertEquals(
                                "Incorrect rc on ledger: " + ledgerId + ", op type: " + opType,
                                expected.longValue(), rc);
                        return null;
                    });
                } finally {
                    latch.countDown();
                }
            }).run();
        }
    }

    /**
     * Remove ledger using lm syncronously.
     *
     * @param lm
     * @param ledgerId
     * @param rcExpected return value expected, -1 to ignore
     * @throws InterruptedException
     */
    void removeLedger(LedgerManager lm, Long ledgerId, Optional<Integer> rcExpected) throws Throwable {
        CountDownLatch latch = new CountDownLatch(1);
        lm.removeLedgerMetadata(
                ledgerId, Version.ANY, new RCCheckCB("removeLedger", latch, rcExpected, ledgerId));
        latch.await();
        throwAsyncErrors();

    }

    /**
     * Create ledger using lm syncronously.
     *
     * @param lm
     * @param ledgerId
     * @param rcExpected return value expected, -1 to ignore
     * @throws InterruptedException
     */
    void createLedger(LedgerManager lm, Long ledgerId, Optional<Integer> rcExpected) throws Throwable {
        LedgerMetadata meta = new LedgerMetadata(
                3, 3, 2,
                BookKeeper.DigestType.CRC32, "passwd".getBytes());
        CountDownLatch latch = new CountDownLatch(1);
        lm.createLedgerMetadata(
                ledgerId, meta, new RCCheckCB("createLedger", latch, rcExpected, ledgerId));
        latch.await();
        throwAsyncErrors();
    }

    static Set<Long> ledgerRangeToSet(LedgerRangeIterator lri) throws IOException {
        Set<Long> ret = new TreeSet<>();
        long last = -1;
        while (lri.hasNext()) {
            LedgerManager.LedgerRange lr = lri.next();
            assertFalse("ledger range must not be empty", lr.getLedgers().isEmpty());
            assertTrue("ledger ranges must not overlap", last < lr.start());
            ret.addAll(lr.getLedgers());
            last = lr.end();
        }
        return ret;
    }

    static Set<Long> getLedgerIdsByUsingAsyncProcessLedgers(LedgerManager lm) throws InterruptedException{
        Set<Long> ledgersReadAsync = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger finalRC = new AtomicInteger();

        lm.asyncProcessLedgers((ledgerId, callback) -> {
            ledgersReadAsync.add(ledgerId);
            callback.processResult(BKException.Code.OK, null, null);
        }, (rc, s, obj) -> {
            finalRC.set(rc);
            latch.countDown();
        }, null, BKException.Code.OK, BKException.Code.ReadException);

        latch.await();
        assertEquals("Final RC of asyncProcessLedgers", BKException.Code.OK, finalRC.get());
        return ledgersReadAsync;
    }

    @Test
    public void testIterateNoLedgers() throws Exception {
        LedgerManager lm = getLedgerManager();
        LedgerRangeIterator lri = lm.getLedgerRanges();
        assertNotNull(lri);
        if (lri.hasNext()) {
            lri.next();
        }

        assertEquals(false, lri.hasNext());
    }

    @Test
    public void testSingleLedger() throws Throwable {
        LedgerManager lm = getLedgerManager();

        long id = 2020202;
        createLedger(lm, id, Optional.of(BKException.Code.OK));

        LedgerRangeIterator lri = lm.getLedgerRanges();
        assertNotNull(lri);
        Set<Long> lids = ledgerRangeToSet(lri);
        assertEquals(lids.size(), 1);
        assertEquals(lids.iterator().next().longValue(), id);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", lids, ledgersReadAsync);
    }

    @Test
    public void testTwoLedgers() throws Throwable {
        LedgerManager lm = getLedgerManager();

        Set<Long> ids = new TreeSet<>(Arrays.asList(101010101L, 2020340302L));
        for (Long id: ids) {
            createLedger(lm, id, Optional.of(BKException.Code.OK));
        }

        LedgerRangeIterator lri = lm.getLedgerRanges();
        assertNotNull(lri);
        Set<Long> returnedIds = ledgerRangeToSet(lri);
        assertEquals(ids, returnedIds);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ids, ledgersReadAsync);
    }

    @Test
    public void testSeveralContiguousLedgers() throws Throwable {
        LedgerManager lm = getLedgerManager();

        Set<Long> ids = new TreeSet<>();
        for (long i = 0; i < 2000; ++i) {
            createLedger(lm, i, Optional.of(BKException.Code.OK));
            ids.add(i);
        }

        LedgerRangeIterator lri = lm.getLedgerRanges();
        assertNotNull(lri);
        Set<Long> returnedIds = ledgerRangeToSet(lri);
        assertEquals(ids, returnedIds);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ids, ledgersReadAsync);
    }

    @Test
    public void testRemovalOfNodeJustTraversed() throws Throwable {
        if (baseConf.getLedgerManagerFactoryClass()
                != LongHierarchicalLedgerManagerFactory.class) {
            return;
        }
        LedgerManager lm = getLedgerManager();

        /* For LHLM, first two should be leaves on the same node, second should be on adjacent level 4 node
         * Removing all 3 once the iterator hits the first should result in the whole tree path ending
         * at that node disappearing.  If this happens after the iterator stops at that leaf, it should
         * result in a few NodeExists errors (handled silently) as the iterator fails back up the tree
         * to the next path.
         */
        Set<Long> toRemove = new TreeSet<>(
                Arrays.asList(
                        3394498498348983841L,
                        3394498498348983842L,
                        3394498498348993841L));

        long first = 2345678901234567890L;
        // Nodes which should be listed anyway
        Set<Long> mustHave = new TreeSet<>(
                Arrays.asList(
                        first,
                        6334994393848474732L));

        Set<Long> ids = new TreeSet<>();
        ids.addAll(toRemove);
        ids.addAll(mustHave);
        for (Long id: ids) {
            createLedger(lm, id, Optional.of(BKException.Code.OK));
        }

        Set<Long> found = new TreeSet<>();
        LedgerRangeIterator lri = lm.getLedgerRanges();
        while (lri.hasNext()) {
            LedgerManager.LedgerRange lr = lri.next();
            found.addAll(lr.getLedgers());

            if (lr.getLedgers().contains(first)) {
                for (long id: toRemove) {
                    removeLedger(lm, id, Optional.of(BKException.Code.OK));
                }
                toRemove.clear();
            }
        }

        for (long id: mustHave) {
            assertTrue(found.contains(id));
        }
    }

    @Test
    public void validateEmptyL4PathSkipped() throws Throwable {
        if (baseConf.getLedgerManagerFactoryClass()
                != LongHierarchicalLedgerManagerFactory.class) {
            return;
        }
        LedgerManager lm = getLedgerManager();

        Set<Long> ids = new TreeSet<>(
                Arrays.asList(
                        2345678901234567890L,
                        3394498498348983841L,
                        6334994393848474732L,
                        7349370101927398483L));
        for (Long id: ids) {
            createLedger(lm, id, Optional.of(BKException.Code.OK));
        }

        String paths[] = {
                "/ledgers/633/4994/3938/4948", // Empty L4 path, must be skipped

        };

        for (String path : paths) {
            ZkUtils.createFullPathOptimistic(
                    zkc,
                    path, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        LedgerRangeIterator lri = lm.getLedgerRanges();
        assertNotNull(lri);
        Set<Long> returnedIds = ledgerRangeToSet(lri);
        assertEquals(ids, returnedIds);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ids, ledgersReadAsync);

        lri = lm.getLedgerRanges();
        int emptyRanges = 0;
        while (lri.hasNext()) {
            if (lri.next().getLedgers().isEmpty()) {
                emptyRanges++;
            }
        }
        assertEquals(0, emptyRanges);
    }

    @Test
    public void testWithSeveralIncompletePaths() throws Throwable {
        if (baseConf.getLedgerManagerFactoryClass()
                != LongHierarchicalLedgerManagerFactory.class) {
            return;
        }
        LedgerManager lm = getLedgerManager();

        Set<Long> ids = new TreeSet<>(
                Arrays.asList(
                        2345678901234567890L,
                        3394498498348983841L,
                        6334994393848474732L,
                        7349370101927398483L));
        for (Long id: ids) {
            createLedger(lm, id, Optional.of(BKException.Code.OK));
        }

        String paths[] = {
                "/ledgers/000/0000/0000", // top level, W-4292762
                "/ledgers/234/5678/9999", // shares two path segments with the first one, comes after
                "/ledgers/339/0000/0000", // shares one path segment with the second one, comes first
                "/ledgers/633/4994/3938/0000", // shares three path segments with the third one, comes first
                "/ledgers/922/3372/0000/0000", // close to max long, at end

        };
        for (String path : paths) {
            ZkUtils.createFullPathOptimistic(
                    zkc,
                    path, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        LedgerRangeIterator lri = lm.getLedgerRanges();
        assertNotNull(lri);
        Set<Long> returnedIds = ledgerRangeToSet(lri);
        assertEquals(ids, returnedIds);

        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ids, ledgersReadAsync);
    }

    @Test
    public void checkConcurrentModifications() throws Throwable {
        final int numWriters = 10;
        final int numCheckers = 10;
        final int numLedgers = 100;
        final long runtime = TimeUnit.NANOSECONDS.convert(2, TimeUnit.SECONDS);
        final boolean longRange =
                baseConf.getLedgerManagerFactoryClass() == LongHierarchicalLedgerManagerFactory.class;

        final Set<Long> mustExist = new TreeSet<>();
        LedgerManager lm = getLedgerManager();
        Random rng = new Random();
        for (int i = 0; i < numLedgers; ++i) {
            long lid = Math.abs(rng.nextLong());
            if (!longRange) {
                lid %= 1000000;
            }
            createLedger(lm, lid, Optional.of(BKException.Code.OK));
            mustExist.add(lid);
        }

        final long start = MathUtils.nowInNano();
        final CountDownLatch latch = new CountDownLatch(1);
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numWriters; ++i) {
            Thread thread = new Thread(safeWrapper(() -> {
                LedgerManager writerLM = getIndependentLedgerManager();
                Random writerRNG = new Random(rng.nextLong());
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Checker interrupted");
                }
                while (MathUtils.elapsedNanos(start) < runtime) {
                    long candidate = 0;
                    do {
                        candidate = Math.abs(writerRNG.nextLong());
                        if (!longRange) {
                            candidate %= 1000000;
                        }
                    } while (mustExist.contains(candidate));
                    try {
                        createLedger(writerLM, candidate, Optional.empty());
                        removeLedger(writerLM, candidate, Optional.empty());
                    } catch (Throwable e) {
                        fail("Got exception thrashing store: " + e.toString());
                    }
                }
            }));
            thread.start();
            threads.add(thread);
        }

        for (int i = 0; i < numCheckers; ++i) {
            Thread thread = new Thread(safeWrapper(() -> {
                LedgerManager checkerLM = getIndependentLedgerManager();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Checker interrupted");
                    e.printStackTrace();
                }
                while (MathUtils.elapsedNanos(start) < runtime) {
                    try {
                        LedgerRangeIterator lri = checkerLM.getLedgerRanges();
                        Set<Long> returnedIds = ledgerRangeToSet(lri);
                        for (long id: mustExist) {
                            assertTrue(returnedIds.contains(id));
                        }

                        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(checkerLM);
                        for (long id: mustExist) {
                            assertTrue(ledgersReadAsync.contains(id));
                        }
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                        fail("Got exception scanning ledgers: " + e.toString());
                    }
                }
            }));
            thread.start();
            threads.add(thread);
        }

        latch.countDown();
        for (Thread thread: threads) {
            thread.join();
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testLedgerParentNode() throws Throwable {
        /*
         * this testcase applies only ZK based ledgermanager so it doesnt work
         * for MSLedgerManager
         */
        Assume.assumeTrue(!baseConf.getLedgerManagerFactoryClass().equals(MSLedgerManagerFactory.class));
        AbstractZkLedgerManager lm = (AbstractZkLedgerManager) getLedgerManager();
        List<Long> ledgerIds;
        if (baseConf.getLedgerManagerFactoryClass().equals(HierarchicalLedgerManagerFactory.class)
                || baseConf.getLedgerManagerFactoryClass().equals(LongHierarchicalLedgerManagerFactory.class)) {
            ledgerIds = Arrays.asList(100L, (Integer.MAX_VALUE * 10L));
        } else {
            ledgerIds = Arrays.asList(100L, (Integer.MAX_VALUE - 10L));
        }
        for (long ledgerId : ledgerIds) {
            String fullLedgerPath = lm.getLedgerPath(ledgerId);
            String ledgerPath = fullLedgerPath.replaceAll(
                ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf) + "/",
                "");
            String[] znodesOfLedger = ledgerPath.split("/");
            Assert.assertTrue(znodesOfLedger[0] + " is supposed to be valid parent ",
                    lm.isLedgerParentNode(znodesOfLedger[0]));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testLedgerManagerFormat() throws Throwable {
        String zkLedgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf);
        /*
         * this testcase applies only ZK based ledgermanager so it doesnt work
         * for MSLedgerManager
         */
        Assume.assumeTrue(!baseConf.getLedgerManagerFactoryClass().equals(MSLedgerManagerFactory.class));
        AbstractZkLedgerManager lm = (AbstractZkLedgerManager) getLedgerManager();
        Collection<Long> ids = Arrays.asList(1234567890L, 2L, 32345L, 23456789L);
        if (baseConf.getLedgerManagerFactoryClass().equals(HierarchicalLedgerManagerFactory.class)
                || baseConf.getLedgerManagerFactoryClass().equals(LongHierarchicalLedgerManagerFactory.class)) {
            ids = new ArrayList<Long>(ids);
            ids.add(Integer.MAX_VALUE * 2L);
            ids.add(1234567891234L);
        }
        for (Long id : ids) {
            createLedger(lm, id, Optional.of(BKException.Code.OK));
        }

        // create some invalid nodes under zkLedgersRootPath
        Collection<String> invalidZnodes = Arrays.asList("12345", "12345678901L", "abc", "123d");
        for (String invalidZnode : invalidZnodes) {
            ZkUtils.createFullPathOptimistic(zkc, zkLedgersRootPath + "/" + invalidZnode,
                    "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        /*
         * get the count of total children under zkLedgersRootPath and also
         * count of the parent nodes of ledgers under zkLedgersRootPath
         */
        List<String> childrenOfLedgersRootPath = zkc.getChildren(zkLedgersRootPath, false);
        int totalChildrenOfLedgersRootPath = childrenOfLedgersRootPath.size();
        int totalParentNodesOfLedgers = 0;
        for (String childOfLedgersRootPath : childrenOfLedgersRootPath) {
            if (lm.isLedgerParentNode(childOfLedgersRootPath)) {
                totalParentNodesOfLedgers++;
            }
        }

        /*
         * after ledgermanagerfactory format only the znodes of created ledgers
         * under zkLedgersRootPath should be deleted recursively but not
         * specialnode or invalid nodes created above
         */
        ledgerManagerFactory.format(baseConf,
                new ZkLayoutManager(zkc, zkLedgersRootPath, ZkUtils.getACLs(baseConf)));
        List<String> childrenOfLedgersRootPathAfterFormat = zkc.getChildren(zkLedgersRootPath, false);
        int totalChildrenOfLedgersRootPathAfterFormat = childrenOfLedgersRootPathAfterFormat.size();
        Assert.assertEquals("totalChildrenOfLedgersRootPathAfterFormat",
                totalChildrenOfLedgersRootPath - totalParentNodesOfLedgers, totalChildrenOfLedgersRootPathAfterFormat);

        Assert.assertTrue("ChildrenOfLedgersRootPathAfterFormat should contain all the invalid znodes created",
                childrenOfLedgersRootPathAfterFormat.containsAll(invalidZnodes));
    }

    @Test
    public void hierarchicalLedgerManagerAsyncProcessLedgersTest() throws Throwable {
        Assume.assumeTrue(baseConf.getLedgerManagerFactoryClass().equals(HierarchicalLedgerManagerFactory.class));
        LedgerManager lm = getLedgerManager();
        LedgerRangeIterator lri = lm.getLedgerRanges();

        Set<Long> ledgerIds = new TreeSet<>(Arrays.asList(1234L, 123456789123456789L));
        for (Long ledgerId : ledgerIds) {
            createLedger(lm, ledgerId, Optional.of(BKException.Code.OK));
        }
        Set<Long> ledgersReadThroughIterator = ledgerRangeToSet(lri);
        assertEquals("Comparing LedgersIds read through Iterator", ledgerIds, ledgersReadThroughIterator);
        Set<Long> ledgersReadAsync = getLedgerIdsByUsingAsyncProcessLedgers(lm);
        assertEquals("Comparing LedgersIds read asynchronously", ledgerIds, ledgersReadAsync);
    }
}
