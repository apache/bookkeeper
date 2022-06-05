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

import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the creation of ledger metadata.
 */
public class LedgerMetadataCreationTest extends LedgerManagerTestCase {
    static final Logger LOG = LoggerFactory.getLogger(LedgerMetadataCreationTest.class);

    public LedgerMetadataCreationTest(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls, 4);
        baseConf.setGcWaitTime(100000);
    }

    @Test
    public void testLedgerCreationAndDeletionWithRandomLedgerIds() throws Exception {
        testExecution(true);
    }

    @Test
    public void testLedgerCreationAndDeletion() throws Exception{
        testExecution(false);
    }

    public void testExecution(boolean randomLedgerId) throws Exception {
        Set<Long> createRequestsLedgerIds = ConcurrentHashMap.newKeySet();
        ConcurrentLinkedDeque<Long> existingLedgerIds = new ConcurrentLinkedDeque<Long>();

        Vector<Long> failedCreates = new Vector<Long>();
        Vector<Long> failedDeletes = new Vector<Long>();
        BookKeeper bookKeeper = new BookKeeper(baseClientConf);

        ExecutorService executor = Executors.newFixedThreadPool(300);
        Random rand = new Random();
        int numberOfOperations = 20000;
        for (int i = 0; i < numberOfOperations; i++) {
            int iteration = i;
            if (rand.nextBoolean() || existingLedgerIds.isEmpty()) {
                executor.submit(() -> {
                    long ledgerId = -1;
                    try {
                        if (randomLedgerId) {
                            do {
                                ledgerId = Math.abs(rand.nextLong());
                                if (!baseClientConf.getLedgerManagerFactoryClass()
                                        .equals(LongHierarchicalLedgerManagerFactory.class)) {
                                    /*
                                     * since LongHierarchicalLedgerManager
                                     * supports ledgerIds of decimal length upto
                                     * 19 digits but other LedgerManagers only
                                     * upto 10 decimals
                                     */
                                    ledgerId %= 9999999999L;
                                }
                            } while (!createRequestsLedgerIds.add(ledgerId));
                        } else {
                            ledgerId = iteration;
                        }
                        bookKeeper.createLedgerAdv(ledgerId, 3, 2, 2, DigestType.CRC32, "passwd".getBytes(), null);
                        existingLedgerIds.add(ledgerId);
                    } catch (Exception e) {
                        LOG.error("Got Exception while creating Ledger with ledgerId " + ledgerId, e);
                        failedCreates.add(ledgerId);
                    }
                });
            } else {
                executor.submit(() -> {
                    Long ledgerId = null;
                    if (rand.nextBoolean()) {
                        ledgerId = existingLedgerIds.pollFirst();
                    } else {
                        ledgerId = existingLedgerIds.pollLast();
                    }
                    if (ledgerId == null) {
                        return;
                    }
                    try {
                        bookKeeper.deleteLedger(ledgerId);
                    } catch (Exception e) {
                        LOG.error("Got Exception while deleting Ledger with ledgerId " + ledgerId, e);
                        failedDeletes.add(ledgerId);
                    }
                });
            }
        }
        executor.shutdown();
        assertTrue("All the ledger create/delete operations should have'been completed",
                executor.awaitTermination(120, TimeUnit.SECONDS));
        assertTrue("There should be no failed creates. But there are " + failedCreates.size() + " failedCreates",
                failedCreates.isEmpty());
        assertTrue("There should be no failed deletes. But there are " + failedDeletes.size() + " failedDeletes",
                failedDeletes.isEmpty());
        bookKeeper.close();
    }

    @Test
    public void testParentNodeDeletion() throws Exception {
        /*
         * run this testcase only for HierarchicalLedgerManager and
         * LongHierarchicalLedgerManager, since we do recursive zNode deletes
         * only for HierarchicalLedgerManager
         */
        Assume.assumeTrue((baseClientConf.getLedgerManagerFactoryClass().equals(HierarchicalLedgerManagerFactory.class)
                || baseClientConf.getLedgerManagerFactoryClass().equals(LongHierarchicalLedgerManagerFactory.class)));

        ZooKeeper zkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000, null);
        BookKeeper bookKeeper = new BookKeeper(baseClientConf);
        bookKeeper.createLedgerAdv(1, 3, 2, 2, DigestType.CRC32, "passwd".getBytes(), null);
        String ledgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseClientConf);
        String parentZnodePath;
        if (baseClientConf.getLedgerManagerFactoryClass().equals(HierarchicalLedgerManagerFactory.class)) {
            /*
             * in HierarchicalLedgerManager (ledgersRootPath)/00/0000/L0001
             * would be the path of the znode for ledger - 1. So when ledger - 1
             * is deleted, (ledgersRootPath)/00 should also be deleted since
             * there are no other children znodes
             */
            parentZnodePath = ledgersRootPath + "/00";

        } else {
            /*
             * in LongHierarchicalLedgerManager
             * (ledgersRootPath)/000/0000/0000/0000/L0001 would be the path of
             * the znode for ledger - 1. So when ledger - 1 is deleted,
             * (ledgersRootPath)/000 should also be deleted since there are no
             * other children znodes
             */
            parentZnodePath = ledgersRootPath + "/000";
        }
        assertTrue(parentZnodePath + " zNode should exist", null != zkc.exists(parentZnodePath, false));
        bookKeeper.deleteLedger(1);
        assertTrue(parentZnodePath + " zNode should not exist anymore", null == zkc.exists(parentZnodePath, false));
        bookKeeper.close();
        zkc.close();
    }
}
