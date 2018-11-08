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
package org.apache.bookkeeper.meta;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the ZK ledger id generator.
 */
public class TestZkLedgerIdGenerator extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestZkLedgerIdGenerator.class);

    ZooKeeperUtil zkutil;
    ZooKeeper zk;

    LedgerIdGenerator ledgerIdGenerator;

    @Override
    @Before
    public void setUp() throws Exception {
        LOG.info("Setting up test");
        super.setUp();

        zkutil = new ZooKeeperUtil();
        zkutil.startCluster();
        zk = zkutil.getZooKeeperClient();

        ledgerIdGenerator = new ZkLedgerIdGenerator(zk,
                "/test-zk-ledger-id-generator", "idgen", ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        LOG.info("Tearing down test");
        ledgerIdGenerator.close();
        zk.close();
        zkutil.killCluster();

        super.tearDown();
    }

    @Test
    public void testGenerateLedgerId() throws Exception {
        // Create *nThread* threads each generate *nLedgers* ledger id,
        // and then check there is no identical ledger id.
        final int nThread = 2;
        final int nLedgers = 2000;
        final CountDownLatch countDownLatch = new CountDownLatch(nThread * nLedgers);

        final AtomicInteger errCount = new AtomicInteger(0);
        final ConcurrentLinkedQueue<Long> ledgerIds = new ConcurrentLinkedQueue<Long>();
        final GenericCallback<Long> cb = new GenericCallback<Long>() {
            @Override
            public void operationComplete(int rc, Long result) {
                if (Code.OK.intValue() == rc) {
                    ledgerIds.add(result);
                } else {
                    errCount.incrementAndGet();
                }
                countDownLatch.countDown();
            }
        };

        long start = System.currentTimeMillis();

        for (int i = 0; i < nThread; i++) {
            new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < nLedgers; j++) {
                        ledgerIdGenerator.generateLedgerId(cb);
                    }
                }
            }.start();
        }

        assertTrue("Wait ledger id generation threads to stop timeout : ",
                countDownLatch.await(30, TimeUnit.SECONDS));
        LOG.info("Number of generated ledger id: {}, time used: {}", ledgerIds.size(),
                System.currentTimeMillis() - start);
        assertEquals("Error occur during ledger id generation : ", 0, errCount.get());

        Set<Long> ledgers = new HashSet<Long>();
        while (!ledgerIds.isEmpty()) {
            Long ledger = ledgerIds.poll();
            assertNotNull("Generated ledger id is null : ", ledger);
            assertFalse("Ledger id [" + ledger + "] conflict : ", ledgers.contains(ledger));
            ledgers.add(ledger);
        }
    }

}
