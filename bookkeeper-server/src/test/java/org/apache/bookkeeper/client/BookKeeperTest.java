package org.apache.bookkeeper.client;

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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BaseTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of the main BookKeeper client
 */
public class BookKeeperTest extends BaseTestCase {
    static Logger LOG = LoggerFactory.getLogger(BookKeeperTest.class);

    DigestType digestType;

    public BookKeeperTest(DigestType digestType) {
        super(4);

        this.digestType = digestType;
    }

    @Test(timeout=60000)
    public void testConstructionZkDelay() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);
        zkUtil.sleepServer(5, l);
        l.await();

        BookKeeper bkc = new BookKeeper(conf);
        bkc.createLedger(digestType, "testPasswd".getBytes()).close();
        bkc.close();
    }

    @Test(timeout=60000)
    public void testConstructionNotConnectedExplicitZk() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);
        zkUtil.sleepServer(5, l);
        l.await();

        ZooKeeper zk = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000,
                            new Watcher() {
                                @Override
                                public void process(WatchedEvent event) {
                                }
                            });
        assertFalse("ZK shouldn't have connected yet", zk.getState().isConnected());
        try {
            BookKeeper bkc = new BookKeeper(conf, zk);
            fail("Shouldn't be able to construct with unconnected zk");
        } catch (KeeperException.ConnectionLossException cle) {
            // correct behaviour
        }
    }

    /**
     * Test that bookkeeper is not able to open ledgers if
     * it provides the wrong password or wrong digest
     */
    @Test(timeout=60000)
    public void testBookkeeperPassword() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkc = new BookKeeper(conf);

        DigestType digestCorrect = digestType;
        byte[] passwdCorrect = "AAAAAAA".getBytes();
        DigestType digestBad = digestType == DigestType.MAC ? DigestType.CRC32 : DigestType.MAC;
        byte[] passwdBad = "BBBBBBB".getBytes();


        LedgerHandle lh = null;
        try {
            lh = bkc.createLedger(digestCorrect, passwdCorrect);
            long id = lh.getId();
            for (int i = 0; i < 100; i++) {
                lh.addEntry("foobar".getBytes());
            }
            lh.close();

            // try open with bad passwd
            try {
                bkc.openLedger(id, digestCorrect, passwdBad);
                fail("Shouldn't be able to open with bad passwd");
            } catch (BKException.BKUnauthorizedAccessException bke) {
                // correct behaviour
            }

            // try open with bad digest
            try {
                bkc.openLedger(id, digestBad, passwdCorrect);
                fail("Shouldn't be able to open with bad digest");
            } catch (BKException.BKDigestMatchException bke) {
                // correct behaviour
            }

            // try open with both bad
            try {
                bkc.openLedger(id, digestBad, passwdBad);
                fail("Shouldn't be able to open with bad passwd and digest");
            } catch (BKException.BKUnauthorizedAccessException bke) {
                // correct behaviour
            }

            // try open with both correct
            bkc.openLedger(id, digestCorrect, passwdCorrect).close();
        } finally {
            if (lh != null) {
                lh.close();
            }
            bkc.close();
        }
    }

    /**
     * Tests that when trying to use a closed BK client object we get
     * a callback error and not an InterruptedException.
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testAsyncReadWithError() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "testPasswd".getBytes());
        bkc.close();

        final AtomicInteger result = new AtomicInteger(0);
        final CountDownLatch counter = new CountDownLatch(1);

        // Try to write, we shoud get and error callback but not an exception
        lh.asyncAddEntry("test".getBytes(), new AddCallback() {
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                result.set(rc);
                counter.countDown();
            }
        }, null);

        counter.await();

        Assert.assertTrue(result.get() != 0);
    }

    /**
     * Test that bookkeeper will close cleanly if close is issued
     * while another operation is in progress.
     */
    @Test(timeout=60000)
    public void testCloseDuringOp() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        for (int i = 0; i < 100; i++) {
            final BookKeeper client = new BookKeeper(conf);
            final CountDownLatch l = new CountDownLatch(1);
            final AtomicBoolean success = new AtomicBoolean(false);
            Thread t = new Thread() {
                    public void run() {
                        try {
                            LedgerHandle lh = client.createLedger(3, 3, digestType, "testPasswd".getBytes());
                            startNewBookie();
                            killBookie(0);
                            lh.asyncAddEntry("test".getBytes(), new AddCallback() {
                                    @Override
                                    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                                        // noop, we don't care if this completes
                                    }
                                }, null);
                            client.close();
                            success.set(true);
                            l.countDown();
                        } catch (Exception e) {
                            LOG.error("Error running test", e);
                            success.set(false);
                            l.countDown();
                        }
                    }
                };
            t.start();
            assertTrue("Close never completed", l.await(10, TimeUnit.SECONDS));
            assertTrue("Close was not successful", success.get());
        }
    }
    
    @Test(timeout=60000)
    public void testIsClosed() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
        .setZkServers(zkUtil.getZooKeeperConnectString());

        BookKeeper bkc = new BookKeeper(conf);
        LedgerHandle lh = bkc.createLedger(digestType, "testPasswd".getBytes());
        Long lId = lh.getId();

        lh.addEntry("000".getBytes());
        boolean result = bkc.isClosed(lId);
        Assert.assertTrue("Ledger shouldn't be flagged as closed!",!result);

        lh.close();
        result = bkc.isClosed(lId);
        Assert.assertTrue("Ledger should be flagged as closed!",result);

        bkc.close();
    }
}
