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

import io.netty.util.IllegalReferenceCountException;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException.BKBookieHandleNotAvailableException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Tests of the main BookKeeper client
 */
public class BookKeeperTest extends BookKeeperClusterTestCase {
    private final static Logger LOG = LoggerFactory.getLogger(BookKeeperTest.class);

    private final DigestType digestType;

    public BookKeeperTest() {
        super(4);
        this.digestType = DigestType.CRC32;
    }

    @Test
    public void testConstructionZkDelay() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);
        zkUtil.sleepServer(200, TimeUnit.MILLISECONDS, l);
        l.await();

        BookKeeper bkc = new BookKeeper(conf);
        bkc.createLedger(digestType, "testPasswd".getBytes()).close();
        bkc.close();
    }

    @Test
    public void testConstructionNotConnectedExplicitZk() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);
        zkUtil.sleepServer(200, TimeUnit.MILLISECONDS, l);
        l.await();

        ZooKeeper zk = new ZooKeeper(
            zkUtil.getZooKeeperConnectString(),
            50,
            event -> {});
        assertFalse("ZK shouldn't have connected yet", zk.getState().isConnected());
        try {
            BookKeeper bkc = new BookKeeper(conf, zk);
            fail("Shouldn't be able to construct with unconnected zk");
        } catch (IOException cle) {
            // correct behaviour
            assertTrue(cle.getCause() instanceof ConnectionLossException);
        }
    }

    /**
     * Test that bookkeeper is not able to open ledgers if
     * it provides the wrong password or wrong digest
     */
    @Test
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
    @Test
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
    @Test
    public void testCloseDuringOp() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        for (int i = 0; i < 10; i++) {
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

    @Test
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

    @Test
    public void testReadFailureCallback() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());

        BookKeeper bkc = new BookKeeper(conf);
        LedgerHandle lh = bkc.createLedger(digestType, "testPasswd".getBytes());

        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("entry-" + i).getBytes());
        }

        stopBKCluster();

        try {
            lh.readEntries(0, numEntries - 1);
            fail("Read operation should have failed");
        } catch (BKBookieHandleNotAvailableException e) {
            // expected
        }

        final CountDownLatch counter = new CountDownLatch(1);
        final AtomicInteger receivedResponses = new AtomicInteger(0);
        final AtomicInteger returnCode = new AtomicInteger();
        lh.asyncReadEntries(0, numEntries - 1, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                returnCode.set(rc);
                receivedResponses.incrementAndGet();
                counter.countDown();
            }
        }, null);

        counter.await();

        // Wait extra time to ensure no extra responses received
        Thread.sleep(1000);

        assertEquals(1, receivedResponses.get());
        assertEquals(BKException.Code.BookieHandleNotAvailableException, returnCode.get());

        bkc.close();

        startBKCluster();
    }

    @Test
    public void testAutoCloseableBookKeeper() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper _bkc;
        try (BookKeeper bkc = new BookKeeper(conf)) {
            _bkc = bkc;
            long ledgerId;
            try (LedgerHandle lh = bkc.createLedger(digestType, "testPasswd".getBytes())) {
                ledgerId = lh.getId();
                for (int i = 0; i < 100; i++) {
                    lh.addEntry("foobar".getBytes());
                }
            }
            Assert.assertTrue("Ledger should be closed!", bkc.isClosed(ledgerId));
        }
        Assert.assertTrue("BookKeeper should be closed!", _bkc.closed);
    }

    @Test
    public void testReadHandleWithNoExplicitLAC() throws Exception {
        ClientConfiguration confWithNoExplicitLAC = new ClientConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString());
        confWithNoExplicitLAC.setExplictLacInterval(0);

        BookKeeper bkcWithNoExplicitLAC = new BookKeeper(confWithNoExplicitLAC);

        LedgerHandle wlh = bkcWithNoExplicitLAC.createLedger(digestType, "testPasswd".getBytes());
        long ledgerId = wlh.getId();
        int numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        LedgerHandle rlh = bkcWithNoExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes());
        Assert.assertTrue(
                "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

        Enumeration<LedgerEntry> entries = rlh.readEntries(0, numOfEntries - 2);
        int entryId = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String entryString = new String(entry.getEntry());
            Assert.assertTrue("Expected entry String: " + ("foobar" + entryId) + " actual entry String: " + entryString,
                    entryString.equals("foobar" + entryId));
            entryId++;
        }

        for (int i = numOfEntries; i < 2 * numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        Thread.sleep(3000);
        // since explicitlacflush policy is not enabled for writeledgerhandle, when we try
        // to read explicitlac for rlh, it will be LedgerHandle.INVALID_ENTRY_ID. But it
        // wont throw some exception.
        long explicitlac = rlh.readExplicitLastConfirmed();
        Assert.assertTrue(
                "Expected Explicit LAC of rlh: " + LedgerHandle.INVALID_ENTRY_ID + " actual ExplicitLAC of rlh: " + explicitlac,
                (explicitlac == LedgerHandle.INVALID_ENTRY_ID));
        Assert.assertTrue(
                "Expected LAC of wlh: " + (2 * numOfEntries - 1) + " actual LAC of rlh: " + wlh.getLastAddConfirmed(),
                (wlh.getLastAddConfirmed() == (2 * numOfEntries - 1)));
        Assert.assertTrue(
                "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

        try {
            rlh.readEntries(numOfEntries - 1, numOfEntries - 1);
            fail("rlh readEntries beyond " + (numOfEntries - 2) + " should fail with ReadException");
        } catch (BKException.BKReadException readException) {
        }

        rlh.close();
        wlh.close();
        bkcWithNoExplicitLAC.close();
    }

    @Test
    public void testReadHandleWithExplicitLAC() throws Exception {
        ClientConfiguration confWithExplicitLAC = new ClientConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString());
        int explicitLacIntervalMillis = 1000;
        confWithExplicitLAC.setExplictLacInterval(explicitLacIntervalMillis);

        BookKeeper bkcWithExplicitLAC = new BookKeeper(confWithExplicitLAC);

        LedgerHandle wlh = bkcWithExplicitLAC.createLedger(digestType, "testPasswd".getBytes());
        long ledgerId = wlh.getId();
        int numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        LedgerHandle rlh = bkcWithExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes());

        Assert.assertTrue(
                "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

        for (int i = numOfEntries; i < 2 * numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        // we need to wait for atleast 2 explicitlacintervals,
        // since in writehandle for the first call
        // lh.getExplicitLastAddConfirmed() will be <
        // lh.getPiggyBackedLastAddConfirmed(),
        // so it wont make explicit writelac in the first run
        Thread.sleep((2 * explicitLacIntervalMillis/1000 + 1) * 1000);
        Assert.assertTrue(
                "Expected LAC of wlh: " + (2 * numOfEntries - 1) + " actual LAC of wlh: " + wlh.getLastAddConfirmed(),
                (wlh.getLastAddConfirmed() == (2 * numOfEntries - 1)));
        // readhandle's lastaddconfirmed wont be updated until readExplicitLastConfirmed call is made   
        Assert.assertTrue(
                "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (numOfEntries - 2)));
        
        long explicitlac = rlh.readExplicitLastConfirmed();
        Assert.assertTrue(
                "Expected Explicit LAC of rlh: " + (2 * numOfEntries - 1) + " actual ExplicitLAC of rlh: " + explicitlac,
                (explicitlac == (2 * numOfEntries - 1)));
        // readExplicitLastConfirmed updates the lac of rlh.
        Assert.assertTrue(
                "Expected LAC of rlh: " + (2 * numOfEntries - 1) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (2 * numOfEntries - 1)));
        
        Enumeration<LedgerEntry> entries = rlh.readEntries(numOfEntries, 2 * numOfEntries - 1);
        int entryId = numOfEntries;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String entryString = new String(entry.getEntry());
            Assert.assertTrue("Expected entry String: " + ("foobar" + entryId) + " actual entry String: " + entryString,
                    entryString.equals("foobar" + entryId));
            entryId++;
        }

        rlh.close();
        wlh.close();
        bkcWithExplicitLAC.close();
    }

    @Test
    public void testReadAfterLastAddConfirmed() throws Exception {

        ClientConfiguration clientConfiguration = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());

        try (BookKeeper bkWriter = new BookKeeper(clientConfiguration)) {
            LedgerHandle writeLh = bkWriter.createLedger(digestType, "testPasswd".getBytes());
            long ledgerId = writeLh.getId();
            int numOfEntries = 5;
            for (int i = 0; i < numOfEntries; i++) {
                writeLh.addEntry(("foobar" + i).getBytes());
            }

            try (BookKeeper bkReader = new BookKeeper(clientConfiguration);
                LedgerHandle rlh = bkReader.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes())) {
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                Assert.assertFalse(writeLh.isClosed());

                // with readUnconfirmedEntries we are able to read all of the entries
                Enumeration<LedgerEntry> entries = rlh.readUnconfirmedEntries(0, numOfEntries - 1);
                int entryId = 0;
                while (entries.hasMoreElements()) {
                    LedgerEntry entry = entries.nextElement();
                    String entryString = new String(entry.getEntry());
                    Assert.assertTrue("Expected entry String: " + ("foobar" + entryId)
                        + " actual entry String: " + entryString,
                        entryString.equals("foobar" + entryId));
                    entryId++;
                }
            }

            try (BookKeeper bkReader = new BookKeeper(clientConfiguration);
                LedgerHandle rlh = bkReader.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes())) {
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                Assert.assertFalse(writeLh.isClosed());

                // without readUnconfirmedEntries we are not able to read all of the entries
                try {
                    rlh.readEntries(0, numOfEntries - 1);
                    fail("shoud not be able to read up to "+ (numOfEntries - 1) + " with readEntries");
                } catch (BKException.BKReadException expected) {
                }

                // read all entries within the 0..LastAddConfirmed range with readEntries
                assertEquals(rlh.getLastAddConfirmed() + 1,
                    Collections.list(rlh.readEntries(0, rlh.getLastAddConfirmed())).size());

                // assert local LAC does not change after reads
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                // read all entries within the 0..LastAddConfirmed range with readUnconfirmedEntries
                assertEquals(rlh.getLastAddConfirmed() + 1,
                    Collections.list(rlh.readUnconfirmedEntries(0, rlh.getLastAddConfirmed())).size());

                // assert local LAC does not change after reads
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                // read all entries within the LastAddConfirmed..numOfEntries - 1 range with readUnconfirmedEntries
                assertEquals(numOfEntries - rlh.getLastAddConfirmed(),
                    Collections.list(rlh.readUnconfirmedEntries(rlh.getLastAddConfirmed(), numOfEntries - 1)).size());

                // assert local LAC does not change after reads
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                try {
                    // read all entries within the LastAddConfirmed..numOfEntries range  with readUnconfirmedEntries
                    // this is an error, we are going outside the range of existing entries
                    rlh.readUnconfirmedEntries(rlh.getLastAddConfirmed(), numOfEntries);
                    fail("the read tried to access data for unexisting entry id "+numOfEntries);
                } catch (BKException.BKNoSuchEntryException expected) {
                    // expecting a BKNoSuchEntryException, as the entry does not exist on bookies
                }

                try {
                    // read all entries within the LastAddConfirmed..numOfEntries range with readEntries
                    // this is an error, we are going outside the range of existing entries
                    rlh.readEntries(rlh.getLastAddConfirmed(), numOfEntries);
                    fail("the read tries to access data for unexisting entry id "+numOfEntries);
                } catch (BKException.BKReadException expected) {
                    // expecting a BKReadException, as the client rejected the request to access entries
                    // after local LastAddConfirmed
                }

            }

            // ensure that after restarting every bookie entries are not lost
            // even entries after the LastAddConfirmed
            restartBookies();

            try (BookKeeper bkReader = new BookKeeper(clientConfiguration);
                LedgerHandle rlh = bkReader.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes())) {
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                Assert.assertFalse(writeLh.isClosed());

                // with readUnconfirmedEntries we are able to read all of the entries
                Enumeration<LedgerEntry> entries = rlh.readUnconfirmedEntries(0, numOfEntries - 1);
                int entryId = 0;
                while (entries.hasMoreElements()) {
                    LedgerEntry entry = entries.nextElement();
                    String entryString = new String(entry.getEntry());
                    Assert.assertTrue("Expected entry String: " + ("foobar" + entryId)
                        + " actual entry String: " + entryString,
                        entryString.equals("foobar" + entryId));
                    entryId++;
                }
            }

            try (BookKeeper bkReader = new BookKeeper(clientConfiguration);
                LedgerHandle rlh = bkReader.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes())) {
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                Assert.assertFalse(writeLh.isClosed());

                // without readUnconfirmedEntries we are not able to read all of the entries
                try {
                    rlh.readEntries(0, numOfEntries - 1);
                    fail("shoud not be able to read up to "+ (numOfEntries - 1) + " with readEntries");
                } catch (BKException.BKReadException expected) {
                }

                // read all entries within the 0..LastAddConfirmed range with readEntries
                assertEquals(rlh.getLastAddConfirmed() + 1,
                    Collections.list(rlh.readEntries(0, rlh.getLastAddConfirmed())).size());

                // assert local LAC does not change after reads
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                // read all entries within the 0..LastAddConfirmed range with readUnconfirmedEntries
                assertEquals(rlh.getLastAddConfirmed() + 1,
                    Collections.list(rlh.readUnconfirmedEntries(0, rlh.getLastAddConfirmed())).size());

                // assert local LAC does not change after reads
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                // read all entries within the LastAddConfirmed..numOfEntries - 1 range with readUnconfirmedEntries
                assertEquals(numOfEntries - rlh.getLastAddConfirmed(),
                    Collections.list(rlh.readUnconfirmedEntries(rlh.getLastAddConfirmed(), numOfEntries - 1)).size());

                // assert local LAC does not change after reads
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

                try {
                    // read all entries within the LastAddConfirmed..numOfEntries range  with readUnconfirmedEntries
                    // this is an error, we are going outside the range of existing entries
                    rlh.readUnconfirmedEntries(rlh.getLastAddConfirmed(), numOfEntries);
                    fail("the read tried to access data for unexisting entry id "+numOfEntries);
                } catch (BKException.BKNoSuchEntryException expected) {
                    // expecting a BKNoSuchEntryException, as the entry does not exist on bookies
                }

                try {
                    // read all entries within the LastAddConfirmed..numOfEntries range with readEntries
                    // this is an error, we are going outside the range of existing entries
                    rlh.readEntries(rlh.getLastAddConfirmed(), numOfEntries);
                    fail("the read tries to access data for unexisting entry id "+numOfEntries);
                } catch (BKException.BKReadException expected) {
                    // expecting a BKReadException, as the client rejected the request to access entries
                    // after local LastAddConfirmed
                }

            }

            // open ledger with fencing, this will repair the ledger and make the last entry readable
            try (BookKeeper bkReader = new BookKeeper(clientConfiguration);
                LedgerHandle rlh = bkReader.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                Assert.assertTrue(
                    "Expected LAC of rlh: " + (numOfEntries - 1) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                    (rlh.getLastAddConfirmed() == (numOfEntries - 1)));

                Assert.assertFalse(writeLh.isClosed());

                // without readUnconfirmedEntries we are not able to read all of the entries
                Enumeration<LedgerEntry> entries = rlh.readEntries(0, numOfEntries - 1);
                int entryId = 0;
                while (entries.hasMoreElements()) {
                    LedgerEntry entry = entries.nextElement();
                    String entryString = new String(entry.getEntry());
                    Assert.assertTrue("Expected entry String: " + ("foobar" + entryId)
                        + " actual entry String: " + entryString,
                        entryString.equals("foobar" + entryId));
                    entryId++;
                }
            }

            try {
                writeLh.close();
                fail("should not be able to close the first LedgerHandler as a recovery has been performed");
            } catch (BKException.BKMetadataVersionException expected) {
            }

        }
    }

    @Test
    public void testReadWriteWithV2WireProtocol() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setUseV2WireProtocol(true);
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        try (BookKeeper bkc = new BookKeeper(conf)) {

            // basic read/write
            {
                long ledgerId;
                try (LedgerHandle lh = bkc.createLedger(digestType, "testPasswd".getBytes())) {
                    ledgerId = lh.getId();
                    for (int i = 0; i < numEntries; i++) {
                        lh.addEntry(data);
                    }
                }
                try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                    assertEquals(numEntries - 1, lh.readLastConfirmed());
                    for (Enumeration<LedgerEntry> readEntries = lh.readEntries(0, numEntries - 1);
                        readEntries.hasMoreElements();) {
                        LedgerEntry entry = readEntries.nextElement();
                        assertArrayEquals(data, entry.getEntry());
                    }
                }
            }

            // basic fencing
            {
                long ledgerId;
                try (LedgerHandle lh2 = bkc.createLedger(digestType, "testPasswd".getBytes())) {
                    ledgerId = lh2.getId();
                    lh2.addEntry(data);
                    try (LedgerHandle lh2_fence = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                    }
                    try {
                        lh2.addEntry(data);
                        fail("ledger should be fenced");
                    } catch (BKException.BKLedgerFencedException ex){
                    }
                }
            }
        }
    }

    @Test
    public void testReadEntryReleaseByteBufs() throws Exception {
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        int numEntries = 10;
        byte[] data = "foobar".getBytes();
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedger(digestType, "testPasswd".getBytes())) {
                ledgerId = lh.getId();
                for (int i = 0; i < numEntries; i++) {
                    lh.addEntry(data);
                }
            }
        }

        // v2 protocol, using pooled buffers
        ClientConfiguration confReader1 = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setUseV2WireProtocol(true)
            .setNettyUsePooledBuffers(true);
        try (BookKeeper bkc = new BookKeeper(confReader1)) {
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                assertEquals(numEntries - 1, lh.readLastConfirmed());
                for (Enumeration<LedgerEntry> readEntries = lh.readEntries(0, numEntries - 1);
                    readEntries.hasMoreElements();) {
                    LedgerEntry entry = readEntries.nextElement();
                    try {
                        entry.data.release();
                    } catch (IllegalReferenceCountException ok) {
                        fail("ByteBuf already released");
                    }
                }
            }
        }

        // v2 protocol, not using pooled buffers
        ClientConfiguration confReader2 = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setUseV2WireProtocol(true)
            .setNettyUsePooledBuffers(false);
        try (BookKeeper bkc = new BookKeeper(confReader2)) {
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                assertEquals(numEntries - 1, lh.readLastConfirmed());
                for (Enumeration<LedgerEntry> readEntries = lh.readEntries(0, numEntries - 1);
                    readEntries.hasMoreElements();) {
                    LedgerEntry entry = readEntries.nextElement();
                    try {
                        entry.data.release();
                    } catch (IllegalReferenceCountException e) {
                        fail("ByteBuf already released");
                    }
                }
            }
        }

        // v3 protocol, not using pooled buffers
        ClientConfiguration confReader3 = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setUseV2WireProtocol(false)
            .setNettyUsePooledBuffers(false);
        try (BookKeeper bkc = new BookKeeper(confReader3)) {
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                assertEquals(numEntries - 1, lh.readLastConfirmed());
                for (Enumeration<LedgerEntry> readEntries = lh.readEntries(0, numEntries - 1);
                    readEntries.hasMoreElements();) {
                    LedgerEntry entry = readEntries.nextElement();
                    assertTrue("Can't release entry " + entry.getEntryId() + ": ref = " + entry.data.refCnt(),
                        entry.data.release());
                    try {
                        assertFalse(entry.data.release());
                        fail("ByteBuf already released");
                    } catch (IllegalReferenceCountException ok) {
                    }
                }
            }
        }

        // v3 protocol, using pooled buffers
        // v3 protocol from 4.5 always "wraps" buffers returned by protobuf
        ClientConfiguration confReader4 = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setUseV2WireProtocol(false)
            .setNettyUsePooledBuffers(true);
        try (BookKeeper bkc = new BookKeeper(confReader4)) {
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                assertEquals(numEntries - 1, lh.readLastConfirmed());
                for (Enumeration<LedgerEntry> readEntries = lh.readEntries(0, numEntries - 1);
                    readEntries.hasMoreElements();) {
                    LedgerEntry entry = readEntries.nextElement();
                    // ButeBufs not reference counter
                    assertTrue("Can't release entry " + entry.getEntryId() + ": ref = " + entry.data.refCnt(),
                        entry.data.release());
                    try {
                        assertFalse(entry.data.release());
                        fail("ByteBuf already released");
                    } catch (IllegalReferenceCountException ok) {
                    }
                }
            }
        }

        // cannot read twice an entry
        ClientConfiguration confReader5 = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        try (BookKeeper bkc = new BookKeeper(confReader5)) {
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                assertEquals(numEntries - 1, lh.readLastConfirmed());
                for (Enumeration<LedgerEntry> readEntries = lh.readEntries(0, numEntries - 1);
                    readEntries.hasMoreElements();) {
                    LedgerEntry entry = readEntries.nextElement();
                    entry.getEntry();
                    try {
                        entry.getEntry();
                        fail("entry data accessed twice");
                    } catch (IllegalStateException ok){
                    }
                    try {
                        entry.getEntryInputStream();
                        fail("entry data accessed twice");
                    } catch (IllegalStateException ok){
                    }
                }
            }
        }
    }
}
