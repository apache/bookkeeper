/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.util.concurrent.SettableFuture;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException.BKClientClosedException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test verifies the behavior of bookkeeper apis, where the operations
 * are being executed through a closed bookkeeper client.
 */
public class BookKeeperCloseTest extends BookKeeperClusterTestCase {

    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // static Logger LOG = Logger.getRootLogger();
    private static final Logger LOG = LoggerFactory
            .getLogger(BookKeeperCloseTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final BiConsumer<Long, Long> NOOP_BICONSUMER = (l, e) -> { };

    public BookKeeperCloseTest() {
        super(3);
    }

    private void restartBookieSlow() throws Exception{
        ServerConfiguration conf = killBookie(0);

        Bookie delayBookie = new BookieImpl(conf) {
                @Override
                public void recoveryAddEntry(ByteBuf entry, WriteCallback cb,
                                             Object ctx, byte[] masterKey)
                        throws IOException, BookieException, InterruptedException {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        // ignore, only interrupted if shutting down,
                        // and an exception would spam the logs
                        Thread.currentThread().interrupt();
                    }
                    super.recoveryAddEntry(entry, cb, ctx, masterKey);
                }

                @Override
                public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb,
                                     Object ctx, byte[] masterKey)
                        throws IOException, BookieException, InterruptedException {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        // ignore, only interrupted if shutting down,
                        // and an exception would spam the logs
                        Thread.currentThread().interrupt();
                    }
                    super.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);
                }

                @Override
                public ByteBuf readEntry(long ledgerId, long entryId)
                        throws IOException, NoLedgerException {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        // ignore, only interrupted if shutting down,
                        // and an exception would spam the logs
                        Thread.currentThread().interrupt();
                    }
                    return super.readEntry(ledgerId, entryId);
                }
            };
        bsConfs.add(conf);
        bs.add(startBookie(conf, delayBookie));
    }

    /**
     * Test that createledger using bookkeeper client which is closed should
     * throw ClientClosedException.
     */
    @Test
    public void testCreateLedger() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Closing bookkeeper client");
        bk.close();
        try {
            bk.createLedger(digestType, PASSWORD.getBytes());
            fail("should have failed, client is closed");
        } catch (BKClientClosedException e) {
            // correct
        }

        // using async, because this could trigger an assertion
        final AtomicInteger returnCode = new AtomicInteger(0);
        final CountDownLatch openLatch = new CountDownLatch(1);
        CreateCallback cb = new CreateCallback() {
            @Override
            public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                returnCode.set(rc);
                openLatch.countDown();
            }
        };
        bk.asyncCreateLedger(3, 2, digestType, PASSWORD.getBytes(), cb,
                             openLatch);

        LOG.info("Waiting to finish the ledger creation");
        // wait for creating the ledger
        assertTrue("create ledger call should have completed",
                openLatch.await(20, TimeUnit.SECONDS));
        assertEquals("Succesfully created ledger through closed bkclient!",
                BKException.Code.ClientClosedException, returnCode.get());
    }

    /**
     * Test that opening a ledger using bookkeeper client which is closed should
     * throw ClientClosedException.
     */
    @Test
    public void testFenceLedger() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        LedgerHandle lh = createLedgerWithEntries(bk, 100);
        LOG.info("Closing bookkeeper client");

        restartBookieSlow();

        bk.close();

        try {
            bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
            fail("should have failed, client is closed");
        } catch (BKClientClosedException e) {
            // correct
        }

        try {
            bk.openLedgerNoRecovery(lh.getId(), digestType, PASSWORD.getBytes());
            fail("should have failed, client is closed");
        } catch (BKClientClosedException e) {
            // correct
        }

        final AtomicInteger returnCode = new AtomicInteger(0);
        final CountDownLatch openLatch = new CountDownLatch(1);
        AsyncCallback.OpenCallback cb = new AsyncCallback.OpenCallback() {
            public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                returnCode.set(rc);
                openLatch.countDown();
            }
        };
        bk.asyncOpenLedger(lh.getId(), digestType, PASSWORD.getBytes(), cb,
                           openLatch);

        LOG.info("Waiting to open the ledger asynchronously");
        assertTrue("Open call should have completed",
                openLatch.await(20, TimeUnit.SECONDS));
        assertTrue("Open should not have succeeded through closed bkclient!",
                   BKException.Code.ClientClosedException == returnCode.get());
    }

    /**
     * Test that deleting a ledger using bookkeeper client which is closed
     * should throw ClientClosedException.
     */
    @Test
    public void testDeleteLedger() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        LedgerHandle lh = createLedgerWithEntries(bk, 100);
        LOG.info("Closing bookkeeper client");
        bk.close();
        try {
            bk.deleteLedger(lh.getId());
            fail("should have failed, client is closed");
        } catch (BKClientClosedException e) {
            // correct
        }

        // using async, because this could trigger an assertion
        final AtomicInteger returnCode = new AtomicInteger(0);
        final CountDownLatch openLatch = new CountDownLatch(1);
        AsyncCallback.DeleteCallback cb = new AsyncCallback.DeleteCallback() {
            public void deleteComplete(int rc, Object ctx) {
                returnCode.set(rc);
                openLatch.countDown();
            }
        };
        bk.asyncDeleteLedger(lh.getId(), cb, openLatch);

        LOG.info("Waiting to delete the ledger asynchronously");
        assertTrue("Delete call should have completed",
                openLatch.await(20, TimeUnit.SECONDS));
        assertEquals("Delete should not have succeeded through closed bkclient!",
                     BKException.Code.ClientClosedException, returnCode.get());
    }

    /**
     * Test that adding entry to a ledger using bookkeeper client which is
     * closed should throw ClientClosedException.
     */
    @Test
    public void testAddLedgerEntry() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        LedgerHandle lh = createLedgerWithEntries(bk, 1);
        LOG.info("Closing bookkeeper client");

        restartBookieSlow();

        bk.close();

        try {
            lh.addEntry("foobar".getBytes());
            fail("should have failed, client is closed");
        } catch (BKClientClosedException e) {
            // correct
        }

        final CountDownLatch completeLatch = new CountDownLatch(1);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        lh.asyncAddEntry("foobar".getBytes(), new AddCallback() {
                public void addComplete(int rccb, LedgerHandle lh, long entryId,
                                        Object ctx) {
                    rc.set(rccb);
                    completeLatch.countDown();
                }
            }, null);

        LOG.info("Waiting to finish adding another entry asynchronously");
        assertTrue("Add entry to ledger call should have completed",
                completeLatch.await(20, TimeUnit.SECONDS));
        assertEquals(
                "Add entry to ledger should not have succeeded through closed bkclient!",
                BKException.Code.ClientClosedException, rc.get());
    }

    /**
     * Test that closing a ledger using bookkeeper client which is closed should
     * throw ClientClosedException.
     */
    @Test
    public void testCloseLedger() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        LedgerHandle lh = createLedgerWithEntries(bk, 100);
        LedgerHandle lh2 = createLedgerWithEntries(bk, 100);

        LOG.info("Closing bookkeeper client");
        bk.close();

        try {
            lh.close();
            fail("should have failed, client is closed");
        } catch (BKClientClosedException e) {
            // correct
        }

        final CountDownLatch completeLatch = new CountDownLatch(1);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        lh2.asyncClose(new CloseCallback() {
                public void closeComplete(int rccb, LedgerHandle lh, Object ctx) {
                    rc.set(rccb);
                    completeLatch.countDown();
                }
            }, null);

        LOG.info("Waiting to finish adding another entry asynchronously");
        assertTrue("Close ledger call should have completed",
                completeLatch.await(20, TimeUnit.SECONDS));
        assertEquals(
                "Close ledger should have succeeded through closed bkclient!",
                BKException.Code.ClientClosedException, rc.get());
    }

    /**
     * Test that reading entry from a ledger using bookkeeper client which is
     * closed should throw ClientClosedException.
     */
    @Test
    public void testReadLedgerEntry() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        int numOfEntries = 100;
        LedgerHandle lh = createLedgerWithEntries(bk, numOfEntries);
        LOG.info("Closing bookkeeper client");

        restartBookieSlow();

        bk.close();

        try {
            lh.readEntries(0, numOfEntries - 1);
            fail("should have failed, client is closed");
        } catch (BKClientClosedException e) {
            // correct
        }

        final CountDownLatch readLatch = new CountDownLatch(1);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        ReadCallback cb = new ReadCallback() {
            @Override
            public void readComplete(int rccb, LedgerHandle lh,
                    Enumeration<LedgerEntry> seq, Object ctx) {
                rc.set(rccb);
                readLatch.countDown();
            }
        };
        lh.asyncReadEntries(0, numOfEntries - 1, cb, readLatch);

        LOG.info("Waiting to finish reading the entries asynchronously");
        assertTrue("Read entry ledger call should have completed",
                readLatch.await(20, TimeUnit.SECONDS));
        assertEquals(
                "Read entry ledger should have succeeded through closed bkclient!",
                BKException.Code.ClientClosedException, rc.get());
    }

    /**
     * Test that readlastconfirmed entry from a ledger using bookkeeper client
     * which is closed should throw ClientClosedException.
     */
    @Test
    public void testReadLastConfirmed() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        LedgerHandle lh = createLedgerWithEntries(bk, 100);
        LOG.info("Closing bookkeeper client");

        // make all bookies slow
        restartBookieSlow();
        restartBookieSlow();
        restartBookieSlow();

        bk.close();

        final CountDownLatch readLatch = new CountDownLatch(1);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        AsyncCallback.ReadLastConfirmedCallback cb = new AsyncCallback.ReadLastConfirmedCallback() {

            @Override
            public void readLastConfirmedComplete(int rccb, long lastConfirmed,
                    Object ctx) {
                rc.set(rccb);
                readLatch.countDown();
            }
        };
        lh.asyncReadLastConfirmed(cb, readLatch);

        LOG.info("Waiting to finish reading last confirmed entry asynchronously");
        assertTrue("ReadLastConfirmed call should have completed",
                readLatch.await(20, TimeUnit.SECONDS));
        assertEquals(
                "ReadLastConfirmed should have succeeded through closed bkclient!",
                BKException.Code.ClientClosedException, rc.get());

        try {
            lh.readLastConfirmed();
            fail("should have failed, client is closed");
        } catch (BKClientClosedException e) {
            // correct
        }
    }

    /**
     * Test that checking a ledger using a closed BK client will
     * throw a ClientClosedException.
     */
    @Test
    public void testLedgerCheck() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        LedgerHandle lh = createLedgerWithEntries(bk, 100);
        LOG.info("Closing bookkeeper client");
        LedgerChecker lc = new LedgerChecker(bk);

        restartBookieSlow();
        bk.close();

        final CountDownLatch postLatch = new CountDownLatch(1);
        final AtomicInteger postRc = new AtomicInteger(BKException.Code.OK);
        lc.checkLedger(lh, new GenericCallback<Set<LedgerFragment>>() {
                @Override
                public void operationComplete(int rc, Set<LedgerFragment> result) {
                    postRc.set(rc);
                    postLatch.countDown();
                }
            });
        assertTrue("checkLedger should have finished", postLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should have client closed exception",
                     postRc.get(), BKException.Code.ClientClosedException);
    }

    private static class CheckerCb implements GenericCallback<Set<LedgerFragment>> {
        CountDownLatch latch = new CountDownLatch(1);
        int rc = BKException.Code.OK;
        Set<LedgerFragment> result = null;

        @Override
        public void operationComplete(int rc, Set<LedgerFragment> result) {
            this.rc = rc;
            this.result = result;
            latch.countDown();
        }

        int getRc(int time, TimeUnit unit) throws Exception {
            if (latch.await(time, unit)) {
                return rc;
            } else {
                throw new Exception("Didn't complete");
            }
        }

        Set<LedgerFragment> getResult(int time, TimeUnit unit) throws Exception {
            if (latch.await(time, unit)) {
                return result;
            } else {
                throw new Exception("Didn't complete");
            }
        }
    }
    /**
     * Test that BookKeeperAdmin operationg using a closed BK client will
     * throw a ClientClosedException.
     */
    @Test
    public void testBookKeeperAdmin() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        try (BookKeeperAdmin bkadmin = new BookKeeperAdmin(bk)) {

            LOG.info("Create ledger and add entries to it");
            LedgerHandle lh1 = createLedgerWithEntries(bk, 100);
            LedgerHandle lh2 = createLedgerWithEntries(bk, 100);
            LedgerHandle lh3 = createLedgerWithEntries(bk, 100);
            lh3.close();

            BookieId bookieToKill = getBookie(0);
            killBookie(bookieToKill);
            startNewBookie();

            CheckerCb checkercb = new CheckerCb();
            LedgerChecker lc = new LedgerChecker(bk);
            lc.checkLedger(lh3, checkercb);
            assertEquals("Should have completed",
                         checkercb.getRc(30, TimeUnit.SECONDS), BKException.Code.OK);
            assertEquals("Should have a missing fragment",
                         1, checkercb.getResult(30, TimeUnit.SECONDS).size());

            // make sure a bookie in each quorum is slow
            restartBookieSlow();
            restartBookieSlow();

            bk.close();

            try {
                bkadmin.openLedger(lh1.getId());
                fail("Shouldn't be able to open with a closed client");
            } catch (BKException.BKClientClosedException cce) {
                // correct behaviour
            }

            try {
                bkadmin.openLedgerNoRecovery(lh1.getId());
                fail("Shouldn't be able to open with a closed client");
            } catch (BKException.BKClientClosedException cce) {
                // correct behaviour
            }

            try {
                bkadmin.recoverBookieData(bookieToKill);
                fail("Shouldn't be able to recover with a closed client");
            } catch (BKException.BKClientClosedException cce) {
                // correct behaviour
            }

            try {
                bkadmin.replicateLedgerFragment(lh3,
                        checkercb.getResult(10, TimeUnit.SECONDS).iterator().next(), NOOP_BICONSUMER);
                fail("Shouldn't be able to replicate with a closed client");
            } catch (BKException.BKClientClosedException cce) {
                // correct behaviour
            }
        }
    }

    /**
     * Test that the bookkeeper client doesn't leave any threads hanging around.
     * See {@link https://issues.apache.org/jira/browse/BOOKKEEPER-804}
     */
    @Test
    public void testBookKeeperCloseThreads() throws Exception {
        ThreadGroup group = new ThreadGroup("test-group");
        final SettableFuture<Void> future = SettableFuture.<Void>create();

        Thread t = new Thread(group, "TestThread") {
                @Override
                public void run() {
                    try {
                        BookKeeper bk = new BookKeeper(baseClientConf);
                        // 9 is a ledger id of an existing ledger
                        LedgerHandle lh = bk.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());
                        lh.addEntry("foobar".getBytes());
                        lh.close();
                        long id = lh.getId();
                        // 9 is a ledger id of an existing ledger
                        lh = bk.openLedgerNoRecovery(id, BookKeeper.DigestType.CRC32, "passwd".getBytes());
                        Enumeration<LedgerEntry> entries = lh.readEntries(0, 0);

                        lh.close();
                        bk.close();
                        future.set(null);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        future.setException(ie);
                    } catch (Exception e) {
                        future.setException(e);
                    }
                }
            };
        t.start();

        future.get();
        t.join();

        // check in a loop for 10 seconds
        // because sometimes it takes a while to threads to go away
        for (int i = 0; i < 10; i++) {
            if (group.activeCount() > 0) {
                Thread[] threads = new Thread[group.activeCount()];
                group.enumerate(threads);
                for (Thread leftover : threads) {
                    LOG.error("Leftover thread after {} secs: {}", i, leftover);
                }
                Thread.sleep(1000);
            } else {
                break;
            }
        }
        assertEquals("Should be no threads left in group", 0, group.activeCount());
    }

    private LedgerHandle createLedgerWithEntries(BookKeeper bk, int numOfEntries)
            throws Exception {
        LedgerHandle lh = bk
                .createLedger(3, 3, digestType, PASSWORD.getBytes());

        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        final CountDownLatch latch = new CountDownLatch(numOfEntries);

        final AddCallback cb = new AddCallback() {
                public void addComplete(int rccb, LedgerHandle lh, long entryId,
                                        Object ctx) {
                    rc.compareAndSet(BKException.Code.OK, rccb);
                    latch.countDown();
                }
            };
        for (int i = 0; i < numOfEntries; i++) {
            lh.asyncAddEntry("foobar".getBytes(), cb, null);
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new Exception("Entries took too long to add");
        }
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }
        return lh;
    }

}
