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

import org.junit.*;
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BaseTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger fencing;
 *
 */
public class TestFencing extends BaseTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestFencing.class);

    DigestType digestType;

    public TestFencing(DigestType digestType) {
        super(10);
        this.digestType = digestType;
    }

    /**
     * Basic fencing test. Create ledger, write to it,
     * open ledger, write again (should fail).
     */
    @Test(timeout=60000)
    public void testBasicFencing() throws Exception {
        /*
         * Create ledger.
         */
        LedgerHandle writelh = null;
        writelh = bkc.createLedger(digestType, "password".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < 10; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        /*
         * Try to open ledger.
         */
        LedgerHandle readlh = bkc.openLedger(writelh.getId(), digestType, "password".getBytes());
        // should have triggered recovery and fencing
        
        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
        }
                
        /*
         * Check if has recovered properly.
         */
        assertTrue("Has not recovered correctly: " + readlh.getLastAddConfirmed() 
                   + " original " + writelh.getLastAddConfirmed(),
                   readlh.getLastAddConfirmed() == writelh.getLastAddConfirmed());
    }

    private static int threadCount = 0;

    class LedgerOpenThread extends Thread {
        private final long ledgerId;
        private long lastConfirmedEntry = 0;

        
        private final DigestType digestType;
        private final CyclicBarrier barrier;

        LedgerOpenThread (DigestType digestType, long ledgerId, CyclicBarrier barrier)
                throws Exception {
            super("TestFencing-LedgerOpenThread-" + threadCount++);
            this.ledgerId = ledgerId;
            this.digestType = digestType;
            this.barrier = barrier;
        }
        
        @Override
        public void run() {
            LedgerHandle lh = null;
            BookKeeper bk = null;
            try {
                barrier.await();
                while(true) {
                    try {
                        bk = new BookKeeper(new ClientConfiguration(baseClientConf), bkc.getZkHandle());
                        
                        lh = bk.openLedger(ledgerId, 
                                           digestType, "".getBytes());
                        lastConfirmedEntry = lh.getLastAddConfirmed();
                        lh.close();
                        break;
                    } catch (BKException.BKMetadataVersionException zke) {
                        LOG.info("Contention with someone else recovering");
                    } catch (BKException.BKLedgerRecoveryException bkre) {
                        LOG.info("Contention with someone else recovering");
                    } finally {
                        if (lh != null) {
                            lh.close();
                        }
                        if (bk != null) {
                            bk.close();
                            bk = null;
                        }
                    }
                }
            } catch (Exception e) {
                // just exit, test should spot bad last add confirmed
                LOG.error("Exception occurred ", e);
            }
            LOG.info("Thread exiting, lastConfirmedEntry = " + lastConfirmedEntry);
        }

        long getLastConfirmedEntry() {
            return lastConfirmedEntry;
        }
    }

    /**
     * Try to open a ledger many times in parallel.
     * All opens should result in a ledger with an equals number of 
     * entries.
     */
    @Test(timeout=60000)
    public void testManyOpenParallel() throws Exception {
        /*
         * Create ledger.
         */
        final LedgerHandle writelh = bkc.createLedger(digestType, "".getBytes());

        final int numRecovery = 10;

        final String tmp = "BookKeeper is cool!";
        final CountDownLatch latch = new CountDownLatch(numRecovery);
        Thread writethread = new Thread() {
                public void run() {
                    try {
                        while (true) {
                            writelh.addEntry(tmp.getBytes());
                            latch.countDown();
                        }
                    } catch (Exception e) {
                        LOG.info("Exception adding entry", e);
                    }
                }
            };
        writethread.start();


        CyclicBarrier barrier = new CyclicBarrier(numRecovery+1);
        LedgerOpenThread threads[] = new LedgerOpenThread[numRecovery];
        for (int i = 0; i < numRecovery; i++) {
            threads[i] = new LedgerOpenThread(digestType, writelh.getId(), barrier);
            threads[i].start();
        }
        latch.await();
        barrier.await(); // should trigger threads to go

        writethread.join();
        long lastConfirmed = writelh.getLastAddConfirmed();
        
        for (int i = 0; i < numRecovery; i++) {
            threads[i].join();
            assertTrue("Added confirmed is incorrect", 
                       lastConfirmed <= threads[i].getLastConfirmedEntry());
        }
    }

    /**
     * Test that opening a ledger in norecovery mode
     * doesn't fence off a ledger
     */
    @Test(timeout=60000)
    public void testNoRecoveryOpen() throws Exception {
        /*
         * Create ledger.
         */
        LedgerHandle writelh = null;
        writelh = bkc.createLedger(digestType, "".getBytes());

        String tmp = "BookKeeper is cool!";
        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        /*
         * Try to open ledger.
         */
        LedgerHandle readlh = bkc.openLedgerNoRecovery(writelh.getId(), 
                                                        digestType, "".getBytes());
        // should not have triggered recovery and fencing
        
        writelh.addEntry(tmp.getBytes());
        long numReadable = readlh.getLastAddConfirmed();
        LOG.error("numRead " + numReadable);
        Enumeration<LedgerEntry> entries = readlh.readEntries(1, numReadable);
        try {
            readlh.readEntries(numReadable+1, numReadable+1);
            fail("Shouldn't have been able to read this far");
        } catch (BKException.BKReadException e) {
            // all is good
        }

        writelh.addEntry(tmp.getBytes());
        long numReadable2 = readlh.getLastAddConfirmed();
        assertEquals("Number of readable entries hasn't changed", numReadable2, numReadable);
        readlh.close();

        writelh.addEntry(tmp.getBytes());
        writelh.close();
    }

    /**
     * create a ledger and write entries. 
     * kill a bookie in the ensemble. Recover.
     * Fence the ledger. Kill another bookie. Recover.
     */
    @Test(timeout=60000)
    public void testFencingInteractionWithBookieRecovery() throws Exception {
        System.setProperty("digestType", digestType.toString());
        System.setProperty("passwd", "testPasswd");

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());

        LedgerHandle writelh = bkc.createLedger(digestType, "testPasswd".getBytes());
        
        String tmp = "Foobar";
        
        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        InetSocketAddress bookieToKill 
            = writelh.getLedgerMetadata().getEnsemble(numEntries).get(0);
        killBookie(bookieToKill);

        // write entries to change ensemble
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        admin.recoverBookieData(bookieToKill, null);
        
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        LedgerHandle readlh = bkc.openLedger(writelh.getId(), 
                                             digestType, "testPasswd".getBytes());
        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
        }

        readlh.close();
        writelh.close();
    }

    /**
     * create a ledger and write entries. 
     * Fence the ledger. Kill a bookie. Recover.
     * Ensure that recover doesn't reallow adding
     */
    @Test(timeout=60000)
    public void testFencingInteractionWithBookieRecovery2() throws Exception {
        System.setProperty("digestType", digestType.toString());
        System.setProperty("passwd", "testPasswd");

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());

        LedgerHandle writelh = bkc.createLedger(digestType, "testPasswd".getBytes());
        
        String tmp = "Foobar";
        
        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        LedgerHandle readlh = bkc.openLedger(writelh.getId(), 
                                             digestType, "testPasswd".getBytes());
        // should be fenced by now
        InetSocketAddress bookieToKill 
            = writelh.getLedgerMetadata().getEnsemble(numEntries).get(0);
        killBookie(bookieToKill);
        admin.recoverBookieData(bookieToKill, null);

        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
        }

        readlh.close();
        writelh.close();
    }

    /**
     * Test that fencing doesn't work with a bad password
     */
    @Test(timeout=60000)
    public void testFencingBadPassword() throws Exception {
        /*
         * Create ledger.
         */
        LedgerHandle writelh = null;
        writelh = bkc.createLedger(digestType, "password1".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < 10; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        /*
         * Try to open ledger.
         */
        try {
            LedgerHandle readlh = bkc.openLedger(writelh.getId(), digestType, "badPassword".getBytes());
            fail("Should not have been able to open with a bad password");
        } catch (BKException.BKUnauthorizedAccessException uue) {
            // correct behaviour
        }
        // should have triggered recovery and fencing

        writelh.addEntry(tmp.getBytes());
    }

    @Test
    public void testFencingAndRestartBookies() throws Exception {
        LedgerHandle writelh = null;
        writelh = bkc.createLedger(digestType, "password".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < 10; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        /*
         * Try to open ledger.
         */
        LedgerHandle readlh = bkc.openLedger(writelh.getId(), digestType,
                                             "password".getBytes());

        restartBookies();

        try {
            writelh.addEntry(tmp.getBytes());
            LOG.error("Should have thrown an exception");
            fail("Should have thrown an exception when trying to write");
        } catch (BKException.BKLedgerFencedException e) {
            // correct behaviour
        }

        readlh.close();
    }
}
