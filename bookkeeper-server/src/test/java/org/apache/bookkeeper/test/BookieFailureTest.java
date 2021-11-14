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
package org.apache.bookkeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Random;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookieServer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test tests read and write, synchronous and asynchronous, strings and
 * integers for a BookKeeper client. The test deployment uses a ZooKeeper server
 * and three BookKeepers.
 *
 */

public class BookieFailureTest extends BookKeeperClusterTestCase
    implements AddCallback, ReadCallback {

    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // private static final Logger LOG = Logger.getRootLogger();
    private static final Logger LOG = LoggerFactory.getLogger(BookieFailureTest.class);

    byte[] ledgerPassword = "aaa".getBytes();
    LedgerHandle lh, lh2;
    long ledgerId;

    // test related variables
    int numEntriesToWrite = 200;
    int maxInt = 2147483647;
    Random rng; // Random Number Generator
    ArrayList<byte[]> entries; // generated entries
    ArrayList<Integer> entriesSize;
    private final DigestType digestType;

    class SyncObj {
        int counter;
        boolean value;
        boolean failureOccurred;
        Enumeration<LedgerEntry> ls;

        public SyncObj() {
            counter = 0;
            value = false;
            failureOccurred = false;
            ls = null;
        }
    }

    public BookieFailureTest() {
        super(4);
        this.digestType = DigestType.CRC32;
        String ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    /**
     * Tests writes and reads when a bookie fails.
     *
     * @throws IOException
     */
    @Test
    public void testAsyncBK1() throws Exception {
        LOG.info("#### BK1 ####");
        auxTestReadWriteAsyncSingleClient(serverByIndex(0));
    }

    @Test
    public void testAsyncBK2() throws Exception {
        LOG.info("#### BK2 ####");
        auxTestReadWriteAsyncSingleClient(serverByIndex(1));
    }

    @Test
    public void testAsyncBK3() throws Exception {
        LOG.info("#### BK3 ####");
        auxTestReadWriteAsyncSingleClient(serverByIndex(2));
    }

    @Test
    public void testAsyncBK4() throws Exception {
        LOG.info("#### BK4 ####");
        auxTestReadWriteAsyncSingleClient(serverByIndex(3));
    }

    @Test
    public void testBookieRecovery() throws Exception {
        //Shutdown all but 1 bookie (should be in it's own test case with 1 bookie)
        assertEquals(4, bookieCount());
        killBookie(0);
        killBookie(0);
        killBookie(0);

        byte[] passwd = "blah".getBytes();
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, passwd);

        int numEntries = 100;
        for (int i = 0; i < numEntries; i++) {
            byte[] data = ("" + i).getBytes();
            lh.addEntry(data);
        }

        assertEquals(1, bookieCount());
        restartBookies();

        assertEquals(numEntries - 1 , lh.getLastAddConfirmed());
        Enumeration<LedgerEntry> entries = lh.readEntries(0, lh.getLastAddConfirmed());

        int numScanned = 0;
        while (entries.hasMoreElements()) {
            assertEquals(("" + numScanned), new String(entries.nextElement().getEntry()));
            numScanned++;
        }
        assertEquals(numEntries, numScanned);
    }

    void auxTestReadWriteAsyncSingleClient(BookieServer bs) throws IOException {
        SyncObj sync = new SyncObj();
        try {
            // Create a ledger
            lh = bkc.createLedger(3, 2, digestType, ledgerPassword);

            ledgerId = lh.getId();
            LOG.info("Ledger ID: " + lh.getId());
            for (int i = 0; i < numEntriesToWrite; i++) {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(rng.nextInt(maxInt));
                entry.position(0);

                entries.add(entry.array());
                entriesSize.add(entry.array().length);
                lh.asyncAddEntry(entry.array(), this, sync);

            }

            LOG.info("Wrote " + numEntriesToWrite + " and now going to fail bookie.");
            // Bookie fail
            bs.shutdown();

            // wait for all entries to be acknowledged
            synchronized (sync) {
                while (sync.counter < numEntriesToWrite) {
                    LOG.debug("Entries counter = " + sync.counter);
                    sync.wait(10000);
                    assertFalse("Failure occurred during write", sync.failureOccurred);
                }
            }

            LOG.debug("*** WRITE COMPLETE ***");
            // close ledger
            lh.close();

            // *** WRITING PART COMPLETE // READ PART BEGINS ***

            // open ledger
            bkc.close();
            bkc = new BookKeeperTestClient(baseClientConf);
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            LOG.debug("Number of entries written: " + (lh.getLastAddConfirmed() + 1));
            assertTrue("Verifying number of entries written", lh.getLastAddConfirmed() == (numEntriesToWrite - 1));

            // read entries

            lh.asyncReadEntries(0, numEntriesToWrite - 1, this, sync);

            synchronized (sync) {
                int i = 0;
                sync.wait(10000);
                assertFalse("Failure occurred during read", sync.failureOccurred);
                assertTrue("Haven't received entries", sync.value);
            }

            LOG.debug("*** READ COMPLETE ***");

            // at this point, Enumeration<LedgerEntry> ls is filled with the returned
            // values
            int i = 0;
            while (sync.ls.hasMoreElements()) {
                ByteBuffer origbb = ByteBuffer.wrap(entries.get(i));
                Integer origEntry = origbb.getInt();
                byte[] entry = sync.ls.nextElement().getEntry();
                ByteBuffer result = ByteBuffer.wrap(entry);

                Integer retrEntry = result.getInt();
                LOG.debug("Retrieved entry: " + i);
                assertTrue("Checking entry " + i + " for equality", origEntry.equals(retrEntry));
                assertTrue("Checking entry " + i + " for size", entry.length == entriesSize.get(i));
                i++;
            }

            assertTrue("Checking number of read entries", i == numEntriesToWrite);

            LOG.info("Verified that entries are ok, and now closing ledger");
            lh.close();
        } catch (BKException e) {
            LOG.error("Caught BKException", e);
            fail(e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Caught InterruptedException", e);
            fail(e.toString());
        }

    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        SyncObj x = (SyncObj) ctx;
        if (rc != 0) {
            LOG.error("Failure during add {} {}", entryId, rc);
            x.failureOccurred = true;
        }
        synchronized (x) {
            x.counter++;
            x.notify();
        }
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
        SyncObj x = (SyncObj) ctx;
        if (rc != 0) {
            LOG.error("Failure during add {}", rc);
            x.failureOccurred = true;
        }
        synchronized (x) {
            x.value = true;
            x.ls = seq;
            x.notify();
        }
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        rng = new Random(System.currentTimeMillis()); // Initialize the Random
        // Number Generator
        entries = new ArrayList<byte[]>(); // initialize the entries list
        entriesSize = new ArrayList<Integer>();

        zkc.close();
    }

    @Test
    public void testLedgerNoRecoveryOpenAfterBKCrashed() throws Exception {
        // Create a ledger
        LedgerHandle beforelh = bkc.createLedger(numBookies, numBookies, digestType, "".getBytes());

        int numEntries = 10;
        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            beforelh.addEntry(tmp.getBytes());
        }

        // shutdown first bookie server
        killBookie(0);

        // try to open ledger no recovery
        LedgerHandle afterlh = bkc.openLedgerNoRecovery(beforelh.getId(), digestType, "".getBytes());

        assertEquals(numEntries - 2, afterlh.getLastAddConfirmed());
    }

    @Test
    public void testLedgerOpenAfterBKCrashed() throws Exception {
        // Create a ledger
        LedgerHandle beforelh = bkc.createLedger(numBookies, numBookies, digestType, "".getBytes());

        int numEntries = 10;
        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            beforelh.addEntry(tmp.getBytes());
        }

        // shutdown first bookie server
        killBookie(0);
        startNewBookie();

        // try to open ledger with recovery
        LedgerHandle afterlh = bkc.openLedger(beforelh.getId(), digestType, "".getBytes());
        assertEquals(beforelh.getLastAddPushed(), afterlh.getLastAddConfirmed());

        // try to open ledger no recovery
        // bookies: 4, ensSize: 3, ackQuorumSize: 2
        LedgerHandle beforelhWithNoRecovery = bkc.createLedger(numBookies - 1 , 2, digestType, "".getBytes());
        for (int i = 0; i < numEntries; i++) {
            beforelhWithNoRecovery.addEntry(tmp.getBytes());
        }

        // shutdown first bookie server
        killBookie(0);

        // try to open ledger no recovery, should be able to open ledger
        bkc.openLedger(beforelhWithNoRecovery.getId(), digestType, "".getBytes());
    }

    /**
     * Verify read last confirmed op, it shouldn't cause any deadlock as new
     * bookie connection is being established and returning the connection
     * notification in the same caller thread. It would be simulated by delaying
     * the future.addlistener() in PerChannelBookieClient after the connection
     * establishment. Now the future.addlistener() will notify back in the same
     * thread and simultaneously invoke the pendingOp.operationComplete() event.
     *
     * <p>BOOKKEEPER-326
     */
    @Test
    public void testReadLastConfirmedOp() throws Exception {
        startNewBookie();
        startNewBookie();
        // Create a ledger
        LedgerHandle beforelh = bkc.createLedger(numBookies + 2,
                numBookies + 2, digestType, "".getBytes());

        int numEntries = 10;
        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            beforelh.addEntry(tmp.getBytes());
        }

        // shutdown first bookie server
        killBookie(0);
        startNewBookie();

        // create new bookie client, and forcing to establish new
        // PerChannelBookieClient connections for recovery flows.
        BookKeeperTestClient bkc1 = new BookKeeperTestClient(baseClientConf);
        // try to open ledger with recovery
        LedgerHandle afterlh = bkc1.openLedger(beforelh.getId(), digestType, ""
                .getBytes());

        assertEquals("Entries got missed", beforelh.getLastAddPushed(), afterlh
                .getLastAddConfirmed());
        bkc1.close();
    }
}
