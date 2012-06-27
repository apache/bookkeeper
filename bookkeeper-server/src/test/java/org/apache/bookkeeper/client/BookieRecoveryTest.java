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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.test.MultiLedgerManagerMultiDigestTestCase;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the bookie recovery admin functionality.
 */
public class BookieRecoveryTest extends MultiLedgerManagerMultiDigestTestCase {
    static Logger LOG = LoggerFactory.getLogger(BookieRecoveryTest.class);

    // Object used for synchronizing async method calls
    class SyncObject {
        boolean value;

        public SyncObject() {
            value = false;
        }
    }

    // Object used for implementing the Bookie RecoverCallback for this jUnit
    // test. This verifies that the operation completed successfully.
    class BookieRecoverCallback implements RecoverCallback {
        boolean success = false;
        @Override
        public void recoverComplete(int rc, Object ctx) {
            LOG.info("Recovered bookie operation completed with rc: " + rc);
            success = rc == BKException.Code.OK;
            SyncObject sync = (SyncObject) ctx;
            synchronized (sync) {
                sync.value = true;
                sync.notify();
            }
        }
    }

    // Objects to use for this jUnit test.
    DigestType digestType;
    SyncObject sync;
    BookieRecoverCallback bookieRecoverCb;
    BookKeeperAdmin bkAdmin;

    // Constructor
    public BookieRecoveryTest(String ledgerManagerFactory, DigestType digestType) {
        super(3);
        this.digestType = digestType;
        LOG.info("Using ledger manager " + ledgerManagerFactory);
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseClientConf.setBookieRecoveryDigestType(digestType);
        baseClientConf.setBookieRecoveryPasswd("".getBytes());
        super.setUp();

        sync = new SyncObject();
        bookieRecoverCb = new BookieRecoverCallback();
        ClientConfiguration adminConf = new ClientConfiguration(baseClientConf);
        adminConf.setZkServers(zkUtil.getZooKeeperConnectString());
        bkAdmin = new BookKeeperAdmin(adminConf);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Release any resources used by the BookKeeperTools instance.
        bkAdmin.close();
        super.tearDown();
    }

    /**
     * Helper method to create a number of ledgers
     *
     * @param numLedgers
     *            Number of ledgers to create
     * @return List of LedgerHandles for each of the ledgers created
     */
    private List<LedgerHandle> createLedgers(int numLedgers)
            throws BKException, IOException, InterruptedException
    {
        return createLedgers(numLedgers, 3, 2);
    }

    /**
     * Helper method to create a number of ledgers
     *
     * @param numLedgers
     *            Number of ledgers to create
     * @param ensemble Ensemble size for ledgers
     * @param quorum Quorum size for ledgers
     * @return List of LedgerHandles for each of the ledgers created
     */
    private List<LedgerHandle> createLedgers(int numLedgers, int ensemble, int quorum)
            throws BKException, IOException,
        InterruptedException {
        List<LedgerHandle> lhs = new ArrayList<LedgerHandle>();
        for (int i = 0; i < numLedgers; i++) {
            lhs.add(bkc.createLedger(ensemble, quorum,
                                     digestType, baseClientConf.getBookieRecoveryPasswd()));
        }
        return lhs;
    }

    private List<LedgerHandle> openLedgers(List<LedgerHandle> oldLhs)
            throws Exception {
        List<LedgerHandle> newLhs = new ArrayList<LedgerHandle>();
        for (LedgerHandle oldLh : oldLhs) {
            newLhs.add(bkc.openLedger(oldLh.getId(), digestType, baseClientConf.getBookieRecoveryPasswd()));
        }
        return newLhs;
    }

    /**
     * Helper method to write dummy ledger entries to all of the ledgers passed.
     *
     * @param numEntries
     *            Number of ledger entries to write for each ledger
     * @param startEntryId
     *            The first entry Id we're expecting to write for each ledger
     * @param lhs
     *            List of LedgerHandles for all ledgers to write entries to
     * @throws BKException
     * @throws InterruptedException
     */
    private void writeEntriestoLedgers(int numEntries, long startEntryId,
                                       List<LedgerHandle> lhs)
        throws BKException, InterruptedException {
        for (LedgerHandle lh : lhs) {
            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(("LedgerId: " + lh.getId() + ", EntryId: " + (startEntryId + i)).getBytes());
            }
        }
    }

    private void closeLedgers(List<LedgerHandle> lhs) throws BKException, InterruptedException {
        for (LedgerHandle lh : lhs) {
            lh.close();
        }
    }

    /**
     * Helper method to verify that we can read the recovered ledger entries.
     *
     * @param oldLhs
     *            Old Ledger Handles
     * @param startEntryId
     *            Start Entry Id to read
     * @param endEntryId
     *            End Entry Id to read
     * @throws BKException
     * @throws InterruptedException
     */
    private void verifyRecoveredLedgers(List<LedgerHandle> oldLhs, long startEntryId, long endEntryId) throws BKException,
        InterruptedException {
        // Get a set of LedgerHandles for all of the ledgers to verify
        List<LedgerHandle> lhs = new ArrayList<LedgerHandle>();
        for (int i = 0; i < oldLhs.size(); i++) {
            lhs.add(bkc.openLedger(oldLhs.get(i).getId(), digestType, baseClientConf.getBookieRecoveryPasswd()));
        }
        // Read the ledger entries to verify that they are all present and
        // correct in the new bookie.
        for (LedgerHandle lh : lhs) {
            Enumeration<LedgerEntry> entries = lh.readEntries(startEntryId, endEntryId);
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                assertTrue(new String(entry.getEntry()).equals("LedgerId: " + entry.getLedgerId() + ", EntryId: "
                           + entry.getEntryId()));
            }
        }

    }

    /**
     * This tests the asynchronous bookie recovery functionality by writing
     * entries into 3 bookies, killing one bookie, starting up a new one to
     * replace it, and then recovering the ledger entries from the killed bookie
     * onto the new one. We'll verify that the entries stored on the killed
     * bookie are properly copied over and restored onto the new one.
     *
     * @throws Exception
     */
    @Test
    public void testAsyncBookieRecoveryToSpecificBookie() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        bs.get(0).shutdown();
        bs.remove(0);

        // Startup a new bookie server
        int newBookiePort = startNewBookie();

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the async recover bookie method.
        InetSocketAddress bookieSrc = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), initialPort);
        InetSocketAddress bookieDest = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), newBookiePort);
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc + ") and replicate it to the new one ("
                 + bookieDest + ")");
        // Initiate the sync object
        sync.value = false;
        bkAdmin.asyncRecoverBookieData(bookieSrc, bookieDest, bookieRecoverCb, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (sync.value == false) {
                sync.wait();
            }
            assertTrue(bookieRecoverCb.success);
        }

        // Verify the recovered ledger entries are okay.
        verifyRecoveredLedgers(lhs, 0, 2 * numMsgs - 1);
    }

    /**
     * This tests the asynchronous bookie recovery functionality by writing
     * entries into 3 bookies, killing one bookie, starting up a few new
     * bookies, and then recovering the ledger entries from the killed bookie
     * onto random available bookie servers. We'll verify that the entries
     * stored on the killed bookie are properly copied over and restored onto
     * the other bookies.
     *
     * @throws Exception
     */
    @Test
    public void testAsyncBookieRecoveryToRandomBookies() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        bs.get(0).shutdown();
        bs.remove(0);

        // Startup three new bookie servers
        for (int i = 0; i < 3; i++) {
            startNewBookie();
        }

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the async recover bookie method.
        InetSocketAddress bookieSrc = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), initialPort);
        InetSocketAddress bookieDest = null;
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
                 + ") and replicate it to a random available one");
        // Initiate the sync object
        sync.value = false;
        bkAdmin.asyncRecoverBookieData(bookieSrc, bookieDest, bookieRecoverCb, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (sync.value == false) {
                sync.wait();
            }
            assertTrue(bookieRecoverCb.success);
        }

        // Verify the recovered ledger entries are okay.
        verifyRecoveredLedgers(lhs, 0, 2 * numMsgs - 1);
    }

    /**
     * This tests the synchronous bookie recovery functionality by writing
     * entries into 3 bookies, killing one bookie, starting up a new one to
     * replace it, and then recovering the ledger entries from the killed bookie
     * onto the new one. We'll verify that the entries stored on the killed
     * bookie are properly copied over and restored onto the new one.
     *
     * @throws Exception
     */
    @Test
    public void testSyncBookieRecoveryToSpecificBookie() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        bs.get(0).shutdown();
        bs.remove(0);

        // Startup a new bookie server
        int newBookiePort = startNewBookie();

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the sync recover bookie method.
        InetSocketAddress bookieSrc = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), initialPort);
        InetSocketAddress bookieDest = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), newBookiePort);
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc + ") and replicate it to the new one ("
                 + bookieDest + ")");
        bkAdmin.recoverBookieData(bookieSrc, bookieDest);

        // Verify the recovered ledger entries are okay.
        verifyRecoveredLedgers(lhs, 0, 2 * numMsgs - 1);
    }

    /**
     * This tests the synchronous bookie recovery functionality by writing
     * entries into 3 bookies, killing one bookie, starting up a few new
     * bookies, and then recovering the ledger entries from the killed bookie
     * onto random available bookie servers. We'll verify that the entries
     * stored on the killed bookie are properly copied over and restored onto
     * the other bookies.
     *
     * @throws Exception
     */
    @Test
    public void testSyncBookieRecoveryToRandomBookies() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        bs.get(0).shutdown();
        bs.remove(0);

        // Startup three new bookie servers
        for (int i = 0; i < 3; i++) {
            startNewBookie();
        }

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the sync recover bookie method.
        InetSocketAddress bookieSrc = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), initialPort);
        InetSocketAddress bookieDest = null;
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
                 + ") and replicate it to a random available one");
        bkAdmin.recoverBookieData(bookieSrc, bookieDest);

        // Verify the recovered ledger entries are okay.
        verifyRecoveredLedgers(lhs, 0, 2 * numMsgs - 1);
    }

    private static class ReplicationVerificationCallback implements ReadEntryCallback {
        final CountDownLatch latch;
        final AtomicLong numSuccess;

        ReplicationVerificationCallback(int numRequests) {
            latch = new CountDownLatch(numRequests);
            numSuccess = new AtomicLong(0);
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
            if (LOG.isDebugEnabled()) {
                InetSocketAddress addr = (InetSocketAddress)ctx;
                LOG.debug("Got " + rc + " for ledger " + ledgerId + " entry " + entryId + " from " + ctx);
            }
            if (rc == BKException.Code.OK) {
                numSuccess.incrementAndGet();
            }
            latch.countDown();
        }

        long await() throws InterruptedException {
            if (latch.await(60, TimeUnit.SECONDS) == false) {
                LOG.warn("Didn't get all responses in verification");
                return 0;
            } else {
                return numSuccess.get();
            }
        }
    }

    private boolean verifyFullyReplicated(LedgerHandle lh, long untilEntry) throws Exception {
        LedgerMetadata md = getLedgerMetadata(lh);

        Map<Long, ArrayList<InetSocketAddress>> ensembles = md.getEnsembles();

        HashMap<Long, Long> ranges = new HashMap<Long, Long>();
        ArrayList<Long> keyList = Collections.list(
                Collections.enumeration(ensembles.keySet()));
        Collections.sort(keyList);
        for (int i = 0; i < keyList.size() - 1; i++) {
            ranges.put(keyList.get(i), keyList.get(i+1));
        }
        ranges.put(keyList.get(keyList.size()-1), untilEntry);

        for (Map.Entry<Long, ArrayList<InetSocketAddress>> e : ensembles.entrySet()) {
            int quorum = md.getQuorumSize();
            long startEntryId = e.getKey();
            long endEntryId = ranges.get(startEntryId);
            long expectedSuccess = quorum*(endEntryId-startEntryId);
            int numRequests = e.getValue().size()*((int)(endEntryId-startEntryId));

            ReplicationVerificationCallback cb = new ReplicationVerificationCallback(numRequests);
            for (long i = startEntryId; i < endEntryId; i++) {
                for (InetSocketAddress addr : e.getValue()) {
                    bkc.bookieClient.readEntry(addr, lh.getId(), i, cb, addr);
                }
            }

            long numSuccess = cb.await();
            if (numSuccess < expectedSuccess) {
                LOG.warn("Fragment not fully replicated ledgerId = " + lh.getId()
                         + " startEntryId = " + startEntryId
                         + " endEntryId = " + endEntryId
                         + " expectedSuccess = " + expectedSuccess
                         + " gotSuccess = " + numSuccess);
                return false;
            }
        }
        return true;
    }

    // Object used for synchronizing async method calls
    class SyncLedgerMetaObject {
        boolean value;
        int rc;
        LedgerMetadata meta;

        public SyncLedgerMetaObject() {
            value = false;
            meta = null;
        }
    }

    private LedgerMetadata getLedgerMetadata(LedgerHandle lh) throws Exception {
        final SyncLedgerMetaObject syncObj = new SyncLedgerMetaObject();
        bkc.getLedgerManager().readLedgerMetadata(lh.getId(), new GenericCallback<LedgerMetadata>() {

            @Override
            public void operationComplete(int rc, LedgerMetadata result) {
                synchronized (syncObj) {
                    syncObj.rc = rc;
                    syncObj.meta = result;
                    syncObj.value = true;
                    syncObj.notify();
                }
            }

        });

        synchronized (syncObj) {
            while (syncObj.value == false) {
                syncObj.wait();
            }
        }
        assertEquals(BKException.Code.OK, syncObj.rc);
        return syncObj.meta;
    }

    private boolean findDupesInEnsembles(List<LedgerHandle> lhs) throws Exception {
        long numDupes = 0;
        for (LedgerHandle lh : lhs) {
            LedgerMetadata md = getLedgerMetadata(lh);
            for (Map.Entry<Long, ArrayList<InetSocketAddress>> e : md.getEnsembles().entrySet()) {
                HashSet<InetSocketAddress> set = new HashSet<InetSocketAddress>();
                long fragment = e.getKey();

                for (InetSocketAddress addr : e.getValue()) {
                    if (set.contains(addr)) {
                        LOG.error("Dupe " + addr + " found in ensemble for fragment " + fragment
                                + " of ledger " + lh.getId());
                        numDupes++;
                    }
                    set.add(addr);
                }
            }
        }
        return numDupes > 0;
    }

    /**
     * Test recoverying the closed ledgers when the failed bookie server is in the last ensemble
     */
    @Test
    public void testBookieRecoveryOnClosedLedgers() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

        // Write the entries for the ledgers with dummy values
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        closeLedgers(lhs);

        // Shutdown last bookie server in last ensemble
        ArrayList<InetSocketAddress> lastEnsemble = lhs.get(0).getLedgerMetadata().getEnsembles()
                                                       .entrySet().iterator().next().getValue();
        InetSocketAddress bookieToKill = lastEnsemble.get(lastEnsemble.size() - 1);
        killBookie(bookieToKill);

        // start a new bookie
        startNewBookie();

        InetSocketAddress bookieDest = null;
        LOG.info("Now recover the data on the killed bookie (" + bookieToKill
               + ") and replicate it to a random available one");

        bkAdmin.recoverBookieData(bookieToKill, bookieDest);
        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs));
            lh.close();
        }
    }

    @Test
    public void testBookieRecoveryOnOpenedLedgers() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

        // Write the entries for the ledgers with dummy values
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        ArrayList<InetSocketAddress> lastEnsemble = lhs.get(0).getLedgerMetadata().getEnsembles()
                                                       .entrySet().iterator().next().getValue();
        InetSocketAddress bookieToKill = lastEnsemble.get(lastEnsemble.size() - 1);
        killBookie(bookieToKill);

        // start a new bookie
        startNewBookie();

        InetSocketAddress bookieDest = null;
        LOG.info("Now recover the data on the killed bookie (" + bookieToKill
               + ") and replicate it to a random available one");

        bkAdmin.recoverBookieData(bookieToKill, bookieDest);

        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs));
        }

        try {
            // we can't write entries
            writeEntriestoLedgers(numMsgs, 0, lhs);
            fail("should not reach here");
        } catch (Exception e) {
        }
    }

    @Test
    public void testBookieRecoveryOnInRecoveryLedger() throws Exception {
        int numMsgs = 10;
        // Create the ledgers
        int numLedgers = 1;
        List<LedgerHandle> lhs = createLedgers(numLedgers, 2, 2);

        // Write the entries for the ledgers with dummy values
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        ArrayList<InetSocketAddress> lastEnsemble = lhs.get(0).getLedgerMetadata().getEnsembles()
                                                       .entrySet().iterator().next().getValue();
        // removed bookie
        InetSocketAddress bookieToKill = lastEnsemble.get(0);
        killBookie(bookieToKill);
        // temp failure
        InetSocketAddress bookieToKill2 = lastEnsemble.get(1);
        ServerConfiguration conf2 = killBookie(bookieToKill2);

        // start a new bookie
        startNewBookie();

        // open these ledgers
        for (LedgerHandle oldLh : lhs) {
            try {
                bkc.openLedger(oldLh.getId(), digestType, baseClientConf.getBookieRecoveryPasswd());
                fail("Should have thrown exception");
            } catch (Exception e) {
            }
        }

        try {
            bkAdmin.recoverBookieData(bookieToKill, null);
            fail("Should have thrown exception");
        } catch (BKException.BKLedgerRecoveryException bke) {
            // correct behaviour
        }

        // restart failed bookie
        bs.add(startBookie(conf2));
        bsConfs.add(conf2);

        // recover them
        bkAdmin.recoverBookieData(bookieToKill, null);

        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs));
        }

        // open ledgers to read metadata
        List<LedgerHandle> newLhs = openLedgers(lhs);
        for (LedgerHandle newLh : newLhs) {
            // first ensemble should contains bookieToKill2 and not contain bookieToKill
            Map.Entry<Long, ArrayList<InetSocketAddress>> entry =
                newLh.getLedgerMetadata().getEnsembles().entrySet().iterator().next();
            assertFalse(entry.getValue().contains(bookieToKill));
            assertTrue(entry.getValue().contains(bookieToKill2));
        }

    }

    @Test
    public void testAsyncBookieRecoveryToRandomBookiesNotEnoughBookies() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        bs.get(0).shutdown();
        bs.remove(0);

        // Call the async recover bookie method.
        InetSocketAddress bookieSrc = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), initialPort);
        InetSocketAddress bookieDest = null;
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
                 + ") and replicate it to a random available one");
        // Initiate the sync object
        sync.value = false;
        try {
            bkAdmin.recoverBookieData(bookieSrc, null);
            fail("Should have thrown exception");
        } catch (BKException.BKLedgerRecoveryException bke) {
            // correct behaviour
        }
    }

    @Test
    public void testSyncBookieRecoveryToRandomBookiesCheckForDupes() throws Exception {
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            // Create the ledgers
            int numLedgers = 3;
            List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

            // Write the entries for the ledgers with dummy values.
            int numMsgs = 100;
            writeEntriestoLedgers(numMsgs, 0, lhs);

            // Shutdown the first bookie server
            LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
            int removeIndex = r.nextInt(bs.size());
            InetSocketAddress bookieSrc = bs.get(removeIndex).getLocalAddress();
            bs.get(removeIndex).shutdown();
            bs.remove(removeIndex);

            // Startup three new bookie servers
            startNewBookie();

            // Write some more entries for the ledgers so a new ensemble will be
            // created for them.
            writeEntriestoLedgers(numMsgs, numMsgs, lhs);

            // Call the async recover bookie method.
            LOG.info("Now recover the data on the killed bookie (" + bookieSrc
                     + ") and replicate it to a random available one");
            // Initiate the sync object
            sync.value = false;
            bkAdmin.recoverBookieData(bookieSrc, null);
            assertFalse("Dupes exist in ensembles", findDupesInEnsembles(lhs));

            // Write some more entries to ensure fencing hasn't broken stuff
            writeEntriestoLedgers(numMsgs, numMsgs*2, lhs);
            for (LedgerHandle lh : lhs) {
                assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs*3));
                lh.close();
            }
        }
    }
}
