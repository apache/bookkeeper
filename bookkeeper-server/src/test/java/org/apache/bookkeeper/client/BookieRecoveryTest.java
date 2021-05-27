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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the bookie recovery admin functionality.
 */
public class BookieRecoveryTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(BookieRecoveryTest.class);

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
    String ledgerManagerFactory;
    SyncObject sync;
    BookieRecoverCallback bookieRecoverCb;
    BookKeeperAdmin bkAdmin;

    // Constructor
    public BookieRecoveryTest() {
        super(3);

        this.digestType = DigestType.CRC32;
        this.ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";
        LOG.info("Using ledger manager " + ledgerManagerFactory);
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseConf.setOpenFileLimit(200); // Limit the number of open files to avoid reaching the proc max
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
        adminConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        bkAdmin = new BookKeeperAdmin(adminConf);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Release any resources used by the BookieRecoveryTest instance.
        if (bkAdmin != null){
            bkAdmin.close();
        }
        super.tearDown();
    }

    /**
     * Helper method to create a number of ledgers.
     *
     * @param numLedgers
     *            Number of ledgers to create
     * @return List of LedgerHandles for each of the ledgers created
     */
    private List<LedgerHandle> createLedgers(int numLedgers)
      throws BKException, IOException, InterruptedException {
        return createLedgers(numLedgers, 3, 2);
    }

    /**
     * Helper method to create a number of ledgers.
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
    private void verifyRecoveredLedgers(List<LedgerHandle> oldLhs, long startEntryId, long endEntryId)
            throws BKException, InterruptedException {
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
     * This tests the bookie recovery functionality with ensemble changes.
     * We'll verify that:
     * - bookie recovery should not affect ensemble change.
     * - ensemble change should not erase changes made by recovery.
     *
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-667}
     */
    @Test
    public void testMetadataConflictWithRecovery() throws Exception {
        metadataConflictWithRecovery(bkc);
    }

    @Test
    public void testMetadataConflictWhenDelayingEnsembleChange() throws Exception {
        ClientConfiguration newConf = new ClientConfiguration(baseClientConf);
        newConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        newConf.setDelayEnsembleChange(true);
        try (BookKeeper newBkc = new BookKeeper(newConf)) {
            metadataConflictWithRecovery(newBkc);
        }
    }

    void metadataConflictWithRecovery(BookKeeper bkc) throws Exception {
        int numEntries = 10;
        byte[] data = "testMetadataConflictWithRecovery".getBytes();

        LedgerHandle lh = bkc.createLedger(2, 2, digestType, baseClientConf.getBookieRecoveryPasswd());
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }
        BookieId bookieToKill = lh.getLedgerMetadata().getEnsembleAt(numEntries - 1).get(1);
        killBookie(bookieToKill);
        startNewBookie();
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }
        bkAdmin.recoverBookieData(bookieToKill);
        // fail another bookie to cause ensemble change again
        bookieToKill = lh.getLedgerMetadata().getEnsembleAt(2 * numEntries - 1).get(1);
        ServerConfiguration confOfKilledBookie = killBookie(bookieToKill);
        startNewBookie();
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }
        // start the killed bookie again
        startAndAddBookie(confOfKilledBookie);

        // all ensembles should be fully replicated since it is recovered
        assertTrue("Not fully replicated", verifyFullyReplicated(lh, 3 * numEntries));
        lh.close();
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
        BookieId bookieSrc = addressByIndex(0);
        killBookie(0);

        // Startup a new bookie server
        startNewBookie();

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the async recover bookie method.
        // Initiate the sync object
        sync.value = false;
        bkAdmin.asyncRecoverBookieData(bookieSrc, bookieRecoverCb, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (!sync.value) {
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

        BookieId bookieSrc = addressByIndex(0);
        killBookie(0);

        // Startup three new bookie servers
        for (int i = 0; i < 3; i++) {
            startNewBookie();
        }

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the async recover bookie method.
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
          + ") and replicate it to a random available one");
        // Initiate the sync object
        sync.value = false;
        bkAdmin.asyncRecoverBookieData(bookieSrc, bookieRecoverCb, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (!sync.value) {
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

        BookieId bookieSrc = addressByIndex(0);
        killBookie(0);

        // Startup a new bookie server
        int newBookiePort = startNewBookie();

        // Write some more entries for the ledgers so a new ensemble will be
        //created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the sync recover bookie method.
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc + ") and replicate it to other bookies");
        bkAdmin.recoverBookieData(bookieSrc);

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

        BookieId bookieSrc = addressByIndex(0);
        killBookie(0);

        // Startup three new bookie servers
        for (int i = 0; i < 3; i++) {
            startNewBookie();
        }

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the sync recover bookie method.
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
          + ") and replicate it to a random available one");
        bkAdmin.recoverBookieData(bookieSrc);

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
        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got " + rc + " for ledger " + ledgerId + " entry " + entryId + " from " + ctx);
            }
            if (rc == BKException.Code.OK) {
                numSuccess.incrementAndGet();
            }
            latch.countDown();
        }

        long await() throws InterruptedException {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                LOG.warn("Didn't get all responses in verification");
                return 0;
            } else {
                return numSuccess.get();
            }
        }
    }

    private boolean verifyFullyReplicated(LedgerHandle lh, long untilEntry) throws Exception {
        LedgerMetadata md = getLedgerMetadata(lh);

        Map<Long, ? extends List<BookieId>> ensembles = md.getAllEnsembles();

        HashMap<Long, Long> ranges = new HashMap<Long, Long>();
        ArrayList<Long> keyList = Collections.list(
          Collections.enumeration(ensembles.keySet()));
        Collections.sort(keyList);
        for (int i = 0; i < keyList.size() - 1; i++) {
            ranges.put(keyList.get(i), keyList.get(i + 1));
        }
        ranges.put(keyList.get(keyList.size() - 1), untilEntry);

        for (Map.Entry<Long, ? extends List<BookieId>> e : ensembles.entrySet()) {
            int quorum = md.getAckQuorumSize();
            long startEntryId = e.getKey();
            long endEntryId = ranges.get(startEntryId);
            long expectedSuccess = quorum * (endEntryId - startEntryId);
            int numRequests = e.getValue().size() * ((int) (endEntryId - startEntryId));

            ReplicationVerificationCallback cb = new ReplicationVerificationCallback(numRequests);
            for (long i = startEntryId; i < endEntryId; i++) {
                for (BookieId addr : e.getValue()) {
                    bkc.getBookieClient().readEntry(addr, lh.getId(), i,
                                                    cb, addr, BookieProtocol.FLAG_NONE);
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
        return bkc.getLedgerManager().readLedgerMetadata(lh.getId()).get().getValue();
    }

    private boolean findDupesInEnsembles(List<LedgerHandle> lhs) throws Exception {
        long numDupes = 0;
        for (LedgerHandle lh : lhs) {
            LedgerMetadata md = getLedgerMetadata(lh);
            for (Map.Entry<Long, ? extends List<BookieId>> e : md.getAllEnsembles().entrySet()) {
                HashSet<BookieId> set = new HashSet<BookieId>();
                long fragment = e.getKey();

                for (BookieId addr : e.getValue()) {
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
     * Test recoverying the closed ledgers when the failed bookie server is in the last ensemble.
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
        List<BookieId> lastEnsemble = lhs.get(0).getLedgerMetadata().getAllEnsembles()
          .entrySet().iterator().next().getValue();
        BookieId bookieToKill = lastEnsemble.get(lastEnsemble.size() - 1);
        killBookie(bookieToKill);

        // start a new bookie
        startNewBookie();

        LOG.info("Now recover the data on the killed bookie (" + bookieToKill
          + ") and replicate it to a random available one");

        bkAdmin.recoverBookieData(bookieToKill);
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
        List<BookieId> lastEnsemble = lhs.get(0).getLedgerMetadata().getAllEnsembles()
          .entrySet().iterator().next().getValue();
        BookieId bookieToKill = lastEnsemble.get(lastEnsemble.size() - 1);
        killBookie(bookieToKill);

        // start a new bookie
        startNewBookie();

        LOG.info("Now recover the data on the killed bookie (" + bookieToKill
          + ") and replicate it to a random available one");

        bkAdmin.recoverBookieData(bookieToKill);

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
        List<BookieId> lastEnsemble = lhs.get(0).getLedgerMetadata().getAllEnsembles()
          .entrySet().iterator().next().getValue();
        // removed bookie
        BookieId bookieToKill = lastEnsemble.get(0);
        killBookie(bookieToKill);
        // temp failure
        BookieId bookieToKill2 = lastEnsemble.get(1);
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
            bkAdmin.recoverBookieData(bookieToKill);
            fail("Should have thrown exception");
        } catch (BKException.BKLedgerRecoveryException bke) {
            // correct behaviour
        }

        // restart failed bookie
        startAndAddBookie(conf2);

        // recover them
        bkAdmin.recoverBookieData(bookieToKill);

        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs));
        }

        // open ledgers to read metadata
        List<LedgerHandle> newLhs = openLedgers(lhs);
        for (LedgerHandle newLh : newLhs) {
            // first ensemble should contains bookieToKill2 and not contain bookieToKill
            Map.Entry<Long, ? extends List<BookieId>> entry =
              newLh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next();
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

        BookieId bookieSrc = addressByIndex(0);
        killBookie(0);

        // Call the async recover bookie method.
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
          + ") and replicate it to a random available one");
        // Initiate the sync object
        sync.value = false;
        try {
            bkAdmin.recoverBookieData(bookieSrc);
            fail("Should have thrown exception");
        } catch (BKException.BKLedgerRecoveryException bke) {
            // correct behaviour
        }
    }

    @Test
    public void testSyncBookieRecoveryToRandomBookiesCheckForDupes() throws Exception {
        Random r = new Random();

        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        int removeIndex = r.nextInt(bookieCount());
        BookieId bookieSrc = addressByIndex(removeIndex);
        killBookie(removeIndex);

        // Startup new bookie server
        startNewBookie();

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, numMsgs, lhs);

        // Call the async recover bookie method.
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
          + ") and replicate it to a random available one");
        // Initiate the sync object
        sync.value = false;
        bkAdmin.recoverBookieData(bookieSrc);

        assertFalse("Dupes exist in ensembles", findDupesInEnsembles(lhs));

        // Write some more entries to ensure fencing hasn't broken stuff
        writeEntriestoLedgers(numMsgs, numMsgs * 2, lhs);

        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs * 3));
            lh.close();
        }
    }

    @Test
    public void recoverWithoutPasswordInConf() throws Exception {
        byte[] passwdCorrect = "AAAAAA".getBytes();
        byte[] passwdBad = "BBBBBB".getBytes();
        DigestType digestCorrect = digestType;

        LedgerHandle lh = bkc.createLedger(3, 2, digestCorrect, passwdCorrect);
        long ledgerId = lh.getId();
        for (int i = 0; i < 100; i++) {
            lh.addEntry("foobar".getBytes());
        }
        lh.close();

        BookieId bookieSrc = addressByIndex(0);
        killBookie(0);

        startNewBookie();

        // Check that entries are missing
        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertFalse("Should be entries missing", verifyFullyReplicated(lh, 100));
        lh.close();

        // Try to recover with bad password in conf
        // This is fine, because it only falls back to the configured
        // password if the password info is missing from the metadata
        ClientConfiguration adminConf = new ClientConfiguration();
        adminConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        adminConf.setBookieRecoveryDigestType(digestCorrect);
        adminConf.setBookieRecoveryPasswd(passwdBad);
        setMetastoreImplClass(adminConf);

        BookKeeperAdmin bka = new BookKeeperAdmin(adminConf);
        bka.recoverBookieData(bookieSrc);
        bka.close();

        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertTrue("Should be back to fully replication", verifyFullyReplicated(lh, 100));
        lh.close();

        bookieSrc = addressByIndex(0);
        killBookie(0);
        startNewBookie();

        // Check that entries are missing
        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertFalse("Should be entries missing", verifyFullyReplicated(lh, 100));
        lh.close();

        // Try to recover with no password in conf
        adminConf = new ClientConfiguration();
        adminConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        setMetastoreImplClass(adminConf);

        bka = new BookKeeperAdmin(adminConf);
        bka.recoverBookieData(bookieSrc);
        bka.close();

        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertTrue("Should be back to fully replication", verifyFullyReplicated(lh, 100));
        lh.close();
    }

}
