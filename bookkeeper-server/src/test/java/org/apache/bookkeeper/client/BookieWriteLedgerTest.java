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

import static org.apache.bookkeeper.client.BookKeeperClientStats.ADD_OP;
import static org.apache.bookkeeper.client.BookKeeperClientStats.ADD_OP_UR;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.READ_OP_DM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.replication.ReplicationTestUtil;
import org.apache.bookkeeper.replication.ReplicationWorker;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing ledger write entry cases.
 */
@RunWith(Parameterized.class)
public class BookieWriteLedgerTest extends
    BookKeeperClusterTestCase implements AddCallback {

    private static final Logger LOG = LoggerFactory
            .getLogger(BookieWriteLedgerTest.class);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { true, true }, { true, false }, { false, true }, { false, false }
        });
    }

    @Parameterized.Parameter(0)
    public boolean useV2;

    @Parameterized.Parameter(1)
    public boolean writeJournal;

    byte[] ledgerPassword = "aaa".getBytes();
    LedgerHandle lh, lh2;
    Enumeration<LedgerEntry> ls;

    // test related variables
    int numEntriesToWrite = 100;
    int maxInt = Integer.MAX_VALUE;
    Random rng; // Random Number Generator
    ArrayList<byte[]> entries1; // generated entries
    ArrayList<byte[]> entries2; // generated entries

    private final DigestType digestType;

    private static class SyncObj {
        volatile int counter;
        volatile int rc;

        public SyncObj() {
            counter = 0;
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        baseConf.setJournalWriteData(writeJournal);
        baseClientConf.setUseV2WireProtocol(useV2);

        super.setUp();
        rng = new Random(0); // Initialize the Random
        // Number Generator
        entries1 = new ArrayList<byte[]>(); // initialize the entries list
        entries2 = new ArrayList<byte[]>(); // initialize the entries list
    }

    public BookieWriteLedgerTest() {
        super(5, 180);
        this.digestType = DigestType.CRC32;
        String ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        /*
         * 'testLedgerCreateAdvWithLedgerIdInLoop2' testcase relies on skipListSizeLimit,
         * so setting it to some small value for making that testcase lite.
         */
        baseConf.setSkipListSizeLimit(4 * 1024 * 1024);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    /**
     * Verify write when few bookie failures in last ensemble and forcing
     * ensemble reformation.
     */
    @Test
    public void testWithMultipleBookieFailuresInLastEnsemble() throws Exception {
        // Create a ledger
        lh = bkc.createLedger(5, 4, digestType, ledgerPassword);
        LOG.info("Ledger ID: " + lh.getId());
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }
        // Start three more bookies
        startNewBookie();
        startNewBookie();
        startNewBookie();

        // Shutdown three bookies in the last ensemble and continue writing
        List<BookieId> ensemble = lh.getLedgerMetadata()
                .getAllEnsembles().entrySet().iterator().next().getValue();
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));
        killBookie(ensemble.get(2));

        int i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;
        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }
        readEntries(lh, entries1);
        lh.close();
    }

    /**
     * Verify write and Read durability stats.
     */
    @Test
    public void testWriteAndReadStats() throws Exception {
        // Create a ledger
        lh = bkc.createLedger(3, 3, 2, digestType, ledgerPassword);

        // write-batch-1
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }
        assertTrue(
                "Stats should have captured a new writes",
                bkc.getTestStatsProvider().getOpStatsLogger(
                        CLIENT_SCOPE + "." + ADD_OP)
                        .getSuccessCount() > 0);

        CountDownLatch sleepLatch1 = new CountDownLatch(1);
        CountDownLatch sleepLatch2 = new CountDownLatch(1);
        List<BookieId> ensemble = lh.getLedgerMetadata()
                .getAllEnsembles().entrySet().iterator().next().getValue();

        sleepBookie(ensemble.get(0), sleepLatch1);

        int i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;

        // write-batch-2

        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }

        // Let the second bookie go to sleep. This forces write timeout and ensemble change
        // Which will be enough time to receive delayed write failures on the write-batch-2

        sleepBookie(ensemble.get(1), sleepLatch2);
        i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;

        // write-batch-3

        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }

        assertTrue(
                "Stats should have captured a new UnderReplication during write",
                bkc.getTestStatsProvider().getCounter(
                        CLIENT_SCOPE + "." + ADD_OP_UR)
                        .get() > 0);

        sleepLatch1.countDown();
        sleepLatch2.countDown();

        // Replace the bookie with a fake bookie
        ServerConfiguration conf = killBookie(ensemble.get(0));
        BookieWriteLedgerTest.CorruptReadBookie corruptBookie = new BookieWriteLedgerTest.CorruptReadBookie(conf);
        startAndAddBookie(conf, corruptBookie);

        i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;

        // write-batch-4

        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }

        readEntries(lh, entries1);
        assertTrue(
                "Stats should have captured DigestMismatch on Read",
                bkc.getTestStatsProvider().getCounter(
                        CLIENT_SCOPE + "." + READ_OP_DM)
                        .get() > 0);
        lh.close();
    }
    /**
     * Verty delayedWriteError causes ensemble changes.
     */
    @Test
    public void testDelayedWriteEnsembleChange() throws Exception {
        // Create a ledger
        lh = bkc.createLedger(3, 3, 2, digestType, ledgerPassword);
        baseClientConf.setAddEntryTimeout(1);

        int numEntriesToWrite = 10;
        // write-batch-1
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }

        CountDownLatch sleepLatch1 = new CountDownLatch(1);

        // get bookie at index-0
        BookieId bookie1 = lh.getCurrentEnsemble().get(0);
        sleepBookie(bookie1, sleepLatch1);

        int i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 10;

        // write-batch-2

        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }
        // Sleep to receive delayed error on the write directed to the sleeping bookie
        Thread.sleep(baseClientConf.getAddEntryTimeout() * 1000 * 2);
        assertTrue(
                "Stats should have captured a new UnderReplication during write",
                bkc.getTestStatsProvider().getCounter(
                        CLIENT_SCOPE + "." + ADD_OP_UR)
                        .get() > 0);

        i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 10;

        // write-batch-3
        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(entry.array());
        }

        sleepLatch1.countDown();
        // get the bookie at index-0 again, this must be different.
        BookieId bookie2 = lh.getCurrentEnsemble().get(0);

        assertFalse(
                "Delayed write error must have forced ensemble change",
                        bookie1.equals(bookie2));
        lh.close();
    }
    /**
     * Verify the functionality Ledgers with different digests.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerDigestTest() throws Exception {
        for (DigestType type: DigestType.values()) {
            lh = bkc.createLedger(5, 3, 2, type, ledgerPassword);

            for (int i = 0; i < numEntriesToWrite; i++) {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(rng.nextInt(maxInt));
                entry.position(0);

                entries1.add(entry.array());
                lh.addEntry(entry.array());
            }

            readEntries(lh, entries1);

            long lid = lh.getId();
            lh.close();
            bkc.deleteLedger(lid);
            entries1.clear();
        }
    }

    /**
     * Verify the functionality of Advanced Ledger which returns
     * LedgerHandleAdv. LedgerHandleAdv takes entryId for addEntry, and let
     * user manage entryId allocation.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateAdv() throws Exception {
        // Create a ledger
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
        }
        // Start one more bookies
        startNewBookie();

        // Shutdown one bookie in the last ensemble and continue writing
        List<BookieId> ensemble = lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next()
                .getValue();
        killBookie(ensemble.get(0));

        int i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;
        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
        }

        readEntries(lh, entries1);
        lh.close();
    }

    /**
     * Verify that attempts to use addEntry() variant that does not require specifying entry id
     * on LedgerHandleAdv results in error.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateAdvAndWriteNonAdv() throws Exception {
        long ledgerId = 0xABCDEF;
        lh = bkc.createLedgerAdv(ledgerId, 3, 3, 2, digestType, ledgerPassword, null);

        ByteBuffer entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);

        try {
            lh.addEntry(entry.array());
            fail("expected IllegalOpException");
        } catch (BKException.BKIllegalOpException e) {
            // pass, expected
        } finally {
            lh.close();
            bkc.deleteLedger(ledgerId);
        }
    }

    /**
     * Verify that LedgerHandleAdv cannnot handle addEntry without the entryId.
     *
     * @throws Exception
     */
    @Test
    public void testNoAddEntryLedgerCreateAdv() throws Exception {

        ByteBuffer entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);

        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        assertTrue(lh instanceof LedgerHandleAdv);

        try {
            lh.addEntry(entry.array());
            fail("using LedgerHandleAdv addEntry without entryId is forbidden");
        } catch (BKException e) {
            assertEquals(e.getCode(), BKException.Code.IllegalOpException);
        }

        try {
            lh.addEntry(entry.array(), 0, 4);
            fail("using LedgerHandleAdv addEntry without entryId is forbidden");
        } catch (BKException e) {
            assertEquals(e.getCode(), BKException.Code.IllegalOpException);
        }

        try {
            CompletableFuture<Object> done = new CompletableFuture<>();
            lh.asyncAddEntry(Unpooled.wrappedBuffer(entry.array()),
                (int rc, LedgerHandle lh1, long entryId, Object ctx) -> {
                SyncCallbackUtils.finish(rc, null, done);
            }, null);
            done.get();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof BKException);
            BKException e = (BKException) ee.getCause();
            assertEquals(e.getCode(), BKException.Code.IllegalOpException);
        }

        try {
            CompletableFuture<Object> done = new CompletableFuture<>();
            lh.asyncAddEntry(entry.array(),
                (int rc, LedgerHandle lh1, long entryId, Object ctx) -> {
                SyncCallbackUtils.finish(rc, null, done);
            }, null);
            done.get();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof BKException);
            BKException e = (BKException) ee.getCause();
            assertEquals(e.getCode(), BKException.Code.IllegalOpException);
        }

        try {
            CompletableFuture<Object> done = new CompletableFuture<>();
            lh.asyncAddEntry(entry.array(), 0, 4,
                (int rc, LedgerHandle lh1, long entryId, Object ctx) -> {
                SyncCallbackUtils.finish(rc, null, done);
            }, null);
            done.get();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof BKException);
            BKException e = (BKException) ee.getCause();
            assertEquals(e.getCode(), BKException.Code.IllegalOpException);
        }
        lh.close();
    }

    /**
     * Verify the functionality of Advanced Ledger which accepts ledgerId as input and returns
     * LedgerHandleAdv. LedgerHandleAdv takes entryId for addEntry, and let
     * user manage entryId allocation.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateAdvWithLedgerId() throws Exception {
        // Create a ledger
        long ledgerId = 0xABCDEF;
        lh = bkc.createLedgerAdv(ledgerId, 5, 3, 2, digestType, ledgerPassword, null);
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
        }
        // Start one more bookies
        startNewBookie();

        // Shutdown one bookie in the last ensemble and continue writing
        List<BookieId> ensemble = lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next()
                .getValue();
        killBookie(ensemble.get(0));

        int i = numEntriesToWrite;
        numEntriesToWrite = numEntriesToWrite + 50;
        for (; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
        }

        readEntries(lh, entries1);
        lh.close();
        bkc.deleteLedger(ledgerId);
    }

    /**
     * Verify the functionality of Ledger create which accepts customMetadata as input.
     * Also verifies that the data written is read back properly.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateWithCustomMetadata() throws Exception {
        // Create a ledger
        long ledgerId;
        int maxLedgers = 10;
        for (int i = 0; i < maxLedgers; i++) {
            Map<String, byte[]> inputCustomMetadataMap = new HashMap<String, byte[]>();
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            // each ledger has different number of key, value pairs.
            for (int j = 0; j < i; j++) {
                inputCustomMetadataMap.put("key" + j, UUID.randomUUID().toString().getBytes());
            }

            if (i < maxLedgers / 2) {
                // 0 to 4 test with createLedger interface
                lh = bkc.createLedger(5, 3, 2, digestType, ledgerPassword, inputCustomMetadataMap);
                ledgerId = lh.getId();
                lh.addEntry(entry.array());
            } else {
                // 5 to 9 test with createLedgerAdv interface
                lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword, inputCustomMetadataMap);
                ledgerId = lh.getId();
                lh.addEntry(0, entry.array());
            }
            lh.close();

            // now reopen the ledger; this should fetch all the metadata stored on zk
            // and the customMetadata written and read should match
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            Map<String, byte[]> outputCustomMetadataMap = lh.getCustomMetadata();
            assertTrue("Can't retrieve proper Custom Data",
                       areByteArrayValMapsEqual(inputCustomMetadataMap, outputCustomMetadataMap));
            lh.close();
            bkc.deleteLedger(ledgerId);
        }
    }

    /**
     * Routine to compare two {@code Map<String, byte[]>}; Since the values in the map are {@code byte[]}, we can't use
     * {@code Map.equals}.
     * @param first
     *          The first map
     * @param second
     *          The second map to compare with
     * @return true if the 2 maps contain the exact set of {@code <K,V>} pairs.
     */
    public static boolean areByteArrayValMapsEqual(Map<String, byte[]> first, Map<String, byte[]> second) {
        if (first == null && second == null) {
            return true;
        }

        // above check confirms that both are not null;
        // if one is null the other isn't; so they must
        // be different
        if (first == null || second == null) {
            return false;
        }

        if (first.size() != second.size()) {
            return false;
        }
        for (Map.Entry<String, byte[]> entry : first.entrySet()) {
            if (!Arrays.equals(entry.getValue(), second.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /*
     * Verify the functionality of Advanced Ledger which accepts ledgerId as
     * input and returns LedgerHandleAdv. LedgerHandleAdv takes entryId for
     * addEntry, and let user manage entryId allocation.
     * This testcase is mainly added for covering missing code coverage branches
     * in LedgerHandleAdv
     *
     * @throws Exception
     */
    @Test
    public void testLedgerHandleAdvFunctionality() throws Exception {
        // Create a ledger
        long ledgerId = 0xABCDEF;
        lh = bkc.createLedgerAdv(ledgerId, 5, 3, 2, digestType, ledgerPassword, null);
        numEntriesToWrite = 3;

        ByteBuffer entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        lh.addEntry(0, entry.array());

        // here asyncAddEntry(final long entryId, final byte[] data, final
        // AddCallback cb, final Object ctx) method is
        // called which is not covered in any other testcase
        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        CountDownLatch latch = new CountDownLatch(1);
        final int[] returnedRC = new int[1];
        lh.asyncAddEntry(1, entry.array(), new AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                CountDownLatch latch = (CountDownLatch) ctx;
                returnedRC[0] = rc;
                latch.countDown();
            }
        }, latch);
        latch.await();
        assertTrue("Returned code is expected to be OK", returnedRC[0] == BKException.Code.OK);

        // here addEntry is called with incorrect offset and length
        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        try {
            lh.addEntry(2, entry.array(), -3, 9);
            fail("AddEntry is called with negative offset and incorrect length,"
                    + "so it is expected to throw RuntimeException/IndexOutOfBoundsException");
        } catch (RuntimeException exception) {
            // expected RuntimeException/IndexOutOfBoundsException
        }

        // here addEntry is called with corrected offset and length and it is
        // supposed to succeed
        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        lh.addEntry(2, entry.array());

        // LedgerHandle is closed for write
        lh.close();

        // here addEntry is called even after the close of the LedgerHandle, so
        // it is expected to throw exception
        entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        try {
            lh.addEntry(3, entry.array());
            fail("AddEntry is called after the close of LedgerHandle,"
                    + "so it is expected to throw BKLedgerClosedException");
        } catch (BKLedgerClosedException exception) {
        }

        readEntries(lh, entries1);
        bkc.deleteLedger(ledgerId);
    }

    /**
     * In a loop create/write/delete the ledger with same ledgerId through
     * the functionality of Advanced Ledger which accepts ledgerId as input.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateAdvWithLedgerIdInLoop() throws Exception {
        int ledgerCount = 40;

        long maxId = 9999999999L;
        if (baseConf.getLedgerManagerFactoryClass().equals(LongHierarchicalLedgerManagerFactory.class)) {
            // since LongHierarchicalLedgerManager supports ledgerIds of decimal length upto 19 digits but other
            // LedgerManagers only upto 10 decimals
            maxId = Long.MAX_VALUE;
        }

        rng.longs(ledgerCount, 0, maxId) // generate a stream of ledger ids
            .mapToObj(ledgerId -> { // create a ledger for each ledger id
                    LOG.info("Creating adv ledger with id {}", ledgerId);
                    return bkc.newCreateLedgerOp()
                        .withEnsembleSize(1).withWriteQuorumSize(1).withAckQuorumSize(1)
                        .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32)
                        .withPassword(ledgerPassword).makeAdv().withLedgerId(ledgerId)
                        .execute()
                        .thenCompose(writer -> { // Add entries to ledger when created
                                LOG.info("Writing stream of {} entries to {}",
                                         numEntriesToWrite, ledgerId);
                                List<ByteBuf> entries = rng.ints(numEntriesToWrite, 0, maxInt)
                                    .mapToObj(i -> {
                                            ByteBuf entry = Unpooled.buffer(4);
                                            entry.retain();
                                            entry.writeInt(i);
                                            return entry;
                                        })
                                    .collect(Collectors.toList());
                                CompletableFuture<?> lastRequest = null;
                                int i = 0;
                                for (ByteBuf entry : entries) {
                                    long entryId = i++;
                                    LOG.info("Writing {}:{} as {}",
                                             ledgerId, entryId, entry.slice().readInt());
                                    lastRequest = writer.writeAsync(entryId, entry);
                                }
                                return lastRequest
                                        .thenApply(___ -> Pair.of(writer, entries));
                            });
                })
            .parallel().map(CompletableFuture::join) // wait for all creations and adds in parallel
            .forEach(e -> { // check that each set of adds succeeded
                    try {
                        WriteAdvHandle handle = e.getLeft();
                        List<ByteBuf> entries = e.getRight();
                        // Read and verify
                        LOG.info("Read entries for ledger: {}", handle.getId());
                        readEntries(handle, entries);
                        entries.forEach(ByteBuf::release);
                        handle.close();
                        bkc.deleteLedger(handle.getId());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        Assert.fail("Test interrupted");
                    } catch (Exception ex) {
                        LOG.info("Readback failed with exception", ex);
                        Assert.fail("Readback failed " + ex.getMessage());
                    }
                });
    }

    /**
     * In a loop create/write/read/delete the ledger with ledgerId through the
     * functionality of Advanced Ledger which accepts ledgerId as input.
     * In this testcase (other testcases don't cover these conditions, hence new
     * testcase is added), we create entries which are greater than
     * SKIP_LIST_MAX_ALLOC_ENTRY size and tried to addEntries so that the total
     * length of data written in this testcase is much greater than
     * SKIP_LIST_SIZE_LIMIT, so that entries will be flushed from EntryMemTable
     * to persistent storage
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateAdvWithLedgerIdInLoop2() throws Exception {

        assertTrue("Here we are expecting Bookies are configured to use SortedLedgerStorage",
                baseConf.getSortedLedgerStorageEnabled());

        long ledgerId;
        int ledgerCount = 10;

        List<List<byte[]>> entryList = new ArrayList<List<byte[]>>();
        LedgerHandle[] lhArray = new LedgerHandle[ledgerCount];
        long skipListSizeLimit = baseConf.getSkipListSizeLimit();
        int skipListArenaMaxAllocSize = baseConf.getSkipListArenaMaxAllocSize();

        List<byte[]> tmpEntry;
        for (int lc = 0; lc < ledgerCount; lc++) {
            tmpEntry = new ArrayList<byte[]>();

            ledgerId = rng.nextLong();
            ledgerId &= Long.MAX_VALUE;
            if (!baseConf.getLedgerManagerFactoryClass().equals(LongHierarchicalLedgerManagerFactory.class)) {
                // since LongHierarchicalLedgerManager supports ledgerIds of
                // decimal length upto 19 digits but other
                // LedgerManagers only upto 10 decimals
                ledgerId %= 9999999999L;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Iteration: {}  LedgerId: {}", lc, ledgerId);
            }
            lh = bkc.createLedgerAdv(ledgerId, 5, 3, 2, digestType, ledgerPassword, null);
            lhArray[lc] = lh;

            long ledgerLength = 0;
            int i = 0;
            while (ledgerLength < ((4 * skipListSizeLimit) / ledgerCount)) {
                int length;
                if (rng.nextBoolean()) {
                    length = Math.abs(rng.nextInt()) % (skipListArenaMaxAllocSize);
                } else {
                    // here we want length to be random no. in the range of skipListArenaMaxAllocSize and
                    // 4*skipListArenaMaxAllocSize
                    length = (Math.abs(rng.nextInt()) % (skipListArenaMaxAllocSize * 3)) + skipListArenaMaxAllocSize;
                }
                byte[] data = new byte[length];
                rng.nextBytes(data);
                tmpEntry.add(data);
                lh.addEntry(i, data);
                ledgerLength += length;
                i++;
            }
            entryList.add(tmpEntry);
        }
        for (int lc = 0; lc < ledgerCount; lc++) {
            // Read and verify
            long lid = lhArray[lc].getId();
            if (LOG.isDebugEnabled()) {
                LOG.debug("readEntries for lc: {} ledgerId: {} ", lc, lhArray[lc].getId());
            }
            readEntriesAndValidateDataArray(lhArray[lc], entryList.get(lc));
            lhArray[lc].close();
            bkc.deleteLedger(lid);
        }
    }

    /**
     * Verify asynchronous writing when few bookie failures in last ensemble.
     */
    @Test
    public void testAsyncWritesWithMultipleFailuresInLastEnsemble()
            throws Exception {
        // Create ledgers
        lh = bkc.createLedger(5, 4, digestType, ledgerPassword);
        lh2 = bkc.createLedger(5, 4, digestType, ledgerPassword);

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            entries2.add(entry.array());
            lh.addEntry(entry.array());
            lh2.addEntry(entry.array());
        }
        // Start three more bookies
        startNewBookie();
        startNewBookie();
        startNewBookie();

        // Shutdown three bookies in the last ensemble and continue writing
        List<BookieId> ensemble = lh.getLedgerMetadata()
                .getAllEnsembles().entrySet().iterator().next().getValue();
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));
        killBookie(ensemble.get(2));

        // adding one more entry to both the ledgers async after multiple bookie
        // failures. This will do asynchronously modifying the ledger metadata
        // simultaneously.
        numEntriesToWrite++;
        ByteBuffer entry = ByteBuffer.allocate(4);
        entry.putInt(rng.nextInt(maxInt));
        entry.position(0);
        entries1.add(entry.array());
        entries2.add(entry.array());

        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        lh.asyncAddEntry(entry.array(), this, syncObj1);
        lh2.asyncAddEntry(entry.array(), this, syncObj2);

        // wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < 1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Entries counter = " + syncObj1.counter);
                }
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // wait for all entries to be acknowledged for the second ledger
        synchronized (syncObj2) {
            while (syncObj2.counter < 1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Entries counter = " + syncObj2.counter);
                }
                syncObj2.wait();
            }
            assertEquals(BKException.Code.OK, syncObj2.rc);
        }

        // reading ledger till the last entry
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
    }

    /**
     * Verify Advanced asynchronous writing with entryIds in reverse order.
     */
    @Test
    public void testLedgerCreateAdvWithAsyncWritesWithBookieFailures() throws Exception {
        // Create ledgers
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        lh2 = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
                entries2.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
            lh2.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj2);
        }
        // Start One more bookie and shutdown one from last ensemble before reading
        startNewBookie();
        List<BookieId> ensemble = lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next()
                .getValue();
        killBookie(ensemble.get(0));

        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < numEntriesToWrite) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Wait for all entries to be acknowledged for the second ledger
        synchronized (syncObj2) {
            while (syncObj2.counter < numEntriesToWrite) {
                syncObj2.wait();
            }
            assertEquals(BKException.Code.OK, syncObj2.rc);
        }

        // Reading ledger till the last entry
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
    }

    /**
     * LedgerHandleAdv out of order writers with ensemble changes.
     * Verify that entry that was written to old ensemble will be
     * written to new enseble too after ensemble change.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerHandleAdvOutOfOrderWriteAndFrocedEnsembleChange() throws Exception {
        // Create a ledger
        long ledgerId = 0xABCDEF;
        SyncObj syncObj1 = new SyncObj();
        ByteBuffer entry;
        lh = bkc.createLedgerAdv(ledgerId, 3, 3, 3, digestType, ledgerPassword, null);
        entry = ByteBuffer.allocate(4);
        // Add entries 0-4
        for (int i = 0; i < 5; i++) {
            entry.rewind();
            entry.putInt(rng.nextInt(maxInt));
            lh.addEntry(i, entry.array());
        }

        // Add 10 as Async Entry, which goes to first ensemble
        ByteBuffer entry1 = ByteBuffer.allocate(4);
        entry1.putInt(rng.nextInt(maxInt));
        lh.asyncAddEntry(10, entry1.array(), 0, entry1.capacity(), this, syncObj1);

        // Make sure entry-10 goes to the bookies and gets response.
        java.util.Queue<PendingAddOp> myPendingAddOps = Whitebox.getInternalState(lh, "pendingAddOps");
        PendingAddOp addOp = null;
        boolean pendingAddOpReceived = false;

        while (!pendingAddOpReceived) {
            addOp = myPendingAddOps.peek();
            if (addOp.entryId == 10 && addOp.completed) {
                pendingAddOpReceived = true;
            } else {
                Thread.sleep(1000);
            }
        }

        CountDownLatch sleepLatch1 = new CountDownLatch(1);
        List<BookieId> ensemble;

        ensemble = lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next().getValue();

        // Put all 3 bookies to sleep and start 3 new ones
        sleepBookie(ensemble.get(0), sleepLatch1);
        sleepBookie(ensemble.get(1), sleepLatch1);
        sleepBookie(ensemble.get(2), sleepLatch1);
        startNewBookie();
        startNewBookie();
        startNewBookie();

        // Original bookies are in sleep, new bookies added.
        // Now add entries 5-9 which forces ensemble changes
        // So at this point entries 0-4, 10 went to first
        // ensemble, 5-9 will go to new ensemble.
        for (int i = 5; i < 10; i++) {
            entry.rewind();
            entry.putInt(rng.nextInt(maxInt));
            lh.addEntry(i, entry.array());
        }

        // Wakeup all 3 bookies that went to sleep
        sleepLatch1.countDown();

        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < 1) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }

        // Close write handle
        lh.close();

        // Open read handle
        lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);

        // Make sure to read all 10 entries.
        for (int i = 0; i < 11; i++) {
            lh.readEntries(i, i);
        }
        lh.close();
        bkc.deleteLedger(ledgerId);
    }

    /**
     * Verify Advanced asynchronous writing with entryIds in pseudo random order with bookie failures between writes.
     */
    @Test
    public void testLedgerCreateAdvWithRandomAsyncWritesWithBookieFailuresBetweenWrites() throws Exception {
        // Create ledgers
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        lh2 = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        int batchSize = 5;
        int i, j;

        // Fill the result buffers first
        for (i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);

            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
                entries2.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (i = 0; i < batchSize; i++) {
            for (j = i; j < numEntriesToWrite; j = j + batchSize) {
                byte[] entry1 = entries1.get(j);
                byte[] entry2 = entries2.get(j);
                lh.asyncAddEntry(j, entry1, 0, entry1.length, this, syncObj1);
                lh2.asyncAddEntry(j, entry2, 0, entry2.length, this, syncObj2);
                if (j == numEntriesToWrite / 2) {
                    // Start One more bookie and shutdown one from last ensemble at half-way
                    startNewBookie();
                    List<BookieId> ensemble = lh.getLedgerMetadata().getAllEnsembles().entrySet()
                            .iterator().next().getValue();
                    killBookie(ensemble.get(0));
                }
            }
        }

        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < numEntriesToWrite) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Wait for all entries to be acknowledged for the second ledger
        synchronized (syncObj2) {
            while (syncObj2.counter < numEntriesToWrite) {
                syncObj2.wait();
            }
            assertEquals(BKException.Code.OK, syncObj2.rc);
        }

        // Reading ledger till the last entry
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
    }

    /**
     * Verify Advanced asynchronous writing with entryIds in pseudo random order.
     */
    @Test
    public void testLedgerCreateAdvWithRandomAsyncWritesWithBookieFailures() throws Exception {
        // Create ledgers
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        lh2 = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        int batchSize = 5;
        int i, j;

        // Fill the result buffers first
        for (i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);

            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
                entries2.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (i = 0; i < batchSize; i++) {
            for (j = i; j < numEntriesToWrite; j = j + batchSize) {
                byte[] entry1 = entries1.get(j);
                byte[] entry2 = entries2.get(j);
                lh.asyncAddEntry(j, entry1, 0, entry1.length, this, syncObj1);
                lh2.asyncAddEntry(j, entry2, 0, entry2.length, this, syncObj2);
            }
        }
        // Start One more bookie and shutdown one from last ensemble before reading
        startNewBookie();
        List<BookieId> ensemble = lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next()
                .getValue();
        killBookie(ensemble.get(0));

        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < numEntriesToWrite) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Wait for all entries to be acknowledged for the second ledger
        synchronized (syncObj2) {
            while (syncObj2.counter < numEntriesToWrite) {
                syncObj2.wait();
            }
            assertEquals(BKException.Code.OK, syncObj2.rc);
        }

        // Reading ledger till the last entry
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
    }

    /**
     * Skips few entries before closing the ledger and assert that the
     * lastAddConfirmed is right before our skipEntryId.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateAdvWithSkipEntries() throws Exception {
        long ledgerId;
        SyncObj syncObj1 = new SyncObj();

        // Create a ledger
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        // Save ledgerId to reopen the ledger
        ledgerId = lh.getId();
        LOG.info("Ledger ID: " + ledgerId);
        int skipEntryId = rng.nextInt(numEntriesToWrite - 1);
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (i == skipEntryId) {
                LOG.info("Skipping entry:{}", skipEntryId);
                continue;
            }
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
        }
        // wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < skipEntryId) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Close the ledger
        lh.close();
        // Open the ledger
        lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
        assertEquals(lh.lastAddConfirmed, skipEntryId - 1);
        lh.close();
    }

    /**
     * Verify the functionality LedgerHandleAdv addEntry with duplicate entryIds.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateAdvSyncAddDuplicateEntryIds() throws Exception {
        // Create a ledger
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        LOG.info("Ledger ID: " + lh.getId());
        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);

            entries1.add(entry.array());
            lh.addEntry(i, entry.array());
            entry.position(0);
        }
        readEntries(lh, entries1);

        int dupEntryId = rng.nextInt(numEntriesToWrite - 1);

        try {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            lh.addEntry(dupEntryId, entry.array());
            fail("Expected exception not thrown");
        } catch (BKException e) {
            // This test expects DuplicateEntryIdException
            assertEquals(e.getCode(), BKException.Code.DuplicateEntryIdException);
        }
        lh.close();
    }

    /**
     * Verify the functionality LedgerHandleAdv asyncAddEntry with duplicate
     * entryIds.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerCreateAdvSyncAsyncAddDuplicateEntryIds() throws Exception {
        long ledgerId;
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();

        // Create a ledger
        lh = bkc.createLedgerAdv(5, 3, 2, digestType, ledgerPassword);
        // Save ledgerId to reopen the ledger
        ledgerId = lh.getId();
        LOG.info("Ledger ID: " + ledgerId);
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            try {
                entries1.add(0, entry.array());
            } catch (Exception e) {
                e.printStackTrace();
            }
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
            if (rng.nextBoolean()) {
                // Attempt to write the same entry
                lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj2);
                synchronized (syncObj2) {
                    while (syncObj2.counter < 1) {
                        syncObj2.wait();
                    }
                    assertEquals(BKException.Code.DuplicateEntryIdException, syncObj2.rc);
                }
            }
        }
        // Wait for all entries to be acknowledged for the first ledger
        synchronized (syncObj1) {
            while (syncObj1.counter < numEntriesToWrite) {
                syncObj1.wait();
            }
            assertEquals(BKException.Code.OK, syncObj1.rc);
        }
        // Close the ledger
        lh.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLedgerCreateAdvByteBufRefCnt() throws Exception {
        long ledgerId = rng.nextLong();
        ledgerId &= Long.MAX_VALUE;
        if (!baseConf.getLedgerManagerFactoryClass().equals(LongHierarchicalLedgerManagerFactory.class)) {
            // since LongHierarchicalLedgerManager supports ledgerIds of
            // decimal length upto 19 digits but other
            // LedgerManagers only upto 10 decimals
            ledgerId %= 9999999999L;
        }

        final LedgerHandle lh = bkc.createLedgerAdv(ledgerId, 5, 3, 2, digestType, ledgerPassword, null);

        final List<AbstractByteBufAllocator> allocs = Lists.newArrayList(
                new PooledByteBufAllocator(true),
                new PooledByteBufAllocator(false),
                new UnpooledByteBufAllocator(true),
                new UnpooledByteBufAllocator(false));

        long entryId = 0;
        for (AbstractByteBufAllocator alloc: allocs) {
            final ByteBuf data = alloc.buffer(10);
            data.writeBytes(("fragment0" + entryId).getBytes());
            assertEquals("ref count on ByteBuf should be 1", 1, data.refCnt());

            CompletableFuture<Integer> cf = new CompletableFuture<>();
            lh.asyncAddEntry(entryId, data, (rc, handle, eId, qwcLatency, ctx) -> {
                CompletableFuture<Integer> future = (CompletableFuture<Integer>) ctx;
                future.complete(rc);
            }, cf);

            int rc = cf.get();
            assertEquals("rc code is OK", BKException.Code.OK, rc);

            for (int i = 0; i < 10; i++) {
                if (data.refCnt() == 0) {
                    break;
                }
                TimeUnit.MILLISECONDS.sleep(250); // recycler runs asynchronously
            }
            assertEquals("writing entry with id " + entryId + ", ref count on ByteBuf should be 0 ",
                    0, data.refCnt());

            org.apache.bookkeeper.client.api.LedgerEntry e = lh.read(entryId, entryId).getEntry(entryId);
            assertEquals("entry data is correct", "fragment0" + entryId, new String(e.getEntryBytes()));
            entryId++;
        }

        bkc.deleteLedger(lh.ledgerId);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLedgerCreateByteBufRefCnt() throws Exception {
        final LedgerHandle lh = bkc.createLedger(5, 3, 2, digestType, ledgerPassword, null);

        final List<AbstractByteBufAllocator> allocs = Lists.newArrayList(
                new PooledByteBufAllocator(true),
                new PooledByteBufAllocator(false),
                new UnpooledByteBufAllocator(true),
                new UnpooledByteBufAllocator(false));

        int entryId = 0;
        for (AbstractByteBufAllocator alloc: allocs) {
            final ByteBuf data = alloc.buffer(10);
            data.writeBytes(("fragment0" + entryId).getBytes());
            assertEquals("ref count on ByteBuf should be 1", 1, data.refCnt());

            CompletableFuture<Integer> cf = new CompletableFuture<>();
            lh.asyncAddEntry(data, (rc, handle, eId, ctx) -> {
                CompletableFuture<Integer> future = (CompletableFuture<Integer>) ctx;
                future.complete(rc);
            }, cf);

            int rc = cf.get();
            assertEquals("rc code is OK", BKException.Code.OK, rc);

            for (int i = 0; i < 10; i++) {
                if (data.refCnt() == 0) {
                    break;
                }
                TimeUnit.MILLISECONDS.sleep(250); // recycler runs asynchronously
            }
            assertEquals("writing entry with id " + entryId + ", ref count on ByteBuf should be 0 ",
                    0, data.refCnt());

            org.apache.bookkeeper.client.api.LedgerEntry e = lh.read(entryId, entryId).getEntry(entryId);
            assertEquals("entry data is correct", "fragment0" + entryId, new String(e.getEntryBytes()));
            entryId++;
        }

        bkc.deleteLedger(lh.ledgerId);
    }

    @Test
    public void testReadLacNotSameWithMetadataLedgerReplication() throws Exception {
       lh = bkc.createLedger(3, 3, 2, digestType, ledgerPassword);
        for (int i = 0; i < 10; ++i) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            lh.addEntry(entry.array());
        }

        List<BookieId> ensemble = lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next().getValue();
        assertEquals(1, lh.getLedgerMetadata().getAllEnsembles().size());
        killBookie(ensemble.get(1));

        try {
            lh.ensembleChangeLoop(ensemble, Collections.singletonMap(1, ensemble.get(1)));
        } catch (Exception e) {
            fail();
        }

        LedgerHandle lh1 = bkc.openLedgerNoRecovery(lh.ledgerId, digestType, ledgerPassword);
        assertEquals(2, lh1.getLedgerMetadata().getAllEnsembles().size());
        List<BookieId> firstEnsemble = lh1.getLedgerMetadata().getAllEnsembles().firstEntry().getValue();

        long entryId = lh1.getLedgerMetadata().getAllEnsembles().lastEntry().getKey() - 1;
        try {
            lh1.readAsync(entryId, entryId).get();
            fail();
        } catch (Exception e) {
            LOG.info("Failed to read entry: {} ", entryId, e);
        }

        MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(
            URI.create(baseConf.getMetadataServiceUri()));
        driver.initialize(
            baseConf,
            NullStatsLogger.INSTANCE);
        // initialize urReplicationManager
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30));


        ReplicationWorker replicationWorker = new ReplicationWorker(baseConf);
        replicationWorker.start();
        String basePath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseClientConf) + '/'
            + BookKeeperConstants.UNDER_REPLICATION_NODE
            + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;

        try {
            underReplicationManager.markLedgerUnderreplicated(lh1.getId(), ensemble.get(1).toString());

            Awaitility.waitAtMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertFalse(ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh1.getId(), basePath))
            );

            assertNotEquals(firstEnsemble, lh1.getLedgerMetadata().getAllEnsembles().firstEntry().getValue());
        } finally {
            replicationWorker.shutdown();
        }
    }

    @Test
    public void testLedgerMetadataTest() throws Exception {
        baseClientConf.setLedgerMetadataFormatVersion(LedgerMetadataSerDe.METADATA_FORMAT_VERSION_2);
        BookKeeperTestClient bkc = new BookKeeperTestClient(baseClientConf, new TestStatsProvider());
        // Create a ledger
        lh = bkc.createLedger(3, 3, 2, digestType, ledgerPassword);
        assertEquals(lh.getLedgerMetadata().getMetadataFormatVersion(), LedgerMetadataSerDe.METADATA_FORMAT_VERSION_2);
        lh.close();
    }

    private void readEntries(LedgerHandle lh, List<byte[]> entries) throws InterruptedException, BKException {
        ls = lh.readEntries(0, numEntriesToWrite - 1);
        int index = 0;
        while (ls.hasMoreElements()) {
            ByteBuffer origbb = ByteBuffer.wrap(entries.get(index++));
            Integer origEntry = origbb.getInt();
            ByteBuffer result = ByteBuffer.wrap(ls.nextElement().getEntry());
            Integer retrEntry = result.getInt();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Length of result: " + result.capacity());
                LOG.debug("Original entry: " + origEntry);
                LOG.debug("Retrieved entry: " + retrEntry);
            }
            assertTrue("Checking entry " + index + " for equality", origEntry
                    .equals(retrEntry));
        }
    }

    private void readEntries(ReadHandle reader, List<ByteBuf> entries) throws Exception {
        assertEquals("Not enough entries in ledger " + reader.getId(),
                     reader.getLastAddConfirmed(), entries.size() - 1);
        try (LedgerEntries readEntries = reader.read(0, reader.getLastAddConfirmed())) {
            int i = 0;
            for (org.apache.bookkeeper.client.api.LedgerEntry e : readEntries) {
                int entryId = i++;
                ByteBuf origEntry = entries.get(entryId);
                ByteBuf readEntry = e.getEntryBuffer();
                assertEquals("Unexpected contents in " + reader.getId() + ":" + entryId, origEntry, readEntry);
            }
        }
    }

    private void readEntriesAndValidateDataArray(LedgerHandle lh, List<byte[]> entries)
            throws InterruptedException, BKException {
        ls = lh.readEntries(0, entries.size() - 1);
        int index = 0;
        while (ls.hasMoreElements()) {
            byte[] originalData = entries.get(index++);
            byte[] receivedData = ls.nextElement().getEntry();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Length of originalData: {}", originalData.length);
                LOG.debug("Length of receivedData: {}", receivedData.length);
            }
            assertEquals(
                    String.format("LedgerID: %d EntryID: %d OriginalDataLength: %d ReceivedDataLength: %d", lh.getId(),
                            (index - 1), originalData.length, receivedData.length),
                    originalData.length, receivedData.length);
            Assert.assertArrayEquals(
                    String.format("Checking LedgerID: %d EntryID: %d  for equality", lh.getId(), (index - 1)),
                    originalData, receivedData);
        }
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        SyncObj x = (SyncObj) ctx;
        synchronized (x) {
            x.rc = rc;
            x.counter++;
            x.notify();
        }
    }

    static class CorruptReadBookie extends TestBookieImpl {

        static final Logger LOG = LoggerFactory.getLogger(CorruptReadBookie.class);
        ByteBuf localBuf;

        public CorruptReadBookie(ServerConfiguration conf)
                throws Exception {
            super(conf);
        }

        @Override
        public ByteBuf readEntry(long ledgerId, long entryId) throws IOException, NoLedgerException, BookieException {
            localBuf = super.readEntry(ledgerId, entryId);

            int capacity = 0;
            while (capacity < localBuf.capacity()) {
                localBuf.setByte(capacity, 0);
                capacity++;
            }
            return localBuf;
        }

    }
}
