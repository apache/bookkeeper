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

import static org.apache.bookkeeper.bookie.BookieException.Code.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Random;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify reads from ledgers with different digest types.
 * This can happen as result of clients using different settings
 * yet reading each other data or configuration change roll out.
 */
@RunWith(Parameterized.class)
public class BookieWriteLedgersWithDifferentDigestsTest extends
    BookKeeperClusterTestCase implements AsyncCallback.AddCallbackWithLatency {

    private static final Logger LOG = LoggerFactory
            .getLogger(BookieWriteLedgersWithDifferentDigestsTest.class);

    byte[] ledgerPassword = "aaa".getBytes();
    LedgerHandle lh;
    Enumeration<LedgerEntry> ls;

    // test related variables
    final int numEntriesToWrite = 20;
    int maxInt = Integer.MAX_VALUE;
    Random rng;
    ArrayList<byte[]> entries1; // generated entries
    ArrayList<byte[]> entries2; // generated entries

    private final DigestType digestType;
    private final DigestType otherDigestType;

    private static class SyncObj {
        volatile int counter;
        volatile int rc;

        public SyncObj() {
            counter = 0;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { {DigestType.MAC }, {DigestType.CRC32}, {DigestType.CRC32C} });
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        rng = new Random(System.currentTimeMillis()); // Initialize the Random
        // Number Generator
        entries1 = new ArrayList<byte[]>(); // initialize the entries list
        entries2 = new ArrayList<byte[]>(); // initialize the entries list
    }

    public BookieWriteLedgersWithDifferentDigestsTest(DigestType digestType) {
        super(3);
        this.digestType = digestType;
        this.otherDigestType = digestType == DigestType.CRC32 ? DigestType.MAC : DigestType.CRC32;
        String ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    @Test
    public void testLedgersWithDifferentDigestTypesNoAutodetection() throws Exception {
        bkc.conf.setEnableDigestTypeAutodetection(false);
        // Create ledgers
        lh = bkc.createLedgerAdv(3, 2, 2, digestType, ledgerPassword);

        final long id = lh.ledgerId;

        LOG.info("Ledger ID: {}, digestType: {}", lh.getId(), digestType);
        SyncObj syncObj1 = new SyncObj();
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            entries1.add(0, entry.array());
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
        }

        // Wait for all entries to be acknowledged
        waitForEntriesAddition(syncObj1, numEntriesToWrite);

        // Reads here work ok because ledger uses digest type set during create
        readEntries(lh, entries1);
        lh.close();

        try {
            bkc.openLedgerNoRecovery(id, otherDigestType, ledgerPassword).close();
            fail("digest mismatch error is expected");
        } catch (BKException bke) {
            // expected
        }
    }

    @Test
    public void testLedgersWithDifferentDigestTypesWithAutodetection() throws Exception {
        bkc.conf.setEnableDigestTypeAutodetection(true);
        // Create ledgers
        lh = bkc.createLedgerAdv(3, 2, 2, digestType, ledgerPassword);

        final long id = lh.ledgerId;

        LOG.info("Ledger ID-1: " + lh.getId());
        SyncObj syncObj1 = new SyncObj();
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            entries1.add(0, entry.array());
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
        }

        // Wait for all entries to be acknowledged
        waitForEntriesAddition(syncObj1, numEntriesToWrite);

        // Reads here work ok because ledger uses digest type set during create
        readEntries(lh, entries1);
        lh.close();

        // open here would fail if provided digest type is used
        // it passes because ledger just uses digest type from its metadata/autodetects it
        lh = bkc.openLedgerNoRecovery(id, otherDigestType, ledgerPassword);
        readEntries(lh, entries1);
        lh.close();
    }

    private void waitForEntriesAddition(SyncObj syncObj, int numEntriesToWrite) throws InterruptedException {
        synchronized (syncObj) {
            while (syncObj.counter < numEntriesToWrite) {
                syncObj.wait();
            }
            assertEquals(BKException.Code.OK, syncObj.rc);
        }
    }

    private void readEntries(LedgerHandle lh, ArrayList<byte[]> entries) throws InterruptedException, BKException {
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

    @Override
    public void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, long qwcLatency, Object ctx) {
        SyncObj x = (SyncObj) ctx;
        captureThrowable(() -> {
            assertTrue("Successful write should have non-zero latency", rc != OK || qwcLatency > 0);
        });
        synchronized (x) {
            x.rc = rc;
            x.counter++;
            x.notify();
        }
    }
}
