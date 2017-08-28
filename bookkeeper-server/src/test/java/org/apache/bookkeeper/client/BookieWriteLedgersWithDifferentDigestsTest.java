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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Random;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Verify reads from ledgers with different digest types.
 * This can happen as result of clients using different settings
 * yet reading each other data or configuration change roll out.
 */
@RunWith(Parameterized.class)
public class BookieWriteLedgersWithDifferentDigestsTest extends
    BookKeeperClusterTestCase implements AddCallback {

    private final static Logger LOG = LoggerFactory
            .getLogger(BookieWriteLedgersWithDifferentDigestsTest.class);

    byte[] ledgerPassword = "aaa".getBytes();
    LedgerHandle lh, lh2;
    Enumeration<LedgerEntry> ls;

    // test related variables
    final int numEntriesToWrite = 20;
    int maxInt = Integer.MAX_VALUE;
    Random rng;
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

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { {DigestType.MAC }, {DigestType.CRC32}});
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
        String ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    @Test
    public void testLedgersWithDifferentDigestTypesNoAutodetection() throws Exception {
    	bkc.conf.setEnableDigestTypeAutodetection(false);
        // Create ledgers
        lh = bkc.createLedgerAdv(3, 2, 2, DigestType.MAC, ledgerPassword);
        
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
        
        try {
	        bkc.openLedgerNoRecovery(id, DigestType.CRC32, ledgerPassword).close();
	        fail("digest mismatch error is expected");
        } catch (BKException bke) {
        	// expected
        }
    }

    @Test
    public void testLedgersWithDifferentDigestTypesWithAutodetection() throws Exception {
    	bkc.conf.setEnableDigestTypeAutodetection(true);
        // Create ledgers
        lh = bkc.createLedgerAdv(3, 2, 2, DigestType.MAC, ledgerPassword);
        lh2 = bkc.createLedgerAdv(3, 2, 2, DigestType.CRC32, ledgerPassword);
        
        final long id = lh.ledgerId;
        final long id2 = lh2.ledgerId;

        LOG.info("Ledger ID-1: " + lh.getId());
        LOG.info("Ledger ID-2: " + lh2.getId());
        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        for (int i = numEntriesToWrite - 1; i >= 0; i--) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(maxInt));
            entry.position(0);
            entries1.add(0, entry.array());
            entries2.add(0, entry.array());
            lh.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj1);
            lh2.asyncAddEntry(i, entry.array(), 0, entry.capacity(), this, syncObj2);
        }

        // Wait for all entries to be acknowledged
        waitForEntriesAddition(syncObj1, numEntriesToWrite);
        waitForEntriesAddition(syncObj2, numEntriesToWrite);

        // Reads here work ok because ledger uses digest type set during create
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
        
        // open here would fail if provided digest type is used
        // it passes because ledger just uses digest type from its metadata/autodetects it
        lh = bkc.openLedgerNoRecovery(id, DigestType.CRC32, ledgerPassword);
        lh2 = bkc.openLedgerNoRecovery(id2, DigestType.MAC, ledgerPassword);
        readEntries(lh, entries1);
        readEntries(lh2, entries2);
        lh.close();
        lh2.close();
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
            LOG.debug("Length of result: " + result.capacity());
            LOG.debug("Original entry: " + origEntry);
            Integer retrEntry = result.getInt();
            LOG.debug("Retrieved entry: " + retrEntry);
            assertTrue("Checking entry " + index + " for equality", origEntry
                    .equals(retrEntry));
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
}
