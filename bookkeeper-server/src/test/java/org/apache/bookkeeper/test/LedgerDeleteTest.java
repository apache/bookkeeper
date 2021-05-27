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

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.util.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the ledger delete functionality both from the BookKeeper
 * client and the server side.
 */
public class LedgerDeleteTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerDeleteTest.class);
    DigestType digestType;

    public LedgerDeleteTest() {
        this("org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory");
    }

    LedgerDeleteTest(String ledgerManagerFactory) {
        super(1);
        LOG.info("Running test case using ledger manager : " + ledgerManagerFactory);
        this.digestType = DigestType.CRC32;
        // set ledger manager name
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseConf.setEntryLogSizeLimit(2 * 1024 * 1024L);
        baseConf.setGcWaitTime(1000);
        baseConf.setEntryLogFilePreAllocationEnabled(false);
        baseConf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        super.setUp();
    }

    /**
     * Common method to create ledgers and write entries to them.
     */
    private LedgerHandle[] writeLedgerEntries(int numLedgers, int msgSize, int numMsgs) throws Exception {
        // Create the ledgers
        LedgerHandle[] lhs = new LedgerHandle[numLedgers];
        for (int i = 0; i < numLedgers; i++) {
            lhs[i] = bkc.createLedger(1, 1, digestType, "".getBytes());
        }

        // Create a dummy message string to write as ledger entries
        StringBuilder msgSB = new StringBuilder();
        for (int i = 0; i < msgSize; i++) {
            msgSB.append("a");
        }
        String msg = msgSB.toString();
        final CountDownLatch completeLatch = new CountDownLatch(numMsgs * numLedgers);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        // Write all of the entries for all of the ledgers
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < numLedgers; j++) {
                lhs[j].asyncAddEntry(msg.getBytes(), new AddCallback() {
                    public void addComplete(int rc2, LedgerHandle lh, long entryId, Object ctx) {
                        rc.compareAndSet(BKException.Code.OK, rc2);
                        completeLatch.countDown();
                    }
                }, null);
            }
        }
        completeLatch.await();
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }

        // Return the ledger handles to the inserted ledgers and entries
        return lhs;
    }

    /**
     * This test writes enough ledger entries to roll over the entry log file.
     * It will then delete all of the ledgers from the client and let the
     * server's EntryLogger garbage collector thread delete the initial entry
     * log file.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerDelete() throws Exception {
        // Write enough ledger entries so that we roll over the initial entryLog (0.log)
        LedgerHandle[] lhs = writeLedgerEntries(3, 1024, 1024);
        // restart bookies to force rolling entry log files
        restartBookies();

        // Delete all of these ledgers from the BookKeeper client
        for (LedgerHandle lh : lhs) {
            bkc.deleteLedger(lh.getId());
        }
        LOG.info("Finished deleting all ledgers so waiting for the GC thread to clean up the entryLogs");
        Thread.sleep(2000);

        // Verify that the first entry log (0.log) has been deleted from all of the Bookie Servers.
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertFalse("Found the entry log file (0.log) that should have been deleted in ledgerDirectory: "
                + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0));
        }
    }

    /**
     * This test is similar to testLedgerDelete() except it will stop and
     * restart the Bookie Servers after it has written out the ledger entries.
     * On restart, there will be existing entry logs and ledger index files for
     * the EntryLogger and LedgerCache to read and store into memory.
     *
     * @throws Exception
     */
    @Test
    public void testLedgerDeleteWithExistingEntryLogs() throws Exception {
        // Write enough ledger entries so that we roll over the initial entryLog (0.log)
        LedgerHandle[] lhs = writeLedgerEntries(3, 1024, 1024);

        /*
         * Shutdown the Bookie Servers and restart them using the same ledger
         * directories. This will test the reading of pre-existing ledger index
         * files in the LedgerCache during startup of a Bookie Server.
         */
        restartBookies();

        // Delete all of these ledgers from the BookKeeper client
        for (LedgerHandle lh : lhs) {
            bkc.deleteLedger(lh.getId());
        }
        LOG.info("Finished deleting all ledgers so waiting for the GC thread to clean up the entryLogs");
        Thread.sleep(2 * baseConf.getGcWaitTime());

        /*
         * Verify that the first two entry logs ([0,1].log) have been deleted
         * from all of the Bookie Servers. When we restart the servers in this
         * test, a new entry log is created. We know then that the first two
         * entry logs should be deleted.
         */
        for (File ledgerDirectory : bookieLedgerDirs()) {
            assertFalse("Found the entry log file ([0,1].log) that should have been deleted in ledgerDirectory: "
                + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1));
        }
    }

}
