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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests that bookie rolling journals.
 */
public class BookieJournalRollingTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(BookieJournalRollingTest.class);

    private final DigestType digestType;

    public BookieJournalRollingTest() {
        super(1);
        this.digestType = DigestType.CRC32;
        this.baseConf.setAllowEphemeralPorts(false);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseConf.setMaxJournalSizeMB(1);
        baseConf.setMaxBackupJournals(2);
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Common method to create ledgers and write entries to them.
     */
    protected LedgerHandle[] writeLedgerEntries(int numLedgers, int msgSize, int numMsgs) throws Exception {
        // Create the ledgers
        LedgerHandle[] lhs = new LedgerHandle[numLedgers];
        long[] ledgerIds = new long[numLedgers];
        for (int i = 0; i < numLedgers; i++) {
            lhs[i] = bkc.createLedger(1, 1, digestType, "".getBytes());
            ledgerIds[i] = lhs[i].getId();
        }
        writeLedgerEntries(lhs, msgSize, numMsgs);
        // Return the ledger handles to the inserted ledgers and entries
        return lhs;
    }

    protected void writeLedgerEntries(LedgerHandle[] lhs, int msgSize, int numMsgs) throws Exception {
        // Create a dummy message string to write as ledger entries
        StringBuilder msgSB = new StringBuilder();
        for (int i = 0; i < msgSize; i++) {
            msgSB.append("a");
        }
        String msg = msgSB.toString();

        final CountDownLatch completeLatch = new CountDownLatch(numMsgs * lhs.length);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

        // Write all of the entries for all of the ledgers
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < lhs.length; j++) {
                StringBuilder sb = new StringBuilder();
                sb.append(lhs[j].getId()).append('-').append(i).append('-')
                  .append(msg);
                lhs[j].asyncAddEntry(sb.toString().getBytes(), new AddCallback() {
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
    }

    protected void validLedgerEntries(long[] ledgerIds, int msgSize, int numMsgs) throws Exception {
        // Open the ledgers
        LedgerHandle[] lhs = new LedgerHandle[ledgerIds.length];
        for (int i = 0; i < lhs.length; i++) {
            lhs[i] = bkc.openLedger(ledgerIds[i], digestType, "".getBytes());
        }

        StringBuilder msgSB = new StringBuilder();
        for (int i = 0; i < msgSize; i++) {
            msgSB.append("a");
        }
        String msg = msgSB.toString();

        int numToRead = 10;
        // read all of the entries for all the ledgers
        for (int j = 0; j < lhs.length; j++) {
            int start = 0;
            int read = Math.min(numToRead, numMsgs - start);
            int end = start + read - 1;
            int entryId = 0;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Validating Entries of Ledger " + ledgerIds[j]);
            }
            while (start < numMsgs) {
                Enumeration<LedgerEntry> seq = lhs[j].readEntries(start, end);
                assertTrue("Enumeration of ledger entries has no element", seq.hasMoreElements());
                while (seq.hasMoreElements()) {
                    LedgerEntry e = seq.nextElement();
                    assertEquals(entryId, e.getEntryId());

                    StringBuilder sb = new StringBuilder();
                    sb.append(ledgerIds[j]).append('-').append(entryId).append('-')
                      .append(msg);
                    assertArrayEquals(sb.toString().getBytes(), e.getEntry());
                    entryId++;
                }
                assertEquals(entryId - 1, end);
                start = end + 1;
                read = Math.min(numToRead, numMsgs - start);
                end = start + read - 1;
            }
        }

        for (LedgerHandle lh : lhs) {
            lh.close();
        }
    }

    /**
     * This test writes enough ledger entries to roll over the journals.
     *
     * <p>It will then keep only 1 journal file before last marked journal
     *
     * @throws Exception
     */
    @Test
    public void testJournalRolling() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Testing Journal Rolling");
        }
        // Write enough ledger entries so that we roll over journals
        LedgerHandle[] lhs = writeLedgerEntries(4, 1024, 1024);
        long[] ledgerIds = new long[lhs.length];
        for (int i = 0; i < lhs.length; i++) {
            ledgerIds[i] = lhs[i].getId();
            lhs[i].close();
        }

        Awaitility.await().untilAsserted(() -> {
                // verify that we only keep at most journal files
                for (File journalDir : bookieJournalDirs()) {
                    File[] journals = journalDir.listFiles();
                    int numJournals = 0;
                    for (File f : journals) {
                        if (!f.getName().endsWith(".txn")) {
                            continue;
                        }
                        ++numJournals;
                    }
                    assertTrue(numJournals <= 2);
                }
            }
        );

        // restart bookies
        // ensure after restart we can read the entries since journals rolls
        restartBookies();
        validLedgerEntries(ledgerIds, 1024, 1024);
    }

    /**
     * This test writes enough ledger entries to roll over the journals
     * without sync up.
     *
     * @throws Exception
     */
    @Test
    public void testJournalRollingWithoutSyncup() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Testing Journal Rolling without sync up");
        }

        // set flush interval to a large value
        // restart bookies
        restartBookies(c -> {
                c.setFlushInterval(999999999);
                c.setAllowEphemeralPorts(false);
                return c;
            });

        // Write enough ledger entries so that we roll over journals
        LedgerHandle[] lhs = writeLedgerEntries(4, 1024, 1024);
        long[] ledgerIds = new long[lhs.length];
        for (int i = 0; i < lhs.length; i++) {
            ledgerIds[i] = lhs[i].getId();
            lhs[i].close();
        }

        // ledger indexes are not flushed
        // and after bookies restarted, journals will be relayed
        // ensure that we can still read the entries
        restartBookies();
        validLedgerEntries(ledgerIds, 1024, 1024);
    }

    /**
     * This test writes enough ledger entries to roll over the journals
     * without sync up.
     *
     * @throws Exception
     */
    @Test
    public void testReplayDeletedLedgerJournalEntries() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Testing replaying journal entries whose ledger has been removed.");
        }

        // Write entries
        LedgerHandle[] lhs = writeLedgerEntries(1, 1024, 10);
        // Wait until all entries are flushed and last mark rolls
        Thread.sleep(3 * baseConf.getFlushInterval());

        // restart bookies with flush interval set to a large value
        // restart bookies
        restartBookies(c -> {
                c.setFlushInterval(999999999);
                c.setAllowEphemeralPorts(false);
                return c;
            });

        // Write entries again to let them existed in journal
        writeLedgerEntries(lhs, 1024, 10);

        // delete them
        for (LedgerHandle lh : lhs) {
            bkc.deleteLedger(lh.getId());
        }
        // wait for gc
        Thread.sleep(2 * confByIndex(0).getGcWaitTime());

        // restart bookies again to trigger replaying journal
        restartBookies();
    }

}
