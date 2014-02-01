package org.apache.bookkeeper.bookie;

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

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Enumeration;

public class GarbageCollectionTest extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(GarbageCollectionTest.class);

    static final int NUM_BOOKIES = 1;
    static final int ENTRY_OVERHEAD_SIZE = 44; // Metadata + CRC + Length
    static final int ENTRY_SIZE = 1024;

    final DigestType digestType;
    final int numEntries;
    final String msg;

    public GarbageCollectionTest() {
        super(NUM_BOOKIES);
        this.digestType = DigestType.CRC32;
        this.numEntries = 100;

        // a dummy message
        StringBuilder msgSB = new StringBuilder();
        for (int i = 0; i < ENTRY_SIZE - ENTRY_OVERHEAD_SIZE; i++) {
            msgSB.append("a");
        }
        msg = msgSB.toString();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        baseConf.setEntryLogSizeLimit(numEntries * ENTRY_SIZE);
        baseConf.setSortedLedgerStorageEnabled(false);
        baseConf.setEntryLogFilePreAllocationEnabled(false);

        super.setUp();
    }

    LedgerHandle prepareData(int numEntries)
            throws Exception {
        LedgerHandle lh = bkc.createLedger(NUM_BOOKIES, NUM_BOOKIES, digestType, "".getBytes());
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(msg.getBytes());
        }
        return lh;
    }

    private void verifyLedger(long lid, long startEntryId, long endEntryId) throws Exception {
        LedgerHandle lh = bkc.openLedger(lid, digestType, "".getBytes());
        Enumeration<LedgerEntry> entries = lh.readEntries(startEntryId, endEntryId);
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals(msg, new String(entry.getEntry()));
        }
    }

    @Test(timeout = 60000)
    public void testReclaimSpaceDuringStartup() throws Exception {
        int totalEntries = 10 * numEntries;

        // Prepare data
        LedgerHandle lh = prepareData(totalEntries);
        lh.close();

        ServerConfiguration killConf = killBookie(0);
        File[] ledgerDirs = killConf.getLedgerDirs();
        assertEquals(1, ledgerDirs.length);
        long usableSpace = ledgerDirs[0].getUsableSpace();
        long totalSpace = ledgerDirs[0].getTotalSpace();
        killConf.setDiskUsageThreshold((1f - ((float) (usableSpace + numEntries * ENTRY_SIZE) / (float) totalSpace)));
        killConf.setDiskUsageWarnThreshold((1f - ((float) (usableSpace + 2 * numEntries * ENTRY_SIZE) / (float) totalSpace)));

        try {
            new Bookie(killConf);
            fail("Should fail as no space to reclaim during startup.");
        } catch (NoWritableLedgerDirException nwlde) {
            // expected
        }
        // entry logs ([0,1].log) should not be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([0,1].log that should not be deleted in ledgerDirectory: "
                       + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1));
        }

        LOG.info("Deleting ledger {}.", lh.getId());

        // delete the ledger
        bkc.deleteLedger(lh.getId());

        LOG.info("Deleted ledger {}.", lh.getId());

        ServerConfiguration newConf = new ServerConfiguration(baseConf);
        newConf.setBookiePort(killConf.getBookiePort());
        newConf.setZkServers(killConf.getZkServers());
        newConf.setJournalDirName(killConf.getJournalDirName());
        newConf.setLedgerDirNames(killConf.getLedgerDirNames());
        newConf.setDiskUsageThreshold(0.95f);
        newConf.setDiskUsageWarnThreshold(0.90f);

        new Bookie(newConf);

        // entry logs ([0,1].log) should not be garbage collected if all the disks are still writable
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([0,1].log that should not be deleted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1));
        }

        // should be able to reclaim disk space after restarted.
        try {
            new Bookie(killConf);
        } catch (NoWritableLedgerDirException nwlde) {
            // we can't rely on checking disk usage in a laptop by deleting a few KB.
            // so it is possible the it still throwing no writable ledger dir.
        }
        // entry logs ([0,1].log) should not be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1].log that should be deleted in ledgerDirectory: "
                    + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1));
        }
    }
}
