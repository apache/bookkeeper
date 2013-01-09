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

import java.util.Enumeration;
import java.util.List;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests that index corruption cases
 */
public class IndexCorruptionTest extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(IndexCorruptionTest.class);

    DigestType digestType;

    int pageSize = 1024;

    public IndexCorruptionTest() {
        super(1);
        this.digestType = DigestType.CRC32;
        baseConf.setPageSize(pageSize);
    }

    @Test(timeout=60000)
    public void testNoSuchLedger() throws Exception {
        LOG.debug("Testing NoSuchLedger");

        Bookie.SyncThread syncThread = bs.get(0).getBookie().syncThread;
        syncThread.suspendSync();
        // Create a ledger
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, "".getBytes());

        // Close the ledger which cause a readEntry(0) call
        LedgerHandle newLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());

        // Create a new ledger to write entries
        String dummyMsg = "NoSuchLedger";
        int numMsgs = 3;
        LedgerHandle wlh = bkc.createLedger(1, 1, digestType, "".getBytes());
        for (int i=0; i<numMsgs; i++) {
            wlh.addEntry(dummyMsg.getBytes());
        }

        syncThread.resumeSync();

        // trigger sync
        Thread.sleep(2 * baseConf.getFlushInterval());

        // restart bookies
        restartBookies();

        Enumeration<LedgerEntry> seq = wlh.readEntries(0, numMsgs - 1);
        assertTrue("Enumeration of ledger entries has no element", seq.hasMoreElements() == true);
        int entryId = 0;
        while (seq.hasMoreElements()) {
            LedgerEntry e = seq.nextElement();
            assertEquals(entryId, e.getEntryId());

            Assert.assertArrayEquals(dummyMsg.getBytes(), e.getEntry());
            ++entryId;
        }
        assertEquals(entryId, numMsgs);
    }

    @Test(timeout=60000)
    public void testEmptyIndexPage() throws Exception {
        LOG.debug("Testing EmptyIndexPage");

        Bookie.SyncThread syncThread = bs.get(0).getBookie().syncThread;
        assertNotNull("Not found SyncThread.", syncThread);

        syncThread.suspendSync();

        // Create a ledger
        LedgerHandle lh1 = bkc.createLedger(1, 1, digestType, "".getBytes());

        String dummyMsg = "NoSuchLedger";

        // write two page entries to ledger 2
        int numMsgs = 2 * pageSize / 8;
        LedgerHandle lh2 = bkc.createLedger(1, 1, digestType, "".getBytes());
        for (int i=0; i<numMsgs; i++) {
            lh2.addEntry(dummyMsg.getBytes());
        }

        syncThread.resumeSync();

        // trigger sync
        Thread.sleep(2 * baseConf.getFlushInterval());

        syncThread.suspendSync();

        // Close ledger 1 which cause a readEntry(0) call
        LedgerHandle newLh1 = bkc.openLedger(lh1.getId(), digestType, "".getBytes());

        // write another 3 entries to ledger 2
        for (int i=0; i<3; i++) {
            lh2.addEntry(dummyMsg.getBytes());
        }

        syncThread.resumeSync();

        // wait for sync again
        Thread.sleep(2 * baseConf.getFlushInterval());

        // restart bookies
        restartBookies();

        numMsgs += 3;
        Enumeration<LedgerEntry> seq = lh2.readEntries(0, numMsgs - 1);
        assertTrue("Enumeration of ledger entries has no element", seq.hasMoreElements() == true);
        int entryId = 0;
        while (seq.hasMoreElements()) {
            LedgerEntry e = seq.nextElement();
            assertEquals(entryId, e.getEntryId());

            Assert.assertArrayEquals(dummyMsg.getBytes(), e.getEntry());
            ++entryId;
        }
        assertEquals(entryId, numMsgs);
    }
}
