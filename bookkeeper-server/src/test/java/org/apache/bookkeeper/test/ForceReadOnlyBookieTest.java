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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Enumeration;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to verify force start readonly bookie.
 */
public class ForceReadOnlyBookieTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ForceReadOnlyBookieTest.class);
    public ForceReadOnlyBookieTest() {
        super(2);
        baseConf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        baseConf.setEntryLogFilePreAllocationEnabled(false);
    }

    /**
     * Check force start readonly bookie.
     */
    @Test
    public void testBookieForceStartAsReadOnly() throws Exception {
        // create ledger, add entries
        LedgerHandle ledger = bkc.createLedger(2, 2, DigestType.MAC,
                "".getBytes());
        for (int i = 0; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }
        ledger.close();
        LOG.info("successed prepare");

        // start bookie 1 as readonly
        confByIndex(1).setReadOnlyModeEnabled(true);
        confByIndex(1).setForceReadOnlyBookie(true);
        restartBookies();
        Bookie bookie = serverByIndex(1).getBookie();

        assertTrue("Bookie should be running and in readonly mode",
                bookie.isRunning() && bookie.isReadOnly());
        LOG.info("successed force start ReadOnlyBookie");

        // Check new bookie with readonly mode enabled.
        File[] ledgerDirs = confByIndex(1).getLedgerDirs();
        assertEquals("Only one ledger dir should be present", 1, ledgerDirs.length);

        // kill the writable bookie
        killBookie(0);
        // read entry from read only bookie
        Enumeration<LedgerEntry> readEntries = ledger.readEntries(0, 9);
        while (readEntries.hasMoreElements()) {
            LedgerEntry entry = readEntries.nextElement();
            assertEquals("Entry should contain correct data", "data",
                    new String(entry.getEntry()));
        }
        LOG.info("successed read entry from ReadOnlyBookie");

        // test will not transfer to Writable mode.
        LedgerDirsManager ledgerDirsManager = ((BookieImpl) bookie).getLedgerDirsManager();
        ledgerDirsManager.addToWritableDirs(new File(ledgerDirs[0], "current"), true);
        assertTrue("Bookie should be running and in readonly mode",
                bookie.isRunning() && bookie.isReadOnly());
        LOG.info("successed: bookie still readonly");
    }
}
