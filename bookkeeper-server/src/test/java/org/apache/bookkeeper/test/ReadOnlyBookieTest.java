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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.PortManager;
import org.awaitility.Awaitility;
import org.junit.Test;

/**
 * Test to verify the readonly feature of bookies.
 */
public class ReadOnlyBookieTest extends BookKeeperClusterTestCase {

    public ReadOnlyBookieTest() {
        super(2);
        baseConf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        baseConf.setEntryLogFilePreAllocationEnabled(false);
        baseConf.setMinUsableSizeForEntryLogCreation(Long.MAX_VALUE);
    }

    /**
     * Check readonly bookie.
     */
    @Test
    public void testBookieShouldServeAsReadOnly() throws Exception {
        killBookie(0);
        baseConf.setReadOnlyModeEnabled(true);
        startNewBookie();
        LedgerHandle ledger = bkc.createLedger(2, 2, DigestType.MAC,
                "".getBytes());

        // Check new bookie with readonly mode enabled.
        File[] ledgerDirs = confByIndex(1).getLedgerDirs();
        assertEquals("Only one ledger dir should be present", 1,
                ledgerDirs.length);
        Bookie bookie = serverByIndex(1).getBookie();
        LedgerDirsManager ledgerDirsManager = ((BookieImpl) bookie).getLedgerDirsManager();

        for (int i = 0; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }

        // Now add the current ledger dir to filled dirs list
        ledgerDirsManager.addToFilledDirs(new File(ledgerDirs[0], "current"));

        try {
            ledger.addEntry("data".getBytes());
            fail("Should fail to add entry since there isn't enough bookies alive.");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // Expected
        }

        assertTrue("Bookie should be running and converted to readonly mode",
                bookie.isRunning() && bookie.isReadOnly());

        // Now kill the other bookie and read entries from the readonly bookie
        killBookie(0);

        Enumeration<LedgerEntry> readEntries = ledger.readEntries(0, 9);
        while (readEntries.hasMoreElements()) {
            LedgerEntry entry = readEntries.nextElement();
            assertEquals("Entry should contain correct data", "data",
                    new String(entry.getEntry()));
        }
    }

    @Test
    public void testBookieShouldTurnWritableFromReadOnly() throws Exception {
        killBookie(0);
        baseConf.setReadOnlyModeEnabled(true);
        baseConf.setDiskCheckInterval(Integer.MAX_VALUE);
        startNewBookie();
        LedgerHandle ledger = bkc.createLedger(2, 2, DigestType.MAC,
                "".getBytes());

        // Check new bookie with readonly mode enabled.
        File[] ledgerDirs = confByIndex(1).getLedgerDirs();
        assertEquals("Only one ledger dir should be present", 1,
                ledgerDirs.length);
        BookieImpl bookie = (BookieImpl) serverByIndex(1).getBookie();
        LedgerDirsManager ledgerDirsManager = bookie.getLedgerDirsManager();

        for (int i = 0; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }

        File testDir = new File(ledgerDirs[0], "current");

        // Now add the current ledger dir to filled dirs list
        ledgerDirsManager.addToFilledDirs(testDir);

        try {
            ledger.addEntry("data".getBytes());
            fail("Should fail to add entry since there isn't enough bookies alive.");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // Expected
        }

        // waitForReadOnlyBookie adds another listener thread to observe the node status of bookie,
        // which may be out of sync with the triggering of node changes in EnsemblePlacementPolicy.
        // This sequence leads to flaky test. So change from watching zk to Awaitility.await().
        Awaitility.await().untilAsserted(() -> {
            assertTrue("Bookie should be running and converted to readonly mode",
                    bookie.isRunning() && bookie.isReadOnly());
        });
        LOG.info("bookie is running {}, readonly {}.", bookie.isRunning(), bookie.isReadOnly());

        // should fail to create ledger
        try {
            bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
            fail("Should fail to create a ledger since there isn't enough bookies alive.");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // Expected.
        }

        // Now add the current ledger dir back to writable dirs list
        ledgerDirsManager.addToWritableDirs(testDir, true);

        // waitForWritableBookie adds another listener thread to observe the node status of bookie,
        // which may be out of sync with the triggering of node changes in EnsemblePlacementPolicy.
        // This sequence leads to flaky test. So change from watching zk to Awaitility.await().
        Awaitility.await().untilAsserted(() -> {
            assertTrue("Bookie should be running and converted back to writable mode", bookie.isRunning()
                    && !bookie.isReadOnly());
        });
        LOG.info("bookie is running {}, readonly {}.", bookie.isRunning(), bookie.isReadOnly());

        LedgerHandle newLedger = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        for (int i = 0; i < 10; i++) {
            newLedger.addEntry("data".getBytes());
        }
        Enumeration<LedgerEntry> readEntries = newLedger.readEntries(0, 9);
        while (readEntries.hasMoreElements()) {
            LedgerEntry entry = readEntries.nextElement();
            assertEquals("Entry should contain correct data", "data", new String(entry.getEntry()));
        }
    }

    /**
     * check readOnlyModeEnabled=false.
     */
    @Test
    public void testBookieShutdownIfReadOnlyModeNotEnabled() throws Exception {
        killBookie(1);
        baseConf.setReadOnlyModeEnabled(false);
        startNewBookie();

        File[] ledgerDirs = confByIndex(1).getLedgerDirs();
        assertEquals("Only one ledger dir should be present", 1,
                ledgerDirs.length);
        BookieImpl bookie = (BookieImpl) serverByIndex(1).getBookie();
        LedgerHandle ledger = bkc.createLedger(2, 2, DigestType.MAC,
                "".getBytes());
        LedgerDirsManager ledgerDirsManager = bookie.getLedgerDirsManager();

        for (int i = 0; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }

        // Now add the current ledger dir to filled dirs list
        ledgerDirsManager.addToFilledDirs(new File(ledgerDirs[0], "current"));

        try {
            ledger.addEntry("data".getBytes());
            fail("Should fail to add entry since there isn't enough bookies alive.");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // Expected
        }

        // wait for up to 10 seconds for bookie to shut down
        for (int i = 0; i < 10 && bookie.isAlive(); i++) {
            Thread.sleep(1000);
        }
        assertFalse("Bookie should shutdown if readOnlyMode not enabled",
                bookie.isAlive());
    }

    /**
     * Check multiple ledger dirs.
     */
    @Test
    public void testBookieContinueWritingIfMultipleLedgersPresent()
            throws Exception {
        startNewBookieWithMultipleLedgerDirs(2);

        File[] ledgerDirs = confByIndex(1).getLedgerDirs();
        assertEquals("Only one ledger dir should be present", 2,
                ledgerDirs.length);
        BookieImpl bookie = (BookieImpl) serverByIndex(1).getBookie();
        LedgerHandle ledger = bkc.createLedger(2, 2, DigestType.MAC,
                "".getBytes());
        LedgerDirsManager ledgerDirsManager = bookie.getLedgerDirsManager();

        for (int i = 0; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }

        // Now add the current ledger dir to filled dirs list
        ledgerDirsManager.addToFilledDirs(new File(ledgerDirs[0], "current"));
        for (int i = 0; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }
        assertEquals("writable dirs should have one dir", 1, ledgerDirsManager
                .getWritableLedgerDirs().size());
        assertTrue("Bookie should shutdown if readOnlyMode not enabled",
                bookie.isAlive());
    }

    private void startNewBookieWithMultipleLedgerDirs(int numOfLedgerDirs)
            throws Exception {
        ServerConfiguration conf = confByIndex(1);
        killBookie(1);

        File[] ledgerDirs = new File[numOfLedgerDirs];
        for (int i = 0; i < numOfLedgerDirs; i++) {
            File dir = tmpDirs.createNew("bookie", "test");
            ledgerDirs[i] = dir;
        }

        ServerConfiguration newConf = newServerConfiguration(
                PortManager.nextFreePort(),
                ledgerDirs[0], ledgerDirs);
        newConf.setDiskCheckInterval(Integer.MAX_VALUE);
        startAndAddBookie(newConf);
    }

    /**
     * Test ledger creation with readonly bookies.
     */
    @Test
    public void testLedgerCreationShouldFailWithReadonlyBookie() throws Exception {
        killBookie(1);
        baseConf.setReadOnlyModeEnabled(true);
        startNewBookie();

        serverByIndex(1).getBookie().getStateManager().transitionToReadOnlyMode().get();
        try {
            bkc.waitForReadOnlyBookie(BookieImpl.getBookieId(confByIndex(1)))
                .get(30, TimeUnit.SECONDS);

            bkc.createLedger(2, 2, DigestType.CRC32, "".getBytes());
            fail("Must throw exception, as there is one readonly bookie");
        } catch (BKException e) {
            // Expected
        }
    }

    /**
     * Try to read closed ledger from restarted ReadOnlyBookie.
     */
    public void testReadFromReadOnlyBookieShouldBeSuccess() throws Exception {
        LedgerHandle ledger = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        for (int i = 0; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }
        ledger.close();
        confByIndex(1).setReadOnlyModeEnabled(true);
        confByIndex(1).setDiskCheckInterval(500);
        restartBookies();

        // Check new bookie with readonly mode enabled.
        File[] ledgerDirs = confByIndex(1).getLedgerDirs();
        assertEquals("Only one ledger dir should be present", 1, ledgerDirs.length);
        BookieImpl bookie = (BookieImpl) serverByIndex(1).getBookie();
        LedgerDirsManager ledgerDirsManager = bookie.getLedgerDirsManager();

        // Now add the current ledger dir to filled dirs list
        ledgerDirsManager.addToFilledDirs(new File(ledgerDirs[0], "current"));

        // Wait till Bookie converts to ReadOnly mode.
        Thread.sleep(1000);
        assertTrue("Bookie should be converted to readonly mode", bookie.isRunning() && bookie.isReadOnly());

        // Now kill the other bookie and read entries from the readonly bookie
        killBookie(0);

        Enumeration<LedgerEntry> readEntries = ledger.readEntries(0, 9);
        while (readEntries.hasMoreElements()) {
            LedgerEntry entry = readEntries.nextElement();
            assertEquals("Entry should contain correct data", "data", new String(entry.getEntry()));
        }
    }
}
