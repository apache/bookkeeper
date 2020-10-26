/*
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
 */
package org.apache.bookkeeper.client;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.TestUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test cases for `Explicit Lac` feature.
 */
@RunWith(Parameterized.class)
public class ExplicitLacTest extends BookKeeperClusterTestCase {

    private final DigestType digestType;

    public ExplicitLacTest(Class<? extends LedgerStorage> storageClass) {
        super(1);
        this.digestType = DigestType.CRC32;
        baseConf.setLedgerStorageClass(storageClass.getName());
        /*
         * to persist explicitLac, journalFormatVersionToWrite should be atleast
         * V6 and fileInfoFormatVersionToWrite should be atleast V1
         */
        baseConf.setJournalFormatVersionToWrite(6);
        baseConf.setFileInfoFormatVersionToWrite(1);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { InterleavedLedgerStorage.class },
            { SortedLedgerStorage.class },
            { DbLedgerStorage.class },
        });
    }

    @Test
    public void testReadHandleWithNoExplicitLAC() throws Exception {
        ClientConfiguration confWithNoExplicitLAC = new ClientConfiguration();
        confWithNoExplicitLAC.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        confWithNoExplicitLAC.setExplictLacInterval(0);

        BookKeeper bkcWithNoExplicitLAC = new BookKeeper(confWithNoExplicitLAC);

        LedgerHandle wlh = bkcWithNoExplicitLAC.createLedger(
            1, 1, 1,
            digestType, "testPasswd".getBytes());
        long ledgerId = wlh.getId();
        int numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        LedgerHandle rlh = bkcWithNoExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes());
        assertTrue(
                "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

        Enumeration<LedgerEntry> entries = rlh.readEntries(0, numOfEntries - 2);
        int entryId = 0;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String entryString = new String(entry.getEntry());
            assertTrue("Expected entry String: " + ("foobar" + entryId) + " actual entry String: " + entryString,
                    entryString.equals("foobar" + entryId));
            entryId++;
        }

        for (int i = numOfEntries; i < 2 * numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        TestUtils.waitUntilLacUpdated(rlh, numOfEntries - 2);

        assertTrue(
                "Expected LAC of wlh: " + (2 * numOfEntries - 1) + " actual LAC of rlh: " + wlh.getLastAddConfirmed(),
                (wlh.getLastAddConfirmed() == (2 * numOfEntries - 1)));
        assertTrue(
                "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

        // since explicitlacflush policy is not enabled for writeledgerhandle, when we try
        // to read explicitlac for rlh, it will be reading up to the piggyback value.
        long explicitlac = rlh.readExplicitLastConfirmed();
        assertTrue(
                "Expected Explicit LAC of rlh: " + (numOfEntries - 2) + " actual ExplicitLAC of rlh: " + explicitlac,
                (explicitlac == (2 * numOfEntries - 2)));

        try {
            rlh.readEntries(2 * numOfEntries - 1, 2 * numOfEntries - 1);
            fail("rlh readEntries beyond " + (2 * numOfEntries - 2) + " should fail with ReadException");
        } catch (BKException.BKReadException readException) {
        }

        rlh.close();
        wlh.close();
        bkcWithNoExplicitLAC.close();
    }

    @Test
    public void testExplicitLACIsPersisted() throws Exception {
        /*
         * In DbLedgerStorage scenario, TransientLedgerInfo is not persisted -
         * https://github.com/apache/bookkeeper/issues/1533.
         *
         * So for this testcase we are ignoring DbLedgerStorage. It can/should
         * be enabled when Issue-1533 is fixed.
         */
        Assume.assumeTrue(!baseConf.getLedgerStorageClass().equals(DbLedgerStorage.class.getName()));
        ClientConfiguration confWithNoExplicitLAC = new ClientConfiguration();
        confWithNoExplicitLAC.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        // enable explicitLacFlush by setting non-zero value for
        // explictLacInterval
        long explictLacInterval = 100;
        confWithNoExplicitLAC.setExplictLacInterval(50);

        BookKeeper bkcWithExplicitLAC = new BookKeeper(confWithNoExplicitLAC);

        LedgerHandle wlh = bkcWithExplicitLAC.createLedger(1, 1, 1, digestType, "testPasswd".getBytes());
        long ledgerId = wlh.getId();
        int numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        LedgerHandle rlh = bkcWithExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes());
        assertEquals("LAC of rlh", (long) numOfEntries - 2, rlh.getLastAddConfirmed());

        for (int i = numOfEntries; i < 2 * numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        assertEquals("LAC of wlh", (2 * numOfEntries - 1), wlh.getLastAddConfirmed());
        assertEquals("LAC of rlh", (long) numOfEntries - 2, rlh.getLastAddConfirmed());
        assertEquals("Read LAC of rlh", (2 * numOfEntries - 2), rlh.readLastAddConfirmed());
        assertEquals("Read explicit LAC of rlh", (2 * numOfEntries - 2), rlh.readExplicitLastConfirmed());

        // we need to wait for atleast 2 explicitlacintervals,
        // since in writehandle for the first call
        // lh.getExplicitLastAddConfirmed() will be <
        // lh.getPiggyBackedLastAddConfirmed(),
        // so it wont make explicit writelac in the first run
        long readExplicitLastConfirmed = TestUtils.waitUntilExplicitLacUpdated(rlh, 2 * numOfEntries - 1);
        assertEquals("Read explicit LAC of rlh after wait for explicitlacflush", (2 * numOfEntries - 1),
                readExplicitLastConfirmed);

        // bookies have to be restarted
        restartBookies();

        /*
         * since explicitLac is persisted we should be able to read explicitLac
         * from the bookies.
         */
        LedgerHandle rlh2 = bkcWithExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes());
        assertEquals("Read explicit LAC of rlh2 after bookies restart", (2 * numOfEntries - 1),
                rlh2.readExplicitLastConfirmed());
        bkcWithExplicitLAC.close();
    }

    @Test
    public void testReadHandleWithExplicitLAC() throws Exception {
        ClientConfiguration confWithExplicitLAC = new ClientConfiguration();
        confWithExplicitLAC.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        int explicitLacIntervalMillis = 1000;
        confWithExplicitLAC.setExplictLacInterval(explicitLacIntervalMillis);

        BookKeeper bkcWithExplicitLAC = new BookKeeper(confWithExplicitLAC);

        LedgerHandle wlh = bkcWithExplicitLAC.createLedger(
            1, 1, 1,
            digestType, "testPasswd".getBytes());
        long ledgerId = wlh.getId();
        int numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        LedgerHandle rlh = bkcWithExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes());

        assertTrue(
                "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

        for (int i = numOfEntries; i < 2 * numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        // we need to wait for atleast 2 explicitlacintervals,
        // since in writehandle for the first call
        // lh.getExplicitLastAddConfirmed() will be <
        // lh.getPiggyBackedLastAddConfirmed(),
        // so it wont make explicit writelac in the first run
        TestUtils.waitUntilLacUpdated(rlh, 2 * numOfEntries - 2);

        assertTrue(
                "Expected LAC of wlh: " + (2 * numOfEntries - 1) + " actual LAC of wlh: " + wlh.getLastAddConfirmed(),
                (wlh.getLastAddConfirmed() == (2 * numOfEntries - 1)));
        // readhandle's lastaddconfirmed wont be updated until readExplicitLastConfirmed call is made
        assertTrue(
                "Expected LAC of rlh: " + (2 * numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (2 * numOfEntries - 2)));

        long explicitlac = TestUtils.waitUntilExplicitLacUpdated(rlh, 2 * numOfEntries - 1);
        assertTrue("Expected Explicit LAC of rlh: " + (2 * numOfEntries - 1)
                + " actual ExplicitLAC of rlh: " + explicitlac,
                (explicitlac == (2 * numOfEntries - 1)));
        // readExplicitLastConfirmed updates the lac of rlh.
        assertTrue(
                "Expected LAC of rlh: " + (2 * numOfEntries - 1) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (2 * numOfEntries - 1)));

        Enumeration<LedgerEntry> entries = rlh.readEntries(numOfEntries, 2 * numOfEntries - 1);
        int entryId = numOfEntries;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String entryString = new String(entry.getEntry());
            assertTrue("Expected entry String: " + ("foobar" + entryId) + " actual entry String: " + entryString,
                    entryString.equals("foobar" + entryId));
            entryId++;
        }

        rlh.close();
        wlh.close();
        bkcWithExplicitLAC.close();
    }

    @Test
    public void testReadHandleWithExplicitLACAndDeferredSync() throws Exception {
        ClientConfiguration confWithExplicitLAC = new ClientConfiguration();
        confWithExplicitLAC.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        int explicitLacIntervalMillis = 1000;
        confWithExplicitLAC.setExplictLacInterval(explicitLacIntervalMillis);

        BookKeeper bkcWithExplicitLAC = new BookKeeper(confWithExplicitLAC);

        LedgerHandle wlh = (LedgerHandle) bkcWithExplicitLAC.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .withDigestType(digestType.toApiDigestType())
                .withPassword("testPasswd".getBytes())
                .execute()
                .get();
        long ledgerId = wlh.getId();

        // start like testReadHandleWithExplicitLAC
        int numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            // if you perform force() + addEntry() you will piggy back LAC as usual
            wlh.force().get();
            wlh.addEntry(("foobar" + i).getBytes());
        }

        LedgerHandle rlh = bkcWithExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, "testPasswd".getBytes());

        assertTrue(
                "Expected LAC of rlh: " + (numOfEntries - 2) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (numOfEntries - 2)));

        for (int i = numOfEntries; i < 2 * numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        // running a force() will update local LAC on the writer
        // ExplicitLAC timer will send the value even without writes
        wlh.force().get();

        // wait for explicit lac to be sent to bookies
        TestUtils.waitUntilExplicitLacUpdated(rlh, 2 * numOfEntries - 2);

        // we need to wait for atleast 2 explicitlacintervals,
        // since in writehandle for the first call
        // lh.getExplicitLastAddConfirmed() will be <
        // lh.getPiggyBackedLastAddConfirmed(),
        // so it wont make explicit writelac in the first run
        TestUtils.waitUntilLacUpdated(rlh, 2 * numOfEntries - 2);

        assertTrue(
                "Expected LAC of wlh: " + (2 * numOfEntries - 1) + " actual LAC of wlh: " + wlh.getLastAddConfirmed(),
                (wlh.getLastAddConfirmed() == (2 * numOfEntries - 1)));

        long explicitlac = TestUtils.waitUntilExplicitLacUpdated(rlh, 2 * numOfEntries - 1);
        assertTrue("Expected Explicit LAC of rlh: " + (2 * numOfEntries - 1)
                + " actual ExplicitLAC of rlh: " + explicitlac,
                (explicitlac == (2 * numOfEntries - 1)));
        // readExplicitLastConfirmed updates the lac of rlh.
        assertTrue(
                "Expected LAC of rlh: " + (2 * numOfEntries - 1) + " actual LAC of rlh: " + rlh.getLastAddConfirmed(),
                (rlh.getLastAddConfirmed() == (2 * numOfEntries - 1)));

        Enumeration<LedgerEntry> entries = rlh.readEntries(numOfEntries, 2 * numOfEntries - 1);
        int entryId = numOfEntries;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            String entryString = new String(entry.getEntry());
            assertTrue("Expected entry String: " + ("foobar" + entryId) + " actual entry String: " + entryString,
                    entryString.equals("foobar" + entryId));
            entryId++;
        }

        rlh.close();
        wlh.close();
        bkcWithExplicitLAC.close();
    }

    @Test
    public void fallbackV3() throws Exception {
        ClientConfiguration v2Conf = new ClientConfiguration();
        v2Conf.setUseV2WireProtocol(true);
        v2Conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        v2Conf.setExplictLacInterval(10);

        BookKeeper bookKeeper = new BookKeeper(v2Conf);
        LedgerHandle write = (LedgerHandle) bookKeeper.createLedger(1,
                                                                    1,
                                                                    1,
                                                                    DigestType.MAC,
                                                                    "pass".getBytes());
        write.addEntry("test".getBytes());
        TestUtils.waitUntilExplicitLacUpdated(write, 0);
        long lac = write.readExplicitLastConfirmed();
        assertEquals(0, lac);
        write.close();
        bookKeeper.close();
    }

}
