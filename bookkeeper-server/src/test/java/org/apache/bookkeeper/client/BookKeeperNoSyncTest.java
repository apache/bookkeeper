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

import java.util.Enumeration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Tests of the main BookKeeper client using VD_JOURNAL
 */
public class BookKeeperNoSyncTest extends BookKeeperClusterTestCase {

    private final static Logger LOG = LoggerFactory.getLogger(BookKeeperNoSyncTest.class);

    private final DigestType digestType;

    public BookKeeperNoSyncTest() {
        super(4);
        this.digestType = DigestType.CRC32;
    }

    @Test
    public void testAddEntryNosynch() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "testPasswd".getBytes(),
                null, LedgerType.VD_JOURNAL)) {
                ledgerId = lh.getId();
                for (int i = 0; i < numEntries - 1; i++) {
                    lh.addEntry(data);
                }                
                long lastEntryID = lh.addEntry(data);
                assertEquals(numEntries - 1, lastEntryID);
                assertEquals(numEntries - 1, lh.getLastAddPushed());
            }
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries - 1);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    assertArrayEquals(data, e.getEntry());
                }
            }
        }
    }

    @Test
    public void testPiggyBackLastAddSyncedEntryOnSync() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedgerAdv(1, 1, 1, digestType, "testPasswd".getBytes(), null, LedgerType.VD_JOURNAL)) {
                ledgerId = lh.getId();
                for (int i = 0; i < numEntries - 2; i++) {
                    long entryId = lh.addEntry(i, data);
                    assertEquals(i, entryId);
                    // LAC must not advance on VD writes, it may advance because of grouping/flushQueueNotEmpty
                    // but in this test it should not advance till the given entry id
                    assertTrue(lh.getLastAddConfirmed() < entryId);
                }                
                
                long entryId = lh.addEntry(numEntries - 2, data);
                assertEquals(numEntries - 2, entryId);
                assertEquals(entryId, lh.getLastAddPushed());
                // forcing a sync, LAC will be able to advance                
                long lastAddSynced = lh.sync(entryId);                
                assertEquals(entryId, lastAddSynced);
                assertEquals(lh.getLastAddConfirmed(), lastAddSynced);                
                long lastEntryId = lh.addEntry(numEntries - 1, data);
                assertEquals(numEntries - 1, lastEntryId);
                assertEquals(lastEntryId, lh.getLastAddPushed());
                lh.sync(lastEntryId);
                assertEquals(numEntries - 1, lh.getLastAddConfirmed());
                assertEquals(numEntries - 1, lh.getLastAddPushed());
            }
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries - 1);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    assertArrayEquals(data, e.getEntry());
                }
            }
        }
    }

     @Test
    public void testPiggyBackLastAddSyncedEntryOnWriteToSameJournalFromOtherLedger() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "testPasswd".getBytes(), null,
                LedgerType.VD_JOURNAL)) {
                ledgerId = lh.getId();
                for (int i = 0; i < numEntries - 2; i++) {
                    long entryId = lh.addEntry(data);
                    // LAC must not advance on VD writes, it may advance because of grouping/flushQueueNotEmpty
                    // but in this test it should not advance till the given entry id
                    assertTrue(lh.getLastAddConfirmed() < entryId);
                }
                

                long entryId = lh.addEntry(data);
                assertEquals(numEntries - 2, entryId);
                assertEquals(entryId, lh.getLastAddPushed());

                // write a sync'd entry, we are using one single journal, it will force an fsync
                try (LedgerHandle lh2 = bkc.createLedger(1, 1, 1, digestType, "testPasswd".getBytes(), null,
                     LedgerType.PD_JOURNAL)) {
                     lh2.addEntry(data);
                }

                // LAC will be able to advance just be adding an entry,
                // but it won't advance to the lastAddSynced
                long entryIdNotYetSynced = lh.addEntry(data);
                assertEquals(numEntries - 1, entryIdNotYetSynced);
                long lastAddSynced2 = lh.getLastAddSynced();
                assertEquals(entryIdNotYetSynced-1, lastAddSynced2);
                long lastAddConfirmed = lh.getLastAddConfirmed();
                assertEquals(lastAddConfirmed, lastAddSynced2);

                // datum on bookies will reflect the last piggybacked value sent, not the client-side value
                long readAddConfirmed = lh.readLastConfirmed();
                assertEquals(lastAddConfirmed-1, readAddConfirmed);
                
            }
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries - 1);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    assertArrayEquals(data, e.getEntry());
                }
            }
        }
    }

    @Test
    public void testSync() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "testPasswd".getBytes(),
                null, LedgerType.VD_JOURNAL)) {
                ledgerId = lh.getId();

                try {
                    lh.sync(0);
                    fail("sync not possible as no entry has ever been written");
                } catch (BKException.BKIncorrectParameterException err){
                }

                for (int i = 0; i < numEntries - 1; i++) {
                    lh.addEntry(data);
                }
                long lastEntryId = lh.addEntry(data);
                // wait for bookies to sync up to the lastEntryId
                // this will advance LastAddConfirmed
                lh.sync(lastEntryId);
                assertEquals(lastEntryId, lh.getLastAddConfirmed());
                assertEquals(lastEntryId, lh.getLastAddPushed());
            }
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries - 1);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    assertArrayEquals(data, e.getEntry());
                }
            }
        }
    }

    @Test
    public void testAutoSyncOnClose() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "testPasswd".getBytes(),
                null, LedgerType.VD_JOURNAL)) {
                ledgerId = lh.getId();

                try {
                    lh.sync(0);
                    fail("sync not possible as no entry has ever been written");
                } catch (BKException.BKIncorrectParameterException err){
                }

                for (int i = 0; i < numEntries - 1; i++) {
                    lh.addEntry(data);
                }
                long lastEntryId = lh.addEntry(data);
                assertEquals(lastEntryId, lh.getLastAddPushed());
                // close operation will automatically perform a 'sync' up to lastAddPushed
            }
            try (LedgerHandle lh = bkc.openLedger(ledgerId, digestType, "testPasswd".getBytes())) {
                Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries - 1);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    assertArrayEquals(data, e.getEntry());
                }
            }
        }
    }
}
