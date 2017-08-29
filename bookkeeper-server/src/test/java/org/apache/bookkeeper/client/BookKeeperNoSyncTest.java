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
 * Tests of the main BookKeeper client using no-synch
 */
public class BookKeeperNoSyncTest extends BookKeeperClusterTestCase {

    private final static Logger LOG = LoggerFactory.getLogger(BookKeeperNoSyncTest.class);

    private final DigestType digestType;

    public BookKeeperNoSyncTest() {
        super(4);
        this.digestType = DigestType.CRC32;
    }

    @Test(timeout = 60000)
    public void testAddEntryNosynch() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "testPasswd".getBytes(),
                null, SyncMode.JOURNAL_NOSYNC)) {
                ledgerId = lh.getId();
                for (int i = 0; i < numEntries - 1; i++) {
                    lh.addEntry(data);
                }
                lh.addEntry(data, SyncMode.JOURNAL_SYNC);
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

    @Test(timeout = 60000)
    public void testPiggyBackLastAddSyncedEntry() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedgerAdv(1, 1, 1, digestType, "testPasswd".getBytes(), null,
                SyncMode.JOURNAL_NOSYNC)) {
                ledgerId = lh.getId();
                for (int i = 0; i < numEntries - 2; i++) {
                    lh.addEntry(i, data, SyncMode.JOURNAL_NOSYNC);
                }
                // LAC must not advance on no-sync writes
                assertEquals(-1, lh.getLastAddConfirmed());
                // forcing a sync, LAC will be able to advance at the next addEntry
                lh.addEntry(numEntries - 2, data, SyncMode.JOURNAL_SYNC);
                assertEquals(numEntries - 2, lh.getLastAddConfirmed());
                lh.addEntry(numEntries - 1, data, SyncMode.JOURNAL_SYNC);
                assertEquals(numEntries - 1, lh.getLastAddConfirmed());
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

    @Test(timeout = 60000)
    public void testSync() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "testPasswd".getBytes(),
                null, SyncMode.JOURNAL_NOSYNC)) {
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

    @Test(timeout = 60000)
    public void testAutoSyncOnClose() throws Exception {
        int numEntries = 100;
        byte[] data = "foobar".getBytes();
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long ledgerId;
        try (BookKeeper bkc = new BookKeeper(confWriter)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "testPasswd".getBytes(),
                null, SyncMode.JOURNAL_NOSYNC)) {
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
