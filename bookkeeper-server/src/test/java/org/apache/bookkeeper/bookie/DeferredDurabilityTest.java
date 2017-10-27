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
package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException.BKReadException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeferredDurabilityTest extends BookKeeperClusterTestCase {

    private final static Logger LOG = LoggerFactory.getLogger(DeferredDurabilityTest.class);
    final ByteBuf data = Unpooled.wrappedBuffer("foobar".getBytes());

    public DeferredDurabilityTest() {
        super(1);
    }

    @Test
    public void testAddEntry() throws Exception {
        int numEntries = 100;

        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                LedgerHandle lh = (LedgerHandle) wh;
                ledgerId = wh.getId();
                for (int i = 0; i < numEntries - 1; i++) {
                    result(wh.append(data.copy()));
                }
                long lastEntryID = result(wh.append(data.copy()));
                assertEquals(numEntries - 1, lastEntryID);
                assertEquals(numEntries - 1, lh.getLastAddPushed());
                result(wh.force());
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                Iterable<LedgerEntry> entries = result(rh.read(0, numEntries - 1));
                checkEntries(entries, data.array());
            }
        }
    }

    @Test
    public void testPiggyBackLastAddSyncedEntryOnForce() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());

        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                LedgerHandle lh = (LedgerHandle) wh;
                ledgerId = wh.getId();
                for (int i = 0; i < numEntries - 2; i++) {
                    long entryId = result(wh.append(data.copy()));
                    assertEquals(i, entryId);
                    assertEquals(-1, wh.getLastAddConfirmed());
                }

                long entryId = result(wh.append(data.copy()));
                assertEquals(numEntries - 2, entryId);
                assertEquals(entryId, lh.getLastAddPushed());
                // forcing a sync, LAC will be able to advance
                long lastAddSynced = result(wh.force());
                assertEquals(entryId, lastAddSynced);
                assertEquals(wh.getLastAddConfirmed(), lastAddSynced);
                long lastEntryId = result(wh.append(data.copy()));
                assertEquals(numEntries - 1, lastEntryId);
                assertEquals(lastEntryId, lh.getLastAddPushed());
                result(wh.force());
                assertEquals(numEntries - 1, wh.getLastAddConfirmed());
                assertEquals(numEntries - 1, lh.getLastAddPushed());
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                Iterable<LedgerEntry> entries = result(rh.read(0, numEntries - 1));
                checkEntries(entries, data.array());
            }
        }
    }

    @Test
    public void testNoSyncOnClose() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            long lastAddConfirmedBeforeClose;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                ledgerId = wh.getId();
                LedgerHandle lh = (LedgerHandle) wh;

                for (int i = 0; i < numEntries - 1; i++) {
                    result(wh.append(data.copy()));
                }
                long lastEntryId = result(wh.append(data.copy()));
                assertEquals(lastEntryId, lh.getLastAddPushed());

                lastAddConfirmedBeforeClose = wh.getLastAddConfirmed();
                assertEquals(-1, wh.getLastAddConfirmed());

                // close operation does not automatically perform a 'sync' up to lastAddPushed
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                if (lastAddConfirmedBeforeClose != -1)  {
                    checkEntries(result(rh.read(0, lastAddConfirmedBeforeClose)), data.array());
                }
                try {
                    result(rh.read(0, numEntries -1));
                    fail("should not be able to read up");
                } catch (BKReadException expected){
                }
            }
        }
    }

    @Test
    public void testSyncBeforeClose() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            long lastAddConfirmedBeforeClose;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                ledgerId = wh.getId();
                LedgerHandle lh = (LedgerHandle) wh;

                for (int i = 0; i < numEntries - 1; i++) {
                    result(wh.append(data.copy()));
                }
                long lastEntryId = result(wh.append(data.copy()));
                assertEquals(lastEntryId, lh.getLastAddPushed());

                // LastAddConfirmed on the client side will be < lh.getLastAddPushed()
                // as LastAddSynced is piggy backed on addResponse LastAddConfirmed may advance, depending on the
                // journal
                assertTrue(wh.getLastAddConfirmed() < lh.getLastAddPushed());

                result(wh.force());
                lastAddConfirmedBeforeClose = wh.getLastAddConfirmed();
                assertEquals(numEntries - 1, lastAddConfirmedBeforeClose);
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                checkEntries(result(rh.read(0, lastAddConfirmedBeforeClose)), data.array());
            }
        }
    }

    @Test
    public void testRestartBookie() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());        
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                ledgerId = wh.getId();
                for (int i = 0; i < numEntries - 1 ; i++) {
                    long entryId = result(wh.append(data.copy()));
                    assertEquals(i, entryId);
                    assertEquals(-1, wh.getLastAddConfirmed());
                }

                assertEquals(numEntries - 2, result(wh.force()).longValue());           
                restartBookies();
                assertEquals(numEntries - 2, result(wh.force()).longValue());
            }
        }
    }

    private static void checkEntries(Iterable<LedgerEntry> entries, byte[] data)
        throws InterruptedException, BKException {
        for (org.apache.bookkeeper.client.api.LedgerEntry le : entries) {
            assertArrayEquals(data, le.getEntry());
        }
    }
}
