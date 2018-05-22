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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Test the bookie journal without sync, driven by client with {@link WriteFlag#DEFERRED_SYNC} write flag.
 */
public class BookieDeferredSyncTest extends BookKeeperClusterTestCase {

    public BookieDeferredSyncTest() {
        super(1);
    }

    @Test
    public void testWriteAndRecovery() throws Exception {
        WriteHandle lh = result(bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute());

        int n = 10;

        long ledgerId = lh.getId();

        for (int i = 0; i < n; i++) {
            lh.append(("entry-" + i).getBytes());
        }

        restartBookies();

        try (ReadHandle readLh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withRecovery(true)
                .withPassword(new byte[0])
                .execute());) {

            try (LedgerEntries entries = readLh.read(0, n - 1)) {
                for (int i = 0; i < n; i++) {
                    org.apache.bookkeeper.client.api.LedgerEntry entry = entries.getEntry(i);
                    assertEquals("entry-" + i, new String(entry.getEntryBytes()));
                }
            }
        }
    }

    @Test
    public void testCloseNoForce() throws Exception {
        WriteHandle lh = result(bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute());

        int n = 10;

        long ledgerId = lh.getId();

        for (int i = 0; i < n; i++) {
            lh.append(("entry-" + i).getBytes());
        }

        // this will close metadata, writing LastAddConfirmed = -1
        assertEquals(-1, lh.getLastAddConfirmed());
        lh.close();

        restartBookies();

        try (ReadHandle readLh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withRecovery(true)
                .withPassword(new byte[0])
                .execute());) {
            assertEquals(-1, readLh.getLastAddConfirmed());
        }

    }

    @Test
    public void testCloseWithForce() throws Exception {
        WriteHandle lh = result(bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute());

        int n = 10;

        long ledgerId = lh.getId();

        for (int i = 0; i < n; i++) {
            lh.append(("entry-" + i).getBytes());
        }

        result(lh.force());
        assertEquals(n - 1, lh.getLastAddConfirmed());

        lh.close();

        restartBookies();

        try (ReadHandle readLh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withRecovery(true)
                .withPassword(new byte[0])
                .execute());) {

            try (LedgerEntries entries = readLh.read(0, n - 1)) {
                for (int i = 0; i < n; i++) {
                    org.apache.bookkeeper.client.api.LedgerEntry entry = entries.getEntry(i);
                    assertEquals("entry-" + i, new String(entry.getEntryBytes()));
                }
            }
        }
    }

    @Test
    public void testForceRoundTripWithDeferredSync() throws Exception {
        try (WriteHandle lh = result(bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute());) {
            int n = 10;
            for (int i = 0; i < n; i++) {
                lh.append(("entry-" + i).getBytes());
            }
            result(lh.force());
            assertEquals(n - 1, lh.getLastAddConfirmed());

            lh.close();
        }
    }

    @Test
    public void testForceRoundTripWithoutDeferredSync() throws Exception {
        try (WriteHandle lh = result(bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(WriteFlag.NONE)
                .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute());) {
            int n = 10;
            for (int i = 0; i < n; i++) {
                lh.append(("entry-" + i).getBytes());
            }
            // this should work even with non-DEFERRED_SYNC writers
            result(lh.force());
            assertEquals(n - 1, lh.getLastAddConfirmed());

            lh.close();
        }
    }

}
