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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import java.util.EnumSet;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Test the bookie journal without sync, driven by client with
 * {@link WriteFlag#DEFERRED_SYNC} write flag.
 */
public class BookieDeferredSyncTest extends BookKeeperClusterTestCase {

    public BookieDeferredSyncTest() {
        super(1);
    }

    @Test
    public void testWriteAndRecovery() throws Exception {
        // this WriteHandle will not be closed
        WriteHandle lh = result(bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute());

        int n = 10;

        long ledgerId = lh.getId();

        for (int i = 0; i < n; i++) {
            lh.append(("entry-" + i).getBytes(UTF_8));
        }

        try (ReadHandle readLh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withRecovery(true)
                .withPassword(new byte[0])
                .execute())) {

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
        testClose(true);
    }

    @Test
    public void testCloseWithForce() throws Exception {
        testClose(false);
    }

    private void testClose(boolean force) throws Exception {
        final int n = 10;
        long ledgerId;
        try (WriteHandle lh = result(bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute())) {

            ledgerId = lh.getId();
            for (int i = 0; i < n; i++) {
                lh.append(("entry-" + i).getBytes(UTF_8));
            }   if (force) {
                // with force() LastAddConfirmed is updated
                result(lh.force());
                // on close metadata will have LastAddConfirmed = n - 1
                assertEquals(n - 1, lh.getLastAddConfirmed());
            } else {
                // on close metadata will have LastAddConfirmed = -1
                assertEquals(-1, lh.getLastAddConfirmed());
            }
        }

        if (force) {
            // the reader will be able to read
            try (ReadHandle readLh = result(bkc.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withRecovery(true)
                    .withPassword(new byte[0])
                    .execute())) {

                try (LedgerEntries entries = readLh.read(0, n - 1)) {
                    for (int i = 0; i < n; i++) {
                        LedgerEntry entry = entries.getEntry(i);
                        assertEquals("entry-" + i, new String(entry.getEntryBytes()));
                    }
                }

                try (LedgerEntries entries = readLh.readUnconfirmed(0, n - 1)) {
                    for (int i = 0; i < n; i++) {
                        LedgerEntry entry = entries.getEntry(i);
                        assertEquals("entry-" + i, new String(entry.getEntryBytes()));
                    }
                }
            }
        } else {
            // reader will see LastAddConfirmed = -1
            try (ReadHandle readLh = result(bkc.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withRecovery(true)
                    .withPassword(new byte[0])
                    .execute())) {
                assertEquals(-1, readLh.getLastAddConfirmed());

                // entry will be readable with readUnconfirmed
                try (LedgerEntries entries = readLh.readUnconfirmed(0, n - 1)) {
                    for (int i = 0; i < n; i++) {
                        LedgerEntry entry = entries.getEntry(i);
                        assertEquals("entry-" + i, new String(entry.getEntryBytes()));
                    }
                }
            }
        }
    }

    @Test
    public void testForceWithDeferredSyncWriteFlags() throws Exception {
        testForce(EnumSet.of(WriteFlag.DEFERRED_SYNC));
    }

    @Test
    public void testForceNoWriteFlag() throws Exception {
        // force API will work even without DEFERRED_SYNC flag
        testForce(WriteFlag.NONE);
    }

    private void testForce(EnumSet<WriteFlag> writeFlags) throws Exception {
        try (WriteHandle lh = result(bkc.newCreateLedgerOp()
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withWriteFlags(writeFlags)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute())) {
            int n = 10;
            for (int i = 0; i < n; i++) {
                lh.append(("entry-" + i).getBytes(UTF_8));
            }
            result(lh.force());
            assertEquals(n - 1, lh.getLastAddConfirmed());
        }
    }

}
