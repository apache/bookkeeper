/**
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

package org.apache.bookkeeper.verifier;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * Driver for a normal Bookkeeper cluster.
 */
class DirectBookkeeperDriver implements BookkeeperVerifier.BookkeeperDriver {
    private final ConcurrentHashMap<Long, LedgerHandle> openHandles = new ConcurrentHashMap<>();
    private BookKeeper client;

    DirectBookkeeperDriver(BookKeeper client) {
        this.client = client;
    }

    @Override
    public void createLedger(long ledgerID, int enSize, int writeQSize, int ackQSize, Consumer<Integer> cb) {
        client.asyncCreateLedgerAdv(
                ledgerID,
                enSize,
                writeQSize,
                ackQSize,
                BookKeeper.DigestType.CRC32,
                new byte[0],
                (rc, lh, ctx) -> {
                    openHandles.put(ledgerID, lh);
                    cb.accept(rc);
                },
                null,
                null);
    }

    @Override
    public void closeLedger(long ledgerID, Consumer<Integer> cb) {
        LedgerHandle handle = openHandles.remove(ledgerID);
        handle.asyncClose(
                (rc, lh, ctx) -> cb.accept(rc),
                null);
    }

    @Override
    public void deleteLedger(long ledgerID, Consumer<Integer> cb) {
        client.asyncDeleteLedger(ledgerID, (rc, ctx) -> {
            cb.accept(rc);
        }, null);
    }

    @Override
    public void writeEntry(long ledgerID, long entryID, byte[] data, Consumer<Integer> cb) {
        LedgerHandle handle;
        handle = openHandles.get(ledgerID);
        if (handle == null) {
            cb.accept(BKException.Code.WriteException);
            return;
        }
        handle.asyncAddEntry(entryID, data, (rc, lh, entryId, ctx) -> {
            cb.accept(rc);
        }, null);
    }

    @Override
    public void readEntries(
            long ledgerID, long firstEntryID, long lastEntryID, BiConsumer<Integer, ArrayList<byte[]>> cb) {
        client.asyncOpenLedgerNoRecovery(ledgerID, BookKeeper.DigestType.CRC32, new byte[0], (rc, lh, ctx) -> {
            if (rc != 0) {
                cb.accept(rc, null);
                return;
            }
            System.out.format("Got handle for read %d -> %d on ledger %d%n", firstEntryID, lastEntryID, ledgerID);
            lh.asyncReadEntries(firstEntryID, lastEntryID, (rc1, lh1, seq, ctx1) -> {
                System.out.format("Read cb %d -> %d on ledger %d%n", firstEntryID, lastEntryID, ledgerID);
                ArrayList<byte[]> results = new ArrayList<>();
                if (rc1 == 0) {
                    while (seq.hasMoreElements()) {
                        results.add(seq.nextElement().getEntry());
                    }
                    System.out.format("About to close handle for read %d -> %d on ledger %d%n",
                            firstEntryID, lastEntryID, ledgerID);
                }
                lh.asyncClose((rc2, lh2, ctx2) -> {
                    System.out.format("Closed handle for read %d -> %d on ledger %d result %d%n",
                            firstEntryID, lastEntryID, ledgerID, rc2);
                    cb.accept(rc1 == 0 ? rc2 : rc1, results);
                }, null);
            }, null);
        }, null);
    }
}
