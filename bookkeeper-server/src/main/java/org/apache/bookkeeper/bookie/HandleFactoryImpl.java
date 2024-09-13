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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.time.Duration;
import org.apache.bookkeeper.bookie.LedgerStorage.LedgerDeletionListener;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;

class HandleFactoryImpl implements HandleFactory, LedgerDeletionListener {
    private final ConcurrentLongHashMap<LedgerDescriptor> ledgers;
    private final ConcurrentLongHashMap<LedgerDescriptor> readOnlyLedgers;

    /**
     * Once the ledger was marked "fenced" before, the ledger was accessed by multi clients. One client is calling
     * "delete" now, and other clients may call "write" continuously later. We mark these ledgers can not be written
     * anymore. And maintains the state for 7 days is safety.
     */
    private final Cache<Long, Boolean> recentlyFencedAndDeletedLedgers = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofDays(7)).build();

    final LedgerStorage ledgerStorage;

    HandleFactoryImpl(LedgerStorage ledgerStorage) {
        this.ledgerStorage = ledgerStorage;
        this.ledgers = ConcurrentLongHashMap.<LedgerDescriptor>newBuilder().build();
        this.readOnlyLedgers = ConcurrentLongHashMap.<LedgerDescriptor>newBuilder().build();

        ledgerStorage.registerLedgerDeletionListener(this);
    }

    @Override
    public LedgerDescriptor getHandle(final long ledgerId, final byte[] masterKey, boolean journalReplay)
            throws IOException, BookieException {
        LedgerDescriptor handle = ledgers.get(ledgerId);

        if (handle == null) {
            if (!journalReplay && recentlyFencedAndDeletedLedgers.getIfPresent(ledgerId) != null) {
                throw BookieException.create(BookieException.Code.LedgerFencedException);
            }
            handle = LedgerDescriptor.create(masterKey, ledgerId, ledgerStorage);
            ledgers.putIfAbsent(ledgerId, handle);
        }

        handle.checkAccess(masterKey);
        return handle;
    }

    @Override
    public LedgerDescriptor getReadOnlyHandle(final long ledgerId) throws IOException, Bookie.NoLedgerException {
        LedgerDescriptor handle = readOnlyLedgers.get(ledgerId);

        if (handle == null) {
            handle = LedgerDescriptor.createReadOnly(ledgerId, ledgerStorage);
            readOnlyLedgers.putIfAbsent(ledgerId, handle);
        }

        return handle;
    }

    private void markIfConflictWritingOccurs(long ledgerId) {
        LedgerDescriptor ledgerDescriptor = ledgers.get(ledgerId);
        try {
            if (ledgerDescriptor != null && ledgerDescriptor.isFenced()) {
                recentlyFencedAndDeletedLedgers.put(ledgerId, true);
            }
        } catch (IOException | BookieException ex) {
            // The ledger is in limbo state.
            recentlyFencedAndDeletedLedgers.put(ledgerId, true);
        }
    }

    @Override
    public void ledgerDeleted(long ledgerId) {
        markIfConflictWritingOccurs(ledgerId);
        // Do delete.
        ledgers.remove(ledgerId);
        readOnlyLedgers.remove(ledgerId);
    }
}
