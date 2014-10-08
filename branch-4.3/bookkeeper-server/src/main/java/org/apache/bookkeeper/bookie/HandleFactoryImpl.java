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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class HandleFactoryImpl implements HandleFactory {
    ConcurrentMap<Long, LedgerDescriptor> ledgers = new ConcurrentHashMap<Long, LedgerDescriptor>();
    ConcurrentMap<Long, LedgerDescriptor> readOnlyLedgers
        = new ConcurrentHashMap<Long, LedgerDescriptor>();

    final LedgerStorage ledgerStorage;

    HandleFactoryImpl(LedgerStorage ledgerStorage) {
        this.ledgerStorage = ledgerStorage;
    }

    @Override
    public LedgerDescriptor getHandle(long ledgerId, byte[] masterKey)
            throws IOException, BookieException {
        LedgerDescriptor handle = null;
        if (null == (handle = ledgers.get(ledgerId))) {
            // LedgerDescriptor#create sets the master key in the ledger storage, calling it
            // twice on the same ledgerId is safe because it eventually puts a value in the ledger cache
            // that guarantees synchronized access across all cached entries.
            handle = ledgers.putIfAbsent(ledgerId, LedgerDescriptor.create(masterKey, ledgerId, ledgerStorage));
            if (null == handle) {
                handle = ledgers.get(ledgerId);
            }
        }
        handle.checkAccess(masterKey);
        return handle;
    }

    @Override
    public LedgerDescriptor getReadOnlyHandle(long ledgerId)
            throws IOException, Bookie.NoLedgerException {
        LedgerDescriptor handle = null;
        if (null == (handle = readOnlyLedgers.get(ledgerId))) {
            handle = readOnlyLedgers.putIfAbsent(ledgerId, LedgerDescriptor.createReadOnly(ledgerId, ledgerStorage));
            if (null == handle) {
                handle = readOnlyLedgers.get(ledgerId);
            }
        }
        return handle;
    }
}
