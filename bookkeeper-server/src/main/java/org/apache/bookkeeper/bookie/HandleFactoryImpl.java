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
import java.util.HashMap;

class HandleFactoryImpl implements HandleFactory {
    HashMap<Long, LedgerDescriptor> ledgers = new HashMap<Long, LedgerDescriptor>();
    HashMap<Long, LedgerDescriptor> readOnlyLedgers
        = new HashMap<Long, LedgerDescriptor>();

    final LedgerStorage ledgerStorage;

    HandleFactoryImpl(LedgerStorage ledgerStorage) {
        this.ledgerStorage = ledgerStorage;
    }

    @Override
    public LedgerDescriptor getHandle(long ledgerId, byte[] masterKey)
            throws IOException, BookieException {
        LedgerDescriptor handle = null;
        synchronized (ledgers) {
            handle = ledgers.get(ledgerId);
            if (handle == null) {
                handle = LedgerDescriptor.create(masterKey, ledgerId, ledgerStorage);
                ledgers.put(ledgerId, handle);
            }
            handle.checkAccess(masterKey);
        }
        return handle;
    }

    @Override
    public LedgerDescriptor getReadOnlyHandle(long ledgerId)
            throws IOException, Bookie.NoLedgerException {
        LedgerDescriptor handle = null;
        synchronized (ledgers) {
            handle = readOnlyLedgers.get(ledgerId);
            if (handle == null) {
                handle = LedgerDescriptor.createReadOnly(ledgerId, ledgerStorage);
                readOnlyLedgers.put(ledgerId, handle);
            }
        }
        return handle;
    }
}