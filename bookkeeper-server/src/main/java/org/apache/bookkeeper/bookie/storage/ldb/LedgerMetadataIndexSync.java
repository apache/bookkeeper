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
package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains an index for the ledgers metadata.
 *
 * <p>Synchronous write mode class,
 * the key is the ledgerId and the value is the {@link LedgerData} content.
 */
public class LedgerMetadataIndexSync extends LedgerMetadataIndex {

    private static final Logger log = LoggerFactory.getLogger(LedgerMetadataIndexSync.class);

    public LedgerMetadataIndexSync(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
                                   StatsLogger stats) throws IOException {
        super(conf, storageFactory, basePath, stats);
    }

    /**
     * Flushes all pending changes.
     */
    @Override
    public void flush() throws IOException {
        LongWrapper key = LongWrapper.get();
        KeyValueStorage.Batch batch = ledgersDb.newBatch();

        try {
            int updatedLedgers = 0;
            while (!pendingLedgersUpdates.isEmpty()) {
                Entry<Long, LedgerData> entry = pendingLedgersUpdates.poll();
                key.set(entry.getKey());
                byte[] value = entry.getValue().toByteArray();
                batch.put(key.array, value);
                ++updatedLedgers;
            }

            if (log.isDebugEnabled()) {
                log.debug("Persisting updates to {} ledgers", updatedLedgers);
            }
        } finally {
            try {
                batch.flush();
                batch.clear();
            } finally {
                key.recycle();
                batch.close();
            }
        }
    }

    @Override
    public void removeDeletedLedgers() throws IOException {
        LongWrapper key = LongWrapper.get();
        KeyValueStorage.Batch batch = ledgersDb.newBatch();

        try {
            int deletedLedgers = 0;
            while (!pendingDeletedLedgers.isEmpty()) {
                long ledgerId = pendingDeletedLedgers.poll();
                key.set(ledgerId);
                batch.remove(key.array);
            }

            if (log.isDebugEnabled()) {
                log.debug("Persisting deletes of ledgers {}", deletedLedgers);
            }
        } finally {
            try {
                batch.flush();
                batch.clear();
            } finally {
                key.recycle();
                batch.close();
            }
        }
    }

    @Override
    synchronized boolean setStorageStateFlags(int expected, int newFlags) throws IOException {
        LongWrapper keyWrapper = LongWrapper.get();
        LongWrapper currentWrapper = LongWrapper.get();
        LongWrapper newFlagsWrapper = LongWrapper.get();

        KeyValueStorage.Batch batch = ledgersDb.newBatch();
        try {
            keyWrapper.set(STORAGE_FLAGS);
            newFlagsWrapper.set(newFlags);
            int current = 0;
            if (ledgersDb.get(keyWrapper.array, currentWrapper.array) >= 0) {
                current = (int) currentWrapper.getValue();
            }
            if (current == expected) {
                batch.put(keyWrapper.array, newFlagsWrapper.array);
                return true;
            }
        } finally {
            try {
                batch.flush();
                batch.clear();
            } finally {
                keyWrapper.recycle();
                currentWrapper.recycle();
                newFlagsWrapper.recycle();
                batch.close();
            }
        }
        return false;
    }
}
