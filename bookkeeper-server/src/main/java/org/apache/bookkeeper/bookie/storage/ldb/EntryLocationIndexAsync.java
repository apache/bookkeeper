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

import com.google.common.collect.Iterables;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import java.io.IOException;

/**
 * Maintains an index of the entry locations in the EntryLogger.
 *
 * <p>For each ledger multiple entries are stored in the same "record", represented
 * by the {@link LedgerIndexPage} class.
 */
public class EntryLocationIndexAsync extends EntryLocationIndex {

    public EntryLocationIndexAsync(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
                                   StatsLogger stats) throws IOException {
        super(conf, storageFactory, basePath, stats);
    }

    @Override
    public void addLocation(long ledgerId, long entryId, long location) throws IOException {
        addLocation(null, ledgerId, entryId, location);
        locationsDb.sync();
    }

    @Override
    public Batch newBatch() {
        return null;
    }

    @Override
    public void updateLocations(Iterable<EntryLocation> newLocations) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Update locations -- {}", Iterables.size(newLocations));
        }

        // Update all the ledger index pages with the new locations
        for (EntryLocation e : newLocations) {
            if (log.isDebugEnabled()) {
                log.debug("Update location - ledger: {} -- entry: {}", e.ledger, e.entry);
            }
            addLocation(null, e.ledger, e.entry, e.location);
        }
        locationsDb.sync();
    }

    @Override
    public void put(Batch batch, byte[] key, byte[] value) throws IOException {
        locationsDb.put(key, value);
    }

    @Override
    public void flush(Batch batch) throws IOException {
        locationsDb.sync();
    }

    @Override
    public void delete(Batch batch, LongPairWrapper keyToDelete) throws IOException {
        locationsDb.delete(keyToDelete.array);
    }

    @Override
    public void close(Batch batch) throws IOException {
        // do nothing
    }
}