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
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit test for {@link EntryLocationIndex}.
 */
public class EntryLocationIndexTest {

    private final ServerConfiguration serverConfiguration = new ServerConfiguration();

    @Test
    public void deleteLedgerTest() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        EntryLocationIndex idx = new EntryLocationIndex(serverConfiguration, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE);

        // Add some dummy indexes
        idx.addLocation(40312, 0, 1);
        idx.addLocation(40313, 10, 2);
        idx.addLocation(40320, 0, 3);

        // Add more indexes in a different batch
        idx.addLocation(40313, 11, 5);
        idx.addLocation(40313, 12, 6);
        idx.addLocation(40320, 1, 7);
        idx.addLocation(40312, 3, 4);

        idx.delete(40313);

        assertEquals(1, idx.getLocation(40312, 0));
        assertEquals(4, idx.getLocation(40312, 3));
        assertEquals(3, idx.getLocation(40320, 0));
        assertEquals(7, idx.getLocation(40320, 1));

        assertEquals(2, idx.getLocation(40313, 10));
        assertEquals(5, idx.getLocation(40313, 11));
        assertEquals(6, idx.getLocation(40313, 12));

        idx.removeOffsetFromDeletedLedgers();

        // After flush the keys will be removed
        assertEquals(0, idx.getLocation(40313, 10));
        assertEquals(0, idx.getLocation(40313, 11));
        assertEquals(0, idx.getLocation(40313, 12));

        idx.close();
    }

    @Test
    public void deleteBatchLedgersTest() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        EntryLocationIndex idx = new EntryLocationIndex(serverConfiguration, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE);

        int numLedgers = 1000;
        int numEntriesPerLedger = 100;

        int location = 0;
        try (KeyValueStorage.Batch batch = idx.newBatch()) {
            for (int entryId = 0; entryId < numEntriesPerLedger; ++entryId) {
                for (int ledgerId = 0; ledgerId < numLedgers; ++ledgerId) {
                    idx.addLocation(batch, ledgerId, entryId, location);
                    location++;
                }
            }
            batch.flush();
        }

        int expectedLocation = 0;
        for (int entryId = 0; entryId < numEntriesPerLedger; ++entryId) {
            for (int ledgerId = 0; ledgerId < numLedgers; ++ledgerId) {
                assertEquals(expectedLocation, idx.getLocation(ledgerId, entryId));
                expectedLocation++;
            }
        }

        for (int ledgerId = 0; ledgerId < numLedgers; ++ledgerId) {
            if (ledgerId % 2 == 0) {
                idx.delete(ledgerId);
            }
        }

        expectedLocation = 0;
        for (int entryId = 0; entryId < numEntriesPerLedger; ++entryId) {
            for (int ledgerId = 0; ledgerId < numLedgers; ++ledgerId) {
                assertEquals(expectedLocation, idx.getLocation(ledgerId, entryId));
                expectedLocation++;
            }
        }

        idx.removeOffsetFromDeletedLedgers();

        expectedLocation = 0;
        for (int entryId = 0; entryId < numEntriesPerLedger; ++entryId) {
            for (int ledgerId = 0; ledgerId < numLedgers; ++ledgerId) {
                if (ledgerId % 2 == 0) {
                    assertEquals(0, idx.getLocation(ledgerId, entryId));
                } else {
                    assertEquals(expectedLocation, idx.getLocation(ledgerId, entryId));
                }
                expectedLocation++;
            }
        }

        idx.close();
    }

    // this tests if a ledger is added after it has been deleted
    @Test
    public void addLedgerAfterDeleteTest() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        EntryLocationIndex idx = new EntryLocationIndex(serverConfiguration, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE);

        // Add some dummy indexes
        idx.addLocation(40312, 0, 1);
        idx.addLocation(40313, 10, 2);
        idx.addLocation(40320, 0, 3);

        idx.delete(40313);

        // Add more indexes in a different batch
        idx.addLocation(40313, 11, 5);
        idx.addLocation(40313, 12, 6);
        idx.addLocation(40320, 1, 7);
        idx.addLocation(40312, 3, 4);

        idx.removeOffsetFromDeletedLedgers();

        assertEquals(0, idx.getLocation(40313, 11));
        assertEquals(0, idx.getLocation(40313, 12));

        idx.close();
    }

    // test non exist entry
    @Test
    public void testDeleteSpecialEntry() throws IOException {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        EntryLocationIndex idx = new EntryLocationIndex(serverConfiguration, KeyValueStorageRocksDB.factory,
                                                        tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE);

        // Add some dummy indexes
        idx.addLocation(40312, -1, 1);
        idx.addLocation(40313, 10, 2);
        idx.addLocation(40320, 0, 3);

        // Add more indexes in a different batch
        idx.addLocation(40313, 11, 5);
        idx.addLocation(40313, 12, 6);
        idx.addLocation(40320, 1, 7);

        // delete a non exist entry
        idx.delete(40312);
        idx.removeOffsetFromDeletedLedgers();

        // another delete entry operation shouldn't effected
        idx.delete(40313);
        idx.removeOffsetFromDeletedLedgers();
        assertEquals(0, idx.getLocation(40312, 10));
    }

    @Test
    public void testEntryIndexLookupLatencyStats() throws IOException {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        TestStatsProvider statsProvider = new TestStatsProvider();
        EntryLocationIndex idx = new EntryLocationIndex(serverConfiguration, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), statsProvider.getStatsLogger("scope"));

        // Add some dummy indexes
        idx.addLocation(40313, 11, 5);

        // successful lookup
        assertEquals(5, idx.getLocation(40313, 11));
        TestStatsProvider.TestOpStatsLogger lookupEntryLocationOpStats =
                statsProvider.getOpStatsLogger("scope.lookup-entry-location");
        assertEquals(1, lookupEntryLocationOpStats.getSuccessCount());
        assertTrue(lookupEntryLocationOpStats.getSuccessAverage() > 0);

        // failed lookup
        assertEquals(0, idx.getLocation(12345, 1));
        assertEquals(1, lookupEntryLocationOpStats.getFailureCount());
        assertEquals(1, lookupEntryLocationOpStats.getSuccessCount());
    }

    @Test
    @Timeout(60)
    public void testClose() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        EntryLocationIndex idx = new EntryLocationIndex(serverConfiguration, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE);

        // mock EntryLocationIndex is compacting
        idx.compacting.set(true);
        AtomicBoolean closeFlag = new AtomicBoolean(false);
        AtomicLong closeEscapedMills = new AtomicLong(0);
        new Thread(() -> {
            try {
                long start = System.currentTimeMillis();
                idx.close();
                closeEscapedMills.set(System.currentTimeMillis() - start);
                closeFlag.set(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
        long sleepMills = 10_000;
        Thread.sleep(sleepMills);
        assertFalse(closeFlag.get());

        // mock EntryLocationIndex finish compacting
        idx.compacting.set(false);
        Awaitility.await().untilAsserted(() -> assertTrue(closeFlag.get()));
        assertTrue(closeEscapedMills.get() >= sleepMills);
    }
}
