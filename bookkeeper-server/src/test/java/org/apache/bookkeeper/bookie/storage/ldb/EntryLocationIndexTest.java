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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

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
}
