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

import com.google.protobuf.ByteString;
import java.io.File;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

/**
 * Unit test for {@link EntryLocationIndex}.
 */
public class LedgerMetadataIndexSyncTest {

    private final ServerConfiguration serverConfiguration = new ServerConfiguration();

    @Test
    public void syncOperationTest() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        serverConfiguration.setDbLedgerMetadataIndexSyncEnable(true);
        LedgerMetadataIndex idx = LedgerMetadataIndex.newInstance(serverConfiguration, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE);

        // Add some dummy indexes
        idx.set(40312, DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(true)
                .setMasterKey(ByteString.EMPTY).build());
        idx.set(40313, DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false)
                .setMasterKey(ByteString.EMPTY).build());
        idx.flush();

        // Add more indexes in a different batch
        idx.set(40314, DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(true)
                .setMasterKey(ByteString.EMPTY).build());
        idx.set(40315, DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false)
                .setMasterKey(ByteString.EMPTY).build());
        idx.flush();

        assertEquals(true, idx.get(40312).getExists());
        assertEquals(true, idx.get(40312).getFenced());
        assertEquals(true, idx.get(40313).getExists());
        assertEquals(false, idx.get(40313).getFenced());

        assertEquals(true, idx.get(40314).getExists());
        assertEquals(true, idx.get(40314).getFenced());
        assertEquals(true, idx.get(40315).getExists());
        assertEquals(false, idx.get(40315).getFenced());

        idx.delete(40313);
        idx.removeDeletedLedgers();

        // After flush the keys will be removed
        assertEquals(true, idx.get(40312).getExists());
        assertEquals(true, idx.get(40312).getFenced());
        try {
            idx.get(40313);
        } catch (Bookie.NoLedgerException e) {
            assertEquals(Bookie.NoLedgerException.class, e.getClass());
        }

        assertEquals(true, idx.get(40314).getExists());
        assertEquals(true, idx.get(40314).getFenced());
        assertEquals(true, idx.get(40315).getExists());
        assertEquals(false, idx.get(40315).getFenced());

        idx.close();
    }
}
