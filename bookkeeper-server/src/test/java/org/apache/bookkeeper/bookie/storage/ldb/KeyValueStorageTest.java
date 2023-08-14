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
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit test for {@link KeyValueStorage}.
 */
@RunWith(Parameterized.class)
public class KeyValueStorageTest {

    private final KeyValueStorageFactory storageFactory;
    private final ServerConfiguration configuration;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { KeyValueStorageRocksDB.factory } });
    }

    public KeyValueStorageTest(KeyValueStorageFactory storageFactory) {
        this.storageFactory = storageFactory;
        this.configuration = new ServerConfiguration();
    }

    private static long fromArray(byte[] array) {
        return ArrayUtil.getLong(array, 0);
    }

    private static byte[] toArray(long n) {
        byte[] b = new byte[8];
        ArrayUtil.setLong(b, 0, n);
        return b;
    }

    @Test
    public void simple() throws Exception {
        File tmpDir = Files.createTempDirectory("junitTemporaryFolder").toFile();
        Files.createDirectory(Paths.get(tmpDir.toString(), "subDir"));

        KeyValueStorage db = storageFactory.newKeyValueStorage(tmpDir.toString(), "subDir", DbConfigType.Default,
                configuration);

        assertEquals(null, db.getFloor(toArray(3)));
        assertEquals(0, db.count());

        db.put(toArray(5), toArray(5));

        assertEquals(null, db.getFloor(toArray(3)));
        assertEquals(1, db.count());

        assertEquals(null, db.getFloor(toArray(5)));
        assertEquals(5, fromArray(db.getFloor(toArray(6)).getKey()));

        db.put(toArray(3), toArray(3));

        assertEquals(null, db.getFloor(toArray(3)));
        assertEquals(2, db.count());

        // //

        db.put(toArray(5), toArray(5));
        // Count can be imprecise
        assertTrue(db.count() > 0);

        assertEquals(null, db.getFloor(toArray(1)));
        assertEquals(null, db.getFloor(toArray(3)));
        assertEquals(3, fromArray(db.getFloor(toArray(5)).getKey()));
        assertEquals(5, fromArray(db.getFloor(toArray(6)).getKey()));
        assertEquals(5, fromArray(db.getFloor(toArray(10)).getKey()));

        // Iterate
        List<Long> foundKeys = Lists.newArrayList();
        try (CloseableIterator<Entry<byte[], byte[]>> iter = db.iterator()) {
            while (iter.hasNext()) {
                foundKeys.add(fromArray(iter.next().getKey()));
            }
        }

        assertEquals(Lists.newArrayList(3L, 5L), foundKeys);

        // Iterate over keys
        foundKeys = Lists.newArrayList();
        CloseableIterator<byte[]> iter2 = db.keys();
        try {
            while (iter2.hasNext()) {
                foundKeys.add(fromArray(iter2.next()));
            }
        } finally {
            iter2.close();
        }

        assertEquals(Lists.newArrayList(3L, 5L), foundKeys);

        // Scan with limits
        foundKeys = Lists.newArrayList();
        iter2 = db.keys(toArray(1), toArray(4));
        try {
            while (iter2.hasNext()) {
                foundKeys.add(fromArray(iter2.next()));
            }
        } finally {
            iter2.close();
        }

        assertEquals(Lists.newArrayList(3L), foundKeys);

        // Test deletion
        db.put(toArray(10), toArray(10));
        db.put(toArray(11), toArray(11));
        db.put(toArray(12), toArray(12));
        db.put(toArray(14), toArray(14));

        // Count can be imprecise
        assertTrue(db.count() > 0);

        assertEquals(10L, fromArray(db.get(toArray(10))));
        db.delete(toArray(10));
        assertEquals(null, db.get(toArray(10)));
        assertTrue(db.count() > 0);

        Batch batch = db.newBatch();
        batch.remove(toArray(11));
        batch.remove(toArray(12));
        batch.remove(toArray(13));
        batch.flush();
        assertEquals(null, db.get(toArray(11)));
        assertEquals(null, db.get(toArray(12)));
        assertEquals(null, db.get(toArray(13)));
        assertEquals(14L, fromArray(db.get(toArray(14))));
        batch.close();

        db.close();
        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void testBatch() throws Exception {

        configuration.setOperationMaxNumbersInSingleRocksDBWriteBatch(5);

        File tmpDir = Files.createTempDirectory("junitTemporaryFolder").toFile();
        Files.createDirectory(Paths.get(tmpDir.toString(), "subDir"));

        KeyValueStorage db = storageFactory.newKeyValueStorage(tmpDir.toString(), "subDir", DbConfigType.Default,
                configuration);

        assertEquals(null, db.getFloor(toArray(3)));
        assertEquals(0, db.count());

        Batch batch = db.newBatch();
        assertEquals(0, batch.batchCount());

        batch.put(toArray(1), toArray(1));
        batch.put(toArray(2), toArray(2));
        assertEquals(2, batch.batchCount());

        batch.put(toArray(3), toArray(3));
        batch.put(toArray(4), toArray(4));
        batch.put(toArray(5), toArray(5));
        assertEquals(0, batch.batchCount());
        batch.put(toArray(6), toArray(6));
        assertEquals(1, batch.batchCount());

        batch.flush();
        assertEquals(1, batch.batchCount());
        batch.close();
        assertEquals(0, batch.batchCount());

        db.close();
        FileUtils.deleteDirectory(tmpDir);
    }
}
