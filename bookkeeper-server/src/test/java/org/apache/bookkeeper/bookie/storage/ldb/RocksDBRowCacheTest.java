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
import static org.junit.Assert.assertNotNull;

import java.io.File;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test RocksDB row cache configuration.
 */
public class RocksDBRowCacheTest {

    private KeyValueStorageRocksDB storage;
    private File tempDir;
    private ServerConfiguration conf;

    @Before
    public void setup() throws Exception {
        tempDir = File.createTempFile("test", "");
        tempDir.delete();
        tempDir.mkdir();

        conf = new ServerConfiguration();
    }

    @After
    public void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir);
        }
    }

    @Test
    public void testRowCacheEnabled() throws Exception {
        // Set row cache size to 64MB
        conf.setProperty("dbStorage_rocksDB_rowCacheSize", 64 * 1024 * 1024L);

        storage = new KeyValueStorageRocksDB(
            tempDir.getAbsolutePath(),
            "test-db",
            KeyValueStorageFactory.DbConfigType.EntryLocation,
            conf
        );

        // Write some test data
        byte[] key1 = "test-key-1".getBytes();
        byte[] value1 = "test-value-1".getBytes();
        byte[] key2 = "test-key-2".getBytes();
        byte[] value2 = "test-value-2".getBytes();

        storage.put(key1, value1);
        storage.put(key2, value2);
        storage.sync();

        // Read back and verify
        byte[] readValue1 = storage.get(key1);
        byte[] readValue2 = storage.get(key2);

        assertNotNull(readValue1);
        assertNotNull(readValue2);
        assertEquals("test-value-1", new String(readValue1));
        assertEquals("test-value-2", new String(readValue2));

        // Multiple reads should benefit from row cache
        for (int i = 0; i < 100; i++) {
            byte[] cachedRead = storage.get(key1);
            assertEquals("test-value-1", new String(cachedRead));
        }
    }

    @Test
    public void testRowCacheDisabledByDefault() throws Exception {
        // Don't set row cache size - should be disabled by default
        storage = new KeyValueStorageRocksDB(
            tempDir.getAbsolutePath(),
            "test-db",
            KeyValueStorageFactory.DbConfigType.EntryLocation,
            conf
        );

        // Should still work without row cache
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        storage.put(key, value);
        storage.sync();

        byte[] readValue = storage.get(key);
        assertNotNull(readValue);
        assertEquals("test-value", new String(readValue));
    }

    @Test
    public void testRowCacheWithLargeData() throws Exception {
        // Set row cache size to 16MB
        conf.setProperty("dbStorage_rocksDB_rowCacheSize", 16 * 1024 * 1024L);

        storage = new KeyValueStorageRocksDB(
            tempDir.getAbsolutePath(),
            "test-db",
            KeyValueStorageFactory.DbConfigType.EntryLocation,
            conf
        );

        // Write larger values that would benefit from row cache
        int numKeys = 1000;
        byte[] largeValue = new byte[1024]; // 1KB per value

        for (int i = 0; i < numKeys; i++) {
            String key = String.format("key-%06d", i);
            storage.put(key.getBytes(), largeValue);
        }
        storage.sync();

        // Read hot keys multiple times - should be cached
        for (int round = 0; round < 10; round++) {
            for (int i = 0; i < 100; i++) { // Read first 100 keys repeatedly
                String key = String.format("key-%06d", i);
                byte[] value = storage.get(key.getBytes());
                assertNotNull(value);
                assertEquals(1024, value.length);
            }
        }
    }
}
