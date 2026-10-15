/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ChecksumType;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.SstFileReader;

public class KeyValueStorageRocksDBTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testEntryLocationPartitionedIndexAndFilters() throws Exception {
        File directory = temporaryFolder.newFolder();
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setEntryLocationRocksdbConf(new File(directory, "entry.conf").toString());
        // Small data blocks give the SST enough index entries to span several 4 KiB index partitions.
        configuration.setProperty("dbStorage_rocksDB_blockSize", 256);
        assertTrue(writeAndCheckEntryLocationSst(configuration, true) > 1);

        configuration.setProperty("dbStorage_rocksDB_partitionedIndexAndFiltersEnabled", false);
        assertEquals(0, writeAndCheckEntryLocationSst(configuration, false));

        configuration = new ServerConfiguration();
        configuration.setEntryLocationRocksdbConf(
                getClass().getClassLoader().getResource("conf/entry_location_rocksdb.conf").getPath());
        assertTrue(writeAndCheckEntryLocationSst(configuration, true) > 0);
    }

    /**
     * Writes 10 ledgers of 1000 entries, compacts them into SSTs and checks their index and filter layout and
     * lookups. Returns the number of index partitions.
     */
    private long writeAndCheckEntryLocationSst(ServerConfiguration configuration, boolean partitioned)
            throws Exception {
        final int ledgers = 10;
        final int entries = 1000;
        File directory = temporaryFolder.newFolder();
        try (KeyValueStorageRocksDB storage = new KeyValueStorageRocksDB(
                directory.toString(), "db", KeyValueStorageFactory.DbConfigType.EntryLocation, configuration)) {
            for (int ledgerId = 0; ledgerId < ledgers; ledgerId++) {
                for (int entryId = 0; entryId < entries; entryId++) {
                    storage.put(entryLocationKey(ledgerId, entryId), location(ledgerId, entryId));
                }
            }
            storage.compact();
            List<Path> ssts;
            try (Stream<Path> files = Files.list(directory.toPath().resolve("db"))) {
                ssts = files.filter(path -> path.toString().endsWith(".sst")).collect(Collectors.toList());
            }
            assertTrue("Compaction must produce an SST", !ssts.isEmpty());
            long indexPartitions = 0;
            for (Path sst : ssts) {
                try (Options options = new Options(); SstFileReader reader = new SstFileReader(options)) {
                    reader.open(sst.toString());
                    indexPartitions += reader.getTableProperties().getIndexPartitions();
                    // 10 bits per key: the filter needs several 4 KiB partitions when it is partitioned.
                    assertTrue(reader.getTableProperties().getFilterSize() > 2 * 4096);
                }
                // The metaindex block is not compressed and names the filter block by its kind.
                String content = new String(Files.readAllBytes(sst), StandardCharsets.ISO_8859_1);
                assertEquals(partitioned, content.contains("partitionedfilter."));
                assertEquals(!partitioned, content.contains("fullfilter."));
            }

            for (int ledgerId = 0; ledgerId < ledgers; ledgerId++) {
                for (int entryId = 0; entryId < entries; entryId++) {
                    assertArrayEquals(location(ledgerId, entryId), storage.get(entryLocationKey(ledgerId, entryId)));
                }
                assertNull(storage.get(entryLocationKey(ledgerId, entries)));
                assertNull(storage.get(entryLocationKey(ledgerId + ledgers, 0)));
                assertArrayEquals(entryLocationKey(ledgerId, entries - 1),
                        storage.getFloor(entryLocationKey(ledgerId, Long.MAX_VALUE)).getKey());
            }
            return indexPartitions;
        }
    }

    private static byte[] entryLocationKey(long ledgerId, long entryId) {
        return ByteBuffer.allocate(16).putLong(ledgerId).putLong(entryId).array();
    }

    private static byte[] location(long ledgerId, long entryId) {
        return ByteBuffer.allocate(8).putLong(ledgerId << 32 | entryId * 1024).array();
    }

    @Test
    public void testRocksDBInitiateWithBookieConfiguration() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setEntryLocationRocksdbConf("entry_location_rocksdb.conf");
        File tmpDir = Files.createTempDirectory("bk-kv-rocksdbtest-conf").toFile();
        Files.createDirectory(Paths.get(tmpDir.toString(), "subDir"));
        KeyValueStorageRocksDB rocksDB = new KeyValueStorageRocksDB(tmpDir.toString(), "subDir",
            KeyValueStorageFactory.DbConfigType.EntryLocation, configuration);
        assertNull(rocksDB.getColumnFamilyDescriptors());

        Options options = (Options) rocksDB.getOptions();
        assertEquals(64 * 1024 * 1024, options.writeBufferSize());
        assertEquals(4, options.maxWriteBufferNumber());
        assertEquals(256 * 1024 * 1024, options.maxBytesForLevelBase());
        assertEquals(true, options.levelCompactionDynamicLevelBytes());
        rocksDB.close();
    }

    @Test
    public void testRocksDBInitiateWithConfigurationFile() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        URL url = getClass().getClassLoader().getResource("test_entry_location_rocksdb.conf");
        configuration.setEntryLocationRocksdbConf(url.getPath());
        File tmpDir = Files.createTempDirectory("bk-kv-rocksdbtest-file").toFile();
        Files.createDirectory(Paths.get(tmpDir.toString(), "subDir"));
        KeyValueStorageRocksDB rocksDB = new KeyValueStorageRocksDB(tmpDir.toString(), "subDir",
            KeyValueStorageFactory.DbConfigType.EntryLocation, configuration);
        assertNotNull(rocksDB.getColumnFamilyDescriptors());

        DBOptions dbOptions = (DBOptions) rocksDB.getOptions();
        assertTrue(dbOptions.createIfMissing());
        assertEquals(1, dbOptions.keepLogFileNum());
        assertEquals(1000, dbOptions.maxTotalWalSize());

        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = rocksDB.getColumnFamilyDescriptors();
        ColumnFamilyOptions familyOptions = columnFamilyDescriptorList.get(0).getOptions();
        assertEquals(CompressionType.LZ4_COMPRESSION, familyOptions.compressionType());
        assertEquals(1024, familyOptions.writeBufferSize());
        assertEquals(1, familyOptions.maxWriteBufferNumber());
        assertEquals(true, familyOptions.levelCompactionDynamicLevelBytes());
        rocksDB.close();
    }

    @Test
    public void testReadChecksumTypeFromBookieConfiguration() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setEntryLocationRocksdbConf("entry_location_rocksdb.conf");
        File tmpDir = Files.createTempDirectory("bk-kv-rocksdbtest-conf").toFile();
        Files.createDirectory(Paths.get(tmpDir.toString(), "subDir"));
        KeyValueStorageRocksDB rocksDB = new KeyValueStorageRocksDB(tmpDir.toString(), "subDir",
            KeyValueStorageFactory.DbConfigType.EntryLocation, configuration);
        assertNull(rocksDB.getColumnFamilyDescriptors());

        Options options = (Options) rocksDB.getOptions();
        assertEquals(ChecksumType.kxxHash, ((BlockBasedTableConfig) options.tableFormatConfig()).checksumType());
    }

    //@Test
    public void testReadChecksumTypeFromConfigurationFile() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        URL url = getClass().getClassLoader().getResource("test_entry_location_rocksdb.conf");
        configuration.setEntryLocationRocksdbConf(url.getPath());
        File tmpDir = Files.createTempDirectory("bk-kv-rocksdbtest-file").toFile();
        Files.createDirectory(Paths.get(tmpDir.toString(), "subDir"));
        KeyValueStorageRocksDB rocksDB = new KeyValueStorageRocksDB(tmpDir.toString(), "subDir",
            KeyValueStorageFactory.DbConfigType.EntryLocation, configuration);
        assertNotNull(rocksDB.getColumnFamilyDescriptors());

        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = rocksDB.getColumnFamilyDescriptors();
        ColumnFamilyOptions familyOptions = columnFamilyDescriptorList.get(0).getOptions();
        // There is a bug in RocksDB, which can't load BlockedBasedTableConfig from Options file.
        // https://github.com/facebook/rocksdb/issues/5297
        // After the PR: https://github.com/facebook/rocksdb/pull/10826 merge, we can turn on this test.
        assertEquals(ChecksumType.kxxHash, ((BlockBasedTableConfig) familyOptions.tableFormatConfig()).checksumType());
    }

    @Test
    public void testLevelCompactionDynamicLevelBytesFromConfigurationFile() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        URL url = getClass().getClassLoader().getResource("conf/entry_location_rocksdb.conf");
        configuration.setEntryLocationRocksdbConf(url.getPath());
        File tmpDir = Files.createTempDirectory("bk-kv-rocksdbtest-file").toFile();
        Files.createDirectory(Paths.get(tmpDir.toString(), "subDir"));
        KeyValueStorageRocksDB rocksDB = new KeyValueStorageRocksDB(tmpDir.toString(), "subDir",
                KeyValueStorageFactory.DbConfigType.EntryLocation, configuration);
        assertNotNull(rocksDB.getColumnFamilyDescriptors());

        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = rocksDB.getColumnFamilyDescriptors();
        ColumnFamilyOptions familyOptions = columnFamilyDescriptorList.get(0).getOptions();
        assertEquals(true, familyOptions.levelCompactionDynamicLevelBytes());
    }

    @Test
    public void testCallCountAfterClose() throws IOException {
        ServerConfiguration configuration = new ServerConfiguration();
        URL url = getClass().getClassLoader().getResource("test_entry_location_rocksdb.conf");
        configuration.setEntryLocationRocksdbConf(url.getPath());
        File tmpDir = Files.createTempDirectory("bk-kv-rocksdbtest-file").toFile();
        Files.createDirectory(Paths.get(tmpDir.toString(), "subDir"));
        KeyValueStorageRocksDB rocksDB = new KeyValueStorageRocksDB(tmpDir.toString(), "subDir",
                KeyValueStorageFactory.DbConfigType.EntryLocation, configuration);
        assertNotNull(rocksDB.getColumnFamilyDescriptors());
        rocksDB.close();
        IOException exception = assertThrows(IOException.class, rocksDB::count);
        assertEquals("RocksDB is closed", exception.getMessage());
    }
}
