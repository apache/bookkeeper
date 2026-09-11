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
import java.nio.ByteOrder;
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

public class KeyValueStorageRocksDBTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCompatibleSstFormatWithAndWithoutConfigurationFiles() throws Exception {
        for (boolean useConfigurationFile : new boolean[]{false, true}) {
            for (KeyValueStorageFactory.DbConfigType type : KeyValueStorageFactory.DbConfigType.values()) {
                File directory = temporaryFolder.newFolder();
                ServerConfiguration configuration = new ServerConfiguration();
                // Explicit nonexistent paths ensure that the fallback does not load local operator settings.
                String defaultConf = new File(directory, "default.conf").toString();
                String ledgerConf = new File(directory, "ledger.conf").toString();
                String entryConf = new File(directory, "entry.conf").toString();
                if (useConfigurationFile) {
                    defaultConf = getClass().getClassLoader().getResource("conf/default_rocksdb.conf").getPath();
                    ledgerConf = getClass().getClassLoader().getResource("conf/ledger_metadata_rocksdb.conf").getPath();
                    entryConf = getClass().getClassLoader().getResource("conf/entry_location_rocksdb.conf").getPath();
                }
                configuration.setDefaultRocksDBConf(defaultConf);
                configuration.setLedgerMetadataRocksdbConf(ledgerConf);
                configuration.setEntryLocationRocksdbConf(entryConf);
                byte[] key = new byte[]{1};
                byte[] value = new byte[]{2};
                try (KeyValueStorageRocksDB storage = new KeyValueStorageRocksDB(
                        directory.toString(), "db", type, configuration)) {
                    storage.put(key, value);
                    storage.sync();
                    storage.compact();
                    assertArrayEquals(value, storage.get(key));
                    List<Path> ssts;
                    try (Stream<Path> files = Files.list(directory.toPath().resolve("db"))) {
                        ssts = files.filter(path -> path.toString().endsWith(".sst")).collect(Collectors.toList());
                    }
                    assertTrue("Compaction must produce an SST", !ssts.isEmpty());
                    for (Path sst : ssts) {
                        byte[] bytes = Files.readAllBytes(sst);
                        // The footer stores a little-endian format version before the 8-byte magic number.
                        // TableProperties.getFormatVersion() is not the block-based table footer version.
                        int format = ByteBuffer.wrap(bytes, bytes.length - 12, 4)
                                .order(ByteOrder.LITTLE_ENDIAN).getInt();
                        assertEquals("SST must be readable by RocksDB 7.9.2: " + sst, 5, format);
                    }
                }
            }
        }
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
