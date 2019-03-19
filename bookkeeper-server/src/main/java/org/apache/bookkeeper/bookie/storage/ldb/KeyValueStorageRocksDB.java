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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.primitives.UnsignedBytes;

//CHECKSTYLE.OFF: IllegalImport
import io.netty.util.internal.PlatformDependent;
//CHECKSTYLE.ON: IllegalImport

import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ChecksumType;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB based implementation of the KeyValueStorage.
 */
public class KeyValueStorageRocksDB implements KeyValueStorage {

    static KeyValueStorageFactory factory = (path, dbConfigType, conf) -> new KeyValueStorageRocksDB(path, dbConfigType,
            conf);

    private final RocksDB db;

    private final WriteOptions optionSync;
    private final WriteOptions optionDontSync;

    private final ReadOptions optionCache;
    private final ReadOptions optionDontCache;

    private final WriteBatch emptyBatch;

    private static final String ROCKSDB_LOG_LEVEL = "dbStorage_rocksDB_logLevel";
    private static final String ROCKSDB_LZ4_COMPRESSION_ENABLED = "dbStorage_rocksDB_lz4CompressionEnabled";
    private static final String ROCKSDB_WRITE_BUFFER_SIZE_MB = "dbStorage_rocksDB_writeBufferSizeMB";
    private static final String ROCKSDB_SST_SIZE_MB = "dbStorage_rocksDB_sstSizeInMB";
    private static final String ROCKSDB_BLOCK_SIZE = "dbStorage_rocksDB_blockSize";
    private static final String ROCKSDB_BLOOM_FILTERS_BITS_PER_KEY = "dbStorage_rocksDB_bloomFilterBitsPerKey";
    private static final String ROCKSDB_BLOCK_CACHE_SIZE = "dbStorage_rocksDB_blockCacheSize";
    private static final String ROCKSDB_NUM_LEVELS = "dbStorage_rocksDB_numLevels";
    private static final String ROCKSDB_NUM_FILES_IN_LEVEL0 = "dbStorage_rocksDB_numFilesInLevel0";
    private static final String ROCKSDB_MAX_SIZE_IN_LEVEL1_MB = "dbStorage_rocksDB_maxSizeInLevel1MB";

    public KeyValueStorageRocksDB(String path, DbConfigType dbConfigType, ServerConfiguration conf) throws IOException {
        this(path, dbConfigType, conf, false);
    }

    public KeyValueStorageRocksDB(String path, DbConfigType dbConfigType, ServerConfiguration conf, boolean readOnly)
            throws IOException {
        try {
            RocksDB.loadLibrary();
        } catch (Throwable t) {
            throw new IOException("Failed to load RocksDB JNI library", t);
        }

        this.optionSync = new WriteOptions();
        this.optionDontSync = new WriteOptions();
        this.optionCache = new ReadOptions();
        this.optionDontCache = new ReadOptions();
        this.emptyBatch = new WriteBatch();

        try (Options options = new Options()) {
            options.setCreateIfMissing(true);

            if (dbConfigType == DbConfigType.Huge) {
                // Set default RocksDB block-cache size to 10% of direct mem, unless override
                long defaultRocksDBBlockCacheSizeBytes = PlatformDependent.maxDirectMemory() / 10;
                long blockCacheSize = DbLedgerStorage.getLongVariableOrDefault(conf, ROCKSDB_BLOCK_CACHE_SIZE,
                        defaultRocksDBBlockCacheSizeBytes);

                long writeBufferSizeMB = conf.getInt(ROCKSDB_WRITE_BUFFER_SIZE_MB, 64);
                long sstSizeMB = conf.getInt(ROCKSDB_SST_SIZE_MB, 64);
                int numLevels = conf.getInt(ROCKSDB_NUM_LEVELS, -1);
                int numFilesInLevel0 = conf.getInt(ROCKSDB_NUM_FILES_IN_LEVEL0, 4);
                long maxSizeInLevel1MB = conf.getLong(ROCKSDB_MAX_SIZE_IN_LEVEL1_MB, 256);
                int blockSize = conf.getInt(ROCKSDB_BLOCK_SIZE, 64 * 1024);
                int bloomFilterBitsPerKey = conf.getInt(ROCKSDB_BLOOM_FILTERS_BITS_PER_KEY, 10);
                boolean lz4CompressionEnabled = conf.getBoolean(ROCKSDB_LZ4_COMPRESSION_ENABLED, true);

                if (lz4CompressionEnabled) {
                    options.setCompressionType(CompressionType.LZ4_COMPRESSION);
                }
                options.setWriteBufferSize(writeBufferSizeMB * 1024 * 1024);
                options.setMaxWriteBufferNumber(4);
                if (numLevels > 0) {
                    options.setNumLevels(numLevels);
                }
                options.setLevelZeroFileNumCompactionTrigger(numFilesInLevel0);
                options.setMaxBytesForLevelBase(maxSizeInLevel1MB * 1024 * 1024);
                options.setMaxBackgroundCompactions(16);
                options.setMaxBackgroundFlushes(16);
                options.setIncreaseParallelism(32);
                options.setMaxTotalWalSize(512 * 1024 * 1024);
                options.setMaxOpenFiles(-1);
                options.setTargetFileSizeBase(sstSizeMB * 1024 * 1024);
                options.setDeleteObsoleteFilesPeriodMicros(TimeUnit.HOURS.toMicros(1));

                BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
                tableOptions.setBlockSize(blockSize);
                tableOptions.setBlockCacheSize(blockCacheSize);
                tableOptions.setFormatVersion(2);
                tableOptions.setChecksumType(ChecksumType.kxxHash);
                if (bloomFilterBitsPerKey > 0) {
                    tableOptions.setFilter(new BloomFilter(bloomFilterBitsPerKey, false));
                }

                // Options best suited for HDDs
                tableOptions.setCacheIndexAndFilterBlocks(true);
                options.setLevelCompactionDynamicLevelBytes(true);

                options.setTableFormatConfig(tableOptions);
            }

            // Configure log level
            String logLevel = conf.getString(ROCKSDB_LOG_LEVEL, "info");
            switch (logLevel) {
            case "debug":
                options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
                break;
            case "info":
                options.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
                break;
            case "warn":
                options.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);
                break;
            case "error":
                options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
                break;
            default:
                log.warn("Unrecognized RockDB log level: {}", logLevel);
            }

            // Keep log files for 1month
            options.setKeepLogFileNum(30);
            options.setLogFileTimeToRoll(TimeUnit.DAYS.toSeconds(1));

            try {
                if (readOnly) {
                    db = RocksDB.openReadOnly(options, path);
                } else {
                    db = RocksDB.open(options, path);
                }
            } catch (RocksDBException e) {
                throw new IOException("Error open RocksDB database", e);
            }
        }

        optionSync.setSync(true);
        optionDontSync.setSync(false);

        optionCache.setFillCache(true);
        optionDontCache.setFillCache(false);
    }

    @Override
    public void close() throws IOException {
        db.close();
        optionSync.close();
        optionDontSync.close();
        optionCache.close();
        optionDontCache.close();
        emptyBatch.close();
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(optionDontSync, key, value);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB put", e);
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB get", e);
        }
    }

    @Override
    public int get(byte[] key, byte[] value) throws IOException {
        try {
            int res = db.get(key, value);
            if (res == RocksDB.NOT_FOUND) {
                return -1;
            } else if (res > value.length) {
                throw new IOException("Value array is too small to fit the result");
            } else {
                return res;
            }
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB get", e);
        }
    }

    @Override
    public Entry<byte[], byte[]> getFloor(byte[] key) throws IOException {
        try (RocksIterator iterator = db.newIterator(optionCache)) {
            // Position the iterator on the record whose key is >= to the supplied key
            iterator.seek(key);

            if (!iterator.isValid()) {
                // There are no entries >= key
                iterator.seekToLast();
                if (iterator.isValid()) {
                    return new EntryWrapper(iterator.key(), iterator.value());
                } else {
                    // Db is empty
                    return null;
                }
            }

            iterator.prev();

            if (!iterator.isValid()) {
                // Iterator is on the 1st entry of the db and this entry key is >= to the target
                // key
                return null;
            } else {
                return new EntryWrapper(iterator.key(), iterator.value());
            }
        }
    }

    @Override
    public Entry<byte[], byte[]> getCeil(byte[] key) throws IOException {
        try (RocksIterator iterator = db.newIterator(optionCache)) {
            // Position the iterator on the record whose key is >= to the supplied key
            iterator.seek(key);

            if (iterator.isValid()) {
                return new EntryWrapper(iterator.key(), iterator.value());
            } else {
                return null;
            }
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        try {
            db.delete(optionDontSync, key);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB delete", e);
        }
    }

    @Override
    public void sync() throws IOException {
        try {
            db.write(optionSync, emptyBatch);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public CloseableIterator<byte[]> keys() {
        final RocksIterator iterator = db.newIterator(optionCache);
        iterator.seekToFirst();

        return new CloseableIterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public byte[] next() {
                checkState(iterator.isValid());
                byte[] key = iterator.key();
                iterator.next();
                return key;
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    @Override
    public CloseableIterator<byte[]> keys(byte[] firstKey, byte[] lastKey) {
        final RocksIterator iterator = db.newIterator(optionCache);
        iterator.seek(firstKey);

        return new CloseableIterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid() && ByteComparator.compare(iterator.key(), lastKey) < 0;
            }

            @Override
            public byte[] next() {
                checkState(iterator.isValid());
                byte[] key = iterator.key();
                iterator.next();
                return key;
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    @Override
    public CloseableIterator<Entry<byte[], byte[]>> iterator() {
        final RocksIterator iterator = db.newIterator(optionDontCache);
        iterator.seekToFirst();
        final EntryWrapper entryWrapper = new EntryWrapper();

        return new CloseableIterator<Entry<byte[], byte[]>>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public Entry<byte[], byte[]> next() {
                checkState(iterator.isValid());
                entryWrapper.key = iterator.key();
                entryWrapper.value = iterator.value();
                iterator.next();
                return entryWrapper;
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    @Override
    public long count() throws IOException {
        try {
            return db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new IOException("Error in getting records count", e);
        }
    }

    @Override
    public Batch newBatch() {
        return new RocksDBBatch();
    }

    private class RocksDBBatch implements Batch {
        private final WriteBatch writeBatch = new WriteBatch();

        @Override
        public void close() {
            writeBatch.close();
        }

        @Override
        public void put(byte[] key, byte[] value) throws IOException {
            try {
                writeBatch.put(key, value);
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        @Override
        public void remove(byte[] key) throws IOException {
            try {
                writeBatch.delete(key);
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        @Override
        public void clear() {
            writeBatch.clear();
        }

        @Override
        public void deleteRange(byte[] beginKey, byte[] endKey) throws IOException {
            try {
                writeBatch.deleteRange(beginKey, endKey);
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        @Override
        public void flush() throws IOException {
            try {
                db.write(optionSync, writeBatch);
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }
    }

    private static final class EntryWrapper implements Entry<byte[], byte[]> {
        // This is not final since the iterator will reuse the same EntryWrapper
        // instance at each step
        private byte[] key;
        private byte[] value;

        public EntryWrapper() {
            this.key = null;
            this.value = null;
        }

        public EntryWrapper(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public byte[] setValue(byte[] value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] getKey() {
            return key;
        }
    }

    private static final Comparator<byte[]> ByteComparator = UnsignedBytes.lexicographicalComparator();

    private static final Logger log = LoggerFactory.getLogger(KeyValueStorageRocksDB.class);
}
