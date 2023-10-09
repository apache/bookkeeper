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


import static com.google.common.base.Preconditions.checkState;
//CHECKSTYLE.OFF: IllegalImport
//CHECKSTYLE.OFF: ImportOrder
import static io.netty.util.internal.PlatformDependent.maxDirectMemory;
//CHECKSTYLE.ON: IllegalImport
//CHECKSTYLE.ON: ImportOrder

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ChecksumType;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.OptionsUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB based implementation of the KeyValueStorage.
 */
public class KeyValueStorageRocksDB implements KeyValueStorage {

    static KeyValueStorageFactory factory = (defaultBasePath, subPath, dbConfigType, conf) ->
            new KeyValueStorageRocksDB(defaultBasePath, subPath, dbConfigType, conf);

    private final RocksDB db;
    private RocksObject options;
    private List<ColumnFamilyDescriptor> columnFamilyDescriptors;

    private final WriteOptions optionSync;
    private final WriteOptions optionDontSync;
    private Cache cache;

    private final ReadOptions optionCache;
    private final ReadOptions optionDontCache;
    private final WriteBatch emptyBatch;
    private final int writeBatchMaxSize;

    private String dbPath;

    private static final String ROCKSDB_LOG_PATH = "dbStorage_rocksDB_logPath";
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
    private static final String ROCKSDB_FORMAT_VERSION = "dbStorage_rocksDB_format_version";
    private static final String ROCKSDB_CHECKSUM_TYPE = "dbStorage_rocksDB_checksum_type";

    public KeyValueStorageRocksDB(String basePath, String subPath, DbConfigType dbConfigType, ServerConfiguration conf)
            throws IOException {
        this(basePath, subPath, dbConfigType, conf, false);
    }

    public KeyValueStorageRocksDB(String basePath, String subPath, DbConfigType dbConfigType, ServerConfiguration conf,
                                  boolean readOnly)
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

        String dbFilePath = "";
        if (dbConfigType == DbConfigType.EntryLocation) {
            dbFilePath = conf.getEntryLocationRocksdbConf();
        } else if (dbConfigType == DbConfigType.LedgerMetadata) {
            dbFilePath = conf.getLedgerMetadataRocksdbConf();
        } else {
            dbFilePath = conf.getDefaultRocksDBConf();
        }
        log.info("Searching for a RocksDB configuration file in {}", dbFilePath);
        if (Paths.get(dbFilePath).toFile().exists()) {
            log.info("Found a RocksDB configuration file and using it to initialize the RocksDB");
            db = initializeRocksDBWithConfFile(basePath, subPath, dbConfigType, conf, readOnly, dbFilePath);
        } else {
            log.info("Haven't found the file and read the configuration from the main bookkeeper configuration");
            db = initializeRocksDBWithBookieConf(basePath, subPath, dbConfigType, conf, readOnly);
        }

        optionSync.setSync(true);
        optionDontSync.setSync(false);

        optionCache.setFillCache(true);
        optionDontCache.setFillCache(false);

        this.writeBatchMaxSize = conf.getMaxOperationNumbersInSingleRocksDBBatch();
    }

    private RocksDB initializeRocksDBWithConfFile(String basePath, String subPath, DbConfigType dbConfigType,
                                               ServerConfiguration conf, boolean readOnly,
                                               String dbFilePath) throws IOException {
        DBOptions dbOptions = new DBOptions();
        final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
        final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        try {
            OptionsUtil.loadOptionsFromFile(dbFilePath, Env.getDefault(), dbOptions, cfDescs, false);
            // Configure file path
            String logPath = conf.getString(ROCKSDB_LOG_PATH, "");
            if (!logPath.isEmpty()) {
                Path logPathSetting = FileSystems.getDefault().getPath(logPath, subPath);
                Files.createDirectories(logPathSetting);
                log.info("RocksDB<{}> log path: {}", subPath, logPathSetting);
                dbOptions.setDbLogDir(logPathSetting.toString());
            }
            this.dbPath = FileSystems.getDefault().getPath(basePath, subPath).toFile().toString();
            this.options = dbOptions;
            this.columnFamilyDescriptors = cfDescs;
            if (readOnly) {
                return RocksDB.openReadOnly(dbOptions, dbPath, cfDescs, cfHandles);
            } else {
                return RocksDB.open(dbOptions, dbPath, cfDescs, cfHandles);
            }
        } catch (RocksDBException e) {
            throw new IOException("Error open RocksDB database", e);
        }
    }

    private RocksDB initializeRocksDBWithBookieConf(String basePath, String subPath, DbConfigType dbConfigType,
                                           ServerConfiguration conf, boolean readOnly) throws IOException {
        Options options = new Options();
        options.setCreateIfMissing(true);
        ChecksumType checksumType = ChecksumType.valueOf(conf.getString(ROCKSDB_CHECKSUM_TYPE, "kxxHash"));

        if (dbConfigType == DbConfigType.EntryLocation) {
            /* Set default RocksDB block-cache size to 10% / numberOfLedgers of direct memory, unless override */
            int ledgerDirsSize = conf.getLedgerDirNames().length;
            long defaultRocksDBBlockCacheSizeBytes = maxDirectMemory() / ledgerDirsSize / 10;
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
            int formatVersion = conf.getInt(ROCKSDB_FORMAT_VERSION, 2);

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
            options.setMaxBackgroundJobs(32);
            options.setIncreaseParallelism(32);
            options.setMaxTotalWalSize(512 * 1024 * 1024);
            options.setMaxOpenFiles(-1);
            options.setTargetFileSizeBase(sstSizeMB * 1024 * 1024);
            options.setDeleteObsoleteFilesPeriodMicros(TimeUnit.HOURS.toMicros(1));

            this.cache = new LRUCache(blockCacheSize);
            BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
            tableOptions.setBlockSize(blockSize);
            tableOptions.setBlockCache(cache);
            tableOptions.setFormatVersion(formatVersion);
            tableOptions.setChecksumType(checksumType);
            if (bloomFilterBitsPerKey > 0) {
                tableOptions.setFilterPolicy(new BloomFilter(bloomFilterBitsPerKey, false));
            }

            // Options best suited for HDDs
            tableOptions.setCacheIndexAndFilterBlocks(true);
            options.setLevelCompactionDynamicLevelBytes(true);

            options.setTableFormatConfig(tableOptions);
        } else {
            this.cache = null;
            BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
            tableOptions.setChecksumType(checksumType);
            options.setTableFormatConfig(tableOptions);
        }

            // Configure file path
        String logPath = conf.getString(ROCKSDB_LOG_PATH, "");
        if (!logPath.isEmpty()) {
            Path logPathSetting = FileSystems.getDefault().getPath(logPath, subPath);
            Files.createDirectories(logPathSetting);
            log.info("RocksDB<{}> log path: {}", subPath, logPathSetting);
            options.setDbLogDir(logPathSetting.toString());
        }
        this.dbPath = FileSystems.getDefault().getPath(basePath, subPath).toFile().toString();

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
        this.options = options;
        try {
            if (readOnly) {
                return RocksDB.openReadOnly(options, dbPath);
            } else {
                return RocksDB.open(options, dbPath);
            }
        } catch (RocksDBException e) {
            throw new IOException("Error open RocksDB database", e);
        }
    }

    @Override
    public void close() throws IOException {
        db.close();
        if (cache != null) {
            cache.close();
        }
        if (options != null) {
            options.close();
        }
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
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Entry<byte[], byte[]> getFloor(byte[] key) throws IOException {
        try (Slice upperBound = new Slice(key);
                 ReadOptions option = new ReadOptions(optionCache).setIterateUpperBound(upperBound);
                 RocksIterator iterator = db.newIterator(option)) {
            iterator.seekToLast();
            if (iterator.isValid()) {
                return new EntryWrapper(iterator.key(), iterator.value());
            }
        }
        return null;
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
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
    public String getDBPath() {
        return dbPath;
    }

    @Override
    public void compact(byte[] firstKey, byte[] lastKey) throws IOException {
        try {
            db.compactRange(firstKey, lastKey);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB compact", e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            final long start = System.currentTimeMillis();
            final int oriRocksDBFileCount = db.getLiveFilesMetaData().size();
            final long oriRocksDBSize = getRocksDBSize();
            log.info("Starting RocksDB {} compact, current RocksDB hold {} files and {} Bytes.",
                    db.getName(), oriRocksDBFileCount, oriRocksDBSize);

            db.compactRange();

            final long end = System.currentTimeMillis();
            final int rocksDBFileCount = db.getLiveFilesMetaData().size();
            final long rocksDBSize = getRocksDBSize();
            log.info("RocksDB {} compact finished {} ms, space reduced {} Bytes, current hold {} files and {} Bytes.",
                    db.getName(), end - start, oriRocksDBSize - rocksDBSize, rocksDBFileCount, rocksDBSize);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB compact", e);
        }
    }

    private long getRocksDBSize() {
        List<LiveFileMetaData> liveFilesMetaData = db.getLiveFilesMetaData();
        long rocksDBFileSize = 0L;
        for (LiveFileMetaData fileMetaData : liveFilesMetaData) {
            rocksDBFileSize += fileMetaData.size();
        }
        return rocksDBFileSize;
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
        final Slice upperBound = new Slice(lastKey);
        final ReadOptions option = new ReadOptions(optionCache).setIterateUpperBound(upperBound);
        final RocksIterator iterator = db.newIterator(option);
        iterator.seek(firstKey);

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
                option.close();
                upperBound.close();
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
        return new RocksDBBatch(writeBatchMaxSize);
    }

    private class RocksDBBatch implements Batch {
        private final WriteBatch writeBatch = new WriteBatch();
        private final int batchSize;
        private int batchCount = 0;

        RocksDBBatch(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void close() {
            writeBatch.close();
            batchCount = 0;
        }

        @Override
        public void put(byte[] key, byte[] value) throws IOException {
            try {
                writeBatch.put(key, value);
                countBatchAndFlushIfNeeded();
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        @Override
        public void remove(byte[] key) throws IOException {
            try {
                writeBatch.delete(key);
                countBatchAndFlushIfNeeded();
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        @Override
        public void clear() {
            writeBatch.clear();
            batchCount = 0;
        }

        @Override
        public void deleteRange(byte[] beginKey, byte[] endKey) throws IOException {
            try {
                writeBatch.deleteRange(beginKey, endKey);
                countBatchAndFlushIfNeeded();
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            }
        }

        private void countBatchAndFlushIfNeeded() throws IOException {
            if (++batchCount >= batchSize) {
                flush();
                clear();
            }
        }

        @Override
        public int batchCount() {
            return batchCount;
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

    RocksDB db() {
        return db;
    }

    List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() {
        return columnFamilyDescriptors;
    }

    RocksObject getOptions() {
        return options;
    }

    private static final Logger log = LoggerFactory.getLogger(KeyValueStorageRocksDB.class);
}
