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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.OptionsUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
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

    private final WriteOptions optionSync;
    private final WriteOptions optionDontSync;

    private final ReadOptions optionCache;
    private final ReadOptions optionDontCache;
    private final WriteBatch emptyBatch;

    private static final String ROCKSDB_LOG_PATH = "dbStorage_rocksDB_logPath";

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
        DBOptions dbOptions = new DBOptions();
        final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
        final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        try {
            if (dbConfigType == DbConfigType.EntryLocation) {
                dbFilePath = conf.getEntryLocationRocksdbConf();
            } else if (dbConfigType == DbConfigType.LedgerMetadata) {
                dbFilePath = conf.getLedgerMetadataRocksdbConf();
            } else {
                dbFilePath = conf.getDefaultRocksDBConf();
            }

            OptionsUtil.loadOptionsFromFile(dbFilePath, Env.getDefault(), dbOptions, cfDescs, false);
            // Configure file path
            String logPath = conf.getString(ROCKSDB_LOG_PATH, "");
            if (!logPath.isEmpty()) {
                Path logPathSetting = FileSystems.getDefault().getPath(logPath, subPath);
                Files.createDirectories(logPathSetting);
                log.info("RocksDB<{}> log path: {}", subPath, logPathSetting);
                dbOptions.setDbLogDir(logPathSetting.toString());
            }
            String path = FileSystems.getDefault().getPath(basePath, subPath).toFile().toString();

            if (readOnly) {
                db = RocksDB.openReadOnly(dbOptions, path, cfDescs, cfHandles);
            } else {
                db = RocksDB.open(dbOptions, path, cfDescs, cfHandles);
            }
        } catch (RocksDBException e) {
            throw new IOException("Error open RocksDB database", e);
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
    public void compact(byte[] firstKey, byte[] lastKey) throws IOException {
        try {
            db.compactRange(firstKey, lastKey);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB compact", e);
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

    private static final Logger log = LoggerFactory.getLogger(KeyValueStorageRocksDB.class);
}
