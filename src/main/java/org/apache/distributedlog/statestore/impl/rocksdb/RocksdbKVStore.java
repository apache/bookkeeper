/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.statestore.impl.rocksdb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.BLOCK_CACHE_SIZE;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.BLOCK_SIZE;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.DEFAULT_CHECKSUM_TYPE;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.DEFAULT_COMPACTION_STYLE;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.DEFAULT_COMPRESSION_TYPE;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.DEFAULT_LOG_LEVEL;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.DEFAULT_PARALLELISM;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.MAX_WRITE_BUFFERS;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksConstants.WRITE_BUFFER_SIZE;

import com.google.common.collect.Sets;
import com.google.common.primitives.SignedBytes;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Set;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.common.coder.Coder;
import org.apache.distributedlog.statestore.api.KV;
import org.apache.distributedlog.statestore.api.KVIterator;
import org.apache.distributedlog.statestore.api.KVMulti;
import org.apache.distributedlog.statestore.api.KVStore;
import org.apache.distributedlog.statestore.api.StateStoreSpec;
import org.apache.distributedlog.statestore.exceptions.InvalidStateStoreException;
import org.apache.distributedlog.statestore.exceptions.StateStoreException;
import org.apache.distributedlog.statestore.exceptions.StateStoreRuntimeException;
import org.apache.distributedlog.statestore.impl.KVImpl;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * A key/value store implemented using {@link http://rocksdb.org/}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Slf4j
public class RocksdbKVStore<K, V> implements KVStore<K, V> {

    // parameters for the store
    protected String name;
    protected Coder<K> keyCoder;
    protected Coder<V> valCoder;

    // rocksdb state
    protected File dbDir;
    protected RocksDB db;

    // iterators
    protected final Set<KVIterator<K, V>> kvIters;

    // options used by rocksdb
    protected Options opts;
    protected WriteOptions writeOpts;
    protected FlushOptions flushOpts;

    // states of the store
    protected volatile boolean isInitialized = false;
    protected volatile boolean closed = false;

    public RocksdbKVStore() {
        // initialize the iterators set
        this.kvIters = Collections.synchronizedSet(Sets.newHashSet());
    }

    protected void checkStoreOpen() {
        if (closed) {
            throw new InvalidStateStoreException("State store " + name + " is already closed");
        }
        if (!isInitialized) {
            throw new InvalidStateStoreException("State Store " + name + " is not initialized yet");
        }
    }

    synchronized RocksDB getDb() {
        return db;
    }

    @Override
    public synchronized String name() {
        return this.name;
    }

    @Override
    public synchronized void init(StateStoreSpec spec) throws StateStoreException {
        checkArgument(spec.localStateStoreDir().isPresent(),
            "local state store directory is not configured");

        this.name = spec.name();

        // initialize the coders
        this.keyCoder = (Coder<K>) spec.keyCoder();
        this.valCoder = (Coder<V>) spec.valCoder();

        // open the rocksdb
        openRocksdb(spec);

        this.isInitialized = true;
    }

    protected void openRocksdb(StateStoreSpec spec) throws StateStoreException {

        // initialize the db options

        final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
        tableConfig.setBlockSize(BLOCK_SIZE);
        tableConfig.setChecksumType(DEFAULT_CHECKSUM_TYPE);

        opts = new Options();
        opts.setTableFormatConfig(tableConfig);
        opts.setWriteBufferSize(WRITE_BUFFER_SIZE);
        opts.setCompressionType(DEFAULT_COMPRESSION_TYPE);
        opts.setCompactionStyle(DEFAULT_COMPACTION_STYLE);
        opts.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        opts.setCreateIfMissing(true);
        opts.setErrorIfExists(false);
        opts.setInfoLogLevel(DEFAULT_LOG_LEVEL);
        opts.setIncreaseParallelism(DEFAULT_PARALLELISM);

        // initialize the write options

        writeOpts = new WriteOptions();
        writeOpts.setDisableWAL(false); // disable wal, since the source of truth will be on distributedlog

        // initialize the flush options

        flushOpts = new FlushOptions();
        flushOpts.setWaitForFlush(true);

        // open the rocksdb

        this.dbDir = spec.localStateStoreDir().get();
        this.db = openLocalDB(dbDir, opts);
    }

    protected RocksDB openLocalDB(File dir, Options options) throws StateStoreException {
        return openRocksdb(dir, options);
    }

    protected static RocksDB openRocksdb(File dir, Options options) throws StateStoreException {
        // make sure the db directory's parent dir is created
        try {
            Files.createDirectories(dir.getParentFile().toPath());
            return RocksDB.open(options, dir.getAbsolutePath());
        } catch (IOException ioe) {
            log.error("Failed to create parent directory {} for opening rocksdb", dir.getParentFile().toPath(), ioe);
            throw new StateStoreException(ioe);
        } catch (RocksDBException dbe) {
            log.error("Failed to open rocksdb at dir {}", dir.getAbsolutePath(), dbe);
            throw new StateStoreException(dbe);
        }
    }

    @Override
    public synchronized void flush() throws StateStoreException {
        if (null == db) {
            return;
        }
        try {
            db.flush(flushOpts);
        } catch (RocksDBException e) {
            throw new StateStoreException("Exception on flushing rocksdb from store " + name, e);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;

        // close iterators
        closeIters();

        // close db
        closeLocalDB();

        // release options
        RocksUtils.close(opts);
        RocksUtils.close(writeOpts);
        RocksUtils.close(flushOpts);
    }

    protected void closeLocalDB() {
        RocksUtils.close(db);
    }

    private void closeIters() {
        Set<KVIterator> iterators;
        synchronized (kvIters) {
            iterators = Sets.newHashSet(kvIters);
        }
        iterators.forEach(KVIterator::close);
    }

    @Override
    public synchronized V get(K key) {
        checkNotNull(key, "key cannot be null");
        checkStoreOpen();

        byte[] keyBytes = keyCoder.encode(key);
        return getRaw(key, keyBytes);
    }

    private V getRaw(K key, byte[] keyBytes) {
        byte[] valBytes = getRawBytes(key, keyBytes);
        if (null == valBytes) {
            return null;
        }
        return valCoder.decode(valBytes);
    }

    protected byte[] getRawBytes(K key, byte[] keyBytes) {
        try {
            return this.db.get(keyBytes);
        } catch (RocksDBException e) {
            throw new StateStoreRuntimeException("Error while getting value for key " + key + " from store " + name, e);
        }
    }

    @Override
    public synchronized KVIterator<K, V> range(K from, K to) {
        checkStoreOpen();

        RocksIterator rocksIter = db.newIterator();
        if (null == from) {
            rocksIter.seekToFirst();
        } else {
            byte[] fromBytes = keyCoder.encode(from);
            rocksIter.seek(fromBytes);
        }
        KVIterator<K, V> kvIter;
        if (null == to) {
            kvIter = new RocksdbKVIterator(name, rocksIter, keyCoder, valCoder);
        } else {
            kvIter = new RocksdbRangeIterator(name, rocksIter, keyCoder, valCoder, to);
        }
        kvIters.add(kvIter);
        return kvIter;
    }

    @Override
    public synchronized void put(K key, V value) {
        checkNotNull(key, "key cannot be null");
        checkStoreOpen();

        byte[] keyBytes = keyCoder.encode(key);
        putRaw(key, keyBytes, value);
    }

    private void putRaw(K key, byte[] keyBytes, V value) {
        try {
            if (null == value) {
                // delete a key if value is null
                db.delete(keyBytes);
            } else {
                byte[] valBytes = valCoder.encode(value);
                db.put(writeOpts, keyBytes, valBytes);
            }
        } catch (RocksDBException e) {
            throw new StateStoreRuntimeException("Error while updating key " + key
                + " to value " + value + " from store " + name, e);
        }
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        checkNotNull(key, "key cannot be null");
        checkStoreOpen();

        byte[] keyBytes = keyCoder.encode(key);
        V oldVal = getRaw(key, keyBytes);
        if (null != oldVal) {
            return oldVal;
        }

        if (value == null) {
            return null;
        }

        putRaw(key, keyBytes, value);
        return null;
    }

    @Override
    public synchronized KVMulti<K, V> multi() {
        checkStoreOpen();

        return new KVMultiImpl();
    }

    @Override
    public synchronized V delete(K key) {
        checkNotNull(key, "key cannot be null");
        checkStoreOpen();

        byte[] keyBytes = keyCoder.encode(key);
        V val = getRaw(key, keyBytes);
        putRaw(key, keyBytes, null);

        return val;
    }

    //
    // Multi
    //

    /**
     * A rocksdb based multi operation.
     */
    class KVMultiImpl implements KVMulti<K, V> {

        private final WriteBatch batch = new WriteBatch();
        private volatile boolean executed = false;

        private void checkExecuted() {
            if (executed) {
                throw new StateStoreRuntimeException("KVMulti#execute() has been called");
            }
        }

        @Override
        public void put(K key, V value) {
            checkNotNull(key, "key cannot be null");
            checkExecuted();

            byte[] keyBytes = keyCoder.encode(key);
            if (null == value) {
                deleteRaw(keyBytes);
            } else {
                putRaw(keyBytes, value);
            }
        }

        private void putRaw(byte[] keyBytes, V value) {
            batch.put(keyBytes, valCoder.encode(value));
        }

        @Override
        public void delete(K key) {
            checkNotNull(key, "key cannot be null");
            checkExecuted();

            byte[] keyBytes = keyCoder.encode(key);
            deleteRaw(keyBytes);
        }

        private void deleteRaw(byte[] keyBytes) {
            batch.remove(keyBytes);
        }

        @Override
        public void deleteRange(K from, K to) {
            checkNotNull(from, "from key cannot be null");
            checkNotNull(to, "to key cannot be null");
            checkExecuted();

            byte[] fromBytes = keyCoder.encode(from);
            byte[] toBytes = keyCoder.encode(to);
            batch.deleteRange(fromBytes, toBytes);
        }

        @Override
        public synchronized void execute() {
            if (executed) {
                return;
            }

            checkStoreOpen();
            executed = true;

            try {
                getDb().write(writeOpts, batch);
            } catch (RocksDBException e) {
                throw new StateStoreRuntimeException("Error while executing a multi operation from store " + name, e);
            } finally {
                RocksUtils.close(batch);
            }
        }
    }

    //
    // Iterators
    //

    /**
     * KV iterator over a rocksdb instance.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    class RocksdbKVIterator implements KVIterator<K, V> {

        final String name;
        final RocksIterator iterator;
        final Coder<K> keyCoder;
        final Coder<V> valCoder;

        private volatile boolean closed = false;

        /**
         * Ensure an iterator is open.
         *
         * @throws InvalidStateStoreException when the store is in closed state.
         */
        private void ensureIteratorOpen() {
            if (closed) {
                throw new InvalidStateStoreException("Rocksdb state store " + name + " is already closed");
            }
        }

        @Override
        public void close() {
            kvIters.remove(this);
            iterator.close();
            closed = true;
        }

        @Override
        public boolean hasNext() {
            ensureIteratorOpen();
            return iterator.isValid();
        }

        private KV<K, V> getKvPair() {
            return new KVImpl<>(keyCoder.decode(iterator.key()), valCoder.decode(iterator.value()));
        }

        @Override
        public KV<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            KV<K, V> kv = getKvPair();
            iterator.next();
            return kv;
        }
    }

    /**
     * KV iterator over a rocksdb, which ends at the provided <i>endKey</i>.
     */
    class RocksdbRangeIterator extends RocksdbKVIterator {

        private final Comparator<byte[]> comparator = SignedBytes.lexicographicalComparator();
        private final byte[] endKeyBytes;

        private RocksdbRangeIterator(String name,
                                     RocksIterator iterator,
                                     Coder<K> keyCoder,
                                     Coder<V> valCoder,
                                     K endKey) {
            super(name, iterator, keyCoder, valCoder);
            checkNotNull(endKey, "End key cannot be null");
            this.endKeyBytes = keyCoder.encode(endKey);
        }



        @Override
        public boolean hasNext() {
            return super.hasNext()
                && comparator.compare(iterator.key(), endKeyBytes) <= 0;
        }
    }


}
