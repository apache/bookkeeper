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

package org.apache.bookkeeper.statelib.impl.kv;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.BLOCK_CACHE_SIZE;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.BLOCK_SIZE;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.DEFAULT_CHECKSUM_TYPE;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.DEFAULT_COMPACTION_STYLE;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.DEFAULT_COMPRESSION_TYPE;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.DEFAULT_LOG_LEVEL;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.DEFAULT_PARALLELISM;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.MAX_WRITE_BUFFERS;
import static org.apache.bookkeeper.statelib.impl.rocksdb.RocksConstants.WRITE_BUFFER_SIZE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.SignedBytes;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.common.kv.KV;
import org.apache.bookkeeper.common.kv.KVImpl;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.InvalidStateStoreException;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreRuntimeException;
import org.apache.bookkeeper.statelib.api.kv.KVIterator;
import org.apache.bookkeeper.statelib.api.kv.KVMulti;
import org.apache.bookkeeper.statelib.api.kv.KVStore;
import org.apache.bookkeeper.statelib.impl.Bytes;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.RocksCheckpointer;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
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

    private static final byte[] METADATA_CF = ".meta".getBytes(UTF_8);
    private static final byte[] DATA_CF = "default".getBytes(UTF_8);
    private static final byte[] LAST_REVISION = ".lrev".getBytes(UTF_8);

    private static final AtomicLongFieldUpdater<RocksdbKVStore> lastRevisionUpdater =
        AtomicLongFieldUpdater.newUpdater(RocksdbKVStore.class, "lastRevision");

    // parameters for the store
    protected String name;
    protected Coder<K> keyCoder;
    protected Coder<V> valCoder;

    // rocksdb state
    protected File dbDir;
    protected RocksDB db;
    protected ColumnFamilyHandle metaCfHandle;
    protected ColumnFamilyHandle dataCfHandle;

    // iterators
    protected final Set<KVIterator<K, V>> kvIters;

    // options used by rocksdb
    protected DBOptions dbOpts;
    protected ColumnFamilyOptions cfOpts;
    protected WriteOptions writeOpts;
    protected FlushOptions flushOpts;

    // states of the store
    protected volatile boolean isInitialized = false;
    protected volatile boolean closed = false;
    protected volatile long lastRevision = -1L;
    private final byte[] lastRevisionBytes = new byte[Long.BYTES];

    // checkpointer store
    private CheckpointStore checkpointStore;
    private ScheduledExecutorService checkpointScheduler;
    // rocksdb checkpointer
    private RocksCheckpointer checkpointer;

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

    @VisibleForTesting
    public synchronized RocksDB getDb() {
        return db;
    }

    @Override
    public synchronized String name() {
        return this.name;
    }

    private void loadRocksdbFromCheckpointStore(StateStoreSpec spec) throws StateStoreException {
        checkNotNull(spec.getCheckpointIOScheduler(),
            "checkpoint io scheduler is not configured");
        checkNotNull(spec.getCheckpointDuration(),
            "checkpoint duration is not configured");

        String dbName = spec.getName();
        File localStorePath = spec.getLocalStateStoreDir();

        RocksCheckpointer.restore(dbName, localStorePath, spec.getCheckpointStore());
    }

    @Override
    public synchronized void checkpoint() {
        log.info("Checkpoint local state store {} at revision {}", name, getLastRevision());
        byte[] checkpointAtRevisionBytes = new byte[Long.BYTES];
        System.arraycopy(lastRevisionBytes, 0, checkpointAtRevisionBytes, 0, checkpointAtRevisionBytes.length);
        checkpointScheduler.submit(() -> {
            try {
                // TODO: move create checkpoint to the checkpoint method
                checkpointer.checkpointAtTxid(checkpointAtRevisionBytes);
            } catch (StateStoreException e) {
                log.error("Failed to checkpoint state store {} at revision {}",
                    name, Bytes.toLong(checkpointAtRevisionBytes, 0), e);
            }
        });
    }

    private void readLastRevision() throws StateStoreException {
        byte[] revisionBytes;
        try {
            revisionBytes = db.get(metaCfHandle, LAST_REVISION);
        } catch (RocksDBException e) {
            throw new StateStoreException("Failed to read last revision from state store " + name(), e);
        }
        if (null == revisionBytes) {
            return;
        }
        long revision = Bytes.toLong(revisionBytes, 0);
        lastRevisionUpdater.set(this, revision);
    }

    @Override
    public long getLastRevision() {
        return lastRevisionUpdater.get(this);
    }

    private void setLastRevision(long lastRevision) {
        lastRevisionUpdater.set(this, lastRevision);
        Bytes.toBytes(lastRevision, lastRevisionBytes, 0);
    }

    private void updateLastRevision(long revision) {
        if (revision >= 0) { // k/v comes from log stream
            if (getLastRevision() >= revision) { // these k/v pairs are duplicates
                return;
            }
            // update revision
            setLastRevision(revision);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void init(StateStoreSpec spec) throws StateStoreException {
        checkNotNull(spec.getLocalStateStoreDir(),
            "local state store directory is not configured");

        checkpointStore = spec.getCheckpointStore();
        if (null != checkpointStore) {
            // load checkpoint from checkpoint store
            loadRocksdbFromCheckpointStore(spec);
        }

        this.name = spec.getName();

        // initialize the coders
        this.keyCoder = (Coder<K>) spec.getKeyCoder();
        this.valCoder = (Coder<V>) spec.getValCoder();

        // open the rocksdb
        openRocksdb(spec);

        // once the rocksdb is opened, read the last revision
        readLastRevision();

        if (null != checkpointStore) {
            checkpointer = new RocksCheckpointer(
                name(),
                dbDir,
                db,
                checkpointStore,
                true,
                true);
            checkpointScheduler = spec.getCheckpointIOScheduler();
        }

        this.isInitialized = true;
    }

    protected void openRocksdb(StateStoreSpec spec) throws StateStoreException {

        // initialize the db options

        final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
        tableConfig.setBlockSize(BLOCK_SIZE);
        tableConfig.setChecksumType(DEFAULT_CHECKSUM_TYPE);

        dbOpts = new DBOptions();
        dbOpts.setCreateIfMissing(true);
        dbOpts.setErrorIfExists(false);
        dbOpts.setInfoLogLevel(DEFAULT_LOG_LEVEL);
        dbOpts.setIncreaseParallelism(DEFAULT_PARALLELISM);
        dbOpts.setCreateMissingColumnFamilies(true);

        cfOpts = new ColumnFamilyOptions();
        cfOpts.setTableFormatConfig(tableConfig);
        cfOpts.setWriteBufferSize(WRITE_BUFFER_SIZE);
        cfOpts.setCompressionType(DEFAULT_COMPRESSION_TYPE);
        cfOpts.setCompactionStyle(DEFAULT_COMPACTION_STYLE);
        cfOpts.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);

        // initialize the write options

        writeOpts = new WriteOptions();
        writeOpts.setDisableWAL(true); // disable wal, since the source of truth will be on distributedlog

        // initialize the flush options

        flushOpts = new FlushOptions();
        flushOpts.setWaitForFlush(true);

        // open the rocksdb

        this.dbDir = spec.getLocalStateStoreDir();
        Pair<RocksDB, List<ColumnFamilyHandle>> dbPair = openLocalDB(dbDir, dbOpts, cfOpts);
        this.db = dbPair.getLeft();
        this.metaCfHandle = dbPair.getRight().get(0);
        this.dataCfHandle = dbPair.getRight().get(1);
    }

    protected Pair<RocksDB, List<ColumnFamilyHandle>> openLocalDB(File dir,
                                                                  DBOptions options,
                                                                  ColumnFamilyOptions cfOpts)
        throws StateStoreException {
        return openRocksdb(dir, options, cfOpts);
    }

    protected static Pair<RocksDB, List<ColumnFamilyHandle>> openRocksdb(
        File dir, DBOptions options, ColumnFamilyOptions cfOpts)
        throws StateStoreException {
        // make sure the db directory's parent dir is created
        ColumnFamilyDescriptor metaDesc = new ColumnFamilyDescriptor(METADATA_CF, cfOpts);
        ColumnFamilyDescriptor dataDesc = new ColumnFamilyDescriptor(DATA_CF, cfOpts);

        try {
            Files.createDirectories(dir.toPath());
            File dbDir = new File(dir, "current");

            if (!dbDir.exists()) {
                // empty state
                String uuid = UUID.randomUUID().toString();
                Path checkpointPath = Paths.get(dir.getAbsolutePath(), "checkpoints", uuid);
                Files.createDirectories(checkpointPath);
                Files.createSymbolicLink(
                    Paths.get(dbDir.getAbsolutePath()),
                    checkpointPath);
            }

            List<ColumnFamilyHandle> cfHandles = Lists.newArrayListWithExpectedSize(2);
            RocksDB db = RocksDB.open(
                options,
                dbDir.getAbsolutePath(),
                Lists.newArrayList(metaDesc, dataDesc),
                cfHandles);
            return Pair.of(db, cfHandles);
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

        if (null != checkpointer) {
            checkpointer.close();
        }

        // close iterators
        closeIters();

        // close db
        closeLocalDB();

        // release options
        RocksUtils.close(dbOpts);
        RocksUtils.close(writeOpts);
        RocksUtils.close(flushOpts);
        RocksUtils.close(cfOpts);
    }

    protected void closeLocalDB() {
        try {
            flush();
        } catch (StateStoreException e) {
            // flush() already logs this exception.
        }

        RocksUtils.close(metaCfHandle);
        RocksUtils.close(dataCfHandle);
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
            return this.db.get(dataCfHandle, keyBytes);
        } catch (RocksDBException e) {
            throw new StateStoreRuntimeException("Error while getting value for key " + key + " from store " + name, e);
        }
    }

    @Override
    public synchronized KVIterator<K, V> range(K from, K to) {
        checkStoreOpen();

        RocksIterator rocksIter = db.newIterator(dataCfHandle);
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
        put(key, value, -1);
    }

    synchronized void put(K key, V value, long revision) {
        checkNotNull(key, "key cannot be null");
        checkStoreOpen();

        updateLastRevision(revision);

        byte[] keyBytes = keyCoder.encode(key);
        putRaw(key, keyBytes, value, revision);
    }

    private void putRaw(K key, byte[] keyBytes, V value, long revision) {
        try {
            WriteBatch batch = new WriteBatch();
            if (revision > 0) {
                // last revision has been set to revision bytes
                batch.put(metaCfHandle, LAST_REVISION, lastRevisionBytes);
            }
            if (null == value) {
                // delete a key if value is null
                batch.delete(dataCfHandle, keyBytes);
            } else {
                byte[] valBytes = valCoder.encode(value);
                batch.put(dataCfHandle, keyBytes, valBytes);
            }

            db.write(writeOpts, batch);
        } catch (RocksDBException e) {
            throw new StateStoreRuntimeException("Error while updating key " + key
                + " to value " + value + " from store " + name, e);
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, -1L);
    }

    synchronized V putIfAbsent(K key, V value, long revision) {
        checkNotNull(key, "key cannot be null");
        checkStoreOpen();

        updateLastRevision(revision);

        byte[] keyBytes = keyCoder.encode(key);
        V oldVal = getRaw(key, keyBytes);
        if (null != oldVal) {
            return oldVal;
        }

        if (value == null) {
            return null;
        }

        putRaw(key, keyBytes, value, revision);
        return null;
    }

    @Override
    public synchronized KVMulti<K, V> multi() {
        checkStoreOpen();

        return new KVMultiImpl();
    }

    @Override
    public synchronized V delete(K key) {
        return delete(key, -1L);
    }

    synchronized V delete(K key, long revision) {
        checkNotNull(key, "key cannot be null");
        checkStoreOpen();

        updateLastRevision(revision);

        byte[] keyBytes = keyCoder.encode(key);
        V val = getRaw(key, keyBytes);
        putRaw(key, keyBytes, null, revision);

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
            try {
                batch.put(dataCfHandle, keyBytes, valCoder.encode(value));
            } catch (RocksDBException e) {
                throw new StateStoreRuntimeException(e);
            }
        }

        @Override
        public void delete(K key) {
            checkNotNull(key, "key cannot be null");
            checkExecuted();

            byte[] keyBytes = keyCoder.encode(key);
            deleteRaw(keyBytes);
        }

        private void deleteRaw(byte[] keyBytes) {
            try {
                batch.delete(dataCfHandle, keyBytes);
            } catch (RocksDBException e) {
                throw new StateStoreRuntimeException(e);
            }
        }

        @Override
        public void deleteRange(K from, K to) {
            checkNotNull(from, "from key cannot be null");
            checkNotNull(to, "to key cannot be null");
            checkExecuted();

            byte[] fromBytes = keyCoder.encode(from);
            byte[] toBytes = keyCoder.encode(to);
            try {
                batch.deleteRange(dataCfHandle, fromBytes, toBytes);
            } catch (RocksDBException e) {
                throw new StateStoreRuntimeException(e);
            }
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
