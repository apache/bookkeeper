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

import static org.apache.bookkeeper.util.BookKeeperConstants.METADATA_CACHE;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException.EntryLogMetadataMapException;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.EntryLogMetadata.EntryLogMetadataRecyclable;
import org.apache.bookkeeper.bookie.EntryLogMetadataMap;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * Persistent entryLogMetadata-map that stores entry-loggers metadata into
 * rocksDB.
 */
@Slf4j
public class PersistentEntryLogMetadataMap implements EntryLogMetadataMap {
    // persistent Rocksdb to store metadata-map
    private final KeyValueStorage metadataMapDB;
    private AtomicBoolean isClosed = new AtomicBoolean(false);

    private static final FastThreadLocal<ByteArrayOutputStream> baos = new FastThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
            return new ByteArrayOutputStream();
        }
    };
    private static final FastThreadLocal<ByteArrayInputStream> bais = new FastThreadLocal<ByteArrayInputStream>() {
        @Override
        protected ByteArrayInputStream initialValue() {
            return new ByteArrayInputStream(new byte[1]);
        }
    };
    private static final FastThreadLocal<DataOutputStream> dataos = new FastThreadLocal<DataOutputStream>() {
        @Override
        protected DataOutputStream initialValue() {
            return new DataOutputStream(baos.get());
        }
    };
    private static final FastThreadLocal<DataInputStream> datais = new FastThreadLocal<DataInputStream>() {
        @Override
        protected DataInputStream initialValue() {
            return new DataInputStream(bais.get());
        }
    };

    public PersistentEntryLogMetadataMap(String metadataPath, ServerConfiguration conf) throws IOException {
        log.info("Loading persistent entrylog metadata-map from {}/{}", metadataPath, METADATA_CACHE);
        File dir = new File(metadataPath);
        if (!dir.mkdirs() && !dir.exists()) {
            String err = "Unable to create directory " + dir;
            log.error(err);
            throw new IOException(err);
        }
        metadataMapDB = KeyValueStorageRocksDB.factory.newKeyValueStorage(metadataPath, METADATA_CACHE,
                DbConfigType.Small, conf);
    }

    @Override
    public boolean containsKey(long entryLogId) throws EntryLogMetadataMapException {
        throwIfClosed();
        LongWrapper key = LongWrapper.get(entryLogId);
        try {
            boolean isExist;
            try {
                isExist = metadataMapDB.get(key.array) != null;
            } catch (IOException e) {
                throw new EntryLogMetadataMapException(e);
            }
            return isExist;
        } finally {
            key.recycle();
        }
    }

    @Override
    public void put(long entryLogId, EntryLogMetadata entryLogMeta) throws EntryLogMetadataMapException {
        throwIfClosed();
        LongWrapper key = LongWrapper.get(entryLogId);
        try {
            baos.get().reset();
            try {
                entryLogMeta.serialize(dataos.get());
                metadataMapDB.put(key.array, baos.get().toByteArray());
            } catch (IllegalStateException | IOException e) {
                log.error("Failed to serialize entrylog-metadata, entryLogId {}", entryLogId);
                throw new EntryLogMetadataMapException(e);
            }
        } finally {
            key.recycle();
        }

    }

    /**
     * {@link EntryLogMetadata} life-cycle in supplied action will be transient
     * and it will be recycled as soon as supplied action is completed.
     */
    @Override
    public void forEach(BiConsumer<Long, EntryLogMetadata> action) throws EntryLogMetadataMapException {
        throwIfClosed();
        CloseableIterator<Entry<byte[], byte[]>> iterator = metadataMapDB.iterator();
        try {
            while (iterator.hasNext()) {
                if (isClosed.get()) {
                    break;
                }
                Entry<byte[], byte[]> entry = iterator.next();
                long entryLogId = ArrayUtil.getLong(entry.getKey(), 0);
                ByteArrayInputStream localBais = bais.get();
                DataInputStream localDatais = datais.get();
                if (localBais.available() < entry.getValue().length) {
                    localBais.close();
                    localDatais.close();
                    ByteArrayInputStream newBais = new ByteArrayInputStream(entry.getValue());
                    bais.set(newBais);
                    datais.set(new DataInputStream(newBais));
                } else {
                    localBais.read(entry.getValue(), 0, entry.getValue().length);
                }
                localBais.reset();
                localDatais.reset();
                EntryLogMetadataRecyclable metadata = EntryLogMetadata.deserialize(datais.get());
                try {
                    action.accept(entryLogId, metadata);
                } finally {
                    metadata.recycle();
                }
            }
        } catch (IOException e) {
            log.error("Failed to iterate over entry-log metadata map {}", e.getMessage(), e);
            throw new EntryLogMetadataMapException(e);
        } finally {
            try {
                iterator.close();
            } catch (IOException e) {
                log.error("Failed to close entry-log metadata-map rocksDB iterator {}", e.getMessage(), e);
            }
        }
    }

    @Override
    public void remove(long entryLogId) throws EntryLogMetadataMapException {
        throwIfClosed();
        LongWrapper key = LongWrapper.get(entryLogId);
        try {
            try {
                metadataMapDB.delete(key.array);
            } catch (IOException e) {
                throw new EntryLogMetadataMapException(e);
            }
        } finally {
            key.recycle();
        }
    }

    @Override
    public int size() throws EntryLogMetadataMapException {
        throwIfClosed();
        try {
            return (int) metadataMapDB.count();
        } catch (IOException e) {
            throw new EntryLogMetadataMapException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            metadataMapDB.close();
        } else {
            log.warn("Attempted to close already closed PersistentEntryLogMetadataMap");
        }
    }

    public void throwIfClosed() throws EntryLogMetadataMapException {
        if (isClosed.get()) {
            final String msg = "Attempted to use PersistentEntryLogMetadataMap after it was closed";
            log.error(msg);
            throw new EntryLogMetadataMapException(new IOException(msg));
        }
    }
}
