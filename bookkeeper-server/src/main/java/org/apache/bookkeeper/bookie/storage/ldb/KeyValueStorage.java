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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

/**
 * Abstraction of a generic key-value local database.
 */
public interface KeyValueStorage extends Closeable {

    void put(byte[] key, byte[] value) throws IOException;

    /**
     * Get the value associated with the given key.
     *
     * @param key
     *            the key to lookup
     * @return the value or null if the key was not found
     */
    byte[] get(byte[] key) throws IOException;

    /**
     * Get the value associated with the given key.
     *
     * <p>This method will use the provided array store the value
     *
     * @param key
     *            the key to lookup
     * @param value
     *            an array where to store the result
     * @return -1 if the entry was not found or the length of the value
     * @throws IOException
     *             if the value array could not hold the result
     */
    int get(byte[] key, byte[] value) throws IOException;

    /**
     * Get the entry whose key is the biggest and it's lesser than the supplied key.
     *
     * <p>For example if the db contains :
     *
     * <pre>
     * {
     *      1 : 'a',
     *      2 : 'b',
     *      3 : 'c'
     * }
     * </pre>
     *
     * <p>Then:
     *
     * <pre>
     * getFloor(3) --> (2, 'b')
     * </pre>
     *
     * @param key
     *            the non-inclusive upper limit key
     * @return the entry before or null if there's no entry before key
     */
    Entry<byte[], byte[]> getFloor(byte[] key) throws IOException;

    /**
     * Get the entry whose key is bigger or equal the supplied key.
     *
     * @param key
     * @return
     * @throws IOException
     */
    Entry<byte[], byte[]> getCeil(byte[] key) throws IOException;

    /**
     *
     * @param key
     * @throws IOException
     */
    void delete(byte[] key) throws IOException;

    /**
     * Get an iterator over to scan sequentially through all the keys in the
     * database.
     *
     * @return
     */
    CloseableIterator<byte[]> keys();

    /**
     * Get an iterator over to scan sequentially through all the keys within a
     * specified range.
     *
     * @param firstKey
     *            the first key in the range (included)
     * @param lastKey
     *            the lastKey in the range (not included)
     *
     */
    CloseableIterator<byte[]> keys(byte[] firstKey, byte[] lastKey);

    /**
     * Return an iterator object that can be used to sequentially scan through all
     * the entries in the database.
     */
    CloseableIterator<Entry<byte[], byte[]>> iterator();

    /**
     * Commit all pending write to durable storage.
     */
    void sync() throws IOException;

    /**
     * @return the number of keys.
     */
    long count() throws IOException;

    /**
     * Iterator interface.
     *
     * @param <T>
     */
    interface CloseableIterator<T> extends Closeable {
        boolean hasNext() throws IOException;

        T next() throws IOException;
    }

    Batch newBatch();

    /**
     * Interface for a batch to be written in the storage.
     */
    public interface Batch extends Closeable {
        void put(byte[] key, byte[] value) throws IOException;

        void remove(byte[] key) throws IOException;

        void deleteRange(byte[] beginKey, byte[] endKey) throws IOException;

        void clear();

        void flush() throws IOException;
    }
}
