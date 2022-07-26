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

package org.apache.bookkeeper.bookie;

import java.io.Closeable;
import java.util.function.BiConsumer;

import org.apache.bookkeeper.bookie.BookieException.EntryLogMetadataMapException;

/**
 * Map-store to store Entrylogger metadata.
 */
public interface EntryLogMetadataMap extends Closeable {

    /**
     * Checks if record with entryLogId exists into the map.
     *
     * @param entryLogId
     * @return
     * @throws EntryLogMetadataMapException
     */
    boolean containsKey(long entryLogId) throws EntryLogMetadataMapException;

    /**
     * Adds entryLogMetadata record into the map.
     *
     * @param entryLogId
     * @param entryLogMeta
     * @throws EntryLogMetadataMapException
     */
    void put(long entryLogId, EntryLogMetadata entryLogMeta) throws EntryLogMetadataMapException;

    /**
     * Performs the given action for each entry in this map until all entries
     * have been processed or the action throws an exception.
     *
     * @param action
     * @throws EntryLogMetadataMapException
     */
    void forEach(BiConsumer<Long, EntryLogMetadata> action) throws EntryLogMetadataMapException;

    /**
     * Performs the given action for the key.
     *
     * @param action
     * @throws EntryLogMetadataMapException
     */
    void forKey(long entryLogId, BiConsumer<Long, EntryLogMetadata> action) throws EntryLogMetadataMapException;

    /**
     * Removes entryLogMetadata record from the map.
     *
     * @param entryLogId
     * @throws EntryLogMetadataMapException
     */
    void remove(long entryLogId) throws EntryLogMetadataMapException;

    /**
     * Returns number of entryLogMetadata records presents into the map.
     *
     * @return
     * @throws EntryLogMetadataMapException
     */
    int size() throws EntryLogMetadataMapException;

    /**
     * Returns true if there are no elements in the map.
     *
     * @return
     */
    default boolean isEmpty() throws EntryLogMetadataMapException {
        return size() == 0;
    }

    /**
     * Clear all records from the map.
     * For unit tests.
     *
     * @throws EntryLogMetadataMapException
     */
    void clear() throws EntryLogMetadataMapException;
}
