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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * In-memory metadata-store to store entry-log metadata-map in memory-map.
 */
public class InMemoryEntryLogMetadataMap implements EntryLogMetadataMap {

    private final Map<Long, EntryLogMetadata> entryLogMetaMap = new ConcurrentHashMap<>();

    @Override
    public boolean containsKey(long entryLogId) {
        return entryLogMetaMap.containsKey(entryLogId);
    }

    @Override
    public void put(long entryLogId, EntryLogMetadata entryLogMeta) {
        entryLogMetaMap.put(entryLogId, entryLogMeta);
    }

    @Override
    public void forEach(BiConsumer<Long, EntryLogMetadata> action) {
        entryLogMetaMap.forEach(action);
    }

    @Override
    public void forKey(long entryLogId, BiConsumer<Long, EntryLogMetadata> action)
            throws BookieException.EntryLogMetadataMapException {
        action.accept(entryLogId, entryLogMetaMap.get(entryLogId));
    }

    @Override
    public void remove(long entryLogId) {
        entryLogMetaMap.remove(entryLogId);
    }

    @Override
    public int size() {
        return entryLogMetaMap.size();
    }

    @Override
    public boolean isEmpty() {
        return entryLogMetaMap.isEmpty();
    }

    @Override
    public void clear() {
        entryLogMetaMap.clear();
    }

    @Override
    public void close() throws IOException {
        entryLogMetaMap.clear();
    }

}
