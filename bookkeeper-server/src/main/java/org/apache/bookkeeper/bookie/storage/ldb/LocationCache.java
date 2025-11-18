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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Location cache.
 *
 * <p> This class is used to cache the location of entries in the entry logger.
 *
 * <p> The location of an entry is the position of the entry in the entry logger.
 */
public class LocationCache {
    private static final Logger log = LoggerFactory.getLogger(LocationCache.class);

    private final Map<Long, Map<Long, Long>> cache = new ConcurrentHashMap<>();

    public long getIfExists(long ledgerId, long entryId) {
        Map<Long, Long> innerMap = cache.get(ledgerId);
        if (innerMap != null) {
            Long aLong = innerMap.get(entryId);
            return aLong == null ? 0L : aLong;
        }
        return 0L;
    }

    public void put(long ledgerId, long entryId, long position) {
        cache.computeIfAbsent(ledgerId, k -> new ConcurrentHashMap<>())
                .put(entryId, position);
    }

    public void removeLedger(long ledgerId) {
        Map<Long, Long> remove = cache.remove(ledgerId);
        if (remove != null) {
            remove.clear();
        }
    }
}
