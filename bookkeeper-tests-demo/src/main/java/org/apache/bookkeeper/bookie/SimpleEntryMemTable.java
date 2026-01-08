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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple in-memory entry table for testing purposes.
 */
public class SimpleEntryMemTable {
    private final Map<String, ByteBuffer> entries = new HashMap<>();
    private final long maxSize;
    private long currentSize = 0;

    public SimpleEntryMemTable(long maxSize) {
        this.maxSize = maxSize;
    }

    public void addEntry(long ledgerId, long entryId, ByteBuffer data) {
        String key = ledgerId + ":" + entryId;
        entries.put(key, data);
        currentSize += data.remaining();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public long getEstimatedSize() {
        return currentSize;
    }

    public void snapshot() {
        entries.clear();
        currentSize = 0;
    }
}
