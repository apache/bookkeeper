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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;

public class WriteCacheTest {

    @Test
    public void simple() throws Exception {
        WriteCache cache = new WriteCache(10 * 1024);

        ByteBuf entry1 = PooledByteBufAllocator.DEFAULT.buffer(1024);
        ByteBufUtil.writeUtf8(entry1, "entry-1");
        entry1.writerIndex(entry1.capacity());

        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        cache.put(1, 1, entry1);

        assertFalse(cache.isEmpty());
        assertEquals(1, cache.count());
        assertEquals(entry1.readableBytes(), cache.size());

        assertEquals(entry1, cache.get(1, 1));
        assertNull(cache.get(1, 2));
        assertNull(cache.get(2, 1));

        assertEquals(entry1, cache.getLastEntry(1));
        assertEquals(null, cache.getLastEntry(2));

        cache.clear();

        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        entry1.release();
        cache.close();
    }

    @Test
    public void cacheFull() throws Exception {
        int cacheSize = 10 * 1024;
        int entrySize = 1024;
        int entriesCount = cacheSize / entrySize;

        WriteCache cache = new WriteCache(cacheSize);

        ByteBuf entry = PooledByteBufAllocator.DEFAULT.buffer(entrySize);
        entry.writerIndex(entry.capacity());

        for (int i = 0; i < entriesCount; i++) {
            assertTrue(cache.put(1, i, entry));
        }

        assertFalse(cache.put(1, 11, entry));

        assertFalse(cache.isEmpty());
        assertEquals(entriesCount, cache.count());
        assertEquals(cacheSize, cache.size());

        AtomicInteger findCount = new AtomicInteger(0);
        cache.forEach((ledgerId, entryId, data) -> {
            findCount.incrementAndGet();
        });

        assertEquals(entriesCount, findCount.get());

        cache.deleteLedger(1);

        findCount.set(0);
        cache.forEach((ledgerId, entryId, data) -> {
            findCount.incrementAndGet();
        });

        assertEquals(0, findCount.get());

        entry.release();
        cache.close();
    }
}
