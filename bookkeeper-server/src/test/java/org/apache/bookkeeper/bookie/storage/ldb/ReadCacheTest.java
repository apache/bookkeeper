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
import static org.junit.Assert.assertNull;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ReadCacheTest {

    @Test
    public void simple() {
        ReadCache cache = new ReadCache(10 * 1024);

        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        ByteBuf entry = Unpooled.wrappedBuffer(new byte[1024]);
        cache.put(1, 0, entry);

        assertEquals(1, cache.count());
        assertEquals(1024, cache.size());

        assertEquals(entry, cache.get(1, 0));
        assertNull(cache.get(1, 1));

        for (int i = 1; i < 10; i++) {
            cache.put(1, i, entry);
        }

        assertEquals(10, cache.count());
        assertEquals(10 * 1024, cache.size());

        cache.put(1, 10, entry);

        // First half of entries will have been evicted
        for (int i = 0; i < 5; i++) {
            assertNull(cache.get(1, i));
        }

        for (int i = 5; i < 11; i++) {
            assertEquals(entry, cache.get(1, i));
        }

        cache.close();
    }
}
