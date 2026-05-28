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
package org.apache.bookkeeper.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;

public class MockBookiesTest {

    private static final BookieId BOOKIE_ID = BookieId.parse("127.0.0.1:3181");
    private static final long LEDGER_ID = 1L;
    private static final long BATCH_RESPONSE_HEADER_SIZE = 24 + 8 + 4;

    @Test
    public void testBatchReadStopsOnMissingSubsequentEntry() throws Exception {
        MockBookies mockBookies = new MockBookies();
        mockBookies.addEntry(BOOKIE_ID, LEDGER_ID, 0L, newEntry(8));

        ByteBufList data = mockBookies.batchReadEntries(BOOKIE_ID, 0, LEDGER_ID, 0L, 2, Long.MAX_VALUE);

        assertNotNull(data);
        assertEquals(1, data.size());
        assertEquals(8, data.getBuffer(0).readableBytes());
    }

    @Test
    public void testBatchReadDoesNotReleaseOversizedSkippedEntry() throws Exception {
        MockBookies mockBookies = new MockBookies();
        mockBookies.addEntry(BOOKIE_ID, LEDGER_ID, 0L, newEntry(8));
        mockBookies.addEntry(BOOKIE_ID, LEDGER_ID, 1L, newEntry(16));

        long maxSize = BATCH_RESPONSE_HEADER_SIZE + 8 + Integer.BYTES + 16 + Integer.BYTES - 1;
        ByteBufList data = mockBookies.batchReadEntries(BOOKIE_ID, 0, LEDGER_ID, 0L, 2, maxSize);

        assertNotNull(data);
        assertEquals(1, data.size());
        assertEquals(8, data.getBuffer(0).readableBytes());
        assertEquals(16, mockBookies.readEntry(BOOKIE_ID, 0, LEDGER_ID, 1L).readableBytes());
    }

    private static ByteBuf newEntry(int size) {
        return Unpooled.buffer(size).writeZero(size);
    }
}
