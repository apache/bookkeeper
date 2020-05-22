/*
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
 */

package org.apache.bookkeeper.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.junit.After;
import org.junit.Test;

/**
 * Unit test for {@link LedgerEntriesImpl}.
 */
public class LedgerEntriesImplTest {
    private final int entryNumber = 7;
    private LedgerEntriesImpl ledgerEntriesImpl;
    private final List<LedgerEntry> entryList = Lists.newArrayList();

    // content for each entry
    private final long ledgerId = 1234L;
    private final long entryId = 5678L;
    private final long length = 9876L;
    private final byte[] dataBytes = "test-ledger-entry-impl".getBytes(UTF_8);
    private final ArrayList<ByteBuf> bufs = Lists.newArrayListWithExpectedSize(entryNumber);

    public LedgerEntriesImplTest () {
        for (int i = 0; i < entryNumber; i++) {
            ByteBuf buf = Unpooled.wrappedBuffer(dataBytes);
            bufs.add(buf);

            entryList.add(LedgerEntryImpl.create(ledgerId + i,
                entryId + i,
                length + i,
                buf));
        }

        ledgerEntriesImpl = LedgerEntriesImpl.create(entryList);
    }

    @After
    public void tearDown() {
        ledgerEntriesImpl.close();

        // References should be released after close.
        bufs.forEach(byteBuf -> assertEquals(0, byteBuf.refCnt()));

        try {
            ledgerEntriesImpl.getEntry(entryId);
            fail("should fail getEntry after close");
        } catch (NullPointerException e) {
            // expected behavior
        }

        try {
            ledgerEntriesImpl.iterator();
            fail("should fail iterator after close");
        } catch (NullPointerException e) {
            // expected behavior
        }
    }

    @Test
    public void testGetEntry() {
        for (int i = 0; i < entryNumber; i++) {
            LedgerEntry entry = ledgerEntriesImpl.getEntry(entryId + i);
            assertEquals(entryList.get(i).getLedgerId(),  entry.getLedgerId());
            assertEquals(entryList.get(i).getEntryId(),  entry.getEntryId());
            assertEquals(entryList.get(i).getLength(),  entry.getLength());

            ByteBuf buf = entry.getEntryBuffer();
            byte[]  content = new byte[buf.readableBytes()];
            buf.readBytes(content);
            assertArrayEquals(dataBytes, content);

            assertEquals(1, entry.getEntryBuffer().refCnt());
        }

        try {
            LedgerEntry entry = ledgerEntriesImpl.getEntry(entryId - 1);
            fail("Should get IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected behavior
        }

        try {
            LedgerEntry entry = ledgerEntriesImpl.getEntry(entryId + entryNumber);
            fail("Should get IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // expected behavior
        }
    }

    @Test
    public void testIterator() {
        Iterator<LedgerEntry> entryIterator = ledgerEntriesImpl.iterator();
        entryIterator.forEachRemaining(ledgerEntry -> assertEquals(1, ledgerEntry.getEntryBuffer().refCnt()));
    }
}
