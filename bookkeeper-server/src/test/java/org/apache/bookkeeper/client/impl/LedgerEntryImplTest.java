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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.junit.After;
import org.junit.Test;

/**
 * Unit test for {@link LedgerEntryImpl}.
 */
public class LedgerEntryImplTest {

    private final long ledgerId;
    private final long entryId;
    private final long length;
    private final byte[] dataBytes;
    private final ByteBuf dataBuf;
    private final LedgerEntryImpl entryImpl;

    public LedgerEntryImplTest() {
        this.ledgerId = 1234L;
        this.entryId = 3579L;
        this.length = 200L;
        this.dataBytes = "test-ledger-entry-impl".getBytes(UTF_8);
        this.dataBuf = Unpooled.wrappedBuffer(dataBytes);
        this.entryImpl = LedgerEntryImpl.create(
            ledgerId,
            entryId,
            length,
            dataBuf);
    }

    @After
    public void teardown() {
        this.entryImpl.close();
        assertEquals(0, dataBuf.refCnt());
    }

    @Test
    public void testGetters() {
        assertEquals(ledgerId, entryImpl.getLedgerId());
        assertEquals(entryId, entryImpl.getEntryId());
        assertEquals(length, entryImpl.getLength());
        assertArrayEquals(dataBytes, entryImpl.getEntryBytes());
        // getEntry should not modify readerIndex
        assertEquals(0, entryImpl.getEntryBuffer().readerIndex());
        assertEquals(dataBytes.length, entryImpl.getEntryBuffer().readableBytes());
        // getEntryNioBuffer should not modify readerIndex
        ByteBuffer nioBuffer = entryImpl.getEntryNioBuffer();
        assertEquals(dataBytes.length, nioBuffer.remaining());
        byte[] readBytes = new byte[nioBuffer.remaining()];
        nioBuffer.get(readBytes);
        assertArrayEquals(dataBytes, readBytes);
        assertEquals(0, entryImpl.getEntryBuffer().readerIndex());
        assertEquals(dataBytes.length, entryImpl.getEntryBuffer().readableBytes());
    }

    @Test
    public void testSetters() {
        assertEquals(ledgerId, entryImpl.getLedgerId());
        assertEquals(entryId, entryImpl.getEntryId());
        assertEquals(length, entryImpl.getLength());

        entryImpl.setLength(length * 2);
        assertEquals(length * 2, entryImpl.getLength());

        entryImpl.setEntryId(entryId * 2);
        assertEquals(entryId * 2, entryImpl.getEntryId());

        byte[] anotherBytes = "another-ledger-entry-impl".getBytes(UTF_8);
        ByteBuf anotherBuf = Unpooled.wrappedBuffer(anotherBytes);

        entryImpl.setEntryBuf(anotherBuf);
        // set buf should release the original buf
        assertEquals(0, dataBuf.refCnt());
    }

    @Test
    public void testDuplicate() {
        LedgerEntryImpl duplicatedEntry = LedgerEntryImpl.duplicate(entryImpl);

        // the underneath buffer should have 2 entries referencing it
        assertEquals(2, dataBuf.refCnt());

        assertEquals(ledgerId, duplicatedEntry.getLedgerId());
        assertEquals(entryId, duplicatedEntry.getEntryId());
        assertEquals(length, duplicatedEntry.getLength());
        assertArrayEquals(dataBytes, duplicatedEntry.getEntryBytes());

        duplicatedEntry.close();
        assertEquals(1, dataBuf.refCnt());
    }

}
