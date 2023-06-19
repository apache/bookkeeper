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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// CHECKSTYLE.OFF: IllegalImport
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.junit.Test;
// CHECKSTYLE.ON: IllegalImport

/**
 * TestBuffer.
 */
public class TestBuffer {

    @Test
    public void testIsAligned() throws Exception {
        assertFalse(Buffer.isAligned(1234));
        assertTrue(Buffer.isAligned(4096));
        assertTrue(Buffer.isAligned(40960));
        assertTrue(Buffer.isAligned(1 << 20));
        assertFalse(Buffer.isAligned(-1));
        assertFalse(Buffer.isAligned(Integer.MAX_VALUE));
        assertFalse(Buffer.isAligned(Integer.MIN_VALUE));
    }

    @Test
    public void testNextAlignment() throws Exception {
        assertEquals(0, Buffer.nextAlignment(0));
        assertEquals(4096, Buffer.nextAlignment(1));
        assertEquals(4096, Buffer.nextAlignment(4096));
        assertEquals(8192, Buffer.nextAlignment(4097));
        assertEquals(0x7FFFF000, Buffer.nextAlignment(0x7FFFF000));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePosition() throws Exception {
        Buffer.nextAlignment(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxAlignment() throws Exception {
        Buffer.nextAlignment(Integer.MAX_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUnaligned() throws Exception {
        new Buffer(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1234);
    }

    @Test
    public void testWriteInt() throws Exception {
        int bufferSize = 1 << 20;
        Buffer b = new Buffer(new NativeIOImpl(), ByteBufAllocator.DEFAULT, bufferSize);
        assertTrue(b.hasSpace(bufferSize));
        assertEquals(0, b.position());
        b.writeInt(0xdeadbeef);


        assertEquals((byte) 0xde, PlatformDependent.getByte(b.pointer() + 0));
        assertEquals((byte) 0xad, PlatformDependent.getByte(b.pointer() + 1));
        assertEquals((byte) 0xbe, PlatformDependent.getByte(b.pointer() + 2));
        assertEquals((byte) 0xef, PlatformDependent.getByte(b.pointer() + 3));

        assertFalse(b.hasSpace(bufferSize));
        assertEquals(Integer.BYTES, b.position());

        for (int i = 0; i < 10000; i++) {
            b.writeInt(i);
        }
        assertEquals(Integer.BYTES * 10001, b.position());
        assertTrue(b.hasSpace(bufferSize - (Integer.BYTES * 10001)));
        assertFalse(b.hasSpace(bufferSize - (Integer.BYTES * 10000)));

        assertEquals(0xdeadbeef, b.readInt(0));
        for (int i = 0; i < 10000; i++) {
            assertEquals(i, b.readInt((i + 1) * Integer.BYTES));
        }
        b.reset();
        assertTrue(b.hasSpace(bufferSize));
        assertEquals(0, b.position());
    }

    @Test
    public void testWriteBuffer() throws Exception {
        ByteBuf bb = Unpooled.buffer(1021);
        fillByteBuf(bb, 0xdeadbeef);
        int bufferSize = 1 << 20;
        Buffer b = new Buffer(new NativeIOImpl(), ByteBufAllocator.DEFAULT, bufferSize);
        assertEquals(0, b.position());
        b.writeByteBuf(bb);
        assertEquals(1021, b.position());
        assertEquals(0, bb.readableBytes());
        bb.clear();
        fillByteBuf(bb, 0xcafecafe);
        b.writeByteBuf(bb);
        assertEquals(0, bb.readableBytes());
        assertEquals(2042, b.position());

        bb = Unpooled.buffer(2042);
        int ret = b.readByteBuf(bb, 0, 2042);
        assertEquals(2042, ret);
        for (int i = 0; i < 1020 / Integer.BYTES; i++) {
            assertEquals(0xdeadbeef, bb.readInt());
        }
        assertEquals((byte) 0xde, bb.readByte());
        for (int i = 0; i < 1020 / Integer.BYTES; i++) {
            assertEquals(0xcafecafe, bb.readInt());
        }
    }

    @Test
    public void testPartialRead() throws Exception {
        ByteBuf bb = Unpooled.buffer(5000);

        Buffer b = new Buffer(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 4096);
        for (int i = 0; i < 4096 / Integer.BYTES; i++) {
            b.writeInt(0xdeadbeef);
        }

        int ret = b.readByteBuf(bb, 0, 5000);
        assertEquals(4096, ret);
    }

    @Test(expected = IOException.class)
    public void testReadIntAtBoundary() throws Exception {
        Buffer b = new Buffer(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 4096);

        for (int i = 0; i < 4096 / Integer.BYTES; i++) {
            b.writeInt(0xdeadbeef);
        }
        assertTrue(b.hasData(4092, Integer.BYTES));
        assertFalse(b.hasData(4093, Integer.BYTES));
        assertFalse(b.hasData(4096, Integer.BYTES));

        b.readInt(4096 - 2);
    }

    @Test(expected = IOException.class)
    public void testReadLongAtBoundary() throws Exception {
        Buffer b = new Buffer(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 4096);

        for (int i = 0; i < 4096 / Integer.BYTES; i++) {
            b.writeInt(0xdeadbeef);
        }
        assertTrue(b.hasData(4088, Long.BYTES));
        assertFalse(b.hasData(4089, Long.BYTES));
        assertFalse(b.hasData(4096, Long.BYTES));

        b.readInt(4096 - 2);
    }

    @Test
    public void testPadToAlignment() throws Exception {
        Buffer b = new Buffer(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1 << 23);

        for (int i = 0; i < 1025; i++) {
            b.writeInt(0xdededede);
        }
        int writtenLength = b.padToAlignment();

        assertEquals(8192, writtenLength);
        assertEquals(0xdededede, b.readInt(1024 * Integer.BYTES));
        for (int i = 1025 * Integer.BYTES; i < writtenLength; i += Integer.BYTES) {
            assertEquals(0xf0f0f0f0, b.readInt(i));
        }
        assertEquals(0, b.readInt(writtenLength));
    }

    @Test
    public void testFree() throws Exception {
        Buffer b = new Buffer(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1 << 23);
        b.free(); // success if process doesn't explode
        b.free();
    }

    static void fillByteBuf(ByteBuf bb, int value) {
        while (bb.writableBytes() >= Integer.BYTES) {
            bb.writeInt(value);
        }
        for (int i = 0; i < Integer.BYTES && bb.writableBytes() > 0; i++) {
            byte b = (byte) (value >> (Integer.BYTES - i - 1) * 8);
            bb.writeByte(b);
        }
    }
}
