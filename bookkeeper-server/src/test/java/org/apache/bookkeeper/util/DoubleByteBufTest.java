/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.bookkeeper.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Test the Double byte buffer.
 */
public class DoubleByteBufTest {

    @Test
    public void testGetBytes() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2, 3 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 4, 5, 6 });
        doTest(b1, b2);
    }

    @Test
    public void testGetBytesWithDoubleByteBufAssource() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 3, 4 });
        ByteBuf b3 = Unpooled.wrappedBuffer(new byte[] { 5, 6 });

        ByteBuf b23 = DoubleByteBuf.get(b2, b3);
        doTest(b1, b23);
    }

    @Test
    public void testGetBytesWithIndex() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2, 3 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 9, 9, 4, 5, 6 });

        // Skip the two '9' from b2
        b2.readByte();
        b2.readByte();

        doTest(b1, b2);
    }

    private void doTest(ByteBuf b1, ByteBuf b2) {
        ByteBuf buf = DoubleByteBuf.get(b1, b2);

        assertEquals(6, buf.readableBytes());
        assertEquals(0, buf.writableBytes());

        ByteBuf dst1 = Unpooled.buffer(6);
        buf.getBytes(0, dst1);
        assertEquals(6, dst1.readableBytes());
        assertEquals(0, dst1.writableBytes());
        assertEquals(Unpooled.wrappedBuffer(new byte[] { 1, 2, 3, 4, 5, 6 }), dst1);

        ByteBuf dst2 = Unpooled.buffer(6);
        buf.getBytes(0, dst2, 4);
        assertEquals(4, dst2.readableBytes());
        assertEquals(2, dst2.writableBytes());
        assertEquals(Unpooled.wrappedBuffer(new byte[] { 1, 2, 3, 4 }), dst2);

        ByteBuf dst3 = Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 0, 0, 0 });
        buf.getBytes(0, dst3, 1, 4);
        assertEquals(6, dst3.readableBytes());
        assertEquals(0, dst3.writableBytes());
        assertEquals(Unpooled.wrappedBuffer(new byte[] { 0, 1, 2, 3, 4, 0 }), dst3);

        ByteBuf dst4 = Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 0, 0, 0 });
        buf.getBytes(2, dst4, 1, 3);
        assertEquals(6, dst4.readableBytes());
        assertEquals(0, dst4.writableBytes());
        assertEquals(Unpooled.wrappedBuffer(new byte[] { 0, 3, 4, 5, 0, 0 }), dst4);

        ByteBuf dst5 = Unpooled.wrappedBuffer(new byte[] { 0, 0, 0, 0, 0, 0 });
        buf.getBytes(3, dst5, 1, 3);
        assertEquals(6, dst5.readableBytes());
        assertEquals(0, dst5.writableBytes());
        assertEquals(Unpooled.wrappedBuffer(new byte[] { 0, 4, 5, 6, 0, 0 }), dst5);
    }

    @Test
    public void testCopyToArray() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 3, 4 });
        ByteBuf b = DoubleByteBuf.get(b1, b2);

        byte[] a1 = new byte[4];
        b.getBytes(0, a1);
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, a1);

        byte[] a2 = new byte[3];
        b.getBytes(1, a2);
        assertArrayEquals(new byte[] { 2, 3, 4 }, a2);
    }

    @Test
    public void testToByteBuffer() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 3, 4 });
        ByteBuf b = DoubleByteBuf.get(b1, b2);

        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }), b.nioBuffer());
    }

    @Test
    public void testNonDirectNioBuffer() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 3, 4 });
        ByteBuf b = DoubleByteBuf.get(b1, b2);
        assertFalse(b1.isDirect());
        assertFalse(b2.isDirect());
        assertFalse(b.isDirect());
        ByteBuffer nioBuffer = b.nioBuffer();
        assertFalse(nioBuffer.isDirect());
    }

    @Test
    public void testNonDirectPlusDirectNioBuffer() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
        ByteBuf b2 = Unpooled.directBuffer(2);
        ByteBuf b = DoubleByteBuf.get(b1, b2);
        assertFalse(b1.isDirect());
        assertTrue(b2.isDirect());
        assertFalse(b.isDirect());
        ByteBuffer nioBuffer = b.nioBuffer();
        assertFalse(nioBuffer.isDirect());
    }

    @Test
    public void testDirectPlusNonDirectNioBuffer() {
        ByteBuf b1 = Unpooled.directBuffer(2);
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
        ByteBuf b = DoubleByteBuf.get(b1, b2);
        assertTrue(b1.isDirect());
        assertFalse(b2.isDirect());
        assertFalse(b.isDirect());
        ByteBuffer nioBuffer = b.nioBuffer();
        assertFalse(nioBuffer.isDirect());
    }

    @Test
    public void testDirectNioBuffer() {
        ByteBuf b1 = Unpooled.directBuffer(2);
        ByteBuf b2 = Unpooled.directBuffer(2);
        ByteBuf b = DoubleByteBuf.get(b1, b2);
        assertTrue(b1.isDirect());
        assertTrue(b2.isDirect());
        assertTrue(b.isDirect());
        ByteBuffer nioBuffer = b.nioBuffer();
        assertTrue(nioBuffer.isDirect());
    }

    /**
     * Verify that readableBytes() returns writerIndex - readerIndex. In this case writerIndex is the end of the buffer
     * and readerIndex is increased by 64.
     *
     * @throws Exception
     */
    @Test
    public void testReadableBytes() throws Exception {
        ByteBuf b1 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PooledByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBuf buf = DoubleByteBuf.get(b1, b2);

        assertEquals(buf.readerIndex(), 0);
        assertEquals(buf.writerIndex(), 256);
        assertEquals(buf.readableBytes(), 256);

        for (int i = 0; i < 4; ++i) {
            buf.skipBytes(64);
            assertEquals(buf.readableBytes(), 256 - 64 * (i + 1));
        }
    }
}
