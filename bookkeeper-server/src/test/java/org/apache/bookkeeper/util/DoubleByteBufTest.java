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

import java.nio.ByteBuffer;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DoubleByteBufTest {

    @Test(timeout = 30000)
    public void testGetBytes() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2, 3 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 4, 5, 6 });
        doTest(b1, b2);
    }

    @Test(timeout = 30000)
    public void testGetBytesWithDoubleByteBufAssource() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 3, 4 });
        ByteBuf b3 = Unpooled.wrappedBuffer(new byte[] { 5, 6 });

        ByteBuf b23 = DoubleByteBuf.get(b2, b3);
        doTest(b1, b23);
    }

    @Test(timeout = 30000)
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

    @Test(timeout = 30000)
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

    @Test(timeout = 30000)
    public void testToByteBuffer() {
        ByteBuf b1 = Unpooled.wrappedBuffer(new byte[] { 1, 2 });
        ByteBuf b2 = Unpooled.wrappedBuffer(new byte[] { 3, 4 });
        ByteBuf b = DoubleByteBuf.get(b1, b2);

        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }), b.nioBuffer());
    }
}
