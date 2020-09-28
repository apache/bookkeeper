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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
// CHECKSTYLE.OFF: IllegalImport
import io.netty.util.internal.PlatformDependent;
// CHECKSTYLE.ON: IllegalImport

import java.io.IOException;

import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.junit.Test;

/**
 * TestBuffer.
 */
public class TestBuffer {

    @Test
    public void testIsAligned() throws Exception {
        assertThat(Buffer.isAligned(1234), equalTo(false));
        assertThat(Buffer.isAligned(4096), equalTo(true));
        assertThat(Buffer.isAligned(40960), equalTo(true));
        assertThat(Buffer.isAligned(1 << 20), equalTo(true));
        assertThat(Buffer.isAligned(-1), equalTo(false));
        assertThat(Buffer.isAligned(Integer.MIN_VALUE), equalTo(false));
        assertThat(Buffer.isAligned(Integer.MAX_VALUE), equalTo(false));
    }

    @Test
    public void testNextAlignment() throws Exception {
        assertThat(Buffer.nextAlignment(0), equalTo(0));
        assertThat(Buffer.nextAlignment(1), equalTo(4096));
        assertThat(Buffer.nextAlignment(4096), equalTo(4096));
        assertThat(Buffer.nextAlignment(4097), equalTo(8192));
        assertThat(Buffer.nextAlignment(0x7FFFF000), equalTo(0x7FFFF000));
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
        new Buffer(new NativeIOImpl(), 1234);
    }

    @Test
    public void testWriteInt() throws Exception {
        int bufferSize = 1 << 20;
        Buffer b = new Buffer(new NativeIOImpl(), bufferSize);
        assertThat(b.hasSpace(bufferSize), equalTo(true));
        assertThat(b.position(), equalTo(0));
        b.writeInt(0xdeadbeef);


        assertThat(PlatformDependent.getByte(b.pointer() + 0), equalTo((byte) 0xde));
        assertThat(PlatformDependent.getByte(b.pointer() + 1), equalTo((byte) 0xad));
        assertThat(PlatformDependent.getByte(b.pointer() + 2), equalTo((byte) 0xbe));
        assertThat(PlatformDependent.getByte(b.pointer() + 3), equalTo((byte) 0xef));

        assertThat(b.hasSpace(bufferSize), equalTo(false));
        assertThat(b.position(), equalTo(Integer.BYTES));

        for (int i = 0; i < 10000; i++) {
            b.writeInt(i);
        }
        assertThat(b.position(), equalTo(Integer.BYTES * 10001));
        assertThat(b.hasSpace(bufferSize - (Integer.BYTES * 10001)), equalTo(true));
        assertThat(b.hasSpace(bufferSize - (Integer.BYTES * 10000)), equalTo(false));

        assertThat(b.readInt(0), equalTo(0xdeadbeef));
        for (int i = 0; i < 10000; i++) {
            assertThat(b.readInt((i + 1) * Integer.BYTES), equalTo(i));
        }
        b.reset();
        assertThat(b.hasSpace(bufferSize), equalTo(true));
        assertThat(b.position(), equalTo(0));
    }

    @Test
    public void testWriteBuffer() throws Exception {
        ByteBuf bb = Unpooled.buffer(1021);
        fillByteBuf(bb, 0xdeadbeef);
        int bufferSize = 1 << 20;
        Buffer b = new Buffer(new NativeIOImpl(), bufferSize);
        assertThat(b.position(), equalTo(0));
        b.writeByteBuf(bb);
        assertThat(b.position(), equalTo(1021));
        assertThat(bb.readableBytes(), equalTo(0));
        bb.clear();
        fillByteBuf(bb, 0xcafecafe);
        b.writeByteBuf(bb);
        assertThat(bb.readableBytes(), equalTo(0));
        assertThat(b.position(), equalTo(2042));

        bb = Unpooled.buffer(2042);
        int ret = b.readByteBuf(bb, 0, 2042);
        assertThat(ret, equalTo(2042));
        for (int i = 0; i < 1020 / Integer.BYTES; i++) {
            assertThat(bb.readInt(), equalTo(0xdeadbeef));
        }
        assertThat(bb.readByte(), equalTo((byte) 0xde));
        for (int i = 0; i < 1020 / Integer.BYTES; i++) {
            assertThat(bb.readInt(), equalTo(0xcafecafe));
        }
    }

    @Test
    public void testPartialRead() throws Exception {
        ByteBuf bb = Unpooled.buffer(5000);

        Buffer b = new Buffer(new NativeIOImpl(), 4096);
        for (int i = 0; i < 4096 / Integer.BYTES; i++) {
            b.writeInt(0xdeadbeef);
        }

        int ret = b.readByteBuf(bb, 0, 5000);
        assertThat(ret, equalTo(4096));
    }

    @Test(expected = IOException.class)
    public void testReadIntAtBoundary() throws Exception {
        Buffer b = new Buffer(new NativeIOImpl(), 4096);

        for (int i = 0; i < 4096 / Integer.BYTES; i++) {
            b.writeInt(0xdeadbeef);
        }
        assertThat(b.hasData(4092, Integer.BYTES), equalTo(true));
        assertThat(b.hasData(4093, Integer.BYTES), equalTo(false));
        assertThat(b.hasData(4096, Integer.BYTES), equalTo(false));
        b.readInt(4096 - 2);
    }

    @Test(expected = IOException.class)
    public void testReadLongAtBoundary() throws Exception {
        Buffer b = new Buffer(new NativeIOImpl(), 4096);

        for (int i = 0; i < 4096 / Integer.BYTES; i++) {
            b.writeInt(0xdeadbeef);
        }
        assertThat(b.hasData(4088, Long.BYTES), equalTo(true));
        assertThat(b.hasData(4089, Long.BYTES), equalTo(false));
        assertThat(b.hasData(4096, Long.BYTES), equalTo(false));
        b.readInt(4096 - 2);
    }

    @Test
    public void testPadToAlignment() throws Exception {
        Buffer b = new Buffer(new NativeIOImpl(), 1 << 23);

        for (int i = 0; i < 1025; i++) {
            b.writeInt(0xdededede);
        }
        int writtenLength = b.padToAlignment();

        assertThat(writtenLength, equalTo(8192));
        assertThat(b.readInt(1024 * Integer.BYTES), equalTo(0xdededede));
        for (int i = 1025 * Integer.BYTES; i < writtenLength; i += Integer.BYTES) {
            assertThat(b.readInt(i), equalTo(0xf0f0f0f0));
        }
        assertThat(b.readInt(writtenLength), equalTo(0));
    }

    @Test
    public void testFree() throws Exception {
        Buffer b = new Buffer(new NativeIOImpl(), 1 << 23);
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
