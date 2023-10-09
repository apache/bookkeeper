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
package org.apache.distributedlog.io;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import org.junit.Test;

/**
 * Test Case for {@link CompressionCodec}.
 */
public class TestCompressionCodec {

    @Test(timeout = 10000)
    public void testUnknownCompressionCodec() throws Exception {
        assertEquals(
                CompressionCodec.Type.UNKNOWN,
                CompressionUtils.stringToType("unknown"));
    }

    @Test(timeout = 10000)
    public void testIdentityCompressionCodec() throws Exception {
        testCompressionCodec(CompressionUtils.getCompressionCodec(CompressionCodec.Type.NONE));
    }

    @Test(timeout = 10000)
    public void testLZ4CompressionCodec() throws Exception {
        testCompressionCodec(CompressionUtils.getCompressionCodec(CompressionCodec.Type.LZ4));
    }

    @Test(timeout = 10000)
    public void testIdentityCompressionCodec2() throws Exception {
        testCompressionCodec2(CompressionUtils.getCompressionCodec(CompressionCodec.Type.NONE));
    }

    @Test(timeout = 10000)
    public void testLZ4CompressionCodec2() throws Exception {
        testCompressionCodec2(CompressionUtils.getCompressionCodec(CompressionCodec.Type.LZ4));
    }

    private void testCompressionCodec(CompressionCodec codec) throws Exception {
        byte[] data = "identity-compression-codec".getBytes(UTF_8);
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        ByteBuf compressedBuf = codec.compress(buf, 0);
        ByteBuf decompressedBuf = codec.decompress(compressedBuf, data.length);
        assertEquals("The length of decompressed buf should be same as the original buffer",
                data.length, decompressedBuf.readableBytes());
        byte[] decompressedData = new byte[data.length];
        decompressedBuf.readBytes(decompressedData);
        assertArrayEquals("The decompressed bytes should be same as the original bytes",
                data, decompressedData);
        ReferenceCountUtil.release(buf);
        ReferenceCountUtil.release(compressedBuf);
        ReferenceCountUtil.release(decompressedBuf);
    }

    private void testCompressionCodec2(CompressionCodec codec) throws Exception {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(32, 4 * 1024 * 1024);
        for (int i = 0; i < 100; i++) {
            ByteBuffer record = ByteBuffer.wrap(("record-" + i).getBytes(UTF_8));
            buffer.writeInt(record.remaining());
            buffer.writeBytes(record);
        }
        byte[] uncompressedData = new byte[buffer.readableBytes()];
        buffer.slice().readBytes(uncompressedData);

        ByteBuf compressedBuf = codec.compress(buffer, 0);
        byte[] compressedData = new byte[compressedBuf.readableBytes()];
        compressedBuf.slice().readBytes(compressedData);

        ByteBuf decompressedBuf = codec.decompress(compressedBuf, uncompressedData.length);
        byte[] decompressedData = new byte[decompressedBuf.readableBytes()];
        decompressedBuf.slice().readBytes(decompressedData);

        ReferenceCountUtil.release(buffer);
        ReferenceCountUtil.release(compressedBuf);
        ReferenceCountUtil.release(decompressedBuf);
    }

}
