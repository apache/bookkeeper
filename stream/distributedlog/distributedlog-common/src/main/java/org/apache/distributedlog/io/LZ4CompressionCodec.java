/**
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * An {@code lz4} based {@link CompressionCodec} implementation.
 *
 * <p>All functions are thread safe.
 */
public class LZ4CompressionCodec implements CompressionCodec {

    public static LZ4CompressionCodec of() {
        return INSTANCE;
    }

    private static final LZ4CompressionCodec INSTANCE = new LZ4CompressionCodec();

    private static final LZ4Factory factory = LZ4Factory.fastestJavaInstance();
    // Used for compression
    private static final LZ4Compressor compressor = factory.fastCompressor();
    // Used to decompress when the size of the output is known
    private static final LZ4FastDecompressor decompressor = factory.fastDecompressor();

    @Override
    public ByteBuf compress(ByteBuf uncompressed, int headerLen) {
        checkNotNull(uncompressed);
        checkArgument(uncompressed.readableBytes() > 0);

        int uncompressedLen = uncompressed.readableBytes();
        int maxLen = compressor.maxCompressedLength(uncompressedLen);

        // get the source bytebuffer
        ByteBuffer uncompressedNio = uncompressed.nioBuffer(uncompressed.readerIndex(), uncompressedLen);
        ByteBuf compressed = PooledByteBufAllocator.DEFAULT.buffer(
                maxLen + headerLen, maxLen + headerLen);
        ByteBuffer compressedNio = compressed.nioBuffer(headerLen, maxLen);

        int compressedLen = compressor.compress(
                uncompressedNio, uncompressedNio.position(), uncompressedLen,
                compressedNio, compressedNio.position(), maxLen);
        compressed.writerIndex(compressedLen + headerLen);

        return compressed;
    }

    @Override
    // length parameter is ignored here because of the way the fastDecompressor works.
    public ByteBuf decompress(ByteBuf compressed, int decompressedSize) {
        checkNotNull(compressed);
        checkArgument(compressed.readableBytes() >= 0);
        checkArgument(decompressedSize >= 0);

        ByteBuf uncompressed = PooledByteBufAllocator.DEFAULT.buffer(decompressedSize, decompressedSize);
        ByteBuffer uncompressedNio = uncompressed.nioBuffer(0, decompressedSize);
        ByteBuffer compressedNio = compressed.nioBuffer(compressed.readerIndex(), compressed.readableBytes());

        decompressor.decompress(
                compressedNio, compressedNio.position(),
                uncompressedNio, uncompressedNio.position(), uncompressedNio.remaining());
        uncompressed.writerIndex(decompressedSize);
        return uncompressed;
    }
}
