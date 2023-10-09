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
package org.apache.distributedlog;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.io.CompressionCodec.Type;
import org.apache.distributedlog.io.CompressionUtils;

/**
 * An enveloped entry written to BookKeeper.
 *
 * <p>Data type in brackets. Interpretation should be on the basis of data types and not individual
 * bytes to honor Endianness.
 *
 * <p>Entry Structure:
 * ---------------
 * Bytes 0                                  : Version (Byte)
 * Bytes 1 - (DATA = 1+Header.length-1)     : Header (Integer)
 * Bytes DATA - DATA+3                      : Payload Length (Integer)
 * BYTES DATA+4 - DATA+4+payload.length-1   : Payload (Byte[])
 *
 *  <p>V1 Header Structure: // Offsets relative to the start of the header.
 * -------------------
 * Bytes 0 - 3                              : Flags (Integer)
 * Bytes 4 - 7                              : Original payload size before compression (Integer)
 *
 * <p>Flags: // 32 Bits
 *      -----
 *      0 ... 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
 *                                      |_|
 *                                       |
 *                               Compression Type
 *Compression Type: // 2 Bits (Least significant)
 *      ----------------
 *      00      : No Compression
 *      01      : LZ4 Compression
 *      10      : Unused
 *      11      : Unused
 */
class EnvelopedEntry {

    public static final byte VERSION_ONE = 1;
    public static final int HEADER_LENGTH =
              Byte.BYTES // version
            + Integer.BYTES // flags
            + Integer.BYTES // decompressed size
            + Integer.BYTES; // compressed size

    static final int VERSION_OFFSET = 0;
    static final int FLAGS_OFFSET = VERSION_OFFSET + Byte.BYTES;
    static final int DECOMPRESSED_SIZE_OFFSET = FLAGS_OFFSET + Integer.BYTES;
    static final int COMPRESSED_SIZE_OFFSET = DECOMPRESSED_SIZE_OFFSET + Integer.BYTES;

    public static final byte CURRENT_VERSION = VERSION_ONE;
    public static final int COMPRESSION_CODEC_MASK = 0x3;

    /**
     * Return an {@link ByteBuf} that reads from the provided {@link ByteBuf}, decompresses the data
     * and returns a new InputStream wrapping the underlying payload.
     *Note that src is modified by this call.
     *
     * @return
     *      New Input stream with the underlying payload.
     * @throws Exception
     */
    public static ByteBuf fromEnvelopedBuf(ByteBuf src, StatsLogger statsLogger)
            throws IOException {
        byte version = src.readByte();
        if (version != CURRENT_VERSION) {
            throw new IOException(String.format("Version mismatch while reading. Received: %d,"
                + " Required: %d", version, CURRENT_VERSION));
        }
        int flags = src.readInt();
        int codecCode = flags & COMPRESSION_CODEC_MASK;
        int originDataLen = src.readInt();
        int actualDataLen = src.readInt();
        ByteBuf compressedBuf = src.slice(src.readerIndex(), actualDataLen);
        ByteBuf decompressedBuf;
        try {
            if (Type.NONE.code() == codecCode && originDataLen != actualDataLen) {
                throw new IOException("Inconsistent data length found for a non-compressed entry : compressed = "
                        + originDataLen + ", actual = " + actualDataLen);
            }
            CompressionCodec codec = CompressionUtils.getCompressionCodec(Type.of(codecCode));
            decompressedBuf = codec.decompress(compressedBuf, originDataLen);
        } finally {
            ReferenceCountUtil.release(compressedBuf);
        }
        return decompressedBuf;
    }

}
