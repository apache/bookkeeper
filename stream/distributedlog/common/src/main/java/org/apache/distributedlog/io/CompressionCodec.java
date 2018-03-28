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

import io.netty.buffer.ByteBuf;

/**
 * Common interface for compression/decompression operations using different compression codecs.
 */
public interface CompressionCodec {
    /**
     * Enum specifying the currently supported compression types.
     */
    enum Type {

        UNKNOWN(-0x1),
        NONE(0x0),
        LZ4(0x1);

        private int code;

        Type(int code) {
            this.code = code;
        }

        public int code() {
            return this.code;
        }

        public static Type of(int code) {
            switch (code) {
                case 0x0:
                    return NONE;
                case 0x1:
                    return LZ4;
                default:
                    return UNKNOWN;
            }
        }

    }

    /**
     * Return the compressed data as a byte buffer.
     *
     * @param uncompressed
     *          The data to be compressed
     * @param headerLen
     *          Account the header len for compressed buffer.
     * @return
     *          The compressed data
     *          The returned byte array is sized to the length of the compressed data
     */
    ByteBuf compress(ByteBuf uncompressed, int headerLen);

    /**
     * Return the decompressed data as a byte array.
     *
     * @param compressed
     *          The data to the decompressed
     * @return
     *          The decompressed data
     */
    ByteBuf decompress(ByteBuf compressed, int decompressedSize);
}
