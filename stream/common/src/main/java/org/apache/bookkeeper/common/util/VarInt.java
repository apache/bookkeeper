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
package org.apache.bookkeeper.common.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Variable-length encoding for integers.
 *
 * <p>Handles, in a common encoding format, signed bytes, shorts, ints, and longs.
 * Takes between 1 and 10 bytes.
 * Less efficient than BigEndian{Int,Long} coder for negative or large numbers.
 * All negative ints are encoded using 5 bytes, longs take 10 bytes.
 */
public class VarInt {

    private static long convertIntToLongNoSignExtend(int v) {
        return v & 0xFFFFFFFFL;
    }

    /**
     * Encodes the given value onto the buffer.
     *
     * @param v   the value
     * @param buf the buffer
     * @throws IOException the io exception
     */
    public static void encode(int v, OutputStream buf) throws IOException {
        encode(convertIntToLongNoSignExtend(v), buf);
    }

    /**
     * Encodes the given value onto the buffer.
     *
     * @param v   the value
     * @param buf the buffer
     * @throws IOException the io exception
     */
    public static void encode(long v, OutputStream buf) throws IOException {
        do {
            // Encode next 7 bits + terminator bit
            long bits = v & 0x7F;
            v >>>= 7;
            int b = (int) (bits | ((v != 0) ? 0x80 : 0));
            buf.write(b);
        } while (v != 0);
    }

    /**
     * Decodes an integer value from the given buffer.
     *
     * @param buf the buffer
     * @return the int value that decoded
     * @throws IOException the io exception
     */
    public static int decodeInt(InputStream buf) throws IOException {
        long r = decodeLong(buf);
        if (r < 0 || r >= 1L << 32) {
            throw new IOException("var int overflow " + r);
        }
        return (int) r;
    }

    /**
     * Decodes a long value from the given buffer.
     *
     * @param buf the buf
     * @return the long value that decoded
     * @throws IOException the io exception
     */
    public static long decodeLong(InputStream buf) throws IOException {
        long result = 0;
        int shift = 0;
        int b;
        do {
            // Get 7 bits from next byte
            b = buf.read();
            if (b < 0) {
                if (shift == 0) {
                    throw new EOFException();
                } else {
                    throw new IOException("varint not terminated");
                }
            }
            long bits = b & 0x7F;
            if (shift >= 64 || (shift == 63 && bits > 1)) {
                // Out of range
                throw new IOException("varint too long");
            }
            result |= bits << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    /**
     * Returns the length of the encoding of the given value (in bytes).
     *
     * @param v the value
     * @return the length
     */
    public static int getLength(int v) {
        return getLength(convertIntToLongNoSignExtend(v));
    }

    /**
     * Returns the length of the encoding of the given value (in bytes).
     *
     * @param v the value
     * @return the length
     */
    public static int getLength(long v) {
        int result = 0;
        do {
            result++;
            v >>>= 7;
        } while (v != 0);
        return result;
    }
}
