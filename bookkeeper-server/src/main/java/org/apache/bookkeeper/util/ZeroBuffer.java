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

package org.apache.bookkeeper.util;

import java.nio.ByteBuffer;

/**
 * Zero buffer utility.
 *
 */

public class ZeroBuffer {
    private static final byte[] zeroBytes = new byte[64 * 1024];

    /**
     * Fill zeros into given buffer.
     * @param dst
     */
    public static void put(ByteBuffer dst) {
        put(dst, dst.remaining());
    }

    /**
     * Fill zeros into given buffer up to given length.
     * @param dst
     * @param length
     */
    public static void put(ByteBuffer dst, int length) {
        while (length > zeroBytes.length) {
            dst.put(zeroBytes);
            length -= zeroBytes.length;
        }
        if (length > 0) {
            dst.put(zeroBytes, 0, length);
        }
    }

    /**
     * Returns read-only zero-filled buffer.
     * @param length
     * @return ByteBuffer
     */
    public static ByteBuffer readOnlyBuffer(int length) {
        ByteBuffer buffer;
        if (length <= zeroBytes.length) {
            buffer = ByteBuffer.wrap(zeroBytes, 0, length);
        } else {
            buffer = ByteBuffer.allocate(length);
            put(buffer);
            buffer.rewind();
        }
        return buffer.asReadOnlyBuffer();
    }
}
