/*
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
 */
package com.scurrilous.circe.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Java9IntHash implements IntHash {
    static final boolean HAS_JAVA9_CRC32C;
    private static final Method UPDATE_BYTES;
    private static final Method UPDATE_DIRECT_BYTEBUFFER;

    private static final String CRC32C_CLASS_NAME = "java.util.zip.CRC32C";

    private static final FastThreadLocal<byte[]> TL_BUFFER = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[4096];
        }
    };

    static {
        boolean hasJava9CRC32C = false;
        Method updateBytes = null;
        Method updateDirectByteBuffer = null;

        try {
            Class<?> c = Class.forName(CRC32C_CLASS_NAME);
            updateBytes = c.getDeclaredMethod("updateBytes", int.class, byte[].class, int.class, int.class);
            updateBytes.setAccessible(true);
            updateDirectByteBuffer =
                    c.getDeclaredMethod("updateDirectByteBuffer", int.class, long.class, int.class, int.class);
            updateDirectByteBuffer.setAccessible(true);

            hasJava9CRC32C = true;
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to use reflected methods: ", e);
            }
            updateBytes = null;
            updateDirectByteBuffer = null;
        }

        HAS_JAVA9_CRC32C = hasJava9CRC32C;
        UPDATE_BYTES = updateBytes;
        UPDATE_DIRECT_BYTEBUFFER = updateDirectByteBuffer;
    }

    @Override
    public int calculate(ByteBuf buffer) {
        return resume(0, buffer);
    }

    @Override
    public int calculate(ByteBuf buffer, int offset, int len) {
        return resume(0, buffer, offset, len);
    }

    private int updateDirectByteBuffer(int current, long address, int offset, int length) {
        try {
            return (int) UPDATE_DIRECT_BYTEBUFFER.invoke(null, current, address, offset, offset + length);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int resume(int current, byte[] array, int offset, int length) {
        // the bit-wise complementing of the input and output is explained in the resume method below
        current = ~current;
        current = updateBytes(current, array, offset, length);
        return ~current;
    }

    private static int updateBytes(int current, byte[] array, int offset, int length) {
        try {
            return (int) UPDATE_BYTES.invoke(null, current, array, offset, offset + length);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int resume(int current, ByteBuf buffer) {
        return resume(current, buffer, buffer.readerIndex(), buffer.readableBytes());
    }

    @Override
    public int resume(int current, ByteBuf buffer, int offset, int len) {
        // flip the bits (bit-wise complements) for the input value.
        // this serves two purposes:
        // - the CRC32C algorithm is designed to start with a seed value of all bits set to 1 (0xffffffff)
        //   - when 0 is passed in initially, ~0 will result in the correct initial value (0xfffffff).
        // - the CRC32C algorithm is designed in a way that the final value is complemented as the last step.
        //   - this method will always complement the return value
        // - for iterative use, the input value should be complemented to continue calculations.
        // This way the algorithm can be used incrementally without a separate initialization step
        // and finalization step.
        current = ~current;

        if (buffer.hasMemoryAddress()) {
            current = updateDirectByteBuffer(current, buffer.memoryAddress(), offset, len);
        } else if (buffer.hasArray()) {
            int arrayOffset = buffer.arrayOffset() + offset;
            current = updateBytes(current, buffer.array(), arrayOffset, len);
        } else {
            byte[] b = TL_BUFFER.get();
            int toRead = len;
            int loopOffset = offset;
            while (toRead > 0) {
                int length = Math.min(toRead, b.length);
                buffer.getBytes(loopOffset, b, 0, length);
                current = updateBytes(current, b, 0, length);
                toRead -= length;
                loopOffset += length;
            }
        }

        // return a complement of the current value to match the CRC32C algorithm's finalization step.
        // if there's another resume step, the value will be complemented to start the next step.
        return ~current;
    }
}
