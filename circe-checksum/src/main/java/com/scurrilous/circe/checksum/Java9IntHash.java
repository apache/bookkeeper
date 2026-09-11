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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import lombok.CustomLog;

@CustomLog
public class Java9IntHash implements IntHash {
    static final boolean HAS_JAVA9_CRC32C;

    // Method handles rather than java.lang.reflect.Method: Method.invoke takes its arguments as an
    // Object[], so every call boxes the checksum, the address and the offsets and allocates the
    // array. Since this runs once per checksummed buffer, that showed up as ~9% of all allocation
    // in a broker under a write-heavy workload. invokeExact on a static final handle passes the
    // primitives straight through and lets the JIT inline the target, allocating nothing.
    private static final MethodHandle UPDATE_BYTES;
    private static final MethodHandle UPDATE_DIRECT_BYTEBUFFER;

    private static final String CRC32C_CLASS_NAME = "java.util.zip.CRC32C";

    private static final FastThreadLocal<byte[]> TL_BUFFER = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[4096];
        }
    };

    static {
        boolean hasJava9CRC32C = false;
        MethodHandle updateBytes = null;
        MethodHandle updateDirectByteBuffer = null;

        try {
            Class<?> c = Class.forName(CRC32C_CLASS_NAME);
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            // The methods are private to java.util.zip, so they are made accessible first and then
            // unreflected: Lookup.unreflect skips its own access check for a method whose accessible
            // flag is already set, which is what lets this reach them without a Java 9+ lookup API.
            Method updateBytesMethod =
                    c.getDeclaredMethod("updateBytes", int.class, byte[].class, int.class, int.class);
            updateBytesMethod.setAccessible(true);
            updateBytes = lookup.unreflect(updateBytesMethod);

            Method updateDirectByteBufferMethod =
                    c.getDeclaredMethod("updateDirectByteBuffer", int.class, long.class, int.class, int.class);
            updateDirectByteBufferMethod.setAccessible(true);
            updateDirectByteBuffer = lookup.unreflect(updateDirectByteBufferMethod);

            hasJava9CRC32C = true;
        } catch (Exception e) {
            log.debug().exception(e).log("Unable to use reflected methods");
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
            // The argument and return types have to match the handle's type exactly for
            // invokeExact: (int, long, int, int)int.
            return (int) UPDATE_DIRECT_BYTEBUFFER.invokeExact(current, address, offset, offset + length);
        } catch (Throwable t) {
            throw asUnchecked(t);
        }
    }

    @Override
    public int resume(int current, byte[] array, int offset, int length) {
        // the bit-wise complementing of the input and output is explained in the resume method below
        current = ~current;
        current = updateBytes(current, array, offset, length);
        return ~current;
    }

    @Override
    public boolean acceptsMemoryAddressBuffer() {
        return true;
    }

    private static int updateBytes(int current, byte[] array, int offset, int length) {
        try {
            // The argument and return types have to match the handle's type exactly for
            // invokeExact: (int, byte[], int, int)int.
            return (int) UPDATE_BYTES.invokeExact(current, array, offset, offset + length);
        } catch (Throwable t) {
            throw asUnchecked(t);
        }
    }

    /**
     * Adapts a failure from {@link MethodHandle#invokeExact}, which is declared to throw
     * {@link Throwable}, to something this method can throw. Unlike {@code Method.invoke}, an
     * exception raised by the target is not wrapped, so it is passed through unchanged when it
     * already is unchecked.
     */
    private static RuntimeException asUnchecked(Throwable t) {
        if (t instanceof Error) {
            throw (Error) t;
        }
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        return new RuntimeException(t);
    }

    @Override
    public int resume(int current, ByteBuf buffer) {
        return resume(current, buffer, buffer.readerIndex(), buffer.readableBytes());
    }

    @Override
    public int resume(int current, ByteBuf buffer, int offset, int len) {
        // The input value is bit-wise complemented for two reasons:
        // 1. The CRC32C algorithm is designed to start with a seed value where all bits are set to 1 (0xffffffff).
        //    When 0 is initially passed in, ~0 results in the correct initial value (0xffffffff).
        // 2. The CRC32C algorithm complements the final value as the last step. This method will always complement
        //    the return value. Therefore, when the algorithm is used iteratively, it is necessary to complement
        //    the input value to continue calculations.
        // This allows the algorithm to be used incrementally without needing separate initialization and
        // finalization steps.
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

        // The current value is complemented to align with the finalization step of the CRC32C algorithm.
        // If there is a subsequent resume step, the value will be complemented again to initiate the next step
        // as described in the comments in the beginning of this method.
        return ~current;
    }
}
