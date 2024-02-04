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
package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.zip.CRC32;
import org.apache.bookkeeper.proto.checksum.CRC32DigestManager.CRC32Digest;

/**
 * Specialized implementation of CRC32 digest that uses reflection on {@link CRC32} class to get access to
 * "updateByteBuffer" method and pass a direct memory pointer.
 */
class DirectMemoryCRC32Digest implements CRC32Digest {

    public static boolean isSupported() {
        return updateBytes != null;
    }

    private int crcValue;

    @Override
    public long getValueAndReset() {
        long value = crcValue & 0xffffffffL;
        crcValue = 0;
        return value;
    }

    @Override
    public void update(ByteBuf buf, int index, int length) {
        try {
            if (buf.hasMemoryAddress()) {
                // Calculate CRC directly from the direct memory pointer
                crcValue = (int) updateByteBuffer.invoke(null, crcValue, buf.memoryAddress(), index, length);
            } else if (buf.hasArray()) {
                // Use the internal method to update from array based
                crcValue = updateArray(crcValue, buf.array(), buf.arrayOffset() + index, length);
            } else {
                // Fallback to data copy if buffer is not contiguous
                byte[] b = new byte[length];
                buf.getBytes(index, b, 0, length);
                crcValue = updateArray(crcValue, b, 0, length);
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static int updateArray(int crcValue, byte[] buf, int offset, int length)
            throws IllegalAccessException, InvocationTargetException {
        return (int) updateBytes.invoke(null, crcValue, buf, offset, length);
    }

    @Override
    public void update(byte[] buffer, int offset, int len) {
        try {
            crcValue = updateArray(crcValue, buffer, offset, len);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Method updateByteBuffer;
    private static final Method updateBytes;

    static {
        // Access CRC32 class private native methods to compute the crc on the ByteBuf direct memory,
        // without necessity to convert to a nio ByteBuffer.
        Method updateByteBufferMethod = null;
        Method updateBytesMethod = null;
        try {
            updateByteBufferMethod = CRC32.class.getDeclaredMethod("updateByteBuffer", int.class, long.class, int.class,
                    int.class);
            updateByteBufferMethod.setAccessible(true);

            updateBytesMethod = CRC32.class.getDeclaredMethod("updateBytes", int.class, byte[].class, int.class,
                    int.class);
            updateBytesMethod.setAccessible(true);
        } catch (Throwable e) {
            updateByteBufferMethod = null;
            updateBytesMethod = null;
        }

        updateByteBuffer = updateByteBufferMethod;
        updateBytes = updateBytesMethod;
    }
}
