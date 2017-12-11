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
package org.apache.bookkeeper.bookie.storage.ldb;

// CHECKSTYLE.OFF: IllegalImport
import io.netty.util.internal.PlatformDependent;
// CHECKSTYLE.ON: IllegalImport

import java.nio.ByteOrder;

/**
 * Utility to serialize/deserialize longs into byte arrays.
 */
class ArrayUtil {

    private static final boolean UNALIGNED = PlatformDependent.isUnaligned();
    private static final boolean HAS_UNSAFE = PlatformDependent.hasUnsafe();
    private static final boolean BIG_ENDIAN_NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    public static long getLong(byte[] array, int index) {
        if (HAS_UNSAFE && UNALIGNED) {
            long v = PlatformDependent.getLong(array, index);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Long.reverseBytes(v);
        }

        return ((long) array[index] & 0xff) << 56 | //
                ((long) array[index + 1] & 0xff) << 48 | //
                ((long) array[index + 2] & 0xff) << 40 | //
                ((long) array[index + 3] & 0xff) << 32 | //
                ((long) array[index + 4] & 0xff) << 24 | //
                ((long) array[index + 5] & 0xff) << 16 | //
                ((long) array[index + 6] & 0xff) << 8 | //
                (long) array[index + 7] & 0xff;
    }

    public static void setLong(byte[] array, int index, long value) {
        if (HAS_UNSAFE && UNALIGNED) {
            PlatformDependent.putLong(array, index, BIG_ENDIAN_NATIVE_ORDER ? value : Long.reverseBytes(value));
        } else {
            array[index] = (byte) (value >>> 56);
            array[index + 1] = (byte) (value >>> 48);
            array[index + 2] = (byte) (value >>> 40);
            array[index + 3] = (byte) (value >>> 32);
            array[index + 4] = (byte) (value >>> 24);
            array[index + 5] = (byte) (value >>> 16);
            array[index + 6] = (byte) (value >>> 8);
            array[index + 7] = (byte) value;
        }
    }

    public static final boolean isArrayAllZeros(final byte[] array) {
        return PlatformDependent.isZero(array, 0, array.length);
    }
}
