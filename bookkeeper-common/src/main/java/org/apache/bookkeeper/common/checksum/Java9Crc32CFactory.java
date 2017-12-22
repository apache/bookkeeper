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
package org.apache.bookkeeper.common.checksum;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * A factory to create java9 CRC32C checksum instances.
 */
class Java9Crc32CFactory implements ChecksumFactory {

    final MethodHandle constructor;
    final Method mUpdateInt;
    final Method mUpdateBytesOffset;
    final Method mUpdateBytes;
    final Method mUpdateByteBuffer;
    final Method mGetValue;
    final Method mReset;

    Java9Crc32CFactory() {
        try {
            Class<?> cls = Class.forName("java.util.zip.CRC32C");
            constructor = MethodHandles.publicLookup().findConstructor(cls, MethodType.methodType(void.class));

            // get all the methods
            mUpdateInt = cls.getMethod("update", int.class);
            mUpdateBytes = cls.getMethod("update", byte[].class);
            mUpdateBytesOffset = cls.getMethod("update", byte[].class, int.class, int.class);
            mUpdateByteBuffer = cls.getMethod("update", ByteBuffer.class);
            mGetValue = cls.getMethod("getValue");
            mReset = cls.getMethod("reset");

        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Checksum create() {
        return new Java9CRC32C(this);
    }
}
