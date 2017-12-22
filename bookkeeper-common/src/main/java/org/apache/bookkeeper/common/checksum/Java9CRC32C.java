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

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

/**
 * An object delegates all the checksum operations.
 */
class Java9CRC32C implements Checksum {

    private final Java9Crc32CFactory factory;
    private final Object checksum;

    Java9CRC32C(Java9Crc32CFactory factory) {
        this.factory = factory;
        try {
            this.checksum = factory.constructor.invoke();
        } catch (Throwable cause) {
            // should never happen
            throw new RuntimeException(cause);
        }
    }

    @Override
    public void update(int b) {
        try {
            factory.mUpdateInt.invoke(checksum, b);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(byte[] b, int off, int len) {
        try {
            factory.mUpdateBytesOffset.invoke(checksum, b, off, len);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(ByteBuffer buffer) {
        try {
            factory.mUpdateByteBuffer.invoke(checksum, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getValue() {
        try {
            return (long) factory.mGetValue.invoke(checksum);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        try {
            factory.mReset.invoke(checksum);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
