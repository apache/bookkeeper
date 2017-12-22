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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.apache.bookkeeper.common.util.Java;
import org.junit.Test;

/**
 * Unit test of different {@link ChecksumFactory}.
 */
public class ChecksumsTest {

    private void testUpdate(ChecksumFactory factory) {
        final byte[] bytes = "test checksum".getBytes(UTF_8);
        final int len = bytes.length;

        Checksum checksum1 = factory.create();
        Checksum checksum2 = factory.create();
        Checksum checksum3 = factory.create();
        Checksum checksum4 = factory.create();

        // checksum 1
        checksum1.update(bytes, 0, len);
        // checksum 2
        for (int i = 0; i < len; i++) {
            checksum2.update(bytes[i]);
        }
        // checksum 3
        checksum3.update(bytes, 0, len / 2);
        checksum3.update(bytes, len / 2, len - len / 2);
        // checksum 4
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        checksum4.update(buffer);

        assertEquals("Crc values should be the same", checksum1.getValue(), checksum2.getValue());
        assertEquals("Crc values should be the same", checksum1.getValue(), checksum3.getValue());
        assertEquals("Crc values should be the same", checksum1.getValue(), checksum4.getValue());
    }

    private void testCRC32Value(ChecksumFactory factory) {
        final byte[] bytes = "Some String".getBytes();

        Checksum checksum = factory.create();
        checksum.update(bytes);
        assertEquals(2021503672, checksum.getValue());
    }

    private void testCRC32CValue(ChecksumFactory factory) {
        final byte[] bytes = "Some String".getBytes();

        Checksum checksum = factory.create();
        checksum.update(bytes);
        assertEquals(608512271, checksum.getValue());
    }

    @Test
    public void testUpdateCRC32() {
        testUpdate(Checksums.crc32());
    }

    @Test
    public void testUpdatePureJavaCRC32C() {
        testUpdate(Checksums.pureJavaCrc32c());
    }

    @Test
    public void testUpdateJava9CRC32C() {
        if (Java.IS_JAVA9_COMPATIBLE) {
            testUpdate(Checksums.java9Crc32c());
        }
    }

    @Test
    public void testUpdateCRC32C() {
        testUpdate(Checksums.crc32c());
    }

    @Test
    public void testCRC32Value() {
        testCRC32Value(Checksums.crc32());
    }

    @Test
    public void testCRC32CValue() {
        testCRC32CValue(Checksums.crc32c());
    }

    @Test
    public void testCRC32CValuePureJava() {
        testCRC32CValue(Checksums.pureJavaCrc32c());
    }

    @Test
    public void testCRC32CValueJava9() {
        if (Java.IS_JAVA9_COMPATIBLE) {
            testCRC32CValue(Checksums.java9Crc32c());
        }
    }

}
