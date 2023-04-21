/*
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
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

/**
 * Testcases for ZeroBuffer.
 */
public class TestZeroBuffer {

    @Test
    public void testPut() {
        ByteBuffer testBuffer;
        byte[] testBufferArray;
        Random rand = new Random();

        // Test1 - trying ZeroBuffer.put on small sized TestBuffer
        testBuffer = ByteBuffer.allocate(5);
        testBufferArray = testBuffer.array();
        testBufferArray[4] = 7;
        Assert.assertFalse("Test1 - It is supposed to contain non-zero byte", isFilledWithZeros(testBufferArray, 0, 5));
        ZeroBuffer.put(testBuffer);
        Assert.assertTrue("Test1 - After calling, ZeroBuffer.put There aren't supposed to be non-zero bytes",
                isFilledWithZeros(testBufferArray, 0, 5));

        // Test2 - trying ZeroBuffer.put on 64*1024 sized TestBuffer
        testBuffer = ByteBuffer.allocate(64 * 1024);
        testBufferArray = testBuffer.array();
        rand.nextBytes(testBufferArray);
        Assert.assertFalse("Test2 - It is supposed to contain random non-zero bytes",
                isFilledWithZeros(testBufferArray, 0, 64 * 1024));
        ZeroBuffer.put(testBuffer);
        Assert.assertTrue("Test2 - After calling, ZeroBuffer.put There aren't supposed to be non-zero bytes",
                isFilledWithZeros(testBufferArray, 0, 64 * 1024));

        // Test3 - trying ZeroBuffer.put on portion (64*1024) of large sized
        // TestBuffer (256*1024)
        testBuffer = ByteBuffer.allocate(256 * 1024);
        testBufferArray = testBuffer.array();
        rand.nextBytes(testBufferArray);
        Assert.assertFalse("Test3 - It is supposed to contain random non-zero bytes",
                isFilledWithZeros(testBufferArray, 64 * 1024, 64 * 1024));
        testBuffer.position(64 * 1024);
        ZeroBuffer.put(testBuffer, 64 * 1024);
        Assert.assertTrue(
                "Test3 - After calling, ZeroBuffer.put There aren't supposed to be non-zero bytes "
                + "in the particular section", isFilledWithZeros(testBufferArray, 64 * 1024, 64 * 1024));
        Assert.assertFalse("Test3 - After calling, ZeroBuffer.put other sections shouldnt be touched",
                isFilledWithZeros(testBufferArray, 0, 64 * 1024));
        Assert.assertFalse("Test3 - After calling, ZeroBuffer.put other sections shouldnt be touched",
                isFilledWithZeros(testBufferArray, 128 * 1024, 128 * 1024));
    }

    @Test
    public void testReadOnlyBuffer() {
        ByteBuffer testReadOnlyBuffer;
        byte[] testBufferArray;

        // Test1 - trying ZeroBuffer.readOnlyBuffer for small size
        testReadOnlyBuffer = ZeroBuffer.readOnlyBuffer(5);
        Assert.assertTrue(
                "Test1 - ReadOnlyBuffer should have remaining 5 bytes but it has " + testReadOnlyBuffer.remaining(),
                testReadOnlyBuffer.remaining() == 5);
        testBufferArray = new byte[5];
        testReadOnlyBuffer.get(testBufferArray);
        Assert.assertTrue("Test1 - supposed to contain only zero bytes", isFilledWithZeros(testBufferArray, 0, 5));

        // Test2 - trying ZeroBuffer.readOnlyBuffer for 64*1024
        testReadOnlyBuffer = ZeroBuffer.readOnlyBuffer(64 * 1024);
        Assert.assertTrue("Test2 - ReadOnlyBuffer should have remaining 64*1024 bytes but it has "
                + testReadOnlyBuffer.remaining(), testReadOnlyBuffer.remaining() == 64 * 1024);
        testBufferArray = new byte[64 * 1024];
        testReadOnlyBuffer.get(testBufferArray);
        Assert.assertTrue("Test2 - supposed to contain only zero bytes",
                isFilledWithZeros(testBufferArray, 0, 64 * 1024));

        // Test3 - trying ZeroBuffer.readOnlyBuffer for > 64*1024
        testReadOnlyBuffer = ZeroBuffer.readOnlyBuffer(128 * 1024);
        Assert.assertTrue("Test3 - ReadOnlyBuffer should have remaining 128*1024 bytes but it has "
                + testReadOnlyBuffer.remaining(), testReadOnlyBuffer.remaining() == 128 * 1024);
        testBufferArray = new byte[128 * 1024];
        testReadOnlyBuffer.get(testBufferArray);
        Assert.assertTrue("Test3 - supposed to contain only zero bytes",
                isFilledWithZeros(testBufferArray, 0, 128 * 1024));
    }

    boolean isFilledWithZeros(byte[] byteArray, int start, int length) {
        for (int i = start; i < (start + length); i++) {
            if (byteArray[i] != 0) {
                return false;
            }
        }
        return true;
    }
}
