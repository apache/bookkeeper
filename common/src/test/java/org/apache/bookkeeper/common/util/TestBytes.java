/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit test for {@link Bytes}.
 */
public class TestBytes {

    @Test
    public void testOneNumber() {
        long timestamp = System.currentTimeMillis();
        byte[] bytes = Bytes.toBytes(timestamp);
        long readTimestamp = Bytes.toLong(bytes, 0);
        assertEquals(timestamp, readTimestamp);
    }

    @Test
    public void testTwoNumbers() {
        long timestamp1 = System.currentTimeMillis();
        long timestamp2 = 2 * timestamp1;
        byte[] bytes = new byte[16];
        Bytes.toBytes(timestamp1, bytes, 0);
        Bytes.toBytes(timestamp2, bytes, 8);
        long readTimestamp1 = Bytes.toLong(bytes, 0);
        long readTimestamp2 = Bytes.toLong(bytes, 8);
        assertEquals(timestamp1, readTimestamp1);
        assertEquals(timestamp2, readTimestamp2);
    }

}
