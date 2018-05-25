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

package org.apache.bookkeeper.common.grpc.netty;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.bookkeeper.common.util.Bytes;
import org.junit.Test;

/**
 * Unit test {@link LongBinaryMarshaller}.
 */
public class LongBinaryMarshallerTest {

    @Test
    public void testParseAndToBytes() {
        long value = System.currentTimeMillis();
        byte[] valueBytes = LongBinaryMarshaller.of().toBytes(value);
        assertArrayEquals(Bytes.toBytes(value), valueBytes);
        long parsedValue = LongBinaryMarshaller.of().parseBytes(valueBytes);
        assertEquals(value, parsedValue);
    }

}
