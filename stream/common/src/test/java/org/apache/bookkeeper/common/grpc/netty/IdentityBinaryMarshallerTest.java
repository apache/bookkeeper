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
import static org.junit.Assert.assertSame;

import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

/**
 * Unit test {@link IdentityBinaryMarshaller}.
 */
public class IdentityBinaryMarshallerTest {

    @Test
    public void testParseAndToBytes() {
        byte[] data = new byte[32];
        ThreadLocalRandom.current().nextBytes(data);
        byte[] dataCopy = new byte[data.length];
        System.arraycopy(data, 0, dataCopy, 0, data.length);

        byte[] serializedData = IdentityBinaryMarshaller.of().toBytes(data);
        // identity binary marshaller should return same object
        assertSame(data, serializedData);
        // identity binary marshaller should return same content
        assertArrayEquals(dataCopy, serializedData);

        byte[] deserializedData = IdentityBinaryMarshaller.of().parseBytes(data);
        // identity binary marshaller should return same object
        assertSame(data, deserializedData);
        // identity binary marshaller should return same content
        assertArrayEquals(dataCopy, deserializedData);
    }

}
