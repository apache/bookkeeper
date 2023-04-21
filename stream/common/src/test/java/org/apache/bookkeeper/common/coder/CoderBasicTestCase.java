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
package org.apache.bookkeeper.common.coder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Basic Test Case for {@link Coder}s.
 */
public abstract class CoderBasicTestCase {

    static <T> byte[] encode(Coder<T> coder, T value) {
        return coder.encode(value);
    }

    static <T> T decode(Coder<T> coder, byte[] bytes) {
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        buf.setIndex(0, bytes.length);
        return coder.decode(buf);
    }

    private static <T> T decodeEncode(Coder<T> coder, T value) {
        byte[] data = encode(coder, value);
        assertEquals(coder.getSerializedSize(value), data.length);
        return decode(coder, data);
    }

    public static <T> void coderDecodeEncodeEqual(Coder<T> coder, T value) {
        assertThat(decodeEncode(coder, value), equalTo(value));
    }
}
