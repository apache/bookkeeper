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
package org.apache.bookkeeper.clients.impl.kv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.kv.PTableWriter;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link ByteBufTableWriterImpl}.
 */
public class ByteBufTableWriterImplTest {

    private PTableWriter<ByteBuf, ByteBuf> pTableWriter;
    private ByteBuf key;
    private ByteBuf value;
    private ByteBufTableWriterImpl tableWriter;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        pTableWriter = mock(PTableWriter.class);
        key = Unpooled.wrappedBuffer("test-key".getBytes(UTF_8));
        value = Unpooled.wrappedBuffer("test-value".getBytes(UTF_8));
        tableWriter = new ByteBufTableWriterImpl(pTableWriter);
    }

    @Test
    public void testWrite() {
        long sequenceId = System.currentTimeMillis();
        tableWriter.write(
            sequenceId,
            key,
            value);
        verify(pTableWriter, times(1))
            .write(eq(sequenceId), same(key), same(key), same(value));
    }

    @Test
    public void testIncrement() {
        long sequenceId = System.currentTimeMillis();
        tableWriter.increment(
            sequenceId,
            key,
            100L);
        verify(pTableWriter, times(1))
            .increment(eq(sequenceId), same(key), same(key), eq(100L));
    }

}
