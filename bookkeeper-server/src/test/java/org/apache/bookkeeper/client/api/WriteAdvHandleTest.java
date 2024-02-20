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

package org.apache.bookkeeper.client.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test for {@link WriteAdvHandle}.
 */
public class WriteAdvHandleTest {

    @Rule
    public final TestName runtime = new TestName();

    private final long entryId;
    private final WriteAdvHandle handle = mock(WriteAdvHandle.class);
    private final LinkedBlockingQueue<ByteBuf> entryQueue;

    public WriteAdvHandleTest() {
        this.entryId = System.currentTimeMillis();
        this.entryQueue = new LinkedBlockingQueue<>();
        doAnswer(invocationOnMock -> {
            ByteBuf buf = invocationOnMock.getArgument(1);
            entryQueue.add(buf);
            return FutureUtils.value(-1L);
        }).when(handle).writeAsync(anyLong(), any(ByteBuf.class));
        when(handle.writeAsync(anyLong(), any(byte[].class))).thenCallRealMethod();
        when(handle.writeAsync(anyLong(), any(byte[].class), anyInt(), anyInt())).thenCallRealMethod();
        when(handle.writeAsync(anyLong(), any(ByteBuffer.class))).thenCallRealMethod();
    }

    @Test
    public void testAppendBytes() throws Exception {
        byte[] testData = runtime.getMethodName().getBytes(UTF_8);
        handle.writeAsync(entryId, testData);

        ByteBuf buffer = entryQueue.take();
        byte[] bufferData = ByteBufUtil.getBytes(buffer);
        assertArrayEquals(testData, bufferData);
        verify(handle, times(1)).writeAsync(eq(entryId), any(ByteBuf.class));
    }

    @Test
    public void testAppendBytes2() throws Exception {
        byte[] testData = runtime.getMethodName().getBytes(UTF_8);
        handle.writeAsync(entryId, testData, 1, testData.length / 2);
        byte[] expectedData = new byte[testData.length / 2];
        System.arraycopy(testData, 1, expectedData, 0, testData.length / 2);

        ByteBuf buffer = entryQueue.take();
        byte[] bufferData = ByteBufUtil.getBytes(buffer);
        assertArrayEquals(expectedData, bufferData);
        verify(handle, times(1)).writeAsync(eq(entryId), any(ByteBuf.class));
    }

    @Test
    public void testAppendByteBuffer() throws Exception {
        byte[] testData = runtime.getMethodName().getBytes(UTF_8);
        handle.writeAsync(entryId, ByteBuffer.wrap(testData, 1, testData.length / 2));
        byte[] expectedData = new byte[testData.length / 2];
        System.arraycopy(testData, 1, expectedData, 0, testData.length / 2);

        ByteBuf buffer = entryQueue.take();
        byte[] bufferData = ByteBufUtil.getBytes(buffer);
        assertArrayEquals(expectedData, bufferData);
        verify(handle, times(1)).writeAsync(eq(entryId), any(ByteBuf.class));
    }

}
