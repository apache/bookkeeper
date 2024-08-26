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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Unit test for the default methods in {@link WriteHandle}.
 */
@Slf4j
public class WriteHandleTest {

    private final WriteHandle handle = mock(WriteHandle.class);
    private final LinkedBlockingQueue<ByteBuf> entryQueue;
    private String testName;

    public WriteHandleTest() throws Exception {
        this.entryQueue = new LinkedBlockingQueue<>();
        doAnswer(invocationOnMock -> {
            ByteBuf buf = invocationOnMock.getArgument(0);
            entryQueue.add(buf);
            return -1L;
        }).when(handle).append(any(ByteBuf.class));
        when(handle.append(any(byte[].class))).thenCallRealMethod();
        when(handle.append(any(byte[].class), anyInt(), anyInt())).thenCallRealMethod();
        when(handle.append(any(ByteBuffer.class))).thenCallRealMethod();
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        testName = testInfo.getDisplayName();
    }

    @Test
    public void testAppendBytes() throws Exception {
        byte[] testData = testName.getBytes(UTF_8);
        handle.append(testData);

        ByteBuf buffer = entryQueue.take();
        byte[] bufferData = ByteBufUtil.getBytes(buffer);
        assertArrayEquals(testData, bufferData);
        verify(handle, times(1)).append(any(ByteBuf.class));
    }

    @Test
    public void testAppendBytes2() throws Exception {
        byte[] testData = testName.getBytes(UTF_8);
        handle.append(testData, 1, testData.length / 2);
        byte[] expectedData = new byte[testData.length / 2];
        System.arraycopy(testData, 1, expectedData, 0, testData.length / 2);

        ByteBuf buffer = entryQueue.take();
        byte[] bufferData = ByteBufUtil.getBytes(buffer);
        assertArrayEquals(expectedData, bufferData);
        verify(handle, times(1)).append(any(ByteBuf.class));
    }

    @Test
    public void testAppendByteBuffer() throws Exception {
        byte[] testData = testName.getBytes(UTF_8);
        handle.append(ByteBuffer.wrap(testData, 1, testData.length / 2));
        byte[] expectedData = new byte[testData.length / 2];
        System.arraycopy(testData, 1, expectedData, 0, testData.length / 2);

        ByteBuf buffer = entryQueue.take();
        byte[] bufferData = ByteBufUtil.getBytes(buffer);
        assertArrayEquals(expectedData, bufferData);
        verify(handle, times(1)).append(any(ByteBuf.class));
    }
}
