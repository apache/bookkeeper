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
package org.apache.bookkeeper.proto;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookieProtocol.ParsedAddRequest;
import org.apache.bookkeeper.proto.BookieProtocol.Response;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link WriteEntryProcessor}.
 */
public class WriteEntryProcessorTest {

    private ParsedAddRequest request;
    private WriteEntryProcessor processor;
    private Channel channel;
    private ChannelHandlerContext ctx;
    private BookieRequestHandler requestHandler;
    private BookieRequestProcessor requestProcessor;
    private Bookie bookie;

    @Before
    public void setup() {
        request = ParsedAddRequest.create(
            BookieProtocol.CURRENT_PROTOCOL_VERSION,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 1,
            (short) 0,
            new byte[0],
            Unpooled.wrappedBuffer("test-entry-data".getBytes(UTF_8)));
        channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);

        requestHandler = mock(BookieRequestHandler.class);
        ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(requestHandler.ctx()).thenReturn(ctx);

        bookie = mock(Bookie.class);
        requestProcessor = mock(BookieRequestProcessor.class);
        when(requestProcessor.getBookie()).thenReturn(bookie);
        when(requestProcessor.getRequestStats()).thenReturn(new RequestStats(NullStatsLogger.INSTANCE));
        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);
        processor = WriteEntryProcessor.create(
            request,
            requestHandler,
            requestProcessor);
    }

    private void reinitRequest(short flags) {
        request.release();
        request.recycle();
        processor.recycle();

        request = ParsedAddRequest.create(
            BookieProtocol.CURRENT_PROTOCOL_VERSION,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 1,
            flags,
            new byte[0],
            Unpooled.wrappedBuffer("test-entry-data".getBytes(UTF_8)));
        processor = WriteEntryProcessor.create(
            request,
            requestHandler,
            requestProcessor);
    }

    @Test
    public void testNoneHighPriorityWritesOnReadOnlyBookie() throws Exception {
        when(bookie.isReadOnly()).thenReturn(true);
        ChannelPromise mockPromise = mock(ChannelPromise.class);
        when(channel.newPromise()).thenReturn(mockPromise);
        when(mockPromise.addListener(any())).thenReturn(mockPromise);

        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return null;
        }).when(channel).writeAndFlush(any(), any());

        processor.run();

        verify(channel, times(1)).writeAndFlush(any(), any());

        latch.await();

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(BookieProtocol.EREADONLY, response.getErrorCode());

        response.release();
        response.recycle();
    }

    @Test
    public void testHighPriorityWritesOnReadOnlyBookieWhenHighPriorityWritesDisallowed() throws Exception {
        reinitRequest(BookieProtocol.FLAG_HIGH_PRIORITY);

        when(bookie.isReadOnly()).thenReturn(true);
        when(bookie.isAvailableForHighPriorityWrites()).thenReturn(false);
        ChannelPromise mockPromise = mock(ChannelPromise.class);
        when(channel.newPromise()).thenReturn(mockPromise);
        when(mockPromise.addListener(any())).thenReturn(mockPromise);

        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return null;
        }).when(channel).writeAndFlush(any(), any());

        processor.run();

        verify(channel, times(1)).writeAndFlush(any(), any());

        latch.await();

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(BookieProtocol.EREADONLY, response.getErrorCode());

        response.release();
        response.recycle();
    }

    @Test
    public void testHighPriorityWritesOnReadOnlyBookieWhenHighPriorityWritesAllowed() throws Exception {
        reinitRequest(BookieProtocol.FLAG_HIGH_PRIORITY);

        when(bookie.isReadOnly()).thenReturn(true);
        when(bookie.isAvailableForHighPriorityWrites()).thenReturn(true);
        ChannelPromise mockPromise = mock(ChannelPromise.class);
        when(channel.newPromise()).thenReturn(mockPromise);
        when(mockPromise.addListener(any())).thenReturn(mockPromise);
        doAnswer(invocationOnMock -> {
            processor.writeComplete(0, request.ledgerId, request.entryId, null, null);
            return null;
        }).when(bookie).addEntry(any(ByteBuf.class), eq(false), same(processor), same(requestHandler), eq(new byte[0]));

        AtomicReference<Integer> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return null;
        }).when(requestHandler).prepareSendResponseV2(anyInt(), any());

        processor.run();

        verify(bookie, times(1))
            .addEntry(any(ByteBuf.class), eq(false), same(processor), same(requestHandler), eq(new byte[0]));
        verify(requestHandler, times(1)).prepareSendResponseV2(anyInt(), any());
//        verify(channel, times(1)).writeAndFlush(any(), any());

        latch.await();

        assertTrue(writtenObject.get() instanceof Integer);
        assertEquals(BookieProtocol.EOK, (int) writtenObject.get());
    }

    @Test
    public void testNormalWritesOnWritableBookie() throws Exception {
        when(bookie.isReadOnly()).thenReturn(false);
        ChannelPromise mockPromise = mock(ChannelPromise.class);
        when(channel.newPromise()).thenReturn(mockPromise);
        when(mockPromise.addListener(any())).thenReturn(mockPromise);
        doAnswer(invocationOnMock -> {
            processor.writeComplete(0, request.ledgerId, request.entryId, null, null);
            return null;
        }).when(bookie).addEntry(any(ByteBuf.class), eq(false), same(processor), same(requestHandler), eq(new byte[0]));

        AtomicReference<Integer> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return null;
        }).when(requestHandler).prepareSendResponseV2(anyInt(), any());

        processor.run();

        verify(bookie, times(1))
            .addEntry(any(ByteBuf.class), eq(false), same(processor), same(requestHandler), eq(new byte[0]));
        verify(requestHandler, times(1)).prepareSendResponseV2(anyInt(), any());

        latch.await();

        assertEquals(BookieProtocol.EOK, (int) writtenObject.get());
    }

    @Test
    public void testWritesCacheFlushTimeout() throws Exception {
        when(bookie.isReadOnly()).thenReturn(false);
        ChannelPromise mockPromise = mock(ChannelPromise.class);
        when(channel.newPromise()).thenReturn(mockPromise);
        when(mockPromise.addListener(any())).thenReturn(mockPromise);
        when(channel.writeAndFlush(any())).thenReturn(mock(ChannelPromise.class));
        doAnswer(invocationOnMock -> {
            throw new BookieException.OperationRejectedException();
        }).when(bookie).addEntry(
                any(ByteBuf.class), eq(false), same(processor), same(requestHandler), eq(new byte[0]));

        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any(), any());

        processor.run();

        verify(bookie, times(1))
                .addEntry(any(ByteBuf.class), eq(false), same(processor), same(requestHandler), eq(new byte[0]));
        verify(channel, times(1)).writeAndFlush(any(Response.class), any());

        latch.await();
        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(BookieProtocol.ETOOMANYREQUESTS, response.getErrorCode());
    }
}
