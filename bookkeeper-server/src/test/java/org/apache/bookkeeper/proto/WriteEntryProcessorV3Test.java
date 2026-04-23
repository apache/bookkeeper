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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link WriteEntryProcessor}.
 */
public class WriteEntryProcessorV3Test {

    private Request request;
    private WriteEntryProcessorV3 processor;

    private Channel channel;
    private BookieRequestHandler requestHandler;
    private BookieRequestProcessor requestProcessor;
    private Bookie bookie;

    @Before
    public void setup() {
        request = Request.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                .setTxnId(System.currentTimeMillis())
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .build())
            .setAddRequest(AddRequest.newBuilder()
                .setLedgerId(System.currentTimeMillis())
                .setEntryId(System.currentTimeMillis() + 1)
                .setBody(ByteString.copyFromUtf8("test-entry-data"))
                .setMasterKey(ByteString.copyFrom(new byte[0]))
                .build())
            .build();
        channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);

        requestHandler = mock(BookieRequestHandler.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(requestHandler.ctx()).thenReturn(ctx);

        bookie = mock(Bookie.class);
        requestProcessor = mock(BookieRequestProcessor.class);
        when(requestProcessor.getBookie()).thenReturn(bookie);
        when(requestProcessor.getWaitTimeoutOnBackpressureMillis()).thenReturn(-1L);
        when(requestProcessor.getRequestStats()).thenReturn(new RequestStats(NullStatsLogger.INSTANCE));
        when(channel.isActive()).thenReturn(true);
        processor = WriteEntryProcessorV3.create(
            request,
            requestHandler,
            requestProcessor);
    }

    private void reinitRequest(int priority) {
        request = Request.newBuilder(request)
            .setHeader(BKPacketHeader.newBuilder(request.getHeader())
                .setPriority(priority)
                .build())
            .build();

        processor = WriteEntryProcessorV3.create(
            request,
            requestHandler,
            requestProcessor);
    }

    @Test
    public void testNoneHighPriorityWritesOnReadOnlyBookie() throws Exception {
        when(bookie.isReadOnly()).thenReturn(true);
        when(channel.voidPromise()).thenReturn(mock(ChannelPromise.class));

        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any());

        processor.run();

        verify(channel, times(1)).writeAndFlush(any(Response.class));

        latch.await();

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(StatusCode.EREADONLY, response.getStatus());
    }

    @Test
    public void testHighPriorityWritesOnReadOnlyBookieWhenHighPriorityWritesDisallowed() throws Exception {
        reinitRequest(100);

        when(bookie.isReadOnly()).thenReturn(true);
        when(bookie.isAvailableForHighPriorityWrites()).thenReturn(false);
        when(channel.voidPromise()).thenReturn(mock(ChannelPromise.class));

        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any());

        processor.run();

        verify(channel, times(1)).writeAndFlush(any(Response.class));

        latch.await();

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(StatusCode.EREADONLY, response.getStatus());
    }

    @Test
    public void testHighPriorityWritesOnReadOnlyBookieWhenHighPriorityWritesAllowed() throws Exception {
        reinitRequest(BookieProtocol.FLAG_HIGH_PRIORITY);

        when(bookie.isReadOnly()).thenReturn(true);
        when(bookie.isAvailableForHighPriorityWrites()).thenReturn(true);
        when(channel.voidPromise()).thenReturn(mock(ChannelPromise.class));

        doAnswer(invocationOnMock -> {
            WriteCallback wc = invocationOnMock.getArgument(2);

            wc.writeComplete(
                0,
                request.getAddRequest().getLedgerId(),
                request.getAddRequest().getEntryId(),
                null,
                null);
            return null;
        }).when(bookie).addEntry(
            any(ByteBuf.class),
            eq(false),
            any(WriteCallback.class),
            same(channel),
            eq(new byte[0]));

        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any());

        processor.run();

        verify(bookie, times(1))
            .addEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), same(channel), eq(new byte[0]));
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        latch.await();

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(StatusCode.EOK, response.getStatus());
    }

    @Test
    public void testNormalWritesOnWritableBookie() throws Exception {
        when(bookie.isReadOnly()).thenReturn(false);
        when(channel.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(channel.writeAndFlush(any())).thenReturn(mock(ChannelPromise.class));
        doAnswer(invocationOnMock -> {
            WriteCallback wc = invocationOnMock.getArgument(2);

            wc.writeComplete(
                0,
                request.getAddRequest().getLedgerId(),
                request.getAddRequest().getEntryId(),
                null,
                null);
            return null;
        }).when(bookie).addEntry(
            any(ByteBuf.class), eq(false), any(WriteCallback.class), same(channel), eq(new byte[0]));

        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any());

        processor.run();

        verify(bookie, times(1))
            .addEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), same(channel), eq(new byte[0]));
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        latch.await();

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(StatusCode.EOK, response.getStatus());
    }

    @Test
    public void testWritesCacheFlushTimeout() throws Exception {
        when(bookie.isReadOnly()).thenReturn(false);
        when(channel.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(channel.writeAndFlush(any())).thenReturn(mock(ChannelPromise.class));
        doAnswer(invocationOnMock -> {
            throw new BookieException.OperationRejectedException();
        }).when(bookie).addEntry(
                any(ByteBuf.class), eq(false), any(WriteCallback.class), same(channel), eq(new byte[0]));

        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any());

        processor.run();

        verify(bookie, times(1))
                .addEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), same(channel), eq(new byte[0]));
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        latch.await();
        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(StatusCode.ETOOMANYREQUESTS, response.getStatus());
    }

    @Test
    public void testWritesWithClientNotAcceptingResponses() throws Exception {
        when(requestProcessor.getWaitTimeoutOnBackpressureMillis()).thenReturn(5L);

        doAnswer(invocationOnMock -> {
            Channel ch = invocationOnMock.getArgument(0);
            ch.close();
            return null;
        }).when(requestProcessor).handleNonWritableChannel(any());

        when(channel.isWritable()).thenReturn(false);

        when(bookie.isReadOnly()).thenReturn(false);
        when(channel.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(channel.writeAndFlush(any())).thenReturn(mock(ChannelPromise.class));
        doAnswer(invocationOnMock -> {
            WriteCallback wc = invocationOnMock.getArgument(2);

            wc.writeComplete(
                    0,
                    request.getAddRequest().getLedgerId(),
                    request.getAddRequest().getEntryId(),
                    null,
                    null);
            return null;
        }).when(bookie).addEntry(
                any(ByteBuf.class), eq(false), any(WriteCallback.class), same(channel), eq(new byte[0]));

        processor.run();

        verify(bookie, times(1))
                .addEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), same(channel), eq(new byte[0]));
        verify(requestProcessor, times(1)).handleNonWritableChannel(channel);
        verify(channel, times(0)).writeAndFlush(any(Response.class));
        verify(channel, times(1)).close();
    }

}
