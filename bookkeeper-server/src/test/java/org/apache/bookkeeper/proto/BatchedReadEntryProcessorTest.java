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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.proto.BookieProtocol.Response;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.Test;



/**
 * Unit test {@link ReadEntryProcessor}.
 */
public class BatchedReadEntryProcessorTest {

    private Channel channel;
    private BookieRequestHandler requestHandler;
    private BookieRequestProcessor requestProcessor;
    private Bookie bookie;

    @Before
    public void setup() throws IOException, BookieException {
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
        when(channel.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(channel.writeAndFlush(any())).thenReturn(mock(ChannelPromise.class));
        EventLoop eventLoop = mock(EventLoop.class);
        when(eventLoop.inEventLoop()).thenReturn(true);
        when(channel.eventLoop()).thenReturn(eventLoop);
        ByteBuf buffer0 = ByteBufAllocator.DEFAULT.buffer(4);
        ByteBuf buffer1 = ByteBufAllocator.DEFAULT.buffer(4);
        ByteBuf buffer2 = ByteBufAllocator.DEFAULT.buffer(4);
        ByteBuf buffer3 = ByteBufAllocator.DEFAULT.buffer(4);
        ByteBuf buffer4 = ByteBufAllocator.DEFAULT.buffer(4);

        when(bookie.readEntry(anyLong(), anyLong())).thenReturn(buffer0).thenReturn(buffer1).thenReturn(buffer2)
                .thenReturn(buffer3).thenReturn(buffer4);
    }

    @Test
    public void testSuccessfulAsynchronousFenceRequest() throws Exception {
        testAsynchronousRequest(true, BookieProtocol.EOK);
    }

    @Test
    public void testFailedAsynchronousFenceRequest() throws Exception {
        testAsynchronousRequest(false, BookieProtocol.EIO);
    }

    private void testAsynchronousRequest(boolean result, int errorCode) throws Exception {
        CompletableFuture<Boolean> fenceResult = FutureUtils.createFuture();
        when(bookie.fenceLedger(anyLong(), any())).thenReturn(fenceResult);

        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            promise.setSuccess();
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any(Response.class));

        long requestId = 0;
        int maxCount = 5;
        long maxSize = 1024;
        ExecutorService service = Executors.newCachedThreadPool();
        long ledgerId = System.currentTimeMillis();
        BookieProtocol.BatchedReadRequest request = BookieProtocol.BatchedReadRequest.create(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, 1, BookieProtocol.FLAG_DO_FENCING, new byte[] {},
                requestId, maxCount, maxSize);
        ReadEntryProcessor processor = BatchedReadEntryProcessor.create(
                request, requestHandler, requestProcessor, service, true, 1024 * 1024 * 5);
        processor.run();

        fenceResult.complete(result);
        latch.await();
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(1, response.getEntryId());
        assertEquals(ledgerId, response.getLedgerId());
        assertEquals(BookieProtocol.BATCH_READ_ENTRY, response.getOpCode());
        assertEquals(errorCode, response.getErrorCode());
        service.shutdown();
    }

    @Test
    public void testSuccessfulSynchronousFenceRequest() throws Exception {
        testSynchronousRequest(true, BookieProtocol.EOK);
    }

    @Test
    public void testFailedSynchronousFenceRequest() throws Exception {
        testSynchronousRequest(false, BookieProtocol.EIO);
    }

    private void testSynchronousRequest(boolean result, int errorCode) throws Exception {
        CompletableFuture<Boolean> fenceResult = FutureUtils.createFuture();
        when(bookie.fenceLedger(anyLong(), any())).thenReturn(fenceResult);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            promise.setSuccess();
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any(Response.class));

        long requestId = 0;
        int maxCount = 5;
        long maxSize = 1024;
        ExecutorService service = Executors.newCachedThreadPool();
        long ledgerId = System.currentTimeMillis();
        BookieProtocol.BatchedReadRequest request = BookieProtocol.BatchedReadRequest.create(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, 1, BookieProtocol.FLAG_DO_FENCING, new byte[] {},
                requestId, maxCount, maxSize);
        ReadEntryProcessor processor = BatchedReadEntryProcessor.create(
                request, requestHandler, requestProcessor, service, true, 1024 * 1024 * 5);
        fenceResult.complete(result);
        processor.run();

        latch.await();
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(1, response.getEntryId());
        assertEquals(ledgerId, response.getLedgerId());
        assertEquals(BookieProtocol.BATCH_READ_ENTRY, response.getOpCode());
        assertEquals(errorCode, response.getErrorCode());
    }

    @Test
    public void testNonFenceRequest() throws Exception {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            promise.setSuccess();
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any(Response.class));

        long requestId = 0;
        int maxCount = 5;
        long maxSize = 1024;
        ExecutorService service = Executors.newCachedThreadPool();
        long ledgerId = System.currentTimeMillis();
        BookieProtocol.BatchedReadRequest request = BookieProtocol.BatchedReadRequest.create(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, 1, BookieProtocol.FLAG_DO_FENCING, new byte[] {},
                requestId, maxCount, maxSize);
        ReadEntryProcessor processor = BatchedReadEntryProcessor.create(
                request, requestHandler, requestProcessor, service, true, 1024 * 1024 * 5);
        processor.run();

        latch.await();
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(1, response.getEntryId());
        assertEquals(ledgerId, response.getLedgerId());
        assertEquals(BookieProtocol.BATCH_READ_ENTRY, response.getOpCode());
        assertEquals(BookieProtocol.EOK, response.getErrorCode());
    }
}