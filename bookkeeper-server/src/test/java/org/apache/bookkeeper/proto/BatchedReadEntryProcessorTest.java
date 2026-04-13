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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import org.apache.bookkeeper.util.ByteBufList;
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

    @Test
    public void testReadDataReturnsFirstEntryWhenSecondWouldOverflowMaxSize() throws Exception {
        long ledgerId = 1234L;
        long firstEntryId = 1L;
        int firstEntrySize = 20;
        long maxSize = 70;
        long expectedRemainingBudget = 10;

        ByteBuf firstEntry = entryBuffer(firstEntrySize);
        when(bookie.readEntry(eq(ledgerId), eq(firstEntryId))).thenReturn(firstEntry);
        when(bookie.readEntryIfFits(eq(ledgerId), eq(firstEntryId + 1), eq(expectedRemainingBudget))).thenReturn(null);

        BatchedReadEntryProcessor processor = createProcessor(ledgerId, firstEntryId, 5, maxSize);
        ByteBufList data = (ByteBufList) processor.readData();
        assertNotNull(data);
        try {
            assertEquals(1, data.size());
        } finally {
            data.release();
        }

        verify(bookie, times(1)).readEntry(eq(ledgerId), eq(firstEntryId));
        verify(bookie, times(1)).readEntryIfFits(eq(ledgerId), eq(firstEntryId + 1), eq(expectedRemainingBudget));
        verify(bookie, times(1)).readEntry(anyLong(), anyLong());
    }

    @Test
    public void testReadDataReturnsFirstEntryEvenIfItAloneExceedsMaxSize() throws Exception {
        long ledgerId = 1235L;
        long firstEntryId = 1L;
        int firstEntrySize = 20;
        long maxSize = 50;

        ByteBuf firstEntry = entryBuffer(firstEntrySize);
        when(bookie.readEntry(eq(ledgerId), eq(firstEntryId))).thenReturn(firstEntry);

        BatchedReadEntryProcessor processor = createProcessor(ledgerId, firstEntryId, 5, maxSize);
        ByteBufList data = (ByteBufList) processor.readData();
        assertNotNull(data);
        try {
            assertEquals(1, data.size());
        } finally {
            data.release();
        }

        verify(bookie, times(1)).readEntry(eq(ledgerId), eq(firstEntryId));
        verify(bookie, never()).readEntryIfFits(anyLong(), anyLong(), anyLong());
    }

    @Test
    public void testReadDataIncludesSecondEntryWhenRemainingBudgetExactlyFitsEntryAndDelimiter() throws Exception {
        long ledgerId = 1236L;
        long firstEntryId = 1L;
        int firstEntrySize = 20;
        int secondEntrySize = 12;
        long exactRemainingBudget = secondEntrySize + 4L;
        long maxSize = 24 + 8 + 4 + firstEntrySize + 4 + exactRemainingBudget;

        ByteBuf firstEntry = entryBuffer(firstEntrySize);
        ByteBuf secondEntry = entryBuffer(secondEntrySize);
        when(bookie.readEntry(eq(ledgerId), eq(firstEntryId))).thenReturn(firstEntry);
        when(bookie.readEntryIfFits(eq(ledgerId), eq(firstEntryId + 1), eq(exactRemainingBudget)))
                .thenReturn(secondEntry);

        BatchedReadEntryProcessor processor = createProcessor(ledgerId, firstEntryId, 5, maxSize);
        ByteBufList data = (ByteBufList) processor.readData();
        assertNotNull(data);
        try {
            assertEquals(2, data.size());
        } finally {
            data.release();
        }

        verify(bookie, times(1)).readEntry(eq(ledgerId), eq(firstEntryId));
        verify(bookie, times(1)).readEntryIfFits(eq(ledgerId), eq(firstEntryId + 1), eq(exactRemainingBudget));
        verify(bookie, times(1)).readEntry(anyLong(), anyLong());
    }

    @Test
    public void testReadDataStopsOnMissingSubsequentEntry() throws Exception {
        long ledgerId = 1237L;
        long firstEntryId = 1L;
        int firstEntrySize = 20;

        ByteBuf firstEntry = entryBuffer(firstEntrySize);
        when(bookie.readEntry(eq(ledgerId), eq(firstEntryId))).thenReturn(firstEntry);
        when(bookie.readEntryIfFits(eq(ledgerId), eq(firstEntryId + 1), anyLong()))
                .thenThrow(new Bookie.NoEntryException(ledgerId, firstEntryId + 1));

        BatchedReadEntryProcessor processor = createProcessor(ledgerId, firstEntryId, 5, 1024);
        ByteBufList data = (ByteBufList) processor.readData();
        assertNotNull(data);
        try {
            assertEquals(1, data.size());
        } finally {
            data.release();
        }
    }

    @Test
    public void testReadDataPropagatesIOExceptionAfterFirstEntryAndReleasesAccumulatedData() throws Exception {
        long ledgerId = 1238L;
        long firstEntryId = 1L;

        ByteBuf firstEntry = entryBuffer(20);
        when(bookie.readEntry(eq(ledgerId), eq(firstEntryId))).thenReturn(firstEntry);
        when(bookie.readEntryIfFits(eq(ledgerId), eq(firstEntryId + 1), anyLong()))
                .thenThrow(new IOException("disk error"));

        BatchedReadEntryProcessor processor = createProcessor(ledgerId, firstEntryId, 5, 1024);
        try {
            processor.readData();
            fail("Should propagate the storage failure");
        } catch (IOException expected) {
            assertEquals(0, firstEntry.refCnt());
        }
    }

    @Test
    public void testProcessPacketReturnsIoErrorWhenSubsequentBoundedReadFails() throws Exception {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            promise.setSuccess();
            latch.countDown();
            return promise;
        }).when(channel).writeAndFlush(any(Response.class));

        long ledgerId = 1239L;
        long firstEntryId = 1L;
        ByteBuf firstEntry = entryBuffer(20);
        when(bookie.readEntry(eq(ledgerId), eq(firstEntryId))).thenReturn(firstEntry);
        when(bookie.readEntryIfFits(eq(ledgerId), eq(firstEntryId + 1), anyLong()))
                .thenThrow(new IOException("disk error"));

        BatchedReadEntryProcessor processor = createProcessor(ledgerId, firstEntryId, 5, 1024);
        processor.run();

        latch.await();
        assertTrue(writtenObject.get() instanceof Response);
        BookieProtocol.BatchedReadResponse response = (BookieProtocol.BatchedReadResponse) writtenObject.get();
        try {
            assertEquals(BookieProtocol.EIO, response.getErrorCode());
            assertEquals(0, response.getData().size());
            assertEquals(0, firstEntry.refCnt());
        } finally {
            response.release();
        }
    }

    private BatchedReadEntryProcessor createProcessor(long ledgerId, long entryId, int maxCount, long maxSize) {
        ExecutorService service = mock(ExecutorService.class);
        BookieProtocol.BatchedReadRequest request = BookieProtocol.BatchedReadRequest.create(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId, BookieProtocol.FLAG_NONE, new byte[] {},
                0L, maxCount, maxSize);
        return BatchedReadEntryProcessor.create(request, requestHandler, requestProcessor, service, true,
                5 * 1024 * 1024);
    }

    private static ByteBuf entryBuffer(int size) {
        ByteBuf entry = ByteBufAllocator.DEFAULT.buffer(size);
        entry.writeZero(size);
        return entry;
    }
}
