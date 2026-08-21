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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.proto.BookieProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookieProtocol.Response;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link ReadEntryProcessor}.
 */
public class ReadEntryProcessorTest {

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
        when(channel.newPromise()).thenReturn(mock(ChannelPromise.class, RETURNS_SELF));
        when(channel.writeAndFlush(any())).thenReturn(mock(ChannelPromise.class));

        EventLoop eventLoop = mock(EventLoop.class);
        when(eventLoop.inEventLoop()).thenReturn(true);
        when(channel.eventLoop()).thenReturn(eventLoop);
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

        ExecutorService service = Executors.newCachedThreadPool();
        long ledgerId = System.currentTimeMillis();
        ReadRequest request = ReadRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId,
                1, BookieProtocol.FLAG_DO_FENCING, new byte[]{});
        ReadEntryProcessor processor = ReadEntryProcessor.create(
                request, requestHandler, requestProcessor, service, true);
        processor.run();

        fenceResult.complete(result);
        latch.await();
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(1, response.getEntryId());
        assertEquals(ledgerId, response.getLedgerId());
        assertEquals(BookieProtocol.READENTRY, response.getOpCode());
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

        long ledgerId = System.currentTimeMillis();
        ReadRequest request = ReadRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId,
                1, BookieProtocol.FLAG_DO_FENCING, new byte[]{});
        ReadEntryProcessor processor = ReadEntryProcessor.create(request, requestHandler, requestProcessor, null, true);
        fenceResult.complete(result);
        processor.run();

        latch.await();
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(1, response.getEntryId());
        assertEquals(ledgerId, response.getLedgerId());
        assertEquals(BookieProtocol.READENTRY, response.getOpCode());
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

        long ledgerId = System.currentTimeMillis();
        ReadRequest request = ReadRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId,
                1, (short) 0, new byte[]{});
        ReadEntryProcessor processor = ReadEntryProcessor.create(request, requestHandler, requestProcessor, null, true);
        processor.run();

        latch.await();
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(1, response.getEntryId());
        assertEquals(ledgerId, response.getLedgerId());
        assertEquals(BookieProtocol.READENTRY, response.getOpCode());
        assertEquals(BookieProtocol.EOK, response.getErrorCode());
    }
    
    /**
     * Blocking request creation on the event loop can starve the write future
     * that a throttled V2 read waits on before releasing a read permit.
     */
    @Test
    public void testCreateDoesNotStarveV2WriteCompletionNeededToReleaseReadPermit() throws Exception {
        EventLoopGroup eventLoopGroup = new DefaultEventLoopGroup(1);
        ExecutorService service = Executors.newSingleThreadExecutor();
        Semaphore readsSemaphore = new Semaphore(1);
        CountDownLatch secondReadBlocked = new CountDownLatch(1);
        CountDownLatch writeCompleted = new CountDownLatch(1);
        CountDownLatch secondCreateReturned = new CountDownLatch(1);
        ReadEntryProcessor[] processor = new ReadEntryProcessor[1];
        try {
            EventLoop eventLoop = eventLoopGroup.next();
            when(channel.eventLoop()).thenReturn(eventLoop);
            ChannelPromise writePromise = new DefaultChannelPromise(channel, eventLoop);
            when(channel.writeAndFlush(any())).thenReturn(writePromise);

            doAnswer(inv -> {
                if (!readsSemaphore.tryAcquire()) {
                    secondReadBlocked.countDown();
                    readsSemaphore.acquireUninterruptibly();
                }
                return null;
            }).when(requestProcessor).onReadRequestStart(any(Channel.class));
            doAnswer(inv -> {
                readsSemaphore.release();
                return null;
            }).when(requestProcessor).onReadRequestFinish();

            requestProcessor.onReadRequestStart(channel);
            Future<?> firstResponse = service.submit(() -> {
                channel.writeAndFlush(new Object()).get();
                requestProcessor.onReadRequestFinish();
                writeCompleted.countDown();
                return null;
            });

            long ledgerId = System.currentTimeMillis();
            ReadRequest request = ReadRequest.create(
                    BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, 1, (short) 0, new byte[]{});
            eventLoop.execute(() -> {
                processor[0] = ReadEntryProcessor.create(request, requestHandler, requestProcessor, null, true);
                secondCreateReturned.countDown();
            });

            long waitUntilNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (secondReadBlocked.getCount() > 0
                    && secondCreateReturned.getCount() > 0
                    && System.nanoTime() < waitUntilNanos) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            assertTrue("second read create should either return or start waiting for a permit",
                    secondReadBlocked.getCount() == 0 || secondCreateReturned.getCount() == 0);
            eventLoop.execute(() -> writePromise.setSuccess());

            try {
                assertTrue("V2 write future completion must not be starved behind a blocked read create",
                        writeCompleted.await(1, TimeUnit.SECONDS));
            } finally {
                if (writeCompleted.getCount() > 0) {
                    readsSemaphore.release();
                }
            }
            assertTrue("second read create should return without blocking the event loop",
                    secondCreateReturned.await(1, TimeUnit.SECONDS));
            firstResponse.get(1, TimeUnit.SECONDS);
        } finally {
            if (processor[0] != null) {
                processor[0].recycle();
            }
            service.shutdownNow();
            eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
        }
    }
}
