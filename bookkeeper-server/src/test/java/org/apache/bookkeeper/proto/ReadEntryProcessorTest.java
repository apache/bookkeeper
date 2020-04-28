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

import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
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
    private BookieRequestProcessor requestProcessor;
    private Bookie bookie;

    @Before
    public void setup() throws IOException, BookieException {
        channel = mock(Channel.class);
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
        SettableFuture<Boolean> fenceResult = SettableFuture.create();
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
        ReadRequest request = new ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId,
                1, BookieProtocol.FLAG_DO_FENCING, new byte[]{});
        ReadEntryProcessor processor = ReadEntryProcessor.create(request, channel, requestProcessor, service, true);
        processor.run();

        fenceResult.set(result);
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
        SettableFuture<Boolean> fenceResult = SettableFuture.create();
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
        ReadRequest request = new ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId,
                1, BookieProtocol.FLAG_DO_FENCING, new byte[]{});
        ReadEntryProcessor processor = ReadEntryProcessor.create(request, channel, requestProcessor, null, true);
        fenceResult.set(result);
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
        ReadRequest request = new ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId,
                1, (short) 0, new byte[]{});
        ReadEntryProcessor processor = ReadEntryProcessor.create(request, channel, requestProcessor, null, true);
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
}
