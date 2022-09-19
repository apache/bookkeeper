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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceLedgerRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link ForceLedgerProcessorV3}.
 */
public class ForceLedgerProcessorV3Test {

    private Request request;
    private ForceLedgerProcessorV3 processor;
    private Channel channel;
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
            .setForceLedgerRequest(ForceLedgerRequest.newBuilder()
                .setLedgerId(System.currentTimeMillis())
                .build())
            .build();
        channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);
        bookie = mock(Bookie.class);
        requestProcessor = mock(BookieRequestProcessor.class);
        when(requestProcessor.getBookie()).thenReturn(bookie);
        when(requestProcessor.getWaitTimeoutOnBackpressureMillis()).thenReturn(-1L);
        when(requestProcessor.getRequestStats()).thenReturn(new RequestStats(NullStatsLogger.INSTANCE));
        when(channel.isActive()).thenReturn(true);
        processor = new ForceLedgerProcessorV3(
            request,
            channel,
            requestProcessor);
    }

    @Test
    public void testForceLedger() throws Exception {
        when(channel.voidPromise()).thenReturn(mock(ChannelPromise.class));
        when(channel.writeAndFlush(any())).thenReturn(mock(ChannelPromise.class));
        doAnswer(invocationOnMock -> {
            WriteCallback wc = invocationOnMock.getArgument(1);

            wc.writeComplete(
                0,
                request.getForceLedgerRequest().getLedgerId(),
                BookieImpl.METAENTRY_ID_FORCE_LEDGER,
                null,
                null);
            return null;
        }).when(bookie).forceLedger(
            eq(request.getForceLedgerRequest().getLedgerId()),
            any(WriteCallback.class),
            same(channel));

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
            .forceLedger(eq(request.getForceLedgerRequest().getLedgerId()),
                    any(WriteCallback.class), same(channel));
        verify(channel, times(1)).writeAndFlush(any(Response.class));

        latch.await();

        assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        assertEquals(StatusCode.EOK, response.getStatus());
    }

}
