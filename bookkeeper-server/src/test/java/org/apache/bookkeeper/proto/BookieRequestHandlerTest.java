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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link BookieRequestHandler}.
 */
public class BookieRequestHandlerTest {

    private BookieRequestProcessor requestProcessor;
    private ChannelGroup allChannels;
    private ServerConfiguration serverConfiguration;
    private WriteEntryProcessor writeEntryProcessor;
    private ChannelHandlerContext ctx;
    private RequestStats requestStats;
    private OpStatsLogger getAddRequestStats;

    @Before
    public void setup() {
        requestProcessor = mock(BookieRequestProcessor.class);
        allChannels = mock(ChannelGroup.class);
        serverConfiguration = mock(ServerConfiguration.class);
        ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(mock(Channel.class));
        when(ctx.alloc()).thenReturn(mock(ByteBufAllocator.class));
        when(ctx.alloc().directBuffer(anyInt())).thenReturn(mock(ByteBuf.class));
        BookieRequestHandler requestHandler = mock(BookieRequestHandler.class);
        when(requestHandler.ctx()).thenReturn(ctx);
        writeEntryProcessor = spy(WriteEntryProcessor
                .create(mock(BookieProtocol.ParsedAddRequest.class), requestHandler, requestProcessor));
        requestStats = mock(RequestStats.class);
        getAddRequestStats = mock(OpStatsLogger.class);

        when(requestProcessor.getRequestStats()).thenReturn(requestStats);
        when(requestProcessor.getRequestStats().getAddRequestStats())
                .thenReturn(getAddRequestStats);
    }

    @Test
    public void testGroupFlushAddRequestStats() {
        BookieRequestHandler bookieRequestHandler =
                new BookieRequestHandler(serverConfiguration, requestProcessor, allChannels);

        BookieRequestHandler spyBrh = spy(bookieRequestHandler);

        when(spyBrh.ctx()).thenReturn(ctx);

        spyBrh.prepareSendResponseV2(0, writeEntryProcessor);

        verify(spyBrh, times(1)).markRequestStatus(anyInt(), any());

        spyBrh.flushPendingResponse();

        verify(spyBrh, times(1)).registerRequestStatus();
        verify(getAddRequestStats, times(1))
                .registerSuccessfulEvent(anyLong(), eq(TimeUnit.NANOSECONDS));

    }
}
