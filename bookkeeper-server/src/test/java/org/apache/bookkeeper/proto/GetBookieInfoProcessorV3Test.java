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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link GetBookieInfoProcessorV3}.
 */
public class GetBookieInfoProcessorV3Test {

    private Channel channel;
    private BookieRequestProcessor requestProcessor;
    private Bookie bookie;
    private RequestStats requestStats;
    private OpStatsLogger getBookieInfoStats;
    private OpStatsLogger channelWriteStats;
    private OpStatsLogger getBookieInfoRequestStats;

    @Before
    public void setup() {
        getBookieInfoStats = mock(OpStatsLogger.class);
        channelWriteStats = mock(OpStatsLogger.class);
        getBookieInfoRequestStats = mock(OpStatsLogger.class);
        requestStats = mock(RequestStats.class);
        requestProcessor = mock(BookieRequestProcessor.class);
        bookie = mock(Bookie.class);
        when(requestProcessor.getBookie()).thenReturn(bookie);
        channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);
        when(channel.isActive()).thenReturn(true);
        when(requestProcessor.getRequestStats()).thenReturn(requestStats);
        when(requestProcessor.getRequestStats().getGetBookieInfoStats())
                .thenReturn(getBookieInfoStats);
        when(requestProcessor.getRequestStats().getChannelWriteStats())
                .thenReturn(channelWriteStats);
        when(requestProcessor.getRequestStats().getGetBookieInfoRequestStats())
                .thenReturn(getBookieInfoRequestStats);
    }

    @Test
    public void testGetBookieInfoProcessorStats() throws IOException {
        final BookkeeperProtocol.BKPacketHeader.Builder headerBuilder =
                BookkeeperProtocol.BKPacketHeader.newBuilder()
                .setVersion(BookkeeperProtocol.ProtocolVersion.VERSION_THREE)
                .setOperation(BookkeeperProtocol.OperationType.GET_BOOKIE_INFO)
                .setTxnId(0);

        final BookkeeperProtocol.GetBookieInfoRequest.Builder getBookieInfoBuilder =
                BookkeeperProtocol.GetBookieInfoRequest.newBuilder()
                        .setRequested(BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE);

        final BookkeeperProtocol.Request getBookieInfoRequest = BookkeeperProtocol.Request.newBuilder()
                .setHeader(headerBuilder)
                .setGetBookieInfoRequest(getBookieInfoBuilder)
                .build();

        GetBookieInfoProcessorV3 getBookieInfo = new GetBookieInfoProcessorV3(
                getBookieInfoRequest, channel, requestProcessor);
        getBookieInfo.run();

        // get BookieInfo succeeded.
        verify(getBookieInfoStats, times(1))
                .registerSuccessfulEvent(anyLong(), eq(TimeUnit.NANOSECONDS));

        // get BookieInfo failed.
        when(requestProcessor.getBookie().getTotalFreeSpace()).thenThrow(new IOException("test for failed."));
        getBookieInfo.run();
        verify(getBookieInfoStats, times(1))
                .registerFailedEvent(anyLong(), eq(TimeUnit.NANOSECONDS));
    }
}