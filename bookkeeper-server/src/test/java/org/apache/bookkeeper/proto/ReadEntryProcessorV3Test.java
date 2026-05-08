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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Stopwatch;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link ReadEntryProcessorV3}.
 */
public class ReadEntryProcessorV3Test {

    private Bookie bookie;
    private BookieRequestHandler requestHandler;
    private BookieRequestProcessor requestProcessor;

    @Before
    public void setup() throws Exception {
        Channel channel = mock(Channel.class);
        requestHandler = mock(BookieRequestHandler.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(requestHandler.ctx()).thenReturn(ctx);

        bookie = mock(Bookie.class);
        requestProcessor = mock(BookieRequestProcessor.class);
        when(requestProcessor.getBookie()).thenReturn(bookie);
        when(requestProcessor.getRequestStats()).thenReturn(new RequestStats(NullStatsLogger.INSTANCE));
    }

    @Test
    public void testNoReadAheadReadFlagIsPassedToBookie() throws Exception {
        long ledgerId = 1L;
        long entryId = 2L;
        when(bookie.readEntry(ledgerId, entryId, true)).thenReturn(Unpooled.wrappedBuffer(new byte[] { 1 }));
        when(bookie.readLastAddConfirmed(ledgerId)).thenReturn(entryId);

        ReadEntryProcessorV3 processor = newProcessor(ReadRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setReadFlags(BookieProtocol.FLAG_NO_READ_AHEAD)
                .build());

        ReadResponse response = processor.readEntry(ReadResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId), entryId, Stopwatch.createStarted());

        assertEquals(StatusCode.EOK, response.getStatus());
        verify(bookie).readEntry(ledgerId, entryId, true);
    }

    @Test
    public void testDefaultReadDoesNotPassNoReadAheadToBookie() throws Exception {
        long ledgerId = 1L;
        long entryId = 2L;
        when(bookie.readEntry(ledgerId, entryId, false)).thenReturn(Unpooled.wrappedBuffer(new byte[] { 1 }));
        when(bookie.readLastAddConfirmed(ledgerId)).thenReturn(entryId);

        ReadEntryProcessorV3 processor = newProcessor(ReadRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .build());

        ReadResponse response = processor.readEntry(ReadResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId), entryId, Stopwatch.createStarted());

        assertEquals(StatusCode.EOK, response.getStatus());
        verify(bookie).readEntry(ledgerId, entryId, false);
    }

    private ReadEntryProcessorV3 newProcessor(ReadRequest readRequest) throws BookieException {
        Request request = Request.newBuilder()
                .setHeader(BKPacketHeader.newBuilder()
                        .setTxnId(System.currentTimeMillis())
                        .setVersion(ProtocolVersion.VERSION_THREE)
                        .setOperation(OperationType.READ_ENTRY)
                        .build())
                .setReadRequest(readRequest)
                .build();
        return new ReadEntryProcessorV3(request, requestHandler, requestProcessor, null);
    }
}
