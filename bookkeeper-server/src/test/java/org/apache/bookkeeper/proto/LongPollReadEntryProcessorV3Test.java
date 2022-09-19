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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;




/**
 * Unit test {@link LongPollReadEntryProcessorV3}.
 */
public class LongPollReadEntryProcessorV3Test {
    ExecutorService executor;
    HashedWheelTimer timer;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadExecutor();
        timer = new HashedWheelTimer();
    }

    @After
    public void teardown() {
        timer.stop();
        executor.shutdownNow();
    }

    @Test
    public void testWatchIsCancelledOnTimeout() throws Exception {
        Request request = Request.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                       .setTxnId(System.currentTimeMillis())
                       .setVersion(ProtocolVersion.VERSION_THREE)
                       .setOperation(OperationType.READ_ENTRY)
                       .build())
            .setReadRequest(ReadRequest.newBuilder()
                            .setLedgerId(10)
                            .setEntryId(1)
                            .setMasterKey(ByteString.copyFrom(new byte[0]))
                            .setPreviousLAC(0)
                            .setTimeOut(1)
                            .build())
            .build();

        Channel channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);
        Bookie bookie = mock(Bookie.class);

        BookieRequestProcessor requestProcessor = mock(BookieRequestProcessor.class);
        when(requestProcessor.getBookie()).thenReturn(bookie);
        when(requestProcessor.getRequestStats()).thenReturn(new RequestStats(NullStatsLogger.INSTANCE));

        when(bookie.waitForLastAddConfirmedUpdate(anyLong(), anyLong(), any()))
            .thenReturn(true);
        when(bookie.readEntry(anyLong(), anyLong())).thenReturn(Unpooled.buffer());
        when(bookie.readLastAddConfirmed(anyLong())).thenReturn(Long.valueOf(1));

        CompletableFuture<Void> cancelFuture = new CompletableFuture<>();

        doAnswer(invocationOnMock -> {
                cancelFuture.complete(null);
                return null;
            }).when(bookie).cancelWaitForLastAddConfirmedUpdate(anyLong(), any());

        LongPollReadEntryProcessorV3 processor = new LongPollReadEntryProcessorV3(
            request,
            channel,
            requestProcessor,
            executor, executor, timer);

        processor.run();

        cancelFuture.get(10, TimeUnit.SECONDS);
    }
}
