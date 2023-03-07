/*
 *
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
 *
 */
package org.apache.bookkeeper.proto;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest.Flag;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

/**
 * Test utility methods from bookie request processor.
 */
public class TestBookieRequestProcessor {

    final BookieRequestProcessor requestProcessor = mock(BookieRequestProcessor.class);

    private final ChannelGroup channelGroup = new DefaultChannelGroup(null);

    @Test
    public void testConstructLongPollThreads() throws Exception {
        // long poll threads == read threads
        ServerConfiguration conf = new ServerConfiguration();
        try (BookieRequestProcessor processor = new BookieRequestProcessor(
            conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
                channelGroup)) {
            assertSame(processor.getReadThreadPool(), processor.getLongPollThreadPool());
        }

        // force create long poll threads if there is no read threads
        conf = new ServerConfiguration();
        conf.setNumReadWorkerThreads(0);
        try (BookieRequestProcessor processor = new BookieRequestProcessor(
            conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
                channelGroup)) {
            assertNull(processor.getReadThreadPool());
            assertNotNull(processor.getLongPollThreadPool());
        }

        // long poll threads and no read threads
        conf = new ServerConfiguration();
        conf.setNumReadWorkerThreads(2);
        conf.setNumLongPollWorkerThreads(2);
        try (BookieRequestProcessor processor = new BookieRequestProcessor(
            conf, mock(Bookie.class), NullStatsLogger.INSTANCE, null, UnpooledByteBufAllocator.DEFAULT,
                channelGroup)) {
            assertNotNull(processor.getReadThreadPool());
            assertNotNull(processor.getLongPollThreadPool());
            assertNotSame(processor.getReadThreadPool(), processor.getLongPollThreadPool());
        }
    }

    @Test
    public void testFlagsV3() {
        ReadRequest read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(ReadRequest.Flag.FENCE_LEDGER).build();
        assertTrue(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(ReadRequest.Flag.ENTRY_PIGGYBACK).build();
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertTrue(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .build();
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        AddRequest add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(AddRequest.Flag.RECOVERY_ADD)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertTrue(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));

        add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertFalse(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));

        add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(AddRequest.Flag.RECOVERY_ADD)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertTrue(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));
    }

    @Test
    public void testToString() {
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder();
        headerBuilder.setVersion(ProtocolVersion.VERSION_THREE);
        headerBuilder.setOperation(OperationType.ADD_ENTRY);
        headerBuilder.setTxnId(5L);
        BKPacketHeader header = headerBuilder.build();

        AddRequest addRequest = AddRequest.newBuilder().setLedgerId(10).setEntryId(1)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).build();
        Request request = Request.newBuilder().setHeader(header).setAddRequest(addRequest).build();

        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        BookieRequestHandler requestHandler = mock(BookieRequestHandler.class);
        when(requestHandler.ctx()).thenReturn(ctx);

        WriteEntryProcessorV3 writeEntryProcessorV3 = new WriteEntryProcessorV3(request, requestHandler,
                requestProcessor);
        String toString = writeEntryProcessorV3.toString();
        assertFalse("writeEntryProcessorV3's toString should have filtered out body", toString.contains("body"));
        assertFalse("writeEntryProcessorV3's toString should have filtered out masterKey",
                toString.contains("masterKey"));
        assertTrue("writeEntryProcessorV3's toString should contain ledgerId", toString.contains("ledgerId"));
        assertTrue("writeEntryProcessorV3's toString should contain entryId", toString.contains("entryId"));
        assertTrue("writeEntryProcessorV3's toString should contain version", toString.contains("version"));
        assertTrue("writeEntryProcessorV3's toString should contain operation", toString.contains("operation"));
        assertTrue("writeEntryProcessorV3's toString should contain txnId", toString.contains("txnId"));
        assertFalse("writeEntryProcessorV3's toString shouldn't contain flag", toString.contains("flag"));
        assertFalse("writeEntryProcessorV3's toString shouldn't contain writeFlags", toString.contains("writeFlags"));

        addRequest = AddRequest.newBuilder().setLedgerId(10).setEntryId(1)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).setFlag(Flag.RECOVERY_ADD).setWriteFlags(0)
                .build();
        request = Request.newBuilder().setHeader(header).setAddRequest(addRequest).build();
        writeEntryProcessorV3 = new WriteEntryProcessorV3(request, requestHandler, requestProcessor);
        toString = writeEntryProcessorV3.toString();
        assertFalse("writeEntryProcessorV3's toString should have filtered out body", toString.contains("body"));
        assertFalse("writeEntryProcessorV3's toString should have filtered out masterKey",
                toString.contains("masterKey"));
        assertTrue("writeEntryProcessorV3's toString should contain flag", toString.contains("flag"));
        assertTrue("writeEntryProcessorV3's toString should contain writeFlags", toString.contains("writeFlags"));

        ReadRequest readRequest = ReadRequest.newBuilder().setLedgerId(10).setEntryId(23)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes())).build();
        request = Request.newBuilder().setHeader(header).setReadRequest(readRequest).build();
        toString = RequestUtils.toSafeString(request);
        assertFalse("ReadRequest's safeString should have filtered out masterKey", toString.contains("masterKey"));
        assertTrue("ReadRequest's safeString should contain ledgerId", toString.contains("ledgerId"));
        assertTrue("ReadRequest's safeString should contain entryId", toString.contains("entryId"));
        assertTrue("ReadRequest's safeString should contain version", toString.contains("version"));
        assertTrue("ReadRequest's safeString should contain operation", toString.contains("operation"));
        assertTrue("ReadRequest's safeString should contain txnId", toString.contains("txnId"));
        assertFalse("ReadRequest's safeString shouldn't contain flag", toString.contains("flag"));
        assertFalse("ReadRequest's safeString shouldn't contain previousLAC", toString.contains("previousLAC"));
        assertFalse("ReadRequest's safeString shouldn't contain timeOut", toString.contains("timeOut"));

        readRequest = ReadRequest.newBuilder().setLedgerId(10).setEntryId(23).setPreviousLAC(2).setTimeOut(100)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes())).setFlag(ReadRequest.Flag.ENTRY_PIGGYBACK)
                .build();
        request = Request.newBuilder().setHeader(header).setReadRequest(readRequest).build();
        toString = RequestUtils.toSafeString(request);
        assertFalse("ReadRequest's safeString should have filtered out masterKey", toString.contains("masterKey"));
        assertTrue("ReadRequest's safeString shouldn contain flag", toString.contains("flag"));
        assertTrue("ReadRequest's safeString shouldn contain previousLAC", toString.contains("previousLAC"));
        assertTrue("ReadRequest's safeString shouldn contain timeOut", toString.contains("timeOut"));

        WriteLacRequest writeLacRequest = WriteLacRequest.newBuilder().setLedgerId(10).setLac(23)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).build();
        request = Request.newBuilder().setHeader(header).setWriteLacRequest(writeLacRequest).build();
        WriteLacProcessorV3 writeLacProcessorV3 = new WriteLacProcessorV3(request, null, requestProcessor);
        toString = writeLacProcessorV3.toString();
        assertFalse("writeLacProcessorV3's toString should have filtered out body", toString.contains("body"));
        assertFalse("writeLacProcessorV3's toString should have filtered out masterKey",
                toString.contains("masterKey"));
        assertTrue("writeLacProcessorV3's toString should contain ledgerId", toString.contains("ledgerId"));
        assertTrue("writeLacProcessorV3's toString should contain lac", toString.contains("lac"));
        assertTrue("writeLacProcessorV3's toString should contain version", toString.contains("version"));
        assertTrue("writeLacProcessorV3's toString should contain operation", toString.contains("operation"));
        assertTrue("writeLacProcessorV3's toString should contain txnId", toString.contains("txnId"));
    }
}
