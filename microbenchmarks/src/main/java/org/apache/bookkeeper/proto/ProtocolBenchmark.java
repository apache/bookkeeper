/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.proto;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.BookieProtoEncoding.EnDecoder;
import org.apache.bookkeeper.proto.BookieProtoEncoding.RequestEnDeCoderPreV3;
import org.apache.bookkeeper.proto.BookieProtoEncoding.RequestEnDecoderV3;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.util.ByteBufList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.slf4j.MDC;

/**
 * Benchmarking serialization and deserialization.
 */
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class ProtocolBenchmark {

    @Param({"10", "100", "1000", "10000"})
    int size;

    byte[] masterKey;
    ByteBuf entry;
    long ledgerId;
    long entryId;
    short flags;
    EnDecoder reqEnDeV2;
    EnDecoder reqEnDeV3;

    @Setup
    public void prepare() {
        this.masterKey = "test-benchmark-key".getBytes(UTF_8);
        byte[] data = new byte[this.size];
        ThreadLocalRandom.current().nextBytes(data);
        this.entry = Unpooled.wrappedBuffer(data);
        this.ledgerId = ThreadLocalRandom.current().nextLong();
        this.entryId = ThreadLocalRandom.current().nextLong();
        this.flags = 1;

        // prepare the encoder
        this.reqEnDeV2 = new RequestEnDeCoderPreV3(null);
        this.reqEnDeV3 = new RequestEnDecoderV3(null);
    }

    @Benchmark
    public void testAddEntryV3() throws Exception {
        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);

        ByteBuf toSend = entry.slice();
        byte[] toSendArray = new byte[toSend.readableBytes()];
        toSend.getBytes(toSend.readerIndex(), toSendArray);
        AddRequest.Builder addBuilder = AddRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setBody(ByteString.copyFrom(toSendArray))
                .setFlag(AddRequest.Flag.RECOVERY_ADD);

        Request request = Request.newBuilder()
                .setHeader(headerBuilder)
                .setAddRequest(addBuilder)
                .build();

        Object res = this.reqEnDeV3.encode(request, ByteBufAllocator.DEFAULT);
        ReferenceCountUtil.release(res);
    }

    @Benchmark
    public void testAddEntryV3WithMdc() throws Exception {
        MDC.put("parent_id", "LetsPutSomeLongParentRequestIdHere");
        MDC.put("request_id", "LetsPutSomeLongRequestIdHere");
        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);

        ByteBuf toSend = entry.slice();
        byte[] toSendArray = new byte[toSend.readableBytes()];
        toSend.getBytes(toSend.readerIndex(), toSendArray);
        AddRequest.Builder addBuilder = AddRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setBody(ByteString.copyFrom(toSendArray))
                .setFlag(AddRequest.Flag.RECOVERY_ADD);

        Request request = PerChannelBookieClient.appendRequestContext(Request.newBuilder())
                .setHeader(headerBuilder)
                .setAddRequest(addBuilder)
                .build();

        Object res = this.reqEnDeV3.encode(request, ByteBufAllocator.DEFAULT);
        ReferenceCountUtil.release(res);
        MDC.clear();
    }

    static Request.Builder appendRequestContextNoMdc(Request.Builder builder) {
        final BookkeeperProtocol.ContextPair context1 = BookkeeperProtocol.ContextPair.newBuilder()
                .setKey("parent_id")
                .setValue("LetsPutSomeLongParentRequestIdHere")
                .build();
        builder.addRequestContext(context1);

        final BookkeeperProtocol.ContextPair context2 = BookkeeperProtocol.ContextPair.newBuilder()
                .setKey("request_id")
                .setValue("LetsPutSomeLongRequestIdHere")
                .build();
        builder.addRequestContext(context2);

        return builder;
    }

    @Benchmark
    public void testAddEntryV3WithExtraContextDataNoMdc() throws Exception {
        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);

        ByteBuf toSend = entry.slice();
        byte[] toSendArray = new byte[toSend.readableBytes()];
        toSend.getBytes(toSend.readerIndex(), toSendArray);
        AddRequest.Builder addBuilder = AddRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setBody(ByteString.copyFrom(toSendArray))
                .setFlag(AddRequest.Flag.RECOVERY_ADD);

        Request request = appendRequestContextNoMdc(Request.newBuilder())
                .setHeader(headerBuilder)
                .setAddRequest(addBuilder)
                .build();

        Object res = this.reqEnDeV3.encode(request, ByteBufAllocator.DEFAULT);
        ReferenceCountUtil.release(res);
    }
}
