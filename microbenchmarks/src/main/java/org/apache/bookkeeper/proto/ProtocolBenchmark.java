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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.BookieProtoEncoding.RequestEnDeCoderPreV3;
import org.apache.bookkeeper.proto.BookieProtoEncoding.RequestEnDecoderV3;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.MDC;

/**
 * Benchmark protocol encoding.
 */
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class ProtocolBenchmark {

    private final long ledgerId = System.currentTimeMillis();
    private final long entryId = ledgerId + 1L;

    private final byte[] masterKey = new byte[20];
    private ByteBuf entry;

    private RequestEnDeCoderPreV3 reqEnDeV2;
    private RequestEnDecoderV3 reqEnDeV3;

    @Setup
    public void setup() {
        // prepare master key
        Random rng = new SecureRandom();
        rng.nextBytes(masterKey);

        // prepare entry data
        byte[] entryData = new byte[1024];
        rng.nextBytes(entryData);
        entry = PooledByteBufAllocator.DEFAULT.buffer(entryData.length);
        entry.writeBytes(entryData);

        // prepare the encoder
        this.reqEnDeV2 = new RequestEnDeCoderPreV3();
        this.reqEnDeV3 = new RequestEnDecoderV3();
    }

    @Benchmark
    public void testAddEntryV3() throws Exception {
        // Build the request and calculate the total size to be included in the packet.
        ByteBuf toSend = entry.slice();
        byte[] toSendArray = new byte[toSend.readableBytes()];
        toSend.getBytes(toSend.readerIndex(), toSendArray);

        Request request = new Request();
        request.setHeader()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);
        request.setAddRequest()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(masterKey)
                .setBody(toSendArray)
                .setFlag(AddRequest.Flag.RECOVERY_ADD);

        Object res = this.reqEnDeV3.encode(request, ByteBufAllocator.DEFAULT);
        ReferenceCountUtil.release(res);
    }

    @Benchmark
    public void testAddEntryV3WithMdc() throws Exception {
        MDC.put("parent_id", "LetsPutSomeLongParentRequestIdHere");
        MDC.put("request_id", "LetsPutSomeLongRequestIdHere");
        // Build the request and calculate the total size to be included in the packet.
        ByteBuf toSend = entry.slice();
        byte[] toSendArray = new byte[toSend.readableBytes()];
        toSend.getBytes(toSend.readerIndex(), toSendArray);

        Request request = new Request();
        request.setHeader()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);
        request.setAddRequest()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(masterKey)
                .setBody(toSendArray)
                .setFlag(AddRequest.Flag.RECOVERY_ADD);
        PerChannelBookieClient.appendRequestContext(request);

        Object res = this.reqEnDeV3.encode(request, ByteBufAllocator.DEFAULT);
        ReferenceCountUtil.release(res);
        MDC.clear();
    }

    static Request appendRequestContextNoMdc(Request request) {
        request.addRequestContext()
                .setKey("parent_id")
                .setValue("LetsPutSomeLongParentRequestIdHere");
        request.addRequestContext()
                .setKey("request_id")
                .setValue("LetsPutSomeLongRequestIdHere");
        return request;
    }

    @Benchmark
    public void testAddEntryV3WithExtraContextDataNoMdc() throws Exception {
        // Build the request and calculate the total size to be included in the packet.
        ByteBuf toSend = entry.slice();
        byte[] toSendArray = new byte[toSend.readableBytes()];
        toSend.getBytes(toSend.readerIndex(), toSendArray);

        Request request = new Request();
        request.setHeader()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(0L);
        request.setAddRequest()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(masterKey)
                .setBody(toSendArray)
                .setFlag(AddRequest.Flag.RECOVERY_ADD);
        appendRequestContextNoMdc(request);

        Object res = this.reqEnDeV3.encode(request, ByteBufAllocator.DEFAULT);
        ReferenceCountUtil.release(res);
    }
}
