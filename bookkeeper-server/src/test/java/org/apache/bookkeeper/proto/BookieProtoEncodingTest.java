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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.apache.bookkeeper.proto.BookieProtoEncoding.RequestEnDeCoderPreV3;
import org.apache.bookkeeper.proto.BookieProtoEncoding.RequestEnDecoderV3;
import org.apache.bookkeeper.proto.BookieProtoEncoding.ResponseDecoder;
import org.apache.bookkeeper.proto.BookieProtoEncoding.ResponseEnDeCoderPreV3;
import org.apache.bookkeeper.proto.BookieProtoEncoding.ResponseEnDecoderV3;
import org.apache.bookkeeper.proto.BookieProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest.Flag;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link BookieProtoEncoding}.
 */
public class BookieProtoEncodingTest {

    private ExtensionRegistry registry;

    @Before
    public void setup() {
        this.registry = ExtensionRegistry.newInstance();
    }

    @Test
    public void testV3ResponseDecoderNoFallback() throws Exception {
        AddResponse v2Resp = AddResponse.create(
            BookieProtocol.CURRENT_PROTOCOL_VERSION,
            BookieProtocol.EOK,
            1L,
            2L);

        BookkeeperProtocol.Response v3Resp = BookkeeperProtocol.Response.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setTxnId(1L)
                .setOperation(OperationType.ADD_ENTRY)
                .build())
            .setStatus(StatusCode.EOK)
            .setAddResponse(BookkeeperProtocol.AddResponse.newBuilder()
                .setStatus(StatusCode.EOK)
                .setLedgerId(1L)
                .setEntryId(2L)
                .build())
            .build();

        List<Object> outList = Lists.newArrayList();
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.fireChannelRead(any())).thenAnswer((iom) -> {
                outList.add(iom.getArgument(0));
                return null;
        });

        ResponseEnDeCoderPreV3 v2Encoder = new ResponseEnDeCoderPreV3(registry);
        ResponseEnDecoderV3 v3Encoder = new ResponseEnDecoderV3(registry);

        ResponseDecoder v3Decoder = new ResponseDecoder(registry, false, false);
        try {
            v3Decoder.channelRead(ctx,
                v2Encoder.encode(v2Resp, UnpooledByteBufAllocator.DEFAULT)
            );
            fail("V3 response decoder should fail on decoding v2 response");
        } catch (InvalidProtocolBufferException e) {
            // expected
        }
        assertEquals(0, outList.size());

        v3Decoder.channelRead(
            ctx,
            v3Encoder.encode(v3Resp, UnpooledByteBufAllocator.DEFAULT));
        assertEquals(1, outList.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testV2RequestDecoderThrowExceptionOnUnknownRequests() throws Exception {
        RequestEnDeCoderPreV3 v2ReqEncoder = new RequestEnDeCoderPreV3(registry);
        RequestEnDecoderV3 v3ReqEncoder = new RequestEnDecoderV3(registry);

        BookkeeperProtocol.Request v3Req = BookkeeperProtocol.Request.newBuilder()
            .setHeader(BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setTxnId(1L)
                .setOperation(OperationType.ADD_ENTRY)
                .build())
            .setAddRequest(BookkeeperProtocol.AddRequest.newBuilder()
                .setLedgerId(1L)
                .setEntryId(2L)
                .setMasterKey(ByteString.copyFrom("", UTF_8))
                .setFlag(Flag.RECOVERY_ADD)
                .setBody(ByteString.copyFrom("test", UTF_8)))
            .build();


        v2ReqEncoder.decode((ByteBuf) v3ReqEncoder.encode(v3Req, UnpooledByteBufAllocator.DEFAULT));
    }

}
