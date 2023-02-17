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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;

public class BookieProtocolTest {

    @Test
    public void testAddRequestTouch() {
        byte[] data = new byte[4];
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);

        ByteBufList byteBufList = ByteBufList.get(byteBuf.retainedSlice());

        BookieProtocol.AddRequest addRequest = BookieProtocol.AddRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                0, 0, (short) 1, "test".getBytes(UTF_8), byteBufList);
        assertEquals(ReferenceCountUtil.touch(addRequest).hashCode(), addRequest.hashCode());
        assertEquals(ReferenceCountUtil.touch(addRequest, "hint").hashCode(), addRequest.hashCode());
        assertEquals(ReferenceCountUtil.retain(addRequest).hashCode(), addRequest.hashCode());
        assertEquals(ReferenceCountUtil.retain(addRequest, 1).hashCode(), addRequest.hashCode());

        addRequest.recycle();
        byteBufList.release();
        byteBuf.release();
    }

    @Test
    public void testParsedAddRequestTouch() {
        byte[] data = new byte[4];
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);

        BookieProtocol.ParsedAddRequest parsedAddRequest = BookieProtocol.ParsedAddRequest.create(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, 0, 0, (short) 1, "test".getBytes(UTF_8), byteBuf);
        assertEquals(ReferenceCountUtil.touch(parsedAddRequest).hashCode(), parsedAddRequest.hashCode());
        assertEquals(ReferenceCountUtil.touch(parsedAddRequest, "hint").hashCode(), parsedAddRequest.hashCode());
        assertEquals(ReferenceCountUtil.retain(parsedAddRequest).hashCode(), parsedAddRequest.hashCode());
        assertEquals(ReferenceCountUtil.retain(parsedAddRequest, 1).hashCode(), parsedAddRequest.hashCode());

        parsedAddRequest.release();
        parsedAddRequest.recycle();
        byteBuf.release();
    }

    @Test
    public void testAddResponseTouch() {
        BookieProtocol.AddResponse addResponse = BookieProtocol.AddResponse.create(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, 0, 0, 0);
        assertEquals(ReferenceCountUtil.touch(addResponse).hashCode(), addResponse.hashCode());
        assertEquals(ReferenceCountUtil.touch(addResponse, "hint").hashCode(), addResponse.hashCode());
        assertEquals(ReferenceCountUtil.retain(addResponse).hashCode(), addResponse.hashCode());
        assertEquals(ReferenceCountUtil.retain(addResponse, 1).hashCode(), addResponse.hashCode());
    }

    @Test
    public void tesAuthRequestTouch() {
        BookkeeperProtocol.AuthMessage authMessage = BookkeeperProtocol.AuthMessage.newBuilder()
                .setAuthPluginName("test").setPayload(ByteString.copyFromUtf8("test")).build();
        BookieProtocol.AuthRequest authRequest = new BookieProtocol.AuthRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                authMessage);
        assertEquals(ReferenceCountUtil.touch(authRequest).hashCode(), authRequest.hashCode());
        assertEquals(ReferenceCountUtil.touch(authRequest, "hint").hashCode(), authRequest.hashCode());
        assertEquals(ReferenceCountUtil.retain(authRequest).hashCode(), authRequest.hashCode());
        assertEquals(ReferenceCountUtil.retain(authRequest, 1).hashCode(), authRequest.hashCode());
    }

    @Test
    public void tesAuthResponseTouch() {
        BookkeeperProtocol.AuthMessage authMessage = BookkeeperProtocol.AuthMessage.newBuilder()
                .setAuthPluginName("test").setPayload(ByteString.copyFromUtf8("test")).build();
        BookieProtocol.AuthResponse authResponse = new BookieProtocol.AuthResponse(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, authMessage);
        assertEquals(ReferenceCountUtil.touch(authResponse).hashCode(), authResponse.hashCode());
        assertEquals(ReferenceCountUtil.touch(authResponse, "hint").hashCode(), authResponse.hashCode());
        assertEquals(ReferenceCountUtil.retain(authResponse, 1).hashCode(), authResponse.hashCode());
    }

    @Test
    public void testErrorResponseTouch() {
        BookieProtocol.ErrorResponse errorResponse = new BookieProtocol.ErrorResponse(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, (byte) 0, 0, 0, 0);
        assertEquals(ReferenceCountUtil.touch(errorResponse).hashCode(), errorResponse.hashCode());
        assertEquals(ReferenceCountUtil.touch(errorResponse, "hint").hashCode(), errorResponse.hashCode());
        assertEquals(ReferenceCountUtil.retain(errorResponse).hashCode(), errorResponse.hashCode());
        assertEquals(ReferenceCountUtil.retain(errorResponse, 1).hashCode(), errorResponse.hashCode());
    }

    @Test
    public void testReadRequestTouch() {
        BookieProtocol.ReadRequest readRequest = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                0, 0, BookieProtocol.FLAG_DO_FENCING, new byte[] {});
        assertEquals(ReferenceCountUtil.touch(readRequest).hashCode(), readRequest.hashCode());
        assertEquals(ReferenceCountUtil.touch(readRequest, "hint").hashCode(), readRequest.hashCode());
        assertEquals(ReferenceCountUtil.retain(readRequest).hashCode(), readRequest.hashCode());
        assertEquals(ReferenceCountUtil.retain(readRequest, 1).hashCode(), readRequest.hashCode());
    }

    @Test
    public void testReadResponseTouch() {
        byte[] data = new byte[4];
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
        BookieProtocol.ReadResponse readResponse = new BookieProtocol.ReadResponse(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, 0, 0, 0, byteBuf);
        assertEquals(ReferenceCountUtil.touch(readResponse).hashCode(), readResponse.hashCode());
        assertEquals(ReferenceCountUtil.touch(readResponse, "1").hashCode(), readResponse.hashCode());
        assertEquals(ReferenceCountUtil.retain(readResponse).hashCode(), readResponse.hashCode());
        //This release for the Explicit retain.
        readResponse.release();
        assertEquals(ReferenceCountUtil.retain(readResponse, 1).hashCode(), readResponse.hashCode());
        //This release for the Explicit retain.
        readResponse.release();

        readResponse.release();
    }
}
