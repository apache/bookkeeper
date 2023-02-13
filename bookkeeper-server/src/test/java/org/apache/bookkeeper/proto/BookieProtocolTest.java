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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class BookieProtocolTest {

    @Test
    public void testAddRequestTouch() {
        byte[] data = new byte[4];
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);

        ByteBufList byteBufList = ByteBufList.get(byteBuf.retainedSlice());

        BookieProtocol.AddRequest addRequest = BookieProtocol.AddRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                0, 0, (short) 1, "test".getBytes(UTF_8), byteBufList);
        assertEquals(ReferenceCountUtil.touch(addRequest).hashCode(), addRequest.hashCode());
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
        parsedAddRequest.release();
        parsedAddRequest.recycle();
        byteBuf.release();
    }

    @Test
    public void testAddResponseTouch() {
        BookieProtocol.AddResponse addResponse = BookieProtocol.AddResponse.create(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, 0, 0, 0);
        assertEquals(ReferenceCountUtil.touch(addResponse).hashCode(), addResponse.hashCode());
    }

    @Test
    public void tesAuthRequestTouch() {
        BookkeeperProtocol.AuthMessage authMessage = BookkeeperProtocol.AuthMessage.newBuilder().build();
        BookieProtocol.AuthRequest authRequest = new BookieProtocol.AuthRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                authMessage);
        assertEquals(ReferenceCountUtil.touch(authRequest).hashCode(), authRequest.hashCode());
    }

    @Test
    public void tesAuthResponseTouch() {
        BookkeeperProtocol.AuthMessage authMessage = BookkeeperProtocol.AuthMessage.newBuilder().build();
        BookieProtocol.AuthResponse authResponse = new BookieProtocol.AuthResponse(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, authMessage);
        assertEquals(ReferenceCountUtil.touch(authResponse).hashCode(), authResponse.hashCode());
    }

    @Test
    public void testErrorResponseTouch() {
        BookieProtocol.ErrorResponse errorResponse = new BookieProtocol.ErrorResponse(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, (byte) 0, 0, 0, 0);
        assertEquals(ReferenceCountUtil.touch(errorResponse).hashCode(), errorResponse.hashCode());
    }

    @Test
    public void testReadRequestTouch() {
        BookieProtocol.ReadRequest readRequest = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                0, 0, BookieProtocol.FLAG_DO_FENCING, new byte[] {});
        assertEquals(ReferenceCountUtil.touch(readRequest).hashCode(), readRequest.hashCode());
    }

    @Test
    public void testReadResponseTouch() {
        byte[] data = new byte[4];
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
        BookieProtocol.ReadResponse readResponse = new BookieProtocol.ReadResponse(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, 0, 0, 0, byteBuf);
        assertEquals(ReferenceCountUtil.touch(readResponse).hashCode(), readResponse.hashCode());
        readResponse.release();
    }
}
