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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import java.nio.channels.ClosedChannelException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;

/**
 * Serverside handler for bookkeeper requests.
 */
@Slf4j
public class BookieRequestHandler extends ChannelInboundHandlerAdapter {

    private static final int DEFAULT_PENDING_RESPONSE_SIZE = 256;

    private final RequestProcessor requestProcessor;
    private final ChannelGroup allChannels;

    private ChannelHandlerContext ctx;

    private ByteBuf pendingSendResponses = null;
    private int maxPendingResponsesSize = DEFAULT_PENDING_RESPONSE_SIZE;

    BookieRequestHandler(ServerConfiguration conf, RequestProcessor processor, ChannelGroup allChannels) {
        this.requestProcessor = processor;
        this.allChannels = allChannels;
    }

    public ChannelHandlerContext ctx() {
        return ctx;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel connected {}", ctx.channel());
        this.ctx = ctx;
        super.channelActive(ctx);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        allChannels.add(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channels disconnected: {}", ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ClosedChannelException) {
            log.info("Client died before request could be completed on {}", ctx.channel(), cause);
            return;
        }
        log.error("Unhandled exception occurred in I/O thread or handler on {}", ctx.channel(), cause);
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof BookkeeperProtocol.Request || msg instanceof BookieProtocol.Request)) {
            ctx.fireChannelRead(msg);
            return;
        }
        requestProcessor.processRequest(msg, this);
    }

    public synchronized void prepareSendResponseV2(int rc, BookieProtocol.ParsedAddRequest req) {
        if (pendingSendResponses == null) {
            pendingSendResponses = ctx().alloc().directBuffer(maxPendingResponsesSize);
        }
        BookieProtoEncoding.ResponseEnDeCoderPreV3.serializeAddResponseInto(rc, req, pendingSendResponses);
    }

    public synchronized void flushPendingResponse() {
        if (pendingSendResponses != null) {
            maxPendingResponsesSize = (int) Math.max(
                    maxPendingResponsesSize * 0.5 + 0.5 * pendingSendResponses.readableBytes(),
                    DEFAULT_PENDING_RESPONSE_SIZE);
            if (ctx().channel().isActive()) {
                ctx().writeAndFlush(pendingSendResponses, ctx.voidPromise());
            } else {
                pendingSendResponses.release();
            }
            pendingSendResponses = null;
        }
    }
}
