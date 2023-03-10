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

import static org.apache.bookkeeper.proto.BookieProtocol.ADDENTRY;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;

/**
 * Serverside handler for bookkeeper requests.
 */
@Slf4j
public class BookieRequestHandler extends ChannelInboundHandlerAdapter {
    private static final int DEFAULT_CAPACITY = 1_000;
    static final Object EVENT_FLUSH_ALL_PENDING_RESPONSES = new Object();
    private final int maxCapacity;

    private final RequestProcessor requestProcessor;
    private final ChannelGroup allChannels;

    private ChannelHandlerContext ctx;
    private final BlockingQueue<BookieProtocol.ParsedAddRequest> msgs;

    private ByteBuf pendingSendResponses = null;
    private int maxPendingResponsesSize;

    BookieRequestHandler(ServerConfiguration conf, RequestProcessor processor, ChannelGroup allChannels) {
        this.requestProcessor = processor;
        this.allChannels = allChannels;
        this.maxCapacity = conf.getMaxAddsInProgressLimit() > 0 ? conf.getMaxAddsInProgressLimit() : DEFAULT_CAPACITY;
        this.msgs = new ArrayBlockingQueue<>(maxCapacity);
    }

    public ChannelHandlerContext ctx() {
        return ctx;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel connected  {}", ctx.channel());
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

        if (msg instanceof BookieProtocol.ParsedAddRequest
            && ADDENTRY == ((BookieProtocol.ParsedAddRequest) msg).getOpCode()
            && !((BookieProtocol.ParsedAddRequest) msg).isHighPriority()
            && ((BookieProtocol.ParsedAddRequest) msg).getProtocolVersion() == BookieProtocol.CURRENT_PROTOCOL_VERSION
            && !((BookieProtocol.ParsedAddRequest) msg).isRecoveryAdd()) {
            msgs.put((BookieProtocol.ParsedAddRequest) msg);
            if (msgs.size() >= maxCapacity) {
                int count = msgs.size();
                List<BookieProtocol.ParsedAddRequest> c = new ArrayList<>(count);
                msgs.drainTo(c, count);
                requestProcessor.processAddRequest(c, this);
            }
        } else {
            requestProcessor.processRequest(msg, this);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (!msgs.isEmpty()) {
            int count = msgs.size();
            List<BookieProtocol.ParsedAddRequest> c = new ArrayList<>(count);
            msgs.drainTo(c, count);
            requestProcessor.processAddRequest(c, this);
        }
    }

    public synchronized void prepareSendResponseV2(int rc, BookieProtocol.ParsedAddRequest req) {
        if (pendingSendResponses == null) {
            pendingSendResponses = ctx.alloc().directBuffer(maxPendingResponsesSize != 0
                    ? maxPendingResponsesSize : 256);
        }

        BookieProtoEncoding.ResponseEnDeCoderPreV3.serializeAddResponseInto(rc, req, pendingSendResponses);
    }

    public synchronized void prepareSendResponseV2(int rc, byte version, byte opCode, long ledgerId, long entryId) {
        if (pendingSendResponses == null) {
            pendingSendResponses = ctx.alloc().directBuffer(maxPendingResponsesSize != 0
                ? maxPendingResponsesSize : 256);
        }
        BookieProtoEncoding.ResponseEnDeCoderPreV3.serializeAddResponseInto(rc, version, opCode, ledgerId, entryId,
            pendingSendResponses);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt == EVENT_FLUSH_ALL_PENDING_RESPONSES) {
            synchronized (this) {
                if (pendingSendResponses != null) {
                    maxPendingResponsesSize = Math.max(maxPendingResponsesSize,
                            pendingSendResponses.readableBytes());
                    if (ctx.channel().isActive()) {
                        ctx.writeAndFlush(pendingSendResponses, ctx.voidPromise());
                    } else {
                        pendingSendResponses.release();
                    }

                    pendingSendResponses = null;
                }
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
