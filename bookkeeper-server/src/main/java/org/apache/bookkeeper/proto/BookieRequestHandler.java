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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;

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
    private long firstGroupEnqueueNanos = 0;
    private int sucReq = 0;
    private int faiReq = 0;
    private long sucOverNanos = 0;
    private long faiOverNanos = 0;
    private OpStatsLogger statsLogger;

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

    public synchronized void prepareSendResponseV2(int rc, WriteEntryProcessor wep) {
        if (pendingSendResponses == null) {
            pendingSendResponses = ctx().alloc().directBuffer(maxPendingResponsesSize);
            firstGroupEnqueueNanos = wep.enqueueNanos;
            statsLogger = wep.requestProcessor.getRequestStats().getAddRequestStats();
        }
        markRequestStatus(rc, wep);
        BookieProtoEncoding.ResponseEnDeCoderPreV3.serializeAddResponseInto(rc, wep.request, pendingSendResponses);
    }

    @VisibleForTesting
    public void markRequestStatus(int rc, WriteEntryProcessor wep) {
        if (BookieProtocol.EOK == rc) {
            sucReq++;
            sucOverNanos += (wep.enqueueNanos - firstGroupEnqueueNanos);
        } else {
            faiReq++;
            faiOverNanos += (wep.enqueueNanos - firstGroupEnqueueNanos);
        }
    }

    @VisibleForTesting
    public void registerRequestStatus() {
        if (statsLogger != null) {
            long avgSucEnqueueNanos = firstGroupEnqueueNanos + (sucReq > 0 ? (sucOverNanos / sucReq) : 0);
            for (int i = 0; i < sucReq; i++) {
                statsLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(avgSucEnqueueNanos), TimeUnit.NANOSECONDS);
            }

            long avgFaiEnqueueNanos = firstGroupEnqueueNanos + (faiReq > 0 ? (faiOverNanos / faiReq) : 0);
            for (int i = 0; i < faiReq; i++) {
                statsLogger.registerFailedEvent(MathUtils.elapsedNanos(avgFaiEnqueueNanos), TimeUnit.NANOSECONDS);
            }
        }

        statsLogger = null;
        firstGroupEnqueueNanos = 0;
        sucReq = 0;
        sucOverNanos = 0;
        faiReq = 0;
        faiOverNanos = 0;
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

            registerRequestStatus();
            pendingSendResponses = null;
        }
    }
}
