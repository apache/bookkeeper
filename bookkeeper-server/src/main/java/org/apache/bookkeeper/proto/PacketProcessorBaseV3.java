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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.StringUtils;

/**
 * A base class for bookkeeper protocol v3 packet processors.
 */
@Slf4j
public abstract class PacketProcessorBaseV3 implements Runnable {

    Request request;
    BookieRequestHandler requestHandler;
    BookieRequestProcessor requestProcessor;
    long enqueueNanos;

    public PacketProcessorBaseV3(Request request, BookieRequestHandler requestHandler,
                                 BookieRequestProcessor requestProcessor) {
        this.request = request;
        this.requestHandler = requestHandler;
        this.requestProcessor = requestProcessor;
        this.enqueueNanos = MathUtils.nowInNano();
    }

    public PacketProcessorBaseV3() {

    }

    protected void init(Request request, BookieRequestHandler requestHandler,
                                 BookieRequestProcessor requestProcessor) {
        this.request = request;
        this.requestHandler = requestHandler;
        this.requestProcessor = requestProcessor;
        this.enqueueNanos = MathUtils.nowInNano();
    }

    protected void reset() {
        this.request = null;
        this.requestHandler = null;
        this.requestProcessor = null;
        this.enqueueNanos = -1;
    }

    protected void sendResponse(StatusCode code, Object response, OpStatsLogger statsLogger) {
        final long writeNanos = MathUtils.nowInNano();

        Channel channel = requestHandler.ctx().channel();
        final long timeOut = requestProcessor.getWaitTimeoutOnBackpressureMillis();
        // save as local variable to make sure `channelWriteStats` and `enqueueNanos`
        // not be reset in ChannelFutureListener
        final OpStatsLogger channelWriteStats = requestProcessor.getRequestStats().getChannelWriteStats();
        final long finalEnqueueNanos = enqueueNanos;

        if (timeOut >= 0 && !channel.isWritable()) {
            if (!requestProcessor.isBlacklisted(channel)) {
                synchronized (channel) {
                    if (!channel.isWritable() && !requestProcessor.isBlacklisted(channel)) {
                        final long waitUntilNanos = writeNanos + TimeUnit.MILLISECONDS.toNanos(timeOut);
                        while (!channel.isWritable() && MathUtils.nowInNano() < waitUntilNanos) {
                            try {
                                TimeUnit.MILLISECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                break;
                            }
                        }
                        if (!channel.isWritable()) {
                            requestProcessor.blacklistChannel(channel);
                            requestProcessor.handleNonWritableChannel(channel);
                        }
                    }
                }
            }

            if (!channel.isWritable()) {
                log.warn("cannot write response to non-writable channel {} for request {}", channel,
                        StringUtils.requestToString(request));
                channelWriteStats.registerFailedEvent(MathUtils.elapsedNanos(writeNanos), TimeUnit.NANOSECONDS);
                statsLogger.registerFailedEvent(MathUtils.elapsedNanos(finalEnqueueNanos), TimeUnit.NANOSECONDS);
                return;
            } else {
                requestProcessor.invalidateBlacklist(channel);
            }
        }

        if (channel.isActive()) {
            channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    long writeElapsedNanos = MathUtils.elapsedNanos(writeNanos);
                    if (!future.isSuccess()) {
                        channelWriteStats.registerFailedEvent(writeElapsedNanos, TimeUnit.NANOSECONDS);
                    } else {
                        channelWriteStats.registerSuccessfulEvent(writeElapsedNanos, TimeUnit.NANOSECONDS);
                    }
                    if (StatusCode.EOK == code) {
                        statsLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(finalEnqueueNanos),
                                TimeUnit.NANOSECONDS);
                    } else {
                        statsLogger.registerFailedEvent(MathUtils.elapsedNanos(finalEnqueueNanos),
                                TimeUnit.NANOSECONDS);
                    }
                }
            });
        } else {
            log.debug("Netty channel {} is inactive, "
                    + "hence bypassing netty channel writeAndFlush during sendResponse", channel);
        }
    }

    protected boolean isVersionCompatible() {
        return this.request.getHeader().getVersion().equals(ProtocolVersion.VERSION_THREE);
    }

    /**
     * Build a header with protocol version 3 and the operation type same as what was in the
     * request.
     * @return
     */
    protected BKPacketHeader getHeader() {
        BKPacketHeader.Builder header = BKPacketHeader.newBuilder();
        header.setVersion(ProtocolVersion.VERSION_THREE);
        header.setOperation(request.getHeader().getOperation());
        header.setTxnId(request.getHeader().getTxnId());
        return header.build();
    }

    @Override
    public String toString() {
        return request.toString();
    }
}
