/**
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

import io.netty.channel.Channel;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for bookeeper packet processors.
 */
abstract class PacketProcessorBase<T extends Request> extends SafeRunnable {
    private static final Logger logger = LoggerFactory.getLogger(PacketProcessorBase.class);
    T request;
    Channel channel;
    BookieRequestProcessor requestProcessor;
    long enqueueNanos;

    protected void init(T request, Channel channel, BookieRequestProcessor requestProcessor) {
        this.request = request;
        this.channel = channel;
        this.requestProcessor = requestProcessor;
        this.enqueueNanos = MathUtils.nowInNano();
    }

    protected void reset() {
        request = null;
        channel = null;
        requestProcessor = null;
        enqueueNanos = -1;
    }

    protected boolean isVersionCompatible() {
        byte version = request.getProtocolVersion();
        if (version < BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                || version > BookieProtocol.CURRENT_PROTOCOL_VERSION) {
            logger.error("Invalid protocol version, expected something between "
                    + BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                    + " & " + BookieProtocol.CURRENT_PROTOCOL_VERSION
                    + ". got " + request.getProtocolVersion());
            return false;
        }
        return true;
    }

    protected void sendResponse(int rc, Object response, OpStatsLogger statsLogger) {
        channel.writeAndFlush(response, channel.voidPromise());
        if (BookieProtocol.EOK == rc) {
            statsLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        } else {
            statsLogger.registerFailedEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void safeRun() {
        if (!isVersionCompatible()) {
            sendResponse(BookieProtocol.EBADVERSION,
                         ResponseBuilder.buildErrorResponse(BookieProtocol.EBADVERSION, request),
                         requestProcessor.getRequestStats().getReadRequestStats());
            return;
        }
        processPacket();
    }

    protected abstract void processPacket();
}
