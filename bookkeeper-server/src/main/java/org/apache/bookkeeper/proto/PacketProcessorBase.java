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

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class PacketProcessorBase implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(PacketProcessorBase.class);
    final Request request;
    final Channel channel;
    final Bookie bookie;

    PacketProcessorBase(Request request, Channel channel, Bookie bookie) {
        this.request = request;
        this.channel = channel;
        this.bookie = bookie;
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

    @Override
    public void run() {
        if (!isVersionCompatible()) {
            channel.write(ResponseBuilder.buildErrorResponse(BookieProtocol.EBADVERSION, request));
            return;
        }
        processPacket();
    }

    protected abstract void processPacket();
}
