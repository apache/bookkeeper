/**
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCountUtil;

class ReadLacProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ReadLacProcessorV3.class);

    public ReadLacProcessorV3(Request request, Channel channel,
                             BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    // Returns null if there is no exception thrown
    private ReadLacResponse getReadLacResponse() {
        final long startTimeNanos = MathUtils.nowInNano();
        ReadLacRequest readLacRequest = request.getReadLacRequest();
        long ledgerId = readLacRequest.getLedgerId();

        final ReadLacResponse.Builder readLacResponse = ReadLacResponse.newBuilder().setLedgerId(ledgerId);

        if (!isVersionCompatible()) {
            readLacResponse.setStatus(StatusCode.EBADVERSION);
            return readLacResponse.build();
        }

        logger.debug("Received ReadLac request: {}", request);
        StatusCode status = StatusCode.EOK;
        ByteBuf lastEntry = null;
        ByteBuf lac = null;
        try {
            lastEntry = requestProcessor.bookie.readEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
            lac = requestProcessor.bookie.getExplicitLac(ledgerId);
            if (lac != null) {
                readLacResponse.setLacBody(ByteString.copyFrom(lac.nioBuffer()));
                readLacResponse.setLastEntryBody(ByteString.copyFrom(lastEntry.nioBuffer()));
            } else {
                status = StatusCode.ENOENTRY;
            }
        } catch (Bookie.NoLedgerException e) {
            status = StatusCode.ENOLEDGER;
            logger.error("No ledger found while performing readLac from ledger: {}", ledgerId);
        } catch (IOException e) {
            status = StatusCode.EIO;
            logger.error("IOException while performing readLac from ledger: {}", ledgerId);
        } finally {
            ReferenceCountUtil.release(lastEntry);
            ReferenceCountUtil.release(lac);
        }

        if (status == StatusCode.EOK) {
            requestProcessor.readLacStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.readLacStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
        }
        // Finally set the status and return
        readLacResponse.setStatus(status);
        return readLacResponse.build();
    }

    @Override
    public void safeRun() {
        ReadLacResponse readLacResponse = getReadLacResponse();
        sendResponse(readLacResponse);
    }

    private void sendResponse(ReadLacResponse readLacResponse) {
        Response.Builder response = Response.newBuilder()
            .setHeader(getHeader())
            .setStatus(readLacResponse.getStatus())
            .setReadLacResponse(readLacResponse);
        sendResponse(response.getStatus(),
                response.build(),
                requestProcessor.readLacRequestStats);
    }
}
