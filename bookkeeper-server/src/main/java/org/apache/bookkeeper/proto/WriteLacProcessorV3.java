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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WriteLacProcessorV3 extends PacketProcessorBaseV3 {
    private final static Logger logger = LoggerFactory.getLogger(WriteLacProcessorV3.class);

    public WriteLacProcessorV3(Request request, Channel channel,
                             BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    // Returns null if there is no exception thrown
    private WriteLacResponse getWriteLacResponse() {
        final long startTimeNanos = MathUtils.nowInNano();
        WriteLacRequest writeLacRequest = request.getWriteLacRequest();
        long lac = writeLacRequest.getLac();
        long ledgerId = writeLacRequest.getLedgerId();

        final WriteLacResponse.Builder writeLacResponse = WriteLacResponse.newBuilder().setLedgerId(ledgerId);

        if (!isVersionCompatible()) {
            writeLacResponse.setStatus(StatusCode.EBADVERSION);
            return writeLacResponse.build();
        }

        if (requestProcessor.bookie.isReadOnly()) {
            logger.warn("BookieServer is running as readonly mode, so rejecting the request from the client!");
            writeLacResponse.setStatus(StatusCode.EREADONLY);
            return writeLacResponse.build();
        }

        StatusCode status = null;
        ByteBuffer lacToAdd = writeLacRequest.getBody().asReadOnlyByteBuffer();
        byte[] masterKey = writeLacRequest.getMasterKey().toByteArray();

        try {
            requestProcessor.bookie.setExplicitLac(lacToAdd, channel, masterKey);
            status = StatusCode.EOK;
        } catch (IOException e) {
            logger.error("Error saving lac for ledger:{}",
                          new Object[] { lac, ledgerId, e });
            status = StatusCode.EIO;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:{} while adding lac:{}",
                                                  ledgerId, lac);
            status = StatusCode.EUA;
        } catch (Throwable t) {
            logger.error("Unexpected exception while writing {}@{} : ",
                    new Object[] { lac, t });
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }

        // If everything is okay, we return null so that the calling function
        // dosn't return a response back to the caller.
        if (status.equals(StatusCode.EOK)) {
            requestProcessor.writeLacStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.writeLacStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        }
        writeLacResponse.setStatus(status);
        return writeLacResponse.build();
    }

    @Override
    public void safeRun() {
        WriteLacResponse writeLacResponse = getWriteLacResponse();
        if (null != writeLacResponse) {
            Response.Builder response = Response.newBuilder()
                    .setHeader(getHeader())
                    .setStatus(writeLacResponse.getStatus())
                    .setWriteLacResponse(writeLacResponse);
            Response resp = response.build();
            sendResponse(writeLacResponse.getStatus(), resp, requestProcessor.writeLacStats);
        }
    }
}


