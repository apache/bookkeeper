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

import io.netty.channel.Channel;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceLedgerRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceLedgerResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ForceLedgerProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ForceLedgerProcessorV3.class);

    public ForceLedgerProcessorV3(Request request, Channel channel,
                             BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    // Returns null if there is no exception thrown
    private ForceLedgerResponse getForceLedgerResponse() {
        final long startTimeNanos = MathUtils.nowInNano();
        ForceLedgerRequest forceLedgerRequest = request.getForceLedgerRequest();
        long ledgerId = forceLedgerRequest.getLedgerId();

        final ForceLedgerResponse.Builder forceLedgerResponse = ForceLedgerResponse.newBuilder().setLedgerId(ledgerId);

        if (!isVersionCompatible()) {
            forceLedgerResponse.setStatus(StatusCode.EBADVERSION);
            return forceLedgerResponse.build();
        }

        if (requestProcessor.getBookie().isReadOnly()) {
            logger.warn("BookieServer is running as readonly mode, so rejecting the request from the client!");
            forceLedgerResponse.setStatus(StatusCode.EREADONLY);
            return forceLedgerResponse.build();
        }

        BookkeeperInternalCallbacks.WriteCallback wcb = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                                      BookieSocketAddress addr, Object ctx) {
                if (BookieProtocol.EOK == rc) {
                    requestProcessor.getForceLedgerStats()
                            .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                            TimeUnit.NANOSECONDS);
                } else {
                    requestProcessor.getForceLedgerStats()
                            .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                            TimeUnit.NANOSECONDS);
                }

                StatusCode status;
                switch (rc) {
                    case BookieProtocol.EOK:
                        status = StatusCode.EOK;
                        break;
                    case BookieProtocol.EIO:
                        status = StatusCode.EIO;
                        break;
                    default:
                        status = StatusCode.EUA;
                        break;
                }
                forceLedgerResponse.setStatus(status);
                Response.Builder response = Response.newBuilder()
                        .setHeader(getHeader())
                        .setStatus(forceLedgerResponse.getStatus())
                        .setForceLedgerResponse(forceLedgerResponse);
                Response resp = response.build();
                sendResponse(status, resp, requestProcessor.getForceLedgerRequestStats());
            }
        };
        StatusCode status = null;
        byte[] masterKey = forceLedgerRequest.getMasterKey().toByteArray();
        try {
            requestProcessor.getBookie().forceLedger(ledgerId, wcb, channel, masterKey);
            status = StatusCode.EOK;
        } catch (Throwable t) {
            logger.error("Unexpected exception while forcing ledger {} : ", ledgerId, t);
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }

        // If everything is okay, we return null so that the calling function
        // doesn't return a response back to the caller.
        if (!status.equals(StatusCode.EOK)) {
            forceLedgerResponse.setStatus(status);
            return forceLedgerResponse.build();
        }
        return null;
    }

    @Override
    public void safeRun() {
        ForceLedgerResponse forceLedgerResponse = getForceLedgerResponse();
        if (null != forceLedgerResponse) {
            Response.Builder response = Response.newBuilder()
                    .setHeader(getHeader())
                    .setStatus(forceLedgerResponse.getStatus())
                    .setForceLedgerResponse(forceLedgerResponse);
            Response resp = response.build();
            sendResponse(forceLedgerResponse.getStatus(), resp, requestProcessor.getForceLedgerRequestStats());
        }
    }

    /**
     * this toString method filters out body and masterKey from the output.
     * masterKey contains the password of the ledger and body is customer data,
     * so it is not appropriate to have these in logs or system output.
     */
    @Override
    public String toString() {
        return RequestUtils.toSafeString(request);
    }
}


