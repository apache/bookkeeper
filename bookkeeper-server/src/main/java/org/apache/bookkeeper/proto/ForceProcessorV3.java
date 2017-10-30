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

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import java.io.IOException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceResponse;

class ForceProcessorV3 extends PacketProcessorBaseV3 implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(ForceProcessorV3.class);

    public ForceProcessorV3(Request request, Channel channel,
        BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    // Returns null if there is no exception thrown
    private ForceResponse getForceResponse() {
        final long startTimeNanos = MathUtils.nowInNano();
        ForceRequest forceRequest = request.getForceRequest();
        long ledgerId = forceRequest.getLedgerId();
        byte[] masterKey = forceRequest.getMasterKey().toByteArray();

        final ForceResponse.Builder forceResponse = ForceResponse.newBuilder().setLedgerId(ledgerId);

        if (!isVersionCompatible()) {
            forceResponse.setStatus(StatusCode.EBADVERSION);
            return forceResponse.build();
        }

        if (requestProcessor.bookie.isReadOnly()) {
            logger.warn("BookieServer is running as readonly mode, so rejecting the request from the client!");
            forceResponse.setStatus(StatusCode.EREADONLY);
            return forceResponse.build();
        }

        BookkeeperInternalCallbacks.ForceCallback wcb = new BookkeeperInternalCallbacks.ForceCallback() {
            @Override
            public void forceComplete(int rc, long ledgerId, long lastAddSynced, BookieSocketAddress addr, Object ctx) {
                logger.debug("sync ledg "+ledgerId+", lastAddSyncedEntry:"+lastAddSynced);
                if (BookieProtocol.EOK == rc) {
                    requestProcessor.forceStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                            TimeUnit.NANOSECONDS);
                } else {
                    requestProcessor.forceStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
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
                forceResponse.setStatus(status);
                forceResponse.setLastAddSynced(lastAddSynced);

                Response.Builder response = Response.newBuilder()
                        .setHeader(getHeader())
                        .setStatus(forceResponse.getStatus())
                        .setForceResponse(forceResponse);
                Response resp = response.build();
                sendResponse(status, resp, requestProcessor.forceRequestStats);
            }
        };

        StatusCode status;
        try {
            requestProcessor.bookie.force(ledgerId, wcb, channel, masterKey);
            status = StatusCode.EOK;
        } catch (IOException e) {
            logger.error("Error forcing ledger {}", new Object[] { ledgerId }, e);
            status = StatusCode.EIO;
        } catch (BookieException.LedgerFencedException e) {
            logger.error("ledger fenced during force of ledger {}", new Object[] { ledgerId }, e);
            status = StatusCode.EFENCED;
        } catch (BookieException e) {
            logger.error("unauthorized access during force of ledger {}", new Object[] { ledgerId }, e);
            status = StatusCode.EUA;
        } catch (Throwable t) {
            logger.error("Unexpected exception while forcing ledger {} ", new Object[] { ledgerId}, t);
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }

       // If everything is okay, we return null so that the calling function
        // doesn't return a response back to the caller.
        if (!status.equals(StatusCode.EOK)) {
            forceResponse.setStatus(status);
            forceResponse.setLastAddSynced(BookieProtocol.INVALID_ENTRY_ID);
            return forceResponse.build();
        }
        return null;
    }

    @Override
    public void safeRun() {
        ForceResponse forceResponse = getForceResponse();
        if (null != forceResponse) {
            Response.Builder response = Response.newBuilder()
                .setHeader(getHeader())
                .setStatus(forceResponse.getStatus())
                .setForceResponse(forceResponse);
            Response resp = response.build();
            sendResponse(forceResponse.getStatus(), resp, requestProcessor.forceRequestStats);
        }
    }
}