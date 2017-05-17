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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WriteEntryProcessorV3 extends PacketProcessorBaseV3 {
    private final static Logger logger = LoggerFactory.getLogger(WriteEntryProcessorV3.class);

    public WriteEntryProcessorV3(Request request, Channel channel,
                                 BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    // Returns null if there is no exception thrown
    private AddResponse getAddResponse() {
        final long startTimeNanos = MathUtils.nowInNano();
        AddRequest addRequest = request.getAddRequest();
        long ledgerId = addRequest.getLedgerId();
        long entryId = addRequest.getEntryId();

        final AddResponse.Builder addResponse = AddResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        if (!isVersionCompatible()) {
            addResponse.setStatus(StatusCode.EBADVERSION);
            return addResponse.build();
        }

        if (requestProcessor.bookie.isReadOnly()) {
            logger.warn("BookieServer is running as readonly mode, so rejecting the request from the client!");
            addResponse.setStatus(StatusCode.EREADONLY);
            return addResponse.build();
        }

        BookkeeperInternalCallbacks.WriteCallback wcb = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                                      BookieSocketAddress addr, Object ctx) {
                if (BookieProtocol.EOK == rc) {
                    requestProcessor.addEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                            TimeUnit.NANOSECONDS);
                } else {
                    requestProcessor.addEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
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
                addResponse.setStatus(status);
                Response.Builder response = Response.newBuilder()
                        .setHeader(getHeader())
                        .setStatus(addResponse.getStatus())
                        .setAddResponse(addResponse);
                Response resp = response.build();
                sendResponse(status, resp, requestProcessor.addRequestStats);
            }
        };
        StatusCode status = null;
        byte[] masterKey = addRequest.getMasterKey().toByteArray();
        ByteBuf entryToAdd = Unpooled.wrappedBuffer(addRequest.getBody().asReadOnlyByteBuffer());
        try {
            if (addRequest.hasFlag() && addRequest.getFlag().equals(AddRequest.Flag.RECOVERY_ADD)) {
                requestProcessor.bookie.recoveryAddEntry(entryToAdd, wcb, channel, masterKey);
            } else {
                requestProcessor.bookie.addEntry(entryToAdd, wcb, channel, masterKey);
            }
            status = StatusCode.EOK;
        } catch (IOException e) {
            logger.error("Error writing entry:{} to ledger:{}",
                         new Object[] { entryId, ledgerId, e });
            status = StatusCode.EIO;
        } catch (BookieException.LedgerFencedException e) {
            logger.debug("Ledger fenced while writing entry:{} to ledger:{}",
                         entryId, ledgerId);
            status = StatusCode.EFENCED;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:{} while writing entry:{}",
                         ledgerId, entryId);
            status = StatusCode.EUA;
        } catch (Throwable t) {
            logger.error("Unexpected exception while writing {}@{} : ",
                         new Object[] { entryId, ledgerId, t });
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }

        // If everything is okay, we return null so that the calling function
        // doesn't return a response back to the caller.
        if (!status.equals(StatusCode.EOK)) {
            addResponse.setStatus(status);
            return addResponse.build();
        }
        return null;
    }

    @Override
    public void safeRun() {
        AddResponse addResponse = getAddResponse();
        if (null != addResponse) {
            // This means there was an error and we should send this back.
            Response.Builder response = Response.newBuilder()
                    .setHeader(getHeader())
                    .setStatus(addResponse.getStatus())
                    .setAddResponse(addResponse);
            Response resp = response.build();
            sendResponse(addResponse.getStatus(), resp,
                         requestProcessor.addRequestStats);
        }
    }
}
