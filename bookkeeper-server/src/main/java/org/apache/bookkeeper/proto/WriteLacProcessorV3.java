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

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacResponse;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class WriteLacProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WriteLacProcessorV3.class);

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

        BookkeeperInternalCallbacks.WriteCallback writeCallback = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                if (BookieProtocol.EOK == rc) {
                    requestProcessor.getRequestStats().getWriteLacStats()
                        .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
                } else {
                    requestProcessor.getRequestStats().getWriteLacStats()
                        .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
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
                writeLacResponse.setStatus(status);
                Response.Builder response = Response.newBuilder()
                        .setHeader(getHeader())
                        .setStatus(writeLacResponse.getStatus())
                        .setWriteLacResponse(writeLacResponse);
                Response resp = response.build();
                sendResponse(status, resp, requestProcessor.getRequestStats().getWriteLacRequestStats());
            }
        };

        StatusCode status = null;
        ByteBuffer lacToAdd = writeLacRequest.getBody().asReadOnlyByteBuffer();
        byte[] masterKey = writeLacRequest.getMasterKey().toByteArray();

        try {
            requestProcessor.bookie.setExplicitLac(Unpooled.wrappedBuffer(lacToAdd), writeCallback, channel, masterKey);
            status = StatusCode.EOK;
        } catch (IOException e) {
            logger.error("Error saving lac {} for ledger:{}",
                    lac, ledgerId, e);
            status = StatusCode.EIO;
        } catch (InterruptedException  e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while saving lac {} for ledger:{}",
                    lac, ledgerId, e);
            status = StatusCode.EIO;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:{} while adding lac:{}",
                    ledgerId, lac, e);
            status = StatusCode.EUA;
        } catch (Throwable t) {
            logger.error("Unexpected exception while writing lac {} for ledger:{}",
                    lac, ledgerId, t);
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }

        // If everything is okay, we return null so that the calling function
        // dosn't return a response back to the caller.
        if (!status.equals(StatusCode.EOK)) {
            requestProcessor.getRequestStats().getWriteLacStats()
                .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            writeLacResponse.setStatus(status);
            return writeLacResponse.build();
        }
        return null;
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
            sendResponse(
                writeLacResponse.getStatus(),
                resp,
                requestProcessor.getRequestStats().getWriteLacRequestStats());
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


