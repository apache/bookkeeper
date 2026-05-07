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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.proto.ReadLacRequest;
import org.apache.bookkeeper.proto.ReadLacResponse;
import org.apache.bookkeeper.proto.Request;
import org.apache.bookkeeper.proto.Response;
import org.apache.bookkeeper.proto.StatusCode;

/**
 * A read processor for v3 last add confirmed messages.
 */
@CustomLog
class ReadLacProcessorV3 extends PacketProcessorBaseV3 implements Runnable {

    public ReadLacProcessorV3(Request request, BookieRequestHandler requestHandler,
                             BookieRequestProcessor requestProcessor) {
        super(request, requestHandler, requestProcessor);
    }

    // Returns null if there is no exception thrown
    private ReadLacResponse getReadLacResponse() {
        final long startTimeNanos = MathUtils.nowInNano();
        ReadLacRequest readLacRequest = request.getReadLacRequest();
        long ledgerId = readLacRequest.getLedgerId();

        final ReadLacResponse readLacResponse = new ReadLacResponse().setLedgerId(ledgerId);

        if (!isVersionCompatible()) {
            readLacResponse.setStatus(StatusCode.EBADVERSION);
            return readLacResponse;
        }

        log.debug().attr("request", request).log("Received ReadLac request");
        StatusCode status = StatusCode.EOK;
        ByteBuf lastEntry = null;
        ByteBuf lac = null;
        try {
            lac = requestProcessor.bookie.getExplicitLac(ledgerId);
            if (lac != null) {
                readLacResponse.setLacBody(ByteBufUtil.getBytes(lac));
            }
        } catch (Bookie.NoLedgerException e) {
            status = StatusCode.ENOLEDGER;
            log.debug()
                    .exception(e)
                    .attr("ledgerId", ledgerId)
                    .log("No ledger found while performing readLac");
        } catch (BookieException.DataUnknownException e) {
            status = StatusCode.EUNKNOWNLEDGERSTATE;
            log.error()
                    .exception(e)
                    .attr("ledgerId", ledgerId)
                    .log("Ledger in unknown state and cannot serve readLac requests");
        } catch (BookieException | IOException e) {
            status = StatusCode.EIO;
            log.error()
                    .exception(e)
                    .attr("ledgerId", ledgerId)
                    .log("IOException while performing readLac from ledger");
        } finally {
            ReferenceCountUtil.release(lac);
        }

        try {
            lastEntry = requestProcessor.bookie.readEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
            if (lastEntry != null) {
                readLacResponse.setLastEntryBody(ByteBufUtil.getBytes(lastEntry));
            }
        } catch (Bookie.NoLedgerException e) {
            status = StatusCode.ENOLEDGER;
            log.debug()
                    .exception(e)
                    .attr("ledgerId", ledgerId)
                    .log("No ledger found while trying to read last entry");
        } catch (BookieException.DataUnknownException e) {
            status = StatusCode.EUNKNOWNLEDGERSTATE;
            log.error()
                    .exception(e)
                    .attr("ledgerId", ledgerId)
                    .log("Ledger in an unknown state while trying to read last entry");
        } catch (BookieException | IOException e) {
            status = StatusCode.EIO;
            log.error()
                    .exception(e)
                    .attr("ledgerId", ledgerId)
                    .log("IOException while trying to read last entry");
        } finally {
            ReferenceCountUtil.release(lastEntry);
        }

        if ((lac == null) && (lastEntry == null)) {
            status = StatusCode.ENOENTRY;
        }

        if (status == StatusCode.EOK) {
            requestProcessor.getRequestStats().getReadLacStats()
                .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.getRequestStats().getReadLacStats()
                .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        }
        // Finally set the status and return
        readLacResponse.setStatus(status);
        return readLacResponse;
    }

    @Override
    public void run() {
        ReadLacResponse readLacResponse = getReadLacResponse();
        sendResponse(readLacResponse);
    }

    private void sendResponse(ReadLacResponse readLacResponse) {
        Response response = new Response();
        response.setHeader().copyFrom(getHeader());
        response.setStatus(readLacResponse.getStatus());
        response.setReadLacResponse().copyFrom(readLacResponse);
        sendResponse(response.getStatus(),
                response,
                requestProcessor.getRequestStats().getReadLacRequestStats());
    }
}
