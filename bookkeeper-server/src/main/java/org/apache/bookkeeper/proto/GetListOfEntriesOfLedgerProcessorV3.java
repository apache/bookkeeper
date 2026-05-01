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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetListOfEntriesOfLedgerRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetListOfEntriesOfLedgerResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;

/**
 * A processor class for v3 entries of a ledger packets.
 */
@CustomLog
public class GetListOfEntriesOfLedgerProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    protected final GetListOfEntriesOfLedgerRequest getListOfEntriesOfLedgerRequest;
    protected final long ledgerId;

    public GetListOfEntriesOfLedgerProcessorV3(Request request, BookieRequestHandler requestHandler,
            BookieRequestProcessor requestProcessor) {
        super(request, requestHandler, requestProcessor);
        this.getListOfEntriesOfLedgerRequest = request.getGetListOfEntriesOfLedgerRequest();
        this.ledgerId = getListOfEntriesOfLedgerRequest.getLedgerId();
    }

    private GetListOfEntriesOfLedgerResponse getListOfEntriesOfLedgerResponse() {
        long startTimeNanos = MathUtils.nowInNano();

        GetListOfEntriesOfLedgerResponse.Builder getListOfEntriesOfLedgerResponse = GetListOfEntriesOfLedgerResponse
                .newBuilder();
        getListOfEntriesOfLedgerResponse.setLedgerId(ledgerId);

        if (!isVersionCompatible()) {
            getListOfEntriesOfLedgerResponse.setStatus(StatusCode.EBADVERSION);
            requestProcessor.getRequestStats().getGetListOfEntriesOfLedgerStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            return getListOfEntriesOfLedgerResponse.build();
        }

        log.debug().attr("request", request).log("Received new getListOfEntriesOfLedger request");
        StatusCode status = StatusCode.EOK;
        AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = null;
        try {
            availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    requestProcessor.bookie.getListOfEntriesOfLedger(ledgerId));
            getListOfEntriesOfLedgerResponse.setAvailabilityOfEntriesOfLedger(
                    ByteString.copyFrom(availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger()));

        } catch (Bookie.NoLedgerException e) {
            status = StatusCode.ENOLEDGER;
            log.error()
                    .exception(e)
                    .attr("ledgerId", ledgerId)
                    .log("No ledger found while performing getListOfEntriesOfLedger");
        } catch (IOException e) {
            status = StatusCode.EIO;
            log.error()
                    .attr("ledgerId", ledgerId)
                    .log("IOException while performing getListOfEntriesOfLedger from ledger");
        }

        if (status == StatusCode.EOK) {
            requestProcessor.getRequestStats().getListOfEntriesOfLedgerStats
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.getRequestStats().getListOfEntriesOfLedgerStats
                    .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        }
        // Finally set the status and return
        getListOfEntriesOfLedgerResponse.setStatus(status);
        return getListOfEntriesOfLedgerResponse.build();
    }

    @Override
    public void run() {
        GetListOfEntriesOfLedgerResponse listOfEntriesOfLedgerResponse = getListOfEntriesOfLedgerResponse();
        Response.Builder response = Response.newBuilder().setHeader(getHeader())
                .setStatus(listOfEntriesOfLedgerResponse.getStatus())
                .setGetListOfEntriesOfLedgerResponse(listOfEntriesOfLedgerResponse);
        Response resp = response.build();
        sendResponse(listOfEntriesOfLedgerResponse.getStatus(), resp,
                requestProcessor.getRequestStats().getListOfEntriesOfLedgerRequestStats);
    }
}
