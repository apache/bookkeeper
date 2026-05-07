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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.proto.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.GetBookieInfoResponse;
import org.apache.bookkeeper.proto.Request;
import org.apache.bookkeeper.proto.Response;
import org.apache.bookkeeper.proto.StatusCode;

/**
 * A processor class for v3 bookie metadata packets.
 */
@CustomLog
public class GetBookieInfoProcessorV3 extends PacketProcessorBaseV3 implements Runnable {

    public GetBookieInfoProcessorV3(Request request, BookieRequestHandler requestHandler,
                                     BookieRequestProcessor requestProcessor) {
        super(request, requestHandler, requestProcessor);
    }

    private GetBookieInfoResponse getGetBookieInfoResponse() {
        long startTimeNanos = MathUtils.nowInNano();
        GetBookieInfoRequest getBookieInfoRequest = request.getGetBookieInfoRequest();
        long requested = getBookieInfoRequest.getRequested();

        GetBookieInfoResponse getBookieInfoResponse = new GetBookieInfoResponse();

        if (!isVersionCompatible()) {
            getBookieInfoResponse.setStatus(StatusCode.EBADVERSION);
            requestProcessor.getRequestStats().getGetBookieInfoStats()
                .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            return getBookieInfoResponse;
        }

        log.debug().attr("request", request).log("Received new getBookieInfo request");
        StatusCode status = StatusCode.EOK;
        long freeDiskSpace = 0L, totalDiskSpace = 0L;
        try {
            if ((requested & GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE) != 0) {
                freeDiskSpace = requestProcessor.getBookie().getTotalFreeSpace();
                getBookieInfoResponse.setFreeDiskSpace(freeDiskSpace);
            }
            if ((requested & GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE) != 0) {
                totalDiskSpace = requestProcessor.getBookie().getTotalDiskSpace();
                getBookieInfoResponse.setTotalDiskCapacity(totalDiskSpace);
            }
            log.debug()
                    .attr("freeDiskSpace", freeDiskSpace)
                    .attr("totalDiskSpace", totalDiskSpace)
                    .log("FreeDiskSpace info");
            requestProcessor.getRequestStats().getGetBookieInfoStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        } catch (IOException e) {
            status = StatusCode.EIO;
            log.error().exception(e).log("IOException while getting freespace/totalspace");
            requestProcessor.getRequestStats().getGetBookieInfoStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        }

        getBookieInfoResponse.setStatus(status);
        return getBookieInfoResponse;
    }

    @Override
    public void run() {
        GetBookieInfoResponse getBookieInfoResponse = getGetBookieInfoResponse();
        sendResponse(getBookieInfoResponse);
    }

    private void sendResponse(GetBookieInfoResponse getBookieInfoResponse) {
        Response response = new Response();
        response.setHeader().copyFrom(getHeader());
        response.setStatus(getBookieInfoResponse.getStatus());
        response.setGetBookieInfoResponse().copyFrom(getBookieInfoResponse);
        sendResponse(response.getStatus(), response,
                     requestProcessor.getRequestStats().getGetBookieInfoRequestStats());
    }
}
